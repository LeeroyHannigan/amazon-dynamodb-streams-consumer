package com.amazon.dynamodbstreams.consumer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.Locale;

/**
 * Locates the {@code amazon-dynamodb-streams-consumer-sidecar} binary. Maven
 * ships a jar, not a native binary, so — like the Go, Node, and .NET clients —
 * the sidecar is downloaded once from the GitHub Release (checksum-verified) and
 * cached. Resolution order: explicit path → env override → cached download →
 * download → PATH.
 */
final class Sidecar {
    private static final String BINARY = "amazon-dynamodb-streams-consumer-sidecar";
    static final String VERSION = "0.1.3";
    private static final String DEFAULT_RELEASE_BASE =
            "https://github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/releases/download";

    private Sidecar() {
    }

    private static String releaseBase() {
        String env = System.getenv("DDB_STREAMS_CONSUMER_RELEASE_BASE");
        return (env == null || env.isEmpty()) ? DEFAULT_RELEASE_BASE : env;
    }

    static String[] platformArch() {
        String osName = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String os = osName.contains("win") ? "windows" : osName.contains("mac") ? "darwin" : "linux";
        String archProp = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        String arch = (archProp.equals("aarch64") || archProp.equals("arm64")) ? "aarch64" : "x86_64";
        String ext = os.equals("windows") ? ".exe" : "";
        return new String[] {os, arch, ext};
    }

    static Path cachePath() {
        String[] pa = platformArch();
        String ext = pa[2];
        String base = System.getenv("XDG_CACHE_HOME");
        Path baseDir = (base == null || base.isEmpty())
                ? Paths.get(System.getProperty("user.home"), ".cache")
                : Paths.get(base);
        return baseDir.resolve("amazon-dynamodb-streams-consumer").resolve(VERSION).resolve(BINARY + ext);
    }

    static String discover(String explicitPath) throws IOException, InterruptedException {
        if (explicitPath != null && !explicitPath.isEmpty()) {
            return explicitPath;
        }
        String env = System.getenv("DDB_STREAMS_CONSUMER_SIDECAR");
        if (env != null && !env.isEmpty()) {
            return env;
        }
        Path cached = cachePath();
        if (Files.isExecutable(cached)) {
            return cached.toString();
        }
        try {
            return download(cached);
        } catch (Exception e) {
            String onPath = onPath();
            if (onPath != null) {
                return onPath;
            }
            throw new IOException("could not obtain the " + BINARY + " sidecar: download failed ("
                    + e.getMessage() + ") and it is not on PATH. Set DDB_STREAMS_CONSUMER_SIDECAR=/path/to/sidecar "
                    + "or install it manually.", e);
        }
    }

    private static String download(Path dst) throws IOException, InterruptedException {
        String[] pa = platformArch();
        String asset = BINARY + "-" + pa[0] + "-" + pa[1] + pa[2];
        String binUrl = trimTrailingSlash(releaseBase()) + "/v" + VERSION + "/" + asset;

        HttpClient http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build();
        String wantLine = getString(http, binUrl + ".sha256").trim();
        String want = wantLine.split("\\s+")[0].toLowerCase(Locale.ROOT);
        byte[] body = getBytes(http, binUrl);
        String got = sha256Hex(body);
        if (!got.equals(want)) {
            throw new IOException("checksum mismatch for " + asset + ": got " + got + " want " + want);
        }

        Files.createDirectories(dst.getParent());
        Path tmp = dst.resolveSibling(dst.getFileName() + ".tmp-" + ProcessHandle.current().pid());
        Files.write(tmp, body);
        tmp.toFile().setExecutable(true, false);
        Files.move(tmp, dst, StandardCopyOption.REPLACE_EXISTING);
        return dst.toString();
    }

    private static String getString(HttpClient http, String url) throws IOException, InterruptedException {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() != 200) {
            throw new IOException("GET " + url + ": HTTP " + r.statusCode());
        }
        return r.body();
    }

    private static byte[] getBytes(HttpClient http, String url) throws IOException, InterruptedException {
        HttpResponse<byte[]> r = http.send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        if (r.statusCode() != 200) {
            throw new IOException("GET " + url + ": HTTP " + r.statusCode());
        }
        return r.body();
    }

    private static String sha256Hex(byte[] data) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(data);
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String onPath() {
        String[] pa = platformArch();
        String name = BINARY + pa[2];
        String path = System.getenv("PATH");
        if (path == null) {
            return null;
        }
        for (String dir : path.split(java.io.File.pathSeparator)) {
            if (dir.isEmpty()) {
                continue;
            }
            Path p = Paths.get(dir, name);
            if (Files.isExecutable(p)) {
                return p.toString();
            }
        }
        return null;
    }

    private static String trimTrailingSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }
}
