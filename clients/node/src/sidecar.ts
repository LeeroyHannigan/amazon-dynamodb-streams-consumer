import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import * as http from 'node:http';
import * as https from 'node:https';
import * as crypto from 'node:crypto';

const BINARY = 'amazon-dynamodb-streams-consumer-sidecar';
export const VERSION = '0.1.0';
const DEFAULT_RELEASE_BASE =
  'https://github.com/LeeroyHannigan/amazon-dynamodb-streams-consumer/releases/download';

function releaseBase(): string {
  return process.env.DDB_STREAMS_CONSUMER_RELEASE_BASE || DEFAULT_RELEASE_BASE;
}

export function platformArch(): { osName: string; arch: string; ext: string } {
  const osName =
    process.platform === 'win32' ? 'windows' : process.platform === 'darwin' ? 'darwin' : 'linux';
  const arch = process.arch === 'x64' ? 'x86_64' : process.arch === 'arm64' ? 'aarch64' : process.arch;
  const ext = osName === 'windows' ? '.exe' : '';
  return { osName, arch, ext };
}

export function cachePath(): string {
  const base = process.env.XDG_CACHE_HOME || path.join(os.homedir(), '.cache');
  const { ext } = platformArch();
  return path.join(base, 'amazon-dynamodb-streams-consumer', VERSION, BINARY + ext);
}

// GET with redirect following (GitHub release assets 302 to a CDN). Accepts
// http or https so it can be exercised against a local test server.
function httpGet(url: string, redirects = 0): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    if (redirects > 5) return reject(new Error('too many redirects'));
    const mod = url.startsWith('http://') ? http : https;
    mod
      .get(url, (res) => {
        const { statusCode, headers } = res;
        if (statusCode && statusCode >= 300 && statusCode < 400 && headers.location) {
          res.resume();
          resolve(httpGet(headers.location, redirects + 1));
          return;
        }
        if (statusCode !== 200) {
          res.resume();
          reject(new Error(`GET ${url}: HTTP ${statusCode}`));
          return;
        }
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => resolve(Buffer.concat(chunks)));
        res.on('error', reject);
      })
      .on('error', reject);
  });
}

async function download(dst: string): Promise<string> {
  const { osName, arch, ext } = platformArch();
  const asset = `${BINARY}-${osName}-${arch}${ext}`;
  const binURL = `${releaseBase().replace(/\/$/, '')}/v${VERSION}/${asset}`;
  const want = (await httpGet(binURL + '.sha256')).toString().trim().split(/\s+/)[0];
  const body = await httpGet(binURL);
  const got = crypto.createHash('sha256').update(body).digest('hex');
  if (got.toLowerCase() !== want.toLowerCase()) {
    throw new Error(`checksum mismatch for ${asset}: got ${got} want ${want}`);
  }
  fs.mkdirSync(path.dirname(dst), { recursive: true });
  const tmp = `${dst}.tmp-${process.pid}`;
  fs.writeFileSync(tmp, body, { mode: 0o755 });
  fs.renameSync(tmp, dst);
  return dst;
}

function onPath(): string | null {
  const { ext } = platformArch();
  const name = BINARY + ext;
  for (const dir of (process.env.PATH || '').split(path.delimiter)) {
    if (!dir) continue;
    const p = path.join(dir, name);
    try {
      fs.accessSync(p, fs.constants.X_OK);
      return p;
    } catch {
      /* not here */
    }
  }
  return null;
}

// Resolution order: explicit path -> env override -> cached download ->
// download -> PATH. npm ships JS not a binary, so the sidecar is fetched once
// and cached; it is still install-and-go.
export async function discoverSidecar(explicit?: string): Promise<string> {
  if (explicit) return explicit;
  if (process.env.DDB_STREAMS_CONSUMER_SIDECAR) return process.env.DDB_STREAMS_CONSUMER_SIDECAR;
  const cached = cachePath();
  try {
    fs.accessSync(cached, fs.constants.X_OK);
    return cached;
  } catch {
    /* need to fetch */
  }
  try {
    return await download(cached);
  } catch (e) {
    const p = onPath();
    if (p) return p;
    const msg = e instanceof Error ? e.message : String(e);
    throw new Error(
      `could not obtain the ${BINARY} sidecar: download failed (${msg}) and it is not on PATH. ` +
        'Set DDB_STREAMS_CONSUMER_SIDECAR=/path/to/sidecar or install it manually'
    );
  }
}
