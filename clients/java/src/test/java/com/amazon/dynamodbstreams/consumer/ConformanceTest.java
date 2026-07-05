package com.amazon.dynamodbstreams.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Drives every shared conformance fixture (conformance/fixtures/*.json) through
// the real Worker against the shared replay_sidecar.py -- no AWS, no real
// sidecar. Mirrors clients/{python,go,node,dotnet} conformance runners.
class ConformanceTest {
    private static final ObjectMapper M = new ObjectMapper();
    // Surefire runs with basedir = clients/java; the conformance dir is two up.
    private static final Path CONF = Paths.get(System.getProperty("user.dir"), "..", "..", "conformance")
            .toAbsolutePath().normalize();

    static Stream<String> fixtures() throws Exception {
        try (Stream<Path> s = Files.list(CONF.resolve("fixtures"))) {
            return s.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".json"))
                    .collect(Collectors.toList())
                    .stream();
        }
    }

    private static final class Collector implements RecordProcessor {
        final Map<String, List<String>> byShard = new LinkedHashMap<>();
        final List<String> ended = new ArrayList<>();

        @Override
        public void processRecords(List<Record> records) {
            for (Record r : records) {
                byShard.computeIfAbsent(r.shardId(), k -> new ArrayList<>()).add(r.sequenceNumber());
            }
        }

        @Override
        public void shardEnded(String shardId) {
            ended.add(shardId);
        }
    }

    @ParameterizedTest(name = "conformance: {0}")
    @MethodSource("fixtures")
    void conformance(String fixtureFile) throws Exception {
        Path fpath = CONF.resolve("fixtures").resolve(fixtureFile);
        JsonNode expect = M.readTree(fpath.toFile()).get("expect");

        Collector c = new Collector();
        Worker w = new Worker(WorkerConfig.builder()
                .streamArn("arn:aws:dynamodb:us-east-1:1:table/T/stream/2026")
                .leaseTable("leases")
                .processor(c)
                .sidecarCmd(List.of("python3", CONF.resolve("replay_sidecar.py").toString(), fpath.toString()))
                .build());

        int code = w.run();

        // Checkpointing: replay exits non-zero on a wrong/absent ack.
        assertEquals(0, code, fixtureFile + ": replay rejected checkpoint acks");

        // Delivery counts + no extra shards.
        JsonNode counts = expect.get("records_per_shard");
        assertEquals(counts.size(), c.byShard.size(), fixtureFile + ": shard count");
        counts.fields().forEachRemaining(e -> {
            int got = c.byShard.getOrDefault(e.getKey(), Collections.emptyList()).size();
            assertEquals(e.getValue().asInt(), got, fixtureFile + ": records_per_shard " + e.getKey());
        });

        // Per-shard order.
        expect.get("record_order").fields().forEachRemaining(e -> {
            List<String> order = new ArrayList<>();
            e.getValue().forEach(n -> order.add(n.asText()));
            assertEquals(order, c.byShard.getOrDefault(e.getKey(), Collections.emptyList()),
                    fixtureFile + ": order " + e.getKey());
        });

        // Lifecycle: shard_ended.
        List<String> expEnded = new ArrayList<>();
        expect.get("shard_ended").forEach(n -> expEnded.add(n.asText()));
        Collections.sort(expEnded);
        List<String> gotEnded = new ArrayList<>(c.ended);
        Collections.sort(gotEnded);
        assertEquals(expEnded, gotEnded, fixtureFile + ": shard_ended");
    }
}
