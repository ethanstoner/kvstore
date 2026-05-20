package com.ethanstoner.kvstore.benchmark;

import com.ethanstoner.kvstore.KvStore;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 3, time = 5)
@Fork(1)
public class KvStoreBenchmark {

    private KvStore store;
    private Path dataDir;
    private int writeCounter;
    private int keySpace = 100_000;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        dataDir = Files.createTempDirectory("kvbench");
        store = new KvStore(dataDir);
        for (int i = 0; i < keySpace; i++) {
            store.put("key-" + String.format("%06d", i), "value-" + i);
        }
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException {
        store.close();
        Files.walk(dataDir)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> { try { Files.delete(p); } catch (IOException ignored) {} });
    }

    @Benchmark
    public void sequentialWrite() throws IOException {
        store.put("seq-" + String.format("%09d", writeCounter++), "benchmark-value");
    }

    @Benchmark
    public void randomWrite() throws IOException {
        int key = ThreadLocalRandom.current().nextInt(keySpace * 2);
        store.put("rnd-" + String.format("%06d", key), "benchmark-value");
    }

    @Benchmark
    public String pointReadExistingKey() throws IOException {
        int key = ThreadLocalRandom.current().nextInt(keySpace);
        return store.get("key-" + String.format("%06d", key)).orElse(null);
    }

    @Benchmark
    public String pointReadMissingKey() throws IOException {
        int key = ThreadLocalRandom.current().nextInt(keySpace) + keySpace * 10;
        return store.get("miss-" + key).orElse(null);
    }

    @Benchmark
    public int scanRange() throws IOException {
        int start = ThreadLocalRandom.current().nextInt(keySpace - 100);
        String from = "key-" + String.format("%06d", start);
        String to = "key-" + String.format("%06d", start + 100);
        return store.scan(from, to).size();
    }
}
