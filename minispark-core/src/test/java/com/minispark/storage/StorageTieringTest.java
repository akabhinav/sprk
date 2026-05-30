package com.minispark.storage;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the bounded memory store + disk spill behind the cache levels.
 * Uses a deliberately tiny memory budget so caching forces eviction/spill.
 */
final class StorageTieringTest {

    /** Counts compute() calls so we can prove cache hits (or misses → recompute). */
    private static final AtomicInteger COMPUTES = new AtomicInteger();

    @Test
    void memory_and_disk_survives_a_tiny_memory_budget() {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setMaster("local[1]")
                        // 4 KB budget — far too small to hold the cached partitions in memory.
                        .set("minispark.memory.store.maxBytes", "4k"))) {

            COMPUTES.set(0);
            RDD<int[]> cached = sc.parallelize(IntStream.range(0, 8).boxed().toList(), 4)
                    .map((RDD.SerializableFunction<Integer, int[]>) i -> {
                        COMPUTES.incrementAndGet();
                        int[] big = new int[2048]; // ~8KB serialized per partition element
                        java.util.Arrays.fill(big, i);
                        return big;
                    })
                    .persist(StorageLevel.MEMORY_AND_DISK);

            assertThat(cached.count()).isEqualTo(8L);
            int afterFirst = COMPUTES.get();

            // Second action: even though memory is far too small, MEMORY_AND_DISK
            // spilled the partitions to disk, so they are NOT recomputed.
            assertThat(cached.count()).isEqualTo(8L);
            int afterSecond = COMPUTES.get();

            assertThat(afterFirst).isEqualTo(8);
            assertThat(afterSecond)
                    .as("MEMORY_AND_DISK should serve from disk on the 2nd action, not recompute")
                    .isEqualTo(afterFirst);
        }
    }

    @Test
    void disk_only_round_trips_blocks() {
        ExecutorLocation loc = ExecutorLocation.LOCAL;
        // Build a manager directly with a 1-byte memory budget to force disk use.
        com.minispark.rpc.RpcEnv env =
                com.minispark.rpc.RpcEnv.create("t", "127.0.0.1", 0, "local", null);
        try {
            NetworkBlockManager bm = new NetworkBlockManager(loc, env, 1, null);
            BlockId id = new BlockId.RDDBlock(42, 0);
            byte[] data = "hello disk".getBytes();
            bm.putBlock(id, data, StorageLevel.DISK_ONLY);
            assertThat(bm.inMemory(id)).isFalse();
            assertThat(bm.onDisk(id)).isTrue();
            assertThat(bm.getBlock(id)).hasValueSatisfying(b -> assertThat(b).isEqualTo(data));
        } finally {
            env.shutdown();
        }
    }
}
