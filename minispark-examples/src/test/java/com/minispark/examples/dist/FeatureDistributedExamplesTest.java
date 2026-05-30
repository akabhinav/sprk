package com.minispark.examples.dist;

import com.minispark.accumulator.Accumulator;
import com.minispark.api.PairRDDFunctions;
import com.minispark.api.Tuple2;
import com.minispark.broadcast.Broadcast;
import com.minispark.rdd.RDD;
import com.minispark.storage.StorageLevel;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distributed examples for engine features (examples 8–13): caching, broadcast
 * variables, accumulators, the sort shuffle, and a wide shuffle. All run across
 * 2 executor JVMs over TCP.
 */
final class FeatureDistributedExamplesTest {

    // Example 8 — cache(): a reused RDD is materialized once per executor.
    @Test
    void ex08_cache_reused_rdd() {
        long[] got = DistTestSupport.runDistributed("ex08-cache", sc -> {
            RDD<Integer> base = sc.parallelize(IntStream.rangeClosed(1, 500).boxed().toList(), 8)
                    .map((RDD.SerializableFunction<Integer, Integer>) x -> x * 2)
                    .cache();
            long count = base.count();          // materializes + caches
            long sum = base.map((RDD.SerializableFunction<Integer, Integer>) x -> x)
                    .reduce((RDD.SerializableBiFunction<Integer, Integer, Integer>) Integer::sum);
            return new long[]{count, sum};
        });
        assertThat(got[0]).isEqualTo(500L);
        assertThat(got[1]).isEqualTo(IntStream.rangeClosed(1, 500).mapToLong(x -> x * 2L).sum());
    }

    // Example 9 — MEMORY_AND_DISK persist with a tiny memory budget forces spill.
    @Test
    void ex09_memory_and_disk_persist() {
        long got = DistTestSupport.runDistributed("ex09-mem-and-disk",
                appName -> DistTestSupport.distConf(appName)
                        .set("minispark.memory.store.maxBytes", "8k"), // tiny → spill to disk
                sc -> {
                    RDD<int[]> cached = sc.parallelize(IntStream.range(0, 64).boxed().toList(), 8)
                            .map((RDD.SerializableFunction<Integer, int[]>) i -> {
                                int[] big = new int[1024];
                                java.util.Arrays.fill(big, i);
                                return big;
                            })
                            .persist(StorageLevel.MEMORY_AND_DISK);
                    cached.count();                 // first pass: populate (spilling)
                    return cached.count();          // second pass: served from disk
                });
        assertThat(got).isEqualTo(64L);
    }

    // Example 10 — broadcast variable: a lookup table shipped once per executor.
    @Test
    void ex10_broadcast_lookup() {
        List<Integer> got = DistTestSupport.runDistributed("ex10-broadcast", sc -> {
            HashMap<Integer, Integer> table = new HashMap<>();
            for (int k = 0; k < 10; k++) table.put(k, k * 100);
            Broadcast<HashMap<Integer, Integer>> bc = sc.broadcast(table);
            return sc.parallelize(IntStream.range(0, 200).boxed().toList(), 8)
                    .map((RDD.SerializableFunction<Integer, Integer>) i -> bc.value().get(i % 10))
                    .collect();
        });
        assertThat(got).hasSize(200);
        assertThat(got).allMatch(v -> v % 100 == 0 && v >= 0 && v <= 900);
    }

    // Example 11 — accumulator: a counter summed across all tasks/executors.
    @Test
    void ex11_accumulator_counts_across_executors() {
        long matched = DistTestSupport.runDistributed("ex11-accumulator", sc -> {
            Accumulator<Long> evens = sc.longAccumulator("evens");
            sc.parallelize(IntStream.range(0, 1000).boxed().toList(), 8)
                    .foreach((RDD.SerializableConsumer<Integer>) i -> {
                        if (i % 2 == 0) evens.add(1L);
                    });
            return evens.value();
        });
        assertThat(matched).isEqualTo(500L);
    }

    // Example 12 — sort shuffle (swapped behind the seam) producing the same result.
    @Test
    void ex12_sort_shuffle_reduceByKey() {
        Map<String, Long> got = DistTestSupport.runDistributed("ex12-sort-shuffle",
                appName -> DistTestSupport.distConf(appName)
                        .set("minispark.shuffle.manager", "sort"),
                sc -> {
                    RDD<Tuple2<String, Long>> keyed = sc.parallelize(
                            IntStream.range(0, 600).boxed().toList(), 8)
                            .mapToPair(i -> new Tuple2<>("k" + (i % 6), 1L)).rdd();
                    List<Tuple2<String, Long>> res =
                            new PairRDDFunctions<>(keyed).reduceByKey(Long::sum).collect();
                    Map<String, Long> m = new HashMap<>();
                    for (Tuple2<String, Long> t : res) m.put(t._1(), t._2());
                    return m;
                });
        assertThat(got).hasSize(6);
        assertThat(got.values()).allMatch(v -> v == 100L);  // 600/6
    }

    // Example 13 — a wide shuffle: many keys, many partitions, average per key.
    @Test
    void ex13_average_per_key() {
        Map<Integer, Double> got = DistTestSupport.runDistributed("ex13-avg-per-key", sc -> {
            List<Tuple2<Integer, Integer>> pairs = new ArrayList<>();
            for (int i = 0; i < 1000; i++) pairs.add(new Tuple2<>(i % 10, i));
            // (sum, count) per key via reduceByKey, then map to average.
            RDD<Tuple2<Integer, Tuple2<Long, Long>>> sc2 = sc.parallelize(pairs, 10)
                    .mapToPair(t -> new Tuple2<>(t._1(), new Tuple2<>((long) t._2(), 1L))).rdd();
            List<Tuple2<Integer, Tuple2<Long, Long>>> reduced =
                    new PairRDDFunctions<>(sc2).reduceByKey(
                            (a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2())).collect();
            Map<Integer, Double> m = new HashMap<>();
            for (Tuple2<Integer, Tuple2<Long, Long>> t : reduced) {
                m.put(t._1(), (double) t._2()._1() / t._2()._2());
            }
            return m;
        });
        // key k holds values k, k+10, k+20, … 990+k → average = k + mean(0,10,…,990)=k+495
        assertThat(got).hasSize(10);
        assertThat(got.get(0)).isEqualTo(495.0);
        assertThat(got.get(9)).isEqualTo(504.0);
    }
}
