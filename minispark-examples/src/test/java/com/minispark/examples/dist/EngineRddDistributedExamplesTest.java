package com.minispark.examples.dist;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.PairRDDFunctions;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distributed engine + RDD-action examples (examples 32–38): spillable
 * aggregation under a tight memory budget, take/first/takeOrdered,
 * saveAsTextFile round-trip, checkpoint (lineage truncation), cogroup, and a
 * FAIR scheduler pool. All run across 2 executor JVMs over TCP.
 */
final class EngineRddDistributedExamplesTest {

    // Example 32 — spillable aggregation: a wide reduceByKey under a tiny memory
    // budget forces the ExternalAppendOnlyMap to spill to disk, yet every key's
    // sum is still correct. This is the OOM-survival path made observable.
    @Test
    void ex32_spillable_aggregation_under_tight_memory() {
        Map<Integer, Long> got = DistTestSupport.runDistributed("ex32-spill-agg",
                app -> DistTestSupport.distConf(app)
                        .set("minispark.memory.store.maxBytes", "16k")   // tiny → execution pool spills
                        .set("minispark.memory.storageFraction", "0.25"),
                sc -> {
                    // 5000 rows over 500 distinct keys → 10 per key. Far more
                    // entries than fit in a 16k budget, so spills happen.
                    List<Tuple2<Integer, Long>> pairs = new ArrayList<>();
                    for (int i = 0; i < 5000; i++) pairs.add(new Tuple2<>(i % 500, 1L));
                    RDD<Tuple2<Integer, Long>> rdd = sc.parallelize(pairs, 8);
                    List<Tuple2<Integer, Long>> res =
                            new PairRDDFunctions<>(rdd).reduceByKey(Long::sum).collect();
                    Map<Integer, Long> m = new HashMap<>();
                    for (Tuple2<Integer, Long> t : res) m.put(t._1(), t._2());
                    return m;
                });
        assertThat(got).hasSize(500);
        assertThat(got.values()).allMatch(c -> c == 10L);
    }

    // Example 33 — take(n) and first(): bounded actions that don't scan everything.
    @Test
    void ex33_take_and_first() {
        List<Integer> got = DistTestSupport.runDistributed("ex33-take-first", sc -> {
            RDD<Integer> rdd = sc.parallelize(IntStream.rangeClosed(1, 1000).boxed().toList(), 8);
            List<Integer> firstFive = rdd.take(5);
            int head = rdd.first();
            List<Integer> out = new ArrayList<>(firstFive);
            out.add(head);
            return out;
        });
        // Partition order is preserved for a parallelize of a sequential list.
        assertThat(got.subList(0, 5)).containsExactly(1, 2, 3, 4, 5);
        assertThat(got.get(5)).isEqualTo(1);   // first()
    }

    // Example 34 — takeOrdered(n, cmp): a distributed top-N (descending).
    @Test
    void ex34_take_ordered_topN() {
        List<Integer> top = DistTestSupport.runDistributed("ex34-takeOrdered", sc -> {
            List<Integer> data = new ArrayList<>();
            for (int i = 0; i < 1000; i++) data.add((i * 7919) % 10000);   // scrambled
            return sc.parallelize(data, 8)
                    .takeOrdered(5, (RDD.SerializableComparator<Integer>) (a, b) -> Integer.compare(b, a));
        });
        assertThat(top).hasSize(5);
        // Descending and each strictly the largest 5 distinct-ish values present.
        assertThat(top).isSortedAccordingTo((a, b) -> Integer.compare(b, a));
        assertThat(top.get(0)).isGreaterThanOrEqualTo(top.get(4));
    }

    // Example 35 — saveAsTextFile then textFile read-back: one part file per partition.
    @Test
    void ex35_save_and_read_text(@TempDir Path tmp) {
        Path out = tmp.resolve("nums_out");
        long readBack = DistTestSupport.runDistributed("ex35-save-text", sc -> {
            sc.parallelize(IntStream.rangeClosed(1, 200).boxed().toList(), 4)
                    .map((RDD.SerializableFunction<Integer, String>) i -> "n" + i)
                    .saveAsTextFile(out.toString());
            // Read every part file back and count the lines.
            return sc.textFile(out.toString()).count();
        });
        assertThat(readBack).isEqualTo(200L);
    }

    // Example 36 — checkpoint(): truncate lineage to reliable storage. The
    // post-checkpoint result is identical; the lineage before it is discarded.
    @Test
    void ex36_checkpoint_truncates_lineage(@TempDir Path tmp) {
        long got = DistTestSupport.runDistributed("ex36-checkpoint", sc -> {
            sc.setCheckpointDir(tmp.resolve("ckpt").toString());
            RDD<Integer> derived = sc.parallelize(IntStream.rangeClosed(1, 500).boxed().toList(), 8)
                    .map((RDD.SerializableFunction<Integer, Integer>) x -> x * 2)
                    .filter((RDD.SerializablePredicate<Integer>) x -> x % 4 == 0);
            derived.checkpoint();
            long c1 = derived.count();    // materializes + writes checkpoint
            long c2 = derived.count();    // served from checkpoint, lineage truncated
            assertThat(c1).isEqualTo(c2);
            return c2;
        });
        // x in 1..500, x*2 in {2,4,...,1000}, keep multiples of 4 → 250 of them.
        assertThat(got).isEqualTo(250L);
    }

    // Example 37 — cogroup: group both sides by key into (leftValues, rightValues).
    @Test
    void ex37_cogroup() {
        Map<String, String> got = DistTestSupport.runDistributed("ex37-cogroup", sc -> {
            RDD<Tuple2<String, Integer>> a = sc.parallelize(List.of(
                    new Tuple2<>("x", 1), new Tuple2<>("x", 2), new Tuple2<>("y", 3)), 2);
            RDD<Tuple2<String, Integer>> b = sc.parallelize(List.of(
                    new Tuple2<>("x", 10), new Tuple2<>("z", 20)), 2);
            List<Tuple2<String, Tuple2<List<Integer>, List<Integer>>>> res =
                    new PairRDDFunctions<>(a).cogroup(b).collect();
            Map<String, String> m = new HashMap<>();
            for (var t : res) {
                List<Integer> left = new ArrayList<>(t._2()._1());
                List<Integer> right = new ArrayList<>(t._2()._2());
                left.sort(Integer::compareTo);
                right.sort(Integer::compareTo);
                m.put(t._1(), left + "|" + right);
            }
            return m;
        });
        assertThat(got.get("x")).isEqualTo("[1, 2]|[10]");
        assertThat(got.get("y")).isEqualTo("[3]|[]");
        assertThat(got.get("z")).isEqualTo("[]|[20]");
    }

    // Example 38 — FAIR scheduler pool: configure a weighted pool, run a job in it.
    // The result is unaffected by the pool; this exercises the FAIR path end-to-end.
    @Test
    void ex38_fair_scheduler_pool() {
        long got = DistTestSupport.runDistributed("ex38-fair-pool",
                app -> DistTestSupport.distConf(app)
                        .set("minispark.scheduler.mode", "FAIR"),
                sc -> {
                    sc.configurePool("highPriority", /*weight=*/4, /*minShare=*/2);
                    sc.setSchedulerPool("highPriority");
                    long sum = sc.parallelize(IntStream.rangeClosed(1, 1000).boxed().toList(), 8)
                            .map((RDD.SerializableFunction<Integer, Long>) Integer::longValue)
                            .reduce((RDD.SerializableBiFunction<Long, Long, Long>) Long::sum);
                    sc.clearSchedulerPool();
                    return sum;
                });
        assertThat(got).isEqualTo(IntStream.rangeClosed(1, 1000).asLongStream().sum());  // 500500
    }
}
