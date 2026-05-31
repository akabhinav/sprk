package com.minispark.scheduler.adaptive;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import com.minispark.rdd.ShuffledRDD;
import com.minispark.shuffle.HashPartitioner;
import com.minispark.status.AppStatusStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end check that AQE's skew-split actually splits a fat reducer into
 * multiple post-shuffle tasks and that the union of those tasks reproduces
 * the un-split data multiset-for-multiset.
 *
 * <p><b>Safe consumption pattern.</b> Skew-split puts records with the same
 * key into multiple output partitions, which breaks key-aware downstream
 * consumers (groupByKey, reduceByKey, cogroup). The safe usage — and the
 * one we test here — is consuming a bare {@code ShuffledRDD} via
 * {@code collect()}: the consumer just concatenates all output partitions,
 * so splitting doesn't change the data, only the task count.
 */
final class AqeSkewSplitIntegrationTest {

    private record Result(List<Tuple2<String, Integer>> rows, int resultStageTasks) {}

    private static Result runJob(boolean skewSplitEnabled, long targetBytes) {
        MiniSparkConf cfg = new MiniSparkConf().setMaster("local[2]")
                .set("minispark.sql.adaptive.enabled", "true")
                // Disable coalesce — we want to see the un-coalesced layout so
                // the test isolates the skew-split contribution to the task count.
                .set("minispark.sql.adaptive.coalescePartitions.targetSizeInBytes", "1")
                .set("minispark.sql.adaptive.coalescePartitions.minPartitionNum", "8");
        if (skewSplitEnabled) {
            cfg.set("minispark.sql.adaptive.skewJoin.enabled", "true")
               // Tiny test thresholds — our skewed reducer is a few KB, not 256 MiB.
               .set("minispark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "100")
               .set("minispark.sql.adaptive.skewJoin.skewedPartitionFactor", "3.0")
               .set("minispark.sql.adaptive.coalescePartitions.targetSizeInBytes", Long.toString(targetBytes));
        }
        try (MiniSparkContext sc = new MiniSparkContext(cfg)) {
            // 8 input partitions × 100 HOT records + 2 distinct cold each.
            // HOT hashes to one reducer (the same across all maps), so that
            // reducer collects ~800 records while siblings collect ~2-3. The
            // per-map contribution to the HOT reducer is ~100 records — well
            // distributed across map ids, which is the precondition for split.
            List<Tuple2<String, Integer>> kvs = new ArrayList<>();
            for (int p = 0; p < 8; p++) {
                for (int i = 0; i < 100; i++) kvs.add(new Tuple2<>("HOT", 1));
                kvs.add(new Tuple2<>("cold" + p + "_a", 1));
                kvs.add(new Tuple2<>("cold" + p + "_b", 1));
            }

            RDD<Tuple2<String, Integer>> pairs = sc.parallelize(kvs, 8)
                    .mapToPair(t -> t).rdd();
            // Bare ShuffledRDD → collect: no downstream operator, so skew-split
            // is semantics-safe. (Aggregating consumers would see partial groups.)
            ShuffledRDD<String, Integer> shuffled =
                    new ShuffledRDD<>(sc, pairs, new HashPartitioner(8));
            List<Tuple2<String, Integer>> result = shuffled.collect();

            sc.awaitListenerBus(5000);
            int resultTasks = -1;
            for (AppStatusStore.StageView v : sc.statusStore().stages()) {
                if ("ResultStage".equals(v.name())) resultTasks = v.numTasks();
            }
            return new Result(result, resultTasks);
        }
    }

    @Test
    void skew_split_increases_post_shuffle_task_count_without_changing_data() {
        Result without = runJob(/*skewSplitEnabled=*/false, 0);
        // Target chosen so each map's HOT contribution (~100 records, a few KB)
        // is well over the threshold needed to make multiple per-map slices.
        Result with    = runJob(/*skewSplitEnabled=*/true, 1_500);

        // Multiset of (key, count_of_this_record) must be identical with or
        // without skew-split: the shuffle delivers the same records either way.
        assertThat(bag(with.rows())).isEqualTo(bag(without.rows()));
        // 8 partitions × 100 HOT records = 800 HOT records flow through.
        assertThat(bag(with.rows())).containsEntry("HOT", 800L);
        // 8 partitions × 2 distinct cold keys = 16 unique cold keys, each once.
        assertThat(bag(with.rows()).values().stream().filter(v -> v == 1L).count()).isEqualTo(16L);

        // The result stage MUST run more tasks with skew-split on than off —
        // that's the whole point: the one fat reducer becomes N sub-tasks.
        assertThat(with.resultStageTasks()).isGreaterThan(without.resultStageTasks());
    }

    private static Map<String, Long> bag(List<Tuple2<String, Integer>> rows) {
        Map<String, Long> out = new HashMap<>();
        for (Tuple2<String, Integer> t : rows) out.merge(t._1(), 1L, Long::sum);
        return out;
    }
}
