package com.minispark.scheduler.adaptive;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.shuffle.HashPartitioner;
import com.minispark.status.AppStatusStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end check that AQE coalesce actually reduces the post-shuffle task
 * count of a real job, and that the answer is identical to the non-AQE run.
 *
 * <p>Verification reads from {@link AppStatusStore}: the ResultStage's
 * {@code numTasks} equals the post-shuffle partition count actually scheduled,
 * so we compare that figure with and without AQE.
 */
final class AqeCoalesceIntegrationTest {

    private record Result(Map<String, Integer> tally, int resultStageTasks) {}

    /**
     * Shuffle a tiny amount of data into 16 reducer partitions and collect.
     * Without AQE → 16 reduce tasks. With AQE → the per-reducer sizes are all
     * small, so the planner fuses everything into a single fat range.
     */
    private static Result runJob(boolean aqe) {
        MiniSparkConf cfg = new MiniSparkConf().setMaster("local[2]");
        if (aqe) {
            cfg.set("minispark.sql.adaptive.enabled", "true")
               .set("minispark.sql.adaptive.coalescePartitions.targetSizeInBytes", "1048576")
               .set("minispark.sql.adaptive.coalescePartitions.minPartitionNum", "1");
        }
        try (MiniSparkContext sc = new MiniSparkContext(cfg)) {
            List<Tuple2<String, Integer>> kvs = new ArrayList<>();
            String[] keys = {"a", "b", "c", "d", "e", "f", "g", "h"};
            for (int i = 0; i < 200; i++) kvs.add(new Tuple2<>(keys[i % keys.length], 1));

            List<Tuple2<String, Integer>> result = sc.parallelize(kvs, 4)
                    .mapToPair(t -> t)
                    .reduceByKey(Integer::sum, new HashPartitioner(16))
                    .collect();

            Map<String, Integer> tally = new HashMap<>();
            for (Tuple2<String, Integer> t : result) tally.merge(t._1(), t._2(), Integer::sum);

            // The status store is fed asynchronously; drain the bus before reading it.
            sc.awaitListenerBus(5000);
            int resultTasks = -1;
            for (AppStatusStore.StageView v : sc.statusStore().stages()) {
                if ("ResultStage".equals(v.name())) resultTasks = v.numTasks();
            }
            return new Result(tally, resultTasks);
        }
    }

    @Test
    void aqe_coalesces_tiny_shuffle_into_one_partition_without_changing_results() {
        Result without = runJob(false);
        Result with    = runJob(true);

        // Same answer either way — AQE must never change job semantics.
        assertThat(with.tally()).isEqualTo(without.tally());
        // 200 records / 8 keys = 25 each.
        assertThat(with.tally()).containsEntry("a", 25).containsEntry("h", 25).hasSize(8);

        // Without AQE we asked for 16 partitions and got 16. With AQE the
        // 16-way layout collapses to 1 post-shuffle task because the total
        // shuffled bytes ≪ the 1 MiB target.
        assertThat(without.resultStageTasks()).isEqualTo(16);
        assertThat(with.resultStageTasks()).isEqualTo(1);
    }

    @Test
    void aqe_respects_minimum_partition_floor() {
        MiniSparkConf cfg = new MiniSparkConf().setMaster("local[2]")
                .set("minispark.sql.adaptive.enabled", "true")
                .set("minispark.sql.adaptive.coalescePartitions.targetSizeInBytes", "1048576")
                .set("minispark.sql.adaptive.coalescePartitions.minPartitionNum", "4");
        try (MiniSparkContext sc = new MiniSparkContext(cfg)) {
            List<Tuple2<String, Integer>> kvs = new ArrayList<>();
            for (int i = 0; i < 200; i++) kvs.add(new Tuple2<>("k" + (i % 8), 1));
            sc.parallelize(kvs, 4)
                    .mapToPair(t -> t)
                    .reduceByKey(Integer::sum, new HashPartitioner(16))
                    .collect();

            sc.awaitListenerBus(5000);
            int resultTasks = -1;
            for (AppStatusStore.StageView v : sc.statusStore().stages()) {
                if ("ResultStage".equals(v.name())) resultTasks = v.numTasks();
            }
            // Without a floor it would collapse to 1; the floor forces ≥ 4.
            assertThat(resultTasks).isEqualTo(4);
        }
    }
}
