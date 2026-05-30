package com.minispark.scheduler;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Speculation: one partition deliberately takes much longer than the others.
 * With speculation on, the scheduler launches a duplicate of the straggler;
 * one copy "wins" while the other finishes (or is ignored). The result must
 * stay correct — that's the contract for any straggler-recovery mechanism.
 */
final class SpeculationTest {

    /** Per-JVM counter so we only stall the very first task of a given partition. */
    private static final AtomicInteger SLOW_PARTITION_RUNS = new AtomicInteger();

    @Test
    void speculative_copy_lets_a_straggler_be_overtaken_without_breaking_results() {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setMaster("local[4]")
                        .set("minispark.speculation", "true")
                        .set("minispark.speculation.intervalMs", "100")
                        .set("minispark.speculation.quantile", "0.5")
                        .set("minispark.speculation.multiplier", "1.2"))) {

            SLOW_PARTITION_RUNS.set(0);

            List<Integer> sums = sc.parallelize(java.util.stream.IntStream.range(0, 8).boxed().toList(), 8)
                    .map((RDD.SerializableFunction<Integer, Integer>) i -> {
                        // Partition 0 stalls on its first attempt only; the speculative
                        // copy runs without the stall and wins.
                        if (i == 0 && SLOW_PARTITION_RUNS.getAndIncrement() == 0) {
                            try { Thread.sleep(3000); } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        return i * 10;
                    })
                    .collect();

            // Result is the same set regardless of whether the slow or the
            // speculated copy reported first (both produce identical output).
            assertThat(sums).containsExactlyInAnyOrder(0, 10, 20, 30, 40, 50, 60, 70);
            // Sanity: the slow path ran at least twice (the original + speculative).
            assertThat(SLOW_PARTITION_RUNS.get()).isGreaterThanOrEqualTo(2);
        }
    }
}
