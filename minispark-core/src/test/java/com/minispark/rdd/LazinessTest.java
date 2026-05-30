package com.minispark.rdd;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Transformations must not execute until an action is invoked. This is what
 * lets the DAGScheduler see the whole pipeline before launching anything.
 *
 * <p>Note the counter is {@code static}. The scheduler backend serializes
 * every task closure even in local mode (to surface "not serializable" bugs
 * early); a captured {@code AtomicInteger} would be cloned on deserialization
 * and the executor would increment a copy invisible to the driver. Globals
 * survive because statics aren't part of serialized object state. Real Spark
 * gives the same answer via {@code Accumulators} — out of scope here.
 */
final class LazinessTest {

    private static final AtomicInteger CALLS = new AtomicInteger();

    @Test
    void transformations_do_not_run_until_action() {
        CALLS.set(0);
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setAppName("lazy").setMaster("local[2]"))) {

            RDD<Integer> mapped = sc.parallelize(List.of(1, 2, 3, 4), 2)
                    .map((RDD.SerializableFunction<Integer, Integer>) i -> {
                        CALLS.incrementAndGet();
                        return i + 1;
                    });

            // No action yet -> compute must not have run.
            assertThat(CALLS.get()).isZero();

            // Action -> all four elements processed.
            long n = mapped.count();
            assertThat(n).isEqualTo(4);
            assertThat(CALLS.get()).isEqualTo(4);
        }
    }
}
