package com.minispark.rdd;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The "R" in RDD has two halves: caching avoids recomputation when you want
 * the same partition twice, and lineage recomputes it when caches are lost.
 * This test pins down the first half.
 */
final class RDDCacheTest {

    /** Counts how many times each partition's compute() runs across the JVM. */
    private static final AtomicInteger COMPUTES = new AtomicInteger();

    @Test
    void cache_avoids_recomputation_on_a_second_action() {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setAppName("cache-test").setMaster("local[1]"))) {

            COMPUTES.set(0);
            RDD<Integer> base = sc.parallelize(List.of(1, 2, 3, 4, 5, 6, 7, 8), 2)
                    .map((RDD.SerializableFunction<Integer, Integer>) x -> {
                        COMPUTES.incrementAndGet();
                        return x * x;
                    });

            // No cache: each action triggers a fresh compute for every element.
            assertThat(base.count()).isEqualTo(8L);
            assertThat(base.collect()).hasSize(8);
            int withoutCache = COMPUTES.get();

            COMPUTES.set(0);
            RDD<Integer> cached = sc.parallelize(List.of(1, 2, 3, 4, 5, 6, 7, 8), 2)
                    .map((RDD.SerializableFunction<Integer, Integer>) x -> {
                        COMPUTES.incrementAndGet();
                        return x * x;
                    })
                    .cache();

            assertThat(cached.count()).isEqualTo(8L);
            int afterFirstAction = COMPUTES.get();
            assertThat(cached.collect()).hasSize(8);
            int afterSecondAction = COMPUTES.get();

            // First action populates the cache (8 computes for 8 elements).
            assertThat(afterFirstAction).isEqualTo(8);
            // Second action serves from the cache: no additional compute() calls.
            assertThat(afterSecondAction).isEqualTo(afterFirstAction);
            // And the totals show the caching genuinely saved work.
            assertThat(afterSecondAction).isLessThan(withoutCache);
        }
    }
}
