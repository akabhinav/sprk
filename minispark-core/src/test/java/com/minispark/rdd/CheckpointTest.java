package com.minispark.rdd;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * After {@code checkpoint()}, an RDD's data comes from reliable storage and its
 * lineage is truncated — so re-running an action does NOT re-execute the
 * upstream transformations.
 */
final class CheckpointTest {

    private static final AtomicInteger UPSTREAM_COMPUTES = new AtomicInteger();

    @Test
    void checkpoint_truncates_lineage_and_avoids_recompute(@TempDir Path tmp) {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setMaster("local[2]"))) {
            sc.setCheckpointDir(tmp.resolve("cp").toString());

            UPSTREAM_COMPUTES.set(0);
            RDD<Integer> mapped = sc.parallelize(IntStream.rangeClosed(1, 20).boxed().toList(), 4)
                    .map((RDD.SerializableFunction<Integer, Integer>) i -> {
                        UPSTREAM_COMPUTES.incrementAndGet();
                        return i * 2;
                    });
            mapped.checkpoint();

            // First action: computes upstream (20 elements) and writes checkpoint files.
            assertThat(mapped.count()).isEqualTo(20L);
            int afterFirst = UPSTREAM_COMPUTES.get();
            assertThat(afterFirst).isEqualTo(20);
            assertThat(mapped.isCheckpointed()).isTrue();

            // Second action: served from checkpoint files; the map lambda must NOT run again.
            List<Integer> values = mapped.collect();
            assertThat(values).hasSize(20);
            assertThat(values).contains(2, 4, 40);
            assertThat(UPSTREAM_COMPUTES.get())
                    .as("checkpoint should serve from disk, not re-run upstream map")
                    .isEqualTo(afterFirst);
        }
    }
}
