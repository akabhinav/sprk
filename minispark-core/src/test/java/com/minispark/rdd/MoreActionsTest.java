package com.minispark.rdd;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** take / first / takeOrdered / saveAsTextFile. */
final class MoreActionsTest {

    @Test
    void take_returns_n_elements_in_partition_order() {
        try (MiniSparkContext sc = new MiniSparkContext(new MiniSparkConf().setMaster("local[3]"))) {
            List<Integer> r = sc.parallelize(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3).take(4);
            assertThat(r).hasSize(4);
        }
    }

    @Test
    void first_returns_an_element() {
        try (MiniSparkContext sc = new MiniSparkContext(new MiniSparkConf().setMaster("local[2]"))) {
            Integer first = sc.parallelize(List.of(7, 8, 9), 1).first();
            assertThat(first).isEqualTo(7);
        }
    }

    @Test
    void takeOrdered_returns_smallest_n_globally() {
        try (MiniSparkContext sc = new MiniSparkContext(new MiniSparkConf().setMaster("local[4]"))) {
            List<Integer> all = new java.util.ArrayList<>();
            for (int i = 100; i >= 1; i--) all.add(i);
            List<Integer> bottom5 = sc.parallelize(all, 4)
                    .takeOrdered(5, (RDD.SerializableComparator<Integer>) Integer::compareTo);
            assertThat(bottom5).containsExactly(1, 2, 3, 4, 5);
        }
    }

    @Test
    void saveAsTextFile_writes_one_file_per_partition(@TempDir Path tmp) throws Exception {
        Path out = tmp.resolve("out");
        try (MiniSparkContext sc = new MiniSparkContext(new MiniSparkConf().setMaster("local[3]"))) {
            sc.parallelize(List.of("alpha", "beta", "gamma", "delta", "epsilon"), 3)
                    .saveAsTextFile(out.toString());
        }
        // 3 partitions → 3 part files.
        List<Path> parts = Files.list(out).sorted().collect(Collectors.toList());
        assertThat(parts).hasSize(3);
        String all = parts.stream()
                .map(p -> { try { return Files.readString(p); } catch (Exception e) { throw new RuntimeException(e); } })
                .collect(Collectors.joining(""));
        assertThat(all).contains("alpha").contains("beta").contains("gamma").contains("delta").contains("epsilon");
    }
}
