package com.minispark.rdd;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Phase 1 sanity: actions on narrow lineages. */
final class RDDActionsTest {

    private MiniSparkContext sc() {
        return new MiniSparkContext(new MiniSparkConf().setAppName("test").setMaster("local[2]"));
    }

    @Test
    void parallelize_count_matches_input() {
        try (MiniSparkContext sc = sc()) {
            assertThat(sc.parallelize(IntStream.rangeClosed(1, 100).boxed().toList(), 4)
                    .count()).isEqualTo(100);
        }
    }

    @Test
    void filter_map_reduce_pipeline() {
        try (MiniSparkContext sc = sc()) {
            int sum = sc.parallelize(IntStream.rangeClosed(1, 10).boxed().toList(), 3)
                    .filter((RDD.SerializablePredicate<Integer>) i -> i % 2 == 0)
                    .map((RDD.SerializableFunction<Integer, Integer>) i -> i * i)
                    .reduce(Integer::sum);
            // 2^2 + 4^2 + 6^2 + 8^2 + 10^2 = 4+16+36+64+100 = 220
            assertThat(sum).isEqualTo(220);
        }
    }

    @Test
    void collect_preserves_partition_order() {
        try (MiniSparkContext sc = sc()) {
            List<Integer> out = sc.parallelize(List.of(1, 2, 3, 4, 5, 6, 7, 8), 4)
                    .map((RDD.SerializableFunction<Integer, Integer>) i -> i * 10)
                    .collect();
            assertThat(out).containsExactly(10, 20, 30, 40, 50, 60, 70, 80);
        }
    }

    @Test
    void flatMap_works() {
        try (MiniSparkContext sc = sc()) {
            List<String> out = sc.parallelize(List.of("a b", "c d e"), 2)
                    .flatMap((RDD.SerializableFunction<String, java.util.Iterator<String>>)
                            s -> List.of(s.split(" ")).iterator())
                    .collect();
            assertThat(out).containsExactly("a", "b", "c", "d", "e");
        }
    }

    @Test
    void mapToPair_chain_compiles() {
        try (MiniSparkContext sc = sc()) {
            long count = sc.parallelize(List.of("x", "y"), 1)
                    .mapToPair(s -> new Tuple2<>(s, 1))
                    .rdd()
                    .count();
            assertThat(count).isEqualTo(2);
        }
    }
}
