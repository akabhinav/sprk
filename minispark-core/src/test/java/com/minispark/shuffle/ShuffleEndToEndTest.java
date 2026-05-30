package com.minispark.shuffle;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Phase 2 sanity: reduceByKey produces correct, deterministic aggregates. */
final class ShuffleEndToEndTest {

    @Test
    void reduceByKey_wordcount_like() {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setAppName("rbk").setMaster("local[4]"))) {

            List<Tuple2<String, Integer>> result = sc.parallelize(
                            List.of("a", "b", "a", "c", "b", "a", "d", "b"), 3)
                    .mapToPair(w -> new Tuple2<>(w, 1))
                    .reduceByKey(Integer::sum)
                    .collect();

            Map<String, Integer> got = new HashMap<>();
            for (Tuple2<String, Integer> t : result) got.put(t._1(), t._2());

            assertThat(got).containsExactlyInAnyOrderEntriesOf(
                    Map.of("a", 3, "b", 3, "c", 1, "d", 1));
        }
    }

    @Test
    void filter_after_reduceByKey_works() {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setAppName("rbk-filter").setMaster("local[4]"))) {

            long frequent = sc.parallelize(
                            List.of("a", "b", "a", "c", "b", "a", "d", "b"), 3)
                    .mapToPair(w -> new Tuple2<>(w, 1))
                    .reduceByKey(Integer::sum)
                    .filter((RDD.SerializablePredicate<Tuple2<String, Integer>>) t -> t._2() > 1)
                    .count();

            assertThat(frequent).isEqualTo(2L); // "a" and "b"
        }
    }
}
