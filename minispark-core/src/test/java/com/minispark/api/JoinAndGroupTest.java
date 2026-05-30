package com.minispark.api;

import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The full pair-RDD vocabulary: groupByKey, cogroup, join, sortByKey. All
 * sit on top of the same {@link com.minispark.rdd.CoGroupedRDD} / shuffle
 * machinery; this test pins their semantics.
 */
final class JoinAndGroupTest {

    @Test
    void groupByKey_collects_all_values_per_key() {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setMaster("local[3]"))) {
            List<Tuple2<String, Integer>> in = List.of(
                    new Tuple2<>("a", 1), new Tuple2<>("b", 2),
                    new Tuple2<>("a", 3), new Tuple2<>("a", 4),
                    new Tuple2<>("b", 5));
            List<Tuple2<String, List<Integer>>> out = sc.parallelize(in, 3)
                    .mapToPair((RDD.SerializableFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>) t -> t)
                    .groupByKey()
                    .collect();

            Map<String, List<Integer>> got = new HashMap<>();
            for (Tuple2<String, List<Integer>> t : out) {
                List<Integer> sorted = new java.util.ArrayList<>(t._2()); sorted.sort(null);
                got.put(t._1(), sorted);
            }
            assertThat(got).hasSize(2);
            assertThat(got.get("a")).containsExactly(1, 3, 4);
            assertThat(got.get("b")).containsExactly(2, 5);
        }
    }

    @Test
    void join_emits_one_record_per_pair_of_values() {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setMaster("local[2]"))) {
            RDD<Tuple2<String, Integer>> left = sc.parallelize(List.of(
                    new Tuple2<>("a", 1), new Tuple2<>("b", 2), new Tuple2<>("a", 3)), 2);
            RDD<Tuple2<String, String>> right = sc.parallelize(List.of(
                    new Tuple2<>("a", "X"), new Tuple2<>("b", "Y"), new Tuple2<>("c", "Z")), 2);

            List<Tuple2<String, Tuple2<Integer, String>>> joined = new PairRDDFunctions<>(left)
                    .join(right).collect();

            // a: (1,X) (3,X);  b: (2,Y);  c: dropped (inner join)
            assertThat(joined).hasSize(3);
            // Convert to a comparable form.
            Map<String, java.util.Set<String>> byKey = new HashMap<>();
            for (Tuple2<String, Tuple2<Integer, String>> j : joined) {
                byKey.computeIfAbsent(j._1(), k -> new java.util.HashSet<>())
                        .add(j._2()._1() + ":" + j._2()._2());
            }
            assertThat(byKey.get("a")).containsExactlyInAnyOrder("1:X", "3:X");
            assertThat(byKey.get("b")).containsExactly("2:Y");
            assertThat(byKey).doesNotContainKey("c");
        }
    }

    @Test
    void sortByKey_produces_globally_sorted_partitions() {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setMaster("local[4]"))) {
            List<Tuple2<Integer, String>> in = new java.util.ArrayList<>();
            for (int i = 1000; i >= 1; i--) in.add(new Tuple2<>(i, "v" + i));

            List<Tuple2<Integer, String>> out = new PairRDDFunctions<>(sc.parallelize(in, 4))
                    .sortByKey()
                    .collect();

            assertThat(out).hasSize(1000);
            // Globally sorted (each adjacent pair in increasing order).
            for (int i = 1; i < out.size(); i++) {
                assertThat(out.get(i)._1()).isGreaterThanOrEqualTo(out.get(i - 1)._1());
            }
            assertThat(out.get(0)._1()).isEqualTo(1);
            assertThat(out.get(999)._1()).isEqualTo(1000);
        }
    }
}
