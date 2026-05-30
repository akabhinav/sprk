package com.minispark.examples.dist;

import com.minispark.api.PairRDDFunctions;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distributed RDD-API examples (examples 1–7). Each runs across 2 executor JVMs
 * over TCP and asserts a result computed against a JDK reference.
 */
final class RddDistributedExamplesTest {

    // Example 1 — map + filter + reduce (a numeric pipeline, narrow ops + action).
    @Test
    void ex01_sum_of_squares_of_evens() {
        long got = DistTestSupport.runDistributed("ex01-sum-sq-evens", sc ->
                sc.parallelize(IntStream.rangeClosed(1, 1000).boxed().toList(), 8)
                        .filter((RDD.SerializablePredicate<Integer>) n -> n % 2 == 0)
                        .map((RDD.SerializableFunction<Integer, Long>) n -> (long) n * n)
                        .reduce((RDD.SerializableBiFunction<Long, Long, Long>) Long::sum));
        long expected = IntStream.rangeClosed(1, 1000).filter(n -> n % 2 == 0)
                .mapToLong(n -> (long) n * n).sum();
        assertThat(got).isEqualTo(expected);
    }

    // Example 2 — flatMap (one element fans out to many) then count.
    @Test
    void ex02_flatmap_tokenize_and_count() {
        List<String> lines = Arrays.asList(
                "the quick brown fox", "jumps over the lazy dog", "the end");
        long got = DistTestSupport.runDistributed("ex02-flatmap", sc ->
                sc.parallelize(lines, 3)
                        .flatMap((RDD.SerializableFunction<String, java.util.Iterator<String>>)
                                l -> Arrays.stream(l.split(" ")).iterator())
                        .count());
        long expected = lines.stream().mapToLong(l -> l.split(" ").length).sum();
        assertThat(got).isEqualTo(expected); // 4 + 5 + 2 = 11
    }

    // Example 3 — reduceByKey (map-side combine + shuffle): word count.
    @Test
    void ex03_word_count_reduceByKey() {
        List<String> lines = Arrays.asList(
                "a b a c", "b b c d", "a d d d", "c c c c");
        Map<String, Integer> got = DistTestSupport.runDistributed("ex03-wordcount", sc -> {
            List<Tuple2<String, Integer>> res = sc.parallelize(lines, 4)
                    .flatMap((RDD.SerializableFunction<String, java.util.Iterator<String>>)
                            l -> Arrays.stream(l.split(" ")).iterator())
                    .mapToPair(w -> new Tuple2<>(w, 1))
                    .reduceByKey(Integer::sum)
                    .collect();
            Map<String, Integer> m = new HashMap<>();
            for (Tuple2<String, Integer> t : res) m.put(t._1(), t._2());
            return m;
        });
        assertThat(got).containsEntry("a", 3).containsEntry("b", 3)
                .containsEntry("c", 6).containsEntry("d", 4);
    }

    // Example 4 — groupByKey (all values per key in one list after shuffle).
    @Test
    void ex04_groupByKey_collect_values() {
        List<Tuple2<String, Integer>> pairs = new ArrayList<>();
        for (int i = 0; i < 100; i++) pairs.add(new Tuple2<>(i % 2 == 0 ? "even" : "odd", i));
        Map<String, Integer> sizes = DistTestSupport.runDistributed("ex04-groupByKey", sc -> {
            RDD<Tuple2<String, Integer>> rdd = sc.parallelize(pairs, 6);
            List<Tuple2<String, List<Integer>>> res =
                    new PairRDDFunctions<>(rdd).groupByKey().collect();
            Map<String, Integer> m = new HashMap<>();
            for (Tuple2<String, List<Integer>> t : res) m.put(t._1(), t._2().size());
            return m;
        });
        assertThat(sizes).containsEntry("even", 50).containsEntry("odd", 50);
    }

    // Example 5 — inner join of two keyed datasets across the shuffle.
    @Test
    void ex05_inner_join() {
        List<Tuple2<Integer, String>> people = Arrays.asList(
                new Tuple2<>(1, "alice"), new Tuple2<>(2, "bob"), new Tuple2<>(3, "carol"));
        List<Tuple2<Integer, String>> orders = Arrays.asList(
                new Tuple2<>(1, "book"), new Tuple2<>(1, "pen"), new Tuple2<>(2, "lamp"),
                new Tuple2<>(4, "desk"));
        List<String> got = DistTestSupport.runDistributed("ex05-join", sc -> {
            RDD<Tuple2<Integer, String>> p = sc.parallelize(people, 3);
            RDD<Tuple2<Integer, String>> o = sc.parallelize(orders, 3);
            List<Tuple2<Integer, Tuple2<String, String>>> joined =
                    new PairRDDFunctions<>(p).join(o).collect();
            List<String> out = new ArrayList<>();
            for (Tuple2<Integer, Tuple2<String, String>> t : joined) {
                out.add(t._2()._1() + ":" + t._2()._2());
            }
            return out;
        });
        // carol(3) and desk(4) have no match; alice gets book+pen, bob gets lamp.
        assertThat(got).containsExactlyInAnyOrder("alice:book", "alice:pen", "bob:lamp");
    }

    // Example 6 — sortByKey (range-partitioned total order across partitions).
    @Test
    void ex06_sortByKey_total_order() {
        List<Tuple2<Integer, String>> pairs = new ArrayList<>();
        for (int i = 0; i < 50; i++) pairs.add(new Tuple2<>((i * 37) % 50, "v" + i));
        List<Integer> keys = DistTestSupport.runDistributed("ex06-sortByKey", sc -> {
            RDD<Tuple2<Integer, String>> rdd = sc.parallelize(pairs, 5);
            List<Tuple2<Integer, String>> sorted = new PairRDDFunctions<>(rdd).sortByKey().collect();
            List<Integer> ks = new ArrayList<>();
            for (Tuple2<Integer, String> t : sorted) ks.add(t._1());
            return ks;
        });
        List<Integer> expected = new ArrayList<>(keys);
        expected.sort(Integer::compareTo);
        assertThat(keys).isEqualTo(expected); // already globally sorted
    }

    // Example 7 — distinct via reduceByKey idiom over a heavily-duplicated input.
    @Test
    void ex07_distinct_count() {
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 1000; i++) data.add(i % 17);   // only 17 distinct values
        long distinct = DistTestSupport.runDistributed("ex07-distinct", sc -> {
            RDD<Tuple2<Integer, Integer>> keyed = sc.parallelize(data, 8)
                    .mapToPair(v -> new Tuple2<>(v, 1)).rdd();
            return new PairRDDFunctions<>(keyed).reduceByKey((a, b) -> a).count();
        });
        assertThat(distinct).isEqualTo(17L);
    }
}
