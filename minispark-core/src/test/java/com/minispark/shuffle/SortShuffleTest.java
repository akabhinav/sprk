package com.minispark.shuffle;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sort shuffle must produce identical results to hash shuffle — that's the
 * whole promise of the {@link ShuffleManager} seam: swap the implementation,
 * the engine behaves the same. Here we run the same reduceByKey under each
 * manager and assert equality.
 */
final class SortShuffleTest {

    private static Map<String, Integer> wordCount(String shuffleManager) {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setMaster("local[4]")
                        .set("minispark.shuffle.manager", shuffleManager))) {

            List<String> lines = Arrays.asList(
                    "the quick brown fox",
                    "the lazy dog and the fox",
                    "quick brown quick dog",
                    "fox fox fox the the the");

            List<Tuple2<String, Integer>> result = sc.parallelize(lines, 4)
                    .flatMap((RDD.SerializableFunction<String, java.util.Iterator<String>>)
                            line -> Arrays.stream(line.split(" ")).iterator())
                    .mapToPair(w -> new Tuple2<>(w, 1))
                    .reduceByKey(Integer::sum)
                    .collect();

            Map<String, Integer> got = new HashMap<>();
            for (Tuple2<String, Integer> t : result) got.put(t._1(), t._2());
            return got;
        }
    }

    @Test
    void sort_shuffle_matches_hash_shuffle() {
        Map<String, Integer> hash = wordCount("hash");
        Map<String, Integer> sort = wordCount("sort");

        // Spot-check a couple of known counts, then assert full equality.
        assertThat(sort).containsEntry("the", 6).containsEntry("fox", 5).containsEntry("quick", 3);
        assertThat(sort).isEqualTo(hash);
    }
}
