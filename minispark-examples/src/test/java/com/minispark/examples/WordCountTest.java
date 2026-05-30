package com.minispark.examples;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end: textFile -> flatMap -> map -> reduceByKey -> collect. */
final class WordCountTest {

    @Test
    void wordcount_matches_jdk_reference(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("input.txt");
        String text =
                "the quick brown fox\n" +
                "jumps over the lazy dog\n" +
                "the dog sleeps\n" +
                "the fox jumps again\n";
        Files.writeString(file, text, StandardCharsets.UTF_8);

        Map<String, Integer> reference = new HashMap<>();
        Arrays.stream(text.toLowerCase().split("\\W+"))
                .filter(s -> !s.isEmpty())
                .forEach(w -> reference.merge(w, 1, Integer::sum));

        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setAppName("wc").setMaster("local[3]"))) {

            List<Tuple2<String, Integer>> result = sc.textFile(file.toString(), 3)
                    .flatMap((RDD.SerializableFunction<String, java.util.Iterator<String>>)
                            line -> Arrays.stream(line.toLowerCase().split("\\W+")).iterator())
                    .filter((RDD.SerializablePredicate<String>) w -> !w.isEmpty())
                    .mapToPair(w -> new Tuple2<>(w, 1))
                    .reduceByKey(Integer::sum)
                    .collect();

            Map<String, Integer> got = new HashMap<>();
            for (Tuple2<String, Integer> t : result) got.put(t._1(), t._2());

            assertThat(got).containsExactlyInAnyOrderEntriesOf(reference);
        }
    }

    @Test
    void wordcount_result_invariant_across_parallelism(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("input.txt");
        Files.writeString(file,
                "alpha beta alpha\nbeta gamma alpha\ndelta alpha beta\n",
                StandardCharsets.UTF_8);

        Map<String, Integer> first = null;
        for (int parallelism : new int[]{1, 2, 4}) {
            try (MiniSparkContext sc = new MiniSparkContext(
                    new MiniSparkConf().setMaster("local[" + parallelism + "]"))) {
                List<Tuple2<String, Integer>> result = sc.textFile(file.toString(), parallelism)
                        .flatMap((RDD.SerializableFunction<String, java.util.Iterator<String>>)
                                line -> Arrays.stream(line.toLowerCase().split("\\W+")).iterator())
                        .filter((RDD.SerializablePredicate<String>) w -> !w.isEmpty())
                        .mapToPair(w -> new Tuple2<>(w, 1))
                        .reduceByKey(Integer::sum)
                        .collect();
                Map<String, Integer> got = new HashMap<>();
                for (Tuple2<String, Integer> t : result) got.put(t._1(), t._2());
                if (first == null) first = got;
                else assertThat(got).isEqualTo(first);
            }
        }
    }
}
