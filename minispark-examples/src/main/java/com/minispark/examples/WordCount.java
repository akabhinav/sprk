package com.minispark.examples;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;

import java.util.Arrays;
import java.util.List;

/**
 * The canonical Spark demo: count word occurrences in a text file.
 *
 * <p>The pipeline below has exactly one shuffle ({@code reduceByKey}), so the
 * DAGScheduler will produce two stages: a {@code ShuffleMapStage} for the
 * tokenize/map-side-combine, and a {@code ResultStage} for the reduce-side
 * combine and {@code collect}.
 */
public final class WordCount {

    public static void main(String[] args) {
        String path = args.length > 0 ? args[0] : "README.txt";

        MiniSparkConf conf = new MiniSparkConf()
                .setAppName("WordCount")
                .setMaster("local[4]");

        try (MiniSparkContext sc = new MiniSparkContext(conf)) {
            List<Tuple2<String, Integer>> counts = sc.textFile(path)
                    .flatMap((RDD.SerializableFunction<String, java.util.Iterator<String>>)
                            line -> Arrays.stream(line.toLowerCase().split("\\W+")).iterator())
                    .filter((RDD.SerializablePredicate<String>) w -> !w.isEmpty())
                    .mapToPair(w -> new Tuple2<>(w, 1))
                    .reduceByKey(Integer::sum)
                    .collect();

            counts.stream()
                    .sorted((a, b) -> Integer.compare(b._2(), a._2()))
                    .limit(20)
                    .forEach(t -> System.out.println(t._1() + "\t" + t._2()));
        }
    }
}
