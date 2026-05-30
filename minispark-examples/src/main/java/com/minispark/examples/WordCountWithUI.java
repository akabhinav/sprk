package com.minispark.examples;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;

import java.util.Arrays;
import java.util.List;

/**
 * WordCount with the web UI enabled. Runs a job, prints the UI URL, and (if a
 * positive number of seconds is given as arg[1]) keeps the driver alive so you
 * can open the page in a browser.
 *
 * <pre>
 *   mvn -pl minispark-examples -am install -DskipTests
 *   mvn -pl minispark-examples exec:java \
 *       -Dexec.mainClass=com.minispark.examples.WordCountWithUI \
 *       -Dexec.args="/path/to/book.txt 60"
 * </pre>
 * Then browse to the printed http://127.0.0.1:4040/ (auto-refreshes every 2s).
 */
public final class WordCountWithUI {

    public static void main(String[] args) throws Exception {
        String path = args.length > 0 ? args[0] : null;
        int keepAliveSeconds = args.length > 1 ? Integer.parseInt(args[1]) : 0;

        MiniSparkConf conf = new MiniSparkConf()
                .setAppName("WordCountWithUI")
                .setMaster("local[4]")
                .set("minispark.ui.enabled", "true")
                .set("minispark.ui.port", "4040");

        try (MiniSparkContext sc = new MiniSparkContext(conf)) {
            System.out.println("MiniSpark UI: http://127.0.0.1:" + sc.uiPort() + "/");

            RDD<String> source = (path != null)
                    ? sc.textFile(path, 8)
                    : sc.parallelize(sampleLines(), 8);

            List<Tuple2<String, Integer>> top = source
                    .flatMap((RDD.SerializableFunction<String, java.util.Iterator<String>>)
                            line -> Arrays.stream(line.toLowerCase().split("\\W+")).iterator())
                    .filter((RDD.SerializablePredicate<String>) w -> !w.isEmpty())
                    .mapToPair(w -> new Tuple2<>(w, 1))
                    .reduceByKey(Integer::sum)
                    .collect();

            top.stream()
                    .sorted((a, b) -> Integer.compare(b._2(), a._2()))
                    .limit(10)
                    .forEach(t -> System.out.println("  " + t._1() + " = " + t._2()));

            if (keepAliveSeconds > 0) {
                System.out.println("Keeping driver alive " + keepAliveSeconds + "s for UI browsing...");
                Thread.sleep(keepAliveSeconds * 1000L);
            }
        }
    }

    private static List<String> sampleLines() {
        return Arrays.asList(
                "the quick brown fox jumps over the lazy dog",
                "the dog barks and the fox runs",
                "a quick fox is a clever fox",
                "the lazy dog sleeps while the quick fox jumps");
    }
}
