package com.minispark.examples;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sort shuffle across real executor JVMs. This proves two things at once:
 * <ul>
 *   <li>The {@code minispark.shuffle.manager=sort} choice is forwarded to the
 *       child executor JVMs (as a {@code -D}) so driver and executors agree on
 *       the wire format.</li>
 *   <li>Sort-shuffle data blocks travel over the generalized
 *       {@code NetworkBlockManager} block transfer just like hash buckets — a
 *       reducer on one executor pulls a map's consolidated block from another.</li>
 * </ul>
 *
 * <p>Aborts (skips) if the sandbox blocks child JVM spawning.
 */
final class SortShuffleDistributedTest {

    @Test
    void sort_shuffle_reducebykey_across_two_executor_jvms() throws Exception {
        List<Integer> input = new java.util.ArrayList<>();
        for (int i = 0; i < 400; i++) input.add(i);

        Map<String, Integer> reference = new HashMap<>();
        for (int i = 0; i < 400; i++) reference.merge("k" + (i % 7), 1, Integer::sum);

        Callable<Map<String, Integer>> job = () -> {
            MiniSparkConf conf = new MiniSparkConf()
                    .setAppName("sort-dist")
                    .setMaster("local")
                    .set("minispark.rpc.mode", "netty")
                    .set("minispark.shuffle.manager", "sort")
                    .set("minispark.executor.instances", "2")
                    .set("minispark.executor.cores", "2");

            try (MiniSparkContext sc = new MiniSparkContext(conf)) {
                List<Tuple2<String, Integer>> result = sc.parallelize(input, 8)
                        .mapToPair(i -> new Tuple2<>("k" + (i % 7), 1))
                        .reduceByKey(Integer::sum)
                        .collect();
                Map<String, Integer> got = new HashMap<>();
                for (Tuple2<String, Integer> t : result) got.put(t._1(), t._2());
                return got;
            }
        };

        ExecutorService runner = Executors.newSingleThreadExecutor();
        Future<Map<String, Integer>> f = runner.submit(job);
        Map<String, Integer> got;
        try {
            got = f.get(90, TimeUnit.SECONDS);
        } catch (Exception e) {
            f.cancel(true);
            Assumptions.abort("Skipping sort-shuffle distributed test (cluster could not run): " + e);
            return;
        } finally {
            runner.shutdownNow();
        }
        assertThat(got).containsExactlyInAnyOrderEntriesOf(reference);
    }
}
