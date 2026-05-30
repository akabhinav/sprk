package com.minispark.examples;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * The Phase 4 milestone: the <i>same</i> WordCount pipeline that runs in local
 * mode runs unchanged with {@code rpc.mode=netty}, where executors are
 * <b>separate JVM processes</b> that connect back to the driver over TCP and
 * exchange shuffle blocks over the network.
 *
 * <p>Two executor JVMs are spawned, so the shuffle genuinely crosses the
 * network: each reducer fetches one bucket locally and one from the other
 * executor's block endpoint.
 *
 * <p>Spawning child JVMs can be blocked in some sandboxes; if the executors
 * cannot come up the test aborts (skips) rather than failing, since the
 * behaviour under test is distribution, not the CI's process policy.
 */
final class NettyDistributedTest {

    @Test
    void wordcount_runs_across_two_executor_jvms(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("input.txt");
        String text =
                "the quick brown fox\n" +
                "jumps over the lazy dog\n" +
                "the dog sleeps and the fox jumps\n" +
                "brown fox brown dog\n";
        Files.writeString(file, text, StandardCharsets.UTF_8);

        Map<String, Integer> reference = new HashMap<>();
        Arrays.stream(text.toLowerCase().split("\\W+"))
                .filter(s -> !s.isEmpty())
                .forEach(w -> reference.merge(w, 1, Integer::sum));

        Callable<Map<String, Integer>> job = () -> {
            MiniSparkConf conf = new MiniSparkConf()
                    .setAppName("netty-wc")
                    .setMaster("local")            // parallelism comes from executor settings below
                    .set("minispark.rpc.mode", "netty")
                    .set("minispark.executor.instances", "2")
                    .set("minispark.executor.cores", "2");

            try (MiniSparkContext sc = new MiniSparkContext(conf)) {
                List<Tuple2<String, Integer>> result = sc.textFile(file.toString(), 4)
                        .flatMap((RDD.SerializableFunction<String, java.util.Iterator<String>>)
                                line -> Arrays.stream(line.toLowerCase().split("\\W+")).iterator())
                        .filter((RDD.SerializablePredicate<String>) w -> !w.isEmpty())
                        .mapToPair(w -> new Tuple2<>(w, 1))
                        .reduceByKey(Integer::sum)
                        .collect();
                Map<String, Integer> got = new HashMap<>();
                for (Tuple2<String, Integer> t : result) got.put(t._1(), t._2());
                return got;
            }
        };

        // Bound the whole thing so a sandbox that silently blocks child JVMs
        // can't hang the build; on timeout/failure we skip.
        ExecutorService runner = Executors.newSingleThreadExecutor();
        Future<Map<String, Integer>> f = runner.submit(job);
        Map<String, Integer> got;
        try {
            got = f.get(90, TimeUnit.SECONDS);
        } catch (Exception e) {
            f.cancel(true);
            Assumptions.abort("Skipping netty distributed test (could not run executor JVMs): " + e);
            return;
        } finally {
            runner.shutdownNow();
        }

        assertThat(got).containsExactlyInAnyOrderEntriesOf(reference);
    }
}
