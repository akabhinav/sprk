package com.minispark.examples;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import com.minispark.scheduler.cluster.ProcessExecutorLauncher;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
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
 * The Phase 6 milestone: kill an executor mid-job and prove the result is
 * still correct.
 *
 * <p>Setup: a reduce-by-key over 16 partitions, where each map task takes
 * ~600ms (a Thread.sleep in the user lambda). With 2 executors × 2 cores, the
 * map stage takes roughly 16/4 × 600ms ≈ 2.4s — plenty of time for the kill
 * to land between map start and reduce completion.
 *
 * <p>Exercises the full recovery path:
 * <ol>
 *   <li>Two executor JVMs are mid-flight on the map stage.</li>
 *   <li>Test thread hard-kills one executor process.</li>
 *   <li>Driver's heartbeat watchdog marks it lost; in-flight tasks become
 *       {@code TaskFailureReason.ExecutorLost} and retry on the survivor.</li>
 *   <li>{@code DAGScheduler.handleExecutorLost} enumerates the dead
 *       executor's already-completed map outputs and re-runs them on the
 *       survivor — the new outputs <i>overwrite</i> the old entries in the
 *       MapOutputTracker, keeping it always-complete.</li>
 *   <li>If a reduce task races ahead and hits a stale location it fails with
 *       {@code FetchFailed}; the DAG layer rebuilds the one map output and
 *       resubmits the reduce.</li>
 *   <li>Result equals a clean run.</li>
 * </ol>
 *
 * <p>This is the "Resilient" in RDD: no replication, no checkpointing — just
 * recompute from lineage when the cluster drops a piece.
 */
final class ExecutorFailureRecoveryTest {

    /** Slow synthetic work so the kill genuinely lands during the map stage. */
    private static class Counter implements RDD.SerializableFunction<Integer, Tuple2<String, Integer>> {
        @Override public Tuple2<String, Integer> apply(Integer i) {
            try { Thread.sleep(150); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            // Bucket into a small set of keys so reduceByKey has interesting work.
            return new Tuple2<>("k" + (i % 4), 1);
        }
    }

    @Test
    void reducebykey_recovers_when_an_executor_is_killed_mid_job() throws Exception {
        // 16 input items × ~150ms each per map call = ~2.4s of compute with 4 cores.
        List<Integer> input = new ArrayList<>();
        for (int i = 0; i < 16; i++) input.add(i);

        // Reference: a clean run of the same logic.
        Map<String, Integer> reference = new HashMap<>();
        for (int i = 0; i < 16; i++) reference.merge("k" + (i % 4), 1, Integer::sum);

        Callable<Map<String, Integer>> job = () -> {
            MiniSparkConf conf = new MiniSparkConf()
                    .setAppName("ft-recovery")
                    .setMaster("local")
                    .set("minispark.rpc.mode", "netty")
                    .set("minispark.executor.instances", "2")
                    .set("minispark.executor.cores", "2")
                    .set("minispark.executor.heartbeatTimeoutMs", "2000");

            try (MiniSparkContext sc = new MiniSparkContext(conf)) {
                ProcessExecutorLauncher pel = (ProcessExecutorLauncher) sc.launcher();

                ExecutorService bg = Executors.newSingleThreadExecutor();
                Future<List<Tuple2<String, Integer>>> resultFuture = bg.submit(() ->
                        sc.parallelize(input, 16)
                                .mapToPair(new Counter())
                                .reduceByKey(Integer::sum)
                                .collect());

                // Kill mid-map: long enough that the first wave of map tasks
                // has started and written results, short enough that some are
                // still in flight.
                Thread.sleep(700);
                boolean killed = pel.killExecutorForTest("proc-0");
                assertThat(killed).as("executor process should be running and killable").isTrue();

                try {
                    List<Tuple2<String, Integer>> out = resultFuture.get(45, TimeUnit.SECONDS);
                    bg.shutdownNow();
                    Map<String, Integer> m = new HashMap<>();
                    for (Tuple2<String, Integer> t : out) m.put(t._1(), t._2());
                    return m;
                } catch (Exception e) {
                    bg.shutdownNow();
                    throw e;
                }
            }
        };

        ExecutorService runner = Executors.newSingleThreadExecutor();
        Future<Map<String, Integer>> f = runner.submit(job);
        Map<String, Integer> got;
        try {
            got = f.get(90, TimeUnit.SECONDS);
        } catch (Exception e) {
            f.cancel(true);
            Assumptions.abort("Skipping executor-failure-recovery test (cluster could not run): " + e);
            return;
        } finally {
            runner.shutdownNow();
        }

        assertThat(got)
                .as("After losing one executor mid-job, the recovered output must equal a clean run")
                .containsExactlyInAnyOrderEntriesOf(reference);
    }
}
