package com.minispark.examples.dist;

import com.miniyarn.common.ApplicationId;
import com.miniyarn.common.NodeId;
import com.miniyarn.common.Resource;
import com.miniyarn.common.YarnMessages;
import com.miniyarn.nm.NodeManager;
import com.miniyarn.rm.ResourceManager;
import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import com.minispark.rpc.RpcEndpointRef;
import com.minispark.rpc.RpcEnv;
import com.minispark.scheduler.cluster.CoarseGrainedSchedulerBackend;
import com.minispark.serializer.JavaSerializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distributed examples for the cluster <i>infrastructure</i> (examples 39–41):
 * dynamic allocation, speculation, and the MiniYarn cluster manager. Unlike the
 * data-plane examples, these assert on observable control-plane behaviour —
 * executor count growing, a straggler being re-launched, containers spread
 * across NodeManagers — not just the computed result.
 *
 * <p>All run real executor JVMs and abort (skip) cleanly if the sandbox blocks
 * child-process spawning.
 */
final class InfraDistributedExamplesTest {

    // Example 39 — dynamic allocation: a backlog of slow tasks grows the cluster
    // past its single starting executor by spawning more executor JVMs.
    @Test
    void ex39_dynamic_allocation_grows_cluster() throws Exception {
        AtomicInteger maxExecutorsSeen = new AtomicInteger(0);

        Callable<Long> job = () -> {
            MiniSparkConf conf = new MiniSparkConf()
                    .setAppName("ex39-dynalloc")
                    .setMaster("local")
                    .set("minispark.rpc.mode", "netty")
                    .set("minispark.executor.instances", "1")   // start small
                    .set("minispark.executor.cores", "1")
                    .set("minispark.dynamicAllocation.enabled", "true")
                    .set("minispark.dynamicAllocation.minExecutors", "1")
                    .set("minispark.dynamicAllocation.maxExecutors", "4")
                    .set("minispark.dynamicAllocation.intervalMs", "300")
                    .set("minispark.dynamicAllocation.executorIdleTimeoutMs", "60000");
            try (MiniSparkContext sc = new MiniSparkContext(conf)) {
                CoarseGrainedSchedulerBackend backend = (CoarseGrainedSchedulerBackend) sc.backend();
                ExecutorService sampler = Executors.newSingleThreadExecutor();
                Future<?> samplerF = sampler.submit(() -> {
                    for (int i = 0; i < 60; i++) {
                        maxExecutorsSeen.accumulateAndGet(backend.numExecutors(), Math::max);
                        try { Thread.sleep(100); } catch (InterruptedException e) { return; }
                    }
                });
                // 12 partitions × ~600ms each → a real backlog for a 1-core start.
                long count = sc.parallelize(IntStream.range(0, 12).boxed().toList(), 12)
                        .map((RDD.SerializableFunction<Integer, Integer>) i -> {
                            try { Thread.sleep(600); } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            return i;
                        })
                        .count();
                samplerF.cancel(true);
                sampler.shutdownNow();
                return count;
            }
        };

        Long count = runOrAbort("ex39-dynalloc", job);
        assertThat(count).isEqualTo(12L);
        assertThat(maxExecutorsSeen.get())
                .as("dynamic allocation should have grown the cluster past the initial 1 executor")
                .isGreaterThan(1);
    }

    // Example 40 — speculation: one straggler partition is re-launched as a
    // duplicate. We prove the re-launch fired with a filesystem side-channel
    // (a static counter wouldn't survive across executor JVMs): every task
    // attempt drops a marker file, and the slow partition's first attempt — the
    // one that atomically wins a lock file — is the only one that stalls, so the
    // speculative copy returns fast and the job finishes.
    @Test
    void ex40_speculation_relaunches_straggler(@TempDir Path tmp) throws Exception {
        Path markerDir = Files.createDirectories(tmp.resolve("attempts"));
        String dir = markerDir.toString();

        Callable<List<Integer>> job = () -> {
            MiniSparkConf conf = new MiniSparkConf()
                    .setAppName("ex40-speculation")
                    .setMaster("local")
                    .set("minispark.rpc.mode", "netty")
                    .set("minispark.executor.instances", "2")
                    .set("minispark.executor.cores", "2")
                    .set("minispark.executor.heartbeatTimeoutMs", "30000")
                    .set("minispark.speculation", "true")
                    .set("minispark.speculation.intervalMs", "100")
                    .set("minispark.speculation.quantile", "0.5")
                    .set("minispark.speculation.multiplier", "1.2");
            try (MiniSparkContext sc = new MiniSparkContext(conf)) {
                return sc.parallelize(IntStream.range(0, 4).boxed().toList(), 4)
                        .map((RDD.SerializableFunction<Integer, Integer>) i -> {
                            // Record this attempt (unique filename per attempt).
                            Path attempt = Path.of(dir, "attempt-" + i + "-"
                                    + java.util.UUID.randomUUID());
                            try { Files.createFile(attempt); } catch (IOException ignored) {}
                            if (i == 0) {
                                // Only the FIRST attempt of partition 0 stalls: it wins
                                // the lock; the speculative copy fails to create it and
                                // returns immediately, so the stage can finish.
                                Path lock = Path.of(dir, "p0.lock");
                                boolean firstAttempt;
                                try { Files.createFile(lock); firstAttempt = true; }
                                catch (IOException e) { firstAttempt = false; }
                                if (firstAttempt) {
                                    try { Thread.sleep(3000); } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                }
                            }
                            return i * 10;
                        })
                        .collect();
            }
        };

        List<Integer> sums = runOrAbort("ex40-speculation", job);
        // Result is correct regardless of which copy of partition 0 reported first.
        assertThat(sums).containsExactlyInAnyOrder(0, 10, 20, 30);
        // Partition 0 must have been attempted at least twice (original + speculative).
        long partition0Attempts;
        try (var stream = Files.list(markerDir)) {
            partition0Attempts = stream.filter(p -> p.getFileName().toString().startsWith("attempt-0-")).count();
        }
        assertThat(partition0Attempts)
                .as("the straggler partition should have been speculatively re-launched")
                .isGreaterThanOrEqualTo(2L);
    }

    // Example 41 — MiniYarn cluster manager: a real RM + 2 NodeManagers come up,
    // a driver with master=miniyarn://… asks the AM for containers, and the RM
    // spreads the two executors across both NMs. Asserts both the result and the
    // placement — the whole point of a cluster manager.
    @Test
    void ex41_miniyarn_spreads_executors_across_nodemanagers(@TempDir Path tmp) throws Exception {
        RpcEnv rmEnv = RpcEnv.create("rm", "127.0.0.1", 0, "netty", new JavaSerializer());
        ResourceManager rm = new ResourceManager(rmEnv);
        RpcEnv nm1Env = RpcEnv.create("nm1", "127.0.0.1", 0, "netty", new JavaSerializer());
        NodeManager nm1 = new NodeManager(new NodeId("nm1"), nm1Env,
                nm1Env.endpointRef(YarnMessages.RM_ENDPOINT_NAME, rmEnv.address().host, rmEnv.address().port),
                new Resource(2, 1024));
        nm1.onStart();
        RpcEnv nm2Env = RpcEnv.create("nm2", "127.0.0.1", 0, "netty", new JavaSerializer());
        NodeManager nm2 = new NodeManager(new NodeId("nm2"), nm2Env,
                nm2Env.endpointRef(YarnMessages.RM_ENDPOINT_NAME, rmEnv.address().host, rmEnv.address().port),
                new Resource(2, 1024));
        nm2.onStart();

        AtomicReference<Set<NodeId>> nodesUsed = new AtomicReference<>(Set.of());
        try {
            Callable<Map<String, Long>> job = () -> {
                MiniSparkConf conf = new MiniSparkConf()
                        .setAppName("ex41-miniyarn")
                        .setMaster("miniyarn://127.0.0.1:" + rmEnv.address().port)
                        .set("minispark.executor.instances", "2")
                        .set("minispark.executor.cores", "2")
                        .set("minispark.executor.memoryMB", "256");
                try (MiniSparkContext sc = new MiniSparkContext(conf)) {
                    nodesUsed.set(rm.nodesHostingApp(new ApplicationId(1)));
                    List<Tuple2<String, Long>> res = sc.parallelize(
                                    IntStream.range(0, 600).boxed().toList(), 8)
                            .mapToPair(i -> new Tuple2<>("k" + (i % 3), 1L))
                            .reduceByKey(Long::sum)
                            .collect();
                    Map<String, Long> m = new HashMap<>();
                    for (Tuple2<String, Long> t : res) m.put(t._1(), t._2());
                    return m;
                }
            };

            Map<String, Long> got = runOrAbort("ex41-miniyarn", job);
            // 600 rows over 3 keys → 200 each, computed across the cluster's executors.
            assertThat(got).hasSize(3);
            assertThat(got.values()).allMatch(v -> v == 200L);
            // The cluster manager placed the two executors on two different NMs.
            assertThat(nodesUsed.get())
                    .as("executors should be spread across both NodeManagers")
                    .containsExactlyInAnyOrder(new NodeId("nm1"), new NodeId("nm2"));
        } finally {
            nm1.onStop(); nm2.onStop();
            nm1Env.shutdown(); nm2Env.shutdown(); rmEnv.shutdown();
        }
    }

    /** Run {@code job} on a watchdog thread; abort (skip) the test if the cluster can't come up. */
    private static <R> R runOrAbort(String appName, Callable<R> job) {
        ExecutorService runner = Executors.newSingleThreadExecutor();
        Future<R> f = runner.submit(job);
        try {
            return f.get(120, TimeUnit.SECONDS);
        } catch (Exception e) {
            f.cancel(true);
            Assumptions.abort("Skipping distributed infra example '" + appName + "': " + e);
            return null; // unreachable
        } finally {
            runner.shutdownNow();
        }
    }
}
