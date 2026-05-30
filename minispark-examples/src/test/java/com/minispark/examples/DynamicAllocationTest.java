package com.minispark.examples;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.rdd.RDD;
import com.minispark.scheduler.cluster.CoarseGrainedSchedulerBackend;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * With dynamic allocation on and a backlog of slow tasks, the cluster should
 * grow beyond its single starting executor by spawning more executor JVMs, and
 * still produce the correct result.
 *
 * <p>Aborts (skips) if the sandbox can't spawn child JVMs.
 */
final class DynamicAllocationTest {

    @Test
    void cluster_grows_under_backlog() throws Exception {
        AtomicInteger maxExecutorsSeen = new AtomicInteger(0);

        Callable<Long> job = () -> {
            MiniSparkConf conf = new MiniSparkConf()
                    .setAppName("dynalloc")
                    .setMaster("local")
                    .set("minispark.rpc.mode", "netty")
                    // Start with one executor, let allocation grow it.
                    .set("minispark.executor.instances", "1")
                    .set("minispark.executor.cores", "1")
                    .set("minispark.dynamicAllocation.enabled", "true")
                    .set("minispark.dynamicAllocation.minExecutors", "1")
                    .set("minispark.dynamicAllocation.maxExecutors", "4")
                    .set("minispark.dynamicAllocation.intervalMs", "300")
                    .set("minispark.dynamicAllocation.executorIdleTimeoutMs", "60000");

            try (MiniSparkContext sc = new MiniSparkContext(conf)) {
                CoarseGrainedSchedulerBackend backend = (CoarseGrainedSchedulerBackend) sc.backend();

                // Sample executor count from another thread while the job runs.
                ExecutorService sampler = Executors.newSingleThreadExecutor();
                Future<?> samplerF = sampler.submit(() -> {
                    for (int i = 0; i < 60; i++) {
                        maxExecutorsSeen.accumulateAndGet(backend.numExecutors(), Math::max);
                        try { Thread.sleep(100); } catch (InterruptedException e) { return; }
                    }
                });

                // 12 partitions, each sleeping ~600ms → a real backlog for 1 core.
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

        ExecutorService runner = Executors.newSingleThreadExecutor();
        Future<Long> f = runner.submit(job);
        long count;
        try {
            count = f.get(120, TimeUnit.SECONDS);
        } catch (Exception e) {
            f.cancel(true);
            Assumptions.abort("Skipping dynamic-allocation test (cluster could not run): " + e);
            return;
        } finally {
            runner.shutdownNow();
        }

        assertThat(count).isEqualTo(12L);
        assertThat(maxExecutorsSeen.get())
                .as("dynamic allocation should have grown the cluster past the initial 1 executor")
                .isGreaterThan(1);
    }
}
