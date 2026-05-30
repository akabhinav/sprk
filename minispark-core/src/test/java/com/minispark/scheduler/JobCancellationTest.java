package com.minispark.scheduler;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A long-running job, cancelled from another thread, should fail fast rather
 * than run to completion.
 *
 * <p>Note we cannot use a {@code CountDownLatch} captured in the task closure to
 * detect "task started": closures are serialized before they run (even in local
 * mode), so the executor would count down a <i>clone</i> of the latch, not the
 * driver's. Instead we sleep a fixed, generous interval before cancelling — far
 * shorter than the task's own sleep, so the cancel still lands mid-flight.
 */
final class JobCancellationTest {

    @Test
    void cancelAllJobs_aborts_a_running_job() throws Exception {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setMaster("local[2]"))) {

            AtomicReference<Throwable> caught = new AtomicReference<>();

            ExecutorService bg = Executors.newSingleThreadExecutor();
            Future<?> job = bg.submit(() -> {
                try {
                    sc.parallelize(List.of(1, 2, 3, 4), 4)
                            .map((RDD.SerializableFunction<Integer, Integer>) i -> {
                                // Long enough that the cancel lands mid-flight.
                                try { Thread.sleep(30_000); } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return i;
                            })
                            .collect();
                } catch (Throwable t) {
                    caught.set(t);
                }
            });

            // Give the job time to submit its stage and get tasks in-flight.
            Thread.sleep(1000);
            sc.cancelAllJobs();

            // The blocked collect() must return (throwing) well before the 30s sleep.
            job.get(10, TimeUnit.SECONDS);
            bg.shutdownNow();

            assertThat(caught.get())
                    .as("cancelled job should surface an exception")
                    .isNotNull();
            // The abort reason is carried as the cause of the wrapping
            // "ResultStage N failed" RuntimeException; check the whole chain.
            String chain = throwableChain(caught.get()).toLowerCase();
            assertThat(chain).contains("abort");
        }
    }

    private static String throwableChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            sb.append(cur).append(" | ");
            if (cur.getCause() == cur) break;
        }
        return sb.toString();
        }
    }
}
