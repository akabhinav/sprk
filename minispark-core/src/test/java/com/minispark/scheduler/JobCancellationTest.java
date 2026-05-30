package com.minispark.scheduler;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.rdd.RDD;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A long-running job, cancelled from another thread, should fail fast rather
 * than run to completion.
 */
final class JobCancellationTest {

    @Test
    void cancelAllJobs_aborts_a_running_job() throws Exception {
        try (MiniSparkContext sc = new MiniSparkContext(
                new MiniSparkConf().setMaster("local[2]"))) {

            CountDownLatch started = new CountDownLatch(1);
            AtomicReference<Throwable> caught = new AtomicReference<>();

            ExecutorService bg = Executors.newSingleThreadExecutor();
            Future<?> job = bg.submit(() -> {
                try {
                    sc.parallelize(List.of(1, 2, 3, 4), 4)
                            .map((RDD.SerializableFunction<Integer, Integer>) i -> {
                                started.countDown();
                                // Long enough that the cancel lands mid-flight.
                                try { Thread.sleep(20_000); } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return i;
                            })
                            .collect();
                } catch (Throwable t) {
                    caught.set(t);
                }
            });

            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(200); // let tasks be in-flight
            sc.cancelAllJobs();

            // The blocked collect() must return (throwing) well before the 20s sleep.
            job.get(10, TimeUnit.SECONDS);
            bg.shutdownNow();

            assertThat(caught.get())
                    .as("cancelled job should surface an exception")
                    .isNotNull();
            assertThat(caught.get().toString().toLowerCase()).contains("abort");
        }
    }
}
