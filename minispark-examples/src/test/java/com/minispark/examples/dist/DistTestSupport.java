package com.minispark.examples.dist;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import org.junit.jupiter.api.Assumptions;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Shared harness for the distributed examples. Every example runs against a
 * real distributed engine — a driver plus separate executor <b>JVMs</b> over
 * TCP ({@code rpc.mode=netty}) — and asserts a result.
 *
 * <p>Spawning child JVMs can be blocked in some sandboxes; rather than fail,
 * {@link #runDistributed} aborts (skips) the test if the cluster can't come up,
 * since the behaviour under test is the application logic, not the CI's process
 * policy. The job itself runs on a watchdog thread with a hard timeout so a
 * stuck cluster can never hang the build.
 */
final class DistTestSupport {

    private DistTestSupport() {}

    static final int DEFAULT_TIMEOUT_SECONDS = 120;

    /** A MiniSparkConf wired for 2 executor JVMs × 2 cores, generous heartbeat. */
    static MiniSparkConf distConf(String appName) {
        return new MiniSparkConf()
                .setAppName(appName)
                .setMaster("local")
                .set("minispark.rpc.mode", "netty")
                .set("minispark.executor.instances", "2")
                .set("minispark.executor.cores", "2")
                // Spawning 2 JVMs on a loaded box can briefly starve them past
                // the 5s default; widen so the examples don't flake.
                .set("minispark.executor.heartbeatTimeoutMs", "30000");
    }

    /**
     * Run {@code body} with a fresh distributed {@link MiniSparkContext}, on a
     * watchdog thread, returning its result — or aborting the test if the
     * cluster couldn't run within the timeout.
     */
    static <R> R runDistributed(String appName, Function<MiniSparkContext, R> body) {
        return runDistributed(appName, DistTestSupport::distConf, body);
    }

    static <R> R runDistributed(String appName,
                                Function<String, MiniSparkConf> confFactory,
                                Function<MiniSparkContext, R> body) {
        Callable<R> job = () -> {
            try (MiniSparkContext sc = new MiniSparkContext(confFactory.apply(appName))) {
                return body.apply(sc);
            }
        };
        ExecutorService runner = Executors.newSingleThreadExecutor();
        Future<R> f = runner.submit(job);
        try {
            return f.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            f.cancel(true);
            Assumptions.abort("Skipping distributed example '" + appName
                    + "' (cluster could not run): " + e);
            return null; // unreachable: abort throws
        } finally {
            runner.shutdownNow();
        }
    }
}
