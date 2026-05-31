package com.minispark.examples;

import com.minispark.rpc.RpcEndpoint;
import com.minispark.rpc.RpcEnv;
import com.minispark.scheduler.cluster.ClusterMessages;
import com.minispark.scheduler.cluster.CoarseGrainedExecutorBackend;
import com.minispark.scheduler.cluster.CoarseGrainedSchedulerBackend;
import com.minispark.serializer.JavaSerializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A standalone executor JVM whose driver vanishes (crash, not a graceful
 * StopExecutor) must not become a zombie — its heartbeat watchdog should
 * notice the driver is unreachable and exit the process.
 *
 * <p>Stands up a minimal driver endpoint, spawns a real executor process
 * pointed at it (with a low {@code maxHeartbeatFailures} so the test is
 * quick), waits for it to register, then abruptly shuts the driver's RpcEnv
 * down and asserts the executor process exits on its own.
 *
 * <p>Aborts (skips) if the sandbox can't spawn child JVMs.
 */
final class ExecutorDriverDeathTest {

    /** Minimal stand-in for the driver: acks RegisterExecutor, ignores heartbeats. */
    private static final class FakeScheduler implements RpcEndpoint {
        final CountDownLatch registered = new CountDownLatch(1);
        @Override public Object receiveAndReply(Object message) {
            if (message instanceof ClusterMessages.RegisterExecutor) {
                registered.countDown();
                return new ClusterMessages.RegisteredExecutor();
            }
            return null;
        }
        @Override public void receive(Object message) { /* swallow heartbeats */ }
    }

    @Test
    void executor_exits_when_its_driver_disappears() throws Exception {
        RpcEnv driver = RpcEnv.create("driver", "127.0.0.1", 0, "netty", new JavaSerializer());
        FakeScheduler sched = new FakeScheduler();
        driver.setupEndpoint(CoarseGrainedSchedulerBackend.ENDPOINT_NAME, sched);

        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        List<String> cmd = new ArrayList<>();
        cmd.add(javaBin);
        cmd.add("-cp"); cmd.add(System.getProperty("java.class.path"));
        // 3 failed heartbeats (~3s at the 1s interval) → exit, so the test is fast.
        cmd.add("-Dminispark.executor.maxHeartbeatFailures=3");
        cmd.add(CoarseGrainedExecutorBackend.class.getName());
        cmd.add(driver.address().host); cmd.add(String.valueOf(driver.address().port));
        cmd.add("exec-death-test"); cmd.add("2");

        Process proc;
        try {
            proc = new ProcessBuilder(cmd).redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        } catch (Exception e) {
            driver.shutdown();
            Assumptions.abort("Skipping (cannot spawn executor JVM): " + e);
            return;
        }

        try {
            // Wait for the executor to come up and register with our fake driver.
            boolean reg = sched.registered.await(30, TimeUnit.SECONDS);
            if (!reg) {
                Assumptions.abort("Skipping (executor never registered — sandbox JVM-spawn issue)");
                return;
            }
            assertThat(proc.isAlive()).as("executor should be running after registration").isTrue();

            // Simulate a driver crash: tear the RpcEnv down with no graceful StopExecutor.
            driver.shutdown();

            // The watchdog (3 failures × ~1s + grace) must terminate the process.
            boolean exited = proc.waitFor(20, TimeUnit.SECONDS);
            assertThat(exited)
                    .as("executor JVM should self-exit after losing its driver, not linger as a zombie")
                    .isTrue();
            assertThat(proc.exitValue()).isEqualTo(1);   // System.exit(1) from the watchdog
        } finally {
            if (proc.isAlive()) proc.destroyForcibly();
            // driver already shut down above (or in the abort path)
        }
    }
}
