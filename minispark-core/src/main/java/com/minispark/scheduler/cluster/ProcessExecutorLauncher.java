package com.minispark.scheduler.cluster;

import com.minispark.rpc.RpcAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Launches each executor as a <b>separate JVM process</b> running
 * {@link CoarseGrainedExecutorBackend#main}. The child connects back to the
 * driver over Netty RPC. This is the first point at which "driver" and
 * "executor" are genuinely different OS processes.
 *
 * <p>The child JVM is started with {@code $JAVA_HOME/bin/java} and the parent's
 * classpath, so application closures (the user's lambdas) resolve identically.
 * Phase 5's MiniYarn replaces this with NodeManagers launching containers on
 * other hosts; the contract — "spawn a process that runs the executor main and
 * dials home" — is the same.
 *
 * Real Spark equivalent: org.apache.spark.deploy.* ExecutorRunner / YARN ExecutorRunnable
 */
public final class ProcessExecutorLauncher implements ExecutorLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessExecutorLauncher.class);

    private final int numExecutors;
    private final int coresPerExecutor;
    /** System properties forwarded to each child JVM (e.g. shuffle manager choice). */
    private final java.util.Map<String, String> systemProps;
    private final List<Process> processes = new ArrayList<>();
    private final java.util.Map<String, Process> byId = new java.util.concurrent.ConcurrentHashMap<>();

    public ProcessExecutorLauncher(int numExecutors, int coresPerExecutor,
                                   java.util.Map<String, String> systemProps) {
        this.numExecutors = numExecutors;
        this.coresPerExecutor = coresPerExecutor;
        this.systemProps = systemProps;
    }

    @Override
    public void launchExecutors(RpcAddress driverAddress) {
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        for (int i = 0; i < numExecutors; i++) {
            String execId = "proc-" + i;
            List<String> cmd = new ArrayList<>();
            cmd.add(javaBin);
            cmd.add("-cp"); cmd.add(classpath);
            // Forward configured system properties (e.g. shuffle manager) so the
            // child builds a SparkEnv compatible with the driver's.
            systemProps.forEach((k, v) -> cmd.add("-D" + k + "=" + v));
            cmd.add(CoarseGrainedExecutorBackend.class.getName());
            cmd.add(driverAddress.host); cmd.add(String.valueOf(driverAddress.port));
            cmd.add(execId); cmd.add(String.valueOf(coresPerExecutor));
            try {
                Process p = new ProcessBuilder(cmd)
                        .redirectErrorStream(true)
                        .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                        .start();
                processes.add(p);
                byId.put(execId, p);
                LOG.info("Spawned executor JVM {} (pid {})", execId, p.pid());
            } catch (Exception e) {
                throw new RuntimeException("Failed to launch executor process " + execId, e);
            }
        }
    }

    /**
     * Test hook: hard-kill the named executor process. The driver's heartbeat
     * watchdog will detect the silence and trigger the lost-executor recovery
     * path. Used by fault-tolerance tests; not part of the production API.
     */
    public boolean killExecutorForTest(String executorId) {
        Process p = byId.get(executorId);
        if (p == null) return false;
        LOG.warn("Test hook: killing executor process {} (pid {})", executorId, p.pid());
        p.destroyForcibly();
        return true;
    }

    @Override
    public void stop() {
        for (Process p : processes) {
            p.destroy();
        }
        for (Process p : processes) {
            try { p.waitFor(5, java.util.concurrent.TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            if (p.isAlive()) p.destroyForcibly();
        }
    }
}
