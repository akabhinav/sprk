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
    private final int executorMemoryMB;
    /** System properties forwarded to each child JVM (e.g. shuffle manager choice). */
    private final java.util.Map<String, String> systemProps;
    private final java.util.Map<String, Process> byId = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicInteger idGen =
            new java.util.concurrent.atomic.AtomicInteger();
    // Captured at launch so runtime requestExecutors() can spawn more.
    private volatile RpcAddress driverAddress;

    public ProcessExecutorLauncher(int numExecutors, int coresPerExecutor,
                                   java.util.Map<String, String> systemProps) {
        this(numExecutors, coresPerExecutor, 0, systemProps);
    }

    public ProcessExecutorLauncher(int numExecutors, int coresPerExecutor, int executorMemoryMB,
                                   java.util.Map<String, String> systemProps) {
        this.numExecutors = numExecutors;
        this.coresPerExecutor = coresPerExecutor;
        this.executorMemoryMB = executorMemoryMB;
        this.systemProps = systemProps;
    }

    @Override
    public void launchExecutors(RpcAddress driverAddress) {
        this.driverAddress = driverAddress;
        for (int i = 0; i < numExecutors; i++) spawnOne();
    }

    @Override public boolean supportsDynamicAllocation() { return true; }

    @Override
    public synchronized List<String> requestExecutors(int n) {
        List<String> ids = new ArrayList<>(n);
        for (int i = 0; i < n; i++) ids.add(spawnOne());
        return ids;
    }

    @Override
    public synchronized void killExecutor(String executorId) {
        Process p = byId.remove(executorId);
        if (p != null) {
            LOG.info("Dynamic allocation: stopping idle executor {} (pid {})", executorId, p.pid());
            p.destroy();
        }
    }

    /** Spawn one executor JVM with a fresh id; returns the id. */
    private String spawnOne() {
        if (driverAddress == null) throw new IllegalStateException("launchExecutors not called yet");
        String execId = "proc-" + idGen.getAndIncrement();
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        List<String> cmd = new ArrayList<>();
        cmd.add(javaBin);
        // Enforce the configured executor memory budget at the JVM level. Without
        // this, executorMemoryMB was advisory: the cluster manager sized the
        // container by it but the JVM heap was the platform default. -Xmx makes
        // it real; OOMs now happen at the configured budget instead of whenever
        // the JVM happens to grow.
        if (executorMemoryMB > 0) cmd.add("-Xmx" + executorMemoryMB + "m");
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
            byId.put(execId, p);
            LOG.info("Spawned executor JVM {} (pid {})", execId, p.pid());
            return execId;
        } catch (Exception e) {
            throw new RuntimeException("Failed to launch executor process " + execId, e);
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
        java.util.Collection<Process> procs = new ArrayList<>(byId.values());
        for (Process p : procs) p.destroy();
        for (Process p : procs) {
            try { p.waitFor(5, java.util.concurrent.TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            if (p.isAlive()) p.destroyForcibly();
        }
    }
}
