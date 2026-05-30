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
    private final List<Process> processes = new ArrayList<>();

    public ProcessExecutorLauncher(int numExecutors, int coresPerExecutor) {
        this.numExecutors = numExecutors;
        this.coresPerExecutor = coresPerExecutor;
    }

    @Override
    public void launchExecutors(RpcAddress driverAddress) {
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        for (int i = 0; i < numExecutors; i++) {
            String execId = "proc-" + i;
            List<String> cmd = new ArrayList<>(List.of(
                    javaBin, "-cp", classpath,
                    CoarseGrainedExecutorBackend.class.getName(),
                    driverAddress.host, String.valueOf(driverAddress.port),
                    execId, String.valueOf(coresPerExecutor)));
            try {
                Process p = new ProcessBuilder(cmd)
                        .redirectErrorStream(true)
                        .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                        .start();
                processes.add(p);
                LOG.info("Spawned executor JVM {} (pid {})", execId, p.pid());
            } catch (Exception e) {
                throw new RuntimeException("Failed to launch executor process " + execId, e);
            }
        }
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
