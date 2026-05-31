package com.minispark.scheduler.cluster;

import com.miniyarn.am.ApplicationMaster;
import com.miniyarn.common.ContainerLaunchContext;
import com.miniyarn.common.Resource;
import com.miniyarn.common.YarnMessages;
import com.minispark.rpc.RpcAddress;
import com.minispark.rpc.RpcEndpointRef;
import com.minispark.rpc.RpcEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * The "executors come from MiniYarn" launcher. Drop-in for
 * {@link ProcessExecutorLauncher}: instead of forking executor JVMs locally,
 * it asks the RM for containers; NodeManagers spawn the same
 * {@link CoarseGrainedExecutorBackend} processes, which then connect back to
 * the driver's coarse-grained scheduler endpoint exactly as before.
 *
 * <p>This is the entire integration point between the engine and the cluster
 * manager — under 100 lines because the seams in §4 of the brief already did
 * the heavy lifting. Real Spark equivalent: {@code YarnSchedulerBackend} +
 * {@code YarnAllocator}.
 */
public final class YarnExecutorLauncher implements ExecutorLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(YarnExecutorLauncher.class);

    private final RpcEnv rpcEnv;
    private final String rmHost;
    private final int rmPort;
    private final String appName;
    private final int numExecutors;
    private final int coresPerExecutor;
    private final int memoryMBPerExecutor;
    private final java.util.Map<String, String> systemProps;

    private ApplicationMaster am;

    public YarnExecutorLauncher(RpcEnv rpcEnv, String rmHost, int rmPort, String appName,
                                int numExecutors, int coresPerExecutor, int memoryMBPerExecutor,
                                java.util.Map<String, String> systemProps) {
        this.rpcEnv = rpcEnv;
        this.rmHost = rmHost;
        this.rmPort = rmPort;
        this.appName = appName;
        this.numExecutors = numExecutors;
        this.coresPerExecutor = coresPerExecutor;
        this.memoryMBPerExecutor = memoryMBPerExecutor;
        this.systemProps = systemProps;
    }

    @Override
    public void launchExecutors(RpcAddress driverAddress) {
        RpcEndpointRef rm = rpcEnv.endpointRef(YarnMessages.RM_ENDPOINT_NAME, rmHost, rmPort);
        this.am = new ApplicationMaster(appName, rpcEnv, rm);
        am.registerWithRM();

        // The launch ctx executors will be started with. Each container becomes a
        // CoarseGrainedExecutorBackend process that dials the driver. The
        // executor id slot is templated and replaced per-container in the AM —
        // but our AM reuses the ctx as-is, so we use a single id-suffix based
        // on the container id implicitly (the driver only cares it's unique).
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        List<String> command = new ArrayList<>();
        command.add(javaBin);
        // Enforce the requested executor memory at the JVM level. The container
        // ask carries the same number; without -Xmx the actual heap was the
        // platform default, making memoryMBPerExecutor advisory. With -Xmx the
        // budget is real — the JVM OOMs at the configured size, not at whatever
        // happens to fit on the host. Reserve a small overhead for native /
        // direct memory (mirrors Spark's spark.executor.memoryOverhead split,
        // which defaults to max(384, 0.1 * executorMemory)).
        if (memoryMBPerExecutor > 0) {
            long overhead = Math.max(384, (long) (memoryMBPerExecutor * 0.10));
            long heap = Math.max(64, memoryMBPerExecutor - overhead);
            command.add("-Xmx" + heap + "m");
        }
        command.add("-cp"); command.add(classpath);
        // Forward configured system properties (e.g. shuffle manager) to the container JVM.
        systemProps.forEach((k, v) -> command.add("-D" + k + "=" + v));
        command.add(CoarseGrainedExecutorBackend.class.getName());
        command.add(driverAddress.host); command.add(String.valueOf(driverAddress.port));
        // The AM expands this token to the allocated container's id so each
        // executor JVM gets a unique id without us tracking them here.
        command.add(ApplicationMaster.CONTAINER_ID_TOKEN);
        command.add(String.valueOf(coresPerExecutor));
        ContainerLaunchContext ctx = new ContainerLaunchContext(command, new HashMap<>());

        am.setNextLaunchCtx(ctx);
        am.requestExecutors(numExecutors, new Resource(coresPerExecutor, memoryMBPerExecutor), ctx);
        LOG.info("YarnExecutorLauncher submitted request for {} executor(s) of {} cores / {} MB",
                numExecutors, coresPerExecutor, memoryMBPerExecutor);
        // Returns immediately. The CoarseGrainedSchedulerBackend's registration
        // latch is what actually waits for executors to come up.
    }

    @Override
    public void stop() {
        // Best-effort: tell the RM the app is done so it can reclaim containers.
        // During shutdown the RPC channel may already be torn down — a failed
        // write here must not propagate out of MiniSparkContext.close().
        if (am != null) {
            try {
                am.unregister("SUCCEEDED");
            } catch (RuntimeException e) {
                LOG.debug("AM unregister during shutdown failed (ignored): {}", e.toString());
            }
        }
    }
}
