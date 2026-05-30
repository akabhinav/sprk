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

    private ApplicationMaster am;

    public YarnExecutorLauncher(RpcEnv rpcEnv, String rmHost, int rmPort, String appName,
                                int numExecutors, int coresPerExecutor, int memoryMBPerExecutor) {
        this.rpcEnv = rpcEnv;
        this.rmHost = rmHost;
        this.rmPort = rmPort;
        this.appName = appName;
        this.numExecutors = numExecutors;
        this.coresPerExecutor = coresPerExecutor;
        this.memoryMBPerExecutor = memoryMBPerExecutor;
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
        List<String> command = new ArrayList<>(List.of(
                javaBin, "-cp", classpath,
                CoarseGrainedExecutorBackend.class.getName(),
                driverAddress.host, String.valueOf(driverAddress.port),
                // The AM expands this token to the allocated container's id so each
                // executor JVM gets a unique id without us tracking them here.
                ApplicationMaster.CONTAINER_ID_TOKEN,
                String.valueOf(coresPerExecutor)));
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
        if (am != null) am.unregister("SUCCEEDED");
    }
}
