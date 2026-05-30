package com.miniyarn.am;

import com.miniyarn.common.ApplicationId;
import com.miniyarn.common.Container;
import com.miniyarn.common.ContainerLaunchContext;
import com.miniyarn.common.Resource;
import com.miniyarn.common.YarnMessages;
import com.minispark.rpc.RpcEndpoint;
import com.minispark.rpc.RpcEndpointRef;
import com.minispark.rpc.RpcEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The per-app broker between an application and the RM/NMs. We run it in the
 * driver's RpcEnv (yarn-client mode) so the driver and AM share an address
 * space — this is what real Spark's {@code yarn-client} deploy mode does and
 * is the simpler mode to learn.
 *
 * <p>Lifecycle driven by the {@link YarnExecutorLauncher}:
 * <ol>
 *   <li>{@link #registerWithRM} once at start.</li>
 *   <li>{@link #requestExecutors} to ask for N executor containers.</li>
 *   <li>RM pushes {@link YarnMessages.ContainersAllocated} → we send
 *       {@link YarnMessages.LaunchContainer} to the owning NM, which spawns
 *       the executor JVM. The executor then connects back to the driver
 *       through the normal {@code CoarseGrainedSchedulerBackend} path.</li>
 *   <li>{@link #unregister} on shutdown so the RM can reclaim resources.</li>
 * </ol>
 *
 * Real Spark equivalent: org.apache.spark.deploy.yarn.ApplicationMaster
 *                        + YarnAllocator.
 */
public final class ApplicationMaster implements RpcEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationMaster.class);

    private final String appName;
    private final RpcEnv rpcEnv;
    private final RpcEndpointRef rmRef;
    private volatile ApplicationId appId;
    private volatile ContainerListener listener = c -> {};

    /** Callback the launcher uses to count incoming executor containers. */
    @FunctionalInterface
    public interface ContainerListener {
        void onContainerCompleted(YarnMessages.ContainerCompleted msg);
    }

    public ApplicationMaster(String appName, RpcEnv rpcEnv, RpcEndpointRef rmRef) {
        this.appName = appName;
        this.rpcEnv = rpcEnv;
        this.rmRef = rmRef;
        rpcEnv.setupEndpoint(YarnMessages.AM_ENDPOINT_NAME, this);
    }

    public void setContainerListener(ContainerListener listener) { this.listener = listener; }

    public ApplicationId appId() { return appId; }

    public void registerWithRM() {
        YarnMessages.RegisteredApplication reg = rmRef.ask(new YarnMessages.RegisterApplication(
                appName, rpcEnv.address().host, rpcEnv.address().port));
        this.appId = reg.appId();
        LOG.info("ApplicationMaster registered as {}", appId);
    }

    public void requestExecutors(int n, Resource each, ContainerLaunchContext launchCtx) {
        rmRef.send(new YarnMessages.RequestContainers(appId, n, each, launchCtx));
        LOG.info("Asked RM for {} executor containers", n);
    }

    public void unregister(String finalStatus) {
        if (appId == null) return;
        rmRef.send(new YarnMessages.UnregisterApplication(appId, finalStatus));
    }

    @Override
    public void receive(Object message) {
        if (message instanceof YarnMessages.ContainersAllocated a) onAllocated(a.containers());
        else if (message instanceof YarnMessages.ContainerCompleted c) {
            LOG.info("Container completed: {} (exit {}, {})", c.containerId(), c.exitCode(), c.diagnostics());
            listener.onContainerCompleted(c);
        }
        else throw new IllegalArgumentException("AM got unexpected message: " + message);
    }

    private void onAllocated(List<Container> containers) {
        LOG.info("RM allocated {} container(s)", containers.size());
        for (Container c : containers) {
            ContainerLaunchContext base = nextLaunchCtx.get();
            if (base == null) {
                LOG.warn("No launch context for {}; dropping", c.id());
                continue;
            }
            // Substitute the per-container token so each child JVM gets a unique
            // executor id derived from its container id. Real YARN does the same
            // via environment-variable expansion in ContainerLaunchContext.
            ContainerLaunchContext ctx = expand(base, c);
            RpcEndpointRef nm = rpcEnv.endpointRef(YarnMessages.NM_ENDPOINT_NAME, c.nodeHost(), c.nodePort());
            try {
                nm.ask(new YarnMessages.LaunchContainer(c, ctx));
            } catch (Exception e) {
                LOG.error("Failed to launch {} on {}: {}", c.id(), c.nodeId(), e.toString());
            }
        }
    }

    /** Replace {@link #CONTAINER_ID_TOKEN} in the command line with this container's id. */
    private static ContainerLaunchContext expand(ContainerLaunchContext base, Container c) {
        List<String> cmd = new java.util.ArrayList<>(base.command().size());
        String id = c.id().toString();
        for (String arg : base.command()) {
            cmd.add(CONTAINER_ID_TOKEN.equals(arg) ? id : arg);
        }
        return new ContainerLaunchContext(cmd, base.environment());
    }

    /** Token an AM client puts in the command line to mean "the allocated container's id". */
    public static final String CONTAINER_ID_TOKEN = "{{CONTAINER_ID}}";

    /**
     * The launch ctx for the next batch of allocated containers. Set by the
     * launcher just before {@link #requestExecutors}; reused for each container
     * (executors in a single request are interchangeable).
     */
    private final java.util.concurrent.atomic.AtomicReference<ContainerLaunchContext> nextLaunchCtx =
            new java.util.concurrent.atomic.AtomicReference<>();

    public void setNextLaunchCtx(ContainerLaunchContext ctx) { nextLaunchCtx.set(ctx); }
}
