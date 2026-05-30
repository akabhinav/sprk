package com.miniyarn.common;

import java.io.Serializable;
import java.util.List;

/**
 * The MiniYarn wire protocol. Three endpoints exchange these:
 * <ul>
 *   <li><b>ResourceManager</b> — accepts NM registration/heartbeats and AM
 *       resource requests; pushes allocations back to AMs.</li>
 *   <li><b>NodeManager</b> — accepts launch/kill requests from the AM.</li>
 *   <li><b>ApplicationMaster</b> — receives allocations and completions from
 *       the RM. In our yarn-client model, lives in the driver JVM.</li>
 * </ul>
 *
 * Mirrors the split of real YARN's APIs (ApplicationMasterProtocol,
 * ContainerManagementProtocol, ResourceManagerAdministrationProtocol).
 */
public final class YarnMessages {
    private YarnMessages() {}

    public static final String RM_ENDPOINT_NAME = "ResourceManager";
    public static final String NM_ENDPOINT_NAME = "NodeManager";
    public static final String AM_ENDPOINT_NAME = "ApplicationMaster";

    // ---------- to RM ----------

    /** NM → RM (ask): "I'm up at host:port with this capacity." */
    public record RegisterNodeManager(NodeId nodeId, String host, int port, Resource total)
            implements Serializable {}
    public record RegisteredNodeManager() implements Serializable {}

    /** NM → RM (send): periodic free-capacity update + per-container statuses. */
    public record NodeHeartbeat(NodeId nodeId, Resource free, List<ContainerStatus> statuses)
            implements Serializable {}

    /** AM → RM (ask): register an application. Returns its assigned id. */
    public record RegisterApplication(String name, String amHost, int amPort)
            implements Serializable {}
    public record RegisteredApplication(ApplicationId appId) implements Serializable {}

    /**
     * AM → RM (send): "I want N containers of this size, run with this
     * launch ctx." RM allocates asynchronously and pushes
     * {@link ContainersAllocated} back to the AM as nodes have capacity.
     */
    public record RequestContainers(ApplicationId appId, int numContainers, Resource each,
                                    ContainerLaunchContext launchCtx) implements Serializable {}

    /** AM → RM (send): give the container back to the pool. */
    public record ReleaseContainer(ApplicationId appId, ContainerId containerId)
            implements Serializable {}

    /** AM → RM (send): the application is done; release everything. */
    public record UnregisterApplication(ApplicationId appId, String finalStatus)
            implements Serializable {}

    // ---------- to NM ----------

    /** AM → NM (ask): start a container's process. */
    public record LaunchContainer(Container container, ContainerLaunchContext launchCtx)
            implements Serializable {}
    public record ContainerLaunched(ContainerId containerId) implements Serializable {}

    /** AM → NM (send): kill a still-running container. */
    public record KillContainer(ContainerId containerId) implements Serializable {}

    // ---------- to AM ----------

    /** RM → AM (send): "here are containers we just allocated to you." */
    public record ContainersAllocated(List<Container> containers) implements Serializable {}

    /** NM → AM (send): a container's process exited. */
    public record ContainerCompleted(ContainerId containerId, int exitCode, String diagnostics)
            implements Serializable {}

    /** State a container is in, as observed by the NM that hosts it. */
    public record ContainerStatus(ContainerId containerId, ContainerState state, int exitCode)
            implements Serializable {}

    public enum ContainerState { RUNNING, COMPLETE, FAILED }
}
