package com.miniyarn.rm;

import com.miniyarn.common.ApplicationId;
import com.miniyarn.common.Container;
import com.miniyarn.common.ContainerId;
import com.miniyarn.common.ContainerLaunchContext;
import com.miniyarn.common.NodeId;
import com.miniyarn.common.Resource;
import com.miniyarn.common.YarnMessages;
import com.miniyarn.common.YarnMessages.ContainerStatus;
import com.minispark.rpc.RpcEndpoint;
import com.minispark.rpc.RpcEndpointRef;
import com.minispark.rpc.RpcEnv;
import com.minispark.serializer.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The master of MiniYarn. Tracks NodeManagers and their free capacity,
 * accepts application registrations, and runs a FIFO + greedy-packing
 * scheduler that turns AM {@code RequestContainers} into {@code Container}
 * allocations pushed to each AM.
 *
 * <p>Single endpoint, single-threaded scheduler under {@link #lock}: simple
 * enough to read in one sitting, but the loop is structurally the same as
 * real YARN's CapacityScheduler/FIFO — the cycle is "free a node → satisfy
 * the head request that fits".
 *
 * Real YARN equivalent: org.apache.hadoop.yarn.server.resourcemanager.ResourceManager
 *                       + ResourceScheduler (FifoScheduler).
 */
public final class ResourceManager implements RpcEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(ResourceManager.class);

    private final RpcEnv rpcEnv;
    private final Object lock = new Object();
    private final Map<NodeId, NodeState> nodes = new HashMap<>();
    private final Map<ApplicationId, AppState> apps = new HashMap<>();
    private final Deque<Demand> pending = new ArrayDeque<>();
    private final AtomicInteger appIdGen = new AtomicInteger();

    /** Per-NM bookkeeping. */
    private static final class NodeState {
        final NodeId id;
        final String host;
        final int port;
        final Resource total;
        Resource free;
        final RpcEndpointRef nmRef;
        NodeState(NodeId id, String host, int port, Resource total, RpcEndpointRef nmRef) {
            this.id = id; this.host = host; this.port = port;
            this.total = total; this.free = total; this.nmRef = nmRef;
        }
    }

    private static final class AppState {
        final ApplicationId id;
        final String name;
        final RpcEndpointRef amRef;
        final AtomicInteger nextContainerId = new AtomicInteger();
        final Map<ContainerId, NodeId> liveContainers = new HashMap<>();
        AppState(ApplicationId id, String name, RpcEndpointRef amRef) {
            this.id = id; this.name = name; this.amRef = amRef;
        }
    }

    /** An outstanding request: N containers of {@code each}, with this launch ctx. */
    private static final class Demand {
        final ApplicationId appId;
        int remaining;
        final Resource each;
        final ContainerLaunchContext launchCtx;
        Demand(ApplicationId appId, int n, Resource each, ContainerLaunchContext ctx) {
            this.appId = appId; this.remaining = n; this.each = each; this.launchCtx = ctx;
        }
    }

    public ResourceManager(RpcEnv rpcEnv) {
        this.rpcEnv = rpcEnv;
        rpcEnv.setupEndpoint(YarnMessages.RM_ENDPOINT_NAME, this);
        LOG.info("ResourceManager listening at {}", rpcEnv.address());
    }

    /** Read-only snapshot of which nodes are currently hosting any of an app's containers. */
    public java.util.Set<NodeId> nodesHostingApp(ApplicationId appId) {
        synchronized (lock) {
            AppState app = apps.get(appId);
            if (app == null) return java.util.Set.of();
            return new java.util.HashSet<>(app.liveContainers.values());
        }
    }

    // ---------- RPC ----------

    @Override
    public Object receiveAndReply(Object message) {
        if (message instanceof YarnMessages.RegisterNodeManager r) return registerNM(r);
        if (message instanceof YarnMessages.RegisterApplication r) return registerApp(r);
        throw new IllegalArgumentException("RM got unexpected ask: " + message);
    }

    @Override
    public void receive(Object message) {
        if (message instanceof YarnMessages.NodeHeartbeat h) handleHeartbeat(h);
        else if (message instanceof YarnMessages.RequestContainers r) handleRequest(r);
        else if (message instanceof YarnMessages.ReleaseContainer r) releaseContainer(r);
        else if (message instanceof YarnMessages.UnregisterApplication u) unregisterApp(u);
        else throw new IllegalArgumentException("RM got unexpected message: " + message);
    }

    // ---------- handlers ----------

    private YarnMessages.RegisteredNodeManager registerNM(YarnMessages.RegisterNodeManager r) {
        RpcEndpointRef nmRef = rpcEnv.endpointRef(YarnMessages.NM_ENDPOINT_NAME, r.host(), r.port());
        synchronized (lock) {
            nodes.put(r.nodeId(), new NodeState(r.nodeId(), r.host(), r.port(), r.total(), nmRef));
            LOG.info("Registered NodeManager {} at {}:{} ({})", r.nodeId().name(), r.host(), r.port(), r.total());
            scheduleAndDispatch();
        }
        return new YarnMessages.RegisteredNodeManager();
    }

    private YarnMessages.RegisteredApplication registerApp(YarnMessages.RegisterApplication r) {
        ApplicationId id = new ApplicationId(appIdGen.incrementAndGet());
        RpcEndpointRef amRef = rpcEnv.endpointRef(YarnMessages.AM_ENDPOINT_NAME, r.amHost(), r.amPort());
        synchronized (lock) { apps.put(id, new AppState(id, r.name(), amRef)); }
        LOG.info("Registered application {} ({}) AM at {}:{}", id, r.name(), r.amHost(), r.amPort());
        return new YarnMessages.RegisteredApplication(id);
    }

    private void handleHeartbeat(YarnMessages.NodeHeartbeat h) {
        synchronized (lock) {
            NodeState n = nodes.get(h.nodeId());
            if (n == null) return;
            // The NM's view of "free" is authoritative — it includes processes that
            // crashed. We retain the RM-tracked allocations as a sanity floor.
            n.free = h.free();
            for (ContainerStatus s : h.statuses()) {
                if (s.state() != YarnMessages.ContainerState.RUNNING) {
                    handleContainerExit(s, n);
                }
            }
            scheduleAndDispatch();
        }
    }

    private void handleContainerExit(ContainerStatus s, NodeState n) {
        for (AppState app : apps.values()) {
            if (app.liveContainers.remove(s.containerId()) != null) {
                LOG.info("Container {} exited ({}, code {})", s.containerId(), s.state(), s.exitCode());
                app.amRef.send(new YarnMessages.ContainerCompleted(
                        s.containerId(), s.exitCode(), s.state().toString()));
                return;
            }
        }
    }

    private void handleRequest(YarnMessages.RequestContainers r) {
        synchronized (lock) {
            if (!apps.containsKey(r.appId())) {
                LOG.warn("Container request for unknown app {}", r.appId());
                return;
            }
            pending.add(new Demand(r.appId(), r.numContainers(), r.each(), r.launchCtx()));
            LOG.info("App {} requested {} containers of {}", r.appId(), r.numContainers(), r.each());
            scheduleAndDispatch();
        }
    }

    private void releaseContainer(YarnMessages.ReleaseContainer r) {
        synchronized (lock) {
            AppState app = apps.get(r.appId());
            if (app == null) return;
            NodeId nodeId = app.liveContainers.remove(r.containerId());
            if (nodeId == null) return;
            // We can't credit the node here — only the NM's heartbeat tells us the
            // process is really gone. Real YARN behaves the same way.
            // But proactively tell the NM to kill the process.
            NodeState n = nodes.get(nodeId);
            if (n != null) n.nmRef.send(new YarnMessages.KillContainer(r.containerId()));
        }
    }

    private void unregisterApp(YarnMessages.UnregisterApplication u) {
        synchronized (lock) {
            AppState app = apps.remove(u.appId());
            if (app == null) return;
            LOG.info("App {} unregistered ({})", u.appId(), u.finalStatus());
            for (Map.Entry<ContainerId, NodeId> e : app.liveContainers.entrySet()) {
                NodeState n = nodes.get(e.getValue());
                if (n != null) n.nmRef.send(new YarnMessages.KillContainer(e.getKey()));
            }
        }
    }

    // ---------- scheduler ----------

    /**
     * Greedy FIFO: for each pending demand (head-of-queue first), try to pack
     * containers onto nodes with enough free resource until either the demand
     * is satisfied or no node fits. Allocations are buffered per AM and sent
     * in a single {@code ContainersAllocated} batch at the end.
     *
     * <p>Held under {@link #lock} so capacity counters never race.
     */
    private void scheduleAndDispatch() {
        Map<ApplicationId, List<Container>> allocations = new HashMap<>();

        for (Demand d : pending) {
            while (d.remaining > 0) {
                NodeState target = findNodeWithCapacity(d.each);
                if (target == null) break;
                target.free = target.free.minus(d.each);
                AppState app = apps.get(d.appId);
                if (app == null) { d.remaining = 0; break; }
                ContainerId cid = new ContainerId(app.id, app.nextContainerId.incrementAndGet());
                Container c = new Container(cid, target.id, target.host, target.port, d.each);
                app.liveContainers.put(cid, target.id);
                allocations.computeIfAbsent(app.id, k -> new ArrayList<>()).add(c);
                d.remaining--;
                LOG.debug("Allocated {} on {}", cid, target.id.name());
            }
        }
        pending.removeIf(d -> d.remaining <= 0);

        // Push allocations after releasing the scheduling pass.
        for (Map.Entry<ApplicationId, List<Container>> e : allocations.entrySet()) {
            AppState app = apps.get(e.getKey());
            if (app != null) app.amRef.send(new YarnMessages.ContainersAllocated(e.getValue()));
        }
    }

    /**
     * Among nodes that fit the request, pick the one whose remaining free
     * capacity (by cores) is largest. That spreads executors across the cluster
     * — strict FIFO+first-fit would pile everything onto whichever node iterates
     * first and is much less educational to watch. Real YARN supports both via
     * scheduler choice (FIFO vs Fair vs Capacity).
     */
    private NodeState findNodeWithCapacity(Resource req) {
        NodeState best = null;
        for (NodeState n : nodes.values()) {
            if (!req.fitsIn(n.free)) continue;
            if (best == null || n.free.cores() > best.free.cores()) best = n;
        }
        return best;
    }

    // ---------- standalone entry point ----------

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8032;
        RpcEnv env = RpcEnv.create("rm", host, port, "netty", new JavaSerializer());
        new ResourceManager(env);
        LOG.info("ResourceManager up; awaiting NodeManagers and applications");
        env.awaitTermination();
    }
}
