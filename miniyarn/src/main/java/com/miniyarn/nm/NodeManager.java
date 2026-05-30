package com.miniyarn.nm;

import com.miniyarn.common.ContainerId;
import com.miniyarn.common.NodeId;
import com.miniyarn.common.Resource;
import com.miniyarn.common.YarnMessages;
import com.miniyarn.common.YarnMessages.ContainerState;
import com.miniyarn.common.YarnMessages.ContainerStatus;
import com.minispark.rpc.RpcEndpoint;
import com.minispark.rpc.RpcEndpointRef;
import com.minispark.rpc.RpcEnv;
import com.minispark.serializer.JavaSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A node's resource broker. Registers its capacity with the RM, heartbeats
 * free resources and container statuses, and launches container processes on
 * demand from the AM.
 *
 * <p>Each launched container is a child JVM whose stdout/stderr are inherited
 * from this process, so logs interleave by default (good enough for learning;
 * real YARN routes them through log aggregation).
 *
 * Real YARN equivalent: org.apache.hadoop.yarn.server.nodemanager.NodeManager
 *                       (plus ContainerExecutor).
 */
public final class NodeManager implements RpcEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(NodeManager.class);

    private final NodeId nodeId;
    private final RpcEnv rpcEnv;
    private final RpcEndpointRef rmRef;
    private final Resource total;

    private final ConcurrentMap<ContainerId, Process> running = new ConcurrentHashMap<>();
    /** Status events accumulated since the last heartbeat (drained each tick). */
    private final List<ContainerStatus> pendingStatuses = new ArrayList<>();
    private final AtomicReference<Resource> free;
    private final ScheduledExecutorService heartbeater =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "nm-heartbeat");
                t.setDaemon(true);
                return t;
            });

    public NodeManager(NodeId nodeId, RpcEnv rpcEnv, RpcEndpointRef rmRef, Resource total) {
        this.nodeId = nodeId;
        this.rpcEnv = rpcEnv;
        this.rmRef = rmRef;
        this.total = total;
        this.free = new AtomicReference<>(total);
        rpcEnv.setupEndpoint(YarnMessages.NM_ENDPOINT_NAME, this);
    }

    @Override
    public void onStart() {
        rmRef.ask(new YarnMessages.RegisterNodeManager(
                nodeId, rpcEnv.address().host, rpcEnv.address().port, total));
        LOG.info("NodeManager {} registered ({})", nodeId.name(), total);
        // Real YARN heartbeats roughly every 1s. We match that ballpark.
        heartbeater.scheduleAtFixedRate(this::heartbeat, 250, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public Object receiveAndReply(Object message) {
        if (message instanceof YarnMessages.LaunchContainer lc) {
            launch(lc);
            return new YarnMessages.ContainerLaunched(lc.container().id());
        }
        throw new IllegalArgumentException("NM got unexpected ask: " + message);
    }

    @Override
    public void receive(Object message) {
        if (message instanceof YarnMessages.KillContainer k) kill(k.containerId());
        else throw new IllegalArgumentException("NM got unexpected message: " + message);
    }

    private void launch(YarnMessages.LaunchContainer lc) {
        Resource req = lc.container().resource();
        Resource updated;
        do {
            Resource cur = free.get();
            if (!req.fitsIn(cur)) {
                // We could reject, but the simplest thing matching real NM behaviour
                // (which always tries to honour the RM) is to over-subscribe. Log it.
                LOG.warn("Launching {} would over-subscribe (free={}, req={})",
                        lc.container().id(), cur, req);
                updated = new Resource(Math.max(0, cur.cores() - req.cores()),
                                       Math.max(0, cur.memoryMB() - req.memoryMB()));
            } else {
                updated = cur.minus(req);
            }
        } while (!free.compareAndSet(free.get(), updated));

        ProcessBuilder pb = new ProcessBuilder(lc.launchCtx().command())
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.INHERIT);
        Map<String, String> env = pb.environment();
        if (lc.launchCtx().environment() != null) env.putAll(lc.launchCtx().environment());
        try {
            Process p = pb.start();
            running.put(lc.container().id(), p);
            LOG.info("Launched {} on node {} (pid {})", lc.container().id(), nodeId.name(), p.pid());
            // When the process exits, free the resources and queue a status update.
            p.onExit().thenAccept(proc -> onContainerExit(lc.container().id(), req, proc.exitValue()));
        } catch (Exception e) {
            // Roll back the reservation and report failure.
            free.updateAndGet(r -> r.plus(req));
            synchronized (pendingStatuses) {
                pendingStatuses.add(new ContainerStatus(lc.container().id(), ContainerState.FAILED, -1));
            }
            LOG.error("Failed to launch container {}: {}", lc.container().id(), e.toString());
        }
    }

    private void kill(ContainerId id) {
        Process p = running.get(id);
        if (p == null) return;
        LOG.info("Killing container {}", id);
        p.destroy();
        try {
            if (!p.waitFor(5, TimeUnit.SECONDS)) p.destroyForcibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            p.destroyForcibly();
        }
    }

    private void onContainerExit(ContainerId id, Resource reserved, int exit) {
        running.remove(id);
        free.updateAndGet(r -> r.plus(reserved));
        ContainerState state = exit == 0 ? ContainerState.COMPLETE : ContainerState.FAILED;
        synchronized (pendingStatuses) {
            pendingStatuses.add(new ContainerStatus(id, state, exit));
        }
        LOG.info("Container {} exited with code {}", id, exit);
    }

    private void heartbeat() {
        List<ContainerStatus> drain;
        synchronized (pendingStatuses) {
            drain = new ArrayList<>(pendingStatuses);
            pendingStatuses.clear();
        }
        try {
            rmRef.send(new YarnMessages.NodeHeartbeat(nodeId, free.get(), drain));
        } catch (Exception e) {
            LOG.warn("Heartbeat failed: {}", e.toString());
        }
    }

    @Override
    public void onStop() {
        heartbeater.shutdownNow();
        running.forEach((id, p) -> p.destroy());
    }

    // ---------- standalone entry point ----------
    // Args: nodeName rmHost rmPort [host port cores memMB]
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: NodeManager <nodeName> <rmHost> <rmPort> [host port cores memMB]");
            System.exit(1);
        }
        String nodeName = args[0];
        String rmHost = args[1];
        int rmPort = Integer.parseInt(args[2]);
        String host = args.length > 3 ? args[3] : "127.0.0.1";
        int port = args.length > 4 ? Integer.parseInt(args[4]) : 0;
        int cores = args.length > 5 ? Integer.parseInt(args[5]) : Runtime.getRuntime().availableProcessors();
        int memMB = args.length > 6 ? Integer.parseInt(args[6]) : 4096;

        RpcEnv env = RpcEnv.create("nm-" + nodeName, host, port, "netty", new JavaSerializer());
        RpcEndpointRef rm = env.endpointRef(YarnMessages.RM_ENDPOINT_NAME, rmHost, rmPort);
        new NodeManager(new NodeId(nodeName), env, rm, new Resource(cores, memMB));
        LOG.info("NodeManager {} up; awaiting container launches", nodeName);
        env.awaitTermination();
    }
}
