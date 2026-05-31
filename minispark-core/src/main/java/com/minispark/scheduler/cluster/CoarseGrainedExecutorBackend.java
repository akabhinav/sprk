package com.minispark.scheduler.cluster;

import com.minispark.executor.Executor;
import com.minispark.executor.SparkEnv;
import com.minispark.executor.TaskContext;
import com.minispark.rpc.RpcEndpoint;
import com.minispark.rpc.RpcEndpointRef;
import com.minispark.rpc.RpcEnv;
import com.minispark.serializer.JavaSerializer;
import com.minispark.serializer.Serializer;
import com.minispark.shuffle.FetchFailedException;
import com.minispark.shuffle.ShuffleManager;
import com.minispark.shuffle.ShuffleManagerFactory;
import com.minispark.storage.ExecutorLocation;
import com.minispark.storage.MapOutputTracker;
import com.minispark.storage.NetworkBlockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Executor-side endpoint. Registers with the driver, then receives
 * {@link ClusterMessages.LaunchTask} messages, runs them on a local
 * {@link Executor}, and reports {@link ClusterMessages.StatusUpdate} back.
 *
 * <p>Two ways it comes to life:
 * <ul>
 *   <li><b>In-process</b> (local mode): constructed by
 *       {@link LocalExecutorLauncher} on the driver's RpcEnv, sharing the
 *       driver's {@link SparkEnv}.</li>
 *   <li><b>Separate JVM</b> (netty mode): {@link #main} is the entry point a
 *       NodeManager-style launcher spawns. It builds its own RpcEnv and
 *       SparkEnv and connects back to the driver over TCP.</li>
 * </ul>
 *
 * Real Spark equivalent: org.apache.spark.executor.CoarseGrainedExecutorBackend
 */
public final class CoarseGrainedExecutorBackend implements RpcEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(CoarseGrainedExecutorBackend.class);

    private final String executorId;
    private final RpcEnv rpcEnv;
    private final RpcEndpointRef driverRef;
    private final Executor executor;
    private final Serializer serializer;
    private final int cores;
    private final boolean ownsRpcEnv; // true only for the standalone-JVM case
    private final ScheduledExecutorService heartbeater =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "executor-heartbeat");
                t.setDaemon(true);
                return t;
            });
    // Driver-liveness watchdog. Heartbeats are one-way; if enough consecutive
    // ones fail, the driver is gone — a standalone executor JVM must exit
    // rather than linger as a zombie holding cores/memory. Touched only on the
    // single heartbeat thread, so no synchronization needed (besides `stopping`).
    private final int maxHeartbeatFailures =
            Integer.getInteger("minispark.executor.maxHeartbeatFailures", 10);
    private int heartbeatFailures = 0;
    private volatile boolean stopping = false;

    public CoarseGrainedExecutorBackend(String executorId, RpcEnv rpcEnv, RpcEndpointRef driverRef,
                                        int cores, Serializer serializer, boolean ownsRpcEnv) {
        this.executorId = executorId;
        this.rpcEnv = rpcEnv;
        this.driverRef = driverRef;
        this.cores = cores;
        this.serializer = serializer;
        this.ownsRpcEnv = ownsRpcEnv;
        this.executor = new Executor(executorId, cores, serializer);
        rpcEnv.setupEndpoint(endpointName(executorId), this);
    }

    public static String endpointName(String executorId) { return "Executor-" + executorId; }

    @Override
    public void onStart() {
        // Register with the driver and block on the ack so we don't start
        // accepting tasks before the driver knows about us.
        LOG.info("Executor {} registering with driver at {}", executorId, driverRef.address());
        Object reply = driverRef.ask(new ClusterMessages.RegisterExecutor(
                executorId, rpcEnv.address().host, rpcEnv.address().port, cores));
        if (reply instanceof ClusterMessages.RegisterExecutorFailed f) {
            throw new IllegalStateException("Executor registration rejected: " + f.reason());
        }
        LOG.info("Executor {} registered", executorId);
        // Driver-watched liveness. Cheap (one-way send), but the lone signal
        // the driver has that this executor's process / network is still up.
        heartbeater.scheduleAtFixedRate(this::sendHeartbeat, 500, 1000, TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeat() {
        if (stopping) return;
        try {
            driverRef.send(new ClusterMessages.Heartbeat(executorId));
            heartbeatFailures = 0;
        } catch (Exception e) {
            heartbeatFailures++;
            LOG.debug("Heartbeat failed ({}/{}): {}", heartbeatFailures, maxHeartbeatFailures, e.toString());
            // The driver vanished. In a standalone executor JVM, exit so we don't
            // become a zombie. In local mode (ownsRpcEnv=false) the executor shares
            // the driver's JVM, so exiting would be suicide-by-driver — never do it.
            if (ownsRpcEnv && !stopping && heartbeatFailures >= maxHeartbeatFailures) {
                LOG.error("Executor {} lost contact with the driver after {} consecutive "
                        + "heartbeat failures; shutting down", executorId, heartbeatFailures);
                stopping = true;
                heartbeater.shutdown();
                executor.shutdown();
                // System.exit (not halt) so shutdown hooks — incl. DiskStore temp
                // cleanup — still run before the process leaves.
                System.exit(1);
            }
        }
    }

    @Override
    public void receive(Object message) {
        if (message instanceof ClusterMessages.LaunchTask lt) {
            executor.launchTask(
                    lt.taskBytes(), lt.stageId(), lt.partitionId(),
                    (ctx, result) -> reportFinished(ctx, lt, result),
                    (ctx, err) -> reportFailed(ctx, lt, err));
        } else if (message instanceof ClusterMessages.StopExecutor) {
            LOG.info("Executor {} stopping", executorId);
            stopping = true;   // graceful stop — don't let the watchdog also fire
            heartbeater.shutdownNow();
            executor.shutdown();
            if (ownsRpcEnv) rpcEnv.shutdown();
        } else {
            throw new IllegalArgumentException("Executor got unexpected message: " + message);
        }
    }

    private void reportFinished(TaskContext ctx, ClusterMessages.LaunchTask lt, Object result) {
        driverRef.send(new ClusterMessages.StatusUpdate(
                executorId, lt.stageId(), lt.partitionId(), ctx.attemptNumber(),
                TaskState.FINISHED, serializer.serialize(result), null,
                ctx.accumulatorUpdates()));
    }

    private void reportFailed(TaskContext ctx, ClusterMessages.LaunchTask lt, Throwable err) {
        // Unwrap to find a FetchFailedException anywhere on the cause chain;
        // it is the one error type the driver handles structurally.
        FetchFailedException ffe = findFetchFailed(err);
        TaskFailureReason reason = ffe != null
                ? new TaskFailureReason.FetchFailed(ffe.shuffleId(), ffe.mapId(), ffe.reduceId(),
                        ffe.badLocation(), ffe.getMessage())
                : new TaskFailureReason.GenericError(String.valueOf(err));
        driverRef.send(new ClusterMessages.StatusUpdate(
                executorId, lt.stageId(), lt.partitionId(), ctx.attemptNumber(),
                TaskState.FAILED, null, reason, ctx.accumulatorUpdates()));
    }

    private static FetchFailedException findFetchFailed(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            if (cur instanceof FetchFailedException ffe) return ffe;
        }
        return null;
    }

    @Override
    public void onStop() {
        stopping = true;
        heartbeater.shutdownNow();
        executor.shutdown();
    }

    /**
     * Entry point for a remote executor process.
     * Args: {@code driverHost driverPort executorId cores}.
     */
    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: CoarseGrainedExecutorBackend <driverHost> <driverPort> <executorId> <cores>");
            System.exit(1);
        }
        String driverHost = args[0];
        int driverPort = Integer.parseInt(args[1]);
        String executorId = args[2];
        int cores = Integer.parseInt(args[3]);

        Serializer serializer = new JavaSerializer();
        RpcEnv rpcEnv = RpcEnv.create("executor-" + executorId, hostname(), 0, "netty", serializer);

        // Build this executor's own SparkEnv: network block manager (serves and
        // fetches shuffle blocks) + a worker MapOutputTracker that queries the
        // driver for map locations.
        ExecutorLocation loc = new ExecutorLocation(rpcEnv.address().host, rpcEnv.address().port);
        long maxMem = Long.parseLong(System.getProperty("minispark.memory.store.maxBytes", "536870912"));
        String localDir = System.getProperty("minispark.local.dir", null);
        NetworkBlockManager blockManager = new NetworkBlockManager(loc, rpcEnv, maxMem, localDir);
        RpcEndpointRef trackerMaster = rpcEnv.endpointRef(MapOutputTracker.ENDPOINT_NAME, driverHost, driverPort);
        MapOutputTracker tracker = MapOutputTracker.worker(trackerMaster);
        // Must match the driver's choice; forwarded as a -D by the launcher.
        String shuffleManagerName = System.getProperty("minispark.shuffle.manager", "hash");
        ShuffleManager shuffleManager =
                ShuffleManagerFactory.create(shuffleManagerName, blockManager, tracker, serializer);
        // Per-executor UnifiedMemoryManager. The split between storage and
        // execution pools is configurable via -D; the total is the same maxMem
        // the MemoryStore already uses. Tasks see this via SparkEnv.memoryManager().
        double storageFraction = Double.parseDouble(
                System.getProperty("minispark.memory.storageFraction", "0.5"));
        com.minispark.memory.UnifiedMemoryManager memoryManager =
                new com.minispark.memory.UnifiedMemoryManager(maxMem, storageFraction);
        blockManager.setUnifiedMemoryManager(memoryManager);
        SparkEnv.set(new SparkEnv(shuffleManager, blockManager, tracker, serializer, memoryManager));

        RpcEndpointRef driverRef = rpcEnv.endpointRef(
                CoarseGrainedSchedulerBackend.ENDPOINT_NAME, driverHost, driverPort);
        new CoarseGrainedExecutorBackend(executorId, rpcEnv, driverRef, cores, serializer, true);

        LOG.info("Executor {} up; awaiting tasks", executorId);
        rpcEnv.awaitTermination();
    }

    /**
     * The address this executor binds and <b>advertises to the driver and to
     * peer executors</b>. On a multi-node cluster this must be a routable IP —
     * a loopback address would make the executor's shuffle blocks unreachable
     * from other hosts (a reducer on another node would dial 127.0.0.1 and hit
     * itself). Resolution order, mirroring Spark's {@code Utils.findLocalInetAddress}:
     * <ol>
     *   <li>explicit {@code -Dminispark.executor.host} (Spark's {@code SPARK_LOCAL_IP});</li>
     *   <li>{@code getLocalHost()} if it resolves to a non-loopback address;</li>
     *   <li>otherwise the first routable (up, non-loopback, non-link-local)
     *       IPv4 found by scanning the network interfaces — the case that
     *       saves a box whose hostname maps to 127.0.0.1 in {@code /etc/hosts};</li>
     *   <li>loopback as a last resort.</li>
     * </ol>
     */
    private static String hostname() {
        String override = System.getProperty("minispark.executor.host");
        if (override != null && !override.isBlank()) return override;
        try {
            java.net.InetAddress local = java.net.InetAddress.getLocalHost();
            if (!local.isLoopbackAddress()) return local.getHostAddress();
            for (java.net.NetworkInterface ni :
                    java.util.Collections.list(java.net.NetworkInterface.getNetworkInterfaces())) {
                if (!ni.isUp() || ni.isLoopback()) continue;
                for (java.net.InetAddress a : java.util.Collections.list(ni.getInetAddresses())) {
                    if (a instanceof java.net.Inet4Address
                            && !a.isLoopbackAddress() && !a.isLinkLocalAddress()) {
                        return a.getHostAddress();
                    }
                }
            }
            return local.getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }
}
