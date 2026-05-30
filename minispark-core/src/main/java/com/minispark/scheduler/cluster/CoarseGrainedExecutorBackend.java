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
        try { driverRef.send(new ClusterMessages.Heartbeat(executorId)); }
        catch (Exception e) { LOG.debug("Heartbeat failed: {}", e.toString()); }
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
        SparkEnv.set(new SparkEnv(shuffleManager, blockManager, tracker, serializer));

        RpcEndpointRef driverRef = rpcEnv.endpointRef(
                CoarseGrainedSchedulerBackend.ENDPOINT_NAME, driverHost, driverPort);
        new CoarseGrainedExecutorBackend(executorId, rpcEnv, driverRef, cores, serializer, true);

        LOG.info("Executor {} up; awaiting tasks", executorId);
        rpcEnv.awaitTermination();
    }

    private static String hostname() {
        try {
            return java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }
}
