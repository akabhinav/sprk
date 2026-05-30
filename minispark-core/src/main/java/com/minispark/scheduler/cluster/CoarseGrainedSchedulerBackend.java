package com.minispark.scheduler.cluster;

import com.minispark.rpc.RpcEndpoint;
import com.minispark.rpc.RpcEndpointRef;
import com.minispark.rpc.RpcEnv;
import com.minispark.scheduler.SchedulerBackend;
import com.minispark.scheduler.Task;
import com.minispark.scheduler.TaskScheduler;
import com.minispark.scheduler.TaskSet;
import com.minispark.serializer.Serializer;
import com.minispark.status.LiveListenerBus;
import com.minispark.status.SchedulerEvent;
import com.minispark.storage.ExecutorLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Driver-side {@link SchedulerBackend} that owns a set of long-lived
 * ("coarse-grained") executors and dispatches tasks to them over an
 * {@link RpcEnv}. It is transport-agnostic: the very same class drives
 * in-process executors over {@link com.minispark.rpc.LocalRpcEnv} and remote
 * executor JVMs over {@link com.minispark.rpc.NettyRpcEnv}. Only the
 * {@link ExecutorLauncher} differs.
 *
 * <p>"Coarse-grained" means an executor registers once and then receives many
 * tasks, as opposed to launching a process per task. This is Spark's model.
 *
 * <p>Phase 6 additions:
 * <ul>
 *   <li>Heartbeat-based liveness: a watchdog marks an executor lost after no
 *       heartbeat for {@code minispark.executor.heartbeatTimeoutMs}.</li>
 *   <li>On lost executor: drop from the registry, fail any in-flight tasks
 *       with {@link TaskFailureReason.ExecutorLost} so the scheduler can
 *       retry them, and notify the DAG layer to clean up map outputs.</li>
 * </ul>
 *
 * Real Spark equivalent: org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
 */
public final class CoarseGrainedSchedulerBackend implements SchedulerBackend {

    public static final String ENDPOINT_NAME = "CoarseGrainedScheduler";
    private static final Logger LOG = LoggerFactory.getLogger(CoarseGrainedSchedulerBackend.class);

    private final TaskScheduler scheduler;
    private final RpcEnv rpcEnv;
    private final Serializer serializer;
    private final ExecutorLauncher launcher;
    private final int expectedExecutors;
    private final int totalCores;
    private final long heartbeatTimeoutMs;
    private final LiveListenerBus listenerBus;

    private final DriverEndpoint driverEndpoint = new DriverEndpoint();
    private RpcEndpointRef driverRef;

    /** Mutable per-executor state, guarded by {@link #lock}. */
    private static final class ExecutorData {
        final String id;
        final RpcEndpointRef ref;
        final ExecutorLocation location;
        final int totalCores;
        int freeCores;
        volatile long lastHeartbeatMs;
        ExecutorData(String id, RpcEndpointRef ref, ExecutorLocation location, int cores, long nowMs) {
            this.id = id; this.ref = ref; this.location = location;
            this.totalCores = cores; this.freeCores = cores; this.lastHeartbeatMs = nowMs;
        }
    }

    private final Object lock = new Object();
    private final Map<String, ExecutorData> executors = new ConcurrentHashMap<>();
    private final Deque<Task<?>> pendingTasks = new ArrayDeque<>();
    /** (stageId,partitionId) -> executorId running it. */
    private final ConcurrentMap<Long, String> runningTasks = new ConcurrentHashMap<>();
    private final CountDownLatch registrationLatch;
    private final ScheduledExecutorService watchdog =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "executor-watchdog");
                t.setDaemon(true);
                return t;
            });

    public CoarseGrainedSchedulerBackend(TaskScheduler scheduler, RpcEnv rpcEnv, Serializer serializer,
                                         ExecutorLauncher launcher, int expectedExecutors, int totalCores,
                                         long heartbeatTimeoutMs, LiveListenerBus listenerBus) {
        this.scheduler = scheduler;
        this.rpcEnv = rpcEnv;
        this.serializer = serializer;
        this.launcher = launcher;
        this.expectedExecutors = expectedExecutors;
        this.totalCores = totalCores;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.listenerBus = listenerBus;
        this.registrationLatch = new CountDownLatch(expectedExecutors);
    }

    private void post(SchedulerEvent e) { if (listenerBus != null) listenerBus.post(e); }

    @Override
    public void start() {
        driverRef = rpcEnv.setupEndpoint(ENDPOINT_NAME, driverEndpoint);
        LOG.info("Driver endpoint up at {}; launching {} executor(s)", rpcEnv.address(), expectedExecutors);
        launcher.launchExecutors(rpcEnv.address());
        // Wait for executors to check in so the first job has resources.
        try {
            if (!registrationLatch.await(60, TimeUnit.SECONDS)) {
                LOG.warn("Only {}/{} executors registered after timeout; proceeding",
                        expectedExecutors - registrationLatch.getCount(), expectedExecutors);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        watchdog.scheduleAtFixedRate(this::checkHeartbeats, heartbeatTimeoutMs / 2,
                heartbeatTimeoutMs / 2, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        watchdog.shutdownNow();
        synchronized (lock) {
            for (ExecutorData e : executors.values()) {
                try { e.ref.send(new ClusterMessages.StopExecutor()); } catch (Exception ignored) {}
            }
        }
        launcher.stop();
        rpcEnv.shutdown();
    }

    @Override
    public void reviveOffers() {
        driverRef.send(new ClusterMessages.ReviveOffers());
    }

    @Override
    public int defaultParallelism() {
        return Math.max(totalCores, 1);
    }

    /** Test/management hook: walk all known executors. */
    public java.util.Set<String> executorIds() {
        return new java.util.HashSet<>(executors.keySet());
    }

    /**
     * Mark an executor lost (called by the watchdog OR proactively when we
     * know a task failed with ExecutorLost). Fails its in-flight tasks so the
     * scheduler can retry them elsewhere, and notifies the DAG layer to clean
     * up any shuffle map outputs the dead executor produced.
     */
    private void removeExecutor(String execId, String reason) {
        ExecutorData removed;
        List<Long> abandoned = new ArrayList<>();
        synchronized (lock) {
            removed = executors.remove(execId);
            if (removed == null) return;
            for (Map.Entry<Long, String> e : runningTasks.entrySet()) {
                if (execId.equals(e.getValue())) abandoned.add(e.getKey());
            }
        }
        LOG.warn("Executor {} marked lost: {} (abandoning {} in-flight tasks)",
                execId, reason, abandoned.size());
        post(new SchedulerEvent.ExecutorRemoved(execId, reason, System.currentTimeMillis()));
        for (Long key : abandoned) {
            runningTasks.remove(key);
            int stageId = (int) (key >> 32);
            int partitionId = (int) (key & 0xffffffffL);
            // Treat in-flight tasks as failed with ExecutorLost so the scheduler
            // applies its retry policy uniformly.
            scheduler.taskFailed(stageId, partitionId, -1,
                    new TaskFailureReason.ExecutorLost(execId));
        }
        scheduler.executorLost(removed.location);
        // The lost executor's slots disappear; revive in case re-queued tasks
        // now fit on the remaining capacity.
        driverRef.send(new ClusterMessages.ReviveOffers());
    }

    private void checkHeartbeats() {
        long now = System.currentTimeMillis();
        Set<String> toRemove = new HashSet<>();
        for (ExecutorData e : executors.values()) {
            if (now - e.lastHeartbeatMs > heartbeatTimeoutMs) toRemove.add(e.id);
        }
        for (String id : toRemove) {
            removeExecutor(id, "heartbeat timeout (" + heartbeatTimeoutMs + "ms)");
        }
    }

    /** Pull queued TaskSets and launch as many as there are free cores. */
    private void makeOffers() {
        synchronized (lock) {
            for (TaskSet ts : scheduler.drainPending()) {
                pendingTasks.addAll(ts.tasks());
            }
            while (!pendingTasks.isEmpty()) {
                ExecutorData target = null;
                for (ExecutorData e : executors.values()) {
                    if (e.freeCores > 0) { target = e; break; }
                }
                if (target == null) break; // no capacity right now; wait for a status update
                Task<?> task = pendingTasks.poll();
                target.freeCores--;
                runningTasks.put(taskKey(task.stageId(), task.partitionId()), target.id);
                byte[] bytes = serializer.serialize(task);
                LOG.debug("Launching task {}/{} on executor {}",
                        task.stageId(), task.partitionId(), target.id);
                post(new SchedulerEvent.TaskStart(task.stageId(), task.partitionId(),
                        target.id, System.currentTimeMillis()));
                scheduler.taskLaunched(task.stageId(), task.partitionId());
                target.ref.send(new ClusterMessages.LaunchTask(task.stageId(), task.partitionId(), bytes));
            }
        }
    }

    private static long taskKey(int stageId, int partitionId) {
        return ((long) stageId << 32) | (partitionId & 0xffffffffL);
    }

    /**
     * The driver's RPC endpoint. Executors register here and report status here.
     *
     * Real Spark equivalent: CoarseGrainedSchedulerBackend.DriverEndpoint
     */
    private final class DriverEndpoint implements RpcEndpoint {

        @Override
        public Object receiveAndReply(Object message) {
            if (message instanceof ClusterMessages.RegisterExecutor reg) {
                RpcEndpointRef execRef = rpcEnv.endpointRef(
                        CoarseGrainedExecutorBackend.endpointName(reg.executorId()),
                        reg.host(), reg.port());
                ExecutorLocation loc = new ExecutorLocation(reg.host(), reg.port());
                synchronized (lock) {
                    executors.put(reg.executorId(),
                            new ExecutorData(reg.executorId(), execRef, loc, reg.cores(),
                                    System.currentTimeMillis()));
                }
                LOG.info("Registered executor {} at {}:{} ({} cores)",
                        reg.executorId(), reg.host(), reg.port(), reg.cores());
                post(new SchedulerEvent.ExecutorAdded(reg.executorId(), reg.host(), reg.port(),
                        reg.cores(), System.currentTimeMillis()));
                registrationLatch.countDown();
                driverRef.send(new ClusterMessages.ReviveOffers());
                return new ClusterMessages.RegisteredExecutor();
            }
            throw new IllegalArgumentException("Driver got unexpected ask: " + message);
        }

        @Override
        public void receive(Object message) {
            if (message instanceof ClusterMessages.ReviveOffers) {
                makeOffers();
            } else if (message instanceof ClusterMessages.StatusUpdate su) {
                handleStatusUpdate(su);
            } else if (message instanceof ClusterMessages.Heartbeat hb) {
                ExecutorData e = executors.get(hb.executorId());
                if (e != null) e.lastHeartbeatMs = System.currentTimeMillis();
            } else {
                throw new IllegalArgumentException("Driver got unexpected message: " + message);
            }
        }

        private void handleStatusUpdate(ClusterMessages.StatusUpdate su) {
            if (su.state() == TaskState.RUNNING) return;
            // Free the slot first so makeOffers can reuse it.
            String execId = runningTasks.remove(taskKey(su.stageId(), su.partitionId()));
            if (execId != null) {
                synchronized (lock) {
                    ExecutorData e = executors.get(execId);
                    if (e != null) e.freeCores++;
                }
            }
            post(new SchedulerEvent.TaskEnd(su.stageId(), su.partitionId(),
                    su.state() == TaskState.FINISHED, System.currentTimeMillis()));
            // Merge accumulator deltas before declaring task completion, so a
            // caller reading an accumulator after the action returns sees the
            // last task's contribution. Real Spark behaves the same way.
            com.minispark.accumulator.AccumulatorContext.mergeAll(su.accumulatorUpdates());
            if (su.state() == TaskState.FINISHED) {
                Object result = su.resultBytes() == null ? null : serializer.deserialize(su.resultBytes());
                scheduler.taskCompleted(su.stageId(), su.partitionId(), result);
            } else { // FAILED
                scheduler.taskFailed(su.stageId(), su.partitionId(), su.attempt(), su.failureReason());
            }
            driverRef.send(new ClusterMessages.ReviveOffers());
        }
    }
}
