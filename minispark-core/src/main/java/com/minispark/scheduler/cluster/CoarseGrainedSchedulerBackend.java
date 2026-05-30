package com.minispark.scheduler.cluster;

import com.minispark.rpc.RpcEndpoint;
import com.minispark.rpc.RpcEndpointRef;
import com.minispark.rpc.RpcEnv;
import com.minispark.scheduler.SchedulerBackend;
import com.minispark.scheduler.Task;
import com.minispark.scheduler.TaskScheduler;
import com.minispark.scheduler.TaskSet;
import com.minispark.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
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

    private final DriverEndpoint driverEndpoint = new DriverEndpoint();
    private RpcEndpointRef driverRef;

    /** Mutable per-executor capacity, guarded by {@link #lock}. */
    private static final class ExecutorData {
        final String id;
        final RpcEndpointRef ref;
        final int totalCores;
        int freeCores;
        ExecutorData(String id, RpcEndpointRef ref, int cores) {
            this.id = id; this.ref = ref; this.totalCores = cores; this.freeCores = cores;
        }
    }

    private final Object lock = new Object();
    private final Map<String, ExecutorData> executors = new ConcurrentHashMap<>();
    private final Deque<Task<?>> pendingTasks = new ArrayDeque<>();
    // (stageId,partitionId) -> executorId running it, so completion frees the right slot.
    private final ConcurrentMap<Long, String> runningTasks = new ConcurrentHashMap<>();
    private final CountDownLatch registrationLatch;

    public CoarseGrainedSchedulerBackend(TaskScheduler scheduler, RpcEnv rpcEnv, Serializer serializer,
                                         ExecutorLauncher launcher, int expectedExecutors, int totalCores) {
        this.scheduler = scheduler;
        this.rpcEnv = rpcEnv;
        this.serializer = serializer;
        this.launcher = launcher;
        this.expectedExecutors = expectedExecutors;
        this.totalCores = totalCores;
        this.registrationLatch = new CountDownLatch(expectedExecutors);
    }

    @Override
    public void start() {
        driverRef = rpcEnv.setupEndpoint(ENDPOINT_NAME, driverEndpoint);
        LOG.info("Driver endpoint up at {}; launching {} executor(s)", rpcEnv.address(), expectedExecutors);
        launcher.launchExecutors(rpcEnv.address());
        // Wait for executors to check in so the first job has resources. Bounded
        // so a misconfigured cluster fails loudly instead of hanging forever.
        try {
            if (!registrationLatch.await(60, TimeUnit.SECONDS)) {
                LOG.warn("Only {}/{} executors registered after timeout; proceeding",
                        expectedExecutors - registrationLatch.getCount(), expectedExecutors);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void stop() {
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
                synchronized (lock) {
                    executors.put(reg.executorId(), new ExecutorData(reg.executorId(), execRef, reg.cores()));
                }
                LOG.info("Registered executor {} at {}:{} ({} cores)",
                        reg.executorId(), reg.host(), reg.port(), reg.cores());
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
            if (su.state() == TaskState.FINISHED) {
                Object result = su.resultBytes() == null ? null : serializer.deserialize(su.resultBytes());
                scheduler.taskCompleted(su.stageId(), su.partitionId(), result);
            } else { // FAILED
                scheduler.taskFailed(su.stageId(), su.partitionId(),
                        new RuntimeException("Executor " + su.executorId() + ": " + su.errorMessage()));
            }
            driverRef.send(new ClusterMessages.ReviveOffers());
        }
    }
}
