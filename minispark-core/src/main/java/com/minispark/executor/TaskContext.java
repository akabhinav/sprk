package com.minispark.executor;

import com.minispark.accumulator.AccumulatorParam;
import com.minispark.memory.TaskMemoryManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Per-task runtime info handed to every {@code RDD.compute()} invocation.
 *
 * <p>Also accumulates side-band state produced during the task — currently
 * accumulator deltas — that the executor backend ships to the driver after
 * the task completes.
 *
 * <p>Exposed via a thread-local so user code that doesn't get a {@code ctx}
 * argument (e.g. {@link com.minispark.accumulator.Accumulator#add}) can still
 * find it. The {@link Executor} sets/clears it around each task body.
 *
 * Real Spark equivalent: org.apache.spark.TaskContext
 */
public final class TaskContext {

    private static final ThreadLocal<TaskContext> CURRENT = new ThreadLocal<>();

    public static TaskContext get() { return CURRENT.get(); }
    public static void setCurrent(TaskContext ctx) { CURRENT.set(ctx); }
    public static void unset() { CURRENT.remove(); }

    private final int stageId;
    private final int partitionId;
    private final int attemptNumber;
    // Accumulator deltas accumulated during this task, by accumulator id.
    private final Map<Long, AccumulatorDelta> accumulatorDeltas = new HashMap<>();
    // Optional per-task memory manager. Set by the Executor around the task body
    // when the SparkEnv is wired with a UnifiedMemoryManager; null otherwise.
    private TaskMemoryManager taskMemoryManager;

    public TaskContext(int stageId, int partitionId, int attemptNumber) {
        this.stageId = stageId;
        this.partitionId = partitionId;
        this.attemptNumber = attemptNumber;
    }

    public int stageId() { return stageId; }
    public int partitionId() { return partitionId; }
    public int attemptNumber() { return attemptNumber; }

    /** Set once by the Executor before running the task body. */
    public void setTaskMemoryManager(TaskMemoryManager tmm) { this.taskMemoryManager = tmm; }
    /** May be {@code null} in setups without a UnifiedMemoryManager (legacy / unit tests). */
    public TaskMemoryManager taskMemoryManager() { return taskMemoryManager; }

    /** Called from {@code Accumulator.add}. Threaded through {@link #accumulatorUpdates}. */
    public <T> void recordAccumulatorUpdate(long accumulatorId, AccumulatorParam<T> param, T delta) {
        @SuppressWarnings("unchecked")
        AccumulatorDelta entry = accumulatorDeltas.computeIfAbsent(accumulatorId,
                id -> new AccumulatorDelta(param, param.zero()));
        @SuppressWarnings("unchecked")
        AccumulatorParam<T> p = (AccumulatorParam<T>) entry.param;
        @SuppressWarnings("unchecked")
        T cur = (T) entry.value;
        entry.value = p.addInPlace(cur, delta);
    }

    /** Snapshot of accumulator id → final delta value, ready to ship to the driver. */
    public Map<Long, Object> accumulatorUpdates() {
        Map<Long, Object> out = new HashMap<>(accumulatorDeltas.size());
        for (Map.Entry<Long, AccumulatorDelta> e : accumulatorDeltas.entrySet()) {
            out.put(e.getKey(), e.getValue().value);
        }
        return out;
    }

    private static final class AccumulatorDelta {
        final AccumulatorParam<?> param;
        Object value;
        AccumulatorDelta(AccumulatorParam<?> param, Object value) {
            this.param = param; this.value = value;
        }
    }

    @Override public String toString() {
        return "TaskContext(stage=" + stageId + ", part=" + partitionId + ", attempt=" + attemptNumber + ")";
    }
}
