package com.minispark.accumulator;

import com.minispark.executor.TaskContext;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A "write-only on executors, read-only on driver" counter, exactly as in
 * Spark.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Driver constructs via {@link com.minispark.api.MiniSparkContext#accumulator}.
 *       The instance is registered with a globally-unique id and a zero value.</li>
 *   <li>The accumulator gets captured by a task closure and serialized. On the
 *       executor, {@link #add} mutates a <i>task-local</i> delta tracked in
 *       {@link TaskContext} (keyed by id), not the driver's value.</li>
 *   <li>On task completion the executor backend ships the deltas back in
 *       {@code StatusUpdate.accumulatorUpdates}; the driver folds them into
 *       the master value via {@link AccumulatorParam#addInPlace}.</li>
 *   <li>The driver reads via {@link #value()}.</li>
 * </ol>
 *
 * <p>This avoids the easy bug of "executor mutates a static field" — that
 * mutation would happen on a different JVM than the one calling {@code value()}.
 *
 * Real Spark equivalent: org.apache.spark.util.AccumulatorV2 (LongAccumulator etc.)
 */
public final class Accumulator<T> implements Serializable {

    /** Driver-side master value. Marked transient: executors must not read it. */
    private transient AtomicReference<T> driverValue;
    private final long id;
    private final String name;
    private final AccumulatorParam<T> param;

    public Accumulator(long id, String name, AccumulatorParam<T> param) {
        this.id = id;
        this.name = name;
        this.param = param;
        this.driverValue = new AtomicReference<>(param.zero());
    }

    public long id() { return id; }
    public String name() { return name; }
    public AccumulatorParam<T> param() { return param; }

    /**
     * Called from user lambdas on executors. Adds {@code delta} to the
     * task-local accumulator state in {@link TaskContext}; the driver merges
     * deltas after the task completes.
     */
    public void add(T delta) {
        TaskContext ctx = TaskContext.get();
        if (ctx == null) {
            // Driver-side call — go straight to the master.
            driverValue.updateAndGet(cur -> param.addInPlace(cur, delta));
            return;
        }
        ctx.recordAccumulatorUpdate(id, param, delta);
    }

    /** Driver merges an executor's delta into the master value. */
    public void mergeDelta(Object delta) {
        @SuppressWarnings("unchecked") T typed = (T) delta;
        driverValue.updateAndGet(cur -> param.addInPlace(cur, typed));
    }

    /** Driver-only read. */
    public T value() {
        if (driverValue == null) {
            throw new IllegalStateException(
                    "Accumulator.value() must be called on the driver, not from a task");
        }
        return driverValue.get();
    }

    /** Reset to zero (driver-only). */
    public void reset() {
        if (driverValue != null) driverValue.set(param.zero());
    }

    // Executors deserialize the handle but must not observe the driver's value
    // — leave driverValue null so any accidental read throws.
    @java.io.Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        // driverValue stays null.
    }
}
