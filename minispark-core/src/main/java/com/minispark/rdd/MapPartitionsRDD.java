package com.minispark.rdd;

import com.minispark.api.MiniSparkContext;
import com.minispark.executor.TaskContext;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * The workhorse RDD behind {@code map}, {@code filter}, {@code flatMap}, and
 * any other transformation expressible as "apply an iterator-to-iterator
 * function to each parent partition". Always carries a single
 * {@link OneToOneDependency}, so it pipelines inside a stage.
 *
 * Real Spark equivalent: org.apache.spark.rdd.MapPartitionsRDD
 */
public final class MapPartitionsRDD<T, U> extends RDD<U> {

    @FunctionalInterface
    public interface PartitionFn<T, U> extends Serializable {
        Iterator<U> apply(TaskContext ctx, Partition split, Iterator<T> parentIter);
    }

    private final RDD<T> parent;
    private final PartitionFn<T, U> f;

    public MapPartitionsRDD(MiniSparkContext sc, RDD<T> parent, PartitionFn<T, U> f) {
        super(sc);
        this.parent = parent;
        this.f = f;
    }

    @Override
    public List<Partition> getPartitions() {
        // 1:1 with parent; reuse parent's partition objects.
        return parent.getPartitions();
    }

    @Override
    public Iterator<U> compute(Partition split, TaskContext ctx) {
        return f.apply(ctx, split, parent.compute(split, ctx));
    }

    @Override
    public List<Dependency<?>> getDependencies() {
        return List.of(new OneToOneDependency<>(parent));
    }
}
