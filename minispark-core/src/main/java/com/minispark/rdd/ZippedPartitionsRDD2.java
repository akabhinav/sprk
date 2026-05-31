package com.minispark.rdd;

import com.minispark.api.MiniSparkContext;
import com.minispark.executor.TaskContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Per-partition zip of two parent RDDs: child partition i feeds the function
 * with parent1's partition i and parent2's partition i. Stays narrow — no
 * shuffle — and so pipelines inside one stage. Requires both parents to have
 * the same partition count; otherwise alignment by index would silently
 * misjoin data.
 *
 * <p>Used by {@code SortMergeJoinExec} to consume two co-partitioned
 * {@code ShuffledRDD}s (same {@link com.minispark.shuffle.HashPartitioner})
 * in a single task that sees both sides' records for the matching reduce
 * partition.
 *
 * Real Spark equivalent: org.apache.spark.rdd.ZippedPartitionsRDD2
 */
public final class ZippedPartitionsRDD2<A, B, C> extends RDD<C> {

    /** Closure that combines two per-partition iterators into the child's iterator. */
    public interface ZipFunction<A, B, C> extends Serializable {
        Iterator<C> apply(Iterator<A> a, Iterator<B> b);
    }

    private static final class ZipSlice implements Partition {
        private final int idx;
        ZipSlice(int idx) { this.idx = idx; }
        public int index() { return idx; }
    }

    private final RDD<A> rdd1;
    private final RDD<B> rdd2;
    private final ZipFunction<A, B, C> f;
    private final List<Partition> partitions;

    public ZippedPartitionsRDD2(MiniSparkContext sc, RDD<A> rdd1, RDD<B> rdd2, ZipFunction<A, B, C> f) {
        super(sc);
        int n1 = rdd1.getPartitions().size();
        int n2 = rdd2.getPartitions().size();
        if (n1 != n2) {
            throw new IllegalArgumentException(
                    "ZippedPartitionsRDD2 requires equal partition counts, got " + n1 + " and " + n2);
        }
        this.rdd1 = rdd1;
        this.rdd2 = rdd2;
        this.f = f;
        List<Partition> p = new ArrayList<>(n1);
        for (int i = 0; i < n1; i++) p.add(new ZipSlice(i));
        this.partitions = p;
    }

    @Override public List<Partition> getPartitions() { return partitions; }

    @Override public List<Dependency<?>> getDependencies() {
        return List.of(new OneToOneDependency<>(rdd1), new OneToOneDependency<>(rdd2));
    }

    @Override
    public Iterator<C> compute(Partition split, TaskContext ctx) {
        int i = split.index();
        Iterator<A> aIt = rdd1.iterator(rdd1.getPartitions().get(i), ctx);
        Iterator<B> bIt = rdd2.iterator(rdd2.getPartitions().get(i), ctx);
        return f.apply(aIt, bIt);
    }
}
