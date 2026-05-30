package com.minispark.rdd;

import com.minispark.api.MiniSparkContext;
import com.minispark.executor.TaskContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Source RDD wrapping an in-memory {@link List}, split into N roughly-equal
 * slices. Used by {@code MiniSparkContext.parallelize}.
 *
 * Real Spark equivalent: org.apache.spark.rdd.ParallelCollectionRDD
 */
public final class ParallelCollectionRDD<T> extends RDD<T> {

    private static final class Slice<T> implements Partition {
        private final int index;
        private final ArrayList<T> data;
        Slice(int index, ArrayList<T> data) { this.index = index; this.data = data; }
        public int index() { return index; }
    }

    private final List<Slice<T>> slices;

    public ParallelCollectionRDD(MiniSparkContext sc, List<T> data, int numSlices) {
        super(sc);
        if (numSlices < 1) throw new IllegalArgumentException("numSlices must be >= 1");
        this.slices = sliceData(data, numSlices);
    }

    private static <T> List<Slice<T>> sliceData(List<T> data, int numSlices) {
        int n = data.size();
        List<Slice<T>> out = new ArrayList<>(numSlices);
        // Even split with the remainder spread across the first few slices.
        // Done this way (rather than chunk-of-floor) so e.g. parallelize(7, 4) yields
        // sizes [2,2,2,1] instead of [1,1,1,4].
        int base = n / numSlices;
        int rem = n % numSlices;
        int cursor = 0;
        for (int i = 0; i < numSlices; i++) {
            int len = base + (i < rem ? 1 : 0);
            ArrayList<T> slice = new ArrayList<>(data.subList(cursor, cursor + len));
            out.add(new Slice<>(i, slice));
            cursor += len;
        }
        return out;
    }

    @Override
    public List<Partition> getPartitions() {
        return Collections.unmodifiableList(slices);
    }

    @Override
    public Iterator<T> compute(Partition split, TaskContext ctx) {
        @SuppressWarnings("unchecked")
        Slice<T> s = (Slice<T>) split;
        return s.data.iterator();
    }

    @Override
    public List<Dependency<?>> getDependencies() {
        return List.of();
    }
}
