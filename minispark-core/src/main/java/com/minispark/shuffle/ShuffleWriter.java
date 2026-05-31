package com.minispark.shuffle;

import com.minispark.api.Tuple2;

import java.util.Iterator;

/**
 * Map-side of a shuffle: consumes the (K,V) iterator produced by a map task
 * and writes one bucket per reduce partition.
 *
 * Real Spark equivalent: org.apache.spark.shuffle.ShuffleWriter
 */
public interface ShuffleWriter<K, V> {
    /**
     * Drains {@code records} into per-reducer buckets and persists them.
     * Returns the byte size of each reducer-partition bucket (indexed by
     * reducer id, length = {@code partitioner.numPartitions()}). The driver
     * aggregates these per-map sizes to drive AQE-style coalesce decisions
     * (see {@link com.minispark.scheduler.adaptive.CoalesceShufflePartitionsRule}).
     */
    long[] write(Iterator<Tuple2<K, V>> records);

    /** Called after {@link #write} to release any handles. */
    void stop(boolean success);
}
