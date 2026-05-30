package com.minispark.shuffle;

import java.io.Serializable;

/**
 * Opaque token identifying a registered shuffle. Created when a
 * {@link com.minispark.rdd.ShuffleDependency} is constructed; consumed by
 * {@link ShuffleManager#getWriter} / {@link ShuffleManager#getReader}.
 *
 * Real Spark equivalent: org.apache.spark.shuffle.ShuffleHandle
 */
public final class ShuffleHandle implements Serializable {
    public final int shuffleId;
    public final int numMaps;
    public final Partitioner partitioner;

    public ShuffleHandle(int shuffleId, int numMaps, Partitioner partitioner) {
        this.shuffleId = shuffleId;
        this.numMaps = numMaps;
        this.partitioner = partitioner;
    }
}
