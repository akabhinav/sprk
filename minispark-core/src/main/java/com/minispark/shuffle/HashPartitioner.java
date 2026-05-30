package com.minispark.shuffle;

import java.util.Objects;

/**
 * Maps a key to a partition by {@code hash(key) mod numPartitions}, taking
 * care with {@link Integer#MIN_VALUE} from {@code Math.abs}.
 *
 * Real Spark equivalent: org.apache.spark.HashPartitioner
 */
public final class HashPartitioner extends Partitioner {
    private final int numPartitions;

    public HashPartitioner(int numPartitions) {
        if (numPartitions < 1) throw new IllegalArgumentException("numPartitions must be >= 1");
        this.numPartitions = numPartitions;
    }

    @Override public int numPartitions() { return numPartitions; }

    @Override
    public int getPartition(Object key) {
        if (key == null) return 0;
        int h = key.hashCode();
        // Java's % can yield negatives; force into [0, n).
        int p = h % numPartitions;
        return p < 0 ? p + numPartitions : p;
    }

    @Override public boolean equals(Object o) {
        return o instanceof HashPartitioner hp && hp.numPartitions == numPartitions;
    }
    @Override public int hashCode() { return Objects.hash(numPartitions); }
}
