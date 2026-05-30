package com.minispark.shuffle;

import java.io.Serializable;

/**
 * Maps a key to a reduce partition index in {@code [0, numPartitions)}.
 *
 * Real Spark equivalent: org.apache.spark.Partitioner
 */
public abstract class Partitioner implements Serializable {
    public abstract int numPartitions();
    public abstract int getPartition(Object key);
}
