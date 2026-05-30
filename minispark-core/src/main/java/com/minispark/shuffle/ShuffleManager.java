package com.minispark.shuffle;

/**
 * Factory for shuffle writers and readers. Pluggable: Phase 2 uses a hash
 * shuffle; sort shuffle is a Phase 6 stretch goal. The DAGScheduler talks
 * only to this interface.
 *
 * Real Spark equivalent: org.apache.spark.shuffle.ShuffleManager
 */
public interface ShuffleManager {
    /** Register a shuffle and obtain its handle. Called when a ShuffleDependency is created. */
    ShuffleHandle registerShuffle(int shuffleId, int numMaps, Partitioner partitioner);

    <K, V> ShuffleWriter<K, V> getWriter(ShuffleHandle handle, int mapId);

    <K, V> ShuffleReader<K, V> getReader(ShuffleHandle handle, int startPartition, int endPartition);

    void unregisterShuffle(int shuffleId);
}
