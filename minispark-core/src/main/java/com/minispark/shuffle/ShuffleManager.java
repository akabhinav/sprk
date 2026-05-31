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

    /** Read every map's bucket(s) for the reducer-id range. Equivalent to
     *  {@link #getReader(ShuffleHandle, int, int, int, int)} with map range {@code [-1, -1)}. */
    default <K, V> ShuffleReader<K, V> getReader(ShuffleHandle handle, int startPartition, int endPartition) {
        return getReader(handle, startPartition, endPartition, -1, -1);
    }

    /**
     * Read a slice of the shuffle: reducer-ids in {@code [startPartition, endPartition)} and
     * map-ids in {@code [startMapId, endMapId)}. A map range of {@code [-1, -1)} means "all maps"
     * — the regular non-skew case.
     *
     * <p>The map-id range enables AQE's {@code OptimizeSkewedPartitionsRule} to split a single
     * skewed reducer into N sub-tasks, each reading a portion of the map outputs for that
     * reducer. The shuffle data is unchanged; only the read plan changes.
     */
    <K, V> ShuffleReader<K, V> getReader(ShuffleHandle handle,
                                          int startPartition, int endPartition,
                                          int startMapId, int endMapId);

    void unregisterShuffle(int shuffleId);
}
