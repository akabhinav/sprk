package com.minispark.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Driver-side registry of "where does shuffle data live". After every map
 * task completes, the executor reports a {@link MapStatus} here; reducers
 * then ask which executor holds each map's bucket for their reduce id.
 *
 * <p>Phase 2 has just one executor, so the answer is always "here", but the
 * indirection is the whole point: in Phase 5 the same registry will hand out
 * remote {@link ExecutorLocation}s and reducers will fetch over RPC.
 *
 * Real Spark equivalent: org.apache.spark.MapOutputTracker(Master|Worker)
 */
public final class MapOutputTracker {

    public record MapStatus(int mapId, ExecutorLocation location) implements java.io.Serializable {}

    // shuffleId -> mapId -> status. Per-shuffle inner map is bounded by numMaps.
    private final Map<Integer, Map<Integer, MapStatus>> byShuffle = new ConcurrentHashMap<>();

    public synchronized void registerShuffle(int shuffleId, int numMaps) {
        byShuffle.computeIfAbsent(shuffleId, k -> new HashMap<>(numMaps));
    }

    public synchronized void registerMapOutput(int shuffleId, int mapId, ExecutorLocation loc) {
        byShuffle.computeIfAbsent(shuffleId, k -> new HashMap<>()).put(mapId, new MapStatus(mapId, loc));
    }

    /** Statuses ordered by mapId so reducers fetch deterministically. */
    public synchronized List<MapStatus> getMapStatuses(int shuffleId) {
        Map<Integer, MapStatus> m = byShuffle.get(shuffleId);
        if (m == null) return List.of();
        return m.values().stream()
                .sorted((a, b) -> Integer.compare(a.mapId, b.mapId))
                .toList();
    }

    public synchronized void unregisterShuffle(int shuffleId) {
        byShuffle.remove(shuffleId);
    }
}
