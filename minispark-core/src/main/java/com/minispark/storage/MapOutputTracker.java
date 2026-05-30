package com.minispark.storage;

import com.minispark.rpc.RpcEndpoint;
import com.minispark.rpc.RpcEndpointRef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of "where each shuffle map output lives".
 *
 * <p>Exists in two roles, mirroring real Spark:
 * <ul>
 *   <li><b>Master</b> (driver): owns the authoritative map. The
 *       {@code DAGScheduler} registers a {@link MapStatus} for every completed
 *       map task. Serves {@link GetMapStatuses} queries from workers.</li>
 *   <li><b>Worker</b> (executor): has no data of its own; on
 *       {@link #getMapStatuses} it asks the master over RPC and caches the
 *       answer per shuffle.</li>
 * </ul>
 *
 * <p>In single-JVM local mode the driver and executors share one master
 * instance, so no RPC happens — reducers read the map directly.
 *
 * Real Spark equivalent: org.apache.spark.MapOutputTracker(Master|Worker)
 */
public final class MapOutputTracker implements RpcEndpoint {

    /** Endpoint name under which the master is published. */
    public static final String ENDPOINT_NAME = "MapOutputTracker";

    public record MapStatus(int mapId, ExecutorLocation location) implements Serializable {}

    /** Worker → Master: "give me all map statuses for this shuffle." */
    public record GetMapStatuses(int shuffleId) implements Serializable {}

    private final boolean isDriver;
    private final RpcEndpointRef masterRef; // null on the master itself

    // Driver-only authoritative state: shuffleId -> mapId -> status.
    private final Map<Integer, Map<Integer, MapStatus>> byShuffle = new ConcurrentHashMap<>();
    // Worker-only cache: shuffleId -> statuses.
    private final Map<Integer, List<MapStatus>> cache = new ConcurrentHashMap<>();

    private MapOutputTracker(boolean isDriver, RpcEndpointRef masterRef) {
        this.isDriver = isDriver;
        this.masterRef = masterRef;
    }

    public static MapOutputTracker master() { return new MapOutputTracker(true, null); }
    public static MapOutputTracker worker(RpcEndpointRef masterRef) {
        return new MapOutputTracker(false, masterRef);
    }

    // ----- master mutations (driver only) -----

    public void registerShuffle(int shuffleId, int numMaps) {
        byShuffle.computeIfAbsent(shuffleId, k -> new HashMap<>(numMaps));
    }

    public synchronized void registerMapOutput(int shuffleId, int mapId, ExecutorLocation loc) {
        byShuffle.computeIfAbsent(shuffleId, k -> new HashMap<>()).put(mapId, new MapStatus(mapId, loc));
    }

    public void unregisterShuffle(int shuffleId) {
        byShuffle.remove(shuffleId);
        cache.remove(shuffleId);
    }

    // ----- reads (driver: local; worker: ask master, cached) -----

    public List<MapStatus> getMapStatuses(int shuffleId) {
        if (isDriver) return localStatuses(shuffleId);
        return cache.computeIfAbsent(shuffleId, id -> masterRef.ask(new GetMapStatuses(id)));
    }

    private synchronized List<MapStatus> localStatuses(int shuffleId) {
        Map<Integer, MapStatus> m = byShuffle.get(shuffleId);
        if (m == null) return List.of();
        List<MapStatus> out = new ArrayList<>(m.values());
        out.sort((a, b) -> Integer.compare(a.mapId(), b.mapId()));
        return out;
    }

    // ----- endpoint (master answers worker queries) -----

    @Override
    public Object receiveAndReply(Object message) {
        if (message instanceof GetMapStatuses g) {
            // Return an ArrayList (serializable) snapshot.
            return new ArrayList<>(localStatuses(g.shuffleId()));
        }
        throw new IllegalArgumentException("MapOutputTracker got unexpected message: " + message);
    }
}
