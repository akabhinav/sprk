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
    // No worker-side cache: a recovery on the driver may change which executor
    // hosts a map output at any time, and a stale cache here would send the
    // reducer back to the dead location forever. Each call asks the master.

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
    }

    /**
     * List every map output currently registered at {@code loc}, without
     * mutating the tracker. The DAG layer uses this to enumerate which map
     * tasks an executor's death requires re-running; the new outputs simply
     * <i>overwrite</i> the old entries by mapId when they finish, keeping the
     * tracker always-complete (so a concurrent reducer never sees a partial
     * statuses list and silently drops records).
     */
    public synchronized List<int[]> mapsAtLocation(ExecutorLocation loc) {
        List<int[]> found = new ArrayList<>();
        for (Map.Entry<Integer, Map<Integer, MapStatus>> e : byShuffle.entrySet()) {
            int shuffleId = e.getKey();
            for (Map.Entry<Integer, MapStatus> me : e.getValue().entrySet()) {
                if (me.getValue().location().equals(loc)) {
                    found.add(new int[]{shuffleId, me.getKey()});
                }
            }
        }
        return found;
    }

    // ----- reads (driver: local; worker: ask master, cached) -----

    public List<MapStatus> getMapStatuses(int shuffleId) {
        if (isDriver) return localStatuses(shuffleId);
        // Always ask the master; recoveries on the driver mean a snapshot we
        // took moments ago might already point to a dead executor.
        return masterRef.ask(new GetMapStatuses(shuffleId));
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
