package com.minispark.broadcast;

import com.minispark.executor.SparkEnv;
import com.minispark.storage.BlockId;
import com.minispark.storage.ExecutorLocation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Block-based broadcast. The driver writes the serialized value into its
 * {@link com.minispark.storage.BlockManager} under a {@link BlockId.BroadcastBlock};
 * the handle carries only the broadcast id and the driver's location. On first
 * {@link #value()} call on a given executor, the value is fetched once over the
 * BlockManager and cached in a per-JVM map, so every later task on that
 * executor reads the local copy.
 *
 * <p>Real Spark's TorrentBroadcast splits the value into chunks and lets
 * executors fetch chunks from each other (BitTorrent-style) to avoid driver
 * hot-spotting. We keep a single chunk pulled from the driver — same idea,
 * minus the peer-to-peer fan-out — and name it the same so the mapping is
 * obvious.
 *
 * Real Spark equivalent: org.apache.spark.broadcast.TorrentBroadcast
 */
public final class TorrentBroadcast<T> implements Broadcast<T> {

    // Per-JVM (i.e. per-executor) cache: a broadcast fetched once is reused by
    // every task on this executor. Static because all tasks share the JVM; keyed
    // by broadcast id so distinct broadcasts don't collide.
    private static final ConcurrentMap<Long, Object> LOCAL_CACHE = new ConcurrentHashMap<>();

    private final long id;
    private final ExecutorLocation driverLocation;

    public TorrentBroadcast(long id, ExecutorLocation driverLocation) {
        this.id = id;
        this.driverLocation = driverLocation;
    }

    @Override public long id() { return id; }

    @Override
    @SuppressWarnings("unchecked")
    public T value() {
        return (T) LOCAL_CACHE.computeIfAbsent(id, k -> fetch());
    }

    private Object fetch() {
        SparkEnv env = SparkEnv.get();
        BlockId.BroadcastBlock blockId = new BlockId.BroadcastBlock(id);
        byte[] bytes = env.blockManager().getRemoteBlock(blockId, driverLocation)
                .orElseThrow(() -> new IllegalStateException("broadcast " + id + " not found at driver"));
        return env.serializer().deserialize(bytes);
    }
}
