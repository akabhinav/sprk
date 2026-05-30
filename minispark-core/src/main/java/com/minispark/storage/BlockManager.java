package com.minispark.storage;

import java.util.Optional;

/**
 * Per-executor block store. Phase 2 only needs the local in-memory side for
 * shuffle blocks; Phase 4's RPC fills in {@link #getRemoteBlock} and Phase 6
 * adds disk/memory tiering for {@code rdd.cache()}.
 *
 * Real Spark equivalent: org.apache.spark.storage.BlockManager
 */
public interface BlockManager {
    /** Where this manager lives. */
    ExecutorLocation location();

    /** Store a block at the default (memory, pinned) level. */
    void putBlock(BlockId id, byte[] data);

    /**
     * Store a block honoring a {@link StorageLevel}: memory (bounded, may evict),
     * disk, or both. The default ignores the level and behaves like
     * {@link #putBlock(BlockId, byte[])}; tiering implementations override.
     */
    default void putBlock(BlockId id, byte[] data, StorageLevel level) {
        putBlock(id, data);
    }

    /** Local lookup only (checks every local tier). */
    Optional<byte[]> getBlock(BlockId id);

    /**
     * Fetch a block hosted by a (possibly remote) executor. The default
     * implementation handles the local case; remote-capable implementations
     * override.
     */
    default Optional<byte[]> getRemoteBlock(BlockId id, ExecutorLocation loc) {
        if (location().equals(loc)) return getBlock(id);
        throw new UnsupportedOperationException(
                "Remote block fetch from " + loc + " not implemented in this BlockManager");
    }
}
