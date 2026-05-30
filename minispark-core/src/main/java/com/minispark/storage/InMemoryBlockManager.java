package com.minispark.storage;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Phase 2 BlockManager: an in-process {@link ConcurrentHashMap}. Simple, but
 * good enough — local-file shuffle is still local-process, so we can keep
 * shuffle bytes in heap until {@link BlockManager} gains a disk tier.
 */
public final class InMemoryBlockManager implements BlockManager {

    private final ExecutorLocation location;
    private final ConcurrentMap<BlockId, byte[]> blocks = new ConcurrentHashMap<>();

    public InMemoryBlockManager(ExecutorLocation location) {
        this.location = location;
    }

    @Override public ExecutorLocation location() { return location; }

    @Override
    public void putBlock(BlockId id, byte[] data) {
        blocks.put(id, data);
    }

    @Override
    public Optional<byte[]> getBlock(BlockId id) {
        return Optional.ofNullable(blocks.get(id));
    }
}
