package com.minispark.storage;

import com.minispark.rpc.RpcEndpoint;
import com.minispark.rpc.RpcEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * A {@link BlockManager} that can serve and fetch <i>any</i> block over RPC,
 * with a two-tier local store: a bounded {@link MemoryStore} and a
 * {@link DiskStore}. Each executor (and the driver) runs one, publishing a
 * {@code "BlockManager"} endpoint on its {@link RpcEnv}.
 *
 * <p>Storage tiering, keyed off {@link StorageLevel}:
 * <ul>
 *   <li>Shuffle/broadcast blocks (the no-level {@code putBlock}) are pinned in
 *       memory — they must not be evicted mid-job.</li>
 *   <li>{@code MEMORY_ONLY} cache blocks go to memory; if they don't fit (after
 *       evicting other cache blocks) they are simply not stored → recomputed.</li>
 *   <li>{@code MEMORY_AND_DISK} blocks go to memory, and on eviction or
 *       no-fit they spill to disk instead of being dropped.</li>
 *   <li>{@code DISK_ONLY} blocks bypass the memory budget entirely.</li>
 * </ul>
 *
 * Real Spark equivalent: org.apache.spark.storage.BlockManager + BlockTransferService
 */
public final class NetworkBlockManager implements BlockManager, RpcEndpoint {

    public static final String ENDPOINT_NAME = "BlockManager";
    private static final Logger LOG = LoggerFactory.getLogger(NetworkBlockManager.class);

    /** Consumer → owning manager: fetch this block's bytes (null reply = absent). */
    public record FetchBlock(BlockId id) implements Serializable {}

    private final ExecutorLocation location;
    private final RpcEnv rpcEnv;
    private final MemoryStore memoryStore;
    private final DiskStore diskStore;

    public NetworkBlockManager(ExecutorLocation location, RpcEnv rpcEnv) {
        this(location, rpcEnv, 512L * 1024 * 1024, null);
    }

    public NetworkBlockManager(ExecutorLocation location, RpcEnv rpcEnv,
                               long maxMemoryBytes, String diskDir) {
        this.location = location;
        this.rpcEnv = rpcEnv;
        this.memoryStore = new MemoryStore(maxMemoryBytes);
        this.diskStore = new DiskStore(diskDir);
        rpcEnv.setupEndpoint(ENDPOINT_NAME, this);
    }

    @Override public ExecutorLocation location() { return location; }

    /** Default: pinned in memory (shuffle/broadcast). Falls back to disk if huge. */
    @Override
    public void putBlock(BlockId id, byte[] data) {
        List<MemoryStore.Evicted> evicted = memoryStore.put(id, data, StorageLevel.MEMORY_ONLY, false);
        if (evicted == null) {
            // Too big for the memory budget even pinned — keep it on disk so the
            // job can still find it. (Shuffle blocks larger than the whole budget.)
            diskStore.put(id, data);
        } else {
            spillEvicted(evicted);
        }
    }

    @Override
    public void putBlock(BlockId id, byte[] data, StorageLevel level) {
        if (level == StorageLevel.DISK_ONLY) {
            diskStore.put(id, data);
            return;
        }
        if (level.useMemory()) {
            List<MemoryStore.Evicted> evicted = memoryStore.put(id, data, level, true);
            if (evicted != null) {
                spillEvicted(evicted);
                return;
            }
            // Didn't fit. Spill to disk if allowed; otherwise drop (recompute later).
            if (level.useDisk()) diskStore.put(id, data);
            else LOG.debug("Block {} did not fit in memory and level is MEMORY_ONLY; dropping", id);
        }
    }

    /** Evicted MEMORY_AND_DISK blocks must not be lost — write them to disk. */
    private void spillEvicted(List<MemoryStore.Evicted> evicted) {
        for (MemoryStore.Evicted e : evicted) {
            if (e.level().useDisk()) diskStore.put(e.id(), e.data());
        }
    }

    @Override
    public Optional<byte[]> getBlock(BlockId id) {
        byte[] mem = memoryStore.get(id);
        if (mem != null) return Optional.of(mem);
        byte[] disk = diskStore.get(id);
        return Optional.ofNullable(disk);
    }

    @Override
    public Optional<byte[]> getRemoteBlock(BlockId id, ExecutorLocation loc) {
        if (location.equals(loc)) return getBlock(id);
        byte[] bytes = rpcEnv.endpointRef(ENDPOINT_NAME, loc.host, loc.port)
                .ask(new FetchBlock(id));
        return Optional.ofNullable(bytes);
    }

    @Override
    public Object receiveAndReply(Object message) {
        if (message instanceof FetchBlock f) {
            return getBlock(f.id()).orElse(null);
        }
        throw new IllegalArgumentException("BlockManager got unexpected message: " + message);
    }

    // ----- introspection (for the UI / tests) -----
    public long memoryUsedBytes() { return memoryStore.usedBytes(); }
    public long memoryMaxBytes() { return memoryStore.maxBytes(); }
    public boolean inMemory(BlockId id) { return memoryStore.contains(id); }
    public boolean onDisk(BlockId id) { return diskStore.contains(id); }
}
