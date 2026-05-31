package com.minispark.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A bounded, LRU-evicting in-memory block store. Tracks total bytes against a
 * budget; when a put would overflow, it evicts least-recently-used <i>evictable</i>
 * blocks (cached RDD partitions) until there's room. Shuffle/broadcast blocks
 * are pinned (not evictable) because dropping them would break an in-flight job.
 *
 * <p>This is the piece that makes {@code MEMORY_AND_DISK} meaningful: eviction
 * returns the dropped blocks so the {@link NetworkBlockManager} can spill the
 * ones whose StorageLevel permits disk.
 *
 * Real Spark equivalent: org.apache.spark.storage.memory.MemoryStore.
 */
public final class MemoryStore {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryStore.class);

    /** An evicted block handed back to the caller (so MEMORY_AND_DISK can spill it). */
    public record Evicted(BlockId id, byte[] data, StorageLevel level) {}

    private static final class Entry {
        final byte[] data; final StorageLevel level; final boolean evictable;
        Entry(byte[] data, StorageLevel level, boolean evictable) {
            this.data = data; this.level = level; this.evictable = evictable;
        }
    }

    private final long maxBytes;
    private long usedBytes;
    // accessOrder=true → iteration order is LRU-first, which is what we evict.
    private final LinkedHashMap<BlockId, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);
    // Callbacks the StorageMemoryPool wires in. {@code onAcquire} fires for
    // every byte added; {@code onRelease} for every byte dropped (by put-driven
    // eviction, by execution-pool reclaim, or by explicit {@link #remove}).
    // Net effect: the pool's memoryUsed mirrors {@link #usedBytes}.
    private java.util.function.LongConsumer onRelease = bytes -> {};
    private java.util.function.LongConsumer onAcquire = bytes -> {};

    public MemoryStore(long maxBytes) { this.maxBytes = maxBytes; }

    /** Set once at startup so the StorageMemoryPool tracks our evictions. */
    public synchronized void setOnReleaseCallback(java.util.function.LongConsumer cb) {
        this.onRelease = cb == null ? bytes -> {} : cb;
    }

    /** Set once at startup so the StorageMemoryPool tracks our acquisitions. */
    public synchronized void setOnAcquireCallback(java.util.function.LongConsumer cb) {
        this.onAcquire = cb == null ? bytes -> {} : cb;
    }

    public synchronized long maxBytes() { return maxBytes; }
    public synchronized long usedBytes() { return usedBytes; }

    /**
     * Try to store {@code data}. {@code evictable} blocks (RDD cache) may be
     * dropped under pressure; pinned blocks (shuffle/broadcast) are never
     * dropped. Returns the blocks evicted to make room (possibly empty), or
     * {@code null} if the block could not be stored even after eviction.
     */
    public synchronized List<Evicted> put(BlockId id, byte[] data, StorageLevel level, boolean evictable) {
        if (data.length > maxBytes) return null; // never fits, don't thrash
        List<Evicted> evicted = new ArrayList<>();
        // Account for replacing an existing entry of the same id.
        Entry prev = entries.remove(id);
        if (prev != null) usedBytes -= prev.data.length;

        while (usedBytes + data.length > maxBytes) {
            BlockId victim = firstEvictable();
            if (victim == null) {
                // Can't free enough; roll back the removal accounting.
                if (prev != null) { entries.put(id, prev); usedBytes += prev.data.length; }
                return null;
            }
            Entry v = entries.remove(victim);
            usedBytes -= v.data.length;
            onRelease.accept(v.data.length);
            evicted.add(new Evicted(victim, v.data, v.level));
            LOG.debug("Evicted {} ({} bytes) to free memory", victim, v.data.length);
        }
        entries.put(id, new Entry(data, level, evictable));
        usedBytes += data.length;
        onAcquire.accept(data.length);
        return evicted;
    }

    private BlockId firstEvictable() {
        for (Iterator<Map.Entry<BlockId, Entry>> it = entries.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<BlockId, Entry> e = it.next();
            if (e.getValue().evictable) return e.getKey();
        }
        return null;
    }

    public synchronized byte[] get(BlockId id) {
        Entry e = entries.get(id);
        return e == null ? null : e.data;
    }

    public synchronized boolean contains(BlockId id) { return entries.containsKey(id); }

    public synchronized void remove(BlockId id) {
        Entry e = entries.remove(id);
        if (e != null) {
            usedBytes -= e.data.length;
            onRelease.accept(e.data.length);
        }
    }

    /**
     * Asked by the storage pool when the execution side needs more memory.
     * Evicts LRU evictable blocks up to {@code bytes} and returns the actual
     * number of bytes released. Pinned blocks (shuffle/broadcast) are never
     * evicted by this path — the execution side just sees less reclaimable
     * memory in that case. Eviction here is fire-and-forget: blocks are
     * dropped outright, not handed to the disk store, since the caller
     * (execution pool reclaim) doesn't know each block's StorageLevel and
     * the cost of double-spilling outweighs the benefit. {@code put}-driven
     * eviction still goes through the MEMORY_AND_DISK spill path.
     */
    public synchronized long evictBytesUpTo(long bytes) {
        long freed = 0;
        while (freed < bytes) {
            BlockId victim = firstEvictable();
            if (victim == null) break;
            Entry v = entries.remove(victim);
            usedBytes -= v.data.length;
            freed += v.data.length;
            LOG.debug("Eviction-on-demand dropped {} ({} bytes)", victim, v.data.length);
        }
        onRelease.accept(freed);
        return freed;
    }
}
