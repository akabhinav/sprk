package com.minispark.memory;

import com.minispark.storage.MemoryStore;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pool accounting + cross-pool borrow / reclaim behaviour. No real disk or
 * RDDs involved — pure unit test of the manager + the two pools.
 */
final class UnifiedMemoryManagerTest {

    @Test
    void initial_split_by_storage_fraction() {
        UnifiedMemoryManager mm = new UnifiedMemoryManager(1000, 0.5);
        assertThat(mm.storagePool().poolSize()).isEqualTo(500L);
        assertThat(mm.executionPool().poolSize()).isEqualTo(500L);
        assertThat(mm.storagePool().poolSize() + mm.executionPool().poolSize())
                .isEqualTo(mm.unifiedMaxBytes());
    }

    @Test
    void execution_acquire_fits_within_pool() {
        UnifiedMemoryManager mm = new UnifiedMemoryManager(1000, 0.5);
        long got = mm.acquireExecutionMemory(200, /*taskId=*/1L);
        assertThat(got).isEqualTo(200L);
        assertThat(mm.executionPool().memoryUsed()).isEqualTo(200L);
        assertThat(mm.executionPool().memoryFree()).isEqualTo(300L);
    }

    @Test
    void execution_reclaims_from_storage_when_its_pool_is_too_small() {
        // Storage pool = 200, exec pool = 800. We ask for 900 from exec.
        // Reclaim 100 from storage (storage floor = 200, currently 200 → can't
        // reclaim from storage since it's already at the floor). Should grant
        // only 800 (exec's current pool).
        UnifiedMemoryManager mm = new UnifiedMemoryManager(1000, 0.2);
        long got = mm.acquireExecutionMemory(900, 1L);
        assertThat(got).isEqualTo(800L);   // limited by what exec pool holds; no floor headroom

        // Now make storage *use* nothing, set its floor lower by starting with
        // a bigger storage fraction. Storage = 500 (floor 500), exec = 500.
        // No room to reclaim; same answer.
        UnifiedMemoryManager mm2 = new UnifiedMemoryManager(1000, 0.5);
        long got2 = mm2.acquireExecutionMemory(900, 1L);
        assertThat(got2).isEqualTo(500L);  // can't grow exec — storage is at its floor
    }

    @Test
    void execution_reclaims_storage_above_floor_by_evicting_blocks() {
        // Storage starts at 60% (600), floor = 600. We borrow up by simulating
        // storage's pool growing to 800 first (so it's now ABOVE the floor and
        // exec can reclaim 200).
        UnifiedMemoryManager mm = new UnifiedMemoryManager(1000, 0.6);
        // Borrow: shrink exec by 200, grow storage by 200 → storage = 800, exec = 200.
        mm.executionPool().decrementPoolSize(200);
        mm.storagePool().incrementPoolSize(200);
        assertThat(mm.storagePool().poolSize()).isEqualTo(800L);
        assertThat(mm.executionPool().poolSize()).isEqualTo(200L);

        // Wire a MemoryStore so storage has something to evict on demand.
        MemoryStore store = new MemoryStore(1000);
        mm.setMemoryStore(store);
        store.setOnReleaseCallback(bytes -> mm.releaseStorageMemory(bytes));
        store.setOnAcquireCallback(bytes -> mm.storagePool().incrementUsedDirectly(bytes));
        // Put a 700-byte cached (evictable) block — so the storage pool is
        // 700/800 used, only 100 bytes free. To give up the 200 bytes exec
        // wants to reclaim, an eviction is required.
        store.put(new com.minispark.storage.BlockId.RDDBlock(1, 0), new byte[700],
                com.minispark.storage.StorageLevel.MEMORY_ONLY, true);
        assertThat(mm.storagePool().memoryUsed()).isEqualTo(700L);

        // Now exec asks for 400. It has 200 in its pool; needs 200 more.
        // Storage can give up (800 - 600=floor) = 200 bytes. Storage pool has
        // 100 free; needs to evict 100 more — but we evict whole blocks, so
        // the 700-byte block is dropped entirely.
        long got = mm.acquireExecutionMemory(400, 1L);
        assertThat(got).isEqualTo(400L);
        // The cached block must be gone (whole-block LRU eviction).
        assertThat(store.usedBytes()).isEqualTo(0L);
        // Pool sizes: storage shrunk by 200, exec grew by 200.
        assertThat(mm.storagePool().poolSize()).isEqualTo(600L);
        assertThat(mm.executionPool().poolSize()).isEqualTo(400L);
    }

    @Test
    void release_returns_bytes_to_pool() {
        UnifiedMemoryManager mm = new UnifiedMemoryManager(1000, 0.5);
        mm.acquireExecutionMemory(200, 1L);
        assertThat(mm.executionPool().memoryUsedForTask(1L)).isEqualTo(200L);
        mm.releaseExecutionMemory(200, 1L);
        assertThat(mm.executionPool().memoryUsedForTask(1L)).isEqualTo(0L);
        assertThat(mm.executionPool().memoryUsed()).isEqualTo(0L);
    }

    @Test
    void release_all_for_task_drains_unreleased_acquisitions() {
        UnifiedMemoryManager mm = new UnifiedMemoryManager(1000, 0.5);
        mm.acquireExecutionMemory(120, 1L);
        mm.acquireExecutionMemory(80, 1L);
        long drained = mm.releaseAllExecutionMemoryForTask(1L);
        assertThat(drained).isEqualTo(200L);
        assertThat(mm.executionPool().memoryUsed()).isEqualTo(0L);
    }

    @Test
    void fair_share_caps_a_single_tasks_growth_when_peers_are_active() {
        UnifiedMemoryManager mm = new UnifiedMemoryManager(1000, 0);  // all exec, no storage
        // Pool = 1000, 2 tasks → fair share = 500 each.
        long t1 = mm.acquireExecutionMemory(1000, 1L);
        assertThat(t1).isEqualTo(1000L);   // task 1 alone → no peer cap
        // task 2 arrives. Task 1 already has 1000; task 2's fair share is 500
        // but the pool has 0 free, so it gets 0.
        long t2 = mm.acquireExecutionMemory(500, 2L);
        assertThat(t2).isEqualTo(0L);
        // Task 1 releases; now task 2 (still registered as active) can take up
        // to its share — but with both alive, share = 500.
        mm.releaseAllExecutionMemoryForTask(1L);
        long t2b = mm.acquireExecutionMemory(500, 2L);
        // Pool free now 1000; one active task → fair share = 1000. Task 2 grabs 500.
        assertThat(t2b).isEqualTo(500L);
    }
}
