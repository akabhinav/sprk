package com.minispark.memory;

import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import com.minispark.storage.BlockId;
import com.minispark.storage.BlockManager;
import com.minispark.storage.ExecutorLocation;
import com.minispark.storage.StorageLevel;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Round-trip tests for the spillable hash map. Uses a {@link FakeBlockManager}
 * that stores spill blocks in an in-memory map (the real {@link BlockManager}
 * would put them on disk), and a {@link UnifiedMemoryManager} sized so a few
 * inserts force a spill — exactly the path that prevents OOM on wide groupBy.
 */
final class ExternalAppendOnlyMapTest {

    /** Combiner: sum integers. */
    private static final RDD.SerializableBiFunction<Integer, Integer, Integer> SUM = Integer::sum;

    @Test
    void no_spill_returns_combined_pairs_in_one_pass() {
        UnifiedMemoryManager mm = new UnifiedMemoryManager(10_000, 0);
        TaskMemoryManager tmm = new TaskMemoryManager(mm, 1L);
        FakeBlockManager bm = new FakeBlockManager();
        ExternalAppendOnlyMap<String, Integer> map = new ExternalAppendOnlyMap<>(tmm, SUM, bm);

        map.insert("a", 1); map.insert("b", 2); map.insert("a", 3); map.insert("c", 4);

        Map<String, Integer> got = collect(map.iterator());
        assertThat(got).containsExactlyInAnyOrderEntriesOf(Map.of("a", 4, "b", 2, "c", 4));
        // No spill was needed — the budget fits.
        assertThat(bm.blocks).isEmpty();
    }

    @Test
    void tight_budget_forces_spill_and_iterator_still_returns_correct_combined_result() {
        // 4 entries × ENTRY_BYTES(80) = 320 bytes new entries. Budget 200 forces
        // at least one spill before all entries fit in memory.
        UnifiedMemoryManager mm = new UnifiedMemoryManager(200, 0);
        TaskMemoryManager tmm = new TaskMemoryManager(mm, 1L);
        FakeBlockManager bm = new FakeBlockManager();
        ExternalAppendOnlyMap<String, Integer> map = new ExternalAppendOnlyMap<>(tmm, SUM, bm);

        // Insert keys that go to different hash buckets so the spill+merge has
        // to combine across files for the same key.
        map.insert("a", 1);
        map.insert("b", 1);
        map.insert("c", 1);
        map.insert("d", 1);
        map.insert("a", 10);    // same-key combine across spills
        map.insert("b", 20);
        map.insert("e", 1);
        map.insert("a", 100);

        // At least one spill must have happened given the tight budget.
        assertThat(bm.blocks).isNotEmpty();

        Map<String, Integer> got = collect(map.iterator());
        // a appeared 3 times: 1 + 10 + 100 = 111
        // b twice: 1 + 20 = 21; c, d, e once each.
        assertThat(got).containsExactlyInAnyOrderEntriesOf(
                Map.of("a", 111, "b", 21, "c", 1, "d", 1, "e", 1));
    }

    @Test
    void many_inserts_with_repeats_match_a_plain_hashmap_baseline() {
        UnifiedMemoryManager mm = new UnifiedMemoryManager(500, 0);
        TaskMemoryManager tmm = new TaskMemoryManager(mm, 1L);
        FakeBlockManager bm = new FakeBlockManager();
        ExternalAppendOnlyMap<Integer, Integer> map = new ExternalAppendOnlyMap<>(tmm, SUM, bm);

        Map<Integer, Integer> baseline = new HashMap<>();
        // 1000 inserts across 50 distinct keys → ~20 hits per key on average.
        for (int i = 0; i < 1000; i++) {
            int k = (i * 31 + 7) % 50;
            int v = i % 10;
            map.insert(k, v);
            baseline.merge(k, v, Integer::sum);
        }
        // Tight budget guarantees multiple spills.
        assertThat(bm.blocks.size()).isGreaterThan(1);

        Map<Integer, Integer> got = collect(map.iterator());
        assertThat(got).isEqualTo(baseline);
    }

    @Test
    void null_keys_supported_via_hashcode_zero_bucket() {
        UnifiedMemoryManager mm = new UnifiedMemoryManager(120, 0);
        TaskMemoryManager tmm = new TaskMemoryManager(mm, 1L);
        FakeBlockManager bm = new FakeBlockManager();
        ExternalAppendOnlyMap<String, Integer> map = new ExternalAppendOnlyMap<>(tmm, SUM, bm);

        map.insert(null, 1);
        map.insert("x", 1);
        map.insert(null, 2);
        map.insert("y", 1);
        map.insert(null, 3);

        Map<String, Integer> got = collect(map.iterator());
        assertThat(got.get(null)).isEqualTo(6);
        assertThat(got.get("x")).isEqualTo(1);
        assertThat(got.get("y")).isEqualTo(1);
    }

    // ----- helpers -----

    private static <K, V> Map<K, V> collect(Iterator<Tuple2<K, V>> it) {
        Map<K, V> out = new HashMap<>();
        while (it.hasNext()) {
            Tuple2<K, V> kv = it.next();
            out.put(kv._1(), kv._2());
        }
        return out;
    }

    /** Stand-in BlockManager that holds spill bytes in memory — fine for unit tests. */
    private static final class FakeBlockManager implements BlockManager {
        final Map<BlockId, byte[]> blocks = new HashMap<>();
        @Override public ExecutorLocation location() { return new ExecutorLocation("fake", 0); }
        @Override public void putBlock(BlockId id, byte[] data) { blocks.put(id, data); }
        @Override public void putBlock(BlockId id, byte[] data, StorageLevel level) { blocks.put(id, data); }
        @Override public Optional<byte[]> getBlock(BlockId id) {
            return Optional.ofNullable(blocks.get(id));
        }
    }
}
