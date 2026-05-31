package com.minispark.scheduler.adaptive;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the coalesce planner — the pure function that drives AQE's
 * "fewer post-shuffle partitions" rewrite.
 */
final class CoalesceShufflePartitionsRuleTest {

    private static int[] flat(List<int[]> ranges) {
        int n = 0;
        for (int[] r : ranges) n += 2;
        int[] out = new int[n];
        int i = 0;
        for (int[] r : ranges) { out[i++] = r[0]; out[i++] = r[1]; }
        return out;
    }

    @Test
    void coalesces_many_tiny_partitions_into_one() {
        long[] sizes = {10, 10, 10, 10, 10, 10, 10, 10}; // 80 bytes total
        List<int[]> ranges = CoalesceShufflePartitionsRule.plan(sizes, 1_000, 1);
        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0)).containsExactly(0, 8);
    }

    @Test
    void leaves_partitions_alone_when_each_already_meets_target() {
        long[] sizes = {1000, 1000, 1000, 1000};
        List<int[]> ranges = CoalesceShufflePartitionsRule.plan(sizes, 1000, 1);
        // Each reducer fills the target on its own, so no merging.
        assertThat(ranges).hasSize(4);
        assertThat(flat(ranges)).containsExactly(0, 1, 1, 2, 2, 3, 3, 4);
    }

    @Test
    void greedily_packs_contiguous_reducers_up_to_target() {
        // target = 100. Greedy scan:
        //   [0]=30 → run=30. [1]=30 → 30+30=60 ≤ 100, append, run=60.
        //   [2]=10 → 60+10=70 ≤ 100, append, run=70.
        //   [3]=50 → 70+50=120 > 100 → cut here. ranges=[0,3). new run=50.
        //   [4]=60 → 50+60=110 > 100 → cut here. ranges += [3,4). new run=60.
        //   [5]=20 → 60+20=80 ≤ 100, append, run=80. [6]=5 → 85 ≤ 100, append.
        // Flush trailing [4,7).
        long[] sizes = {30, 30, 10, 50, 60, 20, 5};
        List<int[]> ranges = CoalesceShufflePartitionsRule.plan(sizes, 100, 1);
        assertThat(ranges).hasSize(3);
        assertThat(flat(ranges)).containsExactly(0, 3, 3, 4, 4, 7);
    }

    @Test
    void respects_minimum_partition_floor() {
        long[] sizes = {10, 10, 10, 10, 10, 10};
        // Without floor everything fuses into one; floor=3 forces splitting back.
        List<int[]> ranges = CoalesceShufflePartitionsRule.plan(sizes, 10_000, 3);
        assertThat(ranges).hasSize(3);
        // Ranges still cover [0,6) end-to-end with no overlaps.
        int prev = 0;
        for (int[] r : ranges) {
            assertThat(r[0]).isEqualTo(prev);
            assertThat(r[1]).isGreaterThan(r[0]);
            prev = r[1];
        }
        assertThat(prev).isEqualTo(6);
    }

    @Test
    void single_oversized_reducer_emits_its_own_range() {
        // The first reducer alone is bigger than the target. We can't split a
        // single reducer (that's the skew-join problem, separate rule), so it
        // must come out as its own range, not be glued to neighbours.
        long[] sizes = {500, 10, 10, 10};
        List<int[]> ranges = CoalesceShufflePartitionsRule.plan(sizes, 100, 1);
        assertThat(ranges).hasSize(2);
        assertThat(ranges.get(0)).containsExactly(0, 1);   // the giant
        assertThat(ranges.get(1)).containsExactly(1, 4);   // the rest fused
    }

    @Test
    void zero_partitions_returns_empty() {
        assertThat(CoalesceShufflePartitionsRule.plan(new long[0], 100, 1)).isEmpty();
    }

    @Test
    void min_partitions_clamped_to_input_count() {
        long[] sizes = {10, 10};
        // minPartitions=99 > n=2: should not crash; result is at most n ranges.
        List<int[]> ranges = CoalesceShufflePartitionsRule.plan(sizes, 1_000, 99);
        assertThat(ranges).hasSizeLessThanOrEqualTo(2);
    }
}
