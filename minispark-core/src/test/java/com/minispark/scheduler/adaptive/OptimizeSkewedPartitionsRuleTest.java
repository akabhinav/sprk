package com.minispark.scheduler.adaptive;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the skew detector + per-map split planner.
 */
final class OptimizeSkewedPartitionsRuleTest {

    @Test
    void no_skew_returns_empty() {
        long[] perReducer = {100, 110, 95, 105, 100};
        long[][] perCell = new long[5][2];
        List<OptimizeSkewedPartitionsRule.SkewedReducer> out =
                OptimizeSkewedPartitionsRule.detectSkew(perReducer, perCell, 10, 5.0, 50);
        assertThat(out).isEmpty();
    }

    @Test
    void single_skewed_reducer_is_detected_and_split() {
        // Reducer 2 carries 10000 bytes; the median of (100,100,10000,100,100) is 100;
        // 10000 > max(threshold=500, 5×100=500) → skewed.
        long[] perReducer = {100, 100, 10000, 100, 100};
        // Per-map sizes for reducer 2: spread roughly evenly across 4 maps.
        long[][] perCell = new long[5][4];
        perCell[2] = new long[]{2500, 2500, 2500, 2500};
        // target = 6000 → greedy fits two maps (2500+2500=5000≤6000) then cuts:
        //   [0]=2500 → run=2500. [1]=2500 → 2500+2500=5000≤6000 → append, run=5000.
        //   [2]=2500 → 5000+2500=7500>6000 → cut. ranges=[{0,2}]. run=2500.
        //   [3]=2500 → 2500+2500=5000≤6000 → append, run=5000.
        // Flush trailing [2,4) → 2 ranges total.
        List<OptimizeSkewedPartitionsRule.SkewedReducer> out =
                OptimizeSkewedPartitionsRule.detectSkew(perReducer, perCell, 500, 5.0, 6000);
        assertThat(out).hasSize(1);
        OptimizeSkewedPartitionsRule.SkewedReducer s = out.get(0);
        assertThat(s.reducerId()).isEqualTo(2);
        assertThat(s.mapIdSplits()).hasSize(2);
        assertThat(s.mapIdSplits().get(0)).containsExactly(0, 2);
        assertThat(s.mapIdSplits().get(1)).containsExactly(2, 4);
    }

    @Test
    void absolute_threshold_floor_protects_small_jobs() {
        // Reducer 2 is 10× the median, but its absolute size (50 bytes) is
        // still tiny. The absolute floor (threshold=10000) keeps the rule
        // from firing on a job that's just small overall.
        long[] perReducer = {5, 5, 50, 5, 5};
        long[][] perCell = new long[5][2];
        perCell[2] = new long[]{25, 25};
        List<OptimizeSkewedPartitionsRule.SkewedReducer> out =
                OptimizeSkewedPartitionsRule.detectSkew(perReducer, perCell, 10000, 5.0, 20);
        assertThat(out).isEmpty();
    }

    @Test
    void factor_floor_prevents_firing_on_even_load() {
        // All reducers similar size; even the "biggest" (110) is well within
        // 5× the median (100). Should not fire.
        long[] perReducer = {100, 110, 100, 90, 100};
        long[][] perCell = new long[5][2];
        for (int i = 0; i < 5; i++) perCell[i] = new long[]{perReducer[i] / 2, perReducer[i] / 2};
        List<OptimizeSkewedPartitionsRule.SkewedReducer> out =
                OptimizeSkewedPartitionsRule.detectSkew(perReducer, perCell, 10, 5.0, 50);
        assertThat(out).isEmpty();
    }

    @Test
    void split_not_emitted_when_single_subtask_would_result() {
        // Reducer is skewed by size, but each map's contribution is tiny — the
        // greedy planner would emit a single range, which gives no benefit.
        // Don't report it as skewed.
        long[] perReducer = {100, 100, 10000, 100, 100};
        long[][] perCell = new long[5][4];
        perCell[2] = new long[]{2500, 2500, 2500, 2500};
        // Target is huge → all maps fit in one range.
        List<OptimizeSkewedPartitionsRule.SkewedReducer> out =
                OptimizeSkewedPartitionsRule.detectSkew(perReducer, perCell, 500, 5.0, 1_000_000);
        assertThat(out).isEmpty();
    }

    @Test
    void plan_map_splits_packs_greedily_left_to_right() {
        long[] perMap = {30, 30, 10, 50, 60, 20, 5};
        List<int[]> ranges = OptimizeSkewedPartitionsRule.planMapSplits(perMap, 100);
        // Same algorithm as CoalesceShufflePartitionsRule.plan, no min-floor.
        assertThat(ranges).hasSize(3);
        assertThat(ranges.get(0)).containsExactly(0, 3);
        assertThat(ranges.get(1)).containsExactly(3, 4);
        assertThat(ranges.get(2)).containsExactly(4, 7);
    }

    @Test
    void empty_per_map_returns_empty() {
        assertThat(OptimizeSkewedPartitionsRule.planMapSplits(new long[0], 100)).isEmpty();
    }
}
