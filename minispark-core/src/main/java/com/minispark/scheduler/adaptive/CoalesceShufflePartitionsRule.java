package com.minispark.scheduler.adaptive;

import java.util.ArrayList;
import java.util.List;

/**
 * AQE-style rule that collapses many small post-shuffle partitions into a
 * smaller number of fatter ones, using actual map-output byte sizes reported
 * by the just-completed {@code ShuffleMapStage}.
 *
 * <p><b>Why it matters.</b> The user picked the shuffle partition count
 * (e.g. 200) when writing the job, before knowing how much data each reducer
 * would actually see. If the real data is small, 200 tiny tasks adds
 * scheduling overhead that dominates the work. AQE waits for the map stage
 * to finish, sees the true per-reducer sizes, and groups contiguous reducers
 * whose summed bytes are under a target — so the downstream stage runs
 * fewer, bigger tasks.
 *
 * <p><b>Algorithm.</b> Greedy left-to-right scan over reducer ids, opening a
 * new range whenever appending the next reducer would push the running total
 * above {@code targetBytes}. Adjacent-only — never reorders reducers, which
 * preserves any range-partitioner ordering downstream code may depend on.
 *
 * <p><b>What this does NOT do (vs real Spark AQE).</b> No skew-join split, no
 * sort-merge → broadcast demotion. Coalesce is the simplest, highest-value AQE
 * rule and the natural first step.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.adaptive.CoalesceShufflePartitions
 */
public final class CoalesceShufflePartitionsRule {

    private CoalesceShufflePartitionsRule() {}

    /**
     * @param perReducerBytes per-reducer total byte counts (length = original
     *                        post-shuffle partition count)
     * @param targetBytes     desired output partition size; each coalesced
     *                        range stays at-or-below this once it contains
     *                        more than one reducer
     * @param minPartitions   never coalesce below this many output partitions
     *                        (so a one-task plan only happens if that's the
     *                        floor too)
     * @return list of {@code [startReducerId, endReducerId)} ranges; concat'd,
     *         they cover {@code [0, perReducerBytes.length)} with no overlaps
     */
    public static List<int[]> plan(long[] perReducerBytes, long targetBytes, int minPartitions) {
        int n = perReducerBytes.length;
        if (n == 0) return List.of();
        if (targetBytes <= 0) targetBytes = 1; // degenerate but harmless
        if (minPartitions < 1) minPartitions = 1;
        if (minPartitions > n) minPartitions = n;

        List<int[]> ranges = new ArrayList<>();
        int start = 0;
        long running = 0;
        for (int i = 0; i < n; i++) {
            long size = perReducerBytes[i];
            // If we already have at least one reducer in the current range AND
            // adding this one would push us strictly past the target, cut here.
            // The ">=" on running guards against the very first reducer being
            // larger than the target (we still emit it as its own range, since
            // splitting one reducer would require a different mechanism).
            if (i > start && running + size > targetBytes) {
                ranges.add(new int[]{start, i});
                start = i;
                running = 0;
            }
            running += size;
        }
        // Flush the trailing range.
        ranges.add(new int[]{start, n});

        // Enforce the minimum partition floor by un-merging from the right.
        // Splitting on a per-reducer boundary is always safe — every range
        // contains at least one reducer, so we just walk back and peel single
        // reducers off the largest tail ranges until we hit the floor.
        while (ranges.size() < minPartitions) {
            int idx = pickSplittable(ranges);
            if (idx < 0) break; // every range is already a single reducer; can't grow further
            int[] r = ranges.get(idx);
            ranges.set(idx, new int[]{r[0], r[1] - 1});
            ranges.add(idx + 1, new int[]{r[1] - 1, r[1]});
        }
        return ranges;
    }

    /** Index of the widest range, or -1 if every range is already a single reducer. */
    private static int pickSplittable(List<int[]> ranges) {
        int best = -1;
        int bestWidth = 1;
        for (int i = 0; i < ranges.size(); i++) {
            int[] r = ranges.get(i);
            int width = r[1] - r[0];
            if (width > bestWidth) {
                best = i;
                bestWidth = width;
            }
        }
        return best;
    }
}
