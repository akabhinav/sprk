package com.minispark.scheduler.adaptive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * AQE rule that detects a single reducer-id whose materialised byte size is
 * far larger than its siblings, and splits its read into multiple sub-tasks
 * — one task per slice of map outputs. The downstream stage runs more
 * smaller tasks instead of one straggler.
 *
 * <p><b>Safety regime.</b> Splitting one reducer across multiple output
 * partitions puts records with the same key into different output
 * partitions. That's safe for "stateless" consumers (collect, map+filter,
 * record-at-a-time flatMap) but breaks key-aware consumers (groupByKey,
 * reduceByKey, cogroup) which assume a key is fully present in one
 * partition. The DAGScheduler's adaptive hook already restricts to
 * ShuffledRDDs whose result-stage consumer chain is all narrow, which
 * happens to be the same safety regime this rule needs.
 *
 * <p><b>Trigger.</b> A reducer is skewed when both:
 * <ol>
 *   <li>its total bytes exceed {@code skewedPartitionThresholdInBytes} (an
 *       absolute floor that keeps the rule from firing on small jobs), AND</li>
 *   <li>its total bytes exceed {@code skewedPartitionFactor × median(non-skewed totals)}
 *       — the "this one is much larger than its peers" check.</li>
 * </ol>
 *
 * <p><b>Split.</b> Each skewed reducer is split greedily across map ids so
 * each sub-task gets at most {@code targetBytes} of data. The map sizes per
 * reducer come from {@code MapOutputTracker} (one byte counter per (map,
 * reducer) cell, populated by the writer end of the shuffle).
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.adaptive.OptimizeSkewedJoin
 * (we operate at the RDD level rather than the join level; same detector + planner).
 */
public final class OptimizeSkewedPartitionsRule {

    private OptimizeSkewedPartitionsRule() {}

    /** Result of {@link #detectSkew}: one entry per reducer that should be split. */
    public record SkewedReducer(int reducerId, List<int[]> mapIdSplits) {}

    /**
     * @param perReducerBytes   total bytes per reducer-id (length = numReducers)
     * @param mapSizesPerReducer {@code [reducerId][mapId]} — bytes that map {@code mapId}
     *                          wrote for reducer {@code reducerId}; rows align with
     *                          {@code perReducerBytes}
     * @param thresholdBytes    absolute floor below which a reducer is never skewed
     * @param factor            multiplier over the non-skewed median to flag as skewed
     * @param targetBytes       desired bytes per sub-task after the split
     */
    public static List<SkewedReducer> detectSkew(long[] perReducerBytes,
                                                  long[][] mapSizesPerReducer,
                                                  long thresholdBytes,
                                                  double factor,
                                                  long targetBytes) {
        int n = perReducerBytes.length;
        if (n == 0) return List.of();
        long median = median(perReducerBytes);
        long skewFloor = (long) Math.max(thresholdBytes, factor * median);

        List<SkewedReducer> out = new ArrayList<>();
        for (int r = 0; r < n; r++) {
            if (perReducerBytes[r] <= skewFloor) continue;
            long[] perMap = mapSizesPerReducer[r];
            List<int[]> splits = planMapSplits(perMap, targetBytes);
            // Only useful if we actually split into more than 1 sub-task; otherwise
            // we'd just rewrite the same single-reducer slice and waste effort.
            if (splits.size() > 1) out.add(new SkewedReducer(r, splits));
        }
        return out;
    }

    /**
     * Greedy left-to-right partition of {@code [0, perMap.length)} into ranges
     * whose summed bytes stay at-or-below {@code targetBytes}. Same shape as
     * {@link CoalesceShufflePartitionsRule#plan}, but over map ids instead of
     * reducer ids — and we don't enforce a minimum partition floor here
     * (the caller decides whether the result is worth applying).
     */
    public static List<int[]> planMapSplits(long[] perMap, long targetBytes) {
        int n = perMap.length;
        if (n == 0) return List.of();
        if (targetBytes <= 0) targetBytes = 1;

        List<int[]> ranges = new ArrayList<>();
        int start = 0;
        long running = 0;
        for (int i = 0; i < n; i++) {
            long size = perMap[i];
            if (i > start && running + size > targetBytes) {
                ranges.add(new int[]{start, i});
                start = i;
                running = 0;
            }
            running += size;
        }
        ranges.add(new int[]{start, n});
        return ranges;
    }

    private static long median(long[] sizes) {
        long[] sorted = sizes.clone();
        Arrays.sort(sorted);
        int mid = sorted.length / 2;
        return (sorted.length % 2 == 1)
                ? sorted[mid]
                : (sorted[mid - 1] + sorted[mid]) / 2;
    }
}
