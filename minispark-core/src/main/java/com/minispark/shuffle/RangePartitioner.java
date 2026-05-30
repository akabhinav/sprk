package com.minispark.shuffle;

import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Range-partitions a sorted key space into roughly equal-sized chunks. Used to
 * make {@code sortByKey} produce a totally-ordered output across partitions
 * (partition i's keys all sort before partition i+1's).
 *
 * <p>Algorithm: reservoir-sample each input partition, gather the samples on
 * the driver, sort them, and pick N−1 equally-spaced range boundaries. Lookup
 * is a binary search.
 *
 * Real Spark equivalent: org.apache.spark.RangePartitioner
 */
public final class RangePartitioner<K extends Comparable<K>> extends Partitioner {

    private final int numPartitions;
    private final Comparator<K> cmp;
    private final K[] bounds; // length == numPartitions - 1

    @SuppressWarnings("unchecked")
    public RangePartitioner(int numPartitions, RDD<? extends Tuple2<K, ?>> rdd, int samplesPerPartition) {
        if (numPartitions < 1) throw new IllegalArgumentException("numPartitions must be >= 1");
        this.numPartitions = numPartitions;
        this.cmp = Comparator.naturalOrder();

        if (numPartitions == 1) {
            this.bounds = (K[]) new Comparable[0];
            return;
        }

        // Run a small job that reservoir-samples each partition's keys.
        final int k = Math.max(20, samplesPerPartition);
        @SuppressWarnings("rawtypes")
        RDD typedRdd = (RDD) rdd;
        @SuppressWarnings("unchecked")
        List<List<K>> sampledByPartition = ((RDD<Tuple2<K, ?>>) typedRdd).context().runJob(
                (RDD<Tuple2<K, ?>>) typedRdd,
                (ctx, it) -> reservoirSample(it, k));

        // Flatten and sort.
        List<K> samples = new ArrayList<>();
        for (List<K> p : sampledByPartition) samples.addAll(p);
        samples.sort(cmp);

        if (samples.isEmpty()) {
            // No data at all; partition every key to 0 (any bounds would do).
            this.bounds = (K[]) new Comparable[0];
            return;
        }

        // Pick N-1 equally-spaced sample positions as boundaries.
        K[] b = (K[]) new Comparable[numPartitions - 1];
        for (int i = 0; i < numPartitions - 1; i++) {
            int idx = (int) ((long) (i + 1) * samples.size() / numPartitions);
            if (idx >= samples.size()) idx = samples.size() - 1;
            b[i] = samples.get(idx);
        }
        this.bounds = b;
    }

    /** Standard reservoir sampling: each item has equal probability of ending up in the sample. */
    private static <K> List<K> reservoirSample(Iterator<? extends Tuple2<K, ?>> it, int k) {
        List<K> reservoir = new ArrayList<>(k);
        Random rnd = new Random();
        long seen = 0;
        while (it.hasNext()) {
            K key = it.next()._1();
            if (reservoir.size() < k) reservoir.add(key);
            else {
                long j = (rnd.nextLong() & Long.MAX_VALUE) % (seen + 1);
                if (j < k) reservoir.set((int) j, key);
            }
            seen++;
        }
        return reservoir;
    }

    @Override public int numPartitions() { return numPartitions; }

    @Override
    @SuppressWarnings("unchecked")
    public int getPartition(Object key) {
        if (bounds.length == 0) return 0;
        K k = (K) key;
        int pos = Arrays.binarySearch(bounds, k, cmp);
        if (pos < 0) pos = -pos - 1;        // insertion point
        return Math.min(pos, numPartitions - 1);
    }
}
