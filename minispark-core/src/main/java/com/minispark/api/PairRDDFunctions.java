package com.minispark.api;

import com.minispark.rdd.CoGroupedRDD;
import com.minispark.rdd.MapPartitionsRDD;
import com.minispark.rdd.RDD;
import com.minispark.rdd.ShuffledRDD;
import com.minispark.shuffle.HashPartitioner;
import com.minispark.shuffle.Partitioner;
import com.minispark.shuffle.RangePartitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Operations available only on {@code RDD<Tuple2<K,V>>}: {@code reduceByKey},
 * {@code groupByKey}, {@code join}, ... All of them introduce a
 * {@link com.minispark.rdd.ShuffleDependency}, so every method here creates a
 * stage boundary.
 *
 * Real Spark equivalent: org.apache.spark.rdd.PairRDDFunctions
 */
public final class PairRDDFunctions<K, V> {

    private final RDD<Tuple2<K, V>> rdd;

    public PairRDDFunctions(RDD<Tuple2<K, V>> rdd) {
        this.rdd = rdd;
    }

    public RDD<Tuple2<K, V>> rdd() { return rdd; }

    public RDD<Tuple2<K, V>> reduceByKey(RDD.SerializableBiFunction<V, V, V> reducer) {
        return reduceByKey(reducer, defaultPartitioner());
    }

    /**
     * Map-side combine + shuffle + reduce-side combine.
     *
     * <p>Why combine on both sides: doing the reduce-by-key partial aggregation
     * on the map side ("combiner") shrinks the shuffle payload — often by orders
     * of magnitude. The same reducer function runs again on the reduce side over
     * the partial results. This is identical to MapReduce's combiner pattern and
     * the whole reason {@code reduceByKey} beats {@code groupByKey} + {@code map}.
     */
    public RDD<Tuple2<K, V>> reduceByKey(RDD.SerializableBiFunction<V, V, V> reducer,
                                         Partitioner partitioner) {
        RDD<Tuple2<K, V>> mapCombined = new MapPartitionsRDD<>(rdd.context(), rdd,
                (ctx, part, it) -> combine(it, reducer));

        ShuffledRDD<K, V> shuffled = new ShuffledRDD<>(rdd.context(), mapCombined, partitioner);

        return new MapPartitionsRDD<>(rdd.context(), shuffled,
                (ctx, part, it) -> combine(it, reducer));
    }

    /**
     * Map-side combine for {@code reduceByKey}-style operations. Goes through
     * {@link com.minispark.memory.ExternalAppendOnlyMap} so the hash table
     * spills to disk when execution memory runs out, instead of OOMing — the
     * single most common cause of OOMs on wide groups in real Spark, now
     * survivable here too.
     *
     * <p>Falls back to a plain in-memory {@link HashMap} when the executor
     * doesn't have a {@link com.minispark.memory.TaskMemoryManager} wired in
     * (e.g. legacy unit-test SparkEnv) — same behaviour as before that path.
     */
    private static <K, V> Iterator<Tuple2<K, V>> combine(Iterator<Tuple2<K, V>> in,
                                                         RDD.SerializableBiFunction<V, V, V> reducer) {
        com.minispark.executor.TaskContext ctx = com.minispark.executor.TaskContext.get();
        com.minispark.memory.TaskMemoryManager tmm = (ctx == null) ? null : ctx.taskMemoryManager();
        if (tmm == null) {
            // Legacy / test path.
            Map<K, V> acc = new HashMap<>();
            while (in.hasNext()) {
                Tuple2<K, V> kv = in.next();
                acc.merge(kv._1(), kv._2(), reducer);
            }
            return acc.entrySet().stream().map(e -> new Tuple2<>(e.getKey(), e.getValue())).iterator();
        }
        com.minispark.memory.ExternalAppendOnlyMap<K, V> map =
                new com.minispark.memory.ExternalAppendOnlyMap<>(
                        tmm, reducer, com.minispark.executor.SparkEnv.get().blockManager());
        while (in.hasNext()) {
            Tuple2<K, V> kv = in.next();
            map.insert(kv._1(), kv._2());
        }
        return map.iterator();
    }

    private Partitioner defaultPartitioner() {
        return new HashPartitioner(rdd.getPartitions().size());
    }

    // ----- groupByKey -----

    /**
     * Group all values for each key. Like {@code reduceByKey(append)} but
     * collected directly into a list, with no map-side combine (every record
     * crosses the shuffle, so prefer {@code reduceByKey} when possible).
     */
    public RDD<Tuple2<K, List<V>>> groupByKey() {
        return groupByKey(defaultPartitioner());
    }

    public RDD<Tuple2<K, List<V>>> groupByKey(Partitioner partitioner) {
        ShuffledRDD<K, V> shuffled = new ShuffledRDD<>(rdd.context(), rdd, partitioner);
        return new MapPartitionsRDD<>(rdd.context(), shuffled, (ctx, part, it) -> {
            Map<K, List<V>> acc = new HashMap<>();
            while (it.hasNext()) {
                Tuple2<K, V> kv = it.next();
                acc.computeIfAbsent(kv._1(), k -> new ArrayList<>()).add(kv._2());
            }
            return acc.entrySet().stream()
                    .map(e -> new Tuple2<>(e.getKey(), e.getValue()))
                    .iterator();
        });
    }

    // ----- cogroup -----

    /**
     * For each key present in either RDD, return the lists of values from each.
     * The shared primitive behind {@link #join} and other paired ops.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <W> RDD<Tuple2<K, Tuple2<List<V>, List<W>>>> cogroup(RDD<Tuple2<K, W>> other) {
        return cogroup(other, defaultPartitioner());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public <W> RDD<Tuple2<K, Tuple2<List<V>, List<W>>>> cogroup(RDD<Tuple2<K, W>> other,
                                                                Partitioner partitioner) {
        CoGroupedRDD<K> cg = new CoGroupedRDD<>(rdd.context(),
                List.of((RDD<? extends Tuple2<K, ?>>) rdd, (RDD<? extends Tuple2<K, ?>>) other),
                partitioner);
        return new MapPartitionsRDD<>(rdd.context(), cg, (ctx, part, it) -> {
            List<Tuple2<K, Tuple2<List<V>, List<W>>>> out = new ArrayList<>();
            while (it.hasNext()) {
                Tuple2<K, List<List<?>>> kgrp = it.next();
                List<V> vs = (List<V>) kgrp._2().get(0);
                List<W> ws = (List<W>) kgrp._2().get(1);
                out.add(new Tuple2<>(kgrp._1(), new Tuple2<>(vs, ws)));
            }
            return out.iterator();
        });
    }

    // ----- join -----

    /**
     * Inner join: for each key present in <i>both</i> RDDs, emit one record per
     * pair of values. Implemented over {@link #cogroup} so the shuffle happens
     * exactly once per parent.
     */
    public <W> RDD<Tuple2<K, Tuple2<V, W>>> join(RDD<Tuple2<K, W>> other) {
        return join(other, defaultPartitioner());
    }

    // ----- sortByKey -----

    /**
     * Globally sort by key: each output partition's keys all sort before the
     * next partition's. Uses a {@link RangePartitioner} sampled from the input
     * for balance, then sorts within each reduce partition.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public RDD<Tuple2<K, V>> sortByKey() {
        return sortByKey(rdd.getPartitions().size());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public RDD<Tuple2<K, V>> sortByKey(int numPartitions) {
        RangePartitioner partitioner = new RangePartitioner<>(numPartitions, (RDD) rdd, 100);
        ShuffledRDD<K, V> shuffled = new ShuffledRDD<>(rdd.context(), rdd, partitioner);
        // Per-partition sort after the shuffle places each key in the right partition.
        return new MapPartitionsRDD<>(rdd.context(), shuffled, (ctx, part, it) -> {
            List<Tuple2<K, V>> buf = new ArrayList<>();
            it.forEachRemaining(buf::add);
            buf.sort((a, b) -> {
                @SuppressWarnings("unchecked") Comparable<Object> ka = (Comparable<Object>) a._1();
                return ka.compareTo(b._1());
            });
            return buf.iterator();
        });
    }

    public <W> RDD<Tuple2<K, Tuple2<V, W>>> join(RDD<Tuple2<K, W>> other, Partitioner partitioner) {
        RDD<Tuple2<K, Tuple2<List<V>, List<W>>>> cg = cogroup(other, partitioner);
        return new MapPartitionsRDD<>(rdd.context(), cg, (ctx, part, it) -> {
            List<Tuple2<K, Tuple2<V, W>>> out = new ArrayList<>();
            while (it.hasNext()) {
                Tuple2<K, Tuple2<List<V>, List<W>>> e = it.next();
                List<V> vs = e._2()._1();
                List<W> ws = e._2()._2();
                if (vs.isEmpty() || ws.isEmpty()) continue;
                for (V v : vs) for (W w : ws) out.add(new Tuple2<>(e._1(), new Tuple2<>(v, w)));
            }
            return out.iterator();
        });
    }
}
