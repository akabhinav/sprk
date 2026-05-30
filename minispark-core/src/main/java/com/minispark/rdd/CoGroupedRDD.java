package com.minispark.rdd;

import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.executor.SparkEnv;
import com.minispark.executor.TaskContext;
import com.minispark.shuffle.Partitioner;
import com.minispark.shuffle.ShuffleReader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The shared primitive for {@code cogroup}, {@code join}, and friends. Takes
 * N parent pair RDDs that share the same {@link Partitioner} and, for each
 * key, returns {@code (key, (Iterable<V0>, Iterable<V1>, ...))} grouped from
 * all parents.
 *
 * <p>Each parent contributes one {@link com.minispark.rdd.ShuffleDependency};
 * compute() reads each shuffle for its assigned reduce partition and merges
 * by key. The N independent shuffles all use the same partitioner so a given
 * key lands in the same reduce partition across all parents.
 *
 * Real Spark equivalent: org.apache.spark.rdd.CoGroupedRDD
 */
public final class CoGroupedRDD<K> extends RDD<Tuple2<K, List<List<?>>>> {

    private static final class CoGroupSlice implements Partition {
        private final int idx;
        CoGroupSlice(int idx) { this.idx = idx; }
        public int index() { return idx; }
    }

    private final List<ShuffleDependency<K, ?>> deps;
    private final Partitioner partitioner;
    private final List<Partition> partitions;

    @SuppressWarnings({"rawtypes", "unchecked"})
    public CoGroupedRDD(MiniSparkContext sc, List<RDD<? extends Tuple2<K, ?>>> parents,
                        Partitioner partitioner) {
        super(sc);
        this.partitioner = partitioner;
        this.deps = new ArrayList<>(parents.size());
        for (RDD<? extends Tuple2<K, ?>> parent : parents) {
            // Cast: ShuffleDependency wants a typed RDD<Tuple2<K,V>>; at runtime erasure
            // makes this safe — the values just flow through as Object.
            RDD<Tuple2<K, Object>> typed = (RDD) parent;
            this.deps.add(new ShuffleDependency<>(typed, partitioner, sc.shuffleManager()));
        }
        List<Partition> p = new ArrayList<>(partitioner.numPartitions());
        for (int i = 0; i < partitioner.numPartitions(); i++) p.add(new CoGroupSlice(i));
        this.partitions = p;
    }

    @Override public List<Partition> getPartitions() { return partitions; }

    @Override
    public List<Dependency<?>> getDependencies() {
        List<Dependency<?>> out = new ArrayList<>(deps.size());
        for (ShuffleDependency<K, ?> d : deps) out.add(d);
        return out;
    }

    @Override public Partitioner partitioner() { return partitioner; }

    @Override
    public Iterator<Tuple2<K, List<List<?>>>> compute(Partition split, TaskContext ctx) {
        int reduceId = split.index();
        // For each parent, read this reducer's records and accumulate into a
        // single map: key -> per-parent value list. LinkedHashMap gives a
        // stable iteration order (test-friendly).
        Map<K, List<List<?>>> merged = new LinkedHashMap<>();
        for (int i = 0; i < deps.size(); i++) {
            ShuffleDependency<K, ?> dep = deps.get(i);
            ShuffleReader<K, ?> reader = SparkEnv.get().shuffleManager()
                    .getReader(dep.handle(), reduceId, reduceId + 1);
            Iterator<? extends Tuple2<K, ?>> it = reader.read();
            while (it.hasNext()) {
                Tuple2<K, ?> kv = it.next();
                List<List<?>> slot = merged.computeIfAbsent(kv._1(), k -> {
                    List<List<?>> lists = new ArrayList<>(deps.size());
                    for (int j = 0; j < deps.size(); j++) lists.add(new ArrayList<>());
                    return lists;
                });
                @SuppressWarnings("unchecked")
                List<Object> bucket = (List<Object>) slot.get(i);
                bucket.add(kv._2());
            }
        }
        List<Tuple2<K, List<List<?>>>> out = new ArrayList<>(merged.size());
        for (Map.Entry<K, List<List<?>>> e : merged.entrySet()) {
            out.add(new Tuple2<>(e.getKey(), e.getValue()));
        }
        return out.iterator();
    }
}
