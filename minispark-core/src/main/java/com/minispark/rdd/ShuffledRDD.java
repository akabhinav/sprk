package com.minispark.rdd;

import com.minispark.api.MiniSparkContext;
import com.minispark.api.Tuple2;
import com.minispark.executor.SparkEnv;
import com.minispark.executor.TaskContext;
import com.minispark.shuffle.Partitioner;
import com.minispark.shuffle.ShuffleReader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * An RDD whose elements come from reading one reduce partition off a
 * {@link ShuffleDependency}. Its {@code compute} grabs a {@link ShuffleReader}
 * from the {@link com.minispark.shuffle.ShuffleManager}, and the
 * DAGScheduler will have already arranged for the upstream map stage to
 * have populated those buckets before this RDD's tasks run.
 *
 * Real Spark equivalent: org.apache.spark.rdd.ShuffledRDD
 */
public final class ShuffledRDD<K, V> extends RDD<Tuple2<K, V>> {

    private static final class ShuffleSlice implements Partition {
        private final int idx;
        ShuffleSlice(int idx) { this.idx = idx; }
        public int index() { return idx; }
    }

    private final ShuffleDependency<K, V> dep;
    private final List<Partition> partitions;

    public ShuffledRDD(MiniSparkContext sc, RDD<Tuple2<K, V>> parent, Partitioner partitioner) {
        super(sc);
        this.dep = new ShuffleDependency<>(parent, partitioner, sc.shuffleManager());
        List<Partition> p = new ArrayList<>(partitioner.numPartitions());
        for (int i = 0; i < partitioner.numPartitions(); i++) p.add(new ShuffleSlice(i));
        this.partitions = p;
    }

    @Override public List<Partition> getPartitions() { return partitions; }

    @Override
    public Iterator<Tuple2<K, V>> compute(Partition split, TaskContext ctx) {
        int reduceId = split.index();
        // SparkEnv lookup, not context() — `context()` is transient and is
        // null after the RDD has been deserialized onto an executor.
        ShuffleReader<K, V> reader = SparkEnv.get().shuffleManager()
                .getReader(dep.handle(), reduceId, reduceId + 1);
        return reader.read();
    }

    @Override public List<Dependency<?>> getDependencies() { return List.of(dep); }

    @Override public Partitioner partitioner() { return dep.partitioner(); }

    public ShuffleDependency<K, V> shuffleDep() { return dep; }
}
