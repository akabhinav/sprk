package com.minispark.rdd;

import com.minispark.api.Tuple2;
import com.minispark.shuffle.Partitioner;
import com.minispark.shuffle.ShuffleHandle;
import com.minispark.shuffle.ShuffleManager;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A wide dependency: each child partition needs data from <i>all</i> parent
 * partitions, grouped by key.
 *
 * <p>This is the only kind of dependency that forces a stage boundary. The
 * scheduler can pipeline narrow deps because, by definition, the parent
 * partition that feeds a child is already known by index. With a shuffle,
 * the child must wait until every parent's records have been bucketed by
 * partitioner and persisted — only then can the child read its bucket.
 *
 * Real Spark equivalent: org.apache.spark.ShuffleDependency
 */
public final class ShuffleDependency<K, V> extends Dependency<Tuple2<K, V>> {
    private static final AtomicInteger SHUFFLE_ID_GEN = new AtomicInteger();

    private final RDD<Tuple2<K, V>> rdd;
    private final Partitioner partitioner;
    private final int shuffleId;
    private final ShuffleHandle handle;

    public ShuffleDependency(RDD<Tuple2<K, V>> rdd,
                             Partitioner partitioner,
                             ShuffleManager shuffleManager) {
        this.rdd = rdd;
        this.partitioner = partitioner;
        this.shuffleId = SHUFFLE_ID_GEN.incrementAndGet();
        this.handle = shuffleManager.registerShuffle(shuffleId, rdd.getPartitions().size(), partitioner);
    }

    @Override public RDD<Tuple2<K, V>> rdd() { return rdd; }
    public Partitioner partitioner() { return partitioner; }
    public int shuffleId() { return shuffleId; }
    public ShuffleHandle handle() { return handle; }
}
