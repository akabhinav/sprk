package com.minispark.api;

import com.minispark.rdd.MapPartitionsRDD;
import com.minispark.rdd.RDD;
import com.minispark.rdd.ShuffledRDD;
import com.minispark.shuffle.HashPartitioner;
import com.minispark.shuffle.Partitioner;

import java.util.HashMap;
import java.util.Iterator;
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

    private static <K, V> Iterator<Tuple2<K, V>> combine(Iterator<Tuple2<K, V>> in,
                                                         RDD.SerializableBiFunction<V, V, V> reducer) {
        Map<K, V> acc = new HashMap<>();
        while (in.hasNext()) {
            Tuple2<K, V> kv = in.next();
            acc.merge(kv._1(), kv._2(), reducer);
        }
        return acc.entrySet().stream()
                .map(e -> new Tuple2<>(e.getKey(), e.getValue()))
                .iterator();
    }

    private Partitioner defaultPartitioner() {
        return new HashPartitioner(rdd.getPartitions().size());
    }
}
