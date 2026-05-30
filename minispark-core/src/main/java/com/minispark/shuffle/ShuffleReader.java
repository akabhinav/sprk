package com.minispark.shuffle;

import com.minispark.api.Tuple2;

import java.util.Iterator;

/**
 * Reduce-side of a shuffle: fetches one reducer's bucket from every map
 * output and returns them as one merged iterator.
 *
 * Real Spark equivalent: org.apache.spark.shuffle.ShuffleReader
 */
public interface ShuffleReader<K, V> {
    Iterator<Tuple2<K, V>> read();
}
