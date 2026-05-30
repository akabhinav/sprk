package com.minispark.accumulator;

import java.io.Serializable;

/**
 * Combiner for accumulator updates. The driver holds the master value and
 * merges executor-sent deltas into it via {@link #addInPlace}.
 *
 * Real Spark equivalent: org.apache.spark.AccumulatorV2.merge / AccumulatorParam (legacy).
 */
public interface AccumulatorParam<T> extends Serializable {
    T zero();
    T addInPlace(T a, T b);

    AccumulatorParam<Long> LONG = new AccumulatorParam<>() {
        @Override public Long zero() { return 0L; }
        @Override public Long addInPlace(Long a, Long b) { return a + b; }
    };

    AccumulatorParam<Double> DOUBLE = new AccumulatorParam<>() {
        @Override public Double zero() { return 0.0; }
        @Override public Double addInPlace(Double a, Double b) { return a + b; }
    };
}
