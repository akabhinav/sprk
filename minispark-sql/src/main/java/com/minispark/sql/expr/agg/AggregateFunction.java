package com.minispark.sql.expr.agg;

import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

import java.io.Serializable;

/**
 * An aggregate over a group of rows, expressed as a buffer that is initialized
 * once, updated per input row, merged across partitions, and finally evaluated
 * to a single value. This four-method shape is exactly what lets a physical
 * aggregate do <b>map-side combine</b>: each partition folds its rows into a
 * partial buffer (update), the shuffle carries only one buffer per key per
 * partition, and the reduce side combines them (merge) — the same win as
 * {@code reduceByKey} over {@code groupByKey}.
 *
 * <p>Buffers must be {@link Serializable} because they cross the shuffle.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.expressions.aggregate
 *                        .{DeclarativeAggregate / AggregateFunction}.
 */
public interface AggregateFunction extends Serializable {

    /** Fresh accumulator for a new group. */
    Object initialize();

    /** Fold one input row into the accumulator; returns the new accumulator. */
    Object update(Object buffer, Row input);

    /** Combine two partial accumulators (reduce-side / cross-partition). */
    Object merge(Object a, Object b);

    /** Final value from a fully-merged accumulator. */
    Object evaluate(Object buffer);

    /** Result column type, given the schema of the input rows. */
    DataType resultType(StructType inputSchema);

    /** Output column name (e.g. {@code "sum(age)"}). */
    String name();

    /**
     * Return a copy with the child expression rebound against {@code schema} via
     * {@code binder} (the analyzer's UnresolvedAttribute → BoundReference pass).
     * {@code count(*)} has no child and returns itself.
     */
    default AggregateFunction bind(StructType schema,
            java.util.function.BiFunction<com.minispark.sql.expr.Expression, StructType,
                    com.minispark.sql.expr.Expression> binder) {
        return this;
    }
}
