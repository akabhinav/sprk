package com.minispark.sql;

import com.minispark.sql.expr.agg.AggregateFunction;
import com.minispark.sql.expr.agg.Aggregates;
import com.minispark.sql.types.StructType;

/**
 * Static factory for aggregate functions used in {@code df.groupBy(...).agg(...)}.
 * The {@code StructType}-needing aggregates (sum/min/max) are constructed
 * against a placeholder schema at build time and re-bound by the analyzer, so
 * callers don't pass a schema here. Mirrors {@code org.apache.spark.sql.functions}.
 */
public final class functions {

    private functions() {}

    // Empty schema is fine at build time: the analyzer rebinds against the real
    // input schema (and Sum/MinMax recompute their result type then).
    private static final StructType EMPTY = StructType.of();

    public static AggregateFunction count() { return Aggregates.Count.star(); }
    public static AggregateFunction count(Column c) { return new Aggregates.Count(c.expr()); }
    public static AggregateFunction sum(Column c) { return new Aggregates.Sum(c.expr(), EMPTY); }
    public static AggregateFunction avg(Column c) { return new Aggregates.Avg(c.expr()); }
    public static AggregateFunction min(Column c) { return new Aggregates.MinMax(c.expr(), true, EMPTY); }
    public static AggregateFunction max(Column c) { return new Aggregates.MinMax(c.expr(), false, EMPTY); }
}
