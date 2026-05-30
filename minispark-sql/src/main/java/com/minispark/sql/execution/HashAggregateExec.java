package com.minispark.sql.execution;

import com.minispark.api.PairRDDFunctions;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.expr.agg.AggregateFunction;
import com.minispark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Physical hash aggregation. Lowers {@code GROUP BY} to the engine's
 * {@code reduceByKey}, which gives <b>map-side combine</b> for free: each
 * partition folds its rows into per-key aggregate buffers (a narrow map),
 * the shuffle carries one merged buffer per key per partition, and the reduce
 * side merges them. Then a final map evaluates each buffer to its result row
 * (grouping cols ++ aggregate results).
 *
 * <p>The grouping key is wrapped in a {@link GroupKey} with value-based
 * equals/hashCode so it partitions correctly through the hash shuffle.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.aggregate.HashAggregateExec.
 */
public final class HashAggregateExec implements PhysicalPlan {

    private final List<Expression> groupingExprs;
    private final List<AggregateFunction> aggregates;
    private final StructType schema;
    private final PhysicalPlan child;

    public HashAggregateExec(List<Expression> groupingExprs, List<AggregateFunction> aggregates,
                             StructType schema, PhysicalPlan child) {
        this.groupingExprs = groupingExprs;
        this.aggregates = aggregates;
        this.schema = schema;
        this.child = child;
    }

    @Override public StructType schema() { return schema; }
    @Override public List<PhysicalPlan> children() { return List.of(child); }
    @Override public String toString() { return "HashAggregateExec " + schema; }

    /** Hashable, serializable grouping key (a tuple of grouping-column values). */
    static final class GroupKey implements Serializable {
        final Object[] values;
        GroupKey(Object[] values) { this.values = values; }
        @Override public boolean equals(Object o) {
            return o instanceof GroupKey g && java.util.Arrays.equals(values, g.values);
        }
        @Override public int hashCode() { return java.util.Arrays.deepHashCode(values); }
    }

    /** Serializable carrier for the per-group aggregate buffers. */
    static final class Buffers implements Serializable {
        final Object[] buf;
        Buffers(Object[] buf) { this.buf = buf; }
    }

    @Override
    public RDD<Row> execute() {
        List<Expression> groups = groupingExprs;
        List<AggregateFunction> aggs = aggregates;

        // 1. Map each row to (groupKey, initialized+updated single-row buffers).
        RDD<Tuple2<GroupKey, Buffers>> pairs = child.execute().map(
                (RDD.SerializableFunction<Row, Tuple2<GroupKey, Buffers>>) row -> {
                    Object[] key = new Object[groups.size()];
                    for (int i = 0; i < groups.size(); i++) key[i] = groups.get(i).eval(row);
                    Object[] b = new Object[aggs.size()];
                    for (int i = 0; i < aggs.size(); i++) {
                        b[i] = aggs.get(i).update(aggs.get(i).initialize(), row);
                    }
                    return new Tuple2<>(new GroupKey(key), new Buffers(b));
                });

        // 2. reduceByKey merges buffers — map-side combine + shuffle + reduce-side combine.
        RDD<Tuple2<GroupKey, Buffers>> reduced =
                new PairRDDFunctions<>(pairs).reduceByKey(
                        (RDD.SerializableBiFunction<Buffers, Buffers, Buffers>) (x, y) -> {
                            Object[] merged = new Object[aggs.size()];
                            for (int i = 0; i < aggs.size(); i++) {
                                merged[i] = aggs.get(i).merge(x.buf[i], y.buf[i]);
                            }
                            return new Buffers(merged);
                        });

        // 3. Emit one result row per group: grouping cols ++ evaluated aggregates.
        return reduced.map((RDD.SerializableFunction<Tuple2<GroupKey, Buffers>, Row>) kv -> {
            Object[] out = new Object[groups.size() + aggs.size()];
            System.arraycopy(kv._1().values, 0, out, 0, groups.size());
            for (int i = 0; i < aggs.size(); i++) {
                out[groups.size() + i] = aggs.get(i).evaluate(kv._2().buf[i]);
            }
            return new Row(out);
        });
    }
}
