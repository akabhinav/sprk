package com.minispark.sql.execution;

import com.minispark.api.MiniSparkContext;
import com.minispark.api.PairRDDFunctions;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.execution.Keys.ValueKey;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.expr.agg.AggregateFunction;
import com.minispark.sql.types.StructType;

import java.io.Serializable;
import java.util.List;

/**
 * Physical hash aggregation. Lowers {@code GROUP BY} to the engine's
 * {@code reduceByKey}, which gives <b>map-side combine</b> for free: each
 * partition folds its rows into per-key aggregate buffers (a narrow map),
 * the shuffle carries one merged buffer per key per partition, and the reduce
 * side merges them. Then a final map evaluates each buffer to its result row
 * (grouping cols ++ aggregate results).
 *
 * <p>The grouping key is a {@link Keys.ValueKey} (normalized so numeric types
 * compare equal across Integer/Long/Double) so it partitions correctly through
 * the hash shuffle.
 *
 * <p><b>Empty global aggregate:</b> a no-GROUP-BY aggregate over an empty input
 * must still return one row (e.g. {@code SELECT count(*)} → 0). reduceByKey over
 * an empty RDD yields nothing, so we detect the empty global case on the driver
 * and emit a single seeded row.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.aggregate.HashAggregateExec.
 */
public final class HashAggregateExec implements PhysicalPlan {

    private final List<Expression> groupingExprs;
    private final List<AggregateFunction> aggregates;
    private final StructType schema;
    private final PhysicalPlan child;
    private final MiniSparkContext sc;

    public HashAggregateExec(List<Expression> groupingExprs, List<AggregateFunction> aggregates,
                             StructType schema, PhysicalPlan child, MiniSparkContext sc) {
        this.groupingExprs = groupingExprs;
        this.aggregates = aggregates;
        this.schema = schema;
        this.child = child;
        this.sc = sc;
    }

    @Override public StructType schema() { return schema; }
    @Override public List<PhysicalPlan> children() { return List.of(child); }
    @Override public String toString() { return "HashAggregateExec " + schema; }

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
        RDD<Tuple2<ValueKey, Buffers>> pairs = child.execute().map(
                (RDD.SerializableFunction<Row, Tuple2<ValueKey, Buffers>>) row -> {
                    Object[] key = new Object[groups.size()];
                    for (int i = 0; i < groups.size(); i++) key[i] = groups.get(i).eval(row);
                    Object[] b = new Object[aggs.size()];
                    for (int i = 0; i < aggs.size(); i++) {
                        b[i] = aggs.get(i).update(aggs.get(i).initialize(), row);
                    }
                    return new Tuple2<>(Keys.groupKey(key), new Buffers(b));
                });

        // 2. reduceByKey merges buffers — map-side combine + shuffle + reduce-side combine.
        RDD<Tuple2<ValueKey, Buffers>> reduced =
                new PairRDDFunctions<>(pairs).reduceByKey(
                        (RDD.SerializableBiFunction<Buffers, Buffers, Buffers>) (x, y) -> {
                            Object[] merged = new Object[aggs.size()];
                            for (int i = 0; i < aggs.size(); i++) {
                                merged[i] = aggs.get(i).merge(x.buf[i], y.buf[i]);
                            }
                            return new Buffers(merged);
                        });

        // 3. Emit one result row per group: grouping cols ++ evaluated aggregates.
        List<Row> rows = reduced.map((RDD.SerializableFunction<Tuple2<ValueKey, Buffers>, Row>) kv -> {
            Object[] out = new Object[groups.size() + aggs.size()];
            System.arraycopy(kv._1().values, 0, out, 0, groups.size());
            for (int i = 0; i < aggs.size(); i++) {
                out[groups.size() + i] = aggs.get(i).evaluate(kv._2().buf[i]);
            }
            return new Row(out);
        }).collect();

        // Global aggregate (no GROUP BY) over an empty input: SQL returns one
        // seeded row (count→0, sum/avg/min/max→null), not zero rows.
        if (rows.isEmpty() && groups.isEmpty()) {
            Object[] out = new Object[aggs.size()];
            for (int i = 0; i < aggs.size(); i++) out[i] = aggs.get(i).evaluate(aggs.get(i).initialize());
            rows = List.of(new Row(out));
        }
        // Re-parallelize the (small, one-row-per-group) result so a parent
        // operator keeps composing on an RDD. Aggregation already collapsed the
        // data, so materializing here is cheap and lets us seed the empty case.
        return sc.parallelize(rows, 1);
    }
}
