package com.minispark.sql.execution;

import com.minispark.api.PairRDDFunctions;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * Physical whole-row deduplication. Keys each row by itself (Row already has
 * value-based equals/hashCode) and {@code reduceByKey} keeps one per key — the
 * same shuffle path as a GROUP BY on every column, which is exactly how Spark
 * implements DISTINCT.
 *
 * Real Spark equivalent: the Aggregate-based Distinct + HashAggregateExec.
 */
public final class DistinctExec implements PhysicalPlan {

    private final PhysicalPlan child;

    public DistinctExec(PhysicalPlan child) { this.child = child; }

    @Override public StructType schema() { return child.schema(); }
    @Override public List<PhysicalPlan> children() { return List.of(child); }
    @Override public String toString() { return "DistinctExec"; }

    @Override
    public RDD<Row> execute() {
        RDD<Tuple2<Row, Row>> keyed = child.execute().map(
                (RDD.SerializableFunction<Row, Tuple2<Row, Row>>) row -> new Tuple2<>(row, row));
        RDD<Tuple2<Row, Row>> reduced = new PairRDDFunctions<>(keyed).reduceByKey(
                (RDD.SerializableBiFunction<Row, Row, Row>) (a, b) -> a);
        return reduced.map((RDD.SerializableFunction<Tuple2<Row, Row>, Row>) Tuple2::_2);
    }
}
