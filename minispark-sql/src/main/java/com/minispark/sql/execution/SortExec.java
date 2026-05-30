package com.minispark.sql.execution;

import com.minispark.api.PairRDDFunctions;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.plan.SortOrder;
import com.minispark.sql.types.StructType;

import java.io.Serializable;
import java.util.List;

/**
 * Physical global sort. Keys each row with a {@link SortKey} (the evaluated
 * ORDER BY values + their directions) and runs the engine's range-partitioned
 * {@code sortByKey}, which gives a <i>total</i> ordering across partitions
 * (range partitioning places key ranges in partition order, then each
 * partition sorts locally). Then drops the key.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.SortExec (+ the
 * range-partitioning exchange a global ORDER BY inserts).
 */
public final class SortExec implements PhysicalPlan {

    private final List<SortOrder> orders;
    private final PhysicalPlan child;

    public SortExec(List<SortOrder> orders, PhysicalPlan child) {
        this.orders = orders;
        this.child = child;
    }

    @Override public StructType schema() { return child.schema(); }
    @Override public List<PhysicalPlan> children() { return List.of(child); }
    @Override public String toString() { return "SortExec " + orders; }

    /** Serializable, Comparable composite sort key: column values + directions. */
    static final class SortKey implements Comparable<SortKey>, Serializable {
        final Object[] values;
        final boolean[] asc;
        SortKey(Object[] values, boolean[] asc) { this.values = values; this.asc = asc; }

        @Override @SuppressWarnings({"unchecked", "rawtypes"})
        public int compareTo(SortKey o) {
            for (int i = 0; i < values.length; i++) {
                Object a = values[i], b = o.values[i];
                int c;
                if (a == null && b == null) c = 0;
                else if (a == null) c = -1;      // nulls first
                else if (b == null) c = 1;
                else if (a instanceof Number && b instanceof Number)
                    c = Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
                else c = ((Comparable) a).compareTo(b);
                if (c != 0) return asc[i] ? c : -c;
            }
            return 0;
        }
    }

    @Override
    public RDD<Row> execute() {
        List<SortOrder> ord = orders;
        boolean[] asc = new boolean[ord.size()];
        for (int i = 0; i < ord.size(); i++) asc[i] = ord.get(i).ascending();

        RDD<Tuple2<SortKey, Row>> keyed = child.execute().map(
                (RDD.SerializableFunction<Row, Tuple2<SortKey, Row>>) row -> {
                    Object[] vals = new Object[ord.size()];
                    for (int i = 0; i < ord.size(); i++) vals[i] = ord.get(i).expr().eval(row);
                    return new Tuple2<>(new SortKey(vals, asc), row);
                });

        RDD<Tuple2<SortKey, Row>> sorted = new PairRDDFunctions<>(keyed).sortByKey();
        return sorted.map((RDD.SerializableFunction<Tuple2<SortKey, Row>, Row>) Tuple2::_2);
    }
}
