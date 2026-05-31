package com.minispark.sql.execution;

import com.minispark.api.MiniSparkContext;
import com.minispark.broadcast.Broadcast;
import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Cartesian product: every left row paired with every right row, with no join
 * condition. Lowered from an unconditional {@code CROSS JOIN} (or an
 * equi-key-less {@code INNER} join). The right side is collected to the driver
 * and broadcast; each left partition then streams out {@code |right|} rows per
 * input row — no shuffle.
 *
 * <p>The output cardinality is {@code |left| × |right|}, so this is the operator
 * the planner reaches for only when there's genuinely no equi-key to hash on.
 * A non-equi <i>condition</i> instead routes to
 * {@link BroadcastNestedLoopJoinExec}, which filters pairs as it emits them.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.joins.CartesianProductExec.
 */
public final class CartesianProductExec implements PhysicalPlan {

    private final StructType schema;
    private final PhysicalPlan left;
    private final PhysicalPlan right;
    private final int leftWidth;
    private final int rightWidth;
    private final MiniSparkContext sc;

    public CartesianProductExec(StructType schema, PhysicalPlan left, PhysicalPlan right,
                                MiniSparkContext sc) {
        this.schema = schema;
        this.left = left;
        this.right = right;
        this.leftWidth = left.schema().size();
        this.rightWidth = right.schema().size();
        this.sc = sc;
    }

    @Override public StructType schema() { return schema; }
    @Override public List<PhysicalPlan> children() { return List.of(left, right); }
    @Override public String toString() { return "CartesianProductExec " + schema; }

    @Override
    public RDD<Row> execute() {
        ArrayList<Row> rightRows = new ArrayList<>(right.execute().collect());
        Broadcast<ArrayList<Row>> bcast = sc.broadcast(rightRows);
        int lw = leftWidth, rw = rightWidth;
        return left.execute().flatMap((RDD.SerializableFunction<Row, Iterator<Row>>) l -> {
            List<Row> others = bcast.value();
            List<Row> out = new ArrayList<>(others.size());
            for (Row r : others) out.add(concat(l, r, lw, rw));
            return out.iterator();
        });
    }

    static Row concat(Row left, Row right, int lw, int rw) {
        Object[] out = new Object[lw + rw];
        if (left != null) for (int i = 0; i < lw; i++) out[i] = left.get(i);
        if (right != null) for (int i = 0; i < rw; i++) out[lw + i] = right.get(i);
        return new Row(out);
    }
}
