package com.minispark.sql.execution;

import com.minispark.api.PairRDDFunctions;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.plan.JoinType;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Physical equi-join via the engine's {@code cogroup}: both sides are keyed by
 * the join columns and shuffled so matching keys land in the same partition;
 * each key then yields the cartesian product of its left and right rows. Outer
 * joins emit null-padded rows when one side is empty for a key.
 *
 * <p>Cogroup is the most uniform way to express all four join types — INNER,
 * LEFT, RIGHT, FULL fall out of which side's row-list we iterate and how we
 * handle the empty side. Real Spark picks among broadcast-hash / shuffle-hash /
 * sort-merge by cost; we use one shuffle-based strategy.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.joins.ShuffledHashJoinExec.
 */
public final class ShuffledHashJoinExec implements PhysicalPlan {

    private final List<Expression> leftKeys;
    private final List<Expression> rightKeys;
    private final JoinType joinType;
    private final StructType schema;
    private final PhysicalPlan left;
    private final PhysicalPlan right;
    private final int leftWidth;
    private final int rightWidth;

    public ShuffledHashJoinExec(List<Expression> leftKeys, List<Expression> rightKeys,
                                JoinType joinType, StructType schema,
                                PhysicalPlan left, PhysicalPlan right) {
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.joinType = joinType;
        this.schema = schema;
        this.left = left;
        this.right = right;
        this.leftWidth = left.schema().size();
        this.rightWidth = right.schema().size();
    }

    @Override public StructType schema() { return schema; }
    @Override public List<PhysicalPlan> children() { return List.of(left, right); }
    @Override public String toString() { return "ShuffledHashJoinExec " + joinType + " " + schema; }

    @Override
    public RDD<Row> execute() {
        List<Expression> lk = leftKeys, rk = rightKeys;

        RDD<Tuple2<Keys.ValueKey, Row>> leftPairs = left.execute().map(
                (RDD.SerializableFunction<Row, Tuple2<Keys.ValueKey, Row>>) row -> new Tuple2<>(key(lk, row), row));
        RDD<Tuple2<Keys.ValueKey, Row>> rightPairs = right.execute().map(
                (RDD.SerializableFunction<Row, Tuple2<Keys.ValueKey, Row>>) row -> new Tuple2<>(key(rk, row), row));

        // cogroup: one entry per key with (allLeftRows, allRightRows). Null-key
        // rows get a unique key (see Keys.joinKey), so they never match another
        // row but still flow through here for OUTER joins to null-pad.
        RDD<Tuple2<Keys.ValueKey, Tuple2<List<Row>, List<Row>>>> cogrouped =
                new PairRDDFunctions<>(leftPairs).cogroup(rightPairs);

        JoinType jt = joinType;
        int lw = leftWidth, rw = rightWidth;

        return cogrouped.flatMap(
                (RDD.SerializableFunction<Tuple2<Keys.ValueKey, Tuple2<List<Row>, List<Row>>>,
                        java.util.Iterator<Row>>) entry -> {
            List<Row> lefts = entry._2()._1();
            List<Row> rights = entry._2()._2();
            List<Row> out = new ArrayList<>();

            boolean emitUnmatchedLeft = jt == JoinType.LEFT || jt == JoinType.FULL;
            boolean emitUnmatchedRight = jt == JoinType.RIGHT || jt == JoinType.FULL;

            if (!lefts.isEmpty() && !rights.isEmpty()) {
                for (Row l : lefts) for (Row r : rights) out.add(concat(l, r, lw, rw));
            } else if (lefts.isEmpty() && emitUnmatchedRight) {
                for (Row r : rights) out.add(concat(null, r, lw, rw));
            } else if (rights.isEmpty() && emitUnmatchedLeft) {
                for (Row l : lefts) out.add(concat(l, null, lw, rw));
            }
            return out.iterator();
        });
    }

    private static Keys.ValueKey key(List<Expression> keys, Row row) {
        Object[] k = new Object[keys.size()];
        for (int i = 0; i < keys.size(); i++) k[i] = keys.get(i).eval(row);
        return Keys.joinKey(k);
    }

    /** Left columns ++ right columns; a null side becomes all-null padding. */
    private static Row concat(Row left, Row right, int lw, int rw) {
        Object[] out = new Object[lw + rw];
        if (left != null) for (int i = 0; i < lw; i++) out[i] = left.get(i);
        if (right != null) for (int i = 0; i < rw; i++) out[lw + i] = right.get(i);
        return new Row(out);
    }
}
