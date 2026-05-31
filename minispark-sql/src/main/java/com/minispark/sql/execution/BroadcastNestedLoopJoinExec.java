package com.minispark.sql.execution;

import com.minispark.api.MiniSparkContext;
import com.minispark.broadcast.Broadcast;
import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.plan.JoinType;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The strategy for joins with <b>no equi-key</b> — a non-equi predicate like
 * {@code a.lo <= b.x AND b.x < a.hi}, or a {@code CROSS JOIN ... ON <pred>}.
 * One side is collected to the driver and broadcast; each row of the streaming
 * side is looped against the whole broadcast side, evaluating the boolean
 * {@code condition} on the combined {@code left ++ right} row.
 *
 * <p>O(|left| × |right|) comparisons — the inevitable cost when there's no key
 * to hash on. The broadcast keeps it shuffle-free.
 *
 * <p><b>Build-side / join-type support.</b> For INNER, LEFT, LEFT_SEMI,
 * LEFT_ANTI and CROSS the right side is broadcast and the left streams; for
 * RIGHT the left is broadcast and the right streams. FULL non-equi is rejected
 * (it would need cross-partition matched-row tracking on the build side) — use
 * an equi-join for FULL. SEMI/ANTI emit the left columns only.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec.
 */
public final class BroadcastNestedLoopJoinExec implements PhysicalPlan {

    private final JoinType joinType;
    private final StructType schema;
    private final PhysicalPlan left;
    private final PhysicalPlan right;
    private final Expression condition;   // over combined left++right ordinals; null = always-true
    private final int leftWidth;
    private final int rightWidth;
    private final MiniSparkContext sc;

    public BroadcastNestedLoopJoinExec(JoinType joinType, StructType schema,
                                       PhysicalPlan left, PhysicalPlan right,
                                       Expression condition, MiniSparkContext sc) {
        if (joinType == JoinType.FULL) {
            throw new IllegalArgumentException(
                    "BroadcastNestedLoopJoinExec does not support FULL non-equi joins");
        }
        this.joinType = joinType;
        this.schema = schema;
        this.left = left;
        this.right = right;
        this.condition = condition;
        this.leftWidth = left.schema().size();
        this.rightWidth = right.schema().size();
        this.sc = sc;
    }

    @Override public StructType schema() { return schema; }
    @Override public List<PhysicalPlan> children() { return List.of(left, right); }
    @Override public String toString() { return "BroadcastNestedLoopJoinExec " + joinType; }

    @Override
    public RDD<Row> execute() {
        // RIGHT outer streams the right side and broadcasts the left; everything
        // else streams the left and broadcasts the right.
        boolean buildRight = joinType != JoinType.RIGHT;
        PhysicalPlan streamPlan = buildRight ? left : right;
        PhysicalPlan buildPlan = buildRight ? right : left;

        ArrayList<Row> buildRows = new ArrayList<>(buildPlan.execute().collect());
        Broadcast<ArrayList<Row>> bcast = sc.broadcast(buildRows);

        JoinType jt = joinType;
        Expression cond = condition;
        int lw = leftWidth, rw = rightWidth;

        return streamPlan.execute().flatMap((RDD.SerializableFunction<Row, Iterator<Row>>) streamRow -> {
            List<Row> build = bcast.value();
            List<Row> out = new ArrayList<>();
            boolean matched = false;

            for (Row b : build) {
                Row leftRow  = buildRight ? streamRow : b;
                Row rightRow = buildRight ? b : streamRow;
                Row combined = concat(leftRow, rightRow, lw, rw);
                if (cond != null && !Boolean.TRUE.equals(cond.eval(combined))) continue;
                matched = true;
                switch (jt) {
                    case INNER, CROSS, LEFT, RIGHT -> out.add(combined);
                    case LEFT_SEMI -> { out.add(leftRow); }   // left columns only
                    case LEFT_ANTI -> { }                     // suppressed below
                    default -> throw new IllegalStateException("unexpected join type " + jt);
                }
                if (jt == JoinType.LEFT_SEMI || jt == JoinType.LEFT_ANTI) break; // one match decides
            }

            // Unmatched-row handling per join type.
            if (!matched) {
                switch (jt) {
                    case LEFT      -> out.add(concat(streamRow, null, lw, rw)); // streamRow is the left
                    case RIGHT     -> out.add(concat(null, streamRow, lw, rw)); // streamRow is the right
                    case LEFT_ANTI -> out.add(streamRow);                       // no match → keep left
                    default        -> { }
                }
            }
            return out.iterator();
        });
    }

    private static Row concat(Row left, Row right, int lw, int rw) {
        Object[] out = new Object[lw + rw];
        if (left != null) for (int i = 0; i < lw; i++) out[i] = left.get(i);
        if (right != null) for (int i = 0; i < rw; i++) out[lw + i] = right.get(i);
        return new Row(out);
    }
}
