package com.minispark.sql.execution;

import com.minispark.api.MiniSparkContext;
import com.minispark.broadcast.Broadcast;
import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.plan.JoinType;
import com.minispark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Map-only equi-join: one side is collected to the driver, packed into a
 * hash-multimap keyed by the join columns, and shipped to every executor as a
 * {@link Broadcast}. The streaming side flows through unshuffled; each task
 * probes the broadcast map per row.
 *
 * <p><b>When the planner picks it.</b> The user attached a {@code broadcast()}
 * hint to the small side, or the side is a tiny {@code LocalRelation} that
 * fits under the auto-broadcast threshold. The big saving vs.
 * {@link ShuffledHashJoinExec}: no shuffle at all on the streaming side, and
 * the build side is shuffled only as "collect + one network hop per executor"
 * rather than a full bucketise-by-key.
 *
 * <p><b>Limitations.</b> FULL OUTER joins need to track which build-side rows
 * were matched across all executors to emit the unmatched ones at the end —
 * that's only correct with a follow-up reduce, defeating the purpose. We
 * support INNER, LEFT, and RIGHT only; the planner falls back to
 * {@link ShuffledHashJoinExec} for FULL.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.joins.BroadcastHashJoinExec.
 */
public final class BroadcastHashJoinExec implements PhysicalPlan {

    private final List<Expression> leftKeys;
    private final List<Expression> rightKeys;
    private final JoinType joinType;
    private final StructType schema;
    private final PhysicalPlan left;
    private final PhysicalPlan right;
    private final boolean buildIsLeft; // which side gets broadcast
    private final int leftWidth;
    private final int rightWidth;
    private final MiniSparkContext sc;

    public BroadcastHashJoinExec(List<Expression> leftKeys, List<Expression> rightKeys,
                                 JoinType joinType, StructType schema,
                                 PhysicalPlan left, PhysicalPlan right,
                                 boolean buildIsLeft, MiniSparkContext sc) {
        if (joinType == JoinType.FULL) {
            throw new IllegalArgumentException(
                    "BroadcastHashJoinExec does not support FULL OUTER; use ShuffledHashJoinExec");
        }
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.joinType = joinType;
        this.schema = schema;
        this.left = left;
        this.right = right;
        this.buildIsLeft = buildIsLeft;
        this.leftWidth = left.schema().size();
        this.rightWidth = right.schema().size();
        this.sc = sc;
    }

    @Override public StructType schema() { return schema; }
    @Override public List<PhysicalPlan> children() { return List.of(left, right); }
    @Override public String toString() {
        return "BroadcastHashJoinExec " + joinType + " build=" + (buildIsLeft ? "LEFT" : "RIGHT");
    }

    @Override
    public RDD<Row> execute() {
        // 1) Materialise the build side to the driver.
        PhysicalPlan buildPlan = buildIsLeft ? left : right;
        PhysicalPlan streamPlan = buildIsLeft ? right : left;
        List<Expression> buildKeys = buildIsLeft ? leftKeys : rightKeys;
        List<Expression> streamKeys = buildIsLeft ? rightKeys : leftKeys;

        List<Row> buildRows = buildPlan.execute().collect();

        // 2) Pack into a hash-multimap. Use the SQL-correct join key (null keys
        //    become unique nonces, so they match nothing — same as the shuffled
        //    join — even though we're not going through a shuffle).
        HashMultimap built = new HashMultimap();
        for (Row r : buildRows) built.add(joinKey(buildKeys, r), r);

        // 3) Broadcast it. Each executor pulls the map once and caches per JVM.
        Broadcast<HashMultimap> bcast = sc.broadcast(built);

        // 4) Probe per streaming row. Capture local primitives — `this` must
        //    not be captured because PhysicalPlan is not Serializable.
        boolean buildLeft = this.buildIsLeft;
        JoinType jt = this.joinType;
        int lw = this.leftWidth;
        int rw = this.rightWidth;
        List<Expression> sk = streamKeys;

        return streamPlan.execute().flatMap(
                (RDD.SerializableFunction<Row, Iterator<Row>>) streamRow -> {
            HashMultimap map = bcast.value();
            Keys.ValueKey probeKey = joinKey(sk, streamRow);
            List<Row> matches = map.get(probeKey); // may be empty
            List<Row> out = new ArrayList<>();

            boolean streamSideIsOuter =
                    (buildLeft && jt == JoinType.RIGHT) ||
                    (!buildLeft && jt == JoinType.LEFT);
            // Note: LEFT with buildLeft=true would require remembering which
            // build rows matched, which a map-only operator can't. The planner
            // only sets buildIsLeft=true when the JoinType allows it (see
            // SparkPlanner.canBuildLeft). Same mirror for RIGHT.

            if (!matches.isEmpty()) {
                for (Row b : matches) {
                    Row leftRow  = buildLeft ? b : streamRow;
                    Row rightRow = buildLeft ? streamRow : b;
                    out.add(concat(leftRow, rightRow, lw, rw));
                }
            } else if (streamSideIsOuter) {
                // Stream row had no match on the build side and the join type
                // wants unmatched stream rows preserved with NULL padding.
                Row leftRow  = buildLeft ? null : streamRow;
                Row rightRow = buildLeft ? streamRow : null;
                out.add(concat(leftRow, rightRow, lw, rw));
            }
            return out.iterator();
        });
    }

    /** Whether a given JoinType permits broadcasting the LEFT side (instead of the right). */
    public static boolean canBuildLeft(JoinType jt) {
        // To build the LEFT side we must not need to preserve unmatched LEFT rows
        // we'll never see on probe — i.e. INNER and RIGHT are safe; LEFT/FULL are not.
        return jt == JoinType.INNER || jt == JoinType.RIGHT;
    }

    /** Whether a given JoinType permits broadcasting the RIGHT side. Symmetric. */
    public static boolean canBuildRight(JoinType jt) {
        return jt == JoinType.INNER || jt == JoinType.LEFT;
    }

    private static Keys.ValueKey joinKey(List<Expression> keys, Row row) {
        Object[] k = new Object[keys.size()];
        for (int i = 0; i < keys.size(); i++) k[i] = keys.get(i).eval(row);
        return Keys.joinKey(k);
    }

    private static Row concat(Row left, Row right, int lw, int rw) {
        Object[] out = new Object[lw + rw];
        if (left != null) for (int i = 0; i < lw; i++) out[i] = left.get(i);
        if (right != null) for (int i = 0; i < rw; i++) out[lw + i] = right.get(i);
        return new Row(out);
    }

    /** Tiny serializable multimap used as the broadcast payload. */
    static final class HashMultimap implements Serializable {
        private final Map<Keys.ValueKey, List<Row>> backing = new HashMap<>();
        void add(Keys.ValueKey k, Row r) {
            backing.computeIfAbsent(k, x -> new ArrayList<>()).add(r);
        }
        List<Row> get(Keys.ValueKey k) {
            List<Row> v = backing.get(k);
            return v == null ? List.of() : v;
        }
    }
}
