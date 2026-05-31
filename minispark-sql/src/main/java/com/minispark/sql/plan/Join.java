package com.minispark.sql.plan;

import com.minispark.sql.expr.Expression;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * A join of two child plans. Two flavours coexist:
 * <ul>
 *   <li><b>Equi-join</b> — {@code leftKeys[i] == rightKeys[i]}; lowered to a
 *       hash/sort-merge/broadcast strategy.</li>
 *   <li><b>Non-equi / cross join</b> — {@code leftKeys} empty, with an optional
 *       boolean {@code condition} over both sides' columns; lowered to a
 *       cartesian-product or broadcast-nested-loop strategy.</li>
 * </ul>
 *
 * <p>Output schema is the left columns followed by the right columns (outer
 * sides made nullable), <i>except</i> {@code LEFT_SEMI}/{@code LEFT_ANTI},
 * which output the left columns only.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.plans.logical.Join.
 */
public final class Join implements LogicalPlan {

    private final LogicalPlan left;
    private final LogicalPlan right;
    private final List<Expression> leftKeys;
    private final List<Expression> rightKeys;
    private final JoinType joinType;
    // Optional extra boolean predicate over the combined (left ++ right) row.
    // Used by the non-equi / cross path; null for a pure equi-join.
    private final Expression condition;

    public Join(LogicalPlan left, LogicalPlan right,
                List<Expression> leftKeys, List<Expression> rightKeys, JoinType joinType) {
        this(left, right, leftKeys, rightKeys, joinType, null);
    }

    public Join(LogicalPlan left, LogicalPlan right,
                List<Expression> leftKeys, List<Expression> rightKeys,
                JoinType joinType, Expression condition) {
        this.left = left;
        this.right = right;
        this.leftKeys = List.copyOf(leftKeys);
        this.rightKeys = List.copyOf(rightKeys);
        this.joinType = joinType;
        this.condition = condition;
    }

    public LogicalPlan left() { return left; }
    public LogicalPlan right() { return right; }
    public List<Expression> leftKeys() { return leftKeys; }
    public List<Expression> rightKeys() { return rightKeys; }
    public JoinType joinType() { return joinType; }
    /** Extra non-equi predicate over the combined row, or {@code null}. */
    public Expression condition() { return condition; }

    /** True for the semi/anti family, whose output is the left columns only. */
    public static boolean outputsLeftOnly(JoinType jt) {
        return jt == JoinType.LEFT_SEMI || jt == JoinType.LEFT_ANTI;
    }

    @Override
    public StructType schema() {
        // SEMI/ANTI are existence tests: only the left columns flow out.
        if (outputsLeftOnly(joinType)) return left.schema();
        boolean leftNullable = joinType == JoinType.RIGHT || joinType == JoinType.FULL;
        boolean rightNullable = joinType == JoinType.LEFT || joinType == JoinType.FULL;
        java.util.Set<String> seen = new java.util.HashSet<>();
        List<StructField> fields = new ArrayList<>();
        for (StructField f : left.schema().fields()) {
            String name = uniqueName(f.name(), seen);
            fields.add(new StructField(name, f.dataType(), f.nullable() || leftNullable));
            seen.add(name);
        }
        for (StructField f : right.schema().fields()) {
            String name = uniqueName(f.name(), seen);
            fields.add(new StructField(name, f.dataType(), f.nullable() || rightNullable));
            seen.add(name);
        }
        return new StructType(fields);
    }

    /**
     * The schema a join {@code condition} binds against: left columns then
     * right columns, names deconflicted, no nullability — i.e. the layout of
     * the combined {@code leftRow ++ rightRow} the executor builds to evaluate
     * the predicate. Distinct from {@link #schema()} (which drops the right
     * side for semi/anti and adds outer nullability).
     */
    public static StructType combinedInputSchema(LogicalPlan left, LogicalPlan right) {
        java.util.Set<String> seen = new java.util.HashSet<>();
        List<StructField> fields = new ArrayList<>();
        for (StructField f : left.schema().fields()) {
            String name = uniqueName(f.name(), seen);
            fields.add(new StructField(name, f.dataType(), f.nullable()));
            seen.add(name);
        }
        for (StructField f : right.schema().fields()) {
            String name = uniqueName(f.name(), seen);
            fields.add(new StructField(name, f.dataType(), f.nullable()));
            seen.add(name);
        }
        return new StructType(fields);
    }

    /** First-free name in the sequence {@code base, base_2, base_3, ...}. */
    private static String uniqueName(String base, java.util.Set<String> seen) {
        if (!seen.contains(base)) return base;
        for (int i = 2; ; i++) {
            String candidate = base + "_" + i;
            if (!seen.contains(candidate)) return candidate;
        }
    }

    @Override public List<LogicalPlan> children() { return List.of(left, right); }

    @Override public LogicalPlan withChildren(List<LogicalPlan> c) {
        return new Join(c.get(0), c.get(1), leftKeys, rightKeys, joinType, condition);
    }

    @Override public boolean resolved() {
        if (!left.resolved() || !right.resolved()) return false;
        for (Expression e : leftKeys) if (!e.resolved()) return false;
        for (Expression e : rightKeys) if (!e.resolved()) return false;
        if (condition != null && !condition.resolved()) return false;
        return true;
    }

    @Override public String toString() {
        if (!leftKeys.isEmpty()) return joinType + " Join " + leftKeys + " = " + rightKeys;
        return joinType + " Join" + (condition != null ? " ON " + condition : " (cartesian)");
    }
}
