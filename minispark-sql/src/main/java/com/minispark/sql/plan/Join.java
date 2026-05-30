package com.minispark.sql.plan;

import com.minispark.sql.expr.Expression;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * An equi-join of two child plans on {@code leftKeys[i] == rightKeys[i]}. The
 * output schema is the left columns followed by the right columns (Spark keeps
 * both sides' join columns; we do the same). Outer joins make the padded side's
 * columns nullable.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.plans.logical.Join.
 */
public final class Join implements LogicalPlan {

    private final LogicalPlan left;
    private final LogicalPlan right;
    private final List<Expression> leftKeys;
    private final List<Expression> rightKeys;
    private final JoinType joinType;

    public Join(LogicalPlan left, LogicalPlan right,
                List<Expression> leftKeys, List<Expression> rightKeys, JoinType joinType) {
        this.left = left;
        this.right = right;
        this.leftKeys = List.copyOf(leftKeys);
        this.rightKeys = List.copyOf(rightKeys);
        this.joinType = joinType;
    }

    public LogicalPlan left() { return left; }
    public LogicalPlan right() { return right; }
    public List<Expression> leftKeys() { return leftKeys; }
    public List<Expression> rightKeys() { return rightKeys; }
    public JoinType joinType() { return joinType; }

    @Override
    public StructType schema() {
        boolean leftNullable = joinType == JoinType.RIGHT || joinType == JoinType.FULL;
        boolean rightNullable = joinType == JoinType.LEFT || joinType == JoinType.FULL;
        // Track names already used (left side) so we can deconflict right-side
        // columns that share a name. Without this, a join key like 'id' present
        // on both sides becomes two fields named 'id'; StructType.indexOf returns
        // the first match, so any by-name reference would silently bind to the
        // left side only and the right-side column would be unreachable.
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
        return new Join(c.get(0), c.get(1), leftKeys, rightKeys, joinType);
    }

    @Override public boolean resolved() {
        if (!left.resolved() || !right.resolved()) return false;
        for (Expression e : leftKeys) if (!e.resolved()) return false;
        for (Expression e : rightKeys) if (!e.resolved()) return false;
        return true;
    }

    @Override public String toString() { return joinType + " Join " + leftKeys + " = " + rightKeys; }
}
