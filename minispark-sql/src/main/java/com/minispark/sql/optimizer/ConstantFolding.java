package com.minispark.sql.optimizer;

import com.minispark.sql.expr.Alias;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.expr.Literal;
import com.minispark.sql.types.DataType;
import com.minispark.sql.plan.Filter;
import com.minispark.sql.plan.LogicalPlan;
import com.minispark.sql.plan.Project;

import java.util.ArrayList;
import java.util.List;

/**
 * Constant folding: any subexpression all of whose leaves are {@link Literal}s
 * is evaluated once at optimize time and replaced by its constant result, so
 * {@code 2 + 3} never gets recomputed per row. Applied to the expressions in
 * Project and Filter nodes.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.optimizer.ConstantFolding.
 */
public final class ConstantFolding implements Rule {

    @Override public String name() { return "ConstantFolding"; }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        List<LogicalPlan> newChildren = new ArrayList<>();
        for (LogicalPlan c : plan.children()) newChildren.add(apply(c));
        LogicalPlan node = plan.children().isEmpty() ? plan : plan.withChildren(newChildren);

        if (node instanceof Project p) {
            List<Expression> folded = new ArrayList<>();
            for (Expression e : p.projectList()) folded.add(fold(e));
            return new Project(folded, p.child());
        }
        if (node instanceof Filter f) {
            return new Filter(fold(f.condition()), f.child());
        }
        return node;
    }

    /** Bottom-up fold: replace a fully-constant subtree with its evaluated Literal. */
    private Expression fold(Expression e) {
        // Don't collapse an Alias into a bare literal — it must keep its name.
        if (e instanceof Alias a) return new Alias(fold(a.child()), a.name());

        List<Expression> children = e.children();
        if (!children.isEmpty()) {
            List<Expression> newChildren = new ArrayList<>(children.size());
            for (Expression c : children) newChildren.add(fold(c));
            e = e.withChildren(newChildren);
        }
        if (isConstant(e) && !(e instanceof Literal)) {
            Object v = e.eval(null);
            return new Literal(v, inferType(v));
        }
        return e;
    }

    private boolean isConstant(Expression e) {
        if (e instanceof Literal) return true;
        if (e.children().isEmpty()) return false; // a bare column ref is not constant
        for (Expression c : e.children()) if (!isConstant(c)) return false;
        return true;
    }

    private DataType inferType(Object v) {
        if (v instanceof Integer) return DataType.INT;
        if (v instanceof Long) return DataType.LONG;
        if (v instanceof Double) return DataType.DOUBLE;
        if (v instanceof Boolean) return DataType.BOOLEAN;
        return DataType.STRING;
    }
}
