package com.minispark.sql.expr.window;

import com.minispark.sql.Row;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * Expression wrapper around a {@link WindowFunction} + its {@link WindowSpec}.
 * Lives in expression trees the same way an aggregate marker does — the
 * planner pulls it out of the SELECT and routes it through
 * {@link com.minispark.sql.execution.WindowExec}.
 *
 * <p>{@link #eval} throws: window expressions cannot be evaluated row-by-row
 * in a plain {@code ProjectExec}; they need the whole partition. Hitting this
 * means the planner failed to lift the expression into a Window plan node.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.expressions.WindowExpression.
 */
public final class WindowExpression implements Expression {

    private final WindowFunction function;
    private final WindowSpec spec;

    public WindowExpression(WindowFunction function, WindowSpec spec) {
        this.function = function;
        this.spec = spec;
    }

    public WindowFunction function() { return function; }
    public WindowSpec spec() { return spec; }

    @Override public DataType dataType(StructType inputSchema) { return function.resultType(inputSchema); }

    @Override
    public Object eval(Row input) {
        throw new UnsupportedOperationException(
                "WindowExpression cannot be evaluated row-by-row; " +
                "the planner must lift it into a WindowExec — got " + function.name());
    }

    @Override public List<Expression> children() { return List.of(); }
    @Override public Expression withChildren(List<Expression> newChildren) { return this; }
    @Override public boolean resolved() {
        for (Expression e : spec.partitionBy()) if (!e.resolved()) return false;
        for (com.minispark.sql.plan.SortOrder o : spec.orderBy()) if (!o.expr().resolved()) return false;
        return true;
    }
    @Override public String name() { return function.name(); }
    @Override public String toString() { return function.name() + "() OVER " + spec; }
}
