package com.minispark.sql;

import com.minispark.sql.expr.Alias;
import com.minispark.sql.expr.Arithmetic;
import com.minispark.sql.expr.BooleanOp;
import com.minispark.sql.expr.Comparison;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.expr.Literal;
import com.minispark.sql.expr.UnresolvedAttribute;

/**
 * A fluent wrapper over an {@link Expression} — the DataFrame DSL. {@code
 * col("a").plus(1).gt(10)} builds the expression tree the planner consumes.
 * Equivalent to Spark's {@code Column} / {@code functions.col}.
 *
 * Real Spark equivalent: org.apache.spark.sql.Column.
 */
public final class Column {

    private final Expression expr;

    public Column(Expression expr) { this.expr = expr; }

    public static Column col(String name) { return new Column(new UnresolvedAttribute(name)); }
    public static Column lit(int v)     { return new Column(Literal.of(v)); }
    public static Column lit(long v)    { return new Column(Literal.of(v)); }
    public static Column lit(double v)  { return new Column(Literal.of(v)); }
    public static Column lit(String v)  { return new Column(Literal.of(v)); }
    public static Column lit(boolean v) { return new Column(Literal.of(v)); }

    public Expression expr() { return expr; }

    // ----- arithmetic -----
    public Column plus(Column o)  { return new Column(Arithmetic.add(expr, o.expr)); }
    public Column plus(Object v)  { return plus(of(v)); }
    public Column minus(Column o) { return new Column(Arithmetic.sub(expr, o.expr)); }
    public Column minus(Object v) { return minus(of(v)); }
    public Column times(Column o) { return new Column(Arithmetic.mul(expr, o.expr)); }
    public Column times(Object v) { return times(of(v)); }
    public Column divide(Column o){ return new Column(Arithmetic.div(expr, o.expr)); }
    public Column divide(Object v){ return divide(of(v)); }

    // ----- comparison -----
    public Column eq(Column o)  { return new Column(Comparison.eq(expr, o.expr)); }
    public Column eq(Object v)  { return eq(of(v)); }
    public Column lt(Column o)  { return new Column(Comparison.lt(expr, o.expr)); }
    public Column lt(Object v)  { return lt(of(v)); }
    public Column le(Column o)  { return new Column(Comparison.le(expr, o.expr)); }
    public Column le(Object v)  { return le(of(v)); }
    public Column gt(Column o)  { return new Column(Comparison.gt(expr, o.expr)); }
    public Column gt(Object v)  { return gt(of(v)); }
    public Column ge(Column o)  { return new Column(Comparison.ge(expr, o.expr)); }
    public Column ge(Object v)  { return ge(of(v)); }

    // ----- boolean -----
    public Column and(Column o) { return new Column(BooleanOp.and(expr, o.expr)); }
    public Column or(Column o)  { return new Column(BooleanOp.or(expr, o.expr)); }

    // ----- naming -----
    public Column as(String alias) { return new Column(new Alias(expr, alias)); }

    private static Column of(Object v) {
        if (v instanceof Column c) return c;
        if (v instanceof Integer i) return lit(i);
        if (v instanceof Long l) return lit(l);
        if (v instanceof Double d) return lit(d);
        if (v instanceof Boolean b) return lit(b);
        return lit(String.valueOf(v));
    }

    @Override public String toString() { return expr.toString(); }
}
