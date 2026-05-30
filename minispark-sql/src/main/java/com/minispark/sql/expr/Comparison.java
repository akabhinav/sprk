package com.minispark.sql.expr;

import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

/**
 * Comparison operators (= != &lt; &lt;= &gt; &gt;=) producing a BOOLEAN. Numbers
 * compare by value with promotion; strings/booleans by natural order; equality
 * works for any type. Null on either side yields null.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.expressions
 *                        .{EqualTo,LessThan,GreaterThan,...}.
 */
public final class Comparison extends BinaryExpression {

    public enum Op { EQ("="), NE("!="), LT("<"), LE("<="), GT(">"), GE(">=");
        final String sym; Op(String s) { this.sym = s; } }

    private final Op op;

    public Comparison(Op op, Expression left, Expression right) {
        super(left, right);
        this.op = op;
    }

    public static Comparison eq(Expression l, Expression r) { return new Comparison(Op.EQ, l, r); }
    public static Comparison lt(Expression l, Expression r) { return new Comparison(Op.LT, l, r); }
    public static Comparison le(Expression l, Expression r) { return new Comparison(Op.LE, l, r); }
    public static Comparison gt(Expression l, Expression r) { return new Comparison(Op.GT, l, r); }
    public static Comparison ge(Expression l, Expression r) { return new Comparison(Op.GE, l, r); }

    @Override public DataType dataType(StructType s) { return DataType.BOOLEAN; }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object eval(Row input) {
        Object a = left.eval(input), b = right.eval(input);
        if (a == null || b == null) return null;
        if (op == Op.EQ) return a.equals(b);
        if (op == Op.NE) return !a.equals(b);
        int cmp;
        if (a instanceof Number && b instanceof Number) {
            cmp = Double.compare(asDouble(a), asDouble(b));
        } else if (a instanceof Comparable ca && b instanceof Comparable) {
            cmp = ca.compareTo(b);
        } else {
            throw new IllegalStateException("Cannot compare " + a + " and " + b);
        }
        return switch (op) {
            case LT -> cmp < 0; case LE -> cmp <= 0; case GT -> cmp > 0; case GE -> cmp >= 0;
            default -> false;
        };
    }

    @Override protected Expression rebuild(Expression l, Expression r) { return new Comparison(op, l, r); }
    @Override public String toString() { return "(" + left + " " + op.sym + " " + right + ")"; }
}
