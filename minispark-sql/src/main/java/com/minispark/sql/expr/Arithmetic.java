package com.minispark.sql.expr;

import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

/**
 * The four arithmetic operators (+ - * /). Result type is LONG when both
 * operands are integral, else DOUBLE — a deliberately tiny promotion rule.
 * Null in either operand yields null (SQL semantics).
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.expressions.{Add,Subtract,Multiply,Divide}.
 */
public final class Arithmetic extends BinaryExpression {

    public enum Op { ADD("+"), SUB("-"), MUL("*"), DIV("/");
        final String sym; Op(String s) { this.sym = s; } }

    private final Op op;

    public Arithmetic(Op op, Expression left, Expression right) {
        super(left, right);
        this.op = op;
    }

    public static Arithmetic add(Expression l, Expression r) { return new Arithmetic(Op.ADD, l, r); }
    public static Arithmetic sub(Expression l, Expression r) { return new Arithmetic(Op.SUB, l, r); }
    public static Arithmetic mul(Expression l, Expression r) { return new Arithmetic(Op.MUL, l, r); }
    public static Arithmetic div(Expression l, Expression r) { return new Arithmetic(Op.DIV, l, r); }

    @Override
    public DataType dataType(StructType s) {
        boolean integral = left.dataType(s) != DataType.DOUBLE && right.dataType(s) != DataType.DOUBLE;
        // Division always yields a DOUBLE (so 1/2 = 0.5, matching most SQL casts).
        return (op == Op.DIV || !integral) ? DataType.DOUBLE : DataType.LONG;
    }

    @Override
    public Object eval(Row input) {
        Object a = left.eval(input), b = right.eval(input);
        if (a == null || b == null) return null;
        if (op == Op.DIV) return asDouble(a) / asDouble(b);
        if (bothIntegral(a, b)) {
            long x = ((Number) a).longValue(), y = ((Number) b).longValue();
            return switch (op) { case ADD -> x + y; case SUB -> x - y; case MUL -> x * y; default -> 0L; };
        }
        double x = asDouble(a), y = asDouble(b);
        return switch (op) { case ADD -> x + y; case SUB -> x - y; case MUL -> x * y; default -> 0d; };
    }

    @Override protected Expression rebuild(Expression l, Expression r) { return new Arithmetic(op, l, r); }
    @Override public String name() { return left.name() + " " + op.sym + " " + right.name(); }
    @Override public String toString() { return "(" + left + " " + op.sym + " " + right + ")"; }
}
