package com.minispark.sql.expr;

import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructType;

/**
 * Boolean AND / OR over two BOOLEAN children, with short-circuit evaluation.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.expressions.{And,Or}.
 */
public final class BooleanOp extends BinaryExpression {

    public enum Op { AND, OR }

    private final Op op;

    public BooleanOp(Op op, Expression left, Expression right) {
        super(left, right);
        this.op = op;
    }

    public static BooleanOp and(Expression l, Expression r) { return new BooleanOp(Op.AND, l, r); }
    public static BooleanOp or(Expression l, Expression r)  { return new BooleanOp(Op.OR, l, r); }

    @Override public DataType dataType(StructType s) { return DataType.BOOLEAN; }

    @Override
    public Object eval(Row input) {
        Object a = left.eval(input);
        // Short-circuit: AND false / OR true decide without evaluating the right.
        if (op == Op.AND && Boolean.FALSE.equals(a)) return false;
        if (op == Op.OR && Boolean.TRUE.equals(a)) return true;
        Object b = right.eval(input);
        if (a == null || b == null) return null;
        boolean x = (Boolean) a, y = (Boolean) b;
        return op == Op.AND ? (x && y) : (x || y);
    }

    @Override protected Expression rebuild(Expression l, Expression r) { return new BooleanOp(op, l, r); }
    @Override public String toString() { return "(" + left + " " + op + " " + right + ")"; }
}
