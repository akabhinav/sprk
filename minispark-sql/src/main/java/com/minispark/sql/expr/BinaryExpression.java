package com.minispark.sql.expr;

import java.util.List;

/**
 * Shared base for two-child expressions: holds {@code left}/{@code right} and
 * implements the generic tree plumbing ({@link #children}, {@link #withChildren})
 * once. Subclasses supply only the type rule and the eval.
 */
public abstract class BinaryExpression implements Expression {

    protected final Expression left;
    protected final Expression right;

    protected BinaryExpression(Expression left, Expression right) {
        this.left = left;
        this.right = right;
    }

    public Expression left() { return left; }
    public Expression right() { return right; }

    @Override public List<Expression> children() { return List.of(left, right); }

    @Override
    public Expression withChildren(List<Expression> c) {
        if (c.size() != 2) throw new IllegalArgumentException("binary expr needs 2 children");
        return rebuild(c.get(0), c.get(1));
    }

    /** Construct a same-type node with new children. */
    protected abstract Expression rebuild(Expression newLeft, Expression newRight);

    /** Numeric promotion helper for arithmetic/comparison. */
    protected static double asDouble(Object o) { return ((Number) o).doubleValue(); }
    protected static boolean bothIntegral(Object a, Object b) {
        return (a instanceof Integer || a instanceof Long)
                && (b instanceof Integer || b instanceof Long);
    }
}
