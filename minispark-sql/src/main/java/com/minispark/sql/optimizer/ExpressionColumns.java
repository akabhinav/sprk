package com.minispark.sql.optimizer;

import com.minispark.sql.expr.BoundReference;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.expr.UnresolvedAttribute;

import java.util.HashSet;
import java.util.Set;

/** Utility: the set of column names an expression tree references. */
final class ExpressionColumns {

    private ExpressionColumns() {}

    static Set<String> referenced(Expression e) {
        Set<String> out = new HashSet<>();
        collect(e, out);
        return out;
    }

    private static void collect(Expression e, Set<String> out) {
        if (e instanceof UnresolvedAttribute u) out.add(u.columnName());
        else if (e instanceof BoundReference b) out.add(b.name());
        for (Expression c : e.children()) collect(c, out);
    }
}
