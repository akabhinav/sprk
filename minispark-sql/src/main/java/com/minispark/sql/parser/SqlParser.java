package com.minispark.sql.parser;

import com.minispark.sql.Column;
import com.minispark.sql.expr.Alias;
import com.minispark.sql.expr.Arithmetic;
import com.minispark.sql.expr.BooleanOp;
import com.minispark.sql.expr.Comparison;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.expr.Literal;
import com.minispark.sql.expr.UnresolvedAttribute;
import com.minispark.sql.expr.agg.AggregateFunction;
import com.minispark.sql.expr.agg.Aggregates;
import com.minispark.sql.plan.Aggregate;
import com.minispark.sql.plan.Filter;
import com.minispark.sql.plan.LogicalPlan;
import com.minispark.sql.plan.Project;
import com.minispark.sql.plan.UnresolvedRelation;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * A recursive-descent parser turning a SQL string into an <i>unresolved</i>
 * {@link LogicalPlan} (table refs and column names left for the analyzer to
 * bind). Supports:
 * <pre>
 *   SELECT &lt;projExpr [AS alias], ...&gt; | *
 *   FROM &lt;table&gt;
 *   [WHERE &lt;predicate&gt;]
 *   [GROUP BY &lt;col, ...&gt;]
 * </pre>
 * with full expression precedence (OR &lt; AND &lt; comparison &lt; add/sub
 * &lt; mul/div &lt; unary/primary) and the aggregate functions
 * {@code count/sum/avg/min/max}.
 *
 * <p>One detail worth noting: a SELECT containing aggregate calls (or a GROUP
 * BY) produces an {@link Aggregate} node; otherwise a {@link Project}. The
 * grammar is small but the structure is exactly real Spark's
 * AstBuilder → unresolved LogicalPlan.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.parser.AstBuilder.
 */
public final class SqlParser {

    private final List<Token> tokens;
    private int pos = 0;

    public SqlParser(String sql) { this.tokens = new Lexer(sql).tokenize(); }

    public static LogicalPlan parsePlan(String sql) { return new SqlParser(sql).query(); }

    // ---------- statement ----------

    /** A projected expression with the alias/name to give its output column. */
    private record SelectItem(Expression expr, String alias, boolean star) {}

    public LogicalPlan query() {
        expectKeyword("SELECT");
        boolean distinct = false;
        if (peekKeyword("DISTINCT")) { advance(); distinct = true; }
        List<SelectItem> items = selectItems();
        expectKeyword("FROM");
        LogicalPlan plan = fromClause();

        if (peekKeyword("WHERE")) {
            advance();
            plan = new Filter(predicate(), plan);
        }

        List<Expression> groupBy = new ArrayList<>();
        if (peekKeyword("GROUP")) {
            advance(); expectKeyword("BY");
            groupBy.add(expression());
            while (match(TokenType.COMMA)) groupBy.add(expression());
        }

        // HAVING is a predicate over the aggregate's output, so it parses now
        // and is applied as a Filter on top of the Aggregate below.
        Expression having = null;
        if (peekKeyword("HAVING")) {
            advance();
            having = predicate();
        }

        // SELECT/WHERE/GROUP BY first build the project-or-aggregate, then
        // HAVING/DISTINCT/ORDER BY/LIMIT wrap that, in SQL evaluation order.
        List<com.minispark.sql.plan.SortOrder> orderBy = new ArrayList<>();
        if (peekKeyword("ORDER")) {
            advance(); expectKeyword("BY");
            orderBy.add(sortItem());
            while (match(TokenType.COMMA)) orderBy.add(sortItem());
        }

        int limit = -1;
        if (peekKeyword("LIMIT")) {
            advance();
            Token n = peekToken();
            if (!n.is(TokenType.INT_LITERAL)) throw new ParseException("LIMIT requires an integer");
            advance();
            limit = Integer.parseInt(n.text());
        }

        expect(TokenType.EOF, "end of statement");

        // --- Decide aggregate vs plain projection ---
        // An aggregate query is one where any aggregate call appears anywhere in
        // SELECT/HAVING/ORDER BY, OR a GROUP BY clause was given. We must scan
        // the full expression trees (not just the top-level), so e.g.
        // `SELECT sum(x) + 1` is detected as an aggregate.
        boolean hasAgg = !groupBy.isEmpty();
        for (SelectItem it : items) if (it.expr != null && containsAggregate(it.expr)) hasAgg = true;
        if (having != null && containsAggregate(having)) hasAgg = true;
        for (var so : orderBy) if (containsAggregate(so.expr())) hasAgg = true;

        LogicalPlan result;
        if (!hasAgg) {
            // Plain projection path: SELECT expressions become the project list.
            List<Expression> proj = new ArrayList<>();
            for (SelectItem it : items) {
                if (it.star) proj.add(new com.minispark.sql.expr.Star());
                else proj.add(it.alias != null ? new Alias(it.expr, it.alias) : it.expr);
            }
            result = new Project(proj, plan);
            if (having != null) {
                throw new ParseException("HAVING requires GROUP BY or aggregate functions");
            }
            if (!orderBy.isEmpty()) result = new com.minispark.sql.plan.Sort(orderBy, result);
        } else {
            // Aggregate path: gather every aggregate referenced in SELECT, HAVING,
            // and ORDER BY into one Aggregate node; SELECT/HAVING/ORDER BY are
            // rewritten so their aggregate references point at the Aggregate's
            // output columns by name. Non-aggregate SELECT items must be in the
            // GROUP BY list (or, if none was given, an error — they have no
            // single value per group).
            List<AggregateFunction> aggs = new ArrayList<>();
            List<String> aggNames = new ArrayList<>();
            // SELECT: classify each item.
            List<Expression> projectExprs = new ArrayList<>();
            for (SelectItem it : items) {
                if (it.star) {
                    throw new ParseException("SELECT * is not allowed with GROUP BY / aggregates");
                }
                // A non-aggregate SELECT item must reference a grouping expression.
                if (!containsAggregate(it.expr)) {
                    if (groupBy.isEmpty()) {
                        throw new ParseException("column '" + it.expr
                                + "' must appear in GROUP BY or be inside an aggregate");
                    }
                    if (!groupingContains(groupBy, it.expr)) {
                        throw new ParseException("column '" + it.expr
                                + "' is not in GROUP BY and not inside an aggregate");
                    }
                }
                Expression rewritten = rewriteAggregates(it.expr, aggs, aggNames);
                projectExprs.add(it.alias != null ? new Alias(rewritten, it.alias) : rewritten);
            }
            // HAVING & ORDER BY: rewrite same way so they reference aggregate outputs.
            Expression havingExpr = having == null ? null : rewriteAggregates(having, aggs, aggNames);
            List<com.minispark.sql.plan.SortOrder> rewrittenOrder = new ArrayList<>();
            for (var so : orderBy) {
                rewrittenOrder.add(new com.minispark.sql.plan.SortOrder(
                        rewriteAggregates(so.expr(), aggs, aggNames), so.ascending()));
            }
            result = new Aggregate(new ArrayList<>(groupBy), aggs, plan);
            if (havingExpr != null) result = new Filter(havingExpr, result);
            result = new Project(projectExprs, result);
            if (!rewrittenOrder.isEmpty()) result = new com.minispark.sql.plan.Sort(rewrittenOrder, result);
        }
        if (distinct) result = new com.minispark.sql.plan.Distinct(result);
        if (limit >= 0) result = new com.minispark.sql.plan.Limit(limit, result);
        return result;
    }

    /** Whether {@code e} (or any descendant) is an aggregate call. */
    private boolean containsAggregate(Expression e) {
        if (e instanceof AggMarker) return true;
        for (Expression c : e.children()) if (containsAggregate(c)) return true;
        return false;
    }

    /**
     * Whether {@code col} appears verbatim (by toString) in {@code groupBy}.
     * This is conservative — a SELECT expression matches a GROUP BY only when
     * they're textually the same — which is enough for our simple grammar
     * (column refs and aliases-of-column-refs).
     */
    private boolean groupingContains(List<Expression> groupBy, Expression col) {
        String s = col.toString();
        for (Expression g : groupBy) if (g.toString().equals(s)) return true;
        return false;
    }

    /**
     * Walk an expression replacing every {@link AggMarker} with an
     * {@link UnresolvedAttribute} that references the aggregate's output column
     * by name (e.g. {@code sum(amount)}). Aggregates are deduped by output name
     * across all callers (SELECT/HAVING/ORDER BY share one list).
     */
    private Expression rewriteAggregates(Expression e,
                                         List<AggregateFunction> aggs, List<String> aggNames) {
        if (e instanceof AggMarker m) {
            String name = m.fn.name();
            if (!aggNames.contains(name)) {
                aggNames.add(name);
                aggs.add(m.fn);
            }
            return new UnresolvedAttribute(name);
        }
        List<Expression> children = e.children();
        if (children.isEmpty()) return e;
        List<Expression> rewritten = new ArrayList<>(children.size());
        for (Expression c : children) rewritten.add(rewriteAggregates(c, aggs, aggNames));
        return e.withChildren(rewritten);
    }

    /**
     * FROM clause: a base table optionally followed by JOIN clauses. We support
     * {@code [INNER|LEFT|RIGHT|FULL] [OUTER] JOIN t ON a = b}.
     */
    private LogicalPlan fromClause() {
        LogicalPlan left = new UnresolvedRelation(identifierName());
        while (isJoinStart()) {
            com.minispark.sql.plan.JoinType type = joinType();
            expectKeyword("JOIN");
            LogicalPlan right = new UnresolvedRelation(identifierName());
            expectKeyword("ON");
            // ON must be an equi-join: leftCol = rightCol (AND leftCol2 = rightCol2 ...).
            List<Expression> lk = new ArrayList<>();
            List<Expression> rk = new ArrayList<>();
            parseEquiJoinCondition(lk, rk);
            left = new com.minispark.sql.plan.Join(left, right, lk, rk, type);
        }
        return left;
    }

    private boolean isJoinStart() {
        return peekKeyword("JOIN") || peekKeyword("INNER") || peekKeyword("LEFT")
                || peekKeyword("RIGHT") || peekKeyword("FULL");
    }

    private com.minispark.sql.plan.JoinType joinType() {
        com.minispark.sql.plan.JoinType t = com.minispark.sql.plan.JoinType.INNER;
        if (peekKeyword("INNER")) { advance(); }
        else if (peekKeyword("LEFT")) { advance(); t = com.minispark.sql.plan.JoinType.LEFT; }
        else if (peekKeyword("RIGHT")) { advance(); t = com.minispark.sql.plan.JoinType.RIGHT; }
        else if (peekKeyword("FULL")) { advance(); t = com.minispark.sql.plan.JoinType.FULL; }
        if (peekKeyword("OUTER")) advance(); // optional noise word
        return t;
    }

    /**
     * Parse {@code col = col [AND col = col]...} into parallel key lists. We use
     * {@code additive()} (the precedence level just below comparison) for each
     * side so the {@code =} is NOT swallowed into an equality expression — it's
     * the join-condition separator we consume explicitly.
     */
    private void parseEquiJoinCondition(List<Expression> lk, List<Expression> rk) {
        do {
            Expression l = additive();
            expect(TokenType.EQ, "= in JOIN ON");
            Expression r = additive();
            lk.add(l);
            rk.add(r);
        } while (peekKeyword("AND") && consume());
    }

    private boolean consume() { advance(); return true; }

    /** One ORDER BY term: expr followed by optional ASC/DESC. */
    private com.minispark.sql.plan.SortOrder sortItem() {
        Expression e = expression();
        boolean asc = true;
        if (peekKeyword("ASC")) advance();
        else if (peekKeyword("DESC")) { advance(); asc = false; }
        return new com.minispark.sql.plan.SortOrder(e, asc);
    }

    private List<SelectItem> selectItems() {
        List<SelectItem> items = new ArrayList<>();
        items.add(selectItem());
        while (match(TokenType.COMMA)) items.add(selectItem());
        return items;
    }

    private SelectItem selectItem() {
        if (peek(TokenType.STAR)) { advance(); return new SelectItem(null, null, true); }
        Expression e = expression();
        String alias = null;
        if (peekKeyword("AS")) { advance(); alias = identifierName(); }
        else if (peek(TokenType.IDENTIFIER)) { alias = identifierName(); } // implicit alias
        return new SelectItem(e, alias, false);
    }

    // ---------- expressions (precedence climbing) ----------

    private Expression predicate() { return orExpr(); }
    private Expression expression() { return orExpr(); }

    private Expression orExpr() {
        Expression left = andExpr();
        while (peekKeyword("OR")) { advance(); left = BooleanOp.or(left, andExpr()); }
        return left;
    }

    private Expression andExpr() {
        Expression left = comparison();
        while (peekKeyword("AND")) { advance(); left = BooleanOp.and(left, comparison()); }
        return left;
    }

    private Expression comparison() {
        Expression left = additive();
        TokenType t = peekType();
        switch (t) {
            case EQ -> { advance(); return Comparison.eq(left, additive()); }
            case NEQ -> { advance(); return new Comparison(Comparison.Op.NE, left, additive()); }
            case LT -> { advance(); return Comparison.lt(left, additive()); }
            case LE -> { advance(); return Comparison.le(left, additive()); }
            case GT -> { advance(); return Comparison.gt(left, additive()); }
            case GE -> { advance(); return Comparison.ge(left, additive()); }
            default -> { return left; }
        }
    }

    private Expression additive() {
        Expression left = multiplicative();
        while (true) {
            if (match(TokenType.PLUS)) left = Arithmetic.add(left, multiplicative());
            else if (match(TokenType.MINUS)) left = Arithmetic.sub(left, multiplicative());
            else return left;
        }
    }

    private Expression multiplicative() {
        Expression left = primary();
        while (true) {
            if (match(TokenType.STAR)) left = Arithmetic.mul(left, primary());
            else if (match(TokenType.SLASH)) left = Arithmetic.div(left, primary());
            else return left;
        }
    }

    private Expression primary() {
        Token t = peekToken();
        if (match(TokenType.LPAREN)) { Expression e = orExpr(); expect(TokenType.RPAREN, ")"); return e; }
        if (t.is(TokenType.INT_LITERAL))    { advance(); return Literal.of(Integer.parseInt(t.text())); }
        if (t.is(TokenType.DOUBLE_LITERAL)) { advance(); return Literal.of(Double.parseDouble(t.text())); }
        if (t.is(TokenType.STRING_LITERAL)) { advance(); return Literal.of(t.text()); }
        if (t.isKeyword("TRUE"))  { advance(); return Literal.of(true); }
        if (t.isKeyword("FALSE")) { advance(); return Literal.of(false); }
        if (t.is(TokenType.IDENTIFIER)) {
            String name = t.text(); advance();
            if (peek(TokenType.LPAREN)) return functionCall(name);
            return new UnresolvedAttribute(name);
        }
        throw new ParseException("unexpected token " + t);
    }

    /** An aggregate function call: a marker carrying the AggregateFunction. */
    private Expression functionCall(String name) {
        expect(TokenType.LPAREN, "(");
        String lname = name.toLowerCase();
        Expression arg = null;
        boolean star = false;
        if (peek(TokenType.STAR)) { advance(); star = true; }
        else if (!peek(TokenType.RPAREN)) { arg = orExpr(); }
        expect(TokenType.RPAREN, ")");

        AggregateFunction fn = switch (lname) {
            case "count" -> star || arg == null ? Aggregates.Count.star() : new Aggregates.Count(arg);
            case "sum" -> new Aggregates.Sum(requireArg(arg, "sum"), StructType.of());
            case "avg" -> new Aggregates.Avg(requireArg(arg, "avg"));
            case "min" -> new Aggregates.MinMax(requireArg(arg, "min"), true, StructType.of());
            case "max" -> new Aggregates.MinMax(requireArg(arg, "max"), false, StructType.of());
            default -> throw new ParseException("unknown function '" + name + "'");
        };
        return new AggMarker(fn);
    }

    private Expression requireArg(Expression arg, String fn) {
        if (arg == null) throw new ParseException(fn + "() requires an argument");
        return arg;
    }

    /**
     * A non-evaluating placeholder that carries an {@link AggregateFunction}
     * found in the select list out to {@link #buildProjectOrAggregate}. It never
     * reaches the analyzer/executor (those see an {@link Aggregate} node built
     * from the wrapped functions instead).
     */
    private static final class AggMarker implements Expression {
        final AggregateFunction fn;
        AggMarker(AggregateFunction fn) { this.fn = fn; }
        @Override public com.minispark.sql.types.DataType dataType(StructType s) { return fn.resultType(s); }
        @Override public Object eval(com.minispark.sql.Row input) {
            throw new IllegalStateException("aggregate marker is not evaluable"); }
        @Override public List<Expression> children() { return List.of(); }
        @Override public Expression withChildren(List<Expression> c) { return this; }
        @Override public String name() { return fn.name(); }
    }

    // ---------- token helpers ----------

    private Token peekToken() { return tokens.get(pos); }
    private TokenType peekType() { return tokens.get(pos).type(); }
    private boolean peek(TokenType t) { return peekType() == t; }
    private boolean peekKeyword(String kw) { return peekToken().isKeyword(kw); }
    private void advance() { if (pos < tokens.size() - 1) pos++; }

    private boolean match(TokenType t) {
        if (peek(t)) { advance(); return true; }
        return false;
    }

    private void expect(TokenType t, String what) {
        if (!peek(t)) throw new ParseException("expected " + what + " but got " + peekToken());
        advance();
    }

    private void expectKeyword(String kw) {
        if (!peekKeyword(kw)) throw new ParseException("expected '" + kw + "' but got " + peekToken());
        advance();
    }

    private String identifierName() {
        Token t = peekToken();
        if (!t.is(TokenType.IDENTIFIER)) throw new ParseException("expected identifier but got " + t);
        advance();
        return t.text();
    }
}
