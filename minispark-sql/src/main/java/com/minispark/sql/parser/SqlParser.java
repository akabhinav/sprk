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
        List<SelectItem> items = selectItems();
        expectKeyword("FROM");
        LogicalPlan plan = new UnresolvedRelation(identifierName());

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

        // SELECT/WHERE/GROUP BY first build the project-or-aggregate, then ORDER
        // BY and LIMIT wrap that (they apply to the already-projected rows).
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

        LogicalPlan result = buildProjectOrAggregate(items, groupBy, plan);
        if (!orderBy.isEmpty()) result = new com.minispark.sql.plan.Sort(orderBy, result);
        if (limit >= 0) result = new com.minispark.sql.plan.Limit(limit, result);
        return result;
    }

    /** One ORDER BY term: expr followed by optional ASC/DESC. */
    private com.minispark.sql.plan.SortOrder sortItem() {
        Expression e = expression();
        boolean asc = true;
        if (peekKeyword("ASC")) advance();
        else if (peekKeyword("DESC")) { advance(); asc = false; }
        return new com.minispark.sql.plan.SortOrder(e, asc);
    }

    /** Decide between a plain projection and an aggregation based on the select list. */
    private LogicalPlan buildProjectOrAggregate(List<SelectItem> items,
                                                List<Expression> groupBy, LogicalPlan child) {
        boolean hasAgg = false;
        for (SelectItem it : items) if (it.expr instanceof AggMarker) hasAgg = true;

        if (!hasAgg && groupBy.isEmpty()) {
            // SELECT a, *, b → a Star expression the analyzer expands to all input columns.
            List<Expression> proj = new ArrayList<>();
            for (SelectItem it : items) {
                if (it.star) { proj.add(new com.minispark.sql.expr.Star()); }
                else proj.add(it.alias != null ? new Alias(it.expr, it.alias) : it.expr);
            }
            return new Project(proj, child);
        }

        // Aggregation: grouping exprs are the non-aggregate select items (plus any
        // explicit GROUP BY), aggregates are the AggMarker items.
        List<Expression> grouping = new ArrayList<>(groupBy);
        List<AggregateFunction> aggs = new ArrayList<>();
        for (SelectItem it : items) {
            if (it.expr instanceof AggMarker m) {
                aggs.add(m.fn);
            } else if (!it.star && groupBy.isEmpty()) {
                grouping.add(it.expr);
            }
        }
        return new Aggregate(grouping, aggs, child);
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
