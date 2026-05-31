package com.minispark.sql;

import com.minispark.sql.analysis.Analyzer;
import com.minispark.sql.execution.PhysicalPlan;
import com.minispark.sql.execution.SparkPlanner;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.optimizer.Optimizer;
import com.minispark.sql.plan.Filter;
import com.minispark.sql.plan.LogicalPlan;
import com.minispark.sql.plan.Project;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * A lazily-evaluated, schema-typed distributed collection of {@link Row}s,
 * backed by a {@link LogicalPlan}. Transformations ({@code select},
 * {@code filter}) return a new DataFrame with an extended plan; actions
 * ({@code collect}, {@code count}) compile the plan — analyze → optimize →
 * plan → execute — and run it on the RDD engine.
 *
 * Real Spark equivalent: org.apache.spark.sql.Dataset/DataFrame.
 */
public final class DataFrame {

    private final MiniSparkSession session;
    private final LogicalPlan logicalPlan;

    DataFrame(MiniSparkSession session, LogicalPlan logicalPlan) {
        this.session = session;
        this.logicalPlan = logicalPlan;
    }

    public LogicalPlan logicalPlan() { return logicalPlan; }

    /** Register this DataFrame as a temp view for {@code spark.sql("... FROM name")}. */
    public void createOrReplaceTempView(String name) {
        session.createOrReplaceTempView(name, this);
    }

    /** Start a write: {@code df.write().option(...).csv(path)} / {@code .json(path)}. */
    public DataFrameWriter write() { return new DataFrameWriter(this); }

    /**
     * The schema this DataFrame produces. Analyzes first so it works on plans
     * built from SQL text (which contain unresolved table/column references
     * until the analyzer binds them).
     */
    public StructType schema() {
        return logicalPlan.resolved()
                ? logicalPlan.schema()
                : session.analyzer().analyze(logicalPlan).schema();
    }

    // ----- transformations (lazy) -----

    public DataFrame select(Column... cols) {
        List<Expression> exprs = new ArrayList<>(cols.length);
        for (Column c : cols) exprs.add(c.expr());
        return new DataFrame(session, new Project(exprs, logicalPlan));
    }

    public DataFrame select(String... colNames) {
        Column[] cols = new Column[colNames.length];
        for (int i = 0; i < colNames.length; i++) cols[i] = Column.col(colNames[i]);
        return select(cols);
    }

    public DataFrame filter(Column condition) {
        return new DataFrame(session, new Filter(condition.expr(), logicalPlan));
    }

    public DataFrame where(Column condition) { return filter(condition); }

    /** Global sort, ascending, by the given columns. */
    public DataFrame orderBy(Column... cols) {
        List<com.minispark.sql.plan.SortOrder> orders = new ArrayList<>(cols.length);
        for (Column c : cols) orders.add(new com.minispark.sql.plan.SortOrder(c.expr(), true));
        return new DataFrame(session, new com.minispark.sql.plan.Sort(orders, logicalPlan));
    }

    /** Global sort with explicit order terms. */
    public DataFrame orderBy(List<com.minispark.sql.plan.SortOrder> orders) {
        return new DataFrame(session, new com.minispark.sql.plan.Sort(orders, logicalPlan));
    }

    /** Keep at most {@code n} rows. */
    public DataFrame limit(int n) {
        return new DataFrame(session, new com.minispark.sql.plan.Limit(n, logicalPlan));
    }

    /** Deduplicate whole rows. */
    public DataFrame distinct() {
        return new DataFrame(session, new com.minispark.sql.plan.Distinct(logicalPlan));
    }

    /**
     * Hint that this DataFrame should be materialised and broadcast when used
     * as one side of a join — the planner then picks
     * {@link com.minispark.sql.execution.BroadcastHashJoinExec} instead of the
     * shuffled variant, avoiding the shuffle on the streaming side entirely.
     * Use for small dimension tables (a few MB or a few thousand rows).
     */
    public DataFrame broadcast() {
        return new DataFrame(session, new com.minispark.sql.plan.BroadcastHint(logicalPlan));
    }

    /**
     * Append a new column derived from {@code value}, named {@code name}. If
     * the column is a window expression ({@code Window.rowNumber().over(...)})
     * it's lifted into a dedicated {@link com.minispark.sql.plan.Window} node;
     * otherwise it's appended via a Project. The new column always lands at
     * the end of the schema. Replacing existing columns of the same name is
     * not supported (yet) — append-only.
     */
    public DataFrame withColumn(String name, Column value) {
        Expression e = value.expr();
        if (e instanceof com.minispark.sql.expr.window.WindowExpression we) {
            return new DataFrame(session, new com.minispark.sql.plan.Window(
                    List.of(we), List.of(name), logicalPlan));
        }
        // Project that keeps every existing column + the new one aliased.
        List<Expression> outputs = new ArrayList<>();
        for (com.minispark.sql.types.StructField f : logicalPlan.schema().fields()) {
            outputs.add(new com.minispark.sql.expr.UnresolvedAttribute(f.name()));
        }
        outputs.add(new com.minispark.sql.expr.Alias(e, name));
        return new DataFrame(session, new com.minispark.sql.plan.Project(outputs, logicalPlan));
    }

    /** Group by the given columns; call {@code .agg(...)} on the result. */
    public GroupedData groupBy(Column... cols) {
        List<Expression> exprs = new ArrayList<>(cols.length);
        for (Column c : cols) exprs.add(c.expr());
        return new GroupedData(session, logicalPlan, exprs);
    }

    public GroupedData groupBy(String... colNames) {
        Column[] cols = new Column[colNames.length];
        for (int i = 0; i < colNames.length; i++) cols[i] = Column.col(colNames[i]);
        return groupBy(cols);
    }

    /** Equi-join with {@code right} on {@code left.key == right.key} (INNER by default). */
    public DataFrame join(DataFrame right, String keyColumn) {
        return join(right, List.of(keyColumn), List.of(keyColumn), com.minispark.sql.plan.JoinType.INNER);
    }

    public DataFrame join(DataFrame right, List<String> leftKeys, List<String> rightKeys,
                          com.minispark.sql.plan.JoinType joinType) {
        List<Expression> lk = new ArrayList<>();
        for (String k : leftKeys) lk.add(Column.col(k).expr());
        List<Expression> rk = new ArrayList<>();
        for (String k : rightKeys) rk.add(Column.col(k).expr());
        return new DataFrame(session,
                new com.minispark.sql.plan.Join(logicalPlan, right.logicalPlan, lk, rk, joinType));
    }

    // ----- actions (eager) -----

    public List<Row> collect() {
        return compile().execute().collect();
    }

    public long count() {
        return compile().execute().count();
    }

    public void show() {
        StructType s = schema();
        System.out.println(String.join(" | ", s.names()));
        for (Row r : collect()) {
            List<String> cells = new ArrayList<>();
            for (int i = 0; i < r.size(); i++) cells.add(String.valueOf(r.get(i)));
            System.out.println(String.join(" | ", cells));
        }
    }

    /** Compile the logical plan all the way to an executable physical plan. */
    public PhysicalPlan compile() {
        LogicalPlan analyzed = session.analyzer().analyze(logicalPlan);
        if (!analyzed.resolved()) {
            throw new IllegalStateException("Plan did not fully resolve:\n" + analyzed.treeString());
        }
        LogicalPlan optimized = session.optimizer().optimize(analyzed);
        return session.planner().plan(optimized);
    }

    /** For inspection/teaching: the analyzed + optimized logical plan tree. */
    public String explain() {
        LogicalPlan analyzed = session.analyzer().analyze(logicalPlan);
        LogicalPlan optimized = session.optimizer().optimize(analyzed);
        return "== Analyzed Logical Plan ==\n" + analyzed.treeString()
                + "\n== Optimized Logical Plan ==\n" + optimized.treeString()
                + "\n== Physical Plan ==\n" + session.planner().plan(optimized).treeString();
    }
}
