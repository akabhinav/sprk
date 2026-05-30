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

    /** The schema this DataFrame produces (computed from the logical plan). */
    public StructType schema() { return logicalPlan.schema(); }

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
