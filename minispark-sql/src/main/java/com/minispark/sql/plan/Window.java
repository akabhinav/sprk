package com.minispark.sql.plan;

import com.minispark.sql.expr.window.WindowExpression;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Logical operator that adds one column per {@link WindowExpression} to its
 * child's output. The child's columns are preserved verbatim; the window
 * outputs are appended in declaration order, each named by the user.
 *
 * <p>This separation from {@link Project} mirrors real Spark: the SQL
 * analyzer / DSL builder extracts window expressions from SELECT lists into
 * dedicated {@code Window} nodes so the planner has one obvious operator to
 * lower. {@code WindowExec} then shuffles by partitionBy, sorts each
 * partition by orderBy, and applies the functions.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.plans.logical.Window.
 */
public final class Window implements LogicalPlan {

    private final List<WindowExpression> windowExprs;
    private final List<String> outputNames;
    private final LogicalPlan child;

    public Window(List<WindowExpression> windowExprs, List<String> outputNames, LogicalPlan child) {
        if (windowExprs.size() != outputNames.size()) {
            throw new IllegalArgumentException(
                    "windowExprs and outputNames must have the same size; got "
                            + windowExprs.size() + " vs " + outputNames.size());
        }
        this.windowExprs = List.copyOf(windowExprs);
        this.outputNames = List.copyOf(outputNames);
        this.child = child;
    }

    public List<WindowExpression> windowExprs() { return windowExprs; }
    public List<String> outputNames() { return outputNames; }
    public LogicalPlan child() { return child; }

    @Override
    public StructType schema() {
        List<StructField> fields = new ArrayList<>(child.schema().fields());
        for (int i = 0; i < windowExprs.size(); i++) {
            DataType dt = windowExprs.get(i).dataType(child.schema());
            // Window functions never return null on a non-null partition (ranking
            // always defined; aggregate windows would also use 0/null for empty
            // frames, which doesn't apply here yet). Mark nullable=true regardless
            // to be conservative against future frame additions.
            fields.add(new StructField(outputNames.get(i), dt, true));
        }
        return new StructType(fields);
    }

    @Override public List<LogicalPlan> children() { return List.of(child); }
    @Override public LogicalPlan withChildren(List<LogicalPlan> c) {
        return new Window(windowExprs, outputNames, c.get(0));
    }

    @Override public boolean resolved() {
        if (!child.resolved()) return false;
        for (WindowExpression w : windowExprs) if (!w.resolved()) return false;
        return true;
    }

    @Override public String toString() {
        return "Window(" + outputNames + " = " + windowExprs + ")";
    }
}
