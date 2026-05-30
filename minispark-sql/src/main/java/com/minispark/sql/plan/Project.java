package com.minispark.sql.plan;

import com.minispark.sql.expr.Expression;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * A projection: {@code SELECT e1, e2, ... FROM child}. Its output schema is one
 * field per projected expression, named by the expression's {@link
 * Expression#name()} and typed against the child's schema.
 *
 * Real Spark equivalent: org.apache.spark.sql.catalyst.plans.logical.Project.
 */
public final class Project implements LogicalPlan {

    private final List<Expression> projectList;
    private final LogicalPlan child;

    public Project(List<Expression> projectList, LogicalPlan child) {
        this.projectList = List.copyOf(projectList);
        this.child = child;
    }

    public List<Expression> projectList() { return projectList; }
    public LogicalPlan child() { return child; }

    @Override
    public StructType schema() {
        StructType in = child.schema();
        List<StructField> fields = new ArrayList<>(projectList.size());
        for (Expression e : projectList) {
            DataType t = e.dataType(in);
            fields.add(StructField.of(e.name(), t));
        }
        return new StructType(fields);
    }

    @Override public List<LogicalPlan> children() { return List.of(child); }
    @Override public LogicalPlan withChildren(List<LogicalPlan> c) {
        return new Project(projectList, c.get(0));
    }

    @Override public boolean resolved() {
        if (!child.resolved()) return false;
        for (Expression e : projectList) if (!e.resolved()) return false;
        return true;
    }

    @Override public String toString() {
        List<String> names = new ArrayList<>();
        for (Expression e : projectList) names.add(e.toString());
        return "Project [" + String.join(", ", names) + "]";
    }
}
