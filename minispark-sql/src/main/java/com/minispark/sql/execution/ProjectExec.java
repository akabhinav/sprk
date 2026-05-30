package com.minispark.sql.execution;

import com.minispark.rdd.RDD;
import com.minispark.sql.Row;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.types.StructType;

import java.util.List;

/**
 * Physical projection: maps each input row to a new row of evaluated
 * expressions. Runs as an RDD {@code map} — a narrow transformation, so it
 * pipelines with neighbours and needs no shuffle.
 *
 * Real Spark equivalent: org.apache.spark.sql.execution.ProjectExec.
 */
public final class ProjectExec implements PhysicalPlan {

    private final List<Expression> projectList;
    private final StructType schema;
    private final PhysicalPlan child;

    public ProjectExec(List<Expression> projectList, StructType schema, PhysicalPlan child) {
        this.projectList = projectList;
        this.schema = schema;
        this.child = child;
    }

    @Override public StructType schema() { return schema; }

    @Override
    public RDD<Row> execute() {
        // The expression list is serializable and captured by the lambda, so it
        // ships to executors with the task — exactly like any RDD closure.
        List<Expression> exprs = projectList;
        return child.execute().map((RDD.SerializableFunction<Row, Row>) row -> {
            Object[] out = new Object[exprs.size()];
            for (int i = 0; i < exprs.size(); i++) out[i] = exprs.get(i).eval(row);
            return new Row(out);
        });
    }

    @Override public List<PhysicalPlan> children() { return List.of(child); }
    @Override public String toString() { return "ProjectExec " + schema; }
}
