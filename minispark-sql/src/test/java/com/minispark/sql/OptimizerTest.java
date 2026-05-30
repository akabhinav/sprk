package com.minispark.sql;

import com.minispark.sql.analysis.Analyzer;
import com.minispark.sql.expr.Arithmetic;
import com.minispark.sql.expr.Expression;
import com.minispark.sql.expr.Literal;
import com.minispark.sql.optimizer.Optimizer;
import com.minispark.sql.plan.Filter;
import com.minispark.sql.plan.LocalRelation;
import com.minispark.sql.plan.LogicalPlan;
import com.minispark.sql.plan.Project;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.minispark.sql.Column.col;
import static com.minispark.sql.Column.lit;
import static org.assertj.core.api.Assertions.assertThat;

/** Verifies the optimizer rules actually rewrite the logical plan. */
final class OptimizerTest {

    private final StructType schema = StructType.of(
            StructField.of("a", DataType.INT),
            StructField.of("b", DataType.INT));

    private final Analyzer analyzer = new Analyzer();
    private final Optimizer optimizer = new Optimizer();

    private LogicalPlan rel() {
        return new LocalRelation(schema, List.of(Row.of(1, 2)));
    }

    @Test
    void constant_folding_collapses_literal_arithmetic() {
        // SELECT (2 + 3) AS c, a
        Expression folded5 = new Arithmetic(Arithmetic.Op.ADD, Literal.of(2), Literal.of(3));
        Project p = new Project(
                List.of(new com.minispark.sql.expr.Alias(folded5, "c"), col("a").expr()),
                rel());

        LogicalPlan optimized = optimizer.optimize(analyzer.analyze(p));
        // The (2+3) subtree must now be a single Literal 5.
        Project op = (Project) optimized;
        Expression first = ((com.minispark.sql.expr.Alias) op.projectList().get(0)).child();
        assertThat(first).isInstanceOf(Literal.class);
        assertThat(((Literal) first).value()).isEqualTo(5L);
    }

    @Test
    void filter_is_pushed_below_projection() {
        // Project([a], Filter(a > 1, rel))  <- already; build Filter above Project:
        // Filter(a > 1, Project([a, b], rel))  → Project([a, b], Filter(a > 1, rel))
        Project proj = new Project(List.of(col("a").expr(), col("b").expr()), rel());
        Filter filter = new Filter(col("a").gt(lit(1)).expr(), proj);

        LogicalPlan optimized = optimizer.optimize(analyzer.analyze(filter));

        // Top node should now be the Project, with a Filter beneath it.
        assertThat(optimized).isInstanceOf(Project.class);
        Project topProject = (Project) optimized;
        assertThat(topProject.child()).isInstanceOf(Filter.class);
    }

    @Test
    void adjacent_filters_are_combined() {
        Filter inner = new Filter(col("a").gt(lit(0)).expr(), rel());
        Filter outer = new Filter(col("b").lt(lit(10)).expr(), inner);

        LogicalPlan optimized = optimizer.optimize(analyzer.analyze(outer));

        // Exactly one Filter should remain (the AND of both), directly over the relation.
        long filterCount = countFilters(optimized);
        assertThat(filterCount).isEqualTo(1);
    }

    private long countFilters(LogicalPlan p) {
        long c = (p instanceof Filter) ? 1 : 0;
        for (LogicalPlan child : p.children()) c += countFilters(child);
        return c;
    }
}
