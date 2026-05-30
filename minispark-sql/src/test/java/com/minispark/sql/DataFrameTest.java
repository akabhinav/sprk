package com.minispark.sql;

import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.minispark.sql.Column.col;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end DataFrame execution: build → analyze → optimize → plan → run on
 * the RDD engine, and check results + the produced schema.
 */
final class DataFrameTest {

    private MiniSparkSession spark;

    private final StructType people = StructType.of(
            StructField.of("name", DataType.STRING),
            StructField.of("age", DataType.INT),
            StructField.of("city", DataType.STRING));

    @BeforeEach void setUp() {
        spark = MiniSparkSession.builder("sql-test", "local[2]");
    }

    @AfterEach void tearDown() {
        if (spark != null) spark.close();
    }

    private DataFrame peopleDf() {
        return spark.createDataFrame(List.of(
                Row.of("alice", 30, "NYC"),
                Row.of("bob", 25, "LA"),
                Row.of("carol", 40, "NYC"),
                Row.of("dave", 19, "SF")
        ), people);
    }

    @Test
    void filter_then_select_projects_and_filters() {
        List<Row> result = peopleDf()
                .filter(col("age").ge(30))
                .select(col("name"), col("city"))
                .collect();

        // alice(30,NYC) and carol(40,NYC) pass age>=30.
        assertThat(result).hasSize(2);
        assertThat(result).extracting(r -> r.getString(0))
                .containsExactlyInAnyOrder("alice", "carol");
        assertThat(result).allSatisfy(r -> assertThat(r.size()).isEqualTo(2));
    }

    @Test
    void projection_schema_reflects_aliases_and_arithmetic() {
        DataFrame df = peopleDf().select(
                col("name"),
                col("age").plus(1).as("next_age"));

        StructType s = df.schema();
        assertThat(s.names()).containsExactly("name", "next_age");
        assertThat(s.type(1)).isEqualTo(DataType.LONG); // int + int -> long

        List<Row> rows = df.collect();
        assertThat(rows).extracting(r -> r.getLong(1))
                .containsExactlyInAnyOrder(31L, 26L, 41L, 20L);
    }

    @Test
    void count_runs_through_the_engine() {
        long n = peopleDf().filter(col("city").eq("NYC")).count();
        assertThat(n).isEqualTo(2L);
    }

    @Test
    void compound_predicate_with_and_or() {
        List<Row> result = peopleDf()
                .filter(col("age").gt(20).and(col("city").eq("NYC")))
                .collect();
        assertThat(result).extracting(r -> r.getString(0))
                .containsExactlyInAnyOrder("alice", "carol");
    }

    @Test
    void unresolved_column_fails_fast() {
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> peopleDf().select(col("nonexistent")).collect())
                .hasMessageContaining("nonexistent");
    }
}
