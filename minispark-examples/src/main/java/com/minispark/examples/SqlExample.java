package com.minispark.examples;

import com.minispark.sql.Column;
import com.minispark.sql.DataFrame;
import com.minispark.sql.MiniSparkSession;
import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;

import java.util.List;

import static com.minispark.sql.Column.col;

/**
 * Demonstrates the Tier-B DataFrame/SQL layer compiling to the RDD engine.
 * Prints the analyzed/optimized/physical plans (so you can see predicate
 * pushdown and constant folding) and the result rows.
 *
 * <pre>
 *   mvn -pl minispark-examples -am install -DskipTests
 *   mvn -pl minispark-examples exec:java -Dexec.mainClass=com.minispark.examples.SqlExample
 * </pre>
 */
public final class SqlExample {

    public static void main(String[] args) {
        StructType schema = StructType.of(
                StructField.of("name", DataType.STRING),
                StructField.of("age", DataType.INT),
                StructField.of("city", DataType.STRING));

        try (MiniSparkSession spark = MiniSparkSession.builder("SqlExample", "local[4]")) {
            DataFrame people = spark.createDataFrame(List.of(
                    Row.of("alice", 30, "NYC"),
                    Row.of("bob", 25, "LA"),
                    Row.of("carol", 40, "NYC"),
                    Row.of("dave", 19, "SF")), schema);

            // Filter first (on the base columns), then project — including a
            // constant-folded arithmetic column (2 + 3 collapses to 5 at optimize time).
            DataFrame query = people
                    .filter(col("city").eq("NYC").and(col("age").gt(20)))
                    .select(col("name"), col("age").plus(Column.lit(2).plus(3)).as("age_plus_5"));

            System.out.println(query.explain());
            System.out.println("== Result (DataFrame DSL) ==");
            query.show();

            // The same query expressed as SQL text via a temp view.
            people.createOrReplaceTempView("people");
            System.out.println("\n== Result (spark.sql) ==");
            spark.sql("SELECT city, count(*), sum(age) FROM people "
                    + "WHERE age > 20 GROUP BY city").show();
        }
    }
}
