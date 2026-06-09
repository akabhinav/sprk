package com.minispark.python;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test of the PySpark bridge: launches a real Python process that
 * drives the JVM gateway, exercising both planes — DataFrame/SQL (native) and
 * RDD transforms with Python lambdas (forked python workers). The Python side
 * connects back to a {@link PySparkGateway} the JVM under test spawns.
 *
 * <p>Aborts (skips) if {@code python3} or {@code cloudpickle} aren't installed —
 * the bridge's hard requirements, the same ones real PySpark has.
 */
final class PyBridgeTest {

    @Test
    void python_drives_both_planes(@TempDir Path tmp) throws Exception {
        String python = System.getenv().getOrDefault("MINISPARK_PYTHON", "python3");
        Assumptions.assumeTrue(hasModule(python, "cloudpickle"),
                "python3 + cloudpickle required for the PySpark bridge");

        // Locate the repo's python/ package (test runs from the module dir).
        Path pkg = Path.of("..", "python").toAbsolutePath().normalize();
        Assumptions.assumeTrue(Files.isDirectory(pkg.resolve("minispark")),
                "python/minispark package not found at " + pkg);

        Path script = tmp.resolve("bridge_test.py");
        Files.writeString(script, """
                from minispark import SparkSession
                spark = SparkSession.builder("pytest", "local[2]")

                # control plane: DataFrame + SQL, native in the JVM
                df = spark.createDataFrame(
                    [["a", 1], ["b", 2], ["a", 3]],
                    [["k", "string"], ["v", "int"]])
                df.createOrReplaceTempView("t")
                rows = spark.sql("SELECT k, sum(v) AS s FROM t GROUP BY k ORDER BY k").collect()
                assert rows == [["a", 4], ["b", 2]], rows

                # data plane: RDD + python lambdas, forked workers
                sc = spark._sc
                assert sc.parallelize([1, 2, 3, 4], 2).map(lambda x: x * x).collect() == [1, 4, 9, 16]
                assert sc.parallelize([1, 2, 3, 4], 2).filter(lambda x: x % 2 == 0).collect() == [2, 4]
                wc = dict((w, c) for w, c in
                          sc.parallelize(["a b a", "b c"], 2)
                            .flatMap(lambda s: s.split())
                            .map(lambda w: (w, 1))
                            .reduceByKey(lambda a, b: a + b)
                            .collect())
                assert wc == {"a": 2, "b": 2, "c": 1}, wc

                spark.stop()
                print("BRIDGE_OK")
                """, StandardCharsets.UTF_8);

        ProcessBuilder pb = new ProcessBuilder(python, script.toString());
        pb.environment().put("MINISPARK_CLASSPATH", System.getProperty("java.class.path"));
        pb.environment().put("PYTHONPATH", pkg.toString());
        pb.redirectErrorStream(true);
        Process proc = pb.start();
        String out = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        boolean done = proc.waitFor(120, TimeUnit.SECONDS);
        if (!done) { proc.destroyForcibly(); }

        assertThat(out).as("bridge output:\n%s", out).contains("BRIDGE_OK");
        assertThat(proc.exitValue()).isEqualTo(0);
    }

    private static boolean hasModule(String python, String module) {
        try {
            Process p = new ProcessBuilder(python, "-c", "import " + module)
                    .redirectErrorStream(true).start();
            return p.waitFor(20, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
