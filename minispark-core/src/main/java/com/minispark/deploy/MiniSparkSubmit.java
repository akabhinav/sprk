package com.minispark.deploy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The MiniSpark equivalent of {@code spark-submit}: a launcher that parses
 * submission options, publishes them as {@code minispark.*} system properties
 * (which {@link com.minispark.api.MiniSparkConf} reads), then reflectively
 * invokes the application's {@code main(String[])}.
 *
 * <p>Because the app runs <i>in this same JVM</i> after the properties are set,
 * a {@code new MiniSparkConf()} inside the app picks up the submitted master,
 * executor count, etc. without the app hard-coding anything — exactly how a real
 * Spark job written against a bare {@code SparkConf} behaves under spark-submit.
 *
 * <pre>
 * Usage:
 *   MiniSparkSubmit --class &lt;FQCN&gt; [options] [app.jar] [app args...]
 *
 * Options (mapped to minispark.* config):
 *   --class &lt;name&gt;            application main class (required)
 *   --master &lt;url&gt;            local[N] | netty | miniyarn://host:port   (minispark.master)
 *   --name &lt;str&gt;              application name                          (minispark.app.name)
 *   --num-executors &lt;n&gt;       executor count                            (minispark.executor.instances)
 *   --executor-cores &lt;n&gt;      cores per executor                        (minispark.executor.cores)
 *   --executor-memory &lt;mb&gt;    executor memory in MB                     (minispark.executor.memoryMB)
 *   --conf k=v                arbitrary config (repeatable)
 *   --jars / --jar / *.jar    added to the classpath (best-effort; see note)
 * </pre>
 *
 * Real Spark equivalent: org.apache.spark.deploy.SparkSubmit.
 */
public final class MiniSparkSubmit {

    /** Maps a submit flag to the config key it sets. */
    private static final Map<String, String> FLAG_TO_KEY = Map.of(
            "--master", "minispark.master",
            "--name", "minispark.app.name",
            "--num-executors", "minispark.executor.instances",
            "--executor-cores", "minispark.executor.cores",
            "--executor-memory", "minispark.executor.memoryMB");

    public static void main(String[] args) throws Exception {
        Submission s = parse(args);
        if (s.mainClass == null) {
            usage("--class is required");
            return;
        }

        // A master that implies remote executors must use the netty transport.
        // spark-submit makes the same implicit choice (cluster managers ⇒ remote).
        String master = s.conf.getOrDefault("minispark.master", "local[*]");
        boolean remote = !master.startsWith("local");
        if (remote && !s.conf.containsKey("minispark.rpc.mode")) {
            s.conf.put("minispark.rpc.mode", "netty");
        }

        // Publish every resolved config as a system property; MiniSparkConf reads
        // these, and (crucially) the executor-launcher forwards the registered
        // ones to the child executor JVMs.
        s.conf.forEach(System::setProperty);

        System.out.println("=== minispark-submit ===");
        System.out.println("  class   : " + s.mainClass);
        s.conf.forEach((k, v) -> System.out.println("  " + k + " = " + v));
        if (!s.jars.isEmpty()) System.out.println("  jars    : " + s.jars);
        System.out.println("  appArgs : " + s.appArgs);
        System.out.println("========================");

        invokeMain(s.mainClass, s.appArgs.toArray(new String[0]));
    }

    /** Reflectively call {@code static void main(String[])} on the app class. */
    private static void invokeMain(String className, String[] appArgs) throws Exception {
        Class<?> clazz;
        try {
            clazz = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("application class not found on classpath: " + className, e);
        }
        Method main;
        try {
            main = clazz.getDeclaredMethod("main", String[].class);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(className + " has no 'public static void main(String[])'", e);
        }
        if (!Modifier.isStatic(main.getModifiers())) {
            throw new IllegalArgumentException(className + ".main must be static");
        }
        main.setAccessible(true);
        try {
            main.invoke(null, (Object) appArgs);
        } catch (InvocationTargetException e) {
            // Surface the application's real exception, not the reflection wrapper.
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw new RuntimeException("application main threw", cause);
        }
    }

    // ----- argument parsing -----

    /** Parsed submission: config map, main class, jars, and the app's own args. */
    static final class Submission {
        final Map<String, String> conf = new LinkedHashMap<>();
        String mainClass;
        final List<String> jars = new ArrayList<>();
        final List<String> appArgs = new ArrayList<>();
    }

    /**
     * Parse submit options up to the first positional token; everything after the
     * recognized options (a {@code *.jar} or the first bare token) is the
     * application's own argument list — matching spark-submit's
     * "[options] app.jar [app args]" shape.
     */
    static Submission parse(String[] args) {
        Submission s = new Submission();
        int i = 0;
        while (i < args.length) {
            String a = args[i];
            if ("--class".equals(a)) {
                s.mainClass = need(args, ++i, "--class");
                i++;
            } else if (FLAG_TO_KEY.containsKey(a)) {
                s.conf.put(FLAG_TO_KEY.get(a), need(args, ++i, a));
                i++;
            } else if ("--conf".equals(a)) {
                String kv = need(args, ++i, "--conf");
                int eq = kv.indexOf('=');
                if (eq < 0) throw new IllegalArgumentException("--conf expects key=value, got: " + kv);
                s.conf.put(kv.substring(0, eq), kv.substring(eq + 1));
                i++;
            } else if ("--jars".equals(a) || "--jar".equals(a)) {
                for (String j : need(args, ++i, a).split(",")) if (!j.isBlank()) s.jars.add(j.trim());
                i++;
            } else if (a.endsWith(".jar")) {
                // The application jar marks the boundary: it + the rest are app args' context.
                s.jars.add(a);
                i++;
                while (i < args.length) s.appArgs.add(args[i++]);
            } else if (a.startsWith("--")) {
                throw new IllegalArgumentException("unknown option: " + a);
            } else {
                // First bare positional with no app jar: treat the rest as app args.
                while (i < args.length) s.appArgs.add(args[i++]);
            }
        }
        return s;
    }

    private static String need(String[] args, int idx, String flag) {
        if (idx >= args.length) throw new IllegalArgumentException(flag + " requires a value");
        return args[idx];
    }

    private static void usage(String error) {
        if (error != null) System.err.println("error: " + error);
        System.err.println("""
            Usage: minispark-submit --class <FQCN> [options] [app.jar] [app args...]
              --master <url>          local[N] | netty | miniyarn://host:port
              --name <str>            application name
              --num-executors <n>     executor count
              --executor-cores <n>    cores per executor
              --executor-memory <mb>  executor memory (MB)
              --conf key=value        arbitrary minispark.* config (repeatable)
            """);
        // Non-zero exit so scripts can detect the misuse.
        throw new IllegalArgumentException(error == null ? "usage" : error);
    }
}
