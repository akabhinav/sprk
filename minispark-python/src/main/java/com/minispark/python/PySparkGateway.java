package com.minispark.python;

import com.minispark.api.MiniSparkConf;
import com.minispark.api.MiniSparkContext;
import com.minispark.api.PairRDDFunctions;
import com.minispark.api.Tuple2;
import com.minispark.rdd.RDD;
import com.minispark.sql.DataFrame;
import com.minispark.sql.MiniSparkSession;
import com.minispark.sql.Row;
import com.minispark.sql.types.DataType;
import com.minispark.sql.types.StructField;
import com.minispark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The PySpark control plane — the role real Spark gives to Py4J. A Python
 * process connects over a TCP socket and drives the JVM by sending one-line
 * JSON commands; the gateway holds a registry of live JVM objects
 * (SparkContext, RDD, DataFrame, …) keyed by id, executes the requested method,
 * and returns either a new handle or a materialised value.
 *
 * <p>DataFrame / SQL operations run entirely in the JVM (Catalyst does the
 * work) — no Python touches the executors, the "fast path". Only RDD
 * operations carrying a Python function go through {@link PythonRDD} and fork
 * a worker.
 *
 * <p>Protocol: newline-delimited JSON. Request {@code {"op":..., ...}};
 * response {@code {"ok":true,"ret":<value>}} or {@code {"ok":false,"err":...}}.
 * A returned handle is {@code {"ref":"<id>"}}.
 *
 * Real Spark equivalent: org.apache.spark.api.python.Py4JServer / PythonGatewayServer.
 */
public final class PySparkGateway {

    private static final Logger LOG = LoggerFactory.getLogger(PySparkGateway.class);

    private final Map<String, Object> registry = new ConcurrentHashMap<>();
    private final AtomicLong ids = new AtomicLong();

    private String put(Object o) { String id = "o" + ids.incrementAndGet(); registry.put(id, o); return id; }
    private Object get(Map<String, Object> req, String field) { return registry.get((String) req.get(field)); }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 0;
        new PySparkGateway().serve(port);
    }

    /** Bind, print the chosen port (so the launcher can hand it to Python), serve one client. */
    public void serve(int port) throws Exception {
        try (ServerSocket server = new ServerSocket()) {
            server.bind(new InetSocketAddress("127.0.0.1", port));
            int bound = server.getLocalPort();
            // The launcher reads this line to learn the port.
            System.out.println("MINISPARK_GATEWAY_PORT=" + bound);
            System.out.flush();
            LOG.info("PySparkGateway listening on 127.0.0.1:{}", bound);
            try (Socket client = server.accept();
                 BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8));
                 BufferedWriter out = new BufferedWriter(new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.isEmpty()) continue;
                    out.write(handle(line)); out.write('\n'); out.flush();
                }
            }
        }
        LOG.info("PySparkGateway client disconnected; exiting");
    }

    private String handle(String line) {
        try {
            Map<String, Object> req = Json.readObject(line);
            Object ret = dispatch((String) req.get("op"), req);
            return Json.write(Map.of("ok", true, "ret", ret == null ? nullVal() : ret));
        } catch (Throwable t) {
            LOG.warn("command failed: {}", t.toString());
            return Json.write(Map.of("ok", false, "err", t.getClass().getSimpleName() + ": " + t.getMessage()));
        }
    }

    private static Object nullVal() { return new java.util.HashMap<>() {{ put("null", true); }}; }

    @SuppressWarnings("unchecked")
    private Object dispatch(String op, Map<String, Object> req) {
        switch (op) {
            // ---- context ----
            case "newContext": {
                MiniSparkConf conf = new MiniSparkConf()
                        .setMaster((String) req.getOrDefault("master", "local[*]"))
                        .setAppName((String) req.getOrDefault("appName", "pyminispark"));
                Map<String, Object> extra = (Map<String, Object>) req.getOrDefault("conf", Map.of());
                for (Map.Entry<String, Object> e : extra.entrySet()) conf.set(e.getKey(), String.valueOf(e.getValue()));
                return ref(put(new MiniSparkContext(conf)));
            }
            case "stop": {
                MiniSparkContext sc = (MiniSparkContext) get(req, "ctx");
                if (sc != null) sc.close();
                return null;
            }

            // ---- RDD sources ----
            case "parallelize": {
                MiniSparkContext sc = (MiniSparkContext) get(req, "ctx");
                List<Object> data = (List<Object>) req.get("data");
                int slices = ((Number) req.getOrDefault("slices", 2)).intValue();
                return ref(put(sc.parallelize(new ArrayList<>(data), slices)));
            }
            case "textFile": {
                MiniSparkContext sc = (MiniSparkContext) get(req, "ctx");
                int slices = ((Number) req.getOrDefault("slices", 2)).intValue();
                RDD<?> rdd = sc.textFile((String) req.get("path"), slices);
                return ref(put(rdd));
            }

            // ---- RDD python transforms (data plane) ----
            case "pyMap":     return ref(put(new PythonRDD(asObjRdd(get(req, "rdd")), pyFunc(req, PythonFunction.EvalType.MAP))));
            case "pyFilter":  return ref(put(new PythonRDD(asObjRdd(get(req, "rdd")), pyFunc(req, PythonFunction.EvalType.FILTER))));
            case "pyFlatMap": return ref(put(new PythonRDD(asObjRdd(get(req, "rdd")), pyFunc(req, PythonFunction.EvalType.FLATMAP))));
            case "reduceByKey": {
                RDD<Object> parent = asObjRdd(get(req, "rdd"));
                // element o is a 2-list [k,v] (python tuple) → pair RDD, group, python-fold each group.
                RDD<Tuple2<Object, Object>> pairs = parent.map(
                        (RDD.SerializableFunction<Object, Tuple2<Object, Object>>) o -> {
                            List<?> l = (List<?>) o; return new Tuple2<>(l.get(0), l.get(1));
                        });
                RDD<Tuple2<Object, List<Object>>> grouped = new PairRDDFunctions<>(pairs).groupByKey();
                @SuppressWarnings({"rawtypes", "unchecked"})
                RDD<Object> groupedObj = (RDD) grouped;
                return ref(put(new PythonRDD(groupedObj, pyFunc(req, PythonFunction.EvalType.GROUP_REDUCE))));
            }

            // ---- RDD actions ----
            case "collect": {
                List<Object> out = new ArrayList<>();
                for (Object o : asObjRdd(get(req, "rdd")).collect()) out.add(toPy(o));
                return out;
            }
            case "count": return asObjRdd(get(req, "rdd")).count();
            case "take": {
                int n = ((Number) req.get("n")).intValue();
                List<Object> out = new ArrayList<>();
                for (Object o : asObjRdd(get(req, "rdd")).take(n)) out.add(toPy(o));
                return out;
            }

            // ---- SQL / DataFrame (native, no python) ----
            case "newSession": return ref(put(MiniSparkSession.on((MiniSparkContext) get(req, "ctx"))));
            case "createDataFrame": {
                MiniSparkSession sp = (MiniSparkSession) get(req, "session");
                StructType schema = parseSchema((List<Object>) req.get("schema"));
                List<Row> rows = new ArrayList<>();
                for (Object r : (List<Object>) req.get("rows")) {
                    List<Object> cells = (List<Object>) r;
                    Object[] vals = new Object[cells.size()];
                    for (int i = 0; i < cells.size(); i++) vals[i] = coerce(cells.get(i), schema.type(i));
                    rows.add(new Row(vals));
                }
                return ref(put(sp.createDataFrame(rows, schema)));
            }
            case "readCsv": {
                MiniSparkSession sp = (MiniSparkSession) get(req, "session");
                DataFrame df = sp.read()
                        .option("header", Boolean.TRUE.equals(req.getOrDefault("header", false)))
                        .option("inferSchema", Boolean.TRUE.equals(req.getOrDefault("inferSchema", false)))
                        .csv((String) req.get("path"));
                return ref(put(df));
            }
            case "sql": return ref(put(((MiniSparkSession) get(req, "session")).sql((String) req.get("query"))));
            case "createOrReplaceTempView":
                ((DataFrame) get(req, "df")).createOrReplaceTempView((String) req.get("name"));
                return null;
            case "dfSelect": {
                List<Object> cols = (List<Object>) req.get("cols");
                return ref(put(((DataFrame) get(req, "df")).select(cols.toArray(new String[0]))));
            }
            case "dfFilter":
                // df.filter("x > 1") — route through the SQL engine via a scratch view.
                return ref(put(filterViaSql((DataFrame) get(req, "df"), (String) req.get("condition"))));
            case "dfCount": return ((DataFrame) get(req, "df")).count();
            case "dfColumns": return ((DataFrame) get(req, "df")).schema().names();
            case "dfCollect": return collectRows((DataFrame) get(req, "df"));
            case "dfShow": return showString((DataFrame) get(req, "df"),
                    ((Number) req.getOrDefault("n", 20)).intValue());

            default: throw new IllegalArgumentException("unknown op: " + op);
        }
    }

    // ----- helpers -----

    private static Map<String, Object> ref(String id) { Map<String, Object> m = new java.util.HashMap<>(); m.put("ref", id); return m; }

    @SuppressWarnings("unchecked")
    private static RDD<Object> asObjRdd(Object rdd) { return (RDD<Object>) rdd; }

    private static PythonFunction pyFunc(Map<String, Object> req, PythonFunction.EvalType type) {
        byte[] command = Base64.getDecoder().decode((String) req.get("func"));
        return new PythonFunction(command, type);
    }

    private static StructType parseSchema(List<Object> fields) {
        List<StructField> sf = new ArrayList<>();
        for (Object f : fields) {
            List<?> pair = (List<?>) f;
            sf.add(StructField.of((String) pair.get(0), dataType((String) pair.get(1))));
        }
        return new StructType(sf);
    }

    /** JSON value (Long/Double/String/Boolean/null) → the boxed type a column expects. */
    private static Object coerce(Object v, DataType t) {
        if (v == null) return null;
        if (t == DataType.INT) return ((Number) v).intValue();
        if (t == DataType.LONG) return ((Number) v).longValue();
        if (t == DataType.DOUBLE) return ((Number) v).doubleValue();
        if (t == DataType.BOOLEAN) return (v instanceof Boolean b) ? b : Boolean.parseBoolean(String.valueOf(v));
        return String.valueOf(v); // STRING
    }

    private static DataType dataType(String t) {
        return switch (t.toLowerCase()) {
            case "int", "integer" -> DataType.INT;
            case "long", "bigint" -> DataType.LONG;
            case "double", "float" -> DataType.DOUBLE;
            case "bool", "boolean" -> DataType.BOOLEAN;
            default -> DataType.STRING;
        };
    }

    private DataFrame filterViaSql(DataFrame df, String condition) {
        String view = "__pyfilter_" + ids.incrementAndGet();
        df.createOrReplaceTempView(view);
        // Reuse the df's session via the gateway's most recent — but DataFrame
        // doesn't expose its session; register+query through any session we hold.
        MiniSparkSession sp = anySession();
        return sp.sql("SELECT * FROM " + view + " WHERE " + condition);
    }

    private MiniSparkSession anySession() {
        for (Object o : registry.values()) if (o instanceof MiniSparkSession s) return s;
        throw new IllegalStateException("no active session");
    }

    private static List<Object> collectRows(DataFrame df) {
        List<Object> out = new ArrayList<>();
        for (Row r : df.collect()) {
            List<Object> cells = new ArrayList<>(r.size());
            for (int i = 0; i < r.size(); i++) cells.add(toPy(r.get(i)));
            out.add(cells);
        }
        return out;
    }

    /** Render a DataFrame as a text table (the df.show() string), built JVM-side. */
    private static String showString(DataFrame df, int n) {
        StructType s = df.schema();
        List<String> names = s.names();
        List<Row> rows = df.collect();
        if (rows.size() > n) rows = rows.subList(0, n);
        int[] w = new int[names.size()];
        for (int i = 0; i < names.size(); i++) w[i] = names.get(i).length();
        for (Row r : rows) for (int i = 0; i < r.size(); i++)
            w[i] = Math.max(w[i], String.valueOf(r.isNullAt(i) ? "null" : r.get(i)).length());
        StringBuilder sb = new StringBuilder();
        sb.append(rowLine(w));
        sb.append(cells(names.toArray(new String[0]), w));
        sb.append(rowLine(w));
        for (Row r : rows) {
            String[] c = new String[r.size()];
            for (int i = 0; i < r.size(); i++) c[i] = r.isNullAt(i) ? "null" : String.valueOf(r.get(i));
            sb.append(cells(c, w));
        }
        sb.append(rowLine(w));
        return sb.toString();
    }

    private static String rowLine(int[] w) {
        StringBuilder sb = new StringBuilder("+");
        for (int x : w) { sb.append("-".repeat(x + 2)); sb.append('+'); }
        return sb.append('\n').toString();
    }

    private static String cells(String[] c, int[] w) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < c.length; i++) {
            sb.append(' ').append(c[i]);
            sb.append(" ".repeat(w[i] - c[i].length())).append(" |");
        }
        return sb.append('\n').toString();
    }

    /** JVM value → JSON-encodable (Tuple2 → list, Row values already primitive). Null-safe. */
    private static Object toPy(Object v) {
        if (v == null) return null;
        if (v instanceof Tuple2<?, ?> t) {
            List<Object> out = new ArrayList<>(2); out.add(toPy(t._1())); out.add(toPy(t._2())); return out;
        }
        if (v instanceof List<?> l) {
            List<Object> out = new ArrayList<>(l.size()); for (Object o : l) out.add(toPy(o)); return out;
        }
        return v;
    }
}
