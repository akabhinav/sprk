package com.minispark.python;

import com.minispark.api.Tuple2;
import com.minispark.executor.TaskContext;
import com.minispark.rdd.Dependency;
import com.minispark.rdd.OneToOneDependency;
import com.minispark.rdd.Partition;
import com.minispark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The PySpark data plane: an RDD whose {@code compute} runs each partition
 * through a forked {@code python} worker, so a user's Python lambda executes
 * on the data. Records are piped over the worker's stdin/stdout as
 * length-prefixed JSON; the function itself travels as cloudpickle bytes the
 * JVM never inspects.
 *
 * <p>A dedicated writer thread feeds the worker while the main thread reads its
 * output — without that split, a partition larger than the OS pipe buffer would
 * deadlock (worker blocked writing results, JVM blocked writing input). This is
 * the same shape as real Spark's {@code PythonRunner}.
 *
 * <p>Every executor host must have a {@code python} interpreter with
 * {@code cloudpickle} installed (set {@code MINISPARK_PYTHON} to choose the
 * binary; defaults to {@code python3}). Real Spark has the identical
 * requirement.
 *
 * Real Spark equivalent: org.apache.spark.api.python.PythonRDD / PythonRunner.
 */
public final class PythonRDD extends RDD<Object> {

    private static final Logger LOG = LoggerFactory.getLogger(PythonRDD.class);
    private static final int END_OF_DATA = -1;

    private final RDD<Object> parent;
    private final PythonFunction func;

    public PythonRDD(RDD<Object> parent, PythonFunction func) {
        super(parent.context());
        this.parent = parent;
        this.func = func;
    }

    @Override public List<Partition> getPartitions() { return parent.getPartitions(); }
    @Override public List<Dependency<?>> getDependencies() { return List.of(new OneToOneDependency<>(parent)); }

    @Override
    public Iterator<Object> compute(Partition split, TaskContext ctx) {
        Iterator<Object> in = parent.iterator(split, ctx);
        Process worker = startWorker();
        List<Object> results = new ArrayList<>();
        Throwable[] writerError = new Throwable[1];

        // Writer thread: stream the partition's records to the worker.
        Thread writer = new Thread(() -> {
            try (DataOutputStream out = new DataOutputStream(worker.getOutputStream())) {
                out.writeInt(func.evalType().code);
                out.writeInt(func.command().length);
                out.write(func.command());
                while (in.hasNext()) {
                    byte[] rec = Json.write(toPy(in.next())).getBytes(StandardCharsets.UTF_8);
                    out.writeInt(rec.length);
                    out.write(rec);
                }
                out.writeInt(END_OF_DATA);
                out.flush();
            } catch (Throwable t) {
                writerError[0] = t;
            }
        }, "python-writer");
        writer.setDaemon(true);
        writer.start();

        // Reader: pull transformed records back.
        try (DataInputStream din = new DataInputStream(new BufferedInputStream(worker.getInputStream()))) {
            while (true) {
                int len = din.readInt();
                if (len == END_OF_DATA) break;
                byte[] buf = din.readNBytes(len);
                results.add(Json.read(new String(buf, StandardCharsets.UTF_8)));
            }
        } catch (IOException e) {
            throw new RuntimeException("python worker read failed: " + drainStderr(worker), e);
        }

        try {
            writer.join(5000);
            if (writerError[0] != null) throw new RuntimeException("python worker write failed", writerError[0]);
            int code = worker.waitFor();
            if (code != 0) throw new RuntimeException("python worker exited " + code + ": " + drainStderr(worker));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            worker.destroyForcibly();
        }
        return results.iterator();
    }

    // ----- JVM record → JSON-encodable shape -----
    private static Object toPy(Object v) {
        if (v instanceof Tuple2<?, ?> t) {
            List<Object> l = new ArrayList<>(2);
            l.add(toPy(t._1())); l.add(toPy(t._2()));
            return l;
        }
        if (v instanceof List<?> list) {
            List<Object> l = new ArrayList<>(list.size());
            for (Object o : list) l.add(toPy(o));
            return l;
        }
        if (v instanceof Object[] arr) {
            List<Object> l = new ArrayList<>(arr.length);
            for (Object o : arr) l.add(toPy(o));
            return l;
        }
        return v; // String / Number / Boolean / null pass through
    }

    // ----- worker process management -----

    private static volatile Path workerScript;

    private Process startWorker() {
        try {
            Path script = ensureWorkerScript();
            String python = System.getenv().getOrDefault("MINISPARK_PYTHON", "python3");
            ProcessBuilder pb = new ProcessBuilder(python, script.toString());
            pb.redirectErrorStream(false);
            return pb.start();
        } catch (IOException e) {
            throw new RuntimeException("failed to start python worker (is python3 + cloudpickle installed?)", e);
        }
    }

    /** Extract the bundled worker script to a temp file once per JVM. */
    private static Path ensureWorkerScript() throws IOException {
        Path s = workerScript;
        if (s != null && Files.exists(s)) return s;
        synchronized (PythonRDD.class) {
            if (workerScript != null && Files.exists(workerScript)) return workerScript;
            try (InputStream res = PythonRDD.class.getResourceAsStream("/minispark_worker.py")) {
                if (res == null) throw new IOException("minispark_worker.py resource missing");
                Path tmp = Files.createTempFile("minispark_worker", ".py");
                Files.write(tmp, res.readAllBytes());
                try { Files.setPosixFilePermissions(tmp, PosixFilePermissions.fromString("rwxr-xr-x")); }
                catch (UnsupportedOperationException ignore) { /* non-posix */ }
                tmp.toFile().deleteOnExit();
                workerScript = tmp;
                return tmp;
            }
        }
    }

    private static String drainStderr(Process p) {
        try (InputStream err = p.getErrorStream()) {
            return new String(err.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "(could not read stderr)";
        }
    }
}
