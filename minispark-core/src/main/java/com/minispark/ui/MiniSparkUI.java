package com.minispark.ui;

import com.minispark.status.AppStatusStore;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/**
 * A read-only web UI over an {@link AppStatusStore}, mirroring (in miniature)
 * Spark's web UI. Uses the JDK's built-in {@link HttpServer} — no servlet
 * container, no extra dependency — and renders one self-refreshing HTML page
 * showing jobs, stages, and executors.
 *
 * <p>Deliberately tiny: the value is seeing the same data the scheduler acts on,
 * not a production dashboard. The interesting design point is that the UI reads
 * only from the event-sourced store, never touching the live scheduler — so it
 * can't perturb or be perturbed by scheduling.
 *
 * Real Spark equivalent: org.apache.spark.ui.SparkUI
 */
public final class MiniSparkUI {

    private static final Logger LOG = LoggerFactory.getLogger(MiniSparkUI.class);

    private final HttpServer server;
    private final AppStatusStore store;
    private final String appName;

    public MiniSparkUI(String appName, AppStatusStore store, String host, int port) {
        this.appName = appName;
        this.store = store;
        try {
            this.server = HttpServer.create(new InetSocketAddress(host, port), 0);
        } catch (IOException e) {
            throw new RuntimeException("Failed to start MiniSparkUI on " + host + ":" + port, e);
        }
        server.createContext("/", this::handle);
        server.setExecutor(Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "minispark-ui");
            t.setDaemon(true);
            return t;
        }));
        server.start();
        LOG.info("MiniSparkUI for '{}' at http://{}:{}/", appName, host, boundPort());
    }

    public int boundPort() { return server.getAddress().getPort(); }

    public void stop() { server.stop(0); }

    private void handle(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
        byte[] body = render().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private String render() {
        StringBuilder sb = new StringBuilder(4096);
        sb.append("<!doctype html><html><head><meta charset='utf-8'>")
          .append("<meta http-equiv='refresh' content='2'>")
          .append("<title>MiniSpark — ").append(esc(appName)).append("</title>")
          .append("<style>body{font-family:system-ui,Arial,sans-serif;margin:2rem;color:#222}")
          .append("h1{font-size:1.4rem}h2{font-size:1.1rem;margin-top:1.5rem}")
          .append("table{border-collapse:collapse;width:100%;margin-top:.5rem}")
          .append("th,td{border:1px solid #ddd;padding:.4rem .6rem;text-align:left;font-size:.9rem}")
          .append("th{background:#f3f3f3}.RUNNING{color:#b8860b}.SUCCEEDED{color:#197d19}")
          .append(".FAILED{color:#c0392b}.bar{background:#eee;border-radius:3px;overflow:hidden;height:14px;width:120px}")
          .append(".bar>span{display:block;height:100%;background:#197d19}</style></head><body>");

        sb.append("<h1>MiniSpark — ").append(esc(appName)).append("</h1>");

        // Executors
        sb.append("<h2>Executors</h2><table><tr><th>ID</th><th>Host:Port</th><th>Cores</th>")
          .append("<th>Status</th><th>Tasks run</th></tr>");
        for (AppStatusStore.ExecutorView x : store.executors()) {
            sb.append("<tr><td>").append(esc(x.executorId())).append("</td><td>")
              .append(esc(x.host())).append(':').append(x.port()).append("</td><td>")
              .append(x.cores()).append("</td><td class='")
              .append(x.active() ? "RUNNING'>active" : "FAILED'>lost")
              .append("</td><td>").append(x.tasksRun()).append("</td></tr>");
        }
        sb.append("</table>");

        // Jobs
        sb.append("<h2>Jobs</h2><table><tr><th>Job</th><th>Stages</th><th>Status</th></tr>");
        for (AppStatusStore.JobView j : store.jobs()) {
            sb.append("<tr><td>").append(j.jobId()).append("</td><td>")
              .append(j.stageIds()).append("</td><td class='").append(j.status())
              .append("'>").append(j.status()).append("</td></tr>");
        }
        sb.append("</table>");

        // Stages
        sb.append("<h2>Stages</h2><table><tr><th>Stage</th><th>Name</th><th>Progress</th>")
          .append("<th>Failed</th><th>Status</th></tr>");
        for (AppStatusStore.StageView s : store.stages()) {
            int pct = s.numTasks() == 0 ? 0 : (100 * s.completedTasks() / s.numTasks());
            sb.append("<tr><td>").append(s.stageId()).append("</td><td>").append(esc(s.name()))
              .append("</td><td><div class='bar'><span style='width:").append(pct).append("%'></span></div>")
              .append(s.completedTasks()).append('/').append(s.numTasks())
              .append("</td><td>").append(s.failedTasks())
              .append("</td><td class='").append(s.status()).append("'>").append(s.status())
              .append("</td></tr>");
        }
        sb.append("</table></body></html>");
        return sb.toString();
    }

    private static String esc(String s) {
        return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}
