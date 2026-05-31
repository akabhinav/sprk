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
        String path = exchange.getRequestURI().getPath();
        String html = "/dag".equals(path) ? renderDag() : render();
        byte[] body = html.getBytes(StandardCharsets.UTF_8);
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
        sb.append("<p><a href='/'>Overview</a> · <a href='/dag'>DAG visualization</a></p>");

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

    // ----- DAG visualization (/dag) -----

    private static final int NODE_W = 210, NODE_H = 66;
    private static final int COL_STEP = NODE_W + 90;   // horizontal gap between dependency levels
    private static final int ROW_STEP = NODE_H + 34;   // vertical gap between sibling stages
    private static final int MARGIN = 28;

    /**
     * Renders one stage-DAG per job: nodes are stages (id, type, task progress),
     * edges are parent→child dependencies (each edge is a shuffle boundary).
     * Laid out left-to-right by dependency level — parents (which produce the
     * shuffle data) on the left, the ResultStage on the right — so data flows
     * in reading order. Pure inline SVG, no JS/CDN, so it works offline.
     */
    private String renderDag() {
        StringBuilder sb = new StringBuilder(8192);
        sb.append("<!doctype html><html><head><meta charset='utf-8'>")
          .append("<meta http-equiv='refresh' content='2'>")
          .append("<title>MiniSpark DAG — ").append(esc(appName)).append("</title>")
          .append("<style>body{font-family:system-ui,Arial,sans-serif;margin:2rem;color:#222}")
          .append("h1{font-size:1.4rem}h2{font-size:1.05rem;margin-top:1.4rem}")
          .append(".job{border:1px solid #e2e2e2;border-radius:6px;padding:.6rem 1rem 1rem;margin-top:1rem}")
          .append("svg{display:block}.legend{font-size:.8rem;color:#555;margin:.3rem 0 .2rem}")
          .append(".legend b{font-weight:600}</style></head><body>");
        sb.append("<h1>MiniSpark DAG — ").append(esc(appName)).append("</h1>");
        sb.append("<p><a href='/'>Overview</a> · <a href='/dag'>DAG visualization</a></p>");
        sb.append("<div class='legend'>Each box is a <b>stage</b> (a set of pipelined tasks); "
                + "each arrow is a <b>shuffle boundary</b> where the parent stage's output is "
                + "re-partitioned for its child. Colour = status "
                + "(<span class='RUNNING'>running</span>, "
                + "<span class='SUCCEEDED'>succeeded</span>, "
                + "<span class='FAILED'>failed</span>).</div>");

        // Index stages by id for quick lookup; one diagram per job.
        java.util.Map<Integer, AppStatusStore.StageView> byId = new java.util.HashMap<>();
        for (AppStatusStore.StageView s : store.stages()) byId.put(s.stageId(), s);

        java.util.List<AppStatusStore.JobView> jobs = store.jobs();
        if (jobs.isEmpty()) {
            sb.append("<p>No jobs have run yet.</p></body></html>");
            return sb.toString();
        }
        for (AppStatusStore.JobView job : jobs) {
            sb.append("<div class='job'><h2>Job ").append(job.jobId())
              .append(" <span class='").append(job.status()).append("'>")
              .append(job.status()).append("</span></h2>");
            appendJobDag(sb, job, byId);
            sb.append("</div>");
        }
        sb.append("</body></html>");
        return sb.toString();
    }

    private void appendJobDag(StringBuilder sb, AppStatusStore.JobView job,
                              java.util.Map<Integer, AppStatusStore.StageView> byId) {
        // Collect this job's stages (skip any not yet recorded in the store).
        java.util.List<AppStatusStore.StageView> stages = new java.util.ArrayList<>();
        for (int id : job.stageIds()) {
            AppStatusStore.StageView s = byId.get(id);
            if (s != null) stages.add(s);
        }
        if (stages.isEmpty()) { sb.append("<p>(stages not yet submitted)</p>"); return; }

        // Level = longest path from a root (stage with no parents in this job),
        // memoized. Edges only count parents that belong to this job.
        java.util.Set<Integer> jobIds = new java.util.HashSet<>(job.stageIds());
        java.util.Map<Integer, Integer> level = new java.util.HashMap<>();
        for (AppStatusStore.StageView s : stages) computeLevel(s.stageId(), byId, jobIds, level);

        // Bucket stages by level and assign a row within each level.
        java.util.Map<Integer, java.util.List<AppStatusStore.StageView>> byLevel =
                new java.util.TreeMap<>();
        for (AppStatusStore.StageView s : stages) {
            byLevel.computeIfAbsent(level.get(s.stageId()), k -> new java.util.ArrayList<>()).add(s);
        }
        int maxLevel = 0, maxRows = 1;
        for (var e : byLevel.entrySet()) {
            maxLevel = Math.max(maxLevel, e.getKey());
            maxRows = Math.max(maxRows, e.getValue().size());
        }

        // Position each node: x by level, y by row within level.
        java.util.Map<Integer, int[]> pos = new java.util.HashMap<>(); // stageId -> [x,y]
        for (var e : byLevel.entrySet()) {
            int lvl = e.getKey();
            java.util.List<AppStatusStore.StageView> col = e.getValue();
            for (int row = 0; row < col.size(); row++) {
                int x = MARGIN + lvl * COL_STEP;
                int y = MARGIN + row * ROW_STEP;
                pos.put(col.get(row).stageId(), new int[]{x, y});
            }
        }
        int width = MARGIN * 2 + (maxLevel + 1) * COL_STEP - 90;
        int height = MARGIN * 2 + maxRows * ROW_STEP - 34;

        sb.append("<svg width='").append(width).append("' height='").append(height)
          .append("' viewBox='0 0 ").append(width).append(' ').append(height).append("'>");
        // Arrowhead marker.
        sb.append("<defs><marker id='arr' viewBox='0 0 10 10' refX='9' refY='5' "
                + "markerWidth='7' markerHeight='7' orient='auto-start-reverse'>"
                + "<path d='M0,0 L10,5 L0,10 z' fill='#888'/></marker></defs>");

        // Edges first (so nodes draw on top). The scheduler records the FULL
        // ancestor set as a stage's parents (a scheduling convenience), so a
        // chain A→B→C also lists A as a parent of C. Draw only IMMEDIATE edges
        // via transitive reduction: skip A→C when A is also an ancestor of some
        // other parent B of C.
        java.util.Map<Integer, java.util.Set<Integer>> ancestors = new java.util.HashMap<>();
        for (AppStatusStore.StageView s : stages) ancestorsOf(s.stageId(), byId, jobIds, ancestors);
        for (AppStatusStore.StageView s : stages) {
            int[] cp = pos.get(s.stageId());
            java.util.List<Integer> parents = s.parentStageIds();
            for (int parentId : parents) {
                int[] pp = pos.get(parentId);
                if (pp == null) continue;  // parent outside this job
                boolean redundant = false;
                for (int other : parents) {
                    if (other != parentId && ancestors.getOrDefault(other, java.util.Set.of()).contains(parentId)) {
                        redundant = true; break;
                    }
                }
                if (redundant) continue;
                int x1 = pp[0] + NODE_W, y1 = pp[1] + NODE_H / 2;   // parent right-center
                int x2 = cp[0],           y2 = cp[1] + NODE_H / 2;  // child left-center
                int mx = (x1 + x2) / 2;
                sb.append("<path d='M").append(x1).append(',').append(y1)
                  .append(" C").append(mx).append(',').append(y1).append(' ')
                  .append(mx).append(',').append(y2).append(' ')
                  .append(x2).append(',').append(y2)
                  .append("' fill='none' stroke='#888' stroke-width='1.5' marker-end='url(#arr)'/>");
            }
        }

        // Nodes.
        for (AppStatusStore.StageView s : stages) {
            int[] p = pos.get(s.stageId());
            String fill = switch (s.status()) {
                case SUCCEEDED -> "#e6f4ea";
                case FAILED -> "#fdecea";
                default -> "#fff6e0";
            };
            String stroke = switch (s.status()) {
                case SUCCEEDED -> "#197d19";
                case FAILED -> "#c0392b";
                default -> "#b8860b";
            };
            int pct = s.numTasks() == 0 ? 0 : (100 * s.completedTasks() / s.numTasks());
            sb.append("<g>");
            sb.append("<rect x='").append(p[0]).append("' y='").append(p[1])
              .append("' width='").append(NODE_W).append("' height='").append(NODE_H)
              .append("' rx='7' fill='").append(fill).append("' stroke='").append(stroke)
              .append("' stroke-width='1.5'/>");
            // Title line: "Stage N · ShuffleMapStage"
            sb.append("<text x='").append(p[0] + 12).append("' y='").append(p[1] + 22)
              .append("' font-size='13' font-weight='600' fill='#222'>Stage ")
              .append(s.stageId()).append(" · ").append(esc(s.name())).append("</text>");
            // Progress text.
            sb.append("<text x='").append(p[0] + 12).append("' y='").append(p[1] + 41)
              .append("' font-size='11.5' fill='#555'>")
              .append(s.completedTasks()).append('/').append(s.numTasks())
              .append(" tasks").append(s.failedTasks() > 0 ? " · " + s.failedTasks() + " failed" : "")
              .append("</text>");
            // Progress bar.
            int barW = NODE_W - 24;
            sb.append("<rect x='").append(p[0] + 12).append("' y='").append(p[1] + 49)
              .append("' width='").append(barW).append("' height='8' rx='4' fill='#eee'/>");
            sb.append("<rect x='").append(p[0] + 12).append("' y='").append(p[1] + 49)
              .append("' width='").append(barW * pct / 100).append("' height='8' rx='4' fill='")
              .append(stroke).append("'/>");
            sb.append("</g>");
        }
        sb.append("</svg>");
    }

    /** Longest-path level from a root, memoized; parents outside {@code jobIds} are ignored. */
    private static int computeLevel(int stageId,
                                    java.util.Map<Integer, AppStatusStore.StageView> byId,
                                    java.util.Set<Integer> jobIds,
                                    java.util.Map<Integer, Integer> memo) {
        Integer cached = memo.get(stageId);
        if (cached != null) return cached;
        AppStatusStore.StageView s = byId.get(stageId);
        int lvl = 0;
        if (s != null) {
            for (int parentId : s.parentStageIds()) {
                if (!jobIds.contains(parentId)) continue;
                lvl = Math.max(lvl, computeLevel(parentId, byId, jobIds, memo) + 1);
            }
        }
        memo.put(stageId, lvl);
        return lvl;
    }

    /** All transitive parent stage ids of {@code stageId}, within {@code jobIds}, memoized. */
    private static java.util.Set<Integer> ancestorsOf(int stageId,
                                                      java.util.Map<Integer, AppStatusStore.StageView> byId,
                                                      java.util.Set<Integer> jobIds,
                                                      java.util.Map<Integer, java.util.Set<Integer>> memo) {
        java.util.Set<Integer> cached = memo.get(stageId);
        if (cached != null) return cached;
        java.util.Set<Integer> all = new java.util.HashSet<>();
        AppStatusStore.StageView s = byId.get(stageId);
        if (s != null) {
            for (int parentId : s.parentStageIds()) {
                if (!jobIds.contains(parentId)) continue;
                all.add(parentId);
                all.addAll(ancestorsOf(parentId, byId, jobIds, memo));
            }
        }
        memo.put(stageId, all);
        return all;
    }

    private static String esc(String s) {
        return s == null ? "" : s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }
}
