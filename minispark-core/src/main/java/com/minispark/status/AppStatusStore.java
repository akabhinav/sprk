package com.minispark.status;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Listens to {@link SchedulerEvent}s and maintains the current state of the
 * application: jobs, stages, and executors. The web UI renders straight off
 * this store. Snapshots ({@code *View}) are immutable copies so the UI thread
 * never races the event-dispatch thread.
 *
 * Real Spark equivalent: org.apache.spark.status.AppStatusStore (+ AppStatusListener).
 */
public final class AppStatusStore implements SchedulerListener {

    public enum Status { RUNNING, SUCCEEDED, FAILED }

    public record JobView(int jobId, List<Integer> stageIds, Status status, long startMs, long endMs) {}
    public record StageView(int stageId, String name, int numTasks, int completedTasks,
                            int failedTasks, Status status, long startMs, long endMs) {}
    public record ExecutorView(String executorId, String host, int port, int cores,
                               boolean active, int tasksRun) {}

    private static final class MutableStage {
        final int stageId; final String name; final int numTasks;
        int completed; int failed; Status status; long start; long end;
        MutableStage(int id, String name, int n, long start) {
            this.stageId = id; this.name = name; this.numTasks = n;
            this.status = Status.RUNNING; this.start = start;
        }
    }
    private static final class MutableExecutor {
        final String id; final String host; final int port; final int cores;
        boolean active = true; int tasksRun;
        MutableExecutor(String id, String host, int port, int cores) {
            this.id = id; this.host = host; this.port = port; this.cores = cores;
        }
    }
    private static final class MutableJob {
        final int jobId; final List<Integer> stageIds; Status status; long start; long end;
        MutableJob(int id, List<Integer> stageIds, long start) {
            this.jobId = id; this.stageIds = stageIds; this.status = Status.RUNNING; this.start = start;
        }
    }

    private final ConcurrentMap<Integer, MutableJob> jobs = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, MutableStage> stages = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, MutableExecutor> executors = new ConcurrentHashMap<>();

    @Override
    public void onEvent(SchedulerEvent event) {
        switch (event) {
            case SchedulerEvent.JobStart e ->
                    jobs.put(e.jobId(), new MutableJob(e.jobId(), e.stageIds(), e.timeMs()));
            case SchedulerEvent.JobEnd e -> {
                MutableJob j = jobs.get(e.jobId());
                if (j != null) { j.status = e.success() ? Status.SUCCEEDED : Status.FAILED; j.end = e.timeMs(); }
            }
            case SchedulerEvent.StageSubmitted e ->
                    stages.put(e.stageId(), new MutableStage(e.stageId(), e.name(), e.numTasks(), e.timeMs()));
            case SchedulerEvent.StageCompleted e -> {
                MutableStage s = stages.get(e.stageId());
                if (s != null) { s.status = e.success() ? Status.SUCCEEDED : Status.FAILED; s.end = e.timeMs(); }
            }
            case SchedulerEvent.TaskStart e -> {
                MutableExecutor x = executors.get(e.executorId());
                if (x != null) x.tasksRun++;
            }
            case SchedulerEvent.TaskEnd e -> {
                MutableStage s = stages.get(e.stageId());
                if (s != null) { if (e.success()) s.completed++; else s.failed++; }
            }
            case SchedulerEvent.ExecutorAdded e ->
                    executors.put(e.executorId(),
                            new MutableExecutor(e.executorId(), e.host(), e.port(), e.cores()));
            case SchedulerEvent.ExecutorRemoved e -> {
                MutableExecutor x = executors.get(e.executorId());
                if (x != null) x.active = false;
            }
        }
    }

    // ----- immutable snapshots for the UI thread -----

    public List<JobView> jobs() {
        List<JobView> out = new ArrayList<>();
        for (MutableJob j : sortedById(jobs)) {
            out.add(new JobView(j.jobId, j.stageIds, j.status, j.start, j.end));
        }
        return out;
    }

    public List<StageView> stages() {
        List<StageView> out = new ArrayList<>();
        for (MutableStage s : sortedById(stages)) {
            out.add(new StageView(s.stageId, s.name, s.numTasks, s.completed, s.failed,
                    s.status, s.start, s.end));
        }
        return out;
    }

    public List<ExecutorView> executors() {
        List<ExecutorView> out = new ArrayList<>();
        for (MutableExecutor x : executors.values()) {
            out.add(new ExecutorView(x.id, x.host, x.port, x.cores, x.active, x.tasksRun));
        }
        out.sort((a, b) -> a.executorId().compareTo(b.executorId()));
        return out;
    }

    private static <T> List<T> sortedById(Map<Integer, T> m) {
        List<Integer> keys = new ArrayList<>(m.keySet());
        keys.sort(Integer::compareTo);
        List<T> out = new ArrayList<>(keys.size());
        for (Integer k : keys) out.add(m.get(k));
        return out;
    }
}
