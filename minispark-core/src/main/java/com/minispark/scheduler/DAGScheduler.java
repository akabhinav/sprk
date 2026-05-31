package com.minispark.scheduler;

import com.minispark.api.MiniSparkConf;
import com.minispark.rdd.Dependency;
import com.minispark.rdd.Partition;
import com.minispark.rdd.RDD;
import com.minispark.rdd.ShuffleDependency;
import com.minispark.rdd.ShuffledRDD;
import com.minispark.scheduler.adaptive.CoalesceShufflePartitionsRule;
import com.minispark.scheduler.cluster.TaskFailureReason;
import com.minispark.status.LiveListenerBus;
import com.minispark.status.SchedulerEvent;
import com.minispark.storage.ExecutorLocation;
import com.minispark.storage.MapOutputTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Turns an RDD lineage into a DAG of {@link Stage}s.
 *
 * <p><b>Algorithm.</b> Walk the lineage backwards from the action's RDD. For
 * every {@link ShuffleDependency} encountered, cut a new
 * {@link ShuffleMapStage} on the parent side and continue walking <i>inside</i>
 * that new stage. Narrow dependencies stay in the current stage. The final
 * RDD belongs to a {@link ResultStage}. Stages are submitted parents-first
 * because each child stage's tasks read shuffle output from its parents.
 *
 * <p><b>Why this is the central idea.</b> A narrow dep means each child
 * partition consumes from a bounded, known-by-index set of parent partitions
 * — so it can run in the same task as its parent (pipelining, no
 * materialization). A wide (shuffle) dep means each child partition needs
 * data from <i>all</i> parent partitions; that demands the parent's full
 * output be persisted by key first, which is exactly what a stage boundary
 * represents.
 *
 * Real Spark equivalent: org.apache.spark.scheduler.DAGScheduler
 */
public final class DAGScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(DAGScheduler.class);

    private final TaskScheduler taskScheduler;
    private final MapOutputTracker mapOutputTracker; // the driver-side master
    private final LiveListenerBus listenerBus;
    private final MiniSparkConf conf;
    private final AtomicInteger stageIdGen = new AtomicInteger();
    private final AtomicInteger jobIdGen = new AtomicInteger();

    // Memoize ShuffleMapStages keyed by shuffleId so a shuffle reused by two
    // downstream stages doesn't get rematerialized.
    private final Map<Integer, ShuffleMapStage> shuffleIdToStage = new HashMap<>();
    // Bookkeeping for tasks of in-flight stages, so we can re-dispatch the same
    // task object on retry (preserving its RDD/partition reference) instead of
    // walking RDDs to rebuild it.
    private final Map<Long, Task<?>> liveTasks = new java.util.concurrent.ConcurrentHashMap<>();

    // Recovery work (executor-lost / fetch-failed) is dispatched here off the
    // RPC/watchdog threads. Single-threaded so concurrent recoveries serialise
    // through one place — simpler reasoning, no double-resubmits.
    private final java.util.concurrent.ExecutorService recoveryExec =
            java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "dag-recovery");
                t.setDaemon(true);
                return t;
            });

    // Locations of executors known to be dead. Used at map-output registration
    // time to avoid publishing stale entries when a map task completed on an
    // executor that has since died — would otherwise force the reducers to
    // discover and recover the dead location via N FetchFailed retries.
    private final java.util.Set<ExecutorLocation> deadLocations =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    private static long taskKey(int stageId, int partitionId) {
        return ((long) stageId << 32) | (partitionId & 0xffffffffL);
    }

    public DAGScheduler(TaskScheduler taskScheduler, MapOutputTracker mapOutputTracker,
                        LiveListenerBus listenerBus) {
        this(taskScheduler, mapOutputTracker, listenerBus, new MiniSparkConf());
    }

    public DAGScheduler(TaskScheduler taskScheduler, MapOutputTracker mapOutputTracker,
                        LiveListenerBus listenerBus, MiniSparkConf conf) {
        this.taskScheduler = taskScheduler;
        this.mapOutputTracker = mapOutputTracker;
        this.listenerBus = listenerBus;
        this.conf = conf != null ? conf : new MiniSparkConf();
        // The scheduler routes structural failures and executor-lost events here
        // so we can run the recovery loop that gives RDDs their "Resilient" R.
        taskScheduler.setDAGEventHandler(new TaskScheduler.DAGEventHandler() {
            @Override public void onFetchFailed(int stageId, int partitionId,
                                                TaskFailureReason.FetchFailed reason) {
                handleFetchFailed(stageId, partitionId, reason);
            }
            @Override public void onExecutorLost(ExecutorLocation loc) {
                handleExecutorLost(loc);
            }
        });
    }

    /** Per-running-job cancellation state. */
    private static final class JobInfo {
        final String group;
        final java.util.concurrent.atomic.AtomicBoolean cancelled =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        // Stage the job is currently blocked on, so cancel can abort it.
        final java.util.concurrent.atomic.AtomicInteger currentStageId =
                new java.util.concurrent.atomic.AtomicInteger(-1);
        JobInfo(String group) { this.group = group; }
    }
    private final Map<Integer, JobInfo> activeJobs = new java.util.concurrent.ConcurrentHashMap<>();

    public <T, U> List<U> runJob(RDD<T> finalRdd, ResultTask.ResultHandler<T, U> handler) {
        return runJob(finalRdd, handler, null);
    }

    public <T, U> List<U> runJob(RDD<T> finalRdd, ResultTask.ResultHandler<T, U> handler, String group) {
        int numPartitions = finalRdd.getPartitions().size();
        int[] toCompute = new int[numPartitions];
        for (int i = 0; i < numPartitions; i++) toCompute[i] = i;

        // Build the stage graph by walking lineage from finalRdd backwards.
        List<ShuffleMapStage> ancestors = new ArrayList<>();
        Set<Integer> visited = new HashSet<>();
        discoverShuffleAncestors(finalRdd, ancestors, visited);

        ResultStage resultStage = new ResultStage(
                stageIdGen.incrementAndGet(),
                finalRdd,
                new ArrayList<>(ancestors), // parents in discovery order; topological enough
                toCompute);

        LOG.info("Job: result stage {} with {} parent shuffle stages",
                resultStage.id(), ancestors.size());

        int jobId = jobIdGen.incrementAndGet();
        List<Integer> stageIds = new ArrayList<>();
        for (ShuffleMapStage s : ancestors) stageIds.add(s.id());
        stageIds.add(resultStage.id());
        post(new SchedulerEvent.JobStart(jobId, stageIds, now()));

        JobInfo info = new JobInfo(group);
        activeJobs.put(jobId, info);
        try {
            // Submit map stages bottom-up: each ShuffleMapStage's parents come
            // earlier in the list because they were discovered later in the recursion.
            // (We reverse to put deepest ancestors first.)
            for (int i = ancestors.size() - 1; i >= 0; i--) {
                checkCancelled(info);
                info.currentStageId.set(ancestors.get(i).id());
                submitShuffleMapStage(ancestors.get(i));
            }
            checkCancelled(info);
            info.currentStageId.set(resultStage.id());
            List<U> result = submitResultStage(resultStage, handler);
            post(new SchedulerEvent.JobEnd(jobId, true, now()));
            return result;
        } catch (RuntimeException e) {
            post(new SchedulerEvent.JobEnd(jobId, false, now()));
            throw e;
        } finally {
            activeJobs.remove(jobId);
        }
    }

    private void checkCancelled(JobInfo info) {
        if (info.cancelled.get()) {
            throw new JobCancelledException("Job cancelled" +
                    (info.group != null ? " (group " + info.group + ")" : ""));
        }
    }

    /** Cancel every running job (optionally filtered to a job group). */
    public void cancelJobs(String group) {
        for (JobInfo info : activeJobs.values()) {
            if (group != null && !group.equals(info.group)) continue;
            info.cancelled.set(true);
            int stageId = info.currentStageId.get();
            if (stageId >= 0) {
                taskScheduler.abortStage(stageId, "job cancelled");
            }
        }
    }

    /** Thrown when a job is cancelled via {@link #cancelJobs}. */
    public static final class JobCancelledException extends RuntimeException {
        public JobCancelledException(String message) { super(message); }
    }

    private void post(SchedulerEvent e) { if (listenerBus != null) listenerBus.post(e); }
    private static long now() { return System.currentTimeMillis(); }

    /** DFS the RDD lineage, registering a ShuffleMapStage for each ShuffleDependency seen. */
    private void discoverShuffleAncestors(RDD<?> rdd,
                                          List<ShuffleMapStage> out,
                                          Set<Integer> visitedRdds) {
        if (!visitedRdds.add(rdd.id())) return;
        // Lineage truncation: a checkpointed RDD reads its partitions from
        // reliable storage in compute(), so we must NOT walk its ancestors —
        // they don't need recomputing. This is the payoff of checkpoint().
        if (rdd.isCheckpointed()) return;
        for (Dependency<?> dep : rdd.getDependencies()) {
            if (dep instanceof ShuffleDependency<?, ?> sd) {
                ShuffleMapStage stage = getOrCreateShuffleMapStage(sd);
                out.add(stage);
                // Continue walking through the shuffle's parent — its own
                // ancestors may include further shuffles.
                discoverShuffleAncestors(sd.rdd(), out, visitedRdds);
            } else {
                discoverShuffleAncestors(dep.rdd(), out, visitedRdds);
            }
        }
    }

    private ShuffleMapStage getOrCreateShuffleMapStage(ShuffleDependency<?, ?> sd) {
        ShuffleMapStage cached = shuffleIdToStage.get(sd.shuffleId());
        if (cached != null) return cached;

        // Parents of this stage are themselves the ShuffleMapStages reachable
        // by walking the lineage of the shuffle's input RDD.
        List<ShuffleMapStage> parents = new ArrayList<>();
        discoverShuffleAncestors(sd.rdd(), parents, new HashSet<>());

        ShuffleMapStage stage = new ShuffleMapStage(
                stageIdGen.incrementAndGet(),
                sd.rdd(),
                new ArrayList<>(parents),
                sd);
        shuffleIdToStage.put(sd.shuffleId(), stage);
        LOG.debug("Created ShuffleMapStage {} for shuffleId {}", stage.id(), sd.shuffleId());
        return stage;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void submitShuffleMapStage(ShuffleMapStage stage) {
        RDD<?> rdd = stage.rdd();
        List<Partition> partitions = rdd.getPartitions();
        List<Task<?>> tasks = new ArrayList<>(partitions.size());
        for (Partition p : partitions) {
            Task<?> t = new ShuffleMapTask(
                    stage.id(), p.index(),
                    (RDD) rdd, p,
                    stage.shuffleDep().handle());
            tasks.add(t);
            liveTasks.put(taskKey(stage.id(), p.index()), t);
        }
        LOG.info("Submitting ShuffleMapStage {}: {} map tasks", stage.id(), tasks.size());
        post(new SchedulerEvent.StageSubmitted(stage.id(), "ShuffleMapStage", tasks.size(), now()));
        int shuffleId = stage.shuffleDep().shuffleId();
        List<TaskResult<?>> results;
        try {
            results = taskScheduler.submitTasks(new TaskSet(stage.id(), tasks)).get();
        } catch (Exception e) {
            post(new SchedulerEvent.StageCompleted(stage.id(), false, now()));
            throw new RuntimeException("ShuffleMapStage " + stage.id() + " failed", e);
        }
        post(new SchedulerEvent.StageCompleted(stage.id(), true, now()));
        // Register each completed map task's output location with the master
        // tracker. Reducers (possibly in other JVMs) query this to find and
        // fetch their buckets. This must happen before any dependent stage runs.
        //
        // If a map task happened to complete on an executor that has since
        // died, the result references a stale location. Rebuild those map
        // partitions on a live executor before publishing — otherwise every
        // reducer would FetchFailed against the dead location and we'd recover
        // one mapId per FetchFailed serially. Doing it once here is dramatically
        // faster and is what real Spark's DAGScheduler does when handling
        // executor-lost events that race with stage completion.
        List<Integer> needsRedo = new ArrayList<>();
        for (TaskResult<?> r : results) {
            MapTaskOutput out = (MapTaskOutput) r.value;
            if (deadLocations.contains(out.location())) {
                needsRedo.add(r.partitionId);
            } else {
                mapOutputTracker.registerMapOutput(shuffleId, r.partitionId,
                        out.location(), out.partitionBytes());
            }
        }
        if (!needsRedo.isEmpty()) {
            LOG.warn("ShuffleMapStage {}: {} task result(s) landed on dead executor(s); recomputing",
                    stage.id(), needsRedo.size());
            rebuildMapPartitions(shuffleId, needsRedo);
        }
    }

    // ---------- recovery ----------

    /**
     * An executor went away (heartbeat timeout). Any shuffle map outputs it
     * produced are gone; queue re-execution of those map tasks asynchronously
     * so the watchdog thread doesn't block. Reduce tasks waiting on these
     * outputs will either be picked up after re-mapping completes, or fail
     * with FetchFailed and follow the recovery loop in {@link #handleFetchFailed}.
     */
    private void handleExecutorLost(ExecutorLocation loc) {
        deadLocations.add(loc);
        recoveryExec.submit(() -> {
            List<int[]> affected = mapOutputTracker.mapsAtLocation(loc);
            if (affected.isEmpty()) {
                LOG.info("Executor at {} lost; no map outputs to recompute", loc);
                return;
            }
            Map<Integer, List<Integer>> byShuffle = new HashMap<>();
            for (int[] sm : affected) byShuffle.computeIfAbsent(sm[0], k -> new ArrayList<>()).add(sm[1]);
            LOG.warn("Executor at {} lost; recomputing {} map output(s) across {} shuffle(s)",
                    loc, affected.size(), byShuffle.size());
            for (Map.Entry<Integer, List<Integer>> e : byShuffle.entrySet()) {
                rebuildMapPartitions(e.getKey(), e.getValue());
            }
        });
    }

    /**
     * A reducer hit a missing/unreachable shuffle block. Re-run that one map
     * task — the new location overwrites the old in the tracker — and put the
     * reduce task back on the queue. We don't remove the old entry first: that
     * would briefly leave the tracker with fewer than {@code numMaps} entries,
     * which a concurrent reducer would silently misinterpret as "this is the
     * complete set" and produce undercounted output.
     */
    private void handleFetchFailed(int reduceStageId, int reducePartitionId,
                                   TaskFailureReason.FetchFailed reason) {
        recoveryExec.submit(() -> {
            rebuildMapPartitions(reason.shuffleId(), List.of(reason.mapId()));
            Task<?> reduceTask = liveTasks.get(taskKey(reduceStageId, reducePartitionId));
            if (reduceTask != null) {
                LOG.info("Resubmitting reduce task {}/{} after FetchFailed",
                        reduceStageId, reducePartitionId);
                taskScheduler.resubmitTask(reduceTask);
            }
        });
    }

    /**
     * Run a fresh TaskSet that re-executes the listed map partitions of
     * {@code shuffleId}, then register the new outputs. Uses a brand-new
     * recovery stage id so it gets its own {@code StageBook} in the
     * TaskScheduler — independent of the original map stage's bookkeeping,
     * which may already have been retired when its first run completed.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void rebuildMapPartitions(int shuffleId, List<Integer> mapIds) {
        ShuffleMapStage origStage = shuffleIdToStage.get(shuffleId);
        if (origStage == null) {
            LOG.warn("Cannot recompute map outputs for unknown shuffle {}", shuffleId);
            return;
        }
        RDD<?> rdd = origStage.rdd();
        List<Partition> parts = rdd.getPartitions();
        int recoveryStageId = stageIdGen.incrementAndGet();
        List<Task<?>> tasks = new ArrayList<>(mapIds.size());
        for (int mapId : mapIds) {
            tasks.add(new ShuffleMapTask(recoveryStageId, mapId,
                    (RDD) rdd, parts.get(mapId), origStage.shuffleDep().handle()));
        }
        LOG.info("Recovery: re-running {} map task(s) for shuffle {} as stage {}",
                tasks.size(), shuffleId, recoveryStageId);
        try {
            List<TaskResult<?>> results = taskScheduler
                    .submitTasks(new TaskSet(recoveryStageId, tasks)).get();
            for (TaskResult<?> r : results) {
                MapTaskOutput out = (MapTaskOutput) r.value;
                mapOutputTracker.registerMapOutput(shuffleId, r.partitionId,
                        out.location(), out.partitionBytes());
            }
            LOG.info("Recovery: registered {} new map output(s) for shuffle {}",
                    results.size(), shuffleId);
        } catch (Exception e) {
            LOG.error("Recovery task set for shuffle {} failed: {}", shuffleId, e.toString());
        }
    }

    @SuppressWarnings("unchecked")
    private <T, U> List<U> submitResultStage(ResultStage stage,
                                             ResultTask.ResultHandler<T, U> handler) {
        RDD<T> rdd = (RDD<T>) stage.rdd();
        // AQE hook: every parent ShuffleMapStage has materialised, so the
        // MapOutputTracker now knows the real per-reducer byte sizes. Replace
        // the default (one partition per reducer) layout of every ShuffledRDD
        // feeding this result stage with a coalesced layout when the workload
        // doesn't justify so many tasks. Gated on minispark.sql.adaptive.enabled.
        maybeCoalesceShuffles(rdd);
        List<Partition> parts = rdd.getPartitions();
        int[] toCompute = stage.partitionsToCompute();
        // AQE may have shrunk the final RDD's partition count below what the
        // ResultStage was created with. Clamp the index list to the new layout
        // (whole-collect semantics still hold: a coalesced partition contains
        // the union of the records its source reducers would have produced).
        if (toCompute.length > parts.size()) {
            int[] clamped = new int[parts.size()];
            for (int i = 0; i < parts.size(); i++) clamped[i] = i;
            toCompute = clamped;
        }
        List<Task<?>> tasks = new ArrayList<>(toCompute.length);
        for (int outputId = 0; outputId < toCompute.length; outputId++) {
            int pIdx = toCompute[outputId];
            ResultTask<T, U> t = new ResultTask<>(stage.id(), pIdx, outputId, rdd, parts.get(pIdx), handler);
            tasks.add(t);
            liveTasks.put(taskKey(stage.id(), pIdx), t);
        }
        LOG.info("Submitting ResultStage {}: {} result tasks", stage.id(), tasks.size());
        post(new SchedulerEvent.StageSubmitted(stage.id(), "ResultStage", tasks.size(), now()));

        List<TaskResult<?>> results;
        try {
            results = taskScheduler.submitTasks(new TaskSet(stage.id(), tasks)).get();
        } catch (Exception e) {
            post(new SchedulerEvent.StageCompleted(stage.id(), false, now()));
            throw new RuntimeException("ResultStage " + stage.id() + " failed", e);
        }
        post(new SchedulerEvent.StageCompleted(stage.id(), true, now()));

        // Order by partitionId so the caller sees deterministic ordering.
        U[] arr = (U[]) new Object[toCompute.length];
        Map<Integer, Integer> partToOutput = new HashMap<>();
        for (int i = 0; i < toCompute.length; i++) partToOutput.put(toCompute[i], i);
        for (TaskResult<?> r : results) {
            Integer idx = partToOutput.get(r.partitionId);
            if (idx != null) arr[idx] = (U) r.value;
        }
        List<U> out = new ArrayList<>(toCompute.length);
        for (U u : arr) out.add(u);
        return out;
    }

    // ---------- Adaptive Query Execution: coalesce shuffle partitions ----------

    /**
     * If AQE is enabled, walk the result RDD's lineage looking for ShuffledRDDs
     * (which are the post-shuffle reads of completed parent map stages) and
     * replace their default per-reducer partition layout with a coalesced one
     * derived from real map-output sizes. Stops walking through any further
     * ShuffleDependency: re-partitioning the parent of another shuffle would
     * break that downstream shuffle's input contract.
     */
    private void maybeCoalesceShuffles(RDD<?> resultRdd) {
        if (!conf.get("minispark.sql.adaptive.enabled", "false").equalsIgnoreCase("true")) return;
        long targetBytes = Long.parseLong(
                conf.get("minispark.sql.adaptive.coalescePartitions.targetSizeInBytes", "67108864"));
        int minPartitions = conf.getInt(
                "minispark.sql.adaptive.coalescePartitions.minPartitionNum", 1);

        Set<Integer> visited = new HashSet<>();
        List<ShuffledRDD<?, ?>> targets = new ArrayList<>();
        findShuffledRdds(resultRdd, targets, visited);

        for (ShuffledRDD<?, ?> sh : targets) {
            int shuffleId = sh.shuffleDep().shuffleId();
            int numReducers = sh.shuffleDep().partitioner().numPartitions();
            long[] sizes = mapOutputTracker.getReducerSizes(shuffleId, numReducers);
            List<int[]> ranges = CoalesceShufflePartitionsRule.plan(sizes, targetBytes, minPartitions);
            if (ranges.size() < numReducers) {
                LOG.info("AQE: coalesced shuffle {} from {} to {} post-shuffle partitions (target={} bytes)",
                        shuffleId, numReducers, ranges.size(), targetBytes);
                sh.applyCoalescedRanges(ranges);
            } else {
                LOG.debug("AQE: shuffle {} stays at {} partitions (no coalesce benefit)",
                        shuffleId, numReducers);
            }
        }
    }

    /** DFS for ShuffledRDDs reachable through narrow dependencies only. */
    private void findShuffledRdds(RDD<?> rdd, List<ShuffledRDD<?, ?>> out, Set<Integer> visited) {
        if (!visited.add(rdd.id())) return;
        if (rdd instanceof ShuffledRDD<?, ?> sh) {
            out.add(sh);
            // Do not recurse into the shuffle's input RDD: that lineage is on
            // the other side of a stage boundary we just materialised, and any
            // ShuffledRDD found there would already have been coalesced as part
            // of an earlier result-stage submit (or is not reachable from this job).
            return;
        }
        for (Dependency<?> d : rdd.getDependencies()) {
            // Don't traverse into another ShuffleDependency's parent — same reason.
            if (d instanceof ShuffleDependency<?, ?>) continue;
            findShuffledRdds(d.rdd(), out, visited);
        }
    }
}
