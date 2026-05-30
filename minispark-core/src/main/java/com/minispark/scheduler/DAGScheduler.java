package com.minispark.scheduler;

import com.minispark.rdd.Dependency;
import com.minispark.rdd.Partition;
import com.minispark.rdd.RDD;
import com.minispark.rdd.ShuffleDependency;
import com.minispark.scheduler.cluster.TaskFailureReason;
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
    private final AtomicInteger stageIdGen = new AtomicInteger();

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

    public DAGScheduler(TaskScheduler taskScheduler, MapOutputTracker mapOutputTracker) {
        this.taskScheduler = taskScheduler;
        this.mapOutputTracker = mapOutputTracker;
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

    public <T, U> List<U> runJob(RDD<T> finalRdd, ResultTask.ResultHandler<T, U> handler) {
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

        // Submit map stages bottom-up: each ShuffleMapStage's parents come
        // earlier in the list because they were discovered later in the recursion.
        // (We reverse to put deepest ancestors first.)
        for (int i = ancestors.size() - 1; i >= 0; i--) {
            ShuffleMapStage s = ancestors.get(i);
            submitShuffleMapStage(s);
        }

        return submitResultStage(resultStage, handler);
    }

    /** DFS the RDD lineage, registering a ShuffleMapStage for each ShuffleDependency seen. */
    private void discoverShuffleAncestors(RDD<?> rdd,
                                          List<ShuffleMapStage> out,
                                          Set<Integer> visitedRdds) {
        if (!visitedRdds.add(rdd.id())) return;
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
        int shuffleId = stage.shuffleDep().shuffleId();
        List<TaskResult<?>> results;
        try {
            results = taskScheduler.submitTasks(new TaskSet(stage.id(), tasks)).get();
        } catch (Exception e) {
            throw new RuntimeException("ShuffleMapStage " + stage.id() + " failed", e);
        }
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
            ExecutorLocation loc = (ExecutorLocation) r.value;
            if (deadLocations.contains(loc)) {
                needsRedo.add(r.partitionId);
            } else {
                mapOutputTracker.registerMapOutput(shuffleId, r.partitionId, loc);
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
                mapOutputTracker.registerMapOutput(shuffleId, r.partitionId, (ExecutorLocation) r.value);
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
        List<Partition> parts = rdd.getPartitions();
        int[] toCompute = stage.partitionsToCompute();
        List<Task<?>> tasks = new ArrayList<>(toCompute.length);
        for (int outputId = 0; outputId < toCompute.length; outputId++) {
            int pIdx = toCompute[outputId];
            ResultTask<T, U> t = new ResultTask<>(stage.id(), pIdx, outputId, rdd, parts.get(pIdx), handler);
            tasks.add(t);
            liveTasks.put(taskKey(stage.id(), pIdx), t);
        }
        LOG.info("Submitting ResultStage {}: {} result tasks", stage.id(), tasks.size());

        List<TaskResult<?>> results;
        try {
            results = taskScheduler.submitTasks(new TaskSet(stage.id(), tasks)).get();
        } catch (Exception e) {
            throw new RuntimeException("ResultStage " + stage.id() + " failed", e);
        }

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

}
