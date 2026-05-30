package com.minispark.scheduler;

import com.minispark.rdd.Dependency;
import com.minispark.rdd.Partition;
import com.minispark.rdd.RDD;
import com.minispark.rdd.ShuffleDependency;
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

    public DAGScheduler(TaskScheduler taskScheduler, MapOutputTracker mapOutputTracker) {
        this.taskScheduler = taskScheduler;
        this.mapOutputTracker = mapOutputTracker;
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
            tasks.add(new ShuffleMapTask(
                    stage.id(), p.index(),
                    (RDD) rdd, p,
                    stage.shuffleDep().handle()));
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
        for (TaskResult<?> r : results) {
            ExecutorLocation loc = (ExecutorLocation) r.value;
            mapOutputTracker.registerMapOutput(shuffleId, r.partitionId, loc);
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
            tasks.add(new ResultTask<>(stage.id(), pIdx, outputId, rdd, parts.get(pIdx), handler));
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
