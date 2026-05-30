# MiniSpark Internals

Working notes that map each phase to its real Spark counterpart.

## Phase 0 — Scaffolding

Multi-module Maven so that boundaries are enforced by the build, not by
discipline:

- `minispark-rpc` — Phase 4 seam (`RpcEnv`, `RpcEndpoint`, `RpcEndpointRef`).
- `minispark-core` — RDDs, scheduler, executor, shuffle, storage.
- `miniyarn` — Phase 5 cluster manager.
- `minispark-examples` — runnable demos.

Logging via SLF4J + Logback. JUnit 5 + AssertJ.

## Phase 1 — RDDs and laziness

An `RDD<T>` is a description of how to compute a partitioned dataset, not
the data itself. Concrete RDDs override three things:

1. `getPartitions()` — how the dataset is sliced.
2. `compute(partition, ctx)` — how to produce the records for one slice.
3. `getDependencies()` — what parent RDDs feed this one.

Transformations (`map`, `filter`, `flatMap`) return a child RDD wrapping the
parent and the user's function. No computation happens until an action
(`collect`, `count`, `reduce`, `foreach`) invokes `MiniSparkContext.runJob`.

**Why laziness matters.** A scheduler that sees the whole pipeline can fuse
narrow transformations into a single stage (running them in one pass over
each partition), schedule with locality awareness, and, most importantly,
recompute a lost partition from its lineage instead of replicating state.
Eager evaluation forecloses all of these. This is the same observation
behind every modern dataflow system.

## Phase 2 — Stages, the DAGScheduler, and shuffle

Two flavors of dependency:

- **Narrow** (`OneToOneDependency`): each child partition reads from a known,
  bounded set of parent partitions. The child can stream from the parent in
  the same task → pipelining → one stage.
- **Wide** (`ShuffleDependency`): each child partition needs records from
  *all* parent partitions, grouped by key. The parent must therefore
  materialize its output first. This is a stage boundary.

The `DAGScheduler` walks the RDD lineage backward from the action's RDD.
At every `ShuffleDependency` it creates a `ShuffleMapStage` rooted at the
shuffle's parent RDD; chains of narrow deps stay in the current stage. The
final RDD belongs to a `ResultStage`. Stages are submitted parents-first
because each child stage's tasks read shuffle output written by its parents.

**Shuffle implementation** (`HashShuffleManager`):
- Map side: each map task partitions its records by `HashPartitioner` into
  `R` buckets and writes one shuffle block per `(shuffleId, mapId, reduceId)`.
- `MapOutputTracker` records which executor holds each map output.
- Reduce side: each reduce task asks the tracker for all map outputs of its
  shuffle, fetches the bucket matching its reduce id from each, and returns
  them as one concatenated iterator.

**Why `reduceByKey` beats `groupByKey` + `map`.** `PairRDDFunctions.reduceByKey`
runs the reducer twice: once before the shuffle (map-side combine,
shrinking the shuffle payload, often by orders of magnitude) and once after
(reduce-side combine). Same final answer, drastically less bytes on the
wire. Identical idea to MapReduce's combiner.

## Phase 3 — Executor + SchedulerBackend seam

This phase is structural rather than functional: nothing new runs that
wasn't running before, but it's run through interfaces that allow
distribution later.

- `TaskScheduler` depends only on the `SchedulerBackend` interface; it has
  no idea whether tasks run in-process or across a network.
- `LocalSchedulerBackend` starts a single in-process `Executor`. Even in
  local mode, every task is serialized before launch and the result is
  deserialized on return — the same code path that Phase 4 will use across
  the wire. This caught two real bugs while building Phase 2:
  - A closure that captured an `AtomicInteger` was being cloned by Java
    serialization, so the executor incremented a copy the driver couldn't
    see. (Fix: use a global, or a real accumulator.)
  - `ShuffleMapTask` originally held the `ShuffleManager` directly. After
    serialization it deserialized to null. (Fix: route through `SparkEnv`,
    a per-JVM singleton initialized at startup. Executors built in Phase 5
    will create their own `SparkEnv`; tasks look up local services there.)

The same lookup pattern moved `ShuffledRDD.compute` off `context()` (a
`transient` field that's null on executors) onto `SparkEnv.get()`.

## What's next

- **Phase 4**: replace direct method calls between driver and executor with
  messages over `RpcEnv` (LocalRpcEnv + NettyRpcEnv).
- **Phase 5**: MiniYarn — ResourceManager, NodeManagers, ApplicationMaster,
  Containers. `YarnSchedulerBackend` slots into the same `SchedulerBackend`
  seam that `LocalSchedulerBackend` uses today.
- **Phase 6**: lineage-based recomputation on executor loss, task retry,
  `rdd.cache()` via `BlockManager`, broadcast variables, sort shuffle,
  speculative execution, a tiny web UI.
