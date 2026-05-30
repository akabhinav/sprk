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

## Phase 4 — RPC seam: driver and executors as separate processes

This is the phase that makes the engine genuinely distributed. Nothing the
scheduler does changed; we swapped *how components talk*.

**The RPC abstraction** (`minispark-rpc`):
- `RpcEndpoint` — a named message handler with `receive` (one-way) and
  `receiveAndReply` (request/response).
- `RpcEndpointRef` — a handle to a (possibly remote) endpoint, with `send`,
  `ask`, `askAsync`. Callers cannot tell local from remote. That's the seam.
- `RpcEnv` — publishes endpoints and hands out refs. Two implementations:
  - `LocalRpcEnv`: in-process, delivers by direct call (one-way sends on a
    virtual-thread dispatcher). No serialization — same-JVM fast path.
  - `NettyRpcEnv`: real TCP. Hand-rolled length-prefixed framing
    (`[int length][serialized TransportMessage]`) over blocking sockets, one
    virtual thread per connection. We chose blocking + virtual threads over a
    NIO selector for readability; Java 21 makes thousands of blocked threads
    cheap. Each env runs a server and opens cached outbound client
    connections; ask-replies return on the same connection via a reader
    thread that completes pending futures keyed by request id.

**The coarse-grained backend** (`CoarseGrainedSchedulerBackend` +
`CoarseGrainedExecutorBackend`) reimplements driver↔executor as RPC:
- `RegisterExecutor` (executor→driver ask): "I'm up at host:port with N cores."
- `LaunchTask` (driver→executor): a serialized task to run.
- `StatusUpdate` (executor→driver): FINISHED + serialized result, or FAILED.
- `ReviveOffers` (driver self-message): try to schedule pending tasks onto
  free cores.

"Coarse-grained" = an executor registers once and serves many tasks (vs a
process per task). The driver tracks free cores per executor and dispatches
greedily; on each `StatusUpdate` it frees the slot and offers again.

**Two transports, one config flag** (`minispark.rpc.mode`):
- `local` (default): `LocalRpcEnv` + one in-process executor that shares the
  driver's `SparkEnv`. Shuffle blocks live in shared heap.
- `netty`: `NettyRpcEnv` + `ProcessExecutorLauncher`, which spawns each
  executor as a **separate JVM** via `ProcessBuilder` using
  `$JAVA_HOME/bin/java` and the parent classpath. Each child builds its own
  `SparkEnv` and dials home over TCP. Set `minispark.executor.instances` and
  `minispark.executor.cores` to scale.

**Distributed shuffle.** Once executors are in different JVMs, two things had
to become network-aware (the interfaces were already shaped for it):
- `MapOutputTracker` split into master (driver, authoritative) and worker
  (executor, asks the master via `GetMapStatuses` and caches). `ShuffleMapTask`
  now returns its `ExecutorLocation`; the driver's `DAGScheduler` registers
  each one with the master after the map stage completes.
- `NetworkBlockManager` serves a `"BlockManager"` endpoint and, on
  `getRemoteBlock`, fetches a bucket from the owning executor with a
  `FetchBlock` RPC. A reducer thus reads one bucket locally and pulls the
  rest over the wire.

Proven by `NettyDistributedTest`: the identical WordCount pipeline runs across
two executor JVMs, with the shuffle crossing the network, producing the same
result as local mode.

Design note: our protocol messages carry only primitives/bytes/addresses,
never an `RpcEndpointRef` — each side rebuilds the refs it needs from its own
`RpcEnv`. Real Spark instead rebinds serialized refs on the receiving env; we
took the simpler route and documented it.

## What's next

- **Phase 5**: MiniYarn — ResourceManager, NodeManagers, ApplicationMaster,
  Containers. A `YarnSchedulerBackend` reuses the `CoarseGrainedExecutorBackend`
  and the same `SchedulerBackend` seam; only the `ExecutorLauncher` changes
  from "spawn a local process" to "ask a NodeManager to launch a container".
- **Phase 6**: lineage-based recomputation on executor loss, task retry,
  `rdd.cache()` via `BlockManager`, broadcast variables, sort shuffle,
  speculative execution, a tiny web UI.
