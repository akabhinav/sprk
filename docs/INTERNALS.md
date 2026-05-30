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

## Phase 5 — MiniYarn: a YARN-like cluster manager

This phase introduces a real resource negotiation layer. The engine no longer
spawns executor processes itself — it asks a cluster manager for containers,
and a separate set of components (RM, NM, AM) decide where they run.

**The cast.** Three new processes, each running on its own RpcEnv:
- `ResourceManager` (master): tracks NodeManagers and their free capacity;
  accepts AM registration and resource requests; runs a FIFO scheduler with a
  least-loaded-fit policy so executors spread across the cluster.
- `NodeManager` (per node): registers its `Resource(cores, memMB)` with the
  RM, heartbeats free capacity every ~1s, and launches container processes
  on demand using `ProcessBuilder` (child JVMs with inherited stdout).
- `ApplicationMaster` (per app, in driver JVM = yarn-client mode): registers
  with the RM, submits `RequestContainers`, receives `ContainersAllocated`,
  and sends `LaunchContainer` to the owning NM. Substitutes a
  `{{CONTAINER_ID}}` token in the command line so each executor JVM gets a
  unique id without the AM tracking placements itself.

**The protocol** (`com.miniyarn.common.YarnMessages`) is one record per wire
message and mirrors real YARN's API split:
```
RegisterNodeManager / NodeHeartbeat            (NM → RM)
RegisterApplication / RequestContainers /
ReleaseContainer / UnregisterApplication       (AM → RM)
ContainersAllocated / ContainerCompleted       (RM/NM → AM)
LaunchContainer / KillContainer                (AM → NM)
```
Allocation and launch are separate hops, like real YARN: the RM only reserves
resources, then the AM tells the NM what to actually run. NMs report container
exits via heartbeat-batched `ContainerStatus`, so the RM frees node capacity
without needing every NM to ask permission to crash.

**The integration with the engine is one class:** `YarnExecutorLauncher`. It
is the third implementation of the `ExecutorLauncher` seam from Phase 4 —
`LocalExecutorLauncher` (in-process), `ProcessExecutorLauncher` (forked JVMs
on this host), `YarnExecutorLauncher` (containers on remote NMs). The
`DAGScheduler`, `TaskScheduler`, and `CoarseGrainedSchedulerBackend` don't
care which one is in use; the executor processes the NM spawns are the same
`CoarseGrainedExecutorBackend.main` from Phase 4 and dial back to the driver
over the same Netty RPC.

**Wiring it up.** `MiniSparkContext` recognises a `miniyarn://host:port`
master URL: it forces `rpc.mode=netty` (executors must be remote-capable) and
selects the YARN launcher. Configuration:
```java
new MiniSparkConf()
    .setMaster("miniyarn://127.0.0.1:8032")
    .set("minispark.executor.instances", "N")
    .set("minispark.executor.cores", "2")
    .set("minispark.executor.memoryMB", "256");
```

**The end-to-end lifecycle** (matches Spark-on-YARN exactly):
1. User constructs `MiniSparkContext` with a `miniyarn://` master.
2. Driver starts its Netty RpcEnv and publishes `CoarseGrainedScheduler`.
3. `YarnExecutorLauncher` constructs an in-process `ApplicationMaster`.
4. AM `RegisterApplication` → RM assigns `app_N`.
5. AM `RequestContainers(N, Resource(cores,mem), launchCtx)`.
6. RM picks the least-loaded fitting node per allocation, pushes
   `ContainersAllocated(List<Container>)` to AM.
7. AM expands the `{{CONTAINER_ID}}` token per container and sends
   `LaunchContainer` to each owning NM.
8. NM forks the executor JVM; `Process.onExit()` enqueues a status update.
9. Executor JVM `CoarseGrainedExecutorBackend.main` connects to the driver,
   sends `RegisterExecutor`. From here the path is identical to Phase 4 —
   tasks dispatched, shuffle blocks fetched between executor block managers
   over their `BlockManager` endpoints.

Proven by `MiniYarnDistributedTest`: RM + 2 NMs (each with capacity for one
container) + driver in one JVM but each on its own Netty port → AM requests
2 executors → RM places one container on **each** NM → NMs spawn 2 executor
JVMs that connect back → WordCount runs with the shuffle crossing the
network → assertion verifies both `nm1` and `nm2` hosted a container.

## Phase 6 — Harden: cache, retry, heartbeats, recovery from lineage

This is the phase that makes the engine "Resilient" rather than just
"Distributed". Four concerns, all wired through the seams the previous
phases set up:

**1. `rdd.cache()` / `persist()`.** A new `StorageLevel` (`NONE` or
`MEMORY_ONLY`), a new `BlockId.RDDBlock(rddId, partitionIndex)`, and an
`RDD.iterator()` wrapper that consults the executor's `BlockManager` before
falling through to `compute()`. First action populates the cache; second
action serves from it. Scope is per-executor (a partition cached on A is
invisible to B and will recompute) — matches Spark's `MEMORY_ONLY`
trade-off. `RDDCacheTest` pins down the "no extra compute() on the second
action" guarantee.

**2. Task retry with per-partition state.** `TaskScheduler` grew a
`StageBook` of `PartitionState` (task, attempts so far, completed?, result).
A failure with attempts left feeds the task back into a `retries` queue and
re-revives offers; only after `maxAttempts` (default 4) does the stage
abort. Duplicate completions (a winning retry racing the original) are
ignored — first success wins.

**3. Structured failures.** `StatusUpdate` carries a sealed
`TaskFailureReason` (`GenericError`, `FetchFailed(shuffleId, mapId,
reduceId, badLocation)`, `ExecutorLost(executorId)`) instead of a free-form
string. The driver dispatches on the variant: `FetchFailed` is the
distinguishing case — it's how a reducer signals "the map output I needed
isn't there". `HashShuffleManager.HashReader` wraps `RemoteRpcException`s
from `getRemoteBlock` into `FetchFailedException`; the executor backend
walks the cause chain to package the structured reason.

**4. Heartbeats + lost-executor detection.** Each executor sends a
`Heartbeat(executorId)` to the driver every second. The driver's watchdog
checks every 1s; an executor unseen for `minispark.executor.heartbeatTimeoutMs`
(default 5s) is removed: its in-flight tasks are reported as
`ExecutorLost` so the scheduler retries them on the survivor, and the DAG
layer is told to scrub map outputs at that location.

**Lineage recomputation — the "R" in RDD.** When `DAGScheduler.handleExecutorLost`
or `handleFetchFailed` fires, recovery work runs on a dedicated single-thread
`recoveryExec`. Each recovery submits a *fresh recovery TaskSet* (new stage id,
new `StageBook`) to re-run the missing map partitions; on completion the new
outputs overwrite the old `MapOutputTracker` entries by mapId. Two
correctness invariants:
- The tracker is **never partially mutated**: outputs are added/overwritten,
  never removed first. A reducer querying mid-recovery still sees
  `numMaps` complete entries (some may point to a dead executor → it'll
  FetchFailed → recovery loop) rather than seeing a short list and
  silently producing an undercount.
- The worker-side `MapOutputTracker` does **not cache**. Every
  `getMapStatuses` call goes to the driver, so the moment a recovery
  updates a location, the next reduce attempt picks it up. Caching is a
  perf optimisation; for a learning engine, the correctness clarity of
  always-fresh lookups wins.

**The race that motivates a key optimisation.** When an executor dies mid
map stage, some map tasks may have already completed and be sitting in the
`StageBook` waiting to be registered with the tracker — at a location that
is now dead. We track `deadLocations` in the DAGScheduler; at registration
time, any result whose location is in this set is rebuilt eagerly before
publishing. Without this, every downstream reducer FetchFailed-loops
through a serial per-mapId recovery (correct but ~16× slower in our
test). With it, the kill-mid-job test recovers in under 5s.

**Side fix:** `TextFileRDD` had a partition-boundary bug found by the
larger fault-tolerance test — when `startInclusive` landed exactly on the
start of a line (the byte before was `\n`), the "skip partial first line"
logic ate a real line. Now it peeks at the previous byte to decide.

**The kill-executor test** (`ExecutorFailureRecoveryTest`) is the Phase 6
milestone: 16-partition reduceByKey with a synthetic per-task delay, 2
executor JVMs, hard-kill one mid-map (`Process.destroyForcibly`). The
result equals a clean run. Logs show the full chain — `killing executor
process proc-0` → watchdog `Executor proc-0 marked lost ... abandoning N
in-flight tasks` → tasks `ExecutorLost — retrying` → DAG `task result(s)
landed on dead executor(s); recomputing` → stage 2 complete → correct
output.

## Stretch goals (part 1) — broadcast variables & sort shuffle

Two engine features that drop into existing seams with zero scheduler change.

**Generalized block transfer.** Both features move non-shuffle bytes, so
`BlockId` became a richer sealed type (`ShuffleBlock`, `ShuffleDataBlock`,
`RDDBlock`, `BroadcastBlock`) and `NetworkBlockManager.FetchBlock` now carries
a `BlockId` directly instead of shuffle-specific ints. The transport is now
block-kind agnostic: new kinds need no RPC changes.

**Broadcast variables.** `sc.broadcast(value)` serializes the value into the
driver's BlockManager under a `BroadcastBlock` and returns a tiny
`TorrentBroadcast` handle (just an id + the driver's location). The handle is
`Serializable`, so a task closure that captures it stays small — the 100 MB
lookup table is *not* copied into every task. On first `broadcast.value()` on
an executor, the value is fetched once over the BlockManager and cached in a
per-JVM map; every later task on that executor reads the local copy. Named
`TorrentBroadcast` after the real class; we pull a single chunk from the driver
rather than doing the BitTorrent-style peer fan-out, but the
fetch-once-per-executor semantics are identical. Broadcast ids are minted from
a JVM-global counter so the per-executor cache (keyed by id) never collides
across contexts. `BroadcastTest` proves a task reads the broadcast value;
`BroadcastDistributedTest` path is exercised via the netty sort test infra.

**Sort-based shuffle.** `SortShuffleManager` is a drop-in for
`HashShuffleManager` selected by `minispark.shuffle.manager=sort`. Where hash
shuffle writes `numMaps × numReduces` blocks, sort shuffle writes **one
consolidated `ShuffleDataBlock` per map task**, holding every reduce
partition's records back-to-back with a per-partition count that acts as the
index. A reducer fetches that one block per map and slices out its own
partition range. This is why real Spark switched defaults: hash shuffle's file
count explodes (10k×10k = 100M files) and exhausts inodes/FDs, while sort
shuffle stays at `numMaps` files regardless of reducer count. We keep the data
in memory (a learning simplification) but the structure — one indexed file per
map — is the real design.

**Both choices flow to executors.** The shuffle manager name must match on
driver and executors (they exchange a format only the matching reader
understands). `ShuffleManagerFactory` builds the chosen one in both places;
`MiniSparkContext` forwards `minispark.shuffle.manager` to each child executor
JVM as a `-D` system property (via the `ProcessExecutorLauncher` /
`YarnExecutorLauncher`, which now take a `systemProps` map).

Proven by: `SortShuffleTest` (sort result == hash result for the same job, in
local mode) and `SortShuffleDistributedTest` (sort shuffle across two executor
JVMs over real TCP, with consolidated map blocks fetched between executors).

## Stretch goals (part 2) — event bus & web UI

A read-only web UI, built the way Spark builds its own: not by reaching into
the scheduler, but by event-sourcing.

**The event bus** (`LiveListenerBus`). The scheduler posts `SchedulerEvent`s
(job start/end, stage submitted/completed, task start/end, executor
added/removed) to a bus. Posting is non-blocking — events go on a queue drained
by a single daemon thread that notifies `SchedulerListener`s in order. Async,
single-consumer is deliberate: the scheduler posts from hot paths (RPC threads,
the DAG thread) and must never block on a slow listener, and single-threaded
dispatch means a listener needs no internal locking. This is exactly Spark's
`LiveListenerBus` design.

**Where events are posted.** `DAGScheduler` fires job and stage events around
`runJob` and the stage submitters; `CoarseGrainedSchedulerBackend` fires
executor add/remove and task start/end. Both took a nullable bus, so the engine
runs fine without one — the UI is purely additive.

**The status store** (`AppStatusStore`). A `SchedulerListener` that folds the
event stream into current state: jobs, stages (with completed/failed task
counts), executors (active? tasks run?). It hands out immutable `*View`
snapshots so the UI thread never races the dispatch thread. This mirrors
Spark's `AppStatusListener` + `AppStatusStore` split — the UI reads only the
store, never the live scheduler, so it can neither perturb nor be perturbed by
scheduling.

**The UI** (`MiniSparkUI`). The JDK's built-in
`com.sun.net.httpserver.HttpServer` — no servlet container, no dependency —
serves one self-refreshing HTML page (Executors / Jobs / Stages, with a little
progress bar per stage). Enabled by `minispark.ui.enabled=true`
(`minispark.ui.port`, default 4040; 0 = ephemeral). Off by default so tests
don't bind ports.

Proven by `MiniSparkUITest`: runs a two-stage reduceByKey with the UI on,
asserts the store shows one job + a ShuffleMapStage + a ResultStage all
SUCCEEDED, then fetches the page over real HTTP and checks the rendered table.
`WordCountWithUI` is a runnable demo (`mvn exec:java ... -Dexec.args="book.txt 60"`)
that prints the URL and keeps the driver alive for browsing.

## Tier A engine completion (part 1) — accumulators, joins, more actions, RangePartitioner, speculation

The plumbing built across phases 1–6 now supports a broader user-facing API and
two long-promised features — straggler-tolerance via speculation and write-only
metrics via accumulators. None of this required scheduler or RPC changes
beyond a single new field (`accumulatorUpdates`) on `StatusUpdate`.

**Accumulators.** `sc.longAccumulator("name")` returns an `Accumulator<Long>`
whose `add()` is callable from any task lambda. Under the hood: each task
context tracks deltas in a thread-local map; on completion the executor backend
attaches them to its `StatusUpdate`; the driver merges via `AccumulatorParam`
into the master value in `AccumulatorContext` (a static registry matching
real Spark's `AccumulatorContext`). The handle deserializes on executors with
its `driverValue` left null, so a task that accidentally calls `.value()` blows
up rather than silently lying.

**More pair-RDD ops.** `CoGroupedRDD` is the new shared primitive: takes N
parent pair RDDs sharing one partitioner, registers one `ShuffleDependency`
per parent, and `compute()` reads each shuffle's reducer-side iterator and
folds keys together. `groupByKey` is implemented over a single shuffle (no
combine; every record crosses the wire). `cogroup` and `join` are tiny adapters
on top of `CoGroupedRDD`. `join` is inner; outer variants would extend the
same shape. `sortByKey` uses a real {@link RangePartitioner} sampled from the
input (reservoir sampling per partition, sorted on the driver, N−1
equally-spaced boundaries) so each output partition's keys all sort before the
next partition's; the post-shuffle MapPartitions sorts within each partition.

**More actions.** `take(n)` / `first()` / `takeOrdered(n, cmp)` /
`saveAsTextFile(path)`. `takeOrdered` keeps a per-partition bounded heap of
size n, then merges on the driver. `saveAsTextFile` writes one
`part-NNNNN` UTF-8 file per partition into the given directory.

**Speculative execution.** Opt-in via `minispark.speculation=true`. A
single-thread `speculator` polls in-flight stages every
`speculation.intervalMs` (default 500). If `speculation.quantile` of a stage's
tasks (default 75%) have completed, any task still running longer than
`speculation.multiplier` × (per-task median runtime) is marked a straggler and
a duplicate is enqueued onto the retries queue — handed to any free executor.
The Phase-6 "first success wins" path in `TaskScheduler.taskCompleted` ensures
we just take whichever copy reports first. Tested with a deliberate 3-second
stall on one partition: the speculative copy overtakes it, the job finishes in
~120ms, and the slow lambda is confirmed to have run twice.

## Tier A — still to come

- Disk-spilling cache (`MEMORY_AND_DISK`)
- Locality-aware scheduling (PROCESS_LOCAL → NODE_LOCAL → ANY ladder with delay)
- Checkpointing (snapshot RDD to durable storage to truncate lineage)
- Dynamic allocation (request more/fewer executors based on backlog)
- Job cancellation and job groups
- Fair scheduler with pools
- Kryo serializer alternative

## Tier B+ — separate multi-session projects

- DataFrame / Dataset / Spark SQL (Catalyst tree, optimizer, code-gen lite)
- Structured Streaming (micro-batch over RDDs)
- Parquet / ORC / JDBC connectors
- Kubernetes / Standalone cluster managers
- Production security stack (Kerberos, SASL, TLS)
- Full Spark UI parity
