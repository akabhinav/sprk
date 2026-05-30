# Component: Web UI & observability (`minispark-core`)

A read-only web UI built the way Spark builds its own: **event-sourced**, never
reaching into the live scheduler.

## Event flow

```mermaid
flowchart LR
    DAG["DAGScheduler"] -->|JobStart/End,\nStageSubmitted/Completed| BUS
    BE["CoarseGrainedSchedulerBackend"] -->|ExecutorAdded/Removed,\nTaskStart/End| BUS
    BUS["LiveListenerBus\n(async, single consumer)"] --> STORE["AppStatusStore\n(folds events → state)"]
    STORE --> UI["MiniSparkUI (HTTP)\nrenders snapshots"]
    UI -->|http :4040| BROWSER["browser"]
```

- **`LiveListenerBus`** — the scheduler `post()`s `SchedulerEvent`s onto a queue
  drained by a **single daemon thread** that notifies listeners in order.
  Posting is non-blocking, so a slow listener can't stall scheduling, and
  listeners need no internal locking.
- **`AppStatusStore`** — a `SchedulerListener` that folds the event stream into
  current job/stage/executor state and hands out **immutable snapshots**, so the
  UI thread never races the dispatch thread.
- **`MiniSparkUI`** — the JDK's built-in `com.sun.net.httpserver.HttpServer`
  (zero deps) serves one self-refreshing HTML page: Executors, Jobs, Stages
  (with progress bars).

The UI reads only the store, so it can neither perturb nor be perturbed by
scheduling — exactly Spark's `AppStatusListener`/`AppStatusStore`/`SparkUI`
split.

## Enabling it

```java
new MiniSparkConf()
    .set("minispark.ui.enabled", "true")
    .set("minispark.ui.port", "4040");   // 0 = ephemeral
```

Off by default (so tests don't bind ports). `sc.uiPort()` returns the bound
port. Demo: `WordCountWithUI` (or `scripts/run-ui-demo.sh`) prints the URL and
keeps the driver alive for browsing.

## Events

| Event | Posted by |
|-------|-----------|
| `JobStart` / `JobEnd` | `DAGScheduler` |
| `StageSubmitted` / `StageCompleted` | `DAGScheduler` |
| `TaskStart` / `TaskEnd` | `CoarseGrainedSchedulerBackend` |
| `ExecutorAdded` / `ExecutorRemoved` | `CoarseGrainedSchedulerBackend` |

## Accumulators

`sc.longAccumulator(name)` / `doubleAccumulator` — write-only-on-executors,
read-on-driver counters. Tasks add deltas; the backend merges each task's
accumulator updates into the driver-side registry on `StatusUpdate`, so a value
read after the action reflects every task's contribution.

## Where to look

| Concern | Class |
|--------|-------|
| Event types | `status/SchedulerEvent.java` |
| Bus | `status/LiveListenerBus.java` |
| Store | `status/AppStatusStore.java` |
| UI | `ui/MiniSparkUI.java` |
| Accumulators | `accumulator/*` |
