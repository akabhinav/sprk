# Component: MiniYarn cluster manager (`miniyarn`)

A YARN-like resource manager that allocates executor processes across nodes. It
is what turns `setMaster("miniyarn://host:8032")` into real containers on real
NodeManagers.

**Real equivalents:** Hadoop YARN `ResourceManager` / `NodeManager`, and Spark's
`ApplicationMaster` + `YarnAllocator`.

## The cast

```mermaid
flowchart TB
    subgraph RMnode["ResourceManager (master)"]
        RM["tracks NodeManagers + free capacity\nFIFO + least-loaded scheduler"]
    end
    subgraph N1["NodeManager nm1"]
        NM1["registers capacity, heartbeats,\nlaunches container processes"]
    end
    subgraph N2["NodeManager nm2"]
        NM2["…"]
    end
    subgraph Driver["Driver JVM"]
        AM["ApplicationMaster\n(yarn-client mode: in the driver)"]
        BE["YarnExecutorLauncher → CoarseGrainedSchedulerBackend"]
    end
    NM1 -->|register/heartbeat| RM
    NM2 -->|register/heartbeat| RM
    AM -->|RegisterApplication / RequestContainers| RM
    RM -->|ContainersAllocated| AM
    AM -->|LaunchContainer| NM1
    AM -->|LaunchContainer| NM2
    NM1 -->|spawn executor JVM| EX1["Executor (container)"]
    NM2 --> EX2["Executor (container)"]
    EX1 -->|RegisterExecutor| BE
    EX2 -->|RegisterExecutor| BE
```

## Full job lifecycle (Spark-on-YARN, mirrored)

```mermaid
sequenceDiagram
    participant U as MiniSparkContext (miniyarn://)
    participant AM as ApplicationMaster
    participant RM as ResourceManager
    participant NM as NodeManager
    participant EX as Executor (container)
    participant DR as Driver backend
    U->>AM: start (YarnExecutorLauncher)
    AM->>RM: RegisterApplication → app_N
    AM->>RM: RequestContainers(N, cores, mem, launchCtx)
    RM->>RM: pick least-loaded fitting node per container
    RM-->>AM: ContainersAllocated([…])
    AM->>NM: LaunchContainer(container, ctx with {{CONTAINER_ID}})
    NM->>EX: ProcessBuilder spawns executor JVM
    EX->>DR: RegisterExecutor (dials the driver directly)
    DR->>EX: LaunchTask … (normal scheduling from here)
    U->>AM: on close → UnregisterApplication
    RM->>NM: KillContainer
```

## Wire protocol (`YarnMessages`)

| Direction | Messages |
|-----------|----------|
| NM → RM | `RegisterNodeManager`, `NodeHeartbeat` (free capacity + container statuses) |
| AM → RM | `RegisterApplication`, `RequestContainers`, `ReleaseContainer`, `UnregisterApplication` |
| RM → AM | `ContainersAllocated` |
| AM → NM | `LaunchContainer`, `KillContainer` |
| NM → AM | `ContainerCompleted` |

## Scheduler

The RM runs a single-threaded **FIFO with least-loaded placement**: for each
pending demand it packs containers onto the node with the most free cores that
still fits, spreading executors across the cluster. Allocation and launch are
**separate hops** (RM only reserves; the AM tells the NM to actually start),
exactly like real YARN.

## The integration point: one class

`YarnExecutorLauncher` is the third implementation of the core engine's
`ExecutorLauncher` seam (alongside `Local` and `Process`). The
`DAGScheduler`/`TaskScheduler`/`CoarseGrainedSchedulerBackend` are **unchanged** —
the executor JVMs the NodeManagers spawn are the same `CoarseGrainedExecutorBackend.main`
used everywhere, and they dial back to the driver over the same Netty RPC.

```mermaid
flowchart LR
    SEAM["ExecutorLauncher (core seam)"]
    SEAM --> L["LocalExecutorLauncher\n(in-process)"]
    SEAM --> P["ProcessExecutorLauncher\n(local JVMs)"]
    SEAM --> Y["YarnExecutorLauncher\n(containers via RM/NM)"]
```

## Running it (separate processes)

See the **[Runbook](../RUNBOOK.md#cluster-mode-miniyarn)**. In short:
`scripts/start-rm.sh` → `scripts/start-nm.sh nm1 …` (×N) → `scripts/submit-yarn.sh`.

## Where to look

| Concern | Class |
|--------|-------|
| Resource model | `common/Resource.java`, `Container.java`, `ContainerId.java` |
| Wire protocol | `common/YarnMessages.java` |
| Master + scheduler | `rm/ResourceManager.java` |
| Node agent + container launch | `nm/NodeManager.java` |
| Per-app broker | `am/ApplicationMaster.java` |
| Engine integration | `minispark-core …/cluster/YarnExecutorLauncher.java` |
