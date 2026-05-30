# MiniSpark Components

This folder documents each subsystem of MiniSpark: what it does, how it's
structured, the key classes, and how it maps to real Apache Spark. Diagrams use
[Mermaid](https://mermaid.js.org/) (renders on GitHub).

## Reading order

1. **[RPC layer](01-rpc.md)** — the messaging seam everything else rides on.
2. **[Core engine](02-core-engine.md)** — RDDs, the DAG/Task schedulers, the executor.
3. **[Storage & shuffle](03-storage-shuffle.md)** — block storage tiers and the two shuffle implementations.
4. **[Fault tolerance & scaling](04-fault-tolerance-scaling.md)** — retries, lineage recovery, speculation, dynamic allocation, fair pools.
5. **[MiniYarn](05-miniyarn.md)** — the cluster manager (RM/NM/AM/containers).
6. **[SQL / DataFrame](06-sql.md)** — the miniature Catalyst on top of the engine.
7. **[Web UI & observability](07-ui-observability.md)** — the event bus and status UI.

## The component map

```mermaid
flowchart TB
    subgraph M_SQL["minispark-sql"]
        direction TB
        DF["DataFrame / Column / MiniSparkSession"]
        PARSE["SqlParser (lexer + recursive descent)"]
        ANA["Analyzer + Catalog"]
        OPT["Optimizer (rule-based)"]
        PLAN["SparkPlanner → PhysicalPlan"]
        DF --> ANA
        PARSE --> ANA --> OPT --> PLAN
    end

    subgraph M_CORE["minispark-core"]
        direction TB
        RDD["RDD lineage + transformations"]
        DAGS["DAGScheduler"]
        TASKS["TaskScheduler (+ pools, speculation)"]
        BACK["CoarseGrainedSchedulerBackend"]
        EXEC["Executor (virtual-thread TaskRunner)"]
        STORE["BlockManager (memory+disk) / MapOutputTracker"]
        SHUF["ShuffleManager (hash | sort)"]
        STATUS["LiveListenerBus → AppStatusStore"]
        UI["MiniSparkUI (HTTP)"]
        RDD --> DAGS --> TASKS --> BACK --> EXEC
        EXEC --> STORE
        EXEC --> SHUF
        BACK --> STATUS --> UI
    end

    subgraph M_RPC["minispark-rpc"]
        RPCENV["RpcEnv (Local | Netty)"]
        SER["Serializer (Java)"]
    end

    subgraph M_YARN["miniyarn"]
        RMC["ResourceManager"]
        NMC["NodeManager"]
        AMC["ApplicationMaster"]
    end

    PLAN -->|builds RDDs| RDD
    BACK -. uses .-> RPCENV
    STORE -. uses .-> RPCENV
    AMC -. uses .-> RPCENV
    BACK -. cluster mode .-> AMC
    AMC --> RMC --> NMC
```

## Module dependencies

```mermaid
flowchart LR
    rpc[minispark-rpc] --> yarn[miniyarn]
    rpc --> core[minispark-core]
    yarn --> core
    core --> sql[minispark-sql]
    core --> ex[minispark-examples]
    sql --> ex
```

## The four seams at a glance

| Interface | Local impl | Distributed impl | Doc |
|-----------|-----------|------------------|-----|
| `SchedulerBackend` | `LocalExecutorLauncher` | `Process` / `Yarn` launcher | [core](02-core-engine.md) |
| `RpcEnv` | `LocalRpcEnv` | `NettyRpcEnv` (TCP) | [rpc](01-rpc.md) |
| `BlockManager` | in-memory only | `NetworkBlockManager` (RPC fetch) | [storage](03-storage-shuffle.md) |
| `ShuffleManager` | `HashShuffleManager` | `SortShuffleManager` | [storage](03-storage-shuffle.md) |
