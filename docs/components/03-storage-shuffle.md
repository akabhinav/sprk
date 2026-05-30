# Component: Storage & Shuffle (`minispark-core`)

How partition bytes are stored, fetched across the network, and exchanged
between map and reduce tasks.

## BlockManager — the storage seam

Every executor (and the driver) runs one `NetworkBlockManager`, publishing a
`"BlockManager"` RPC endpoint. It addresses everything by `BlockId` and serves a
two-tier local store with remote fetch.

```mermaid
flowchart TB
    subgraph BM["NetworkBlockManager (per executor)"]
        MEM["MemoryStore\nbounded, LRU eviction"]
        DISK["DiskStore\nfiles under local dir"]
        EP["RPC endpoint: FetchBlock(BlockId) → bytes"]
    end
    MEM -. "spill on evict\n(MEMORY_AND_DISK)" .-> DISK
    OTHER["another executor's\nBlockManager"] -. getRemoteBlock .-> EP
```

| BlockId kind | Used for |
|--------------|----------|
| `ShuffleBlock(shuffle, map, reduce)` | hash-shuffle bucket |
| `ShuffleDataBlock(shuffle, map)` | sort-shuffle consolidated map output |
| `RDDBlock(rdd, partition)` | a cached RDD partition |
| `BroadcastBlock(id)` | a broadcast variable |

**Storage levels** (`StorageLevel`): `MEMORY_ONLY` (drop if it won't fit →
recompute), `MEMORY_AND_DISK` (spill instead of dropping), `DISK_ONLY`.
`MemoryStore` is bounded (`minispark.memory.store.maxBytes`) and LRU-evicts
*evictable* cache blocks; shuffle/broadcast blocks are pinned.

## MapOutputTracker — where shuffle outputs live

Split into a driver **master** (authoritative) and executor **workers**.

```mermaid
sequenceDiagram
    participant MT as ShuffleMapTask (executor)
    participant DR as Driver (DAGScheduler + master tracker)
    participant RT as ReduceTask (executor)
    MT->>MT: write buckets to local BlockManager
    MT-->>DR: returns its ExecutorLocation
    DR->>DR: registerMapOutput(shuffle, mapId, location)
    RT->>DR: getMapStatuses(shuffle)  (worker asks master)
    DR-->>RT: list of (mapId, location)
    RT->>RT: fetch each bucket via BlockManager.getRemoteBlock
```

The driver registers map-output locations from completed map tasks; reducers ask
the master for the locations, then pull buckets from the owning executors. (No
worker-side caching — recoveries change locations, so every lookup is fresh.)

## The shuffle seam — two implementations

```mermaid
classDiagram
    class ShuffleManager {
        <<interface>>
        +registerShuffle(id, numMaps, partitioner) ShuffleHandle
        +getWriter(handle, mapId) ShuffleWriter
        +getReader(handle, start, end) ShuffleReader
    }
    ShuffleManager <|-- HashShuffleManager
    ShuffleManager <|-- SortShuffleManager
```

Selected by `minispark.shuffle.manager = hash | sort`. Driver and executors must
agree (the choice is forwarded to executor JVMs as a `-D`).

### Hash shuffle (default)

Each map task writes **one bucket per reducer** (`numMaps × numReduces` blocks).
Simple; the historical Spark design.

```mermaid
flowchart LR
    M0["map 0"] --> B00["(0→r0)"] & B01["(0→r1)"]
    M1["map 1"] --> B10["(1→r0)"] & B11["(1→r1)"]
    B00 --> R0["reduce 0"]
    B10 --> R0
    B01 --> R1["reduce 1"]
    B11 --> R1
```

### Sort shuffle

Each map task writes **one consolidated block** holding all reduce partitions
back-to-back, with per-partition counts acting as an index. `numMaps` blocks
regardless of reducer count — this is why real Spark switched defaults (hash
shuffle's file count explodes at scale).

```mermaid
flowchart LR
    M0["map 0"] --> D0["ShuffleDataBlock 0\n[part0 | part1 | …]"]
    M1["map 1"] --> D1["ShuffleDataBlock 1\n[part0 | part1 | …]"]
    D0 -- "slice part r0" --> R0["reduce 0"]
    D1 -- "slice part r0" --> R0
    D0 -- "slice part r1" --> R1["reduce 1"]
    D1 -- "slice part r1" --> R1
```

**Map-side combine:** `reduceByKey` folds each partition into partial results
*before* the shuffle, so only one value per key per partition crosses the
network — the same win over `groupByKey` that real Spark has.

## Partitioners

`HashPartitioner` (`hash(key) mod n`, MIN_VALUE-safe) and `RangePartitioner`
(samples the data to build sorted range bounds; backs `sortByKey` / `ORDER BY`).

## Broadcast variables

`sc.broadcast(value)` writes the value into the driver's BlockManager once; the
returned `TorrentBroadcast` handle is tiny (id + driver location). Each executor
fetches-and-caches the value on first `.value()` — so a big lookup table ships
once per executor, not once per task.

## Where to look

| Concern | Class |
|--------|-------|
| Block store + remote fetch | `storage/NetworkBlockManager.java`, `MemoryStore`, `DiskStore` |
| Block identity | `storage/BlockId.java` |
| Map-output registry | `storage/MapOutputTracker.java` |
| Hash / sort shuffle | `shuffle/HashShuffleManager.java`, `SortShuffleManager.java` |
| Partitioning | `shuffle/HashPartitioner.java`, `RangePartitioner.java` |
| Broadcast | `broadcast/TorrentBroadcast.java` |
