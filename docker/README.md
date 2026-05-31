# Multi-node MiniSpark on Docker

Runs a genuine multi-node cluster — a **ResourceManager, two NodeManagers, and a
submit client, each in its own container** with its own hostname/IP on a
user-defined bridge network. Executor JVMs are spawned by the NodeManagers
*inside their containers* and advertise their container IP, so executor↔driver
RPC, executor↔executor shuffle fetch, and broadcast all cross real
container-to-container TCP — not loopback.

A `shared` named volume is mounted at `/shared` on every node so the file-I/O
examples (`saveAsTextFile`, `checkpoint`, CSV round-trip) work across nodes —
the real-cluster requirement of shared storage (HDFS/S3/NFS), met here by one
volume.

## Run it

```bash
docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml up -d rm nm1 nm2   # cluster

# the FULL example battery — every feature category, asserted, across the cluster
docker compose -f docker/docker-compose.yml run --rm all
# …or just WordCount via the spark-submit equivalent
docker compose -f docker/docker-compose.yml run --rm submit

docker compose -f docker/docker-compose.yml down -v            # tear down
```

The `all` service runs `AllExamplesDriver` — it prints `PASS`/`FAIL` per
example and exits non-zero if any check fails. The `submit` service runs
`MiniSparkSubmit --class WordCount`; the RM places the two executors across
`nm1` and `nm2`.

## What was verified

The **full example battery — 33 checks covering every feature category** — was
run across this topology and **all pass with no serialization errors**
(`ALL_EXAMPLES pass=33 fail=0`):

| Category | Checks |
|----------|--------|
| RDD core | sum-of-squares, flatMap/count, wordcount, groupByKey, join, sortByKey, distinct, take/first, takeOrdered, cogroup |
| Caching / storage | cache, MEMORY_AND_DISK spill |
| Broadcast / accumulator | broadcast lookup, accumulator |
| Shuffle | sort shuffle, spillable aggregation |
| Scheduling | FAIR pool |
| File I/O (shared volume) | saveAsTextFile→read, checkpoint, CSV read→write→read |
| AQE | coalesce + skew split |
| SQL | filter/select, GROUP BY, HAVING+ORDER BY, ORDER BY LIMIT, window (row_number/rank/dense_rank), inner/left/right/full/semi/cross/non-equi joins |

The live container run (4 containers, distinct bridge IPs `172.18.0.2/3/4`)
verified the core groups with executors registering at their **container IPs**
and `reduceByKey` shuffles crossing between containers; the committed
`AllExamplesDriver` extends that to all 33 checks (also continuously verified
on a multi-process cluster by `MultiNodeClusterTest`).

> Surfacing this exposed and fixed a real multi-node bug: executors had used
> `InetAddress.getLocalHost()`, which returns loopback when the hostname maps to
> `127.0.0.1` — they now resolve a routable NIC (Spark's
> `Utils.findLocalInetAddress` behaviour), overridable via
> `-Dminispark.executor.host`.

For a single-host automated version of the same proof (bound to the host's
routable IP rather than separate containers), see
`minispark-examples/.../MultiNodeClusterTest.java`.
