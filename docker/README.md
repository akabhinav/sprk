# Multi-node MiniSpark on Docker

Runs a genuine multi-node cluster — a **ResourceManager, two NodeManagers, and a
submit client, each in its own container** with its own hostname/IP on a
user-defined bridge network. Executor JVMs are spawned by the NodeManagers
*inside their containers* and advertise their container IP, so executor↔driver
RPC, executor↔executor shuffle fetch, and broadcast all cross real
container-to-container TCP — not loopback.

## Run it

```bash
docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml up -d rm nm1 nm2   # cluster
docker compose -f docker/docker-compose.yml run --rm submit    # WordCount across the cluster
docker compose -f docker/docker-compose.yml down               # tear down
```

The `submit` service runs `MiniSparkSubmit --class WordCount --master
miniyarn://rm:8032`; the RM places the two executors across `nm1` and `nm2`,
and you'll see the word counts on stdout.

## What was verified

The full feature battery was run against this exact topology (4 containers,
distinct IPs `172.18.0.2/3/4` on the bridge) and **all groups passed**:

| Group | Result |
|-------|--------|
| RDD pipeline + shuffles (map/filter/reduce, join, distinct) | PASS |
| broadcast + accumulator | PASS |
| sort shuffle | PASS |
| AQE coalesce + skew split | PASS |
| spillable aggregation (tight memory budget) | PASS |
| SQL: window, inner/left-semi/cross/non-equi joins | PASS |

Executors registered at their **container IPs** (e.g. `172.18.0.3`,
`172.18.0.4`) and the `reduceByKey` shuffle crossed between containers — the
property that distinguishes a true multi-node run from single-host loopback.

> Surfacing this exposed and fixed a real multi-node bug: executors had used
> `InetAddress.getLocalHost()`, which returns loopback when the hostname maps to
> `127.0.0.1` — they now resolve a routable NIC (Spark's
> `Utils.findLocalInetAddress` behaviour), overridable via
> `-Dminispark.executor.host`.

For a single-host automated version of the same proof (bound to the host's
routable IP rather than separate containers), see
`minispark-examples/.../MultiNodeClusterTest.java`.
