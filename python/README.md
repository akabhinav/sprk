# MiniSpark for Python (PySpark-shaped)

A PySpark-style client over the MiniSpark JVM engine, built **the way real Spark
does it** — two planes:

| Plane | What runs where | Mechanism |
|-------|-----------------|-----------|
| **Control** (DataFrame / SQL) | entirely in the **JVM** (Catalyst-mini + RDD executor) | a socket **gateway** — Python objects are thin proxies holding handles to JVM objects (the role **Py4J** plays in real PySpark) |
| **Data** (RDD + Python lambdas) | your lambda runs in a **forked `python` worker on the executor** | the function travels as **cloudpickle**; records are piped to the worker as length-prefixed JSON (real Spark's **PythonRDD / PythonRunner**) |

The split is the whole point: `spark.sql(...)` / `df.filter(...)` never touch
Python on the executors (full JVM speed), while `rdd.map(lambda …)` does — and
*that* is the genuinely cross-language part.

## Requirements

- The built JVM project (`mvn install`).
- `python3` with **cloudpickle** (`pip install cloudpickle`) — on the driver
  **and on every executor host**, since the worker is forked there. Real Spark
  has the identical requirement.

## Run

```bash
scripts/pyminispark.sh python/examples/demo.py
```

The launcher builds the project, assembles the JVM classpath, sets `PYTHONPATH`,
and runs your script. `SKIP_BUILD=1` skips the rebuild; `MINISPARK_PYTHON`
chooses the interpreter.

## Example

```python
from minispark import SparkSession

spark = SparkSession.builder("demo", "local[2]")

# --- control plane: runs in the JVM, no python on executors ---
people = spark.createDataFrame(
    [["alice", 30, "NYC"], ["bob", 25, "LA"], ["carol", 35, "NYC"]],
    [["name", "string"], ["age", "int"], ["city", "string"]])
people.createOrReplaceTempView("people")
spark.sql("SELECT city, count(*) AS n, avg(age) FROM people GROUP BY city").show()
people.filter("age >= 30").select("name", "city").show()

# --- data plane: python lambdas, forked workers on the executor ---
sc = spark._sc
sc.parallelize([1, 2, 3, 4]).map(lambda x: x * x).collect()      # [1, 4, 9, 16]
(sc.parallelize(["the fox", "the dog the"], 2)
   .flatMap(lambda s: s.split())
   .map(lambda w: (w, 1))
   .reduceByKey(lambda a, b: a + b)
   .collect())                                                   # [['the', 3], ['fox', 1], ['dog', 1]]

spark.stop()
```

## On a cluster (incl. Docker)

Point the session at a cluster master and the control plane just works:

```python
spark = SparkSession.builder("demo", "miniyarn://rm:8032")
```

For the **data plane** on a cluster, every executor host (or container) must
have `python3` + `cloudpickle` — exactly Spark's "Python on every node"
requirement. The provided `docker/` image installs both, so RDD-lambda jobs run
across containers too.

## API surface

- `SparkSession` — `builder`, `read.csv`, `createDataFrame`, `sql`, `stop`
- `DataFrame` — `createOrReplaceTempView`, `select`, `filter`/`where`, `columns`,
  `count`, `collect`, `show`
- `SparkContext` (`spark._sc`) — `parallelize`, `textFile`
- `RDD` — `map`, `filter`, `flatMap`, `reduceByKey`, `collect`, `count`, `take`

## What's not (yet) here vs real PySpark

- Pandas/vectorized (Arrow) UDFs — real Spark's fast path for Python UDFs.
- Worker reuse via a daemon (we fork one worker per partition).
- The full DataFrame DSL (`groupBy().agg(...)` fluent API) — use `spark.sql(...)`.
- Accumulators / broadcast from the Python side.

These are the natural next steps; the architecture (gateway + PythonRDD worker
pipe) is the same one they'd build on.
