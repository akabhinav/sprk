"""
A PySpark-style demo against MiniSpark. Shows both planes:
  * DataFrame / SQL — runs natively in the JVM (no Python on executors).
  * RDD with Python lambdas — forks a python worker on the executor (data plane).

Run:  scripts/pyminispark.sh python/examples/demo.py
"""
from minispark import SparkSession

spark = SparkSession.builder("py-demo", "local[2]")

print("== DataFrame / SQL (runs in the JVM) ==")
people = spark.createDataFrame(
    [["alice", 30, "NYC"], ["bob", 25, "LA"], ["carol", 35, "NYC"]],
    [["name", "string"], ["age", "int"], ["city", "string"]],
)
people.createOrReplaceTempView("people")
spark.sql("SELECT city, count(*) AS n, avg(age) AS avg_age FROM people GROUP BY city ORDER BY city").show()
people.filter("age >= 30").select("name", "city").show()

print("== RDD with Python lambdas (forks a python worker on the executor) ==")
sc = spark._sc

nums = sc.parallelize(range(1, 11), 4)
print("sum of squares of evens:",
      sum(nums.filter(lambda x: x % 2 == 0).map(lambda x: x * x).collect()))

text = ["the quick brown fox", "the lazy dog and the fox", "fox fox dog the the the"]
counts = (sc.parallelize(text, 3)
            .flatMap(lambda line: line.split())
            .map(lambda w: (w, 1))
            .reduceByKey(lambda a, b: a + b)
            .collect())
print("word counts:", sorted((w, c) for w, c in counts))

spark.stop()
print("OK")
