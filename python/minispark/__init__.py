"""
MiniSpark for Python — a PySpark-shaped client over the MiniSpark JVM engine,
built the way real Spark does it:

  * control plane (this package): thin proxies that drive the JVM over a socket
    gateway (the Py4J role); DataFrame / SQL ops run natively in the JVM.
  * data plane (PythonRDD on the JVM): RDD transforms carrying a Python lambda
    fork a `python` worker on the executor and pipe records through it.

    >>> from minispark import SparkSession
    >>> spark = SparkSession.builder("demo", "local[*]")
    >>> spark.sql("SELECT 1 AS x").show()
    >>> sc = spark._sc
    >>> sc.parallelize([1, 2, 3]).map(lambda x: x * 10).collect()
    [10, 20, 30]
"""
from .context import SparkContext
from .rdd import RDD
from .sql import DataFrame, SparkSession

__all__ = ["SparkContext", "SparkSession", "DataFrame", "RDD"]
