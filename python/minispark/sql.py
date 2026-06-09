"""
DataFrame / SQL proxies — the "fast path". Every operation here runs entirely
in the JVM (the engine's Catalyst-mini optimizer + RDD executor); no Python
runs on the executors. This mirrors real PySpark, where the DataFrame API is a
thin Py4J veneer over the JVM Dataset.
"""
from .context import SparkContext


class SparkSession:
    def __init__(self, master="local[*]", appName="pyminispark", conf=None, _sc=None):
        self._sc = _sc or SparkContext(master, appName, conf)
        self._gw = self._sc._gw
        self._ref = self._gw.call("newSession", ctx=self._sc._ref.id)

    @classmethod
    def builder(cls, appName="pyminispark", master="local[*]", conf=None):
        return SparkSession(master=master, appName=appName, conf=conf)

    @property
    def read(self):
        return DataFrameReader(self)

    def createDataFrame(self, rows, schema):
        """rows: list of lists; schema: list of (name, type) pairs."""
        return DataFrame(self, self._gw.call(
            "createDataFrame", session=self._ref.id,
            rows=[list(r) for r in rows], schema=[list(f) for f in schema]))

    def sql(self, query):
        return DataFrame(self, self._gw.call("sql", session=self._ref.id, query=query))

    def stop(self):
        self._sc.stop()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop()


class DataFrameReader:
    def __init__(self, session):
        self._s = session

    def csv(self, path, header=False, inferSchema=False):
        return DataFrame(self._s, self._s._gw.call(
            "readCsv", session=self._s._ref.id, path=path,
            header=bool(header), inferSchema=bool(inferSchema)))


class DataFrame:
    def __init__(self, session, ref):
        self._s = session
        self._gw = session._gw
        self._ref = ref

    def createOrReplaceTempView(self, name):
        self._gw.call("createOrReplaceTempView", df=self._ref.id, name=name)

    def select(self, *cols):
        return DataFrame(self._s, self._gw.call("dfSelect", df=self._ref.id, cols=list(cols)))

    def filter(self, condition):
        """condition is a SQL boolean string, e.g. \"age >= 60\"."""
        return DataFrame(self._s, self._gw.call("dfFilter", df=self._ref.id, condition=condition))

    where = filter

    def columns(self):
        return self._gw.call("dfColumns", df=self._ref.id)

    def count(self):
        return self._gw.call("dfCount", df=self._ref.id)

    def collect(self):
        return self._gw.call("dfCollect", df=self._ref.id)

    def show(self, n=20):
        print(self._gw.call("dfShow", df=self._ref.id, n=n), end="")
