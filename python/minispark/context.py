"""SparkContext proxy — the entry point for the RDD API."""
from .gateway import Gateway
from .rdd import RDD


class SparkContext:
    def __init__(self, master="local[*]", appName="pyminispark", conf=None):
        self._gw = Gateway()
        self._ref = self._gw.call("newContext", master=master, appName=appName, conf=conf or {})

    def parallelize(self, data, numSlices=2):
        return RDD(self, self._gw.call("parallelize", ctx=self._ref.id, data=list(data), slices=numSlices))

    def textFile(self, path, numSlices=2):
        return RDD(self, self._gw.call("textFile", ctx=self._ref.id, path=path, slices=numSlices))

    def stop(self):
        self._gw.call("stop", ctx=self._ref.id)
        self._gw.close()

    # context-manager sugar
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop()
