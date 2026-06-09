"""
RDD proxy. Transformations carrying a Python function (map/filter/flatMap/
reduceByKey) ship the function as cloudpickle to the JVM, which forks a Python
worker on the executor to run it (the data plane). Actions materialise results
back to the driver.
"""
from .gateway import dump_func


class RDD:
    def __init__(self, ctx, ref):
        self._ctx = ctx
        self._gw = ctx._gw
        self._ref = ref

    # ---- transformations (run a Python function on executor data) ----
    def map(self, f):
        return RDD(self._ctx, self._gw.call("pyMap", rdd=self._ref.id, func=dump_func(f)))

    def filter(self, f):
        return RDD(self._ctx, self._gw.call("pyFilter", rdd=self._ref.id, func=dump_func(f)))

    def flatMap(self, f):
        return RDD(self._ctx, self._gw.call("pyFlatMap", rdd=self._ref.id, func=dump_func(f)))

    def reduceByKey(self, f):
        """On a (k, v) RDD: shuffle by key, fold each group's values with f."""
        return RDD(self._ctx, self._gw.call("reduceByKey", rdd=self._ref.id, func=dump_func(f)))

    # ---- actions ----
    def collect(self):
        return self._gw.call("collect", rdd=self._ref.id)

    def count(self):
        return self._gw.call("count", rdd=self._ref.id)

    def take(self, n):
        return self._gw.call("take", rdd=self._ref.id, n=n)
