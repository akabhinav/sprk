"""
The control-plane client — the Python end of the bridge (the role Py4J plays in
real PySpark). Launches the JVM PySparkGateway as a subprocess, connects over a
socket, and exchanges one-line JSON commands. Python objects (RDD, DataFrame,
…) are thin proxies holding a string handle to a live JVM object.
"""
import atexit
import base64
import json
import os
import socket
import subprocess
import sys

import cloudpickle


class _Ref:
    """A handle to a JVM object in the gateway registry."""
    __slots__ = ("id",)
    def __init__(self, id):
        self.id = id


class Gateway:
    def __init__(self):
        cp = os.environ.get("MINISPARK_CLASSPATH")
        if not cp:
            raise RuntimeError(
                "MINISPARK_CLASSPATH is not set. Run via scripts/pyminispark.sh, "
                "which builds the project and sets the classpath."
            )
        java_home = os.environ.get("JAVA_HOME")
        java = os.path.join(java_home, "bin", "java") if java_home else "java"
        self.proc = subprocess.Popen(
            [java, "-cp", cp, "com.minispark.python.PySparkGateway", "0"],
            stdout=subprocess.PIPE, stderr=sys.stderr,
        )
        port = None
        for raw in self.proc.stdout:
            line = raw.decode("utf-8", "replace").strip()
            if line.startswith("MINISPARK_GATEWAY_PORT="):
                port = int(line.split("=", 1)[1])
                break
        if port is None:
            raise RuntimeError("gateway did not report a port (did the JVM start?)")
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.io = self.sock.makefile("rw", encoding="utf-8", newline="\n")
        atexit.register(self.close)

    def call(self, op, **kw):
        kw["op"] = op
        self.io.write(json.dumps(kw))
        self.io.write("\n")
        self.io.flush()
        resp = json.loads(self.io.readline())
        if not resp.get("ok"):
            raise RuntimeError("MiniSpark error: " + str(resp.get("err")))
        return _unwrap(resp.get("ret"))

    def close(self):
        try:
            self.io.close()
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(5)
            except Exception:
                self.proc.kill()


def _unwrap(ret):
    """JSON return value → a _Ref handle, None, or a plain value."""
    if isinstance(ret, dict):
        if "ref" in ret:
            return _Ref(ret["ref"])
        if "null" in ret:
            return None
    return ret


def dump_func(f):
    """cloudpickle a user function and base64-encode it for the JSON wire."""
    return base64.b64encode(cloudpickle.dumps(f)).decode("ascii")
