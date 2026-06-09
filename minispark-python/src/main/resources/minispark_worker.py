"""
MiniSpark Python worker — the executor-side half of the data plane.

Forked by PythonRDD on the JVM executor. Reads, over stdin:
  int32 evalType
  int32 funcLen, funcLen bytes  (cloudpickle of the user's callable)
  then per record: int32 len, len bytes (UTF-8 JSON), terminated by int32 -1
Applies the function to each record per evalType and writes results back over
stdout in the same length-prefixed JSON framing.

All ints are big-endian, matching Java's DataInputStream/DataOutputStream.
Mirrors real Spark's pyspark/worker.py.
"""
import sys
import json
import struct
import functools
import cloudpickle

# evalType codes — must match PythonFunction.EvalType
MAP, FILTER, FLATMAP, GROUP_REDUCE = 0, 1, 2, 3
END_OF_DATA = -1


def _read_int(stream):
    data = stream.read(4)
    if len(data) < 4:
        raise EOFError("short read")
    return struct.unpack(">i", data)[0]


def _read_n(stream, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            raise EOFError("short read")
        buf.extend(chunk)
    return bytes(buf)


def _write_record(stream, obj):
    data = json.dumps(obj).encode("utf-8")
    stream.write(struct.pack(">i", len(data)))
    stream.write(data)


def main():
    inp = sys.stdin.buffer
    out = sys.stdout.buffer

    eval_type = _read_int(inp)
    func_len = _read_int(inp)
    func = cloudpickle.loads(_read_n(inp, func_len))

    while True:
        rec_len = _read_int(inp)
        if rec_len == END_OF_DATA:
            break
        record = json.loads(_read_n(inp, rec_len).decode("utf-8"))

        if eval_type == MAP:
            _write_record(out, func(record))
        elif eval_type == FILTER:
            if func(record):
                _write_record(out, record)
        elif eval_type == FLATMAP:
            for item in func(record):
                _write_record(out, item)
        elif eval_type == GROUP_REDUCE:
            # record is [key, [v0, v1, ...]] — fold the values with the reducer.
            key, values = record[0], record[1]
            _write_record(out, [key, functools.reduce(func, values)])
        else:
            raise ValueError("unknown evalType %d" % eval_type)

    out.write(struct.pack(">i", END_OF_DATA))
    out.flush()


if __name__ == "__main__":
    main()
