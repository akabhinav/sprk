package com.minispark.python;

import java.io.Serializable;

/**
 * A user's Python function shipped to the executors: the raw bytes of its
 * cloudpickle (so closures / globals survive), plus how to apply it. The JVM
 * never interprets these bytes — it hands them to the {@code python} worker,
 * which unpickles and runs them. Mirrors the {@code command} that real Spark's
 * {@code PythonFunction} carries.
 */
public final class PythonFunction implements Serializable {

    /** How the worker applies the function to each record. */
    public enum EvalType {
        MAP(0), FILTER(1), FLATMAP(2), GROUP_REDUCE(3);
        public final int code;
        EvalType(int code) { this.code = code; }
    }

    private final byte[] command;     // cloudpickle of the python callable
    private final EvalType evalType;

    public PythonFunction(byte[] command, EvalType evalType) {
        this.command = command;
        this.evalType = evalType;
    }

    public byte[] command() { return command; }
    public EvalType evalType() { return evalType; }
}
