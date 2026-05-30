package com.minispark.sql.parser;

/** Thrown on a lexical or grammatical error while parsing a SQL string. */
public final class ParseException extends RuntimeException {
    public ParseException(String message) { super(message); }
}
