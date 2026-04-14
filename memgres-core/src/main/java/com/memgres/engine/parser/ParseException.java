package com.memgres.engine.parser;

import com.memgres.engine.MemgresException;

/**
 * Exception thrown when parsing fails.
 */
public class ParseException extends MemgresException {

    public ParseException(String message, Token token) {
        super("syntax error: " + message + " at position " + token.position() + " near '" + token.value() + "'", "42601");
    }

    public ParseException(String message, Token token, String sqlState) {
        super("syntax error: " + message + " at position " + token.position() + " near '" + token.value() + "'", sqlState);
    }
}
