package com.memgres.engine.parser;

import com.memgres.engine.MemgresException;

/**
 * Exception thrown when parsing fails.
 */
public class ParseException extends MemgresException {

    public ParseException(String message, Token token) {
        super("syntax error at or near \"" + token.value() + "\"", "42601");
        if (token.position() >= 0) setPosition(token.position() + 1); // 1-based
    }

    public ParseException(String message, Token token, String sqlState) {
        super("syntax error at or near \"" + token.value() + "\"", sqlState);
        if (token.position() >= 0) setPosition(token.position() + 1); // 1-based
    }
}
