package com.memgres.engine.parser;

/**
 * A single token produced by the SQL lexer.
 */
public final class Token {
    public final TokenType type;
    public final String value;
    public final int position;

    public Token(TokenType type, String value, int position) {
        this.type = type;
        this.value = value;
        this.position = position;
    }

    @Override
    public String toString() {
        return type + "(" + value + ")@" + position;
    }

    public TokenType type() { return type; }
    public String value() { return value; }
    public int position() { return position; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Token that = (Token) o;
        return java.util.Objects.equals(type, that.type)
            && java.util.Objects.equals(value, that.value)
            && position == that.position;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(type, value, position);
    }
}
