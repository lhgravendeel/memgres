package com.memgres.engine.parser;

/**
 * A single token produced by the SQL lexer.
 */
public final class Token {
    public final TokenType type;
    public final String value;
    public final int position;
    /** The word as it was written, before the lexer folded its case. Null when it is unchanged. */
    private final String raw;

    public Token(TokenType type, String value, int position) {
        this(type, value, position, null);
    }

    public Token(TokenType type, String value, int position, String raw) {
        this.type = type;
        this.value = value;
        this.position = position;
        this.raw = raw;
    }

    /**
     * The token's text as the statement spelled it.
     *
     * <p>PostgreSQL quotes the user's own spelling back in a syntax error, so the message points
     * at something findable in the statement rather than at the lexer's folded form.
     */
    public String raw() { return raw != null ? raw : value; }

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
