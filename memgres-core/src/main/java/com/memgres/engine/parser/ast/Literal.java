package com.memgres.engine.parser.ast;

/**
 * A literal value: integer, float, string, boolean, null, bit string.
 */
public final class Literal implements Expression {
    public final LiteralType literalType;
    public final String value;
    /**
     * Where the word stood in the statement text, counted from zero, or -1 when the node was not
     * read from one. PostgreSQL points a complaint about a keyword at the keyword itself, and
     * DEFAULT is the one literal a statement can be refused for holding, so where it was written
     * is kept with it rather than looked for in the text afterwards — a search that took the
     * place of the first DEFAULT the statement spells, legal or not.
     */
    private final int offset;

    public Literal(LiteralType literalType, String value) {
        this(literalType, value, -1);
    }

    public Literal(LiteralType literalType, String value, int offset) {
        this.literalType = literalType;
        this.value = value;
        this.offset = offset;
    }

    public enum LiteralType {
        INTEGER, FLOAT, STRING, BOOLEAN, NULL, DEFAULT, BIT_STRING
    }

    public static Literal ofInt(String value) { return new Literal(LiteralType.INTEGER, value); }
    public static Literal ofFloat(String value) { return new Literal(LiteralType.FLOAT, value); }
    public static Literal ofString(String value) { return new Literal(LiteralType.STRING, value); }
    public static Literal ofBoolean(boolean value) { return new Literal(LiteralType.BOOLEAN, String.valueOf(value)); }
    public static Literal ofNull() { return new Literal(LiteralType.NULL, null); }
    public static Literal ofDefault() { return new Literal(LiteralType.DEFAULT, null); }
    public static Literal ofDefault(int offset) { return new Literal(LiteralType.DEFAULT, null, offset); }
    public static Literal ofBitString(String value) { return new Literal(LiteralType.BIT_STRING, value); }

    public LiteralType literalType() { return literalType; }
    public String value() { return value; }
    public int offset() { return offset; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Literal that = (Literal) o;
        return java.util.Objects.equals(literalType, that.literalType)
            && java.util.Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(literalType, value);
    }

    @Override
    public String toString() {
        return "Literal[literalType=" + literalType + ", " + "value=" + value + "]";
    }
}
