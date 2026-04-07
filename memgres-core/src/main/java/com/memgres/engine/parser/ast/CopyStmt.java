package com.memgres.engine.parser.ast;

import java.util.List;
import java.util.Objects;

/**
 * COPY table (columns) FROM/TO source [WITH (options)] [WHERE ...]
 */
public final class CopyStmt implements Statement {
    public final String table;
    public final List<String> columns;
    public final boolean isFrom;                // false = COPY TO (export)
    public final String source;                 // filename or 'STDIN'/'STDOUT'
    public final String format;                 // 'csv', 'text', 'binary'
    public final String delimiter;
    public final String nullString;
    public final boolean header;
    public final List<List<String>> inlineData; // for COPY FROM STDIN with inline data
    public final Statement subquery;            // for COPY (SELECT ...) TO form
    public final String quote;                  // CSV quote character (default '"')
    public final String escape;                 // CSV escape character (default = quote)
    public final List<String> forceQuote;       // CSV FORCE_QUOTE column list
    public final List<String> forceNotNull;     // CSV FORCE_NOT_NULL column list
    public final boolean freeze;                // FREEZE option
    public final String encoding;               // ENCODING option
    public final String whereClause;            // WHERE clause for COPY TO
    public final String onError;                // ON_ERROR option ('stop' or 'ignore')
    public final String defaultString;

    public CopyStmt(
            String table,
            List<String> columns,
            boolean isFrom,
            String source,
            String format,
            String delimiter,
            String nullString,
            boolean header,
            List<List<String>> inlineData,
            Statement subquery,
            String quote,
            String escape,
            List<String> forceQuote,
            List<String> forceNotNull,
            boolean freeze,
            String encoding,
            String whereClause,
            String onError,
            String defaultString
    ) {
        this.table = table;
        this.columns = columns;
        this.isFrom = isFrom;
        this.source = source;
        this.format = format;
        this.delimiter = delimiter;
        this.nullString = nullString;
        this.header = header;
        this.inlineData = inlineData;
        this.subquery = subquery;
        this.quote = quote;
        this.escape = escape;
        this.forceQuote = forceQuote;
        this.forceNotNull = forceNotNull;
        this.freeze = freeze;
        this.encoding = encoding;
        this.whereClause = whereClause;
        this.onError = onError;
        this.defaultString = defaultString;
    }

    /** Full constructor without onError/defaultString (backward-compatible). */
    public CopyStmt(String table, List<String> columns, boolean isFrom, String source,
                    String format, String delimiter, String nullString, boolean header,
                    List<List<String>> inlineData, Statement subquery,
                    String quote, String escape, List<String> forceQuote, List<String> forceNotNull,
                    boolean freeze, String encoding, String whereClause) {
        this(table, columns, isFrom, source, format, delimiter, nullString, header,
             inlineData, subquery, quote, escape, forceQuote, forceNotNull, freeze, encoding,
             whereClause, null, null);
    }

    /** Constructor without new options (backward-compatible). */
    public CopyStmt(String table, List<String> columns, boolean isFrom, String source,
                    String format, String delimiter, String nullString, boolean header,
                    List<List<String>> inlineData, Statement subquery) {
        this(table, columns, isFrom, source, format, delimiter, nullString, header,
             inlineData, subquery, "\"", "\"", null, null, false, null, null);
    }

    /** Backward-compatible constructor without subquery. */
    public CopyStmt(String table, List<String> columns, boolean isFrom, String source,
                    String format, String delimiter, String nullString, boolean header,
                    List<List<String>> inlineData) {
        this(table, columns, isFrom, source, format, delimiter, nullString, header, inlineData, null);
    }

    public String table() { return table; }
    public List<String> columns() { return columns; }
    public boolean isFrom() { return isFrom; }
    public String source() { return source; }
    public String format() { return format; }
    public String delimiter() { return delimiter; }
    public String nullString() { return nullString; }
    public boolean header() { return header; }
    public List<List<String>> inlineData() { return inlineData; }
    public Statement subquery() { return subquery; }
    public String quote() { return quote; }
    public String escape() { return escape; }
    public List<String> forceQuote() { return forceQuote; }
    public List<String> forceNotNull() { return forceNotNull; }
    public boolean freeze() { return freeze; }
    public String encoding() { return encoding; }
    public String whereClause() { return whereClause; }
    public String onError() { return onError; }
    public String defaultString() { return defaultString; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CopyStmt that = (CopyStmt) o;
        return isFrom == that.isFrom
            && header == that.header
            && freeze == that.freeze
            && Objects.equals(table, that.table)
            && Objects.equals(columns, that.columns)
            && Objects.equals(source, that.source)
            && Objects.equals(format, that.format)
            && Objects.equals(delimiter, that.delimiter)
            && Objects.equals(nullString, that.nullString)
            && Objects.equals(inlineData, that.inlineData)
            && Objects.equals(subquery, that.subquery)
            && Objects.equals(quote, that.quote)
            && Objects.equals(escape, that.escape)
            && Objects.equals(forceQuote, that.forceQuote)
            && Objects.equals(forceNotNull, that.forceNotNull)
            && Objects.equals(encoding, that.encoding)
            && Objects.equals(whereClause, that.whereClause)
            && Objects.equals(onError, that.onError)
            && Objects.equals(defaultString, that.defaultString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, columns, isFrom, source, format, delimiter, nullString,
                header, inlineData, subquery, quote, escape, forceQuote, forceNotNull,
                freeze, encoding, whereClause, onError, defaultString);
    }

    @Override
    public String toString() {
        return "CopyStmt[" +
                "table=" + table +
                ", columns=" + columns +
                ", isFrom=" + isFrom +
                ", source=" + source +
                ", format=" + format +
                ", delimiter=" + delimiter +
                ", nullString=" + nullString +
                ", header=" + header +
                ", inlineData=" + inlineData +
                ", subquery=" + subquery +
                ", quote=" + quote +
                ", escape=" + escape +
                ", forceQuote=" + forceQuote +
                ", forceNotNull=" + forceNotNull +
                ", freeze=" + freeze +
                ", encoding=" + encoding +
                ", whereClause=" + whereClause +
                ", onError=" + onError +
                ", defaultString=" + defaultString +
                "]";
    }
}
