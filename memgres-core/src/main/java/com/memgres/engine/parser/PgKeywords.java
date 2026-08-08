package com.memgres.engine.parser;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Which keywords a name is allowed to be.
 *
 * <p>PostgreSQL sorts its keywords into four classes, and where a word may stand depends on which
 * class it is in. A <em>reserved</em> word and a <em>type/function name</em> word cannot be a
 * column, a table alias or a parameter; a <em>col_name</em> word can be a column but not a function
 * or type name; an <em>unreserved</em> word can be anything.
 *
 * <p>Without the distinction the parser could only be uniformly strict or uniformly lax, and it was
 * lax: {@code CREATE TABLE t (order int)} declared a column called order, {@code SELECT * FROM t AS
 * left} aliased a table to a join keyword, and both are text PostgreSQL refuses. Being lax the
 * other way costs just as much — {@code SELECT exists FROM t} reads a perfectly ordinary column and
 * was a syntax error here.
 *
 * <p>Only the words this lexer treats as keywords need to appear; anything else is already an
 * ordinary identifier.
 */
final class PgKeywords {

    private PgKeywords() {
    }

    /** Cannot be a column name, a table alias, or a parameter name. */
    private static final Set<String> RESERVED = set(
            "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC", "ASYMMETRIC", "BOTH",
            "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "CONSTRAINT", "CREATE", "CURRENT_CATALOG",
            "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER",
            "DEFAULT", "DEFERRABLE", "DESC", "DISTINCT", "DO", "ELSE", "END", "EXCEPT", "FALSE",
            "FETCH", "FOR", "FOREIGN", "FROM", "GRANT", "GROUP", "HAVING", "IN", "INITIALLY",
            "INTERSECT", "INTO", "LATERAL", "LEADING", "LIMIT", "LOCALTIME", "LOCALTIMESTAMP",
            "NOT", "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER", "PLACING", "PRIMARY",
            "REFERENCES", "RETURNING", "SELECT", "SESSION_USER", "SOME", "SYMMETRIC", "SYSTEM_USER",
            "TABLE", "THEN", "TO", "TRAILING", "TRUE", "UNION", "UNIQUE", "USER", "USING",
            "VARIADIC", "WHEN", "WHERE", "WINDOW", "WITH");

    /**
     * Cannot be a column name or a bare table alias either: these are the words that begin a join
     * or name a type-taking function, and allowing them would make {@code t left} ambiguous.
     */
    private static final Set<String> TYPE_FUNC_NAME = set(
            "AUTHORIZATION", "BINARY", "COLLATION", "CONCURRENTLY", "CROSS", "CURRENT_SCHEMA",
            "FREEZE", "FULL", "ILIKE", "INNER", "IS", "ISNULL", "JOIN", "LEFT", "LIKE", "NATURAL",
            "NOTNULL", "OUTER", "OVERLAPS", "RIGHT", "SIMILAR", "TABLESAMPLE", "VERBOSE");

    /**
     * May be a column name, and commonly is one, but names a construct of the grammar when it is
     * followed by an open parenthesis. {@code SELECT trim FROM t} reads the column.
     */
    private static final Set<String> COL_NAME = set(
            "BETWEEN", "BIGINT", "BIT", "BOOLEAN", "CHAR", "CHARACTER", "COALESCE", "DEC",
            "DECIMAL", "EXISTS", "EXTRACT", "FLOAT", "GREATEST", "GROUPING", "INOUT", "INT",
            "INTEGER", "INTERVAL", "JSON", "JSON_ARRAY", "JSON_ARRAYAGG", "JSON_EXISTS",
            "JSON_OBJECT", "JSON_OBJECTAGG", "JSON_QUERY", "JSON_SCALAR", "JSON_SERIALIZE",
            "JSON_TABLE", "JSON_VALUE", "LEAST", "MERGE_ACTION", "NATIONAL", "NCHAR", "NONE",
            "NORMALIZE", "NULLIF", "NUMERIC", "OUT", "OVERLAY", "POSITION", "PRECISION", "REAL",
            "ROW", "SETOF", "SMALLINT", "SUBSTRING", "TIME", "TIMESTAMP", "TREAT", "TRIM", "VALUES",
            "VARCHAR", "XMLATTRIBUTES", "XMLCONCAT", "XMLELEMENT", "XMLEXISTS", "XMLFOREST",
            "XMLNAMESPACES", "XMLPARSE", "XMLPI", "XMLROOT", "XMLSERIALIZE", "XMLTABLE");

    private static Set<String> set(String... words) {
        return Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(words)));
    }

    /** Whether this word may name a column, a table alias or a parameter. */
    static boolean canBeColumnName(String word) {
        if (word == null) return false;
        String upper = word.toUpperCase();
        return !RESERVED.contains(upper) && !TYPE_FUNC_NAME.contains(upper);
    }

    /** Whether this word names a construct of the grammar but is still allowed as a column. */
    static boolean isColumnNameKeyword(String word) {
        return word != null && COL_NAME.contains(word.toUpperCase());
    }

    /** Whether this word is one PostgreSQL reserves outright. */
    static boolean isReserved(String word) {
        return word != null && RESERVED.contains(word.toUpperCase());
    }
}
