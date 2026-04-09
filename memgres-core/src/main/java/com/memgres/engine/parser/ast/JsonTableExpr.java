package com.memgres.engine.parser.ast;

import java.util.List;
import java.util.Map;

/**
 * JSON_TABLE(expr, path COLUMNS (...)) — used as a FROM item.
 * Stored inside SelectStmt.FunctionFrom with special column definitions.
 */
public final class JsonTableExpr implements Expression {
    public final Expression input;
    public final Expression path;
    public final List<JsonTableColumn> columns;
    public final Map<String, Expression> passing;
    public final JsonExistsExpr.OnBehavior onError;

    public JsonTableExpr(Expression input, Expression path, List<JsonTableColumn> columns,
                         Map<String, Expression> passing, JsonExistsExpr.OnBehavior onError) {
        this.input = input;
        this.path = path;
        this.columns = columns;
        this.passing = passing;
        this.onError = onError;
    }

    /** A column definition inside JSON_TABLE COLUMNS clause. */
    public static final class JsonTableColumn {
        public final String name;
        public final String typeName; // null for FOR ORDINALITY
        public final Expression pathExpr; // null for FOR ORDINALITY
        public final boolean forOrdinality;
        public final boolean existsPath;
        public final Expression defaultOnEmpty;
        public final Expression defaultOnError;
        public final List<JsonTableColumn> nestedColumns; // for NESTED PATH
        public final Expression nestedPath;

        public JsonTableColumn(String name, String typeName, Expression pathExpr,
                               boolean forOrdinality, boolean existsPath,
                               Expression defaultOnEmpty, Expression defaultOnError,
                               List<JsonTableColumn> nestedColumns, Expression nestedPath) {
            this.name = name;
            this.typeName = typeName;
            this.pathExpr = pathExpr;
            this.forOrdinality = forOrdinality;
            this.existsPath = existsPath;
            this.defaultOnEmpty = defaultOnEmpty;
            this.defaultOnError = defaultOnError;
            this.nestedColumns = nestedColumns;
            this.nestedPath = nestedPath;
        }

        /** Simple typed column with path */
        public static JsonTableColumn typed(String name, String type, Expression path,
                                            Expression defaultOnEmpty, Expression defaultOnError) {
            return new JsonTableColumn(name, type, path, false, false, defaultOnEmpty, defaultOnError, null, null);
        }

        /** FOR ORDINALITY column */
        public static JsonTableColumn ordinality(String name) {
            return new JsonTableColumn(name, null, null, true, false, null, null, null, null);
        }

        /** EXISTS PATH column */
        public static JsonTableColumn exists(String name, String type, Expression path) {
            return new JsonTableColumn(name, type, path, false, true, null, null, null, null);
        }

        /** NESTED PATH column group */
        public static JsonTableColumn nested(Expression nestedPath, List<JsonTableColumn> columns) {
            return new JsonTableColumn(null, null, null, false, false, null, null, columns, nestedPath);
        }
    }

    public Expression input() { return input; }
    public Expression path() { return path; }
    public List<JsonTableColumn> columns() { return columns; }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return java.util.Objects.equals(input, ((JsonTableExpr) o).input);
    }
    @Override public int hashCode() { return java.util.Objects.hash(input, path); }
    @Override public String toString() { return "JsonTableExpr[columns=" + (columns != null ? columns.size() : 0) + "]"; }
}
