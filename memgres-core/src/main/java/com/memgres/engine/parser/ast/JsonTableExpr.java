package com.memgres.engine.parser.ast;

import com.memgres.engine.DataType;

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
        /** Whether FORMAT JSON was written, which makes the column read its item as a document. */
        public final boolean formatJson;
        public final JsonQueryExpr.WrapperBehavior wrapper;
        public final JsonQueryExpr.QuotesBehavior quotes; // null = unwritten
        public final JsonExistsExpr.OnBehavior onEmpty;
        public final Expression defaultOnEmpty;
        public final JsonExistsExpr.OnBehavior onError;
        public final Expression defaultOnError;
        public final List<JsonTableColumn> nestedColumns; // for NESTED PATH
        public final Expression nestedPath;

        public JsonTableColumn(String name, String typeName, Expression pathExpr,
                               boolean forOrdinality, boolean existsPath, boolean formatJson,
                               JsonQueryExpr.WrapperBehavior wrapper,
                               JsonQueryExpr.QuotesBehavior quotes,
                               JsonExistsExpr.OnBehavior onEmpty, Expression defaultOnEmpty,
                               JsonExistsExpr.OnBehavior onError, Expression defaultOnError,
                               List<JsonTableColumn> nestedColumns, Expression nestedPath) {
            this.name = name;
            this.typeName = typeName;
            this.pathExpr = pathExpr;
            this.forOrdinality = forOrdinality;
            this.existsPath = existsPath;
            this.formatJson = formatJson;
            this.wrapper = wrapper == null ? JsonQueryExpr.WrapperBehavior.NONE : wrapper;
            this.quotes = quotes;
            this.onEmpty = onEmpty;
            this.defaultOnEmpty = defaultOnEmpty;
            this.onError = onError;
            this.defaultOnError = defaultOnError;
            this.nestedColumns = nestedColumns;
            this.nestedPath = nestedPath;
        }

        /** FOR ORDINALITY column */
        public static JsonTableColumn ordinality(String name) {
            return new JsonTableColumn(name, null, null, true, false, false, null, null,
                    null, null, null, null, null, null);
        }

        /** EXISTS PATH column */
        public static JsonTableColumn exists(String name, String type, Expression path,
                                             JsonExistsExpr.OnBehavior onError) {
            return new JsonTableColumn(name, type, path, false, true, false, null, null,
                    null, null, onError, null, null, null);
        }

        /**
         * Whether a column written this way reads its item as a document rather than as a
         * scalar -- PostgreSQL's "formatted" column against its "scalar" one.
         *
         * <p>A column says so by FORMAT JSON, or by writing a wrapper or quotes clause, which
         * only a document reading has. Failing that its type settles it: the json types hold a
         * document, and the array types hold an array's items rather than one scalar.
         */
        public static boolean readsDocument(String typeName, boolean formatJson,
                                            JsonQueryExpr.WrapperBehavior wrapper,
                                            JsonQueryExpr.QuotesBehavior quotes) {
            if (formatJson || quotes != null
                    || (wrapper != null && wrapper != JsonQueryExpr.WrapperBehavior.NONE)) {
                return true;
            }
            int open = typeName == null ? -1 : typeName.indexOf('(');
            String base = typeName == null ? null
                    : (open < 0 ? typeName.trim() : typeName.substring(0, open).trim());
            DataType type = DataType.fromPgName(base);
            return type == DataType.JSON || type == DataType.JSONB || DataType.isArrayType(type);
        }

        /** NESTED PATH column group */
        public static JsonTableColumn nested(Expression nestedPath, List<JsonTableColumn> columns) {
            return new JsonTableColumn(null, null, null, false, false, false, null, null,
                    null, null, null, null, columns, nestedPath);
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
