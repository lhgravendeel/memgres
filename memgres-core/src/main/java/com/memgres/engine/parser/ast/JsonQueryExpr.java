package com.memgres.engine.parser.ast;

import java.util.Map;

/**
 * JSON_QUERY(expr, path [RETURNING type] [WITH [CONDITIONAL|UNCONDITIONAL] WRAPPER]
 *   [KEEP|OMIT QUOTES] [{EMPTY ARRAY|EMPTY OBJECT|NULL|ERROR|DEFAULT expr} ON EMPTY]
 *   [{EMPTY ARRAY|EMPTY OBJECT|NULL|ERROR|DEFAULT expr} ON ERROR])
 */
public final class JsonQueryExpr implements Expression {
    public final Expression input;
    public final Expression path;
    public final String returningType;
    public final Map<String, Expression> passing;
    public final WrapperBehavior wrapper;
    public final QuotesBehavior quotes;
    public final JsonExistsExpr.OnBehavior onEmpty;
    public final Expression defaultOnEmpty;
    public final JsonExistsExpr.OnBehavior onError;
    public final Expression defaultOnError;

    public JsonQueryExpr(Expression input, Expression path, String returningType,
                         Map<String, Expression> passing,
                         WrapperBehavior wrapper, QuotesBehavior quotes,
                         JsonExistsExpr.OnBehavior onEmpty, Expression defaultOnEmpty,
                         JsonExistsExpr.OnBehavior onError, Expression defaultOnError) {
        this.input = input;
        this.path = path;
        this.returningType = returningType;
        this.passing = passing;
        this.wrapper = wrapper;
        this.quotes = quotes;
        this.onEmpty = onEmpty;
        this.defaultOnEmpty = defaultOnEmpty;
        this.onError = onError;
        this.defaultOnError = defaultOnError;
    }

    /**
     * Which wrapper clause was written. NONE is the absence of one, which is not the same as
     * WITHOUT_WRAPPER: both ask for the items unwrapped, but writing the clause at all says the
     * reading is of documents -- which for a JSON_TABLE column its type would otherwise settle.
     */
    public enum WrapperBehavior { NONE, WITHOUT_WRAPPER, WITH_WRAPPER, WITH_CONDITIONAL_WRAPPER }
    public enum QuotesBehavior { KEEP, OMIT }

    public Expression input() { return input; }
    public Expression path() { return path; }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JsonQueryExpr that = (JsonQueryExpr) o;
        return java.util.Objects.equals(input, that.input) && java.util.Objects.equals(path, that.path);
    }
    @Override public int hashCode() { return java.util.Objects.hash(input, path); }
    @Override public String toString() { return "JsonQueryExpr[input=" + input + ", path=" + path + "]"; }
}
