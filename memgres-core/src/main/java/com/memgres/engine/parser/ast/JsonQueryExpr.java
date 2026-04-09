package com.memgres.engine.parser.ast;

import java.util.Map;

/**
 * JSON_QUERY(expr, path [RETURNING type] [WITH [CONDITIONAL|UNCONDITIONAL] WRAPPER]
 *   [KEEP|OMIT QUOTES] [EMPTY ARRAY|EMPTY OBJECT|NULL|ERROR ON EMPTY] [NULL|ERROR ON ERROR])
 */
public final class JsonQueryExpr implements Expression {
    public final Expression input;
    public final Expression path;
    public final String returningType;
    public final Map<String, Expression> passing;
    public final WrapperBehavior wrapper;
    public final QuotesBehavior quotes;
    public final JsonExistsExpr.OnBehavior onEmpty;
    public final JsonExistsExpr.OnBehavior onError;

    public JsonQueryExpr(Expression input, Expression path, String returningType,
                         Map<String, Expression> passing,
                         WrapperBehavior wrapper, QuotesBehavior quotes,
                         JsonExistsExpr.OnBehavior onEmpty, JsonExistsExpr.OnBehavior onError) {
        this.input = input;
        this.path = path;
        this.returningType = returningType;
        this.passing = passing;
        this.wrapper = wrapper;
        this.quotes = quotes;
        this.onEmpty = onEmpty;
        this.onError = onError;
    }

    public enum WrapperBehavior { NONE, WITH_WRAPPER, WITH_CONDITIONAL_WRAPPER }
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
