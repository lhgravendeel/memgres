package com.memgres.engine.parser.ast;

import java.util.Map;

/**
 * JSON_VALUE(expr, path [RETURNING type] [DEFAULT val ON EMPTY] [DEFAULT val ON ERROR|NULL ON EMPTY|ERROR ON ERROR])
 */
public final class JsonValueExpr implements Expression {
    public final Expression input;
    public final Expression path;
    public final String returningType; // null = text
    public final Map<String, Expression> passing;
    public final JsonExistsExpr.OnBehavior onEmpty; // NULL_VAL, ERROR, or DEFAULT via defaultOnEmpty
    public final Expression defaultOnEmpty;
    public final JsonExistsExpr.OnBehavior onError;
    public final Expression defaultOnError;

    public JsonValueExpr(Expression input, Expression path, String returningType,
                         Map<String, Expression> passing,
                         JsonExistsExpr.OnBehavior onEmpty, Expression defaultOnEmpty,
                         JsonExistsExpr.OnBehavior onError, Expression defaultOnError) {
        this.input = input;
        this.path = path;
        this.returningType = returningType;
        this.passing = passing;
        this.onEmpty = onEmpty;
        this.defaultOnEmpty = defaultOnEmpty;
        this.onError = onError;
        this.defaultOnError = defaultOnError;
    }

    public Expression input() { return input; }
    public Expression path() { return path; }
    public String returningType() { return returningType; }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JsonValueExpr that = (JsonValueExpr) o;
        return java.util.Objects.equals(input, that.input) && java.util.Objects.equals(path, that.path);
    }
    @Override public int hashCode() { return java.util.Objects.hash(input, path); }
    @Override public String toString() { return "JsonValueExpr[input=" + input + ", path=" + path + "]"; }
}
