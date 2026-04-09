package com.memgres.engine.parser.ast;

import java.util.Map;

/**
 * JSON_EXISTS(expr, path [PASSING vars] [TRUE|FALSE|UNKNOWN|ERROR ON ERROR])
 */
public final class JsonExistsExpr implements Expression {
    public final Expression input;
    public final Expression path;
    public final Map<String, Expression> passing;
    public final OnBehavior onError; // null = default (FALSE)

    public JsonExistsExpr(Expression input, Expression path, Map<String, Expression> passing, OnBehavior onError) {
        this.input = input;
        this.path = path;
        this.passing = passing;
        this.onError = onError;
    }

    public enum OnBehavior { TRUE_VAL, FALSE_VAL, UNKNOWN_VAL, ERROR, NULL_VAL, EMPTY_ARRAY, EMPTY_OBJECT }

    public Expression input() { return input; }
    public Expression path() { return path; }
    public Map<String, Expression> passing() { return passing; }
    public OnBehavior onError() { return onError; }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JsonExistsExpr that = (JsonExistsExpr) o;
        return java.util.Objects.equals(input, that.input) && java.util.Objects.equals(path, that.path)
                && java.util.Objects.equals(passing, that.passing) && onError == that.onError;
    }
    @Override public int hashCode() { return java.util.Objects.hash(input, path, passing, onError); }
    @Override public String toString() { return "JsonExistsExpr[input=" + input + ", path=" + path + "]"; }
}
