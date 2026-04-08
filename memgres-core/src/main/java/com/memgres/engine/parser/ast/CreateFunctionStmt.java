package com.memgres.engine.parser.ast;

import java.util.List;
import java.util.Objects;

/**
 * CREATE [OR REPLACE] FUNCTION|PROCEDURE name(params) [RETURNS type] AS $$body$$ LANGUAGE lang
 */
public final class CreateFunctionStmt implements Statement {
    public final String name;
    public final String schema;               // explicit schema qualifier (null if unqualified)
    public final String params;               // raw parameter text (kept for backward compat)
    public final List<FuncParam> parsedParams;
    public final String returnType;
    public final String body;
    public final String language;
    public final boolean orReplace;
    public final boolean isProcedure;
    public final boolean securityDefiner;     // false = SECURITY INVOKER (default)
    public final boolean strict;              // STRICT or RETURNS NULL ON NULL INPUT

    public CreateFunctionStmt(
            String name,
            String schema,
            String params,
            List<FuncParam> parsedParams,
            String returnType,
            String body,
            String language,
            boolean orReplace,
            boolean isProcedure,
            boolean securityDefiner,
            boolean strict
    ) {
        this.name = name;
        this.schema = schema;
        this.params = params;
        this.parsedParams = parsedParams;
        this.returnType = returnType;
        this.body = body;
        this.language = language;
        this.orReplace = orReplace;
        this.isProcedure = isProcedure;
        this.securityDefiner = securityDefiner;
        this.strict = strict;
    }

    public static final class FuncParam {
        public final String name;
        public final String typeName;
        public final String mode;
        public final String defaultExpr;

        public FuncParam(String name, String typeName, String mode, String defaultExpr) {
            this.name = name;
            this.typeName = typeName;
            this.mode = mode;
            this.defaultExpr = defaultExpr;
        }

        public FuncParam(String name, String typeName, String mode) {
            this(name, typeName, mode, null);
        }

        public String name() { return name; }
        public String typeName() { return typeName; }
        public String mode() { return mode; }
        public String defaultExpr() { return defaultExpr; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FuncParam that = (FuncParam) o;
            return Objects.equals(name, that.name)
                && Objects.equals(typeName, that.typeName)
                && Objects.equals(mode, that.mode)
                && Objects.equals(defaultExpr, that.defaultExpr);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, typeName, mode, defaultExpr);
        }

        @Override
        public String toString() {
            return "FuncParam[name=" + name + ", " + "typeName=" + typeName + ", " + "mode=" + mode + ", " + "defaultExpr=" + defaultExpr + "]";
        }
    }

    public String name() { return name; }
    public String schema() { return schema; }
    public String params() { return params; }
    public List<FuncParam> parsedParams() { return parsedParams; }
    public String returnType() { return returnType; }
    public String body() { return body; }
    public String language() { return language; }
    public boolean orReplace() { return orReplace; }
    public boolean isProcedure() { return isProcedure; }
    public boolean securityDefiner() { return securityDefiner; }
    public boolean strict() { return strict; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateFunctionStmt that = (CreateFunctionStmt) o;
        return orReplace == that.orReplace
            && isProcedure == that.isProcedure
            && securityDefiner == that.securityDefiner
            && strict == that.strict
            && Objects.equals(name, that.name)
            && Objects.equals(schema, that.schema)
            && Objects.equals(params, that.params)
            && Objects.equals(parsedParams, that.parsedParams)
            && Objects.equals(returnType, that.returnType)
            && Objects.equals(body, that.body)
            && Objects.equals(language, that.language);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, schema, params, parsedParams, returnType, body, language, orReplace, isProcedure, securityDefiner, strict);
    }

    @Override
    public String toString() {
        return "CreateFunctionStmt[name=" + name + ", " + "schema=" + schema + ", " + "params=" + params + ", " + "parsedParams=" + parsedParams + ", " + "returnType=" + returnType + ", " + "body=" + body + ", " + "language=" + language + ", " + "orReplace=" + orReplace + ", " + "isProcedure=" + isProcedure + ", " + "securityDefiner=" + securityDefiner + ", " + "strict=" + strict + "]";
    }
}
