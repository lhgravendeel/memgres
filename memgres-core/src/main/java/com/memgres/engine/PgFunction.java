package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.List;

/**
 * Represents a stored PL/pgSQL function or procedure.
 */
public class PgFunction {

        public static final class Param {
        public final String name;
        public final String typeName;
        public final String mode;
        public final String defaultExpr;

        public Param(String name, String typeName, String mode, String defaultExpr) {
            this.name = name;
            this.typeName = typeName;
            this.mode = mode;
            this.defaultExpr = defaultExpr;
        }

        public String name() { return name; }
        public String typeName() { return typeName; }
        public String mode() { return mode; }
        public String defaultExpr() { return defaultExpr; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Param that = (Param) o;
            return java.util.Objects.equals(name, that.name)
                && java.util.Objects.equals(typeName, that.typeName)
                && java.util.Objects.equals(mode, that.mode)
                && java.util.Objects.equals(defaultExpr, that.defaultExpr);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, typeName, mode, defaultExpr);
        }

        @Override
        public String toString() {
            return "Param[name=" + name + ", " + "typeName=" + typeName + ", " + "mode=" + mode + ", " + "defaultExpr=" + defaultExpr + "]";
        }
    }

    private final String name;
    private final String returnType;
    private final String body;
    private final String language;
    private final List<Param> params;
    private final boolean procedure;
    private String schemaName;
    private boolean securityDefiner;
    private boolean strict;
    private String owner;

    public PgFunction(String name, String returnType, String body, String language) {
        this(name, returnType, body, language, Cols.listOf(), false);
    }

    public PgFunction(String name, String returnType, String body, String language,
                      List<Param> params, boolean procedure) {
        this.name = name;
        this.returnType = returnType;
        this.body = body;
        this.language = language;
        this.params = params;
        this.procedure = procedure;
    }

    public String getName() { return name; }
    public String getReturnType() { return returnType; }
    public String getBody() { return body; }
    public String getLanguage() { return language; }
    public List<Param> getParams() { return params; }
    public boolean isProcedure() { return procedure; }
    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schema) { this.schemaName = schema; }
    public boolean isSecurityDefiner() { return securityDefiner; }
    public void setSecurityDefiner(boolean securityDefiner) { this.securityDefiner = securityDefiner; }
    public boolean isStrict() { return strict; }
    public void setStrict(boolean strict) { this.strict = strict; }
    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }
}
