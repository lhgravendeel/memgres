package com.memgres.engine.parser.ast;

/**
 * Represents OPERATOR(schema.op)(arg1, arg2), the prefix/function-call style qualified operator.
 * This is distinct from the infix form (expr OPERATOR(schema.op) expr) because PostgreSQL applies
 * different type-resolution rules: the prefix form requires a valid search_path for argument type
 * resolution, and fails with "operator does not exist" when search_path contains non-existent schemas.
 */
public final class QualifiedOperatorExpr implements Expression {
    public final String schema;
    public final String opSymbol;
    public final Expression inner;

    public QualifiedOperatorExpr(String schema, String opSymbol, Expression inner) {
        this.schema = schema;
        this.opSymbol = opSymbol;
        this.inner = inner;
    }

    public String schema() { return schema; }
    public String opSymbol() { return opSymbol; }
    public Expression inner() { return inner; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QualifiedOperatorExpr that = (QualifiedOperatorExpr) o;
        return java.util.Objects.equals(schema, that.schema)
            && java.util.Objects.equals(opSymbol, that.opSymbol)
            && java.util.Objects.equals(inner, that.inner);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(schema, opSymbol, inner);
    }

    @Override
    public String toString() {
        return "QualifiedOperatorExpr[schema=" + schema + ", " + "opSymbol=" + opSymbol + ", " + "inner=" + inner + "]";
    }
}
