package com.memgres.engine.parser.ast;

/**
 * CREATE OPERATOR FAMILY name USING method
 */
public final class CreateOperatorFamilyStmt implements Statement {
    public final String schema;  // may be null
    public final String name;
    public final String method;  // btree, hash, gist, gin, etc.

    public CreateOperatorFamilyStmt(String schema, String name, String method) {
        this.schema = schema;
        this.name = name;
        this.method = method;
    }

    public String schema() { return schema; }
    public String name() { return name; }
    public String method() { return method; }
}
