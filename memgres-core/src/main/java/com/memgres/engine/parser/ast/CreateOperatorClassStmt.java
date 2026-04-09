package com.memgres.engine.parser.ast;

/**
 * CREATE OPERATOR CLASS name [DEFAULT] FOR TYPE type USING method [FAMILY family] AS ...
 */
public final class CreateOperatorClassStmt implements Statement {
    public final String schema;      // may be null
    public final String name;
    public final boolean isDefault;
    public final String forType;
    public final String method;      // btree, hash, gist, gin, etc.
    public final String familyName;  // FAMILY clause, may be null (auto-created)

    public CreateOperatorClassStmt(String schema, String name, boolean isDefault,
                                   String forType, String method, String familyName) {
        this.schema = schema;
        this.name = name;
        this.isDefault = isDefault;
        this.forType = forType;
        this.method = method;
        this.familyName = familyName;
    }

    public String schema() { return schema; }
    public String name() { return name; }
    public boolean isDefault() { return isDefault; }
    public String forType() { return forType; }
    public String method() { return method; }
    public String familyName() { return familyName; }
}
