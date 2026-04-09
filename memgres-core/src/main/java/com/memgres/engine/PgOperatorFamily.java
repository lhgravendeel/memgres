package com.memgres.engine;

/**
 * Represents a user-defined operator family created via CREATE OPERATOR FAMILY.
 */
public class PgOperatorFamily {
    private String name;
    private final String method;       // index access method: btree, hash, gist, gin, etc.
    private String schemaName;
    private String owner;

    public PgOperatorFamily(String name, String method) {
        this.name = name;
        this.method = method;
        this.schemaName = "public";
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getMethod() { return method; }
    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }
    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }

    /**
     * Key for storage: name + method (same name can exist for different methods).
     */
    public String getKey() {
        return name.toLowerCase() + ":" + method.toLowerCase();
    }
}
