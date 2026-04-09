package com.memgres.engine;

/**
 * Represents a user-defined operator class created via CREATE OPERATOR CLASS.
 */
public class PgOperatorClass {
    private String name;
    private final String forType;      // data type this class is for
    private final String method;       // index access method: btree, hash, gist, gin, etc.
    private final boolean isDefault;   // whether this is the default opclass for the type+method
    private String familyName;         // operator family name (may be null — auto-created)
    private String schemaName;
    private String owner;

    public PgOperatorClass(String name, String forType, String method, boolean isDefault) {
        this.name = name;
        this.forType = forType;
        this.method = method;
        this.isDefault = isDefault;
        this.schemaName = "public";
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getForType() { return forType; }
    public String getMethod() { return method; }
    public boolean isDefault() { return isDefault; }
    public String getFamilyName() { return familyName; }
    public void setFamilyName(String familyName) { this.familyName = familyName; }
    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }
    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }

    /**
     * Key for storage: name + method.
     */
    public String getKey() {
        return name.toLowerCase() + ":" + method.toLowerCase();
    }
}
