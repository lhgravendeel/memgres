package com.memgres.engine;

/**
 * Wrapper for regnamespace values that holds both the OID and the schema name.
 * Compares equal to both Integer OIDs and String names (like RegtypeValue).
 */
public final class RegnamespaceValue {
    public final int oid;
    public final String name;

    public RegnamespaceValue(int oid, String name) {
        this.oid = oid;
        this.name = name;
    }

    @Override
    public String toString() {
        return name; // Display as schema name (PG behavior)
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RegnamespaceValue) return oid == ((RegnamespaceValue) obj).oid;
        if (obj instanceof Number) return oid == ((Number) obj).intValue();
        if (obj instanceof String) return name.equalsIgnoreCase((String) obj);
        return false;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(oid);
    }

    public int oid() { return oid; }
    public String name() { return name; }
}
