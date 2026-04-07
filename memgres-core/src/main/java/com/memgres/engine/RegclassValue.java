package com.memgres.engine;

/**
 * Wrapper for regclass values that holds both the OID (for catalog comparisons)
 * and the name (for display). Compares equal to both Integer OIDs and String names.
 */
public final class RegclassValue {
    public final int oid;
    public final String name;

    public RegclassValue(int oid, String name) {
        this.oid = oid;
        this.name = name;
    }

    @Override
    public String toString() {
        return name; // Display as name (PG behavior)
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RegclassValue) return oid == ((RegclassValue) obj).oid;
        if (obj instanceof Number) return oid == ((Number) obj).intValue();
        if (obj instanceof String) return name.equalsIgnoreCase(((String) obj));
        return false;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(oid);
    }

    public int oid() { return oid; }
    public String name() { return name; }
}
