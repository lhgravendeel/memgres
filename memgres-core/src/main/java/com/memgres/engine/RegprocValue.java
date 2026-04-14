package com.memgres.engine;

/**
 * Wrapper for regproc values that holds both the OID (for catalog comparisons)
 * and the function name (for display). Compares equal to both Integer OIDs and String names.
 */
public final class RegprocValue {
    public final int oid;
    public final String name;

    public RegprocValue(int oid, String name) {
        this.oid = oid;
        this.name = name;
    }

    @Override
    public String toString() {
        return name; // Display as name (PG behavior)
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RegprocValue) return oid == ((RegprocValue) obj).oid;
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
