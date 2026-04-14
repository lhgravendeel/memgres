package com.memgres.engine;

/**
 * Wrapper for regtype values that holds both the OID and the canonical type name.
 * Compares equal to both Integer OIDs and String names.
 */
public final class RegtypeValue {
    public final int oid;
    public final String name;

    public RegtypeValue(int oid, String name) {
        this.oid = oid;
        this.name = name;
    }

    @Override
    public String toString() {
        return name; // Display as canonical name (PG behavior)
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RegtypeValue) return oid == ((RegtypeValue) obj).oid;
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
