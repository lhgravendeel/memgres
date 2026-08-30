package com.memgres.engine;

/**
 * A role reference: the OID the catalogs hold and the name a reader writes it as.
 *
 * <p>Compares equal to both the OID and the name, so a catalogue column holding one can be joined
 * to pg_authid.oid and compared against a role name in the same query.
 */
public final class RegroleValue {
    public final int oid;
    public final String name;

    public RegroleValue(int oid, String name) {
        this.oid = oid;
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RegroleValue) return oid == ((RegroleValue) obj).oid;
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
