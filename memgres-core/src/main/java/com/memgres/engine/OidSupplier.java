package com.memgres.engine;

import java.util.Map;

/**
 * Interface for OID resolution, decoupling catalog builders from SystemCatalog's OID state.
 */
public interface OidSupplier {
    /** Get or assign a stable OID for the given key. */
    int oid(String key);

    /** The full OID map (used by pg_get_indexdef, ::regclass, etc.). */
    Map<String, Integer> getOidMap();
}
