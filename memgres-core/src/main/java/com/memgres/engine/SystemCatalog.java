package com.memgres.engine;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Virtual system catalog tables (pg_catalog.* and information_schema.*).
 * Delegates to {@link PgCatalogBuilder} and {@link InfoSchemaBuilder} for row generation.
 */
public class SystemCatalog implements OidSupplier {

    private final Database database;

    // OID counters, stable within a session but generated on the fly
    private final Map<String, Integer> oidMap = new HashMap<>();
    private final AtomicInteger oidCounter = new AtomicInteger(16384); // start above built-in OIDs

    private final PgCatalogBuilder pgCatalogBuilder;
    private final InfoSchemaBuilder infoSchemaBuilder;

    public SystemCatalog(Database database) {
        this.database = database;
        // Pre-seed the bootstrap superuser role OID to 10 (PG convention).
        // pg_dump expects this when looking up namespace/table owners.
        oidMap.put("role:memgres", 10);

        // Pre-seed standard PG catalog table OIDs. pg_dump uses these as `tableoid`
        // in binary search lookups (CatalogId key), so they MUST match the real PG values.
        oidMap.put("rel:pg_catalog.pg_type", 1247);
        oidMap.put("rel:pg_catalog.pg_attribute", 1249);
        oidMap.put("rel:pg_catalog.pg_proc", 1255);
        oidMap.put("rel:pg_catalog.pg_class", 1259);
        oidMap.put("rel:pg_catalog.pg_authid", 1260);
        oidMap.put("rel:pg_catalog.pg_auth_members", 1261);
        oidMap.put("rel:pg_catalog.pg_database", 1262);
        oidMap.put("rel:pg_catalog.pg_tablespace", 1213);
        oidMap.put("rel:pg_catalog.pg_am", 2601);
        oidMap.put("rel:pg_catalog.pg_attrdef", 2604);
        oidMap.put("rel:pg_catalog.pg_constraint", 2606);
        oidMap.put("rel:pg_catalog.pg_inherits", 2611);
        oidMap.put("rel:pg_catalog.pg_index", 2610);
        oidMap.put("rel:pg_catalog.pg_depend", 2608);
        oidMap.put("rel:pg_catalog.pg_description", 2609);
        oidMap.put("rel:pg_catalog.pg_cast", 2600);
        oidMap.put("rel:pg_catalog.pg_namespace", 2615);
        oidMap.put("rel:pg_catalog.pg_conversion", 2607);
        oidMap.put("rel:pg_catalog.pg_rewrite", 2618);
        oidMap.put("rel:pg_catalog.pg_trigger", 2620);
        oidMap.put("rel:pg_catalog.pg_language", 2612);
        oidMap.put("rel:pg_catalog.pg_opclass", 2616);
        oidMap.put("rel:pg_catalog.pg_operator", 2617);
        oidMap.put("rel:pg_catalog.pg_opfamily", 2753);
        oidMap.put("rel:pg_catalog.pg_collation", 3456);
        oidMap.put("rel:pg_catalog.pg_extension", 3079);
        oidMap.put("rel:pg_catalog.pg_enum", 3501);
        oidMap.put("rel:pg_catalog.pg_sequence", 2224);
        oidMap.put("rel:pg_catalog.pg_shdescription", 2396);
        oidMap.put("rel:pg_catalog.pg_shdepend", 1214);
        oidMap.put("rel:pg_catalog.pg_roles", 12764); // pg_roles is a view over pg_authid (distinct OID)
        oidMap.put("rel:pg_catalog.pg_settings", 2662);
        oidMap.put("rel:pg_catalog.pg_default_acl", 826);
        oidMap.put("rel:pg_catalog.pg_event_trigger", 3466);
        oidMap.put("rel:pg_catalog.pg_foreign_data_wrapper", 2328);
        oidMap.put("rel:pg_catalog.pg_foreign_server", 1417);
        oidMap.put("rel:pg_catalog.pg_foreign_table", 3118);
        oidMap.put("rel:pg_catalog.pg_policy", 3256);
        oidMap.put("rel:pg_catalog.pg_publication", 6104);
        oidMap.put("rel:pg_catalog.pg_publication_rel", 6106);
        oidMap.put("rel:pg_catalog.pg_statistic_ext", 3381);
        oidMap.put("rel:pg_catalog.pg_largeobject_metadata", 2995);
        oidMap.put("rel:pg_catalog.pg_init_privs", 3394);
        oidMap.put("rel:pg_catalog.pg_transform", 3576);
        oidMap.put("rel:pg_catalog.pg_ts_config", 3602);
        oidMap.put("rel:pg_catalog.pg_ts_dict", 3600);
        oidMap.put("rel:pg_catalog.pg_ts_parser", 3601);
        oidMap.put("rel:pg_catalog.pg_ts_template", 3764);

        // Standard namespace OIDs
        oidMap.put("ns:pg_catalog", 11);
        oidMap.put("ns:public", 2200);
        oidMap.put("ns:information_schema", 13240);
        oidMap.put("ns:pg_toast", 99);

        this.pgCatalogBuilder = new PgCatalogBuilder(database, this);
        this.infoSchemaBuilder = new InfoSchemaBuilder(database, this);
    }

    /**
     * Check if this is a system catalog table reference.
     */
    public static boolean isSystemCatalog(String schema, String table) {
        if (schema == null) {
            String lower = table.toLowerCase();
            return lower.startsWith("pg_") || lower.startsWith("information_schema.");
        }
        String s = schema.toLowerCase();
        return s.equals("pg_catalog") || s.equals("information_schema");
    }

    /**
     * Resolve a system catalog table, returning a virtual Table with rows.
     * Returns null if this is not a recognized catalog table.
     */
    public Table resolve(String schema, String tableName) {
        return resolve(schema, tableName, null);
    }

    /**
     * Resolve a system catalog table with session context for session-scoped views
     * (pg_prepared_statements, pg_cursors).
     */
    public Table resolve(String schema, String tableName, Session session) {
        String tbl = tableName.toLowerCase();
        String sch = schema != null ? schema.toLowerCase() : null;

        // pg_catalog tables
        if (sch == null && tbl.startsWith("pg_") || "pg_catalog".equals(sch)) {
            return pgCatalogBuilder.build(tbl, session);
        }

        // information_schema tables
        if ("information_schema".equals(sch)) {
            return infoSchemaBuilder.build(tbl);
        }

        return null;
    }

    @Override
    public int oid(String key) {
        return oidMap.computeIfAbsent(key, k -> oidCounter.getAndIncrement());
    }

    /** Public accessor for looking up OIDs by key (used by ::regclass cast). */
    public int getOid(String key) {
        return oid(key);
    }

    /** Public accessor for the full OID map (used by pg_get_indexdef etc.). */
    @Override
    public Map<String, Integer> getOidMap() {
        return oidMap;
    }
}
