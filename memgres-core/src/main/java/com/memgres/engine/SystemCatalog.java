package com.memgres.engine;

import java.util.*;

/**
 * Virtual system catalog tables (pg_catalog.* and information_schema.*).
 * Delegates to {@link PgCatalogBuilder} and {@link InfoSchemaBuilder} for row generation.
 */
public class SystemCatalog implements OidSupplier {

    private final Database database;

    private final PgCatalogBuilder pgCatalogBuilder;
    private final InfoSchemaBuilder infoSchemaBuilder;

    /**
     * Where OIDs come from and where they stay. The register belongs to the database rather than
     * to this catalog: an OID is the same number on every connection, and a rename run on one
     * connection has to move the OID everybody reads. See {@link ObjectIdentity}.
     */
    private final ObjectIdentity identity;

    /** The register's map. The OIDs PostgreSQL fixes by convention are seeded into it below. */
    private final Map<String, Integer> oidMap;

    public SystemCatalog(Database database) {
        this.database = database;
        this.identity = database.objectIdentity();
        this.oidMap = identity.oidMap();
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
     * The catalog relations already built for the statement now running, or null when nothing is
     * running. A catalog relation is derived from the database on every reference, and a
     * correlated subquery over one references it once per outer row: the NOT EXISTS integrity
     * checks a tool runs against pg_operator or pg_proc rebuilt thousands of rows thousands of
     * times and took seconds where PostgreSQL takes milliseconds.
     */
    private Map<String, Table> statementCache;

    /**
     * Begin a statement: catalog relations built from here on are reused until it ends.
     * A statement sees one state of the database, so building the same relation twice within
     * one can only produce the same rows again.
     */
    public void beginStatement() {
        statementCache = new HashMap<>();
    }

    /** End a statement, so the next one builds the catalog from the database afresh. */
    public void endStatement() {
        statementCache = null;
    }

    /**
     * Drop what has been built for the statement now running. Called when a statement changes
     * the database mid-flight — a data-modifying CTE, a function body that runs DDL — so a
     * later reference in the same statement does not read a relation built before the change.
     */
    public void invalidateStatementCache() {
        if (statementCache != null) statementCache.clear();
    }

    /** The register the OIDs come from, for the statements that have to tell it what they did. */
    ObjectIdentity identity() {
        return identity;
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

        boolean isPgCatalog = (sch == null && tbl.startsWith("pg_")) || "pg_catalog".equals(sch);
        boolean isInfoSchema = "information_schema".equals(sch);
        if (!isPgCatalog && !isInfoSchema) return null;

        Map<String, Table> cache = statementCache;
        // A session-scoped view answers for the session that asked, so the two forms are
        // cached apart rather than one standing in for the other.
        String key = (session != null ? "s:" : "-:") + (isPgCatalog ? "pg_catalog." : "information_schema.") + tbl;
        if (cache != null) {
            Table hit = cache.get(key);
            if (hit != null) return hit;
        }
        Table built = isPgCatalog ? pgCatalogBuilder.build(tbl, session)
                : infoSchemaBuilder.build(tbl, session);
        // A catalog is composed on demand here and stored on disk in PostgreSQL, but it is a table
        // either way: its rows have the system columns, where an information_schema view's do not.
        if (built != null && isPgCatalog) built.setStoresRows(true);
        if (cache != null && built != null) cache.put(key, built);
        return built;
    }

    @Override
    public int oid(String key) {
        return identity.oid(key);
    }

    /** Public accessor for looking up OIDs by key (used by ::regclass cast). */
    public int getOid(String key) {
        return oid(key);
    }

    /**
     * The object key an OID was handed out for, or null. This is what lets ::regproc and
     * ::regtype print a name for an OID read out of a catalog column instead of the number
     * back again.
     */
    public String keyForOid(int oid) {
        return identity.keyForOid(oid);
    }

    /** Allocate and return the next available OID. */
    public int nextOid() {
        return identity.nextOid();
    }

    /** Public accessor for the full OID map (used by pg_get_indexdef etc.). */
    @Override
    public Map<String, Integer> getOidMap() {
        return oidMap;
    }
}
