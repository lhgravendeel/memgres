package com.memgres.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * A foreign table is a relation like any other: it lives in one schema, it owns its name there
 * against every other relation kind, and it goes away when its schema does.
 *
 * <p>Memgres keeps foreign tables in a flat map on {@link Database} that records only their bare
 * name, so this class answers the question that map cannot — which schema a foreign table is in.
 * The answer is kept in the same schema-object registry that already attributes sequences and
 * indexes to their schema, which means a foreign table whose schema is dropped loses its
 * attribution along with it and is treated as gone, the way {@code DROP SCHEMA ... CASCADE}
 * removes it in PostgreSQL.
 */
final class ForeignTables {

    /** The registry category foreign tables are recorded under. */
    static final String OBJECT_TYPE = "foreign table";

    private ForeignTables() {
    }

    /** Record which schema a newly created foreign table belongs to. */
    static void register(Database db, String schema, String name) {
        if (db == null || name == null) return;
        db.registerSchemaObject(schema == null ? "public" : schema, OBJECT_TYPE, name);
    }

    /** Forget a foreign table's schema, for a DROP or for a name being taken over. */
    static void unregister(Database db, String schema, String name) {
        if (db == null || name == null) return;
        db.unregisterSchemaObject(schema == null ? "public" : schema, OBJECT_TYPE, name);
    }

    /** Drop this name's attribution from every schema, whichever one holds it. */
    static void unregisterEverywhere(Database db, String name) {
        if (db == null || name == null) return;
        for (String schema : db.getSchemas().keySet()) {
            db.unregisterSchemaObject(schema, OBJECT_TYPE, name);
        }
    }

    /**
     * The schema holding the foreign table of this name, or null when no schema does. Null means
     * the name is either unknown or orphaned — its schema was dropped out from under it — and an
     * orphan is not a relation any more, so nothing should report it.
     */
    static String schemaOf(Database db, String name) {
        if (db == null || name == null) return null;
        String key = OBJECT_TYPE + ":" + name.toLowerCase(Locale.ROOT);
        for (String schema : db.getSchemas().keySet()) {
            if (db.getSchemaObjects(schema).contains(key)) return schema;
        }
        return null;
    }

    /** The foreign table of this name in this schema, or null when that schema has no such one. */
    static Database.FdwForeignTable lookup(Database db, String schema, String name) {
        if (db == null || name == null) return null;
        Database.FdwForeignTable ft = db.getForeignTables().get(bare(name).toLowerCase(Locale.ROOT));
        if (ft == null) return null;
        String home = schemaOf(db, bare(name));
        if (home == null) return null;
        String wanted = schema == null ? "public" : schema;
        return home.equalsIgnoreCase(wanted) ? ft : null;
    }

    /** True when this schema holds a foreign table of this name. */
    static boolean existsIn(Database db, String schema, String name) {
        return lookup(db, schema, name) != null;
    }

    /**
     * Every foreign table that is still in a schema, paired with the schema it is in. Each entry
     * is {@code {schemaName, FdwForeignTable}}. Orphans left behind by a dropped schema are left
     * out, so the catalogs stop reporting them.
     */
    static List<Object[]> live(Database db) {
        List<Object[]> out = new ArrayList<Object[]>();
        if (db == null) return out;
        for (Database.FdwForeignTable ft : db.getForeignTables().values()) {
            String schema = schemaOf(db, ft.tableName);
            if (schema == null) continue;
            out.add(new Object[]{schema, ft});
        }
        return out;
    }

    /**
     * What reading or writing a foreign table comes to. A foreign table is served by its wrapper,
     * and memgres loads no wrapper: every one it holds is a wrapper without a handler, which is
     * what PostgreSQL says when a query reaches a foreign table of one — not that the relation is
     * missing, because it is not missing, it is unserved.
     */
    static MemgresException noHandler(Database db, Database.FdwForeignTable ft) {
        String wrapper = ft.serverName;
        Database.FdwServer server = db.getForeignServer(ft.serverName);
        if (server != null && server.fdwName != null && !server.fdwName.isEmpty()) {
            wrapper = server.fdwName;
        }
        if (wrapper == null || wrapper.isEmpty()) wrapper = "unknown";
        return new MemgresException("foreign-data wrapper \"" + wrapper + "\" has no handler", "55000");
    }

    /** Strip a schema qualifier a caller may have left on the name. */
    private static String bare(String name) {
        int dot = name.lastIndexOf('.');
        return dot >= 0 ? name.substring(dot + 1) : name;
    }
}
