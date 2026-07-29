package com.memgres.engine;

import java.util.Set;

/**
 * Tables, views, materialized views, sequences and indexes are all <em>relations</em>, and in
 * PostgreSQL a schema holds exactly one relation of a given name whatever its kind. Creating a
 * table where a sequence of that name already lives is {@code 42P07}, and dropping an index with
 * {@code DROP TABLE} is {@code 42809} rather than a silent success.
 *
 * <p>Memgres stores the five kinds in separate maps, so this class answers the one question those
 * maps cannot: which kind of relation, if any, currently owns a name in a schema.
 */
final class RelationNamespace {

    static final String TABLE = "table";
    static final String VIEW = "view";
    static final String MATVIEW = "materialized view";
    static final String SEQUENCE = "sequence";
    static final String INDEX = "index";

    private RelationNamespace() {}

    /**
     * The kind of relation named {@code name} in {@code schemaName}, or null when the name is free.
     * Kinds are the words PostgreSQL uses in its own messages.
     */
    static String kindOf(Database db, String schemaName, String name) {
        if (db == null || name == null) return null;
        String bare = bareName(name);
        String schema = schemaName == null ? "public" : schemaName.toLowerCase();
        Schema s = db.getSchema(schema);
        if (s != null && s.getTable(bare) != null) return TABLE;
        Database.ViewDef v = db.getView(schema, bare);
        if (v != null && sameSchema(v.schemaName, schema)) return v.materialized ? MATVIEW : VIEW;
        if (db.hasSequence(bare) && attributedTo(db, schema, "sequence", bare)) return SEQUENCE;
        if (db.hasIndex(bare) && attributedTo(db, schema, "index", bare)) return INDEX;
        if (constraintIndexHere(s, bare)) return INDEX;
        return null;
    }

    /**
     * A PRIMARY KEY or UNIQUE constraint is backed by an index that owns its name in the schema
     * just as a written one does, so {@code t_pkey} and {@code t_a_key} are taken names even
     * though nobody wrote a CREATE INDEX for them.
     */
    private static boolean constraintIndexHere(Schema s, String bare) {
        if (s == null) return false;
        for (Table t : s.getTables().values()) {
            for (StoredConstraint c : t.getConstraints()) {
                if (c.getType() != StoredConstraint.Type.PRIMARY_KEY
                        && c.getType() != StoredConstraint.Type.UNIQUE
                        && c.getType() != StoredConstraint.Type.EXCLUDE) {
                    continue;
                }
                if (c.getName() != null && c.getName().equalsIgnoreCase(bare)) return true;
            }
        }
        return false;
    }

    /**
     * Refuse a CREATE whose name is already a relation of any kind in this schema. Passing
     * {@code ignoreKind} lets a statement do its own, better-worded check for its own kind
     * (CREATE TABLE IF NOT EXISTS, CREATE OR REPLACE VIEW) before this one runs.
     */
    static void requireFree(Database db, String schemaName, String name, String ignoreKind) {
        String kind = kindOf(db, schemaName, name);
        if (kind == null || kind.equals(ignoreKind)) return;
        throw new MemgresException("relation \"" + bareName(name) + "\" already exists", "42P07");
    }

    /** Refuse a DROP that names a relation of the wrong kind: {@code "x" is not a table}. */
    static void requireKind(Database db, String schemaName, String name, String wantedKind) {
        String kind = kindOf(db, schemaName, name);
        if (kind == null || kind.equals(wantedKind)) return;
        throw new MemgresException("\"" + bareName(name) + "\" is not " + article(wantedKind)
                + " " + wantedKind, "42809");
    }

    /** True when a relation of some kind other than {@code kind} owns the name. */
    static boolean takenByOtherKind(Database db, String schemaName, String name, String kind) {
        String found = kindOf(db, schemaName, name);
        return found != null && !found.equals(kind);
    }

    private static String article(String kind) {
        return kind.startsWith("i") ? "an" : "a";
    }

    /** Strip a schema qualifier a caller may have left on the name. */
    static String bareName(String name) {
        int dot = name.lastIndexOf('.');
        return dot >= 0 ? name.substring(dot + 1) : name;
    }

    private static boolean sameSchema(String a, String b) {
        if (a == null) return "public".equalsIgnoreCase(b);
        return a.equalsIgnoreCase(b);
    }

    /**
     * Sequences and indexes live in flat maps, so a name is attributed to a schema through the
     * schema object registry. When the registry knows nothing about the name anywhere — an object
     * created by a path that does not register — it counts as living in {@code public}, which is
     * where every such path puts it.
     */
    private static boolean attributedTo(Database db, String schema, String type, String name) {
        String key = type + ":" + name.toLowerCase();
        Set<String> here = db.getSchemaObjects(schema);
        if (here.contains(key)) return true;
        for (String other : db.getSchemas().keySet()) {
            if (db.getSchemaObjects(other).contains(key)) return false;
        }
        return "public".equals(schema);
    }
}
