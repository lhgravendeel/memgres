package com.memgres.engine;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

/**
 * Enums, composites, ranges, domains and shells are all <em>types</em>, and in PostgreSQL a schema
 * holds exactly one type of a given name whatever its kind — but two schemas each hold their own.
 * {@code CREATE TYPE a.e AS ENUM ('x')} and {@code CREATE TYPE b.e AS ENUM ('p')} both succeed and
 * pg_type carries two rows called {@code e}; a column declared {@code a.e} keeps reading a.e's
 * labels whatever b.e later says.
 *
 * <p>Memgres keeps the five kinds in separate maps, so this class owns the two things those maps
 * cannot answer on their own: the key a type of a name in a schema is stored under, and which
 * schema a written name resolves to.
 *
 * <p>This is the same shape as {@link RelationNamespace}, which answers the matching question for
 * tables, views, sequences and indexes.
 */
final class TypeNamespace {

    private TypeNamespace() {
    }

    /**
     * The key a type called {@code name} in {@code schema} is stored under. {@code name} is
     * already bare — the caller has separated any qualifier the statement wrote. Splitting again
     * here would break a name that has a dot of its own: {@code CREATE DOMAIN "a.b"} names one
     * type called {@code a.b} in the current schema, and reading its own name as a qualifier
     * filed it under the wrong key and left it undroppable.
     */
    static String key(String schema, String name) {
        String s = schema == null || schema.isEmpty() ? "public" : schema.toLowerCase(Locale.ROOT);
        return s + "." + name.toLowerCase(Locale.ROOT);
    }

    /** The schema half of a key. */
    static String schemaOfKey(String key) {
        int dot = key.indexOf('.');
        return dot < 0 ? "public" : key.substring(0, dot);
    }

    /** The type-name half of a key. */
    static String nameOfKey(String key) {
        int dot = key.indexOf('.');
        return dot < 0 ? key : key.substring(dot + 1);
    }

    /** The schema a written name names, or null when it was written bare. */
    static String writtenSchema(String written) {
        if (written == null) return null;
        int dot = written.indexOf('.');
        return dot > 0 ? written.substring(0, dot) : null;
    }

    /** A written name with any schema qualifier stripped. */
    static String bare(String written) {
        if (written == null) return null;
        int dot = written.lastIndexOf('.');
        return dot >= 0 ? written.substring(dot + 1) : written;
    }

    /**
     * The key {@code written} denotes among {@code keys}, or null when none does.
     *
     * <p>A qualified name says which schema to look in and PostgreSQL looks only there. A bare one
     * is the search path's business, and this method is reached from places that have no session
     * to ask — the catalogs, information_schema, a stored column definition — so it falls back to
     * public first and then to the one schema that holds the name, which is the only answer that
     * can be right when there is only one. When two schemas hold it and nobody said which,
     * {@link #resolve} is the entry point that consults the search path.
     */
    static String find(Collection<String> keys, String written) {
        if (written == null || keys == null) return null;
        String schema = writtenSchema(written);
        if (schema != null) {
            String exact = key(schema, bare(written));
            return keys.contains(exact) ? exact : null;
        }
        String lower = written.toLowerCase(Locale.ROOT);
        String pub = "public." + lower;
        if (keys.contains(pub)) return pub;
        String suffix = "." + lower;
        String found = null;
        for (String k : keys) {
            if (k.endsWith(suffix)) {
                if (found != null) return null;
                found = k;
            }
        }
        return found;
    }

    /**
     * The key {@code written} denotes for this session, or null when nothing does.
     *
     * <p>A bare name is resolved along the search path, so with {@code search_path = b} the name
     * {@code e} is b's and with {@code search_path = a} it is a's — the same statement text
     * reading two different types, which is what a search path is for.
     */
    static String resolve(Database db, Session session, String written) {
        if (db == null || written == null) return null;
        if (writtenSchema(written) != null) return find(db.typeKeys(), written);
        String lower = written.toLowerCase(Locale.ROOT);
        Collection<String> keys = db.typeKeys();
        for (String schema : searchPath(db, session)) {
            String candidate = schema + "." + lower;
            if (keys.contains(candidate)) return candidate;
        }
        // Only the search path answers an unqualified name. Falling back to any schema at all
        // meant a type in a schema this session cannot see was still reachable by its bare name.
        return null;
    }

    /**
     * A written name rewritten as {@code schema.name} once it is known which schema holds it, so
     * that whatever stores it — a column definition, a cast, a function's argument list — records
     * the type it resolved to rather than a name that may later mean something else. A name that
     * denotes no user-defined type is handed back untouched: it is a built-in, or a mistake for
     * whoever asked for it to report.
     */
    static String qualify(Database db, Session session, String written) {
        String resolved = resolve(db, session, written);
        return resolved == null ? written : resolved;
    }

    /**
     * The type a statement named, when the statement kept the schema and the name apart. Nothing
     * is split again here, so {@code CREATE DOMAIN "a.b"} — one name with a dot of its own — is
     * looked up as the one name it is rather than as a qualifier and a name.
     *
     * <p>Only the search path answers an unqualified name. Reaching past it would let
     * {@code DROP TYPE e} take away a type in a schema this session cannot even see.
     */
    static String resolveParts(Database db, Session session, String schema, String name) {
        if (db == null || name == null) return null;
        Collection<String> keys = db.typeKeys();
        String lower = name.toLowerCase(Locale.ROOT);
        if (schema != null) {
            String candidate = schema.toLowerCase(Locale.ROOT) + "." + lower;
            return keys.contains(candidate) ? candidate : null;
        }
        for (String s : searchPath(db, session)) {
            String candidate = s + "." + lower;
            if (keys.contains(candidate)) return candidate;
        }
        return null;
    }

    /** Schemas an unqualified type name is looked up in, in precedence order. */
    private static List<String> searchPath(Database db, Session session) {
        LinkedHashSet<String> path = new LinkedHashSet<String>();
        if (session != null) {
            String temp = session.getTempSchemaName();
            if (temp != null) path.add(temp.toLowerCase(Locale.ROOT));
            for (String s : session.getEffectiveSearchPath(false)) {
                path.add(s.toLowerCase(Locale.ROOT));
            }
        }
        path.add("public");
        return new java.util.ArrayList<String>(path);
    }

    /**
     * Refuse a CREATE whose name is already a type of any kind in this schema. A shell does not
     * count: filling in a shell is exactly what the second half of a base-type definition does.
     */
    static void requireFree(Database db, String schema, String name) {
        String k = key(schema, name);
        if (db.getCustomEnums().containsKey(k)
                || db.getCompositeTypes().containsKey(k)
                || db.getRangeTypes().containsKey(k)
                || db.getDomains().containsKey(k)) {
            throw PgErrors.duplicateObject("type", name);
        }
    }

    /** The schema the type a written name denotes lives in, or null when nothing denotes one. */
    static String schemaOf(Database db, String written) {
        String k = find(db.typeKeys(), written);
        return k == null ? null : schemaOfKey(k);
    }

    /**
     * The catalog key a type's OID is minted under. Two schemas each holding an {@code e} need two
     * OIDs, so the schema is part of the key exactly as it is part of the type's identity.
     */
    static String oidKey(String schema, String name) {
        return "type:" + key(schema, name);
    }

    /**
     * A stored type name written the way PostgreSQL names that type for this session: bare when
     * the search path would find it, qualified when it would not. This is what an error message
     * and {@code format_type} print, and it is why {@code public.e} reads back as {@code e}.
     */
    static String display(Database db, Session session, String stored) {
        if (stored == null) return stored;
        String key = resolve(db, session, stored);
        if (key == null) return stored;
        String bare = nameOfKey(key);
        // Bare only when the search path itself reaches it. The "one schema holds it" fallback
        // that resolve() ends with is a reader's convenience and not something PostgreSQL would
        // have written, so it does not license dropping the qualifier.
        Collection<String> keys = db.typeKeys();
        for (String schema : searchPath(db, session)) {
            String candidate = schema + "." + bare;
            if (keys.contains(candidate)) return candidate.equals(key) ? bare : key;
        }
        return key;
    }

    /** The catalog key for the type a written name denotes, or null when nothing denotes one. */
    static String oidKeyFor(Database db, String written) {
        String k = find(db.typeKeys(), written);
        return k == null ? null : "type:" + k;
    }

    static final String ENUM = "enum";
    static final String COMPOSITE = "composite";
    static final String DOMAIN = "domain";
    static final String RANGE = "range";
    static final String SHELL = "shell";

    /** The kind of type that holds {@code name} in {@code schema}, or null when the name is free. */
    static String kindOf(Database db, String schema, String name) {
        if (db == null || name == null) return null;
        String k = key(schema == null ? "public" : schema, name);
        if (db.getCustomEnums().containsKey(k)) return ENUM;
        if (db.getCompositeTypes().containsKey(k)) return COMPOSITE;
        if (db.getDomains().containsKey(k)) return DOMAIN;
        if (db.getRangeTypes().containsKey(k)) return RANGE;
        if (db.getShellTypes().contains(k)) return SHELL;
        return null;
    }

    /**
     * A type name written the way a reader of this session would have written it, resolved against
     * an explicit search path rather than a session. Same rule as {@link #display}: bare when the
     * path reaches the schema holding it, qualified when it does not.
     */
    static String displayName(Database db, List<String> searchPath, String stored) {
        if (db == null || stored == null) return stored;
        Collection<String> keys = db.typeKeys();
        String k = find(keys, stored);
        if (k == null) return stored;
        String bare = nameOfKey(k);
        if (searchPath != null) {
            for (String schema : searchPath) {
                String candidate = schema.toLowerCase(Locale.ROOT) + "." + bare;
                if (keys.contains(candidate)) return candidate.equals(k) ? bare : k;
            }
        }
        return k;
    }

    /**
     * PostgreSQL's own hint when a relation's name is taken by a type. It is worth saying, because
     * the reader is looking at a CREATE TABLE and the collision is in a namespace they did not
     * write anything in.
     */
    private static final String ROW_TYPE_HINT = "\n  Hint: A relation has an associated type of"
            + " the same name, so you must use a name that doesn't conflict with any existing type.";

    /**
     * Refuse a CREATE of a relation whose name is already a type in this schema. A table, a view
     * and a foreign table each carry a row type of their own name, so a name an enum, a domain or
     * a range answers to is taken for them even though no relation holds it. A shell is different:
     * it is a name reserved and waiting to be filled in, which is what the new relation's row type
     * does, so it does not collide.
     */
    static void requireCreatableRowType(Database db, String schema, String name) {
        String kind = kindOf(db, schema, name);
        if (kind != null && !SHELL.equals(kind)) {
            throw new MemgresException("type \"" + name + "\" already exists"
                    + ROW_TYPE_HINT, "42710");
        }
    }

    /**
     * The same check for a rename, and a shell counts here. A CREATE fills a reserved name in;
     * a rename arrives at one already holding a row type, so the shell has nothing to fill and
     * the two names collide.
     */
    static void requireRenameableRowType(Database db, String schema, String name) {
        if (kindOf(db, schema, name) != null) {
            // No hint here. PostgreSQL offers the advice where a name is being chosen for a new
            // relation, and gives a rename that lands on a taken name the bare refusal.
            throw new MemgresException("type \"" + name + "\" already exists", "42710");
        }
    }

    /**
     * Refuse a CREATE TYPE whose name is taken. The type namespace is checked first and reports
     * {@code 42710}: a table, a view and a materialized view each own a row type, so their names
     * are taken for types too. A composite type owns a {@code pg_class} row as well as a
     * {@code pg_type} row, so it then has to find the relation name free and reports {@code 42P07}
     * when it is not — which is why PostgreSQL accepts {@code CREATE TYPE s AS ENUM} beside a
     * sequence named {@code s} but refuses {@code CREATE TYPE s AS (...)}.
     */
    static void requireCreatableType(Database db, String schema, String name, boolean asRelation) {
        String bare = name;
        requireFree(db, schema, bare);
        if (db.getShellTypes().contains(key(schema, bare))
                || rowTypeOwner(db, schema, bare) != null) {
            throw PgErrors.duplicateObject("type", bare);
        }
        if (asRelation && RelationNamespace.kindOf(db, schema, bare) != null) {
            throw new MemgresException("relation \"" + bare + "\" already exists", "42P07");
        }
    }

    /**
     * What owns the row type of this name, worded as PostgreSQL words it — {@code table},
     * {@code view}, {@code materialized view}, {@code foreign table} — or null when nothing does.
     * A row type is not something {@code DROP TYPE} may take away on its own.
     */
    static String rowTypeOwner(Database db, String schema, String name) {
        String kind = RelationNamespace.kindOf(db, schema, bare(name));
        if (kind == null || RelationNamespace.COMPOSITE.equals(kind)) return null;
        if (RelationNamespace.SEQUENCE.equals(kind) || RelationNamespace.INDEX.equals(kind)) return null;
        return kind;
    }
}
