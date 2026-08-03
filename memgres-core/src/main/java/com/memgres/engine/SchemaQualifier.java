package com.memgres.engine;

import java.util.List;
import java.util.Locale;

/**
 * The one rule every schema qualifier obeys: it has to name a schema that is there.
 *
 * <p>A qualified name is read in two steps — find the schema, then find the object in it — and
 * which of those two failed decides what PostgreSQL says. A schema that does not exist is
 * {@code 3F000} and names the schema; a schema that does exist but holds no such object is the
 * object's own complaint, {@code 42P01} for a relation and {@code 42704} for a type. Measured
 * against PostgreSQL 18, the split is not the same everywhere:
 *
 * <ul>
 *   <li>A <b>type name</b> is 3F000 wherever one is written — in a cast, in a column definition, in
 *       a domain's base type, in a function's argument or return type, in a PREPARE parameter.</li>
 *   <li>A <b>relation named by a DDL statement</b> is 3F000: CREATE INDEX, ALTER TABLE, TRUNCATE,
 *       DROP, COMMENT, GRANT, ANALYZE and REINDEX all resolve the schema before they look for the
 *       relation, and so does a CREATE that says which schema to create in.</li>
 *   <li>A <b>relation read or written by a query</b> is not. {@code SELECT * FROM nosuch.t} is
 *       42P01 naming {@code nosuch.t}, because the range table is built by a lookup that reports
 *       the relation it could not find rather than the schema it could not look in. So is the same
 *       relation in INSERT, UPDATE, DELETE and a view body.</li>
 * </ul>
 */
final class SchemaQualifier {

    private SchemaQualifier() {
    }

    /**
     * {@code 3F000} for the first type name in the statement written under a schema that is not
     * there, if any.
     *
     * <p>The qualifiers arrive in the order the statement writes them, which is the order
     * PostgreSQL reads them in: {@code SELECT 1::a.int4, 1::b.int4} names {@code a}. A type is
     * resolved before the expression it is applied to, so this outranks a column that is not there
     * and a function that does not resolve — but not the range table, which is built first of all,
     * and that is what the caller runs ahead of this.
     */
    static void rejectMissingTypeSchemas(Database database, Session session, List<String> written) {
        if (written == null || written.isEmpty()) return;
        String missingSchema = firstMissing(database, session, written);
        if (missingSchema != null) throw missing(missingSchema);
        String unknownType = firstUnknownType(database, written);
        if (unknownType != null) throw noSuchType(unknownType);
    }

    /** The first written name whose schema is not there, or null when every one of them resolves. */
    static String firstMissing(Database database, Session session, List<String> written) {
        if (written == null) return null;
        for (String name : written) {
            String qualifier = qualifierOf(name);
            if (qualifier == null) qualifier = name;
            if (!exists(database, session, qualifier)) return qualifier;
        }
        return null;
    }

    /**
     * The first written name whose schema is there and does not hold it, or null.
     *
     * <p>A qualified type name says which schema to find the type in, and PostgreSQL looks only
     * there: {@code public.int4} and {@code pg_toast.int4} are both {@code 42704} even though
     * {@code int4} is a type, because neither schema holds it. The built-in types are pg_catalog's
     * and nothing else's, and a type this database was told about answers wherever it was made —
     * memgres keeps those by bare name, so any schema that exists may name one, which errs towards
     * accepting rather than refusing a type the user really did create.
     *
     * <p>Only what can be shown to be in the wrong schema is refused here. A name nothing in this
     * engine answers to is left to the cast itself, which reports it as the type it could not find:
     * the list of names {@link DataType} knows is not the list of names a cast accepts —
     * {@code record} and {@code anyelement} are two it takes and that one has no entry for — and
     * reading it as though it were refused both.
     */
    static String firstUnknownType(Database database, List<String> written) {
        if (written == null || database == null) return null;
        for (String name : written) {
            String qualifier = qualifierOf(name);
            if (qualifier == null) continue;
            String bare = name.substring(qualifier.length() + 1);
            boolean ours = declaredHere(database, bare) || namesARelation(database, bare);
            if ("pg_catalog".equalsIgnoreCase(qualifier)) {
                // pg_catalog holds the built-in types and nothing this database was told about.
                if (ours || GRAMMAR_SPELLINGS.contains(bare.toLowerCase(Locale.ROOT))) return name;
                continue;
            }
            // Anywhere else, a built-in type name is one that schema does not hold.
            if (!ours && DataType.isPgCatalogTypeName(bare)) return name;
        }
        return null;
    }

    /**
     * The spellings PostgreSQL's grammar reads as types without pg_catalog holding one of that
     * name. {@code integer} is the SQL word for {@code int4} and {@code serial} is shorthand for a
     * column definition, so neither answers to {@code pg_catalog.} anything — which is how
     * PostgreSQL tells a real type name from a word its parser rewrites.
     */
    private static final java.util.Set<String> GRAMMAR_SPELLINGS = new java.util.HashSet<String>(
            java.util.Arrays.asList("int", "integer", "smallint", "bigint", "real", "boolean",
                    "decimal", "dec", "float", "serial", "serial2", "serial4", "serial8",
                    "smallserial", "bigserial"));

    /** Whether some relation of that name is there; every relation mints a type of its own name. */
    private static boolean namesARelation(Database database, String bare) {
        if (database.getTable(bare) != null || database.hasView(bare)) return true;
        for (Schema schema : database.getSchemas().values()) {
            if (schema != null && schema.getTables().containsKey(bare.toLowerCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }

    /** Whether this database has been told about a type of that name, in whatever schema. */
    private static boolean declaredHere(Database database, String bare) {
        return database.isCustomEnum(bare)
                || database.getDomain(bare) != null
                || database.getCompositeType(bare) != null
                || database.getRangeTypes().containsKey(bare)
                || database.getShellTypes().contains(bare);
    }

    static MemgresException noSuchType(String written) {
        return new MemgresException("type \"" + written + "\" does not exist", "42704");
    }

    /**
     * {@code 3F000} when a DDL statement names a schema that is not there. A null qualifier means
     * the statement wrote no schema at all, which is the search path's business and not this rule's.
     */
    static void requireSchema(Database database, Session session, String qualifier) {
        if (qualifier == null) return;
        if (!exists(database, session, qualifier)) throw missing(qualifier);
    }

    static MemgresException missing(String qualifier) {
        return new MemgresException("schema \"" + qualifier + "\" does not exist", "3F000");
    }

    /**
     * Whether a written qualifier names a schema. {@code pg_temp} is an alias for whatever this
     * session's temporary schema is called, and it names one only once the session has made one —
     * which is exactly when PostgreSQL stops reporting it as missing.
     */
    static boolean exists(Database database, Session session, String qualifier) {
        if (database == null || qualifier == null) return true;
        // The catalogs are always there whether or not this engine keeps a Schema object for them.
        if (isBuiltinSchema(qualifier)) return true;
        if (database.getSchema(qualifier) != null) return true;
        if (!"pg_temp".equalsIgnoreCase(qualifier)) return false;
        String temp = session == null ? null : session.getTempSchemaName();
        return temp != null && database.getSchema(temp) != null;
    }

    /** The schema a name of the form {@code schema.object} was written under, or null. */
    static String qualifierOf(String qualifiedName) {
        if (qualifiedName == null) return null;
        int dot = qualifiedName.indexOf('.');
        return dot > 0 ? qualifiedName.substring(0, dot) : null;
    }

    /** A qualifier that names one of the schemas every session can see without creating it. */
    static boolean isBuiltinSchema(String qualifier) {
        if (qualifier == null) return false;
        String lower = qualifier.toLowerCase(Locale.ROOT);
        return lower.equals("pg_catalog") || lower.equals("information_schema")
                || lower.equals("public")
                // Every database has one, and it holds no type and no relation a query may name,
                // so a name written under it is the object's own complaint rather than 3F000.
                || lower.equals("pg_toast");
    }
}
