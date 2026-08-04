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
    static void rejectMissingTypeSchemas(Database database, Session session,
                                         SystemCatalog catalog, List<String> written) {
        if (written == null || written.isEmpty()) return;
        String missingSchema = firstMissing(database, session, written);
        if (missingSchema != null) throw missing(missingSchema);
        String unknownType = firstUnknownType(database, catalog, written);
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
     * {@code int4} is a type, because neither schema holds it, and so is {@code public.pg_class},
     * whose relation is pg_catalog's. The name is reported as it was written, which is what
     * distinguishes this complaint from the same one made later about a bare name.
     */
    static String firstUnknownType(Database database, SystemCatalog catalog, List<String> written) {
        if (written == null || database == null) return null;
        for (String name : written) {
            String qualifier = qualifierOf(name);
            if (qualifier == null) continue;
            String bare = name.substring(qualifier.length() + 1);
            if (!heldBy(database, catalog, qualifier, bare)) return name;
        }
        return null;
    }

    /**
     * Whether the schema written holds a type of that name.
     *
     * <p>pg_catalog holds the types PostgreSQL ships and the catalog relations, and nothing this
     * database was told about; every other schema holds what was made in it, and
     * information_schema holds the five domains the standard describes itself in. A type memgres
     * implements where PostgreSQL has none — citext and hstore, which an extension would install
     * into whichever schema it was added to — answers wherever it is written, because there is no
     * schema this engine could call the wrong one for it.
     */
    private static boolean heldBy(Database database, SystemCatalog catalog,
                                  String qualifier, String bare) {
        String lower = bare.toLowerCase(Locale.ROOT);
        if (DataType.installedByAnExtension(lower)) return true;
        if ("pg_catalog".equalsIgnoreCase(qualifier)) {
            if (GRAMMAR_SPELLINGS.contains(lower)) return false;
            if (declaredIn(database, qualifier, bare)) return false;
            return DataType.isPgCatalogTypeName(lower)
                    || PolymorphicTypes.names().contains(lower)
                    || PSEUDO_TYPES.contains(lower)
                    || PgInternalTypes.holds(lower)
                    || namesARelation(database, catalog, qualifier, bare);
        }
        if (InformationSchemaTypes.holds(qualifier, lower)) return true;
        return declaredIn(database, qualifier, bare)
                || namesARelation(database, catalog, qualifier, bare);
    }

    /**
     * The spellings PostgreSQL's grammar reads as types without pg_catalog holding one of that
     * name. {@code integer} is the SQL word for {@code int4} and {@code serial} is shorthand for a
     * column definition, so neither answers to {@code pg_catalog.} anything — which is how
     * PostgreSQL tells a real type name from a word its parser rewrites. The words that only ever
     * stand as half of a multi-word spelling are here for the same reason.
     */
    private static final java.util.Set<String> GRAMMAR_SPELLINGS = new java.util.HashSet<String>(
            java.util.Arrays.asList("int", "integer", "smallint", "bigint", "real", "boolean",
                    "decimal", "dec", "float", "serial", "serial2", "serial4", "serial8",
                    "smallserial", "bigserial", "character", "varying", "precision", "national",
                    "double"));

    /** The pseudo-types: no values of their own, and still names pg_catalog holds. */
    private static final java.util.Set<String> PSEUDO_TYPES = new java.util.HashSet<String>(
            java.util.Arrays.asList("any", "record", "trigger", "event_trigger", "void",
                    "internal", "cstring", "unknown", "aclitem"));

    /**
     * Whether that schema holds a relation of that name; every relation mints a type of its own.
     * A catalog relation is derived rather than kept, so the catalog is asked for it by name.
     */
    private static boolean namesARelation(Database database, SystemCatalog catalog,
                                          String qualifier, String bare) {
        if (SystemCatalog.isSystemCatalog(qualifier, bare)) {
            try {
                return catalog != null && catalog.resolve(qualifier, bare) != null;
            } catch (RuntimeException e) {
                return false;
            }
        }
        Schema schema = database.getSchema(qualifier);
        if (schema == null) return false;
        return schema.getTables().containsKey(bare.toLowerCase(Locale.ROOT))
                || database.hasView(qualifier, bare);
    }

    /**
     * Whether that schema holds a type of that name. One namespace per schema holds all five
     * kinds of user-defined type, so this is one lookup against that namespace — and it is the
     * written schema's, because {@code a.e} is not answered by an {@code e} that lives in b.
     */
    private static boolean declaredIn(Database database, String qualifier, String bare) {
        return database.typeKeys().contains(TypeNamespace.key(qualifier, bare));
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

    /**
     * The schema a written qualifier really names. {@code pg_temp} is not a schema of its own but
     * an alias for this session's temporary one, and treating it as a literal name turned
     * {@code nextval('pg_temp.s')} into a missing relation.
     */
    static String resolveAlias(Session session, String qualifier) {
        if (qualifier == null || !"pg_temp".equalsIgnoreCase(qualifier)) return qualifier;
        return session == null ? qualifier : session.getTempSchemaName();
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
