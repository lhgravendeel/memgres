package com.memgres.engine;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * The extensions this server can install, and what each one is.
 *
 * <p>An extension is available or it is not: PostgreSQL looks for a control file and refuses the
 * statement when there is none. CREATE EXTENSION here accepted any name at all, so a typo, or a
 * name of an extension memgres does not implement, reported success and left an application
 * waiting for functions and types that were never going to appear.
 *
 * <p>This is also what {@code pg_available_extensions} lists, so the two cannot disagree about
 * what the server has.
 */
final class Extensions {
    private Extensions() {}

    /** What one extension's control file says, as the catalogue reports it. */
    static final class Entry {
        final String defaultVersion;
        final String description;
        final boolean trusted;
        final boolean relocatable;
        final String schema;
        final String requires;
        final String[] versions;

        Entry(String defaultVersion, String description, boolean trusted, boolean relocatable,
              String schema, String requires, String[] versions) {
            this.defaultVersion = defaultVersion;
            this.description = description;
            this.trusted = trusted;
            this.relocatable = relocatable;
            this.schema = schema;
            this.requires = requires;
            this.versions = versions;
        }
    }

    private static Entry entry(String defaultVersion, String description, boolean trusted,
                               boolean relocatable, String schema, String requires,
                               String versions) {
        return new Entry(defaultVersion, description, trusted, relocatable, schema, requires,
                versions.split(","));
    }

    /** Name to control-file entry, in the order the catalogue lists them. */
    private static final Map<String, Entry> AVAILABLE = available();

    private static Map<String, Entry> available() {
        Map<String, Entry> m = new LinkedHashMap<String, Entry>();
        m.put("amcheck", entry("1.5", "functions for verifying relation integrity", false, true, null, null, "1.0,1.1,1.2,1.3,1.4,1.5"));
        m.put("autoinc", entry("1.0", "functions for autoincrementing fields", false, true, null, null, "1.0"));
        m.put("bloom", entry("1.0", "bloom access method - signature file based index", false, true, null, null, "1.0"));
        m.put("bool_plperl", entry("1.0", "transform between bool and plperl", true, true, null, "{plperl}", "1.0"));
        m.put("bool_plperlu", entry("1.0", "transform between bool and plperlu", false, true, null, "{plperlu}", "1.0"));
        m.put("btree_gin", entry("1.3", "support for indexing common datatypes in GIN", true, true, null, null, "1.0,1.1,1.2,1.3"));
        m.put("btree_gist", entry("1.8", "support for indexing common datatypes in GiST", true, true, null, null, "1.2,1.3,1.4,1.5,1.6,1.7,1.8"));
        m.put("citext", entry("1.8", "data type for case-insensitive character strings", true, true, null, null, "1.4,1.5,1.6,1.7,1.8"));
        m.put("cube", entry("1.5", "data type for multidimensional cubes", true, true, null, null, "1.2,1.3,1.4,1.5"));
        m.put("dblink", entry("1.2", "connect to other PostgreSQL databases from within a database", false, true, null, null, "1.2"));
        m.put("dict_int", entry("1.0", "text search dictionary template for integers", true, true, null, null, "1.0"));
        m.put("dict_xsyn", entry("1.0", "text search dictionary template for extended synonym processing", false, true, null, null, "1.0"));
        m.put("earthdistance", entry("1.2", "calculate great-circle distances on the surface of the Earth", false, true, null, "{cube}", "1.1,1.2"));
        m.put("file_fdw", entry("1.0", "foreign-data wrapper for flat file access", false, true, null, null, "1.0"));
        m.put("fuzzystrmatch", entry("1.2", "determine similarities and distance between strings", true, true, null, null, "1.1,1.2"));
        m.put("hstore", entry("1.8", "data type for storing sets of (key, value) pairs", true, true, null, null, "1.4,1.5,1.6,1.7,1.8"));
        m.put("hstore_plperl", entry("1.0", "transform between hstore and plperl", false, true, null, "{hstore,plperl}", "1.0"));
        m.put("hstore_plperlu", entry("1.0", "transform between hstore and plperlu", false, true, null, "{hstore,plperlu}", "1.0"));
        m.put("hstore_plpython3u", entry("1.0", "transform between hstore and plpython3u", false, true, null, "{hstore,plpython3u}", "1.0"));
        m.put("insert_username", entry("1.0", "functions for tracking who changed a table", false, true, null, null, "1.0"));
        m.put("intagg", entry("1.1", "integer aggregator and enumerator (obsolete)", false, true, null, null, "1.1"));
        m.put("intarray", entry("1.5", "functions, operators, and index support for 1-D arrays of integers", true, true, null, null, "1.2,1.3,1.4,1.5"));
        m.put("isn", entry("1.3", "data types for international product numbering standards", true, true, null, null, "1.1,1.2,1.3"));
        m.put("jsonb_plperl", entry("1.0", "transform between jsonb and plperl", true, true, null, "{plperl}", "1.0"));
        m.put("jsonb_plperlu", entry("1.0", "transform between jsonb and plperlu", false, true, null, "{plperlu}", "1.0"));
        m.put("jsonb_plpython3u", entry("1.0", "transform between jsonb and plpython3u", false, true, null, "{plpython3u}", "1.0"));
        m.put("lo", entry("1.2", "Large Object maintenance", true, true, null, null, "1.1,1.2"));
        m.put("ltree", entry("1.3", "data type for hierarchical tree-like structures", true, true, null, null, "1.1,1.2,1.3"));
        m.put("ltree_plpython3u", entry("1.0", "transform between ltree and plpython3u", false, true, null, "{ltree,plpython3u}", "1.0"));
        m.put("moddatetime", entry("1.0", "functions for tracking last modification time", false, true, null, null, "1.0"));
        m.put("pageinspect", entry("1.13", "inspect the contents of database pages at a low level", false, true, null, null, "1.10,1.11,1.12,1.13,1.5,1.6,1.7,1.8,1.9"));
        m.put("pg_buffercache", entry("1.6", "examine the shared buffer cache", false, true, null, null, "1.2,1.3,1.4,1.5,1.6"));
        m.put("pg_freespacemap", entry("1.3", "examine the free space map (FSM)", false, true, null, null, "1.1,1.2,1.3"));
        m.put("pg_logicalinspect", entry("1.0", "functions to inspect logical decoding components", false, true, null, null, "1.0"));
        m.put("pg_prewarm", entry("1.2", "prewarm relation data", false, true, null, null, "1.1,1.2"));
        m.put("pg_stat_statements", entry("1.12", "track planning and execution statistics of all SQL statements executed", false, true, null, null, "1.10,1.11,1.12,1.4,1.5,1.6,1.7,1.8,1.9"));
        m.put("pg_surgery", entry("1.0", "extension to perform surgery on a damaged relation", false, true, null, null, "1.0"));
        m.put("pg_trgm", entry("1.6", "text similarity measurement and index searching based on trigrams", true, true, null, null, "1.3,1.4,1.5,1.6"));
        m.put("pg_visibility", entry("1.2", "examine the visibility map (VM) and page-level visibility info", false, true, null, null, "1.1,1.2"));
        m.put("pg_walinspect", entry("1.1", "functions to inspect contents of PostgreSQL Write-Ahead Log", false, true, null, null, "1.0,1.1"));
        m.put("pgcrypto", entry("1.4", "cryptographic functions", true, true, null, null, "1.3,1.4"));
        m.put("pgrowlocks", entry("1.2", "show row-level locking information", false, true, null, null, "1.2"));
        m.put("pgstattuple", entry("1.5", "show tuple-level statistics", false, true, null, null, "1.4,1.5"));
        m.put("plperl", entry("1.0", "PL/Perl procedural language", true, false, "pg_catalog", null, "1.0"));
        m.put("plperlu", entry("1.0", "PL/PerlU untrusted procedural language", false, false, "pg_catalog", null, "1.0"));
        m.put("plpgsql", entry("1.0", "PL/pgSQL procedural language", true, false, "pg_catalog", null, "1.0"));
        m.put("plpython3u", entry("1.0", "PL/Python3U untrusted procedural language", false, false, "pg_catalog", null, "1.0"));
        m.put("pltcl", entry("1.0", "PL/Tcl procedural language", true, false, "pg_catalog", null, "1.0"));
        m.put("pltclu", entry("1.0", "PL/TclU untrusted procedural language", false, false, "pg_catalog", null, "1.0"));
        m.put("postgres_fdw", entry("1.2", "foreign-data wrapper for remote PostgreSQL servers", false, true, null, null, "1.0,1.1,1.2"));
        m.put("refint", entry("1.0", "functions for implementing referential integrity (obsolete)", false, true, null, null, "1.0"));
        m.put("seg", entry("1.4", "data type for representing line segments or floating-point intervals", true, true, null, null, "1.1,1.2,1.3,1.4"));
        m.put("sslinfo", entry("1.2", "information about SSL certificates", false, true, null, null, "1.2"));
        m.put("tablefunc", entry("1.0", "functions that manipulate whole tables, including crosstab", true, true, null, null, "1.0"));
        m.put("tcn", entry("1.0", "Triggered change notifications", true, true, null, null, "1.0"));
        m.put("tsm_system_rows", entry("1.0", "TABLESAMPLE method which accepts number of rows as a limit", true, true, null, null, "1.0"));
        m.put("tsm_system_time", entry("1.0", "TABLESAMPLE method which accepts time in milliseconds as a limit", true, true, null, null, "1.0"));
        m.put("unaccent", entry("1.1", "text search dictionary that removes accents", true, true, null, null, "1.1"));
        m.put("uuid-ossp", entry("1.1", "generate universally unique identifiers (UUIDs)", true, true, null, null, "1.1"));
        m.put("xml2", entry("1.2", "XPath querying and XSLT", false, false, null, null, "1.1,1.2"));
        return Collections.unmodifiableMap(m);
    }

    static boolean isAvailable(String name) {
        return name != null && AVAILABLE.containsKey(name.toLowerCase(Locale.ROOT));
    }

    /** Refuse an extension this server has no control file for, the way PostgreSQL does. */
    static void requireAvailable(String name) {
        if (!isAvailable(name)) {
            throw new MemgresException(
                    "extension \"" + name + "\" is not available", "0A000");
        }
    }

    static Entry entryFor(String name) {
        return AVAILABLE.get(name == null ? "" : name.toLowerCase(Locale.ROOT));
    }

    static String defaultVersion(String name) {
        Entry e = entryFor(name);
        return e == null ? "1.0" : e.defaultVersion;
    }

    static String description(String name) {
        Entry e = entryFor(name);
        return e == null ? null : e.description;
    }

    static Iterable<String> names() {
        return AVAILABLE.keySet();
    }

    /** Whether the extension may be installed by a role that is not a superuser. */
    static boolean isTrusted(String name) {
        Entry e = entryFor(name);
        return e != null && e.trusted;
    }
}
