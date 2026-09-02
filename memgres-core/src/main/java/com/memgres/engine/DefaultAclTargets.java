package com.memgres.engine;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Which privileges ALTER DEFAULT PRIVILEGES may name for each kind of object.
 *
 * <p>A privilege belongs to a kind of object: a relation has SELECT and INSERT, a function has
 * EXECUTE, a type has USAGE. Written down without asking, {@code GRANT EXECUTE ON TABLES} was
 * recorded as a default that nothing could ever match, and the statement reported success —
 * so the grant silently never happened.
 */
final class DefaultAclTargets {
    private DefaultAclTargets() {}

    /** The object kinds the clause may name, the noun PostgreSQL uses for one, and its privileges. */
    private static final Map<String, String[]> TARGETS = targets();

    private static Map<String, String[]> targets() {
        Map<String, String[]> m = new HashMap<String, String[]>();
        m.put("TABLES", new String[]{"relation",
                "SELECT INSERT UPDATE DELETE TRUNCATE REFERENCES TRIGGER MAINTAIN"});
        m.put("SEQUENCES", new String[]{"sequence", "SELECT UPDATE USAGE"});
        m.put("FUNCTIONS", new String[]{"function", "EXECUTE"});
        m.put("ROUTINES", new String[]{"function", "EXECUTE"});
        m.put("TYPES", new String[]{"type", "USAGE"});
        m.put("SCHEMAS", new String[]{"schema", "CREATE USAGE"});
        m.put("LARGE OBJECTS", new String[]{"large object", "SELECT UPDATE"});
        return m;
    }

    /** The privilege words PostgreSQL knows at all, whatever kind of object they belong to. */
    private static final Set<String> KNOWN_PRIVILEGES = new HashSet<String>(Arrays.asList(
            ("SELECT INSERT UPDATE DELETE TRUNCATE REFERENCES TRIGGER MAINTAIN CREATE CONNECT "
                    + "TEMPORARY TEMP EXECUTE USAGE SET ALTER SYSTEM ALL").split(" ")));

    /** The kinds of object the clause may name. Anything else is not a spelling of the grammar. */
    private static final Set<String> KNOWN_KINDS = new HashSet<String>(Arrays.asList(
            "TABLES", "SEQUENCES", "FUNCTIONS", "ROUTINES", "TYPES", "SCHEMAS", "LARGE OBJECTS"));

    /**
     * Refuse a privilege the named kind of object does not have, and the one clause combination
     * PostgreSQL rejects outright.
     */
    static void check(List<String> privileges, String objectType, boolean inSchemaWritten) {
        if (objectType == null) return;
        String kind = objectType.toUpperCase(Locale.ROOT);
        // A word that is not a privilege at all is not a wrong privilege for something — it is
        // not part of the grammar, and PostgreSQL says so before it considers the object kind.
        for (String written : privileges) {
            if (written != null && !KNOWN_PRIVILEGES.contains(written.toUpperCase(Locale.ROOT))) {
                // Raised while the statement is analysed, not while it is read, so PostgreSQL
                // reports it with no place in the text.
                throw new MemgresException("unrecognized privilege type \""
                        + written.toLowerCase(Locale.ROOT) + "\"", "42601").suppressPosition();
            }
        }
        if (!KNOWN_KINDS.contains(kind)) {
            throw new MemgresException("syntax error at or near \"" + objectType + "\"", "42601");
        }
        // Defaults for schemas, and for large objects, are not defaults within a schema, so
        // naming one contradicts the other.
        if (inSchemaWritten && ("SCHEMAS".equals(kind) || "LARGE OBJECTS".equals(kind))) {
            throw new MemgresException("cannot use IN SCHEMA clause when using GRANT/REVOKE ON "
                    + kind, "0LP01").suppressPosition();
        }
        String[] target = TARGETS.get(kind);
        if (target == null) return;
        Set<String> allowed = new HashSet<String>(Arrays.asList(target[1].split(" ")));
        for (String written : privileges) {
            if (written == null) continue;
            String p = written.toUpperCase(Locale.ROOT);
            if ("ALL".equals(p) || allowed.contains(p)) continue;
            throw new MemgresException(
                    "invalid privilege type " + p + " for " + target[0], "0LP01")
                    .suppressPosition();
        }
    }
}
