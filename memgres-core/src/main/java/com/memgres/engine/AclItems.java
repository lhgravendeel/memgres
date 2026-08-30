package com.memgres.engine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The {@code aclitem[]} an object's catalogue column reports.
 *
 * <p>An ACL is not the list of grants somebody wrote. PostgreSQL keeps it null while nobody has
 * written one at all — which means "the defaults apply" — and materialises it the first time a
 * GRANT or a REVOKE names the object. What it materialises with is the owner holding everything
 * that kind of object has, and, for the kinds PUBLIC holds something on by default, PUBLIC's
 * entry ahead of it. So the first grant of one privilege to one role produces three entries and
 * not one, and revoking every grant away again leaves the owner's entry standing rather than
 * returning the column to null.
 *
 * <p>Within an entry the privileges are written in one fixed order, PostgreSQL's own, whatever
 * order they were granted in. Built from the order the grants arrived, {@code arw} came back as
 * {@code awr} and a schema-comparison tool saw two different ACLs.
 */
final class AclItems {

    /**
     * The order PostgreSQL writes privileges in, one letter each. Every object kind's letters are
     * a subset of this, so one string orders them all.
     */
    private static final String ORDER = "arwdDxtXUCTcsAm";

    /** What each kind of object has to give, and what PUBLIC holds on it without being granted. */
    private static final Map<String, String[]> KINDS = kinds();

    private static Map<String, String[]> kinds() {
        Map<String, String[]> m = new LinkedHashMap<>();
        m.put("TABLE", new String[]{"arwdDxtm", ""});
        m.put("SEQUENCE", new String[]{"rwU", ""});
        m.put("SCHEMA", new String[]{"UC", ""});
        m.put("DATABASE", new String[]{"CTc", "Tc"});
        m.put("FUNCTION", new String[]{"X", "X"});
        m.put("TYPE", new String[]{"U", "U"});
        m.put("LANGUAGE", new String[]{"U", "U"});
        m.put("TABLESPACE", new String[]{"C", ""});
        m.put("FOREIGN DATA WRAPPER", new String[]{"U", ""});
        m.put("SERVER", new String[]{"U", ""});
        // A column takes its default from the relation it belongs to, so its ACL carries no
        // owner entry at all -- only the column-level grants somebody wrote.
        m.put("COLUMN", new String[]{"arwx", null});
        return m;
    }

    private AclItems() {}

    /** Every privilege the kind has, as its letters, or null for a kind that is not modelled. */
    static String allOf(String kind) {
        String[] set = KINDS.get(normalise(kind));
        return set == null ? null : set[0];
    }

    private static String normalise(String kind) {
        if (kind == null) return "TABLE";
        String upper = kind.toUpperCase(Locale.ROOT);
        switch (upper) {
            case "VIEW":
            case "MATERIALIZED VIEW":
            case "FOREIGN TABLE":
            case "RELATION":
                return "TABLE";
            case "PROCEDURE":
            case "ROUTINE":
                return "FUNCTION";
            case "DOMAIN":
                return "TYPE";
            default:
                return upper;
        }
    }

    /** One grant, as the catalogue records it: who holds it, what, and whether onward. */
    static final class Grant {
        final String grantee;
        final String privilege;
        final boolean grantable;

        Grant(String grantee, String privilege, boolean grantable) {
            this.grantee = grantee;
            this.privilege = privilege;
            this.grantable = grantable;
        }
    }

    /**
     * The ACL text for an object, or null when nobody has written one.
     *
     * @param kind    the object kind, which decides the letters and PUBLIC's default
     * @param owner   the role the object belongs to, which holds everything on it
     * @param touched whether a GRANT or a REVOKE has ever named this object
     * @param grants  the grants written on it, in the order they were written
     */
    static String text(String kind, String owner, boolean touched, List<Grant> grants) {
        return text(kind, owner, touched, grants, null);
    }

    /**
     * As above, with the order the grantees first appeared in. PostgreSQL appends each new entry
     * to the end of the list, so that order is the order the ACL reads back in.
     */
    static String text(String kind, String owner, boolean touched, List<Grant> grants,
                       List<String> granteeOrder) {
        String normalised = normalise(kind);
        String[] set = KINDS.get(normalised);
        if (set == null) return null;
        boolean column = "COLUMN".equals(normalised);
        if (!touched && (grants == null || grants.isEmpty())) return null;

        // Grantee -> the letters it holds, and the letters it holds with grant option.
        Map<String, StringBuilder> held = new LinkedHashMap<>();
        Map<String, StringBuilder> onward = new LinkedHashMap<>();
        String ownerName = owner == null ? "memgres" : owner;
        if (!column) {
            // PUBLIC's default comes first where there is one, then the owner's own.
            if (set[1] != null && !set[1].isEmpty()) held.put("", new StringBuilder(set[1]));
            held.put(ownerName, new StringBuilder(set[0]));
        }
        if (grants != null) {
            List<Grant> inOrder = new ArrayList<>(grants);
            if (granteeOrder != null && !granteeOrder.isEmpty()) {
                inOrder.sort((l, r) -> Integer.compare(
                        placeOf(granteeOrder, l.grantee), placeOf(granteeOrder, r.grantee)));
            }
            for (Grant grant : inOrder) {
                String letters = lettersFor(grant.privilege, set[0]);
                if (letters.isEmpty()) continue;
                String grantee = grant.grantee == null
                        || grant.grantee.equalsIgnoreCase("public") ? "" : grant.grantee;
                held.computeIfAbsent(grantee, k -> new StringBuilder()).append(letters);
                if (grant.grantable) {
                    onward.computeIfAbsent(grantee, k -> new StringBuilder()).append(letters);
                }
            }
        }
        if (held.isEmpty()) return null;

        StringBuilder out = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, StringBuilder> entry : held.entrySet()) {
            String letters = ordered(entry.getValue().toString(), set[0]);
            if (letters.isEmpty()) continue;
            if (!first) out.append(',');
            first = false;
            out.append(entry.getKey()).append('=');
            String grantable = onward.containsKey(entry.getKey())
                    ? onward.get(entry.getKey()).toString() : "";
            // A grant somebody made to themselves is grantable by definition, and PostgreSQL
            // does not write the star for it: the owner's own entry carries no stars.
            if (!column && entry.getKey().equals(ownerName)) grantable = "";
            for (int i = 0; i < letters.length(); i++) {
                out.append(letters.charAt(i));
                if (grantable.indexOf(letters.charAt(i)) >= 0) out.append('*');
            }
            out.append('/').append(ownerName);
        }
        out.append('}');
        return first ? null : out.toString();
    }

    private static int placeOf(List<String> order, String grantee) {
        String name = grantee == null ? "" : grantee.toLowerCase(Locale.ROOT);
        int at = order.indexOf(name);
        return at < 0 ? order.size() : at;
    }

    /** The letters a privilege name stands for, with ALL standing for the kind's whole set. */
    private static String lettersFor(String privilege, String all) {
        if (privilege == null) return "";
        String upper = privilege.toUpperCase(Locale.ROOT).trim();
        if ("ALL".equals(upper) || "ALL PRIVILEGES".equals(upper)) return all;
        String letter = letterOf(upper);
        // A privilege the kind does not have is not one it can hold: GRANT ALL on a sequence
        // gives rwU and nothing else, whatever the writer spelled out.
        return letter == null || all.indexOf(letter.charAt(0)) < 0 ? "" : letter;
    }

    /** The single letter PostgreSQL records a privilege as. */
    static String letterOf(String privilege) {
        if (privilege == null) return null;
        switch (privilege.toUpperCase(Locale.ROOT).trim()) {
            case "INSERT": return "a";
            case "SELECT": return "r";
            case "UPDATE": return "w";
            case "DELETE": return "d";
            case "TRUNCATE": return "D";
            case "REFERENCES": return "x";
            case "TRIGGER": return "t";
            case "EXECUTE": return "X";
            case "USAGE": return "U";
            case "CREATE": return "C";
            case "TEMPORARY":
            case "TEMP": return "T";
            case "CONNECT": return "c";
            case "SET": return "s";
            case "ALTER SYSTEM": return "A";
            case "MAINTAIN": return "m";
            default: return null;
        }
    }

    /** The privilege name a letter stands for, for a reader that goes the other way. */
    static String privilegeOf(char letter) {
        switch (letter) {
            case 'a': return "INSERT";
            case 'r': return "SELECT";
            case 'w': return "UPDATE";
            case 'd': return "DELETE";
            case 'D': return "TRUNCATE";
            case 'x': return "REFERENCES";
            case 't': return "TRIGGER";
            case 'X': return "EXECUTE";
            case 'U': return "USAGE";
            case 'C': return "CREATE";
            case 'T': return "TEMPORARY";
            case 'c': return "CONNECT";
            case 's': return "SET";
            case 'A': return "ALTER SYSTEM";
            case 'm': return "MAINTAIN";
            default: return null;
        }
    }

    /** The letters held, written once each in PostgreSQL's order and no other. */
    private static String ordered(String letters, String all) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < ORDER.length(); i++) {
            char c = ORDER.charAt(i);
            if (letters.indexOf(c) >= 0 && all.indexOf(c) >= 0) out.append(c);
        }
        return out.toString();
    }

    /** The grants recorded for one object, read out of the store the executor writes to. */
    static List<Grant> grantsOn(Database database, String objectType, String objectName) {
        List<Grant> out = new ArrayList<>();
        for (Map.Entry<String, java.util.Set<String>> entry
                : database.getAllRolePrivileges().entrySet()) {
            for (String recorded : entry.getValue()) {
                String[] parts = recorded.split(":", 3);
                if (parts.length != 3) continue;
                if (!parts[1].equalsIgnoreCase(objectType)) continue;
                if (!parts[2].equalsIgnoreCase(objectName)) continue;
                String privilege = parts[0];
                // A grant made onward is recorded beside the grant itself rather than instead
                // of it, so the marker entry says only that the grant is grantable.
                if (privilege.endsWith("_GRANT_OPTION")) continue;
                boolean grantable = entry.getValue().contains(
                        privilege + "_GRANT_OPTION:" + parts[1] + ":" + parts[2]);
                out.add(new Grant(entry.getKey(), privilege, grantable));
            }
        }
        return out;
    }
}
