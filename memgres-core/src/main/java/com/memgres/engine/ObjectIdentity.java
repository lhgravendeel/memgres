package com.memgres.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The register of object identities: which OID was handed out for which object, and what has to
 * follow that object when it changes name or schema.
 *
 * <p>Memgres hands out OIDs from a map keyed by {@code "rel:<schema>.<name>"} or
 * {@code "type:<name>"}, so an OID is really a hash of the name: renaming a table would mint a
 * second OID for it, the first one would go on naming something that no longer exists, and
 * dropping a table and creating it again would hand back the identical number. PostgreSQL assigns
 * an OID once, at creation, and never moves it and never reuses it: a tool that caches
 * {@code 't'::regclass::oid} and looks it up after {@code ALTER TABLE t RENAME TO t2} must find
 * {@code t2}, a tool that looks up the old name must get {@code 42P01}, and an OID whose object
 * has been dropped must print as the bare number.
 *
 * <p>This register is <em>told</em> what happened. The statement that renames an object calls
 * {@link #relationRenamed} with the identity it had and the identity it now has; the statement
 * that creates one calls {@link #relationCreated}; after a statement that drops something
 * {@link #sweepDead} retires the OIDs of the keys that no longer name anything. An earlier version
 * inferred all of this from a census of every relation's name taken before each statement and
 * diffed against the previous one — a name that vanished beside a name that appeared was read as
 * a rename. That inference cannot be made safe: a DROP beside a CREATE reads as a rename and the
 * new object inherits a dead OID, two renames in one statement swap identities, and a comment or
 * an owner is carried onto an object in another schema that the statement never named. Every
 * decision here is keyed on one object at a time and nothing is ever paired.
 *
 * <p>The register is shared by every connection to a database, because a rename run on one
 * connection has to be visible to the OIDs the next connection reads.
 *
 * <p>A type is referenced by name from the columns declared with it, and those references are not
 * rewritten when the type is renamed, so a renamed type also leaves a forwarding entry: a column
 * still declared {@code e} reports the OID of the type now called {@code e2}, which is the OID
 * PostgreSQL has recorded on that column all along.
 */
final class ObjectIdentity {

    private final Database database;

    /** Object key to OID. Shared across connections, so concurrent readers need a safe map. */
    private final Map<String, Integer> oidMap = new ConcurrentHashMap<String, Integer>();

    /** OIDs are handed out above the range PostgreSQL reserves, and are never handed out twice. */
    private final AtomicInteger oidCounter = new AtomicInteger(16384);

    /** A type key that no longer names anything, and the key its identity answers to now. */
    private final Map<String, String> typeForward = new ConcurrentHashMap<String, String>();

    /**
     * Bumped whenever a key is rebound or released, so caches over the map can notice. A rebind
     * leaves the map the same size, so a lost increment would leave a stale cache standing: two
     * connections renaming at once have to be counted apart.
     */
    private final AtomicInteger mutations = new AtomicInteger();

    /**
     * Reverse of the OID map, rebuilt when the map has grown or an identity has moved. Published
     * whole rather than mutated: the register is shared by every connection, and taking a lock to
     * read it would put every catalog query on one connection behind every other's.
     */
    private volatile Reverse reverse;

    /** A reverse index and the state of the OID map it was built from. */
    private static final class Reverse {
        final Map<Integer, String> keysByOid;
        final int size;
        final int mutations;

        Reverse(Map<Integer, String> keysByOid, int size, int mutations) {
            this.keysByOid = keysByOid;
            this.size = size;
            this.mutations = mutations;
        }
    }

    ObjectIdentity(Database database) {
        this.database = database;
    }

    // ---------------------------------------------------------------- handing out OIDs

    /** The OID for this key, minting one the first time the key is asked about. */
    int oid(String key) {
        Integer known = oidMap.get(key);
        if (known != null) return known;
        // A column keeps the word it was declared with when the type is renamed. PostgreSQL
        // recorded the type's OID there, so the old word answers with the OID of the type that
        // used to carry it rather than minting a second one for a name nothing holds.
        String forward = forwarded(key);
        if (forward != null) return oid(forward);
        int fresh = oidCounter.getAndIncrement();
        Integer raced = oidMap.putIfAbsent(key, fresh);
        return raced != null ? raced : fresh;
    }

    /**
     * The map itself, which {@link SystemCatalog} seeds with the OIDs PostgreSQL fixes by
     * convention and which the catalog builders scan to answer "what is OID n?".
     */
    Map<String, Integer> oidMap() {
        return oidMap;
    }

    int nextOid() {
        return oidCounter.getAndIncrement();
    }

    /**
     * The object key an OID was handed out for, or null. This is what lets {@code ::regproc} and
     * {@code ::regtype} print a name for an OID read out of a catalog column instead of the
     * number back again — a scan of the map per value would be quadratic over a catalog query, so
     * the reverse is kept and rebuilt only when a key has been added, rebound or retired.
     */
    String keyForOid(int oid) {
        Reverse held = reverse;
        int size = oidMap.size();
        int seen = mutations.get();
        if (held == null || held.size != size || held.mutations != seen) {
            Map<Integer, String> built = new HashMap<Integer, String>();
            for (Map.Entry<String, Integer> e : oidMap.entrySet()) {
                if (!built.containsKey(e.getValue())) built.put(e.getValue(), e.getKey());
            }
            // Two connections that notice the same change both build one and the second wins;
            // they built the same index, so which one is published does not matter.
            held = new Reverse(built, size, seen);
            reverse = held;
        }
        return held.keysByOid.get(oid);
    }

    // ---------------------------------------------------------------- being told what happened

    /**
     * A relation has just been created under this name. Anything the key still carried belonged to
     * an object that is gone — PostgreSQL never hands a dropped OID to a new object — so the key
     * is cleared and the next question about it mints a fresh number.
     */
    void relationCreated(String schema, String name) {
        String key = relKey(schema, name);
        if (oidMap.remove(key) != null) mutations.incrementAndGet();
        // The number is handed out here, not at the first question about the name, because
        // PostgreSQL assigns an OID when the object is created: the numbers then run in creation
        // order, and everything ordered by OID -- pg_class as pg_dump reads it, the list of what
        // depends on a type -- comes back in the order the objects were made rather than in the
        // order some catalog query happened to walk the schemas.
        oid(key);
    }

    /**
     * Throw away a comment left behind by an object that is gone, before a relation is created
     * under the name it was filed against. PostgreSQL drops a comment with the object it
     * describes; memgres files comments by bare name alone, so this is called only when nothing
     * anywhere answers to the name — a comment under a name still in use is that object's.
     *
     * <p>The moment matters. It has to be before the CREATE and not after, because CREATE TABLE
     * ... LIKE ... INCLUDING COMMENTS files comments of its own while it runs, and clearing
     * afterwards threw those away.
     */
    void forgetOrphanedComments(String name) {
        forgetCommentsEverywhere(null, name);
    }

    /** A user type has just been created under this name; the same reasoning as for a relation. */
    void typeCreated(String name) {
        if (oidMap.remove(typeKey(name)) != null) mutations.incrementAndGet();
        if (oidMap.remove(typeKey(name + "[]")) != null) mutations.incrementAndGet();
        typeForward.remove(typeKey(name));
        for (Iterator<Map.Entry<String, String>> it = typeForward.entrySet().iterator(); it.hasNext(); ) {
            if (it.next().getValue().equals(typeKey(name))) it.remove();
        }
        forgetCommentsEverywhere("e", name);
    }

    /**
     * A relation has changed name, schema, or both, and is the same object as before: the OID goes
     * with it, and so does everything memgres files under an object's name.
     *
     * @param kind the {@code pg_class} relkind letter — {@code r}, {@code v}, {@code m},
     *             {@code S} or {@code i}
     */
    void relationRenamed(String kind, String oldSchema, String oldName,
                         String newSchema, String newName) {
        String oldKey = relKey(oldSchema, oldName);
        String newKey = relKey(newSchema, newName);
        if (oldKey.equals(newKey)) return;
        Integer oid = oidMap.remove(oldKey);
        if (oid != null) {
            oidMap.put(newKey, oid);
            mutations.incrementAndGet();
        }
        boolean renamed = !bare(oldName).equalsIgnoreCase(bare(newName));
        // Everything else memgres files under the object's name — its comment, its grants, the
        // table an index was built on — has to follow it, or the rename silently drops what
        // PostgreSQL keeps.
        if (renamed && unambiguous(oldName, newName, newSchema)) {
            moveComments(kind, oldSchema, bare(oldName), newSchema, bare(newName));
        }
        movePrivileges(oldSchema, bare(oldName), newSchema, bare(newName));
        moveOwner(kind, oldSchema, bare(oldName), newSchema, bare(newName));
        // A trigger and a rule are on the relation, not on its name. Both registries are keyed by
        // bare name, so they can only be moved when nothing else answers to either name — the
        // same reason a comment stays put.
        if (("r".equals(kind) || "v".equals(kind) || "m".equals(kind))
                && unambiguous(oldName, newName, newSchema)) {
            database.retargetTriggers(bare(oldName), bare(newName), newSchema);
            database.retargetRules(bare(oldName), bare(newName));
        }
        if ("r".equals(kind)) retargetIndexes(oldSchema, bare(oldName), newSchema, bare(newName));
        if (renamed) sweepOtherSpellings(bare(oldName));
    }

    /**
     * A user type has changed name. Columns declared with the old word are not rewritten, so the
     * old key forwards to the new one and goes on answering with the type's OID.
     *
     * @param kind {@code d} for a domain, {@code e} for an enum, {@code c} for a composite
     */
    void typeRenamed(String kind, String oldName, String newName) {
        String oldKey = typeKey(oldName);
        String newKey = typeKey(newName);
        if (oldKey.equals(newKey)) return;
        moveTypeOid(oldKey, newKey);
        if (!"d".equals(kind)) moveTypeOid(typeKey(oldName + "[]"), typeKey(newName + "[]"));
        moveComments(kind, TypeNamespace.schemaOfKey(oldName), TypeNamespace.nameOfKey(oldName),
                TypeNamespace.schemaOfKey(newName), TypeNamespace.nameOfKey(newName));
    }

    private void moveTypeOid(String oldKey, String newKey) {
        Integer oid = oidMap.remove(oldKey);
        if (oid != null) {
            oidMap.put(newKey, oid);
            mutations.incrementAndGet();
        }
        // Recorded whether or not an OID has been handed out yet: a column declared with the type
        // will ask for one later, and by then the word it holds names nothing.
        typeForward.put(oldKey, newKey);
    }

    /** A user type has been dropped: its OID stops naming it and nothing forwards to it. */
    void typeDropped(String kind, String name) {
        if (oidMap.remove(typeKey(name)) != null) mutations.incrementAndGet();
        if (oidMap.remove(typeKey(name + "[]")) != null) mutations.incrementAndGet();
        typeForward.remove(typeKey(name));
        for (Iterator<Map.Entry<String, String>> it = typeForward.entrySet().iterator(); it.hasNext(); ) {
            if (it.next().getValue().startsWith(typeKey(name))) it.remove();
        }
        forgetComments(kind, TypeNamespace.schemaOfKey(name), TypeNamespace.nameOfKey(name));
    }

    /**
     * Retire the OID of every relation key that no longer names anything. Called after a statement
     * that drops relations — including the ones a CASCADE takes down without naming them, and the
     * whole contents of a dropped schema. Each key is checked against the live database on its
     * own: nothing is paired with anything, so a key can only be retired because the object it
     * named is really gone.
     */
    void sweepDead() {
        List<String> dead = null;
        for (String key : oidMap.keySet()) {
            if (!key.startsWith("rel:")) continue;
            String rest = key.substring(4);
            int dot = rest.lastIndexOf('.');
            if (dot < 0) continue;
            String schema = rest.substring(0, dot);
            // The catalog's own relations are not in any schema memgres stores.
            if ("pg_catalog".equals(schema) || "information_schema".equals(schema)) continue;
            String name = rest.substring(dot + 1);
            if (RelationNamespace.kindOf(database, schema, name) != null) continue;
            if (dead == null) dead = new ArrayList<String>();
            dead.add(key);
        }
        if (dead == null) return;
        for (String key : dead) {
            if (oidMap.remove(key) != null) mutations.incrementAndGet();
        }
    }

    // ---------------------------------------------------------------- forwarding

    /**
     * The OID a reference left behind by a type rename should answer with, or null when the key
     * names nothing that was renamed. A column declared {@code c e} keeps the word {@code e} after
     * {@code ALTER TYPE e RENAME TO e2}; PostgreSQL recorded the type's OID on that column, so the
     * word has to resolve to the same OID it did before the rename.
     */
    String forwarded(String key) {
        if (typeForward.isEmpty() || !key.startsWith("type:")) return null;
        if (namesALiveType(key)) return null;
        String cursor = key;
        for (int hop = 0; hop < 8; hop++) {
            String next = typeForward.get(cursor);
            if (next == null) break;
            cursor = next;
        }
        return cursor.equals(key) ? null : cursor;
    }

    private boolean namesALiveType(String key) {
        String name = key.substring("type:".length());
        if (name.endsWith("[]")) name = name.substring(0, name.length() - 2);
        String lower = name.toLowerCase();
        return database.getCustomEnums().containsKey(lower)
                || database.getDomains().containsKey(lower)
                || database.getCompositeTypes().containsKey(lower);
    }

    // ------------------------------------------------------- what follows the name

    /**
     * Whether the comment store can tell this rename's object apart from every other. Comments are
     * filed under a bare, unqualified name, so when a second relation anywhere answers to the old
     * name or to the new one, moving the entry would take a comment off the object that owns it
     * and put it on the one that does not. Leaving it where it is loses nothing: it goes on
     * describing whichever relation still holds the name.
     */
    private boolean unambiguous(String oldName, String newName, String newSchema) {
        return otherRelationsNamed(oldName, newSchema) == 0
                && otherRelationsNamed(newName, newSchema) == 0;
    }

    /**
     * How many live relations carry this bare name in a schema other than {@code exceptSchema}.
     * The renamed object itself lives in {@code exceptSchema} by the time this is asked.
     */
    private int otherRelationsNamed(String name, String exceptSchema) {
        String bare = bare(name);
        String except = exceptSchema == null ? "public" : exceptSchema.toLowerCase();
        int count = 0;
        for (String schema : database.getSchemas().keySet()) {
            if (schema.equalsIgnoreCase(except)) continue;
            if (RelationNamespace.kindOf(database, schema, bare) != null) count++;
        }
        return count;
    }

    /**
     * The comment store is keyed by object type and bare name, so a renamed table's comment is
     * left answering to a name nothing holds — and reappears if the table is renamed back, which
     * is what showed the store is keyed by name rather than by the object.
     */
    private void moveComments(String kind, String oldSchema, String oldName,
                              String newSchema, String newName) {
        Map<String, String> comments = database.getComments();
        if (comments.isEmpty()) return;
        // A comment is keyed by the schema the object lives in as well as its name, so a.t's
        // comment is carried to b.t and nothing is said about anyone else's t.
        String from = Database.commentKey(oldSchema, oldName);
        String to = Database.commentKey(newSchema, newName);
        for (String type : commentTypes(kind)) {
            String text = comments.remove(type + ":" + from);
            if (text != null) comments.put(type + ":" + to, text);
        }
        if (!hasColumns(kind)) return;
        String prefix = "column:" + from + ".";
        for (String key : columnCommentKeys(comments, prefix)) {
            String text = comments.remove(key);
            if (text != null) comments.put("column:" + to + "." + key.substring(prefix.length()), text);
        }
    }

    /**
     * Throw away what was said about an object that no longer exists, so an object created later
     * under the same name does not inherit it. PostgreSQL drops a comment with the object it
     * describes.
     */
    private void forgetComments(String kind, String schema, String name) {
        Map<String, String> comments = database.getComments();
        if (comments.isEmpty()) return;
        String key = Database.commentKey(schema, bare(name));
        for (String type : commentTypes(kind)) comments.remove(type + ":" + key);
        if (kind != null && !hasColumns(kind)) return;
        String prefix = "column:" + key + ".";
        for (String k : columnCommentKeys(comments, prefix)) comments.remove(k);
    }

    /**
     * The same, for a name nothing anywhere answers to any more: the schema it was said of is not
     * known, so every schema is swept. Only a name no object holds reaches this.
     */
    private void forgetCommentsEverywhere(String kind, String name) {
        for (String schema : database.getSchemas().keySet()) {
            forgetComments(kind, schema, name);
        }
    }

    private static List<String> columnCommentKeys(Map<String, String> comments, String prefix) {
        List<String> keys = new ArrayList<String>();
        for (String key : comments.keySet()) {
            if (key.startsWith(prefix)) keys.add(key);
        }
        return keys;
    }

    private static boolean hasColumns(String kind) {
        return kind == null || "r".equals(kind) || "v".equals(kind) || "m".equals(kind);
    }

    /**
     * The words COMMENT ON files a comment under, for each kind of thing that can be renamed.
     * A null kind means the kind is not known, which is every word a relation can be filed under.
     */
    private static String[] commentTypes(String kind) {
        if (kind == null) {
            return new String[]{"table", "view", "materialized view", "sequence", "index", "relation"};
        }
        if ("v".equals(kind)) return new String[]{"view", "relation"};
        if ("m".equals(kind)) return new String[]{"materialized view", "view", "relation"};
        if ("S".equals(kind)) return new String[]{"sequence", "relation"};
        if ("i".equals(kind)) return new String[]{"index", "relation"};
        if ("d".equals(kind)) return new String[]{"domain", "type"};
        if ("e".equals(kind) || "c".equals(kind)) return new String[]{"type"};
        return new String[]{"table", "relation"};
    }

    /**
     * A grant is recorded against the schema-qualified name, so GRANT SELECT ON t TO PUBLIC stops
     * applying the moment t is renamed. PostgreSQL keeps the grant on the object.
     */
    private void movePrivileges(String oldSchema, String oldName, String newSchema, String newName) {
        String from = ":" + AstExecutor.privilegeKey(oldSchema, oldName);
        String to = ":" + AstExecutor.privilegeKey(newSchema, newName);
        if (from.equals(to)) return;
        for (Set<String> held : database.getAllRolePrivileges().values()) {
            List<String> moved = null;
            for (String entry : held) {
                if (entry.toLowerCase().endsWith(from)) {
                    if (moved == null) moved = new ArrayList<String>();
                    moved.add(entry);
                }
            }
            if (moved == null) continue;
            for (String entry : moved) {
                held.remove(entry);
                held.add(entry.substring(0, entry.length() - from.length()) + to);
            }
        }
    }

    /** Ownership is filed under the same kind of name, and is lost the same way. */
    private void moveOwner(String kind, String oldSchema, String oldName,
                           String newSchema, String newName) {
        // A composite type's relation has no owner entry of its own to move.
        if ("c".equals(kind)) return;
        String type = "v".equals(kind) || "m".equals(kind) ? "view"
                : "S".equals(kind) ? "sequence" : "i".equals(kind) ? "index" : "table";
        boolean bareKeyed = "sequence".equals(type) || "index".equals(type);
        // Sequences and indexes are owned under a bare name, so an owner can only be moved when
        // no other schema holds a relation of either name — otherwise the entry being moved may
        // be the other object's, and moving it takes an owner off an object nobody named.
        if (bareKeyed && !unambiguous(oldName, newName, newSchema)) return;
        String from = bareKeyed ? type + ":" + oldName : type + ":" + oldSchema + "." + oldName;
        String to = bareKeyed ? type + ":" + newName : type + ":" + newSchema + "." + newName;
        if (from.equalsIgnoreCase(to)) return;
        String owner = database.getObjectOwner(from);
        if (owner == null) return;
        database.removeObjectOwner(from);
        database.setObjectOwner(to, owner);
    }

    /**
     * An index records the table it was built on by name, so a table rename orphans it: it leaves
     * pg_index, pg_indexes and pg_class altogether, while PostgreSQL renames nothing and keeps it.
     */
    private void retargetIndexes(String oldSchema, String oldName, String newSchema, String newName) {
        String from = oldSchema + "." + oldName;
        String to = newSchema + "." + newName;
        if (from.equalsIgnoreCase(to)) return;
        for (Map.Entry<String, String> e : database.getIndexTableNames().entrySet()) {
            if (from.equalsIgnoreCase(e.getValue())) e.setValue(to);
        }
    }

    /**
     * The same relation can have been given a key under more than one schema — an unqualified
     * reference is keyed under the search path's first schema whether the relation lives there or
     * not — so a rename has to retire those too, or the freed name goes on resolving through the
     * spelling nobody looked at.
     */
    private void sweepOtherSpellings(String bare) {
        for (String schema : database.getSchemas().keySet()) {
            if (RelationNamespace.kindOf(database, schema, bare) != null) return;
        }
        String suffix = "." + bare;
        for (Iterator<Map.Entry<String, Integer>> it = oidMap.entrySet().iterator(); it.hasNext(); ) {
            String k = it.next().getKey();
            if (!k.startsWith("rel:") || !k.toLowerCase().endsWith(suffix.toLowerCase())) continue;
            if (k.startsWith("rel:pg_catalog.") || k.startsWith("rel:information_schema.")) continue;
            it.remove();
            mutations.incrementAndGet();
        }
    }

    // ---------------------------------------------------------------- keys

    private static String relKey(String schema, String name) {
        // Spelled exactly as the catalog builders spell it: the schema map's own key and the
        // relation's stored name. Folding either here would build a key nobody else reads.
        return "rel:" + (schema == null ? "public" : schema) + "." + bare(name);
    }

    private static String typeKey(String name) {
        return "type:" + name;
    }

    /** Strip a schema qualifier a caller may have left on the name. */
    private static String bare(String name) {
        int dot = name.lastIndexOf('.');
        return dot >= 0 ? name.substring(dot + 1) : name;
    }
}
