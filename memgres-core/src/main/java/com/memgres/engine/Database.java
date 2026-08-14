package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.CreateTypeStmt;
import com.memgres.engine.parser.ast.Statement;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * In-memory database engine. Holds schemas, tables, custom types, functions, triggers, sequences, and views.
 */
public class Database {

    private final Map<String, Schema> schemas = new ConcurrentHashMap<>();
    /** Schema-level ACLs: schema name -> list of ACL items (e.g. "role=U/grantor"). */
    private final Map<String, List<String>> schemaAcls = new ConcurrentHashMap<>();
    private final Map<String, CustomEnum> customEnums = new ConcurrentHashMap<>();
    private final Map<String, PgFunction> functions = new ConcurrentHashMap<>();
    private final Map<String, List<PgFunction>> functionOverloads = new ConcurrentHashMap<>();
    private final Map<String, List<PgTrigger>> triggers = new ConcurrentHashMap<>();
    private final Map<String, Sequence> sequences = new ConcurrentHashMap<>();
    private final Map<String, ViewDef> views = new ConcurrentHashMap<>();
    // The five kinds of user-defined type all live in one namespace per schema, so each is keyed
    // by TypeNamespace.key(schema, name) rather than by the bare name: two schemas may each hold
    // a type called e, and a column declared with one has to keep reading that one's definition.
    private final Map<String, DomainType> domains = new ConcurrentHashMap<>();
    private final Map<String, List<CreateTypeStmt.CompositeField>> compositeTypes = new ConcurrentHashMap<>();
    private final Map<String, String> rangeTypes = new ConcurrentHashMap<>(); // range type key → subtype name
    private final Set<String> shellTypes = ConcurrentHashMap.newKeySet(); // CREATE TYPE name; with no definition
    private final Map<String, PgAggregate> userAggregates = new ConcurrentHashMap<>();
    private final Map<String, PgOperator> userOperators = new ConcurrentHashMap<>();
    private final Map<String, PgOperatorFamily> userOperatorFamilies = new ConcurrentHashMap<>();
    private final Map<String, PgOperatorClass> userOperatorClasses = new ConcurrentHashMap<>();
    private final Map<String, List<String>> indexColumns = new ConcurrentHashMap<>();
    private final Map<String, String> indexTableNames = new ConcurrentHashMap<>(); // index name → schema.table
    private final Map<String, Boolean> indexUniqueFlags = new ConcurrentHashMap<>(); // index name → is unique
    private final Map<String, String> indexWhereClauses = new ConcurrentHashMap<>(); // index name → WHERE predicate
    private final Map<String, String> indexMethods = new ConcurrentHashMap<>(); // index name → access method (btree, hash, etc.)
    private final Map<String, Map<String, String>> indexReloptions = new ConcurrentHashMap<>(); // index name → storage params
    private final Map<String, List<String>> indexColumnOptions = new ConcurrentHashMap<>(); // index name → per-column options (DESC, opclass, NULLS FIRST/LAST)
    private final Map<String, List<String>> indexIncludeColumns = new ConcurrentHashMap<>(); // index name → INCLUDE columns
    private final Map<String, Boolean> indexNullsNotDistinct = new ConcurrentHashMap<>(); // index name → NULLS NOT DISTINCT
    private final Map<String, String> indexParentIndex = new ConcurrentHashMap<>(); // child index name → parent index name (ALTER INDEX ATTACH PARTITION)
    private final Map<String, PgEventTrigger> eventTriggers = new ConcurrentHashMap<>(); // event trigger name → definition
    private final Map<String, ExtendedStatistic> extendedStatistics = new ConcurrentHashMap<>();

    /**
     * Which OID was handed out for which object. It belongs to the database and not to a
     * connection's catalog: PostgreSQL's OIDs are a property of the object, so a table has the
     * same number on every connection and a rename run on one moves the number for all of them.
     */
    private final ObjectIdentity objectIdentity = new ObjectIdentity(this);

    // User-defined casts: each entry is [sourceOid(int), targetOid(int), castFunc(int), castContext(String), castMethod(String)]
    private final java.util.List<Object[]> userDefinedCasts = new java.util.concurrent.CopyOnWriteArrayList<>();


    // ---- FDW catalog objects ----
    private final Map<String, FdwWrapper> foreignDataWrappers = new ConcurrentHashMap<>();
    private final Map<String, FdwServer> foreignServers = new ConcurrentHashMap<>();
    private final Map<String, FdwUserMapping> foreignUserMappings = new ConcurrentHashMap<>(); // key: serverName:userName
    private final Map<String, FdwForeignTable> foreignTables = new ConcurrentHashMap<>();

    // ---- Publication / Subscription catalog objects ----
    private final Map<String, PubDef> publications = new ConcurrentHashMap<>();
    private final Map<String, SubDef> subscriptions = new ConcurrentHashMap<>();

    // ---- Replication slots ----
    private final Map<String, ReplicationSlot> replicationSlots = new ConcurrentHashMap<>();

    // ---- Collation catalog objects ----
    private final Map<String, CollationDef> userCollations = new ConcurrentHashMap<>();

    /** User-defined collation metadata (CREATE COLLATION). */
    public static class CollationDef {
        public final String name;
        public final String provider;   // "c" (libc), "i" (icu), "d" (default)
        public final String locale;     // locale string
        public final String lcCollate;
        public final String lcCtype;
        public final boolean deterministic;
        public final String fromCollation; // if created with FROM clause
        public CollationDef(String name, String provider, String locale, String lcCollate, String lcCtype, boolean deterministic, String fromCollation) {
            this.name = name; this.provider = provider; this.locale = locale;
            this.lcCollate = lcCollate; this.lcCtype = lcCtype;
            this.deterministic = deterministic; this.fromCollation = fromCollation;
        }
    }

    // ---- Stub catalog objects ----
    /**
     * Objects memgres accepts but does not implement — conversions, tablespaces, procedural
     * languages. It stores no behaviour for them, but it has to remember that they exist:
     * PostgreSQL refuses an ALTER on a name that was never created, and quietly succeeding
     * there reports that a rename happened when nothing did.
     */
    private final Map<String, Set<String>> stubObjects = new ConcurrentHashMap<>();

    /** Built-in members of each stub kind, which exist without ever being created. */
    private static final Map<String, Set<String>> BUILTIN_STUBS = builtinStubs();

    private static Map<String, Set<String>> builtinStubs() {
        Map<String, Set<String>> m = new java.util.HashMap<>();
        Set<String> langs = new java.util.HashSet<>();
        langs.add("internal"); langs.add("c"); langs.add("sql"); langs.add("plpgsql");
        m.put("language", langs);
        Set<String> spaces = new java.util.HashSet<>();
        spaces.add("pg_default"); spaces.add("pg_global");
        m.put("tablespace", spaces);
        return m;
    }

    public void addStubObject(String kind, String name) {
        if (name == null) return;
        stubObjects.computeIfAbsent(kind.toLowerCase(), k -> ConcurrentHashMap.newKeySet())
                .add(name.toLowerCase());
    }

    public boolean hasStubObject(String kind, String name) {
        if (name == null) return false;
        String lower = name.toLowerCase();
        Set<String> builtin = BUILTIN_STUBS.get(kind.toLowerCase());
        if (builtin != null && builtin.contains(lower)) return true;
        Set<String> created = stubObjects.get(kind.toLowerCase());
        return created != null && created.contains(lower);
    }

    public void removeStubObject(String kind, String name) {
        if (name == null) return;
        Set<String> created = stubObjects.get(kind.toLowerCase());
        if (created != null) created.remove(name.toLowerCase());
    }

    public void renameStubObject(String kind, String oldName, String newName) {
        removeStubObject(kind, oldName);
        addStubObject(kind, newName);
    }

    public void addCollation(CollationDef coll) { userCollations.put(coll.name.toLowerCase(), coll); }
    public CollationDef getCollation(String name) { return userCollations.get(name.toLowerCase()); }
    public Map<String, CollationDef> getUserCollations() { return userCollations; }
    public void removeCollation(String name) { userCollations.remove(name.toLowerCase()); }

    // ---- Text Search catalog objects ----
    private final Map<String, TsConfigDef> tsConfigs = new ConcurrentHashMap<>();
    private final Map<String, TsDictDef> tsDicts = new ConcurrentHashMap<>();
    // key: configName + "\0" + tokenType, value: dictName
    private final Map<String, String> tsConfigMaps = new ConcurrentHashMap<>();

    /** Text Search Configuration metadata. */
    public static class TsConfigDef {
        public final String name;
        public final String parserName; // parser or null if COPY
        public final String copyFrom;   // source config or null if PARSER
        public TsConfigDef(String name, String parserName, String copyFrom) {
            this.name = name; this.parserName = parserName; this.copyFrom = copyFrom;
        }
    }

    /** Text Search Dictionary metadata. */
    public static class TsDictDef {
        public final String name;
        public final String template;
        public final String options; // e.g. "STOPWORDS = english"
        public TsDictDef(String name, String template, String options) {
            this.name = name; this.template = template; this.options = options;
        }
    }

    public void addTsConfig(TsConfigDef cfg) { tsConfigs.put(cfg.name.toLowerCase(), cfg); }
    public void removeTsConfig(String name) { tsConfigs.remove(name.toLowerCase()); }
    public Map<String, TsConfigDef> getTsConfigs() { return tsConfigs; }

    public void addTsDict(TsDictDef dict) { tsDicts.put(dict.name.toLowerCase(), dict); }
    public void removeTsDict(String name) { tsDicts.remove(name.toLowerCase()); }
    public Map<String, TsDictDef> getTsDicts() { return tsDicts; }

    public void addTsConfigMap(String configName, String tokenType, String dictName) {
        tsConfigMaps.put(configName.toLowerCase() + "\0" + tokenType.toLowerCase(), dictName);
    }
    public Map<String, String> getTsConfigMaps() { return tsConfigMaps; }

    /** Foreign Data Wrapper metadata. */
    public static class FdwWrapper {
        public final String name;
        public final String options; // PG array format or null
        public FdwWrapper(String name, String options) { this.name = name; this.options = options; }
    }

    /** Foreign Server metadata. */
    public static class FdwServer {
        public final String name;
        public final String fdwName;
        public String options; // PG array format or null
        public FdwServer(String name, String fdwName, String options) { this.name = name; this.fdwName = fdwName; this.options = options; }
    }

    /** User Mapping metadata. */
    public static class FdwUserMapping {
        public final String serverName;
        public final String userName; // "PUBLIC" or actual user name
        public final String options;
        public FdwUserMapping(String serverName, String userName, String options) { this.serverName = serverName; this.userName = userName; this.options = options; }
    }

    /** Foreign Table metadata. */
    public static class FdwForeignTable {
        public final String tableName;
        public final String serverName;
        public final String options;
        public final List<String[]> columns; // each: {name, type}
        public FdwForeignTable(String tableName, String serverName, String options, List<String[]> columns) {
            this.tableName = tableName; this.serverName = serverName; this.options = options; this.columns = columns;
        }
    }

    /** Publication metadata. */
    public static class PubDef {
        public final String name;
        public final boolean allTables;
        public final List<String> tables; // mutable for ALTER ADD TABLE
        public final String schemaName; // for TABLES IN SCHEMA, or null
        public PubDef(String name, boolean allTables, List<String> tables, String schemaName) {
            this.name = name; this.allTables = allTables; this.tables = tables != null ? new ArrayList<>(tables) : new ArrayList<>(); this.schemaName = schemaName;
        }
    }

    /** Subscription metadata. */
    public static class SubDef {
        public final String name;
        public final String conninfo;
        public final String publication;
        public SubDef(String name, String conninfo, String publication) { this.name = name; this.conninfo = conninfo; this.publication = publication; }
    }

    /** Replication slot metadata. */
    public static class ReplicationSlot {
        public final String slotName;
        public final String plugin;
        public final String slotType; // "logical" or "physical"
        public ReplicationSlot(String slotName, String plugin, String slotType) { this.slotName = slotName; this.plugin = plugin; this.slotType = slotType; }
    }

    private final NotificationManager notificationManager = new NotificationManager();
    private final LargeObjectStore largeObjectStore = new LargeObjectStore();
    private DatabaseRegistry databaseRegistry;

    // Transaction commit counter for pg_stat_database
    private final AtomicLong xactCommitCount = new AtomicLong(0);

    // Set of analyzed table names (schema.table) for pg_statistic
    private final Set<String> analyzedTables = ConcurrentHashMap.newKeySet();

    // Set of clustered index names (for pg_index.indisclustered)
    private final Set<String> clusteredIndexes = ConcurrentHashMap.newKeySet();

    // Row locks: maps table name to (row identity -> list of lock entries)
    private final Map<String, Map<Object[], List<LockEntry>>> rowLocks = new ConcurrentHashMap<>();

    // Wait-for graph: maps a waiting session to the session it is waiting for (for deadlock detection)
    private final Map<Session, Session> waitingFor = new ConcurrentHashMap<>();

    /** Monotonic acquisition counter so ROLLBACK TO SAVEPOINT can release only newer locks. */
    private final AtomicLong rowLockSeq = new AtomicLong(0);

    /** A single row-lock entry recording the holding session and the requested lock mode. */
    public static class LockEntry {
        public final Session session;
        public final String mode; // "UPDATE", "NO KEY UPDATE", "SHARE", "KEY SHARE"
        public final long seq;    // acquisition order, compared against savepoint marks

        public LockEntry(Session session, String mode, long seq) {
            this.session = session;
            this.mode = mode;
            this.seq = seq;
        }
    }

    /** The current acquisition mark; locks taken after it are released by ROLLBACK TO SAVEPOINT. */
    public long currentRowLockMark() {
        return rowLockSeq.get();
    }

    /**
     * Release the session's row locks acquired after {@code mark}. PG rolls a subtransaction's
     * row locks back with it, so a savepoint rollback frees whatever it took.
     */
    public void releaseRowLocksAfter(Session session, long mark) {
        for (Map<Object[], List<LockEntry>> locks : rowLocks.values()) {
            synchronized (locks) {
                for (List<LockEntry> entries : locks.values()) {
                    entries.removeIf(e -> e.session == session && e.seq > mark);
                }
                locks.entrySet().removeIf(e -> e.getValue().isEmpty());
            }
        }
    }

    /**
     * Returns true if two lock modes are compatible (can coexist on the same row).
     * Compatibility matrix (PostgreSQL semantics):
     * <pre>
     *                  FOR KEY SHARE  FOR SHARE  FOR NO KEY UPDATE  FOR UPDATE
     * FOR KEY SHARE         ✓             ✓             ✓              ✗
     * FOR SHARE             ✓             ✓             ✗              ✗
     * FOR NO KEY UPDATE     ✓             ✗             ✗              ✗
     * FOR UPDATE            ✗             ✗             ✗              ✗
     * </pre>
     */
    private static boolean lockModesCompatible(String modeA, String modeB) {
        if ("UPDATE".equals(modeA) || "UPDATE".equals(modeB)) {
            return false;
        }
        // NO KEY UPDATE is only compatible with KEY SHARE
        if ("NO KEY UPDATE".equals(modeA) || "NO KEY UPDATE".equals(modeB)) {
            return "KEY SHARE".equals(modeA) || "KEY SHARE".equals(modeB);
        }
        // SHARE is compatible with SHARE and KEY SHARE
        // KEY SHARE is compatible with KEY SHARE, SHARE, and NO KEY UPDATE (handled above)
        return true;
    }

    // Table-level locks: table key -> list of (session, mode)
    public static class TableLockEntry {
        public final Session session;
        public final String mode;
        public TableLockEntry(Session session, String mode) { this.session = session; this.mode = mode; }
    }
    private final Map<String, List<TableLockEntry>> tableLevelLocks = new ConcurrentHashMap<>();
    private final Object tableLockMonitor = new Object();

    /**
     * Acquire a table-level lock. If NOWAIT and conflicting lock exists, throws 55P03.
     * Otherwise blocks until the lock can be acquired.
     */
    public void acquireTableLock(String tableKey, String mode, Session session, boolean nowait) {
        acquireTableLock(tableKey, mode, session, nowait, lockWaitBudget(session));
    }

    /**
     * How long a lock wait may last, in milliseconds, or {@link Long#MAX_VALUE} for no limit.
     *
     * <p>{@code lock_timeout} is the answer when it is set. When it is not, PostgreSQL waits:
     * a wait that gives up on its own schedule turns a lock another session is about to release
     * into an error the caller never asked for, and makes {@code lock_timeout} decorative. The
     * wait stays interruptible, so {@code statement_timeout} and a client cancel still end it.
     */
    private static long lockWaitBudget(Session session) {
        if (session == null) return Long.MAX_VALUE;
        long configured = GucSettings.parseTimeoutMillis(session.getGucSettings().get("lock_timeout"));
        return configured > 0 ? configured : Long.MAX_VALUE;
    }

    /**
     * Acquire a table-level lock, waiting at most {@code timeoutMs} for a conflicting holder
     * to go away. A session that already holds this mode on the relation keeps its single
     * entry rather than stacking one per statement.
     */
    public void acquireTableLock(String tableKey, String mode, Session session, boolean nowait, long timeoutMs) {
        final long deadline = timeoutMs == Long.MAX_VALUE
                ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMs;
        final String relation = relationNameOf(tableKey);
        boolean registered = false;
        try {
            synchronized (tableLockMonitor) {
                while (true) {
                    List<TableLockEntry> entries = tableLevelLocks.computeIfAbsent(tableKey, k -> new java.util.concurrent.CopyOnWriteArrayList<>());
                    Session holder = null;
                    boolean alreadyHeld = false;
                    for (TableLockEntry e : entries) {
                        if (e.session == session) {
                            if (mode.equals(e.mode)) alreadyHeld = true;
                        } else if (!tableLockModesCompatible(e.mode, mode)) {
                            holder = e.session;
                            break;
                        }
                    }
                    if (holder == null) {
                        if (!alreadyHeld) entries.add(new TableLockEntry(session, mode));
                        return;
                    }
                    if (nowait) {
                        throw new MemgresException(
                                "could not obtain lock on relation \"" + relation + "\"", "55P03");
                    }
                    // Two sessions each holding what the other is waiting for will never make
                    // progress, and waiting for lock_timeout (or forever, when it is unset) turns
                    // that into a session no client can get an answer out of. PostgreSQL reports
                    // it on the waiter that closes the cycle, which is what this does.
                    if (session != null) {
                        waitingFor.put(session, holder);
                        registered = true;
                        if (hasDeadlock(session, holder)) {
                            throw new MemgresException("deadlock detected", "40P01");
                        }
                    }
                    StatementCancel.check();
                    long remaining = deadline == Long.MAX_VALUE ? 50L : deadline - System.currentTimeMillis();
                    if (remaining <= 0) {
                        throw new MemgresException("canceling statement due to lock timeout", "55P03");
                    }
                    try { tableLockMonitor.wait(Math.min(remaining, 50L)); } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw StatementCancel.canceled();
                    }
                }
            }
        } finally {
            if (registered) waitingFor.remove(session);
        }
    }

    /** The bare relation name inside a {@code schema.table} lock key, for an error message. */
    private static String relationNameOf(String tableKey) {
        if (tableKey == null) return "";
        int dot = tableKey.lastIndexOf('.');
        return dot >= 0 ? tableKey.substring(dot + 1) : tableKey;
    }

    /** Release all table-level locks for a session. */
    public void releaseTableLocks(Session session) {
        synchronized (tableLockMonitor) {
            for (List<TableLockEntry> entries : tableLevelLocks.values()) {
                entries.removeIf(e -> e.session == session);
            }
            tableLockMonitor.notifyAll();
        }
    }

    /** Check if a session can write to a table (no conflicting exclusive locks from other sessions). */
    public void checkTableLockForDml(String tableKey, Session session) {
        List<TableLockEntry> entries = tableLevelLocks.get(tableKey);
        if (entries == null) return;
        synchronized (tableLockMonitor) {
            while (true) {
                boolean conflict = false;
                entries = tableLevelLocks.get(tableKey);
                if (entries == null) return;
                for (TableLockEntry e : entries) {
                    if (e.session != session && !tableLockModesCompatible(e.mode, "RowExclusiveLock")) {
                        conflict = true;
                        break;
                    }
                }
                if (!conflict) return;
                try { tableLockMonitor.wait(5000); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new MemgresException("lock wait interrupted", "57014");
                }
            }
        }
    }

    /** Table-level lock compatibility. ACCESS EXCLUSIVE conflicts with everything. */
    private static boolean tableLockModesCompatible(String modeA, String modeB) {
        // ACCESS EXCLUSIVE conflicts with ALL modes
        if ("AccessExclusiveLock".equals(modeA) || "AccessExclusiveLock".equals(modeB)) return false;
        // EXCLUSIVE conflicts with ROW SHARE, ROW EXCLUSIVE, SHARE UPDATE EXCLUSIVE, SHARE, SHARE ROW EXCLUSIVE, EXCLUSIVE
        if ("ExclusiveLock".equals(modeA) || "ExclusiveLock".equals(modeB)) {
            // Only compatible with ACCESS SHARE (handled above as not AccessExclusive)
            return "AccessShareLock".equals(modeA) || "AccessShareLock".equals(modeB);
        }
        // For now, simplified: all other combos are compatible for the tests we need
        return true;
    }

    // Advisory locks: lock id -> per-session holds. All access is guarded by advisoryMonitor,
    // which also serves as the wait/notify point for blocking acquisitions.
    private final Map<AdvisoryLockId, List<AdvisoryHold>> advisoryLocks = new HashMap<>();
    private final Object advisoryMonitor = new Object();

    /**
     * Identifies an advisory lock target. PostgreSQL keeps the one-argument (single bigint)
     * and two-argument (two int4) forms in distinct keyspaces: pg_advisory_lock(1) does not
     * conflict with pg_advisory_lock(0, 1).
     */
    public static final class AdvisoryLockId {
        private final long key;
        private final boolean pairForm;

        public AdvisoryLockId(long key, boolean pairForm) {
            this.key = key;
            this.pairForm = pairForm;
        }

        /** classid column for pg_locks: high 32 bits of the key (first int of the two-arg form). */
        public int classId() { return (int) (key >>> 32); }

        /** objid column for pg_locks: low 32 bits of the key (second int of the two-arg form). */
        public int objId() { return (int) key; }

        /** objsubid column for pg_locks: 1 for the one-arg form, 2 for the two-arg form (PG convention). */
        public short objSubId() { return pairForm ? (short) 2 : (short) 1; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof AdvisoryLockId)) return false;
            AdvisoryLockId other = (AdvisoryLockId) o;
            return key == other.key && pairForm == other.pairForm;
        }

        @Override
        public int hashCode() {
            return 31 * (int) (key ^ (key >>> 32)) + (pairForm ? 1 : 0);
        }

        @Override
        public String toString() {
            return pairForm ? "(" + classId() + "," + objId() + ")" : String.valueOf(key);
        }
    }

    /**
     * One session's holds on one advisory lock. PostgreSQL reference-counts advisory lock
     * acquisitions, tracks shared vs exclusive mode separately, and distinguishes
     * session-level ownership (released only by explicit unlock or disconnect) from
     * transaction-level ownership (released automatically at COMMIT/ROLLBACK and never
     * releasable by pg_advisory_unlock).
     */
    private static final class AdvisoryHold {
        final Session session;
        int sessionExclusive;
        int sessionShared;
        int xactExclusive;
        int xactShared;

        AdvisoryHold(Session session) { this.session = session; }

        boolean holdsExclusive() { return sessionExclusive + xactExclusive > 0; }
        boolean holdsShared() { return sessionShared + xactShared > 0; }
        boolean empty() { return sessionExclusive == 0 && sessionShared == 0 && xactExclusive == 0 && xactShared == 0; }
    }

    /** Read-only row describing one advisory lock hold, for the pg_locks catalog view. */
    public static final class AdvisoryLockRow {
        public final int classId;
        public final int objId;
        public final short objSubId;
        public final Session session;
        public final boolean exclusive;

        AdvisoryLockRow(int classId, int objId, short objSubId, Session session, boolean exclusive) {
            this.classId = classId;
            this.objId = objId;
            this.objSubId = objSubId;
            this.session = session;
            this.exclusive = exclusive;
        }
    }

    // Roles: name -> attributes map
    private final Map<String, Map<String, String>> roles = new ConcurrentHashMap<>();

    // Role memberships: granted role (lowercase) -> set of member roles (lowercase)
    private final Map<String, Set<String>> roleMemberships = new ConcurrentHashMap<>();

    // admin_option flag: "grantedRole|memberRole" (lowercase) -> true if GRANT ... WITH ADMIN OPTION was used
    private final Map<String, Boolean> roleAdminOptions = new ConcurrentHashMap<>();

    // Granted privileges: role (lowercase) -> set of "privilege:objectType:objectName" entries
    private final Map<String, Set<String>> rolePrivileges = new ConcurrentHashMap<>();

    // Whether a relation has, or once had, rules: "everhad:<schema>.<relation>" keys
    private final Map<String, String> rules = new ConcurrentHashMap<>();

    // What each relation's rules do, in the order PostgreSQL fires them: by the relation's bare
    // name, with each rule carrying the schema of the relation it was written on
    private final Map<String, List<StoredRule>> relationRules = new ConcurrentHashMap<>();

    // The order rules were written in, which is the order the catalogs describe them in
    private final AtomicLong ruleCreations = new AtomicLong();

    // Object comments: "objectType:objectName" -> comment text
    private final Map<String, String> comments = new ConcurrentHashMap<>();

    // Schema object registry: maps schema name -> set of "type:name" entries
    // Used by DROP SCHEMA CASCADE to find objects belonging to a schema.
    private final Map<String, Set<String>> schemaObjectRegistry = new ConcurrentHashMap<>();

    // Object ownership: "objectType:objectName" -> owner role name (lowercase)
    private final Map<String, String> objectOwners = new ConcurrentHashMap<>();

    // Installed extensions: extension name -> version string
    private final Map<String, String> installedExtensions = new ConcurrentHashMap<>();

    // Extension schema: extension name -> schema name
    private final Map<String, String> extensionSchemas = new ConcurrentHashMap<>();

    // Two-phase commit: prepared transactions keyed by GID
    private final Map<String, PreparedTransaction> preparedTransactions = new ConcurrentHashMap<>();

    /** Represents a prepared (two-phase) transaction ready to be committed or rolled back by any session. */
    public static class PreparedTransaction {
        public final String gid;
        public final long transactionId;
        public final java.time.OffsetDateTime prepared;
        public final String owner;
        public final String database;
        public final List<Session.UndoEntry> undoLog;
        public final Map<String, Set<Object[]>> uncommittedInserts;
        public final Map<String, Map<Object[], Object[]>> uncommittedUpdates;
        public final Map<String, List<Object[]>> uncommittedDeletes;

        public PreparedTransaction(String gid, long transactionId, java.time.OffsetDateTime prepared,
                                   String owner, String database,
                                   List<Session.UndoEntry> undoLog,
                                   Map<String, Set<Object[]>> uncommittedInserts,
                                   Map<String, Map<Object[], Object[]>> uncommittedUpdates,
                                   Map<String, List<Object[]>> uncommittedDeletes) {
            this.gid = gid;
            this.transactionId = transactionId;
            this.prepared = prepared;
            this.owner = owner;
            this.database = database;
            this.undoLog = undoLog;
            this.uncommittedInserts = uncommittedInserts;
            this.uncommittedUpdates = uncommittedUpdates;
            this.uncommittedDeletes = uncommittedDeletes;
        }
    }

    /** Store a prepared transaction. Throws if the GID already exists. */
    public void addPreparedTransaction(PreparedTransaction pt) {
        if (preparedTransactions.putIfAbsent(pt.gid, pt) != null) {
            throw new MemgresException("transaction identifier \"" + pt.gid + "\" is already in use", "42710");
        }
    }

    /** Retrieve and remove a prepared transaction by GID. Returns null if not found. */
    public PreparedTransaction removePreparedTransaction(String gid) {
        return preparedTransactions.remove(gid);
    }

    /** Get all currently prepared transactions (for pg_prepared_xacts view). */
    public Map<String, PreparedTransaction> getPreparedTransactions() {
        return preparedTransactions;
    }

    // Default ACL entries from ALTER DEFAULT PRIVILEGES
    private final List<DefaultAclEntry> defaultAcls = new ArrayList<>();

    /** An entry recorded by ALTER DEFAULT PRIVILEGES. */
    public static class DefaultAclEntry {
        public final String grantor;    // role that issued the statement (null = current user placeholder)
        public final String schema;     // IN SCHEMA value (may be null)
        public final String objectType; // "TABLES", "SEQUENCES", "FUNCTIONS", "TYPES", "SCHEMAS"
        public final List<String> privileges;
        public final List<String> grantees;
        public final boolean isGrant;

        public DefaultAclEntry(String grantor, String schema, String objectType,
                               List<String> privileges, List<String> grantees, boolean isGrant) {
            this.grantor = grantor;
            this.schema = schema;
            this.objectType = objectType;
            this.privileges = privileges;
            this.grantees = grantees;
            this.isGrant = isGrant;
        }
    }

    public void addDefaultAcl(DefaultAclEntry entry) { defaultAcls.add(entry); }

    public void removeDefaultAcl(String schema, String objectType, List<String> grantees) {
        defaultAcls.removeIf(e -> e.isGrant
                && objectTypeMatches(e.objectType, objectType)
                && schemaMatches(e.schema, schema)
                && grantees.stream().anyMatch(g -> e.grantees.contains(g)));
    }

    private static boolean objectTypeMatches(String a, String b) {
        return a != null && a.equalsIgnoreCase(b);
    }

    private static boolean schemaMatches(String a, String b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.equalsIgnoreCase(b);
    }

    public List<DefaultAclEntry> getDefaultAcls() { return defaultAcls; }

    // Global transaction ID counter (monotonically increasing, like PG's xid)
    private final AtomicLong nextTransactionId = new AtomicLong(1);

    /** Allocate the next global transaction ID. */
    public long allocateTransactionId() { return nextTransactionId.getAndIncrement(); }

    // Per-table row metadata for system columns (xmin, xmax, cmin, cmax)
    // Key: row identity (Object[]), Value: [xmin, xmax, cmin, cmax]
    private final Map<String, Map<Object[], long[]>> tableRowMeta = new ConcurrentHashMap<>();

    /**
     * What the rows of the relation this name reaches answer for their system columns.
     *
     * <p>The book belongs to the relation rather than to the name, because that is what PostgreSQL
     * leaves untouched when a relation is renamed or moved to another schema: the rows go on
     * naming the transaction that inserted them and sitting where they always sat, so xmin and
     * cmin still answer after an ALTER TABLE that only changed what the relation is called. A name
     * no relation answers to -- one built for a query, one whose relation has been dropped -- keeps
     * a book of its own, which is what every name had before.
     */
    public Map<Object[], long[]> getRowMeta(String tableKey) {
        Table storage = relationNamed(tableKey);
        if (storage != null) return storage.getRowMeta();
        return tableRowMeta.computeIfAbsent(tableKey, k -> new IdentityHashMap<>());
    }

    /** The relation a {@code schema.table} key names, or null when nothing answers to it. */
    private Table relationNamed(String tableKey) {
        if (tableKey == null) return null;
        int dot = tableKey.indexOf('.');
        if (dot < 0) return null;
        Schema schema = schemas.get(tableKey.substring(0, dot));
        return schema == null ? null : schema.getTable(tableKey.substring(dot + 1));
    }

    /**
     * Record xmin/cmin metadata for a newly inserted row.
     *
     * <p>The line pointer comes from the relation that stores the row rather than from a counter
     * held under its name: a name PostgreSQL has used before says nothing about where the
     * relation now standing under it puts its tuples, because that relation has a file of its own
     * and numbers from one again. A counter left behind under a name nothing answers to any more
     * was also a counter nothing could ever reach.
     */
    public void setRowInsertMeta(String tableKey, Table storage, Object[] row, long xmin, long cmin) {
        long ctid = storage.nextTupleId();
        getRowMeta(tableKey).put(row, new long[]{xmin, 0, cmin, 0, ctid});
    }

    /** Update xmin metadata for an updated row (new ctid). */
    public void setRowUpdateMeta(String tableKey, Table storage, Object[] row, long xmin, long cmin) {
        long ctid = storage.nextTupleId();
        getRowMeta(tableKey).put(row, new long[]{xmin, 0, cmin, 0, ctid});
    }

    /**
     * Record which transaction deleted a row.
     *
     * <p>PostgreSQL's DELETE does not take the version away: it writes the deleting transaction's
     * id into that version's xmax and leaves it where it stands, which is what makes
     * {@code DELETE ... RETURNING xmax} answer the id rather than zero. The mark stays whatever
     * becomes of the transaction, so a row a rolled-back DELETE puts back still names the
     * transaction that tried to remove it.
     */
    public void setRowDeleteMeta(String tableKey, Object[] row, long xmax) {
        long[] meta = getRowMeta(tableKey).get(row);
        if (meta != null && meta.length > 1) meta[1] = xmax;
    }

    /** Remove row metadata (on delete). */
    public void removeRowMeta(String tableKey, Object[] row) {
        Table storage = relationNamed(tableKey);
        Map<Object[], long[]> meta = storage != null ? storage.getRowMeta()
                : tableRowMeta.get(tableKey);
        if (meta != null) meta.remove(row);
    }

    // Active sessions registry (for MVCC visibility)
    private final Set<Session> activeSessions = ConcurrentHashMap.newKeySet();

    // SSI: recently committed serializable transactions (for write-skew detection across commits)
    private final List<CommittedSsiInfo> recentlyCommittedSsi =
            java.util.Collections.synchronizedList(new ArrayList<>());

    // Exported snapshots: snapshot_id -> table snapshots
    private final Map<String, Map<String, List<Object[]>>> exportedSnapshots = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong snapshotCounter = new java.util.concurrent.atomic.AtomicLong(1);

    /** Export a snapshot: capture current state of all tables and return a snapshot ID. */
    public String exportSnapshot() {
        long id = snapshotCounter.getAndIncrement();
        String snapshotId = String.format("%08X-%08X-%d", id, id, 1);
        Map<String, List<Object[]>> snapshot = new LinkedHashMap<>();
        for (Map.Entry<String, Schema> schemaEntry : schemas.entrySet()) {
            String schemaName = schemaEntry.getKey();
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                String key = schemaName + "." + tableEntry.getKey();
                List<Object[]> rows = tableEntry.getValue().getRows();
                List<Object[]> copy = new ArrayList<>(rows.size());
                for (Object[] row : rows) copy.add(java.util.Arrays.copyOf(row, row.length));
                snapshot.put(key, copy);
            }
        }
        exportedSnapshots.put(snapshotId, snapshot);
        return snapshotId;
    }

    /** Import a previously exported snapshot. Returns the snapshot data or null. */
    public Map<String, List<Object[]>> importSnapshot(String snapshotId) {
        return exportedSnapshots.get(snapshotId);
    }

    /** Info about a recently committed serializable transaction's read/write sets. */
    public static class CommittedSsiInfo {
        private final Set<String> readTables;
        private final Set<String> writeTables;
        private final long commitTime;
        private final long sequence;

        public CommittedSsiInfo(Set<String> readTables, Set<String> writeTables, long sequence) {
            this.readTables = readTables;
            this.writeTables = writeTables;
            this.commitTime = System.currentTimeMillis();
            this.sequence = sequence;
        }

        public Set<String> readTables() { return readTables; }
        public Set<String> writeTables() { return writeTables; }
        public long commitTime() { return commitTime; }
        public long sequence() { return sequence; }
    }

    private final AtomicLong ssiSequence = new AtomicLong(0);

    /** Allocate a monotonic SSI sequence number (used to track transaction ordering). */
    public long allocateSsiSequence() { return ssiSequence.incrementAndGet(); }

    /** Record a committed serializable transaction's read/write sets. */
    public void recordCommittedSsiTransaction(Set<String> readTables, Set<String> writeTables) {
        recentlyCommittedSsi.add(new CommittedSsiInfo(readTables, writeTables, ssiSequence.incrementAndGet()));
        // Prune old entries (keep last 60 seconds)
        long cutoff = System.currentTimeMillis() - 60_000;
        recentlyCommittedSsi.removeIf(info -> info.commitTime() < cutoff);
    }

    /** Get recently committed serializable transactions. */
    public List<CommittedSsiInfo> getRecentlyCommittedSsiTransactions() {
        return new ArrayList<>(recentlyCommittedSsi);
    }

    // Connection tracking
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private volatile int maxConnections = 100;
    private volatile int maxPreparedTransactions = 0; // PG default: disabled

    public int getMaxPreparedTransactions() { return maxPreparedTransactions; }
    public void setMaxPreparedTransactions(int max) { this.maxPreparedTransactions = max; }

    public Database() {
        schemas.put("public", new Schema("public"));
        // Default superuser roles (similar to PG's postgres role)
        createRole("memgres", Cols.mapOf("SUPERUSER", "true", "LOGIN", "true"));
        createRole("postgres", Cols.mapOf("SUPERUSER", "true", "LOGIN", "true"));
        createRole("test", Cols.mapOf("SUPERUSER", "true", "LOGIN", "true"));
        // Register built-in trigger functions
        PgFunction suppressFunc = new PgFunction("suppress_redundant_updates_trigger", "trigger",
                "-- built-in: suppresses redundant updates", "internal");
        suppressFunc.setSchemaName("pg_catalog");
        addFunction(suppressFunc);
        // Register pg_sleep so ALTER FUNCTION pg_sleep(...) works (PG has it in pg_proc)
        PgFunction pgSleepFunc = new PgFunction("pg_sleep", "void",
                "-- built-in: pg_sleep", "internal",
                Cols.listOf(new PgFunction.Param("seconds", "double precision", "IN", null)), false);
        pgSleepFunc.setSchemaName("pg_catalog");
        pgSleepFunc.setVolatility("VOLATILE");
        pgSleepFunc.setStrict(true);
        addFunction(pgSleepFunc);
        // Register pg_sleep_for so it appears in pg_proc
        PgFunction pgSleepForFunc = new PgFunction("pg_sleep_for", "void",
                "-- built-in: pg_sleep_for", "internal",
                Cols.listOf(new PgFunction.Param("duration", "interval", "IN", null)), false);
        pgSleepForFunc.setSchemaName("pg_catalog");
        pgSleepForFunc.setVolatility("VOLATILE");
        pgSleepForFunc.setStrict(true);
        addFunction(pgSleepForFunc);
        // Register pg_sleep_until so it appears in pg_proc
        PgFunction pgSleepUntilFunc = new PgFunction("pg_sleep_until", "void",
                "-- built-in: pg_sleep_until", "internal",
                Cols.listOf(new PgFunction.Param("wakeup", "timestamp with time zone", "IN", null)), false);
        pgSleepUntilFunc.setSchemaName("pg_catalog");
        pgSleepUntilFunc.setVolatility("VOLATILE");
        pgSleepUntilFunc.setStrict(true);
        addFunction(pgSleepUntilFunc);
        // Register the built-in comparison functions so CREATE OPERATOR ... PROCEDURE = int4eq
        // resolves. They are functions and not procedures: PostgreSQL marks these prokind='f',
        // and calling one that had been marked 'p' was refused with "int4eq is a procedure" -- a
        // row that also broke PostgreSQL's own rule that a procedure returns void, since these
        // return boolean.
        String[][] builtinCompFuncs = {
                {"int4eq", "boolean", "integer,integer"},
                {"int4ne", "boolean", "integer,integer"},
                {"int4lt", "boolean", "integer,integer"},
                {"int4gt", "boolean", "integer,integer"},
                {"int4le", "boolean", "integer,integer"},
                {"int4ge", "boolean", "integer,integer"},
        };
        for (String[] f : builtinCompFuncs) {
            java.util.List<PgFunction.Param> params = new java.util.ArrayList<>();
            for (String pType : f[2].split(",")) {
                params.add(new PgFunction.Param(null, pType.trim(), "IN", null));
            }
            PgFunction fn = new PgFunction(f[0], f[1], "-- built-in: " + f[0], "internal", params, false);
            fn.setSchemaName("pg_catalog");
            fn.setVolatility("IMMUTABLE");
            fn.setStrict(true);
            addFunction(fn);
        }
    }

    // ---- Connection management ----

    public void setMaxConnections(int max) {
        this.maxConnections = max;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    /**
     * Try to register a new connection. Returns true if the connection is accepted,
     * false if the max connection limit has been reached.
     */
    public boolean registerConnection() {
        while (true) {
            int current = activeConnections.get();
            if (current >= maxConnections) {
                return false;
            }
            if (activeConnections.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /**
     * Unregister a connection (on disconnect).
     */
    public void unregisterConnection() {
        activeConnections.decrementAndGet();
    }

    // ---- Session registry for MVCC ----

    public void registerSession(Session session) {
        activeSessions.add(session);
    }

    public void unregisterSession(Session session) {
        activeSessions.remove(session);
    }

    /** Get all active sessions (for MVCC visibility checks). */
    public Set<Session> getActiveSessions() {
        return activeSessions;
    }

    // Transaction commit counter
    public void incrementXactCommit() { xactCommitCount.incrementAndGet(); }
    public long getXactCommitCount() { return xactCommitCount.get(); }

    // Analyzed tables tracking
    public void recordAnalyzedTable(String schemaTable) { analyzedTables.add(schemaTable); }
    public Set<String> getAnalyzedTables() { return analyzedTables; }

    // Clustered index tracking
    public void setClusteredIndex(String indexName) { clusteredIndexes.add(idxName(indexName).toLowerCase()); }
    public boolean isClusteredIndex(String indexName) { return clusteredIndexes.contains(idxName(indexName).toLowerCase()); }

    /** Snapshot of all advisory lock holds (for pg_locks). One row per (session, lock, mode). */
    public List<AdvisoryLockRow> getAdvisoryLockRows() {
        List<AdvisoryLockRow> rows = new ArrayList<>();
        synchronized (advisoryMonitor) {
            for (Map.Entry<AdvisoryLockId, List<AdvisoryHold>> e : advisoryLocks.entrySet()) {
                AdvisoryLockId id = e.getKey();
                for (AdvisoryHold h : e.getValue()) {
                    if (h.holdsExclusive()) {
                        rows.add(new AdvisoryLockRow(id.classId(), id.objId(), id.objSubId(), h.session, true));
                    }
                    if (h.holdsShared()) {
                        rows.add(new AdvisoryLockRow(id.classId(), id.objId(), id.objSubId(), h.session, false));
                    }
                }
            }
        }
        return rows;
    }

    public Schema getSchema(String name) {
        return schemas.get(name);
    }

    public Schema getOrCreateSchema(String name) {
        boolean existed = schemas.containsKey(name);
        Schema schema = schemas.computeIfAbsent(name, Schema::new);
        if (!existed) markUncommittedObject(schema, CURRENT_VIEWER.get());
        return schema;
    }

    /**
     * Rename a schema, carrying its contents with it. The schema object holds the tables, so it
     * is re-keyed rather than rebuilt; anything that records the schema name separately is
     * updated so the catalog and name resolution keep agreeing.
     */
    public void renameSchema(String oldName, String newName) {
        Schema existing = schemas.get(oldName);
        if (existing == null) {
            throw new MemgresException("schema \"" + oldName + "\" does not exist", "3F000");
        }
        if (schemas.containsKey(newName)) {
            throw new MemgresException("schema \"" + newName + "\" already exists", "42P06");
        }
        Schema renamed = new Schema(newName);
        for (Map.Entry<String, Table> t : existing.getTables().entrySet()) {
            renamed.addTable(t.getValue());
        }
        schemas.remove(oldName);
        schemas.put(newName, renamed);
        for (Map.Entry<String, ViewDef> e : new LinkedHashMap<>(views).entrySet()) {
            ViewDef v = e.getValue();
            if (oldName.equalsIgnoreCase(v.schemaName())) {
                // The view is keyed by the schema it lives in, which just changed its name.
                views.remove(e.getKey());
                addView(new ViewDef(v.name(), newName, v.query(), v.orReplace(),
                        v.materialized(), v.cachedColumns(), v.cachedRows(), v.sourceSQL(),
                        v.checkOption(), v.reloptions(), v.populated()));
            }
        }
    }

    public Map<String, Schema> getSchemas() {
        return visible(schemas);
    }

    /** The register that says which OID belongs to which object. @see ObjectIdentity */
    ObjectIdentity objectIdentity() {
        return objectIdentity;
    }

    /**
     * Lookup a table by unqualified name, searching pg_catalog first, then public.
     */
    public Table getTable(String name) {
        Schema pgCatalog = schemas.get("pg_catalog");
        if (pgCatalog != null) {
            Table t = pgCatalog.getTable(name);
            if (t != null) return t;
        }
        Schema pub = schemas.get("public");
        if (pub != null) {
            Table t = pub.getTable(name);
            if (t != null) return t;
        }
        for (Schema s : schemas.values()) {
            Table t = s.getTable(name);
            if (t != null) return t;
        }
        return null;
    }

    public void addSchemaAcl(String schemaName, String aclItem) {
        schemaAcls.computeIfAbsent(schemaName.toLowerCase(), k -> new java.util.ArrayList<>()).add(aclItem);
    }

    public List<String> getSchemaAcl(String schemaName) {
        return schemaAcls.get(schemaName.toLowerCase());
    }

    public void removeSchema(String name) {
        schemas.remove(name);
    }

    public NotificationManager getNotificationManager() {
        return notificationManager;
    }

    public LargeObjectStore getLargeObjectStore() {
        return largeObjectStore;
    }

    public DatabaseRegistry getDatabaseRegistry() {
        return databaseRegistry;
    }

    public void setDatabaseRegistry(DatabaseRegistry databaseRegistry) {
        this.databaseRegistry = databaseRegistry;
    }

    /**
     * Every key under which some user-defined type of any kind is stored. One namespace per
     * schema holds all five kinds, so this is what decides whether a name is taken and which
     * schema a bare one resolves to.
     */
    Set<String> typeKeys() {
        Set<String> keys = new java.util.LinkedHashSet<>();
        keys.addAll(customEnums.keySet());
        keys.addAll(compositeTypes.keySet());
        keys.addAll(rangeTypes.keySet());
        keys.addAll(domains.keySet());
        keys.addAll(shellTypes);
        return keys;
    }

    // Custom ENUM types
    public void addCustomEnum(CustomEnum customEnum) {
        customEnums.put(TypeNamespace.key(customEnum.getSchemaName(), customEnum.getName()), customEnum);
        markUncommittedObject(customEnum, CURRENT_VIEWER.get());
    }

    public CustomEnum getCustomEnum(String name) {
        String key = TypeNamespace.find(customEnums.keySet(), name);
        return key == null ? null : customEnums.get(key);
    }

    /** The enum of that name in that schema, and only that one. */
    public CustomEnum getCustomEnum(String schema, String name) {
        return customEnums.get(TypeNamespace.key(schema, name));
    }

    public boolean isCustomEnum(String typeName) {
        return TypeNamespace.find(customEnums.keySet(), typeName) != null;
    }

    public void replaceCustomEnum(CustomEnum e) {
        addCustomEnum(e);
    }

    public void removeCustomEnum(String name) {
        String key = TypeNamespace.find(customEnums.keySet(), name);
        if (key != null) customEnums.remove(key);
    }

    public Map<String, CustomEnum> getCustomEnums() {
        return visible(customEnums);
    }

    // Composite types
    public void addCompositeType(String name, List<CreateTypeStmt.CompositeField> fields) {
        addCompositeType(TypeNamespace.writtenSchema(name), name, fields);
    }

    public void addCompositeType(String schema, String name, List<CreateTypeStmt.CompositeField> fields) {
        compositeTypes.put(TypeNamespace.key(schema, name), fields);
        markUncommittedObject(fields, CURRENT_VIEWER.get());
    }

    public boolean isCompositeType(String name) {
        return TypeNamespace.find(compositeTypes.keySet(), name) != null;
    }

    public List<CreateTypeStmt.CompositeField> getCompositeType(String name) {
        String key = TypeNamespace.find(compositeTypes.keySet(), name);
        return key == null ? null : liveAttributes(compositeTypes.get(key));
    }

    /**
     * Give a composite type that already exists a new attribute list.
     *
     * <p>ALTER TYPE ... ADD/DROP/RENAME/ALTER ATTRIBUTE rewrites the list rather than editing it in
     * place, and the map {@link #getCompositeTypes()} hands back is filtered to what the reading
     * session may see -- while another session holds uncommitted DDL that reading is a copy, and
     * writing through the copy threw the change away: the ALTER reported success while the type kept
     * the attributes it had, and the next statement that named the new attribute was told there was
     * no such column. What a type is made of is not a question of what one session can see, so it is
     * written to the stored map. The new list inherits whatever the old one recorded about a
     * transaction that has not committed yet, or a type created and then altered inside one
     * still-open transaction would become visible to every other session the moment it was altered.
     */
    public void replaceCompositeFields(String key, List<CreateTypeStmt.CompositeField> fields) {
        if (key == null) return;
        List<CreateTypeStmt.CompositeField> previous = compositeTypes.put(key, fields);
        if (previous == null) return;
        Session creator = uncommittedObjects.remove(previous);
        if (creator != null) uncommittedObjects.put(fields, creator);
    }

    private static final String DROPPED_ATTRIBUTE_PREFIX = "........pg.dropped.";
    private static final String DROPPED_ATTRIBUTE_SUFFIX = "........";

    /**
     * The name PostgreSQL gives an attribute that ALTER TYPE ... DROP ATTRIBUTE took away.
     *
     * <p>A dropped attribute is not deleted: its pg_attribute row stays behind, marked dropped and
     * renamed to something no statement could have written, so the attributes after it keep their
     * numbers and the next one added takes a number of its own rather than the one just freed.
     * pg_attribute is where a client reads the shape of a composite, and a row that simply vanished
     * told it the type had renumbered itself.
     */
    public static String droppedAttributeName(int attnum) {
        return DROPPED_ATTRIBUTE_PREFIX + attnum + DROPPED_ATTRIBUTE_SUFFIX;
    }

    /** Whether this attribute is one of those placeholders rather than a live attribute. */
    public static boolean isDroppedAttribute(CreateTypeStmt.CompositeField field) {
        return field != null && field.name() != null
                && field.name().startsWith(DROPPED_ATTRIBUTE_PREFIX)
                && field.name().endsWith(DROPPED_ATTRIBUTE_SUFFIX);
    }

    /**
     * A composite type's attributes without the placeholders a drop left behind. The placeholders
     * hold their numbers in the catalogs, but the type itself is made of what is left: a value of it
     * has one field per surviving attribute, which is what everything outside the catalogs asks for.
     * The list itself comes back when nothing was dropped, which is the ordinary case.
     */
    private static List<CreateTypeStmt.CompositeField> liveAttributes(
            List<CreateTypeStmt.CompositeField> fields) {
        if (fields == null) return null;
        List<CreateTypeStmt.CompositeField> live = new ArrayList<>();
        for (CreateTypeStmt.CompositeField f : fields) {
            if (!isDroppedAttribute(f)) live.add(f);
        }
        return live.size() == fields.size() ? fields : live;
    }

    /**
     * Fields of the composite type a name denotes. Every table implicitly defines one with its
     * own name and columns, so a row can be cast to a table's type the way PG allows.
     */
    public List<CreateTypeStmt.CompositeField> getRowType(String name) {
        List<CreateTypeStmt.CompositeField> explicit = getCompositeType(name);
        if (explicit != null) return explicit;
        Table table = getTable(name);
        if (table == null) return null;
        List<CreateTypeStmt.CompositeField> fields = new ArrayList<>();
        for (Column col : table.getColumns()) {
            fields.add(new CreateTypeStmt.CompositeField(col.getName(),
                    col.getType() != null ? col.getType().name().toLowerCase() : "text"));
        }
        return fields;
    }

    public void removeCompositeType(String name) {
        String key = TypeNamespace.find(compositeTypes.keySet(), name);
        if (key != null) compositeTypes.remove(key);
    }

    public Map<String, List<CreateTypeStmt.CompositeField>> getCompositeTypes() {
        return visible(compositeTypes);
    }

    // User-defined range types
    public void addRangeType(String name, String subtype) {
        addRangeType(TypeNamespace.writtenSchema(name), name, subtype);
    }

    public void addRangeType(String schema, String name, String subtype) {
        rangeTypes.put(TypeNamespace.key(schema, name), subtype);
    }

    /** The subtype of the range type a written name denotes, or null. */
    public String getRangeSubtype(String name) {
        String key = TypeNamespace.find(rangeTypes.keySet(), name);
        return key == null ? null : rangeTypes.get(key);
    }

    public boolean isRangeType(String name) {
        return TypeNamespace.find(rangeTypes.keySet(), name) != null;
    }

    public void removeRangeType(String name) {
        String key = TypeNamespace.find(rangeTypes.keySet(), name);
        if (key != null) rangeTypes.remove(key);
    }

    public Map<String, String> getRangeTypes() {
        return rangeTypes;
    }

    // Shell types: a name reserved by CREATE TYPE name; with no definition yet
    public void addShellType(String name) {
        addShellType(TypeNamespace.writtenSchema(name), name);
    }

    public void addShellType(String schema, String name) {
        shellTypes.add(TypeNamespace.key(schema, name));
    }

    public boolean isShellType(String name) {
        return TypeNamespace.find(shellTypes, name) != null;
    }

    public void removeShellType(String name) {
        String key = TypeNamespace.find(shellTypes, name);
        if (key != null) shellTypes.remove(key);
    }

    public Set<String> getShellTypes() {
        return shellTypes;
    }

    public void addUserCast(int sourceOid, int targetOid, int castFunc, String castContext, String castMethod) {
        addUserCast(sourceOid, targetOid, castFunc, castContext, castMethod, null);
    }

    /**
     * Record a cast, and the function that performs it.
     *
     * <p>The name was thrown away and only a zero kept in its place, so a cast created WITH
     * FUNCTION was listed in pg_cast and then never used: the value went through the ordinary
     * text conversion instead, and an enum cast to an integer was read as a number rather than
     * passed to the function that was written for it.
     */
    public void addUserCast(int sourceOid, int targetOid, int castFunc, String castContext,
                            String castMethod, String functionName) {
        userDefinedCasts.add(new Object[]{sourceOid, targetOid, castFunc, castContext, castMethod,
                functionName});
    }

    /** The function that casts {@code sourceOid} to {@code targetOid}, or null when none does. */
    public String castFunctionFor(int sourceOid, int targetOid) {
        for (Object[] cast : userDefinedCasts) {
            if ((int) cast[0] == sourceOid && (int) cast[1] == targetOid
                    && cast.length > 5 && cast[5] != null) {
                return (String) cast[5];
            }
        }
        return null;
    }

    public java.util.List<Object[]> getUserDefinedCasts() {
        return userDefinedCasts;
    }

    public void removeUserCast(int sourceOid, int targetOid) {
        userDefinedCasts.removeIf(c -> (int) c[0] == sourceOid && (int) c[1] == targetOid);
    }

    // User-defined aggregates
    public void addAggregate(PgAggregate agg) {
        userAggregates.put(agg.getName().toLowerCase(), agg);
    }

    public PgAggregate getAggregate(String name) {
        return userAggregates.get(name.toLowerCase());
    }

    public boolean hasAggregate(String name) {
        return userAggregates.containsKey(name.toLowerCase());
    }

    public void removeAggregate(String name) {
        userAggregates.remove(name.toLowerCase());
    }

    public Map<String, PgAggregate> getUserAggregates() {
        return userAggregates;
    }

    // User-defined operators (keyed by name+argtypes for overloading)
    public void addOperator(PgOperator op) {
        userOperators.put(op.getKey().toLowerCase(), op);
    }

    public PgOperator getOperator(String key) {
        return userOperators.get(key.toLowerCase());
    }

    public boolean hasOperator(String key) {
        return userOperators.containsKey(key.toLowerCase());
    }

    public void removeOperator(String key) {
        userOperators.remove(key.toLowerCase());
    }

    public Map<String, PgOperator> getUserOperators() {
        return userOperators;
    }

    /** Find all operators with a given name (across all arg type combinations). */
    public java.util.List<PgOperator> getOperatorsByName(String name) {
        java.util.List<PgOperator> result = new java.util.ArrayList<>();
        for (PgOperator op : userOperators.values()) {
            if (op.getName().equals(name)) result.add(op);
        }
        return result;
    }

    // User-defined operator families
    public void addOperatorFamily(PgOperatorFamily fam) {
        userOperatorFamilies.put(fam.getKey(), fam);
    }

    public PgOperatorFamily getOperatorFamily(String key) {
        return userOperatorFamilies.get(key);
    }

    public boolean hasOperatorFamily(String key) {
        return userOperatorFamilies.containsKey(key);
    }

    public void removeOperatorFamily(String key) {
        userOperatorFamilies.remove(key);
    }

    public Map<String, PgOperatorFamily> getUserOperatorFamilies() {
        return userOperatorFamilies;
    }

    // User-defined operator classes
    public void addOperatorClass(PgOperatorClass cls) {
        userOperatorClasses.put(cls.getKey(), cls);
    }

    public PgOperatorClass getOperatorClass(String key) {
        return userOperatorClasses.get(key);
    }

    public boolean hasOperatorClass(String key) {
        return userOperatorClasses.containsKey(key);
    }

    public void removeOperatorClass(String key) {
        userOperatorClasses.remove(key);
    }

    public Map<String, PgOperatorClass> getUserOperatorClasses() {
        return userOperatorClasses;
    }

    /** Remove all operator classes belonging to a given family. */
    public void removeOperatorClassesByFamily(String familyName) {
        userOperatorClasses.entrySet().removeIf(e ->
                familyName.equalsIgnoreCase(e.getValue().getFamilyName()));
    }

    // Functions, stored by name, supporting overloads with different parameter types
    public void addFunction(PgFunction function) {
        String key = function.getName().toLowerCase();
        List<PgFunction> overloads = functionOverloads.computeIfAbsent(key, k -> new ArrayList<>());
        overloads.add(function);
        functions.put(key, function); // last-added wins for simple name lookup
        // The number is handed out here rather than at the first question about the routine,
        // because PostgreSQL assigns an OID when the object is created: everything ordered by OID
        // -- what a schema holds, what depends on a type -- then comes back in the order the
        // objects were made rather than in the order some map happened to be walked.
        objectIdentity.oid("proc:" + function.getName());
        markUncommittedObject(function, CURRENT_VIEWER.get());
    }

    /**
     * Every routine the database holds, overloads included. {@link #getFunctions} answers with one
     * routine per name, which is enough to call one but not to say what depends on a type: two
     * overloads of a name are two objects, and only the one declared in terms of the type stands
     * in the way of dropping it.
     */
    public List<PgFunction> getAllFunctionOverloads() {
        List<PgFunction> all = new ArrayList<>();
        for (List<PgFunction> overloads : functionOverloads.values()) {
            for (PgFunction f : overloads) {
                if (visibleOne(f) != null) all.add(f);
            }
        }
        return all;
    }

    /**
     * Take away exactly this routine, leaving every other overload of its name standing. Removing
     * by name alone took away overloads nothing depended on.
     */
    public void removeFunctionOverload(PgFunction function) {
        removeMatchingOverloads(function.getName(), f -> f == function);
    }

    /** Returns the single function with this name, or the first overload if multiple exist. */
    public PgFunction getFunction(String name) {
        // A name that carries its schema names one schema's function and no other's, so it is
        // answered from that schema: looking the whole "schema.fn" text up as a bare name found
        // nothing, and a trigger written to call one was refused for a function that exists.
        int dot = name == null ? -1 : name.lastIndexOf('.');
        if (dot > 0) {
            return getFunction(name.substring(0, dot), name.substring(dot + 1));
        }
        String key = name.toLowerCase();
        List<PgFunction> overloads = functionOverloads.get(key);
        if (overloads != null && !overloads.isEmpty()) return overloads.get(0);
        return functions.get(key);
    }

    /** Returns a function matching both name and schema, or null. */
    public PgFunction getFunction(String schema, String name) {
        if (schema == null) return getFunction(name);
        String key = name.toLowerCase();
        List<PgFunction> overloads = functionOverloads.get(key);
        if (overloads != null) {
            for (PgFunction f : overloads) {
                if (schema.equalsIgnoreCase(f.getSchemaName())) return f;
            }
        }
        PgFunction single = functions.get(key);
        if (single != null && schema.equalsIgnoreCase(single.getSchemaName())) return single;
        return null;
    }

    /** Returns all overloads for the given function name. */
    public List<PgFunction> getFunctionOverloads(String name) {
        List<PgFunction> overloads = functionOverloads.get(name.toLowerCase());
        return overloads != null ? overloads : Cols.listOf();
    }

    /**
     * Returns the overloads of this name that live in the given schema. Functions are a
     * per-schema namespace in PG, so the same name may exist in several schemas at once and
     * every by-name lookup has to say which one it means.
     */
    public List<PgFunction> getFunctionOverloads(String schema, String name) {
        List<PgFunction> result = new ArrayList<>();
        for (PgFunction f : getFunctionOverloads(name)) {
            if (schemaOf(f).equalsIgnoreCase(schema)) result.add(f);
        }
        return result;
    }

    /** A function with no recorded schema is treated as living in public. */
    /** The schema that holds this relation, or null when none does. */
    public String schemaNameOf(Table table) {
        if (table == null) return null;
        for (Map.Entry<String, Schema> e : schemas.entrySet()) {
            if (e.getValue().getTable(table.getName()) == table) return e.getKey();
        }
        return null;
    }

    public static String schemaOf(PgFunction f) {
        return f.getSchemaName() != null ? f.getSchemaName() : "public";
    }

    /**
     * A variadic parameter normally has to be given at least one value, but one declared with a
     * default supplies its own, so the call may leave it out entirely.
     */
    private static boolean variadicMayBeOmitted(PgFunction f) {
        for (PgFunction.Param p : f.getParams()) {
            if ("VARIADIC".equalsIgnoreCase(p.mode())) {
                return p.defaultExpr() != null;
            }
        }
        return false;
    }

    /** Finds the best matching overload by argument count and types. */
    public PgFunction resolveFunction(String name, int argCount, List<String> argTypeHints) {
        return resolveFunction(getFunctionOverloads(name), argCount, argTypeHints);
    }

    /**
     * Finds the best matching overload among an already-narrowed candidate list. Callers that
     * have applied schema visibility pass the surviving candidates in search_path order.
     */
    public PgFunction resolveFunction(List<PgFunction> overloads, int argCount, List<String> argTypeHints) {
        if (overloads == null || overloads.isEmpty()) return null;
        // For a single overload, return it unless type hints indicate a clearly incompatible call.
        // This preserves correct behavior for functions with default params or INOUT params
        // where arg count may not match param count, while still rejecting calls like
        // fn_to_drop(1) when only fn_to_drop(text) exists (integer !-> text in PG).
        //
        // We only reject when a numeric-family hint targets a text-family param, matching
        // PG's rule that integer literals don't implicitly cast to text in function resolution.
        // Other mismatches are allowed because type hints can be unreliable (e.g., JDBC may
        // report 'text' for a numeric expression result).
        if (overloads.size() == 1) {
            if (argTypeHints == null || argTypeHints.isEmpty()
                    || argTypeHints.stream().allMatch(h -> h == null)) {
                // For VARIADIC functions with unknown types, reject if no variadic args provided
                PgFunction single = overloads.get(0);
                boolean hasVariadic = single.getParams().stream().anyMatch(p -> "VARIADIC".equalsIgnoreCase(p.mode()));
                if (hasVariadic && !variadicMayBeOmitted(single)) {
                    long nonVariadicCount = single.getParams().stream()
                            .filter(p -> !"OUT".equalsIgnoreCase(p.mode()) && !"VARIADIC".equalsIgnoreCase(p.mode()))
                            .count();
                    if (argCount <= nonVariadicCount) return null;
                }
                return single;
            }
            PgFunction f = overloads.get(0);
            // For VARIADIC functions, reject if no args provided for the variadic parameter
            boolean fHasVariadic = f.getParams().stream().anyMatch(p -> "VARIADIC".equalsIgnoreCase(p.mode()));
            List<PgFunction.Param> inputParams = f.getParams().stream()
                    .filter(p -> !"OUT".equalsIgnoreCase(p.mode()) && !"VARIADIC".equalsIgnoreCase(p.mode()))
                    .collect(Collectors.toList());
            if (fHasVariadic && !variadicMayBeOmitted(f) && argCount <= inputParams.size()) return null;
            boolean hasIncompatible = false;
            for (int i = 0; i < argTypeHints.size() && i < inputParams.size(); i++) {
                String hint = argTypeHints.get(i);
                String paramType = inputParams.get(i).typeName();
                if (hint != null && paramType != null && isNumericToTextMismatch(hint, paramType)) {
                    hasIncompatible = true;
                    break;
                }
            }
            if (!hasIncompatible) {
                return f;
            }
        }

        // Try to match by argument count and type hints
        for (PgFunction f : overloads) {
            List<PgFunction.Param> inputParams = f.getParams().stream()
                    .filter(p -> !"OUT".equalsIgnoreCase(p.mode()) && !"VARIADIC".equalsIgnoreCase(p.mode()))
                    .collect(Collectors.toList());
            boolean hasVariadic = f.getParams().stream().anyMatch(p -> "VARIADIC".equalsIgnoreCase(p.mode()));
            if (hasVariadic) {
                // VARIADIC functions require at least one arg beyond the non-variadic params
                // PG rejects calls where no args are provided for the VARIADIC parameter
                if (argCount <= inputParams.size()) continue;
            } else {
                if (inputParams.size() != argCount) continue;
            }
            if (argTypeHints != null && !argTypeHints.isEmpty()) {
                boolean match = true;
                for (int i = 0; i < argTypeHints.size() && i < inputParams.size(); i++) {
                    String hint = argTypeHints.get(i);
                    String paramType = inputParams.get(i).typeName();
                    if (hint != null && paramType != null && !typesCompatible(hint, paramType)) {
                        match = false;
                        break;
                    }
                }
                if (match) return f;
            }
        }
        // Fallback: match by arg count only — but only when no type hints were provided.
        // When type hints exist and no overload matched, the call is genuinely unresolvable
        // (e.g., fn_to_drop(integer) was dropped, fn_to_drop(text) remains, calling fn_to_drop(1)).
        if (argTypeHints == null || argTypeHints.isEmpty() || argTypeHints.stream().allMatch(h -> h == null)) {
            // First pass: prefer non-variadic exact matches
            for (PgFunction f : overloads) {
                long inputCount = f.getParams().stream()
                        .filter(p -> !"OUT".equalsIgnoreCase(p.mode()) && !"VARIADIC".equalsIgnoreCase(p.mode()))
                        .count();
                boolean hasVariadic = f.getParams().stream().anyMatch(p -> "VARIADIC".equalsIgnoreCase(p.mode()));
                if (!hasVariadic && inputCount == argCount) return f;
            }
            // Second pass: allow VARIADIC only when extra args are provided beyond the required params
            for (PgFunction f : overloads) {
                long inputCount = f.getParams().stream()
                        .filter(p -> !"OUT".equalsIgnoreCase(p.mode()) && !"VARIADIC".equalsIgnoreCase(p.mode()))
                        .count();
                boolean hasVariadic = f.getParams().stream().anyMatch(p -> "VARIADIC".equalsIgnoreCase(p.mode()));
                if (hasVariadic && argCount > inputCount) return f;
            }
            return overloads.get(0); // fallback to first
        }
        return null;
    }

    /**
     * Returns true if the arg type hint is from the numeric family and the param type
     * is from the text family. In PG, numeric types do NOT implicitly cast to text
     * for function resolution purposes (e.g., fn(integer) should not match fn(text)).
     */
    private boolean isNumericToTextMismatch(String argType, String paramType) {
        String a = argType.toLowerCase().trim();
        String p = paramType.toLowerCase().trim();
        Set<String> numeric = Cols.setOf("int", "integer", "int4", "bigint", "int8", "smallint", "int2",
                "numeric", "decimal", "real", "float", "float4", "float8", "double precision");
        Set<String> textual = Cols.setOf("text", "varchar", "character varying", "char", "character", "name");
        return numeric.contains(a) && textual.contains(p);
    }

    public boolean typesCompatible(String argType, String paramType) {
        String a = argType.toLowerCase().trim();
        String p = paramType.toLowerCase().trim();
        if (a.equals(p)) return true;
        // Numeric family
        Set<String> numeric = Cols.setOf("int", "integer", "int4", "bigint", "int8", "smallint", "int2",
                "numeric", "decimal", "real", "float", "float4", "float8", "double precision");
        // Text family
        Set<String> textual = Cols.setOf("text", "varchar", "character varying", "char", "character", "name");
        if (numeric.contains(a) && numeric.contains(p)) return true;
        if (textual.contains(a) && textual.contains(p)) return true;
        return false;
    }

    public void removeFunction(String name) {
        String key = name.toLowerCase();
        functions.remove(key);
        functionOverloads.remove(key);
    }

    /** Remove every overload of this name that lives in the given schema. */
    public void removeFunction(String schema, String name) {
        removeMatchingOverloads(name, f -> schema == null || schemaOf(f).equalsIgnoreCase(schema));
    }

    /** Remove a specific overload by name and param types. */
    public void removeFunction(String name, List<String> paramTypes) {
        removeFunction(null, name, paramTypes);
    }

    /** Remove a specific overload by schema, name and param types. A null schema matches any. */
    public void removeFunction(String schema, String name, List<String> paramTypes) {
        removeMatchingOverloads(name, f -> {
            if (schema != null && !schemaOf(f).equalsIgnoreCase(schema)) return false;
            List<String> fTypes = f.getParams().stream()
                    .filter(p -> !"OUT".equalsIgnoreCase(p.mode()))
                    .map(PgFunction.Param::typeName)
                    .collect(Collectors.toList());
            if (fTypes.size() != paramTypes.size()) return false;
            for (int i = 0; i < fTypes.size(); i++) {
                if (!typesCompatible(fTypes.get(i), paramTypes.get(i))) return false;
            }
            return true;
        });
    }

    private void removeMatchingOverloads(String name, java.util.function.Predicate<PgFunction> matches) {
        String key = name.toLowerCase();
        List<PgFunction> overloads = functionOverloads.get(key);
        if (overloads == null) return;
        overloads.removeIf(matches);
        if (overloads.isEmpty()) {
            functionOverloads.remove(key);
            functions.remove(key);
        } else {
            functions.put(key, overloads.get(0));
        }
    }

    /** Rename a single specific overload of a function/procedure. */
    public void renameFunctionOverload(PgFunction func, String newName) {
        String oldName = func.getName();
        String oldKey = oldName.toLowerCase();
        String newKey = newName.toLowerCase();
        // Remove this specific overload from the old name's overload list
        List<PgFunction> oldOverloads = functionOverloads.get(oldKey);
        if (oldOverloads != null) {
            oldOverloads.remove(func);
            if (oldOverloads.isEmpty()) {
                functionOverloads.remove(oldKey);
                functions.remove(oldKey);
            } else {
                functions.put(oldKey, oldOverloads.get(0));
            }
        }
        // Update the function's name
        func.setName(newName);
        // Add to the new name's overload list
        List<PgFunction> newOverloads = functionOverloads.computeIfAbsent(newKey, k -> new ArrayList<>());
        newOverloads.add(func);
        functions.put(newKey, func);
        // Update schema registry
        if (oldOverloads == null || oldOverloads.isEmpty()) {
            for (Map.Entry<String, Set<String>> entry : schemaObjectRegistry.entrySet()) {
                if (entry.getValue().remove("function:" + oldKey)) {
                    entry.getValue().add("function:" + newKey);
                }
            }
        } else {
            // Old name still exists (other overloads remain), just register the new name
            for (Map.Entry<String, Set<String>> entry : schemaObjectRegistry.entrySet()) {
                entry.getValue().add("function:" + newKey);
            }
        }
        // Update object ownership key
        String oldOwner = objectOwners.remove("function:" + oldKey);
        if (oldOwner != null) objectOwners.put("function:" + newKey, oldOwner);
    }

    /** Rename a function/procedure: re-key in all maps, update the PgFunction name field. */
    public void renameFunction(String oldName, String newName) {
        String oldKey = oldName.toLowerCase();
        String newKey = newName.toLowerCase();
        List<PgFunction> overloads = functionOverloads.remove(oldKey);
        PgFunction single = functions.remove(oldKey);
        if (overloads != null) {
            for (PgFunction f : overloads) f.setName(newName);
            functionOverloads.put(newKey, overloads);
        }
        if (single != null) {
            single.setName(newName);
            functions.put(newKey, single);
        }
        // Update schema registry
        for (Map.Entry<String, Set<String>> entry : schemaObjectRegistry.entrySet()) {
            if (entry.getValue().remove("function:" + oldKey)) {
                entry.getValue().add("function:" + newKey);
            }
        }
        // Update object ownership key
        String oldOwner = objectOwners.remove("function:" + oldKey);
        if (oldOwner != null) objectOwners.put("function:" + newKey, oldOwner);
    }

    public Map<String, PgFunction> getFunctions() {
        return visible(functions);
    }

    // Triggers
    public void addTrigger(PgTrigger trigger) {
        triggers.computeIfAbsent(trigger.getTableName().toLowerCase(), k -> new ArrayList<>())
                .add(trigger);
    }

    public List<PgTrigger> getTriggersForTable(String tableName) {
        syncPartitionTriggers();
        List<PgTrigger> list = triggers.get(tableName.toLowerCase());
        if (list == null || list.isEmpty()) return Cols.listOf();
        List<PgTrigger> sorted = new ArrayList<>(list);
        sorted.sort(java.util.Comparator.comparing(t -> t.getName().toLowerCase()));
        return sorted;
    }

    public Map<String, List<PgTrigger>> getAllTriggers() {
        syncPartitionTriggers();
        return triggers;
    }

    /**
     * Give every partition its own copy of the row triggers the relation it belongs to carries.
     *
     * <p>PostgreSQL clones a partitioned table's FOR EACH ROW triggers onto each partition: the
     * partition owns a pg_trigger row whose tgparentid points back at the trigger it was cloned
     * from, which is why the partition's catalog shows the trigger and why DROP TRIGGER on the
     * partition is refused. A statement-level trigger fires once for the statement, on the
     * relation it was declared on, and is not cloned.
     *
     * <p>Partitions are created and attached long after the trigger was written, and detached
     * again, so the copies are brought up to date whenever the registry is read rather than at
     * the one place a partition happens to be attached. The same pass takes a copy away when its
     * relation is no longer a partition of the relation it was cloned from, or when the original
     * has been dropped, which is what PostgreSQL does at DETACH PARTITION and at DROP TRIGGER.
     */
    private void syncPartitionTriggers() {
        if (triggers.isEmpty()) return;
        Iterator<Map.Entry<String, List<PgTrigger>>> stale = triggers.entrySet().iterator();
        while (stale.hasNext()) {
            List<PgTrigger> list = stale.next().getValue();
            if (list.isEmpty()) continue;
            list.removeIf(t -> t.getClonedFromTable() != null && !stillCloned(t));
            // A relation left holding nothing but copies that have gone carries no triggers at
            // all; an entry that was already empty is left as it was found.
            if (list.isEmpty()) stale.remove();
        }
        for (Map.Entry<String, List<PgTrigger>> entry : new ArrayList<>(triggers.entrySet())) {
            for (PgTrigger trigger : new ArrayList<>(entry.getValue())) {
                if (trigger.getClonedFromTable() != null || trigger.isForEachStatement()) continue;
                Table on = relationOf(trigger);
                if (on != null && !on.getPartitions().isEmpty()) cloneOntoPartitions(trigger, on);
            }
        }
    }

    /** Copy one trigger down the partition tree, each copy recording its immediate parent. */
    private void cloneOntoPartitions(PgTrigger trigger, Table on) {
        for (Table partition : on.getPartitions()) {
            cloneOntoPartitions(cloneOnto(trigger, on, partition), partition);
        }
    }

    /**
     * The partition's copy of a trigger, made if it is not there yet. One the partition already
     * carries under that name and event stands: PostgreSQL refuses a CREATE TRIGGER on the parent
     * that would collide with it, so a collision here is a trigger somebody wrote by hand.
     */
    private PgTrigger cloneOnto(PgTrigger trigger, Table parent, Table partition) {
        List<PgTrigger> here = triggers.computeIfAbsent(
                partition.getName().toLowerCase(), k -> new ArrayList<>());
        for (PgTrigger existing : here) {
            if (existing.getName().equalsIgnoreCase(trigger.getName())
                    && existing.getEvent() == trigger.getEvent()) {
                return existing;
            }
        }
        PgTrigger clone = new PgTrigger(trigger.getName(), trigger.getTiming(), trigger.getEvent(),
                partition.getName(), trigger.getFunctionName(), trigger.getUpdateColumns(),
                trigger.getNewTransitionTable(), trigger.getOldTransitionTable(),
                trigger.isForEachStatement(), trigger.getWhenClause(), trigger.isDeferrable(),
                trigger.isInitiallyDeferred(), trigger.getArgs());
        clone.setSchemaName(trigger.getSchemaName());
        clone.setEnabledState(trigger.getEnabledState());
        clone.setConstraintTrigger(trigger.isConstraintTrigger());
        clone.setConstraintRelation(trigger.getConstraintRelation());
        clone.setClonedFromTable(parent.getName());
        here.add(clone);
        return clone;
    }

    /** Whether a copy's relation is still a partition of the one that carries the original. */
    private boolean stillCloned(PgTrigger clone) {
        Table on = relationOf(clone);
        // A relation this cannot find is one being dropped; its triggers go with the relation.
        if (on == null) return true;
        Table parent = on.getPartitionParent();
        if (parent == null || !parent.getName().equalsIgnoreCase(clone.getClonedFromTable())) {
            return false;
        }
        List<PgTrigger> original = triggers.get(parent.getName().toLowerCase());
        if (original == null) return false;
        for (PgTrigger t : original) {
            if (t.getName().equalsIgnoreCase(clone.getName()) && t.getEvent() == clone.getEvent()) {
                return true;
            }
        }
        return false;
    }

    /** The relation a trigger sits on, in the schema the trigger recorded. */
    private Table relationOf(PgTrigger trigger) {
        if (trigger.getTableName() == null) return null;
        Schema schema = getSchema(
                trigger.getSchemaName() != null ? trigger.getSchemaName() : "public");
        return schema == null ? null : schema.getTable(trigger.getTableName());
    }

    public void removeTrigger(String name, String tableName) {
        List<PgTrigger> list = triggers.get(tableName.toLowerCase());
        if (list != null) {
            list.removeIf(t -> t.getName().equalsIgnoreCase(name));
        }
    }

    /**
     * Removes the triggers belonging to one schema's table. The registry is keyed by bare
     * name, so dropping s2.tt must not take the triggers on public.tt with it. Triggers
     * with no recorded schema are treated as belonging to the table being dropped.
     */
    public void removeTriggersForTable(String schemaName, String tableName) {
        String key = tableName.toLowerCase();
        List<PgTrigger> list = triggers.get(key);
        if (list == null) return;
        List<PgTrigger> keep = new ArrayList<>();
        for (PgTrigger t : list) {
            if (schemaName != null && t.getSchemaName() != null
                    && !t.getSchemaName().equalsIgnoreCase(schemaName)) {
                keep.add(t);
            }
        }
        if (keep.isEmpty()) triggers.remove(key);
        else triggers.put(key, keep);
    }

    /** Triggers on the named table, restricted to one schema where that is recorded. */
    public List<PgTrigger> getTriggersForTable(String schemaName, String tableName) {
        syncPartitionTriggers();
        List<PgTrigger> list = triggers.get(tableName.toLowerCase());
        if (list == null) return new ArrayList<>();
        if (schemaName == null) return new ArrayList<>(list);
        List<PgTrigger> out = new ArrayList<>();
        for (PgTrigger t : list) {
            if (t.getSchemaName() == null || t.getSchemaName().equalsIgnoreCase(schemaName)) out.add(t);
        }
        return out;
    }

    /** Re-registers triggers removed by a DROP TABLE that a rollback has undone. */
    public void restoreTriggersForTable(String tableName, List<PgTrigger> restored) {
        if (restored == null || restored.isEmpty()) return;
        String key = tableName.toLowerCase();
        List<PgTrigger> list = triggers.computeIfAbsent(key, k -> new ArrayList<>());
        for (PgTrigger t : restored) {
            if (!list.contains(t)) list.add(t);
        }
    }

    public void removeTriggersForTable(String tableName) {
        triggers.remove(tableName.toLowerCase());
    }

    // Event triggers
    public void addEventTrigger(PgEventTrigger et) {
        eventTriggers.put(et.getName().toLowerCase(), et);
    }

    public PgEventTrigger getEventTrigger(String name) {
        return eventTriggers.get(name.toLowerCase());
    }

    public void removeEventTrigger(String name) {
        eventTriggers.remove(name.toLowerCase());
    }

    public Map<String, PgEventTrigger> getAllEventTriggers() {
        return eventTriggers;
    }

    // Extended statistics
    public void addExtendedStatistic(ExtendedStatistic stat) {
        extendedStatistics.put(stat.getName().toLowerCase(), stat);
    }

    public ExtendedStatistic getExtendedStatistic(String name) {
        return extendedStatistics.get(name.toLowerCase());
    }

    public void removeExtendedStatistic(String name) {
        extendedStatistics.remove(name.toLowerCase());
    }

    public Map<String, ExtendedStatistic> getAllExtendedStatistics() {
        return extendedStatistics;
    }

    // Sequences
    // ---- Objects created by a transaction that has not committed yet ----

    /**
     * Relations, sequences and types a still-open transaction created, and who created them.
     *
     * <p>DDL is transactional: until the transaction commits, the object may never have existed.
     * Another session that can read it has read something it may have to un-read, and if the
     * creator rolls back it has read a relation that never existed at all.
     */
    private final Map<Object, Session> uncommittedObjects =
            java.util.Collections.synchronizedMap(new java.util.IdentityHashMap<Object, Session>());

    /** Record that {@code creator} made this object inside a transaction that is still open. */
    public void markUncommittedObject(Object object, Session creator) {
        if (object == null || creator == null || !creator.isInTransaction()) return;
        uncommittedObjects.put(object, creator);
    }

    /** Everything this session created is now permanent (or gone); stop hiding it. */
    public void clearUncommittedObjects(Session creator) {
        if (creator == null) return;
        for (Schema schema : schemas.values()) schema.forgetDroppedBy(creator);
        synchronized (uncommittedObjects) {
            uncommittedObjects.values().removeIf(s -> s == creator);
        }
    }

    /** False only for an object another session created in a transaction that is still open. */
    public boolean isObjectVisibleTo(Object object, Session viewer) {
        if (object == null || uncommittedObjects.isEmpty()) return true;
        Session creator = uncommittedObjects.get(object);
        return creator == null || creator == viewer || !creator.isInTransaction();
    }

    /**
     * True when this session made the object inside a transaction that is still open.
     *
     * <p>COPY FREEZE asks: it writes rows already visible to everybody, which is only safe while
     * nobody else can have seen the relation at all.
     */
    public boolean wasCreatedBy(Object object, Session creator) {
        if (object == null || creator == null) return false;
        return uncommittedObjects.get(object) == creator;
    }

    /**
     * The session whose statement is running on this thread, so a listing built deep inside the
     * engine can tell whose uncommitted objects it is allowed to show.
     *
     * <p>Threading a session parameter through every catalog builder would touch a hundred call
     * sites for a question that only ever has one answer per statement. It is set for the length
     * of a statement and restored afterwards, so nesting (a function body, a trigger) is safe.
     */
    private static final ThreadLocal<Session> CURRENT_VIEWER = new ThreadLocal<Session>();

    /** Bind the session running this thread's statement; returns the one it replaced. */
    /** The session whose statement is running on this thread, or null outside one. */
    static Session currentViewer() {
        return CURRENT_VIEWER.get();
    }

    public static Session bindViewer(Session viewer) {
        Session previous = CURRENT_VIEWER.get();
        if (viewer == null) CURRENT_VIEWER.remove(); else CURRENT_VIEWER.set(viewer);
        return previous;
    }

    /**
     * {@code tables} without the relations a still-open transaction of another session created.
     *
     * <p>Returns the map itself when there is nothing to hide, which is the ordinary case, so
     * neither a copy nor a wrapper is made for a statement no uncommitted DDL is running beside.
     */
    static Map<String, Table> visibleTables(Map<String, Table> tables) {
        return visible(tables);
    }

    /**
     * The same for any map of objects a statement can create: views, sequences, indexes, schemas,
     * types, functions. DDL is transactional whatever it creates, so every kind is hidden from
     * other sessions by the same rule and through the same one place.
     *
     * <p>Returns the map itself when there is nothing to hide, which is the ordinary case, so
     * neither a copy nor a wrapper is made for a statement no uncommitted DDL is running beside.
     */
    static <V> Map<String, V> visible(Map<String, V> objects) {
        Session viewer = CURRENT_VIEWER.get();
        if (viewer == null) return objects;
        Database db = viewer.getDatabase();
        if (db == null || db.uncommittedObjects.isEmpty()) return objects;
        Map<String, V> shown = null;
        for (Map.Entry<String, V> e : objects.entrySet()) {
            if (db.isObjectVisibleTo(e.getValue(), viewer)) continue;
            if (shown == null) shown = new java.util.LinkedHashMap<String, V>(objects);
            shown.remove(e.getKey());
        }
        return shown == null ? objects : shown;
    }

    /** A relation's columns without those an open transaction of another session added. */
    static List<Column> visibleColumns(List<Column> columns) {
        Session viewer = CURRENT_VIEWER.get();
        if (viewer == null) return columns;
        Database db = viewer.getDatabase();
        if (db == null || db.uncommittedObjects.isEmpty()) return columns;
        List<Column> shown = null;
        for (int i = 0; i < columns.size(); i++) {
            if (db.isObjectVisibleTo(columns.get(i), viewer)) continue;
            if (shown == null) shown = new ArrayList<Column>(columns);
            shown.remove(columns.get(i));
        }
        return shown == null ? columns : shown;
    }

    /** The same for a set of names an object kind is tracked by rather than a map. */
    static java.util.Set<String> visibleNames(java.util.Set<String> names,
                                              Map<String, ?> byName) {
        Session viewer = CURRENT_VIEWER.get();
        if (viewer == null) return names;
        Database db = viewer.getDatabase();
        if (db == null || db.uncommittedObjects.isEmpty()) return names;
        java.util.Set<String> shown = null;
        for (String name : names) {
            Object o = byName == null ? null : byName.get(name);
            if (o == null || db.isObjectVisibleTo(o, viewer)) continue;
            if (shown == null) shown = new java.util.LinkedHashSet<String>(names);
            shown.remove(name);
        }
        return shown == null ? names : shown;
    }

    // Sequences
    //
    // A sequence is a relation, so it belongs to one schema and two schemas may each hold one of
    // the same name. The map is keyed by both. Keying it by the bare name alone made
    // `DROP SEQUENCE other.s` destroy the sequence in the schema that really held it, made
    // `nextval('other.s')` advance it, and gave two tables in different schemas with a `serial`
    // column of the same name one shared counter.

    /** The key a sequence is stored under: its schema and its name, both folded. */
    static String seqKey(String schemaName, String name) {
        String schema = schemaName == null || schemaName.isEmpty() ? "public" : schemaName;
        return schema.toLowerCase() + "." + name.toLowerCase();
    }

    public void addSequence(Sequence sequence) {
        sequences.put(seqKey(sequence.getSchemaName(), sequence.getName()), sequence);
        markUncommittedObject(sequence, CURRENT_VIEWER.get());
    }

    /** The sequence of this name in this schema, and in no other. */
    public Sequence getSequence(String schemaName, String name) {
        if (name == null) return null;
        return visibleOne(sequences.get(seqKey(schemaName, name)));
    }

    /**
     * The object, or nothing when another session created it inside a transaction that is still
     * open. DDL is transactional, so until that transaction commits the object may never have
     * existed and a second session must be told the name reaches nothing -- not that it reaches
     * a relation of the wrong kind, which is what a lookup that skipped this reported.
     */
    private <T> T visibleOne(T object) {
        if (object == null || uncommittedObjects.isEmpty()) return object;
        Session viewer = CURRENT_VIEWER.get();
        if (viewer == null) return object;
        return isObjectVisibleTo(object, viewer) ? object : null;
    }

    /**
     * The sequence a written name refers to. A qualified name reaches the sequence in the schema it
     * names and nowhere else; a bare one, written by a caller that has no schema to offer, finds
     * one of that name preferring public.
     */
    public Sequence getSequence(String name) {
        if (name == null) return null;
        int dot = name.indexOf('.');
        if (dot > 0) return getSequence(name.substring(0, dot), name.substring(dot + 1));
        Sequence pub = visibleOne(sequences.get(seqKey("public", name)));
        if (pub != null) return pub;
        for (Map.Entry<String, Sequence> e : sequences.entrySet()) {
            if (!e.getValue().getName().equalsIgnoreCase(name)) continue;
            Sequence seq = visibleOne(e.getValue());
            if (seq != null) return seq;
        }
        return null;
    }

    /**
     * The sequence a name written in a statement refers to: the schema it names if it names one,
     * otherwise the first schema on the search path that holds a sequence of that name.
     */
    public Sequence resolveSequence(List<String> searchPath, String written) {
        if (written == null) return null;
        int dot = written.indexOf('.');
        if (dot > 0) return getSequence(written.substring(0, dot), written.substring(dot + 1));
        if (searchPath != null) {
            for (String schema : searchPath) {
                Sequence seq = getSequence(schema, written);
                if (seq != null) return seq;
            }
        }
        return null;
    }

    /**
     * The sequence a column default draws from. The default writes the name the way it was written
     * at CREATE time — bare for a {@code serial} column, which means the sequence in the table's
     * own schema, and qualified when it names another's.
     *
     * <p>A bare name that the table's own schema cannot answer still means something: it was
     * resolved through the search path when the column was created, so it can name a sequence
     * anywhere. Stopping at the table's schema made a table depending on another schema's
     * sequence insert nothing but a 22P02 on the default's own text.
     */
    public Sequence getSequenceFor(String tableSchema, String written) {
        if (written == null) return null;
        if (written.indexOf('.') > 0) return getSequence(written);
        Sequence own = getSequence(tableSchema, written);
        return own != null ? own : getSequence(written);
    }

    public void removeSequence(String name) {
        Sequence seq = getSequence(name);
        if (seq != null) sequences.remove(seqKey(seq.getSchemaName(), seq.getName()));
    }

    /** Drop the sequence of this name in this schema. */
    public void removeSequence(String schemaName, String name) {
        sequences.remove(seqKey(schemaName, name));
    }

    /** Remove every sequence belonging to the named schema (a temporary schema going away). */
    public void removeSequencesInSchema(String schemaName) {
        String prefix = schemaName.toLowerCase() + ".";
        sequences.keySet().removeIf(k -> k.startsWith(prefix));
    }

    public boolean hasSequence(String name) {
        return getSequence(name) != null;
    }

    /** Whether this schema holds a sequence of this name. */
    public boolean hasSequence(String schemaName, String name) {
        return getSequence(schemaName, name) != null;
    }

    // Rules

    /**
     * Joins the actions of a multi-action rule inside the single stored string. A NUL cannot
     * appear in SQL text, so splitting on it cannot cut a statement in half the way a semicolon
     * would.
     */
    public static final String RULE_ACTION_SEPARATOR = "\0";

    /** The actions of a stored rule body, in the order they were written. */
    public static String[] ruleActions(String storedBody) {
        return storedBody.split(RULE_ACTION_SEPARATOR, -1);
    }

    /**
     * One CREATE RULE, kept whole. PostgreSQL holds a rule under its own name, fires every rule an
     * event carries in rule-name order, and reads each rule's own WHERE to decide which rows its
     * actions run for. Keying the behaviour by relation and event instead left room for one rule
     * per pair, so a second CREATE RULE silently replaced the first and dropping or disabling any
     * one of them retired them all.
     *
     * <p>A rule belongs to a relation, and a relation belongs to a schema, so the rule carries the
     * schema of the relation it was written on. Two schemas may each hold a relation of the same
     * name and in PostgreSQL each carries its own rules; filing them under the bare name alone
     * fired both relations' rules for a write to either, and let a CREATE or a DROP of one take
     * the other's away.
     */
    public static final class StoredRule {
        private String schema;
        private String table;
        private String name;
        private final String event;
        private final boolean instead;
        private final String qualification;
        private final String body;
        private final long created;
        private String definition;
        private String enabledState = "O";
        private boolean disabled;

        StoredRule(String schema, String table, String name, String event, boolean instead,
                   String qualification, String body, long created) {
            this.schema = schema;
            this.table = table;
            this.name = name;
            this.event = event;
            this.instead = instead;
            this.qualification = qualification;
            this.body = body;
            this.created = created;
        }

        /** The schema of the relation the rule is on, which is the schema the rule is in. */
        public String getSchema() { return schema; }

        /** The relation the rule is on. */
        public String getTable() { return table; }

        public String getName() { return name; }

        public String getEvent() { return event; }

        public boolean isInstead() { return instead; }

        /** The rule's WHERE, or null when it fires for every row the statement touches. */
        public String getQualification() { return qualification; }

        /** The rule's actions, joined by {@link Database#RULE_ACTION_SEPARATOR}. */
        public String getBody() { return body; }

        /** The rule as {@code pg_get_ruledef} writes it, or null until it has been described. */
        public String getDefinition() { return definition; }

        /**
         * Which of PostgreSQL's four firing modes the rule is in, as {@code pg_rewrite.ev_enabled}
         * spells them: {@code O} origin (the default), {@code D} disabled, {@code R} replica,
         * {@code A} always. Only {@code O} and {@code A} fire in an ordinary session -- a replica
         * rule waits for a session in replica mode -- so the behaviour follows the code rather
         * than being set apart from it.
         */
        public String getEnabledState() { return enabledState; }

        public boolean isDisabled() { return disabled; }

        /** Whether the rule performs no action at all: DO INSTEAD NOTHING and DO ALSO NOTHING. */
        public boolean isNothing() { return body == null || body.isEmpty(); }
    }

    /**
     * Whether a rule filed under a relation's name is one a lookup in {@code schema} reaches. A
     * caller with no schema to go on reaches all of them, which is what a lookup by the bare name
     * did for every one of them.
     */
    private static boolean ruleInSchema(StoredRule rule, String schema) {
        return schema == null || rule.schema == null || rule.schema.equalsIgnoreCase(schema);
    }

    /**
     * Register a rule under its own name, replacing one of the same name as CREATE OR REPLACE RULE
     * does. The list is kept in rule-name order because that is the order PostgreSQL fires them in.
     * Only a rule on the same relation is replaced: another schema's relation of that name keeps
     * whatever it was written with.
     */
    public void addRule(String schema, String ruleName, String table, String event, boolean instead,
                        String body, String qualification) {
        String key = table.toLowerCase();
        List<StoredRule> rebuilt = new ArrayList<>();
        List<StoredRule> existing = relationRules.get(key);
        if (existing != null) {
            for (StoredRule r : existing) {
                if (!r.getName().equalsIgnoreCase(ruleName) || !ruleInSchema(r, schema)) {
                    rebuilt.add(r);
                }
            }
        }
        rebuilt.add(new StoredRule(schema, table, ruleName, event == null ? "" : event.toUpperCase(),
                instead, qualification, body, ruleCreations.incrementAndGet()));
        rebuilt.sort((a, b) -> a.getName().compareToIgnoreCase(b.getName()));
        relationRules.put(key, rebuilt);
    }

    /**
     * The rules this relation carries for the event, in the order they fire. A disabled rule keeps
     * its place in the catalogs but does not fire, so it is left out.
     */
    public List<StoredRule> getRules(String schema, String table, String event) {
        List<StoredRule> out = new ArrayList<>();
        List<StoredRule> list = relationRules.get(table.toLowerCase());
        if (list == null) return out;
        String want = event.toUpperCase();
        for (StoredRule r : list) {
            if (!r.isDisabled() && want.equals(r.getEvent()) && ruleInSchema(r, schema)) out.add(r);
        }
        return out;
    }

    private StoredRule findRule(String schema, String ruleName, String table) {
        List<StoredRule> list = relationRules.get(table.toLowerCase());
        if (list == null) return null;
        for (StoredRule r : list) {
            if (r.getName().equalsIgnoreCase(ruleName) && ruleInSchema(r, schema)) return r;
        }
        return null;
    }

    public boolean hasRule(String schema, String ruleName, String table) {
        return findRule(schema, ruleName, table) != null;
    }

    /**
     * ALTER RULE ... RENAME TO: the rule keeps its event and its action, under a new name.
     * Reporting success without re-keying leaves the rule answering only to a name that is
     * no longer written anywhere, so DROP RULE on the new name cannot find it. The name also
     * decides when the rule fires relative to the others, so the list is put back in order.
     */
    public void renameRule(String schema, String ruleName, String table, String newName) {
        StoredRule rule = findRule(schema, ruleName, table);
        if (rule == null) return;
        // What a rule's actions name is filed under the rule's name too, so it is re-filed here or
        // the drop of a relation the renamed rule writes to stops being refused.
        RuleDependency dependency =
                ruleDependencies.remove(ruleDependencyKey(ruleName, rule.schema, table));
        rule.name = newName;
        if (dependency != null) {
            ruleDependencies.put(ruleDependencyKey(newName, rule.schema, table),
                    new RuleDependency(newName, rule.schema, table, dependency.relations));
        }
        List<StoredRule> resorted = new ArrayList<>(relationRules.get(table.toLowerCase()));
        resorted.sort((a, b) -> a.getName().compareToIgnoreCase(b.getName()));
        relationRules.put(table.toLowerCase(), resorted);
    }

    /**
     * The constraint trigger of this name anywhere in the database, or null. A constraint trigger
     * is a constraint as well as a trigger -- SET CONSTRAINTS names it, and pg_constraint holds a
     * row for it -- but it is kept here rather than among a table's stored constraints, so a
     * lookup that searches only those cannot find it.
     */
    public PgTrigger findConstraintTrigger(String name) {
        for (List<PgTrigger> onOneRelation : triggers.values()) {
            for (PgTrigger t : onOneRelation) {
                if (t.isConstraintTrigger() && t.getName().equalsIgnoreCase(name)) return t;
            }
        }
        return null;
    }

    // What each rule's actions name, so a DROP of one of those relations can be refused
    private final Map<String, RuleDependency> ruleDependencies = new ConcurrentHashMap<>();

    /**
     * The relations one rule's actions name, each under the schema that holds it: PostgreSQL prints
     * the rule's own name and the relation it is on when it refuses to drop something the rule
     * needs, and it records the relation itself rather than the name it was written under, so
     * another schema's relation of the same name is nothing to the rule.
     */
    private static final class RuleDependency {
        private final String ruleName;
        private final String schema;
        private final String table;
        private final List<String> relations;

        RuleDependency(String ruleName, String schema, String table, List<String> relations) {
            this.ruleName = ruleName;
            this.schema = schema;
            this.table = table;
            this.relations = relations;
        }

        boolean names(String relationSchema, String relation) {
            String wanted = RelationNamespace.bareName(relation);
            for (String r : relations) {
                if (!RelationNamespace.bareName(r).equalsIgnoreCase(wanted)) continue;
                // A relation recorded without a schema was written before one could be worked out
                // for it, and answers to the bare name as it always did.
                int dot = r.lastIndexOf('.');
                if (dot <= 0 || relationSchema == null) return true;
                if (r.substring(0, dot).equalsIgnoreCase(relationSchema)) return true;
            }
            return false;
        }
    }

    private static String ruleDependencyKey(String ruleName, String schema, String table) {
        return ruleName.toLowerCase() + ":" + (schema == null ? "" : schema.toLowerCase())
                + "." + table.toLowerCase();
    }

    /**
     * Record which relations a rule's actions name. PostgreSQL writes a pg_depend row for each of
     * them, and that is what makes DROP TABLE refuse while a rule that writes to the table is
     * still there: without it the rule outlives its target and the ruled relation cannot be
     * written to at all.
     */
    public void addRuleDependencies(String schema, String ruleName, String table,
                                    List<String> relations) {
        String key = ruleDependencyKey(ruleName, schema, table);
        if (relations == null || relations.isEmpty()) {
            ruleDependencies.remove(key);
            return;
        }
        ruleDependencies.put(key,
                new RuleDependency(ruleName, schema, table, new ArrayList<>(relations)));
    }

    /**
     * The rules whose actions name this relation, as {rule name, relation, schema of the
     * relation}: what a DROP of it is refused for, and what a CASCADE takes with it. A rule whose
     * own relation has gone is left out, because it can no longer fire. They come back in the
     * order they were written, which is the order PostgreSQL reports them in.
     */
    public List<String[]> rulesDependingOn(String schema, String table) {
        List<StoredRule> found = new ArrayList<>();
        for (RuleDependency dep : ruleDependencies.values()) {
            if (!dep.names(schema, table)) continue;
            StoredRule rule = findRule(dep.schema, dep.ruleName, dep.table);
            if (rule == null || !relationStillThere(dep.schema, dep.table)) continue;
            found.add(rule);
        }
        found.sort((a, b) -> Long.compare(a.created, b.created));
        List<String[]> out = new ArrayList<>();
        for (StoredRule r : found) out.add(new String[]{ r.name, r.table, r.schema });
        return out;
    }

    /** Whether this schema still holds a relation of this bare name, of any kind. */
    private boolean relationStillThere(String schema, String name) {
        for (Schema s : schemas.values()) {
            if (schema != null && !s.getName().equalsIgnoreCase(schema)) continue;
            if (s.getTable(name) != null) return true;
        }
        return (schema == null ? getView(name) : getView(schema, name)) != null;
    }

    /** DROP RULE: only the named rule goes, and the others on the relation go on firing. */
    public void removeRule(String schema, String ruleName, String table) {
        for (Map.Entry<String, RuleDependency> e : new ArrayList<>(ruleDependencies.entrySet())) {
            RuleDependency dep = e.getValue();
            if (dep.ruleName.equalsIgnoreCase(ruleName) && dep.table.equalsIgnoreCase(table)
                    && (schema == null || dep.schema == null
                        || dep.schema.equalsIgnoreCase(schema))) {
                ruleDependencies.remove(e.getKey());
            }
        }
        String key = table.toLowerCase();
        List<StoredRule> existing = relationRules.get(key);
        if (existing == null) return;
        List<StoredRule> kept = new ArrayList<>();
        for (StoredRule r : existing) {
            if (!r.getName().equalsIgnoreCase(ruleName) || !ruleInSchema(r, schema)) kept.add(r);
        }
        relationRules.put(key, kept);
    }

    /**
     * Park or restore a named rule for ALTER TABLE ... DISABLE/ENABLE RULE. Only that rule stops
     * firing; it stays registered so it keeps its place in the catalogs and can be switched back on.
     *
     * @return false when no rule of that name is defined on the relation
     */
    public boolean setRuleEnabled(String schema, String ruleName, String table, boolean enabled) {
        StoredRule rule = findRule(schema, ruleName, table);
        if (rule == null) return false;
        rule.disabled = !enabled;
        return true;
    }

    /**
     * Record which of PostgreSQL's four firing modes a rule is in, as {@code pg_rewrite.ev_enabled}
     * spells them.
     *
     * @return false when no rule of that name is defined on the relation
     */
    public boolean setRuleEnabledState(String schema, String ruleName, String table, String state) {
        StoredRule rule = findRule(schema, ruleName, table);
        if (rule == null) return false;
        rule.disabled = !("O".equals(state) || "A".equals(state));
        rule.enabledState = state;
        return true;
    }

    /**
     * Remember a rule well enough for the catalogs to describe it: the text {@code pg_get_ruledef}
     * writes goes on the rule itself, beside the event it fires on and whether it replaces the
     * statement, so nothing has to be parsed back out of the text.
     */
    public void addRuleDefinition(String schema, String ruleName, String table, String definition) {
        // pg_class.relhasrules is documented as "has (or once had) rules": PostgreSQL only clears
        // the flag at VACUUM, so dropping the rule leaves it standing.
        rules.put(everHadKey(schema, table), "t");
        StoredRule rule = findRule(schema, ruleName, table);
        if (rule != null) rule.definition = definition;
    }

    /**
     * Every rule written with CREATE RULE, in the order they were written -- which is the order
     * PostgreSQL's catalogs hand them back in. A rule is described by the relation it is on, so
     * two relations may each carry one of the same name and both are here.
     */
    public List<StoredRule> getRuleEntries() {
        List<StoredRule> all = new ArrayList<>();
        for (List<StoredRule> onOneRelation : relationRules.values()) all.addAll(onOneRelation);
        all.sort((a, b) -> Long.compare(a.created, b.created));
        return all;
    }

    private static String everHadKey(String schema, String table) {
        return "everhad:" + (schema == null ? "" : schema.toLowerCase())
                + "." + table.toLowerCase();
    }

    /** Whether a relation has, or once had, a rule -- what {@code relhasrules} reports. */
    public boolean everHadRules(String schema, String table) {
        if (schema != null) return rules.containsKey(everHadKey(schema, table));
        String underAnySchema = "." + table.toLowerCase();
        for (String key : rules.keySet()) {
            if (key.startsWith("everhad:") && key.endsWith(underAnySchema)) return true;
        }
        return false;
    }

    /**
     * A renamed or moved relation takes its rules with it. PostgreSQL records the relation a rule
     * is on as an OID, so neither a rename nor a move to another schema is anything to the rule;
     * memgres files a rule under the relation's name and schema, so both are rewritten here or the
     * rule stops firing, drops out of {@code pg_rules}, and cannot be dropped by name any more.
     *
     * @see ObjectIdentity#relationRenamed
     */
    void retargetRules(String oldSchema, String oldTable, String newSchema, String newTable) {
        String from = oldTable.toLowerCase();
        String to = newTable.toLowerCase();
        String everHad = rules.remove(everHadKey(oldSchema, oldTable));
        if (everHad != null) rules.put(everHadKey(newSchema, newTable), everHad);
        List<StoredRule> underName = relationRules.get(from);
        if (underName == null) return;
        List<StoredRule> moved = new ArrayList<>();
        List<StoredRule> stay = new ArrayList<>();
        for (StoredRule r : underName) {
            if (ruleInSchema(r, oldSchema)) moved.add(r); else stay.add(r);
        }
        for (StoredRule r : moved) {
            // What the rule's actions name is filed under the relation the rule is on as well.
            RuleDependency dependency =
                    ruleDependencies.remove(ruleDependencyKey(r.name, r.schema, r.table));
            r.schema = newSchema;
            r.table = newTable;
            if (dependency != null) {
                ruleDependencies.put(ruleDependencyKey(r.name, newSchema, newTable),
                        new RuleDependency(r.name, newSchema, newTable, dependency.relations));
            }
        }
        // A move to another schema leaves the rules where they are: they are filed under the
        // relation's bare name, which has not changed.
        if (from.equals(to)) return;
        if (stay.isEmpty()) relationRules.remove(from); else relationRules.put(from, stay);
        List<StoredRule> atTarget = relationRules.get(to);
        if (atTarget != null) moved.addAll(atTarget);
        moved.sort((a, b) -> a.getName().compareToIgnoreCase(b.getName()));
        relationRules.put(to, moved);
    }

    /**
     * A renamed relation takes its triggers with it. They are registered under the relation's bare
     * name and each one records the name it fires for, so a rename leaves them firing for a
     * relation that is gone and reporting a relation OID of zero in {@code pg_trigger}.
     *
     * @see ObjectIdentity#relationRenamed
     */
    void retargetTriggers(String oldTable, String newTable, String newSchema) {
        String from = oldTable.toLowerCase();
        String to = newTable.toLowerCase();
        List<PgTrigger> list = triggers.remove(from);
        if (list == null || list.isEmpty()) return;
        List<PgTrigger> rebuilt = new ArrayList<>();
        for (PgTrigger t : list) {
            // The table name is fixed at construction, so the trigger is rebuilt around the name
            // it now fires for; everything else about it is carried over unchanged.
            PgTrigger moved = new PgTrigger(t.getName(), t.getTiming(), t.getEvent(), newTable,
                    t.getFunctionName(), t.getUpdateColumns(), t.getNewTransitionTable(),
                    t.getOldTransitionTable(), t.isForEachStatement(), t.getWhenClause(),
                    t.isDeferrable(), t.isInitiallyDeferred(), t.getArgs());
            moved.setSchemaName(newSchema != null ? newSchema : t.getSchemaName());
            moved.setEnabledState(t.getEnabledState());
            moved.setConstraintTrigger(t.isConstraintTrigger());
            moved.setConstraintRelation(t.getConstraintRelation());
            rebuilt.add(moved);
        }
        List<PgTrigger> atTarget = triggers.get(to);
        if (atTarget != null) rebuilt.addAll(0, atTarget);
        triggers.put(to, rebuilt);
    }

    /**
     * Drop the rules a relation carries, for a relation that is being dropped.
     *
     * <p>PostgreSQL records a rule as part of the relation it sits on, so dropping the relation
     * deletes its rules with it -- no CASCADE is needed and none of them is named in the cascade
     * report, because nothing outside the relation depends on them. Leaving them registered kept
     * them in {@code pg_rules} describing a relation that was no longer there.
     */
    public void dropRulesOn(String schema, String table) {
        List<StoredRule> carried = relationRules.get(table.toLowerCase());
        if (carried != null) {
            // Each goes through the ordinary drop, so what the rule depended on is forgotten too.
            for (StoredRule r : new ArrayList<>(carried)) {
                if (ruleInSchema(r, schema)) removeRule(schema, r.getName(), table);
            }
        }
        clearRuleHistory(schema, table);
    }

    /**
     * Drop every rule that goes when this relation goes: the ones written on it, and the ones
     * written on some other relation whose actions name it.
     *
     * <p>PostgreSQL records what a rule's actions name as a dependency of the rule itself, which is
     * why a DROP of one of those relations is refused without CASCADE and deletes the rule with
     * CASCADE. A rule left standing reached for a relation that was no longer there, so the
     * relation the rule sits on could not be written to at all -- a relation in one schema became
     * unwritable because some other schema had been dropped, and the write that found out lost its
     * row. The relation a rule sits on belongs to a schema, so only the rules that named this one
     * go; another schema's relation of the same name is nothing to them.
     */
    public void dropRulesGoingWith(String schema, String table) {
        for (String[] dependent : rulesDependingOn(schema, table)) {
            removeRule(dependent[2], dependent[0], dependent[1]);
        }
        dropRulesOn(schema, table);
    }

    /**
     * Everything some rules amount to, so that a DROP which is rolled back brings them back with
     * it. PostgreSQL deletes the rules along with what they belong to and restores both together;
     * keeping only the relation left it standing without the rules it was written with, and they
     * could not even be dropped by name afterwards.
     */
    public static final class RuleSnapshot {
        private final List<StoredRule> carried;
        private final Map<String, String> catalogued;
        private final Map<String, RuleDependency> dependencies;

        RuleSnapshot(List<StoredRule> carried, Map<String, String> catalogued,
                     Map<String, RuleDependency> dependencies) {
            this.carried = carried;
            this.catalogued = catalogued;
            this.dependencies = dependencies;
        }
    }

    /** What {@link #dropRulesOn} is about to take away, or null where the relation carries none. */
    public RuleSnapshot snapshotRulesOn(String schema, String table) {
        List<StoredRule> underName = relationRules.get(table.toLowerCase());
        if (underName == null) return null;
        List<StoredRule> carried = new ArrayList<>();
        for (StoredRule r : underName) {
            if (ruleInSchema(r, schema)) carried.add(r);
        }
        return snapshotOf(carried);
    }

    /**
     * What {@link #dropRulesGoingWith} is about to take away, or null where there is nothing.
     *
     * <p>PostgreSQL rolls a catalogue change back whole, so a drop that took both the rules a
     * relation carried and the rules elsewhere that named it has to bring both back together.
     */
    public RuleSnapshot snapshotRulesGoingWith(String schema, String table) {
        List<StoredRule> carried = new ArrayList<>();
        for (String[] dependent : rulesDependingOn(schema, table)) {
            StoredRule rule = findRule(dependent[2], dependent[0], dependent[1]);
            if (rule != null && !carried.contains(rule)) carried.add(rule);
        }
        List<StoredRule> underName = relationRules.get(table.toLowerCase());
        if (underName != null) {
            for (StoredRule r : underName) {
                if (ruleInSchema(r, schema) && !carried.contains(r)) carried.add(r);
            }
        }
        return snapshotOf(carried);
    }

    /**
     * Put a schema back exactly as it was, for a DROP SCHEMA that has been rolled back.
     *
     * <p>The schema object holds its relations and what another session's still-open transaction
     * may see of them, so the rollback restores the object the drop took away rather than building
     * a fresh one under the name and losing that with it.
     */
    public void restoreSchema(Schema schema) {
        if (schema != null) schemas.put(schema.getName(), schema);
    }

    /**
     * What one rule amounts to. A rule that merely writes to the relation being dropped sits on a
     * relation of its own, so it is in no relation's snapshot; a CASCADE takes it away and a
     * rollback has to find it again.
     */
    public RuleSnapshot snapshotRule(String schema, String ruleName, String table) {
        StoredRule rule = findRule(schema, ruleName, table);
        return rule == null ? null : snapshotOf(Cols.listOf(rule));
    }

    private RuleSnapshot snapshotOf(List<StoredRule> carried) {
        if (carried.isEmpty()) return null;
        Map<String, String> catalogued = new LinkedHashMap<>();
        Map<String, RuleDependency> dependencies = new LinkedHashMap<>();
        for (StoredRule r : carried) {
            String everHad = rules.get(everHadKey(r.schema, r.table));
            if (everHad != null) catalogued.put(everHadKey(r.schema, r.table), everHad);
            String key = ruleDependencyKey(r.name, r.schema, r.table);
            RuleDependency dependency = ruleDependencies.get(key);
            if (dependency != null) dependencies.put(key, dependency);
        }
        return new RuleSnapshot(new ArrayList<>(carried), catalogued, dependencies);
    }

    /** Puts back what a DROP took away, for a DROP that has been rolled back. */
    public void restoreRules(RuleSnapshot snapshot) {
        if (snapshot == null) return;
        for (StoredRule r : snapshot.carried) {
            String key = r.table.toLowerCase();
            List<StoredRule> rebuilt = new ArrayList<>();
            List<StoredRule> existing = relationRules.get(key);
            if (existing != null) {
                for (StoredRule there : existing) {
                    if (there != r && (!there.getName().equalsIgnoreCase(r.name)
                            || !ruleInSchema(there, r.schema))) {
                        rebuilt.add(there);
                    }
                }
            }
            rebuilt.add(r);
            rebuilt.sort((a, b) -> a.getName().compareToIgnoreCase(b.getName()));
            relationRules.put(key, rebuilt);
        }
        rules.putAll(snapshot.catalogued);
        ruleDependencies.putAll(snapshot.dependencies);
    }

    /**
     * Forget every rule this relation carried, for a relation newly created under its name.
     * Dropping a relation takes its rules with it in PostgreSQL, so a table created under the same
     * name afterwards starts with none and with relhasrules false. Another schema's relation of
     * that name is a different relation and keeps its own.
     */
    public void clearRuleHistory(String schema, String table) {
        rules.remove(everHadKey(schema, table));
        String key = table.toLowerCase();
        List<StoredRule> gone = relationRules.get(key);
        if (gone == null) return;
        List<StoredRule> kept = new ArrayList<>();
        for (StoredRule r : gone) {
            if (!ruleInSchema(r, schema)) kept.add(r);
        }
        if (kept.isEmpty()) relationRules.remove(key); else relationRules.put(key, kept);
    }

    /**
     * The name of the first rule this relation carries for the event, or null when it carries none.
     * A statement PostgreSQL will not rewrite asks only whether there is one; everything that fires
     * rules reads them through {@link #getRules}, which hands back each rule's own WHERE.
     */
    public String getRule(String schema, String table, String event) {
        List<StoredRule> list = relationRules.get(table.toLowerCase());
        if (list == null) return null;
        String want = event.toUpperCase();
        for (StoredRule r : list) {
            if (want.equals(r.getEvent()) && ruleInSchema(r, schema)) return r.getName();
        }
        return null;
    }

    // Comments. The name half of a key is schema-qualified for everything a schema holds, so
    // COMMENT ON TABLE a.t and COMMENT ON TABLE b.t are two comments rather than one.
    public void addComment(String objectType, String objectName, String comment) {
        if (comment == null) {
            comments.remove(objectType + ":" + objectName);
        } else {
            comments.put(objectType + ":" + objectName, comment);
        }
    }

    public String getComment(String objectType, String objectName) {
        return comments.get(objectType + ":" + objectName);
    }

    /**
     * The key a comment on an object called {@code name} in {@code schema} is stored under.
     * Schemas, roles, databases and extensions are not held by a schema, so they key by name
     * alone; {@link #globalCommentType} says which those are.
     */
    public static String commentKey(String schema, String name) {
        if (name == null) return null;
        String s = schema == null || schema.isEmpty() ? "public" : schema.toLowerCase();
        return s + "." + name.toLowerCase();
    }

    /** Whether an object of this kind lives outside every schema, so its comment keys by name. */
    public static boolean globalCommentType(String objectType) {
        if (objectType == null) return false;
        String t = objectType.toLowerCase();
        return t.equals("schema") || t.equals("database") || t.equals("role") || t.equals("user")
                || t.equals("extension") || t.equals("tablespace") || t.equals("language")
                || t.equals("procedural language") || t.equals("foreign data wrapper")
                || t.equals("server") || t.equals("publication") || t.equals("subscription")
                || t.equals("event trigger") || t.equals("access method")
                || t.equals("large object") || t.equals("cast") || t.equals("transform");
    }

    /**
     * Carry a comment from one key to another. A rename or a SET SCHEMA leaves the same object
     * with a different key, and what was said about it is said about it still.
     */
    public void moveComment(String objectType, String fromKey, String toKey) {
        if (fromKey == null || toKey == null || fromKey.equals(toKey)) return;
        String text = comments.remove(objectType + ":" + fromKey);
        if (text != null) comments.put(objectType + ":" + toKey, text);
    }

    /** Carry every comment whose key starts with {@code fromPrefix} over to {@code toPrefix}. */
    public void moveCommentsUnder(String objectType, String fromPrefix, String toPrefix) {
        if (fromPrefix == null || toPrefix == null || fromPrefix.equals(toPrefix)) return;
        String from = objectType + ":" + fromPrefix;
        List<String> keys = new ArrayList<>(comments.keySet());
        for (String k : keys) {
            if (k.startsWith(from)) {
                String text = comments.remove(k);
                if (text != null) {
                    comments.put(objectType + ":" + toPrefix + k.substring(from.length()), text);
                }
            }
        }
    }

    public Map<String, String> getComments() {
        return comments;
    }

    // Indexes
    //
    // An index is a relation, so it lives in a schema — the schema of the table it indexes — and
    // two schemas may each hold one of the same name. Every index map is keyed by
    // {@code schema.name}: keyed by the bare name alone, a second CREATE INDEX of a name already
    // used in another schema was refused, and ALTER INDEX other.i renamed whichever one it found.

    /**
     * The key an index of this name in this schema is stored under. PostgreSQL folds an unquoted
     * identifier and keeps a quoted one, so {@code "MixedCase"} and {@code mixedcase} are two
     * indexes and the name is stored exactly as it was written; the schema half is folded because
     * a schema name always is by the time it reaches here.
     */
    static String idxKey(String schemaName, String name) {
        String schema = schemaName == null || schemaName.isEmpty() ? "public" : schemaName;
        return schema.toLowerCase() + "." + name;
    }

    /** The name half of an index key. */
    static String idxName(String key) {
        int dot = key.indexOf('.');
        return dot > 0 ? key.substring(dot + 1) : key;
    }

    /** The schema half of an index key. */
    static String idxSchema(String key) {
        int dot = key.indexOf('.');
        return dot > 0 ? key.substring(0, dot) : "public";
    }

    /**
     * The key a written index name resolves to. A qualified name reaches the index in the schema
     * it names; a bare one, from a caller with no schema to offer, finds the one index of that
     * name wherever it lives. Names that reach here already folded still resolve.
     */
    private String ixKey(String name) {
        if (name == null) return null;
        int dot = name.indexOf('.');
        if (dot > 0) {
            String exact = idxKey(name.substring(0, dot), name.substring(dot + 1));
            if (indexColumns.containsKey(exact) || indexTableNames.containsKey(exact)) return exact;
            String ci = matchKey(exact, true);
            return ci != null ? ci : exact;
        }
        String bare = matchKey(name, false);
        if (bare != null) return bare;
        String folded = matchKey(name, true);
        return folded != null ? folded : idxKey("public", name);
    }

    /**
     * The stored key whose name half matches, exactly or after folding. A qualified {@code want}
     * has to agree on the schema too; a bare one answers from whichever schema holds it.
     */
    private String matchKey(String want, boolean fold) {
        boolean qualified = want.indexOf('.') > 0;
        for (String key : indexColumns.keySet()) {
            String candidate = qualified ? key : idxName(key);
            if (fold ? candidate.equalsIgnoreCase(want) : candidate.equals(want)) return key;
        }
        for (String key : indexTableNames.keySet()) {
            String candidate = qualified ? key : idxName(key);
            if (fold ? candidate.equalsIgnoreCase(want) : candidate.equals(want)) return key;
        }
        return null;
    }

    /**
     * The index a name written in a statement refers to, as {@code schema.name}, or null. A
     * qualified name is answered by the schema it names and by no other; a bare one by the first
     * schema on the search path that holds an index of that name.
     */
    public String resolveIndexName(List<String> searchPath, String written) {
        if (written == null) return null;
        int dot = written.indexOf('.');
        if (dot > 0) {
            String key = idxKey(written.substring(0, dot), written.substring(dot + 1));
            return indexColumns.containsKey(key) ? key : null;
        }
        if (searchPath != null) {
            for (String schema : searchPath) {
                String key = idxKey(schema, written);
                if (indexColumns.containsKey(key)) return key;
            }
        }
        return null;
    }

    // Index metadata (for USING INDEX lookups)
    public void addIndex(String schemaName, String name, List<String> columns) {
        indexColumns.put(idxKey(schemaName, name), columns);
        // The column list is what every index listing is built from, so hiding it hides the index.
        markUncommittedObject(columns, CURRENT_VIEWER.get());
    }

    public void addIndexMeta(String schemaName, String name, String tableName, boolean isUnique) {
        String key = idxKey(schemaName, name);
        indexTableNames.put(key, tableName);
        indexUniqueFlags.put(key, isUnique);
    }

    /** Whether this schema holds an index of exactly this name. */
    public boolean hasIndex(String schemaName, String name) {
        return name != null && indexColumns.containsKey(idxKey(schemaName, name));
    }

    /** Drop the index of this name in this schema. */
    public void removeIndex(String schemaName, String name) {
        removeIndexKey(idxKey(schemaName, name));
    }

    public void addIndexMeta(String schemaName, String name, String tableName, boolean isUnique,
                             String method, String whereClause) {
        String key = idxKey(schemaName, name);
        indexTableNames.put(key, tableName);
        indexUniqueFlags.put(key, isUnique);
        if (method != null) indexMethods.put(key, method);
        if (whereClause != null) indexWhereClauses.put(key, whereClause);
    }

    public void setIndexColumnOptions(String schemaName, String name, List<String> options) {
        if (options != null) indexColumnOptions.put(idxKey(schemaName, name), options);
    }

    public List<String> getIndexColumnOptions(String name) {
        return indexColumnOptions.get(ixKey(name));
    }

    public void setIndexIncludeColumns(String schemaName, String name, List<String> cols) {
        if (cols != null && !cols.isEmpty()) indexIncludeColumns.put(idxKey(schemaName, name), cols);
    }

    public List<String> getIndexIncludeColumns(String name) {
        return indexIncludeColumns.get(ixKey(name));
    }

    public void setIndexNullsNotDistinct(String schemaName, String name, boolean value) {
        if (value) indexNullsNotDistinct.put(idxKey(schemaName, name), true);
    }

    public boolean isIndexNullsNotDistinct(String name) {
        return indexNullsNotDistinct.getOrDefault(ixKey(name), false);
    }

    public void setIndexParent(String childIndex, String parentIndex) {
        indexParentIndex.put(ixKey(childIndex), ixKey(parentIndex));
    }

    public Map<String, String> getIndexParentMap() {
        return indexParentIndex;
    }

    public String getIndexMethod(String name) {
        return indexMethods.getOrDefault(ixKey(name), "btree");
    }

    public String getIndexWhereClause(String name) {
        return indexWhereClauses.get(ixKey(name));
    }

    public String getIndexTable(String name) {
        return indexTableNames.get(ixKey(name));
    }

    /** Returns all index → table name mappings (used by CLUSTER to find clustered indexes for a table). */
    public Map<String, String> getIndexTableNames() {
        return indexTableNames;
    }

    public boolean isUniqueIndex(String name) {
        return indexUniqueFlags.getOrDefault(ixKey(name), false);
    }

    public List<String> getIndexColumns(String name) {
        return indexColumns.get(ixKey(name));
    }

    public Map<String, String> getIndexReloptions(String name) {
        return indexReloptions.get(ixKey(name));
    }

    public void setIndexReloptions(String name, Map<String, String> opts) {
        indexReloptions.put(ixKey(name), opts);
    }

    public void removeIndexReloptions(String name) {
        indexReloptions.remove(ixKey(name));
    }

    /**
     * True when an index anywhere answers to exactly this name. Kept for callers that have no
     * schema to offer — asking "is this name a relation at all". A CREATE that has to decide
     * whether a name is free asks {@link #hasIndex(String, String)} about one schema instead.
     */
    public boolean hasIndex(String name) {
        if (name == null) return false;
        if (name.indexOf('.') > 0) return indexColumns.containsKey(ixKey(name));
        for (String key : indexColumns.keySet()) {
            if (idxName(key).equals(name)) return true;
        }
        return false;
    }

    public void removeIndex(String name) {
        removeIndexKey(ixKey(name));
    }

    private void removeIndexKey(String key) {
        indexColumns.remove(key);
        indexTableNames.remove(key);
        indexUniqueFlags.remove(key);
        indexWhereClauses.remove(key);
        indexMethods.remove(key);
        indexReloptions.remove(key);
        indexColumnOptions.remove(key);
        indexIncludeColumns.remove(key);
        indexNullsNotDistinct.remove(key);
    }

    /**
     * Move an index into another schema, keeping its name. An index lives where its table does,
     * so this happens when the table is moved and never on its own.
     */
    public void moveIndex(String oldKey, String newSchema, String newOwnerTable) {
        String newKey = idxKey(newSchema, idxName(oldKey));
        rekeyIndex(oldKey, newKey);
        if (newOwnerTable != null) indexTableNames.put(newKey, newOwnerTable);
    }

    /**
     * Rename an index: re-key across all index maps and update the schema registry. A rename
     * never moves the index out of its schema, so the new key keeps the old key's schema.
     */
    public void renameIndex(String oldName, String newName) {
        String oldKey = ixKey(oldName);
        String newKey = idxKey(idxSchema(oldKey), idxName(newName));
        rekeyIndex(oldKey, newKey);
    }

    /** Move every index map's entry from one key to another. */
    private void rekeyIndex(String oldKey, String newKey) {
        if (oldKey.equals(newKey)) return;
        List<String> cols = indexColumns.remove(oldKey);
        if (cols != null) indexColumns.put(newKey, cols);
        String tbl = indexTableNames.remove(oldKey);
        if (tbl != null) indexTableNames.put(newKey, tbl);
        Boolean uniq = indexUniqueFlags.remove(oldKey);
        if (uniq != null) indexUniqueFlags.put(newKey, uniq);
        String where = indexWhereClauses.remove(oldKey);
        if (where != null) indexWhereClauses.put(newKey, where);
        String method = indexMethods.remove(oldKey);
        if (method != null) indexMethods.put(newKey, method);
        Map<String, String> opts = indexReloptions.remove(oldKey);
        if (opts != null) indexReloptions.put(newKey, opts);
        List<String> colOpts = indexColumnOptions.remove(oldKey);
        if (colOpts != null) indexColumnOptions.put(newKey, colOpts);
        List<String> inclCols = indexIncludeColumns.remove(oldKey);
        if (inclCols != null) indexIncludeColumns.put(newKey, inclCols);
        Boolean nnd = indexNullsNotDistinct.remove(oldKey);
        if (nnd != null) indexNullsNotDistinct.put(newKey, nnd);
        // Update schema registry, which records the bare name under the schema that holds it.
        String oldBare = idxName(oldKey).toLowerCase();
        String newBare = idxName(newKey).toLowerCase();
        Set<String> from = schemaObjectRegistry.get(idxSchema(oldKey));
        if (from != null) from.remove("index:" + oldBare);
        registerSchemaObject(idxSchema(newKey), "index", newBare);
        // Update object ownership key
        String oldOwner = objectOwners.remove("index:" + idxName(oldKey));
        if (oldOwner != null) objectOwners.put("index:" + idxName(newKey), oldOwner);
    }

    public Map<String, List<String>> getIndexColumns() {
        return visible(indexColumns);
    }

    // Views
    //
    // A view belongs to a schema, so two schemas may each hold one of the same name and a
    // qualified reference has to reach the one it names. The map is keyed by both, and a bare
    // name still finds the only view that answers to it.
    public void addView(ViewDef view) {
        views.put(viewKey(view.schemaName(), view.name()), view);
        markUncommittedObject(view, CURRENT_VIEWER.get());
    }

    /** The key a view is stored under: its schema and its name. */
    private static String viewKey(String schemaName, String name) {
        String schema = schemaName == null ? "public" : schemaName;
        return schema.toLowerCase() + "." + name.toLowerCase();
    }

    /**
     * The view a name refers to. A qualified name reaches the view in the schema it names; a
     * bare one finds a view of that name in any schema, preferring public.
     */
    public ViewDef getView(String name) {
        if (name == null) return null;
        String lower = name.toLowerCase();
        ViewDef exact = views.get(lower);
        if (exact != null) return exact;
        if (lower.indexOf('.') >= 0) {
            // A qualified name names one schema's view and no other's.
            return null;
        }
        ViewDef found = views.get("public." + lower);
        if (found != null) return found;
        for (Map.Entry<String, ViewDef> e : views.entrySet()) {
            if (e.getValue().name().equalsIgnoreCase(lower)) return e.getValue();
        }
        return null;
    }

    /** The view of this name in this schema, or null. */
    public ViewDef getView(String schemaName, String name) {
        if (name == null) return null;
        if (name.indexOf('.') >= 0) return getView(name);
        return views.get(viewKey(schemaName, name));
    }

    public void removeView(String name) {
        ViewDef view = getView(name);
        if (view != null) views.remove(viewKey(view.schemaName(), view.name()));
    }

    /** Drop the view of this name in this schema. */
    public void removeView(String schemaName, String name) {
        ViewDef view = getView(schemaName, name);
        if (view != null) views.remove(viewKey(view.schemaName(), view.name()));
    }

    public boolean hasView(String name) {
        return getView(name) != null;
    }

    /** Whether this schema holds a view of this name. */
    public boolean hasView(String schemaName, String name) {
        return getView(schemaName, name) != null;
    }

    public Map<String, ViewDef> getViews() {
        return visible(views);
    }

    public Map<String, Sequence> getSequences() {
        return visible(sequences);
    }

    // Domain types
    public void addDomain(DomainType domain) {
        domains.put(TypeNamespace.key(domain.getSchemaName(), domain.getName()), domain);
        markUncommittedObject(domain, CURRENT_VIEWER.get());
    }

    public DomainType getDomain(String name) {
        String key = TypeNamespace.find(domains.keySet(), name);
        return key == null ? null : domains.get(key);
    }

    /** The domain of that name in that schema, and only that one. */
    public DomainType getDomain(String schema, String name) {
        return domains.get(TypeNamespace.key(schema, name));
    }

    public void removeDomain(String name) {
        String key = TypeNamespace.find(domains.keySet(), name);
        if (key != null) domains.remove(key);
    }

    public boolean isDomain(String name) {
        return TypeNamespace.find(domains.keySet(), name) != null;
    }

    public Map<String, DomainType> getDomains() {
        return visible(domains);
    }

    // ==================== Advisory locks ====================

    /** Find this session's hold in the list, or null. Caller must hold advisoryMonitor. */
    private static AdvisoryHold findAdvisoryHold(List<AdvisoryHold> holds, Session session) {
        if (holds == null) return null;
        for (AdvisoryHold h : holds) {
            if (h.session == session) return h;
        }
        return null;
    }

    /**
     * Returns a session whose hold conflicts with the requested acquisition, or null when
     * the lock can be granted. Shared requests conflict only with exclusive holds; exclusive
     * requests conflict with any hold. A session never conflicts with itself (PG allows the
     * same backend to stack modes freely). Caller must hold advisoryMonitor.
     */
    /**
     * The transaction-level advisory locks this session holds, per lock.
     *
     * <p>A savepoint records them so rolling back to it can put them back as they were. They
     * belong to the transaction, and a piece of a transaction that is undone did not take them.
     */
    public synchronized Map<AdvisoryLockId, int[]> advisoryXactHolds(Session session) {
        Map<AdvisoryLockId, int[]> held = new LinkedHashMap<>();
        for (Map.Entry<AdvisoryLockId, List<AdvisoryHold>> e : advisoryLocks.entrySet()) {
            for (AdvisoryHold hold : e.getValue()) {
                if (hold.session == session && (hold.xactExclusive > 0 || hold.xactShared > 0)) {
                    held.put(e.getKey(), new int[]{hold.xactExclusive, hold.xactShared});
                }
            }
        }
        return held;
    }

    /** Put this session's transaction-level advisory locks back to a recorded state. */
    public synchronized void restoreAdvisoryXactHolds(Session session, Map<AdvisoryLockId, int[]> held) {
        if (held == null) return;
        Iterator<Map.Entry<AdvisoryLockId, List<AdvisoryHold>>> it = advisoryLocks.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<AdvisoryLockId, List<AdvisoryHold>> e = it.next();
            int[] want = held.get(e.getKey());
            Iterator<AdvisoryHold> holds = e.getValue().iterator();
            while (holds.hasNext()) {
                AdvisoryHold hold = holds.next();
                if (hold.session != session) continue;
                hold.xactExclusive = want == null ? 0 : want[0];
                hold.xactShared = want == null ? 0 : want[1];
                if (hold.empty()) holds.remove();
            }
            if (e.getValue().isEmpty()) it.remove();
        }
        notifyAll();
    }

    private Session advisoryBlocker(AdvisoryLockId id, Session session, boolean shared) {
        List<AdvisoryHold> holds = advisoryLocks.get(id);
        if (holds == null) return null;
        for (AdvisoryHold h : holds) {
            if (h.session == session) continue;
            if (h.holdsExclusive() || (!shared && h.holdsShared())) return h.session;
        }
        return null;
    }

    /** Record one acquisition (increment the matching reference count). Caller must hold advisoryMonitor. */
    private void grantAdvisory(AdvisoryLockId id, Session session, boolean shared, boolean xact) {
        List<AdvisoryHold> holds = advisoryLocks.get(id);
        if (holds == null) {
            holds = new ArrayList<>();
            advisoryLocks.put(id, holds);
        }
        AdvisoryHold h = findAdvisoryHold(holds, session);
        if (h == null) {
            h = new AdvisoryHold(session);
            holds.add(h);
        }
        if (xact) {
            if (shared) h.xactShared++; else h.xactExclusive++;
        } else {
            if (shared) h.sessionShared++; else h.sessionExclusive++;
        }
    }

    /** Drop empty hold entries and empty lock lists. Caller must hold advisoryMonitor. */
    private void cleanupAdvisory(AdvisoryLockId id, List<AdvisoryHold> holds, AdvisoryHold h) {
        if (h.empty()) holds.remove(h);
        if (holds.isEmpty()) advisoryLocks.remove(id);
    }

    /**
     * Non-blocking advisory lock acquisition (pg_try_advisory_lock and friends).
     * Each successful call increments a per-(session, lock, mode, ownership) reference
     * count and needs a matching unlock (or transaction end for xact ownership).
     */
    public boolean tryAdvisoryLock(AdvisoryLockId id, Session session, boolean shared, boolean xact) {
        synchronized (advisoryMonitor) {
            if (advisoryBlocker(id, session, shared) != null) return false;
            grantAdvisory(id, session, shared, xact);
            return true;
        }
    }

    /**
     * Blocking advisory lock acquisition (pg_advisory_lock and friends). Waits on the
     * advisory monitor until the lock is grantable; every release path calls notifyAll.
     * Like PostgreSQL, the wait blocks only the calling backend's thread.
     *
     * @throws MemgresException 40P01 when a wait-for cycle with another session is detected
     * @throws MemgresException 55P03 when lock_timeout (or the safety timeout) expires
     * @throws MemgresException 57014 when the wait is interrupted (statement cancel)
     */
    public void advisoryLock(AdvisoryLockId id, Session session, boolean shared, boolean xact) {
        final long timeoutMs = lockWaitBudget(session);
        final long deadline = timeoutMs == Long.MAX_VALUE
                ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMs;

        synchronized (advisoryMonitor) {
            try {
                while (true) {
                    Session blocker = advisoryBlocker(id, session, shared);
                    if (blocker == null) {
                        grantAdvisory(id, session, shared, xact);
                        return;
                    }
                    if (session != null) {
                        // Deadlock detection via the shared wait-for graph (also used by row locks).
                        // The session that completes the cycle (the last to arrive) is always the
                        // victim — this matches PG's behavior where the deadlock detector aborts
                        // the waiter that triggers detection.
                        waitingFor.put(session, blocker);
                        if (advisoryDeadlockVictim(session, blocker) != null) {
                            waitingFor.remove(session);
                            throw new MemgresException("deadlock detected", "40P01");
                        }
                    }
                    StatementCancel.check();
                    if (System.currentTimeMillis() >= deadline) {
                        throw new MemgresException("canceling statement due to lock timeout", "55P03");
                    }
                    try {
                        // Short slices so deadlocks formed after we started waiting are detected.
                        advisoryMonitor.wait(50L);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw StatementCancel.canceled();
                    }
                }
            } finally {
                if (session != null) waitingFor.remove(session);
            }
        }
    }

    /**
     * Release one session-level hold of the given mode (pg_advisory_unlock /
     * pg_advisory_unlock_shared). Returns false as a no-op when the session does not hold
     * the lock in that mode; transaction-level holds are never released here (PG semantics).
     */
    public boolean advisoryUnlock(AdvisoryLockId id, Session session, boolean shared) {
        synchronized (advisoryMonitor) {
            List<AdvisoryHold> holds = advisoryLocks.get(id);
            AdvisoryHold h = findAdvisoryHold(holds, session);
            if (h == null) return false;
            if (shared) {
                if (h.sessionShared == 0) return false;
                h.sessionShared--;
            } else {
                if (h.sessionExclusive == 0) return false;
                h.sessionExclusive--;
            }
            cleanupAdvisory(id, holds, h);
            advisoryMonitor.notifyAll();
            return true;
        }
    }

    /**
     * pg_advisory_unlock_all: release every session-level hold of this session regardless of
     * reference counts. Transaction-level holds are left in place (they end with the transaction).
     */
    public void advisoryUnlockAll(Session session) {
        synchronized (advisoryMonitor) {
            Iterator<Map.Entry<AdvisoryLockId, List<AdvisoryHold>>> it = advisoryLocks.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<AdvisoryLockId, List<AdvisoryHold>> e = it.next();
                List<AdvisoryHold> holds = e.getValue();
                AdvisoryHold h = findAdvisoryHold(holds, session);
                if (h != null) {
                    h.sessionExclusive = 0;
                    h.sessionShared = 0;
                    if (h.empty()) holds.remove(h);
                    if (holds.isEmpty()) it.remove();
                }
            }
            advisoryMonitor.notifyAll();
        }
    }

    /** Release all transaction-scoped advisory holds of a session. Called at COMMIT/ROLLBACK/PREPARE. */
    public void releaseXactAdvisoryLocks(Session session) {
        synchronized (advisoryMonitor) {
            Iterator<Map.Entry<AdvisoryLockId, List<AdvisoryHold>>> it = advisoryLocks.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<AdvisoryLockId, List<AdvisoryHold>> e = it.next();
                List<AdvisoryHold> holds = e.getValue();
                AdvisoryHold h = findAdvisoryHold(holds, session);
                if (h != null) {
                    h.xactExclusive = 0;
                    h.xactShared = 0;
                    if (h.empty()) holds.remove(h);
                    if (holds.isEmpty()) it.remove();
                }
            }
            advisoryMonitor.notifyAll();
        }
    }

    /** Release every advisory hold (both ownerships) of a session. Called on disconnect. */
    public void releaseAllAdvisoryLocks(Session session) {
        synchronized (advisoryMonitor) {
            Iterator<Map.Entry<AdvisoryLockId, List<AdvisoryHold>>> it = advisoryLocks.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<AdvisoryLockId, List<AdvisoryHold>> e = it.next();
                List<AdvisoryHold> holds = e.getValue();
                AdvisoryHold h = findAdvisoryHold(holds, session);
                if (h != null) {
                    holds.remove(h);
                    if (holds.isEmpty()) it.remove();
                }
            }
            advisoryMonitor.notifyAll();
        }
    }

    // Row-level locks

    public boolean tryLockRow(String tableName, Object[] row, Session session) {
        return tryLockRow(tableName, row, session, "UPDATE");
    }

    public boolean tryLockRow(String tableName, Object[] row, Session session, String mode) {
        Map<Object[], List<LockEntry>> locks = rowLocks.computeIfAbsent(tableName.toLowerCase(), k -> new IdentityHashMap<>());
        synchronized (locks) {
            List<LockEntry> entries = locks.get(row);
            if (entries != null) {
                // Check compatibility with all existing lock holders from other sessions
                for (LockEntry existing : entries) {
                    if (existing.session != session && !lockModesCompatible(existing.mode, mode)) {
                        return false; // incompatible lock held by another session
                    }
                }
                // Remove any prior entry for this session (will re-add with potentially upgraded mode)
                entries.removeIf(e -> e.session == session);
            } else {
                entries = new ArrayList<>();
                locks.put(row, entries);
            }
            entries.add(new LockEntry(session, mode, rowLockSeq.incrementAndGet()));
            return true;
        }
    }

    /**
     * Returns the session currently holding an incompatible lock on this row, or null if none.
     * Must be called with the locks map's monitor held.
     */
    private Session getBlockingSession(List<LockEntry> entries, Session requester, String mode) {
        if (entries == null) return null;
        for (LockEntry existing : entries) {
            if (existing.session != requester && !lockModesCompatible(existing.mode, mode)) {
                return existing.session;
            }
        }
        return null;
    }

    /**
     * Walks the wait-for chain starting at {@code blocker}. If it cycles back to
     * {@code requester} (a deadlock), returns the requester (non-null signals deadlock).
     * Returns null when there is no deadlock involving {@code requester}.
     */
    private Session advisoryDeadlockVictim(Session requester, Session blocker) {
        Set<Session> visited = new HashSet<>();
        Session current = blocker;
        while (current != null) {
            if (current == requester) return requester;
            if (!visited.add(current)) return null; // cycle that does not involve the requester
            current = waitingFor.get(current);
        }
        return null;
    }

    /**
     * Checks whether following the wait-for chain starting at {@code blocker} eventually leads back to
     * {@code requester}, indicating a deadlock cycle.
     */
    /**
     * Wait for another session's in-flight write to settle, without holding any table lock.
     *
     * <p>A uniqueness check cannot decide anything about a row another session has inserted but
     * not committed: whether it is really a duplicate depends on that transaction. Waiting is the
     * only correct answer, and it has to happen with the table lock released, because rolling the
     * other transaction back needs that same lock.
     *
     * @param stillBlocked re-evaluated on each poll; the wait ends when it becomes false
     * @throws MemgresException {@code 40P01} if waiting would close a cycle,
     *         {@code 55P03} if the lock timeout expires
     */
    public void awaitConcurrentWrite(Session waiter, Session blocker,
                                      java.util.function.BooleanSupplier stillBlocked,
                                      String relationName) {
        if (waiter == null || blocker == null) return;
        long timeoutMs = lockWaitBudget(waiter);
        long deadline = timeoutMs == Long.MAX_VALUE ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMs;
        waitingFor.put(waiter, blocker);
        try {
            while (stillBlocked.getAsBoolean()) {
                // A transaction that can no longer commit anything cannot make its write
                // permanent, so there is nothing left to learn from it. Waiting on one is how two
                // sessions that broke each other's statement end up waiting for each other with
                // no way out. The test has to be the same one the callers use to decide whether
                // the row is still in the way: returning here for a blocker they still count
                // turns their retry loop into a spin that no cancel can reach.
                if (blocker.isDoomed() || !blocker.isInTransaction()) return;
                if (hasDeadlock(waiter, blocker)) {
                    throw new MemgresException("deadlock detected", "40P01");
                }
                StatementCancel.check();
                if (System.currentTimeMillis() >= deadline) {
                    throw new MemgresException("canceling statement due to lock timeout", "55P03");
                }
                try {
                    Thread.sleep(5L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw StatementCancel.canceled();
                }
            }
        } finally {
            waitingFor.remove(waiter);
        }
    }

    private boolean hasDeadlock(Session requester, Session blocker) {
        Set<Session> visited = new HashSet<>();
        Session current = blocker;
        while (current != null) {
            if (current == requester) return true;
            if (!visited.add(current)) return false; // a cycle this session is not part of
            current = waitingFor.get(current);
        }
        return false;
    }

    /**
     * Acquire a row lock for normal FOR UPDATE/SHARE (without NOWAIT/SKIP LOCKED).
     * Blocks with polling until the lock is available or a deadlock / timeout is detected.
     *
     * @throws MemgresException with SQLSTATE 40P01 when a deadlock is detected
     * @throws MemgresException with SQLSTATE 55P03 when the lock timeout expires
     */
    public void lockRowWaiting(String tableName, Object[] row, Session session, String mode) {
        final long timeoutMs = lockWaitBudget(session);
        final long pollMs = 10L;
        final long deadline = timeoutMs == Long.MAX_VALUE
                ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMs;

        Map<Object[], List<LockEntry>> locks =
                rowLocks.computeIfAbsent(tableName.toLowerCase(), k -> new IdentityHashMap<>());

        while (true) {
            Session blocker;
            synchronized (locks) {
                List<LockEntry> entries = locks.get(row);
                // A row lock belonging to a transaction that has already failed is not a lock any
                // more: PostgreSQL's abort makes the tuple version dead, so the next writer takes
                // the row straight away instead of waiting for a ROLLBACK that may never be sent.
                if (entries != null) entries.removeIf(e -> e.session != null && e.session.isDoomed());
                blocker = getBlockingSession(entries, session, mode);
                if (blocker == null) {
                    // Lock is available, acquire it
                    if (entries == null) {
                        entries = new ArrayList<>();
                        locks.put(row, entries);
                    }
                    entries.removeIf(e -> e.session == session);
                    entries.add(new LockEntry(session, mode, rowLockSeq.incrementAndGet()));
                    waitingFor.remove(session); // no longer waiting
                    return;
                }
            }
            // Lock not available; check for deadlock before waiting
            if (hasDeadlock(session, blocker)) {
                waitingFor.remove(session);
                throw new MemgresException("deadlock detected", "40P01");
            }
            // Register that this session is waiting for the blocker
            waitingFor.put(session, blocker);

            // Check cancellation and timeout
            try {
                StatementCancel.check();
            } catch (RuntimeException e) {
                waitingFor.remove(session);
                throw e;
            }
            if (System.currentTimeMillis() >= deadline) {
                waitingFor.remove(session);
                throw new MemgresException("canceling statement due to lock timeout", "55P03");
            }

            // Sleep briefly before retrying
            try {
                Thread.sleep(pollMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                waitingFor.remove(session);
                throw StatementCancel.canceled();
            }
        }
    }

    public void unlockRow(String tableName, Object[] row) {
        Map<Object[], List<LockEntry>> locks = rowLocks.get(tableName.toLowerCase());
        if (locks != null) {
            synchronized (locks) {
                locks.remove(row);
            }
        }
    }

    public void unlockAllRows(Session session) {
        for (Map<Object[], List<LockEntry>> locks : rowLocks.values()) {
            synchronized (locks) {
                for (List<LockEntry> entries : locks.values()) {
                    entries.removeIf(e -> e.session == session);
                }
                locks.entrySet().removeIf(e -> e.getValue().isEmpty());
            }
        }
        // Clean up any wait-for entries involving this session
        waitingFor.remove(session);
        waitingFor.entrySet().removeIf(e -> e.getValue() == session);
    }

    /**
     * Returns true if the given row array is the MVCC old-values copy for a live row
     * currently being updated by another active session (uncommitted UPDATE).
     *
     * When MVCC visibility substitutes old values for a row being updated by another
     * session, the row binding in a FOR UPDATE SKIP LOCKED scan holds the old-values
     * copy rather than the actual live row.  Because the copy has a different identity
     * it is not in the row-lock map, so {@code tryLockRow} would always succeed on it --
     * allowing two workers to claim the same logical job.  This method detects that
     * situation so the SKIP LOCKED loop can skip the row instead.
     */
    /**
     * When another session holds an uncommitted UPDATE on a row, this session's snapshot binds
     * the old-values copy rather than the live array. Row locks are keyed by the live row's
     * identity, so resolve the copy back before locking; otherwise a FOR UPDATE against a row
     * someone else is updating locks a private copy and sees no conflict.
     */
    public Object[] liveRowForSnapshotCopy(Object[] row, Session currentSession) {
        for (Session other : activeSessions) {
            if (other == currentSession) continue;
            if (!other.isInTransaction()) continue;
            try {
                for (Map<Object[], Object[]> tableUpdates : other.getAllUncommittedUpdates().values()) {
                    synchronized (tableUpdates) {
                        for (Map.Entry<Object[], Object[]> e : tableUpdates.entrySet()) {
                            if (e.getValue() == row) return e.getKey();
                        }
                    }
                }
            } catch (Exception ignored) {
                // A concurrent commit cleared the map; the caller's own row is then correct
            }
        }
        return row;
    }

    public boolean isRowBeingUpdatedByOtherSession(Object[] row, Session currentSession) {
        for (Session other : activeSessions) {
            if (other == currentSession) continue;
            if (!other.isInTransaction()) continue;
            try {
                // uncommittedUpdates is a ConcurrentHashMap; inner maps are synchronizedMap-wrapped
                // IdentityHashMaps, safe to iterate without CME.
                for (Map<Object[], Object[]> tableUpdates : other.getAllUncommittedUpdates().values()) {
                    // Snapshot values to avoid issues with concurrent modification of inner map
                    Object[][] values;
                    synchronized (tableUpdates) {
                        values = tableUpdates.values().toArray(new Object[0][]);
                    }
                    for (Object[] oldValues : values) {
                        if (oldValues == row) {
                            return true;
                        }
                    }
                }
            } catch (Exception e) {
                // Concurrent commit/rollback cleared the map during iteration --
                // conservatively treat the row as being updated.
                return true;
            }
        }
        return false;
    }

    /**
     * Stored view definition.
     */
        public static final class ViewDef {
        public final String name;
        public final String schemaName;
        public final Statement query;
        public final boolean orReplace;
        public final boolean materialized;
        public final List<Column> cachedColumns;
        public final List<Object[]> cachedRows;
        // sourceSQL is what pg_get_viewdef echoes; a rename of a referenced relation rewrites it
        public String sourceSQL;
        public final String checkOption;
        public final Map<String, String> reloptions;
        /**
         * Whether a materialized view holds data. False for CREATE MATERIALIZED VIEW ...
         * WITH NO DATA (and after REFRESH ... WITH NO DATA) until the next REFRESH.
         * Always true for regular views.
         */
        public final boolean populated;

        public ViewDef(
                String name,
                String schemaName,
                Statement query,
                boolean orReplace,
                boolean materialized,
                List<Column> cachedColumns,
                List<Object[]> cachedRows,
                String sourceSQL,
                String checkOption
        ) {
            this(name, schemaName, query, orReplace, materialized, cachedColumns, cachedRows, sourceSQL, checkOption, null);
        }

        public ViewDef(
                String name,
                String schemaName,
                Statement query,
                boolean orReplace,
                boolean materialized,
                List<Column> cachedColumns,
                List<Object[]> cachedRows,
                String sourceSQL,
                String checkOption,
                Map<String, String> reloptions
        ) {
            this(name, schemaName, query, orReplace, materialized, cachedColumns, cachedRows, sourceSQL, checkOption, reloptions, true);
        }

        public ViewDef(
                String name,
                String schemaName,
                Statement query,
                boolean orReplace,
                boolean materialized,
                List<Column> cachedColumns,
                List<Object[]> cachedRows,
                String sourceSQL,
                String checkOption,
                Map<String, String> reloptions,
                boolean populated
        ) {
            this.name = name;
            this.schemaName = schemaName;
            this.query = query;
            this.orReplace = orReplace;
            this.materialized = materialized;
            this.cachedColumns = cachedColumns;
            this.cachedRows = cachedRows;
            this.sourceSQL = sourceSQL;
            this.checkOption = checkOption;
            this.reloptions = reloptions;
            this.populated = populated;
        }

        /** Convenience constructor (full, without checkOption). */
        public ViewDef(String name, String schemaName, Statement query, boolean orReplace, boolean materialized,
                       List<Column> cachedColumns, List<Object[]> cachedRows, String sourceSQL) {
            this(name, schemaName, query, orReplace, materialized, cachedColumns, cachedRows, sourceSQL, null);
        }

        /** Convenience constructor for regular views (with schema). */
        public ViewDef(String name, String schemaName, Statement query, boolean orReplace) {
            this(name, schemaName, query, orReplace, false, null, null, null, null);
        }

        /** Convenience constructor for regular views (no schema, defaults to public). */
        public ViewDef(String name, Statement query, boolean orReplace) {
            this(name, "public", query, orReplace, false, null, null, null, null);
        }

        /** Convenience constructor for materialized views (no sourceSQL). */
        public ViewDef(String name, Statement query, boolean orReplace, boolean materialized,
                       List<Column> cachedColumns, List<Object[]> cachedRows) {
            this(name, "public", query, orReplace, materialized, cachedColumns, cachedRows, null, null);
        }

        /** Convenience constructor for materialized views with schema (no sourceSQL). */
        public ViewDef(String name, String schemaName, Statement query, boolean orReplace, boolean materialized,
                       List<Column> cachedColumns, List<Object[]> cachedRows) {
            this(name, schemaName, query, orReplace, materialized, cachedColumns, cachedRows, null, null);
        }

        public String name() { return name; }
        public String schemaName() { return schemaName; }
        public Statement query() { return query; }
        public boolean orReplace() { return orReplace; }
        public boolean materialized() { return materialized; }
        public List<Column> cachedColumns() { return cachedColumns; }
        public List<Object[]> cachedRows() { return cachedRows; }
        public String sourceSQL() { return sourceSQL; }
        public String checkOption() { return checkOption; }
        public Map<String, String> reloptions() { return reloptions; }
        public boolean populated() { return populated; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ViewDef that = (ViewDef) o;
            return java.util.Objects.equals(name, that.name)
                && java.util.Objects.equals(schemaName, that.schemaName)
                && java.util.Objects.equals(query, that.query)
                && orReplace == that.orReplace
                && materialized == that.materialized
                && java.util.Objects.equals(cachedColumns, that.cachedColumns)
                && java.util.Objects.equals(cachedRows, that.cachedRows)
                && java.util.Objects.equals(sourceSQL, that.sourceSQL)
                && java.util.Objects.equals(checkOption, that.checkOption);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, schemaName, query, orReplace, materialized, cachedColumns, cachedRows, sourceSQL, checkOption);
        }

        @Override
        public String toString() {
            return "ViewDef[name=" + name + ", " + "schemaName=" + schemaName + ", " + "query=" + query + ", " + "orReplace=" + orReplace + ", " + "materialized=" + materialized + ", " + "cachedColumns=" + cachedColumns + ", " + "cachedRows=" + cachedRows + ", " + "sourceSQL=" + sourceSQL + ", " + "checkOption=" + checkOption + "]";
        }
    }

    // ---- Role management ----

    public void createRole(String name, Map<String, String> attributes) {
        roles.put(name.toLowerCase(), new ConcurrentHashMap<>(attributes));
    }

    public boolean hasRole(String name) {
        return roles.containsKey(name.toLowerCase());
    }

    public Map<String, String> getRole(String name) {
        return roles.get(name.toLowerCase());
    }

    public void removeRole(String name) {
        String lower = name.toLowerCase();
        roles.remove(lower);
        // Clean up memberships: remove this role as a member from all groups
        for (Set<String> members : roleMemberships.values()) {
            members.remove(lower);
        }
        // Remove empty membership entries
        roleMemberships.entrySet().removeIf(e -> e.getValue().isEmpty());
        // Also remove this role's own membership entry (if it was a group)
        roleMemberships.remove(lower);
    }

    public Map<String, Map<String, String>> getRoles() {
        return roles;
    }

    // ---- Role membership ----

    public void addRoleMembership(String grantedRole, String memberRole) {
        addRoleMembership(grantedRole, memberRole, false);
    }

    public void addRoleMembership(String grantedRole, String memberRole, boolean withAdminOption) {
        roleMemberships.computeIfAbsent(grantedRole.toLowerCase(), k -> ConcurrentHashMap.newKeySet())
                .add(memberRole.toLowerCase());
        String key = grantedRole.toLowerCase() + "|" + memberRole.toLowerCase();
        if (withAdminOption) {
            roleAdminOptions.put(key, true);
        }
    }

    public boolean hasAdminOption(String grantedRole, String memberRole) {
        return Boolean.TRUE.equals(
                roleAdminOptions.get(grantedRole.toLowerCase() + "|" + memberRole.toLowerCase()));
    }

    public void removeRoleMembership(String grantedRole, String memberRole) {
        Set<String> members = roleMemberships.get(grantedRole.toLowerCase());
        if (members != null) {
            members.remove(memberRole.toLowerCase());
            if (members.isEmpty()) roleMemberships.remove(grantedRole.toLowerCase());
        }
    }

    /**
     * Whether {@code member} holds the rights of {@code role}, directly or through a role it is a
     * member of. Membership is transitive in PostgreSQL, so reading only the direct grants
     * answered no for a role that reaches the rights by one more step.
     */
    public boolean isRoleMemberOf(String member, String role) {
        if (member == null || role == null) return false;
        String want = role.toLowerCase();
        Set<String> seen = ConcurrentHashMap.newKeySet();
        Deque<String> pending = new ArrayDeque<>();
        pending.add(member.toLowerCase());
        while (!pending.isEmpty()) {
            String current = pending.poll();
            if (!seen.add(current)) continue;
            if (current.equals(want)) return true;
            for (Map.Entry<String, Set<String>> entry : roleMemberships.entrySet()) {
                if (entry.getValue().contains(current)) pending.add(entry.getKey());
            }
        }
        return false;
    }

    public boolean hasRoleMemberships(String roleName) {
        Set<String> members = roleMemberships.get(roleName.toLowerCase());
        return members != null && !members.isEmpty();
    }

    public void removeAllRoleMemberships(String roleName) {
        String lower = roleName.toLowerCase();
        // Remove the role as a granted role (revoke all members)
        roleMemberships.remove(lower);
        // Remove the role as a member of other roles
        for (Set<String> members : roleMemberships.values()) {
            members.remove(lower);
        }
    }

    public Map<String, Set<String>> getRoleMemberships() {
        return roleMemberships;
    }

    // ---- Role privilege tracking ----

    public void addRolePrivilege(String role, String privilege, String objectType, String objectName) {
        rolePrivileges.computeIfAbsent(role.toLowerCase(), k -> ConcurrentHashMap.newKeySet())
                .add(privilege + ":" + objectType + ":" + objectName);
    }

    public void removeRolePrivilege(String role, String privilege, String objectType, String objectName) {
        Set<String> privs = rolePrivileges.get(role.toLowerCase());
        if (privs != null) {
            if ("ALL".equalsIgnoreCase(privilege)) {
                // Remove all privileges on this object (case-insensitive suffix match)
                final String suffixLower = ":" + objectType.toLowerCase() + ":" + objectName.toLowerCase();
                privs.removeIf(p -> p.toLowerCase().endsWith(suffixLower));
                // If revoking ALL on a TABLE, also remove any column-level grants for that table
                if ("TABLE".equalsIgnoreCase(objectType)) {
                    final String colPrefixLower = ":column:" + objectName.toLowerCase() + ".";
                    privs.removeIf(p -> p.toLowerCase().contains(colPrefixLower));
                }
            } else {
                // Case-insensitive exact match
                final String keyLower = privilege.toLowerCase() + ":" + objectType.toLowerCase() + ":" + objectName.toLowerCase();
                privs.removeIf(p -> p.toLowerCase().equals(keyLower));
            }
            if (privs.isEmpty()) rolePrivileges.remove(role.toLowerCase());
        }
    }

    public boolean hasRolePrivileges(String role) {
        Set<String> privs = rolePrivileges.get(role.toLowerCase());
        return privs != null && !privs.isEmpty();
    }

    /** Remove all privileges held by a specific role (e.g. when the role is dropped). */
    public void removeAllRolePrivileges(String role) {
        rolePrivileges.remove(role.toLowerCase());
    }

    /** Get all privileges held by a specific role. */
    public Set<String> getRolePrivileges(String role) {
        Set<String> privs = rolePrivileges.get(role.toLowerCase());
        return privs != null ? privs : java.util.Collections.emptySet();
    }

    /** Get the entire rolePrivileges map (for privilege inspection). */
    public Map<String, Set<String>> getAllRolePrivileges() {
        return rolePrivileges;
    }

    /** Remove all privileges granted on a specific object (called when the object is dropped). */
    public void removePrivilegesOnObject(String objectType, String objectName) {
        String suffix = ":" + objectType + ":" + objectName;
        for (Map.Entry<String, Set<String>> entry : rolePrivileges.entrySet()) {
            entry.getValue().removeIf(p -> p.endsWith(suffix));
        }
        // Clean up empty entries
        rolePrivileges.entrySet().removeIf(e -> e.getValue().isEmpty());
    }

    // ---- Object ownership ----

    /** Set the owner of an object. Key format: "type:name" (e.g., "table:public.my_table"). */
    public void setObjectOwner(String objectKey, String ownerRole) {
        objectOwners.put(objectKey.toLowerCase(), ownerRole.toLowerCase());
    }

    /** Get the owner of an object, or null if not tracked. */
    public String getObjectOwner(String objectKey) {
        return objectOwners.get(objectKey.toLowerCase());
    }

    /** Remove ownership entry for an object. */
    public void removeObjectOwner(String objectKey) {
        objectOwners.remove(objectKey.toLowerCase());
    }

    /** Check if a role owns any objects. */
    public boolean roleOwnsObjects(String roleName) {
        String lower = roleName.toLowerCase();
        return objectOwners.containsValue(lower);
    }

    /** Get all object keys owned by a role. */
    public List<String> getObjectsOwnedBy(String roleName) {
        String lower = roleName.toLowerCase();
        List<String> owned = new ArrayList<>();
        for (Map.Entry<String, String> entry : objectOwners.entrySet()) {
            if (lower.equals(entry.getValue())) {
                owned.add(entry.getKey());
            }
        }
        return owned;
    }

    /** Transfer all objects owned by one role to another. */
    public void reassignOwned(String fromRole, String toRole) {
        String fromLower = fromRole.toLowerCase();
        String toLower = toRole.toLowerCase();
        for (Map.Entry<String, String> entry : objectOwners.entrySet()) {
            if (fromLower.equals(entry.getValue())) {
                entry.setValue(toLower);
            }
        }
    }

    // ---- Extension management ----

    public void addExtension(String name, String version) {
        installedExtensions.put(name.toLowerCase(), version);
    }

    public void addExtension(String name, String version, String schema) {
        installedExtensions.put(name.toLowerCase(), version);
        if (schema != null) {
            extensionSchemas.put(name.toLowerCase(), schema);
        }
    }

    public void setExtensionSchema(String name, String schema) {
        extensionSchemas.put(name.toLowerCase(), schema);
    }

    public String getExtensionSchema(String name) {
        return extensionSchemas.get(name.toLowerCase());
    }

    public void removeExtension(String name) {
        installedExtensions.remove(name.toLowerCase());
        extensionSchemas.remove(name.toLowerCase());
    }

    public boolean hasExtension(String name) {
        return installedExtensions.containsKey(name.toLowerCase());
    }

    public Map<String, String> getInstalledExtensions() {
        return installedExtensions;
    }

    public Map<String, String> getExtensionSchemas() {
        return extensionSchemas;
    }

    // ---- Schema object registry ----

    /**
     * Register an object as belonging to a schema.
     * @param schemaName the schema this object belongs to
     * @param objectType a category like "enum", "composite", "sequence", "domain", "index", "function", "view", "trigger"
     * @param objectName the name of the object (lowercased)
     */
    public void registerSchemaObject(String schemaName, String objectType, String objectName) {
        schemaObjectRegistry
                .computeIfAbsent(schemaName.toLowerCase(), k -> ConcurrentHashMap.newKeySet())
                .add(objectType + ":" + objectName.toLowerCase());
    }

    /** Forget one registered object, for a DDL statement that is being undone. */
    public void unregisterSchemaObject(String schemaName, String objectType, String objectName) {
        Set<String> entries = schemaObjectRegistry.get(schemaName.toLowerCase());
        if (entries != null) entries.remove(objectType + ":" + objectName.toLowerCase());
    }

    /**
     * Get all registered objects for a schema.
     */
    public Set<String> getSchemaObjects(String schemaName) {
        return schemaObjectRegistry.getOrDefault(schemaName.toLowerCase(), Cols.setOf());
    }

    /**
     * Remove schema from registry.
     */
    public void removeSchemaObjects(String schemaName) {
        schemaObjectRegistry.remove(schemaName.toLowerCase());
    }

    // ---- FDW accessors ----

    public Map<String, FdwWrapper> getForeignDataWrappers() { return foreignDataWrappers; }
    public void addForeignDataWrapper(FdwWrapper w) { foreignDataWrappers.put(w.name.toLowerCase(), w); }
    public void removeForeignDataWrapper(String name) { foreignDataWrappers.remove(name.toLowerCase()); }

    public Map<String, FdwServer> getForeignServers() { return foreignServers; }
    public void addForeignServer(FdwServer s) { foreignServers.put(s.name.toLowerCase(), s); }
    public FdwServer getForeignServer(String name) { return foreignServers.get(name.toLowerCase()); }
    public void removeForeignServer(String name) { foreignServers.remove(name.toLowerCase()); }

    public Map<String, FdwUserMapping> getForeignUserMappings() { return foreignUserMappings; }
    public void addForeignUserMapping(FdwUserMapping m) { foreignUserMappings.put((m.serverName + ":" + m.userName).toLowerCase(), m); }

    public Map<String, FdwForeignTable> getForeignTables() { return foreignTables; }
    public void addForeignTable(FdwForeignTable ft) { foreignTables.put(ft.tableName.toLowerCase(), ft); }
    public void removeForeignTable(String name) { foreignTables.remove(name.toLowerCase()); }

    // ---- Publication / Subscription accessors ----

    public Map<String, PubDef> getPublications() { return publications; }
    public void addPublication(PubDef p) { publications.put(p.name.toLowerCase(), p); }
    public PubDef getPublication(String name) { return publications.get(name.toLowerCase()); }
    public void removePublication(String name) { publications.remove(name.toLowerCase()); }

    public Map<String, SubDef> getSubscriptions() { return subscriptions; }
    public void addSubscription(SubDef s) { subscriptions.put(s.name.toLowerCase(), s); }
    public void removeSubscription(String name) { subscriptions.remove(name.toLowerCase()); }

    // ---- Replication slot accessors ----

    public Map<String, ReplicationSlot> getReplicationSlots() { return replicationSlots; }
    public void addReplicationSlot(ReplicationSlot s) { replicationSlots.put(s.slotName.toLowerCase(), s); }
    public void removeReplicationSlot(String name) { replicationSlots.remove(name.toLowerCase()); }
}
