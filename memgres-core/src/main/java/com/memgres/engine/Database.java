package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.CreateTypeStmt;
import com.memgres.engine.parser.ast.Statement;

import java.util.ArrayList;
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
    private final Map<String, DomainType> domains = new ConcurrentHashMap<>();
    private final Map<String, List<CreateTypeStmt.CompositeField>> compositeTypes = new ConcurrentHashMap<>();
    private final Map<String, String> rangeTypes = new ConcurrentHashMap<>(); // range type name → subtype name
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

    public void addCollation(CollationDef coll) { userCollations.put(coll.name.toLowerCase(), coll); }
    public CollationDef getCollation(String name) { return userCollations.get(name.toLowerCase()); }
    public Map<String, CollationDef> getUserCollations() { return userCollations; }

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

    /** A single row-lock entry recording the holding session and the requested lock mode. */
    public static class LockEntry {
        public final Session session;
        public final String mode; // "UPDATE", "NO KEY UPDATE", "SHARE", "KEY SHARE"

        public LockEntry(Session session, String mode) {
            this.session = session;
            this.mode = mode;
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
        synchronized (tableLockMonitor) {
            while (true) {
                List<TableLockEntry> entries = tableLevelLocks.computeIfAbsent(tableKey, k -> new java.util.concurrent.CopyOnWriteArrayList<>());
                boolean conflict = false;
                for (TableLockEntry e : entries) {
                    if (e.session != session && !tableLockModesCompatible(e.mode, mode)) {
                        conflict = true;
                        break;
                    }
                }
                if (!conflict) {
                    entries.add(new TableLockEntry(session, mode));
                    return;
                }
                if (nowait) {
                    throw new MemgresException("could not obtain lock on relation", "55P03");
                }
                try { tableLockMonitor.wait(5000); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new MemgresException("lock wait interrupted", "57014");
                }
            }
        }
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

    // Advisory locks: key -> set of sessions holding the lock
    private final Map<Long, Set<Session>> advisoryLocks = new ConcurrentHashMap<>();
    // Advisory shared locks: key -> set of sessions holding the shared lock
    private final Set<Long> advisorySharedKeys = ConcurrentHashMap.newKeySet();

    // Roles: name -> attributes map
    private final Map<String, Map<String, String>> roles = new ConcurrentHashMap<>();

    // Role memberships: granted role (lowercase) -> set of member roles (lowercase)
    private final Map<String, Set<String>> roleMemberships = new ConcurrentHashMap<>();

    // admin_option flag: "grantedRole|memberRole" (lowercase) -> true if GRANT ... WITH ADMIN OPTION was used
    private final Map<String, Boolean> roleAdminOptions = new ConcurrentHashMap<>();

    // Granted privileges: role (lowercase) -> set of "privilege:objectType:objectName" entries
    private final Map<String, Set<String>> rolePrivileges = new ConcurrentHashMap<>();

    // Rules: "table:EVENT" -> rule type (e.g., "INSTEAD_NOTHING")
    private final Map<String, String> rules = new ConcurrentHashMap<>();

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

    /** Get or create the row metadata map for a given table key (schema.table). */
    public Map<Object[], long[]> getRowMeta(String tableKey) {
        return tableRowMeta.computeIfAbsent(tableKey, k -> new IdentityHashMap<>());
    }

    // Per-table ctid counter for generating unique tuple IDs
    private final Map<String, AtomicLong> tableCtidCounters = new ConcurrentHashMap<>();

    /** Record xmin/cmin metadata for a newly inserted row. */
    public void setRowInsertMeta(String tableKey, Object[] row, long xmin, long cmin) {
        long ctid = tableCtidCounters.computeIfAbsent(tableKey, k -> new AtomicLong(0)).incrementAndGet();
        getRowMeta(tableKey).put(row, new long[]{xmin, 0, cmin, 0, ctid});
    }

    /** Update xmin metadata for an updated row (new ctid). */
    public void setRowUpdateMeta(String tableKey, Object[] row, long xmin, long cmin) {
        long ctid = tableCtidCounters.computeIfAbsent(tableKey, k -> new AtomicLong(0)).incrementAndGet();
        getRowMeta(tableKey).put(row, new long[]{xmin, 0, cmin, 0, ctid});
    }

    /** Remove row metadata (on delete). */
    public void removeRowMeta(String tableKey, Object[] row) {
        Map<Object[], long[]> meta = tableRowMeta.get(tableKey);
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
        // Register built-in comparison functions so CREATE OPERATOR PROCEDURE = int4eq etc. works
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
            PgFunction fn = new PgFunction(f[0], f[1], "-- built-in: " + f[0], "internal", params, true);
            fn.setSchemaName("pg_catalog");
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
    public void setClusteredIndex(String indexName) { clusteredIndexes.add(indexName.toLowerCase()); }
    public boolean isClusteredIndex(String indexName) { return clusteredIndexes.contains(indexName.toLowerCase()); }

    /** Get advisory locks map (for pg_locks). */
    public Map<Long, Set<Session>> getAdvisoryLocks() {
        return advisoryLocks;
    }

    public Schema getSchema(String name) {
        return schemas.get(name);
    }

    public Schema getOrCreateSchema(String name) {
        return schemas.computeIfAbsent(name, Schema::new);
    }

    public Map<String, Schema> getSchemas() {
        return schemas;
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

    // Custom ENUM types
    public void addCustomEnum(CustomEnum customEnum) {
        customEnums.put(customEnum.getName().toLowerCase(), customEnum);
    }

    public CustomEnum getCustomEnum(String name) {
        return customEnums.get(name.toLowerCase());
    }

    public boolean isCustomEnum(String typeName) {
        return customEnums.containsKey(typeName.toLowerCase());
    }

    public void replaceCustomEnum(CustomEnum e) {
        customEnums.put(e.getName().toLowerCase(), e);
    }

    public void removeCustomEnum(String name) {
        customEnums.remove(name.toLowerCase());
    }

    public Map<String, CustomEnum> getCustomEnums() {
        return customEnums;
    }

    // Composite types
    public void addCompositeType(String name, List<CreateTypeStmt.CompositeField> fields) {
        compositeTypes.put(name.toLowerCase(), fields);
    }

    public boolean isCompositeType(String name) {
        return compositeTypes.containsKey(name.toLowerCase());
    }

    public List<CreateTypeStmt.CompositeField> getCompositeType(String name) {
        return compositeTypes.get(name.toLowerCase());
    }

    public void removeCompositeType(String name) {
        compositeTypes.remove(name.toLowerCase());
    }

    public Map<String, List<CreateTypeStmt.CompositeField>> getCompositeTypes() {
        return compositeTypes;
    }

    // User-defined range types
    public void addRangeType(String name, String subtype) {
        rangeTypes.put(name.toLowerCase(), subtype);
    }

    public Map<String, String> getRangeTypes() {
        return rangeTypes;
    }

    public void addUserCast(int sourceOid, int targetOid, int castFunc, String castContext, String castMethod) {
        userDefinedCasts.add(new Object[]{sourceOid, targetOid, castFunc, castContext, castMethod});
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
    }

    /** Returns the single function with this name, or the first overload if multiple exist. */
    public PgFunction getFunction(String name) {
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

    /** Finds the best matching overload by argument count and types. */
    public PgFunction resolveFunction(String name, int argCount, List<String> argTypeHints) {
        List<PgFunction> overloads = getFunctionOverloads(name);
        if (overloads.isEmpty()) return null;
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
                if (hasVariadic) {
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
            if (fHasVariadic && argCount <= inputParams.size()) return null;
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

    /** Remove a specific overload by name and param types. */
    public void removeFunction(String name, List<String> paramTypes) {
        String key = name.toLowerCase();
        List<PgFunction> overloads = functionOverloads.get(key);
        if (overloads == null) return;
        overloads.removeIf(f -> {
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
        return functions;
    }

    // Triggers
    public void addTrigger(PgTrigger trigger) {
        triggers.computeIfAbsent(trigger.getTableName().toLowerCase(), k -> new ArrayList<>())
                .add(trigger);
    }

    public List<PgTrigger> getTriggersForTable(String tableName) {
        return triggers.getOrDefault(tableName.toLowerCase(), Cols.listOf());
    }

    public Map<String, List<PgTrigger>> getAllTriggers() {
        return triggers;
    }

    public void removeTrigger(String name, String tableName) {
        List<PgTrigger> list = triggers.get(tableName.toLowerCase());
        if (list != null) {
            list.removeIf(t -> t.getName().equalsIgnoreCase(name));
        }
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
    public void addSequence(Sequence sequence) {
        sequences.put(sequence.getName().toLowerCase(), sequence);
    }

    public Sequence getSequence(String name) {
        return sequences.get(name.toLowerCase());
    }

    public void removeSequence(String name) {
        sequences.remove(name.toLowerCase());
    }

    /** Remove all sequences whose name starts with the given prefix. */
    public void removeSequencesWithPrefix(String prefix) {
        String lowerPrefix = prefix.toLowerCase();
        sequences.keySet().removeIf(k -> k.startsWith(lowerPrefix));
    }

    public boolean hasSequence(String name) {
        return sequences.containsKey(name.toLowerCase());
    }

    // Rules
    public void addRule(String table, String event, String ruleType) {
        rules.put(table.toLowerCase() + ":" + event.toUpperCase(), ruleType);
    }

    public void addRuleByName(String ruleName, String table) {
        rules.put("name:" + ruleName.toLowerCase() + ":" + table.toLowerCase(), "exists");
    }

    public boolean hasRule(String ruleName, String table) {
        return rules.containsKey("name:" + ruleName.toLowerCase() + ":" + table.toLowerCase());
    }

    public void removeRule(String ruleName, String table) {
        rules.remove("name:" + ruleName.toLowerCase() + ":" + table.toLowerCase());
        rules.remove("def:" + ruleName.toLowerCase());
    }

    public void addRuleDefinition(String ruleName, String table, String definition) {
        rules.put("def:" + ruleName.toLowerCase(), table.toLowerCase() + "|" + definition);
    }

    public java.util.Map<String, String[]> getRuleDefinitions() {
        java.util.Map<String, String[]> result = new java.util.LinkedHashMap<>();
        for (java.util.Map.Entry<String, String> e : rules.entrySet()) {
            if (e.getKey().startsWith("def:")) {
                String ruleName = e.getKey().substring(4);
                int pipe = e.getValue().indexOf('|');
                if (pipe >= 0) {
                    result.put(ruleName, new String[]{ e.getValue().substring(0, pipe), e.getValue().substring(pipe + 1) });
                }
            }
        }
        return result;
    }

    public String getRule(String table, String event) {
        return rules.get(table.toLowerCase() + ":" + event.toUpperCase());
    }

    // Comments
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

    public Map<String, String> getComments() {
        return comments;
    }

    // Index metadata (for USING INDEX lookups)
    public void addIndex(String name, List<String> columns) {
        indexColumns.put(name.toLowerCase(), columns);
    }

    public void addIndexMeta(String name, String tableName, boolean isUnique) {
        indexTableNames.put(name.toLowerCase(), tableName);
        indexUniqueFlags.put(name.toLowerCase(), isUnique);
    }

    public void addIndexMeta(String name, String tableName, boolean isUnique, String method, String whereClause) {
        indexTableNames.put(name.toLowerCase(), tableName);
        indexUniqueFlags.put(name.toLowerCase(), isUnique);
        if (method != null) indexMethods.put(name.toLowerCase(), method);
        if (whereClause != null) indexWhereClauses.put(name.toLowerCase(), whereClause);
    }

    public void setIndexColumnOptions(String name, List<String> options) {
        if (options != null) indexColumnOptions.put(name.toLowerCase(), options);
    }

    public List<String> getIndexColumnOptions(String name) {
        return indexColumnOptions.get(name.toLowerCase());
    }

    public void setIndexIncludeColumns(String name, List<String> cols) {
        if (cols != null && !cols.isEmpty()) indexIncludeColumns.put(name.toLowerCase(), cols);
    }

    public List<String> getIndexIncludeColumns(String name) {
        return indexIncludeColumns.get(name.toLowerCase());
    }

    public void setIndexNullsNotDistinct(String name, boolean value) {
        if (value) indexNullsNotDistinct.put(name.toLowerCase(), true);
    }

    public boolean isIndexNullsNotDistinct(String name) {
        return indexNullsNotDistinct.getOrDefault(name.toLowerCase(), false);
    }

    public void setIndexParent(String childIndex, String parentIndex) {
        indexParentIndex.put(childIndex.toLowerCase(), parentIndex.toLowerCase());
    }

    public Map<String, String> getIndexParentMap() {
        return indexParentIndex;
    }

    public String getIndexMethod(String name) {
        return indexMethods.getOrDefault(name.toLowerCase(), "btree");
    }

    public String getIndexWhereClause(String name) {
        return indexWhereClauses.get(name.toLowerCase());
    }

    public String getIndexTable(String name) {
        return indexTableNames.get(name.toLowerCase());
    }

    public boolean isUniqueIndex(String name) {
        return indexUniqueFlags.getOrDefault(name.toLowerCase(), false);
    }

    public List<String> getIndexColumns(String name) {
        return indexColumns.get(name.toLowerCase());
    }

    public Map<String, String> getIndexReloptions(String name) {
        return indexReloptions.get(name.toLowerCase());
    }

    public void setIndexReloptions(String name, Map<String, String> opts) {
        indexReloptions.put(name.toLowerCase(), opts);
    }

    public void removeIndexReloptions(String name) {
        indexReloptions.remove(name.toLowerCase());
    }

    public boolean hasIndex(String name) {
        return indexColumns.containsKey(name.toLowerCase());
    }

    public void removeIndex(String name) {
        indexColumns.remove(name.toLowerCase());
        indexTableNames.remove(name.toLowerCase());
        indexUniqueFlags.remove(name.toLowerCase());
        indexWhereClauses.remove(name.toLowerCase());
        indexMethods.remove(name.toLowerCase());
        indexReloptions.remove(name.toLowerCase());
        indexColumnOptions.remove(name.toLowerCase());
        indexIncludeColumns.remove(name.toLowerCase());
        indexNullsNotDistinct.remove(name.toLowerCase());
    }

    /** Rename an index: re-key across all index maps and update schema registry. */
    public void renameIndex(String oldName, String newName) {
        String oldKey = oldName.toLowerCase();
        String newKey = newName.toLowerCase();
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
        // Update schema registry
        for (Map.Entry<String, Set<String>> entry : schemaObjectRegistry.entrySet()) {
            if (entry.getValue().remove("index:" + oldKey)) {
                entry.getValue().add("index:" + newKey);
            }
        }
        // Update object ownership key
        String oldOwner = objectOwners.remove("index:" + oldKey);
        if (oldOwner != null) objectOwners.put("index:" + newKey, oldOwner);
    }

    public Map<String, List<String>> getIndexColumns() {
        return indexColumns;
    }

    // Views
    public void addView(ViewDef view) {
        views.put(view.name().toLowerCase(), view);
    }

    public ViewDef getView(String name) {
        return views.get(name.toLowerCase());
    }

    public void removeView(String name) {
        views.remove(name.toLowerCase());
    }

    public boolean hasView(String name) {
        return views.containsKey(name.toLowerCase());
    }

    public Map<String, ViewDef> getViews() {
        return views;
    }

    public Map<String, Sequence> getSequences() {
        return sequences;
    }

    // Domain types
    public void addDomain(DomainType domain) {
        domains.put(domain.getName().toLowerCase(), domain);
    }

    public DomainType getDomain(String name) {
        return domains.get(name.toLowerCase());
    }

    public void removeDomain(String name) {
        domains.remove(name.toLowerCase());
    }

    public boolean isDomain(String name) {
        return domains.containsKey(name.toLowerCase());
    }

    public Map<String, DomainType> getDomains() {
        return domains;
    }

    // Advisory locks
    public boolean tryAdvisoryLock(long key, Session session) {
        Set<Session> holders = advisoryLocks.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet());
        // Synchronize on the holder set to make check-then-add atomic.
        // Without this, two sessions could both see an empty set, both pass
        // the check, and both add themselves — violating mutual exclusion.
        synchronized (holders) {
            // Exclusive lock: fail if any other session holds it (exclusive or shared)
            if (advisorySharedKeys.contains(key)) {
                // Key is held in shared mode by someone; exclusive lock fails if other sessions hold it
                for (Session holder : holders) {
                    if (holder != session) return false;
                }
            } else {
                for (Session holder : holders) {
                    if (holder != session) return false;
                }
            }
            holders.add(session);
            return true;
        }
    }

    /** Try to acquire a shared advisory lock. Multiple sessions can hold shared locks concurrently. */
    public boolean tryAdvisoryLockShared(long key, Session session) {
        Set<Session> holders = advisoryLocks.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet());
        synchronized (holders) {
            // Shared lock: fail only if another session holds an exclusive lock
            if (!advisorySharedKeys.contains(key) && !holders.isEmpty()) {
                // Key held exclusively by someone; check if it's us
                for (Session holder : holders) {
                    if (holder != session) return false;
                }
            }
            advisorySharedKeys.add(key);
            holders.add(session);
            return true;
        }
    }

    public boolean advisoryUnlock(long key, Session session) {
        Set<Session> holders = advisoryLocks.get(key);
        if (holders != null) {
            synchronized (holders) {
                boolean removed = holders.remove(session);
                if (holders.isEmpty()) {
                    advisorySharedKeys.remove(key);
                }
                return removed;
            }
        }
        return false;
    }

    public void advisoryUnlockAll(Session session) {
        for (Map.Entry<Long, Set<Session>> entry : advisoryLocks.entrySet()) {
            synchronized (entry.getValue()) {
                entry.getValue().remove(session);
                if (entry.getValue().isEmpty()) {
                    advisorySharedKeys.remove(entry.getKey());
                }
            }
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
            entries.add(new LockEntry(session, mode));
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
     * Checks whether following the wait-for chain starting at {@code blocker} eventually leads back to
     * {@code requester}, indicating a deadlock cycle.
     */
    private boolean hasDeadlock(Session requester, Session blocker) {
        Session current = blocker;
        while (current != null) {
            if (current == requester) return true;
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
        final long safetyTimeoutMs = 5_000L; // fallback when lock_timeout is 0 (disabled)
        long lockTimeoutMs = GucSettings.parseTimeoutMillis(session.getGucSettings().get("lock_timeout"));
        final long timeoutMs = lockTimeoutMs > 0 ? lockTimeoutMs : safetyTimeoutMs;
        final long pollMs = 10L;
        final long deadline = System.currentTimeMillis() + timeoutMs;

        Map<Object[], List<LockEntry>> locks =
                rowLocks.computeIfAbsent(tableName.toLowerCase(), k -> new IdentityHashMap<>());

        while (true) {
            Session blocker;
            synchronized (locks) {
                List<LockEntry> entries = locks.get(row);
                blocker = getBlockingSession(entries, session, mode);
                if (blocker == null) {
                    // Lock is available, acquire it
                    if (entries == null) {
                        entries = new ArrayList<>();
                        locks.put(row, entries);
                    }
                    entries.removeIf(e -> e.session == session);
                    entries.add(new LockEntry(session, mode));
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

            // Check timeout
            if (System.currentTimeMillis() >= deadline) {
                waitingFor.remove(session);
                throw new MemgresException("could not obtain lock on row in relation \"" + tableName + "\"", "55P03");
            }

            // Sleep briefly before retrying
            try {
                Thread.sleep(pollMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                waitingFor.remove(session);
                throw new MemgresException("interrupted while waiting for row lock", "57014");
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
        public final String sourceSQL;
        public final String checkOption;
        public final Map<String, String> reloptions;

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
