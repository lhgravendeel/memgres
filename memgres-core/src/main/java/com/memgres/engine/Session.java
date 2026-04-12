package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.Statement;

import java.util.*;
import java.util.concurrent.*;

/**
 * Per-connection session state. Tracks transaction status, undo log for rollback,
 * savepoints, prepared statements, and cursors. Each connection gets its own Session
 * with its own AstExecutor.
 */
public class Session {

    public enum TransactionStatus { IDLE, IN_TRANSACTION, FAILED }

    private final Database database;
    private final AstExecutor executor;
    private String databaseName = "memgres";
    private DatabaseRegistry databaseRegistry;
    private volatile TransactionStatus status = TransactionStatus.IDLE;
    private final List<UndoEntry> undoLog = new ArrayList<>();
    private final LinkedHashMap<String, Integer> savepoints = new LinkedHashMap<>();
    private final List<DeferredFkCheck> deferredFkChecks = new ArrayList<>();
    private boolean allConstraintsDeferred = false; // SET CONSTRAINTS ALL DEFERRED
    private boolean allConstraintsImmediate = false; // SET CONSTRAINTS ALL IMMEDIATE
    private final Set<String> immediateConstraintNames = new java.util.HashSet<>();
    private final Set<String> deferredConstraintNames = new java.util.HashSet<>(); // per-name overrides
    private final java.util.Queue<Notification> pendingNotifications = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private final List<Notification> deferredNotifications = new ArrayList<>(); // notifications pending COMMIT

    /** Pending notices (RAISE NOTICE/WARNING, DDL skipped notices) to be sent to the client. */
        public static final class PgNotice {
        public final String severity;
        public final String sqlState;
        public final String message;
        public final String hint;

        public PgNotice(String severity, String sqlState, String message, String hint) {
            this.severity = severity;
            this.sqlState = sqlState;
            this.message = message;
            this.hint = hint;
        }

        public String severity() { return severity; }
        public String sqlState() { return sqlState; }
        public String message() { return message; }
        public String hint() { return hint; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PgNotice that = (PgNotice) o;
            return java.util.Objects.equals(severity, that.severity)
                && java.util.Objects.equals(sqlState, that.sqlState)
                && java.util.Objects.equals(message, that.message)
                && java.util.Objects.equals(hint, that.hint);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(severity, sqlState, message, hint);
        }

        @Override
        public String toString() {
            return "PgNotice[severity=" + severity + ", " + "sqlState=" + sqlState + ", " + "message=" + message + ", " + "hint=" + hint + "]";
        }
    }
    private final List<PgNotice> pendingNotices = new ArrayList<>();
    private final LinkedHashMap<String, Integer> savepointNotifCounts = new LinkedHashMap<>(); // savepoint → deferred notification count
    private final int pid = System.identityHashCode(this);
    private final String tempSchemaName = "pg_temp_" + Math.abs(System.identityHashCode(this));

    // Prepared statements: name -> PreparedStmt
    private final Map<String, PreparedStmt> preparedStatements = new LinkedHashMap<>();

    // Cursors: name -> CursorState
    private final Map<String, CursorState> cursors = new LinkedHashMap<>();

    // GUC settings for this session
    private final GucSettings gucSettings = new GucSettings();

    // Shared scheduler for statement_timeout cancellation (one per JVM is sufficient)
    private static final ScheduledExecutorService TIMEOUT_SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "memgres-stmt-timeout");
                t.setDaemon(true);
                return t;
            });

    // Transaction-scoped advisory locks: released on commit/rollback
    private final Set<Long> xactAdvisoryLocks = new LinkedHashSet<>();

    // Temp tables with ON COMMIT DROP: schema.table pairs to drop on commit
    private final List<String[]> onCommitDropTables = new ArrayList<>();
    // Temp tables with ON COMMIT DELETE ROWS: schema.table pairs to truncate on commit
    private final List<String[]> onCommitDeleteRowsTables = new ArrayList<>();

    // Session metadata for pg_stat_activity
    private String connectingUser;
    private String applicationName = "";
    private final java.time.OffsetDateTime backendStart = java.time.OffsetDateTime.now();
    private volatile String currentQuery;
    private volatile String state = "idle";
    private volatile java.time.OffsetDateTime queryStart;
    private volatile java.time.OffsetDateTime stateChange = java.time.OffsetDateTime.now();
    private volatile java.time.OffsetDateTime xactStart;
    // Transaction timestamp: frozen at BEGIN for now()/current_timestamp stability
    private java.time.OffsetDateTime transactionTimestamp = null;

    /** Stored prepared statement. inferredParamCount is the max $N index found in body when no explicit types are given. */
        public static final class PreparedStmt {
        public final String name;
        public final List<String> paramTypes;
        public final Statement body;
        public final int inferredParamCount;
        public final String sqlText;
        public final java.time.OffsetDateTime prepareTime;
        public final boolean fromSql;
        public final List<String> resultTypes;
        /** Execution counter: Memgres has no planner, so all executions count as custom plans (PG 14+). */
        private final java.util.concurrent.atomic.AtomicLong customPlanCount = new java.util.concurrent.atomic.AtomicLong(0);

        public PreparedStmt(String name, List<String> paramTypes, Statement body, int inferredParamCount,
                            String sqlText, java.time.OffsetDateTime prepareTime, boolean fromSql,
                            List<String> resultTypes) {
            this.name = name;
            this.paramTypes = paramTypes;
            this.body = body;
            this.inferredParamCount = inferredParamCount;
            this.sqlText = sqlText;
            this.prepareTime = prepareTime;
            this.fromSql = fromSql;
            this.resultTypes = resultTypes;
        }

        public PreparedStmt(String name, List<String> paramTypes, Statement body, int inferredParamCount,
                            String sqlText, java.time.OffsetDateTime prepareTime, boolean fromSql) {
            this(name, paramTypes, body, inferredParamCount, sqlText, prepareTime, fromSql, null);
        }

        public PreparedStmt(String name, List<String> paramTypes, Statement body, int inferredParamCount) {
            this(name, paramTypes, body, inferredParamCount, null, java.time.OffsetDateTime.now(), true);
        }

        public PreparedStmt(String name, List<String> paramTypes, Statement body) {
            this(name, paramTypes, body, 0);
        }

        public String name() { return name; }
        public List<String> paramTypes() { return paramTypes; }
        public Statement body() { return body; }
        public int inferredParamCount() { return inferredParamCount; }
        public String sqlText() { return sqlText; }
        public java.time.OffsetDateTime prepareTime() { return prepareTime; }
        public boolean fromSql() { return fromSql; }
        public List<String> resultTypes() { return resultTypes; }
        /** Increment execution counter (called on each EXECUTE). */
        public void recordExecution() { customPlanCount.incrementAndGet(); }
        /** Get custom plan execution count (PG 14+). */
        public long customPlans() { return customPlanCount.get(); }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PreparedStmt that = (PreparedStmt) o;
            return java.util.Objects.equals(name, that.name)
                && java.util.Objects.equals(paramTypes, that.paramTypes)
                && java.util.Objects.equals(body, that.body)
                && inferredParamCount == that.inferredParamCount;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, paramTypes, body, inferredParamCount);
        }

        @Override
        public String toString() {
            return "PreparedStmt[name=" + name + ", " + "paramTypes=" + paramTypes + ", " + "body=" + body + ", " + "inferredParamCount=" + inferredParamCount + "]";
        }
    }

    /** Cursor state: stores query results and current position. */
    public static class CursorState {
        private final String name;
        private final List<Column> columns;
        private final List<Object[]> rows;
        private int position = -1; // before first row
        private final String queryText;
        private final boolean holdable;
        private final boolean binary;
        private final boolean scrollable;
        private final java.time.OffsetDateTime creationTime;

        public CursorState(String name, List<Column> columns, List<Object[]> rows,
                           String queryText, boolean holdable, boolean binary, boolean scrollable) {
            this.name = name;
            this.columns = columns;
            this.rows = rows;
            this.queryText = queryText;
            this.holdable = holdable;
            this.binary = binary;
            this.scrollable = scrollable;
            this.creationTime = java.time.OffsetDateTime.now();
        }

        public CursorState(String name, List<Column> columns, List<Object[]> rows) {
            this(name, columns, rows, null, false, false, false);
        }

        public String getName() { return name; }
        public List<Column> getColumns() { return columns; }
        public int getRowCount() { return rows.size(); }
        public int getPosition() { return position; }
        public String getQueryText() { return queryText; }
        public boolean isHoldable() { return holdable; }
        public boolean isBinary() { return binary; }
        public boolean isScrollable() { return scrollable; }
        public java.time.OffsetDateTime getCreationTime() { return creationTime; }

        /** Get row at index, or null if out of bounds. */
        public Object[] getRow(int idx) {
            if (idx >= 0 && idx < rows.size()) return rows.get(idx);
            return null;
        }

        public void setPosition(int pos) { this.position = pos; }
    }

    // MVCC: uncommitted inserts per table (schema.table -> set of row references)
    // ConcurrentHashMap + ConcurrentHashMap.newKeySet inner sets for thread-safe cross-session reads.
    private volatile Map<String, Set<Object[]>> uncommittedInserts = new ConcurrentHashMap<>();
    // MVCC: uncommitted updates per table (schema.table -> map of current row -> old values)
    // ConcurrentHashMap for thread-safe cross-session reads in isRowBeingUpdatedByOtherSession.
    private volatile Map<String, Map<Object[], Object[]>> uncommittedUpdates = new ConcurrentHashMap<>();
    // MVCC: uncommitted deletes per table (schema.table -> list of deleted rows)
    // ConcurrentHashMap + CopyOnWriteArrayList inner lists for thread-safe cross-session reads.
    private volatile Map<String, List<Object[]>> uncommittedDeletes = new ConcurrentHashMap<>();
    // MVCC: snapshots for REPEATABLE READ (schema.table -> snapshot of rows at first read)
    private final Map<String, List<Object[]>> rrSnapshots = new LinkedHashMap<>();

    public Session(Database database) {
        this.database = database;
        this.executor = new AstExecutor(database, this);
        // Sync max_connections GUC with the actual database setting
        gucSettings.set("max_connections", String.valueOf(database.getMaxConnections()));
        // Register with database for MVCC visibility
        database.registerSession(this);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String name) {
        this.databaseName = name;
    }

    public DatabaseRegistry getDatabaseRegistry() {
        return databaseRegistry;
    }

    public void setDatabaseRegistry(DatabaseRegistry registry) {
        this.databaseRegistry = registry;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    /**
     * Restore session status to the given value.
     * Used by the wire protocol layer after metadata-only execution that should not affect transaction state.
     */
    public void restoreStatus(TransactionStatus saved) {
        this.status = saved;
    }

    /**
     * Returns the PostgreSQL ReadyForQuery status byte.
     */
    public char getReadyForQueryStatus() {
        switch (status) {
            case IDLE:
                return 'I';
            case IN_TRANSACTION:
                return 'T';
            case FAILED:
                return 'E';
            default:
                throw new IllegalStateException("Unknown status: " + status);
        }
    }

    /** Insert a single row during COPY FROM, called by PgWireHandler during copy-in mode.
     *  Returns the inserted row Object[] for atomicity tracking (null if BEFORE trigger skipped). */
    public Object[] executeCopyFromRow(com.memgres.engine.parser.ast.CopyStmt stmt, java.util.List<String> values) {
        return executor.dmlExecutor.executeCopyFromRow(stmt, values);
    }

    /** Split an optionally schema-qualified name into {schema, table}. */
    private String[] splitSchemaTable(String name) {
        if (name != null && name.contains(".")) {
            int dot = name.indexOf('.');
            return new String[]{name.substring(0, dot), name.substring(dot + 1)};
        }
        return new String[]{"public", name};
    }

    /** Get column count for a table, used by PgWireHandler for CopyInResponse. */
    public int getTableColumnCount(String tableName) {
        String[] st = splitSchemaTable(tableName);
        Table table = executor.resolveTable(st[0], st[1]);
        return table.getColumns().size();
    }

    /** Resolve a table by name, used by PgWireHandler for binary COPY type resolution. */
    public Table resolveTable(String tableName) {
        String[] st = splitSchemaTable(tableName);
        return executor.resolveTable(st[0], st[1]);
    }

    /** Delete specific rows from a table, used for COPY atomicity rollback. */
    public void deleteInsertedRows(String tableName, java.util.Set<Object[]> rows) {
        String[] st = splitSchemaTable(tableName);
        Table table = executor.resolveTable(st[0], st[1]);
        table.deleteRows(rows);
    }

    public QueryResult execute(String sql) {
        return execute(sql, Cols.listOf());
    }

    public QueryResult execute(String sql, List<Object> parameters) {
        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        if (sql.isEmpty()) {
            return QueryResult.empty();
        }

        // Check if this is a transaction command (allowed even in FAILED state)
        String upper = sql.toUpperCase().trim();
        boolean isTransactionCmd = upper.startsWith("BEGIN") || upper.startsWith("START TRANSACTION")
                || upper.startsWith("COMMIT") || upper.startsWith("END")
                || upper.startsWith("ROLLBACK") || upper.startsWith("SAVEPOINT")
                || upper.startsWith("RELEASE") || upper.startsWith("PREPARE TRANSACTION");

        // In FAILED state, only ROLLBACK (and SAVEPOINT-related) commands are allowed
        if (status == TransactionStatus.FAILED && !isTransactionCmd) {
            throw new MemgresException(
                    "current transaction is aborted, commands ignored until end of transaction block",
                    "25P02");
        }

        // statement_timeout: schedule a thread interrupt if timeout > 0
        long timeoutMs = 0;
        if (!isTransactionCmd) {
            String timeoutVal = gucSettings.get("statement_timeout");
            timeoutMs = GucSettings.parseTimeoutMillis(timeoutVal);
        }

        Thread execThread = Thread.currentThread();
        ScheduledFuture<?> timeoutTask = null;
        if (timeoutMs > 0) {
            final Thread t = execThread;
            timeoutTask = TIMEOUT_SCHEDULER.schedule(t::interrupt, timeoutMs, TimeUnit.MILLISECONDS);
        }

        try {
            QueryResult result = executor.execute(sql, parameters);
            // If thread was interrupted but returned normally, clear and ignore
            // (the timeout fired after the statement completed)
            Thread.interrupted(); // clear interrupt flag
            return result;
        } catch (MemgresException e) {
            Thread.interrupted(); // clear interrupt flag
            if (status == TransactionStatus.IN_TRANSACTION) {
                status = TransactionStatus.FAILED;
            }
            // For deadlock (40P01), automatically release this session's row locks so the
            // waiting session can proceed (mirrors PostgreSQL's automatic victim rollback).
            if ("40P01".equals(e.getSqlState())) {
                database.unlockAllRows(this);
            }
            throw e;
        } catch (RuntimeException e) {
            Thread.interrupted(); // clear interrupt flag
            if (status == TransactionStatus.IN_TRANSACTION) {
                status = TransactionStatus.FAILED;
            }
            // If the execution thread was interrupted (by our timeout or by cancel),
            // wrap the exception as a query_canceled error (SQLSTATE 57014)
            if (e.getCause() instanceof InterruptedException
                    || e instanceof MemgresException && "57014".equals(((MemgresException) e).getSqlState())) {
                MemgresException me = (MemgresException) e;
                MemgresException canceled = new MemgresException("canceling statement due to statement timeout", "57014");
                if (status == TransactionStatus.IN_TRANSACTION) {
                    status = TransactionStatus.FAILED;
                }
                throw canceled;
            }
            throw e;
        } finally {
            if (timeoutTask != null) {
                timeoutTask.cancel(false);
            }
        }
    }

    /**
     * Try to infer SELECT column metadata without fully executing the query.
     * Returns null if the SQL is not a SELECT or column inference fails.
     */
    public QueryResult tryInferSelectColumns(String sql) {
        try {
            sql = sql.trim();
            if (sql.endsWith(";")) sql = sql.substring(0, sql.length() - 1).trim();
            Statement stmt = com.memgres.engine.parser.Parser.parse(sql);
            if (stmt instanceof com.memgres.engine.parser.ast.SelectStmt && ((com.memgres.engine.parser.ast.SelectStmt) stmt).targets() != null) {
                com.memgres.engine.parser.ast.SelectStmt sel = (com.memgres.engine.parser.ast.SelectStmt) stmt;
                List<Column> columns = new ArrayList<>();
                for (com.memgres.engine.parser.ast.SelectStmt.SelectTarget target : sel.targets()) {
                    String alias = target.alias();
                    if (alias == null) alias = executor.exprToAlias(target.expr());
                    DataType type = executor.inferExprType(target.expr());
                    columns.add(new Column(alias, type, true, false, null));
                }
                return QueryResult.select(columns, new ArrayList<>());
            }
        } catch (Exception ignored) {}
        return null;
    }

    // ---- Transaction lifecycle ----

    public void begin() {
        if (status == TransactionStatus.IN_TRANSACTION || status == TransactionStatus.FAILED) {
            // Already in a transaction (or failed state); PostgreSQL issues a WARNING but doesn't error
            return;
        }
        status = TransactionStatus.IN_TRANSACTION;
        transactionTimestamp = java.time.OffsetDateTime.now();
        undoLog.clear();
        savepoints.clear();
    }

    /** Returns the transaction start timestamp (frozen for now()/current_timestamp stability), or null if not in a transaction. */
    public java.time.OffsetDateTime getTransactionTimestamp() {
        return transactionTimestamp;
    }

    public void commit() {
        // Validate deferred constraints before committing
        try {
            // First, validate deferred PK/UNIQUE constraints (whole-table scan, deduplicated)
            Set<String> validatedUnique = new java.util.HashSet<>();
            for (DeferredFkCheck check : deferredFkChecks) {
                StoredConstraint sc = check.constraint();
                if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE) {
                    String key = System.identityHashCode(check.table()) + ":" + sc.getName();
                    if (validatedUnique.add(key)) {
                        executor.constraintValidator.validateDeferredUniqueness(check.table(), sc);
                    }
                }
            }
            // Then validate other deferred constraints (CHECK, FK, EXCLUDE)
            for (DeferredFkCheck check : deferredFkChecks) {
                StoredConstraint sc = check.constraint();
                if (sc.getType() != StoredConstraint.Type.PRIMARY_KEY && sc.getType() != StoredConstraint.Type.UNIQUE) {
                    executor.constraintValidator.validateDeferredConstraint(check.table(), check.row(), sc);
                }
            }
        } catch (MemgresException e) {
            // Deferred constraint failed, rollback
            rollback();
            throw e;
        }
        // Swap MVCC maps to new empty instances (atomic from cross-session readers' perspective)
        uncommittedInserts = new ConcurrentHashMap<>();
        uncommittedUpdates = new ConcurrentHashMap<>();
        uncommittedDeletes = new ConcurrentHashMap<>();
        rrSnapshots.clear();
        // Clear transaction-scoped GUC overrides (SET LOCAL)
        gucSettings.clearTransactionOverrides();
        // Reset per-transaction GUCs (transaction_read_only, transaction_isolation)
        gucSettings.reset("transaction_read_only");
        gucSettings.reset("transaction_isolation");
        // Discard undo log; changes are permanent
        undoLog.clear();
        savepoints.clear();
        deferredFkChecks.clear();
        allConstraintsDeferred = false;
        allConstraintsImmediate = false;
        deferredConstraintNames.clear();
        immediateConstraintNames.clear();
        // Flush deferred notifications; they are now committed
        for (Notification n : deferredNotifications) {
            database.getNotificationManager().notify(n.channel(), n.payload(), n.pid());
        }
        deferredNotifications.clear();
        savepointNotifCounts.clear();
        savepointMvccSnapshots.clear();
        // Drop temp tables with ON COMMIT DROP
        for (String[] pair : onCommitDropTables) {
            Schema s = database.getSchema(pair[0]);
            if (s != null) s.removeTable(pair[1]);
        }
        onCommitDropTables.clear();
        // Truncate temp tables with ON COMMIT DELETE ROWS
        for (String[] pair : onCommitDeleteRowsTables) {
            Schema s = database.getSchema(pair[0]);
            if (s != null) {
                Table t = s.getTable(pair[1]);
                if (t != null) t.clearRows();
            }
        }
        // Note: don't clear onCommitDeleteRowsTables because the table persists across transactions
        // Release transaction-scoped advisory locks
        releaseXactAdvisoryLocks();
        // Release all row-level locks held by this session
        database.unlockAllRows(this);
        // Destroy non-holdable cursors (PG behavior: only WITH HOLD cursors survive COMMIT)
        destroyNonHoldableCursors();
        transactionTimestamp = null;
        status = TransactionStatus.IDLE;
    }

    public void rollback() {
        // Swap MVCC maps to new empty instances (atomic from cross-session readers' perspective)
        uncommittedInserts = new ConcurrentHashMap<>();
        uncommittedUpdates = new ConcurrentHashMap<>();
        uncommittedDeletes = new ConcurrentHashMap<>();
        rrSnapshots.clear();
        // Clear transaction-scoped GUC overrides (SET LOCAL)
        gucSettings.clearTransactionOverrides();
        // Reset per-transaction GUCs (transaction_read_only, transaction_isolation)
        gucSettings.reset("transaction_read_only");
        gucSettings.reset("transaction_isolation");
        // Apply undo log in reverse
        applyUndo(0);
        undoLog.clear();
        savepoints.clear();
        deferredFkChecks.clear();
        allConstraintsDeferred = false;
        allConstraintsImmediate = false;
        deferredConstraintNames.clear();
        immediateConstraintNames.clear();
        // Discard deferred notifications; transaction was rolled back
        deferredNotifications.clear();
        savepointNotifCounts.clear();
        savepointMvccSnapshots.clear();
        onCommitDropTables.clear();
        // Release transaction-scoped advisory locks
        releaseXactAdvisoryLocks();
        // Release all row-level locks held by this session
        database.unlockAllRows(this);
        // Destroy all cursors on rollback (PG behavior: all cursors destroyed on ROLLBACK)
        cursors.clear();
        transactionTimestamp = null;
        status = TransactionStatus.IDLE;
    }

    // MVCC map snapshots per savepoint — used to restore MVCC state on ROLLBACK TO SAVEPOINT
    private final Map<String, MvccSnapshot> savepointMvccSnapshots = new LinkedHashMap<>();

    public void savepoint(String name) {
        if (status != TransactionStatus.IN_TRANSACTION) {
            // Implicit BEGIN
            begin();
        }
        String key = name.toLowerCase();
        savepoints.put(key, undoLog.size());
        savepointNotifCounts.put(key, deferredNotifications.size());
        // Snapshot current MVCC maps so we can restore on ROLLBACK TO SAVEPOINT.
        // Deep-copy the outer maps; inner collections are identity-based.
        savepointMvccSnapshots.put(key, MvccSnapshot.capture(uncommittedInserts, uncommittedUpdates, uncommittedDeletes));
    }

    /** Snapshot of MVCC tracking maps at savepoint creation time. */
    private static class MvccSnapshot {
        final Map<String, Set<Object[]>> inserts;
        final Map<String, Map<Object[], Object[]>> updates;
        final Map<String, List<Object[]>> deletes;

        MvccSnapshot(Map<String, Set<Object[]>> inserts,
                     Map<String, Map<Object[], Object[]>> updates,
                     Map<String, List<Object[]>> deletes) {
            this.inserts = inserts;
            this.updates = updates;
            this.deletes = deletes;
        }

        static MvccSnapshot capture(Map<String, Set<Object[]>> inserts,
                                     Map<String, Map<Object[], Object[]>> updates,
                                     Map<String, List<Object[]>> deletes) {
            // Deep-copy: new CHM with copies of inner synchronized collections
            Map<String, Set<Object[]>> iCopy = new ConcurrentHashMap<>();
            for (Map.Entry<String, Set<Object[]>> e : inserts.entrySet()) {
                Set<Object[]> copy = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));
                synchronized (e.getValue()) {
                    copy.addAll(e.getValue());
                }
                iCopy.put(e.getKey(), copy);
            }
            Map<String, Map<Object[], Object[]>> uCopy = new ConcurrentHashMap<>();
            for (Map.Entry<String, Map<Object[], Object[]>> e : updates.entrySet()) {
                synchronized (e.getValue()) {
                    uCopy.put(e.getKey(), Collections.synchronizedMap(new IdentityHashMap<>(e.getValue())));
                }
            }
            Map<String, List<Object[]>> dCopy = new ConcurrentHashMap<>();
            for (Map.Entry<String, List<Object[]>> e : deletes.entrySet()) {
                synchronized (e.getValue()) {
                    dCopy.put(e.getKey(), Collections.synchronizedList(new ArrayList<>(e.getValue())));
                }
            }
            return new MvccSnapshot(iCopy, uCopy, dCopy);
        }
    }

    public void releaseSavepoint(String name) {
        if (status == TransactionStatus.IDLE) {
            throw new MemgresException("RELEASE SAVEPOINT can only be used in transaction blocks", "25P01");
        }
        String key = name.toLowerCase();
        if (!savepoints.containsKey(key)) {
            throw new MemgresException("savepoint \"" + name + "\" does not exist", "3B001");
        }
        savepoints.remove(key);
    }

    public void rollbackToSavepoint(String name) {
        if (status == TransactionStatus.IDLE) {
            throw new MemgresException("ROLLBACK TO SAVEPOINT can only be used in transaction blocks", "25P01");
        }
        String key = name.toLowerCase();
        if (!savepoints.containsKey(key)) {
            throw new MemgresException("savepoint \"" + name + "\" does not exist", "3B001");
        }
        int position = savepoints.get(key);

        // Restore MVCC maps to their state at savepoint creation time.
        // This must happen BEFORE applyUndo so that concurrent readers see consistent
        // MVCC visibility during the undo process. The volatile swap is atomic from
        // the perspective of cross-session reads.
        MvccSnapshot snapshot = savepointMvccSnapshots.get(key);
        if (snapshot != null) {
            uncommittedInserts = snapshot.inserts;
            uncommittedUpdates = snapshot.updates;
            uncommittedDeletes = snapshot.deletes;
        }

        applyUndo(position);

        // Truncate deferred notifications to the savepoint's count
        Integer notifCount = savepointNotifCounts.get(key);
        if (notifCount != null && notifCount < deferredNotifications.size()) {
            deferredNotifications.subList(notifCount, deferredNotifications.size()).clear();
        }

        // Remove savepoints created after this one
        List<String> toRemove = new ArrayList<>();
        boolean found = false;
        for (String sp : savepoints.keySet()) {
            if (found) {
                toRemove.add(sp);
            }
            if (sp.equals(key)) {
                found = true;
            }
        }
        for (String sp : toRemove) {
            savepoints.remove(sp);
            savepointNotifCounts.remove(sp);
            savepointMvccSnapshots.remove(sp);
        }

        // Transaction is no longer in FAILED state after rolling back to savepoint
        if (status == TransactionStatus.FAILED) {
            status = TransactionStatus.IN_TRANSACTION;
        }
    }


    public boolean isInTransaction() {
        return status == TransactionStatus.IN_TRANSACTION || status == TransactionStatus.FAILED;
    }

    // Notice support (RAISE NOTICE/WARNING, DDL skipped notices)
    public void addNotice(String severity, String sqlState, String message, String hint) {
        pendingNotices.add(new PgNotice(severity, sqlState, message, hint));
    }

    public List<PgNotice> drainPendingNotices() {
        if (pendingNotices.isEmpty()) return Cols.listOf();
        List<PgNotice> drained = new ArrayList<>(pendingNotices);
        pendingNotices.clear();
        return drained;
    }

    // Notification support
    public void addNotification(Notification notification) {
        pendingNotifications.add(notification);
    }

    public java.util.Queue<Notification> getPendingNotifications() {
        return pendingNotifications;
    }

    /**
     * Queue a notification for delivery. If inside a transaction, the notification
     * is deferred until COMMIT (PG behavior). If in autocommit mode (IDLE), the
     * notification is sent immediately via the NotificationManager.
     */
    public void queueNotification(String channel, String payload) {
        Notification n = new Notification(pid, channel, payload != null ? payload : "");
        if (status == TransactionStatus.IDLE) {
            // Autocommit mode, deliver immediately
            database.getNotificationManager().notify(channel, payload != null ? payload : "", pid);
        } else {
            // Inside transaction, defer until COMMIT
            deferredNotifications.add(n);
        }
    }

    /** Get the number of deferred notifications (for savepoint tracking). */
    int getDeferredNotificationCount() {
        return deferredNotifications.size();
    }

    public int getPid() {
        return pid;
    }

    public Database getDatabase() {
        return database;
    }

    public GucSettings getGucSettings() {
        return gucSettings;
    }

    /** Returns the unique temp schema name for this session. */
    public String getTempSchemaName() {
        return tempSchemaName;
    }

    // ---- pg_stat_activity metadata ----
    public String getConnectingUser() { return connectingUser; }
    public void setConnectingUser(String u) { this.connectingUser = u; }
    public String getApplicationName() { return applicationName; }
    public void setApplicationName(String n) { this.applicationName = n != null ? n : ""; }
    public java.time.OffsetDateTime getBackendStart() { return backendStart; }
    public String getCurrentQuery() { return currentQuery; }
    public String getState() { return state; }
    public java.time.OffsetDateTime getQueryStart() { return queryStart; }
    public java.time.OffsetDateTime getStateChange() { return stateChange; }
    public java.time.OffsetDateTime getXactStart() { return xactStart; }
    public void setQueryState(String query) {
        this.currentQuery = query;
        this.state = "active";
        this.queryStart = java.time.OffsetDateTime.now();
        this.stateChange = this.queryStart;
    }
    public void setIdleState() {
        this.state = status == TransactionStatus.IN_TRANSACTION ? "idle in transaction"
                : status == TransactionStatus.FAILED ? "idle in transaction (aborted)" : "idle";
        this.stateChange = java.time.OffsetDateTime.now();
    }
    public void setXactStart(java.time.OffsetDateTime t) { this.xactStart = t; }
    public void clearXactStart() { this.xactStart = null; }

    /** Clean up session on disconnect: rollback uncommitted work, drop temp objects, unregister. */
    public void close() {
        if (isInTransaction()) {
            rollback();
        }
        // Explicitly clear session-scoped state
        cursors.clear();
        preparedStatements.clear();
        dropTempObjects();
        database.unregisterSession(this);
    }

    public void dropTempObjects() {
        Schema tempSchema = database.getSchema(tempSchemaName);
        if (tempSchema != null) {
            for (String tableName : new java.util.ArrayList<>(tempSchema.getTables().keySet())) {
                tempSchema.removeTable(tableName);
            }
            database.removeSchema(tempSchemaName);
        }
        // Also remove any temp sequences
        database.removeSequencesWithPrefix(tempSchemaName + ".");
    }

    /**
     * Returns the current effective schema, the first valid schema in search_path.
     * This is what PG returns from current_schema().
     */
    public String getEffectiveSchema() {
        String searchPath = gucSettings.get("search_path");
        if (searchPath != null) {
            for (String sp : searchPath.split(",")) {
                String s = sp.trim().replace("\"", "").replace("'", "");
                if (s.isEmpty() || s.equals("$user")) continue;
                // pg_catalog is always a valid schema (virtual, not stored in Database)
                if ("pg_catalog".equals(s) || "information_schema".equals(s)
                        || database.getSchema(s) != null) return s;
            }
        }
        return "public";
    }

    /**
     * Returns the full effective search path as an ordered list of schema names.
     * Matches PG's current_schemas() behavior.
     */
    public List<String> getEffectiveSearchPath(boolean includeImplicit) {
        List<String> result = new ArrayList<>();
        if (includeImplicit) result.add("pg_catalog");
        String searchPath = gucSettings.get("search_path");
        if (searchPath != null) {
            for (String sp : searchPath.split(",")) {
                String s = sp.trim().replace("\"", "").replace("'", "");
                if (s.isEmpty() || s.equals("$user")) continue;
                if (!result.contains(s)) result.add(s);
            }
        }
        if (result.isEmpty() || (!result.contains("public") && result.size() == 1 && result.get(0).equals("pg_catalog"))) {
            result.add("public");
        }
        return result;
    }

    // ---- Prepared statements ----

    public void addPreparedStatement(String name, PreparedStmt stmt) {
        preparedStatements.put(name.toLowerCase(), stmt);
    }

    public PreparedStmt getPreparedStatement(String name) {
        return preparedStatements.get(name.toLowerCase());
    }

    public void removePreparedStatement(String name) {
        preparedStatements.remove(name.toLowerCase());
    }

    public void removeAllPreparedStatements() {
        preparedStatements.clear();
    }

    public Collection<PreparedStmt> getAllPreparedStatements() {
        return Collections.unmodifiableCollection(preparedStatements.values());
    }

    // ---- Cursors ----

    public void addCursor(String name, CursorState cursor) {
        cursors.put(name.toLowerCase(), cursor);
    }

    public CursorState getCursor(String name) {
        return cursors.get(name.toLowerCase());
    }

    public void removeCursor(String name) {
        cursors.remove(name.toLowerCase());
    }

    public void removeAllCursors() {
        cursors.clear();
    }

    /** Destroy non-holdable cursors at COMMIT time (PG behavior). WITH HOLD cursors survive. */
    public void destroyNonHoldableCursors() {
        cursors.entrySet().removeIf(e -> !e.getValue().isHoldable());
    }

    public Collection<CursorState> getAllCursors() {
        return Collections.unmodifiableCollection(cursors.values());
    }

    // ---- ON COMMIT DROP tables ----

    /** Track an advisory lock acquired with pg_advisory_xact_lock / pg_try_advisory_xact_lock. */
    public void addXactAdvisoryLock(long key) {
        xactAdvisoryLocks.add(key);
    }

    /** Release all transaction-scoped advisory locks. Called on commit/rollback. */
    private void releaseXactAdvisoryLocks() {
        for (Long key : xactAdvisoryLocks) {
            database.advisoryUnlock(key, this);
        }
        xactAdvisoryLocks.clear();
    }

    public void registerOnCommitDrop(String schema, String tableName) {
        onCommitDropTables.add(new String[]{schema, tableName});
    }

    public void registerOnCommitDeleteRows(String schema, String tableName) {
        onCommitDeleteRowsTables.add(new String[]{schema, tableName});
    }

    // ---- Deferred constraint checks ----

        public static final class DeferredFkCheck {
        public final Table table;
        public final Object[] row;
        public final StoredConstraint constraint;

        public DeferredFkCheck(Table table, Object[] row, StoredConstraint constraint) {
            this.table = table;
            this.row = row;
            this.constraint = constraint;
        }

        public Table table() { return table; }
        public Object[] row() { return row; }
        public StoredConstraint constraint() { return constraint; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeferredFkCheck that = (DeferredFkCheck) o;
            return java.util.Objects.equals(table, that.table)
                && java.util.Arrays.equals(row, that.row)
                && java.util.Objects.equals(constraint, that.constraint);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(table, java.util.Arrays.hashCode(row), constraint);
        }

        @Override
        public String toString() {
            return "DeferredFkCheck[table=" + table + ", " + "row=" + java.util.Arrays.toString(row) + ", " + "constraint=" + constraint + "]";
        }
    }

    public void addDeferredCheck(Table table, Object[] row, StoredConstraint constraint) {
        deferredFkChecks.add(new DeferredFkCheck(table, row, constraint));
    }

    public void setAllConstraintsDeferred(boolean deferred) {
        if (deferred) {
            this.allConstraintsDeferred = true;
            this.allConstraintsImmediate = false;
            immediateConstraintNames.clear();
        } else {
            // SET CONSTRAINTS ALL IMMEDIATE
            this.allConstraintsImmediate = true;
            this.allConstraintsDeferred = false;
            deferredConstraintNames.clear();
        }
    }

    public void setConstraintDeferred(String constraintName, boolean deferred) {
        String lcName = constraintName.toLowerCase();
        if (deferred) {
            deferredConstraintNames.add(lcName);
            immediateConstraintNames.remove(lcName);
        } else {
            immediateConstraintNames.add(lcName);
            deferredConstraintNames.remove(lcName);
        }
    }

    /** Check if a constraint should be deferred right now (SET CONSTRAINTS overrides). */
    public boolean isConstraintCurrentlyDeferred(StoredConstraint sc) {
        if (!sc.isDeferrable()) return false;
        // Per-constraint explicit override takes priority
        if (sc.getName() != null) {
            String lcName = sc.getName().toLowerCase();
            if (deferredConstraintNames.contains(lcName)) return true;
            if (immediateConstraintNames.contains(lcName)) return false;
        }
        // SET CONSTRAINTS ALL overrides
        if (allConstraintsDeferred) return true;
        if (allConstraintsImmediate) return false;
        // Fall back to constraint's own INITIALLY DEFERRED setting
        return sc.isInitiallyDeferred();
    }

    // ---- MVCC visibility tracking ----

    /** Track an uncommitted insert for this session. */
    public void trackUncommittedInsert(String schemaTable, Object[] row) {
        if (status == TransactionStatus.IN_TRANSACTION) {
            uncommittedInserts.computeIfAbsent(schemaTable,
                    k -> Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()))).add(row);
        }
    }

    /** Track an uncommitted update for this session. */
    public void trackUncommittedUpdate(String schemaTable, Object[] row, Object[] oldValues) {
        if (status == TransactionStatus.IN_TRANSACTION) {
            Map<Object[], Object[]> tableUpdates = uncommittedUpdates.computeIfAbsent(schemaTable,
                    k -> Collections.synchronizedMap(new IdentityHashMap<>()));
            // Only record the FIRST (original) old value; don't overwrite with intermediate values
            tableUpdates.putIfAbsent(row, oldValues);
        }
    }

    /** Track an uncommitted delete for this session. */
    public void trackUncommittedDelete(String schemaTable, List<Object[]> rows) {
        if (status == TransactionStatus.IN_TRANSACTION) {
            uncommittedDeletes.computeIfAbsent(schemaTable,
                    k -> Collections.synchronizedList(new ArrayList<>())).addAll(rows);
        }
    }

    /** Get uncommitted inserts for a table.
     *  Returns a snapshot copy safe for cross-thread iteration.
     *  Collections.synchronizedSet.iterator() is NOT synchronized, so iterating
     *  the live set from another thread while the owning session adds entries would
     *  race on the underlying IdentityHashMap. We copy under the set's monitor. */
    public Set<Object[]> getUncommittedInserts(String schemaTable) {
        Set<Object[]> live = uncommittedInserts.get(schemaTable);
        if (live == null) return Collections.emptySet();
        Set<Object[]> copy = Collections.newSetFromMap(new IdentityHashMap<>());
        synchronized (live) {
            copy.addAll(live);
        }
        return copy;
    }

    /** Get uncommitted updates for a table (current row -> old values).
     *  Returns a snapshot copy safe for cross-thread iteration. */
    public Map<Object[], Object[]> getUncommittedUpdates(String schemaTable) {
        Map<Object[], Object[]> live = uncommittedUpdates.get(schemaTable);
        if (live == null) return Collections.emptyMap();
        synchronized (live) {
            return new IdentityHashMap<>(live);
        }
    }

    /** Get all uncommitted updates across all tables (schemaTable -> (current row -> old values)). */
    public Map<String, Map<Object[], Object[]>> getAllUncommittedUpdates() {
        return uncommittedUpdates;
    }

    /** Get uncommitted deletes for a table.
     *  Returns a snapshot copy safe for cross-thread iteration. */
    public List<Object[]> getUncommittedDeletes(String schemaTable) {
        List<Object[]> live = uncommittedDeletes.get(schemaTable);
        if (live == null) return Collections.emptyList();
        synchronized (live) {
            return new ArrayList<>(live);
        }
    }

    /** Get or create a REPEATABLE READ snapshot for a table. Returns null if not in RR mode. */
    public List<Object[]> getOrCreateRRSnapshot(String schemaTable, List<Object[]> currentVisibleRows) {
        String isolation = getEffectiveIsolationLevel();
        if (!"repeatable read".equals(isolation) && !"serializable".equals(isolation)) {
            return null; // Not in RR/SERIALIZABLE mode
        }
        if (status != TransactionStatus.IN_TRANSACTION) {
            return null; // Not in a transaction
        }
        return rrSnapshots.computeIfAbsent(schemaTable, k -> {
            // Deep copy the visible rows for the snapshot
            List<Object[]> snapshot = new ArrayList<>(currentVisibleRows.size());
            for (Object[] row : currentVisibleRows) {
                snapshot.add(Arrays.copyOf(row, row.length));
            }
            return snapshot;
        });
    }

    /** Check if a REPEATABLE READ snapshot already exists for this table. */
    public boolean hasRRSnapshot(String schemaTable) {
        return rrSnapshots.containsKey(schemaTable);
    }

    /** Get existing RR snapshot (returns null if none exists). */
    public List<Object[]> getRRSnapshot(String schemaTable) {
        return rrSnapshots.get(schemaTable);
    }

    /** Get the effective isolation level for this session's current transaction. */
    public String getEffectiveIsolationLevel() {
        // transaction_isolation (SET TRANSACTION) takes precedence if explicitly set
        if (gucSettings.hasSessionOverride("transaction_isolation")) {
            String txnLevel = gucSettings.get("transaction_isolation");
            if (txnLevel != null && !txnLevel.isEmpty()) return txnLevel.toLowerCase();
        }
        // Then check default_transaction_isolation (SET SESSION CHARACTERISTICS / setTransactionIsolation)
        String defaultLevel = gucSettings.get("default_transaction_isolation");
        if (defaultLevel != null && !defaultLevel.isEmpty()) return defaultLevel.toLowerCase();
        return "read committed";
    }

    /** Check if the current transaction is read-only. */
    public boolean isReadOnly() {
        String val = gucSettings.get("default_transaction_read_only");
        if ("on".equalsIgnoreCase(val)) return true;
        val = gucSettings.get("transaction_read_only");
        return "on".equalsIgnoreCase(val);
    }

    // ---- Undo log ----

    public void recordUndo(UndoEntry entry) {
        if (status == TransactionStatus.IN_TRANSACTION) {
            undoLog.add(entry);
        }
    }

    private void applyUndo(int fromPosition) {
        // Apply in reverse order from end to fromPosition
        for (int i = undoLog.size() - 1; i >= fromPosition; i--) {
            UndoEntry entry = undoLog.get(i);
            entry.undo(database);
        }
        // Truncate the undo log
        if (fromPosition < undoLog.size()) {
            undoLog.subList(fromPosition, undoLog.size()).clear();
        }
    }

    // ---- Undo entry types ----

    public interface UndoEntry {
        void undo(Database db);
    }

    /** Undo an INSERT by removing the row. */
        public static final class InsertUndo implements UndoEntry {
        public final String schema;
        public final String tableName;
        public final Object[] row;

        public InsertUndo(String schema, String tableName, Object[] row) {
            this.schema = schema;
            this.tableName = tableName;
            this.row = row;
        }

        @Override
        public void undo(Database db) {
            Schema s = db.getSchema(schema);
            if (s == null) return;
            Table table = s.getTable(tableName);
            if (table == null) return;
            table.removeRow(row);
        }

        public String schema() { return schema; }
        public String tableName() { return tableName; }
        public Object[] row() { return row; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InsertUndo that = (InsertUndo) o;
            return java.util.Objects.equals(schema, that.schema)
                && java.util.Objects.equals(tableName, that.tableName)
                && java.util.Arrays.equals(row, that.row);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(schema, tableName, java.util.Arrays.hashCode(row));
        }

        @Override
        public String toString() {
            return "InsertUndo[schema=" + schema + ", " + "tableName=" + tableName + ", " + "row=" + java.util.Arrays.toString(row) + "]";
        }
    }

    /** Undo a DELETE by re-inserting the rows. */
        public static final class DeleteUndo implements UndoEntry {
        public final String schema;
        public final String tableName;
        public final List<Object[]> rows;

        public DeleteUndo(String schema, String tableName, List<Object[]> rows) {
            this.schema = schema;
            this.tableName = tableName;
            this.rows = rows;
        }

        @Override
        public void undo(Database db) {
            Schema s = db.getSchema(schema);
            if (s == null) return;
            Table table = s.getTable(tableName);
            if (table == null) return;
            for (Object[] row : rows) {
                table.insertRow(row);
            }
        }

        public String schema() { return schema; }
        public String tableName() { return tableName; }
        public List<Object[]> rows() { return rows; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeleteUndo that = (DeleteUndo) o;
            return java.util.Objects.equals(schema, that.schema)
                && java.util.Objects.equals(tableName, that.tableName)
                && java.util.Objects.equals(rows, that.rows);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(schema, tableName, rows);
        }

        @Override
        public String toString() {
            return "DeleteUndo[schema=" + schema + ", " + "tableName=" + tableName + ", " + "rows=" + rows + "]";
        }
    }

    /** Undo an UPDATE by restoring old values. */
        public static final class UpdateUndo implements UndoEntry {
        public final String schema;
        public final String tableName;
        public final Object[] row;
        public final Object[] oldValues;

        public UpdateUndo(String schema, String tableName, Object[] row, Object[] oldValues) {
            this.schema = schema;
            this.tableName = tableName;
            this.row = row;
            this.oldValues = oldValues;
        }

        @Override
        public void undo(Database db) {
            Schema s = db.getSchema(schema);
            Table table = s != null ? s.getTable(tableName) : null;
            if (table != null) {
                Object[] currentValues = java.util.Arrays.copyOf(row, row.length);
                table.updateRowInPlace(row, currentValues, oldValues);
            } else {
                // Fallback: table might have been dropped
                System.arraycopy(oldValues, 0, row, 0, oldValues.length);
            }
        }

        public String schema() { return schema; }
        public String tableName() { return tableName; }
        public Object[] row() { return row; }
        public Object[] oldValues() { return oldValues; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            UpdateUndo that = (UpdateUndo) o;
            return java.util.Objects.equals(schema, that.schema)
                && java.util.Objects.equals(tableName, that.tableName)
                && java.util.Arrays.equals(row, that.row)
                && java.util.Arrays.equals(oldValues, that.oldValues);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(schema, tableName, java.util.Arrays.hashCode(row), java.util.Arrays.hashCode(oldValues));
        }

        @Override
        public String toString() {
            return "UpdateUndo[schema=" + schema + ", " + "tableName=" + tableName + ", " + "row=" + java.util.Arrays.toString(row) + ", " + "oldValues=" + java.util.Arrays.toString(oldValues) + "]";
        }
    }

    /** Undo a CREATE TABLE by dropping it. */
        public static final class CreateTableUndo implements UndoEntry {
        public final String schema;
        public final String tableName;

        public CreateTableUndo(String schema, String tableName) {
            this.schema = schema;
            this.tableName = tableName;
        }

        @Override
        public void undo(Database db) {
            Schema s = db.getSchema(schema);
            if (s != null) s.removeTable(tableName);
        }

        public String schema() { return schema; }
        public String tableName() { return tableName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CreateTableUndo that = (CreateTableUndo) o;
            return java.util.Objects.equals(schema, that.schema)
                && java.util.Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(schema, tableName);
        }

        @Override
        public String toString() {
            return "CreateTableUndo[schema=" + schema + ", " + "tableName=" + tableName + "]";
        }
    }

    /** Undo a DROP TABLE by re-adding it. */
        public static final class DropTableUndo implements UndoEntry {
        public final String schema;
        public final String tableName;
        public final Table table;

        public DropTableUndo(String schema, String tableName, Table table) {
            this.schema = schema;
            this.tableName = tableName;
            this.table = table;
        }

        @Override
        public void undo(Database db) {
            Schema s = db.getSchema(schema);
            if (s != null) s.addTable(table);
        }

        public String schema() { return schema; }
        public String tableName() { return tableName; }
        public Table table() { return table; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DropTableUndo that = (DropTableUndo) o;
            return java.util.Objects.equals(schema, that.schema)
                && java.util.Objects.equals(tableName, that.tableName)
                && java.util.Objects.equals(table, that.table);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(schema, tableName, table);
        }

        @Override
        public String toString() {
            return "DropTableUndo[schema=" + schema + ", " + "tableName=" + tableName + ", " + "table=" + table + "]";
        }
    }

    /** Undo a TRUNCATE by re-inserting rows and restoring serial counter. */
        public static final class TruncateUndo implements UndoEntry {
        public final String schema;
        public final String tableName;
        public final List<Object[]> rows;
        public final long serialCounter;

        public TruncateUndo(String schema, String tableName, List<Object[]> rows, long serialCounter) {
            this.schema = schema;
            this.tableName = tableName;
            this.rows = rows;
            this.serialCounter = serialCounter;
        }

        @Override
        public void undo(Database db) {
            Schema s = db.getSchema(schema);
            if (s == null) return;
            Table table = s.getTable(tableName);
            if (table == null) return;
            for (Object[] row : rows) {
                table.insertRow(row);
            }
            table.resetSerialCounter(serialCounter);
        }

        public String schema() { return schema; }
        public String tableName() { return tableName; }
        public List<Object[]> rows() { return rows; }
        public long serialCounter() { return serialCounter; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TruncateUndo that = (TruncateUndo) o;
            return java.util.Objects.equals(schema, that.schema)
                && java.util.Objects.equals(tableName, that.tableName)
                && java.util.Objects.equals(rows, that.rows)
                && serialCounter == that.serialCounter;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(schema, tableName, rows, serialCounter);
        }

        @Override
        public String toString() {
            return "TruncateUndo[schema=" + schema + ", " + "tableName=" + tableName + ", " + "rows=" + rows + ", " + "serialCounter=" + serialCounter + "]";
        }
    }

    /** Undo a CREATE SEQUENCE. */
        public static final class CreateSequenceUndo implements UndoEntry {
        public final String seqName;

        public CreateSequenceUndo(String seqName) {
            this.seqName = seqName;
        }

        @Override
        public void undo(Database db) {
            db.removeSequence(seqName);
        }

        public String seqName() { return seqName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CreateSequenceUndo that = (CreateSequenceUndo) o;
            return java.util.Objects.equals(seqName, that.seqName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(seqName);
        }

        @Override
        public String toString() {
            return "CreateSequenceUndo[seqName=" + seqName + "]";
        }
    }

    /** Undo a DROP SEQUENCE. */
        public static final class DropSequenceUndo implements UndoEntry {
        public final String seqName;
        public final Sequence seq;

        public DropSequenceUndo(String seqName, Sequence seq) {
            this.seqName = seqName;
            this.seq = seq;
        }

        @Override
        public void undo(Database db) {
            db.addSequence(seq);
        }

        public String seqName() { return seqName; }
        public Sequence seq() { return seq; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DropSequenceUndo that = (DropSequenceUndo) o;
            return java.util.Objects.equals(seqName, that.seqName)
                && java.util.Objects.equals(seq, that.seq);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(seqName, seq);
        }

        @Override
        public String toString() {
            return "DropSequenceUndo[seqName=" + seqName + ", " + "seq=" + seq + "]";
        }
    }

    /** Undo a CREATE VIEW. */
        public static final class CreateViewUndo implements UndoEntry {
        public final String viewName;

        public CreateViewUndo(String viewName) {
            this.viewName = viewName;
        }

        @Override
        public void undo(Database db) {
            db.removeView(viewName);
        }

        public String viewName() { return viewName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CreateViewUndo that = (CreateViewUndo) o;
            return java.util.Objects.equals(viewName, that.viewName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(viewName);
        }

        @Override
        public String toString() {
            return "CreateViewUndo[viewName=" + viewName + "]";
        }
    }

    /** Undo a DROP VIEW. */
        public static final class DropViewUndo implements UndoEntry {
        public final String viewName;
        public final Database.ViewDef view;

        public DropViewUndo(String viewName, Database.ViewDef view) {
            this.viewName = viewName;
            this.view = view;
        }

        @Override
        public void undo(Database db) {
            db.addView(view);
        }

        public String viewName() { return viewName; }
        public Database.ViewDef view() { return view; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DropViewUndo that = (DropViewUndo) o;
            return java.util.Objects.equals(viewName, that.viewName)
                && java.util.Objects.equals(view, that.view);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(viewName, view);
        }

        @Override
        public String toString() {
            return "DropViewUndo[viewName=" + viewName + ", " + "view=" + view + "]";
        }
    }

    /** Undo an ADD CONSTRAINT. */
        public static final class AddConstraintUndo implements UndoEntry {
        public final String schema;
        public final String tableName;
        public final String constraintName;

        public AddConstraintUndo(String schema, String tableName, String constraintName) {
            this.schema = schema;
            this.tableName = tableName;
            this.constraintName = constraintName;
        }

        @Override
        public void undo(Database db) {
            Schema s = db.getSchema(schema);
            if (s == null) return;
            Table table = s.getTable(tableName);
            if (table != null) table.removeConstraint(constraintName);
        }

        public String schema() { return schema; }
        public String tableName() { return tableName; }
        public String constraintName() { return constraintName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AddConstraintUndo that = (AddConstraintUndo) o;
            return java.util.Objects.equals(schema, that.schema)
                && java.util.Objects.equals(tableName, that.tableName)
                && java.util.Objects.equals(constraintName, that.constraintName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(schema, tableName, constraintName);
        }

        @Override
        public String toString() {
            return "AddConstraintUndo[schema=" + schema + ", " + "tableName=" + tableName + ", " + "constraintName=" + constraintName + "]";
        }
    }

    /** Undo a DROP CONSTRAINT. */
        public static final class DropConstraintUndo implements UndoEntry {
        public final String schema;
        public final String tableName;
        public final StoredConstraint constraint;

        public DropConstraintUndo(String schema, String tableName, StoredConstraint constraint) {
            this.schema = schema;
            this.tableName = tableName;
            this.constraint = constraint;
        }

        @Override
        public void undo(Database db) {
            Schema s = db.getSchema(schema);
            if (s == null) return;
            Table table = s.getTable(tableName);
            if (table != null) table.addConstraint(constraint);
        }

        public String schema() { return schema; }
        public String tableName() { return tableName; }
        public StoredConstraint constraint() { return constraint; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DropConstraintUndo that = (DropConstraintUndo) o;
            return java.util.Objects.equals(schema, that.schema)
                && java.util.Objects.equals(tableName, that.tableName)
                && java.util.Objects.equals(constraint, that.constraint);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(schema, tableName, constraint);
        }

        @Override
        public String toString() {
            return "DropConstraintUndo[schema=" + schema + ", " + "tableName=" + tableName + ", " + "constraint=" + constraint + "]";
        }
    }

    /** Undo ADD COLUMN by removing it. */
        public static final class AddColumnUndo implements UndoEntry {
        public final String schema;
        public final String tableName;
        public final String columnName;

        public AddColumnUndo(String schema, String tableName, String columnName) {
            this.schema = schema;
            this.tableName = tableName;
            this.columnName = columnName;
        }

        @Override
        public void undo(Database db) {
            Schema s = db.getSchema(schema);
            if (s == null) return;
            Table table = s.getTable(tableName);
            if (table != null) table.removeColumn(columnName);
        }

        public String schema() { return schema; }
        public String tableName() { return tableName; }
        public String columnName() { return columnName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AddColumnUndo that = (AddColumnUndo) o;
            return java.util.Objects.equals(schema, that.schema)
                && java.util.Objects.equals(tableName, that.tableName)
                && java.util.Objects.equals(columnName, that.columnName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(schema, tableName, columnName);
        }

        @Override
        public String toString() {
            return "AddColumnUndo[schema=" + schema + ", " + "tableName=" + tableName + ", " + "columnName=" + columnName + "]";
        }
    }

    /** Undo DROP COLUMN by re-adding it. */
        public static final class DropColumnUndo implements UndoEntry {
        public final String schema;
        public final String tableName;
        public final Column column;
        public final int position;
        public final List<Object> values;

        public DropColumnUndo(
                String schema,
                String tableName,
                Column column,
                int position,
                List<Object> values
        ) {
            this.schema = schema;
            this.tableName = tableName;
            this.column = column;
            this.position = position;
            this.values = values;
        }

        @Override
        public void undo(Database db) {
            Schema s = db.getSchema(schema);
            if (s == null) return;
            Table table = s.getTable(tableName);
            if (table != null) {
                table.addColumnAt(column, position, values);
            }
        }

        public String schema() { return schema; }
        public String tableName() { return tableName; }
        public Column column() { return column; }
        public int position() { return position; }
        public List<Object> values() { return values; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DropColumnUndo that = (DropColumnUndo) o;
            return java.util.Objects.equals(schema, that.schema)
                && java.util.Objects.equals(tableName, that.tableName)
                && java.util.Objects.equals(column, that.column)
                && position == that.position
                && java.util.Objects.equals(values, that.values);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(schema, tableName, column, position, values);
        }

        @Override
        public String toString() {
            return "DropColumnUndo[schema=" + schema + ", " + "tableName=" + tableName + ", " + "column=" + column + ", " + "position=" + position + ", " + "values=" + values + "]";
        }
    }

    /** Undo RENAME COLUMN. */
        public static final class RenameColumnUndo implements UndoEntry {
        public final String schema;
        public final String tableName;
        public final String newName;
        public final String oldName;

        public RenameColumnUndo(String schema, String tableName, String newName, String oldName) {
            this.schema = schema;
            this.tableName = tableName;
            this.newName = newName;
            this.oldName = oldName;
        }

        @Override
        public void undo(Database db) {
            Schema s = db.getSchema(schema);
            if (s == null) return;
            Table table = s.getTable(tableName);
            if (table != null) table.renameColumn(newName, oldName);
        }

        public String schema() { return schema; }
        public String tableName() { return tableName; }
        public String newName() { return newName; }
        public String oldName() { return oldName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RenameColumnUndo that = (RenameColumnUndo) o;
            return java.util.Objects.equals(schema, that.schema)
                && java.util.Objects.equals(tableName, that.tableName)
                && java.util.Objects.equals(newName, that.newName)
                && java.util.Objects.equals(oldName, that.oldName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(schema, tableName, newName, oldName);
        }

        @Override
        public String toString() {
            return "RenameColumnUndo[schema=" + schema + ", " + "tableName=" + tableName + ", " + "newName=" + newName + ", " + "oldName=" + oldName + "]";
        }
    }

    /** Undo CREATE INDEX by dropping it. */
        public static final class CreateIndexUndo implements UndoEntry {
        public final String indexName;

        public CreateIndexUndo(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public void undo(Database db) {
            db.removeIndex(indexName);
        }

        public String indexName() { return indexName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CreateIndexUndo that = (CreateIndexUndo) o;
            return java.util.Objects.equals(indexName, that.indexName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(indexName);
        }

        @Override
        public String toString() {
            return "CreateIndexUndo[indexName=" + indexName + "]";
        }
    }

    /** Undo a CREATE FUNCTION by removing it. */
        public static final class CreateFunctionUndo implements UndoEntry {
        public final String funcName;

        public CreateFunctionUndo(String funcName) {
            this.funcName = funcName;
        }

        @Override
        public void undo(Database db) {
            db.removeFunction(funcName);
        }

        public String funcName() { return funcName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CreateFunctionUndo that = (CreateFunctionUndo) o;
            return java.util.Objects.equals(funcName, that.funcName);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(funcName);
        }

        @Override
        public String toString() {
            return "CreateFunctionUndo[funcName=" + funcName + "]";
        }
    }
}
