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

    /** The executor this session's statements run through. */
    public AstExecutor executor() { return executor; }

    private String databaseName = "memgres";
    private DatabaseRegistry databaseRegistry;
    private volatile TransactionStatus status = TransactionStatus.IDLE;
    private final List<UndoEntry> undoLog = new ArrayList<>();
    /**
     * Established savepoints, innermost last.
     *
     * <p>A savepoint name is not a key: {@code SAVEPOINT s} twice establishes two, the second
     * shadowing the first, and releasing one name uncovers the one under it. Keying by name
     * instead discards every savepoint that shares a name the moment one of them is released.
     */
    private final List<SavepointFrame> savepoints = new ArrayList<>();
    private final List<DeferredFkCheck> deferredFkChecks = new ArrayList<>();
    private final List<Runnable> deferredTriggers = new ArrayList<>();
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
    private final int pid = System.identityHashCode(this);
    private final String tempSchemaName = "pg_temp_" + Math.abs(System.identityHashCode(this));

    // Prepared statements: name -> PreparedStmt
    private final Map<String, PreparedStmt> preparedStatements = new LinkedHashMap<>();

    // Cursors: name -> CursorState
    private final Map<String, CursorState> cursors = new LinkedHashMap<>();

    // GUC settings for this session
    private final GucSettings gucSettings = new GucSettings();
    // Snapshot of session GUC overrides at BEGIN for transactional rollback (M13)
    private java.util.Map<String, String> gucSessionSnapshot = null;

    // Shared scheduler for statement_timeout cancellation (one per JVM is sufficient)
    private static final ScheduledExecutorService TIMEOUT_SCHEDULER =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "memgres-stmt-timeout");
                t.setDaemon(true);
                return t;
            });

    // Explicit table locks acquired via LOCK TABLE: table_key -> lock mode (e.g. "AccessExclusiveLock")
    private final Map<String, String> tableLocks = new LinkedHashMap<>();

    // Tracks function call depth for procedure transaction control validation.
    // When > 0, we are inside a function (not procedure) and COMMIT/ROLLBACK is forbidden.
    private int functionCallDepth = 0;

    /** Whether the current transaction was started by an explicit BEGIN from the user (not an implicit procedure txn). */
    private boolean explicitTransactionBlock = false;

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

    // Per-session sequence cache: seq name -> [nextCachedValue, valuesStillCached, resetGeneration]
    private final Map<String, long[]> sequenceCache = new LinkedHashMap<>();

    /**
     * Get the next value of a sequence, drawing from this session's cached block if CACHE &gt; 1.
     *
     * <p>The block is whatever the sequence could actually spare, which may be less than CACHE:
     * reserving the full cache against the bounds would make a sequence whose cache is wider than
     * its range fail on its very first call, when PostgreSQL hands out every value it has and only
     * fails once one really passes the bound.
     */
    public long nextvalCached(Sequence seq) {
        if (seq.getCache() <= 1 || seq.isCycle()) {
            return seq.nextVal();
        }
        String key = seq.getName().toLowerCase();
        long generation = seq.getResetGeneration();
        long[] cached = sequenceCache.get(key);
        // setval and ALTER SEQUENCE RESTART move the counter under the block this session
        // reserved, so those values are no longer what the sequence would hand out. PG discards
        // the cache there, and without that a reset sequence goes on serving the old numbers.
        if (cached != null && cached[2] != generation) {
            sequenceCache.remove(key);
            cached = null;
        }
        if (cached != null && cached[1] > 0) {
            long val = cached[0];
            cached[0] += seq.getIncrementBy();
            cached[1]--;
            return val;
        }
        long[] block = seq.nextValBlock(seq.getCache());
        long first = block[0];
        long remaining = block[1] - 1;
        if (remaining > 0) {
            sequenceCache.put(key, new long[]{first + seq.getIncrementBy(), remaining, generation});
        } else {
            sequenceCache.remove(key);
        }
        return first;
    }

    /** Clear session sequence cache (on disconnect). */
    public void clearSequenceCache() { sequenceCache.clear(); }

    // Transaction ID (assigned from Database.allocateTransactionId() at BEGIN)
    private long transactionId = 0;
    // Command counter within current transaction (incremented per statement)
    private long commandId = 0;

    /** Get the current transaction ID, allocating one if needed (for autocommit DML). */
    public long getTransactionId() {
        if (transactionId == 0) {
            transactionId = database.allocateTransactionId();
        }
        return transactionId;
    }
    /** Get the current command ID within the transaction. */
    public long getCommandId() { return commandId; }
    /** Increment command counter (called before each statement execution). */
    public void incrementCommandId() { commandId++; }
    /** Reset virtual transaction ID after autocommit statement completes. */
    public void resetAutocommitTxId() {
        if (status == TransactionStatus.IDLE) {
            transactionId = 0;
            commandId = 0;
            // An autocommit statement's transaction ends here, so its locks end with it —
            // PG never leaves a FOR UPDATE or relation lock behind after an implicit commit.
            database.unlockAllRows(this);
            database.releaseTableLocks(this);
        }
    }

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
        /** Execution counters: PG 14+ tracks generic vs custom plans separately.
         *  Queries without parameters use generic plans; parameterized queries use custom plans. */
        private final java.util.concurrent.atomic.AtomicLong customPlanCount = new java.util.concurrent.atomic.AtomicLong(0);
        private final java.util.concurrent.atomic.AtomicLong genericPlanCount = new java.util.concurrent.atomic.AtomicLong(0);

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
        /** Increment execution counter (called on each EXECUTE).
         *  Queries without parameters use generic plans; parameterized use custom plans.
         *  This applies to both SQL-level and protocol-level prepared statements. */
        public void recordExecution() {
            boolean hasParams = (paramTypes != null && !paramTypes.isEmpty()) || inferredParamCount > 0;
            if (hasParams) {
                customPlanCount.incrementAndGet();
            } else {
                genericPlanCount.incrementAndGet();
            }
        }
        /** Get custom plan execution count (PG 14+). */
        public long customPlans() { return customPlanCount.get(); }
        /** Get generic plan execution count (PG 14+). */
        public long genericPlans() { return genericPlanCount.get(); }

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
        private final boolean explicitNoScroll;
        private final java.time.OffsetDateTime creationTime;
        private boolean committed; // true after the declaring transaction commits
        /** The stored rows each answer came from, when the query was a scan that has them. */
        private List<List<RowContext.TableBinding>> provenance;
        /** Whether the query was written FOR UPDATE or FOR SHARE, which changes what is refused. */
        private boolean locking;

        public CursorState(String name, List<Column> columns, List<Object[]> rows,
                           String queryText, boolean holdable, boolean binary, boolean scrollable,
                           boolean explicitNoScroll) {
            this.name = name;
            this.columns = columns;
            this.rows = rows;
            this.queryText = queryText;
            this.holdable = holdable;
            this.binary = binary;
            this.scrollable = scrollable;
            this.explicitNoScroll = explicitNoScroll;
            this.creationTime = java.time.OffsetDateTime.now();
        }

        public CursorState(String name, List<Column> columns, List<Object[]> rows,
                           String queryText, boolean holdable, boolean binary, boolean scrollable) {
            this(name, columns, rows, queryText, holdable, binary, scrollable, false);
        }

        public CursorState(String name, List<Column> columns, List<Object[]> rows) {
            this(name, columns, rows, null, false, false, false, false);
        }

        public String getName() { return name; }
        public List<Column> getColumns() { return columns; }
        public int getRowCount() { return rows.size(); }
        public int getPosition() { return position; }
        public String getQueryText() { return queryText; }
        public boolean isHoldable() { return holdable; }
        public boolean isBinary() { return binary; }
        public boolean isScrollable() { return scrollable; }
        public boolean isExplicitNoScroll() { return explicitNoScroll; }
        public boolean isCommitted() { return committed; }
        public void markCommitted() { this.committed = true; }
        public java.time.OffsetDateTime getCreationTime() { return creationTime; }

        /** Get row at index, or null if out of bounds. */
        public Object[] getRow(int idx) {
            if (idx >= 0 && idx < rows.size()) return rows.get(idx);
            return null;
        }

        public void setPosition(int pos) { this.position = pos; }

        public void setProvenance(List<List<RowContext.TableBinding>> rowProvenance) {
            this.provenance = rowProvenance;
        }

        public void setLocking(boolean forUpdateOrShare) { this.locking = forUpdateOrShare; }

        public boolean isLocking() { return locking; }

        /**
         * The stored row of {@code table} this cursor is on, or null when its query is not a scan
         * that reads that table. PostgreSQL calls a cursor that reads the table directly a simply
         * updatable scan of it, and only such a cursor can be named by WHERE CURRENT OF.
         */
        public Object[] currentRowOf(Table table) {
            if (provenance == null || position < 0 || position >= provenance.size()) return null;
            for (RowContext.TableBinding binding : provenance.get(position)) {
                if (binding.row == null) continue;
                if (binding.table == table || binding.sourceTable == table) return binding.row;
            }
            return null;
        }

        /** Whether the query behind this cursor reads {@code table} at all. */
        public boolean scans(Table table) {
            if (provenance == null) return false;
            for (List<RowContext.TableBinding> bindings : provenance) {
                for (RowContext.TableBinding binding : bindings) {
                    if (binding.table == table || binding.sourceTable == table) return true;
                }
            }
            return false;
        }
    }

    // SSI: tables read and written by this serializable transaction (for write-skew detection)
    private final Set<String> ssiReadTables = ConcurrentHashMap.newKeySet();
    private final Set<String> ssiWriteTables = ConcurrentHashMap.newKeySet();
    private long ssiTxnStartSeq = 0;

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

    /** Increment function call depth (entering a non-procedure function). */
    public void enterFunctionCall() { functionCallDepth++; }

    /** Decrement function call depth (leaving a non-procedure function). */
    public void exitFunctionCall() { if (functionCallDepth > 0) functionCallDepth--; }

    /** Whether we are inside a function context where transaction control is forbidden. */
    public boolean isInFunctionContext() { return functionCallDepth > 0; }

    /** Mark the current transaction as an explicit transaction block (started by user BEGIN). */
    public void setExplicitTransactionBlock(boolean explicit) { this.explicitTransactionBlock = explicit; }

    /** Whether the current transaction was started by an explicit BEGIN from the user. */
    public boolean isExplicitTransactionBlock() { return explicitTransactionBlock; }

    /** Whether the transaction is in the FAILED state. */
    public boolean isFailed() { return status == TransactionStatus.FAILED; }

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

    /** True once this transaction has run something that fixes its snapshot. */
    private boolean queryRanInTransaction = false;

    /**
     * Statements that configure the session rather than read the database.
     *
     * <p>The list is PostgreSQL's: {@code PortalRunUtility} runs a utility statement without a
     * snapshot only when {@code CommandIsReadOnly} says it needs none, which covers the settings
     * commands, {@code LOCK}, and the asynchronous-notification commands. {@code DEALLOCATE} and
     * {@code DISCARD} are not on it, and neither is anything that reads a table.
     */
    private static boolean isSnapshotFree(String upper) {
        return upper.startsWith("SET ") || upper.equals("SET")
                || upper.startsWith("SHOW ") || upper.startsWith("RESET ")
                || upper.startsWith("LOCK ")
                || upper.startsWith("LISTEN ") || upper.startsWith("UNLISTEN ")
                || upper.startsWith("NOTIFY ") || upper.equals("CHECKPOINT")
                || upper.startsWith("CHECKPOINT ");
    }

    /** True when this transaction has already run a statement that took its snapshot. */
    public boolean hasRunQueryInTransaction() { return queryRanInTransaction; }

    /** True when a savepoint is open, so the current work is inside a subtransaction. */
    public boolean hasSubtransaction() { return !savepoints.isEmpty(); }

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

        // An aborted transaction can only be ended or wound back to a savepoint. Establishing or
        // releasing a savepoint is ordinary work that PG refuses like any other statement — only
        // COMMIT/ROLLBACK (which end it) and ROLLBACK TO SAVEPOINT (which recovers it) get through.
        boolean allowedWhileAborted = upper.startsWith("COMMIT") || upper.startsWith("END")
                || upper.startsWith("ABORT") || upper.startsWith("ROLLBACK");
        if (status == TransactionStatus.FAILED && !allowedWhileAborted) {
            // The statement is read before the transaction is judged unfit to run it, so text
            // that is not a statement at all is a syntax error even here. Refusing first told a
            // client its transaction was aborted when what it had actually sent was nonsense.
            try {
                com.memgres.engine.parser.Parser.parse(sql, new ArrayList<String>());
            } catch (MemgresException parseFailure) {
                throw parseFailure;
            } catch (RuntimeException ignored) {
                // Anything the parser could not make sense of in another way is left to the
                // aborted-transaction report below, which is the answer for a readable statement.
            }
            throw new MemgresException(
                    "current transaction is aborted, commands ignored until end of transaction block",
                    "25P02");
        }
        // Track whether this transaction has taken a snapshot yet: SET TRANSACTION ISOLATION LEVEL
        // and [NOT] DEFERRABLE are only meaningful before one has been taken. Settings and
        // transaction control do not take one.
        if (status == TransactionStatus.IN_TRANSACTION && !isTransactionCmd && !isSnapshotFree(upper)) {
            queryRanInTransaction = true;
        }

        // statement_timeout: arm a deadline for this statement. PG re-reads the setting at the
        // start of every statement, so a SET is never limited by the value it is installing, and
        // transaction commands run unlimited.
        long timeoutMs = 0;
        if (!isTransactionCmd) {
            String timeoutVal = gucSettings.get("statement_timeout");
            timeoutMs = GucSettings.parseTimeoutMillis(timeoutVal);
        }

        final Thread execThread = Thread.currentThread();
        final StatementCancel.Token outerToken = StatementCancel.current();
        final StatementCancel.Token token = new StatementCancel.Token();
        StatementCancel.bind(token);
        ScheduledFuture<?> timeoutTask = null;
        if (timeoutMs > 0) {
            timeoutTask = TIMEOUT_SCHEDULER.schedule(new Runnable() {
                @Override
                public void run() {
                    token.request(StatementCancel.TIMEOUT_MESSAGE);
                    // Evaluation loops poll the token; a statement parked in a sleep or a lock
                    // wait is not looking at anything and needs the interrupt to come out.
                    execThread.interrupt();
                }
            }, timeoutMs, TimeUnit.MILLISECONDS);
        }

        executingStatement = true;
        // Everything this statement reads is read as this session, including the catalog listings
        // built too deep in the engine to be handed one.
        final Session outerViewer = Database.bindViewer(this);
        try {
            QueryResult result = executor.execute(sql, parameters);
            // A timeout that arrives after the statement has already produced its answer is too
            // late to cancel anything: PG reports the answer.
            resetAutocommitTxId();
            return result;
        } catch (RuntimeException e) {
            if (status == TransactionStatus.IN_TRANSACTION) {
                status = TransactionStatus.FAILED;
            }
            RuntimeException reported = reportedFailure(e, token);
            // For deadlock (40P01), automatically release this session's row locks so the
            // waiting session can proceed (mirrors PostgreSQL's automatic victim rollback).
            if (reported instanceof MemgresException
                    && "40P01".equals(((MemgresException) reported).getSqlState())) {
                database.unlockAllRows(this);
            }
            throw reported;
        } finally {
            executingStatement = false;
            Database.bindViewer(outerViewer);
            if (timeoutTask != null && !timeoutTask.cancel(false)) {
                // Too late to cancel: the task is already interrupting us. Let it finish, or the
                // interrupt lands after the clear below and kills the next statement instead.
                try {
                    timeoutTask.get();
                } catch (Exception ignored) {
                    // Cancelled or interrupted while waiting; either way the task is done.
                }
            }
            StatementCancel.bind(outerToken);
            // The interrupt was ours. Leaving it set would cancel whatever runs next on this thread.
            Thread.interrupted();
        }
    }

    /**
     * The error to report for a statement that failed while a cancel was pending.
     *
     * <p>Whatever a loop happened to throw on the way out, a cancelled statement reports PG's
     * cancellation error. A genuine failure is passed through untouched: PG reports the error the
     * statement really hit, not the timeout that arrived a moment behind it.
     */
    private static RuntimeException reportedFailure(RuntimeException e, StatementCancel.Token token) {
        if (!token.isRequested()) return e;
        boolean fromCancel = e.getCause() instanceof InterruptedException
                || (e instanceof MemgresException && "57014".equals(((MemgresException) e).getSqlState()));
        return fromCancel ? new MemgresException(token.message(), "57014") : e;
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
        ssiTxnStartSeq = database.allocateSsiSequence();
        transactionId = database.allocateTransactionId();
        commandId = 0;
        undoLog.clear();
        savepoints.clear();
        queryRanInTransaction = false;
        database.clearUncommittedObjects(this);
        // M13: snapshot session GUC overrides so plain SET can be rolled back
        gucSessionSnapshot = gucSettings.snapshotSessionOverrides();
        // LISTEN is undone by ROLLBACK the way any other statement is: a channel subscribed to in
        // a transaction that did not commit was never subscribed to.
        listenSnapshot = new LinkedHashSet<>(database.getNotificationManager().getListeningChannels(this));
    }

    /** The channels this session was listening to when the transaction began. */
    private Set<String> listenSnapshot;

    /** Put back the channels the session was listening to before work that did not stand. */
    private void restoreListens(Set<String> snapshot) {
        if (snapshot == null) return;
        NotificationManager manager = database.getNotificationManager();
        for (String channel : manager.getListeningChannels(this)) {
            if (!snapshot.contains(channel)) manager.unlisten(this, channel);
        }
        for (String channel : snapshot) manager.listen(this, channel);
    }

    /** Returns the transaction start timestamp (frozen for now()/current_timestamp stability), or null if not in a transaction. */
    public java.time.OffsetDateTime getTransactionTimestamp() {
        return transactionTimestamp;
    }

    public void commit() {
        // If the transaction is in FAILED (aborted) state, COMMIT acts as ROLLBACK (PG behavior)
        if (status == TransactionStatus.FAILED) {
            rollback();
            return;
        }
        // SSI write-skew detection: must happen before any commit side effects
        try {
            checkSsiConflicts();
        } catch (MemgresException e) {
            // SSI conflict detected, rollback
            rollback();
            throw e;
        }
        // Record committed SSI info for future transactions to check against
        if (isSerializable() && !ssiWriteTables.isEmpty()) {
            database.recordCommittedSsiTransaction(
                new HashSet<>(ssiReadTables), new HashSet<>(ssiWriteTables));
        }
        // Validate deferred constraints before committing
        try {
            validateDeferredChecks(deferredFkChecks);
        } catch (MemgresException e) {
            // Deferred constraint failed, rollback
            rollback();
            throw e;
        }
        // Fire deferred triggers (CONSTRAINT TRIGGER ... DEFERRABLE INITIALLY DEFERRED)
        try {
            for (Runnable trigger : deferredTriggers) {
                trigger.run();
            }
        } catch (MemgresException e) {
            rollback();
            throw e;
        }
        deferredTriggers.clear();
        // Clear SSI tracking
        clearSsiState();
        // Swap MVCC maps to new empty instances (atomic from cross-session readers' perspective)
        uncommittedInserts = new ConcurrentHashMap<>();
        uncommittedUpdates = new ConcurrentHashMap<>();
        uncommittedDeletes = new ConcurrentHashMap<>();
        rrSnapshots.clear();
        rrSnapshotTaken = false;
        snapshotImported = false;
        // Clear transaction-scoped GUC overrides (SET LOCAL)
        gucSettings.clearTransactionOverrides();
        // M13: discard GUC snapshot (changes are committed, no rollback needed)
        gucSessionSnapshot = null;
        // Reset per-transaction GUCs (transaction_read_only, transaction_isolation)
        gucSettings.reset("transaction_read_only");
        gucSettings.reset("transaction_isolation");
        gucSettings.reset("transaction_deferrable");
        // Discard undo log; changes are permanent
        undoLog.clear();
        savepoints.clear();
        queryRanInTransaction = false;
        database.clearUncommittedObjects(this);
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
        releaseTableLocks();
        // Release all row-level locks held by this session
        database.unlockAllRows(this);
        // Destroy non-holdable cursors (PG behavior: only WITH HOLD cursors survive COMMIT)
        destroyNonHoldableCursors();
        transactionTimestamp = null;
        explicitTransactionBlock = false;
        status = TransactionStatus.IDLE;
        database.incrementXactCommit();
    }

    public void rollback() {
        // Clear SSI tracking
        clearSsiState();
        // Undo the writes before forgetting which rows they were. Clearing the MVCC maps first
        // leaves a window in which the rows are still in the table but no longer marked as this
        // session's uncommitted work, so another session would briefly read them as committed.
        applyUndo(0);
        // Swap MVCC maps to new empty instances (atomic from cross-session readers' perspective)
        uncommittedInserts = new ConcurrentHashMap<>();
        uncommittedUpdates = new ConcurrentHashMap<>();
        uncommittedDeletes = new ConcurrentHashMap<>();
        rrSnapshots.clear();
        rrSnapshotTaken = false;
        snapshotImported = false;
        // Clear transaction-scoped GUC overrides (SET LOCAL)
        gucSettings.clearTransactionOverrides();
        // M13: restore session GUC overrides to pre-BEGIN state (plain SET rollback)
        if (gucSessionSnapshot != null) {
            gucSettings.restoreSessionOverrides(gucSessionSnapshot);
            gucSessionSnapshot = null;
        }
        restoreSessionIdentity();
        restoreListens(listenSnapshot);
        listenSnapshot = null;
        // Reset per-transaction GUCs (transaction_read_only, transaction_isolation)
        gucSettings.reset("transaction_read_only");
        gucSettings.reset("transaction_isolation");
        gucSettings.reset("transaction_deferrable");
        undoLog.clear();
        savepoints.clear();
        queryRanInTransaction = false;
        database.clearUncommittedObjects(this);
        deferredFkChecks.clear();
        deferredTriggers.clear();
        allConstraintsDeferred = false;
        allConstraintsImmediate = false;
        deferredConstraintNames.clear();
        immediateConstraintNames.clear();
        // Discard deferred notifications; transaction was rolled back
        deferredNotifications.clear();
        onCommitDropTables.clear();
        // Release transaction-scoped advisory locks
        releaseXactAdvisoryLocks();
        releaseTableLocks();
        // Release all row-level locks held by this session
        database.unlockAllRows(this);
        // Destroy cursors on rollback. Holdable cursors that were already committed
        // (promoted to session-level) survive ROLLBACK, matching PG behavior.
        cursors.entrySet().removeIf(e -> !e.getValue().isHoldable() || !e.getValue().isCommitted());
        transactionTimestamp = null;
        explicitTransactionBlock = false;
        status = TransactionStatus.IDLE;
    }

    /**
     * Prepare the current transaction for two-phase commit.
     * Detaches the uncommitted state and undo log from this session and returns them
     * packaged in a PreparedTransaction, then resets the session to IDLE.
     */
    public Database.PreparedTransaction prepareTransaction(String gid) {
        if (status != TransactionStatus.IN_TRANSACTION) {
            throw new MemgresException("PREPARE TRANSACTION can only be used in transaction blocks", "25P01");
        }
        Map<String, Set<Object[]>> capturedInserts = uncommittedInserts;
        Map<String, Map<Object[], Object[]>> capturedUpdates = uncommittedUpdates;
        Map<String, List<Object[]>> capturedDeletes = uncommittedDeletes;
        List<UndoEntry> capturedUndo = new ArrayList<>(undoLog);

        String owner = connectingUser != null ? connectingUser : "memgres";
        String dbName = databaseName != null ? databaseName : "memgres";

        Database.PreparedTransaction pt = new Database.PreparedTransaction(
                gid, transactionId, java.time.OffsetDateTime.now(),
                owner, dbName,
                capturedUndo, capturedInserts, capturedUpdates, capturedDeletes);

        uncommittedInserts = new ConcurrentHashMap<>();
        uncommittedUpdates = new ConcurrentHashMap<>();
        uncommittedDeletes = new ConcurrentHashMap<>();
        rrSnapshots.clear();
        rrSnapshotTaken = false;
        snapshotImported = false;
        gucSettings.clearTransactionOverrides();
        gucSettings.reset("transaction_read_only");
        gucSettings.reset("transaction_isolation");
        gucSettings.reset("transaction_deferrable");
        undoLog.clear();
        savepoints.clear();
        queryRanInTransaction = false;
        database.clearUncommittedObjects(this);
        deferredFkChecks.clear();
        allConstraintsDeferred = false;
        allConstraintsImmediate = false;
        deferredConstraintNames.clear();
        immediateConstraintNames.clear();
        deferredNotifications.clear();
        onCommitDropTables.clear();
        releaseXactAdvisoryLocks();
        database.unlockAllRows(this);
        releaseTableLocks();
        destroyNonHoldableCursors();
        transactionTimestamp = null;
        explicitTransactionBlock = false;
        status = TransactionStatus.IDLE;

        return pt;
    }

    /**
     * Commit a previously prepared transaction. Clears its MVCC uncommitted maps
     * (making changes permanent) without applying undo.
     */
    public static void commitPreparedTransaction(Database.PreparedTransaction pt) {
        pt.uncommittedInserts.clear();
        pt.uncommittedUpdates.clear();
        pt.uncommittedDeletes.clear();
    }

    /**
     * Rollback a previously prepared transaction. Applies the undo log in reverse
     * to revert all changes, then clears MVCC maps.
     */
    public static void rollbackPreparedTransaction(Database db, Database.PreparedTransaction pt) {
        for (int i = pt.undoLog.size() - 1; i >= 0; i--) {
            pt.undoLog.get(i).undo(db);
        }
        pt.uncommittedInserts.clear();
        pt.uncommittedUpdates.clear();
        pt.uncommittedDeletes.clear();
    }

    /** Everything one savepoint has to be able to put back. */
    private static final class SavepointFrame {
        final String name;
        final int undoPosition;
        final int notificationCount;
        /** Postponed checks queued before the savepoint; ones queued after it die with it. */
        final int deferredCheckCount;
        final long lockMark;
        final Map<String, String> sessionGucs;
        final Map<String, String> localGucs;
        final MvccSnapshot mvcc;
        /** The cursors that existed when the savepoint was taken; later ones die with it. */
        final Set<String> cursorNames;
        /** The channels subscribed to, and the advisory locks held, when it was taken. */
        Set<String> listens;
        Map<Database.AdvisoryLockId, int[]> advisoryHolds;

        SavepointFrame(String name, int undoPosition, int notificationCount,
                       int deferredCheckCount, long lockMark,
                       Map<String, String> sessionGucs, Map<String, String> localGucs,
                       MvccSnapshot mvcc, Set<String> cursorNames) {
            this.name = name;
            this.undoPosition = undoPosition;
            this.notificationCount = notificationCount;
            this.deferredCheckCount = deferredCheckCount;
            this.lockMark = lockMark;
            this.sessionGucs = sessionGucs;
            this.localGucs = localGucs;
            this.mvcc = mvcc;
            this.cursorNames = cursorNames;
        }
    }

    /**
     * Index of the innermost savepoint carrying this name, or -1.
     *
     * <p>The name arrives already folded the way it was written: {@code SAVEPOINT s} lowercases,
     * {@code SAVEPOINT "S"} does not. Folding again here would make the two the same savepoint,
     * so {@code ROLLBACK TO s} would wind back one that was never established under that name.
     */
    private int findSavepoint(String name) {
        for (int i = savepoints.size() - 1; i >= 0; i--) {
            if (savepoints.get(i).name.equals(name)) return i;
        }
        return -1;
    }

    /**
     * A savepoint marks a point inside a transaction block to come back to, so there has to be a
     * block: outside one there is nothing for it to divide and nothing later to roll back to,
     * and PostgreSQL refuses all three savepoint commands rather than open a transaction of its
     * own that the next statement would immediately commit.
     */
    private void requireTransactionBlock(String command) {
        if (!explicitTransactionBlock) {
            throw new MemgresException(
                    command + " can only be used in transaction blocks", "25P01");
        }
    }

    /**
     * Once the transaction has failed the only thing left to do with it is leave it, so
     * PostgreSQL lets ROLLBACK, COMMIT and ROLLBACK TO SAVEPOINT through and refuses everything
     * else with 25P02 — SAVEPOINT and RELEASE SAVEPOINT included. Marking a point inside a block
     * that can no longer do any work would only give the caller somewhere to come back to that
     * is already aborted.
     */
    private void refuseInAbortedTransaction() {
        if (status == TransactionStatus.FAILED) {
            throw new MemgresException(
                    "current transaction is aborted, commands ignored until end of transaction block",
                    "25P02");
        }
    }

    public void savepoint(String name) {
        requireTransactionBlock("SAVEPOINT");
        refuseInAbortedTransaction();
        internalSavepoint(name);
    }

    /**
     * Put back the user the session speaks as, after its settings have been.
     *
     * <p>SET SESSION AUTHORIZATION changes a setting and a field beside it. The setting was on the
     * undo log and the field was not, so a rolled-back transaction left the session still
     * answering current_user with the role it had given up.
     */
    private void restoreSessionIdentity() {
        String authorized = gucSettings.get("session_authorization");
        if (authorized != null && !authorized.isEmpty()) setConnectingUser(authorized);
    }

    /** A savepoint the engine takes for itself, such as a PL/pgSQL exception block. */
    public void internalSavepoint(String name) {
        if (status != TransactionStatus.IN_TRANSACTION) {
            // Implicit BEGIN
            begin();
        }
        SavepointFrame frame = new SavepointFrame(name, undoLog.size(),
                deferredNotifications.size(), deferredFkChecks.size(),
                database.currentRowLockMark(),
                getGucSettings().snapshotSessionOverrides(),
                getGucSettings().snapshotTransactionOverrides(),
                // Snapshot current MVCC maps so we can restore on ROLLBACK TO SAVEPOINT.
                // Deep-copy the outer maps; inner collections are identity-based.
                MvccSnapshot.capture(uncommittedInserts, uncommittedUpdates, uncommittedDeletes),
                new LinkedHashSet<>(cursors.keySet()));
        frame.listens = new LinkedHashSet<>(database.getNotificationManager().getListeningChannels(this));
        frame.advisoryHolds = database.advisoryXactHolds(this);
        savepoints.add(frame);
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
        requireTransactionBlock("RELEASE SAVEPOINT");
        refuseInAbortedTransaction();
        internalReleaseSavepoint(name);
    }

    /** Release of a savepoint the engine took for itself. */
    public void internalReleaseSavepoint(String name) {
        if (status == TransactionStatus.IDLE) {
            throw new MemgresException("RELEASE SAVEPOINT can only be used in transaction blocks", "25P01");
        }
        int index = findSavepoint(name);
        if (index < 0) {
            throw new MemgresException("savepoint \"" + name + "\" does not exist", "3B001");
        }
        // Releasing a savepoint also destroys every savepoint established after it.
        savepoints.subList(index, savepoints.size()).clear();
    }

    public void rollbackToSavepoint(String name) {
        requireTransactionBlock("ROLLBACK TO SAVEPOINT");
        internalRollbackToSavepoint(name);
    }

    /** Rollback to a savepoint the engine took for itself. */
    public void internalRollbackToSavepoint(String name) {
        if (status == TransactionStatus.IDLE) {
            throw new MemgresException("ROLLBACK TO SAVEPOINT can only be used in transaction blocks", "25P01");
        }
        int index = findSavepoint(name);
        if (index < 0) {
            throw new MemgresException("savepoint \"" + name + "\" does not exist", "3B001");
        }
        SavepointFrame frame = savepoints.get(index);

        // Restore MVCC maps to their state at savepoint creation time.
        // This must happen BEFORE applyUndo so that concurrent readers see consistent
        // MVCC visibility during the undo process. The volatile swap is atomic from
        // the perspective of cross-session reads.
        if (frame.mvcc != null) {
            uncommittedInserts = frame.mvcc.inserts;
            uncommittedUpdates = frame.mvcc.updates;
            uncommittedDeletes = frame.mvcc.deletes;
        }

        applyUndo(frame.undoPosition);

        // Row locks and GUC values set after the savepoint go away with the subtransaction
        database.releaseRowLocksAfter(this, frame.lockMark);
        if (frame.sessionGucs != null) {
            getGucSettings().restoreSessionOverrides(frame.sessionGucs);
        }
        if (frame.localGucs != null) {
            getGucSettings().restoreTransactionOverrides(frame.localGucs);
        }
        restoreSessionIdentity();
        restoreListens(frame.listens);
        database.restoreAdvisoryXactHolds(this, frame.advisoryHolds);

        // Truncate deferred notifications to the savepoint's count
        if (frame.notificationCount < deferredNotifications.size()) {
            deferredNotifications.subList(frame.notificationCount, deferredNotifications.size()).clear();
        }

        // A postponed constraint check queued inside the subtransaction refers to a row the undo
        // has just taken back. Keeping it would fail the COMMIT over a write nobody made.
        if (frame.deferredCheckCount < deferredFkChecks.size()) {
            deferredFkChecks.subList(frame.deferredCheckCount, deferredFkChecks.size()).clear();
        }

        // A cursor opened inside the subtransaction goes away with it. Closing one does not
        // come back, and a cursor that was only moved keeps the position the FETCH left it at,
        // so only cursors that did not exist at the savepoint are removed.
        if (frame.cursorNames != null) {
            cursors.keySet().retainAll(frame.cursorNames);
        }

        // Savepoints established after this one are destroyed; this one survives, so it can be
        // rolled back to again.
        if (index + 1 < savepoints.size()) {
            savepoints.subList(index + 1, savepoints.size()).clear();
        }

        // Transaction is no longer in FAILED state after rolling back to savepoint
        if (status == TransactionStatus.FAILED) {
            status = TransactionStatus.IN_TRANSACTION;
        }
    }


    public boolean isInTransaction() {
        return status == TransactionStatus.IN_TRANSACTION || status == TransactionStatus.FAILED;
    }

    /**
     * True when this transaction has already failed. Its writes cannot become permanent, so
     * another session waiting on one of them has nothing left to wait for.
     */
    public boolean isTransactionAborted() {
        return status == TransactionStatus.FAILED;
    }

    /**
     * True when nothing this transaction has written can ever become visible to anyone else.
     *
     * <p>PostgreSQL records the abort the moment the statement fails, so from that instant every
     * row the transaction wrote is dead: another session neither waits for it nor sees its key.
     * The one way back is a savepoint, which undoes only the failed subtransaction and leaves the
     * work before it alive — so a transaction holding a savepoint is still worth waiting for.
     */
    public boolean isDoomed() {
        return status == TransactionStatus.FAILED && savepoints.isEmpty();
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

    /** Installed by the wire handler so a notification can reach an idle listener at once. */
    private volatile Runnable notificationSink;
    /** True while this session is running a statement, when a push would interleave messages. */
    private volatile boolean executingStatement;

    public void setNotificationSink(Runnable flusher) {
        this.notificationSink = flusher;
    }

    public boolean isExecutingStatement() {
        return executingStatement;
    }

    public void addNotification(Notification notification) {
        pendingNotifications.add(notification);
        // PG delivers to a listener that is sitting idle on the socket, not only to one that
        // happens to issue another statement, so ask this connection to write what it has. The
        // notification stays on the queue until something writes it: taking it off here and
        // handing it to a write that has not happened yet left it in neither place, and a
        // listener whose next statement drained the queue in that moment found it empty and
        // read the notification only after its own ReadyForQuery -- which is one statement too
        // late for a client that asked for its notifications when that statement returned.
        Runnable flusher = notificationSink;
        if (flusher != null && !executingStatement) flusher.run();
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
            // Inside transaction, defer until COMMIT.
            // PG delivers identical notifications (same channel + payload) signaled
            // multiple times within one transaction only once: the enqueue is skipped
            // when an identical entry is already pending in this transaction.
            for (Notification pending : deferredNotifications) {
                if (pending.channel().equals(n.channel()) && pending.payload().equals(n.payload())) {
                    return;
                }
            }
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

    /**
     * Resolves the stable OID for a named catalog object (e.g. {@code "type:" + enumTypeName})
     * using this session's own {@link SystemCatalog}. Used by the wire-protocol layer to
     * advertise the real per-type OID for custom enum columns in RowDescription, instead of a
     * placeholder value the client can't resolve via a pg_type lookup.
     */
    public int resolveOid(String key) {
        return executor.getSystemCatalog().getOid(key);
    }

    /**
     * The catalog key of the user-defined type a written name denotes. A column records the name
     * it was declared with, and two schemas may each hold a type of that name, so this resolves
     * the one that column's OID belongs to.
     */
    public String typeOidKey(String written) {
        String key = TypeNamespace.oidKeyFor(database, written);
        return key != null ? key : TypeNamespace.oidKey(null, written);
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
        // Release all advisory locks (session- and transaction-level) held by this session,
        // matching PG's backend-exit cleanup.
        database.releaseAllAdvisoryLocks(this);
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
        database.removeSequencesInSchema(tempSchemaName);
    }

    /**
     * Returns the current effective schema, the first valid schema in search_path.
     * This is what PG returns from current_schema().
     */
    public String getEffectiveSchema() {
        String searchPath = gucSettings.get("search_path");
        if (searchPath != null) {
            // Check if search_path is effectively empty (all entries blank or $user)
            boolean hasEntries = false;
            for (String sp : searchPath.split(",")) {
                String s = sp.trim().replace("\"", "").replace("'", "");
                if (s.isEmpty() || s.equals("$user")) continue;
                hasEntries = true;
                // pg_catalog is always a valid schema (virtual, not stored in Database)
                if ("pg_catalog".equals(s) || "information_schema".equals(s)
                        || database.getSchema(s) != null) return s;
            }
            // If search_path has entries but none are valid schemas, still return "public"
            // (this handles cases like SET search_path = 'nonexistent')
            // If search_path is genuinely empty, return "public" for DDL default schema
        }
        return "public";
    }

    /**
     * The schema a CREATE lands in. Reading tolerates a search_path entry that names nothing --
     * the entry is simply skipped -- but creating does not: with no usable entry there is
     * nowhere to put the object, and PG says so rather than quietly using public.
     */
    public String getCreationSchema() {
        String searchPath = gucSettings.get("search_path");
        if (searchPath != null) {
            boolean hasEntries = false;
            for (String sp : searchPath.split(",")) {
                String s = sp.trim().replace("\"", "").replace("'", "");
                if (s.isEmpty() || s.equals("$user")) continue;
                hasEntries = true;
                if ("pg_catalog".equals(s) || "information_schema".equals(s)
                        || database.getSchema(s) != null) return s;
            }
            // A path that names schemas but reaches none of them, and a path written empty,
            // both select nothing to create in. Falling back to public created the relation
            // somewhere the reader had excluded.
            throw new MemgresException("no schema has been selected to create in", "3F000");
        }
        return "public";
    }

    /**
     * Returns the full effective search path as an ordered list of schema names.
     * Matches PG's current_schemas() behavior.
     */
    /**
     * The search path as {@code current_schemas} reports it: the schemas that are there.
     *
     * <p>A path may name a schema that does not exist — nothing stops {@code SET search_path} from
     * naming one — and PostgreSQL simply does not look in it, so it is not part of the path either.
     * Reporting the names as written listed a schema no name could ever resolve in, and adding
     * "public" whether or not the path asked for it reported one the reader had excluded.
     */
    public List<String> getExistingSearchPath(boolean includeImplicit) {
        // What the path names, in the order it names it, keeping only the schemas that are there.
        List<String> named = new ArrayList<>();
        String searchPath = gucSettings.get("search_path");
        String tempSchema = getTempSchemaName();
        boolean namesTemp = false;
        boolean namesCatalog = false;
        if (searchPath != null) {
            for (String sp : searchPath.split(",")) {
                String s = sp.trim().replace("\"", "").replace("'", "");
                if (s.isEmpty()) continue;
                if (s.equals("$user")) s = getConnectingUser();
                if (s == null) continue;
                if (s.equals("pg_temp") || s.equals(tempSchema)) {
                    namesTemp = true;
                    if (database.getSchema(tempSchema) == null) continue;
                    s = tempSchema;
                } else if (s.equals("pg_catalog")) {
                    namesCatalog = true;
                }
                if (named.contains(s)) continue;
                if ("pg_catalog".equals(s) || "information_schema".equals(s)
                        || database.getSchema(s) != null) {
                    named.add(s);
                }
            }
        }
        if (!includeImplicit) return named;
        // The implicit schemas come first — the session's temporary one, then the catalogue —
        // but only while the path has not said where it wants them. A path that names pg_catalog
        // puts it where it was named, and prepending it regardless reported an order the reader
        // had deliberately changed.
        List<String> result = new ArrayList<>();
        if (!namesTemp && database.getSchema(tempSchema) != null) result.add(tempSchema);
        if (!namesCatalog) result.add("pg_catalog");
        for (String s : named) {
            if (!result.contains(s)) result.add(s);
        }
        return result;
    }

    /** The first schema of the path that exists, or null when none of them does. */
    public String getReportedSchema() {
        List<String> path = getExistingSearchPath(false);
        return path.isEmpty() ? null : path.get(0);
    }

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

    // A prepared statement and a cursor are named by an identifier, which is folded to lower case
    // when it is written plainly and kept as it stands when it is quoted. Folding the name again
    // here made PREPARE "P" and EXECUTE p the same statement, which in PostgreSQL they are not.

    public void addPreparedStatement(String name, PreparedStmt stmt) {
        preparedStatements.put(name, stmt);
    }

    public PreparedStmt getPreparedStatement(String name) {
        return preparedStatements.get(name);
    }

    public void removePreparedStatement(String name) {
        preparedStatements.remove(name);
    }

    public void removeAllPreparedStatements() {
        preparedStatements.clear();
    }

    public Collection<PreparedStmt> getAllPreparedStatements() {
        return Collections.unmodifiableCollection(preparedStatements.values());
    }

    // ---- Cursors ----

    /** How many portals this session has had to name for itself. */
    private final java.util.concurrent.atomic.AtomicInteger unnamedPortals =
            new java.util.concurrent.atomic.AtomicInteger(0);

    /**
     * The name PostgreSQL gives a cursor that was opened without one. It is not the variable's
     * name: two functions each declaring a cursor called {@code c} open two different portals, and
     * the name is what a caller handed the refcursor back has to FETCH from.
     */
    public String nextUnnamedPortal() {
        return "<unnamed portal " + unnamedPortals.incrementAndGet() + ">";
    }

    public void addCursor(String name, CursorState cursor) {
        cursors.put(name, cursor);
    }

    public CursorState getCursor(String name) {
        return cursors.get(name);
    }

    public void removeCursor(String name) {
        cursors.remove(name);
    }

    public void removeAllCursors() {
        cursors.clear();
    }

    /** Destroy non-holdable cursors at COMMIT time (PG behavior). WITH HOLD cursors survive and are marked as committed. */
    public void destroyNonHoldableCursors() {
        cursors.entrySet().removeIf(e -> !e.getValue().isHoldable());
        // Mark surviving holdable cursors as committed (session-level)
        for (CursorState c : cursors.values()) {
            if (c.isHoldable()) c.markCommitted();
        }
    }

    public Collection<CursorState> getAllCursors() {
        return Collections.unmodifiableCollection(cursors.values());
    }

    // ---- ON COMMIT DROP tables ----

    /** Release all transaction-scoped advisory locks. Called on commit/rollback. */
    private void releaseXactAdvisoryLocks() {
        database.releaseXactAdvisoryLocks(this);
    }

    /** Track explicit LOCK TABLE lock for pg_locks visibility. */
    public void addTableLock(String tableKey, String mode) {
        tableLocks.put(tableKey, mode);
    }

    /**
     * The lock a statement takes on a relation it touches, and how strong it is.
     *
     * <p>pg_locks used to show an AccessShareLock on every relation for every session, whether or
     * not anything had touched it and whether or not a transaction was open. So a reader looking
     * for what a transaction holds saw locks nobody had taken, and never saw that an UPDATE holds
     * a stronger one than a SELECT.
     */
    private final Map<String, String> relationLocks = new java.util.concurrent.ConcurrentHashMap<>();

    /** Rank of the lock modes a statement can take, weakest first. */
    private static final List<String> RELATION_LOCK_STRENGTH = java.util.Arrays.asList(
            "AccessShareLock", "RowShareLock", "RowExclusiveLock", "ShareUpdateExclusiveLock",
            "ShareLock", "ShareRowExclusiveLock", "ExclusiveLock", "AccessExclusiveLock");

    /** Record that this statement touched {@code tableKey}, keeping the strongest lock taken. */
    public void recordRelationLock(String tableKey, String mode) {
        if (tableKey == null || mode == null) return;
        // Locks belong to a transaction; outside one PostgreSQL has already let them go by the
        // time anything can look.
        if (status != TransactionStatus.IN_TRANSACTION) return;
        String held = relationLocks.get(tableKey);
        if (held != null
                && RELATION_LOCK_STRENGTH.indexOf(held) >= RELATION_LOCK_STRENGTH.indexOf(mode)) {
            return;
        }
        relationLocks.put(tableKey, mode);
    }

    /** The relations this session's transaction holds a lock on, and the mode of each. */
    public Map<String, String> getRelationLocks() {
        return relationLocks;
    }

    /** Get explicit table locks. */
    public Map<String, String> getTableLocks() {
        return tableLocks;
    }

    /** Release all explicit table locks. Called on commit/rollback. */
    public void releaseTableLocks() {
        // The locks a statement took go with the transaction that took them.
        relationLocks.clear();
        tableLocks.clear();
        if (database != null) {
            database.releaseTableLocks(this);
        }
    }

    public void registerOnCommitDrop(String schema, String tableName) {
        onCommitDropTables.add(new String[]{schema, tableName});
    }

    public void registerOnCommitDeleteRows(String schema, String tableName) {
        onCommitDeleteRowsTables.add(new String[]{schema, tableName});
    }

    // ---- Nested execution (a function or DO body running inside one outer statement) ----

    /** Depth of function/DO bodies currently running; 0 means we are at statement level. */
    private int nestedExecutionDepth;
    /** ON COMMIT DROP tables owed to the end of the outer statement, in autocommit. */
    private final List<String[]> statementEndDropTables = new ArrayList<>();

    public void enterNestedExecution() {
        nestedExecutionDepth++;
    }

    /**
     * A body that has finished ends the statement it was part of, once the outermost one returns.
     * Anything a nested body created ON COMMIT DROP dies here — the equivalent, for an autocommit
     * statement, of the implicit transaction committing.
     */
    public void exitNestedExecution() {
        if (nestedExecutionDepth > 0) nestedExecutionDepth--;
        if (nestedExecutionDepth > 0 || statementEndDropTables.isEmpty()) return;
        for (String[] pair : statementEndDropTables) {
            Schema s = database.getSchema(pair[0]);
            if (s != null) s.removeTable(pair[1]);
        }
        statementEndDropTables.clear();
    }

    public boolean isInNestedExecution() {
        return nestedExecutionDepth > 0;
    }

    // ---- Recursion depth (PostgreSQL's max_stack_depth) ----

    /** Routine bodies — functions, procedures, DO blocks — currently on the call chain. */
    private int routineDepth;
    /** Trigger firings currently on the call chain. */
    private int triggerDepth;

    /**
     * Recursion that never terminates has to be reported as PostgreSQL reports it — {@code 54001}
     * naming the limit — rather than left to exhaust the Java stack, which surfaces as an internal
     * error from wherever the stack happened to give out.
     */
    public void enterRoutine() {
        if (routineDepth >= PgErrors.MAX_ROUTINE_DEPTH) {
            throw PgErrors.stackDepthExceeded();
        }
        routineDepth++;
    }

    public void exitRoutine() {
        if (routineDepth > 0) routineDepth--;
    }

    /**
     * Whether a function, procedure or DO block is on the call chain.
     *
     * <p>Several statements cannot run from one — VACUUM and DISCARD among them — and the gates
     * that refuse them asked whether a transaction was open instead, which a DO block's implicit
     * transaction leaves false. So they ran from inside a routine and PostgreSQL refuses them.
     */
    public boolean isInRoutine() {
        return routineDepth > 0;
    }

    public void enterTriggerCall() {
        if (triggerDepth >= PgErrors.MAX_TRIGGER_DEPTH) {
            throw PgErrors.stackDepthExceeded();
        }
        triggerDepth++;
    }

    public void exitTriggerCall() {
        if (triggerDepth > 0) triggerDepth--;
    }

    public void registerStatementEndDrop(String schema, String tableName) {
        statementEndDropTables.add(new String[]{schema, tableName});
    }

    // ---- Deferred constraint checks ----

        public static final class DeferredFkCheck {
        public final Table table;
        public final Object[] row;
        public final StoredConstraint constraint;
        /**
         * Set only for the referenced side of a foreign key: the table whose row was deleted or
         * whose key was changed, leaving {@link #row} of {@link #table} possibly pointing at
         * nothing. Null for every check that belongs to the row's own table.
         */
        public final Table referencedTable;

        public DeferredFkCheck(Table table, Object[] row, StoredConstraint constraint) {
            this(table, row, constraint, null);
        }

        public DeferredFkCheck(Table table, Object[] row, StoredConstraint constraint,
                               Table referencedTable) {
            this.table = table;
            this.row = row;
            this.constraint = constraint;
            this.referencedTable = referencedTable;
        }

        public Table table() { return table; }
        public Object[] row() { return row; }
        public StoredConstraint constraint() { return constraint; }
        public Table referencedTable() { return referencedTable; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeferredFkCheck that = (DeferredFkCheck) o;
            return java.util.Objects.equals(table, that.table)
                && java.util.Arrays.equals(row, that.row)
                && java.util.Objects.equals(constraint, that.constraint)
                && java.util.Objects.equals(referencedTable, that.referencedTable);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(table, java.util.Arrays.hashCode(row), constraint,
                    referencedTable);
        }

        @Override
        public String toString() {
            return "DeferredFkCheck[table=" + table + ", " + "row=" + java.util.Arrays.toString(row) + ", " + "constraint=" + constraint + "]";
        }
    }

    public void addDeferredCheck(Table table, Object[] row, StoredConstraint constraint) {
        deferredFkChecks.add(new DeferredFkCheck(table, row, constraint));
    }

    /**
     * Postpone the referenced side of a foreign key: a row of {@code referencedTable} has been
     * deleted, or had its key changed, while {@code childRow} still refers to the old key. The
     * check runs again at COMMIT, by which time the transaction may have put the key back or
     * moved the child row, either of which makes it pass.
     */
    public void addDeferredReferencedCheck(Table childTable, Object[] childRow,
                                           StoredConstraint constraint, Table referencedTable) {
        deferredFkChecks.add(new DeferredFkCheck(childTable, childRow, constraint, referencedTable));
    }

    /**
     * Run a set of postponed constraint checks. Uniqueness is checked once per constraint over the
     * whole table, so a run of inserts that collide is reported once; the row-scoped checks
     * (CHECK, FK, EXCLUDE) are then run against the rows that were recorded.
     */
    private void validateDeferredChecks(List<DeferredFkCheck> checks) {
        Set<String> validatedUnique = new java.util.HashSet<>();
        for (DeferredFkCheck check : checks) {
            StoredConstraint sc = check.constraint();
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE) {
                String key = System.identityHashCode(check.table()) + ":" + sc.getName();
                if (validatedUnique.add(key)) {
                    executor.constraintValidator.validateDeferredUniqueness(check.table(), sc);
                }
            }
        }
        for (DeferredFkCheck check : checks) {
            StoredConstraint sc = check.constraint();
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE) {
                continue;
            }
            if (check.referencedTable() != null) {
                executor.constraintValidator.validateDeferredReferencedFk(
                        check.referencedTable(), check.table(), check.row(), sc);
            } else {
                executor.constraintValidator.validateDeferredConstraint(check.table(), check.row(), sc);
            }
        }
    }

    /**
     * Run the checks postponed so far, as {@code SET CONSTRAINTS ... IMMEDIATE} asks. Checks that
     * pass are done with and are not repeated at COMMIT; a failure leaves them pending, because
     * the transaction is going to be rolled back anyway.
     *
     * @param constraintName restrict to one constraint, or null for every pending check
     */
    public void runPendingDeferredChecks(String constraintName) {
        if (deferredFkChecks.isEmpty()) return;
        List<DeferredFkCheck> selected = new ArrayList<>();
        for (DeferredFkCheck check : deferredFkChecks) {
            String scName = check.constraint().getName();
            if (constraintName == null
                    || (scName != null && scName.equalsIgnoreCase(constraintName))) {
                selected.add(check);
            }
        }
        if (selected.isEmpty()) return;
        validateDeferredChecks(selected);
        deferredFkChecks.removeAll(selected);
    }

    public void addDeferredTrigger(Runnable trigger) {
        deferredTriggers.add(trigger);
    }

    /** Forget every SET CONSTRAINTS override, as the end of a transaction does. */
    public void clearConstraintModes() {
        allConstraintsDeferred = false;
        allConstraintsImmediate = false;
        deferredConstraintNames.clear();
        immediateConstraintNames.clear();
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
            if (isSerializable()) ssiWriteTables.add(schemaTable);
            // Keep an existing RR snapshot in sync: this transaction's own
            // uncommitted changes must remain visible to itself. Add the live
            // row reference so later in-place updates are visible too.
            List<Object[]> snapshot = rrSnapshots.get(schemaTable);
            if (snapshot != null) snapshot.add(row);
        }
    }

    /** Track an uncommitted update for this session. */
    public void trackUncommittedUpdate(String schemaTable, Object[] row, Object[] oldValues) {
        if (status == TransactionStatus.IN_TRANSACTION) {
            Map<Object[], Object[]> tableUpdates = uncommittedUpdates.computeIfAbsent(schemaTable,
                    k -> Collections.synchronizedMap(new IdentityHashMap<>()));
            // Only record the FIRST (original) old value; don't overwrite with intermediate values
            tableUpdates.putIfAbsent(row, oldValues);
            if (isSerializable()) ssiWriteTables.add(schemaTable);
            // Keep an existing RR snapshot in sync: swap the snapshotted copy
            // (holding the pre-update values) for the live row reference so this
            // transaction sees its own update.
            List<Object[]> snapshot = rrSnapshots.get(schemaTable);
            if (snapshot != null) swapInLiveRow(snapshot, row, oldValues);
        }
    }

    /** Track an uncommitted delete for this session. */
    public void trackUncommittedDelete(String schemaTable, List<Object[]> rows) {
        if (status == TransactionStatus.IN_TRANSACTION) {
            uncommittedDeletes.computeIfAbsent(schemaTable,
                    k -> Collections.synchronizedList(new ArrayList<>())).addAll(rows);
            if (isSerializable()) ssiWriteTables.add(schemaTable);
            // Keep an existing RR snapshot in sync: this transaction must no
            // longer see rows it deleted itself.
            List<Object[]> snapshot = rrSnapshots.get(schemaTable);
            if (snapshot != null) {
                for (Object[] row : rows) {
                    removeRowFromSnapshot(snapshot, row);
                }
            }
        }
    }

    /**
     * Replace the snapshot entry matching {@code matchValues} with the live
     * {@code row} reference. If the live row is already present (e.g. an insert
     * from this transaction), nothing needs to change: in-place updates to it
     * are visible automatically.
     */
    private static void swapInLiveRow(List<Object[]> snapshot, Object[] row, Object[] matchValues) {
        for (int i = 0; i < snapshot.size(); i++) {
            if (snapshot.get(i) == row) return;
        }
        for (int i = 0; i < snapshot.size(); i++) {
            if (Arrays.deepEquals(snapshot.get(i), matchValues)) {
                snapshot.set(i, row);
                return;
            }
        }
    }

    /** Remove one snapshot entry matching the given row (by identity, else by value). */
    private static void removeRowFromSnapshot(List<Object[]> snapshot, Object[] row) {
        for (int i = 0; i < snapshot.size(); i++) {
            if (snapshot.get(i) == row) {
                snapshot.remove(i);
                return;
            }
        }
        for (int i = 0; i < snapshot.size(); i++) {
            if (Arrays.deepEquals(snapshot.get(i), row)) {
                snapshot.remove(i);
                return;
            }
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

    /** True while this session still has uncommitted inserts, updates or deletes on a table. */
    public boolean hasUncommittedWork(String schemaTable) {
        return !getUncommittedInserts(schemaTable).isEmpty()
                || !getUncommittedUpdates(schemaTable).isEmpty()
                || !getUncommittedDeletes(schemaTable).isEmpty();
    }

    // Flag: true once the first RR/SERIALIZABLE snapshot has been taken in this transaction.
    // Once set, all subsequent table reads that don't already have a snapshot use current visible rows
    // (which is correct because no committed changes from other transactions should be visible).
    private boolean rrSnapshotTaken = false;
    private boolean snapshotImported = false;

    /** Get or create a REPEATABLE READ snapshot for a table. Returns null if not in RR mode. */
    public List<Object[]> getOrCreateRRSnapshot(String schemaTable, List<Object[]> currentVisibleRows) {
        String isolation = getEffectiveIsolationLevel();
        if (!"repeatable read".equals(isolation) && !"serializable".equals(isolation)) {
            return null; // Not in RR/SERIALIZABLE mode
        }
        if (status != TransactionStatus.IN_TRANSACTION) {
            return null; // Not in a transaction
        }
        // PG takes a transaction-wide snapshot at the first statement.
        // On first snapshot, eagerly snapshot ALL user tables so subsequent reads
        // see a consistent point-in-time across all tables.
        if (!rrSnapshotTaken) {
            rrSnapshotTaken = true;
            snapshotAllTables();
        }
        // If an imported snapshot already exists for this table, use it as-is.
        // This happens when SET TRANSACTION SNAPSHOT imported another session's snapshot.
        if (snapshotImported && rrSnapshots.containsKey(schemaTable)) {
            return rrSnapshots.get(schemaTable);
        }
        // Prefer the MVCC-visible rows from the caller over what
        // snapshotAllTables() stored: the caller's rows come through
        // applyMvccVisibility (filtered the same way) but additionally include
        // partition/inheritance child rows via getAllRowsWithSource.
        List<Object[]> snapshot = new ArrayList<>(currentVisibleRows.size());
        for (Object[] row : currentVisibleRows) {
            snapshot.add(Arrays.copyOf(row, row.length));
        }
        rrSnapshots.put(schemaTable, snapshot);
        return snapshot;
    }

    /** Whether this transaction has taken its initial snapshot (for RR/SERIALIZABLE). */
    public boolean isRRSnapshotTaken() { return rrSnapshotTaken; }

    /** C9: Clear the RR snapshot for a table (after TRUNCATE empties it). */
    public void clearRRSnapshotForTable(String schemaTable) {
        List<Object[]> snapshot = rrSnapshots.get(schemaTable);
        if (snapshot != null) snapshot.clear();
    }

    /**
     * Drop the RR snapshot for a table this session just reshaped with DDL. The snapshot rows
     * still have the old column count, so reading them after ALTER TABLE would index past their
     * end; PG simply shows the session its own DDL.
     */
    public void discardRRSnapshotForTable(String schemaTable) {
        rrSnapshots.remove(schemaTable);
    }

    /**
     * M7: RR write-write conflict detection.
     * Under REPEATABLE READ, if we try to modify a row that was changed by a concurrent
     * committed transaction (i.e., it's not in our snapshot), raise 40001.
     */
    public void checkRRWriteConflict(String schemaTable, Object[] oldValues) {
        String isolation = getEffectiveIsolationLevel();
        if (!"repeatable read".equals(isolation) && !"serializable".equals(isolation)) return;
        if (status != TransactionStatus.IN_TRANSACTION) return;
        List<Object[]> snapshot = rrSnapshots.get(schemaTable);
        if (snapshot == null) return;
        // Check if oldValues (the row we're about to modify) exists in our snapshot.
        // If it doesn't, another transaction must have modified it since our snapshot.
        for (Object[] snapRow : snapshot) {
            if (snapRow == oldValues || Arrays.deepEquals(snapRow, oldValues)) {
                return; // Row is in our snapshot — no conflict
            }
        }
        // Row not found in snapshot → concurrent modification
        throw new MemgresException(
            "could not serialize access due to concurrent update", "40001");
    }

    /** C10: Sync parent snapshot when an INSERT was routed to a child partition. */
    public void syncParentSnapshotOnInsert(String parentSchemaTable, Object[] row) {
        if (status != TransactionStatus.IN_TRANSACTION) return;
        List<Object[]> snapshot = rrSnapshots.get(parentSchemaTable);
        if (snapshot != null) snapshot.add(row);
    }

    /** Eagerly snapshot all user tables for transaction-wide consistency. */
    private void snapshotAllTables() {
        if (database == null) return;
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            String schemaName = schemaEntry.getKey();
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                String key = schemaName + "." + tableEntry.getKey();
                if (!rrSnapshots.containsKey(key)) {
                    rrSnapshots.put(key, buildCommittedSnapshot(key, tableEntry.getValue()));
                }
            }
        }
    }

    /**
     * Build a snapshot of a table's committed state (plus this session's own
     * uncommitted changes). Tables are mutated in place, so the live rows may
     * contain other sessions' uncommitted changes; reverse-apply them:
     * exclude their uncommitted inserts, restore the old values of their
     * uncommitted updates, and re-add their uncommitted deletes. This mirrors
     * FromResolver.applyMvccVisibility.
     */
    private List<Object[]> buildCommittedSnapshot(String schemaTable, Table table) {
        Set<Object[]> otherInserts = Collections.newSetFromMap(new IdentityHashMap<>());
        Map<Object[], Object[]> otherUpdates = new IdentityHashMap<>();
        List<Object[]> otherDeletes = new ArrayList<>();
        for (Session other : database.getActiveSessions()) {
            if (other == this || !other.isInTransaction()) continue;
            otherInserts.addAll(other.getUncommittedInserts(schemaTable));
            otherUpdates.putAll(other.getUncommittedUpdates(schemaTable));
            otherDeletes.addAll(other.getUncommittedDeletes(schemaTable));
        }
        List<Object[]> rows = table.getRows();
        List<Object[]> snapshot = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            if (otherInserts.contains(row)) continue; // uncommitted insert: invisible
            Object[] oldValues = otherUpdates.get(row);
            Object[] src = oldValues != null ? oldValues : row;
            snapshot.add(Arrays.copyOf(src, src.length));
        }
        for (Object[] deletedRow : otherDeletes) {
            if (otherInserts.contains(deletedRow)) continue; // inserted+deleted in same txn
            // If the row was updated before being deleted, the committed state
            // is the pre-update old values, not the row's current contents.
            Object[] oldValues = otherUpdates.get(deletedRow);
            Object[] src = oldValues != null ? oldValues : deletedRow;
            snapshot.add(Arrays.copyOf(src, src.length));
        }
        return snapshot;
    }

    /** Check if a REPEATABLE READ snapshot already exists for this table. */
    public boolean hasRRSnapshot(String schemaTable) {
        return rrSnapshots.containsKey(schemaTable);
    }

    /** Get existing RR snapshot (returns null if none exists). */
    public List<Object[]> getRRSnapshot(String schemaTable) {
        return rrSnapshots.get(schemaTable);
    }


    /** Import an exported snapshot into this session's RR snapshots. */
    public void importSnapshot(Database db, String snapshotId) {
        // Whether the transaction may take another transaction's snapshot at all is settled
        // before the identifier is read, so a session at READ COMMITTED is told what is wrong
        // with the transaction rather than what is wrong with a name it was never going to use.
        String level = getEffectiveIsolationLevel();
        if (level != null && !level.startsWith("serializable") && !level.startsWith("repeatable")) {
            throw new MemgresException("a snapshot-importing transaction must have isolation level "
                    + "SERIALIZABLE or REPEATABLE READ", "0A000");
        }
        Map<String, List<Object[]>> snap = db.importSnapshot(snapshotId);
        if (snap == null) {
            throw new MemgresException("invalid snapshot identifier: \"" + snapshotId + "\"", "22023");
        }
        rrSnapshots.clear();
        for (Map.Entry<String, List<Object[]>> entry : snap.entrySet()) {
            List<Object[]> copied = new ArrayList<>(entry.getValue().size());
            for (Object[] row : entry.getValue()) {
                copied.add(java.util.Arrays.copyOf(row, row.length));
            }
            rrSnapshots.put(entry.getKey(), copied);
        }
        rrSnapshotTaken = true;
        snapshotImported = true;
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

    /**
     * Check if the current transaction is read-only.
     *
     * <p>{@code default_transaction_read_only} only supplies the starting value:
     * {@code BEGIN READ WRITE} (or {@code SET TRANSACTION READ WRITE}) says what this
     * transaction is, and has to win. Letting the session default win instead refuses
     * the writes of a transaction that explicitly asked to make them.
     */
    public boolean isReadOnly() {
        if (gucSettings.hasSessionOverride("transaction_read_only")) {
            return "on".equalsIgnoreCase(gucSettings.get("transaction_read_only"));
        }
        return "on".equalsIgnoreCase(gucSettings.get("default_transaction_read_only"));
    }

    /** Check if the current transaction uses SERIALIZABLE isolation. */
    public boolean isSerializable() {
        return "serializable".equals(getEffectiveIsolationLevel());
    }

    /** Track that this serializable transaction read from a table. */
    public void trackSsiRead(String schemaTable) {
        if (status == TransactionStatus.IN_TRANSACTION && isSerializable()) {
            ssiReadTables.add(schemaTable);
        }
    }

    /** Get SSI read tables (for cross-session conflict detection). */
    public Set<String> getSsiReadTables() { return ssiReadTables; }

    /** Get SSI write tables (for cross-session conflict detection). */
    public Set<String> getSsiWriteTables() { return ssiWriteTables; }

    /**
     * SSI write-skew detection at commit time.
     * Checks for rw-conflict cycles with recently committed serializable transactions.
     * The first transaction to commit always succeeds; subsequent conflicting ones fail.
     * If a dangerous structure is found, throws serialization_failure (40001).
     */
    private void checkSsiConflicts() {
        if (!isSerializable()) return;
        if (ssiWriteTables.isEmpty() && ssiReadTables.isEmpty()) return;

        // Check against recently committed serializable transactions for conflicts.
        // Only consider transactions that committed after this transaction began,
        // as earlier commits are already reflected in our snapshot.
        for (Database.CommittedSsiInfo info : database.getRecentlyCommittedSsiTransactions()) {
            if (info.sequence() <= ssiTxnStartSeq) continue;
            // Check for rw-conflict: this read X, other wrote X (phantom prevention)
            // If another serializable transaction committed writes to a table we read,
            // and we also write (to any table), we have a potential serialization anomaly.
            boolean thisReadOtherWrote = false;
            for (String table : ssiReadTables) {
                if (info.writeTables().contains(table)) {
                    thisReadOtherWrote = true;
                    break;
                }
            }
            if (thisReadOtherWrote && !ssiWriteTables.isEmpty()) {
                throw new MemgresException(
                    "could not serialize access due to read/write dependencies among transactions",
                    "40001");
            }

            // Check for rw-conflict cycle (write-skew):
            // other read Y, this wrote Y (rw-dependency: other -> this)
            if (thisReadOtherWrote) {
                boolean otherReadThisWrote = false;
                for (String table : info.readTables()) {
                    if (ssiWriteTables.contains(table)) {
                        otherReadThisWrote = true;
                        break;
                    }
                }
                if (otherReadThisWrote) {
                    throw new MemgresException(
                        "could not serialize access due to read/write dependencies among transactions",
                        "40001");
                }
            }
        }
    }

    /** Clear SSI tracking state (called on commit/rollback). */
    private void clearSsiState() {
        ssiReadTables.clear();
        ssiWriteTables.clear();
        ssiTxnStartSeq = 0;
    }

    // ---- Undo log ----

    public void recordUndo(UndoEntry entry) {
        if (status == TransactionStatus.IN_TRANSACTION || stmtScopeDepth > 0) {
            undoLog.add(entry);
        }
    }

    /** Where the undo log stands now, for a caller that may need to drop what it adds. */
    public int undoMark() {
        return undoLog.size();
    }

    /**
     * Forget the undo entries recorded since {@code mark}.
     *
     * <p>For a caller that has already put the tables back by another route: the DML paths keep a
     * snapshot of their target tables when triggers are involved and restore it wholesale on
     * failure, and applying the row-by-row undo on top of that would write the same rows twice.
     */
    public void discardUndoSince(int mark) {
        if (mark >= 0 && mark < undoLog.size()) {
            undoLog.subList(mark, undoLog.size()).clear();
        }
    }

    /**
     * Forget everything the statement now running has recorded. Used by a DML path that has put
     * its target tables back from a snapshot of its own: the entries describe changes that have
     * already been reversed, and applying them again would write the same rows a second time.
     */
    public void discardUndoForCurrentStatement() {
        discardUndoSince(stmtScopeMark);
    }

    /** Nesting depth of the statement now running; only the outermost owns the scope. */
    private int stmtScopeDepth = 0;
    /** Where the undo log stood when the outermost statement began, or -1 outside one. */
    private int stmtScopeMark = -1;
    /** Whether that statement began outside an explicit transaction. */
    private boolean stmtScopeOutsideTransaction = false;

    /**
     * Open the scope a single statement runs in.
     *
     * <p>Outside an explicit transaction every statement is a transaction of its own, so a
     * statement that fails partway must leave nothing behind: PostgreSQL rolls back a multi-row
     * UPDATE that divides by zero on its third row, and a query whose data-modifying WITH item has
     * already written when a later clause turns out to be invalid. That needs the writes to be
     * undoable, which they were not -- the undo log only recorded inside a transaction.
     *
     * <p>Nested calls (a CTE, a function body) join the scope the outermost statement opened
     * rather than starting one of their own, so an inner failure that an outer statement catches
     * does not undo work the outer statement means to keep.
     */
    public void beginStatementScope() {
        if (stmtScopeDepth == 0) {
            stmtScopeMark = undoLog.size();
            stmtScopeOutsideTransaction = status != TransactionStatus.IN_TRANSACTION;
        }
        stmtScopeDepth++;
    }

    /**
     * Close it. Inside a transaction the entries stay for COMMIT or ROLLBACK to deal with;
     * outside one they are undone if the statement failed and dropped if it succeeded, because
     * nothing after the statement can roll it back.
     */
    public void endStatementScope(boolean failed) {
        if (stmtScopeDepth > 0) stmtScopeDepth--;
        if (stmtScopeDepth > 0) return;
        int mark = stmtScopeMark;
        boolean outside = stmtScopeOutsideTransaction;
        stmtScopeMark = -1;
        stmtScopeOutsideTransaction = false;
        if (mark < 0 || !outside || status == TransactionStatus.IN_TRANSACTION) return;
        if (mark > undoLog.size()) return;
        if (failed) {
            applyUndo(mark);
        } else if (mark < undoLog.size()) {
            undoLog.subList(mark, undoLog.size()).clear();
        }
    }

    private void applyUndo(int fromPosition) {
        // Apply in reverse order from end to fromPosition
        for (int i = undoLog.size() - 1; i >= fromPosition; i--) {
            UndoEntry entry = undoLog.get(i);
            syncSnapshotBeforeUndo(entry);
            entry.undo(database);
        }
        // Truncate the undo log
        if (fromPosition < undoLog.size()) {
            undoLog.subList(fromPosition, undoLog.size()).clear();
        }
    }

    /**
     * Keep RR snapshots consistent when this session's own DML is undone
     * (ROLLBACK TO SAVEPOINT while the transaction stays open). Full ROLLBACK
     * clears the snapshots before applying undo, making this a no-op there.
     */
    private void syncSnapshotBeforeUndo(UndoEntry entry) {
        if (rrSnapshots.isEmpty()) return;
        if (entry instanceof InsertUndo) {
            InsertUndo iu = (InsertUndo) entry;
            List<Object[]> snapshot = rrSnapshots.get(iu.schema + "." + iu.tableName);
            if (snapshot != null) removeRowFromSnapshot(snapshot, iu.row);
        } else if (entry instanceof DeleteUndo) {
            DeleteUndo du = (DeleteUndo) entry;
            List<Object[]> snapshot = rrSnapshots.get(du.schema + "." + du.tableName);
            if (snapshot != null) snapshot.addAll(du.rows);
        } else if (entry instanceof UpdateUndo) {
            UpdateUndo uu = (UpdateUndo) entry;
            List<Object[]> snapshot = rrSnapshots.get(uu.schema + "." + uu.tableName);
            // Ensure the snapshot holds the live row reference (matched by its
            // current, pre-undo contents) so the in-place restore of the old
            // values is visible to this transaction.
            if (snapshot != null) swapInLiveRow(snapshot, uu.row, uu.row);
        } else if (entry instanceof TruncateUndo) {
            // C9: Restore the snapshot to the pre-truncate rows on savepoint rollback
            TruncateUndo tu = (TruncateUndo) entry;
            List<Object[]> snapshot = rrSnapshots.get(tu.schema + "." + tu.tableName);
            if (snapshot != null) {
                snapshot.clear();
                snapshot.addAll(tu.rows);
            }
        }
    }

    // ---- Undo entry types ----

    public interface UndoEntry {
        void undo(Database db);
    }

    /**
     * Undo a CREATE TYPE ... AS ENUM.
     *
     * <p>DDL is transactional in PostgreSQL, so a type whose transaction rolled back never
     * existed. Leaving it behind makes a type nobody created castable from every session.
     */
    public static final class CreateEnumTypeUndo implements UndoEntry {
        public final String schema;
        public final String typeName;

        public CreateEnumTypeUndo(String schema, String typeName) {
            this.schema = schema;
            this.typeName = typeName;
        }

        @Override
        public void undo(Database db) {
            db.removeCustomEnum(schema + "." + typeName);
            db.unregisterSchemaObject(schema, "enum", typeName);
        }
    }

    /**
     * Undo a CREATE TYPE ... AS (...). A composite owns a pg_class row as well as a pg_type row,
     * so both go when the transaction that made them does.
     */
    public static final class CreateCompositeTypeUndo implements UndoEntry {
        public final String schema;
        public final String typeName;

        public CreateCompositeTypeUndo(String schema, String typeName) {
            this.schema = schema;
            this.typeName = typeName;
        }

        @Override
        public void undo(Database db) {
            db.getCompositeTypes().remove(TypeNamespace.key(schema, typeName));
            db.unregisterSchemaObject(schema, "composite", typeName);
        }
    }

    /**
     * Undo a CREATE TYPE ... AS RANGE. A range is a type like any other, and a transaction that
     * rolled back created none: the name it took has to be free again for the next statement.
     */
    public static final class CreateRangeTypeUndo implements UndoEntry {
        public final String schema;
        public final String typeName;

        public CreateRangeTypeUndo(String schema, String typeName) {
            this.schema = schema;
            this.typeName = typeName;
        }

        @Override
        public void undo(Database db) {
            db.getRangeTypes().remove(TypeNamespace.key(schema, typeName));
            db.unregisterSchemaObject(schema, "range", typeName);
        }
    }

    /** Undo a CREATE DOMAIN. */
    public static final class CreateDomainUndo implements UndoEntry {
        public final String schema;
        public final String typeName;

        public CreateDomainUndo(String schema, String typeName) {
            this.schema = schema;
            this.typeName = typeName;
        }

        @Override
        public void undo(Database db) {
            db.getDomains().remove(TypeNamespace.key(schema, typeName));
            db.unregisterSchemaObject(schema, "domain", typeName);
        }
    }

    /**
     * Undo an ALTER TABLE ... RENAME. A rename is a name appearing and a name going away, and a
     * rolled-back transaction has done neither — leaving the new name behind meant the relation
     * answered to a name no committed statement ever gave it.
     */
    public static final class RenameTableUndo implements UndoEntry {
        public final String schemaName;
        public final String oldName;
        public final String newName;
        public final Table original;

        public RenameTableUndo(String schemaName, String oldName, String newName, Table original) {
            this.schemaName = schemaName;
            this.oldName = oldName;
            this.newName = newName;
            this.original = original;
        }

        @Override
        public void undo(Database db) {
            Schema schema = db.getSchema(schemaName);
            if (schema == null) return;
            schema.removeTable(newName);
            if (original != null) schema.addTable(original);
        }
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
            if (s != null) {
                Table t = s.getTable(tableName);
                // If the created table was a partition, detach it from its parent's routing list
                if (t != null && t.getPartitionParent() != null) {
                    t.getPartitionParent().removePartition(t);
                }
                s.removeTable(tableName);
            }
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
        /** Triggers the DROP removed; a rollback has to put them back with the table. */
        public final java.util.List<PgTrigger> triggers;

        public DropTableUndo(String schema, String tableName, Table table) {
            this(schema, tableName, table, null);
        }

        public DropTableUndo(String schema, String tableName, Table table,
                             java.util.List<PgTrigger> triggers) {
            this.schema = schema;
            this.tableName = tableName;
            this.table = table;
            this.triggers = triggers;
        }

        @Override
        public void undo(Database db) {
            Schema s = db.getSchema(schema);
            if (s != null) s.addTable(table);
            db.restoreTriggersForTable(tableName, triggers);
            // If the dropped table was a partition, re-attach it to its parent's routing list
            Table parent = table.getPartitionParent();
            if (parent != null && !parent.getPartitions().contains(table)) {
                parent.addPartition(table);
            }
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

    /** C11: Undo a sequence restart (from TRUNCATE ... RESTART IDENTITY). */
    public static final class SequenceRestartUndo implements UndoEntry {
        public final String seqName;
        public final long previousValue;
        public final boolean wasCalled;

        public SequenceRestartUndo(String seqName, long previousValue, boolean wasCalled) {
            this.seqName = seqName;
            this.previousValue = previousValue;
            this.wasCalled = wasCalled;
        }

        @Override
        public void undo(Database db) {
            Sequence seq = db.getSequence(seqName);
            if (seq == null) return;
            if (wasCalled) {
                seq.setVal(previousValue);
            } else {
                seq.restart(previousValue);
            }
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
    /**
     * Undo a CREATE SCHEMA. DDL is transactional, so a schema a rolled-back transaction created
     * has to go with it — leaving it behind made the next CREATE SCHEMA of that name fail with
     * 42P06 for a schema nobody had successfully created.
     */
    public static final class CreateSchemaUndo implements UndoEntry {
        public final String schemaName;

        public CreateSchemaUndo(String schemaName) {
            this.schemaName = schemaName;
        }

        @Override
        public void undo(Database db) {
            db.removeSchema(schemaName);
        }

        public String schemaName() { return schemaName; }

        @Override
        public String toString() {
            return "CreateSchemaUndo[schemaName=" + schemaName + "]";
        }
    }

        public static final class CreateViewUndo implements UndoEntry {
        public final String viewName;
        /** The schema the view was created in: without it the undo dropped whichever view of
         *  that name it found first, so rolling back a create in one schema removed another
         *  schema's view of the same name and left the created one standing. */
        public final String schemaName;

        public CreateViewUndo(String viewName) {
            this(null, viewName);
        }

        public CreateViewUndo(String schemaName, String viewName) {
            this.schemaName = schemaName;
            this.viewName = viewName;
        }

        @Override
        public void undo(Database db) {
            if (schemaName != null) db.removeView(schemaName, viewName);
            else db.removeView(viewName);
        }

        public String viewName() { return viewName; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CreateViewUndo that = (CreateViewUndo) o;
            return java.util.Objects.equals(viewName, that.viewName)
                    && java.util.Objects.equals(schemaName, that.schemaName);
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

    /**
     * Undo an ALTER DOMAIN. The domain object is shared by every session and by every column
     * declared with it, so an ALTER inside a transaction that rolls back has to put back what
     * was there — otherwise the rolled-back rule keeps refusing rows nobody ever forbade.
     */
    public static final class AlterDomainUndo implements UndoEntry {
        public final String domainName;
        private final boolean notNull;
        private final String defaultValue;
        private final List<DomainType.NamedConstraint> constraints;

        public AlterDomainUndo(DomainType domain) {
            this.domainName = domain.getName();
            this.notNull = domain.isNotNull();
            this.defaultValue = domain.getDefaultValue();
            this.constraints = new ArrayList<>();
            // The constraint objects are mutable — RENAME CONSTRAINT and VALIDATE change them
            // in place — so the snapshot has to hold copies rather than the same objects.
            for (DomainType.NamedConstraint nc : domain.getNamedConstraints()) {
                this.constraints.add(new DomainType.NamedConstraint(nc.name(), nc.rawCheckExpr(),
                        nc.parsedCheck(), nc.isValidated()));
            }
        }

        @Override
        public void undo(Database db) {
            DomainType domain = db.getDomain(domainName);
            if (domain == null) return;
            domain.setNotNull(notNull);
            domain.setDefaultValue(defaultValue);
            domain.getNamedConstraints().clear();
            domain.getNamedConstraints().addAll(constraints);
        }

        @Override
        public String toString() {
            return "AlterDomainUndo[domainName=" + domainName + "]";
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
