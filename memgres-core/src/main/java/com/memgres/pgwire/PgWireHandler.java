package com.memgres.pgwire;


import com.memgres.core.Memgres;
import com.memgres.engine.*;
import com.memgres.engine.DatabaseRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles decoded PostgreSQL wire protocol messages and sends responses.
 * Supports both simple and extended query protocols.
 * Delegates to PgWireBinaryCodec, PgWireCopyHandler, PgWireDescribeHelper, PgWireValueFormatter.
 */
public class PgWireHandler extends SimpleChannelInboundHandler<PgWireMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(PgWireHandler.class);

    private final DatabaseRegistry registry;
    private Database database;
    private Session session;
    private final CancelRegistry cancelRegistry;
    private PgWireCopyHandler copyHandler;
    private PgWireDescribeHelper describeHelper;
    private boolean connectionRegistered;
    private int backendPid;
    private byte[] backendSecretKey = new byte[0];
    /** The minor protocol version the client and this server settled on; 2 asks for more of us. */
    private int protocolMinor;
    private String databaseName;

    /** Prepared statement: stores the SQL and parameter OIDs from Parse. */
        private static final class PreparedStmt {
        public final String sql;
        public final int[] paramOids;

        public PreparedStmt(String sql, int[] paramOids) {
            this.sql = sql;
            this.paramOids = paramOids;
        }

        public String sql() { return sql; }
        public int[] paramOids() { return paramOids; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PreparedStmt that = (PreparedStmt) o;
            return java.util.Objects.equals(sql, that.sql)
                && java.util.Arrays.equals(paramOids, that.paramOids);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(sql, java.util.Arrays.hashCode(paramOids));
        }

        @Override
        public String toString() {
            return "PreparedStmt[sql=" + sql + ", " + "paramOids=" + java.util.Arrays.toString(paramOids) + "]";
        }
    }
    /** Tracks whether Describe Statement sent RowDescription for a named prepared statement */
    private final Map<String, Boolean> stmtDescribed = new HashMap<>();

    /** Portal: stores the SQL, bound parameter values, and result format codes from Bind. */
    private static class Portal {
        final String sql;
        final List<Object> paramValues;
        final short[] resultFormatCodes;
        QueryResult suspendedResult;
        int suspendedOffset;
        QueryResult describeResult;
        /**
         * What the run Describe made of this portal's statement was refused with. PostgreSQL runs
         * a statement once, so that refusal is the portal's answer and Execute reports it; asking
         * again put the statement to a question whose circumstances the first run had used up.
         */
        RuntimeException describeFailure;
        boolean rowDescriptionSent;
        boolean describeAttempted;
        /**
         * True once the portal has delivered everything it has. A cleared suspendedResult cannot
         * say so on its own — it looks exactly like a portal that has not started — so an Execute
         * on a finished portal ran the statement a second time, side effects and all.
         */
        boolean done;
        /** Whether the finished portal returned rows, which is what decides how PG answers it. */
        boolean rowReturning;
        /** The finished result's kind, so the zero-count tag names the right verb. */
        QueryResult.Type completedType;
        String stmtName = "";

        Portal(String sql, List<Object> paramValues, short[] resultFormatCodes) {
            this.sql = sql;
            this.paramValues = paramValues;
            this.resultFormatCodes = resultFormatCodes;
        }

        String sql() { return sql; }
        List<Object> paramValues() { return paramValues; }
        short[] resultFormatCodes() { return resultFormatCodes; }
    }

    private final Map<String, PreparedStmt> preparedStatements = new HashMap<>();
    private final Map<String, Portal> portals = new HashMap<>();
    private boolean rowDescSentByDescribe;
    private boolean errorPendingUntilSync;
    /** Set between asking for a password and being given one; nothing else may run in between. */
    private boolean awaitingPassword;
    /** The frontend message being dispatched, so a failure can be answered the way PG answers it. */
    private byte currentFrontendType;

    public PgWireHandler(DatabaseRegistry registry, CancelRegistry cancelRegistry) {
        this.registry = registry;
        this.cancelRegistry = cancelRegistry;
        // database/session/copyHandler/describeHelper are initialized in handleStartup
        // when we know which database the client wants to connect to.
        // For safety, set defaults to the default database (handles edge cases).
        this.database = registry.getDefaultDatabase();
        this.databaseName = registry.getDefaultDatabaseName();
        this.session = new Session(database);
        this.session.setDatabaseName(databaseName);
        this.session.setDatabaseRegistry(registry);
        this.copyHandler = new PgWireCopyHandler(session, this);
        this.describeHelper = new PgWireDescribeHelper(session, database);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, PgWireMessage msg) {
        cancelIdleInTransactionTimeout();
        try {
            dispatch(ctx, msg);
        } finally {
            armIdleInTransactionTimeout(ctx);
        }
    }

    private void dispatch(ChannelHandlerContext ctx, PgWireMessage msg) {
        currentFrontendType = (byte) frontendTypeOf(msg);
        // A message the decoder could not read is answered before anything else: the connection
        // is out of step with its peer, and nothing later in this method is true of it.
        if (msg.getType() == PgWireMessage.Type.PROTOCOL_ERROR) {
            handleProtocolError(ctx, msg);
            return;
        }
        if (awaitingPassword && msg.getType() != PgWireMessage.Type.PASSWORD) {
            // PG names the message it got where the password was due, and hangs up: a connection
            // that has not authenticated does not get to run statements.
            sendFatal(ctx, "08P01", "expected password response, got message type "
                    + frontendTypeOf(msg));
            closeAfterFlush(ctx);
            return;
        }
        if (errorPendingUntilSync) {
            // Sync is the only thing that ends it. Answering a Query, a Flush or a Terminate in
            // the meantime finished statements the client had already been told were skipped.
            if (msg.getType() == PgWireMessage.Type.SYNC) {
                handleSync(ctx);
            } else if (Memgres.logAllStatements) {
                LOG.info("[PROTO] Discarding {} (errorPendingUntilSync)", msg.getType());
            }
            return;
        }
        if (copyHandler.inCopyFromMode) {
            switch (msg.getType()) {
                case COPY_DATA:
                    copyHandler.handleCopyData(ctx, msg);
                    break;
                case COPY_DONE:
                    copyHandler.handleCopyDone(ctx);
                    break;
                case COPY_FAIL:
                    copyHandler.handleCopyFail(ctx, msg);
                    break;
                case SYNC: {
                    break;
                }
                default:
                    LOG.warn("[PROTO] Unexpected message type {} during COPY FROM", msg.getType());
                    break;
            }
            return;
        }
        switch (msg.getType()) {
            case SSL_REQUEST:
            case GSSENC_REQUEST:
                declineEncryption(ctx);
                break;
            case NEGOTIATE_PROTOCOL:
                sendNegotiateProtocolVersion(ctx, msg);
                break;
            case FUNCTION_CALL:
                handleFunctionCall(ctx, msg);
                break;
            case STARTUP:
                handleStartup(ctx, msg);
                break;
            case PASSWORD:
                handlePassword(ctx, msg);
                break;
            case QUERY:
                handleQuery(ctx, msg);
                break;
            case PARSE:
                handleParse(ctx, msg);
                break;
            case BIND:
                handleBind(ctx, msg);
                break;
            case DESCRIBE:
                handleDescribe(ctx, msg);
                break;
            case EXECUTE:
                handleExecute(ctx, msg);
                break;
            case SYNC: {
                if (Memgres.logAllStatements) LOG.info("[PROTO] Sync"); handleSync(ctx); 
                break;
            }
            case FLUSH: {
                if (Memgres.logAllStatements) LOG.info("[PROTO] Flush"); handleFlush(ctx); 
                break;
            }
            case CLOSE:
                handleClose(ctx, msg);
                break;
            case TERMINATE:
                ctx.close();
                break;
            case COPY_DATA:
            case COPY_DONE:
            case COPY_FAIL:
                LOG.warn("[PROTO] COPY message {} received outside copy mode", msg.getType());
                break;
        }
    }

    // ---- idle_in_transaction_session_timeout ----

    /** Armed while the client sits idle holding an open transaction; null the rest of the time. */
    private io.netty.util.concurrent.ScheduledFuture<?> idleInTransactionTask;

    private void cancelIdleInTransactionTimeout() {
        if (idleInTransactionTask != null) {
            idleInTransactionTask.cancel(false);
            idleInTransactionTask = null;
        }
    }

    /**
     * A transaction left open holds locks and pins the oldest snapshot, so PG puts a limit on how
     * long a client may sit on one and drops the connection when it is exceeded. The clock only
     * runs between messages: it is restarted whenever the client says something.
     */
    private void armIdleInTransactionTimeout(final ChannelHandlerContext ctx) {
        if (session == null || !session.isInTransaction() || !ctx.channel().isActive()) return;
        long timeoutMs = GucSettings.parseTimeoutMillis(
                session.getGucSettings().get("idle_in_transaction_session_timeout"));
        if (timeoutMs <= 0) return;
        idleInTransactionTask = ctx.executor().schedule(new Runnable() {
            @Override
            public void run() {
                idleInTransactionTask = null;
                if (session == null || !session.isInTransaction()) return;
                session.rollback();
                sendFatal(ctx, "25P03", "terminating connection due to idle-in-transaction timeout");
                ctx.close();
            }
        }, timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    // ---- Connection lifecycle ----

    /**
     * A message whose bytes did not match the length above them. PostgreSQL answers it the way it
     * answers any other error: an ErrorResponse, and then either a ReadyForQuery or, if the
     * message was extended-query work, silence until the client sends Sync. A header that could
     * not be believed at all is not answered, because there is nothing to answer about.
     */
    private void handleProtocolError(ChannelHandlerContext ctx, PgWireMessage msg) {
        if (msg.isFatal()) {
            if (msg.getSqlState() != null) sendFatal(ctx, msg.getSqlState(), msg.getQuery());
            closeAfterFlush(ctx);
            return;
        }
        sendErrorSimple(ctx, msg.getSqlState(), msg.getQuery());
        if (PgWireDecoder.isExtendedQueryMessage(msg.getOffendingType())) {
            extendedErrorReported(ctx);
        } else {
            sendReadyForQuery(ctx, session);
        }
    }

    private static void closeAfterFlush(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(ctx.alloc().buffer(0)).addListener(future -> ctx.close());
    }

    /** The frontend message type byte a decoded message came from, for PG's own wording. */
    private static int frontendTypeOf(PgWireMessage msg) {
        switch (msg.getType()) {
            case QUERY: return 'Q';
            case PARSE: return 'P';
            case BIND: return 'B';
            case DESCRIBE: return 'D';
            case EXECUTE: return 'E';
            case SYNC: return 'S';
            case FLUSH: return 'H';
            case CLOSE: return 'C';
            case TERMINATE: return 'X';
            case FUNCTION_CALL: return 'F';
            case COPY_DATA: return 'd';
            case COPY_DONE: return 'c';
            case COPY_FAIL: return 'f';
            default: return 0;
        }
    }

    /** Neither SSL nor GSSAPI encryption is offered, and PG declines both with a single byte. */
    private void declineEncryption(ChannelHandlerContext ctx) {
        ByteBuf buf = ctx.alloc().buffer(1);
        buf.writeByte('N');
        ctx.writeAndFlush(buf);
    }

    /** Tell a client that asked for more than this server has what it is actually going to get. */
    private void sendNegotiateProtocolVersion(ChannelHandlerContext ctx, PgWireMessage msg) {
        java.util.List<String> unsupported = msg.getUnsupportedOptions();
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('v');
        int lengthIdx = buf.writerIndex();
        buf.writeInt(0);
        buf.writeInt((3 << 16) | msg.getProtocolMinor());
        buf.writeInt(unsupported.size());
        for (String option : unsupported) {
            PgWireValueFormatter.writeCString(buf, option);
        }
        buf.setInt(lengthIdx, buf.writerIndex() - lengthIdx);
        ctx.write(buf);
    }

    private void handleStartup(ChannelHandlerContext ctx, PgWireMessage msg) {
        protocolMinor = msg.getProtocolMinor();
        Map<String, String> params = msg.getParameters();
        if (params != null) {
            // Resolve target database from startup parameters
            String requestedDb = params.get("database");
            if (requestedDb != null && !requestedDb.isEmpty()) {
                Database resolved = registry.getDatabase(requestedDb);
                if (resolved == null) {
                    if (registry.isAutoCreateDatabases()) {
                        registry.createDatabase(requestedDb);
                        resolved = registry.getDatabase(requestedDb);
                    } else {
                        sendErrorSimple(ctx, "3D000", "database \"" + requestedDb + "\" does not exist");
                        ctx.writeAndFlush(ctx.alloc().buffer(0)).addListener(future -> ctx.close());
                        return;
                    }
                }
                // Close the default session created in the constructor before switching
                if (this.session != null) {
                    this.session.close();
                }
                this.database = resolved;
                this.databaseName = requestedDb;
                this.session = new Session(database);
                this.session.setDatabaseName(requestedDb);
                this.session.setDatabaseRegistry(registry);
                this.copyHandler = new PgWireCopyHandler(session, this);
                this.describeHelper = new PgWireDescribeHelper(session, database);
            }

            String connectingUser = params.get("user");
            if (connectingUser != null && !connectingUser.isEmpty()) {
                session.getGucSettings().set("session_authorization", connectingUser);
                session.getGucSettings().setBootDefault("session_authorization", connectingUser);
                session.getGucSettings().setBootDefault("role", connectingUser);
                session.setConnectingUser(connectingUser);
                if (!database.hasRole(connectingUser)) {
                    database.createRole(connectingUser, new java.util.HashMap<>());
                }
            }
            String appName = params.get("application_name");
            if (appName != null) {
                session.setApplicationName(appName);
                session.getGucSettings().set("application_name", appName);
            }
        }

        ByteBuf auth = ctx.alloc().buffer();
        auth.writeByte('R');
        auth.writeInt(8);
        auth.writeInt(3); // cleartext password
        ctx.write(auth);
        ctx.flush();
        awaitingPassword = true;
    }

    private void handlePassword(ChannelHandlerContext ctx, PgWireMessage msg) {
        if (!awaitingPassword) {
            // Once a connection is up, a password message is as much a stranger as any other
            // type PG does not expect there; re-running the greeting for one let a client
            // reopen the session it was already inside.
            sendFatal(ctx, "08P01", "invalid frontend message type 112");
            closeAfterFlush(ctx);
            return;
        }
        awaitingPassword = false;
        if (!database.registerConnection()) {
            sendErrorSimple(ctx, "53300", "sorry, too many clients already");
            ctx.writeAndFlush(ctx.alloc().buffer(0)).addListener(future -> ctx.close());
            return;
        }
        connectionRegistered = true;

        ByteBuf authOk = ctx.alloc().buffer();
        authOk.writeByte('R');
        authOk.writeInt(8);
        authOk.writeInt(0);
        ctx.write(authOk);

        sendParameterStatus(ctx, "server_version", "18.0");
        sendParameterStatus(ctx, "server_encoding", "UTF8");
        sendParameterStatus(ctx, "client_encoding", "UTF8");
        sendParameterStatus(ctx, "DateStyle", "ISO, MDY");
        sendParameterStatus(ctx, "integer_datetimes", "on");
        sendParameterStatus(ctx, "standard_conforming_strings", "on");
        sendParameterStatus(ctx, "TimeZone", "UTC");
        sendParameterStatus(ctx, "application_name",
                session.getGucSettings() != null && session.getGucSettings().get("application_name") != null
                        ? session.getGucSettings().get("application_name") : "");
        sendParameterStatus(ctx, "IntervalStyle", "postgres");
        sendParameterStatus(ctx, "is_superuser", "on");

        backendPid = cancelRegistry.nextPid();
        // Protocol 3.2 asks for a cancel key long enough not to be guessed; 3.0 has room for four
        // bytes and no more, so the key is as long as the version in force allows.
        backendSecretKey = new byte[protocolMinor >= 2 ? 32 : 4];
        new java.security.SecureRandom().nextBytes(backendSecretKey);
        cancelRegistry.register(backendPid, backendSecretKey);
        ByteBuf keyData = ctx.alloc().buffer();
        keyData.writeByte('K');
        keyData.writeInt(8 + backendSecretKey.length);
        keyData.writeInt(backendPid);
        keyData.writeBytes(backendSecretKey);
        ctx.write(keyData);

        // From here on the connection may sit idle waiting on the socket; a NOTIFY has to
        // reach it then, not only when it next issues a statement.
        installNotificationSink(ctx);
        sendReadyForQuery(ctx, session);
    }

    /**
     * Push a NOTIFY straight to this connection. The notifying session runs on another thread,
     * so the write is scheduled onto this channel's event loop; Netty serialises it against
     * whatever this connection is writing.
     */
    private void installNotificationSink(ChannelHandlerContext ctx) {
        final Session target = this.session;
        target.setNotificationSink(() -> {
            if (!ctx.channel().isActive()) return;
            ctx.channel().eventLoop().execute(() -> {
                if (!ctx.channel().isActive()) return;
                if (drainNotifications(ctx, target)) ctx.flush();
            });
        });
    }

    // ---- Query execution with cancel support ----

    private QueryResult executeWithCancel(String sql) {
        cancelRegistry.setExecutingThread(backendPid, backendSecretKey, Thread.currentThread());
        try {
            return session.execute(sql);
        } finally {
            cancelRegistry.setExecutingThread(backendPid, backendSecretKey, null);
        }
    }

    private QueryResult executeWithCancel(String sql, List<Object> params) {
        cancelRegistry.setExecutingThread(backendPid, backendSecretKey, Thread.currentThread());
        try {
            return session.execute(sql, params);
        } finally {
            cancelRegistry.setExecutingThread(backendPid, backendSecretKey, null);
        }
    }

    // ---- Simple query protocol ----

    private void handleQuery(ChannelHandlerContext ctx, PgWireMessage msg) {
        String sql = msg.getQuery();
        try {
            String[] statements = splitStatements(sql);
            boolean batchFailed = false;
            for (String stmt : statements) {
                if (Memgres.logAllStatements) LOG.info("Executing statement: {}", stmt);
                stmt = stmt.trim();
                if (stmt.isEmpty()) continue;
                if (batchFailed) continue;

                try {
                    session.setQueryState(stmt);
                    QueryResult result = executeWithCancel(stmt);
                    session.setIdleState();
                    // Count autocommit statements as committed transactions
                    if (!session.isExplicitTransactionBlock()) {
                        database.incrementXactCommit();
                        // PG releases xact-level advisory locks at statement end in autocommit
                        database.releaseXactAdvisoryLocks(session);
                    }
                    sendQueryResult(ctx, result);
                    // Emit ParameterStatus updates for tracked GUC parameters after SET
                    if (result.getType() == QueryResult.Type.SET) {
                        emitParameterStatusUpdates(ctx, stmt);
                    }
                } catch (MemgresException e) {
                    enrichErrorPosition(e, stmt);
                    // Log errors that occur inside transactions — these cascade and cause
                    // all subsequent commands to fail with 25P02, making root-cause hard to find.
                    if (session != null && session.isInTransaction() && !"25P02".equals(e.getSqlState())) {
                        LOG.warn("Error in transaction [{}]: {} (SQL: {})",
                                e.getSqlState(), e.getMessage(), stmt);
                    }
                    sendErrorWithDetails(ctx, e, false);
                    batchFailed = true;
                } catch (ArithmeticException e) {
                    String errMsg = e.getMessage() != null ? e.getMessage() : "arithmetic error";
                    if (errMsg.contains("/ by zero") || errMsg.contains("divide by zero") || errMsg.contains("Division by zero")) {
                        sendErrorSimple(ctx, "22012", "division by zero");
                    } else {
                        sendErrorSimple(ctx, "22003", errMsg);
                    }
                    batchFailed = true;
                } catch (Exception | StackOverflowError e) {
                    // Catch unexpected throwables (NPE, ClassCast, blown stack, etc.) during
                    // query execution or result sending (e.g. COPY TO stdout value formatting).
                    // Without this, the exception propagates to the outer catch which
                    // doesn't set batchFailed, or worse, to exceptionCaught() which
                    // previously killed the connection — causing pg_dump "worker died".
                    LOG.error("Unexpected error executing statement: {}", stmt, e);
                    MemgresException translated = PgErrors.translate(e);
                    sendErrorSimple(ctx, translated.getSqlState(), translated.getMessage());
                    batchFailed = true;
                }
            }
        } catch (Exception | StackOverflowError e) {
            LOG.error("Error executing query: {}", sql, e);
            MemgresException translated = PgErrors.translate(e);
            sendErrorSimple(ctx, translated.getSqlState(), translated.getMessage());
        }
        if (!copyHandler.inCopyFromMode) {
            // In autocommit mode, reset failed transaction state (PG auto-rolls back)
            if (session != null && session.isFailed() && !session.isExplicitTransactionBlock()) {
                session.rollback();
            }
            // A simple query takes the unnamed prepared statement and the unnamed portal with it.
            // They stand for whatever the client was last doing over the extended protocol, and a
            // Query is a new thing to be doing: PostgreSQL lets go of both, so a Bind that reaches
            // for the statement afterwards, or an Execute that reaches for the portal, is reaching
            // for something that is no longer there rather than running work from before.
            preparedStatements.remove("");
            portals.remove("");
            dropPortalsOutsideTransaction();
            sendReadyForQuery(ctx, session);
        }
    }

    // ---- Extended query protocol ----

    private void handleParse(ChannelHandlerContext ctx, PgWireMessage msg) {
        String stmtName = msg.getStatementName();
        String sql = msg.getQuery();
        int[] paramOids = msg.getParameterOids();

        if (Memgres.logAllStatements) {
            LOG.info("[PROTO] Parse stmt='{}' params={} sql={}", stmtName,
                paramOids != null ? paramOids.length : 0,
                sql != null ? sql.substring(0, Math.min(800, sql.length())).replace("\n", " ") : "(null)");
        }

        try {
            analyzeAtParse(stmtName, sql);
        } catch (MemgresException e) {
            enrichErrorPosition(e, sql);
            sendErrorWithDetails(ctx, e, true);
            extendedErrorReported(ctx);
            return;
        } catch (RuntimeException | StackOverflowError e) {
            // Reading a statement is not the place to invent a failure. Whatever the analyzer
            // could not make sense of is left to Execute, which is where it was reported before.
            LOG.debug("[PROTO] Parse-time analysis skipped: {}", e.toString());
        }

        preparedStatements.put(stmtName, new PreparedStmt(sql, paramOids));
        if (stmtName != null && !stmtName.isEmpty()) {
            stmtDescribed.remove(stmtName);
            // Bridge named protocol-level prepared statements to Session for pg_prepared_statements visibility
            bridgeProtocolPreparedToSession(stmtName, sql, paramOids);
        }

        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('1');
        buf.writeInt(4);
        ctx.write(buf);
    }

    /**
     * Everything PostgreSQL settles while it reads a statement, before it answers ParseComplete.
     *
     * <p>Analysis happens where the statement is parsed, so a client that misspells a keyword or
     * names a relation that is not there hears about it at Parse — before it has been told the
     * statement and its bindings were good. memgres left every such fault to Execute, which put
     * the answer one or two messages after the one that carried the mistake.
     *
     * <p>The order is PostgreSQL's own: the text has to read as a statement, and as one statement;
     * a block that has already failed then refuses everything but its own end; the names in the
     * statement are resolved after that; and the prepared statement is stored last, which is where
     * a name already in use is noticed.
     */
    private void analyzeAtParse(String stmtName, String sql) {
        if (sql == null) return;
        int commands = commandsIn(sql).length;
        // Nothing but comments and semicolons is the empty query, which PG parses and answers.
        if (commands == 0) return;
        if (commands > 1) {
            // Text that does not read as a statement is still the syntax error it is, so it is
            // parsed before its commands are counted.
            com.memgres.engine.parser.Parser.parseAll(sql);
            throw new MemgresException(
                    "cannot insert multiple commands into a prepared statement", "42601");
        }
        com.memgres.engine.parser.ast.Statement body = com.memgres.engine.parser.Parser.parse(sql);
        if (session != null && session.isTransactionAborted() && !endsAbortedBlock(sql)) {
            throw new MemgresException(
                    "current transaction is aborted, commands ignored until end of transaction block",
                    "25P02");
        }
        if (body != null && session != null) session.executor().analyzeWithoutRunning(body);
        // A named prepared statement is not replaced by a second Parse under the same name: the
        // client closes it first, and PostgreSQL refuses rather than take away a statement whose
        // portals are still running. The unnamed one is the one every Parse replaces.
        if (stmtName != null && !stmtName.isEmpty() && preparedStatements.containsKey(stmtName)) {
            throw new MemgresException(
                    "prepared statement \"" + stmtName + "\" already exists", "42P05");
        }
    }

    /**
     * The statements a piece of text holds. A segment carrying only a comment is not one of them:
     * treating it as a statement answered a query whose semicolon is followed by a comment with
     * the comment's empty result instead of the query's own rows.
     */
    private String[] commandsIn(String sql) {
        List<String> commands = new ArrayList<>();
        for (String part : splitStatements(sql)) {
            if (!PgWireDescribeHelper.stripLeadingComments(part).trim().isEmpty()) commands.add(part);
        }
        return commands.toArray(new String[0]);
    }

    /**
     * Whether the statement is one a failed transaction block still runs: the ones that end it.
     * PostgreSQL refuses everything else at the message that carried it — Parse, Bind and Execute
     * alike — rather than let a client believe work was queued behind an error.
     */
    private static boolean endsAbortedBlock(String sql) {
        if (sql == null) return false;
        String upper = sql.trim().toUpperCase();
        return upper.startsWith("COMMIT") || upper.startsWith("END")
                || upper.startsWith("ABORT") || upper.startsWith("ROLLBACK");
    }

    /**
     * Bridge a named protocol-level prepared statement to Session so it appears
     * in pg_prepared_statements with from_sql = false (matching real PG behavior).
     */
    private void bridgeProtocolPreparedToSession(String name, String sql, int[] paramOids) {
        try {
            // Convert parameter OIDs to type names
            java.util.List<String> paramTypes = new java.util.ArrayList<>();
            if (paramOids != null) {
                for (int oid : paramOids) {
                    if (oid == 0) continue; // unspecified type
                    com.memgres.engine.DataType dt = com.memgres.engine.DataType.fromOid(oid);
                    paramTypes.add(dt != null ? dt.toRegtypeDisplay() : "unknown");
                }
            }
            // Parse the SQL to get the AST body
            com.memgres.engine.parser.ast.Statement body = null;
            try {
                body = com.memgres.engine.parser.Parser.parse(sql);
            } catch (Exception ignored) {
                // Parse may fail for some protocol-level queries; store without body
            }
            // Remove existing if overwriting (PG allows Parse to silently overwrite)
            if (session.getPreparedStatement(name) != null) {
                session.removePreparedStatement(name);
            }
            int inferredCount = 0;
            if (paramOids != null) inferredCount = paramOids.length;
            // Infer result types via dry-run (LIMIT 0)
            java.util.List<String> resultTypes = inferProtocolResultTypes(sql, body);
            session.addPreparedStatement(name,
                    new com.memgres.engine.Session.PreparedStmt(name, paramTypes, body, inferredCount,
                            sql, java.time.OffsetDateTime.now(), false, resultTypes));
        } catch (Exception e) {
            // Don't let catalog bridging failures break protocol handling
            LOG.debug("[PROTO] Failed to bridge prepared statement '{}' to session: {}", name, e.getMessage());
        }
    }

    /**
     * Infer result column types for a protocol-level prepared statement.
     * Uses LIMIT 0 dry-run for SELECT only. For DML RETURNING, infers from table schema.
     * Returns null for non-query statements.
     */
    private java.util.List<String> inferProtocolResultTypes(String sql, com.memgres.engine.parser.ast.Statement body) {
        try {
            if (sql == null) return null;
            String upper = sql.trim().toUpperCase();
            boolean isSelect = upper.startsWith("SELECT") || upper.startsWith("WITH") || upper.startsWith("VALUES");
            if (isSelect) {
                // SELECT: safe to dry-run with LIMIT 0
                com.memgres.engine.Session.TransactionStatus saved = session.getStatus();
                try {
                    String drySql = sql.replaceAll("\\$\\d+", "NULL").replaceAll(";\\s*$", "").trim();
                    if (!drySql.toUpperCase().contains("LIMIT")) drySql = drySql + " LIMIT 0";
                    com.memgres.engine.QueryResult result = session.execute(drySql, new java.util.ArrayList<>());
                    if (result.getColumns() != null && !result.getColumns().isEmpty()) {
                        java.util.List<String> types = new java.util.ArrayList<>();
                        for (com.memgres.engine.Column col : result.getColumns()) {
                            types.add(col.getType().toRegtypeDisplay());
                        }
                        return types;
                    }
                } catch (Exception e) {
                    session.restoreStatus(saved);
                    LOG.debug("[PROTO] Failed to infer SELECT result types: {}", e.getMessage());
                }
            }
            // DML with RETURNING: infer from AST and table schema (no execution — avoids side effects)
            if (body != null) {
                return inferResultTypesFromAst(body);
            }
        } catch (Exception e) {
            LOG.debug("[PROTO] Failed to infer result types: {}", e.getMessage());
        }
        return null;
    }

    /** Infer result column types from AST by looking up the target table's schema. */
    private java.util.List<String> inferResultTypesFromAst(com.memgres.engine.parser.ast.Statement body) {
        try {
            java.util.List<com.memgres.engine.parser.ast.SelectStmt.SelectTarget> returning = null;
            String tableName = null;
            String schemaName = null;
            if (body instanceof com.memgres.engine.parser.ast.InsertStmt) {
                com.memgres.engine.parser.ast.InsertStmt ins = (com.memgres.engine.parser.ast.InsertStmt) body;
                returning = ins.returning;
                tableName = ins.table;
                schemaName = ins.schema;
            } else if (body instanceof com.memgres.engine.parser.ast.UpdateStmt) {
                com.memgres.engine.parser.ast.UpdateStmt upd = (com.memgres.engine.parser.ast.UpdateStmt) body;
                returning = upd.returning;
                tableName = upd.table;
                schemaName = upd.schema;
            } else if (body instanceof com.memgres.engine.parser.ast.DeleteStmt) {
                com.memgres.engine.parser.ast.DeleteStmt del = (com.memgres.engine.parser.ast.DeleteStmt) body;
                returning = del.returning;
                tableName = del.table;
                schemaName = del.schema;
            }
            if (returning == null || returning.isEmpty()) return null;
            // Resolve the table to get column types
            if (schemaName == null) schemaName = "public";
            com.memgres.engine.Table table = null;
            for (com.memgres.engine.Schema s : database.getSchemas().values()) {
                com.memgres.engine.Table t = s.getTable(tableName);
                if (t != null) { table = t; break; }
            }
            if (table == null) return null;
            // Map RETURNING targets to column types
            return mapReturningToTypes(returning, table);
        } catch (Exception e) {
            return null;
        }
    }

    private java.util.List<String> mapReturningToTypes(
            java.util.List<com.memgres.engine.parser.ast.SelectStmt.SelectTarget> returning,
            com.memgres.engine.Table table) {
        java.util.List<String> types = new java.util.ArrayList<>();
        for (com.memgres.engine.parser.ast.SelectStmt.SelectTarget target : returning) {
            com.memgres.engine.parser.ast.Expression expr = target.expr();
            if (expr instanceof com.memgres.engine.parser.ast.WildcardExpr) {
                // RETURNING * — add all columns
                for (com.memgres.engine.Column col : table.getColumns()) {
                    types.add(col.getType().toRegtypeDisplay());
                }
            } else if (expr instanceof com.memgres.engine.parser.ast.ColumnRef) {
                String colName = ((com.memgres.engine.parser.ast.ColumnRef) expr).column();
                int colIdx = table.getColumnIndex(colName);
                types.add(colIdx >= 0 ? table.getColumns().get(colIdx).getType().toRegtypeDisplay() : "text");
            } else {
                // Expression — default to text
                types.add("text");
            }
        }
        return types;
    }

    /**
     * Whether a statement of this shape carries its parameters in the Bind message. PREPARE,
     * CREATE, EXECUTE and DO all write $N inside a body of their own, where it is not a
     * placeholder the protocol fills in, so what the text holds says nothing about the message.
     */
    private static boolean carriesBindParameters(String sql) {
        if (sql == null) return false;
        String trimmed = sql.replaceAll("^\\s+", "").toUpperCase();
        return trimmed.startsWith("SELECT") || trimmed.startsWith("INSERT")
                || trimmed.startsWith("UPDATE") || trimmed.startsWith("DELETE")
                || trimmed.startsWith("EXPLAIN") || trimmed.startsWith("WITH")
                || trimmed.startsWith("VALUES") || trimmed.startsWith("TABLE");
    }

    /**
     * How many parameters the statement was prepared for, or -1 when the text cannot say. A type
     * declared at Parse counts even where the text does not use it, because that is the count
     * PostgreSQL holds the Bind message against.
     */
    private static int requiredParameterCount(PreparedStmt prepared) {
        int declared = prepared.paramOids() != null ? prepared.paramOids().length : 0;
        if (declared == 0 && !carriesBindParameters(prepared.sql())) return -1;
        return Math.max(declared, maxParamPlaceholder(prepared.sql()));
    }

    /** Scan SQL for $N parameter placeholders, return the highest N (0 if none).
     *  Only counts in DML/EXPLAIN statements. Skips PREPARE, CREATE, ALTER, DROP, etc.
     *  Skips single-quoted strings, dollar-quoted strings, and SQL comments. */
    private static int maxParamPlaceholder(String sql) {
        if (!carriesBindParameters(sql)) return 0;

        int max = 0;
        int len = sql.length();
        for (int i = 0; i < len; i++) {
            char c = sql.charAt(i);
            // Skip single-quoted strings
            if (c == '\'') {
                i++;
                while (i < len) {
                    if (sql.charAt(i) == '\'') {
                        if (i + 1 < len && sql.charAt(i + 1) == '\'') { i += 2; continue; }
                        break;
                    }
                    i++;
                }
                continue;
            }
            // Skip -- line comments
            if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
                i = sql.indexOf('\n', i);
                if (i < 0) break;
                continue;
            }
            // Skip /* block comments */
            if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
                i = sql.indexOf("*/", i + 2);
                if (i < 0) break;
                i++;
                continue;
            }
            // Skip dollar-quoted strings ($$ or $tag$)
            if (c == '$') {
                int tagEnd = i + 1;
                while (tagEnd < len && (Character.isLetterOrDigit(sql.charAt(tagEnd)) || sql.charAt(tagEnd) == '_')) {
                    tagEnd++;
                }
                if (tagEnd < len && sql.charAt(tagEnd) == '$') {
                    String tag = sql.substring(i, tagEnd + 1);
                    int closePos = sql.indexOf(tag, tagEnd + 1);
                    if (closePos >= 0) {
                        i = closePos + tag.length() - 1;
                        continue;
                    }
                }
                // $N parameter placeholder. A number too large to be one is left to the parser,
                // which names it; reading it here without a guard threw before the parser was
                // ever reached and the escape reported an internal error.
                if (i + 1 < len && Character.isDigit(sql.charAt(i + 1))) {
                    int j = i + 1;
                    while (j < len && Character.isDigit(sql.charAt(j))) j++;
                    try {
                        int n = Integer.parseInt(sql.substring(i + 1, j));
                        if (n > max) max = n;
                    } catch (NumberFormatException ignored) {
                        // not a parameter number this protocol can carry
                    }
                    i = j - 1;
                }
            }
        }
        return max;
    }

    private void handleBind(ChannelHandlerContext ctx, PgWireMessage msg) {
        String portalName = msg.getPortalName() != null ? msg.getPortalName() : "";
        String stmtName = msg.getStatementName() != null ? msg.getStatementName() : "";

        PreparedStmt prepared = preparedStatements.get(stmtName);
        if (prepared == null) {
            sendExtendedError(ctx, "26000", noSuchPreparedStatement(stmtName));
            return;
        }

        // Validate bind parameter count matches $N placeholders in the SQL
        int suppliedParams = msg.getParameterValues() != null ? msg.getParameterValues().length : 0;
        short[] formatCodes = msg.getParameterFormatCodes();
        // A format code list either says nothing, says one thing for all the parameters, or says
        // one thing per parameter. Any other length is a message that cannot be read as it stands.
        if (formatCodes != null && formatCodes.length > 1 && formatCodes.length != suppliedParams) {
            sendExtendedError(ctx, "08P01", "bind message has " + formatCodes.length
                    + " parameter formats but " + suppliedParams + " parameters");
            return;
        }
        // The statement was prepared for a number of parameters, and Bind supplies exactly that
        // many. Too many is as much a mismatch as too few: the extra value is bound to nothing,
        // and the statement that would run is not the one the client thinks it sent.
        int requiredParams = requiredParameterCount(prepared);
        if (requiredParams >= 0 && suppliedParams != requiredParams) {
            sendExtendedError(ctx, "08P01",
                    "bind message supplies " + suppliedParams + " parameters, but prepared statement \"" +
                    stmtName + "\" requires " + requiredParams);
            return;
        }
        if (session != null && session.isTransactionAborted() && !endsAbortedBlock(prepared.sql())) {
            sendExtendedError(ctx, "25P02",
                    "current transaction is aborted, commands ignored until end of transaction block");
            return;
        }

        // A portal belongs to the transaction that made it and lives until that transaction ends,
        // so while a block is open the name is still taken: PostgreSQL refuses a second Bind to it
        // rather than take away a portal the client may still be reading from. The unnamed portal
        // is the one every Bind replaces. The portal is made before the values are read, which is
        // why a name already in use is answered for ahead of a value that will not read.
        if (!portalName.isEmpty() && portals.containsKey(portalName)) {
            sendExtendedError(ctx, "42P03", "cursor \"" + portalName + "\" already exists");
            return;
        }

        List<Object> paramValues = new ArrayList<>();
        byte[][] rawValues = msg.getParameterValues();
        if (rawValues != null) {
            int[] resolvedOids = null;
            for (int i = 0; i < rawValues.length; i++) {
                short format = 0;
                if (formatCodes != null && formatCodes.length > 0) {
                    format = formatCodes.length == 1 ? formatCodes[0] : formatCodes[i];
                }
                // Text and binary are the two shapes a parameter arrives in, and PostgreSQL
                // refuses the message rather than guess at what a third would have meant.
                if (format != 0 && format != 1) {
                    sendExtendedError(ctx, "22023", "unsupported format code: " + format);
                    return;
                }
                if (rawValues[i] == null) {
                    paramValues.add(null);
                    continue;
                }
                int paramOid = (prepared.paramOids() != null && i < prepared.paramOids().length)
                        ? prepared.paramOids()[i] : 0;
                if (format == 0) {
                    // A value is read as the type the parameter resolved to, which is the type the
                    // client declared or, where it declared none, the one the statement itself
                    // says the parameter has to be.
                    int resolved = paramOid;
                    if (resolved == 0) {
                        if (resolvedOids == null) {
                            resolvedOids = PgWireParamTypes.infer(prepared.sql(), rawValues.length,
                                    database, session);
                        }
                        if (resolvedOids != null && i < resolvedOids.length) {
                            resolved = resolvedOids[i];
                        }
                    }
                    String text = new String(rawValues[i], StandardCharsets.UTF_8);
                    MemgresException unreadable =
                            PgWireParamValues.unreadable(session, text, resolved);
                    if (unreadable != null) {
                        sendErrorWithDetails(ctx, unreadable, true);
                        extendedErrorReported(ctx);
                        return;
                    }
                    paramValues.add(text);
                } else {
                    MemgresException wrongLength =
                            PgWireParamValues.wrongBinaryLength(rawValues[i], paramOid, i + 1);
                    if (wrongLength != null) {
                        sendExtendedError(ctx, wrongLength.getSqlState(), wrongLength.getMessage());
                        return;
                    }
                    paramValues.add(PgWireBinaryCodec.decodeBinaryParam(rawValues[i], paramOid));
                }
            }
        }

        // The result formats are applied to the portal's row description, so a list naming more of
        // them than the statement has columns is a message that cannot be carried out. A list of
        // one stands for every column however many there are, and a statement that answers with no
        // rows has no description to apply them to, so neither is held against a count.
        short[] resultFormats = msg.getResultFormatCodes();
        if (resultFormats != null && resultFormats.length > 1) {
            int columns = resultColumnCount(prepared.sql());
            if (columns >= 0 && resultFormats.length != columns) {
                sendExtendedError(ctx, "08P01", "bind message has " + resultFormats.length
                        + " result formats but query has " + columns + " columns");
                return;
            }
        }

        Portal portal = new Portal(prepared.sql(), paramValues, msg.getResultFormatCodes());
        portal.rowDescriptionSent = rowDescSentByDescribe
                || stmtDescribed.getOrDefault(stmtName, false);
        portal.stmtName = stmtName;
        portals.put(portalName, portal);

        if (Memgres.logAllStatements) LOG.info("[PROTO] Bind portal='{}' stmt='{}' params={} rowDescAlready={}",
                portalName, stmtName, paramValues.size(), portal.rowDescriptionSent);

        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('2');
        buf.writeInt(4);
        ctx.write(buf);
    }

    /**
     * How PostgreSQL names a prepared statement that is not there. The unnamed one has no name to
     * write down, so it is described rather than quoted: a client told that a statement called ""
     * was missing went looking for one it had never sent.
     */
    private static String noSuchPreparedStatement(String name) {
        return name == null || name.isEmpty()
                ? "unnamed prepared statement does not exist"
                : "prepared statement \"" + name + "\" does not exist";
    }

    /**
     * How many columns the statement answers with, or -1 when reading it does not settle that.
     * Reading is all this may do: Bind runs nothing, and a statement is not free to run.
     */
    private int resultColumnCount(String sql) {
        if (sql == null || session == null) return -1;
        try {
            com.memgres.engine.parser.ast.Statement body =
                    com.memgres.engine.parser.Parser.parse(sql);
            if (body == null) return -1;
            return session.executor().resultColumnsWithoutRunning(body);
        } catch (RuntimeException | StackOverflowError e) {
            // A statement this cannot read is one Parse has already answered for; the message
            // stands as it was sent.
            return -1;
        }
    }

    private void handleDescribe(ChannelHandlerContext ctx, PgWireMessage msg) {
        byte descType = msg.getDescribeType();
        String name = msg.getStatementName() != null ? msg.getStatementName() : "";
        if (Memgres.logAllStatements) LOG.info("[PROTO] Describe {} name='{}'", descType == 'S' ? "Statement" : "Portal", name);

        if (descType == 'S') {
            PreparedStmt prepared = preparedStatements.get(name);
            if (prepared == null) {
                sendExtendedError(ctx, "26000", noSuchPreparedStatement(name));
                return;
            }
            try {
                boolean sent = describeHelper.describeStatement(ctx, name, prepared.sql(), prepared.paramOids());
                if (sent) markStatementDescribed(name);
            } catch (PgWireDescribeHelper.DescribeExecutionFailedException dfe) {
                sendExtendedError(ctx, dfe.sqlState, dfe.getMessage());
            }
        } else {
            Portal portal = portals.get(name);
            if (portal != null) portal.describeAttempted = true;
            if (portal == null) {
                LOG.warn("[PROTO] Describe Portal: portal '{}' does not exist!", name);
                sendExtendedError(ctx, "34000", "portal \"" + name + "\" does not exist");
                return;
            }
            // Set session state to 'active' during Describe — portal description may
            // execute the query to infer columns, and pg_stat_activity should show 'active'.
            if (session != null) session.setQueryState(portal.sql());
            PgWireDescribeHelper.DescribePortalResult result;
            // H7: Register executing thread so Statement.cancel() can interrupt Describe execution
            cancelRegistry.setExecutingThread(backendPid, backendSecretKey, Thread.currentThread());
            try {
                result = describeHelper.describePortal(ctx, portal.sql(), portal.paramValues());
            } catch (PgWireDescribeHelper.DescribeExecutionFailedException dfe) {
                sendExtendedError(ctx, dfe.sqlState, dfe.getMessage());
                return;
            } finally {
                cancelRegistry.setExecutingThread(backendPid, backendSecretKey, null);
                if (session != null) session.setIdleState();
            }
            if (result.rowDescSent()) {
                rowDescSentByDescribe = true;
                portal.rowDescriptionSent = true;
            }
            // A Describe that had to run the statement to learn its shape has already applied it,
            // so Execute has to report that run rather than start another. Keeping the result only
            // when a row description went with it left the statement to run a second time whenever
            // it turned out to have no columns.
            if (result.cachedResult() != null) {
                portal.describeResult = result.cachedResult();
            }
            if (result.failure() != null) {
                portal.describeFailure = result.failure();
            }
        }
    }

    private void markStatementDescribed(String name) {
        rowDescSentByDescribe = true;
        if (name != null && !name.isEmpty()) stmtDescribed.put(name, true);
    }

    /**
     * The function-call sub-protocol, which is how a JDBC client reaches the large-object
     * functions. Treated as a message type nothing understood, it was skipped in silence and the
     * client waited for a reply that was never going to come.
     */
    private void handleFunctionCall(ChannelHandlerContext ctx, PgWireMessage msg) {
        int oid = msg.getFunctionOid();
        try {
            Object[] proc = lookupProcByOid(oid);
            if (proc == null) {
                sendErrorSimple(ctx, "42883", "function with OID " + oid + " does not exist");
                sendReadyForQuery(ctx, session);
                return;
            }
            if (msg.getParameterValues() == null) {
                sendErrorSimple(ctx, "08P01", msg.getQuery());
                sendReadyForQuery(ctx, session);
                return;
            }
            byte[][] rawArgs = msg.getParameterValues();
            String name = (String) proc[0];
            int[] argTypes = (int[]) proc[1];
            int resultType = (Integer) proc[2];
            if (rawArgs.length != argTypes.length) {
                sendErrorSimple(ctx, "08P01", "function call message contains " + rawArgs.length
                        + " arguments but function requires " + argTypes.length);
                sendReadyForQuery(ctx, session);
                return;
            }

            List<Object> args = new ArrayList<>();
            short[] formats = msg.getParameterFormatCodes();
            StringBuilder call = new StringBuilder("SELECT \"")
                    .append(name.replace("\"", "\"\"")).append("\"(");
            for (int i = 0; i < rawArgs.length; i++) {
                if (i > 0) call.append(", ");
                // Each argument is read as the type the function declares for it, as PG reads
                // them: an argument sent as text is a text until something says otherwise.
                DataType declared = DataType.fromOid(argTypes[i]);
                call.append("$").append(i + 1);
                if (declared != null) call.append("::").append(declared.getPgName());
                if (rawArgs[i] == null) {
                    args.add(null);
                    continue;
                }
                short format = 0;
                if (formats != null && formats.length > 0) {
                    format = formats.length == 1 ? formats[0] : formats[i];
                }
                args.add(format == 0
                        ? new String(rawArgs[i], StandardCharsets.UTF_8)
                        : PgWireBinaryCodec.decodeBinaryParam(rawArgs[i], argTypes[i]));
            }
            call.append(")");

            QueryResult result = session.execute(call.toString(), args);
            Object value = result.getRows() != null && !result.getRows().isEmpty()
                    ? result.getRows().get(0)[0] : null;
            // What goes on the wire is the type pg_proc declares, not the one the value happens
            // to be carried as: lo_creat is declared an oid and answered eight bytes, and a
            // driver reading four of them found a large object it could not open.
            sendFunctionCallResponse(ctx, value, DataType.fromOid(resultType),
                    msg.getResultFormat());
        } catch (MemgresException e) {
            sendErrorSimple(ctx, e.getSqlState() != null ? e.getSqlState() : "XX000", e.getMessage());
        } catch (Exception e) {
            sendErrorSimple(ctx, "XX000", String.valueOf(e.getMessage()));
        }
        sendReadyForQuery(ctx, session);
    }

    /** The name and argument types of the pg_proc row an OID names, or null when it names none. */
    private Object[] lookupProcByOid(int oid) {
        QueryResult found = session.execute(
                "SELECT proname, proargtypes::text, prorettype::int FROM pg_catalog.pg_proc "
                        + "WHERE oid = " + oid, new ArrayList<>());
        if (found.getRows() == null || found.getRows().isEmpty()) return null;
        Object[] row = found.getRows().get(0);
        String written = row[1] == null ? "" : row[1].toString().trim();
        String[] parts = written.isEmpty() ? new String[0] : written.split("\\s+");
        int[] argTypes = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            try {
                argTypes[i] = Integer.parseInt(parts[i]);
            } catch (NumberFormatException ignored) {
                argTypes[i] = 0;
            }
        }
        int resultType = row[2] instanceof Number ? ((Number) row[2]).intValue() : 0;
        return new Object[]{String.valueOf(row[0]), argTypes, Integer.valueOf(resultType)};
    }

    private void sendFunctionCallResponse(ChannelHandlerContext ctx, Object value, DataType type,
                                          short resultFormat) {
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('V');
        int lengthIdx = buf.writerIndex();
        buf.writeInt(0);
        if (value == null) {
            buf.writeInt(-1);
        } else if (resultFormat == 1) {
            PgWireBinaryCodec.writeBinaryValue(buf, value, type);
        } else {
            GucSettings guc = session != null ? session.getGucSettings() : null;
            byte[] bytes = PgWireValueFormatter.formatValue(value, guc)
                    .getBytes(StandardCharsets.UTF_8);
            buf.writeInt(bytes.length);
            buf.writeBytes(bytes);
        }
        buf.setInt(lengthIdx, buf.writerIndex() - lengthIdx);
        ctx.write(buf);
    }

    private void handleExecute(ChannelHandlerContext ctx, PgWireMessage msg) {
        String portalName = msg.getPortalName() != null ? msg.getPortalName() : "";
        int maxRows = msg.getMaxRows();

        // Only a portal a Bind made is executed. The prepared statement a portal was built from is
        // not a portal: it carries no parameter values and no result formats, and PostgreSQL runs
        // nothing from it, so reaching for the unnamed statement when the named portal was missing
        // ran work the client had not asked for — the statement a later Parse had put there, or
        // one whose portal the transaction that made it had already taken away.
        Portal portal = portals.get(portalName);

        // PostgreSQL holds no portal of that name and says so, rather than answer as though the
        // client had sent an empty query: an Execute naming a portal that was never bound — or one
        // the transaction that made it took away with it — is a mistake worth reporting.
        if (portal == null) {
            sendExtendedError(ctx, "34000", "portal \"" + portalName + "\" does not exist");
            return;
        }

        if (portal.sql() == null || portal.sql().trim().isEmpty()) {
            if (Memgres.logAllStatements) LOG.info("[PROTO] Execute → EmptyQueryResponse (no portal/sql)");
            ByteBuf buf = ctx.alloc().buffer();
            buf.writeByte('I');
            buf.writeInt(4);
            ctx.write(buf);
            return;
        }

        // A portal that has run to its end is finished. PostgreSQL answers one that returned rows
        // with an empty result of the same kind, and refuses one that did not — it does not run
        // the statement again, which had been writing a second row and creating a second table.
        if (portal.done) {
            if (portal.rowReturning) {
                sendCommandCompleteWithNotices(ctx, exhaustedPortalTag(portal.completedType));
            } else {
                sendExtendedError(ctx, "55000", "portal \"" + portalName + "\" cannot be run");
            }
            return;
        }

        String sqlSnip = portal.sql().substring(0, Math.min(70, portal.sql().length())).replace("\n", " ");

        try {
            // Track custom_plans for protocol-level prepared statement executions (PG 14+).
            // Increment before execution to match PG behavior (counts even on failure).
            if (portal.stmtName != null && !portal.stmtName.isEmpty() && session != null) {
                Session.PreparedStmt ps = session.getPreparedStatement(portal.stmtName);
                if (ps != null) ps.recordExecution();
            }

            QueryResult result;
            String source;

            // Set session state to 'active' during query execution (matches PG behavior
            // for pg_stat_activity). This mirrors what the Simple Query path does.
            if (session != null) session.setQueryState(portal.sql());

            if (portal.suspendedResult != null) {
                result = portal.suspendedResult;
                source = "suspended";
            } else if (portal.describeResult != null) {
                result = portal.describeResult;
                portal.describeResult = null;
                source = "cached";
            } else if (portal.describeFailure != null) {
                // The statement has already run, and that run refused it. PostgreSQL runs it once
                // and reports the refusal here, so this is where it is reported: running it over
                // asked a question whose circumstances had moved on, and a statement that had lost
                // a race with another session won the re-run and answered as though it had never
                // been refused at all.
                RuntimeException refused = portal.describeFailure;
                portal.describeFailure = null;
                // A refused statement leaves a block fit for nothing but its own end. That was
                // undone after the describing run so the statement's shape could still be worked
                // out, and it takes effect here, where the refusal is reported.
                if (session != null
                        && session.getStatus() == Session.TransactionStatus.IN_TRANSACTION) {
                    session.restoreStatus(Session.TransactionStatus.FAILED);
                }
                throw refused;
            } else {
                source = "fresh";
                String[] stmts = commandsIn(portal.sql());
                if (stmts.length > 1) {
                    for (int si = 0; si < stmts.length - 1; si++) {
                        String s = stmts[si].trim();
                        if (!s.isEmpty()) {
                            try {
                                executeWithCancel(s, portal.paramValues());
                            } catch (MemgresException e) {
                                enrichErrorPosition(e, s);
                                sendErrorWithDetails(ctx, e, true);
                                extendedErrorReported(ctx);
                                return;
                            }
                        }
                    }
                    String lastStmt = stmts[stmts.length - 1].trim();
                    result = lastStmt.isEmpty()
                            ? QueryResult.message(QueryResult.Type.EMPTY, "")
                            : executeWithCancel(lastStmt, portal.paramValues());
                } else {
                    result = executeWithCancel(portal.sql(), portal.paramValues());
                }
            }

            int rowCount = result.getRows() != null ? result.getRows().size() : 0;
            if (Memgres.logAllStatements) LOG.info("[PROTO] Execute → {} type={} rows={} rowDescSent={} {}",
                    source, result.getType(), rowCount, portal.rowDescriptionSent, sqlSnip);

            rowDescSentByDescribe = false;

            // Any portal that returns rows can be fetched from, which includes INSERT, UPDATE,
            // DELETE and MERGE with RETURNING. COPY is its own protocol and is not one of them.
            boolean rowReturning = result.getType() != QueryResult.Type.COPY_OUT
                    && result.getType() != QueryResult.Type.COPY_IN
                    && result.getRows() != null && result.getColumns() != null
                    && !result.getColumns().isEmpty();

            // Text and binary are the two shapes a column can be written in. PostgreSQL refuses
            // the Execute before it writes a row rather than send one in a format the client asked
            // for and it does not have.
            if (rowReturning) {
                int badFormat = unsupportedResultFormat(portal.resultFormatCodes(),
                        result.getColumns().size());
                if (badFormat >= 0) {
                    sendExtendedError(ctx, "22023", "unsupported format code: " + badFormat);
                    return;
                }
            }

            // Handle maxRows (cursor-based fetching with portal suspend/resume)
            if (maxRows > 0 && rowReturning) {
                List<Object[]> allRows = result.getRows();
                int offset = portal.suspendedOffset;
                int end = Math.min(offset + maxRows, allRows.size());
                for (int i = offset; i < end; i++) {
                    sendDataRow(ctx, allRows.get(i), result.getColumns(), portal.resultFormatCodes());
                }
                // PostgreSQL suspends whenever the limit was reached, even when reaching it took
                // the last row: whether anything follows is the next Execute's answer to give.
                if (end - offset == maxRows) {
                    portal.suspendedResult = result;
                    portal.suspendedOffset = end;
                    sendPortalSuspended(ctx);
                } else {
                    portal.suspendedResult = null;
                    portal.suspendedOffset = 0;
                    portal.done = true;
                    portal.rowReturning = true;
                    portal.completedType = result.getType();
                    sendCommandCompleteWithNotices(ctx, commandTag(result, end - offset));
                }
            } else {
                // CALL with OUT params: PG sends RowDescription during Execute (not Describe)
                String upperSql = PgWireDescribeHelper.stripLeadingComments(portal.sql()).toUpperCase();
                if (upperSql.startsWith("CALL") && result.getType() == QueryResult.Type.SELECT
                        && !result.getColumns().isEmpty() && !portal.rowDescriptionSent) {
                    sendRowDescription(ctx, result);
                    for (Object[] row : result.getRows()) sendDataRow(ctx, row, result.getColumns(), portal.resultFormatCodes());
                    sendCommandCompleteWithNotices(ctx, "CALL");
                } else if (!portal.rowDescriptionSent && portal.describeAttempted
                        && result.getType() == QueryResult.Type.SELECT
                        && !result.getColumns().isEmpty() && !result.getRows().isEmpty()) {
                    // Describe was attempted but sent NoData (speculative execution failed), then
                    // Execute succeeded. Sending DataRow without prior RowDescription violates
                    // protocol. Just send CommandComplete — side effects (e.g. lock) still happened.
                    sendCommandCompleteWithNotices(ctx, commandTag(result));
                } else {
                    sendResultDataOnly(ctx, result, portal.resultFormatCodes());
                }
                portal.done = true;
                portal.rowReturning = rowReturning;
                portal.completedType = result.getType();
            }
            // Emit ParameterStatus updates for tracked GUC parameters after SET
            if (result.getType() == QueryResult.Type.SET) {
                emitParameterStatusUpdates(ctx, portal.sql());
            }
        } catch (MemgresException e) {
            LOG.warn("[PROTO] Execute ERROR {}: {} | {}", e.getSqlState(), e.getMessage(), sqlSnip);
            enrichErrorPosition(e, portal.sql());
            sendErrorWithDetails(ctx, e, true);
            extendedErrorReported(ctx);
        } catch (ArithmeticException e) {
            String errMsg = e.getMessage() != null ? e.getMessage() : "arithmetic error";
            LOG.warn("[PROTO] Execute ARITH ERROR: {} | {}", errMsg, sqlSnip);
            if (errMsg.contains("/ by zero") || errMsg.contains("divide by zero") || errMsg.contains("Division by zero")) {
                sendExtendedError(ctx, "22012", "division by zero");
            } else {
                sendExtendedError(ctx, "22003", errMsg);
            }
        } catch (Exception | StackOverflowError e) {
            LOG.error("[PROTO] Execute INTERNAL ERROR: {} | {}", e.getMessage(), sqlSnip, e);
            MemgresException translated = PgErrors.translate(e);
            enrichErrorPosition(translated, portal.sql());
            sendErrorWithDetails(ctx, translated, true);
            extendedErrorReported(ctx);
        } finally {
            if (session != null) session.setIdleState();
        }
    }

    private void handleSync(ChannelHandlerContext ctx) {
        rowDescSentByDescribe = false;
        errorPendingUntilSync = false;
        // In autocommit mode, reset failed transaction state (PG auto-rolls back)
        if (session != null && !session.isExplicitTransactionBlock()) {
            if (session.isFailed()) {
                session.rollback();
            } else {
                // PG releases xact-level advisory locks at statement end in autocommit
                database.releaseXactAdvisoryLocks(session);
            }
        }
        dropPortalsOutsideTransaction();
        sendReadyForQuery(ctx, session);
    }

    /**
     * A portal belongs to the transaction that made it, and PostgreSQL drops every one of them
     * when that transaction ends: the name is free again afterwards, and a portal cannot be run a
     * second time from outside the block that bound it. In autocommit each statement is its own
     * transaction, which is why an unfinished portal does not outlive the ReadyForQuery.
     */
    private void dropPortalsOutsideTransaction() {
        if (!portals.isEmpty() && (session == null || !session.isInTransaction())) {
            portals.clear();
        }
    }

    /** The first result format code that is neither text nor binary, or -1 when they all are. */
    private static int unsupportedResultFormat(short[] codes, int columns) {
        if (codes == null || codes.length == 0) return -1;
        for (int i = 0; i < columns; i++) {
            short code = codes.length == 1 ? codes[0] : (i < codes.length ? codes[i] : 0);
            if (code != 0 && code != 1) return code;
        }
        return -1;
    }

    private void handleFlush(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    private void handleClose(ChannelHandlerContext ctx, PgWireMessage msg) {
        byte closeType = msg.getCloseType();
        String name = msg.getStatementName() != null ? msg.getStatementName() : "";
        if (Memgres.logAllStatements) LOG.info("[PROTO] Close {} name='{}'", closeType == 'S' ? "Statement" : "Portal", name);

        if (closeType == 'S') {
            preparedStatements.remove(name);
            stmtDescribed.remove(name);
            // Also remove from Session (protocol-level bridge)
            if (!name.isEmpty() && session.getPreparedStatement(name) != null) {
                session.removePreparedStatement(name);
            }
        } else {
            portals.remove(name);
        }

        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('3');
        buf.writeInt(4);
        ctx.write(buf);
    }

    // ---- Result sending (DRY: unified command tag) ----

    /** Get the PG command tag for a QueryResult type. */
    private static String commandTag(QueryResult result) {
        switch (result.getType()) {
            case SELECT:
                return "SELECT " + result.getRows().size();
            case INSERT:
                return "INSERT 0 " + result.getAffectedRows();
            case UPDATE:
                return "UPDATE " + result.getAffectedRows();
            case DELETE:
                return "DELETE " + result.getAffectedRows();
            case MERGE:
                return "MERGE " + result.getAffectedRows();
            case SELECT_INTO:
                return "SELECT " + result.getAffectedRows();
            case CREATE_TABLE:
                return "CREATE TABLE";
            case DROP_TABLE:
                return "DROP TABLE";
            case CREATE_TYPE:
                return "CREATE TYPE";
            case ALTER_TYPE:
                return "ALTER TYPE";
            case CREATE_FUNCTION:
                return "CREATE FUNCTION";
            case CREATE_TRIGGER:
                return "CREATE TRIGGER";
            case CALL:
                return "CALL";
            case SET:
                return result.getMessage() != null ? result.getMessage() : "SET";
            case BEGIN:
                // BEGIN and START TRANSACTION open the same block, and the tag names the one the
                // client wrote.
                return result.getMessage() != null ? result.getMessage() : "BEGIN";
            case COMMIT:
                return "COMMIT";
            case ROLLBACK:
                return "ROLLBACK";
            case COPY_OUT:
            case COPY_IN:
            case EMPTY:
                return null;
            default:
                throw new IllegalStateException("Unknown result type: " + result.getType());
        }
    }

    /**
     * The tag for one Execute of a portal that returns rows. PostgreSQL counts the rows that
     * Execute delivered — a resumed portal reports its own rows, not the whole result — while a
     * portal that returns none keeps the statement's affected-row count.
     */
    private static String commandTag(QueryResult result, int rowsThisExecute) {
        switch (result.getType()) {
            case SELECT:
            case SELECT_INTO:
                return "SELECT " + rowsThisExecute;
            case INSERT:
                return "INSERT 0 " + rowsThisExecute;
            case UPDATE:
                return "UPDATE " + rowsThisExecute;
            case DELETE:
                return "DELETE " + rowsThisExecute;
            case MERGE:
                return "MERGE " + rowsThisExecute;
            default:
                return commandTag(result);
        }
    }

    /** What PostgreSQL answers an Execute on a portal that has already delivered everything. */
    private static String exhaustedPortalTag(QueryResult.Type type) {
        if (type == null) return "SELECT 0";
        switch (type) {
            case INSERT:
                return "INSERT 0 0";
            case UPDATE:
                return "UPDATE 0";
            case DELETE:
                return "DELETE 0";
            case MERGE:
                return "MERGE 0";
            default:
                return "SELECT 0";
        }
    }

    /** Send a full query result (simple query protocol): RowDescription + DataRows + CommandComplete. */
    private void sendQueryResult(ChannelHandlerContext ctx, QueryResult result) {
        switch (result.getType()) {
            case SELECT: {
                sendRowDescription(ctx, result);
                for (Object[] row : result.getRows()) sendDataRow(ctx, row, null, null);
                sendCommandCompleteWithNotices(ctx, commandTag(result));
                break;
            }
            case INSERT:
            case UPDATE:
            case DELETE:
            case MERGE: {
                if (!result.getColumns().isEmpty()) {
                    sendRowDescription(ctx, result);
                    for (Object[] row : result.getRows()) sendDataRow(ctx, row, null, null);
                }
                sendCommandCompleteWithNotices(ctx, commandTag(result));
                break;
            }
            case COPY_OUT:
                copyHandler.sendCopyOutResult(ctx, result);
                break;
            case COPY_IN:
                copyHandler.sendCopyInResult(ctx, result, false);
                break;
            case EMPTY: {
                ByteBuf buf = ctx.alloc().buffer();
                buf.writeByte('I');
                buf.writeInt(4);
                ctx.write(buf);
                break;
            }
            default:
                sendCommandCompleteWithNotices(ctx, commandTag(result));
                break;
        }
    }

    /**
     * Send Execute result (extended protocol). NEVER sends RowDescription.
     * RowDescription is only sent by Describe; Execute sends DataRow* + CommandComplete.
     */
    private void sendResultDataOnly(ChannelHandlerContext ctx, QueryResult result, short[] resultFormatCodes) {
        switch (result.getType()) {
            case SELECT: {
                for (Object[] row : result.getRows()) sendDataRow(ctx, row, result.getColumns(), resultFormatCodes);
                sendCommandCompleteWithNotices(ctx, commandTag(result));
                break;
            }
            case INSERT:
            case UPDATE:
            case DELETE:
            case MERGE: {
                if (!result.getColumns().isEmpty()) {
                    for (Object[] row : result.getRows()) sendDataRow(ctx, row, result.getColumns(), resultFormatCodes);
                }
                sendCommandCompleteWithNotices(ctx, commandTag(result));
                break;
            }
            case COPY_OUT:
                copyHandler.sendCopyOutResult(ctx, result);
                break;
            case COPY_IN:
                copyHandler.sendCopyInResult(ctx, result, true);
                break;
            case EMPTY: {
                ByteBuf buf = ctx.alloc().buffer();
                buf.writeByte('I');
                buf.writeInt(4);
                ctx.write(buf);
                break;
            }
            default:
                sendCommandCompleteWithNotices(ctx, commandTag(result));
                break;
        }
    }

    // ---- Wire protocol message helpers ----

    private void sendRowDescription(ChannelHandlerContext ctx, QueryResult result) {
        ByteBuf buf = ctx.alloc().buffer();
        PgWireValueFormatter.sendRowDescription(buf, result.getColumns(), session);
        ctx.write(buf);
    }

    private void sendDataRow(ChannelHandlerContext ctx, Object[] row,
                              List<Column> columns, short[] resultFormatCodes) {
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('D');
        int lengthIdx = buf.writerIndex();
        buf.writeInt(0);
        buf.writeShort(row.length);

        for (int i = 0; i < row.length; i++) {
            Object val = row[i];
            if (val == null) {
                buf.writeInt(-1);
            } else {
                short format = 0;
                if (resultFormatCodes != null && resultFormatCodes.length > 0) {
                    format = resultFormatCodes.length == 1 ? resultFormatCodes[0] : (i < resultFormatCodes.length ? resultFormatCodes[i] : 0);
                }
                Column col = (columns != null && i < columns.size()) ? columns.get(i) : null;
                if (format == 1 && col != null) {
                    PgWireBinaryCodec.writeBinaryValue(buf, val, col);
                } else {
                    GucSettings guc = session != null ? session.getGucSettings() : null;
                    String text = (col != null && col.getArrayElementType() != null)
                            ? PgWireValueFormatter.formatArray(val, col.getArrayElementType(), guc)
                            : PgWireValueFormatter.formatValue(val, guc);
                    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
                    buf.writeInt(bytes.length);
                    buf.writeBytes(bytes);
                }
            }
        }

        buf.setInt(lengthIdx, buf.writerIndex() - lengthIdx);
        ctx.write(buf);
    }

    static void sendCommandComplete(ChannelHandlerContext ctx, String tag) {
        // Flush pending notices. The Session is accessed via the handler's instance,
        // but this is a static helper called from CopyHandler too. In that case,
        // notices are flushed by the handler's sendQueryResult path.
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('C');
        byte[] tagBytes = tag.getBytes(StandardCharsets.UTF_8);
        buf.writeInt(4 + tagBytes.length + 1);
        buf.writeBytes(tagBytes);
        buf.writeByte(0);
        ctx.write(buf);
    }

    /** Flush pending notices before CommandComplete (instance method for simple/extended protocol). */
    private void sendCommandCompleteWithNotices(ChannelHandlerContext ctx, String tag) {
        flushPendingNotices(ctx);
        sendCommandComplete(ctx, tag);
    }

    private void sendPortalSuspended(ChannelHandlerContext ctx) {
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('s');
        buf.writeInt(4);
        ctx.write(buf);
    }

    /**
     * Writes whatever notifications this session is holding, and says whether it wrote any.
     *
     * <p>Both the push and the ReadyForQuery drain take from the one queue, so each notification
     * is written once by whichever gets there first and none is lost between them.
     */
    private static boolean drainNotifications(ChannelHandlerContext ctx, Session session) {
        boolean wrote = false;
        Notification notification;
        while ((notification = session.getPendingNotifications().poll()) != null) {
            sendNotificationResponse(ctx, notification);
            wrote = true;
        }
        return wrote;
    }

    static void sendReadyForQuery(ChannelHandlerContext ctx, Session session) {
        drainNotifications(ctx, session);

        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('Z');
        buf.writeInt(5);
        buf.writeByte(session.getReadyForQueryStatus());
        ctx.writeAndFlush(buf);
    }

    private static void sendNotificationResponse(ChannelHandlerContext ctx, Notification n) {
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('A');
        int startIdx = buf.writerIndex();
        buf.writeInt(0);
        buf.writeInt(n.pid());
        PgWireValueFormatter.writeCString(buf, n.channel());
        PgWireValueFormatter.writeCString(buf, n.payload());
        buf.setInt(startIdx, buf.writerIndex() - startIdx);
        ctx.write(buf);
    }

    /** Send an error for simple query protocol (no error flag). */
    static void sendErrorSimple(ChannelHandlerContext ctx, String sqlState, String message) {
        sendError(ctx, sqlState, message, false);
    }

    /** Send an error with full diagnostic fields from a MemgresException. */
    static void sendErrorWithDetails(ChannelHandlerContext ctx, MemgresException ex, boolean isExtended) {
        sendErrorWithDetails(ctx, ex, isExtended, null);
    }

    /**
     * The same, and with it what the server was doing when it raised: the field a client prints
     * as CONTEXT.
     *
     * <p>PostgreSQL sends it whenever the error came out of something reading on the client's
     * behalf rather than out of the statement text, which is what a COPY is: the relation, the
     * line of the input reached and the line itself are the only way a sender of thousands of
     * lines can tell which one the server would not take.
     */
    static void sendErrorWithDetails(ChannelHandlerContext ctx, MemgresException ex,
                                     boolean isExtended, String context) {
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('E');
        int lengthIdx = buf.writerIndex();
        buf.writeInt(0);
        buf.writeByte('S');
        PgWireValueFormatter.writeCString(buf, "ERROR");
        buf.writeByte('V');
        PgWireValueFormatter.writeCString(buf, "ERROR");
        buf.writeByte('C');
        PgWireValueFormatter.writeCString(buf, ex.getSqlState() != null ? ex.getSqlState() : "XX000");
        buf.writeByte('M');
        PgWireValueFormatter.writeCString(buf, ex.getMessage());
        if (ex.getDetail() != null) {
            buf.writeByte('D');
            PgWireValueFormatter.writeCString(buf, ex.getDetail());
        }
        if (ex.getHint() != null) {
            buf.writeByte('H');
            PgWireValueFormatter.writeCString(buf, ex.getHint());
        }
        if (ex.getPosition() > 0) {
            buf.writeByte('P');
            PgWireValueFormatter.writeCString(buf, String.valueOf(ex.getPosition()));
        }
        // Where the caller named no context of its own, the error's stands: PostgreSQL reports
        // the frames of whatever was running on the statement's behalf -- a PL/pgSQL function, a
        // trigger -- whether or not a COPY was reading when it raised.
        String where = context != null ? context : ex.getPgContext();
        if (where != null) {
            buf.writeByte('W');
            PgWireValueFormatter.writeCString(buf, where);
        }
        if (ex.getSchema() != null) {
            buf.writeByte('s');
            PgWireValueFormatter.writeCString(buf, ex.getSchema());
        }
        if (ex.getTable() != null) {
            buf.writeByte('t');
            PgWireValueFormatter.writeCString(buf, ex.getTable());
        }
        if (ex.getColumn() != null) {
            buf.writeByte('c');
            PgWireValueFormatter.writeCString(buf, ex.getColumn());
        }
        if (ex.getConstraint() != null) {
            buf.writeByte('n');
            PgWireValueFormatter.writeCString(buf, ex.getConstraint());
        }
        if (ex.getDatatype() != null) {
            buf.writeByte('d');
            PgWireValueFormatter.writeCString(buf, ex.getDatatype());
        }
        // File, Line, Routine stub fields (always populated by real PG)
        buf.writeByte('F');
        PgWireValueFormatter.writeCString(buf, "postgres.c");
        buf.writeByte('L');
        PgWireValueFormatter.writeCString(buf, "1");
        buf.writeByte('R');
        PgWireValueFormatter.writeCString(buf, "exec_simple_query");
        buf.writeByte(0);
        buf.setInt(lengthIdx, buf.writerIndex() - lengthIdx);
        ctx.write(buf);
    }

    /** Send an error for extended query protocol (sets error flag to skip until Sync). */
    private void sendExtendedError(ChannelHandlerContext ctx, String sqlState, String message) {
        sendError(ctx, sqlState, message, true);
        extendedErrorReported(ctx);
    }

    /**
     * A COPY under the extended protocol failed, so everything up to Sync is skipped just as it
     * is for any other extended error. The copy handler sends its own ErrorResponse — it is the
     * one that knows what went wrong — and this is what stops the messages that follow.
     */
    void setErrorPendingUntilSync(ChannelHandlerContext ctx) {
        extendedErrorReported(ctx);
    }

    /**
     * What an ErrorResponse inside an extended-query sequence carries with it. PostgreSQL does all
     * three for every error, whichever layer raised it.
     *
     * <p>Everything up to Sync is skipped. The transaction the error happened in is aborted: from
     * that moment the block can do no more work, the statements after it are refused with 25P02,
     * and COMMIT throws away what it had done rather than making it permanent. Only a statement
     * that reached the executor used to abort here, so an error the protocol layer raised for
     * itself — a portal that has already run to its end, a message whose bytes could not be read —
     * left the block open and running: ReadyForQuery answered T where PostgreSQL answers E.
     *
     * <p>And the bytes go out now rather than at the next Sync, because PostgreSQL flushes as soon
     * as it has written an ErrorResponse. Nothing else would push them: the messages up to Sync are
     * skipped, and a client's Flush among them is skipped with them, so a client that flushes and
     * waits for the answer was waiting on a buffer.
     */
    private void extendedErrorReported(ChannelHandlerContext ctx) {
        errorPendingUntilSync = true;
        if (session != null && session.getStatus() == Session.TransactionStatus.IN_TRANSACTION) {
            session.restoreStatus(Session.TransactionStatus.FAILED);
        }
        ctx.flush();
    }

    /** A connection the server is about to drop; PG reports these at FATAL, not ERROR. */
    private static void sendFatal(ChannelHandlerContext ctx, String sqlState, String message) {
        LOG.warn("[PROTO] Sending FATAL ErrorResponse: sqlState={} msg={}", sqlState, message);
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('E');
        int lengthIdx = buf.writerIndex();
        buf.writeInt(0);
        buf.writeByte('S');
        PgWireValueFormatter.writeCString(buf, "FATAL");
        buf.writeByte('V');
        PgWireValueFormatter.writeCString(buf, "FATAL");
        buf.writeByte('C');
        PgWireValueFormatter.writeCString(buf, sqlState);
        buf.writeByte('M');
        PgWireValueFormatter.writeCString(buf, message);
        buf.writeByte('F');
        PgWireValueFormatter.writeCString(buf, "postgres.c");
        buf.writeByte('L');
        PgWireValueFormatter.writeCString(buf, "1");
        buf.writeByte('R');
        PgWireValueFormatter.writeCString(buf, "ProcessInterrupts");
        buf.writeByte(0);
        buf.setInt(lengthIdx, buf.writerIndex() - lengthIdx);
        ctx.writeAndFlush(buf);
    }

    private static void sendError(ChannelHandlerContext ctx, String sqlState, String message, boolean isExtendedProtocol) {
        LOG.warn("[PROTO] Sending ErrorResponse: sqlState={} extended={} msg={}", sqlState, isExtendedProtocol, message);
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('E');
        int lengthIdx = buf.writerIndex();
        buf.writeInt(0);
        buf.writeByte('S');
        PgWireValueFormatter.writeCString(buf, "ERROR");
        buf.writeByte('V');
        PgWireValueFormatter.writeCString(buf, "ERROR");
        buf.writeByte('C');
        PgWireValueFormatter.writeCString(buf, sqlState);
        buf.writeByte('M');
        PgWireValueFormatter.writeCString(buf, message);
        // File, Line, Routine stub fields (always populated by real PG)
        buf.writeByte('F');
        PgWireValueFormatter.writeCString(buf, "postgres.c");
        buf.writeByte('L');
        PgWireValueFormatter.writeCString(buf, "1");
        buf.writeByte('R');
        PgWireValueFormatter.writeCString(buf, "exec_simple_query");
        buf.writeByte(0);
        buf.setInt(lengthIdx, buf.writerIndex() - lengthIdx);
        ctx.write(buf);
    }

    /**
     * Enrich a MemgresException with position information by finding the
     * referenced object name (table, column, etc.) in the SQL text.
     */
    private static void enrichErrorPosition(MemgresException e, String sql) {
        if (e.getPosition() > 0 || sql == null) return;
        // Errors PostgreSQL raises with no parse location of their own get no Position at all,
        // even though their message quotes a name that could be found in the text.
        if (e.isPositionSuppressed()) return;
        String msg = e.getMessage();
        if (msg == null) return;
        // A throw site that knows which word it is complaining about names it, because the word is
        // not always the one the message quotes -- "not allowed in LIMIT" points at the call.
        if (e.getPositionToken() != null) {
            int at = findToken(sql, e.getPositionToken());
            if (at >= 0) e.setPosition(at + 1);
            return;
        }
        // A row that breaks a constraint breaks it as it is stored, and a write to a column the
        // relation computes for itself is refused as the statement is rewritten. Neither is read
        // off a place in the text, so PostgreSQL sends no Position for them — while their messages
        // quote a column or a relation name that the search below finds somewhere in the statement
        // anyway: for UPDATE t SET d = DEFAULT it found the "d" inside the word UPDATE.
        if (hasNoParseLocation(e.getSqlState())) return;
        // Extract quoted name from error message patterns like: relation "foo" does not exist
        // or column "bar" does not exist, or at or near "token"
        String name = null;
        int qStart = msg.indexOf('"');
        if (qStart >= 0) {
            int qEnd = msg.indexOf('"', qStart + 1);
            if (qEnd > qStart) {
                name = msg.substring(qStart + 1, qEnd);
            }
        }
        if (name != null && !name.isEmpty()) {
            // Find the name in the SQL (case-insensitive)
            String lowerSql = sql.toLowerCase();
            String lowerName = name.toLowerCase();
            int idx = lowerSql.indexOf(lowerName);
            if (idx >= 0) {
                e.setPosition(idx + 1); // 1-based
                return;
            }
        }
        // For syntax errors where no quoted name was found, try "at or near" pattern
        // or just set position to 1 for general errors
        String sqlState = e.getSqlState();
        if ("42601".equals(sqlState) || (msg.toLowerCase().contains("syntax") && "42000".equals(sqlState))) {
            // Set position to approximately where the error is — use SELECT FROM case
            // Try to find the problematic token
            e.setPosition(1);
        }
        // For relation/column does not exist, if we couldn't find the exact name, set position 1
        if ("42P01".equals(sqlState) || "42703".equals(sqlState)) {
            if (e.getPosition() == 0) e.setPosition(1);
        }
    }

    /**
     * The offset of {@code token} in {@code sql} as a whole word, or -1. Whole-word so that a
     * function named in the message is not found inside a longer identifier that contains it.
     */
    private static int findToken(String sql, String token) {
        String lowerSql = sql.toLowerCase();
        String lowerToken = token.toLowerCase();
        int from = 0;
        while (true) {
            int idx = lowerSql.indexOf(lowerToken, from);
            if (idx < 0) return -1;
            boolean startOk = idx == 0 || !isIdentChar(lowerSql.charAt(idx - 1));
            int end = idx + lowerToken.length();
            boolean endOk = end >= lowerSql.length() || !isIdentChar(lowerSql.charAt(end));
            if (startOk && endOk) return idx;
            from = idx + 1;
        }
    }

    private static boolean isIdentChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }

    /**
     * True for the errors PostgreSQL raises with nowhere in the statement behind them.
     *
     * <p>Integrity constraint violations — class 23, whether they come from a row being stored or
     * from a relation being altered under rows it already holds — are found by the executor rather
     * than by the parser, and a value written to an identity or generated column is refused by the
     * rewriter. Measured against PostgreSQL 18: not one of them carries a P field. A data exception
     * is a different matter and keeps its position, because a constant the parser coerced does have
     * a place in the text.
     */
    private static boolean hasNoParseLocation(String sqlState) {
        return sqlState != null && (sqlState.startsWith("23") || sqlState.equals("428C9"));
    }

    /** After a SET command, emit ParameterStatus messages for tracked GUC parameters. */
    private void emitParameterStatusUpdates(ChannelHandlerContext ctx, String sql) {
        if (session == null || session.getGucSettings() == null) return;
        // Parse "SET <param> TO <value>" or "SET <param> = <value>"
        String upper = sql.trim().toUpperCase();
        if (!upper.startsWith("SET ")) return;
        // Emit ParameterStatus for GUC parameters that PG 18 reports to clients.
        // Note: pgjdbc will disconnect (08006) if DateStyle changes to non-ISO
        // or client_encoding changes from UTF8 — this matches real PG behavior.
        String[] tracked = {"application_name", "DateStyle", "IntervalStyle",
                "is_superuser", "session_authorization",
                "standard_conforming_strings", "TimeZone"};
        for (String param : tracked) {
            if (upper.contains(param.toUpperCase())) {
                String val = session.getGucSettings().get(param);
                if (val != null) {
                    sendParameterStatus(ctx, param, val);
                }
            }
        }
    }

    private void sendParameterStatus(ChannelHandlerContext ctx, String name, String value) {
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('S');
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        buf.writeInt(4 + nameBytes.length + 1 + valueBytes.length + 1);
        buf.writeBytes(nameBytes);
        buf.writeByte(0);
        buf.writeBytes(valueBytes);
        buf.writeByte(0);
        ctx.write(buf);
    }

    /** Flush pending notices as NoticeResponse messages, filtered by client_min_messages. */
    private void flushPendingNotices(ChannelHandlerContext ctx) {
        List<Session.PgNotice> notices = session.drainPendingNotices();
        int minLevel = getClientMinMessagesLevel();
        for (Session.PgNotice notice : notices) {
            // INFO is the one level client_min_messages does not govern: PostgreSQL always
            // sends it to the client, which is what makes RAISE INFO usable for output
            boolean always = "INFO".equalsIgnoreCase(notice.severity());
            if (always || noticeSeverityLevel(notice.severity()) >= minLevel) {
                sendNoticeResponse(ctx, notice);
            }
        }
    }

    /** Map severity string to numeric level (higher = more important). */
    private static int noticeSeverityLevel(String severity) {
        if (severity == null) return 5; // NOTICE default
        switch (severity.toUpperCase()) {
            case "DEBUG": case "DEBUG1": case "DEBUG2": case "DEBUG3": case "DEBUG4": case "DEBUG5":
                return 1;
            case "LOG":
                return 2;
            case "INFO":
                return 3;
            case "NOTICE":
                return 5;
            case "WARNING":
                return 6;
            case "ERROR":
                return 7;
            default:
                return 5;
        }
    }

    /** Get the numeric level for client_min_messages GUC setting. */
    private int getClientMinMessagesLevel() {
        if (session == null || session.getGucSettings() == null) return 5; // default NOTICE
        String setting = session.getGucSettings().get("client_min_messages");
        if (setting == null) return 5;
        return noticeSeverityLevel(setting);
    }

    private void sendNoticeResponse(ChannelHandlerContext ctx, Session.PgNotice notice) {
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('N');
        int lengthIdx = buf.writerIndex();
        buf.writeInt(0);

        String severity = notice.severity() != null ? notice.severity() : "NOTICE";
        buf.writeByte('S');
        PgWireValueFormatter.writeCString(buf, severity);
        buf.writeByte('V');
        PgWireValueFormatter.writeCString(buf, severity);

        String sqlState = notice.sqlState() != null ? notice.sqlState() : "00000";
        buf.writeByte('C');
        PgWireValueFormatter.writeCString(buf, sqlState);

        String message = notice.message() != null ? notice.message() : "";
        buf.writeByte('M');
        PgWireValueFormatter.writeCString(buf, message);

        // A notice's DETAIL is a field of its own, the way an error's is: a client reads it
        // through ServerErrorMessage.getDetail() and would never see it folded into the message.
        if (notice.detail() != null && !notice.detail().isEmpty()) {
            buf.writeByte('D');
            PgWireValueFormatter.writeCString(buf, notice.detail());
        }

        if (notice.hint() != null && !notice.hint().isEmpty()) {
            buf.writeByte('H');
            PgWireValueFormatter.writeCString(buf, notice.hint());
        }

        buf.writeByte(0);
        buf.setInt(lengthIdx, buf.writerIndex() - lengthIdx);
        ctx.write(buf);
    }

    // ---- Statement splitting ----

    /**
     * Check if the word at position i in sql matches the given keyword (case-insensitive),
     * and is bounded by non-identifier characters on both sides.
     */
    private static boolean matchWordAt(String sql, int i, String keyword) {
        int len = keyword.length();
        if (i + len > sql.length()) return false;
        if (!sql.regionMatches(true, i, keyword, 0, len)) return false;
        // Check boundary before
        if (i > 0 && Character.isLetterOrDigit(sql.charAt(i - 1))) return false;
        // Check boundary after
        if (i + len < sql.length() && (Character.isLetterOrDigit(sql.charAt(i + len)) || sql.charAt(i + len) == '_')) return false;
        return true;
    }

    private String[] splitStatements(String sql) {
        java.util.List<String> statements = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        char stringChar = 0;
        boolean eString = false; // true when inside E'...' (backslash escaping)
        // Track BEGIN ATOMIC ... END blocks so semicolons inside are not treated as statement separators.
        // caseDepth counts nested CASE expressions whose END should not close the block.
        boolean inBeginAtomic = false;
        int caseDepth = 0;
        // A multi-action rule body — DO ALSO ( a; b; ) — puts semicolons inside parentheses,
        // where they separate the rule's actions rather than one statement from the next.
        int parenDepth = 0;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            if (!inString && c == '$') {
                int j = i + 1;
                while (j < sql.length() && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) j++;
                if (j < sql.length() && sql.charAt(j) == '$') {
                    String delimiter = sql.substring(i, j + 1);
                    current.append(delimiter);
                    i = j + 1;
                    int close = -1;
                    // Dollar-quoted bodies are fully literal — no apostrophe tracking needed.
                    for (int k = i; k <= sql.length() - delimiter.length(); k++) {
                        if (sql.startsWith(delimiter, k)) { close = k; break; }
                    }
                    if (close >= 0) {
                        current.append(sql, i, close + delimiter.length());
                        i = close + delimiter.length() - 1;
                    } else {
                        current.append(sql.substring(i));
                        i = sql.length() - 1;
                    }
                    continue;
                }
                if (j == i + 1 && j < sql.length() && Character.isWhitespace(sql.charAt(j))) {
                    current.append(c);
                    i = j;
                    int close = -1;
                    for (int k = i; k < sql.length(); k++) {
                        if (sql.charAt(k) == '$') {
                            if (k + 1 >= sql.length() || sql.charAt(k + 1) == ';' || Character.isWhitespace(sql.charAt(k + 1))) {
                                close = k;
                                break;
                            }
                        }
                    }
                    if (close >= 0) {
                        current.append(sql, i, close + 1);
                        i = close;
                    } else {
                        current.append(sql.substring(i));
                        i = sql.length() - 1;
                    }
                    continue;
                }
                current.append(c);
                continue;
            }

            if (inString) {
                current.append(c);
                if (eString && c == '\\' && i + 1 < sql.length()) {
                    // In E-strings, backslash escapes the next character
                    current.append(sql.charAt(++i));
                } else if (c == stringChar) {
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == stringChar) {
                        current.append(sql.charAt(++i));
                    } else {
                        inString = false;
                        eString = false;
                    }
                }
            } else if (c == '\'' || c == '"') {
                inString = true;
                stringChar = c;
                // Check if this is an E-string (E' or e') — E must not be part of a longer identifier
                if (c == '\'' && current.length() > 0) {
                    char prev = current.charAt(current.length() - 1);
                    if ((prev == 'E' || prev == 'e') &&
                            (current.length() == 1 || !Character.isLetterOrDigit(current.charAt(current.length() - 2)) && current.charAt(current.length() - 2) != '_')) {
                        eString = true;
                    }
                }
                current.append(c);
            } else if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
                current.append(c);
                i++;
                current.append(sql.charAt(i));
                int depth = 1;
                while (i + 1 < sql.length() && depth > 0) {
                    i++;
                    char bc = sql.charAt(i);
                    current.append(bc);
                    if (bc == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
                        depth++; i++; current.append(sql.charAt(i));
                    } else if (bc == '*' && i + 1 < sql.length() && sql.charAt(i + 1) == '/') {
                        depth--; i++; current.append(sql.charAt(i));
                    }
                }
            } else if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                int eol = sql.indexOf('\n', i);
                if (eol < 0) eol = sql.length();
                current.append(sql, i, eol);
                i = eol - 1;
            } else if (c == ';') {
                if (inBeginAtomic || parenDepth > 0) {
                    // Inside BEGIN ATOMIC block — semicolons are part of the body, not statement separators
                    current.append(c);
                } else {
                    String stmt = current.toString().trim();
                    if (!stmt.isEmpty()) statements.add(stmt);
                    current = new StringBuilder();
                    inBeginAtomic = false;
                    caseDepth = 0;
                }
            } else {
                if (c == '(') parenDepth++;
                else if (c == ')' && parenDepth > 0) parenDepth--;
                // Detect BEGIN ATOMIC, CASE, and END keywords to track block nesting
                if (!inString && Character.isLetter(c)) {
                    if (inBeginAtomic) {
                        if (matchWordAt(sql, i, "CASE")) {
                            caseDepth++;
                        } else if (matchWordAt(sql, i, "END")) {
                            if (caseDepth > 0) {
                                caseDepth--;
                            } else {
                                // This END closes the BEGIN ATOMIC block
                                current.append(sql, i, i + 3);
                                i += 2; // advance past "END" (loop will i++ once more)
                                inBeginAtomic = false;
                                caseDepth = 0;
                                continue;
                            }
                        }
                    } else if (matchWordAt(sql, i, "BEGIN")) {
                        // Check if followed by ATOMIC
                        int afterBegin = i + 5; // length of "BEGIN"
                        // Skip whitespace
                        while (afterBegin < sql.length() && Character.isWhitespace(sql.charAt(afterBegin))) afterBegin++;
                        if (matchWordAt(sql, afterBegin, "ATOMIC")) {
                            inBeginAtomic = true;
                            caseDepth = 0;
                        }
                    }
                }
                current.append(c);
            }
        }

        String last = current.toString().trim();
        if (!last.isEmpty()) statements.add(last);
        return statements.toArray(new String[0]);
    }

    // ---- Channel lifecycle ----

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cancelIdleInTransactionTimeout();
        if (session != null) session.close();
        if (connectionRegistered) {
            database.unregisterConnection();
            connectionRegistered = false;
        }
        if (backendPid != 0) {
            cancelRegistry.unregister(backendPid, backendSecretKey);
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof java.io.IOException) {
            // Network-level errors (broken pipe, connection reset) — just close
            LOG.debug("Connection I/O error: {}", cause.getMessage());
            ctx.close();
            return;
        }
        LOG.error("Connection error", cause);
        // Try to send an error response instead of killing the connection.
        // pg_dump workers die with "worker process died unexpectedly" when the
        // connection is closed abruptly; sending a proper ErrorResponse lets
        // libpq report the real error and avoids the cascade.
        try {
            if (ctx.channel().isActive()) {
                MemgresException translated = PgErrors.translate(cause);
                sendErrorSimple(ctx, translated.getSqlState(), translated.getMessage());
                // A ReadyForQuery nobody asked for leaves the connection one response ahead of
                // its client for good: every later statement hands back the previous statement's
                // result. Inside an extended-query sequence the client's own Sync produces it, so
                // this only answers where the client is actually waiting for one.
                if (PgWireDecoder.isExtendedQueryMessage(currentFrontendType)) {
                    extendedErrorReported(ctx);
                } else {
                    sendReadyForQuery(ctx, session);
                }
            }
        } catch (Exception e) {
            // If sending the error also fails, close the connection
            LOG.debug("Failed to send error response, closing connection", e);
            ctx.close();
        }
    }
}
