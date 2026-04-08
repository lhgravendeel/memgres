package com.memgres.pgwire;

import com.memgres.engine.util.Cols;

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
    private int backendSecretKey;
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
        boolean rowDescriptionSent;
        boolean describeAttempted;
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
        this.copyHandler = new PgWireCopyHandler(session);
        this.describeHelper = new PgWireDescribeHelper(session, database);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, PgWireMessage msg) {
        if (errorPendingUntilSync) {
            switch (msg.getType()) {
                case QUERY: {
                    errorPendingUntilSync = false; handleQuery(ctx, msg); 
                    break;
                }
                case SYNC:
                    handleSync(ctx);
                    break;
                case FLUSH:
                    handleFlush(ctx);
                    break;
                case TERMINATE:
                    ctx.close();
                    break;
                default: {
                    if (Memgres.logAllStatements) LOG.info("[PROTO] Discarding {} (errorPendingUntilSync)", msg.getType());
                    break;
                }
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
                handleSslRequest(ctx);
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

    // ---- Connection lifecycle ----

    private void handleSslRequest(ChannelHandlerContext ctx) {
        ByteBuf buf = ctx.alloc().buffer(1);
        buf.writeByte('N');
        ctx.writeAndFlush(buf);
    }

    private void handleStartup(ChannelHandlerContext ctx, PgWireMessage msg) {
        Map<String, String> params = msg.getParameters();
        if (params != null) {
            // Resolve target database from startup parameters
            String requestedDb = params.get("database");
            if (requestedDb != null && !requestedDb.isEmpty()) {
                Database resolved = registry.getDatabase(requestedDb);
                if (resolved == null) {
                    // Auto-create databases on connect for compatibility
                    registry.createDatabase(requestedDb);
                    resolved = registry.getDatabase(requestedDb);
                }
                this.database = resolved;
                this.databaseName = requestedDb;
                this.session = new Session(database);
                this.session.setDatabaseName(requestedDb);
                this.session.setDatabaseRegistry(registry);
                this.copyHandler = new PgWireCopyHandler(session);
                this.describeHelper = new PgWireDescribeHelper(session, database);
            }

            String connectingUser = params.get("user");
            if (connectingUser != null && !connectingUser.isEmpty()) {
                session.getGucSettings().set("session_authorization", connectingUser);
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
    }

    private void handlePassword(ChannelHandlerContext ctx, PgWireMessage msg) {
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

        backendPid = cancelRegistry.nextPid();
        backendSecretKey = (int) (Math.random() * Integer.MAX_VALUE);
        cancelRegistry.register(backendPid, backendSecretKey);
        ByteBuf keyData = ctx.alloc().buffer();
        keyData.writeByte('K');
        keyData.writeInt(12);
        keyData.writeInt(backendPid);
        keyData.writeInt(backendSecretKey);
        ctx.write(keyData);

        sendReadyForQuery(ctx, session);
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
                    sendQueryResult(ctx, result);
                } catch (MemgresException e) {
                    sendErrorSimple(ctx, e.getSqlState(), e.getMessage());
                    batchFailed = true;
                } catch (ArithmeticException e) {
                    String errMsg = e.getMessage() != null ? e.getMessage() : "arithmetic error";
                    if (errMsg.contains("/ by zero") || errMsg.contains("divide by zero") || errMsg.contains("Division by zero")) {
                        sendErrorSimple(ctx, "22012", "division by zero");
                    } else {
                        sendErrorSimple(ctx, "22003", errMsg);
                    }
                    batchFailed = true;
                }
            }
        } catch (Exception e) {
            LOG.error("Error executing query: {}", sql, e);
            sendErrorSimple(ctx, "XX000", "Internal error: " + e.getMessage());
        }
        if (!copyHandler.inCopyFromMode) {
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

        preparedStatements.put(stmtName, new PreparedStmt(sql, paramOids));
        if (stmtName != null && !stmtName.isEmpty()) {
            stmtDescribed.remove(stmtName);
        }

        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('1');
        buf.writeInt(4);
        ctx.write(buf);
    }

    private void handleBind(ChannelHandlerContext ctx, PgWireMessage msg) {
        String portalName = msg.getPortalName() != null ? msg.getPortalName() : "";
        String stmtName = msg.getStatementName() != null ? msg.getStatementName() : "";

        PreparedStmt prepared = preparedStatements.get(stmtName);
        if (prepared == null) {
            sendExtendedError(ctx, "26000", "prepared statement \"" + stmtName + "\" does not exist");
            return;
        }

        List<Object> paramValues = new ArrayList<>();
        byte[][] rawValues = msg.getParameterValues();
        short[] formatCodes = msg.getParameterFormatCodes();
        if (rawValues != null) {
            for (int i = 0; i < rawValues.length; i++) {
                if (rawValues[i] == null) {
                    paramValues.add(null);
                } else {
                    short format = 0;
                    if (formatCodes != null && formatCodes.length > 0) {
                        format = formatCodes.length == 1 ? formatCodes[0] : formatCodes[i];
                    }
                    if (format == 0) {
                        paramValues.add(new String(rawValues[i], StandardCharsets.UTF_8));
                    } else {
                        int paramOid = (prepared.paramOids() != null && i < prepared.paramOids().length)
                                ? prepared.paramOids()[i] : 0;
                        paramValues.add(PgWireBinaryCodec.decodeBinaryParam(rawValues[i], paramOid));
                    }
                }
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

    private void handleDescribe(ChannelHandlerContext ctx, PgWireMessage msg) {
        byte descType = msg.getDescribeType();
        String name = msg.getStatementName() != null ? msg.getStatementName() : "";
        if (Memgres.logAllStatements) LOG.info("[PROTO] Describe {} name='{}'", descType == 'S' ? "Statement" : "Portal", name);

        if (descType == 'S') {
            PreparedStmt prepared = preparedStatements.get(name);
            if (prepared == null) {
                sendExtendedError(ctx, "26000", "prepared statement \"" + name + "\" does not exist");
                return;
            }
            boolean sent = describeHelper.describeStatement(ctx, name, prepared.sql(), prepared.paramOids());
            if (sent) markStatementDescribed(name);
        } else {
            Portal portal = portals.get(name);
            if (portal != null) portal.describeAttempted = true;
            if (portal == null) {
                LOG.warn("[PROTO] Describe Portal: portal '{}' does not exist!", name);
                sendExtendedError(ctx, "34000", "portal \"" + name + "\" does not exist");
                return;
            }
            PgWireDescribeHelper.DescribePortalResult result = describeHelper.describePortal(ctx, portal.sql(), portal.paramValues());
            if (result.rowDescSent()) {
                rowDescSentByDescribe = true;
                portal.rowDescriptionSent = true;
                if (result.cachedResult() != null) {
                    portal.describeResult = result.cachedResult();
                }
            }
        }
    }

    private void markStatementDescribed(String name) {
        rowDescSentByDescribe = true;
        if (name != null && !name.isEmpty()) stmtDescribed.put(name, true);
    }

    private void handleExecute(ChannelHandlerContext ctx, PgWireMessage msg) {
        String portalName = msg.getPortalName() != null ? msg.getPortalName() : "";
        int maxRows = msg.getMaxRows();

        Portal portal = portals.get(portalName);
        if (portal == null) {
            PreparedStmt unnamed = preparedStatements.get("");
            if (unnamed != null) {
                portal = new Portal(unnamed.sql(), Cols.listOf(), null);
            }
        }

        if (portal == null || portal.sql() == null || portal.sql().trim().isEmpty()) {
            if (Memgres.logAllStatements) LOG.info("[PROTO] Execute → EmptyQueryResponse (no portal/sql)");
            ByteBuf buf = ctx.alloc().buffer();
            buf.writeByte('I');
            buf.writeInt(4);
            ctx.write(buf);
            return;
        }

        String sqlSnip = portal.sql().substring(0, Math.min(70, portal.sql().length())).replace("\n", " ");

        try {
            QueryResult result;
            String source;

            if (portal.suspendedResult != null) {
                result = portal.suspendedResult;
                source = "suspended";
            } else if (portal.describeResult != null) {
                result = portal.describeResult;
                portal.describeResult = null;
                source = "cached";
            } else {
                source = "fresh";
                String[] stmts = splitStatements(portal.sql());
                if (stmts.length > 1) {
                    for (int si = 0; si < stmts.length - 1; si++) {
                        String s = stmts[si].trim();
                        if (!s.isEmpty()) {
                            try {
                                executeWithCancel(s, portal.paramValues());
                            } catch (MemgresException e) {
                                sendExtendedError(ctx, e.getSqlState(), e.getMessage());
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

            // Handle maxRows (cursor-based fetching with portal suspend/resume)
            if (maxRows > 0 && result.getType() == QueryResult.Type.SELECT) {
                List<Object[]> allRows = result.getRows();
                int offset = portal.suspendedOffset;
                int end = Math.min(offset + maxRows, allRows.size());
                for (int i = offset; i < end; i++) {
                    sendDataRow(ctx, allRows.get(i), result.getColumns(), portal.resultFormatCodes());
                }
                if (end < allRows.size()) {
                    portal.suspendedResult = result;
                    portal.suspendedOffset = end;
                    sendPortalSuspended(ctx);
                } else {
                    portal.suspendedResult = null;
                    portal.suspendedOffset = 0;
                    sendCommandCompleteWithNotices(ctx, "SELECT " + allRows.size());
                }
            } else {
                sendResultDataOnly(ctx, result, portal.resultFormatCodes());
            }
        } catch (MemgresException e) {
            LOG.warn("[PROTO] Execute ERROR {}: {} | {}", e.getSqlState(), e.getMessage(), sqlSnip);
            sendExtendedError(ctx, e.getSqlState(), e.getMessage());
        } catch (ArithmeticException e) {
            String errMsg = e.getMessage() != null ? e.getMessage() : "arithmetic error";
            LOG.warn("[PROTO] Execute ARITH ERROR: {} | {}", errMsg, sqlSnip);
            if (errMsg.contains("/ by zero") || errMsg.contains("divide by zero") || errMsg.contains("Division by zero")) {
                sendExtendedError(ctx, "22012", "division by zero");
            } else {
                sendExtendedError(ctx, "22003", errMsg);
            }
        } catch (Exception e) {
            LOG.error("[PROTO] Execute INTERNAL ERROR: {} | {}", e.getMessage(), sqlSnip, e);
            sendExtendedError(ctx, "XX000", "Internal error: " + e.getMessage());
        }
    }

    private void handleSync(ChannelHandlerContext ctx) {
        rowDescSentByDescribe = false;
        errorPendingUntilSync = false;
        sendReadyForQuery(ctx, session);
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
                return "BEGIN";
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
                copyHandler.sendCopyInResult(ctx, result);
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
                copyHandler.sendCopyInResult(ctx, result);
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
        PgWireValueFormatter.sendRowDescription(buf, result.getColumns());
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
                if (format == 1 && columns != null && i < columns.size()) {
                    PgWireBinaryCodec.writeBinaryValue(buf, val, columns.get(i).getType());
                } else {
                    String text = PgWireValueFormatter.formatValue(val,
                            session != null ? session.getGucSettings() : null);
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

    static void sendReadyForQuery(ChannelHandlerContext ctx, Session session) {
        // Drain pending NOTIFY messages
        Notification notification;
        while ((notification = session.getPendingNotifications().poll()) != null) {
            sendNotificationResponse(ctx, notification);
        }

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

    /** Send an error for extended query protocol (sets error flag to skip until Sync). */
    private void sendExtendedError(ChannelHandlerContext ctx, String sqlState, String message) {
        sendError(ctx, sqlState, message, true);
        errorPendingUntilSync = true;
    }

    private static void sendError(ChannelHandlerContext ctx, String sqlState, String message, boolean isExtendedProtocol) {
        LOG.warn("[PROTO] Sending ErrorResponse: sqlState={} extended={} msg={}", sqlState, isExtendedProtocol, message);
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('E');
        int lengthIdx = buf.writerIndex();
        buf.writeInt(0);
        buf.writeByte('S');
        PgWireValueFormatter.writeCString(buf, "ERROR");
        buf.writeByte('C');
        PgWireValueFormatter.writeCString(buf, sqlState);
        buf.writeByte('M');
        PgWireValueFormatter.writeCString(buf, message);
        buf.writeByte(0);
        buf.setInt(lengthIdx, buf.writerIndex() - lengthIdx);
        ctx.write(buf);
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

    /** Flush pending notices as NoticeResponse messages. */
    private void flushPendingNotices(ChannelHandlerContext ctx) {
        List<Session.PgNotice> notices = session.drainPendingNotices();
        for (Session.PgNotice notice : notices) {
            sendNoticeResponse(ctx, notice);
        }
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

        if (notice.hint() != null && !notice.hint().isEmpty()) {
            buf.writeByte('H');
            PgWireValueFormatter.writeCString(buf, notice.hint());
        }

        buf.writeByte(0);
        buf.setInt(lengthIdx, buf.writerIndex() - lengthIdx);
        ctx.write(buf);
    }

    // ---- Statement splitting ----

    private String[] splitStatements(String sql) {
        java.util.List<String> statements = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        char stringChar = 0;

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
                    boolean inBodyString = false;
                    for (int k = i; k <= sql.length() - delimiter.length(); k++) {
                        char bc = sql.charAt(k);
                        if (inBodyString) {
                            if (bc == '\'' && k + 1 < sql.length() && sql.charAt(k + 1) == '\'') { k++; }
                            else if (bc == '\'') { inBodyString = false; }
                            continue;
                        }
                        if (bc == '\'') { inBodyString = true; continue; }
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
                if (c == stringChar) {
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == stringChar) {
                        current.append(sql.charAt(++i));
                    } else {
                        inString = false;
                    }
                }
            } else if (c == '\'' || c == '"') {
                inString = true;
                stringChar = c;
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
                String stmt = current.toString().trim();
                if (!stmt.isEmpty()) statements.add(stmt);
                current = new StringBuilder();
            } else {
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
        if (session != null) session.dropTempObjects();
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
        LOG.error("Connection error", cause);
        ctx.close();
    }
}
