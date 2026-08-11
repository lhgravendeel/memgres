package com.memgres.pgwire;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import com.memgres.engine.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Statement metadata inference for Describe (Statement/Portal) in extended query protocol.
 * Determines column metadata without executing side-effecting statements.
 */
class PgWireDescribeHelper {

    private static final Logger LOG = LoggerFactory.getLogger(PgWireDescribeHelper.class);

    private final Session session;
    private final Database database;

    PgWireDescribeHelper(Session session, Database database) {
        this.session = session;
        this.database = database;
    }

    /**
     * Thrown by describeStatement/describePortal instead of falling back to NoData when a
     * statement is confirmed row-returning (isQueryStatement / isSafeToDescribe) but the
     * describe-time inference execution genuinely failed. PostgreSQL surfaces errors at
     * Describe time too, so the caller (PgWireHandler) should convert this into a real
     * ErrorResponse rather than silently telling the client "no result set" — the previous
     * behavior left the client believing a row-returning statement returns nothing, which
     * surfaces downstream as a confusing client-side "no results"/"missing field structure"
     * failure instead of the actual server-side error.
     */
    static final class DescribeExecutionFailedException extends RuntimeException {
        final String sqlState;

        DescribeExecutionFailedException(String sqlState, String message) {
            super(message);
            this.sqlState = sqlState;
        }
    }

    /**
     * A deadlock, lock timeout, or cancel raised while a describe probe waited on a lock
     * is a real statement outcome, not a metadata-inference miss. Swallowing it into
     * NoData and retrying re-enters the same lock wait: with two mutually-blocked
     * sessions, each side's 40P01 gets eaten by its own Describe and the wait-for edges
     * flap until a timeout fires — the deadlock is detected repeatedly but never
     * surfaces. These states must propagate to the client as the statement's error.
     */
    private static String lockWaitSqlState(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof MemgresException) {
                String s = ((MemgresException) t).getSqlState();
                if ("40P01".equals(s) || "55P03".equals(s) || "57014".equals(s)) return s;
            }
        }
        return null;
    }

    /**
     * The error to report for a statement that could not be described.
     *
     * <p>Describing a query runs it, and a query with parameters is run with nulls standing in for
     * them, so a statement the parser stops on is reported at whatever the null was written over.
     * The client wrote a parameter there, and that is what PostgreSQL names: reading the statement
     * as it was sent puts the parameter back into the message. Only a statement that fails as
     * written is re-reported — one that parses on its own was refused for the substitution, and
     * the substituted message is the true one.
     */
    private Exception failureAsWritten(String sql, Exception failure) {
        if (!"42601".equals(sqlStateOf(failure)) || countParameters(sql) == 0) return failure;
        try {
            com.memgres.engine.parser.Parser.parse(sql);
            return failure;
        } catch (RuntimeException asWritten) {
            return asWritten;
        }
    }

    /**
     * Whether a query already says how many rows it wants. Describing a SELECT runs it for its
     * column types with a row count of zero appended, and a query that limits itself takes no
     * second limit: {@code FETCH FIRST n ROWS ONLY} is the same clause spelled the other way, and
     * PostgreSQL rejects a LIMIT written after it as readily as it rejects two LIMITs.
     */
    private static boolean alreadyBounded(String sql) {
        String upper = sql.toUpperCase();
        return upper.contains("LIMIT") || upper.contains("FETCH");
    }

    private static String sqlStateOf(Exception e) {
        if (e instanceof MemgresException) {
            String state = ((MemgresException) e).getSqlState();
            if (state != null) return state;
        }
        return "XX000";
    }

    // ---- Describe Statement ----

    /**
     * Describe a prepared statement: send ParameterDescription + RowDescription or NoData.
     * Returns true if RowDescription was sent (so the caller can set rowDescSentByDescribe).
     */
    boolean describeStatement(ChannelHandlerContext ctx, String name, String sql, int[] paramOids) {
        if (sql == null || sql.trim().isEmpty()) {
            sendNoData(ctx);
            return false;
        }

        sendParameterDescription(ctx, sql, paramOids);

        // DML with RETURNING: infer columns from table schema
        String upper = stripLeadingComments(sql).toUpperCase();
        if (upper.contains("RETURNING") && (upper.startsWith("INSERT") || upper.startsWith("UPDATE") || upper.startsWith("DELETE") || upper.startsWith("MERGE"))) {
            List<Column> returningCols = inferReturningColumns(sql);
            if (returningCols != null) {
                sendRowDescription(ctx, QueryResult.select(returningCols, Cols.listOf()));
                return true;
            }
            if (countParameters(sql) > 0) {
                try {
                    String nullSql = replaceParamsWithNull(sql);
                    nullSql = nullSql.replaceAll(";\\s*$", "").trim();
                    if (!alreadyBounded(nullSql)) nullSql = nullSql + " LIMIT 0";
                    QueryResult result = session.execute(nullSql, Cols.listOf());
                    if (!result.getColumns().isEmpty()) {
                        sendRowDescription(ctx, result);
                        return true;
                    }
                } catch (Exception e) {
                    LOG.debug("Failed to infer DML RETURNING columns: {}", e.getMessage());
                }
            }
            sendNoData(ctx);
            return false;
        }

        // SHOW statements: execute directly (read-only)
        String upperSql = stripLeadingComments(sql).toUpperCase();
        if (upperSql.startsWith("SHOW")) {
            try {
                QueryResult result = session.execute(sql.replaceAll(";\\s*$", "").trim(), Cols.listOf());
                if (result.getType() == QueryResult.Type.SELECT || !result.getColumns().isEmpty()) {
                    sendRowDescription(ctx, result);
                    return true;
                }
            } catch (Exception e) {
                LOG.debug("Failed to describe SHOW: {}", e.getMessage());
            }
        }

        // CALL with OUT/INOUT params: infer columns from procedure definition
        if (upperSql.startsWith("CALL")) {
            List<Column> callCols = inferCallOutColumns(sql);
            if (callCols != null) {
                sendRowDescription(ctx, QueryResult.select(callCols, Cols.listOf()));
                return true;
            }
            sendNoData(ctx);
            return false;
        }

        // SELECT (safe to describe): use LIMIT 0 to get column metadata
        Exception describeFailure = null;
        if (isSafeToDescribe(sql)) {
            Session.TransactionStatus savedStatus = session.getStatus();
            try {
                String metaSql = countParameters(sql) > 0 ? replaceParamsWithNull(sql) : sql;
                metaSql = metaSql.replaceAll(";\\s*$", "").trim();
                metaSql = metaSql.replaceAll("--[^\\n]*$", "").trim();
                if (!alreadyBounded(metaSql)) metaSql = metaSql + " LIMIT 0";
                QueryResult result = session.execute(metaSql, Cols.listOf());
                if (result.getType() == QueryResult.Type.SELECT || !result.getColumns().isEmpty()) {
                    sendRowDescription(ctx, result);
                    if (Memgres.logAllStatements) LOG.info("[PROTO] Describe Stmt → RowDesc ({} cols) {}",
                            result.getColumns().size(), sql.substring(0, Math.min(70, sql.length())).replace("\n"," "));
                    return true;
                }
            } catch (Exception e) {
                LOG.warn("[PROTO] Describe Stmt LIMIT 0 failed: {} | {}", e.getMessage(),
                        sql.substring(0, Math.min(70, sql.length())).replace("\n"," "));
                session.restoreStatus(savedStatus);
                String lockState = lockWaitSqlState(e);
                if (lockState != null) {
                    throw new DescribeExecutionFailedException(lockState, e.getMessage());
                }
                describeFailure = e;
            }
        }

        // FETCH, EXECUTE: try to infer column metadata
        List<Column> cols = inferColumns(sql);
        if (cols != null) {
            sendRowDescription(ctx, QueryResult.select(cols, Cols.listOf()));
            if (Memgres.logAllStatements) LOG.info("[PROTO] Describe Stmt → RowDesc (infer, {} cols) {}",
                    cols.size(), sql.substring(0, Math.min(70, sql.length())).replace("\n"," "));
            return true;
        }

        if (isQueryStatement(sql)) {
            // A row-returning statement must never be told "NoData" — that leaves the
            // client believing it returns zero columns/no result set at all, which then
            // surfaces downstream as a confusing client-side failure (e.g. jdbi's
            // NoResultsException) instead of the real server-side error. Surface it now.
            if (describeFailure != null) {
                Exception reported = failureAsWritten(sql, describeFailure);
                throw new DescribeExecutionFailedException(sqlStateOf(reported), reported.getMessage());
            }
            LOG.warn("[PROTO] Describe Stmt fell through to NoData for query: {}",
                    sql.substring(0, Math.min(120, sql.length())).replace("\n"," "));
        }
        if (Memgres.logAllStatements) LOG.info("[PROTO] Describe Stmt → NoData: {}", sql.substring(0, Math.min(70, sql.length())).replace("\n"," "));
        sendNoData(ctx);
        return false;
    }

    // ---- Describe Portal ----

    /**
     * Describe a portal: send RowDescription or NoData.
     * May execute the SQL to determine columns (for safe queries or DML RETURNING).
     * Returns a DescribePortalResult with the result (if executed) and whether RowDescription was sent.
     */
    DescribePortalResult describePortal(ChannelHandlerContext ctx, String sql, List<Object> paramValues) {
        String sqlSnip = sql != null ? sql.substring(0, Math.min(70, sql.length())).replace("\n", " ") : "(null)";
        if (sql == null || sql.trim().isEmpty()) {
            if (Memgres.logAllStatements) LOG.info("[PROTO] Describe Portal → NoData (empty sql)");
            sendNoData(ctx);
            return new DescribePortalResult(false, null);
        }

        // DML with RETURNING
        String upper = stripLeadingComments(sql).toUpperCase();
        boolean isDmlReturning = upper.contains("RETURNING") && (upper.startsWith("INSERT") || upper.startsWith("UPDATE") || upper.startsWith("DELETE") || upper.startsWith("MERGE"));
        boolean isCteDmlReturning = upper.startsWith("WITH") && !isWithSelect(upper) && upper.contains("RETURNING");
        if (isDmlReturning || isCteDmlReturning) {
            if (isDmlReturning) {
                List<Column> returningCols = inferReturningColumns(sql);
                if (returningCols != null) {
                    sendRowDescription(ctx, QueryResult.select(returningCols, Cols.listOf()));
                    if (Memgres.logAllStatements) LOG.info("[PROTO] Describe Portal → RowDesc (DML RETURNING infer, {} cols) {}", returningCols.size(), sqlSnip);
                    return new DescribePortalResult(true, null);
                }
            }
            try {
                QueryResult result = session.execute(sql, paramValues);
                if (!result.getColumns().isEmpty()) {
                    sendRowDescription(ctx, result);
                    if (Memgres.logAllStatements) LOG.info("[PROTO] Describe Portal → RowDesc (DML exec, {} cols, {} rows) {}", result.getColumns().size(), result.getRows().size(), sqlSnip);
                    return new DescribePortalResult(true, result);
                }
                // The statement has already run. It turned out to have no columns to describe,
                // but discarding the result here let Execute run it a second time -- every row
                // written twice, and a primary key turning that into a duplicate-key error on a
                // statement that had already applied. NoData describes it; this is the result
                // Execute reports.
                sendNoData(ctx);
                return new DescribePortalResult(false, result);
            } catch (Exception e) {
                LOG.warn("[PROTO] Describe Portal DML exec failed: {} | {}", e.getMessage(), sqlSnip);
            }
        }

        // CALL: check for OUT/INOUT params by inspecting procedure signature without executing.
        // Only execute during Describe if the procedure has OUT/INOUT params (needs RowDescription).
        // For procedures without OUT/INOUT params, send NoData and let Execute handle the actual call.
        if (upper.startsWith("CALL")) {
            boolean hasOutParams = false;
            try {
                // Parse the CALL to get procedure name and look up its signature
                String callBody = sql.substring(4).trim();
                String procName = callBody.split("\\s*\\(", 2)[0].trim();
                com.memgres.engine.PgFunction proc;
                if (procName.contains(".")) {
                    String[] parts = procName.split("\\.", 2);
                    proc = session.getDatabase().getFunction(parts[0], parts[1]);
                } else {
                    proc = session.getDatabase().getFunction(procName);
                }
                if (proc != null) {
                    for (com.memgres.engine.PgFunction.Param p : proc.getParams()) {
                        String mode = p.mode() != null ? p.mode().toUpperCase() : "IN";
                        if ("OUT".equals(mode) || "INOUT".equals(mode)) {
                            hasOutParams = true;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                // Fallback: if we can't determine, don't execute during Describe
            }
            if (hasOutParams) {
                try {
                    QueryResult result = session.execute(sql, paramValues);
                    if (result.getType() == QueryResult.Type.SELECT && !result.getColumns().isEmpty()) {
                        sendRowDescription(ctx, QueryResult.select(result.getColumns(), Cols.listOf()));
                        if (Memgres.logAllStatements) LOG.info("[PROTO] Describe Portal → RowDesc (CALL OUT, {} cols) {}", result.getColumns().size(), sqlSnip);
                        return new DescribePortalResult(true, result);
                    }
                } catch (Exception e) {
                    LOG.debug("[PROTO] Describe Portal CALL exec failed: {} | {}", e.getMessage(), sqlSnip);
                }
            }
            sendNoData(ctx);
            return new DescribePortalResult(false, null);
        }

        // Note on policy: unlike describeStatement (which probes with $N params replaced by
        // NULL, so a probe failure can be spurious/unrepresentative of the real bound values —
        // that mismatch is exactly Bug 3's NPE-swallowed-to-NoData scenario), describePortal
        // probes by executing the SQL with the *real* bound parameter values already known from
        // Bind. So if this probe fails, the subsequent real Execute (same SQL, same real values)
        // will fail identically and surface the actual ErrorResponse there — falling back to
        // NoData here is safe (no row-vs-no-row mismatch can occur) and, empirically, throwing
        // here instead caused a regression: it discards the pipelined Execute message, which
        // otherwise doubles as the autocommit-safe recovery for the transient FAILED status this
        // probe leaves behind (see Session.execute()'s IN_TRANSACTION -> FAILED transition).
        if (isSafeToDescribe(sql)) {
            Session.TransactionStatus savedStatusPortal = session.getStatus();
            try {
                QueryResult result = session.execute(sql, paramValues);
                if (result.getType() == QueryResult.Type.SELECT || !result.getColumns().isEmpty()) {
                    sendRowDescription(ctx, result);
                    if (Memgres.logAllStatements) LOG.info("[PROTO] Describe Portal → RowDesc ({} cols, {} rows) {}",
                            result.getColumns().size(), result.getRows().size(), sqlSnip);
                    return new DescribePortalResult(true, result);
                }
            } catch (Exception | StackOverflowError e) {
                LOG.warn("[PROTO] Describe Portal FAILED: {} | {}", e.getMessage(), sqlSnip);
                LOG.debug("Full exception:", e);
                session.restoreStatus(savedStatusPortal);
                String lockState = lockWaitSqlState(e);
                if (lockState != null) {
                    throw new DescribeExecutionFailedException(lockState, e.getMessage());
                }
                // Fallback: re-execute with LIMIT 0 to get column metadata without side effects
                String upper2 = stripLeadingComments(sql).toUpperCase();
                if (upper2.startsWith("SELECT")) {
                    Session.TransactionStatus savedFallback = session.getStatus();
                    try {
                        String metaSql = sql.replaceAll(";\\s*$", "").trim()
                                .replaceAll("--[^\\n]*$", "").trim();
                        metaSql = metaSql.replaceAll("(?i)\\bFOR\\s+(UPDATE|NO\\s+KEY\\s+UPDATE|SHARE|KEY\\s+SHARE)(\\s+(SKIP\\s+LOCKED|NOWAIT|OF\\s+\\w+))*", "").trim();
                        if (!alreadyBounded(metaSql)) metaSql = metaSql + " LIMIT 0";
                        QueryResult metaResult = session.execute(metaSql, paramValues);
                        if (metaResult.getType() == QueryResult.Type.SELECT || !metaResult.getColumns().isEmpty()) {
                            sendRowDescription(ctx, metaResult);
                            if (Memgres.logAllStatements) LOG.info("[PROTO] Describe Portal → RowDesc (fallback LIMIT 0, {} cols) {}", metaResult.getColumns().size(), sqlSnip);
                            return new DescribePortalResult(true, null);
                        }
                    } catch (Exception fallbackEx) {
                        LOG.debug("[PROTO] Describe Portal fallback also failed: {}", fallbackEx.getMessage());
                        session.restoreStatus(savedFallback);
                    }
                }
            }
        }

        List<Column> cols = inferColumns(sql);
        if (cols != null) {
            sendRowDescription(ctx, QueryResult.select(cols, Cols.listOf()));
            if (Memgres.logAllStatements) LOG.info("[PROTO] Describe Portal → RowDesc (infer, {} cols) {}", cols.size(), sqlSnip);
            return new DescribePortalResult(true, null);
        }

        if (Memgres.logAllStatements) LOG.info("[PROTO] Describe Portal → NoData: {}", sqlSnip);
        sendNoData(ctx);
        return new DescribePortalResult(false, null);
    }

    /** Result from describePortal: whether RowDescription was sent, and the cached result (if query was executed). */
        public static final class DescribePortalResult {
        public final boolean rowDescSent;
        public final QueryResult cachedResult;

        public DescribePortalResult(boolean rowDescSent, QueryResult cachedResult) {
            this.rowDescSent = rowDescSent;
            this.cachedResult = cachedResult;
        }

        public boolean rowDescSent() { return rowDescSent; }
        public QueryResult cachedResult() { return cachedResult; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DescribePortalResult that = (DescribePortalResult) o;
            return rowDescSent == that.rowDescSent
                && java.util.Objects.equals(cachedResult, that.cachedResult);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(rowDescSent, cachedResult);
        }

        @Override
        public String toString() {
            return "DescribePortalResult[rowDescSent=" + rowDescSent + ", " + "cachedResult=" + cachedResult + "]";
        }
    }

    // ---- SQL classification helpers ----

    boolean isSafeToDescribe(String sql) {
        String upper = stripLeadingComments(sql).toUpperCase();
        if (upper.startsWith("INSERT") || upper.startsWith("UPDATE")
                || upper.startsWith("DELETE") || upper.startsWith("MERGE")) return false;
        if (upper.startsWith("FETCH") || upper.startsWith("EXECUTE")
                || upper.startsWith("MOVE")) return false;
        if (upper.startsWith("SELECT") && isSelectInto(upper)) return false;
        return isQueryStatement(sql);
    }

    static boolean isSelectInto(String upper) {
        int depth = 0;
        boolean inString = false;
        for (int i = 0; i < upper.length() - 4; i++) {
            char c = upper.charAt(i);
            if (c == '\'') { inString = !inString; continue; }
            if (inString) continue;
            if (c == '(') { depth++; continue; }
            if (c == ')') { if (depth > 0) depth--; continue; }
            if (depth > 0) continue;
            if (upper.startsWith(" INTO ", i) || upper.startsWith("\tINTO ", i) || upper.startsWith("\nINTO ", i)) {
                return true;
            }
        }
        return false;
    }

    boolean isQueryStatement(String sql) {
        String upper = stripLeadingComments(sql).toUpperCase();
        if (upper.startsWith("SELECT") || upper.startsWith("VALUES") || upper.startsWith("COPY")
                || upper.startsWith("EXPLAIN") || upper.startsWith("(")
                || upper.startsWith("SHOW") || upper.startsWith("TABLE")) {
            return true;
        }
        if (upper.startsWith("WITH")) return isWithSelect(upper);
        if (upper.contains("RETURNING") && (upper.startsWith("INSERT")
                || upper.startsWith("UPDATE") || upper.startsWith("DELETE"))) {
            return true;
        }
        return false;
    }

    /**
     * Scans a (already-uppercased) "WITH ..." statement to decide whether its main
     * statement is a SELECT (row-returning) or a DML statement (INSERT/UPDATE/DELETE).
     * Must skip over string/dollar-quoted literals and comments: a paren or DML keyword
     * appearing inside a literal (e.g. {@code WITH x AS (SELECT ') DELETE' AS s) SELECT ...})
     * must never desync the paren-depth tracking or be mistaken for a real keyword.
     */
    boolean isWithSelect(String upper) {
        int i = 4;
        int len = upper.length();
        while (i < len) {
            char c = upper.charAt(i);
            if (c == '(') {
                i = skipBalancedParens(upper, i);
                continue;
            }
            int afterLiteral = skipLiteralOrComment(upper, i);
            if (afterLiteral > i) {
                i = afterLiteral;
                continue;
            }
            if (c == 'S' && matchesKeyword(upper, i, "SELECT")) {
                return true;
            } else if (c == 'I' && matchesKeyword(upper, i, "INSERT")) {
                return false;
            } else if (c == 'U' && matchesKeyword(upper, i, "UPDATE")) {
                return false;
            } else if (c == 'D' && matchesKeyword(upper, i, "DELETE")) {
                return false;
            } else {
                i++;
            }
        }
        return true;
    }

    /**
     * Skips a balanced {@code (...)} group starting at {@code upper.charAt(i) == '('},
     * honoring string/dollar-quoted literals and comments nested inside so their content
     * (parens, keywords) never affects the paren-depth count. Returns the index just past
     * the matching {@code ')'} (or {@code len} if unterminated).
     */
    private static int skipBalancedParens(String upper, int i) {
        int len = upper.length();
        int depth = 1;
        i++;
        while (i < len && depth > 0) {
            char c = upper.charAt(i);
            if (c == '(') {
                depth++;
                i++;
                continue;
            }
            if (c == ')') {
                depth--;
                i++;
                continue;
            }
            int afterLiteral = skipLiteralOrComment(upper, i);
            if (afterLiteral > i) {
                i = afterLiteral;
                continue;
            }
            i++;
        }
        return i;
    }

    /**
     * If position {@code i} begins a single-quoted string, a double-quoted identifier, a
     * dollar-quoted string ({@code $$...$$} or {@code $tag$...$tag$}), a line comment
     * ({@code --}), or a block comment ({@code /* ... * /}), returns the index just past its
     * end. Otherwise returns {@code i} unchanged (nothing to skip at this position).
     */
    private static int skipLiteralOrComment(String upper, int i) {
        int len = upper.length();
        char c = upper.charAt(i);
        if (c == '\'' || c == '"') {
            int j = i + 1;
            while (j < len) {
                if (upper.charAt(j) == c) {
                    if (j + 1 < len && upper.charAt(j + 1) == c) {
                        j += 2;
                        continue;
                    }
                    return j + 1;
                }
                j++;
            }
            return len; // unterminated literal: consume to end rather than desync further
        }
        if (c == '-' && i + 1 < len && upper.charAt(i + 1) == '-') {
            int j = i + 2;
            while (j < len && upper.charAt(j) != '\n') j++;
            return j;
        }
        if (c == '/' && i + 1 < len && upper.charAt(i + 1) == '*') {
            int j = i + 2;
            int depth = 1;
            while (j + 1 < len && depth > 0) {
                if (upper.charAt(j) == '/' && upper.charAt(j + 1) == '*') {
                    depth++;
                    j += 2;
                } else if (upper.charAt(j) == '*' && upper.charAt(j + 1) == '/') {
                    depth--;
                    j += 2;
                } else {
                    j++;
                }
            }
            return j;
        }
        if (c == '$') {
            int tagEnd = matchDollarQuoteTagEnd(upper, i);
            if (tagEnd >= 0) {
                String tag = upper.substring(i, tagEnd);
                int closeIdx = upper.indexOf(tag, tagEnd);
                return closeIdx >= 0 ? closeIdx + tag.length() : len;
            }
        }
        return i;
    }

    /**
     * If position {@code i} begins a dollar-quote opening tag ({@code $$} or {@code $tag$}),
     * returns the index just past the closing {@code $} of the tag; otherwise returns -1.
     */
    private static int matchDollarQuoteTagEnd(String upper, int i) {
        int len = upper.length();
        int j = i + 1;
        while (j < len && (Character.isLetterOrDigit(upper.charAt(j)) || upper.charAt(j) == '_')) j++;
        if (j < len && upper.charAt(j) == '$') return j + 1;
        return -1;
    }

    private static boolean matchesKeyword(String upper, int i, String keyword) {
        int len = upper.length();
        int kwLen = keyword.length();
        return i + kwLen <= len && upper.regionMatches(i, keyword, 0, kwLen)
                && (i + kwLen >= len || !Character.isLetterOrDigit(upper.charAt(i + kwLen)));
    }

    // ---- CALL OUT param inference ----

    /**
     * For CALL statements, look up the procedure and check for OUT/INOUT params.
     * Returns column metadata if the procedure has OUT params, null otherwise.
     */
    private List<Column> inferCallOutColumns(String sql) {
        try {
            String stripped = stripLeadingComments(sql).trim();
            // Extract procedure name from "CALL proc_name(...)"
            String afterCall = stripped.substring(4).trim(); // skip "CALL"
            int parenIdx = afterCall.indexOf('(');
            if (parenIdx < 0) return null;
            String procName = afterCall.substring(0, parenIdx).trim().toLowerCase();
            // Look up the function/procedure
            PgFunction func;
            if (procName.contains(".")) {
                String[] parts = procName.split("\\.", 2);
                func = database.getFunction(parts[0], parts[1]);
            } else {
                func = database.getFunction(procName);
            }
            if (func == null || !func.isProcedure()) return null;
            // Collect OUT/INOUT params
            List<Column> outCols = new ArrayList<>();
            for (PgFunction.Param p : func.getParams()) {
                String mode = p.mode() != null ? p.mode().toUpperCase() : "IN";
                if ("OUT".equals(mode) || "INOUT".equals(mode)) {
                    String colName = p.name() != null ? p.name() : "column" + (outCols.size() + 1);
                    outCols.add(new Column(colName, DataType.TEXT, true, false, null));
                }
            }
            return outCols.isEmpty() ? null : outCols;
        } catch (Exception e) {
            LOG.debug("Failed to infer CALL OUT columns: {}", e.getMessage());
            return null;
        }
    }

    // ---- Column inference ----

    List<Column> inferColumns(String sql) {
        String upper = stripLeadingComments(sql).toUpperCase();
        if (upper.startsWith("FETCH")) {
            String cursorName = extractCursorName(sql);
            if (cursorName != null) {
                Session.CursorState cursor = session.getCursor(cursorName);
                if (cursor != null) return cursor.getColumns();
            }
        }
        if (upper.startsWith("EXECUTE")) {
            String planName = extractPlanName(sql);
            if (planName != null) {
                Session.PreparedStmt plan = session.getPreparedStatement(planName);
                if (plan != null && plan.body() != null) {
                    boolean isSelect = plan.body() instanceof com.memgres.engine.parser.ast.SelectStmt
                            || plan.body() instanceof com.memgres.engine.parser.ast.SetOpStmt;
                    boolean hasDmlReturning = false;
                    if (plan.body() instanceof com.memgres.engine.parser.ast.InsertStmt) {
                        com.memgres.engine.parser.ast.InsertStmt ins = (com.memgres.engine.parser.ast.InsertStmt) plan.body();
                        hasDmlReturning = ins.returning() != null && !ins.returning().isEmpty();
                    } else if (plan.body() instanceof com.memgres.engine.parser.ast.UpdateStmt) {
                        com.memgres.engine.parser.ast.UpdateStmt upd = (com.memgres.engine.parser.ast.UpdateStmt) plan.body();
                        hasDmlReturning = upd.returning() != null && !upd.returning().isEmpty();
                    } else if (plan.body() instanceof com.memgres.engine.parser.ast.DeleteStmt) {
                        com.memgres.engine.parser.ast.DeleteStmt del = (com.memgres.engine.parser.ast.DeleteStmt) plan.body();
                        hasDmlReturning = del.returning() != null && !del.returning().isEmpty();
                    }
                    if (isSelect) {
                        try {
                            QueryResult result = session.execute(sql, Cols.listOf());
                            if (!result.getColumns().isEmpty()) return result.getColumns();
                        } catch (Exception e) { /* fall through */ }
                    } else if (hasDmlReturning) {
                        return inferDmlReturningFromPlan(plan);
                    }
                }
            }
        }
        return null;
    }

    private List<Column> inferDmlReturningFromPlan(Session.PreparedStmt plan) {
        String tblName = null;
        List<com.memgres.engine.parser.ast.SelectStmt.SelectTarget> retTargets = null;
        if (plan.body() instanceof com.memgres.engine.parser.ast.InsertStmt) {
            com.memgres.engine.parser.ast.InsertStmt ins = (com.memgres.engine.parser.ast.InsertStmt) plan.body();
            tblName = ins.table(); retTargets = ins.returning();
        } else if (plan.body() instanceof com.memgres.engine.parser.ast.UpdateStmt) {
            com.memgres.engine.parser.ast.UpdateStmt upd = (com.memgres.engine.parser.ast.UpdateStmt) plan.body();
            tblName = upd.table(); retTargets = upd.returning();
        } else if (plan.body() instanceof com.memgres.engine.parser.ast.DeleteStmt) {
            com.memgres.engine.parser.ast.DeleteStmt del = (com.memgres.engine.parser.ast.DeleteStmt) plan.body();
            tblName = del.table(); retTargets = del.returning();
        }
        if (tblName == null) return null;
        try {
            for (Schema schema : session.getDatabase().getSchemas().values()) {
                Table tbl = schema.getTable(tblName);
                if (tbl != null) {
                    boolean isStar = retTargets != null && retTargets.stream()
                            .anyMatch(t -> t.expr() instanceof com.memgres.engine.parser.ast.WildcardExpr);
                    if (isStar || retTargets == null) return tbl.getColumns();
                    List<Column> cols = new ArrayList<>();
                    for (com.memgres.engine.parser.ast.SelectStmt.SelectTarget t : retTargets) {
                        String colName = t.alias();
                        if (colName == null && t.expr() instanceof com.memgres.engine.parser.ast.ColumnRef) {
                            com.memgres.engine.parser.ast.ColumnRef cr = (com.memgres.engine.parser.ast.ColumnRef) t.expr();
                            colName = cr.column();
                        }
                        if (colName == null) colName = "?column?";
                        int ci = tbl.getColumnIndex(colName);
                        if (ci >= 0) cols.add(tbl.getColumns().get(ci));
                        else cols.add(new Column(colName, DataType.TEXT, true, false, null));
                    }
                    return cols;
                }
            }
        } catch (Exception e) { /* fall through */ }
        return null;
    }

    List<Column> inferReturningColumns(String sql) {
        String upper = stripLeadingComments(sql).toUpperCase();
        String tableName = null;
        if (upper.startsWith("INSERT")) {
            int intoIdx = upper.indexOf("INTO");
            if (intoIdx >= 0) {
                String rest = sql.substring(intoIdx + 4).trim();
                int endIdx = rest.indexOf(' ');
                int parenIdx = rest.indexOf('(');
                if (parenIdx >= 0 && (endIdx < 0 || parenIdx < endIdx)) endIdx = parenIdx;
                tableName = endIdx > 0 ? rest.substring(0, endIdx).trim() : rest.trim();
            }
        } else if (upper.startsWith("UPDATE")) {
            String rest = sql.substring(6).trim();
            int endIdx = rest.indexOf(' ');
            tableName = endIdx > 0 ? rest.substring(0, endIdx).trim() : rest.trim();
        } else if (upper.startsWith("DELETE")) {
            int fromIdx = upper.indexOf("FROM");
            if (fromIdx >= 0) {
                String rest = sql.substring(fromIdx + 4).trim();
                int endIdx = rest.indexOf(' ');
                tableName = endIdx > 0 ? rest.substring(0, endIdx).trim() : rest.trim();
            }
        }
        // For MERGE, extract both target and source tables
        Table mergeSourceTable = null;
        if (upper.startsWith("MERGE")) {
            int intoIdx = upper.indexOf("INTO");
            if (intoIdx >= 0) {
                String rest = sql.substring(intoIdx + 4).trim();
                int endIdx = rest.indexOf(' ');
                tableName = endIdx > 0 ? rest.substring(0, endIdx).trim() : rest.trim();
            }
            // Extract source table name from USING clause
            int usingIdx = upper.indexOf("USING");
            if (usingIdx >= 0) {
                String usingRest = sql.substring(usingIdx + 5).trim();
                int endIdx = usingRest.indexOf(' ');
                String srcName = endIdx > 0 ? usingRest.substring(0, endIdx).trim() : usingRest.trim();
                srcName = srcName.replace("\"", "");
                for (Schema schema : database.getSchemas().values()) {
                    mergeSourceTable = schema.getTable(srcName.toLowerCase());
                    if (mergeSourceTable == null) mergeSourceTable = schema.getTable(srcName);
                    if (mergeSourceTable != null) break;
                }
            }
        }
        if (tableName == null) return null;
        tableName = tableName.replace("\"", "");
        Table table = null;
        for (Schema schema : database.getSchemas().values()) {
            table = schema.getTable(tableName.toLowerCase());
            if (table == null) table = schema.getTable(tableName);
            if (table != null) break;
        }
        if (table == null) return null;

        int retIdx = upper.lastIndexOf("RETURNING");
        if (retIdx < 0) return null;
        String retPart = sql.substring(retIdx + "RETURNING".length()).trim();
        if (retPart.equals("*")) {
            if (!upper.startsWith("MERGE")) return new ArrayList<>(table.getColumns());
            // A MERGE's RETURNING reads over the join the statement walks, and it walks the source
            // first: PostgreSQL answers the source's columns and then the target's. A source that
            // is no relation of its own — a VALUES list, a subquery — has no columns to read off
            // the catalogue, so nothing is described from the text and the statement itself says.
            if (mergeSourceTable == null) return null;
            List<Column> cols = new ArrayList<>(mergeSourceTable.getColumns());
            cols.addAll(table.getColumns());
            return cols;
        }
        List<Column> result = new ArrayList<>();
        for (String colExpr : retPart.split(",")) {
            String item = colExpr.trim().replace("\"", "");
            // The alias is the name PostgreSQL puts in the row description: `a AS r2` is labelled
            // r2, not a. Dropping it here and adding the base table's own Column made the wire
            // label disagree with the one the engine builds for the same statement. withName keeps
            // the column's type metadata, which is what the type OID is worked out from.
            String alias = null;
            String colName = item;
            int asIdx = item.toUpperCase().indexOf(" AS ");
            if (asIdx >= 0) {
                alias = item.substring(asIdx + 4).trim();
                colName = item.substring(0, asIdx).trim();
            }
            Column found = null;
            for (Column c : table.getColumns()) {
                if (c.getName().equalsIgnoreCase(colName)) { found = c; break; }
            }
            if (found == null) return null;
            result.add(alias == null || alias.isEmpty() ? found : found.withName(alias));
        }
        return result.isEmpty() ? null : result;
    }

    // ---- Parameter helpers ----

    static int countParameters(String sql) {
        int max = 0;
        boolean inString = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (inString) {
                if (c == '\'' && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') { i++; }
                else if (c == '\'') { inString = false; }
                continue;
            }
            if (c == '\'') { inString = true; continue; }
            if (c == '$' && i + 1 < sql.length() && Character.isDigit(sql.charAt(i + 1))) {
                int j = i + 1;
                while (j < sql.length() && Character.isDigit(sql.charAt(j))) j++;
                // A parameter number a client wrote may be anything at all; reading it without a
                // guard threw out of Describe, and the escape left the connection a response ahead.
                int paramNum = parameterNumber(sql.substring(i + 1, j));
                if (paramNum > max) max = paramNum;
                i = j - 1;
            }
        }
        return max;
    }

    /** The number a {@code $N} placeholder names, or 0 when it names none this protocol can carry. */
    private static int parameterNumber(String digits) {
        try {
            return Integer.parseInt(digits);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    static String replaceParamsWithNull(String sql) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' || c == '"') {
                sb.append(c);
                i++;
                while (i < sql.length()) {
                    sb.append(sql.charAt(i));
                    if (sql.charAt(i) == c) {
                        if (i + 1 < sql.length() && sql.charAt(i + 1) == c) {
                            sb.append(sql.charAt(++i));
                        } else {
                            break;
                        }
                    }
                    i++;
                }
            } else if (c == '$' && i + 1 < sql.length() && Character.isDigit(sql.charAt(i + 1))) {
                int j = i + 1;
                while (j < sql.length() && Character.isDigit(sql.charAt(j))) j++;
                sb.append("NULL");
                i = j - 1;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    // ---- Utility ----

    static String stripLeadingComments(String sql) {
        int i = 0;
        int len = sql.length();
        while (i < len) {
            if (Character.isWhitespace(sql.charAt(i))) { i++; continue; }
            if (i + 1 < len && sql.charAt(i) == '-' && sql.charAt(i + 1) == '-') {
                i += 2;
                while (i < len && sql.charAt(i) != '\n') i++;
                if (i < len) i++;
                continue;
            }
            if (i + 1 < len && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') {
                i += 2;
                int depth = 1;
                while (i + 1 < len && depth > 0) {
                    if (sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') { depth++; i += 2; }
                    else if (sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') { depth--; i += 2; }
                    else i++;
                }
                continue;
            }
            break;
        }
        return i < len ? sql.substring(i).trim() : "";
    }

    /** The words that may stand between FETCH or MOVE and the cursor it names. */
    private static final java.util.Set<String> FETCH_DIRECTIONS =
            new java.util.HashSet<>(java.util.Arrays.asList(
                    "NEXT", "PRIOR", "FIRST", "LAST", "ABSOLUTE", "RELATIVE",
                    "FORWARD", "BACKWARD", "ALL", "FROM", "IN"));

    /**
     * The cursor a FETCH or MOVE names.
     *
     * <p>The name was looked for after a FROM or an IN, so {@code FETCH c} -- where the direction
     * and the word are both left out -- named no cursor at all, and the rows it returned reached
     * the client with no shape to read them by. It was also read out of the upper-cased text with
     * its quotes left on, so {@code FETCH ALL FROM "C"} looked for a cursor spelled with quotes
     * around an upper-cased name and found none.
     */
    private static String extractCursorName(String sql) {
        String rest = stripLeadingComments(sql).trim();
        while (rest.endsWith(";")) rest = rest.substring(0, rest.length() - 1).trim();
        int keyword = endOfWord(rest);
        if (keyword < 0) return null;
        rest = rest.substring(keyword).trim();
        // Everything the statement may say about where to move goes first, and a count is written
        // as a plain number, so what is left when neither is what the cursor is called.
        while (!rest.isEmpty() && rest.charAt(0) != '"') {
            int end = endOfWord(rest);
            String word = end < 0 ? rest : rest.substring(0, end);
            String upper = word.toUpperCase(java.util.Locale.ROOT);
            if (!FETCH_DIRECTIONS.contains(upper) && !upper.matches("[-+]?[0-9]+")) break;
            if (end < 0) return null;
            rest = rest.substring(end).trim();
        }
        if (rest.isEmpty()) return null;
        if (rest.charAt(0) == '"') {
            int close = rest.indexOf('"', 1);
            return close < 0 ? null : rest.substring(1, close);
        }
        int end = endOfWord(rest);
        String name = end < 0 ? rest : rest.substring(0, end);
        return name.isEmpty() ? null : name.toLowerCase(java.util.Locale.ROOT);
    }

    /** Where the first word of {@code text} ends, or -1 when the whole of it is one word. */
    private static int endOfWord(String text) {
        for (int i = 0; i < text.length(); i++) {
            if (Character.isWhitespace(text.charAt(i))) return i;
        }
        return -1;
    }

    /**
     * The prepared statement a name in an EXECUTE refers to.
     *
     * <p>The name was read out of the upper-cased text with its quotes left on, so
     * {@code EXECUTE "p"} looked for a statement called {@code "P"} — quotes and all — found
     * none, and described no columns while the execution still sent rows. The client then had
     * tuples with no shape to read them by.
     */
    private static String extractPlanName(String sql) {
        String rest = sql.substring(sql.toUpperCase(java.util.Locale.ROOT)
                .indexOf("EXECUTE") + "EXECUTE".length()).trim();
        if (rest.startsWith("\"")) {
            int close = rest.indexOf('"', 1);
            if (close < 0) return null;
            String quoted = rest.substring(1, close);
            return quoted.isEmpty() ? null : quoted;
        }
        int endIdx = rest.indexOf('(');
        if (endIdx < 0) endIdx = rest.indexOf(' ');
        if (endIdx < 0) endIdx = rest.indexOf(';');
        String name = endIdx > 0 ? rest.substring(0, endIdx).trim() : rest.replace(";", "").trim();
        return name.isEmpty() ? null : name.toLowerCase(java.util.Locale.ROOT);
    }

    // ---- Wire protocol helpers ----

    private void sendParameterDescription(ChannelHandlerContext ctx, String sql, int[] oids) {
        int numParams = countParameters(sql);
        int[] inferred = inferParamOidsFromCasts(sql, numParams);
        int[] fromContext;
        try {
            fromContext = PgWireParamTypes.infer(sql, numParams, database, session);
        } catch (MemgresException e) {
            // The statement names a call PostgreSQL cannot choose between. That is the answer to
            // a Describe as much as to an Execute, and it is the answer PostgreSQL gives first.
            throw new DescribeExecutionFailedException(sqlStateOf(e), e.getMessage());
        }
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('t');
        buf.writeInt(4 + 2 + numParams * 4);
        buf.writeShort(numParams);
        for (int i = 0; i < numParams; i++) {
            int oid;
            if (oids != null && i < oids.length && oids[i] != 0) {
                oid = oids[i];                       // client-specified type wins
            } else if (inferred[i] != 0) {
                oid = inferred[i];                   // inferred from an enclosing cast context
            } else if (fromContext[i] != 0) {
                oid = fromContext[i];                // resolved from what the statement compares it to
            } else {
                oid = 25;                            // default to TEXT (not 0, which crashes pgjdbc)
            }
            buf.writeInt(oid);
        }
        ctx.write(buf);
    }

    /**
     * Infer parameter type OIDs from an enclosing cast context, e.g. {@code $1::int8},
     * {@code $2::timestamp}, or {@code CAST($3 AS numeric)}. PG reports the cast target type for
     * such parameters; without this, an unspecified parameter falls back to TEXT and tools like
     * pgjdbc's {@code ParameterMetaData.getParameterType} report the wrong type. Returns an array
     * of length {@code numParams} (0 where nothing could be inferred).
     */
    static int[] inferParamOidsFromCasts(String sql, int numParams) {
        int[] result = new int[numParams];
        if (numParams == 0) return result;
        boolean inString = false;
        int len = sql.length();
        for (int i = 0; i < len; i++) {
            char c = sql.charAt(i);
            if (inString) {
                if (c == '\'' && i + 1 < len && sql.charAt(i + 1) == '\'') { i++; }
                else if (c == '\'') { inString = false; }
                continue;
            }
            if (c == '\'') { inString = true; continue; }
            if (c == '$' && i + 1 < len && Character.isDigit(sql.charAt(i + 1))) {
                int j = i + 1;
                while (j < len && Character.isDigit(sql.charAt(j))) j++;
                int paramNum = parameterNumber(sql.substring(i + 1, j));
                int k = j;
                while (k < len && Character.isWhitespace(sql.charAt(k))) k++;
                String typeName = null;
                if (k + 1 < len && sql.charAt(k) == ':' && sql.charAt(k + 1) == ':') {
                    // $N::type
                    k += 2;
                    while (k < len && Character.isWhitespace(sql.charAt(k))) k++;
                    typeName = readTypeName(sql, k);
                } else if (k + 1 < len && (sql.charAt(k) == 'A' || sql.charAt(k) == 'a')
                        && (sql.charAt(k + 1) == 'S' || sql.charAt(k + 1) == 's')
                        && (k + 2 >= len || !Character.isLetterOrDigit(sql.charAt(k + 2)))) {
                    // CAST($N AS type)
                    k += 2;
                    while (k < len && Character.isWhitespace(sql.charAt(k))) k++;
                    typeName = readTypeName(sql, k);
                }
                if (typeName != null && paramNum >= 1 && paramNum <= numParams) {
                    DataType dt = DataType.fromPgName(typeName);
                    if (dt != null) result[paramNum - 1] = dt.getOid();
                }
                i = j - 1;
            }
        }
        return result;
    }

    /**
     * Read a SQL type name starting at {@code pos}: a (possibly multi-word) base name, an optional
     * {@code (p[,s])} precision, and an optional {@code []} array marker. Returns a name suitable
     * for {@link DataType#fromPgName} (precision/array markers stripped, since only the base OID is
     * needed here), or null if no identifier is present.
     */
    private static String readTypeName(String sql, int pos) {
        int len = sql.length();
        int start = pos;
        while (pos < len && (Character.isLetterOrDigit(sql.charAt(pos)) || sql.charAt(pos) == '_' || sql.charAt(pos) == '.')) {
            pos++;
        }
        if (pos == start) return null;
        String base = sql.substring(start, pos);
        // Drop schema qualification (e.g. pg_catalog.int8 -> int8).
        int dot = base.lastIndexOf('.');
        if (dot >= 0) base = base.substring(dot + 1);
        // Greedily absorb multi-word type spellings (double precision, timestamp with time zone, ...).
        StringBuilder multi = new StringBuilder(base);
        int p = pos;
        String best = base;
        for (int words = 0; words < 3; words++) {
            int q = p;
            while (q < len && Character.isWhitespace(sql.charAt(q))) q++;
            int ws = q;
            while (q < len && Character.isLetter(sql.charAt(q))) q++;
            if (q == ws) break;
            multi.append(' ').append(sql.substring(ws, q));
            if (DataType.fromPgName(multi.toString()) != null) {
                best = multi.toString();
                p = q;
            }
        }
        return best;
    }

    private void sendRowDescription(ChannelHandlerContext ctx, QueryResult result) {
        ByteBuf buf = ctx.alloc().buffer();
        PgWireValueFormatter.sendRowDescription(buf, result.getColumns(), session);
        ctx.write(buf);
    }

    static void sendNoData(ChannelHandlerContext ctx) {
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('n');
        buf.writeInt(4);
        ctx.write(buf);
    }
}
