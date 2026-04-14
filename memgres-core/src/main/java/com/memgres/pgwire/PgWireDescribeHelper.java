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
                    if (!nullSql.toUpperCase().contains("LIMIT")) nullSql = nullSql + " LIMIT 0";
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
        if (isSafeToDescribe(sql)) {
            Session.TransactionStatus savedStatus = session.getStatus();
            try {
                String metaSql = countParameters(sql) > 0 ? replaceParamsWithNull(sql) : sql;
                metaSql = metaSql.replaceAll(";\\s*$", "").trim();
                metaSql = metaSql.replaceAll("--[^\\n]*$", "").trim();
                if (!metaSql.toUpperCase().contains("LIMIT")) metaSql = metaSql + " LIMIT 0";
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
            } catch (Exception e) {
                LOG.warn("[PROTO] Describe Portal FAILED: {} | {}", e.getMessage(), sqlSnip);
                LOG.debug("Full exception:", e);
                session.restoreStatus(savedStatusPortal);
                // Fallback for SELECT with FOR UPDATE/SHARE
                String upper2 = stripLeadingComments(sql).toUpperCase();
                if (upper2.startsWith("SELECT") && upper2.contains("FOR ")) {
                    Session.TransactionStatus savedFallback = session.getStatus();
                    try {
                        String metaSql = sql.replaceAll(";\\s*$", "").trim()
                                .replaceAll("--[^\\n]*$", "").trim();
                        metaSql = metaSql.replaceAll("(?i)\\bFOR\\s+(UPDATE|NO\\s+KEY\\s+UPDATE|SHARE|KEY\\s+SHARE)(\\s+(SKIP\\s+LOCKED|NOWAIT|OF\\s+\\w+))*", "").trim();
                        if (!metaSql.toUpperCase().contains("LIMIT")) metaSql = metaSql + " LIMIT 0";
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

    boolean isWithSelect(String upper) {
        int i = 4;
        int len = upper.length();
        while (i < len) {
            char c = upper.charAt(i);
            if (c == '(') {
                int depth = 1;
                i++;
                while (i < len && depth > 0) {
                    if (upper.charAt(i) == '(') depth++;
                    else if (upper.charAt(i) == ')') depth--;
                    i++;
                }
            } else if (c == 'S' && i + 6 <= len && upper.substring(i, i + 6).equals("SELECT")
                    && (i + 6 >= len || !Character.isLetterOrDigit(upper.charAt(i + 6)))) {
                return true;
            } else if (c == 'I' && i + 6 <= len && upper.substring(i, i + 6).equals("INSERT")
                    && (i + 6 >= len || !Character.isLetterOrDigit(upper.charAt(i + 6)))) {
                return false;
            } else if (c == 'U' && i + 6 <= len && upper.substring(i, i + 6).equals("UPDATE")
                    && (i + 6 >= len || !Character.isLetterOrDigit(upper.charAt(i + 6)))) {
                return false;
            } else if (c == 'D' && i + 6 <= len && upper.substring(i, i + 6).equals("DELETE")
                    && (i + 6 >= len || !Character.isLetterOrDigit(upper.charAt(i + 6)))) {
                return false;
            } else {
                i++;
            }
        }
        return true;
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
            String cursorName = extractCursorName(upper);
            if (cursorName != null) {
                Session.CursorState cursor = session.getCursor(cursorName);
                if (cursor != null) return cursor.getColumns();
            }
        }
        if (upper.startsWith("EXECUTE")) {
            String planName = extractPlanName(upper);
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
            List<Column> cols = new ArrayList<>(table.getColumns());
            if (mergeSourceTable != null) {
                cols.addAll(mergeSourceTable.getColumns());
            }
            return cols;
        }
        List<Column> result = new ArrayList<>();
        for (String colExpr : retPart.split(",")) {
            String colName = colExpr.trim().replace("\"", "");
            int asIdx = colName.toUpperCase().indexOf(" AS ");
            if (asIdx >= 0) colName = colName.substring(0, asIdx).trim();
            Column found = null;
            for (Column c : table.getColumns()) {
                if (c.getName().equalsIgnoreCase(colName)) { found = c; break; }
            }
            if (found != null) result.add(found);
            else return null;
        }
        return result.isEmpty() ? null : result;
    }

    // ---- Parameter helpers ----

    static int countParameters(String sql) {
        int max = 0;
        for (int i = 0; i < sql.length() - 1; i++) {
            if (sql.charAt(i) == '$' && Character.isDigit(sql.charAt(i + 1))) {
                int j = i + 1;
                while (j < sql.length() && Character.isDigit(sql.charAt(j))) j++;
                int paramNum = Integer.parseInt(sql.substring(i + 1, j));
                if (paramNum > max) max = paramNum;
                i = j - 1;
            }
        }
        return max;
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

    private static String extractCursorName(String upperSql) {
        int fromIdx = upperSql.indexOf(" FROM ");
        if (fromIdx < 0) fromIdx = upperSql.indexOf(" IN ");
        if (fromIdx < 0) return null;
        String rest = upperSql.substring(fromIdx).trim();
        int spaceIdx = rest.indexOf(' ');
        if (spaceIdx < 0) return null;
        rest = rest.substring(spaceIdx).trim();
        int endIdx = rest.indexOf(' ');
        String name = endIdx > 0 ? rest.substring(0, endIdx) : rest;
        name = name.replace(";", "").trim();
        return name.isEmpty() ? null : name.toLowerCase();
    }

    private static String extractPlanName(String upperSql) {
        String rest = upperSql.substring("EXECUTE".length()).trim();
        int endIdx = rest.indexOf('(');
        if (endIdx < 0) endIdx = rest.indexOf(' ');
        if (endIdx < 0) endIdx = rest.indexOf(';');
        String name = endIdx > 0 ? rest.substring(0, endIdx).trim() : rest.replace(";", "").trim();
        return name.isEmpty() ? null : name.toLowerCase();
    }

    // ---- Wire protocol helpers ----

    private void sendParameterDescription(ChannelHandlerContext ctx, String sql, int[] oids) {
        int numParams = countParameters(sql);
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('t');
        buf.writeInt(4 + 2 + numParams * 4);
        buf.writeShort(numParams);
        for (int i = 0; i < numParams; i++) {
            int oid = (oids != null && i < oids.length && oids[i] != 0) ? oids[i] : 0;
            buf.writeInt(oid);
        }
        ctx.write(buf);
    }

    private void sendRowDescription(ChannelHandlerContext ctx, QueryResult result) {
        ByteBuf buf = ctx.alloc().buffer();
        PgWireValueFormatter.sendRowDescription(buf, result.getColumns());
        ctx.write(buf);
    }

    static void sendNoData(ChannelHandlerContext ctx) {
        ByteBuf buf = ctx.alloc().buffer();
        buf.writeByte('n');
        buf.writeInt(4);
        ctx.write(buf);
    }
}
