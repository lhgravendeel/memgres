package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import com.memgres.engine.util.Strs;

import java.util.*;

/**
 * Handles transactions, EXPLAIN, LISTEN/NOTIFY, roles, and policies.
 * Extracted from DdlExecutor to separate concerns.
 */
class DdlAdminExecutor {
    private final DdlExecutor ddl;
    private final AstExecutor executor;

    DdlAdminExecutor(DdlExecutor ddl) {
        this.ddl = ddl;
        this.executor = ddl.executor;
    }

    // ---- TRANSACTION ----

    /**
     * Statements that only mean something inside a transaction block. PostgreSQL refuses them
     * outside one rather than treating them as a no-op, because a script that reaches
     * {@code SAVEPOINT} or {@code COMMIT AND CHAIN} with no transaction open is not doing what
     * it was written to do.
     */
    private void requireTransactionBlock(String command) {
        if (executor.session != null && !executor.session.isInTransaction()) {
            throw new MemgresException(command + " can only be used in transaction blocks", "25P01");
        }
    }

    QueryResult executeTransaction(TransactionStmt stmt) {
        if (executor.session != null) {
            switch (stmt.action()) {
                case BEGIN: {
                    executor.session.begin();
                    executor.session.setExplicitTransactionBlock(true);
                    if (stmt.isolationLevel() != null) {
                        executor.session.getGucSettings().set("transaction_isolation", stmt.isolationLevel());
                    }
                    if (stmt.readOnly() != null) {
                        executor.session.getGucSettings().set("transaction_read_only", stmt.readOnly() ? "on" : "off");
                    }
                    if (stmt.deferrable() != null) {
                        executor.session.getGucSettings().set("transaction_deferrable", stmt.deferrable() ? "on" : "off");
                    }
                    break;
                }
                case COMMIT: {
                    if (stmt.chain()) requireTransactionBlock("COMMIT AND CHAIN");
                    String savedIso = chainedValue(stmt, "transaction_isolation");
                    String savedRo = chainedValue(stmt, "transaction_read_only");
                    String savedDef = chainedValue(stmt, "transaction_deferrable");
                    executor.session.commit();
                    if (stmt.chain()) {
                        executor.session.begin();
                        executor.session.setExplicitTransactionBlock(true);
                        if (savedIso != null) executor.session.getGucSettings().set("transaction_isolation", savedIso);
                        if (savedRo != null) executor.session.getGucSettings().set("transaction_read_only", savedRo);
                        if (savedDef != null) executor.session.getGucSettings().set("transaction_deferrable", savedDef);
                    }
                    break;
                }
                case ROLLBACK: {
                    if (stmt.chain()) requireTransactionBlock("ROLLBACK AND CHAIN");
                    String savedIso = chainedValue(stmt, "transaction_isolation");
                    String savedRo = chainedValue(stmt, "transaction_read_only");
                    String savedDef = chainedValue(stmt, "transaction_deferrable");
                    executor.session.rollback();
                    if (stmt.chain()) {
                        executor.session.begin();
                        executor.session.setExplicitTransactionBlock(true);
                        if (savedIso != null) executor.session.getGucSettings().set("transaction_isolation", savedIso);
                        if (savedRo != null) executor.session.getGucSettings().set("transaction_read_only", savedRo);
                        if (savedDef != null) executor.session.getGucSettings().set("transaction_deferrable", savedDef);
                    }
                    break;
                }
                case SAVEPOINT:
                    requireTransactionBlock("SAVEPOINT");
                    executor.session.savepoint(stmt.savepointName());
                    break;
                case RELEASE_SAVEPOINT:
                    requireTransactionBlock("RELEASE SAVEPOINT");
                    executor.session.releaseSavepoint(stmt.savepointName());
                    break;
                case ROLLBACK_TO_SAVEPOINT:
                    requireTransactionBlock("ROLLBACK TO SAVEPOINT");
                    executor.session.rollbackToSavepoint(stmt.savepointName());
                    break;
                case PREPARE_TRANSACTION: {
                    // PG default: max_prepared_transactions = 0, which disables PREPARE TRANSACTION
                    int maxPrepared = executor.database.getMaxPreparedTransactions();
                    if (maxPrepared <= 0) {
                        throw new MemgresException("prepared transactions are disabled\n  Hint: Set max_prepared_transactions to a nonzero value.", "55000");
                    }
                    String gid = stmt.savepointName();
                    Database.PreparedTransaction pt = executor.session.prepareTransaction(gid);
                    executor.database.addPreparedTransaction(pt);
                    break;
                }
                case COMMIT_PREPARED: {
                    String gid = stmt.savepointName();
                    Database.PreparedTransaction pt = executor.database.removePreparedTransaction(gid);
                    if (pt == null) {
                        throw new MemgresException("prepared transaction with identifier \"" + gid + "\" does not exist", "42704");
                    }
                    Session.commitPreparedTransaction(pt);
                    break;
                }
                case ROLLBACK_PREPARED: {
                    String gid = stmt.savepointName();
                    Database.PreparedTransaction pt = executor.database.removePreparedTransaction(gid);
                    if (pt == null) {
                        throw new MemgresException("prepared transaction with identifier \"" + gid + "\" does not exist", "42704");
                    }
                    Session.rollbackPreparedTransaction(executor.database, pt);
                    break;
                }
            }
        }
        switch (stmt.action()) {
            case BEGIN:
                return QueryResult.message(QueryResult.Type.BEGIN, "BEGIN");
            case COMMIT:
                return QueryResult.message(QueryResult.Type.COMMIT, "COMMIT");
            case ROLLBACK:
                return QueryResult.message(QueryResult.Type.ROLLBACK, "ROLLBACK");
            case SAVEPOINT:
                return QueryResult.message(QueryResult.Type.SET, "SAVEPOINT");
            case RELEASE_SAVEPOINT:
                return QueryResult.message(QueryResult.Type.SET, "RELEASE");
            case ROLLBACK_TO_SAVEPOINT:
                return QueryResult.message(QueryResult.Type.ROLLBACK, "ROLLBACK");
            case PREPARE_TRANSACTION:
                return QueryResult.message(QueryResult.Type.SET, "PREPARE TRANSACTION");
            case COMMIT_PREPARED:
                return QueryResult.message(QueryResult.Type.COMMIT, "COMMIT PREPARED");
            case ROLLBACK_PREPARED:
                return QueryResult.message(QueryResult.Type.ROLLBACK, "ROLLBACK PREPARED");
            default:
                throw new IllegalStateException("Unknown transaction action: " + stmt.action());
        }
    }

    /**
     * The value AND CHAIN carries into the next transaction: only one this transaction actually
     * set. Carrying a value that was merely inherited from the session default would pin it,
     * so a later change to the default would stop reaching the chained transaction.
     */
    private String chainedValue(TransactionStmt stmt, String setting) {
        if (!stmt.chain()) return null;
        GucSettings gucs = executor.session.getGucSettings();
        return gucs.hasSessionOverride(setting) ? gucs.get(setting) : null;
    }

    // ---- EXPLAIN ----

    QueryResult executeExplain(ExplainStmt stmt) {
        if (stmt.statement() == null) {
            throw new MemgresException("syntax error at end of input", "42601");
        }
        List<String> planLines = new ArrayList<>();
        long startTime = 0;
        QueryResult actualResult = null;

        if (stmt.statement() instanceof SelectStmt && ((SelectStmt) stmt.statement()).from() != null) {
            SelectStmt sel = (SelectStmt) stmt.statement();
            HashSet<String> cteNames = new HashSet<String>();
            if (sel.withClauses() != null) {
                for (SelectStmt.CommonTableExpr cte : sel.withClauses()) {
                    cteNames.add(cte.name().toLowerCase());
                }
            }
            for (SelectStmt.FromItem fromItem : sel.from()) {
                validateFromItemExists(fromItem, cteNames);
            }
        }

        if (stmt.deferredOptionError() != null) {
            String sqlState = stmt.deferredOptionSqlState() != null ? stmt.deferredOptionSqlState() : "22023";
            throw new MemgresException(stmt.deferredOptionError(), sqlState);
        }

        // PG: EXPLAIN (WAL) requires ANALYZE
        if (stmt.wal && !stmt.analyze) {
            throw new MemgresException("EXPLAIN option WAL requires ANALYZE", "22023");
        }

        if (stmt.analyze()) {
            startTime = System.nanoTime();
            actualResult = executor.executeStatement(stmt.statement());
        }

        buildPlanLines(stmt.statement(), planLines, 0, stmt.analyze(), startTime, actualResult, stmt.costs(), stmt.verbose());
        appendExplainExtras(stmt, planLines);

        if (planLines.isEmpty()) {
            planLines.add("Memgres in-memory scan");
        }

        List<Column> planCols = Cols.listOf(new Column("QUERY PLAN", DataType.TEXT, true, false, null));
        if (stmt.format().equals("JSON")) {
            StringBuilder json = new StringBuilder("[\n  {\n    \"Plan\": {\n");
            json.append("      \"Node Type\": \"").append(planLines.get(0).trim()).append("\"\n");
            json.append("    }\n  }\n]");
            return QueryResult.select(planCols, Collections.singletonList(new Object[]{json.toString()}));
        }
        if (stmt.format().equals("XML")) {
            StringBuilder xml = new StringBuilder("<explain xmlns=\"http://www.postgresql.org/2009/explain\">\n");
            xml.append("  <Query>\n    <Plan>\n      <Node-Type>").append(planLines.get(0).trim())
                    .append("</Node-Type>\n    </Plan>\n  </Query>\n</explain>");
            return QueryResult.select(planCols, Collections.singletonList(new Object[]{xml.toString()}));
        }
        if (stmt.format().equals("YAML")) {
            StringBuilder yaml = new StringBuilder("- Plan:\n");
            yaml.append("    Node Type: \"").append(planLines.get(0).trim()).append("\"\n");
            return QueryResult.select(planCols, Collections.singletonList(new Object[]{yaml.toString()}));
        }

        List<Column> cols = Cols.listOf(new Column("QUERY PLAN", DataType.TEXT, true, false, null));
        List<Object[]> rows = new ArrayList<>();
        if (!stmt.costs() && !stmt.analyze()) {
            rows.add(new Object[]{String.join("\n", planLines)});
        } else {
            for (String line : planLines) {
                rows.add(new Object[]{line});
            }
        }
        return QueryResult.select(cols, rows);
    }

    private void validateFromItemExists(SelectStmt.FromItem fromItem, Set<String> cteNames) {
        if (fromItem instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef tr = (SelectStmt.TableRef) fromItem;
            if (tr.schema() == null && cteNames.contains(tr.table().toLowerCase())) return;
            String schema = tr.schema() != null ? tr.schema() : executor.defaultSchema();
            try {
                executor.resolveTable(schema, tr.table());
            } catch (MemgresException e) {
                throw new MemgresException("relation \"" + tr.table() + "\" does not exist", "42P01");
            }
        } else if (fromItem instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom jf = (SelectStmt.JoinFrom) fromItem;
            validateFromItemExists(jf.left(), cteNames);
            validateFromItemExists(jf.right(), cteNames);
        }
    }

    private void buildPlanLines(Statement stmt, List<String> lines, int indent, boolean analyze,
                                 long startTime, QueryResult actualResult, boolean costs, boolean verbose) {
        String prefix = Strs.repeat("  ", indent);
        String arrow = indent > 0 ? "->  " : "";

        if (stmt instanceof SelectStmt) {
            SelectStmt sel = (SelectStmt) stmt;
            if (sel.from() == null || sel.from().isEmpty()) {
                String resultLine = prefix + arrow + "Result";
                if (costs || analyze) {
                    resultLine += "  (cost=0.00..0.01 rows=1 width=4)";
                }
                lines.add(resultLine);
                if (verbose) {
                    lines.add(prefix + "  Output: (...)");
                }
            } else {
                boolean hasJoin = sel.from().stream().anyMatch(f -> f instanceof SelectStmt.JoinFrom);
                if (hasJoin) {
                    lines.add(prefix + arrow + "Nested Loop");
                } else if (sel.groupBy() != null && !sel.groupBy().isEmpty()) {
                    lines.add(prefix + arrow + "HashAggregate");
                } else {
                    String tableName = sel.from().get(0) instanceof SelectStmt.TableRef ? ((SelectStmt.TableRef) sel.from().get(0)).table() : "subquery";
                    int rowCount = 0;
                    try {
                        Table t = executor.resolveTable("public", tableName);
                        rowCount = t.getRows().size();
                    } catch (Exception e) { /* ignore */ }
                    String scanLine = prefix + arrow + "Seq Scan on " + tableName;
                    if (analyze && actualResult != null) {
                        double elapsed = (System.nanoTime() - startTime) / 1_000_000.0;
                        int actualRows = actualResult.getRows().size();
                        scanLine += String.format("  (cost=0.00..1.%02d rows=%d width=0) (actual time=%.3f..%.3f rows=%d loops=1)",
                                rowCount, rowCount, elapsed, elapsed, actualRows);
                    } else if (costs) {
                        scanLine += "  (cost=0.00..1.0" + String.format("%02d", rowCount) + " rows=" + rowCount + " width=0)";
                    }
                    lines.add(scanLine);
                }
                if (verbose) {
                    // Output columns line for EXPLAIN VERBOSE
                    if (sel.targets != null && !sel.targets.isEmpty()) {
                        StringBuilder outputCols = new StringBuilder(prefix + "  Output: ");
                        for (int i = 0; i < sel.targets.size(); i++) {
                            if (i > 0) outputCols.append(", ");
                            SelectStmt.SelectTarget st = sel.targets.get(i);
                            if (st.alias() != null) {
                                outputCols.append(st.alias());
                            } else {
                                outputCols.append(SqlUnparser.exprToSql(st.expr()));
                            }
                        }
                        lines.add(outputCols.toString());
                    } else {
                        lines.add(prefix + "  Output: (...)");
                    }
                }
                if (sel.where() != null) {
                    lines.add(prefix + "  Filter: (...)");
                    if (analyze && actualResult != null) {
                        lines.add(prefix + "  Rows Removed by Filter: 0");
                    }
                }
                if (analyze && !verbose) {
                    lines.add(prefix + "  Output: (...)");
                }
            }
            if (sel.orderBy() != null && !sel.orderBy().isEmpty()) {
                if (analyze && actualResult != null) {
                    double elapsed = (System.nanoTime() - startTime) / 1_000_000.0;
                    lines.add(prefix + "Sort");
                    lines.add(prefix + "  Sort Key: (...)");
                    lines.add(prefix + String.format("  Sort Method: quicksort  (actual time=%.3f..%.3f rows=%d loops=1)",
                            elapsed, elapsed, actualResult.getRows().size()));
                } else {
                    lines.add(prefix + "  Sort Key: (...)");
                }
            }
            if (sel.limit() != null) {
                lines.add(prefix + "  Limit: (...)");
            }
        } else if (stmt instanceof InsertStmt) {
            InsertStmt ins = (InsertStmt) stmt;
            lines.add(prefix + arrow + "Insert on " + ins.table());
        } else if (stmt instanceof UpdateStmt) {
            UpdateStmt upd = (UpdateStmt) stmt;
            lines.add(prefix + arrow + "Update on " + upd.table());
        } else if (stmt instanceof DeleteStmt) {
            DeleteStmt del = (DeleteStmt) stmt;
            lines.add(prefix + arrow + "Delete on " + del.table());
        } else {
            lines.add(prefix + arrow + "Memgres in-memory operation");
        }

        if (analyze && actualResult != null) {
            double elapsed = (System.nanoTime() - startTime) / 1_000_000.0;
            lines.add(String.format("Planning Time: %.3f ms", elapsed * 0.1));
            lines.add(String.format("Execution Time: %.3f ms", elapsed));
        }
    }

    /** Append stub lines for PG 16+/17+ EXPLAIN options (MEMORY, SERIALIZE, BUFFERS, WAL). */
    private void appendExplainExtras(ExplainStmt stmt, List<String> planLines) {
        if (stmt.buffers && stmt.analyze) {
            planLines.add("Buffers: shared hit=0");
        }
        if (stmt.wal) {
            planLines.add("WAL: records=0 fpi=0 bytes=0");
        }
        if (stmt.memory()) {
            planLines.add("Memory: used=0kB  allocated=0kB");
        }
        if (stmt.serialize()) {
            planLines.add("Serialization: output=0kB  format=text");
        }
        if (stmt.settings) {
            planLines.add("Settings: (none)");
        }
    }

    // ---- LISTEN / NOTIFY / UNLISTEN ----

    QueryResult executeListen(ListenStmt stmt) {
        if (executor.session != null) {
            executor.database.getNotificationManager().listen(executor.session, stmt.channel());
        }
        return QueryResult.message(QueryResult.Type.SET, "LISTEN");
    }

    QueryResult executeNotify(NotifyStmt stmt) {
        // The statement form is bounded the same way pg_notify is
        if (stmt.channel() == null || stmt.channel().trim().isEmpty()) {
            throw new MemgresException("channel name cannot be empty", "22023");
        }
        if (stmt.payload() != null && stmt.payload().length() >= 8000) {
            throw new MemgresException("payload string too long", "22023");
        }
        if (executor.session != null) {
            executor.session.queueNotification(stmt.channel(), stmt.payload());
        } else {
            executor.database.getNotificationManager().notify(stmt.channel(), stmt.payload(), 0);
        }
        return QueryResult.message(QueryResult.Type.SET, "NOTIFY");
    }

    QueryResult executeUnlisten(UnlistenStmt stmt) {
        if (executor.session != null) {
            if (stmt.channel() == null) {
                executor.database.getNotificationManager().unlistenAll(executor.session);
            } else {
                executor.database.getNotificationManager().unlisten(executor.session, stmt.channel());
            }
        }
        return QueryResult.message(QueryResult.Type.SET, "UNLISTEN");
    }

    // ---- CREATE POLICY ----

    QueryResult executeCreatePolicy(CreatePolicyStmt stmt) {
        // A view has no row security, so a policy on one would be recorded and never applied —
        // and a reader of the schema would conclude the view is protected when it is not.
        if (executor.database.hasView(stmt.table())) {
            throw PgErrors.wrongObjectType("\"" + stmt.table() + "\" is not a table");
        }
        Table table = executor.resolveTable("public", stmt.table());
        for (RlsPolicy existing : table.getRlsPolicies()) {
            if (existing.getName().equalsIgnoreCase(stmt.name())) {
                throw new MemgresException("policy \"" + stmt.name() + "\" for table \""
                        + stmt.table() + "\" already exists", "42710");
            }
        }
        if (stmt.roles() != null) {
            for (String role : stmt.roles()) {
                if (role.equalsIgnoreCase("public") || role.equalsIgnoreCase("current_user")
                        || role.equalsIgnoreCase("session_user") || role.equalsIgnoreCase("current_role")) {
                    continue;
                }
                if (!executor.database.hasRole(role)) {
                    throw PgErrors.undefinedObject("role", role);
                }
            }
        }
        // StoredExprCheck names the same context and reaches the boolean check itself, after the
        // aggregate one PostgreSQL raises first.
        StoredExprCheck check = StoredExprCheck.forPolicy(table);
        check.check(stmt.usingExpr(), executor.selectExecutor);
        check.check(stmt.withCheckExpr(), executor.selectExecutor);
        table.addRlsPolicy(new RlsPolicy(stmt.name(), stmt.command(),
                stmt.usingExpr(), stmt.withCheckExpr(), stmt.roles(), stmt.policyType()));
        return QueryResult.message(QueryResult.Type.SET, "CREATE POLICY");
    }

    // ---- ALTER POLICY ----

    /** PUBLIC names every role at once rather than one that has to be found in the catalog. */
    private static final String PUBLIC_ROLE = "public";

    QueryResult executeAlterPolicy(AlterPolicyStmt stmt) {
        Table table = executor.resolveTable("public", stmt.table());
        RlsPolicy found = null;
        for (RlsPolicy p : table.getRlsPolicies()) {
            if (p.getName().equalsIgnoreCase(stmt.name())) { found = p; break; }
        }
        if (found == null) {
            throw new MemgresException("policy \"" + stmt.name() + "\" for table \"" + stmt.table() + "\" does not exist", "42704");
        }
        // Which clause a policy may carry follows from the command it guards, and ALTER cannot
        // change that command — so a clause the command has no use for is refused here too. A
        // SELECT or DELETE creates no row to check; an INSERT has no existing row to test.
        String policyCommand = found.getCommand() == null ? "ALL" : found.getCommand().toUpperCase();
        if (stmt.withCheckExpr() != null
                && ("SELECT".equals(policyCommand) || "DELETE".equals(policyCommand))) {
            throw PgErrors.syntax("only USING expression allowed for SELECT, DELETE");
        }
        if (stmt.usingExpr() != null && "INSERT".equals(policyCommand)) {
            throw PgErrors.syntax("only WITH CHECK expression allowed for INSERT");
        }
        // Every role a TO clause names has to exist. PostgreSQL checks them before it changes
        // anything, so a policy is never left applying to a role that is not there.
        for (String role : stmt.roles()) {
            if (PUBLIC_ROLE.equalsIgnoreCase(role)) continue;
            String resolved = executor.ddlExecutor.resolveOwnerName(role);
            if (!executor.database.hasRole(resolved)) {
                throw new MemgresException("role \"" + resolved + "\" does not exist", "42704");
            }
        }
        if (stmt.renameTo() != null) {
            // Renaming onto a name another policy on this table already answers to would leave
            // two of them with one name.
            for (RlsPolicy p : table.getRlsPolicies()) {
                if (p.getName().equalsIgnoreCase(stmt.renameTo())
                        && !p.getName().equalsIgnoreCase(stmt.name())) {
                    throw new MemgresException("policy \"" + stmt.renameTo() + "\" for table \""
                            + stmt.table() + "\" already exists", "42710");
                }
            }
            for (int i = 0; i < table.getRlsPolicies().size(); i++) {
                RlsPolicy p = table.getRlsPolicies().get(i);
                if (p.getName().equalsIgnoreCase(stmt.name())) {
                    table.getRlsPolicies().set(i, new RlsPolicy(stmt.renameTo(), p.getCommand(),
                            p.getUsingExpr(), p.getWithCheckExpr(), p.getRoles()));
                    break;
                }
            }
        } else {
            for (int i = 0; i < table.getRlsPolicies().size(); i++) {
                RlsPolicy p = table.getRlsPolicies().get(i);
                if (p.getName().equalsIgnoreCase(stmt.name())) {
                    Expression using = stmt.usingExpr() != null ? stmt.usingExpr() : p.getUsingExpr();
                    Expression withCheck = stmt.withCheckExpr() != null ? stmt.withCheckExpr() : p.getWithCheckExpr();
                    table.getRlsPolicies().set(i, new RlsPolicy(p.getName(), p.getCommand(), using, withCheck, p.getRoles()));
                    break;
                }
            }
        }
        return QueryResult.message(QueryResult.Type.SET, "ALTER POLICY");
    }

    // ---- CREATE ROLE ----

    QueryResult executeCreateRole(CreateRoleStmt stmt) {
        if (executor.database.hasRole(stmt.name())) {
            throw new MemgresException("role \"" + stmt.name() + "\" already exists", "42710");
        }
        executor.database.createRole(stmt.name(), stmt.options());
        // M12: Process IN ROLE clause — add this role as member of specified roles
        if (stmt.inRoles() != null) {
            for (String parentRole : stmt.inRoles()) {
                executor.database.addRoleMembership(parentRole, stmt.name(), false);
            }
        }
        return QueryResult.message(QueryResult.Type.SET, stmt.isUser() ? "CREATE ROLE" : "CREATE ROLE");
    }

    // ---- ALTER ROLE ----

    QueryResult executeAlterRole(AlterRoleStmt stmt) {
        String roleName = stmt.name();
        if (!roleName.equalsIgnoreCase("current_user") && !roleName.equalsIgnoreCase("session_user")
                && !roleName.equalsIgnoreCase("all") && !executor.database.hasRole(roleName)) {
            throw new MemgresException("role \"" + roleName + "\" does not exist", "42704");
        }
        if (stmt.renameTo() != null) {
            // Renaming onto a role that already exists would merge two roles into one and lose
            // whichever set of privileges was written second.
            if (executor.database.hasRole(stmt.renameTo())) {
                throw new MemgresException("role \"" + stmt.renameTo() + "\" already exists", "42710");
            }
            Map<String, String> attrs = executor.database.getRole(stmt.name());
            if (attrs != null) {
                executor.database.removeRole(stmt.name());
                executor.database.createRole(stmt.renameTo(), attrs);
            }
        } else if (!stmt.options().isEmpty()) {
            Map<String, String> existing = executor.database.getRole(stmt.name());
            if (existing != null) {
                // Handle SET_CONFIG specially: append to ROLCONFIG list
                String setConfig = stmt.options().get("SET_CONFIG");
                if (setConfig != null) {
                    String prev = existing.get("ROLCONFIG");
                    if (prev != null && !prev.isEmpty()) {
                        existing.put("ROLCONFIG", prev + "," + setConfig);
                    } else {
                        existing.put("ROLCONFIG", setConfig);
                    }
                }
                // Apply other options normally
                for (Map.Entry<String, String> e : stmt.options().entrySet()) {
                    if (!"SET_CONFIG".equals(e.getKey())) {
                        existing.put(e.getKey(), e.getValue());
                    }
                }
            }
        }
        return QueryResult.message(QueryResult.Type.SET, "ALTER ROLE");
    }

    // ---- DROP ROLE ----

    QueryResult executeDropRole(DropRoleStmt stmt) {
        if (!stmt.ifExists() && !executor.database.hasRole(stmt.name())) {
            throw new MemgresException("role \"" + stmt.name() + "\" does not exist", "42704");
        }
        if (stmt.ifExists() && !executor.database.hasRole(stmt.name())) {
            return QueryResult.message(QueryResult.Type.SET, "DROP ROLE");
        }
        if (executor.database.roleOwnsObjects(stmt.name())) {
            throw new MemgresException("role \"" + stmt.name() + "\" cannot be dropped because some objects depend on it\n  "
                    + "Detail: owner of " + describeOwnedObjects(stmt.name()), "2BP01");
        }
        executor.database.removeAllRoleMemberships(stmt.name());
        executor.database.removeAllRolePrivileges(stmt.name());
        executor.database.removeRole(stmt.name());
        return QueryResult.message(QueryResult.Type.SET, "DROP ROLE");
    }

    /** Drop all objects owned by the specified role. */
    void executeDropOwned(String roleName) {
        List<String> owned = executor.database.getObjectsOwnedBy(roleName);
        for (String key : owned) {
            int colon = key.indexOf(':');
            if (colon < 0) continue;
            String type = key.substring(0, colon);
            String name = key.substring(colon + 1);
            switch (type) {
                case "table": {
                    int dot = name.indexOf('.');
                    if (dot > 0) {
                        ddl.tableExecutor.dropSingleTable(name.substring(0, dot), name.substring(dot + 1), true, true);
                    }
                    break;
                }
                case "view": {
                    int vDot = name.indexOf('.');
                    String viewName = vDot > 0 ? name.substring(vDot + 1) : name;
                    executor.database.removeView(viewName);
                    break;
                }
                case "sequence":
                    executor.database.removeSequence(name);
                    break;
                case "function":
                    executor.database.removeFunction(name);
                    break;
                case "schema": {
                    if (!"public".equalsIgnoreCase(name)) {
                        executor.database.removeSchema(name);
                    }
                    break;
                }
                default: {
                    break;
                }
            }
            executor.database.removeObjectOwner(key);
        }
    }

    private String describeOwnedObjects(String roleName) {
        List<String> owned = executor.database.getObjectsOwnedBy(roleName);
        if (owned.isEmpty()) return "no objects";
        String key = owned.get(0);
        int colon = key.indexOf(':');
        if (colon > 0) {
            return key.substring(0, colon) + " " + key.substring(colon + 1);
        }
        return key;
    }

    // ---- CREATE RULE ----

    QueryResult executeCreateRule(CreateRuleStmt s) {
        // Validate target table/view exists
        Table on = executor.resolveTable(executor.defaultSchema(), s.table());
        checkRuleDefinition(s);
        checkRuleQualification(s, on);
        String joined = String.join(Database.RULE_ACTION_SEPARATOR, s.commands());
        // DO ALSO NOTHING and DO NOTHING are rules that do nothing, not rules whose action is the
        // word NOTHING. Registering the word made the next write try to run it as a statement.
        if ("NOTHING".equalsIgnoreCase(joined.trim())) joined = "";
        // Store INSTEAD NOTHING rules for enforcement
        if ("INSTEAD".equals(s.action()) && "NOTHING".equals(s.command())) {
            executor.database.addRule(s.table(), s.event(), "INSTEAD_NOTHING");
        } else if ("INSTEAD".equals(s.action()) && !joined.isEmpty()) {
            executor.database.addRule(s.table(), s.event(), "INSTEAD:" + joined);
        } else if ("ALSO".equals(s.action()) && !joined.isEmpty()) {
            executor.database.addRule(s.table(), s.event(), "ALSO:" + joined);
        }
        // The qualification decides which rows the rule fires for, so it is kept with it.
        executor.database.addRuleQualification(s.table(), s.event(), s.whereClause());
        // Track rule name with full definition for pg_rules
        executor.database.addRuleByName(s.name(), s.table(), s.event());
        boolean instead = "INSTEAD".equals(s.action());
        executor.database.addRuleDefinition(s.name(), s.table(),
                ruleDefinitionText(s, instead), s.event(), instead);
        return QueryResult.message(QueryResult.Type.SET, "CREATE RULE");
    }

    /**
     * A rule as {@code pg_get_ruledef} writes it: the header on its own line, the event indented
     * under it, the relation schema-qualified, and ALSO left unwritten because it is the default.
     * A qualification goes on a line of its own between the two.
     */
    private String ruleDefinitionText(CreateRuleStmt s, boolean instead) {
        StringBuilder sb = new StringBuilder("CREATE RULE ").append(s.name()).append(" AS");
        sb.append("\n    ON ").append(s.event()).append(" TO ")
                .append(executor.defaultSchema()).append('.').append(s.table());
        if (s.whereClause() != null) {
            sb.append("\n   WHERE ").append(normaliseRuleQualification(s));
        }
        List<String> commands = s.commands();
        boolean nothing = commands.isEmpty()
                || (commands.size() == 1 && "NOTHING".equals(commands.get(0)));
        if (nothing) {
            return sb.append(" DO ").append(instead ? "INSTEAD " : " ").append("NOTHING;").toString();
        }
        List<String> written = new ArrayList<>();
        for (String command : commands) written.add(normaliseRuleAction(command));
        // A single action is written after an extra space where ALSO would have stood; the
        // parenthesised form of several actions does without it.
        sb.append(" DO ").append(instead ? "INSTEAD " : "").append(written.size() > 1 ? "" : " ");
        if (written.size() == 1) return sb.append(written.get(0)).append(';').toString();
        // Several actions are written as PostgreSQL writes them: parenthesised, each closed by
        // its own semicolon, with the closing parenthesis on a line of its own. Writing them
        // bare left the definition unable to be read back as the rule it came from.
        sb.append("( ");
        for (int i = 0; i < written.size(); i++) {
            if (i > 0) sb.append("\n ");
            sb.append(written.get(i)).append(';');
        }
        return sb.append("\n);").toString();
    }

    /**
     * A rule's qualification as {@code pg_get_ruledef} writes it: the analysed expression, so it
     * comes back parenthesised and with OLD and NEW written in lower case. The token text the
     * parser kept came out as {@code NEW . b > 5}.
     */
    private String normaliseRuleQualification(CreateRuleStmt s) {
        try {
            Expression parsed = new com.memgres.engine.parser.Parser(
                    new com.memgres.engine.parser.Lexer(s.whereClause()).tokenize()).parseExpression();
            Table on = executor.resolveTable(executor.defaultSchema(), s.table());
            return lowerRowAliases(RuleDeparser.deparse(parsed, RuleDeparser.forTable(on)));
        } catch (RuntimeException e) {
            return s.whereClause();
        }
    }

    /**
     * OLD and NEW are relation names in the rewritten query, and PostgreSQL writes them in lower
     * case rather than as the quoted identifiers a deparser would otherwise make of them.
     */
    private static String lowerRowAliases(String sql) {
        return sql.replace("\"OLD\".", "old.").replace("\"NEW\".", "new.");
    }

    /**
     * One rule action as {@code pg_get_ruledef} writes it. PostgreSQL deparses the analysed
     * statement rather than echoing the text: an INSERT gets its target column list written out
     * and each value printed in the column's own type. Anything this engine cannot rewrite that
     * way is echoed as it was written, which is what the whole definition used to be.
     */
    private String normaliseRuleAction(String action) {
        try {
            com.memgres.engine.parser.ast.Statement parsed =
                    com.memgres.engine.parser.Parser.parse(action);
            if (!(parsed instanceof InsertStmt)) return action;
            InsertStmt ins = (InsertStmt) parsed;
            if (ins.values() == null || ins.values().isEmpty()) return action;
            Table target = executor.resolveTable(executor.defaultSchema(), ins.table());
            List<String> columnNames = new ArrayList<>();
            if (ins.columns() != null && !ins.columns().isEmpty()) {
                columnNames.addAll(ins.columns());
            } else {
                for (Column c : target.getColumns()) columnNames.add(c.getName());
            }
            StringBuilder sb = new StringBuilder("INSERT INTO ").append(ins.table()).append(" (");
            for (int i = 0; i < columnNames.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(columnNames.get(i));
            }
            sb.append(")\n  VALUES ");
            RuleDeparser.ColumnTypes types = RuleDeparser.forTable(target);
            for (int r = 0; r < ins.values().size(); r++) {
                if (r > 0) sb.append(", ");
                List<Expression> row = ins.values().get(r);
                sb.append('(');
                for (int i = 0; i < row.size(); i++) {
                    if (i > 0) sb.append(", ");
                    RuleDeparser.PgType want = null;
                    if (i < columnNames.size()) {
                        int idx = target.getColumnIndex(columnNames.get(i));
                        if (idx >= 0) want = RuleDeparser.fromColumn(target.getColumns().get(idx));
                    }
                    sb.append(RuleDeparser.deparseValue(row.get(i), want, types));
                }
                sb.append(')');
            }
            return lowerRowAliases(sb.toString());
        } catch (RuntimeException e) {
            return action;
        }
    }

    /**
     * An ON SELECT rule is how PostgreSQL represents a view, so a table may not carry one: its
     * own storage and the rule would both claim to define what the relation contains. The OLD/NEW
     * restrictions follow from the event — an INSERT has no prior row, a DELETE has no new one.
     */
    private void checkRuleDefinition(CreateRuleStmt s) {
        if ("SELECT".equals(s.event()) && !executor.database.hasView(s.table())) {
            if (s.whereClause() != null) {
                // The qualification of an ON SELECT rule is resolved against OLD, which a
                // SELECT rule has no row for.
                throw PgErrors.invalidObjectState("ON SELECT rule cannot use OLD");
            }
            throw PgErrors.wrongObjectType(
                    "relation \"" + s.table() + "\" cannot have ON SELECT rules");
        }
        if (!s.orReplace() && executor.database.hasRule(s.name(), s.table())) {
            throw new MemgresException("rule \"" + s.name() + "\" for relation \""
                    + s.table() + "\" already exists", "42710");
        }
        String forbidden = "INSERT".equals(s.event()) ? "OLD" : "DELETE".equals(s.event()) ? "NEW" : null;
        if (forbidden == null) return;
        for (String action : s.commands()) {
            if (referencesRowAlias(action, forbidden)) {
                throw PgErrors.invalidObjectState(
                        "ON " + s.event() + " rule cannot use " + forbidden);
            }
        }
    }

    /**
     * A rule's qualification decides which rows the rule fires for, so it is a condition:
     * PostgreSQL coerces it to boolean when the rule is written, naming the WHERE it stands in.
     * OLD and NEW are the names its columns answer to, alongside the relation's own.
     */
    private void checkRuleQualification(CreateRuleStmt s, Table on) {
        if (s.whereClause() == null) return;
        // A qualification is kept as the text it was written as, so its type names are read here
        // rather than when the statement itself was parsed.
        java.util.List<String> typeSchemas = new java.util.ArrayList<String>();
        Expression qualification;
        try {
            qualification = com.memgres.engine.parser.Parser.parseExpression(
                    s.whereClause(), typeSchemas);
        } catch (RuntimeException ignored) {
            return; // a qualification this cannot read is reported when the rule fires
        }
        SchemaQualifier.rejectMissingTypeSchemas(executor.database, executor.session, executor.getSystemCatalog(), typeSchemas);
        java.util.Set<String> aliases = new java.util.LinkedHashSet<>();
        aliases.add("old");
        aliases.add("new");
        if (on != null && on.getName() != null) aliases.add(on.getName().toLowerCase());
        // A qualification is read one row at a time, so nothing needing a group belongs in it,
        // and no call in it may carry a clause only an aggregate has a use for.
        executor.selectExecutor.placementCheck.rejectStoredDefinition(qualification, "WHERE", null);
        BooleanContext.check(qualification, "WHERE", BooleanContext.Types.of(on, aliases));
    }

    /** True when a rule action names {@code OLD.x} or {@code NEW.x} anywhere inside it. */
    private static boolean referencesRowAlias(String action, String alias) {
        com.memgres.engine.parser.ast.Statement parsed;
        try {
            parsed = com.memgres.engine.parser.Parser.parse(action);
        } catch (RuntimeException e) {
            return false; // an action this engine cannot parse is reported when the rule fires
        }
        return AstWalk.anyMatch(parsed, n -> n instanceof ColumnRef
                && ((ColumnRef) n).table() != null
                && ((ColumnRef) n).table().equalsIgnoreCase(alias));
    }

    // ---- CREATE SCHEMA ----

    QueryResult executeCreateSchema(CreateSchemaStmt s) {
        if (executor.database.getSchema(s.name()) != null) {
            if (s.ifNotExists()) return QueryResult.message(QueryResult.Type.SET, "CREATE SCHEMA");
            throw new MemgresException("schema \"" + s.name() + "\" already exists", "42P06");
        }
        executor.database.getOrCreateSchema(s.name());
        String owner = s.authorization() != null ? s.authorization() : executor.sessionUser();
        executor.database.setObjectOwner("schema:" + s.name(), owner);
        return QueryResult.message(QueryResult.Type.SET, "CREATE SCHEMA");
    }
}
