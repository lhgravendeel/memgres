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
                    break;
                }
                case COMMIT: {
                    String savedIso = stmt.chain() ? executor.session.getGucSettings().get("transaction_isolation") : null;
                    String savedRo = stmt.chain() ? executor.session.getGucSettings().get("transaction_read_only") : null;
                    executor.session.commit();
                    if (stmt.chain()) {
                        executor.session.begin();
                        if (savedIso != null) executor.session.getGucSettings().set("transaction_isolation", savedIso);
                        if (savedRo != null) executor.session.getGucSettings().set("transaction_read_only", savedRo);
                    }
                    break;
                }
                case ROLLBACK: {
                    String savedIso = stmt.chain() ? executor.session.getGucSettings().get("transaction_isolation") : null;
                    String savedRo = stmt.chain() ? executor.session.getGucSettings().get("transaction_read_only") : null;
                    executor.session.rollback();
                    if (stmt.chain()) {
                        executor.session.begin();
                        if (savedIso != null) executor.session.getGucSettings().set("transaction_isolation", savedIso);
                        if (savedRo != null) executor.session.getGucSettings().set("transaction_read_only", savedRo);
                    }
                    break;
                }
                case SAVEPOINT:
                    executor.session.savepoint(stmt.savepointName());
                    break;
                case RELEASE_SAVEPOINT:
                    executor.session.releaseSavepoint(stmt.savepointName());
                    break;
                case ROLLBACK_TO_SAVEPOINT:
                    executor.session.rollbackToSavepoint(stmt.savepointName());
                    break;
                case PREPARE_TRANSACTION:
                case COMMIT_PREPARED:
                case ROLLBACK_PREPARED: {
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

        if (stmt.analyze()) {
            startTime = System.nanoTime();
            actualResult = executor.executeStatement(stmt.statement());
        }

        buildPlanLines(stmt.statement(), planLines, 0, stmt.analyze(), startTime, actualResult, stmt.costs());

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
                                 long startTime, QueryResult actualResult, boolean costs) {
        String prefix = Strs.repeat("  ", indent);
        String arrow = indent > 0 ? "->  " : "";

        if (stmt instanceof SelectStmt) {
            SelectStmt sel = (SelectStmt) stmt;
            if (sel.from() == null || sel.from().isEmpty()) {
                lines.add(prefix + arrow + "Result");
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
                if (sel.where() != null) {
                    lines.add(prefix + "  Filter: (...)");
                    if (analyze && actualResult != null) {
                        lines.add(prefix + "  Rows Removed by Filter: 0");
                    }
                }
                if (analyze) {
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

    // ---- LISTEN / NOTIFY / UNLISTEN ----

    QueryResult executeListen(ListenStmt stmt) {
        if (executor.session != null) {
            executor.database.getNotificationManager().listen(executor.session, stmt.channel());
        }
        return QueryResult.message(QueryResult.Type.SET, "LISTEN");
    }

    QueryResult executeNotify(NotifyStmt stmt) {
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
        Table table = executor.resolveTable("public", stmt.table());
        table.addRlsPolicy(new RlsPolicy(stmt.name(), stmt.command(),
                stmt.usingExpr(), stmt.withCheckExpr(), stmt.roles()));
        return QueryResult.message(QueryResult.Type.SET, "CREATE POLICY");
    }

    // ---- ALTER POLICY ----

    QueryResult executeAlterPolicy(AlterPolicyStmt stmt) {
        Table table = executor.resolveTable("public", stmt.table());
        boolean policyFound = false;
        for (RlsPolicy p : table.getRlsPolicies()) {
            if (p.getName().equalsIgnoreCase(stmt.name())) { policyFound = true; break; }
        }
        if (!policyFound) {
            throw new MemgresException("policy \"" + stmt.name() + "\" for table \"" + stmt.table() + "\" does not exist", "42704");
        }
        if (stmt.renameTo() != null) {
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
            Map<String, String> attrs = executor.database.getRole(stmt.name());
            if (attrs != null) {
                executor.database.removeRole(stmt.name());
                executor.database.createRole(stmt.renameTo(), attrs);
            }
        } else if (!stmt.options().isEmpty()) {
            Map<String, String> existing = executor.database.getRole(stmt.name());
            if (existing != null) {
                existing.putAll(stmt.options());
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
        executor.resolveTable(executor.defaultSchema(), s.table());
        // Store INSTEAD NOTHING rules for enforcement
        if ("INSTEAD".equals(s.action()) && "NOTHING".equals(s.command())) {
            executor.database.addRule(s.table(), s.event(), "INSTEAD_NOTHING");
        } else if ("INSTEAD".equals(s.action()) && s.command() != null && !s.command().isEmpty()) {
            executor.database.addRule(s.table(), s.event(), "INSTEAD:" + s.command());
        }
        // Track rule name for DROP RULE support
        executor.database.addRuleByName(s.name(), s.table());
        return QueryResult.message(QueryResult.Type.SET, "CREATE RULE");
    }

    // ---- CREATE SCHEMA ----

    QueryResult executeCreateSchema(CreateSchemaStmt s) {
        if (executor.database.getSchema(s.name()) != null) {
            if (s.ifNotExists()) return QueryResult.message(QueryResult.Type.SET, "CREATE SCHEMA");
            throw new MemgresException("schema \"" + s.name() + "\" already exists", "42P06");
        }
        executor.database.getOrCreateSchema(s.name());
        executor.database.setObjectOwner("schema:" + s.name(), executor.sessionUser());
        return QueryResult.message(QueryResult.Type.SET, "CREATE SCHEMA");
    }
}
