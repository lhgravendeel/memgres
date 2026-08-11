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
        // Whether the COMMIT below found a transaction block that had already failed, which is what
        // decides the tag it answers with.
        boolean discarded = false;
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
                    // A COMMIT with no transaction open is not refused, but it makes nothing
                    // permanent either: PostgreSQL warns rather than let a script believe the work
                    // before it had been committed by this statement.
                    if (!executor.session.isInTransaction()) {
                        executor.session.addNotice("WARNING", "25P01",
                                "there is no transaction in progress", null);
                    }
                    // A transaction block that has already failed can only be thrown away, and
                    // PostgreSQL reports what the COMMIT did rather than what it was asked to do:
                    // the command tag is ROLLBACK. Answering COMMIT left a client reading the tag
                    // unable to tell that everything its transaction wrote had been discarded.
                    discarded = executor.session.isFailed();
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
                        throw new MemgresException("prepared transactions are disabled"
                                + "\n  Hint: Set \"max_prepared_transactions\" to a nonzero value.",
                                "55000");
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
                return discarded
                        ? QueryResult.message(QueryResult.Type.ROLLBACK, "ROLLBACK")
                        : QueryResult.message(QueryResult.Type.COMMIT, "COMMIT");
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

    /**
     * EXPLAIN: analyse the statement, build the plan tree, and print it.
     *
     * <p>The statement is read but not run unless ANALYZE was asked for, so a cursor is not left
     * open and a table is not emptied by asking what the plan would be.
     */
    QueryResult executeExplain(ExplainStmt stmt) {
        if (stmt.statement() == null) {
            throw new MemgresException("syntax error at end of input", "42601");
        }

        // Parse analysis first: a relation or column that is not there is what a reader hears
        // about before a misspelled option, exactly as PostgreSQL orders the two.
        new StatementAnalyzer(executor).analyze(stmt.statement());

        if (stmt.deferredOptionError() != null) {
            String sqlState = stmt.deferredOptionSqlState() != null ? stmt.deferredOptionSqlState() : "22023";
            throw new MemgresException(stmt.deferredOptionError(), sqlState);
        }

        long startTime = 0;
        QueryResult actualResult = null;
        if (stmt.analyze()) {
            startTime = System.nanoTime();
            // What ANALYZE runs is the plan, not the statement that carries it: explaining a
            // DECLARE runs its query, and does not leave a cursor behind for the session.
            Statement toRun = stmt.statement();
            if (toRun instanceof DeclareCursorStmt) toRun = ((DeclareCursorStmt) toRun).query();
            else if (toRun instanceof CreateTableAsStmt) toRun = ((CreateTableAsStmt) toRun).query();
            actualResult = executor.executeStatement(toRun);
        }

        ExplainPlan plan = new ExplainPlanBuilder(executor, stmt.verbose()).build(stmt.statement());

        List<Column> planCols = Cols.listOf(new Column("QUERY PLAN", DataType.TEXT, true, false, null));
        if (stmt.format().equals("JSON")) {
            return QueryResult.select(planCols,
                    Collections.singletonList(new Object[]{plan.renderJson(stmt.costs())}));
        }
        if (stmt.format().equals("XML")) {
            return QueryResult.select(planCols,
                    Collections.singletonList(new Object[]{plan.renderXml(stmt.costs())}));
        }
        if (stmt.format().equals("YAML")) {
            return QueryResult.select(planCols,
                    Collections.singletonList(new Object[]{plan.renderYaml(stmt.costs())}));
        }

        List<String> planLines = new ArrayList<>();
        plan.renderText(planLines, 0, rootSuffix(stmt, startTime, actualResult));
        appendExplainExtras(stmt, planLines, startTime,
                actualResult == null || actualResult.getRows() == null
                        ? 0 : actualResult.getRows().size());

        List<Object[]> rows = new ArrayList<>();
        for (String line : planLines) rows.add(new Object[]{line});
        return QueryResult.select(planCols, rows);
    }

    /**
     * What follows the top node's name: its estimated cost, and under ANALYZE what it actually did.
     * PostgreSQL prints the timings only when TIMING is on, and the row count always.
     */
    private String rootSuffix(ExplainStmt stmt, long startTime, QueryResult actualResult) {
        StringBuilder sb = new StringBuilder();
        if (stmt.costs()) sb.append("  (cost=0.00..0.01 rows=1 width=4)");
        if (stmt.analyze()) {
            long rows = actualResult == null || actualResult.getRows() == null
                    ? 0 : actualResult.getRows().size();
            double elapsed = (System.nanoTime() - startTime) / 1_000_000.0;
            sb.append(' ');
            if (stmt.timing) {
                sb.append(String.format(Locale.ROOT, "(actual time=%.3f..%.3f rows=%d.00 loops=1)",
                        elapsed, elapsed, rows));
            } else {
                sb.append(String.format(Locale.ROOT, "(actual rows=%d.00 loops=1)", rows));
            }
        }
        return sb.length() == 0 ? null : sb.toString();
    }

    /** The lines that follow the plan: buffers, WAL, memory, settings and the summary totals. */
    private void appendExplainExtras(ExplainStmt stmt, List<String> planLines, long startTime,
                                     long serializedRows) {
        if (stmt.buffers && stmt.analyze) {
            planLines.add("Buffers: shared hit=0");
        }
        if (stmt.wal) {
            planLines.add("WAL: records=0 fpi=0 bytes=0");
        }
        if (stmt.memory()) {
            planLines.add("Planning:");
            planLines.add("  Memory: used=0kB  allocated=0kB");
        }
        if (stmt.settings) {
            String shown = changedSettings();
            if (shown != null) planLines.add("Settings: " + shown);
        }
        if (stmt.serialize()) {
            // What SERIALIZE reports is how much was written for the client, rounded up to the
            // kilobyte it occupies: a query that answered rows wrote something.
            planLines.add("Serialization: output=" + (serializedRows > 0 ? 1 : 0)
                    + "kB  format=" + stmt.serializeMode);
        }
        if (stmt.summary) {
            double elapsed = startTime == 0 ? 0.0 : (System.nanoTime() - startTime) / 1_000_000.0;
            planLines.add(String.format(Locale.ROOT, "Planning Time: %.3f ms", elapsed * 0.1));
            if (stmt.analyze) {
                planLines.add(String.format(Locale.ROOT, "Execution Time: %.3f ms", elapsed));
            }
        }
    }

    /**
     * The settings this session changed from their built-in defaults, as EXPLAIN (SETTINGS) lists
     * them. Naming none of them at all was a line PostgreSQL never prints.
     */
    private String changedSettings() {
        GucSettings gucs = executor.session == null ? null : executor.session.getGucSettings();
        if (gucs == null) return null;
        List<String> shown = new ArrayList<>();
        for (String name : gucs.changedFromDefault()) {
            shown.add(name + " = '" + gucs.getWithUnit(name) + "'");
        }
        return shown.isEmpty() ? null : String.join(", ", shown);
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
        // The bound is on the bytes the payload takes, not on the characters it is written with:
        // four thousand two-byte characters are eight thousand bytes and one too many.
        if (stmt.payload() != null
                && stmt.payload().getBytes(java.nio.charset.StandardCharsets.UTF_8).length >= 8000) {
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
        // A policy decides which of the relation's rows a role can see, so writing one is the
        // owner's to do — a role holding only SELECT could otherwise grant itself every row.
        executor.requireTableOwner(executor.defaultSchema(), stmt.table());
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
        // Changing what a policy admits is as much the owner's to do as writing one.
        executor.requireTableOwner(executor.defaultSchema(), stmt.table());
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
        if (colon < 0) return key;
        String kind = key.substring(0, colon);
        String name = key.substring(colon + 1);
        int dot = name.indexOf('.');
        String schema = dot > 0 ? name.substring(0, dot) : null;
        String bare = dot > 0 ? name.substring(dot + 1) : name;
        // A materialized view is stored beside the plain views and the ownership key does not
        // say which of the two it is, so the view itself is asked.
        Database.ViewDef view = "view".equals(kind) ? executor.database.getView(bare) : null;
        if (view != null && view.materialized()) kind = "materialized view";
        return kind + " " + RelationNamespace.shownName(executor.searchPathSchemas(), schema, bare);
    }

    // ---- CREATE RULE ----

    QueryResult executeCreateRule(CreateRuleStmt s) {
        // What kind of relation was named settles before it is opened as a table: a sequence and a
        // materialized view carry no rules at all, and a view already has the one ON SELECT rule
        // that says what it contains.
        checkRuleRelationKind(s);
        // Validate target table/view exists
        Table on = executor.resolveTable(executor.defaultSchema(), s.table());
        checkRuleDefinition(s);
        checkRuleQualification(s, on);
        // A rule's actions are analysed as the rule is written rather than when it fires, so a
        // relation or a column an action names that is not there is reported by the CREATE RULE
        // that wrote it instead of by whoever writes to the relation next.
        List<String> dependsOn = checkRuleActions(s, on);
        String joined = String.join(Database.RULE_ACTION_SEPARATOR, s.commands());
        // DO ALSO NOTHING and DO NOTHING are rules that do nothing, not rules whose action is the
        // word NOTHING. Registering the word made the next write try to run it as a statement.
        if ("NOTHING".equalsIgnoreCase(joined.trim())) joined = "";
        boolean instead = "INSTEAD".equals(s.action());
        // Every rule is registered under its own name, with its own WHERE: PostgreSQL fires all the
        // rules an event carries, in rule-name order, and each rule's WHERE decides which rows its
        // actions run for.
        executor.database.addRule(s.name(), s.table(), s.event(), instead, joined, s.whereClause());
        // Track rule name with full definition for pg_rules
        executor.database.addRuleDefinition(s.name(), s.table(),
                ruleDefinitionText(s, instead), s.event(), instead);
        // What the actions name is what the rule depends on, and PostgreSQL records it: dropping
        // one of those relations is refused while the rule that writes to it is still there.
        executor.database.addRuleDependencies(s.name(), s.table(), dependsOn);
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
            MemgresException e = PgErrors.wrongObjectType(
                    "relation \"" + s.table() + "\" cannot have ON SELECT rules");
            e.setDetail("This operation is not supported for tables.");
            throw e;
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
        // OLD and NEW in a qualification are the rows of the relation the rule is on, and their
        // columns are that relation's: PostgreSQL resolves them while it is writing the rule.
        // Leaving them unresolved stored a rule whose WHERE could never be evaluated, and the
        // relation could not be written to at all from that point on.
        checkActionRowReferences(qualification,
                executor.database.hasView(s.table()) ? null : on);
        // A call in the qualification is resolved there and then as well, by name and argument
        // list together, so a name nothing answers to is a function that does not exist. Naming
        // the argument types is what the refusal needs, and in a qualification they come from the
        // relation the rule is on, which both OLD and NEW stand for.
        FilterCheck.reject(executor.selectExecutor, qualification, null);
        resolveRuleCalls(qualification,
                ruleScope(executor.database.hasView(s.table()) ? null : on, null));
        // A qualification is read one row at a time, so nothing needing a group belongs in it,
        // and no call in it may carry a clause only an aggregate has a use for.
        executor.selectExecutor.placementCheck.rejectStoredDefinition(qualification, "WHERE", null);
        BooleanContext.check(qualification, "WHERE", BooleanContext.Types.of(on, aliases));
    }

    /**
     * Which kinds of relation may carry a rule. PostgreSQL opens the relation first, so a name
     * that reaches nothing is a missing relation whatever the rule said; but a sequence cannot
     * have rules at all, a materialized view is never rewritten, and an ON SELECT rule on a view
     * would be a second definition of what the view contains.
     */
    private void checkRuleRelationKind(CreateRuleStmt s) {
        if (s.table() == null) return;
        String kind = RelationNamespace.kindOf(
                executor.database, executor.defaultSchema(), s.table());
        if (RelationNamespace.SEQUENCE.equals(kind)) {
            MemgresException noRules = PgErrors.wrongObjectType(
                    "relation \"" + s.table() + "\" cannot have rules");
            noRules.setDetail("This operation is not supported for sequences.");
            throw noRules;
        }
        if (RelationNamespace.MATVIEW.equals(kind)) {
            throw PgErrors.notImplemented("rules on materialized views are not supported");
        }
        if (RelationNamespace.VIEW.equals(kind) && "SELECT".equals(s.event())) {
            throw new MemgresException("\"" + s.table() + "\" is already a view", "55000");
        }
    }

    /**
     * The commands a rule's action list may hold. PostgreSQL's grammar admits only these, so
     * anything else is a syntax error at the word that opened it rather than a rule that is
     * stored and then found to be unrunnable.
     */
    private static final Set<String> RULE_ACTION_COMMANDS =
            Cols.setOf("select", "insert", "update", "delete", "notify", "with", "values", "table");

    /**
     * Analyse a rule's actions the way PostgreSQL does when the rule is written: every relation an
     * action names has to be there, and OLD and NEW resolve against the relation the rule is on.
     * Storing the text unread left both mistakes for whoever wrote to the relation next, with the
     * statement that made them long gone.
     *
     * @return the relations the actions name, which the rule then depends on
     */
    private List<String> checkRuleActions(CreateRuleStmt s, Table on) {
        List<String> dependsOn = new ArrayList<>();
        // A view's rule resolves OLD and NEW against the view's own columns, and what a view
        // resolves to here is the relation behind it, whose columns may be named differently.
        Table rowSource = executor.database.hasView(s.table()) ? null : on;
        for (String action : s.commands()) {
            String written = action == null ? "" : action.trim();
            if (written.isEmpty() || "NOTHING".equalsIgnoreCase(written)) continue;
            rejectNonRuleCommand(written);
            com.memgres.engine.parser.ast.Statement parsed;
            try {
                parsed = com.memgres.engine.parser.Parser.parse(written);
            } catch (RuntimeException unreadable) {
                continue; // an action this engine cannot read is reported when the rule fires
            }
            List<String> named = actionRelations(parsed);
            for (String relation : named) {
                if (!relationVisible(relation)) {
                    throw new MemgresException(
                            "relation \"" + relation + "\" does not exist", "42P01");
                }
                if (!relation.equalsIgnoreCase(s.table()) && !dependsOn.contains(relation)) {
                    dependsOn.add(relation);
                }
            }
            // An INSERT's column list is matched against the relation before anything it is handed
            // is read, so a column the relation does not hold is what PostgreSQL reports even when
            // the values name something missing as well.
            if (parsed instanceof InsertStmt) {
                InsertStmt insert = (InsertStmt) parsed;
                rejectMissingActionColumns(insert.schema(), insert.table(), insert.columns());
            }
            checkActionRowReferences(parsed, rowSource);
            // The calls an action makes are resolved as the rule is written, by name and argument
            // list together. A name nothing answers to used to be left for the write that fires
            // the rule, so the mistake was reported by whoever inserted into the relation next --
            // and until the rule was dropped, nothing could be written to it at all.
            FilterCheck.reject(executor.selectExecutor, parsed, null);
            resolveRuleCalls(parsed, ruleScope(rowSource, named));
            // An UPDATE's assignments are read before its targets are matched to the relation's
            // columns, so a target the relation does not hold is reported after whatever the
            // expression assigned to it is itself wrong about.
            if (parsed instanceof UpdateStmt) {
                UpdateStmt update = (UpdateStmt) parsed;
                List<String> targets = new ArrayList<>();
                if (update.setClauses() != null) {
                    for (InsertStmt.SetClause set : update.setClauses()) targets.add(set.column());
                }
                rejectMissingActionColumns(update.schema(), update.table(), targets);
            }
        }
        return dependsOn;
    }

    /**
     * The columns an action names in the relation it writes to. PostgreSQL matches an INSERT's
     * column list, and an UPDATE's assignment targets, against that relation while it is writing
     * the rule: {@code column "nope" of relation "log" does not exist}, 42703. Storing the action
     * unread left the mistake for whoever wrote to the relation the rule is on.
     */
    private void rejectMissingActionColumns(String schema, String table, List<String> columns) {
        if (table == null || columns == null || columns.isEmpty()) return;
        Table target;
        try {
            target = executor.resolveTable(
                    schema == null ? executor.defaultSchema() : schema, table);
        } catch (RuntimeException unreachable) {
            return; // a relation this cannot open is reported for being missing, not for a column
        }
        if (target == null) return;
        for (String column : columns) {
            if (column == null || target.getColumnIndex(column) >= 0) continue;
            // A system column is not one the relation's definition lists, and writing to one is a
            // complaint of its own rather than a column that is not there.
            if (DdlDefinitionChecks.isSystemColumnName(column)) continue;
            throw new MemgresException("column \"" + column + "\" of relation \""
                    + table + "\" does not exist", "42703");
        }
    }

    /**
     * The names a rule's action or its qualification resolves against. PostgreSQL builds a range
     * table for a rule while it is writing the rule — OLD and NEW, both of which are rows of the
     * relation the rule is on, alongside whatever relations the action itself names — and that is
     * what lets it name the type of every argument a call was handed. Without those types a call
     * can only be judged on its name, and {@code nosuch(new.v)} is then stored unread; PostgreSQL
     * refuses it as {@code function nosuch(text) does not exist}.
     *
     * @param rowSource the relation OLD and NEW stand for, or null where it is not settled here
     * @param named     the relations the action names, or null for a qualification, which names none
     */
    private QueryLevelScope ruleScope(Table rowSource, List<String> named) {
        List<RowContext.TableBinding> bindings = new ArrayList<>();
        if (rowSource != null) {
            bindings.add(new RowContext.TableBinding(rowSource, "old", null));
            bindings.add(new RowContext.TableBinding(rowSource, "new", null));
        }
        if (named != null) {
            for (String written : named) {
                int dot = written.lastIndexOf('.');
                String schema = dot > 0 ? written.substring(0, dot) : null;
                String bare = dot > 0 ? written.substring(dot + 1) : written;
                // A catalog relation is described rather than stored, and memgres spells some of
                // its columns with a type of its own where PostgreSQL declares a narrower one, so
                // reading one here would resolve away a call PostgreSQL runs.
                if (SystemCatalog.isSystemCatalog(schema, bare)) continue;
                Table table;
                try {
                    table = executor.resolveTable(
                            schema == null ? executor.defaultSchema() : schema, bare);
                } catch (RuntimeException unreachable) {
                    continue; // a name that reaches nothing was already reported as such
                }
                if (table == null || table.isFunctionResult()) continue;
                bindings.add(new RowContext.TableBinding(table, bare, null));
            }
        }
        return new QueryLevelScope(executor.selectExecutor, bindings, null, null);
    }

    /**
     * Resolves every call a rule's action or qualification makes, by name and argument list
     * together, which is how PostgreSQL resolves a call: a name nothing answers to, an argument
     * count no signature of that name takes, and argument types no signature of it accepts are all
     * the same refusal — {@code function upper(text, text) does not exist}, 42883 — and all three
     * are settled while the rule is being written rather than left for the next write to the
     * relation, which until the rule was dropped could not be made at all.
     */
    private static void resolveRuleCalls(Object node, QueryLevelScope scope) {
        if (node == null) return;
        final List<Object> calls = new ArrayList<>();
        AstWalk.forEach(node, n -> {
            if (n instanceof FunctionCallExpr || n instanceof WindowFuncExpr) calls.add(n);
        });
        for (Object call : calls) {
            String name = call instanceof FunctionCallExpr
                    ? ((FunctionCallExpr) call).name() : ((WindowFuncExpr) call).name();
            if (name == null) continue;
            // count(*) counts rows rather than values and has no argument list to resolve against.
            if (call instanceof FunctionCallExpr && ((FunctionCallExpr) call).star()) continue;
            scope.rejectUnresolvableCall(call, QueryLevelScope.bareName(name));
        }
    }

    /** The command an action is, which is its first word, and whether a rule may hold it. */
    private static void rejectNonRuleCommand(String action) {
        int end = 0;
        while (end < action.length() && Character.isLetter(action.charAt(end))) end++;
        String word = action.substring(0, end);
        if (word.isEmpty() || RULE_ACTION_COMMANDS.contains(word.toLowerCase())) return;
        throw PgErrors.syntax("syntax error at or near \"" + word + "\"");
    }

    /**
     * The relations an action names: the one it writes to, and every one in a FROM clause. A name
     * the action's own WITH clause binds is that query's, and OLD and NEW are the rows the
     * rewriter puts in scope rather than relations to go looking for.
     */
    private static List<String> actionRelations(com.memgres.engine.parser.ast.Statement parsed) {
        final Set<String> bound = new LinkedHashSet<>();
        bound.add("old");
        bound.add("new");
        AstWalk.forEach(parsed, node -> {
            if (node instanceof SelectStmt.CommonTableExpr) {
                String cteName = ((SelectStmt.CommonTableExpr) node).name;
                if (cteName != null) bound.add(cteName.toLowerCase());
            }
        });
        final List<String> named = new ArrayList<>();
        if (parsed instanceof InsertStmt) {
            addActionRelation(named, ((InsertStmt) parsed).schema(), ((InsertStmt) parsed).table());
        } else if (parsed instanceof UpdateStmt) {
            addActionRelation(named, ((UpdateStmt) parsed).schema(), ((UpdateStmt) parsed).table());
        } else if (parsed instanceof DeleteStmt) {
            addActionRelation(named, ((DeleteStmt) parsed).schema(), ((DeleteStmt) parsed).table());
        }
        AstWalk.forEach(parsed, node -> {
            if (node instanceof SelectStmt.TableRef) {
                SelectStmt.TableRef ref = (SelectStmt.TableRef) node;
                addActionRelation(named, ref.schema(), ref.table());
            }
        });
        List<String> out = new ArrayList<>();
        for (String relation : named) {
            if (!bound.contains(RelationNamespace.bareName(relation).toLowerCase())) out.add(relation);
        }
        return out;
    }

    private static void addActionRelation(List<String> named, String schema, String table) {
        if (table == null || table.isEmpty()) return;
        String written = schema == null ? table : schema + "." + table;
        if (!named.contains(written)) named.add(written);
    }

    /** Whether this session can reach a relation of any kind under the name an action wrote. */
    private boolean relationVisible(String written) {
        int dot = written.lastIndexOf('.');
        String schema = dot > 0 ? written.substring(0, dot) : null;
        String bare = dot > 0 ? written.substring(dot + 1) : written;
        // A catalog relation belongs to no schema's tables: it is built when it is read.
        if (SystemCatalog.isSystemCatalog(schema, bare)) return true;
        if (schema != null) {
            return RelationNamespace.kindOf(executor.database, schema, bare) != null;
        }
        for (String candidate : executor.searchPathSchemas()) {
            if (RelationNamespace.kindOf(executor.database, candidate, bare) != null) return true;
        }
        // A relation the path does not reach by kind may still be one this session can open --
        // a temporary table lives in a schema of its own.
        return executor.resolveTableSafe(bare) != null || executor.database.getView(bare) != null;
    }

    /**
     * OLD and NEW in an action or in the rule's own qualification are the rows of the relation the
     * rule is on, so their columns are that relation's columns -- along with the system columns
     * every relation carries.
     */
    private static void checkActionRowReferences(Object parsed, Table on) {
        if (on == null) return;
        Object missing = AstWalk.findFirst(parsed, node -> {
            if (!(node instanceof ColumnRef)) return false;
            ColumnRef ref = (ColumnRef) node;
            if (ref.table() == null || ref.column() == null) return false;
            if (!ref.table().equalsIgnoreCase("old") && !ref.table().equalsIgnoreCase("new")) {
                return false;
            }
            return !DdlDefinitionChecks.isSystemColumnName(ref.column())
                    && on.getColumnIndex(ref.column()) < 0;
        });
        if (missing == null) return;
        ColumnRef ref = (ColumnRef) missing;
        throw new MemgresException("column " + ref.table().toLowerCase() + "." + ref.column()
                + " does not exist", "42703");
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
        executor.recordUndo(new Session.CreateSchemaUndo(s.name()));
        String owner = s.authorization() != null ? s.authorization() : executor.sessionUser();
        executor.database.setObjectOwner("schema:" + s.name(), owner);
        return QueryResult.message(QueryResult.Type.SET, "CREATE SCHEMA");
    }
}
