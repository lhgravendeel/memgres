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
                    // A block that is already open cannot be opened again, and PostgreSQL warns
                    // rather than refuse: the statements after this one belong to the block that
                    // was already running, not to a new one this BEGIN started, and a script that
                    // ends "its" transaction ends theirs.
                    if (executor.session.isInTransaction()) {
                        executor.session.addNotice("WARNING", "25001",
                                "there is already a transaction in progress", null);
                    }
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
                    // A ROLLBACK with no transaction open undoes nothing, and PostgreSQL warns
                    // rather than let a script believe the work before it had been thrown away.
                    if (!executor.session.isInTransaction()) {
                        executor.session.addNotice("WARNING", "25P01",
                                "there is no transaction in progress", null);
                    }
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
                // BEGIN and START TRANSACTION open the same block, and PostgreSQL answers each
                // with the verb it was given: a client reading the tag is told what it asked for.
                return QueryResult.message(QueryResult.Type.BEGIN,
                        stmt.startTransaction() ? "START TRANSACTION" : "BEGIN");
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
        // A qualifier on the relation says which schema holds it, and a schema that is not there
        // is what PostgreSQL reports rather than the relation being missing from it.
        SchemaQualifier.requireSchema(executor.database, executor.session, stmt.schema());
        // A view has no row security, so a policy on one would be recorded and never applied —
        // and a reader of the schema would conclude the view is protected when it is not.
        if (stmt.schema() != null ? executor.database.hasView(stmt.schema(), stmt.table())
                : executor.database.hasView(stmt.table())) {
            throw PgErrors.wrongObjectType("\"" + stmt.table() + "\" is not a table");
        }
        // The same holds of every other kind of relation a name can reach. A sequence, an index
        // and a composite type each own a row of pg_class, and none of them has rows of its own
        // for a policy to decide about; a foreign table's rows are somebody else's to guard.
        String relationKind = RelationNamespace.kindOf(executor.database,
                executor.relationSchemaOf(stmt.schema(), stmt.table()), stmt.table());
        if (relationKind != null && !RelationNamespace.TABLE.equals(relationKind)) {
            throw PgErrors.wrongObjectType("\"" + stmt.table() + "\" is not a table");
        }
        Table table = executor.resolveTable(policySchema(stmt.schema()), stmt.table(),
                stmt.schema() != null);
        // A policy decides which of the relation's rows a role can see, so writing one is the
        // owner's to do — a role holding only SELECT could otherwise grant itself every row.
        executor.requireTableOwner(
                stmt.schema() != null ? stmt.schema() : executor.defaultSchema(), stmt.table());
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
        // PostgreSQL numbers a pg_policy row when the policy is created, and a refusal listing
        // what hangs from one object walks its dependency catalogue in that order -- so a policy
        // written before a view is reported ahead of it and one written after is reported behind
        // it. Nothing minted a number for a policy at all, so every one of them came last.
        String policyHome = executor.database.schemaNameOf(table);
        if (policyHome != null) {
            executor.identity().policyCreated(policyHome, table.getName(), stmt.name());
        }
        return QueryResult.message(QueryResult.Type.SET, "CREATE POLICY");
    }

    /**
     * The schema a policy's relation is looked for in. A policy belongs to its relation rather
     * than to the relation's name, so two schemas may each hold a relation of one name and each
     * carry policies of its own; a name written bare is looked for where it always was.
     */
    private static String policySchema(String writtenSchema) {
        return writtenSchema != null ? writtenSchema : "public";
    }

    // ---- ALTER POLICY ----

    /** PUBLIC names every role at once rather than one that has to be found in the catalog. */
    private static final String PUBLIC_ROLE = "public";

    QueryResult executeAlterPolicy(AlterPolicyStmt stmt) {
        // A qualifier on the relation says which schema holds it, and a schema that is not there
        // is what PostgreSQL reports rather than the relation being missing from it.
        SchemaQualifier.requireSchema(executor.database, executor.session, stmt.schema());
        Table table = executor.resolveTable(policySchema(stmt.schema()), stmt.table(),
                stmt.schema() != null);
        // Changing what a policy admits is as much the owner's to do as writing one.
        executor.requireTableOwner(
                stmt.schema() != null ? stmt.schema() : executor.defaultSchema(), stmt.table());
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
        // A qualifier on the relation says which schema holds it, and a schema that is not there
        // is what PostgreSQL reports rather than the relation being missing from it.
        SchemaQualifier.requireSchema(executor.database, executor.session, s.schema());
        // What kind of relation was named settles before it is opened as a table: a sequence and a
        // materialized view carry no rules at all, and a view already has the one ON SELECT rule
        // that says what it contains.
        checkRuleRelationKind(s);
        // Validate target table/view exists
        Table on = executor.resolveTable(ruleSchema(s), s.table(), s.schema() != null);
        checkRuleDefinition(s);
        checkRuleQualification(s, on);
        // A rule's actions are analysed as the rule is written rather than when it fires, so a
        // relation or a column an action names that is not there is reported by the CREATE RULE
        // that wrote it instead of by whoever writes to the relation next.
        List<String> dependsOn = checkRuleActions(s, on);
        String joined = String.join(Database.RULE_ACTION_SEPARATOR,
                actionsBoundToTheirSchemas(s.commands()));
        // DO ALSO NOTHING and DO NOTHING are rules that do nothing, not rules whose action is the
        // word NOTHING. Registering the word made the next write try to run it as a statement.
        if ("NOTHING".equalsIgnoreCase(joined.trim())) joined = "";
        boolean instead = "INSTEAD".equals(s.action());
        // Every rule is registered under its own name, on the relation in the schema it was
        // written on, with its own WHERE: PostgreSQL fires all the rules an event carries, in
        // rule-name order, and each rule's WHERE decides which rows its actions run for.
        executor.database.addRule(ruleSchema(s), s.name(), s.table(), s.event(), instead,
                joined, s.whereClause());
        // Track rule name with full definition for pg_rules
        executor.database.addRuleDefinition(ruleSchema(s), s.name(), s.table(),
                ruleDefinitionText(s, instead));
        // What the actions name is what the rule depends on, and PostgreSQL records it: dropping
        // one of those relations is refused while the rule that writes to it is still there.
        executor.database.addRuleDependencies(ruleSchema(s), s.name(), s.table(),
                relationsWithTheirSchemas(dependsOn));
        return QueryResult.message(QueryResult.Type.SET, "CREATE RULE");
    }

    /**
     * What the rule's actions name, each under the schema that holds it. PostgreSQL records the
     * relation itself, so a name written bare is settled here against the search path as it stands
     * while the rule is being written: matching on the bare name alone made a rule that writes to
     * one schema's relation refuse the drop of another schema's relation of the same name.
     */
    private List<String> relationsWithTheirSchemas(List<String> written) {
        List<String> out = new ArrayList<>();
        for (String relation : written) {
            out.add(executor.relationSchemaOf(null, relation) + "."
                    + RelationNamespace.bareName(relation));
        }
        return out;
    }

    /**
     * The rule's actions, each writing to the relation it named at the moment the rule was written.
     *
     * <p>PostgreSQL analyses an action when the rule is written and records the relation itself, so
     * a name written without a schema means whatever the search path reached then and goes on
     * meaning it however the path stands when the rule fires. Memgres runs an action from the text
     * it was written as, so the schema is put into that text here; an action written in a shape the
     * qualifier cannot be placed in is left exactly as it was and resolves as it did before.
     */
    private List<String> actionsBoundToTheirSchemas(List<String> commands) {
        List<String> out = new ArrayList<>();
        for (String command : commands) out.add(actionBoundToItsSchema(command));
        return out;
    }

    private String actionBoundToItsSchema(String command) {
        String written;
        try {
            com.memgres.engine.parser.ast.Statement parsed =
                    com.memgres.engine.parser.Parser.parse(command);
            if (parsed instanceof InsertStmt) {
                if (((InsertStmt) parsed).schema() != null) return command;
                written = ((InsertStmt) parsed).table();
            } else if (parsed instanceof UpdateStmt) {
                if (((UpdateStmt) parsed).schema() != null) return command;
                written = ((UpdateStmt) parsed).table();
            } else if (parsed instanceof DeleteStmt) {
                if (((DeleteStmt) parsed).schema() != null) return command;
                written = ((DeleteStmt) parsed).table();
            } else {
                return command;
            }
        } catch (RuntimeException notParsed) {
            return command;
        }
        if (written == null || written.indexOf('.') >= 0) return command;
        String schema = executor.relationSchemaOf(null, written);
        if (schema == null) return command;
        // The relation a write names is the first one its text names, so the first whole word that
        // spells it is the one the schema goes in front of.
        int at = firstWholeWord(command, written);
        if (at < 0) return command;
        String bound = command.substring(0, at) + schema + "." + command.substring(at);
        // Only a rewrite that reads back as the same statement against the same relation is kept:
        // anything this could not place exactly goes on being resolved when the rule fires.
        try {
            com.memgres.engine.parser.ast.Statement reparsed =
                    com.memgres.engine.parser.Parser.parse(bound);
            String reboundSchema = reparsed instanceof InsertStmt ? ((InsertStmt) reparsed).schema()
                    : reparsed instanceof UpdateStmt ? ((UpdateStmt) reparsed).schema()
                    : reparsed instanceof DeleteStmt ? ((DeleteStmt) reparsed).schema() : null;
            String reboundTable = reparsed instanceof InsertStmt ? ((InsertStmt) reparsed).table()
                    : reparsed instanceof UpdateStmt ? ((UpdateStmt) reparsed).table()
                    : reparsed instanceof DeleteStmt ? ((DeleteStmt) reparsed).table() : null;
            if (!schema.equalsIgnoreCase(reboundSchema) || !written.equalsIgnoreCase(reboundTable)) {
                return command;
            }
        } catch (RuntimeException notParsed) {
            return command;
        }
        return bound;
    }

    /**
     * Where {@code word} stands in {@code text} as a word of its own, or -1. A name inside quotes
     * is left alone: a qualifier written in front of the opening quote would be read as part of
     * the quoted name rather than as the schema it is.
     */
    private static int firstWholeWord(String text, String word) {
        String lower = text.toLowerCase();
        String wanted = word.toLowerCase();
        for (int at = lower.indexOf(wanted); at >= 0; at = lower.indexOf(wanted, at + 1)) {
            char before = at == 0 ? ' ' : text.charAt(at - 1);
            int after = at + wanted.length();
            char next = after >= text.length() ? ' ' : text.charAt(after);
            if (before == '"' || next == '"') continue;
            if (Character.isLetterOrDigit(before) || before == '_' || before == '.'
                    || Character.isLetterOrDigit(next) || next == '_' || next == '.') {
                continue;
            }
            return at;
        }
        return -1;
    }

    /**
     * A rule as {@code pg_get_ruledef} writes it: the header on its own line, the event indented
     * under it, the relation schema-qualified, and ALSO left unwritten because it is the default.
     * A qualification goes on a line of its own between the two.
     */
    private String ruleDefinitionText(CreateRuleStmt s, boolean instead) {
        StringBuilder sb = new StringBuilder("CREATE RULE ").append(s.name()).append(" AS");
        sb.append("\n    ON ").append(s.event()).append(" TO ")
                .append(ruleSchema(s)).append('.').append(s.table());
        if (s.whereClause() != null) {
            sb.append("\n   WHERE ").append(normaliseRuleQualification(s));
        }
        List<String> commands = s.commands();
        boolean nothing = commands.isEmpty()
                || (commands.size() == 1 && "NOTHING".equals(commands.get(0)));
        if (nothing) {
            // NOTHING is not a statement, so it does not carry the space a deparsed statement
            // brings with it: PostgreSQL writes DO NOTHING where an action gives DO  DELETE FROM.
            return sb.append(" DO ").append(instead ? "INSTEAD " : "").append("NOTHING;").toString();
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
            Table on = executor.resolveTable(ruleSchema(s), s.table(), s.schema() != null);
            // A qualification is resolved against the rows the event has: an INSERT has only the
            // new row and a DELETE only the old one, so a column written bare there is that row's
            // and PostgreSQL writes it back under that row's name. An UPDATE has both rows, which
            // is why a bare column there names neither and is refused as ambiguous.
            String row = "INSERT".equals(s.event()) ? "NEW"
                    : "DELETE".equals(s.event()) ? "OLD" : null;
            return lowerRowAliases(RuleDeparser.deparse(parsed, row == null
                    ? RuleDeparser.forTable(on) : RuleDeparser.forRuleTarget(row, on)));
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
     * and each value printed in the column's own type, and an UPDATE or a DELETE gets every column
     * printed under the relation it was resolved to. Anything this engine cannot rewrite that
     * way is echoed as it was written, which is what the whole definition used to be.
     */
    private String normaliseRuleAction(String action) {
        try {
            com.memgres.engine.parser.ast.Statement parsed =
                    com.memgres.engine.parser.Parser.parse(action);
            if (parsed instanceof UpdateStmt) {
                return normaliseUpdateAction((UpdateStmt) parsed, action);
            }
            if (parsed instanceof DeleteStmt) {
                return normaliseDeleteAction((DeleteStmt) parsed, action);
            }
            // An action that only reads is a query like any other, and PostgreSQL writes it out the
            // way it writes a view's: laid out, with the relation in front of every column, because
            // a rule is analysed against a range table holding OLD and NEW beside what it reads.
            if (parsed instanceof SelectStmt || parsed instanceof SetOpStmt) {
                return normaliseSelectAction(parsed, action);
            }
            if (!(parsed instanceof InsertStmt)) return action;
            InsertStmt ins = (InsertStmt) parsed;
            if (ins.selectStmt() != null) return normaliseInsertSelectAction(ins, action);
            if (ins.values() == null || ins.values().isEmpty()) return action;
            Table target = executor.resolveTable(
                    ins.schema() == null ? executor.defaultSchema() : ins.schema(), ins.table());
            List<String> columnNames = new ArrayList<>();
            if (ins.columns() != null && !ins.columns().isEmpty()) {
                columnNames.addAll(ins.columns());
            } else {
                for (Column c : target.getColumns()) columnNames.add(c.getName());
            }
            // PostgreSQL settles which relation an action writes to when the rule is written, and
            // then prints it without its schema wherever the reader's search path reaches it. So
            // the schema the name resolved to is what is stored, and the reading takes it off
            // again -- how the definition reads is the reader's search path's business, not the
            // writer's, and echoing the qualification as written answered for neither.
            String actionSchema = ins.schema() != null ? ins.schema()
                    : executor.relationSchemaOf(null, ins.table());
            StringBuilder sb = new StringBuilder("INSERT INTO ")
                    .append(actionSchema == null ? "" : actionSchema + ".")
                    .append(ins.table()).append(" (");
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
            appendActionTail(sb, null, ins.returning(), target,
                    ins.alias() != null ? ins.alias() : ins.table());
            return lowerRowAliases(sb.toString());
        } catch (RuntimeException e) {
            return action;
        }
    }

    /**
     * A rule action that only reads, as {@code pg_get_ruledef} writes it. A clause the deparser
     * has nothing to say for is not one to write half of: such an action is echoed as it was
     * written, which is what every reading action used to be.
     */
    private String normaliseSelectAction(Statement query, String action) {
        if (query instanceof SelectStmt) {
            SelectStmt select = (SelectStmt) query;
            if (select.lockClause() != null) return action;
            if (select.groupingSets() != null && !select.groupingSets().isEmpty()) return action;
        }
        Statement settled = withStarsSettled(query);
        return lowerRowAliases(withoutLeadingSpace(ViewDeparser.ruleQuery(settled, 0, true,
                ViewDeparser.columnTypesOf(executor.database, settled, executor.defaultSchema()))));
    }

    /**
     * An INSERT whose rows come from a query, as {@code pg_get_ruledef} writes it: the columns
     * being written listed out, and the query deparsed under them. The query's own column names go
     * unwritten, because an INSERT reads the rows it is given by position and never by name.
     */
    private String normaliseInsertSelectAction(InsertStmt ins, String action) {
        if (ins.onConflict() != null) return action;
        if (ins.withClauses() != null && !ins.withClauses().isEmpty()) return action;
        Table target = executor.resolveTable(
                ins.schema() == null ? executor.defaultSchema() : ins.schema(), ins.table());
        Statement source = withStarsSettled(ins.selectStmt());
        List<String> columnNames = new ArrayList<>();
        if (ins.columns() != null && !ins.columns().isEmpty()) {
            columnNames.addAll(ins.columns());
        } else {
            // Written with no column list, an INSERT writes the relation's first columns, as many
            // of them as the query hands it. A query whose width the text does not settle -- a
            // star this could not read -- stands for all of them.
            int width = SelectStmt.writtenWidth(source);
            for (Column c : target.getColumns()) {
                if (width >= 0 && columnNames.size() >= width) break;
                columnNames.add(c.getName());
            }
        }
        String actionSchema = ins.schema() != null ? ins.schema()
                : executor.relationSchemaOf(null, ins.table());
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(actionSchema == null ? "" : actionSchema + ".")
                .append(ins.table()).append(" (");
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(columnNames.get(i));
        }
        sb.append(") ").append(ViewDeparser.ruleQuery(source, ACTION_CLAUSE_INDENT, false,
                ViewDeparser.columnTypesOf(executor.database, source, executor.defaultSchema())));
        appendActionTail(sb, null, ins.returning(), target,
                ins.alias() != null ? ins.alias() : ins.table());
        return lowerRowAliases(sb.toString());
    }

    /**
     * A star in a rule action written out as the columns it stood for.
     *
     * <p>PostgreSQL settles what a star means when the rule is written and records those columns,
     * so the definition goes on naming them however the relation changes afterwards. A star this
     * cannot settle -- one over anything but a plain relation, or over a relation that is not
     * there -- is left the star it was, and the action goes on being read that way.
     */
    private Statement withStarsSettled(Statement query) {
        if (!(query instanceof SelectStmt)) return query;
        SelectStmt select = (SelectStmt) query;
        List<SelectStmt.SelectTarget> targets = select.targets();
        List<SelectStmt.FromItem> from = select.from();
        if (targets == null || from == null || from.isEmpty()) return query;
        boolean anyStar = false;
        for (SelectStmt.SelectTarget item : targets) {
            if (item.expr() instanceof WildcardExpr) { anyStar = true; break; }
        }
        if (!anyStar) return query;
        List<String> names = new ArrayList<>();
        List<Table> relations = new ArrayList<>();
        for (SelectStmt.FromItem item : from) {
            if (!(item instanceof SelectStmt.TableRef)) return query;
            SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
            if (ref.columnAliases() != null && !ref.columnAliases().isEmpty()) return query;
            Table relation;
            try {
                relation = executor.resolveTable(
                        ref.schema() == null ? executor.defaultSchema() : ref.schema(), ref.table());
            } catch (RuntimeException notThere) {
                return query;
            }
            if (relation == null) return query;
            names.add(ref.alias() != null ? ref.alias() : ref.table());
            relations.add(relation);
        }
        List<SelectStmt.SelectTarget> settled = new ArrayList<>();
        for (SelectStmt.SelectTarget item : targets) {
            if (!(item.expr() instanceof WildcardExpr)) { settled.add(item); continue; }
            String over = ((WildcardExpr) item.expr()).table();
            boolean stood = false;
            for (int i = 0; i < names.size(); i++) {
                if (over != null && !over.equalsIgnoreCase(names.get(i))) continue;
                stood = true;
                for (Column c : relations.get(i).getColumns()) {
                    settled.add(new SelectStmt.SelectTarget(
                            new ColumnRef(names.get(i), c.getName()), null));
                }
            }
            if (!stood) return query;
        }
        return new SelectStmt(select.distinct(), select.distinctOn(), settled, select.from(),
                select.where(), select.groupBy(), select.having(), select.windowDefs(),
                select.orderBy(), select.limit(), select.offset(), select.withClauses(),
                select.groupingSets(), select.lockClause(), select.withTies());
    }

    /**
     * The indentation a rule action's clauses have reached by the time one of them writes a query:
     * PostgreSQL moves in one step for the statement itself, so everything inside it stands there.
     */
    private static final int ACTION_CLAUSE_INDENT = 8;

    /**
     * A deparsed statement without the space PostgreSQL writes in front of every one of them. The
     * rule writes that space itself, in the place ALSO would have stood.
     */
    private static String withoutLeadingSpace(String written) {
        return written.startsWith(" ") ? written.substring(1) : written;
    }

    /**
     * An UPDATE action as {@code pg_get_ruledef} writes it.
     *
     * <p>A relation the action reads beside the one it writes to stands on a line of its own, the
     * way a query's FROM does; a column written without a qualifier is still read against the
     * relation being written, which is where an assignment's own column comes from.
     */
    private String normaliseUpdateAction(UpdateStmt upd, String action) {
        if (upd.withClauses() != null && !upd.withClauses().isEmpty()) return action;
        if (upd.setClauses() == null || upd.setClauses().isEmpty()) return action;
        Table target = executor.resolveTable(
                upd.schema() == null ? executor.defaultSchema() : upd.schema(), upd.table());
        String named = upd.alias() != null ? upd.alias() : upd.table();
        RuleDeparser.ColumnTypes types = RuleDeparser.forRuleAction(named, target,
                executor.database, executor.defaultSchema());
        StringBuilder sb = new StringBuilder("UPDATE ");
        if (upd.only()) sb.append("ONLY ");
        sb.append(actionRelation(upd.schema(), upd.table(), upd.alias())).append(" SET ");
        for (int i = 0; i < upd.setClauses().size(); i++) {
            InsertStmt.SetClause set = upd.setClauses().get(i);
            // An assignment naming part of a value is written back with the brackets or the field
            // that named it, which this cannot put back, so it is left as it was written.
            if (set.subField() != null || set.subscripts() != null) return action;
            if (i > 0) sb.append(", ");
            int at = target.getColumnIndex(set.column());
            RuleDeparser.PgType want = at < 0 ? null
                    : RuleDeparser.fromColumn(target.getColumns().get(at));
            sb.append(set.column()).append(" = ")
                    .append(RuleDeparser.deparseValue(set.value(), want, types));
        }
        if (upd.from() != null && !upd.from().isEmpty()) {
            sb.append(ViewDeparser.ruleFromClause(upd.from()));
        }
        appendActionTail(sb, upd.where(), upd.returning(), target, named);
        return lowerRowAliases(sb.toString());
    }

    /** A DELETE action as {@code pg_get_ruledef} writes it. */
    private String normaliseDeleteAction(DeleteStmt del, String action) {
        if (del.using() != null && !del.using().isEmpty()) return action;
        if (del.withClauses() != null && !del.withClauses().isEmpty()) return action;
        Table target = executor.resolveTable(
                del.schema() == null ? executor.defaultSchema() : del.schema(), del.table());
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        if (del.only()) sb.append("ONLY ");
        sb.append(actionRelation(del.schema(), del.table(), del.alias()));
        appendActionTail(sb, del.where(), del.returning(), target,
                del.alias() != null ? del.alias() : del.table());
        return lowerRowAliases(sb.toString());
    }

    /**
     * The relation an action writes to, under the schema it resolved to and beside the alias it
     * was given. PostgreSQL settles which relation the action names as the rule is written and
     * then prints it without its schema wherever the reader's search path reaches it, so the
     * schema is what is stored and the reading takes it off again. The alias is kept because it is
     * the name the action's own columns are written under; the AS that introduced it is not, since
     * what was analysed is the alias rather than the word.
     */
    private String actionRelation(String writtenSchema, String table, String alias) {
        String schema = writtenSchema != null ? writtenSchema
                : executor.relationSchemaOf(null, table);
        return (schema == null ? "" : schema + ".") + table + (alias == null ? "" : " " + alias);
    }

    /**
     * The clauses that close a rule action. PostgreSQL indents a deparsed rule even where it is
     * not asked to print prettily, so a qualification and a returning list each begin a line of
     * their own and every returning item after the first stands on a line of its own too.
     */
    private void appendActionTail(StringBuilder sb, Expression where,
                                  List<SelectStmt.SelectTarget> returning,
                                  Table target, String named) {
        RuleDeparser.ColumnTypes types = RuleDeparser.forRuleAction(named, target,
                executor.database, executor.defaultSchema());
        if (where != null) {
            sb.append("\n  WHERE ").append(RuleDeparser.deparse(where, types));
        }
        if (returning == null || returning.isEmpty()) return;
        List<String> items = new ArrayList<>();
        for (SelectStmt.SelectTarget item : returning) {
            // A star is not kept as a star: what was analysed is the list of columns it stood for,
            // and that list is what is read back.
            if (item.expr() instanceof WildcardExpr) {
                for (Column c : target.getColumns()) {
                    items.add(RuleDeparser.quoteIdentifier(named) + "."
                            + RuleDeparser.quoteIdentifier(c.getName()));
                }
                continue;
            }
            // A label the item would answer to anyway is not written out again: PostgreSQL writes
            // AS only where the name asked for differs from the one the expression carries.
            String label = item.alias();
            if (label != null && item.expr() instanceof ColumnRef
                    && label.equals(((ColumnRef) item.expr()).column())) {
                label = null;
            }
            String written = RuleDeparser.deparse(item.expr(), types);
            items.add(label == null ? written : written + " AS " + label);
        }
        for (int i = 0; i < items.size(); i++) {
            sb.append(i == 0 ? "\n  RETURNING " : ",\n    ").append(items.get(i));
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
        if (!s.orReplace() && executor.database.hasRule(ruleSchema(s), s.name(), s.table())) {
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
        // Every other column the qualification names is resolved here too. A qualification reads
        // OLD and NEW unqualified as well as by name -- PostgreSQL puts both in scope for it, and
        // for no action -- so a bare column of the ruled relation is one of its own.
        if (on != null && !executor.database.hasView(s.table())) {
            rejectMissingRuleColumns(qualification, ruleColumnScope(qualification, on, true));
        }
        // An UPDATE carries the row as it was and the row as it will be, and a qualification reads
        // both of them by name alone, so a column written there with no row in front of it answers
        // to both at once and PostgreSQL will not choose between them. An INSERT has only the new
        // row and a DELETE only the old one, which is why the same column is read there.
        if ("UPDATE".equals(s.event())) {
            // A system column is one every row of a table carries, so it stands in both of them
            // as well; a view's rows carry none, and a name written there is simply missing.
            rejectAmbiguousRowColumn(qualification, ruledRelationColumns(s, on),
                    !executor.database.hasView(s.table()));
        }
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
                executor.database, ruleSchema(s), s.table());
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
        // Whether an action already carried a RETURNING list, which is what the rule answers with.
        boolean seenReturning = false;
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
            // Every other column the action names is resolved against the relations the action
            // itself holds. OLD and NEW are in scope for an action by name alone, and so is the
            // relation an INSERT writes to, so a bare column of either is not one the action can
            // reach: PostgreSQL reports it missing and says the name it names is in a table this
            // part of the query cannot reference.
            rejectMissingRuleColumns(parsed, ruleColumnScope(parsed, rowSource, false));
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
            seenReturning |= checkRuleReturning(s, parsed, rowSource, seenReturning);
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
        MemgresException e = new MemgresException("column " + ref.table().toLowerCase() + "."
                + ref.column() + " does not exist", "42703");
        // OLD and NEW are relations in the rewritten query, so a near miss among the ruled
        // relation's columns is offered under the name the rule wrote rather than bare.
        String hint = RowContext.suggestClosestColumn(ref.column(), Collections.singletonList(
                new RowContext.TableBinding(on, ref.table().toLowerCase(), null)));
        if (hint != null) e.setHint(hint);
        throw e;
    }

    /** The schema the relation a rule is written on is looked for in, and the rule is filed in. */
    private String ruleSchema(CreateRuleStmt s) {
        return executor.relationSchemaOf(s.schema(), s.table());
    }

    /**
     * The relations a rule's action or qualification resolves its columns against, or null where
     * something in it answers to names no relation holds: a CTE, a subquery or a function written
     * in a FROM clause, an alias list that renames a relation's columns, or a view, whose columns
     * are its own rather than those of the relation this resolves it to. Answering null leaves the
     * columns unresolved, which is what became of all of them before.
     */
    private List<RowContext.TableBinding> ruleColumnSources(Object node) {
        final Set<String> withItems = new LinkedHashSet<>();
        final List<SelectStmt.TableRef> written = new ArrayList<>();
        final boolean[] opaque = new boolean[1];
        AstWalk.forEach(node, n -> {
            if (n instanceof SelectStmt.CommonTableExpr) {
                String cte = ((SelectStmt.CommonTableExpr) n).name;
                if (cte != null) withItems.add(cte.toLowerCase());
            } else if (n instanceof SelectStmt.SubqueryFrom || n instanceof SelectStmt.FunctionFrom) {
                opaque[0] = true;
            } else if (n instanceof SelectStmt.TableRef) {
                written.add((SelectStmt.TableRef) n);
            }
        });
        if (opaque[0]) return null;
        List<RowContext.TableBinding> sources = new ArrayList<>();
        // The relation a statement writes to answers for its own columns too: an UPDATE's
        // assignments and a DELETE's WHERE are read against the rows being written.
        if (node instanceof InsertStmt) {
            InsertStmt insert = (InsertStmt) node;
            if (!addRuleColumnSource(sources, insert.schema(), insert.table(), null)) return null;
        } else if (node instanceof UpdateStmt) {
            UpdateStmt update = (UpdateStmt) node;
            if (!addRuleColumnSource(sources, update.schema(), update.table(), null)) return null;
        } else if (node instanceof DeleteStmt) {
            DeleteStmt delete = (DeleteStmt) node;
            if (!addRuleColumnSource(sources, delete.schema(), delete.table(), null)) return null;
        }
        for (SelectStmt.TableRef ref : written) {
            if (ref.table() == null || withItems.contains(ref.table().toLowerCase())) return null;
            if (ref.columnAliases() != null && !ref.columnAliases().isEmpty()) return null;
            if (!addRuleColumnSource(sources, ref.schema(), ref.table(), ref.alias())) return null;
        }
        return sources;
    }

    /**
     * Binds one relation an action names under the name its columns answer to, which is its alias
     * wherever it was given one.
     *
     * @return false where the name reaches nothing whose columns can be read here
     */
    private boolean addRuleColumnSource(List<RowContext.TableBinding> sources,
                                        String schema, String table, String alias) {
        if (table == null) return false;
        // A catalog relation is described rather than stored, so there is no relation to read.
        if (SystemCatalog.isSystemCatalog(schema, table)) return false;
        if (executor.database.getView(table) != null) return false;
        Table resolved;
        try {
            resolved = executor.resolveTable(
                    schema == null ? executor.defaultSchema() : schema, table);
        } catch (RuntimeException unreachable) {
            return false; // a name that reaches nothing was already reported as such
        }
        if (resolved == null || resolved.isFunctionResult()) return false;
        sources.add(new RowContext.TableBinding(resolved, alias == null ? table : alias, null));
        return true;
    }

    /**
     * What a rule's action or qualification reads its columns against.
     *
     * <p>PostgreSQL keeps two lists of relations while it writes a rule, and they are not the
     * same one. The range table holds every relation the statement names, OLD and NEW among them;
     * the namespace holds the ones a bare column may be read from. An action reaches OLD and NEW
     * by name alone, and the relation an INSERT writes to the same way -- that relation's columns
     * answer bare inside a RETURNING list or an ON CONFLICT clause, which read the row being
     * written, and nowhere else. What is in the namespace settles whether a column is there; what
     * is in the range table is what the complaint about one that is not is worded from.
     */
    private static final class RuleColumnScope {
        private final List<RowContext.TableBinding> rangeTable = new ArrayList<>();
        private final List<RowContext.TableBinding> namespace = new ArrayList<>();
        private final List<RowContext.TableBinding> writtenTo = new ArrayList<>();
        private final Set<Object> readsWrittenTo =
                Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
        // The references standing where EXCLUDED is in scope beside the relation being written to.
        private Set<Object> besideExcluded =
                Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());

        /** The relations a bare column standing at this node may be read from. */
        private List<RowContext.TableBinding> visibleTo(Object ref) {
            if (writtenTo.isEmpty() || !readsWrittenTo.contains(ref)) return namespace;
            List<RowContext.TableBinding> both = new ArrayList<>(namespace);
            both.addAll(writtenTo);
            return both;
        }

        /** The relations a qualified column may name, the one being written to among them. */
        private List<RowContext.TableBinding> named() {
            List<RowContext.TableBinding> all = new ArrayList<>(writtenTo);
            all.addAll(namespace);
            return all;
        }
    }

    /**
     * The scope one action or one qualification is read in, or null where something in it answers
     * to names no relation holds.
     *
     * @param rowsUnqualified whether a bare column may be read from OLD and NEW, which is so of a
     *                        qualification and of no action
     */
    private RuleColumnScope ruleColumnScope(Object node, Table rowSource, boolean rowsUnqualified) {
        List<RowContext.TableBinding> named = ruleColumnSources(node);
        if (named == null) return null;
        final RuleColumnScope scope = new RuleColumnScope();
        if (rowSource != null) {
            List<RowContext.TableBinding> rows = Cols.listOf(
                    new RowContext.TableBinding(rowSource, "old", null),
                    new RowContext.TableBinding(rowSource, "new", null));
            scope.rangeTable.addAll(rows);
            if (rowsUnqualified) scope.namespace.addAll(rows);
        }
        scope.rangeTable.addAll(named);
        List<RowContext.TableBinding> readable = new ArrayList<>(named);
        if (node instanceof InsertStmt && !readable.isEmpty()) {
            // The relation being written to comes first out of ruleColumnSources. A value being
            // written is not read from the row it is being written into, so it is out of the
            // namespace -- except where the statement reads that row back.
            scope.writtenTo.add(readable.remove(0));
            InsertStmt insert = (InsertStmt) node;
            AstWalk.forEach(insert.returning(), n -> {
                if (n instanceof ColumnRef) scope.readsWrittenTo.add(n);
            });
            AstWalk.forEach(insert.onConflict(), n -> {
                if (n instanceof ColumnRef) scope.readsWrittenTo.add(n);
            });
            scope.besideExcluded = columnRefsBesideExcluded(insert.onConflict());
        }
        scope.namespace.addAll(readable);
        return scope;
    }

    /**
     * The columns written where EXCLUDED stands beside the relation being written to: the
     * assignments of an ON CONFLICT DO UPDATE and its own WHERE.
     *
     * <p>A sub-select written inside one of them brings relations of its own and is left out, so
     * that a name it reads is judged against those rather than against the two rows the conflict
     * clause holds.
     */
    private static Set<Object> columnRefsBesideExcluded(InsertStmt.OnConflict onConflict) {
        Set<Object> refs = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
        if (onConflict == null) return refs;
        Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
        final Deque<Object> queue = new ArrayDeque<>();
        if (onConflict.doUpdate() != null) {
            for (InsertStmt.SetClause set : onConflict.doUpdate()) {
                if (set.value() != null) queue.add(set.value());
            }
        }
        if (onConflict.doUpdateWhereClause() != null) queue.add(onConflict.doUpdateWhereClause());
        while (!queue.isEmpty()) {
            Object node = queue.poll();
            if (node instanceof com.memgres.engine.parser.ast.Statement) continue;
            if (!seen.add(node)) continue;
            if (node instanceof ColumnRef) {
                refs.add(node);
                continue;
            }
            AstWalk.forEachChild(node, queue::add);
        }
        return refs;
    }

    /**
     * Refuses the first column a rule names that nothing in scope where it stands supplies.
     *
     * <p>A name the range table holds out of reach of the part of the query that wrote it is
     * reported as such rather than as a name nothing answers to: PostgreSQL names the one relation
     * that holds it, or says there are several and to write a qualified name. Only where no
     * relation holds the name at all does it look for a near miss.
     */
    private static void rejectMissingRuleColumns(Object node, RuleColumnScope scope) {
        if (scope == null) return;
        final List<ColumnRef> refs = new ArrayList<>();
        final Set<String> outputNames = new LinkedHashSet<>();
        AstWalk.forEach(node, n -> {
            if (n instanceof ColumnRef) refs.add((ColumnRef) n);
            // A name the select list gives an expression is one ORDER BY and GROUP BY answer to.
            if (n instanceof SelectStmt.SelectTarget) {
                String alias = ((SelectStmt.SelectTarget) n).alias();
                if (alias != null) outputNames.add(alias.toLowerCase());
            }
        });
        for (ColumnRef ref : refs) {
            String column = ref.column();
            if (column == null || "*".equals(column)
                    || DdlDefinitionChecks.isSystemColumnName(column)) {
                continue;
            }
            if (ref.table() == null) {
                List<RowContext.TableBinding> readable = scope.visibleTo(ref);
                // Inside an ON CONFLICT DO UPDATE the row already there and the row being written
                // are both in scope, and EXCLUDED holds every column the relation holds, so a
                // column of it written without a relation name answers to both. PostgreSQL refuses
                // to choose between them rather than pick one.
                if (scope.besideExcluded.contains(ref)
                        && suppliesRuleColumn(scope.writtenTo, column)) {
                    throw new MemgresException(
                            "column reference \"" + column + "\" is ambiguous", "42702");
                }
                if (suppliesRuleColumn(readable, column)) continue;
                if (outputNames.contains(column.toLowerCase())) continue;
                MemgresException e = new MemgresException(
                        "column \"" + column + "\" does not exist", "42703");
                List<String> outOfReach = new ArrayList<>();
                for (RowContext.TableBinding b : scope.rangeTable) {
                    if (isOneOf(readable, b) || b.table().getColumnIndex(column) < 0) continue;
                    outOfReach.add(b.alias() != null ? b.alias() : b.table().getName());
                }
                if (outOfReach.size() == 1) {
                    e.setDetail("There is a column named \"" + column + "\" in table \""
                            + outOfReach.get(0) + "\", but it cannot be referenced from this part"
                            + " of the query.");
                } else if (!outOfReach.isEmpty()) {
                    e.setDetail("There are columns named \"" + column + "\", but they are in"
                            + " tables that cannot be referenced from this part of the query.");
                    e.setHint("Try using a table-qualified name.");
                } else {
                    String hint = RowContext.suggestClosestColumnAcross(column, scope.rangeTable);
                    if (hint != null) e.setHint(hint);
                }
                throw e;
            }
            // A qualifier written with a schema of its own names the relation in that schema, and
            // one nothing in scope answers to names something else again -- a relation an
            // enclosing query holds, a composite value, a type being cast to.
            if (ref.schema() != null) continue;
            List<RowContext.TableBinding> qualified = new ArrayList<>();
            for (RowContext.TableBinding b : scope.named()) {
                if (b.alias() != null && b.alias().equalsIgnoreCase(ref.table())) qualified.add(b);
            }
            if (qualified.isEmpty() || suppliesRuleColumn(qualified, column)) continue;
            MemgresException e = new MemgresException(
                    "column " + ref.table() + "." + column + " does not exist", "42703");
            String hint = RowContext.suggestClosestColumn(column, qualified);
            if (hint != null) e.setHint(hint);
            throw e;
        }
    }

    /** Whether this very binding is one of them: one relation bound twice is bound twice. */
    private static boolean isOneOf(List<RowContext.TableBinding> bindings,
                                   RowContext.TableBinding one) {
        for (RowContext.TableBinding b : bindings) {
            if (b == one) return true;
        }
        return false;
    }

    /**
     * What a rule's action may hand back. PostgreSQL answers the statement that fired the rule
     * from the action's RETURNING list, so only a rule that stands in for the statement may carry
     * one -- there is nothing for a rule that runs beside it to answer with -- and the list has to
     * describe the relation the rule is on, whatever the relation the action writes to holds.
     */
    private boolean checkRuleReturning(CreateRuleStmt s,
                                       com.memgres.engine.parser.ast.Statement parsed,
                                       Table on, boolean seenReturning) {
        List<SelectStmt.SelectTarget> returning = null;
        String actionSchema = null;
        String actionTable = null;
        if (parsed instanceof InsertStmt) {
            returning = ((InsertStmt) parsed).returning();
            actionSchema = ((InsertStmt) parsed).schema();
            actionTable = ((InsertStmt) parsed).table();
        } else if (parsed instanceof UpdateStmt) {
            returning = ((UpdateStmt) parsed).returning();
            actionSchema = ((UpdateStmt) parsed).schema();
            actionTable = ((UpdateStmt) parsed).table();
        } else if (parsed instanceof DeleteStmt) {
            returning = ((DeleteStmt) parsed).returning();
            actionSchema = ((DeleteStmt) parsed).schema();
            actionTable = ((DeleteStmt) parsed).table();
        }
        if (returning == null || returning.isEmpty()) return false;
        // The statement a rule stands in for is answered from one action's RETURNING list, so
        // there is no choosing between two of them; and a rule that only speaks for some of the
        // rows leaves the rest with nothing to be answered from at all.
        if (seenReturning) {
            throw PgErrors.notImplemented("cannot have multiple RETURNING lists in a rule");
        }
        if (s.whereClause() != null) {
            throw PgErrors.notImplemented(
                    "RETURNING lists are not supported in conditional rules");
        }
        if (!"INSTEAD".equals(s.action())) {
            throw PgErrors.notImplemented(
                    "RETURNING lists are not supported in non-INSTEAD rules");
        }
        // The list has to describe the relation the rule is on, and a view's columns are its own
        // rather than those of the relation this resolves the view to.
        List<Column> wanted = ruledRelationColumns(s, on);
        List<Column> entries = returningEntryColumns(returning, actionSchema, actionTable, wanted);
        if (wanted == null || entries == null) return true;
        if (entries.size() != wanted.size()) {
            throw new MemgresException("RETURNING list has too "
                    + (entries.size() < wanted.size() ? "few" : "many") + " entries", "42P17");
        }
        for (int i = 0; i < entries.size(); i++) {
            // An entry whose type this cannot name is left unjudged rather than guessed at, and a
            // column of a view the query behind it settled no type for cannot be judged against.
            if (entries.get(i) == null || entries.get(i).getType() == null
                    || wanted.get(i).getType() == null) {
                continue;
            }
            String gave = CatalogHelper.pgTypeName(entries.get(i).getType());
            String needs = CatalogHelper.pgTypeName(wanted.get(i).getType());
            if (gave.equals(needs)) continue;
            MemgresException e = new MemgresException("RETURNING list's entry " + (i + 1)
                    + " has different type from column \"" + wanted.get(i).getName() + "\"",
                    "42P17");
            e.setDetail("RETURNING list entry has type " + gave + ", but column has type "
                    + needs + ".");
            throw e;
        }
        return true;
    }

    /**
     * The columns an action's RETURNING list stands for, one per entry, with a null where the
     * entry is an expression this cannot name a type for. A star is expanded against the relation
     * the action writes to, because that is what settles how many entries the list really has.
     * Null where the list holds something whose width cannot be worked out here.
     *
     * @param ruled the columns of the relation the rule is on, which OLD and NEW stand for
     */
    private List<Column> returningEntryColumns(List<SelectStmt.SelectTarget> returning,
                                               String actionSchema, String actionTable,
                                               List<Column> ruled) {
        Table target;
        try {
            target = actionTable == null ? null : executor.resolveTable(
                    actionSchema == null ? executor.defaultSchema() : actionSchema, actionTable);
        } catch (RuntimeException unreachable) {
            return null; // a relation this cannot open says nothing about the list's width
        }
        if (target == null) return null;
        List<Column> entries = new ArrayList<>();
        for (SelectStmt.SelectTarget entry : returning) {
            if (entry.expr() instanceof WildcardExpr) {
                WildcardExpr star = (WildcardExpr) entry.expr();
                if (star.table() != null && !star.table().equalsIgnoreCase(target.getName())) {
                    return null;
                }
                entries.addAll(target.getColumns());
                continue;
            }
            if (entry.expr() instanceof ColumnRef) {
                ColumnRef ref = (ColumnRef) entry.expr();
                if (ref.table() != null && ("old".equalsIgnoreCase(ref.table())
                        || "new".equalsIgnoreCase(ref.table()))) {
                    entries.add(namedColumn(ruled, ref.column()));
                    continue;
                }
                int index = target.getColumnIndex(ref.column());
                entries.add(index >= 0 ? target.getColumns().get(index) : null);
                continue;
            }
            entries.add(null);
        }
        return entries;
    }

    /** The column of this list that answers to a name, or null when none does. */
    private static Column namedColumn(List<Column> columns, String name) {
        if (columns == null || name == null) return null;
        for (Column column : columns) {
            if (name.equalsIgnoreCase(column.getName())) return column;
        }
        return null;
    }

    /**
     * The columns the relation a rule is on answers with. A view's are its own rather than those
     * of the relation behind it: PostgreSQL resolves what a rule hands back, and what its
     * qualification names, against the view's own column list, while the relation a view resolves
     * to here is the one underneath it, whose columns may be named and typed differently. Null
     * where this cannot say what they are, which leaves whatever is judged against them unjudged.
     */
    private List<Column> ruledRelationColumns(CreateRuleStmt s, Table on) {
        Database.ViewDef view = executor.database.getView(ruleSchema(s), s.table());
        if (view == null) return on == null ? null : on.getColumns();
        List<Column> columns = view.cachedColumns();
        return columns == null || columns.isEmpty() ? null : columns;
    }

    /**
     * Refuses a column an UPDATE rule's qualification names without saying which of its two rows
     * it belongs to. Both rows are in scope there, so such a name answers to a column of each of
     * them at once and PostgreSQL refuses rather than pick one. A name written inside a sub-select
     * of the qualification is left alone: it reads from the relations that sub-select names.
     */
    private static void rejectAmbiguousRowColumn(Expression qualification, List<Column> ruled,
                                                 boolean rowsCarrySystemColumns) {
        if (qualification == null || ruled == null) return;
        final List<ColumnRef> bare = new ArrayList<>();
        AstWalk.forEachOutside(qualification,
                node -> node instanceof com.memgres.engine.parser.ast.Statement,
                node -> {
                    if (node instanceof ColumnRef && ((ColumnRef) node).table() == null) {
                        bare.add((ColumnRef) node);
                    }
                });
        for (ColumnRef ref : bare) {
            if (namedColumn(ruled, ref.column()) == null
                    && !(rowsCarrySystemColumns
                            && DdlDefinitionChecks.isSystemColumnName(ref.column()))) {
                continue;
            }
            throw new MemgresException(
                    "column reference \"" + ref.column() + "\" is ambiguous", "42702");
        }
    }

    /** Whether one of these relations holds a column of that name. */
    private static boolean suppliesRuleColumn(List<RowContext.TableBinding> sources, String column) {
        for (RowContext.TableBinding b : sources) {
            if (b.table().getColumnIndex(column) >= 0) return true;
        }
        return false;
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
