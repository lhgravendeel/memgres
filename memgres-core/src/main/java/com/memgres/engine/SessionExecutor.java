package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import com.memgres.engine.plpgsql.PlpgsqlExecutor;
import com.memgres.engine.util.Strs;

import java.util.*;

/**
 * Handles session management commands: SET/SHOW/RESET, COMMENT, DO blocks, DISCARD,
 * LOCK, GRANT/REVOKE, prepared statements, and cursors.
 * Extracted from AstExecutor to separate session concerns from expression evaluation.
 */
class SessionExecutor {

    private final AstExecutor executor;

    SessionExecutor(AstExecutor executor) {
        this.executor = executor;
    }

    // ---- SET / SHOW / RESET ----

    QueryResult executeSetStmt(SetStmt stmt) {
        GucSettings guc = executor.session != null ? executor.session.getGucSettings() : null;
        String name = stmt.name().toLowerCase();

        if (name.startsWith("comment:")) {
            String[] parts = stmt.name().split(":", 3);
            if (parts.length >= 3) {
                String objType = parts[1].toUpperCase();
                String objName = parts[2];
                // Strip schema prefix for resolution and storage (e.g., "public.customers" -> "customers")
                String schemaName = executor.defaultSchema();
                String bareName = objName;
                if (objName.contains(".") && !objType.equals("COLUMN")) {
                    int dot = objName.indexOf('.');
                    schemaName = objName.substring(0, dot);
                    bareName = objName.substring(dot + 1);
                }
                if (objType.equals("TABLE") || objType.equals("RELATION")) {
                    try {
                        executor.resolveTable(schemaName, bareName);
                    } catch (MemgresException e) {
                        throw new MemgresException("relation \"" + objName + "\" does not exist", "42P01");
                    }
                } else if (objType.equals("VIEW")) {
                    if (!executor.database.hasView(bareName)) {
                        throw new MemgresException("view \"" + objName + "\" does not exist", "42P01");
                    }
                } else if (objType.equals("INDEX")) {
                    // Allow COMMENT ON INDEX for both explicitly created indexes
                    // and PK/UNIQUE constraint-backed indexes (PG allows both).
                    if (!executor.database.hasIndex(bareName)) {
                        // Fallback: check if the name matches a constraint name on any
                        // table in the relevant schema (constraint-backed indexes like
                        // tablename_pkey are stored as constraints, not as indexes).
                        boolean foundConstraint = false;
                        Schema schema = executor.database.getSchema(schemaName);
                        if (schema != null) {
                            for (Table t : schema.getTables().values()) {
                                for (StoredConstraint sc : t.getConstraints()) {
                                    if (sc.getName() != null && sc.getName().equalsIgnoreCase(bareName)) {
                                        foundConstraint = true;
                                        break;
                                    }
                                }
                                if (foundConstraint) break;
                            }
                        }
                        if (!foundConstraint) {
                            throw new MemgresException("relation \"" + objName + "\" does not exist", "42P01");
                        }
                    }
                } else if (objType.equals("COLUMN")) {
                    // Column names are "table.col" or "schema.table.col"
                    if (objName.contains(".")) {
                        String tablePart = objName.substring(0, objName.lastIndexOf('.'));
                        String colPart = objName.substring(objName.lastIndexOf('.') + 1);
                        // tablePart may be schema-qualified: "public.customers"
                        String colSchema = executor.defaultSchema();
                        String colTable = tablePart;
                        if (tablePart.contains(".")) {
                            int dot = tablePart.indexOf('.');
                            colSchema = tablePart.substring(0, dot);
                            colTable = tablePart.substring(dot + 1);
                        }
                        try {
                            Table commentTable = executor.resolveTable(colSchema, colTable);
                            if (commentTable.getColumnIndex(colPart) < 0) {
                                throw new MemgresException("column \"" + colPart + "\" of relation \"" + colTable + "\" does not exist", "42703");
                            }
                        } catch (MemgresException e) {
                            if ("42703".equals(e.getSqlState())) throw e;
                            throw new MemgresException("relation \"" + tablePart + "\" does not exist", "42P01");
                        }
                        // Store with bare table.column name for consistent lookup
                        bareName = colTable + "." + colPart;
                    }
                } else if (objType.equals("SCHEMA")) {
                    String schemaN = bareName;
                    if (executor.database.getSchema(schemaN) == null) {
                        throw new MemgresException("schema \"" + schemaN + "\" does not exist", "3F000");
                    }
                }
                executor.database.addComment(parts[1].toLowerCase(), bareName.toLowerCase(), stmt.value());
            }
            return QueryResult.message(QueryResult.Type.SET, "COMMENT");
        }

        if (name.equals("do_block")) {
            String body = stmt.value();
            if (body != null && !Strs.isBlank(body)) {
                executePlpgsqlBlock(body);
            }
            return QueryResult.message(QueryResult.Type.SET, "DO");
        }

        if (name.equals("show")) {
            String param = stmt.value();
            if (param.equalsIgnoreCase("ALL")) {
                List<Column> cols = Cols.listOf(
                        new Column("name", DataType.TEXT, true, false, null),
                        new Column("setting", DataType.TEXT, true, false, null),
                        new Column("description", DataType.TEXT, true, false, null));
                List<Object[]> rows = new ArrayList<>();
                if (guc != null) {
                    for (Map.Entry<String, String> e : guc.getAll().entrySet()) {
                        rows.add(new Object[]{e.getKey(), e.getValue(), ""});
                    }
                }
                return QueryResult.select(cols, rows);
            }
            if (guc != null && !guc.isKnown(param) && !param.isEmpty() && !param.contains(".")) {
                throw new MemgresException("unrecognized configuration parameter \"" + param + "\"", "42704");
            }
            String value = guc != null ? guc.get(param) : null;
            // Bug fix: SHOW transaction_isolation should reflect default_transaction_isolation
            // when SET SESSION CHARACTERISTICS has been used but SET TRANSACTION has not.
            // The JDBC driver sets default_transaction_isolation via
            // "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ..." and reads it
            // back via "SHOW TRANSACTION ISOLATION LEVEL" (mapped to transaction_isolation).
            if (param.equalsIgnoreCase("transaction_isolation") && guc != null
                    && !guc.hasSessionOverride("transaction_isolation")) {
                String defaultLevel = guc.get("default_transaction_isolation");
                if (defaultLevel != null && !defaultLevel.isEmpty()) {
                    value = defaultLevel;
                }
            }
            if (value == null) value = "";
            // PG preserves the canonical parameter name case (e.g. "TimeZone" not "timezone")
            String colName = guc != null ? guc.getCanonicalName(param) : param;
            List<Column> cols = Cols.listOf(new Column(colName, DataType.TEXT, true, false, null));
            List<Object[]> rows = new ArrayList<>();
            rows.add(new Object[]{value});
            return QueryResult.select(cols, rows);
        }

        if (name.equals("reset")) {
            String param = stmt.value();
            if (guc != null) {
                if (param.equalsIgnoreCase("ALL")) {
                    guc.resetAll();
                } else {
                    guc.reset(param);
                }
            }
            return QueryResult.message(QueryResult.Type.SET, "RESET");
        }

        if (name.equals("max_connections")) {
            throw new MemgresException("parameter \"max_connections\" cannot be changed without restarting the server");
        }

        if (name.equals("role")) {
            String role = stmt.value();
            if (role != null && !role.equalsIgnoreCase("NONE") && !role.equalsIgnoreCase("DEFAULT")
                    && !role.equalsIgnoreCase("current_user") && !role.equalsIgnoreCase("session_user")) {
                String sessionUser = guc != null ? guc.get("session_authorization") : "test";
                if (sessionUser == null) sessionUser = "test";
                // Check if role exists
                if (!executor.database.getRoles().containsKey(role.toLowerCase())
                        && !role.equalsIgnoreCase(sessionUser)
                        && !role.equalsIgnoreCase("test") && !role.equalsIgnoreCase("postgres")) {
                    throw new MemgresException("invalid value for parameter \"role\": \"" + role + "\"", "22023");
                }
                // Superusers can SET ROLE to any role without membership.
                if (!role.equalsIgnoreCase(sessionUser)
                        && !sessionUser.equalsIgnoreCase("postgres")
                        && !sessionUser.equalsIgnoreCase("test")
                        && !sessionUser.equalsIgnoreCase("memgres")) {
                    // Check if session user is a member of target role
                    Set<String> members = executor.database.getRoleMemberships().get(role.toLowerCase());
                    if (members == null || !members.contains(sessionUser.toLowerCase())) {
                        throw new MemgresException("permission denied to set role \"" + role + "\"", "42501");
                    }
                }
            }
            if (guc != null) guc.set(name, stmt.value());
            return QueryResult.message(QueryResult.Type.SET, "SET");
        }

        if (name.equals("reindex")) {
            String val = stmt.value();
            if (val != null && val.contains(":")) {
                String[] reindexParts = val.split(":", 2);
                String targetType = reindexParts[0];
                String targetName = reindexParts[1];
                if (targetType.equals("TABLE")) {
                    executor.resolveTable(executor.defaultSchema(), targetName);
                } else if (targetType.equals("INDEX")) {
                    if (!executor.database.hasIndex(targetName)) {
                        // Fallback: PK/UNIQUE constraint-backed indexes are stored as
                        // constraints, not in the index map. Check constraint names.
                        boolean foundConstraint = false;
                        String schema = executor.defaultSchema();
                        Schema s = executor.database.getSchema(schema);
                        if (s != null) {
                            for (Table t : s.getTables().values()) {
                                for (StoredConstraint sc : t.getConstraints()) {
                                    if (sc.getName() != null && sc.getName().equalsIgnoreCase(targetName)) {
                                        foundConstraint = true;
                                        break;
                                    }
                                }
                                if (foundConstraint) break;
                            }
                        }
                        if (!foundConstraint) {
                            throw new MemgresException("index \"" + targetName + "\" does not exist", "42704");
                        }
                    }
                }
            }
            return QueryResult.message(QueryResult.Type.SET, "REINDEX");
        }

        if (name.equals("analyze") || name.equals("vacuum")) {
            String val = stmt.value();
            if (val != null && val.startsWith("table:")) {
                String tblName = val.substring("table:".length());
                executor.resolveTable(executor.defaultSchema(), tblName);
            }
            return QueryResult.message(QueryResult.Type.SET, name.equals("analyze") ? "ANALYZE" : "VACUUM");
        }

        // CREATE DATABASE / DROP DATABASE
        if (name.equals("create_database")) {
            if (executor.session != null && executor.session.isInTransaction()) {
                throw new MemgresException("CREATE DATABASE cannot run inside a transaction block", "25001");
            }
            String dbName = stmt.value();
            DatabaseRegistry reg = executor.session != null ? executor.session.getDatabaseRegistry() : null;
            if (reg == null) {
                // No registry -- fall through as noop for backward compat
            } else if (reg.exists(dbName)) {
                throw new MemgresException("database \"" + dbName + "\" already exists", "42P04");
            } else {
                reg.createDatabase(dbName);
            }
            return QueryResult.message(QueryResult.Type.SET, "CREATE DATABASE");
        }
        if (name.startsWith("drop_database")) {
            if (executor.session != null && executor.session.isInTransaction()) {
                throw new MemgresException("DROP DATABASE cannot run inside a transaction block", "25001");
            }
            boolean ifExists = name.contains("if_exists");
            boolean force = name.contains("force");
            String dbName = stmt.value();
            DatabaseRegistry reg = executor.session != null ? executor.session.getDatabaseRegistry() : null;
            if (reg == null) {
                // No registry -- fall through as noop for backward compat
            } else if (!reg.exists(dbName)) {
                if (!ifExists) {
                    throw new MemgresException("database \"" + dbName + "\" does not exist", "3D000");
                }
            } else {
                if (dbName.equals(executor.session.getDatabaseName())) {
                    throw new MemgresException("cannot drop the currently open database", "55006");
                }
                Database targetDb = reg.getDatabase(dbName);
                if (targetDb != null) {
                    java.util.Set<Session> otherSessions = targetDb.getActiveSessions();
                    if (!otherSessions.isEmpty()) {
                        if (force) {
                            for (Session s : new java.util.ArrayList<>(otherSessions)) {
                                s.close();
                            }
                        } else {
                            throw new MemgresException(
                                "database \"" + dbName + "\" is being accessed by other users", "55006");
                        }
                    }
                }
                reg.dropDatabase(dbName);
            }
            return QueryResult.message(QueryResult.Type.SET, "DROP DATABASE");
        }
        if (name.equals("alter_database_rename")) {
            if (executor.session != null && executor.session.isInTransaction()) {
                throw new MemgresException("ALTER DATABASE cannot run inside a transaction block", "25001");
            }
            String[] parts = stmt.value().split("\0");
            String oldName = parts[0];
            String newName = parts[1];
            DatabaseRegistry reg = executor.session != null ? executor.session.getDatabaseRegistry() : null;
            if (reg == null) {
                // noop
            } else if (!reg.exists(oldName)) {
                throw new MemgresException("database \"" + oldName + "\" does not exist", "3D000");
            } else if (reg.exists(newName)) {
                throw new MemgresException("database \"" + newName + "\" already exists", "42P04");
            } else if (oldName.equals(executor.session.getDatabaseName())) {
                throw new MemgresException("current database cannot be renamed", "55006");
            } else {
                Database targetDb = reg.getDatabase(oldName);
                if (targetDb != null && !targetDb.getActiveSessions().isEmpty()) {
                    throw new MemgresException(
                        "database \"" + oldName + "\" is being accessed by other users", "55006");
                }
                reg.renameDatabase(oldName, newName);
            }
            return QueryResult.message(QueryResult.Type.SET, "ALTER DATABASE");
        }

        Set<String> internalNames = Cols.setOf("constraints", "transaction",
                "create_noop", "alter_noop", "drop_noop",
                "drop_owned", "reassign_owned", "do_block", "comment",
                "security_label", "analyze", "vacuum",
                "cluster", "checkpoint", "load");

        // Accept unknown SET parameters silently because pg_dump and other tools send many
        // PG-specific SET commands (synchronize_seqscans, lock_timeout, etc.) that are
        // no-ops for Memgres but must not cause errors.

        if (guc != null && !internalNames.contains(name)) {
            String value = stmt.value();
            // SET param TO DEFAULT is equivalent to RESET param
            if (value != null && value.equalsIgnoreCase("DEFAULT")) {
                guc.reset(name);
                return QueryResult.message(QueryResult.Type.SET, "SET");
            }
            // Validate type based on parameter name
            if (value != null && !value.isEmpty()) {
                validateGucValue(name, value);
            }
            if (stmt.isLocal()) {
                guc.setLocal(name, value);
            } else {
                guc.set(name, value);
            }
        }
        return QueryResult.message(QueryResult.Type.SET, "SET");
    }

    // ---- DO block ----

    /** Validate a GUC parameter value based on the known type of the parameter. */
    private void validateGucValue(String name, String value) {
        if (value == null || Strs.isBlank(value)) return;
        String lname = name.toLowerCase();
        // Boolean parameters
        if (lname.equals("enable_seqscan") || lname.equals("enable_hashjoin") || lname.equals("enable_indexscan")
                || lname.startsWith("enable_") || lname.equals("fsync") || lname.equals("log_checkpoints")
                || lname.equals("log_connections") || lname.equals("log_disconnections")) {
            String lv = value.toLowerCase().trim();
            if (!lv.equals("on") && !lv.equals("off") && !lv.equals("true") && !lv.equals("false")
                    && !lv.equals("yes") && !lv.equals("no") && !lv.equals("1") && !lv.equals("0")) {
                throw new MemgresException("invalid value for parameter \"" + name + "\": \"" + value + "\"", "22023");
            }
            return;
        }
        // Memory / integer parameters (accept numbers with optional unit like MB, kB, etc.)
        if (lname.equals("work_mem") || lname.equals("maintenance_work_mem") || lname.equals("shared_buffers")
                || lname.equals("effective_cache_size") || lname.equals("max_connections")
                || lname.equals("max_wal_size") || lname.equals("min_wal_size")
                || lname.endsWith("_mem") || lname.endsWith("_buffers")) {
            String trimmed = value.trim().replaceAll("\\s*(kB|MB|GB|TB|B)$", "");
            try {
                Long.parseLong(trimmed);
            } catch (NumberFormatException e) {
                throw new MemgresException("invalid value for parameter \"" + name + "\": \"" + value + "\"", "22023");
            }
            return;
        }
        // search_path: PG does not validate schema existence at SET time; it only matters at resolution time
        if (lname.equals("search_path")) {
            return;
        }
        // TimeZone
        if (lname.equals("timezone")) {
            String tz = value.trim();
            // Remove surrounding quotes if present
            if ((tz.startsWith("'") && tz.endsWith("'")) || (tz.startsWith("\"") && tz.endsWith("\""))) {
                tz = tz.substring(1, tz.length() - 1);
            }
            if (tz.equalsIgnoreCase("UTC") || tz.equalsIgnoreCase("LOCAL") || tz.equalsIgnoreCase("DEFAULT")) return;
            try {
                java.time.ZoneId.of(tz);
            } catch (Exception e) {
                throw new MemgresException("unrecognized time zone name: \"" + tz + "\"", "22023");
            }
        }
    }

    private void executePlpgsqlBlock(String body) {
        PlpgsqlExecutor plExec = new PlpgsqlExecutor(executor, executor.database, executor.session);
        plExec.executeDoBlock(body);
    }

    // ---- DISCARD ----

    QueryResult executeDiscard(DiscardStmt stmt) {
        if (executor.session != null) {
            String target = stmt.target().toUpperCase();
            if (target.equals("ALL")) {
                executor.session.getGucSettings().resetAll();
                executor.session.removeAllPreparedStatements();
                executor.session.removeAllCursors();
                // Drop all temp tables for this session
                executor.session.dropTempObjects();
            } else if (target.equals("PLANS")) {
                executor.session.removeAllPreparedStatements();
            } else if (target.equals("TEMP") || target.equals("TEMPORARY")) {
                executor.session.dropTempObjects();
            }
        }
        return QueryResult.message(QueryResult.Type.SET, "DISCARD " + stmt.target().toUpperCase());
    }

    // ---- LOCK TABLE ----

    private static final Set<String> VALID_LOCK_MODES = Cols.setOf(
            "ACCESS SHARE", "ROW SHARE", "ROW EXCLUSIVE", "SHARE UPDATE EXCLUSIVE",
            "SHARE", "SHARE ROW EXCLUSIVE", "EXCLUSIVE", "ACCESS EXCLUSIVE");

    QueryResult executeLock(LockStmt stmt) {
        // Validate lock mode first; syntax errors take priority over transaction state
        if (stmt.lockMode() != null && !VALID_LOCK_MODES.contains(stmt.lockMode().toUpperCase())) {
            throw new MemgresException("syntax error at or near \"" + stmt.lockMode().split("\\s+")[0].toLowerCase() + "\"", "42601");
        }
        // PG requires LOCK to be inside an explicit transaction
        if (executor.session == null || !executor.session.isInTransaction()) {
            throw new MemgresException("LOCK TABLE can only be used in transaction blocks", "25P01");
        }
        // Validate table exists (handle schema-qualified names)
        String schema = executor.defaultSchema();
        String table = stmt.tableName();
        if (table.contains(".")) {
            int dot = table.indexOf('.');
            schema = table.substring(0, dot);
            table = table.substring(dot + 1);
        }
        executor.resolveTable(schema, table);
        return QueryResult.message(QueryResult.Type.SET, "LOCK TABLE");
    }

    // ---- GRANT / REVOKE ----

    private static final Set<String> TABLE_PRIVILEGES = Cols.setOf(
            "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "ALL");

    QueryResult executeGrant(GrantStmt s) {
        if (s.isRoleGrant()) {
            // Track role memberships
            if (s.privileges() != null && s.grantees() != null) {
                for (String grantedRole : s.privileges()) {
                    for (String member : s.grantees()) {
                        executor.database.addRoleMembership(grantedRole, member);
                    }
                }
            }
            return QueryResult.message(QueryResult.Type.SET, "GRANT");
        }
        // Validate object exists FIRST (PG checks object before role)
        // Validate object exists for SCHEMA grants
        if (s.objectType() != null && s.objectType().equals("SCHEMA") && s.objectName() != null) {
            if (executor.database.getSchema(s.objectName()) == null) {
                throw new MemgresException("schema \"" + s.objectName() + "\" does not exist", "3F000");
            }
        }
        // Validate object exists for TABLE grants (or no objectType = default TABLE)
        if (s.objectName() != null && (s.objectType() == null || s.objectType().equals("TABLE"))) {
            try { executor.resolveTable(executor.defaultSchema(), s.objectName()); }
            catch (MemgresException e) {
                throw new MemgresException("relation \"" + s.objectName() + "\" does not exist", "42P01");
            }
        }
        // Validate grantee role exists
        if (s.grantees() != null) {
            for (String grantee : s.grantees()) {
                String g = grantee.toLowerCase();
                if (!g.equals("public") && !g.equals("test") && !g.equals("postgres")
                        && !executor.database.getRoles().containsKey(g)) {
                    throw new MemgresException("role \"" + grantee + "\" does not exist", "42704");
                }
            }
        }
        // Additional TABLE grant validations
        if (s.objectType() != null && s.objectType().equals("TABLE") && s.objectName() != null) {
            // Validate privilege types for tables
            for (String priv : s.privileges()) {
                if (!TABLE_PRIVILEGES.contains(priv)) {
                    throw new MemgresException("invalid privilege type " + priv + " for table", "0LP01");
                }
            }
            // Validate column-level privileges: check column exists
            if (s.columns() != null) {
                Table table = executor.resolveTable(executor.defaultSchema(), s.objectName());
                for (String col : s.columns()) {
                    if (table.getColumnIndex(col) < 0) {
                        throw new MemgresException("column \"" + col + "\" of relation \"" + s.objectName() + "\" does not exist", "42703");
                    }
                }
            }
        }
        // Track granted privileges for role dependency checks (DROP ROLE)
        if (s.objectName() != null && s.grantees() != null && s.objectType() != null) {
            for (String grantee : s.grantees()) {
                for (String priv : s.privileges()) {
                    if (s.columns() != null && !s.columns().isEmpty()) {
                        // Column-level grant: store as COLUMN objectType with "tableName.colName"
                        for (String col : s.columns()) {
                            executor.database.addRolePrivilege(grantee, priv, "COLUMN", s.objectName() + "." + col);
                        }
                    } else {
                        executor.database.addRolePrivilege(grantee, priv, s.objectType(), s.objectName());
                    }
                }
            }
        }
        return QueryResult.message(QueryResult.Type.SET, "GRANT");
    }

    QueryResult executeRevoke(RevokeStmt s) {
        if (s.isRoleGrant()) {
            // Remove role memberships
            if (s.privileges() != null && s.grantees() != null) {
                for (String grantedRole : s.privileges()) {
                    for (String member : s.grantees()) {
                        executor.database.removeRoleMembership(grantedRole, member);
                    }
                }
            }
            return QueryResult.message(QueryResult.Type.SET, "REVOKE");
        }
        // Validate object exists for TABLE grants
        if (s.objectType() != null && s.objectType().equals("TABLE") && s.objectName() != null) {
            try { executor.resolveTable(executor.defaultSchema(), s.objectName()); }
            catch (MemgresException e) {
                throw new MemgresException("relation \"" + s.objectName() + "\" does not exist", "42P01");
            }
        }
        // Track privilege removal
        if (s.objectName() != null && s.grantees() != null && s.objectType() != null) {
            for (String grantee : s.grantees()) {
                for (String priv : s.privileges()) {
                    executor.database.removeRolePrivilege(grantee, priv, s.objectType(), s.objectName());
                }
            }
        }
        return QueryResult.message(QueryResult.Type.SET, "REVOKE");
    }

    // ---- PREPARE / EXECUTE / DEALLOCATE ----

    QueryResult executePrepare(PrepareStmt stmt) {
        if (executor.session.getPreparedStatement(stmt.name()) != null) {
            throw new MemgresException("prepared statement \"" + stmt.name() + "\" already exists", "42P05");
        }
        // Always infer param count from $N references in body
        List<String> paramTypes = stmt.paramTypes();
        int inferredCount = maxParamIndex(stmt.body());
        // Validate the body at PREPARE time (PG does full analysis/type-checking here)
        validatePreparedBody(stmt.body(), paramTypes);
        executor.session.addPreparedStatement(stmt.name(),
                new Session.PreparedStmt(stmt.name(), paramTypes, stmt.body(), inferredCount));
        return QueryResult.message(QueryResult.Type.SET, "PREPARE");
    }

    /**
     * Validate a prepared statement body at PREPARE time, matching PG behavior
     * which performs type analysis before storing the prepared statement.
     */
    private void validatePreparedBody(Statement body, List<String> paramTypes) {
        if (body instanceof SelectStmt) {
            SelectStmt select = (SelectStmt) body;
            // Validate CASE expressions in the select list for type compatibility
            if (select.targets() != null) {
                for (SelectStmt.SelectTarget target : select.targets()) {
                    validateExpressionTree(target.expr(), paramTypes);
                }
            }
            // Validate LIMIT expression: must be compatible with integer, not text
            if (select.limit() != null) {
                validateLimitType(select.limit(), paramTypes);
            }
            // Validate WHERE clause
            if (select.where() != null) {
                validateExpressionTree(select.where(), paramTypes);
            }
            // Validate bare parameter references and anonymous ROW field access
            if (select.targets() != null) {
                for (SelectStmt.SelectTarget target : select.targets()) {
                    validateAnonymousRowAccess(target.expr());
                    // PG infers text for bare $N used directly as a select target
                    // Only check for untyped params in complex expressions
                    if (!(target.expr() instanceof ParamRef)) {
                        checkForUntypedParams(target.expr(), paramTypes, false);
                    }
                }
            }
        }
    }

    /**
     * Check for field access on anonymous ROW constructors.
     * PG rejects these at PREPARE time since the record type is unknown.
     */
    private void validateAnonymousRowAccess(Expression expr) {
        if (expr == null) return;
        // FieldAccessExpr on anonymous ROW, always rejected
        if (expr instanceof FieldAccessExpr) {
            FieldAccessExpr fa = (FieldAccessExpr) expr;
            if (fa.expr() instanceof ArrayExpr && ((ArrayExpr) fa.expr()).isRow()) {
                ArrayExpr arr = (ArrayExpr) fa.expr();
                throw new MemgresException(
                    "could not identify column \"" + fa.field() + "\" in record data type", "42601");
            }
            validateAnonymousRowAccess(fa.expr());
        }
        // Recurse into sub-expressions
        if (expr instanceof IsNullExpr) {
            IsNullExpr isn = (IsNullExpr) expr;
            validateAnonymousRowAccess(isn.expr());
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            for (Expression arg : fn.args()) {
                validateAnonymousRowAccess(arg);
            }
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr b = (BinaryExpr) expr;
            validateAnonymousRowAccess(b.left());
            validateAnonymousRowAccess(b.right());
        }
        if (expr instanceof CastExpr) {
            CastExpr ce = (CastExpr) expr;
            validateAnonymousRowAccess(ce.expr());
        }
        if (expr instanceof ArrayExpr) {
            ArrayExpr arr = (ArrayExpr) expr;
            for (Expression elem : arr.elements()) {
                validateAnonymousRowAccess(elem);
            }
        }
    }

    /**
     * Check for parameter references ($N) that have no type context at all.
     * PG 18+ rejects these at PREPARE time with "could not determine data type of parameter $N".
     * Type context is provided by: declared param types, CastExpr, COALESCE with typed siblings,
     * CASE WHEN (boolean context), operators providing type context.
     * @param hasTypeContext true if an ancestor provides type context (e.g., inside COALESCE, CAST, etc.)
     */
    private void checkForUntypedParams(Expression expr, List<String> paramTypes, boolean hasTypeContext) {
        if (expr == null) return;
        if (expr instanceof ParamRef) {
            ParamRef pr = (ParamRef) expr;
            int idx = pr.index() - 1;
            boolean hasDeclaredType = paramTypes != null && idx >= 0 && idx < paramTypes.size();
            if (!hasDeclaredType && !hasTypeContext) {
                throw new MemgresException(
                    "could not determine data type of parameter $" + pr.index(), "42P18");
            }
            return;
        }
        // CastExpr provides type context for its inner expression
        if (expr instanceof CastExpr) {
            CastExpr ce = (CastExpr) expr;
            checkForUntypedParams(ce.expr(), paramTypes, true);
            return;
        }
        // COALESCE provides type context if any sibling is typed
        if (expr instanceof FunctionCallExpr && "coalesce".equalsIgnoreCase(((FunctionCallExpr) expr).name())) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            // COALESCE provides type context to all args
            for (Expression arg : fn.args()) {
                checkForUntypedParams(arg, paramTypes, true);
            }
            return;
        }
        // CASE WHEN provides boolean context for condition, type context from results
        if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            for (CaseExpr.WhenClause w : c.whenClauses()) {
                checkForUntypedParams(w.condition(), paramTypes, true); // boolean context
                checkForUntypedParams(w.result(), paramTypes, true); // type context from siblings
            }
            if (c.elseExpr() != null) checkForUntypedParams(c.elseExpr(), paramTypes, true);
            return;
        }
        // BinaryExpr with typed operand provides context
        if (expr instanceof BinaryExpr) {
            BinaryExpr b = (BinaryExpr) expr;
            // If one side is typed (literal, cast, etc.), the other gets context
            boolean leftTyped = !(b.left() instanceof ParamRef);
            boolean rightTyped = !(b.right() instanceof ParamRef);
            checkForUntypedParams(b.left(), paramTypes, hasTypeContext || rightTyped);
            checkForUntypedParams(b.right(), paramTypes, hasTypeContext || leftTyped);
            return;
        }
        // IS NULL / IS NOT NULL - does NOT provide type context
        if (expr instanceof IsNullExpr) {
            IsNullExpr isn = (IsNullExpr) expr;
            checkForUntypedParams(isn.expr(), paramTypes, hasTypeContext);
            return;
        }
        // Function calls (other than COALESCE) - pass through context
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            for (Expression arg : fn.args()) {
                checkForUntypedParams(arg, paramTypes, hasTypeContext);
            }
            return;
        }
        // InExpr
        if (expr instanceof InExpr) {
            InExpr in = (InExpr) expr;
            checkForUntypedParams(in.expr(), paramTypes, hasTypeContext);
            for (Expression v : in.values()) {
                checkForUntypedParams(v, paramTypes, hasTypeContext);
            }
            return;
        }
        // ArrayExpr (ROW or ARRAY)
        if (expr instanceof ArrayExpr) {
            ArrayExpr arr = (ArrayExpr) expr;
            for (Expression elem : arr.elements()) {
                checkForUntypedParams(elem, paramTypes, hasTypeContext);
            }
            return;
        }
        // FieldAccessExpr
        if (expr instanceof FieldAccessExpr) {
            FieldAccessExpr fa = (FieldAccessExpr) expr;
            checkForUntypedParams(fa.expr(), paramTypes, hasTypeContext);
            return;
        }
        // UnaryExpr
        if (expr instanceof UnaryExpr) {
            UnaryExpr ue = (UnaryExpr) expr;
            checkForUntypedParams(ue.operand(), paramTypes, hasTypeContext);
            return;
        }
        // AnyAllArrayExpr
        if (expr instanceof AnyAllArrayExpr) {
            AnyAllArrayExpr aaa = (AnyAllArrayExpr) expr;
            checkForUntypedParams(aaa.left(), paramTypes, hasTypeContext);
            checkForUntypedParams(aaa.array(), paramTypes, hasTypeContext);
        }
    }

    /** Walk expression tree and validate type constraints. */
    private void validateExpressionTree(Expression expr, List<String> paramTypes) {
        if (expr == null) return;
        if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            // Delegate to existing CASE branch type validation
            executor.validateCaseBranchTypesForPrepare(c);
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            // Check ANY/ALL with incompatible parameter types
            String name = fn.name().toLowerCase();
            if ("any".equals(name) || "all".equals(name)) {
                if (!fn.args().isEmpty()) {
                    Expression arg = fn.args().get(0);
                    if (arg instanceof ParamRef) {
                        ParamRef pr = (ParamRef) arg;
                        int idx = pr.index() - 1;
                        if (paramTypes != null && idx >= 0 && idx < paramTypes.size()) {
                            String pType = paramTypes.get(idx).toLowerCase();
                            if (!pType.contains("[]") && !pType.contains("array")) {
                                throw new MemgresException(
                                    "op ANY/ALL (array) requires array on right side", "42809");
                            }
                        }
                    }
                }
            }
        }
        // Check ANY/ALL(array_expr) with non-array parameter type
        if (expr instanceof AnyAllArrayExpr) {
            AnyAllArrayExpr aaa = (AnyAllArrayExpr) expr;
            Expression arrayExpr = aaa.array();
            if (arrayExpr instanceof ParamRef) {
                ParamRef pr = (ParamRef) arrayExpr;
                int idx = pr.index() - 1;
                if (paramTypes != null && idx >= 0 && idx < paramTypes.size()) {
                    String pType = paramTypes.get(idx).toLowerCase();
                    if (!pType.contains("[]") && !pType.contains("array")) {
                        throw new MemgresException(
                            "op ANY/ALL (array) requires array on right side", "42809");
                    }
                }
            }
            validateExpressionTree(aaa.left(), paramTypes);
            validateExpressionTree(aaa.array(), paramTypes);
        }
        // Check InExpr from = ANY($param) where param is non-array type
        if (expr instanceof InExpr) {
            InExpr in = (InExpr) expr;
            if (in.fromAny()) {
                for (Expression elem : in.values()) {
                    if (elem instanceof ParamRef) {
                        ParamRef pr = (ParamRef) elem;
                        int idx = pr.index() - 1;
                        if (paramTypes != null && idx >= 0 && idx < paramTypes.size()) {
                            String pType = paramTypes.get(idx).toLowerCase();
                            if (!pType.contains("[]") && !pType.contains("array")) {
                                throw new MemgresException(
                                    "op ANY/ALL (array) requires array on right side", "42809");
                            }
                        }
                    }
                }
            }
            for (Expression elem : in.values()) {
                validateExpressionTree(elem, paramTypes);
            }
            validateExpressionTree(in.expr(), paramTypes);
        }
        // Recurse into sub-expressions
        if (expr instanceof BinaryExpr) {
            BinaryExpr b = (BinaryExpr) expr;
            validateExpressionTree(b.left(), paramTypes);
            validateExpressionTree(b.right(), paramTypes);
        }
        if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            for (CaseExpr.WhenClause w : c.whenClauses()) {
                validateExpressionTree(w.condition(), paramTypes);
                validateExpressionTree(w.result(), paramTypes);
            }
            if (c.elseExpr() != null) validateExpressionTree(c.elseExpr(), paramTypes);
        }
        if (expr instanceof CastExpr) {
            CastExpr ce = (CastExpr) expr;
            validateExpressionTree(ce.expr(), paramTypes);
        }
        if (expr instanceof UnaryExpr) {
            UnaryExpr ue = (UnaryExpr) expr;
            validateExpressionTree(ue.operand(), paramTypes);
        }
    }

    /** Validate that LIMIT expression is compatible with integer type. */
    private void validateLimitType(Expression limitExpr, List<String> paramTypes) {
        if (limitExpr instanceof ParamRef) {
            ParamRef pr = (ParamRef) limitExpr;
            int idx = pr.index() - 1;
            if (paramTypes != null && idx >= 0 && idx < paramTypes.size()) {
                String pType = paramTypes.get(idx).toLowerCase();
                if ("text".equals(pType) || "varchar".equals(pType) || "character varying".equals(pType)) {
                    throw new MemgresException(
                        "argument of LIMIT must be type bigint, not type " + pType, "42804");
                }
            }
        }
    }

    QueryResult executeExecuteStmt(ExecuteStmt stmt) {
        Session.PreparedStmt prepared = executor.session.getPreparedStatement(stmt.name());
        if (prepared == null) {
            throw new MemgresException("prepared statement \"" + stmt.name() + "\" does not exist", "26000");
        }
        // Validate parameter count: use max of explicit types and inferred param refs from $N
        int declaredCount = (prepared.paramTypes() != null && !prepared.paramTypes().isEmpty())
                ? prepared.paramTypes().size() : 0;
        int inferredFromBody = prepared.inferredParamCount();
        // If no explicit types, use inferred count; otherwise use max of both to catch $N > declared count
        int expectedParams = declaredCount > 0 ? Math.max(declaredCount, inferredFromBody) : inferredFromBody;
        int actualParams = stmt.params() != null ? stmt.params().size() : 0;
        if (actualParams != expectedParams) {
            throw new MemgresException("wrong number of parameters for prepared statement \"" + stmt.name()
                    + "\": expected " + expectedParams + ", got " + actualParams, "42601");
        }
        // Bind parameters
        List<Object> savedParams = new ArrayList<>(executor.boundParameters);
        try {
            executor.boundParameters.clear();
            if (stmt.params() != null) {
                for (Expression param : stmt.params()) {
                    executor.boundParameters.add(executor.evalExpr(param, null));
                }
            }
            try {
                return executor.executeStatement(prepared.body());
            } catch (MemgresException me) {
                // Remap type coercion errors (e.g., passing 'x' for an int param) to 22P02.
                // When a prepared statement declares parameter types and the supplied value
                // cannot be coerced, PG returns 22P02 (invalid_text_representation).
                // Memgres may throw 42883 (undefined_function/operator) instead.
                if ("42883".equals(me.getSqlState()) && declaredCount > 0) {
                    throw new MemgresException(me.getMessage(), "22P02");
                }
                throw me;
            }
        } finally {
            executor.boundParameters.clear();
            executor.boundParameters.addAll(savedParams);
        }
    }

    QueryResult executeDeallocate(DeallocateStmt stmt) {
        if (stmt.all()) {
            executor.session.removeAllPreparedStatements();
        } else {
            if (executor.session.getPreparedStatement(stmt.name()) == null) {
                throw new MemgresException("prepared statement \"" + stmt.name() + "\" does not exist", "26000");
            }
            executor.session.removePreparedStatement(stmt.name());
        }
        return QueryResult.message(QueryResult.Type.SET, "DEALLOCATE");
    }

    // ---- Parameter inference helpers ----

    /** Walk an AST tree to find the maximum $N parameter index. Returns 0 if no params found. */
    private int maxParamIndex(Statement stmt) {
        int[] max = {0};
        walkExpressions(stmt, max);
        return max[0];
    }

    private void walkExpressions(Object node, int[] max) {
        if (node == null) return;
        if (node instanceof ParamRef) {
            ParamRef p = (ParamRef) node;
            if (p.index() > max[0]) max[0] = p.index();
            return;
        }
        // Walk into common expression types
        if (node instanceof BinaryExpr) {
            BinaryExpr b = (BinaryExpr) node;
            walkExpressions(b.left(), max);
            walkExpressions(b.right(), max);
        } else if (node instanceof UnaryExpr) {
            UnaryExpr u = (UnaryExpr) node;
            walkExpressions(u.operand(), max);
        } else if (node instanceof CastExpr) {
            CastExpr c = (CastExpr) node;
            walkExpressions(c.expr(), max);
        } else if (node instanceof FunctionCallExpr) {
            FunctionCallExpr fc = (FunctionCallExpr) node;
            if (fc.args() != null) fc.args().forEach(a -> walkExpressions(a, max));
        } else if (node instanceof IsNullExpr) {
            IsNullExpr n = (IsNullExpr) node;
            walkExpressions(n.expr(), max);
        } else if (node instanceof IsBooleanExpr) {
            IsBooleanExpr ib = (IsBooleanExpr) node;
            walkExpressions(ib.expr(), max);
        } else if (node instanceof BetweenExpr) {
            BetweenExpr bt = (BetweenExpr) node;
            walkExpressions(bt.expr(), max);
            walkExpressions(bt.low(), max);
            walkExpressions(bt.high(), max);
        } else if (node instanceof InExpr) {
            InExpr ie = (InExpr) node;
            walkExpressions(ie.expr(), max);
            if (ie.values() != null) ie.values().forEach(v -> walkExpressions(v, max));
        } else if (node instanceof LikeExpr) {
            LikeExpr le = (LikeExpr) node;
            walkExpressions(le.left(), max);
            walkExpressions(le.pattern(), max);
        } else if (node instanceof CaseExpr) {
            CaseExpr ce = (CaseExpr) node;
            walkExpressions(ce.operand(), max);
            if (ce.whenClauses() != null) {
                for (CaseExpr.WhenClause w : ce.whenClauses()) {
                    walkExpressions(w.condition(), max);
                    walkExpressions(w.result(), max);
                }
            }
            walkExpressions(ce.elseExpr(), max);
        } else if (node instanceof ArrayExpr) {
            ArrayExpr ae = (ArrayExpr) node;
            if (ae.elements() != null) ae.elements().forEach(e -> walkExpressions(e, max));
        } else if (node instanceof AtTimeZoneExpr) {
            AtTimeZoneExpr atz = (AtTimeZoneExpr) node;
            walkExpressions(atz.expr(), max);
            walkExpressions(atz.zone(), max);
        } else if (node instanceof AnyAllArrayExpr) {
            AnyAllArrayExpr aa = (AnyAllArrayExpr) node;
            walkExpressions(aa.left(), max);
            walkExpressions(aa.array(), max);
        } else if (node instanceof SubqueryExpr) {
            SubqueryExpr sq = (SubqueryExpr) node;
            walkExpressions(sq.subquery(), max);
        } else if (node instanceof ExistsExpr) {
            ExistsExpr ex = (ExistsExpr) node;
            walkExpressions(ex.subquery(), max);
        } else if (node instanceof NamedArgExpr) {
            NamedArgExpr na = (NamedArgExpr) node;
            walkExpressions(na.value(), max);
        } else if (node instanceof FieldAccessExpr) {
            FieldAccessExpr fa = (FieldAccessExpr) node;
            walkExpressions(fa.expr(), max);
        } else if (node instanceof ArraySliceExpr) {
            ArraySliceExpr as = (ArraySliceExpr) node;
            walkExpressions(as.array(), max);
            walkExpressions(as.lower(), max);
            walkExpressions(as.upper(), max);
        }
        // Walk into statement types
        else if (node instanceof SelectStmt) {
            SelectStmt sel = (SelectStmt) node;
            if (sel.targets() != null) sel.targets().forEach(t -> walkExpressions(t.expr(), max));
            walkExpressions(sel.where(), max);
            walkExpressions(sel.having(), max);
            if (sel.orderBy() != null) sel.orderBy().forEach(o -> walkExpressions(o.expr(), max));
            walkExpressions(sel.limit(), max);
            walkExpressions(sel.offset(), max);
            if (sel.from() != null) {
                for (SelectStmt.FromItem f : sel.from()) {
                    if (f instanceof SelectStmt.SubqueryFrom) walkExpressions(((SelectStmt.SubqueryFrom) f).subquery(), max);
                }
            }
        } else if (node instanceof InsertStmt) {
            InsertStmt ins = (InsertStmt) node;
            if (ins.values() != null) {
                for (List<Expression> row : ins.values()) {
                    if (row != null) row.forEach(v -> walkExpressions(v, max));
                }
            }
            if (ins.returning() != null) ins.returning().forEach(r -> walkExpressions(r.expr(), max));
            if (ins.selectStmt() != null) walkExpressions(ins.selectStmt(), max);
        } else if (node instanceof UpdateStmt) {
            UpdateStmt upd = (UpdateStmt) node;
            if (upd.setClauses() != null) upd.setClauses().forEach(a -> walkExpressions(a.value(), max));
            walkExpressions(upd.where(), max);
            if (upd.returning() != null) upd.returning().forEach(r -> walkExpressions(r.expr(), max));
        } else if (node instanceof DeleteStmt) {
            DeleteStmt del = (DeleteStmt) node;
            walkExpressions(del.where(), max);
            if (del.returning() != null) del.returning().forEach(r -> walkExpressions(r.expr(), max));
        } else if (node instanceof SetOpStmt) {
            SetOpStmt sop = (SetOpStmt) node;
            walkExpressions(sop.left(), max);
            walkExpressions(sop.right(), max);
        }
    }

    // ---- Cursors ----

    QueryResult executeDeclareCursor(DeclareCursorStmt stmt) {
        if (executor.session.getCursor(stmt.name()) != null) {
            throw new MemgresException("cursor \"" + stmt.name() + "\" already exists");
        }
        // Execute the query to get all results
        QueryResult result = executor.selectExecutor.executeSelect(stmt.query());
        List<Object[]> rows = result.getRows() != null ? new ArrayList<>(result.getRows()) : new ArrayList<>();
        List<Column> columns = result.getColumns() != null ? result.getColumns() : Cols.listOf();
        executor.session.addCursor(stmt.name(), new Session.CursorState(stmt.name(), columns, rows));
        return QueryResult.message(QueryResult.Type.SET, "DECLARE CURSOR");
    }

    QueryResult executeFetch(FetchStmt stmt) {
        Session.CursorState cursor = executor.session.getCursor(stmt.cursorName());
        if (cursor == null) {
            throw new MemgresException("cursor \"" + stmt.cursorName() + "\" does not exist");
        }
        List<Object[]> fetched = cursorFetch(cursor, stmt.direction(), stmt.count());

        if (stmt.isMove()) {
            return QueryResult.command(QueryResult.Type.SET, fetched.size());
        }
        return QueryResult.select(cursor.getColumns(), fetched);
    }

    private List<Object[]> cursorFetch(Session.CursorState cursor, FetchStmt.Direction dir, int count) {
        int pos = cursor.getPosition();
        int total = cursor.getRowCount();
        List<Object[]> result = new ArrayList<>();

        switch (dir) {
            case NEXT:
                addRow(result, cursor, pos + 1);
                break;
            case PRIOR:
                addRow(result, cursor, pos - 1);
                break;
            case FIRST:
                addRow(result, cursor, 0);
                break;
            case LAST:
                addRow(result, cursor, total - 1);
                break;
            case ABSOLUTE:
                addRow(result, cursor, count > 0 ? count - 1 : total + count);
                break;
            case RELATIVE:
                addRow(result, cursor, pos + count);
                break;
            case FORWARD: {
                for (int i = 0; i < count; i++) addRow(result, cursor, pos + 1 + i); if (!result.isEmpty()) cursor.setPosition(pos + result.size()); 
                break;
            }
            case FORWARD_ALL:
            case ALL: {
                for (int i = pos + 1; i < total; i++) addRow(result, cursor, i); if (result.isEmpty()) cursor.setPosition(total); 
                break;
            }
            case BACKWARD: {
                for (int i = 0; i < count; i++) addRow(result, cursor, pos - 1 - i); 
                break;
            }
            case BACKWARD_ALL: {
                for (int i = pos - 1; i >= 0; i--) addRow(result, cursor, i); 
                break;
            }
        }
        return result;
    }

    private void addRow(List<Object[]> result, Session.CursorState cursor, int idx) {
        Object[] row = cursor.getRow(idx);
        if (row != null) {
            result.add(row);
            cursor.setPosition(idx);
        }
    }

    QueryResult executeClose(CloseStmt stmt) {
        if (stmt.all()) {
            executor.session.removeAllCursors();
        } else {
            if (executor.session.getCursor(stmt.cursorName()) == null) {
                throw new MemgresException("cursor \"" + stmt.cursorName() + "\" does not exist");
            }
            executor.session.removeCursor(stmt.cursorName());
        }
        return QueryResult.message(QueryResult.Type.SET, "CLOSE CURSOR");
    }
}
