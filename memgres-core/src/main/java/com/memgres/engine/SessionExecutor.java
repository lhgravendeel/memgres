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

    /**
     * The schema a relation named by a COMMENT lives in: the one written, or the first on the
     * search path that holds a relation of that name. A comment is keyed by where the object is,
     * not by what the session's default schema happened to be.
     */
    private String relationSchema(String writtenSchema, String bareName) {
        if (writtenSchema != null) return writtenSchema.toLowerCase();
        if (bareName != null) {
            for (String schema : executor.searchPathSchemas()) {
                if (RelationNamespace.kindOf(executor.database, schema, bareName) != null) {
                    return schema;
                }
            }
        }
        return executor.defaultSchema();
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
                String writtenSchema = null;
                String bareName = objName;
                // CONSTRAINT/TRIGGER/RULE/POLICY names are qualified by their relation, not a
                // schema, so the prefix must survive into the stored key
                boolean relationScoped = objType.equals("CONSTRAINT") || objType.equals("TRIGGER")
                        || objType.equals("RULE") || objType.equals("POLICY");
                // A routine's argument list is part of what was written but not of its name.
                String writtenArgs = null;
                int signatureAt = bareName.indexOf('(');
                if (signatureAt > 0) {
                    writtenArgs = bareName.substring(signatureAt);
                    objName = objName.substring(0, objName.indexOf('('));
                    bareName = bareName.substring(0, signatureAt);
                }
                if (objName.contains(".") && !objType.equals("COLUMN") && !relationScoped) {
                    int dot = objName.indexOf('.');
                    writtenSchema = objName.substring(0, dot);
                    bareName = objName.substring(dot + 1);
                    // COMMENT resolves the schema first and reports that when it is not there,
                    // rather than the object it never got as far as looking for.
                    SchemaQualifier.requireSchema(executor.database, executor.session, writtenSchema);
                }
                // Where the object actually is, which is what its comment is keyed by: the schema
                // written, or the first one on the search path that holds it. The kinds that are
                // not relations settle it for themselves below.
                String schemaName = relationSchema(writtenSchema, bareName);
                if (objType.equals("TABLE") || objType.equals("RELATION")) {
                    // A sequence is a relation but it is not a table, and PostgreSQL says which
                    // it is rather than filing the comment against the wrong kind of object.
                    if (executor.database.hasSequence(schemaName, bareName)) {
                        throw new MemgresException("\"" + bareName + "\" is not a table", "42809");
                    }
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
                            SchemaQualifier.requireSchema(
                                    executor.database, executor.session, colSchema);
                        }
                        try {
                            // A composite type has attributes and a pg_class row of its own, so a
                            // comment can be filed against one of them exactly as against a
                            // table's column. Only relations were looked for, so it was not.
                            List<CreateTypeStmt.CompositeField> fields =
                                    executor.database.getCompositeType(colTable);
                            if (fields != null) {
                                boolean known = false;
                                for (CreateTypeStmt.CompositeField f : fields) {
                                    if (f.name().equalsIgnoreCase(colPart)) { known = true; break; }
                                }
                                if (!known) {
                                    throw new MemgresException("column \"" + colPart
                                            + "\" of relation \"" + colTable + "\" does not exist", "42703");
                                }
                            } else {
                                Table commentTable = executor.resolveTable(colSchema, colTable);
                                if (commentTable.getColumnIndex(colPart) < 0) {
                                    throw new MemgresException("column \"" + colPart + "\" of relation \"" + colTable + "\" does not exist", "42703");
                                }
                            }
                        } catch (MemgresException e) {
                            if ("42703".equals(e.getSqlState())) throw e;
                            throw new MemgresException("relation \"" + tablePart + "\" does not exist", "42P01");
                        }
                        // Store the column under its table's own schema: two schemas may each hold
                        // a t with an x, and what was said about one is not said about the other.
                        schemaName = tablePart.contains(".")
                                ? colSchema.toLowerCase() : relationSchema(null, colTable);
                        bareName = colTable + "." + colPart;
                    }
                } else if (objType.equals("FUNCTION") || objType.equals("PROCEDURE") || objType.equals("ROUTINE")) {
                    String wanted = objType;
                    // Normalize PROCEDURE/ROUTINE to FUNCTION for comment storage
                    objType = "FUNCTION";
                    String routine = bareName;
                    PgFunction fn = routineWithSignature(routine, writtenArgs);
                    if (fn == null) {
                        throw new MemgresException("function " + commentRoutineSignature(routine, writtenArgs)
                                + " does not exist", "42883");
                    }
                    // FUNCTION and PROCEDURE each name one kind of routine, and ROUTINE either.
                    if (wanted.equals("FUNCTION") && fn.isProcedure()) {
                        throw new MemgresException(commentRoutineSignature(bareName, writtenArgs)
                                + " is not a function", "42809");
                    }
                    if (wanted.equals("PROCEDURE") && !fn.isProcedure()) {
                        throw new MemgresException(commentRoutineSignature(bareName, writtenArgs)
                                + " is not a procedure", "42809");
                    }
                    if (writtenSchema == null) schemaName = Database.schemaOf(fn);
                    bareName = routine;
                } else if (objType.equals("SCHEMA")) {
                    String schemaN = bareName;
                    if (executor.database.getSchema(schemaN) == null) {
                        throw new MemgresException("schema \"" + schemaN + "\" does not exist", "3F000");
                    }
                } else if (objType.equals("TYPE") || objType.equals("DOMAIN")) {
                    // A domain is a type; PostgreSQL keeps both comments in pg_type's namespace,
                    // and which schema's type this is is the search path's to answer.
                    String typeKey = TypeNamespace.resolve(executor.database, executor.session,
                            writtenSchema == null ? bareName : writtenSchema + "." + bareName);
                    if (typeKey == null) {
                        throw new MemgresException("type \"" + objName + "\" does not exist", "42704");
                    }
                    objType = "TYPE";
                    schemaName = TypeNamespace.schemaOfKey(typeKey);
                    bareName = TypeNamespace.nameOfKey(typeKey);
                } else if (objType.equals("SEQUENCE")) {
                    if (!executor.database.hasSequence(schemaName, bareName)) {
                        throw new MemgresException(
                                "relation \"" + objName + "\" does not exist", "42P01");
                    }
                } else if (objType.equals("MATERIALIZED VIEW")) {
                    Database.ViewDef mv = executor.database.getView(bareName);
                    if (mv == null || !mv.materialized) {
                        throw new MemgresException(
                                "relation \"" + objName + "\" does not exist", "42P01");
                    }
                } else if (objType.equals("ROLE") || objType.equals("USER")) {
                    if (!executor.database.getRoles().containsKey(bareName.toLowerCase())) {
                        throw new MemgresException(
                                "role \"" + bareName + "\" does not exist", "42704");
                    }
                } else if (objType.equals("EXTENSION")) {
                    if (!executor.database.hasExtension(bareName)) {
                        throw new MemgresException(
                                "extension \"" + bareName + "\" does not exist", "42704");
                    }
                } else if (objType.equals("LANGUAGE")) {
                    if (!languageExists(bareName)) {
                        throw new MemgresException(
                                "language \"" + bareName + "\" does not exist", "42704");
                    }
                } else if (objType.equals("COLLATION")) {
                    if (executor.database.getCollation(bareName) == null
                            && !BUILT_IN_COLLATIONS.contains(bareName.toLowerCase())) {
                        throw new MemgresException("collation \"" + bareName
                                + "\" for encoding \"UTF8\" does not exist", "42704");
                    }
                } else if (objType.equals("EVENT TRIGGER")) {
                    if (executor.database.getEventTrigger(bareName) == null) {
                        throw new MemgresException(
                                "event trigger \"" + bareName + "\" does not exist", "42704");
                    }
                } else if (objType.equals("LARGE OBJECT")) {
                    long oid;
                    try {
                        oid = Long.parseLong(bareName.trim());
                    } catch (NumberFormatException e) {
                        oid = -1;
                    }
                    if (oid < 0 || !executor.database.getLargeObjectStore().exists(oid)) {
                        throw new MemgresException(
                                "large object " + bareName + " does not exist", "42704");
                    }
                } else if (objType.equals("AGGREGATE")) {
                    // An aggregate is named the way a function is, and the complaint is the same
                    // shape: the routine is missing, or it is there and is not an aggregate.
                    String routine = bareName;
                    boolean isAggregate = executor.database.getUserAggregates()
                            .containsKey(routine.toLowerCase());
                    PgFunction fn = executor.database.getFunction(routine);
                    if (fn == null && !isAggregate) {
                        throw new MemgresException("aggregate " + commentRoutineSignature(bareName, writtenArgs)
                                + " does not exist", "42883");
                    }
                    if (!isAggregate) {
                        throw new MemgresException("function " + commentRoutineSignature(bareName, writtenArgs)
                                + " is not an aggregate", "42809");
                    }
                    objType = "FUNCTION";
                    if (writtenSchema == null && fn != null) schemaName = Database.schemaOf(fn);
                    bareName = routine;
                } else if (relationScoped) {
                    // The name is "<relation>.<object>", and the relation may carry a schema of
                    // its own. Written, it settles which relation this is; bare, the search path
                    // does — and either way the comment is filed under that relation's schema.
                    String[] scoped = bareName.split("\\.", 3);
                    if (scoped.length == 3) {
                        SchemaQualifier.requireSchema(executor.database, executor.session, scoped[0]);
                        schemaName = scoped[0].toLowerCase();
                        bareName = scoped[1] + "." + scoped[2];
                    } else if (scoped.length == 2) {
                        schemaName = relationSchema(null, scoped[0]);
                    }
                }
                String key = Database.globalCommentType(objType)
                        ? bareName.toLowerCase() : Database.commentKey(schemaName, bareName);
                executor.database.addComment(objType.toLowerCase(), key, stmt.value());
            }
            return QueryResult.message(QueryResult.Type.SET, "COMMENT");
        }

        if (name.equals("do_block")) {
            // Only a language with an inline handler can carry a DO block, and only a language
            // the catalogue has at all can be named. Discarding the word ran every block as
            // PL/pgSQL, whatever it said it was written in.
            String language = stmt.auxiliary() == null ? "plpgsql" : stmt.auxiliary();
            requireInlineLanguage(language);
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
                        GucSettings.Def def = GucSettings.definition(e.getKey());
                        rows.add(new Object[]{guc.getCanonicalName(e.getKey()),
                                guc.getForDisplay(e.getKey()),
                                def != null && def.shortDesc != null ? def.shortDesc : ""});
                    }
                }
                return QueryResult.select(cols, rows);
            }
            if (param.equalsIgnoreCase("pg_stat_statements.max")) {
                throw new MemgresException("unrecognized configuration parameter \"pg_stat_statements.max\"", "42704");
            }
            if (guc != null && !guc.isKnown(param) && !param.isEmpty() && !param.contains(".")) {
                throw new MemgresException("unrecognized configuration parameter \"" + param + "\"", "42704");
            }
            String value = guc != null ? guc.getForDisplay(param) : null;
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
                } else if (param.equalsIgnoreCase("session_authorization")) {
                    // H36: RESET SESSION AUTHORIZATION restores identity like
                    // SET SESSION AUTHORIZATION DEFAULT — current_user/session_user
                    // must revert to the connection's boot-time user, not just SHOW.
                    guc.reset("session_authorization");
                    guc.reset("role");
                    if (executor.session != null) {
                        String bootUser = guc.get("session_authorization");
                        executor.session.setConnectingUser(bootUser);
                    }
                } else {
                    // PG rejects RESET of unknown flat params with 42704
                    if (!param.contains(".") && !guc.isKnown(param)) {
                        throw new MemgresException("unrecognized configuration parameter \"" + param + "\"", "42704");
                    }
                    // Putting a parameter back to its default is still changing it, so a preset
                    // the server fixed at startup refuses RESET exactly as it refuses SET.
                    GucSettings.checkAssignable(param, null);
                    // A transaction-scoped setting has no session value to fall back to: it is
                    // derived afresh from its default_ counterpart when a transaction starts, so
                    // there is nothing for RESET to restore and PG says so.
                    if (isTransactionScopedGuc(param)) {
                        throw new MemgresException(
                                "parameter \"" + param.toLowerCase() + "\" cannot be reset", "0A000");
                    }
                    guc.reset(param);
                }
            }
            return QueryResult.message(QueryResult.Type.SET, "RESET");
        }

        if (name.equals("max_connections")) {
            // A parameter fixed at server start is refused with the class PostgreSQL uses for
            // exactly that, the same one wal_level and block_size already answer with here.
            throw new MemgresException(
                    "parameter \"max_connections\" cannot be changed without restarting the server",
                    "55P02");
        }

        if (name.equals("max_prepared_transactions")) {
            GucSettings.checkAssignable(name, stmt.value());
            try {
                int val = Integer.parseInt(stmt.value());
                executor.database.setMaxPreparedTransactions(val);
            } catch (NumberFormatException e) {
                throw new MemgresException("invalid value for parameter \"max_prepared_transactions\": \"" + stmt.value() + "\"", "22023");
            }
            if (guc != null) guc.set(name, stmt.value());
            return QueryResult.message(QueryResult.Type.SET, "SET");
        }

        if (name.equals("session_authorization")) {
            String user = stmt.value();
            if (user != null && !user.equalsIgnoreCase("DEFAULT") && !user.equalsIgnoreCase("NONE")) {
                requireRole(user, "session_authorization");
                if (guc != null) {
                    guc.set("session_authorization", user);
                    // SET SESSION AUTHORIZATION also resets ROLE to the new session user
                    guc.set("role", user);
                }
                // Update the session's connecting user so current_user/session_user reflect it
                if (executor.session != null) {
                    executor.session.setConnectingUser(user);
                }
            } else {
                // DEFAULT: reset to boot default
                if (guc != null) {
                    guc.reset("session_authorization");
                    guc.reset("role");
                }
                if (executor.session != null) {
                    String bootUser = guc != null ? guc.get("session_authorization") : "test";
                    executor.session.setConnectingUser(bootUser);
                }
            }
            return QueryResult.message(QueryResult.Type.SET, "SET");
        }

        if (name.equals("role")) {
            String role = stmt.value();
            if (role != null && !role.equalsIgnoreCase("NONE") && !role.equalsIgnoreCase("DEFAULT")
                    && !role.equalsIgnoreCase("current_user") && !role.equalsIgnoreCase("session_user")) {
                String sessionUser = guc != null ? guc.get("session_authorization") : "test";
                if (sessionUser == null) sessionUser = "test";
                requireRole(role, "role");
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

        if (name.equals("transaction_snapshot")) {
            String snapshotId = stmt.value();
            if (executor.session != null) {
                executor.session.importSnapshot(executor.database, snapshotId);
            }
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
            if (name.equals("vacuum") && executor.session != null && executor.session.isInRoutine()) {
                throw new MemgresException("VACUUM cannot be executed from a function", "25001");
            }
            // VACUUM cannot run inside a transaction block
            if (name.equals("vacuum") && executor.session != null && executor.session.isInTransaction()) {
                throw new MemgresException("VACUUM cannot run inside a transaction block", "25001");
            }
            String val = stmt.value();
            // For VACUUM, parse flags: "verbose,analyze,table:foo" or "verbose,ok"
            boolean vacuumAnalyze = false;
            boolean vacuumVerbose = false;
            if (name.equals("vacuum") && val != null) {
                if (val.contains("verbose,")) { vacuumVerbose = true; val = val.replace("verbose,", ""); }
                if (val.contains("analyze,")) { vacuumAnalyze = true; val = val.replace("analyze,", ""); }
            }
            // Parse optional column list from ANALYZE value
            String analyzeColumns = null;
            if (val != null && val.contains(",columns:")) {
                int idx = val.indexOf(",columns:");
                analyzeColumns = val.substring(idx + ",columns:".length());
                val = val.substring(0, idx);
            }
            java.time.OffsetDateTime now = java.time.OffsetDateTime.now();
            if (val != null && val.startsWith("table:")) {
                String tblName = val.substring("table:".length());
                // The name may carry its schema, which is which table this is rather than part
                // of its name.
                String tblSchema = executor.defaultSchema();
                int dot = tblName.lastIndexOf('.');
                if (dot > 0) {
                    tblSchema = tblName.substring(0, dot);
                    tblName = tblName.substring(dot + 1);
                }
                Table resolvedTable = executor.resolveTable(tblSchema, tblName);
                // Record analyzed table for pg_statistic
                if (name.equals("analyze") || vacuumAnalyze) {
                    executor.database.recordAnalyzedTable(tblSchema + "." + tblName);
                    resolvedTable.setLastAnalyze(now);
                }
                if (name.equals("vacuum")) {
                    resolvedTable.setLastVacuum(now);
                }
                if (name.equals("analyze")) {
                    resolvedTable.setLastAnalyze(now);
                }
            } else if (name.equals("analyze")) {
                // ANALYZE without a table name: analyze all tables
                for (Map.Entry<String, Schema> schemaEntry : executor.database.getSchemas().entrySet()) {
                    for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                        executor.database.recordAnalyzedTable(schemaEntry.getKey() + "." + tableEntry.getKey());
                        tableEntry.getValue().setLastAnalyze(now);
                    }
                }
            } else if (name.equals("vacuum")) {
                // VACUUM without a table name: vacuum all tables
                for (Map.Entry<String, Schema> schemaEntry : executor.database.getSchemas().entrySet()) {
                    for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                        tableEntry.getValue().setLastVacuum(now);
                        if (vacuumAnalyze) {
                            executor.database.recordAnalyzedTable(schemaEntry.getKey() + "." + tableEntry.getKey());
                            tableEntry.getValue().setLastAnalyze(now);
                        }
                    }
                }
            }
            // Mark extended statistics as analyzed for the table(s)
            if (name.equals("analyze") || vacuumAnalyze) {
                for (ExtendedStatistic es : executor.database.getAllExtendedStatistics().values()) {
                    if (val != null && val.startsWith("table:")) {
                        String tblName = val.substring("table:".length());
                        if (es.getTableName().equalsIgnoreCase(tblName)) {
                            es.setAnalyzed(true);
                        }
                    } else {
                        es.setAnalyzed(true);
                    }
                }
            }
            // VACUUM VERBOSE: emit NOTICE
            if (vacuumVerbose && executor.session != null) {
                String tblInfo = (val != null && val.startsWith("table:")) ? val.substring("table:".length()) : "all tables";
                executor.session.addNotice("NOTICE", "00000",
                        "vacuuming \"public." + tblInfo + "\"", null);
            }
            return QueryResult.message(QueryResult.Type.SET, name.equals("analyze") ? "ANALYZE" : "VACUUM");
        }

        if (name.equals("cluster")) {
            String val = stmt.value();
            // Parse table and index from value: "table:foo,index:bar"
            if (val != null && val.contains("table:")) {
                String tblName = null;
                String idxName = null;
                for (String part : val.split(",")) {
                    if (part.startsWith("table:")) tblName = part.substring("table:".length());
                    if (part.startsWith("index:")) idxName = part.substring("index:".length());
                }
                if (tblName != null) {
                    executor.resolveTable(executor.defaultSchema(), tblName);
                    // L8: CLUSTER table without specifying an index requires a previously clustered index
                    if (idxName == null) {
                        boolean hasClustered = false;
                        // Scan all known indexes for this table to see if any are clustered
                        for (Map.Entry<String, String> e : executor.database.getIndexTableNames().entrySet()) {
                            String idxTable = e.getValue();
                            if (idxTable != null && (idxTable.equalsIgnoreCase(tblName)
                                    || idxTable.endsWith("." + tblName))) {
                                if (executor.database.isClusteredIndex(e.getKey())) {
                                    hasClustered = true;
                                    break;
                                }
                            }
                        }
                        if (!hasClustered) {
                            throw new MemgresException(
                                "there is no previously clustered index for table \"" + tblName + "\"", "42704");
                        }
                    }
                }
                if (idxName != null) {
                    executor.database.setClusteredIndex(idxName);
                }
            }
            return QueryResult.message(QueryResult.Type.SET, "CLUSTER");
        }

        // ---- Text Search DDL handling ----
        if (name.equals("create_ts_config")) {
            String[] parts = stmt.value().split("\0", -1);
            String cfgName = parts[0];
            String copyFrom = parts.length > 1 && !parts[1].isEmpty() ? parts[1] : null;
            String parserName = parts.length > 2 && !parts[2].isEmpty() ? parts[2] : null;
            executor.database.addTsConfig(new Database.TsConfigDef(cfgName, parserName, copyFrom));
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("create_ts_dict")) {
            String[] parts = stmt.value().split("\0", -1);
            String dictName = parts[0];
            String template = parts.length > 1 && !parts[1].isEmpty() ? parts[1] : null;
            String options = parts.length > 2 && !parts[2].isEmpty() ? parts[2] : null;
            executor.database.addTsDict(new Database.TsDictDef(dictName, template, options));
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("drop_ts_configuration")) {
            executor.database.removeTsConfig(stmt.value());
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("drop_ts_dictionary")) {
            executor.database.removeTsDict(stmt.value());
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("drop_ts_parser") || name.equals("drop_ts_template")) {
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("alter_ts_config_mapping")) {
            String[] parts = stmt.value().split("\0", -1);
            String cfgName = parts[0];
            executor.ddlExecutor.requireObjectExists("text search configuration", cfgName);
            String[] tokenTypes = parts[1].split(",");
            String dictNames = parts[2]; // comma-separated dict names
            for (String tt : tokenTypes) {
                executor.database.addTsConfigMap(cfgName, tt.trim(), dictNames);
            }
            return QueryResult.command(QueryResult.Type.SET, 0);
        }

        // ---- FDW DDL handling ----
        if (name.equals("create_fdw")) {
            String[] parts = stmt.value().split("\0", -1);
            String fdwName = parts[0];
            String options = parts.length > 1 && !parts[1].isEmpty() ? parts[1] : null;
            executor.database.addForeignDataWrapper(new Database.FdwWrapper(fdwName, options));
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("drop_fdw")) {
            executor.database.removeForeignDataWrapper(stmt.value());
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("create_server")) {
            String[] parts = stmt.value().split("\0", -1);
            String srvName = parts[0];
            String fdwName = parts.length > 1 ? parts[1] : "";
            String options = parts.length > 2 && !parts[2].isEmpty() ? parts[2] : null;
            executor.database.addForeignServer(new Database.FdwServer(srvName, fdwName, options));
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("drop_server")) {
            executor.database.removeForeignServer(stmt.value());
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("alter_server_options")) {
            String[] parts = stmt.value().split("\0", -1);
            String srvName = parts[0];
            String newOptions = parts.length > 1 && !parts[1].isEmpty() ? parts[1] : null;
            executor.ddlExecutor.requireObjectExists("server", srvName);
            Database.FdwServer srv = executor.database.getForeignServer(srvName);
            if (srv != null) {
                srv.options = newOptions != null ? newOptions : srv.options;
            }
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("create_user_mapping")) {
            String[] parts = stmt.value().split("\0", -1);
            String serverName = parts[0];
            String userName = parts.length > 1 ? parts[1] : "PUBLIC";
            String options = parts.length > 2 && !parts[2].isEmpty() ? parts[2] : null;
            executor.database.addForeignUserMapping(new Database.FdwUserMapping(serverName, userName, options));
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("drop_user_mapping")) {
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("create_foreign_table")) {
            String[] parts = stmt.value().split("\0", -1);
            String ftName = parts[0];
            String serverName = parts.length > 1 ? parts[1] : "";
            String options = parts.length > 2 && !parts[2].isEmpty() ? parts[2] : null;
            String colStr = parts.length > 3 ? parts[3] : "";
            java.util.List<String[]> columns = new java.util.ArrayList<>();
            if (!colStr.isEmpty()) {
                for (String colDef : colStr.split("\n")) {
                    String[] colParts = colDef.split("\t", 2);
                    columns.add(colParts);
                }
            }
            // A foreign table is created in the schema the statement names, or in the first
            // schema of the search_path -- and it takes that name in that schema, so a relation
            // of any other kind already holding it is 42P07.
            String written = parts.length > 4 && !parts[4].isEmpty() ? parts[4] : null;
            if (written != null) {
                SchemaQualifier.requireSchema(executor.database, executor.session, written);
            }
            String ftSchema = written != null ? written : executor.defaultSchema();
            // IF NOT EXISTS steps aside for whatever holds the name, foreign table or not: PG
            // asks the relation namespace, not its own catalog, and leaves what is there alone.
            boolean ifNotExists = parts.length > 5 && "true".equals(parts[5]);
            if (ifNotExists && RelationNamespace.kindOf(executor.database, ftSchema, ftName) != null) {
                return QueryResult.command(QueryResult.Type.SET, 0);
            }
            RelationNamespace.requireFree(executor.database, ftSchema, ftName, null);
            // memgres keys foreign tables by their bare name, so a name reused in a second
            // schema replaces the first; forgetting the old attribution keeps the catalogs
            // from reporting one foreign table as living in two schemas at once.
            ForeignTables.unregisterEverywhere(executor.database, ftName);
            executor.database.addForeignTable(new Database.FdwForeignTable(ftName, serverName, options, columns));
            ForeignTables.register(executor.database, ftSchema, ftName);
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("drop_foreign_table")) {
            String[] parts = stmt.value().split("\0", -1);
            String ftName = parts[0];
            String written = parts.length > 1 && !parts[1].isEmpty() ? parts[1] : null;
            boolean ifExists = parts.length > 2 && "true".equals(parts[2]);
            if (written != null) {
                SchemaQualifier.requireSchema(executor.database, executor.session, written);
            }
            String ftSchema = written != null ? written : executor.defaultSchema();
            if (!ForeignTables.existsIn(executor.database, ftSchema, ftName)) {
                if (ifExists) return QueryResult.command(QueryResult.Type.SET, 0);
                // Something else of that name in this schema is a wrong-kind drop; nothing of
                // that name is a foreign table that is not there.
                RelationNamespace.requireKind(executor.database, ftSchema, ftName,
                        RelationNamespace.FOREIGN_TABLE);
                throw new MemgresException("foreign table \"" + ftName + "\" does not exist", "42704");
            }
            executor.database.removeForeignTable(ftName);
            ForeignTables.unregister(executor.database, ftSchema, ftName);
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("import_foreign_schema")) {
            String serverName = stmt.value();
            // Look up the server to get its FDW name for the error message
            String fdwName = null;
            if (serverName != null && !serverName.isEmpty()) {
                Database.FdwServer srv = executor.database.getForeignServer(serverName);
                if (srv != null) {
                    fdwName = srv.fdwName;
                }
            }
            String displayFdw = (fdwName != null && !fdwName.isEmpty()) ? fdwName
                    : (serverName != null && !serverName.isEmpty() ? serverName : "unknown");
            throw new MemgresException("foreign-data wrapper \"" + displayFdw + "\" has no handler", "55000");
        }

        // ---- Publication / Subscription DDL handling ----
        if (name.equals("create_publication")) {
            String[] parts = stmt.value().split("\0", -1);
            String pubName = parts[0];
            boolean allTables = parts.length > 1 && "true".equals(parts[1]);
            java.util.List<String> tables = parts.length > 2 && !parts[2].isEmpty()
                    ? java.util.Arrays.asList(parts[2].split(",")) : new java.util.ArrayList<>();
            String schemaName = parts.length > 3 && !parts[3].isEmpty() ? parts[3] : null;
            executor.database.addPublication(new Database.PubDef(pubName, allTables, tables, schemaName));
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("alter_publication_rename")) {
            String[] parts = stmt.value().split("\0", 2);
            Database.PubDef pub = requirePublication(parts[0]);
            if (!parts[0].equalsIgnoreCase(parts[1])
                    && executor.database.getPublication(parts[1]) != null) {
                throw new MemgresException(
                        "publication \"" + parts[1] + "\" already exists", "42710");
            }
            executor.database.removePublication(parts[0]);
            executor.database.addPublication(new Database.PubDef(parts[1], pub.allTables,
                    pub.tables, pub.schemaName));
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("alter_publication_noop")) {
            requirePublication(stmt.value());
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("alter_publication_add_table")) {
            String[] parts = stmt.value().split("\0", 2);
            String pubName = parts[0];
            String[] newTables = parts.length > 1 ? parts[1].split(",") : new String[0];
            Database.PubDef pub = requirePublication(pubName);
            // A publication lists relations, so a name that is not one cannot join it, and one
            // already listed would be listed twice.
            for (String t : newTables) {
                if (t.isEmpty()) continue;
                requirePublishableRelation(t);
                if (pub != null && containsIgnoreCase(pub.tables, t)) {
                    throw new MemgresException("relation \"" + t
                            + "\" is already member of publication \"" + pubName + "\"", "42710");
                }
            }
            if (pub != null) {
                for (String t : newTables) {
                    if (!t.isEmpty()) pub.tables.add(t);
                }
            }
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("alter_publication_set_table")) {
            String[] parts = stmt.value().split("\0", 2);
            String pubName = parts[0];
            String[] newTables = parts.length > 1 ? parts[1].split(",") : new String[0];
            Database.PubDef pub = requirePublication(pubName);
            for (String t : newTables) {
                if (!t.isEmpty()) requirePublishableRelation(t);
            }
            if (pub != null) {
                pub.tables.clear();
                for (String t : newTables) {
                    if (!t.isEmpty()) pub.tables.add(t);
                }
            }
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("alter_publication_drop_table")) {
            String[] parts = stmt.value().split("\0", 2);
            String pubName = parts[0];
            String[] dropTables = parts.length > 1 ? parts[1].split(",") : new String[0];
            Database.PubDef pub = requirePublication(pubName);
            for (String t : dropTables) {
                if (t.isEmpty()) continue;
                requirePublishableRelation(t);
                if (pub != null && !containsIgnoreCase(pub.tables, t)) {
                    throw new MemgresException("relation \"" + t
                            + "\" is not part of the publication", "42704");
                }
            }
            if (pub != null) {
                for (String t : dropTables) pub.tables.remove(t);
            }
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("alter_publication_owner")) {
            String[] parts = stmt.value().split("\0", 2);
            requirePublication(parts[0]);
            requireRoleForAlter(parts.length > 1 ? parts[1] : "");
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("alter_publication_options")) {
            String[] parts = stmt.value().split("\0", 2);
            requirePublication(parts[0]);
            String keys = parts.length > 1 ? parts[1] : "";
            for (String key : keys.split(",")) {
                if (key.isEmpty()) continue;
                if (!PUBLICATION_PARAMETERS.contains(key.toLowerCase())) {
                    throw new MemgresException(
                            "unrecognized publication parameter: \"" + key + "\"", "42601");
                }
            }
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("drop_publication")) {
            executor.database.removePublication(stmt.value());
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("create_subscription")) {
            // Subscriptions require an actual replication connection which memgres cannot provide.
            // Store in catalog for DDL coverage compatibility.
            String[] parts = stmt.value().split("\0", -1);
            String subName = parts.length > 0 ? parts[0] : "";
            String conninfo = parts.length > 1 ? parts[1] : "";
            String pubName = parts.length > 2 ? parts[2] : "";
            executor.database.addSubscription(new Database.SubDef(subName, conninfo, pubName));
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("drop_subscription")) {
            executor.database.removeSubscription(stmt.value());
            return QueryResult.command(QueryResult.Type.SET, 0);
        }

        if (name.equals("alter_aggregate")) {
            String[] parts = stmt.value().split("\0", -1);
            java.util.List<String> argTypes = parts[1].isEmpty()
                    ? Cols.listOf() : java.util.Arrays.asList(parts[1].split("\1"));
            PgAggregate agg = executor.database.getAggregate(parts[0]);
            if (agg == null || !DdlObjectExecutor.aggregateArgsMatch(agg, argTypes)) {
                throw new MemgresException("aggregate " + parts[0] + "("
                        + DdlObjectExecutor.canonicalTypeList(argTypes) + ") does not exist", "42883");
            }
            // Neither a move nor a change of owner is recorded for an aggregate, but the schema
            // and the role still have to exist for PostgreSQL to accept the statement.
            if (parts.length > 4 && !parts[4].isEmpty()) requireSchemaForAlter(parts[4]);
            if (parts.length > 5 && !parts[5].isEmpty()) requireRoleForAlter(parts[5]);
            if ("rename".equals(parts[2])) {
                executor.database.removeAggregate(parts[0]);
                PgAggregate renamed = new PgAggregate(parts[3], agg.getSfunc(), agg.getStype(),
                        agg.getInitcond(), agg.getFinalfunc(), agg.getCombinefunc(), agg.getSortop(),
                        agg.getArgTypes());
                renamed.setSchemaName(agg.getSchemaName());
                executor.database.addAggregate(renamed);
            }
            return QueryResult.command(QueryResult.Type.SET, 0);
        }

        // CREATE STATISTICS
        if (name.equals("create_statistics")) {
            String[] parts = stmt.value().split("\0", -1);
            String statName = parts[0];
            String tableName = parts[1];
            String colsStr = parts[2];
            String kindsStr = parts.length > 3 ? parts[3] : "";
            java.util.List<String> cols = colsStr.isEmpty() ? Cols.listOf() : java.util.Arrays.asList(colsStr.split(","));
            java.util.List<String> kinds = kindsStr.isEmpty() ? Cols.listOf() : java.util.Arrays.asList(kindsStr.split(","));
            boolean ifNotExists = parts.length > 4 && "1".equals(parts[4]);
            if (ifNotExists && executor.database.getExtendedStatistic(statName) != null) {
                if (executor.session != null) {
                    executor.session.addNotice("NOTICE", "42710",
                            "statistics object \"" + statName + "\" already exists, skipping", null);
                }
                return QueryResult.command(QueryResult.Type.SET, 0);
            }
            validateStatisticsDefinition(statName, tableName, cols, kinds);
            ExtendedStatistic stat = new ExtendedStatistic(statName, tableName, cols, kinds);
            executor.database.addExtendedStatistic(stat);
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("alter_statistics_rename")) {
            String[] parts = stmt.value().split("\0", 2);
            ExtendedStatistic stat = requireStatistic(parts[0]);
            // Renaming onto a name already taken would drop the object that holds it, so the
            // collision has to be refused before anything is removed.
            if (executor.database.getExtendedStatistic(parts[1]) != null) {
                throw new MemgresException("statistics object \"" + parts[1]
                        + "\" already exists in schema \"" + executor.defaultSchema() + "\"", "42710");
            }
            executor.database.removeExtendedStatistic(parts[0]);
            stat.setName(parts[1]);
            executor.database.addExtendedStatistic(stat);
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("alter_statistics_move")) {
            String[] parts = stmt.value().split("\0", -1);
            requireStatistic(parts[0]);
            if (parts.length > 1 && !parts[1].isEmpty()) requireSchemaForAlter(parts[1]);
            if (parts.length > 2 && !parts[2].isEmpty()) requireRoleForAlter(parts[2]);
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("alter_statistics_target")) {
            String[] parts = stmt.value().split("\0", 2);
            requireStatistic(parts[0]).setStattarget(Integer.parseInt(parts[1]));
            return QueryResult.command(QueryResult.Type.SET, 0);
        }
        if (name.equals("drop_statistics")) {
            String[] parts = stmt.value().split("\0", -1);
            boolean ifExists = parts.length > 1 && "1".equals(parts[1]);
            if (!ifExists || executor.database.getExtendedStatistic(parts[0]) != null) {
                requireStatistic(parts[0]);
                executor.database.removeExtendedStatistic(parts[0]);
            }
            return QueryResult.command(QueryResult.Type.SET, 0);
        }

        // CREATE OPERATOR - silently accepted as no-op
        if (name.equals("create_operator")) {
            return QueryResult.command(QueryResult.Type.SET, 0);
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
                    if (force) {
                        for (Session s : new java.util.ArrayList<>(otherSessions)) {
                            s.close();
                        }
                    } else {
                        otherSessions = waitForSessionsToEnd(targetDb);
                        if (!otherSessions.isEmpty()) {
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
                if (targetDb != null && !waitForSessionsToEnd(targetDb).isEmpty()) {
                    throw new MemgresException(
                        "database \"" + oldName + "\" is being accessed by other users", "55006");
                }
                reg.renameDatabase(oldName, newName);
            }
            return QueryResult.message(QueryResult.Type.SET, "ALTER DATABASE");
        }

        if ("set_transaction".equals(name)) {
            applySetTransaction(stmt.value());
            return QueryResult.message(QueryResult.Type.SET, "SET");
        }
        if ("session_characteristics".equals(name)) {
            applySessionCharacteristics(stmt.value());
            return QueryResult.message(QueryResult.Type.SET, "SET");
        }

        // SET CONSTRAINTS { ALL | name [, ...] } { DEFERRED | IMMEDIATE }
        if ("constraints".equals(name) && stmt.value() != null && !stmt.value().isEmpty()) {
            String val = stmt.value();
            int colonIdx = val.lastIndexOf(':');
            if (colonIdx > 0) {
                String namesStr = val.substring(0, colonIdx);
                String mode = val.substring(colonIdx + 1);
                boolean deferred = "DEFERRED".equalsIgnoreCase(mode);
                if ("ALL".equals(namesStr)) {
                    if (executor.session != null) {
                        executor.session.setAllConstraintsDeferred(deferred);
                        // Switching to IMMEDIATE is what makes the checks happen now: PG runs
                        // everything still pending at that point, so a violation is reported by
                        // the statement that asked for the check rather than surviving to COMMIT
                        // — or disappearing entirely if the transaction rolls back.
                        if (!deferred) executor.session.runPendingDeferredChecks(null);
                    }
                } else {
                    // A name that matches no constraint would silently do nothing, leaving the
                    // caller believing it had changed when a constraint fires. A name that is
                    // not deferrable cannot be deferred at all, and PG says so rather than
                    // storing an override that never applies.
                    for (String cn : namesStr.split(",")) {
                        String constraintName = bareConstraintName(cn.trim());
                        StoredConstraint sc = findConstraint(constraintName);
                        if (sc == null) {
                            throw PgErrors.undefinedObject("constraint", constraintName);
                        }
                        // IMMEDIATE is what a non-deferrable constraint already is, so only asking
                        // for DEFERRED is a request it cannot honour.
                        if (deferred && !sc.isDeferrable()) {
                            throw new MemgresException(
                                    "constraint \"" + constraintName + "\" is not deferrable", "42809");
                        }
                    }
                    if (executor.session != null) {
                        for (String cn : namesStr.split(",")) {
                            executor.session.setConstraintDeferred(bareConstraintName(cn.trim()), deferred);
                        }
                        if (!deferred) {
                            for (String cn : namesStr.split(",")) {
                                executor.session.runPendingDeferredChecks(bareConstraintName(cn.trim()));
                            }
                        }
                    }
                }
                // Outside a transaction block the statement is its own transaction, so the mode
                // it set ends with it. Letting it persist made a later transaction check a
                // deferred constraint immediately, which is the opposite of what was asked for
                // and had nothing to do with the statement that asked.
                if (executor.session != null && !executor.session.isExplicitTransactionBlock()) {
                    executor.session.clearConstraintModes();
                }
                return QueryResult.message(QueryResult.Type.SET, "SET CONSTRAINTS");
            }
        }

        if (name.equals("create_stub") || name.equals("drop_stub") || name.equals("alter_stub")
                || name.equals("alter_rule") || name.equals("alter_trigger")) {
            return executeStubObject(name, stmt.value());
        }

        if (name.equals("alter_object")) {
            return executor.ddlExecutor.executeAlterObject(stmt.value());
        }
        if (name.equals("drop_object")) {
            return executor.ddlExecutor.executeDropObject(stmt.value());
        }

        if (name.equals("alter_extension_set_schema")) {
            String val = stmt.value();
            int colonIdx = val.indexOf(':');
            if (colonIdx > 0) {
                String extName = val.substring(0, colonIdx);
                String newSchema = val.substring(colonIdx + 1);
                executor.ddlExecutor.requireObjectExists("extension", extName);
                executor.database.setExtensionSchema(extName, newSchema);
            }
            return QueryResult.message(QueryResult.Type.SET, "ALTER EXTENSION");
        }
        if (name.equals("alter_extension_update")) {
            // No-op for now - just acknowledge
            return QueryResult.message(QueryResult.Type.SET, "ALTER EXTENSION");
        }

        if (name.equals("security_label")) {
            String val = stmt.value();
            if (val != null && val.startsWith("provider:")) {
                String providerName = val.substring("provider:".length());
                throw new MemgresException(
                        "security label provider \"" + providerName + "\" is not loaded", "22023");
            }
            throw new MemgresException("no security label providers have been loaded", "22023");
        }

        Set<String> internalNames = Cols.setOf("constraints", "transaction",
                "create_noop", "alter_noop", "drop_noop",
                "create_statistics", "alter_statistics_rename", "alter_statistics_target",
                "alter_statistics_move", "drop_statistics",
                "create_fdw", "drop_fdw", "create_server", "drop_server", "alter_server_options",
                "create_user_mapping", "drop_user_mapping",
                "create_foreign_table", "drop_foreign_table", "import_foreign_schema",
                "create_publication", "alter_publication_add_table", "alter_publication_set_table",
                "alter_publication_drop_table", "alter_publication_rename",
                "alter_publication_owner", "alter_publication_options",
                "alter_publication_noop", "drop_publication",
                "create_subscription", "drop_subscription",
                "create_ts_config", "create_ts_dict", "drop_ts_configuration", "drop_ts_dictionary",
                "drop_ts_parser", "drop_ts_template", "alter_ts_config_mapping",
                "drop_owned", "reassign_owned", "do_block", "comment",
                "analyze", "vacuum",
                "cluster", "checkpoint", "load");

        // PG rejects unknown flat (non-dotted) parameter names with 42704.
        // Dotted names (e.g. my_ext.setting) are allowed as custom variables.
        if (guc != null && !internalNames.contains(name) && !name.contains(".") && !guc.isKnown(name)) {
            throw new MemgresException("unrecognized configuration parameter \"" + name + "\"", "42704");
        }

        if (guc != null && !internalNames.contains(name)) {
            String value = stmt.value();
            // SET param TO DEFAULT is equivalent to RESET param, and is refused for the same
            // parameters RESET is: putting one back to its default is still changing it.
            if (value != null && value.equalsIgnoreCase("DEFAULT")) {
                GucSettings.checkAssignable(name, null);
                guc.reset(name);
                return QueryResult.message(QueryResult.Type.SET, "SET");
            }
            // Validate type based on parameter name
            if (value != null && !value.isEmpty()) {
                validateGucValue(name, value);
            }
            // H37: Normalize DateStyle to canonical PG form before storing
            if (name.equals("datestyle") && value != null) {
                value = normalizeDateStyle(value, guc.get("datestyle"));
            }
            if (stmt.isLocal()) {
                if (executor.session != null && !executor.session.isInTransaction()) {
                    // M13: SET LOCAL outside transaction is a warning + no-op (PG behavior)
                    executor.session.addNotice("WARNING", "25P01",
                            "SET LOCAL can only be used in transaction blocks", null);
                    // Don't apply the value — it's a no-op outside transactions
                } else {
                    guc.setLocal(name, value);
                }
            } else if (TRANSACTION_SCOPED_GUCS.contains(name)
                    && executor.session != null && !executor.session.isInTransaction()) {
                // These belong to a transaction and are reset when the next one starts, so
                // setting one with no transaction open changes nothing that outlives the
                // statement. Storing it would make it stick, which PG never does.
                executor.session.addNotice("WARNING", "25P01",
                        "SET TRANSACTION can only be used in transaction blocks", null);
            } else {
                guc.set(name, value);
            }
        }
        return QueryResult.message(QueryResult.Type.SET, "SET");
    }

    /** The encodings a client may ask to speak. */
    private static final Set<String> KNOWN_CLIENT_ENCODINGS = new HashSet<>(Arrays.asList(
            "UTF8", "UNICODE", "SQLASCII", "LATIN1", "LATIN2", "LATIN3", "LATIN4", "LATIN5",
            "LATIN6", "LATIN7", "LATIN8", "LATIN9", "LATIN10", "WIN1250", "WIN1251", "WIN1252",
            "WIN1253", "WIN1254", "WIN1255", "WIN1256", "WIN1257", "WIN1258", "WIN866", "WIN874",
            "KOI8R", "KOI8U", "ISO88595", "ISO88596", "ISO88597", "ISO88598", "EUCJP", "EUCCN",
            "EUCKR", "EUCTW", "EUCJIS2004", "SJIS", "SHIFTJIS", "SHIFTJIS2004", "BIG5", "GBK",
            "UHC", "GB18030", "JOHAB", "MULE_INTERNAL", "MULEINTERNAL"));

    /** A setting value as it was written, with the quotes that carried it taken off. */
    private static String unquoteSetting(String value) {
        String trimmed = value.trim();
        if (trimmed.length() >= 2
                && ((trimmed.startsWith("'") && trimmed.endsWith("'"))
                    || (trimmed.startsWith("\"") && trimmed.endsWith("\"")))) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    /** GUCs whose value belongs to one transaction and is re-derived when the next one starts. */
    private static final Set<String> TRANSACTION_SCOPED_GUCS = Cols.setOf(
            "transaction_isolation", "transaction_read_only", "transaction_deferrable");

    /** True for a setting that belongs to the current transaction rather than to the session. */
    static boolean isTransactionScopedGuc(String name) {
        return name != null && TRANSACTION_SCOPED_GUCS.contains(name.toLowerCase());
    }

    // ---- DO block ----

    /** The isolation levels PostgreSQL accepts, in the order its hint lists them. */
    private static final List<String> ISOLATION_LEVELS = java.util.Arrays.asList(
            "serializable", "repeatable read", "read committed", "read uncommitted");

    /** Validate a GUC parameter value based on the known type of the parameter. */
    void validateGucValue(String name, String value) {
        if (value == null || Strs.isBlank(value)) return;
        String lname = name.toLowerCase();
        // An isolation level the engine does not have would leave the session claiming an
        // isolation it is not providing, so the name is checked where it is set.
        if (lname.equals("default_transaction_isolation") || lname.equals("transaction_isolation")) {
            String level = value.trim();
            if (level.length() >= 2 && ((level.startsWith("'") && level.endsWith("'"))
                    || (level.startsWith("\"") && level.endsWith("\"")))) {
                level = level.substring(1, level.length() - 1);
            }
            level = level.toLowerCase();
            if (!ISOLATION_LEVELS.contains(level)) {
                throw new MemgresException("invalid value for parameter \"" + lname + "\": \"" + level + "\""
                        + "\n  Hint: Available values: " + String.join(", ", ISOLATION_LEVELS) + ".", "22023");
            }
            return;
        }
        // The parameter's own definition decides what may be assigned to it: a preset cannot be
        // set at all, an enum takes one of its listed values, and a number has to land inside the
        // range pg_settings reports. Only the settings memgres carries no definition for, and the
        // few whose spelling needs its own reading, are judged below.
        GucSettings.checkAssignable(lname, value);
        boolean defined = GucSettings.definition(lname) != null;
        // Boolean parameters — includes row_security, jit, synchronize_seqscans, etc.
        if (!defined && (lname.equals("enable_seqscan") || lname.equals("enable_hashjoin") || lname.equals("enable_indexscan")
                || lname.startsWith("enable_") || lname.equals("fsync") || lname.equals("log_checkpoints")
                || lname.equals("log_connections") || lname.equals("log_disconnections")
                || lname.equals("row_security") || lname.equals("jit")
                || lname.equals("synchronize_seqscans") || lname.equals("check_function_bodies")
                || lname.equals("synchronous_commit") || lname.equals("ssl")
                || lname.equals("parallel_leader_participation"))) {
            String lv = value.toLowerCase().trim();
            if (!lv.equals("on") && !lv.equals("off") && !lv.equals("true") && !lv.equals("false")
                    && !lv.equals("yes") && !lv.equals("no") && !lv.equals("1") && !lv.equals("0")) {
                throw new MemgresException("parameter \"" + name + "\" requires a Boolean value", "22023");
            }
            return;
        }
        // DateStyle: validate format (PG 22023 for invalid)
        if (lname.equals("datestyle")) {
            String trimmed = value.trim();
            // Remove surrounding quotes
            if ((trimmed.startsWith("'") && trimmed.endsWith("'"))
                    || (trimmed.startsWith("\"") && trimmed.endsWith("\""))) {
                trimmed = trimmed.substring(1, trimmed.length() - 1);
            }
            // Parse comma-separated parts and validate each
            String[] parts = trimmed.split("[,\\s]+");
            boolean validStyle = false;
            boolean validOrder = false;
            for (String part : parts) {
                String p = part.trim().toLowerCase();
                if (p.isEmpty()) continue;
                if (p.equals("iso") || p.equals("sql") || p.equals("postgres") || p.equals("german")) {
                    validStyle = true;
                } else if (p.equals("dmy") || p.equals("mdy") || p.equals("ymd")
                        || p.equals("euro") || p.equals("us")) {
                    validOrder = true;
                } else {
                    throw new MemgresException("invalid value for parameter \"DateStyle\": \"" + value + "\"", "22023");
                }
            }
            if (!validStyle && !validOrder) {
                throw new MemgresException("invalid value for parameter \"DateStyle\": \"" + value + "\"", "22023");
            }
            // Normalize to PG canonical form (e.g. "ISO, DMY")
            // This prevents bad ParameterStatus values from killing pgjdbc (H37)
            return;
        }
        // statement_timeout / lock_timeout / timeout params: must be numeric or have valid unit
        if (!defined && (lname.equals("statement_timeout") || lname.equals("lock_timeout")
                || lname.equals("idle_in_transaction_session_timeout") || lname.equals("transaction_timeout"))) {
            long ms = GucSettings.parseTimeoutMillis(value);
            if (ms < 0) {
                throw new MemgresException("invalid value for parameter \"" + name + "\": \"" + value + "\"", "22023");
            }
            return;
        }
        // Memory / integer parameters (accept numbers with optional unit like MB, kB, etc.)
        if (!defined && (lname.equals("work_mem") || lname.equals("maintenance_work_mem") || lname.equals("shared_buffers")
                || lname.equals("effective_cache_size") || lname.equals("max_connections")
                || lname.equals("max_wal_size") || lname.equals("min_wal_size")
                || lname.endsWith("_mem") || lname.endsWith("_buffers"))) {
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
        // An encoding the server cannot speak is not a client encoding, and setting one would
        // have every later value written in a form the client cannot read.
        if (lname.equals("client_encoding")) {
            String encoding = unquoteSetting(value).toUpperCase(java.util.Locale.ROOT)
                    .replace("-", "").replace("_", "");
            if (!KNOWN_CLIENT_ENCODINGS.contains(encoding)) {
                throw new MemgresException("invalid value for parameter \"client_encoding\": \""
                        + unquoteSetting(value) + "\"", "22023");
            }
            return;
        }
        // A role a session may be set to has to be one the server has.
        if (lname.equals("role") || lname.equals("session_authorization")) {
            String role = unquoteSetting(value);
            if (!role.equalsIgnoreCase("none") && !role.equalsIgnoreCase("default")) {
                requireRole(role, lname);
            }
            return;
        }
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
                // A zone nobody has is an invalid value for the parameter, which is how
                // PostgreSQL reports it, by statement or by set_config alike.
                throw new MemgresException(
                        "invalid value for parameter \"TimeZone\": \"" + tz + "\"", "22023");
            }
        }
    }

    /** Read one mode out of the encoded "iso=...;ro=on;def=off" form, or null when absent. */
    private static String encodedMode(String encoded, String key) {
        if (encoded == null) return null;
        for (String part : encoded.split(";")) {
            if (part.startsWith(key + "=")) return part.substring(key.length() + 1);
        }
        return null;
    }

    /**
     * Apply {@code SET TRANSACTION transaction_mode [, ...]}.
     *
     * <p>Isolation and deferrability describe the snapshot a transaction runs against, so they can
     * only be chosen before it has taken one; PostgreSQL refuses them afterwards rather than
     * pretending a statement already run happened at the new level. Outside a transaction block
     * the statement has nothing to configure and PostgreSQL warns instead of changing the session
     * — silently rewriting the session default would outlive the statement that asked for it.
     */
    private void applySetTransaction(String encoded) {
        Session session = executor.session;
        if (session == null) return;
        String iso = encodedMode(encoded, "iso");
        String ro = encodedMode(encoded, "ro");
        String def = encodedMode(encoded, "def");
        // A procedural body runs inside the transaction the calling statement started, and that
        // statement is a query the transaction has already run. Read as a session outside any
        // block, a DO block setting the isolation level was allowed to say nothing at all.
        if (iso != null && (session.isInFunctionContext() || session.isInRoutine())) {
            throw new MemgresException(
                    "SET TRANSACTION ISOLATION LEVEL must be called before any query", "25001");
        }
        if (!session.isInTransaction()) {
            if (iso != null) warnOutsideBlock(session, "SET TRANSACTION");
            if (ro != null) warnOutsideBlock(session, "SET TRANSACTION");
            if (def != null) warnOutsideBlock(session, "SET TRANSACTION");
            return;
        }
        if (iso != null) {
            if (session.hasSubtransaction()) {
                throw new MemgresException(
                        "SET TRANSACTION ISOLATION LEVEL must not be called in a subtransaction", "25001");
            }
            if (session.hasRunQueryInTransaction()) {
                throw new MemgresException(
                        "SET TRANSACTION ISOLATION LEVEL must be called before any query", "25001");
            }
            session.getGucSettings().set("transaction_isolation", iso);
        }
        if (def != null) {
            if (session.hasSubtransaction()) {
                throw new MemgresException(
                        "SET TRANSACTION [NOT] DEFERRABLE must not be called in a subtransaction", "25001");
            }
            if (session.hasRunQueryInTransaction()) {
                throw new MemgresException(
                        "SET TRANSACTION [NOT] DEFERRABLE must be called before any query", "25001");
            }
            session.getGucSettings().set("transaction_deferrable", def);
        }
        if (ro != null) {
            // A transaction may be made read-only at any point, but a read-only one cannot be
            // made read-write again once it has run a query: that would change the mode a
            // statement already ran under. Only isolation and deferrability were guarded, so the
            // one direction that does matter went through silently.
            boolean currentlyReadOnly = "on".equals(session.getGucSettings().get("transaction_read_only"));
            if ("off".equals(ro) && currentlyReadOnly && session.hasRunQueryInTransaction()) {
                throw new MemgresException(
                        "transaction read-write mode must be set before any query", "25001");
            }
            session.getGucSettings().set("transaction_read_only", ro);
        }
    }

    private static void warnOutsideBlock(Session session, String command) {
        session.addNotice("WARNING", "25P01",
                command + " can only be used in transaction blocks", null);
    }

    /** Apply {@code SET SESSION CHARACTERISTICS AS TRANSACTION transaction_mode [, ...]}. */
    private void applySessionCharacteristics(String encoded) {
        Session session = executor.session;
        if (session == null) return;
        String iso = encodedMode(encoded, "iso");
        String ro = encodedMode(encoded, "ro");
        String def = encodedMode(encoded, "def");
        if (iso != null) session.getGucSettings().set("default_transaction_isolation", iso);
        if (ro != null) session.getGucSettings().set("default_transaction_read_only", ro);
        if (def != null) session.getGucSettings().set("default_transaction_deferrable", def);
    }

    /** Normalize a DateStyle value to PG canonical form (e.g. "ISO, DMY"). */
    /**
     * DateStyle holds two things — an output style and a field order — and a value may name
     * either or both. The one it does not name keeps what it had; defaulting it instead meant
     * {@code SET datestyle = 'ISO'} silently put the order back to MDY.
     */
    private static String normalizeDateStyle(String value, String current) {
        String trimmed = value.trim();
        if ((trimmed.startsWith("'") && trimmed.endsWith("'"))
                || (trimmed.startsWith("\"") && trimmed.endsWith("\""))) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        String lower = trimmed.toLowerCase();
        String currentLower = current == null ? "" : current.toLowerCase();
        String style = null;
        if (lower.contains("sql")) style = "SQL";
        else if (lower.contains("postgres")) style = "Postgres";
        else if (lower.contains("german")) style = "German";
        else if (lower.contains("iso")) style = "ISO";
        if (style == null) {
            if (currentLower.contains("sql")) style = "SQL";
            else if (currentLower.contains("postgres")) style = "Postgres";
            else if (currentLower.contains("german")) style = "German";
            else style = "ISO";
        }
        String order = null;
        if (lower.contains("dmy") || lower.contains("euro")) order = "DMY";
        else if (lower.contains("ymd")) order = "YMD";
        else if (lower.contains("us") || lower.contains("mdy")) order = "MDY";
        if (order == null) {
            if (currentLower.contains("dmy")) order = "DMY";
            else if (currentLower.contains("ymd")) order = "YMD";
            else order = "MDY";
        }
        return style + ", " + order;
    }

    /** The role a grant names, with the session's own spelled out. */
    private String resolveRoleSpec(String role) {
        if (role == null) return null;
        String lower = role.toLowerCase();
        if (lower.equals("current_user") || lower.equals("session_user")
                || lower.equals("current_role")) {
            return executor.sessionUser();
        }
        return role;
    }

    /**
     * A role named in a membership grant has to exist — PUBLIC included, which is a grantee for
     * privileges but is not a role that can hold or be held as a membership.
     */
    private void requireGrantRole(String role) {
        if (role == null) return;
        String lower = role.toLowerCase();
        // A grant may name the session's own role rather than spell it out.
        if (lower.equals("current_user") || lower.equals("session_user")
                || lower.equals("current_role")) {
            return;
        }
        if (executor.database.getRoles().containsKey(lower)) return;
        String connecting = executor.session != null ? executor.session.getConnectingUser() : null;
        if (connecting != null && role.equalsIgnoreCase(connecting)) return;
        throw new MemgresException("role \"" + lower + "\" does not exist", "42704");
    }

    /** The collations every server has, whatever the catalogue was told to hold. */
    private static final Set<String> BUILT_IN_COLLATIONS = new HashSet<>(Arrays.asList(
            "default", "c", "posix", "ucs_basic", "unicode", "pg_c_utf8"));

    /** Whether pg_language has a row for this language. */
    private boolean languageExists(String language) {
        Table languages = executor.systemCatalog.resolve("pg_catalog", "pg_language", executor.session);
        int nameIdx = languages == null ? -1 : languages.getColumnIndex("lanname");
        if (nameIdx < 0) return false;
        for (Object[] row : languages.getRows()) {
            if (language.equalsIgnoreCase(String.valueOf(row[nameIdx]))) return true;
        }
        return false;
    }

    /**
     * A routine written the way a message names it: the name, then the argument types under the
     * names a reader would write them with.
     */
    private static String commentRoutineSignature(String name, String writtenArgs) {
        if (writtenArgs == null) return name;
        String args = writtenArgs.substring(1, writtenArgs.lastIndexOf(')')).trim();
        if (args.isEmpty()) return name + "()";
        StringBuilder sb = new StringBuilder(name).append('(');
        String[] parts = args.split(",");
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(readableTypeSpelling(parts[i].trim()));
        }
        return sb.append(')').toString();
    }

    /**
     * The routine of this name whose parameters are the ones written, or null when none is.
     * A name with no argument list names the routine of that name whatever it takes.
     */
    private PgFunction routineWithSignature(String name, String writtenArgs) {
        List<PgFunction> overloads = executor.database.getFunctionOverloads(name);
        if (overloads == null || overloads.isEmpty()) return null;
        if (writtenArgs == null) return overloads.get(0);
        String wanted = writtenArgs.substring(1, writtenArgs.lastIndexOf(')')).trim();
        List<String> want = new ArrayList<>();
        if (!wanted.isEmpty()) {
            for (String part : wanted.split(",")) want.add(readableTypeSpelling(part.trim()));
        }
        for (PgFunction fn : overloads) {
            List<String> have = new ArrayList<>();
            for (PgFunction.Param p : fn.getParams()) {
                String mode = p.mode() == null ? "IN" : p.mode().toUpperCase();
                if ("OUT".equals(mode)) continue;
                have.add(readableTypeSpelling(p.typeName() == null ? "" : p.typeName().trim()));
            }
            if (have.equals(want)) return fn;
        }
        return null;
    }

    private static String readableTypeSpelling(String written) {
        DataType type = DataType.fromPgName(written.toLowerCase());
        if (type == null) return written;
        return CatalogSystemFunctions.pgTypeDisplayName(type);
    }

    /**
     * A role named by SET ROLE or SET SESSION AUTHORIZATION has to be one that exists.
     *
     * <p>The complaint was that the value was invalid for the parameter, which is what PostgreSQL
     * says for a parameter whose value is out of range; for a role it names the role that is not
     * there. The check also waved through three names spelled into the code — test, postgres and
     * memgres — so those were accepted whether or not the server had them.
     */
    private void requireRole(String role, String parameter) {
        if (role == null) return;
        String lower = role.toLowerCase();
        if (executor.database.getRoles().containsKey(lower)) return;
        String connecting = executor.session != null ? executor.session.getConnectingUser() : null;
        if (connecting != null && role.equalsIgnoreCase(connecting)) return;
        throw new MemgresException("role \"" + role + "\" does not exist", "22023");
    }

    /**
     * Refuse a language that cannot carry a DO block. A language the catalogue does not have does
     * not exist at all; one that has no inline handler — sql, c, internal — has no inline form.
     */
    private void requireInlineLanguage(String language) {
        Table languages = executor.systemCatalog.resolve("pg_catalog", "pg_language", executor.session);
        int nameIdx = languages == null ? -1 : languages.getColumnIndex("lanname");
        int inlineIdx = languages == null ? -1 : languages.getColumnIndex("laninline");
        Object inline = null;
        boolean known = false;
        if (nameIdx >= 0) {
            for (Object[] row : languages.getRows()) {
                if (language.equals(row[nameIdx])) {
                    known = true;
                    inline = inlineIdx >= 0 ? row[inlineIdx] : null;
                    break;
                }
            }
        }
        if (!known) {
            throw new MemgresException("language \"" + language + "\" does not exist", "42704");
        }
        if (!(inline instanceof Number) || ((Number) inline).intValue() == 0) {
            throw new MemgresException(
                    "language \"" + language + "\" does not support inline code execution", "0A000");
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
                if (executor.session.isInRoutine()) {
                    throw new MemgresException(
                        "DISCARD ALL cannot be executed from a function", "25001");
                }
                // PG: DISCARD ALL cannot run inside a transaction block
                if (executor.session.isInTransaction()) {
                    throw new MemgresException(
                        "DISCARD ALL cannot run inside a transaction block", "25001");
                }
                executor.session.getGucSettings().resetAll();
                // RESET ROLE and RESET SESSION AUTHORIZATION are part of DISCARD ALL, so the
                // session speaks as the user it connected as again. Resetting the settings and
                // leaving the field beside them meant it kept answering as the role it gave up.
                executor.session.setConnectingUser(
                        executor.session.getGucSettings().get("session_authorization"));
                // L7: PG resets application_name to '' on DISCARD ALL
                executor.session.getGucSettings().setBootDefault("application_name", "");
                executor.session.removeAllPreparedStatements();
                executor.session.removeAllCursors();
                // Drop all temp tables for this session
                executor.session.dropTempObjects();
                // UNLISTEN * — cancel all notification subscriptions (PG DISCARD ALL behavior)
                if (executor.database.getNotificationManager() != null) {
                    executor.database.getNotificationManager().unlistenAll(executor.session);
                }
                // pg_advisory_unlock_all() — release all session-level advisory locks
                executor.database.advisoryUnlockAll(executor.session);
            } else if (target.equals("PLANS")) {
                // PG DISCARD PLANS invalidates cached query plans, forcing re-planning on next use.
                // It does NOT deallocate prepared statements. Since Memgres has no plan cache, this is a no-op.
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
        // PG requires LOCK to be inside an explicit transaction. A routine body is one: a
        // procedure or DO block runs inside a transaction whether or not the caller opened it,
        // so a LOCK written in one is inside a transaction block and PostgreSQL takes it.
        if (executor.session == null
                || (!executor.session.isInTransaction() && !executor.session.isInRoutine())) {
            throw new MemgresException("LOCK TABLE can only be used in transaction blocks", "25P01");
        }
        // Convert PG mode name to pg_locks mode column format (e.g. "ACCESS EXCLUSIVE" -> "AccessExclusiveLock")
        String mode = stmt.lockMode() != null ? stmt.lockMode().toUpperCase() : "ACCESS EXCLUSIVE";
        StringBuilder modeName = new StringBuilder();
        for (String word : mode.split("\\s+")) {
            modeName.append(Character.toUpperCase(word.charAt(0))).append(word.substring(1).toLowerCase());
        }
        modeName.append("Lock");
        String modeStr = modeName.toString();

        // Lock each table in the comma-separated list
        for (String tblName : stmt.tableNames()) {
            String schema = executor.defaultSchema();
            String table = tblName;
            if (table.contains(".")) {
                int dot = table.indexOf('.');
                schema = table.substring(0, dot);
                table = table.substring(dot + 1);
            }
            // A catalogue is a relation and can be locked like one; only user tables were looked
            // for, so LOCK TABLE pg_class said the relation was not there.
            try {
                executor.resolveTable(schema, table);
            } catch (MemgresException e) {
                if (executor.systemCatalog.resolve(
                        tblName.contains(".") ? schema : null, table, executor.session) == null) {
                    throw e;
                }
            }
            String tableKey = schema + "." + table;
            executor.session.addTableLock(tableKey, modeStr);
            executor.database.acquireTableLock(tableKey, modeStr, executor.session, stmt.nowait());
        }
        return QueryResult.message(QueryResult.Type.SET, "LOCK TABLE");
    }

    // ---- GRANT / REVOKE ----

    private static final Set<String> TABLE_PRIVILEGES = Cols.setOf(
            "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN", "ALL");

    /** The privileges a column can carry; the rest are table-wide only. */
    private static final Set<String> COLUMN_PRIVILEGES = Cols.setOf(
            "SELECT", "INSERT", "UPDATE", "REFERENCES", "ALL");

    /**
     * Every privilege keyword the grammar knows. A name outside this set is not "you may not
     * grant that here" but a syntax error, and PostgreSQL reports it as one — lower-cased,
     * because the parser has already folded it.
     */
    private static final Set<String> KNOWN_PRIVILEGES = Cols.setOf(
            "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN",
            "EXECUTE", "USAGE", "CREATE", "TEMPORARY", "TEMP", "CONNECT", "SET", "ALTER SYSTEM",
            "RULE", "ALL");

    /**
     * The sessions still on a database once those that are leaving have left.
     *
     * <p>A client that has just disconnected is not gone the moment its own {@code close()}
     * returns: the bytes are on their way, and the server takes the connection down when it reads
     * them. So a script that closes its connection and immediately drops the database it was using
     * is racing its own disconnection, and it is a script PostgreSQL runs -- because DROP DATABASE
     * there does not read the count once and give up. It waits for the other backends to exit,
     * polling for five seconds before it reports the database as in use, and only a connection
     * that is really still there outlasts that.
     *
     * <p>The wait costs nothing where there is nothing to wait for, and it ends the moment the
     * last one goes.
     */
    private java.util.Set<Session> waitForSessionsToEnd(Database targetDb) {
        java.util.Set<Session> others = targetDb.getActiveSessions();
        if (others.isEmpty()) return others;
        long deadline = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            others = targetDb.getActiveSessions();
            if (others.isEmpty()) break;
        }
        return others;
    }

    QueryResult executeGrant(GrantStmt s) {
        // Validate GRANTED BY grantor matches current user
        if (s.grantor() != null) {
            String currentUser = executor.sessionUser();
            String grantorName = s.grantor();
            if (!grantorName.equalsIgnoreCase("current_user") && !grantorName.equalsIgnoreCase("session_user")
                    && !grantorName.equalsIgnoreCase("current_role")
                    && !grantorName.equalsIgnoreCase(currentUser)) {
                // A grantor is a role first: naming one that is not there is a missing role, and
                // only a role that exists can then be one the session is not allowed to act as.
                requireGrantRole(grantorName);
                if (s.isRoleGrant()) {
                    throw new MemgresException(
                            "permission denied to grant privileges as role \"" + grantorName + "\"", "42501");
                }
                throw new MemgresException("grantor must be current user", "0A000");
            }
        }

        if (s.isRoleGrant()) {
            // Membership is between two roles, and both have to be roles. Recording a name that
            // is not one built a membership nothing could ever be a member of, and pg_has_role
            // then answered about roles the server does not have.
            if (s.privileges() != null && s.grantees() != null) {
                // The grantor is resolved before the roles the grant is between.
                if (s.grantor() != null) requireGrantRole(s.grantor());
                // PostgreSQL reads the grantees next, so a missing member is what it names.
                for (String member : s.grantees()) requireGrantRole(member);
                for (String grantedRole : s.privileges()) requireGrantRole(grantedRole);
                for (String grantedRole : s.privileges()) {
                    for (String member : s.grantees()) {
                        // CURRENT_USER names the session's own role; recording it by that
                        // spelling made a membership of a role nobody is.
                        executor.database.addRoleMembership(resolveRoleSpec(grantedRole),
                                resolveRoleSpec(member), s.withAdminOption());
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
        // M10: Support schema-qualified names (schema.table)
        if (s.objectName() != null && (s.objectType() == null || s.objectType().equals("TABLE"))) {
            String grantSchema = executor.defaultSchema();
            String grantTable = s.objectName();
            if (grantTable.contains(".")) {
                int dot = grantTable.indexOf('.');
                grantSchema = grantTable.substring(0, dot);
                grantTable = grantTable.substring(dot + 1);
                // As for every DDL statement that opens a relation by a written qualifier.
                SchemaQualifier.requireSchema(executor.database, executor.session, grantSchema);
            }
            try { executor.resolveTable(grantSchema, grantTable); }
            catch (MemgresException e) {
                // A view is a valid GRANT target even when it is not auto-updatable.
                if (executor.database.getView(grantTable) == null) {
                    throw new MemgresException("relation \"" + s.objectName() + "\" does not exist", "42P01");
                }
            }
        }
        // Validate object exists for FOREIGN SERVER grants
        if (s.objectType() != null && s.objectType().equals("FOREIGN SERVER") && s.objectName() != null) {
            // Check if the foreign server exists in pg_foreign_server catalog
            Table fsCatalog = executor.database.getSchema("pg_catalog") != null
                    ? executor.database.getSchema("pg_catalog").getTable("pg_foreign_server") : null;
            boolean found = false;
            if (fsCatalog != null) {
                int nameIdx = fsCatalog.getColumnIndex("srvname");
                if (nameIdx >= 0) {
                    for (Object[] row : fsCatalog.getRows()) {
                        if (s.objectName().equalsIgnoreCase(String.valueOf(row[nameIdx]))) {
                            found = true;
                            break;
                        }
                    }
                }
            }
            if (!found) {
                throw new MemgresException("server \"" + s.objectName() + "\" does not exist", "42704");
            }
        }
        // Validate grantee role exists
        if (s.grantees() != null) {
            for (String grantee : s.grantees()) {
                String g = grantee.toLowerCase();
                if (!g.equals("public") && !g.equals("test") && !g.equals("postgres")
                        && !g.equals("current_user") && !g.equals("session_user") && !g.equals("current_role")
                        && !executor.database.getRoles().containsKey(g)) {
                    throw new MemgresException("role \"" + grantee + "\" does not exist", "42704");
                }
            }
        }
        // Additional TABLE grant validations
        if (s.objectType() != null && s.objectType().equals("TABLE") && s.objectName() != null) {
            boolean columnLevel = s.columns() != null && !s.columns().isEmpty();
            for (String priv : s.privileges()) {
                if (!KNOWN_PRIVILEGES.contains(priv)) {
                    throw PgErrors.syntax("unrecognized privilege type \"" + priv.toLowerCase() + "\"");
                }
                if (columnLevel) {
                    if (!COLUMN_PRIVILEGES.contains(priv)) {
                        throw new MemgresException("invalid privilege type " + priv + " for column", "0LP01");
                    }
                } else if (!TABLE_PRIVILEGES.contains(priv)) {
                    // USAGE is a sequence right, so it survives the first check PG makes on a
                    // relation target and is rejected by the later, table-specific one.
                    String kind = priv.equals("USAGE") ? "table" : "relation";
                    throw new MemgresException("invalid privilege type " + priv + " for " + kind, "0LP01");
                }
            }
            // A grant option is a property of a role, and PUBLIC is not one.
            if (s.withGrantOption() && s.grantees() != null) {
                for (String grantee : s.grantees()) {
                    if (grantee.equalsIgnoreCase("public")) {
                        throw new MemgresException("grant options can only be granted to roles", "0LP01");
                    }
                }
            }
            // Validate column-level privileges: check column exists
            if (s.columns() != null) {
                String colSchema = executor.defaultSchema();
                String colTable = s.objectName();
                if (colTable.contains(".")) {
                    int dot = colTable.indexOf('.');
                    colSchema = colTable.substring(0, dot);
                    colTable = colTable.substring(dot + 1);
                }
                Table table = executor.resolveTable(colSchema, colTable);
                for (String col : s.columns()) {
                    if (table.getColumnIndex(col) < 0) {
                        throw new MemgresException("column \"" + col + "\" of relation \"" + s.objectName() + "\" does not exist", "42703");
                    }
                }
            }
        }
        // M9: Validate grantor holds privilege WITH GRANT OPTION (non-superuser, non-owner)
        // PG falls back to session_user when current_role can't grant (select_best_grantor)
        if (s.objectName() != null && s.objectType() != null && "TABLE".equalsIgnoreCase(s.objectType())) {
            String currentRole = executor.currentRole();
            String sessionUser = executor.sessionUser();
            // Check if current role or session user is superuser
            boolean isSuperuser = isRoleSuperuser(currentRole);
            if (!isSuperuser && !currentRole.equalsIgnoreCase(sessionUser)) {
                isSuperuser = isRoleSuperuser(sessionUser);
            }
            if (!isSuperuser) {
                // Check if current role or session user is the owner
                String bareObj = s.objectName().contains(".") ? s.objectName().substring(s.objectName().indexOf('.') + 1) : s.objectName();
                String schForOwner = s.objectName().contains(".") ? s.objectName().substring(0, s.objectName().indexOf('.')) : executor.defaultSchema();
                String ownerKey = "table:" + schForOwner.toLowerCase() + "." + bareObj.toLowerCase();
                String owner = executor.database.getObjectOwner(ownerKey);
                boolean isOwner = owner != null && (owner.equalsIgnoreCase(currentRole) || owner.equalsIgnoreCase(sessionUser));
                if (!isOwner) {
                    for (String priv : s.privileges()) {
                        // Must hold the privilege WITH GRANT OPTION
                        if (!executor.hasPrivilegeDirectOrInherited(currentRole, priv + "_GRANT_OPTION", "TABLE", AstExecutor.privilegeKey(schForOwner, bareObj))) {
                            throw new MemgresException(
                                "permission denied for table \"" + bareObj + "\"", "42501");
                        }
                    }
                }
            }
        }

        // Track granted privileges for role dependency checks (DROP ROLE)
        if (s.objectName() != null && s.grantees() != null && s.objectType() != null) {
            // Privileges on tables are keyed by schema-qualified name, so a grant on
            // s1.t cannot be matched by a lookup for s2.t.
            String bareObjectName = s.objectName();
            if ("TABLE".equalsIgnoreCase(s.objectType())) {
                bareObjectName = AstExecutor.privilegeKey(executor.defaultSchema(), bareObjectName);
            }
            // Expand "ALL TABLES IN SCHEMA" to individual table grants
            if (s.objectType().startsWith("ALL TABLES IN SCHEMA")) {
                Schema schema = executor.database.getSchema(s.objectName());
                if (schema != null) {
                    for (String grantee : s.grantees()) {
                        for (String priv : s.privileges()) {
                            for (String tableName : schema.getTables().keySet()) {
                                executor.database.addRolePrivilege(grantee, priv, "TABLE",
                                        AstExecutor.privilegeKey(s.objectName(), tableName));
                            }
                        }
                    }
                }
            } else {
                for (String grantee : s.grantees()) {
                    for (String priv : s.privileges()) {
                        if (s.columns() != null && !s.columns().isEmpty()) {
                            // Column-level grant: store as COLUMN objectType with "tableName.colName"
                            for (String col : s.columns()) {
                                executor.database.addRolePrivilege(grantee, priv, "COLUMN", bareObjectName + "." + col);
                            }
                        } else {
                            executor.database.addRolePrivilege(grantee, priv, s.objectType(), bareObjectName);
                            // M9: Track grant option separately
                            if (s.withGrantOption()) {
                                executor.database.addRolePrivilege(grantee, priv + "_GRANT_OPTION", s.objectType(), bareObjectName);
                            }
                        }
                    }
                }
            }
        }
        // Record schema-level ACL for pg_namespace.nspacl
        if ("SCHEMA".equals(s.objectType()) && s.objectName() != null && s.grantees() != null) {
            String grantor = executor.currentRole();
            for (String grantee : s.grantees()) {
                StringBuilder aclItem = new StringBuilder();
                aclItem.append(grantee).append("=");
                for (String priv : s.privileges()) {
                    switch (priv.toUpperCase()) {
                        case "USAGE": aclItem.append("U"); break;
                        case "CREATE": aclItem.append("C"); break;
                        case "ALL": aclItem.append("UC"); break;
                    }
                }
                aclItem.append("/").append(grantor);
                executor.database.addSchemaAcl(s.objectName(), aclItem.toString());
            }
        }
        return QueryResult.message(QueryResult.Type.SET, "GRANT");
    }

    QueryResult executeRevoke(RevokeStmt s) {
        if (s.isRoleGrant()) {
            // Taking a membership away names the same two roles granting one does, and both
            // have to be roles for there to be a membership between them.
            if (s.privileges() != null && s.grantees() != null) {
                for (String member : s.grantees()) requireGrantRole(member);
                for (String grantedRole : s.privileges()) requireGrantRole(grantedRole);
                for (String grantedRole : s.privileges()) {
                    for (String member : s.grantees()) {
                        executor.database.removeRoleMembership(resolveRoleSpec(grantedRole),
                                resolveRoleSpec(member));
                    }
                }
            }
            return QueryResult.message(QueryResult.Type.SET, "REVOKE");
        }
        // Validate object exists for TABLE grants
        if (s.objectType() != null && s.objectType().equals("TABLE") && s.objectName() != null) {
            // M10: Support schema-qualified names
            String rSchema = executor.defaultSchema();
            String rTable = s.objectName();
            if (rTable.contains(".")) {
                int dot = rTable.indexOf('.');
                rSchema = rTable.substring(0, dot);
                rTable = rTable.substring(dot + 1);
            }
            try { executor.resolveTable(rSchema, rTable); }
            catch (MemgresException e) {
                throw new MemgresException("relation \"" + s.objectName() + "\" does not exist", "42P01");
            }
        }
        // Track privilege removal
        if (s.objectName() != null && s.grantees() != null && s.objectType() != null) {
            // Match the schema-qualified key GRANT stored
            String bareObjName = s.objectName();
            if ("TABLE".equalsIgnoreCase(s.objectType())) {
                bareObjName = AstExecutor.privilegeKey(executor.defaultSchema(), bareObjName);
            }
            for (String grantee : s.grantees()) {
                for (String priv : s.privileges()) {
                    if (s.columns() != null && !s.columns().isEmpty()) {
                        for (String col : s.columns()) {
                            executor.database.removeRolePrivilege(grantee, priv, "COLUMN", bareObjName + "." + col);
                        }
                    } else if (s.grantOptionFor()) {
                        // M9: REVOKE GRANT OPTION FOR — only remove the grant option flag, keep the privilege
                        executor.database.removeRolePrivilege(grantee, priv + "_GRANT_OPTION", s.objectType(), bareObjName);
                    } else {
                        executor.database.removeRolePrivilege(grantee, priv, s.objectType(), bareObjName);
                        // Also remove grant option when revoking the privilege itself
                        executor.database.removeRolePrivilege(grantee, priv + "_GRANT_OPTION", s.objectType(), bareObjName);
                    }
                }
            }
            // CASCADE: also revoke matching privileges from all other roles on same object
            if (s.cascade()) {
                for (String priv : s.privileges()) {
                    String suffixLower = ":" + s.objectType().toLowerCase() + ":" + bareObjName.toLowerCase();
                    String prefixLower = priv.toLowerCase() + suffixLower;
                    for (java.util.Map.Entry<String, java.util.Set<String>> entry
                            : executor.database.getAllRolePrivileges().entrySet()) {
                        entry.getValue().removeIf(p -> p.toLowerCase().equals(prefixLower));
                    }
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
        // PG only allows optimizable statements (SELECT, INSERT, UPDATE, DELETE, MERGE, VALUES,
        // and set operations like UNION/INTERSECT/EXCEPT) in PREPARE.
        // DDL / utility statements are rejected with 42601.
        if (!(stmt.body() instanceof com.memgres.engine.parser.ast.SelectStmt)
                && !(stmt.body() instanceof com.memgres.engine.parser.ast.InsertStmt)
                && !(stmt.body() instanceof com.memgres.engine.parser.ast.UpdateStmt)
                && !(stmt.body() instanceof com.memgres.engine.parser.ast.DeleteStmt)
                && !(stmt.body() instanceof com.memgres.engine.parser.ast.SetOpStmt)
                && !(stmt.body() instanceof com.memgres.engine.parser.ast.MergeStmt)
                && !(stmt.body() instanceof com.memgres.engine.parser.ast.CreateTableAsStmt)) {
            throw new MemgresException("utility statements cannot be prepared", "42601");
        }
        // Always infer param count from $N references in body
        List<String> paramTypes = declaredParameterTypes(stmt.paramTypes());
        int inferredCount = maxParamIndex(stmt.body());
        // The relations and columns the query names have to be there now. PREPARE is where
        // PostgreSQL analyses the statement, so a table that is missing is reported here and the
        // statement is not remembered — EXECUTE then says there is no such prepared statement,
        // rather than repeating the analysis failure at every execution.
        new StatementAnalyzer(executor).analyze(stmt.body());
        // Every parameter the query uses has to have a type. One that was never written about —
        // $1 where only $2 appears — has nothing to infer from, and PostgreSQL names it.
        requireEveryParameterTyped(inferredCount, paramTypes, stmt.body());
        // Validate the body at PREPARE time (PG does full analysis/type-checking here)
        validatePreparedBody(stmt.body(), paramTypes);
        // Extract original body SQL from the raw PREPARE statement for verbatim pg_prepared_statements display.
        // Falls back to AST reconstruction if original SQL is unavailable.
        String sqlText = extractPrepareBodySql(executor.currentRawSql, stmt.body());
        // Infer result types for pg_prepared_statements.result_types (PG 16+)
        List<String> resultTypes = inferResultTypes(stmt.body());
        executor.session.addPreparedStatement(stmt.name(),
                new Session.PreparedStmt(stmt.name(), paramTypes, stmt.body(), inferredCount,
                        sqlText, java.time.OffsetDateTime.now(), true, resultTypes));
        return QueryResult.message(QueryResult.Type.SET, "PREPARE");
    }

    /**
     * Extract the body portion of a PREPARE statement from the original SQL text.
     * Real PG stores the verbatim body text, not a reconstructed form.
     * Finds " AS " outside of string literals and quoted identifiers.
     */
    private String extractPrepareBodySql(String rawSql, Statement body) {
        if (rawSql != null) {
            int idx = findKeywordOutsideQuotes(rawSql, " AS ", 0);
            if (idx >= 0) {
                return rawSql.substring(idx + 4).trim();
            }
        }
        return SqlUnparser.toSql(body);
    }

    /**
     * Extract the query portion of a DECLARE CURSOR statement from the original SQL text.
     * Real PG stores the verbatim query text (after FOR), not a reconstructed form.
     * Finds " FOR " after the CURSOR keyword, outside of string literals and quoted identifiers.
     */
    private String extractDeclareQuerySql(String rawSql, Statement query) {
        if (rawSql != null) {
            // First find CURSOR keyword to anchor the search, then find FOR after it
            int cursorIdx = findKeywordOutsideQuotes(rawSql, "CURSOR", 0);
            if (cursorIdx >= 0) {
                int forIdx = findKeywordOutsideQuotes(rawSql, " FOR ", cursorIdx + 6);
                if (forIdx >= 0) {
                    return rawSql.substring(forIdx + 5).trim();
                }
            }
        }
        return SqlUnparser.toSql(query);
    }

    /**
     * Find a keyword in SQL text, skipping single-quoted strings and double-quoted identifiers.
     * Returns the index of the match, or -1 if not found.
     */
    /**
     * Find a keyword in SQL text, skipping:
     * - Single-quoted strings ('...' with '' escaping)
     * - Double-quoted identifiers ("..." with "" escaping)
     * - Dollar-quoted strings ($$...$$ and $tag$...$tag$)
     * - Block comments (/&#42; ... &#42;/)
     * - Line comments (-- ... \n)
     * Returns the index of the match, or -1 if not found.
     */
    private static int findKeywordOutsideQuotes(String sql, String keyword, int startPos) {
        String upper = sql.toUpperCase();
        String keyUpper = keyword.toUpperCase();
        int len = sql.length();
        int i = startPos;
        while (i < len) {
            char c = sql.charAt(i);
            if (c == '\'') {
                // Skip single-quoted string (with '' escaping)
                i++;
                while (i < len) {
                    if (sql.charAt(i) == '\'') {
                        if (i + 1 < len && sql.charAt(i + 1) == '\'') {
                            i += 2;
                        } else {
                            i++;
                            break;
                        }
                    } else {
                        i++;
                    }
                }
            } else if (c == '"') {
                // Skip double-quoted identifier (with "" escaping)
                i++;
                while (i < len) {
                    if (sql.charAt(i) == '"') {
                        if (i + 1 < len && sql.charAt(i + 1) == '"') {
                            i += 2;
                        } else {
                            i++;
                            break;
                        }
                    } else {
                        i++;
                    }
                }
            } else if (c == '$') {
                // Possible dollar-quoted string: $$ or $tag$
                int tagEnd = findDollarTag(sql, i);
                if (tagEnd > i) {
                    String tag = sql.substring(i, tagEnd); // e.g., "$$" or "$tag$"
                    int closeIdx = sql.indexOf(tag, tagEnd);
                    if (closeIdx >= 0) {
                        i = closeIdx + tag.length();
                    } else {
                        i = len; // unclosed dollar quote — skip to end
                    }
                } else {
                    i++;
                }
            } else if (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-') {
                // Skip line comment (-- ... \n)
                i += 2;
                while (i < len && sql.charAt(i) != '\n') {
                    i++;
                }
                if (i < len) i++; // skip the \n
            } else if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
                // Skip block comment (/* ... */), supports nesting
                i += 2;
                int depth = 1;
                while (i < len && depth > 0) {
                    if (sql.charAt(i) == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
                        depth++;
                        i += 2;
                    } else if (sql.charAt(i) == '*' && i + 1 < len && sql.charAt(i + 1) == '/') {
                        depth--;
                        i += 2;
                    } else {
                        i++;
                    }
                }
            } else if (upper.startsWith(keyUpper, i)) {
                return i;
            } else {
                i++;
            }
        }
        return -1;
    }

    /** Find the end of a dollar-quote tag starting at pos. Returns pos if not a valid tag. */
    private static int findDollarTag(String sql, int pos) {
        if (pos >= sql.length() || sql.charAt(pos) != '$') return pos;
        int j = pos + 1;
        if (j >= sql.length()) return pos;
        // $$ (empty tag)
        if (sql.charAt(j) == '$') return j + 1;
        // $identifier$ tag — tag must start with letter or underscore (PG identifier rules),
        // NOT a digit (which would be $1 parameter reference)
        if (!Character.isLetter(sql.charAt(j)) && sql.charAt(j) != '_') return pos;
        j++;
        while (j < sql.length()) {
            char ch = sql.charAt(j);
            if (ch == '$') return j + 1;
            if (!Character.isLetterOrDigit(ch) && ch != '_') return pos; // not a valid tag
            j++;
        }
        return pos; // unclosed tag start
    }

    /**
     * Infer result column types from a prepared statement body.
     * Returns null for DML without RETURNING (PG behavior).
     */
    private List<String> inferResultTypes(Statement body) {
        try {
            // For SELECT: safe dry-run with LIMIT 0
            if (body instanceof SelectStmt || body instanceof SetOpStmt) {
                return inferResultTypesViaDryRun(body);
            }
            // For DML with RETURNING: infer from table schema (no execution — avoids side effects)
            if (body instanceof InsertStmt && ((InsertStmt) body).returning != null && !((InsertStmt) body).returning.isEmpty()) {
                return inferResultTypesFromTable((InsertStmt) body);
            }
            if (body instanceof UpdateStmt && ((UpdateStmt) body).returning != null && !((UpdateStmt) body).returning.isEmpty()) {
                return inferResultTypesFromTable((UpdateStmt) body);
            }
            if (body instanceof DeleteStmt && ((DeleteStmt) body).returning != null && !((DeleteStmt) body).returning.isEmpty()) {
                return inferResultTypesFromTable((DeleteStmt) body);
            }
            // DML without RETURNING: null (PG behavior)
            return null;
        } catch (Exception e) {
            // Type inference is best-effort; don't fail PREPARE
            return null;
        }
    }

    /**
     * Execute a SELECT statement as a dry run (LIMIT 0) to infer result column types.
     */
    private List<String> inferResultTypesViaDryRun(Statement body) {
        try {
            SelectStmt sel = body instanceof SelectStmt ? (SelectStmt) body
                    : body instanceof SetOpStmt ? firstSelectArm((SetOpStmt) body) : null;
            if (sel == null || sel.targets() == null || sel.targets().isEmpty()) return null;
            // The relations are opened for their columns, not for their rows: what a query answers
            // is settled by what it is written as. Running it to read the shape back ran what the
            // target list called, so preparing a query over nextval() moved the sequence and
            // preparing one over a function wrote whatever that function writes.
            List<RowContext.TableBinding> bindings = sel.from() == null || sel.from().isEmpty()
                    ? null : executor.fromResolver.resolveTableBindings(sel.from());
            RowContext shape = bindings == null || bindings.isEmpty() ? null : new RowContext(bindings);
            List<String> types = new ArrayList<>();
            for (SelectStmt.SelectTarget target : sel.targets()) {
                if (target.expr() instanceof WildcardExpr) {
                    if (bindings == null) return null;
                    for (RowContext.TableBinding b : bindings) {
                        Table t = b.sourceTable != null ? b.sourceTable : b.table();
                        if (t == null) return null;
                        for (Column col : t.getColumns()) types.add(col.getType().toRegtypeDisplay());
                    }
                    continue;
                }
                String declared = executor.binaryOpEvaluator.declaredTypeForResolution(
                        target.expr(), shape);
                if (declared == null) declared = shapeOnlyType(target.expr(), shape);
                if (declared == null) return null;
                types.add(declared);
            }
            return types.isEmpty() ? null : types;
        } catch (Exception e) {
            // Reading the shape is best-effort: a query whose columns cannot be named without
            // running it simply has none recorded.
        }
        return null;
    }

    /**
     * The type of an expression that can be worked out by evaluating it, because evaluating it
     * changes nothing: a literal, and the casts and operators built over literals. A call is
     * excluded whatever it is called — a function is free to write, and reading a prepared
     * statement's shape is not permission to let it.
     */
    private String shapeOnlyType(Expression expr, RowContext shape) {
        if (!isValueOnly(expr)) return null;
        try {
            Object value = executor.evalExpr(expr, shape);
            return value == null ? null : AstExecutor.pgTypeNameOf(value);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** Whether an expression is built only from literals, casts and operators over them. */
    private static boolean isValueOnly(Expression expr) {
        if (expr instanceof Literal) return true;
        if (expr instanceof CastExpr) return isValueOnly(((CastExpr) expr).expr());
        if (expr instanceof UnaryExpr) return isValueOnly(((UnaryExpr) expr).operand());
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            return isValueOnly(bin.left()) && isValueOnly(bin.right());
        }
        return false;
    }

    /** The arm of a set operation whose columns the whole of it answers with. */
    private static SelectStmt firstSelectArm(SetOpStmt setOp) {
        Statement left = setOp.left();
        if (left instanceof SelectStmt) return (SelectStmt) left;
        if (left instanceof SetOpStmt) return firstSelectArm((SetOpStmt) left);
        return null;
    }

    /**
     * Infer result types for DML with RETURNING by resolving table schema.
     * Avoids executing the DML (which could cause side effects or constraint violations).
     */
    private List<String> inferResultTypesFromTable(Statement body) {
        try {
            String tableName = null;
            String schemaName = null;
            List<SelectStmt.SelectTarget> returning = null;
            if (body instanceof InsertStmt) {
                InsertStmt ins = (InsertStmt) body;
                tableName = ins.table; schemaName = ins.schema; returning = ins.returning;
            } else if (body instanceof UpdateStmt) {
                UpdateStmt upd = (UpdateStmt) body;
                tableName = upd.table; schemaName = upd.schema; returning = upd.returning;
            } else if (body instanceof DeleteStmt) {
                DeleteStmt del = (DeleteStmt) body;
                tableName = del.table; schemaName = del.schema; returning = del.returning;
            }
            if (returning == null || returning.isEmpty() || tableName == null) return null;
            // Resolve table
            if (schemaName == null) schemaName = executor.defaultSchema();
            Table table = executor.resolveTable(schemaName, tableName);
            // Map RETURNING targets to column types
            List<String> types = new ArrayList<>();
            for (SelectStmt.SelectTarget target : returning) {
                Expression expr = target.expr();
                if (expr instanceof WildcardExpr) {
                    for (Column col : table.getColumns()) {
                        types.add(col.getType().toRegtypeDisplay());
                    }
                } else if (expr instanceof ColumnRef) {
                    String colName = ((ColumnRef) expr).column();
                    int colIdx = table.getColumnIndex(colName);
                    types.add(colIdx >= 0 ? table.getColumns().get(colIdx).getType().toRegtypeDisplay() : "text");
                } else {
                    types.add("text"); // expression default
                }
            }
            return types;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Validate a prepared statement body at PREPARE time, matching PG behavior
     * which performs type analysis before storing the prepared statement.
     */
    /**
     * The parameter types as PREPARE records them.
     *
     * <p>A declared parameter type is a type, not a type with a size: PostgreSQL drops the
     * modifier, so {@code PREPARE p (varchar(3))} takes a varchar of any length and
     * {@code PREPARE p (numeric(2,1))} does not round what it is given. Keeping the modifier
     * turned the declaration into a cast, which truncated and rounded the caller's values.
     */
    private List<String> declaredParameterTypes(List<String> declared) {
        if (declared == null) return null;
        List<String> plain = new ArrayList<>();
        for (String type : declared) {
            if (type == null) { plain.add(null); continue; }
            String name = type.trim();
            int paren = name.indexOf('(');
            if (paren > 0) {
                String tail = name.substring(name.indexOf(')') + 1);
                name = name.substring(0, paren).trim() + tail;
            }
            requireTypeExists(name);
            plain.add(name);
        }
        return plain;
    }

    /** A declared parameter type that names nothing is reported the way any missing type is. */
    private void requireTypeExists(String name) {
        String bare = name.toLowerCase().trim();
        while (bare.endsWith("[]")) bare = bare.substring(0, bare.length() - 2).trim();
        if (bare.isEmpty()) return;
        if (DataType.fromPgName(bare) != null) return;
        if (executor.database.isCompositeType(bare) || executor.database.getDomain(bare) != null
                || executor.database.getCustomEnum(bare) != null
                || executor.database.isRangeType(bare)) {
            return;
        }
        throw new MemgresException("type \"" + name + "\" does not exist", "42704");
    }

    /**
     * Every parameter up to the highest one written has to be one the query says something about.
     * {@code SELECT $2} uses $2 and never mentions $1, so $1 has no type to be given.
     */
    private void requireEveryParameterTyped(int highest, List<String> declared, Statement body) {
        int declaredCount = declared == null ? 0 : declared.size();
        if (highest <= declaredCount) return;
        Set<Integer> used = new HashSet<>();
        collectParamIndexes(body, used);
        for (int i = 1; i <= highest; i++) {
            if (i <= declaredCount) continue;
            if (!used.contains(Integer.valueOf(i))) {
                throw new MemgresException(
                        "could not determine data type of parameter $" + i, "42P18");
            }
        }
    }

    /** The parameter numbers a statement actually writes. */
    private void collectParamIndexes(Object node, Set<Integer> found) {
        if (node == null) return;
        if (node instanceof ParamRef) {
            found.add(Integer.valueOf(((ParamRef) node).index()));
            return;
        }
        if (node instanceof Iterable) {
            for (Object o : (Iterable<?>) node) collectParamIndexes(o, found);
            return;
        }
        if (!isAstNodeClass(node.getClass())) return;
        for (java.lang.reflect.Field field : node.getClass().getDeclaredFields()) {
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) continue;
            try {
                field.setAccessible(true);
                collectParamIndexes(field.get(node), found);
            } catch (Exception e) { /* inaccessible: treat as leaf */ }
        }
    }

    private void validatePreparedBody(Statement body, List<String> paramTypes) {
        validatePreparedQueryShape(body);
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
     * The analysis a prepared body gets for its shape rather than its types: PostgreSQL analyses a
     * PREPARE body in full, so a FROM clause that names one relation twice and an ORDER BY over a
     * set operation that names no output column are both refused when the statement is prepared
     * and not when it is executed. Neither check reads a row, so both can be made here.
     *
     * <p>Only a parameter is judged in the ORDER BY. Which expressions are output columns is a
     * question about the arms' select lists, which the set-operation executor answers with the
     * columns in hand; a parameter is the one item that can never be an output column name however
     * the arms are written.
     */
    private void validatePreparedQueryShape(Statement body) {
        if (body instanceof SelectStmt) {
            executor.selectExecutor.validateFromClause(((SelectStmt) body).from());
            return;
        }
        if (!(body instanceof SetOpStmt)) return;
        SetOpStmt setOp = (SetOpStmt) body;
        validatePreparedQueryShape(setOp.left());
        validatePreparedQueryShape(setOp.right());
        if (setOp.orderBy() == null) return;
        for (SelectStmt.OrderByItem item : setOp.orderBy()) {
            Expression expr = item.expr();
            if (expr instanceof CollateExpr) expr = ((CollateExpr) expr).expr();
            if (expr instanceof ParamRef) throw SelectSetOpExecutor.notAnOutputColumn();
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
        if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr c = (CustomOperatorExpr) expr;
            if (c.left() != null) validateAnonymousRowAccess(c.left());
            validateAnonymousRowAccess(c.right());
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
    /** Whether this operand offers a type to the other side of an operator. */
    private static boolean isTypedOperand(Expression expr, List<String> paramTypes) {
        if (!(expr instanceof ParamRef)) return true;
        int idx = ((ParamRef) expr).index() - 1;
        return paramTypes != null && idx >= 0 && idx < paramTypes.size();
    }

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
            // If one side is typed (literal, cast, etc.), the other gets context. A parameter
            // the statement declared a type for is typed too: PREPARE p (int) AS SELECT $1 + $2
            // gives $2 the integer $1 already is, which is how PostgreSQL types it.
            boolean leftTyped = isTypedOperand(b.left(), paramTypes);
            boolean rightTyped = isTypedOperand(b.right(), paramTypes);
            checkForUntypedParams(b.left(), paramTypes, hasTypeContext || rightTyped);
            checkForUntypedParams(b.right(), paramTypes, hasTypeContext || leftTyped);
            return;
        }
        // CustomOperatorExpr with typed operand provides context
        if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr c = (CustomOperatorExpr) expr;
            boolean leftTyped = c.left() != null && !(c.left() instanceof ParamRef);
            boolean rightTyped = !(c.right() instanceof ParamRef);
            if (c.left() != null) checkForUntypedParams(c.left(), paramTypes, hasTypeContext || rightTyped);
            checkForUntypedParams(c.right(), paramTypes, hasTypeContext || leftTyped);
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
        if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr c = (CustomOperatorExpr) expr;
            if (c.left() != null) validateExpressionTree(c.left(), paramTypes);
            validateExpressionTree(c.right(), paramTypes);
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
            // How many were expected and how many arrived is the detail behind the complaint, not
            // the complaint itself, and a client matching on the message read a different one.
            throw new MemgresException(
                    "wrong number of parameters for prepared statement \"" + stmt.name() + "\"", "42601");
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
            // Validate parameter types at binding time (PG checks this eagerly)
            if (declaredCount > 0 && prepared.paramTypes() != null) {
                for (int pi = 0; pi < Math.min(executor.boundParameters.size(), prepared.paramTypes().size()); pi++) {
                    Object val = executor.boundParameters.get(pi);
                    String declaredType = prepared.paramTypes().get(pi);
                    if (val != null && declaredType != null) {
                        try {
                            Object coerced = executor.castEvaluator.applyCast(val, declaredType);
                            executor.boundParameters.set(pi, coerced);
                        } catch (Exception e) {
                            throw new MemgresException(
                                    "invalid input syntax for type " + declaredType + ": \"" + val + "\"",
                                    "22P02");
                        }
                    }
                }
            }
            try {
                prepared.recordExecution();
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

    /**
     * Generic AST walk used for $N parameter inference. Instead of hand-enumerating every
     * expression slot of every statement type (which historically missed JOIN ON conditions,
     * CTE bodies, GROUP BY/HAVING, VALUES-in-FROM, set-operation branches, window definitions,
     * DISTINCT ON, ON CONFLICT clauses, ...), this reflectively descends into every field of
     * every AST node. All AST node classes (and their nested helper classes such as
     * {@code SelectStmt.JoinFrom} or {@code SelectStmt.CommonTableExpr}) live in the
     * {@code com.memgres.engine.parser.ast} package, so any object from that package is walked;
     * collections and maps are traversed element-wise; everything else is a leaf.
     */
    private void walkExpressions(Object node, int[] max) {
        walkForParamRefs(node, max,
                java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Object, Boolean>()));
    }

    private void walkForParamRefs(Object node, int[] max, java.util.Set<Object> visited) {
        if (node == null) return;
        if (node instanceof ParamRef) {
            ParamRef p = (ParamRef) node;
            if (p.index() > max[0]) max[0] = p.index();
            return;
        }
        if (node instanceof Iterable) {
            for (Object item : (Iterable<?>) node) {
                walkForParamRefs(item, max, visited);
            }
            return;
        }
        if (node instanceof java.util.Map) {
            for (Object value : ((java.util.Map<?, ?>) node).values()) {
                walkForParamRefs(value, max, visited);
            }
            return;
        }
        Class<?> cls = node.getClass();
        if (node instanceof Enum || !isAstNodeClass(cls)) return;
        if (!visited.add(node)) return; // identity-based guard against shared/cyclic subtrees
        for (Class<?> c = cls; c != null && c != Object.class; c = c.getSuperclass()) {
            for (java.lang.reflect.Field field : c.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) continue;
                Class<?> fieldType = field.getType();
                // Skip leaf-typed fields that can never contain an AST node
                if (fieldType.isPrimitive() || fieldType == String.class || fieldType.isEnum()
                        || Number.class.isAssignableFrom(fieldType)
                        || fieldType == Boolean.class || fieldType == Character.class) {
                    continue;
                }
                Object value;
                try {
                    field.setAccessible(true);
                    value = field.get(node);
                } catch (Exception e) {
                    continue; // inaccessible field: treat as leaf
                }
                walkForParamRefs(value, max, visited);
            }
        }
    }

    /** True if the class is an AST node (or a nested helper class of one). */
    private static boolean isAstNodeClass(Class<?> cls) {
        return cls.getName().startsWith("com.memgres.engine.parser.ast.");
    }

    // ---- Cursors ----

    QueryResult executeDeclareCursor(DeclareCursorStmt stmt) {
        // PG: non-holdable cursors require an explicit transaction block
        if (!stmt.withHold() && !executor.session.isExplicitTransactionBlock()) {
            throw new MemgresException("DECLARE CURSOR can only be used in transaction blocks", "25P01");
        }
        if (executor.session.getCursor(stmt.name()) != null) {
            throw new MemgresException("cursor \"" + stmt.name() + "\" already exists", "42P03");
        }
        // Execute the query to get all results (may be SELECT or UNION/INTERSECT/EXCEPT), keeping
        // the stored rows behind each answer so WHERE CURRENT OF can name a row rather than
        // hunting for one whose columns happen to match.
        List<List<RowContext.TableBinding>> provenance = new ArrayList<>();
        QueryResult result;
        List<List<RowContext.TableBinding>> outerProvenance = executor.cursorRowProvenance;
        executor.cursorRowProvenance = provenance;
        try {
            result = executor.executeStatement(stmt.query());
        } finally {
            executor.cursorRowProvenance = outerProvenance;
        }
        List<Object[]> rows = result.getRows() != null ? new ArrayList<>(result.getRows()) : new ArrayList<>();
        List<Column> columns = result.getColumns() != null ? result.getColumns() : Cols.listOf();
        // PG stores the full DECLARE statement in pg_cursors.statement, not just the query.
        String queryText = executor.currentRawSql != null ? executor.currentRawSql.trim() :
                ("DECLARE " + stmt.name() + (stmt.scroll ? " SCROLL" : "")
                 + " CURSOR" + (stmt.withHold ? " WITH HOLD" : "")
                 + " FOR " + SqlUnparser.toSql(stmt.query()));
        Session.CursorState cursor = new Session.CursorState(stmt.name(), columns, rows,
                queryText, stmt.withHold, stmt.binary, stmt.scroll, stmt.explicitNoScroll);
        // Only a scan that answered one row per stored row can be positioned onto one of them.
        if (provenance.size() == rows.size()) cursor.setProvenance(provenance);
        cursor.setLocking(stmt.query() instanceof SelectStmt
                && ((SelectStmt) stmt.query()).lockClause() != null);
        executor.session.addCursor(stmt.name(), cursor);
        return QueryResult.message(QueryResult.Type.SET, "DECLARE CURSOR");
    }

    QueryResult executeFetch(FetchStmt stmt) {
        Session.CursorState cursor = executor.session.getCursor(stmt.cursorName());
        if (cursor == null) {
            throw new MemgresException("cursor \"" + stmt.cursorName() + "\" does not exist", "34000");
        }
        // PG: only explicitly declared NO SCROLL cursors reject backward movement.
        // Default cursors (no SCROLL/NO SCROLL keyword) are effectively scrollable in PG 18.
        if (cursor.isExplicitNoScroll()) {
            switch (stmt.direction()) {
                case PRIOR:
                case FIRST:
                case LAST:
                case ABSOLUTE:
                    throw new MemgresException("cursor can only scan forward", "55000");
                case RELATIVE:
                    if (stmt.count() < 0) {
                        throw new MemgresException("cursor can only scan forward", "55000");
                    }
                    break;
                case BACKWARD:
                case BACKWARD_ALL:
                    throw new MemgresException("cursor can only scan forward", "55000");
                default:
                    break;
            }
        }
        List<Object[]> fetched = cursorFetch(cursor, stmt.direction(), stmt.count());

        if (stmt.isMove()) {
            // MOVE's command tag carries the number of rows it passed over, and a client reads
            // that back as the statement's row count. Tagging it "SET" reports 0 every time.
            return QueryResult.message(QueryResult.Type.SET, "MOVE " + fetched.size());
        }
        return QueryResult.select(cursor.getColumns(), fetched);
    }

    private List<Object[]> cursorFetch(Session.CursorState cursor, FetchStmt.Direction dir, int count) {
        int pos = cursor.getPosition();
        int total = cursor.getRowCount();
        List<Object[]> result = new ArrayList<>();

        switch (dir) {
            case NEXT: {
                // PG: moves position forward by 1 regardless of whether row exists
                int target = pos + 1;
                addRow(result, cursor, target);
                if (result.isEmpty()) cursor.setPosition(Math.min(target, total));
                break;
            }
            case PRIOR: {
                // PG: moves position backward by 1 regardless of whether row exists
                int target = pos - 1;
                addRow(result, cursor, target);
                if (result.isEmpty()) cursor.setPosition(Math.max(target, -1));
                break;
            }
            case FIRST:
                addRow(result, cursor, 0);
                if (result.isEmpty()) cursor.setPosition(-1);
                break;
            case LAST:
                addRow(result, cursor, total - 1);
                // PG: FETCH LAST is ABSOLUTE -1; on an empty result it returns
                // nothing and positions before the first row
                if (result.isEmpty()) cursor.setPosition(-1);
                break;
            case ABSOLUTE: {
                // PG: ABSOLUTE 0 positions before first row (returns nothing)
                // ABSOLUTE N (N>0): position to Nth row (1-based)
                // ABSOLUTE -N: position from end
                if (count == 0) {
                    cursor.setPosition(-1); // before first
                } else {
                    int target = count > 0 ? count - 1 : total + count;
                    addRow(result, cursor, target);
                    // PG: an out-of-range target still repositions the cursor:
                    // beyond the end → after last row (PRIOR then returns the last row);
                    // beyond the start → before first row (NEXT then returns the first row)
                    if (result.isEmpty()) {
                        cursor.setPosition(target >= total ? total : -1);
                    }
                }
                break;
            }
            case RELATIVE: {
                int target = pos + count;
                addRow(result, cursor, target);
                if (result.isEmpty()) {
                    cursor.setPosition(target < 0 ? -1 : Math.min(target, total));
                }
                break;
            }
            case FORWARD: {
                if (count == 0) {
                    // PG: FETCH FORWARD 0 returns the current row without moving
                    addRow(result, cursor, pos);
                    if (!result.isEmpty()) cursor.setPosition(pos);
                    break;
                }
                int lastTarget = pos;
                for (int i = 0; i < count; i++) {
                    lastTarget = pos + 1 + i;
                    addRow(result, cursor, lastTarget);
                }
                // PG: position advances even if rows not found
                if (result.isEmpty() && count > 0) {
                    cursor.setPosition(Math.min(lastTarget, total));
                } else if (!result.isEmpty()) {
                    cursor.setPosition(pos + result.size());
                }
                break;
            }
            case FORWARD_ALL:
            case ALL: {
                for (int i = pos + 1; i < total; i++) addRow(result, cursor, i);
                // PG: position moves to "after last" regardless of whether rows were found
                cursor.setPosition(total);
                break;
            }
            case BACKWARD: {
                if (count == 0) {
                    // PG: FETCH BACKWARD 0 returns the current row without moving
                    addRow(result, cursor, pos);
                    if (!result.isEmpty()) cursor.setPosition(pos);
                    break;
                }
                int lastTarget = pos;
                for (int i = 0; i < count; i++) {
                    lastTarget = pos - 1 - i;
                    addRow(result, cursor, lastTarget);
                }
                // PG: position moves backward even if rows not found
                if (result.isEmpty() && count > 0) {
                    cursor.setPosition(Math.max(lastTarget, -1));
                }
                break;
            }
            case BACKWARD_ALL: {
                for (int i = pos - 1; i >= 0; i--) addRow(result, cursor, i);
                // PG: position moves to "before first" regardless
                cursor.setPosition(-1);
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
                throw new MemgresException("cursor \"" + stmt.cursorName() + "\" does not exist", "34000");
            }
            executor.session.removeCursor(stmt.cursorName());
        }
        return QueryResult.message(QueryResult.Type.SET, "CLOSE CURSOR");
    }

    /** True when some table in the database carries a constraint of this name. */
    private boolean constraintExists(String name) {
        return findConstraint(name) != null;
    }

    /**
     * The constraint name inside a possibly schema-qualified SET CONSTRAINTS entry.
     *
     * <p>PostgreSQL takes {@code schema.name} here, and reports the schema when it does not exist.
     * Splitting on the dot and looking up the first half instead reports that the schema is a
     * constraint that does not exist, which is a refusal of valid SQL.
     */
    private String bareConstraintName(String name) {
        int dot = name.lastIndexOf('.');
        if (dot < 0) return name;
        String schema = name.substring(0, dot);
        if (!executor.database.getSchemas().containsKey(schema.toLowerCase())) {
            throw new MemgresException("schema \"" + schema + "\" does not exist", "3F000");
        }
        return name.substring(dot + 1);
    }

    /** The first constraint anywhere in the database with this name, or null. */
    private StoredConstraint findConstraint(String name) {
        for (Schema schema : executor.database.getSchemas().values()) {
            for (Table table : schema.getTables().values()) {
                StoredConstraint sc = table.getConstraint(name);
                if (sc != null) return sc;
            }
        }
        return null;
    }

    // ---- CREATE / ALTER / DROP STATISTICS ----

    /** The statistics kinds PG 18 can build. */
    private static final Set<String> STATISTICS_KINDS =
            Cols.setOf("ndistinct", "dependencies", "mcv");

    /**
     * An ALTER that names a publication nobody created is a mistake worth reporting: accepting it
     * reads as "the change was applied", and the next thing to look at the catalog disagrees.
     */
    private Database.PubDef requirePublication(String pubName) {
        Database.PubDef pub = executor.database.getPublication(pubName);
        if (pub == null) {
            throw new MemgresException(
                    "publication \"" + pubName + "\" does not exist", "42704");
        }
        return pub;
    }

    private ExtendedStatistic requireStatistic(String statName) {
        ExtendedStatistic stat = executor.database.getExtendedStatistic(statName);
        if (stat == null) {
            throw new MemgresException(
                    "statistics object \"" + statName + "\" does not exist", "42704");
        }
        return stat;
    }

    /**
     * Extended statistics describe a correlation between columns of one table, so anything the
     * planner could not read them back from — an unknown kind, a column that is not there, a
     * single column — is rejected at definition time rather than stored and ignored.
     */
    private void validateStatisticsDefinition(String statName, String tableName,
                                              List<String> cols, List<String> kinds) {
        for (String kind : kinds) {
            if (!STATISTICS_KINDS.contains(kind.toLowerCase())) {
                throw new MemgresException(
                        "unrecognized statistics kind \"" + kind + "\"", "42601");
            }
        }
        if (executor.database.getView(tableName) != null) {
            throw new MemgresException(
                    "cannot define statistics for relation \"" + tableName + "\"", "42809");
        }
        Table table = executor.resolveTable(executor.defaultSchema(), tableName);
        if (executor.database.getExtendedStatistic(statName) != null) {
            throw new MemgresException(
                    "statistics object \"" + statName + "\" already exists", "42710");
        }
        Set<String> seen = new LinkedHashSet<>();
        int plainColumns = 0;
        for (String col : cols) {
            if (col.startsWith("(")) continue; // an expression, not a column reference
            if (table.getColumnIndex(col) < 0) {
                throw new MemgresException("column \"" + col + "\" does not exist", "42703");
            }
            if (!seen.add(col.toLowerCase())) {
                throw new MemgresException(
                        "duplicate column name in statistics definition", "42701");
            }
            plainColumns++;
        }
        if (plainColumns == cols.size() && cols.size() < 2) {
            throw new MemgresException("extended statistics require at least 2 columns", "42P17");
        }
    }

    private boolean isRoleSuperuser(String role) {
        Map<String, String> roleAttrs = executor.database.getRole(role);
        if (roleAttrs != null && "true".equalsIgnoreCase(roleAttrs.get("SUPERUSER"))) return true;
        if (roleAttrs == null) {
            String lower = role.toLowerCase();
            return "memgres".equals(lower) || "test".equals(lower) || "postgres".equals(lower);
        }
        return false;
    }

    /** Separator inside a stub statement's encoded payload; SQL text cannot contain it. */
    private static final String STUB_SEP = "\u0001";

    /** The options {@code ALTER PUBLICATION ... SET (...)} takes; anything else is a 42601. */
    private static final Set<String> PUBLICATION_PARAMETERS =
            Cols.setOf("publish", "publish_via_partition_root");

    /** Whether a list already names this relation, whatever case it was written in. */
    private static boolean containsIgnoreCase(java.util.Collection<String> names, String name) {
        for (String n : names) {
            if (n != null && n.equalsIgnoreCase(name)) return true;
        }
        return false;
    }

    /**
     * A publication lists relations, so a name it is given has to be one. PostgreSQL reports a
     * name that is not a relation as a missing relation rather than accepting it into the list.
     */
    private void requirePublishableRelation(String tableName) {
        if (executor.ddlExecutor.resolveTableOrNull(tableName) == null) {
            throw new MemgresException(
                    "relation \"" + tableName + "\" does not exist", "42P01");
        }
    }

    /** The schema a SET SCHEMA names has to exist, whatever the statement then does with it. */
    private void requireSchemaForAlter(String schemaName) {
        if (executor.database.getSchema(schemaName) == null) {
            throw new MemgresException("schema " + quoted(schemaName) + " does not exist", "3F000");
        }
    }

    /** The same for the role an OWNER TO names. */
    private void requireRoleForAlter(String roleName) {
        String resolved = executor.ddlExecutor.resolveOwnerName(roleName);
        if (!executor.database.hasRole(resolved)) {
            throw new MemgresException("role " + quoted(resolved) + " does not exist", "42704");
        }
    }

    /** A name as PostgreSQL prints it in a message: wrapped in double quotes. */
    private static String quoted(String name) {
        return (char) 34 + name + (char) 34;
    }

    /** Collations PostgreSQL ships with, which exist without ever being created. */
    private static final Set<String> BUILTIN_COLLATIONS = Cols.setOf(
            "default", "c", "posix", "c.utf-8", "c.utf8", "en_us", "en_us.utf-8", "en_us.utf8",
            "und-x-icu", "en-us-x-icu", "en-x-icu", "ucs_basic", "unicode", "pg_c_utf8");

    /**
     * Conversions, tablespaces and procedural languages are accepted here without being
     * implemented, but PostgreSQL still refuses an ALTER on one that was never created — and
     * reporting success for a rename that did not happen is what makes the next statement fail
     * somewhere unrelated. The names are remembered so the existence check can be honest.
     */
    private QueryResult executeStubObject(String kind, String payload) {
        String[] parts = (payload == null ? "" : payload).split(STUB_SEP, -1);
        String first = parts.length > 0 ? parts[0] : "";
        String second = parts.length > 1 ? parts[1] : "";
        String third = parts.length > 2 ? parts[2] : "";
        String intoSchema = parts.length > 3 ? parts[3] : "";
        String newOwner = parts.length > 4 ? parts[4] : "";
        if (kind.equals("create_stub")) {
            executor.database.addStubObject(first, second);
            return QueryResult.message(QueryResult.Type.SET, "CREATE");
        }
        if (kind.equals("drop_stub")) {
            executor.database.removeStubObject(first, second);
            return QueryResult.message(QueryResult.Type.SET, "DROP");
        }
        if (kind.equals("alter_rule")) {
            if (!executor.database.hasRule(first, second)) {
                throw new MemgresException("rule \"" + first + "\" for relation \""
                        + second + "\" does not exist", "42704");
            }
            if (!third.isEmpty()) {
                if (executor.database.hasRule(third, second)) {
                    throw new MemgresException("rule \"" + third + "\" for relation \""
                            + second + "\" already exists", "42710");
                }
                executor.database.renameRule(first, second, third);
            }
            return QueryResult.message(QueryResult.Type.SET, "ALTER RULE");
        }
        if (kind.equals("alter_trigger")) {
            PgTrigger found = null;
            for (PgTrigger t : executor.database.getTriggersForTable(second)) {
                if (t.getName().equalsIgnoreCase(first)) { found = t; break; }
            }
            if (found == null) {
                throw new MemgresException("trigger \"" + first + "\" for table \""
                        + second + "\" does not exist", "42704");
            }
            if (!third.isEmpty()) {
                for (PgTrigger t : executor.database.getTriggersForTable(second)) {
                    if (t.getName().equalsIgnoreCase(third)) {
                        throw new MemgresException("trigger \"" + third + "\" for relation \""
                                + second + "\" already exists", "42710");
                    }
                }
                found.setName(third);
            }
            return QueryResult.message(QueryResult.Type.SET, "ALTER TRIGGER");
        }
        // alter_stub: kind, name, new name (empty when the action was not a rename)
        boolean isCollation = first.equals("collation");
        boolean exists = isCollation
                ? executor.database.getCollation(second) != null
                        || executor.database.hasStubObject("collation", second)
                        || BUILTIN_COLLATIONS.contains(second.toLowerCase())
                : executor.database.hasStubObject(first, second);
        if (!exists) {
            throw new MemgresException(isCollation
                    ? "collation \"" + second + "\" for encoding \"UTF8\" does not exist"
                    : first + " \"" + second + "\" does not exist", "42704");
        }
        // The object is there; now the thing the statement points at has to be too. PostgreSQL
        // checks the target of a SET SCHEMA or an OWNER TO whether or not it goes on to record
        // anything, and a script told the move happened will believe it did.
        if (!intoSchema.isEmpty()) requireSchemaForAlter(intoSchema);
        if (!newOwner.isEmpty()) requireRoleForAlter(newOwner);
        if (!third.isEmpty()) {
            if (isCollation) {
                Database.CollationDef coll = executor.database.getCollation(second);
                if (coll != null) {
                    executor.database.getUserCollations().remove(second.toLowerCase());
                    executor.database.addCollation(new Database.CollationDef(third, coll.provider,
                            coll.locale, coll.lcCollate, coll.lcCtype, coll.deterministic,
                            coll.fromCollation));
                }
            }
            executor.database.renameStubObject(first, second, third);
        }
        return QueryResult.message(QueryResult.Type.SET, "ALTER");
    }
}
