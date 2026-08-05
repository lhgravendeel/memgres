package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * Evaluates catalog metadata / introspection function calls: pg_get_indexdef,
 * pg_get_constraintdef, pg_get_viewdef, format_type, obj_description,
 * to_regclass, to_regtype, etc.  Extracted from CatalogSystemFunctions.
 */
class CatalogMetadataFunctions {

    static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    /** The write-ahead log position memgres reports: it keeps no log, so nothing has moved. */
    private static final String ZERO_LSN = "0/0";

    /**
     * The functions that answer with a write-ahead log position. Their value is carried as the
     * text it prints as, so pg_typeof has nothing but the name to read the type off.
     */
    private static final Set<String> LSN_FUNCTIONS = Cols.setOf(
            "pg_switch_wal", "pg_current_wal_lsn", "pg_current_wal_insert_lsn",
            "pg_current_wal_flush_lsn", "pg_last_wal_receive_lsn", "pg_last_wal_replay_lsn");

    /**
     * Names of tables that are genuinely known to exist in pg_catalog or information_schema.
     * Used by to_regclass to avoid treating every arbitrary name as a valid catalog table,
     * because SystemCatalog.resolve() returns an emptyTable for any unrecognised pg_-prefixed
     * name (to support "SELECT * FROM pg_unknown" returning empty rather than erroring).
     */
    private static final Set<String> KNOWN_PG_CATALOG_TABLES = Cols.setOf(
            "pg_class", "pg_attribute", "pg_type", "pg_namespace", "pg_constraint",
            "pg_index", "pg_proc", "pg_description", "pg_settings", "pg_tables",
            "pg_indexes", "pg_views", "pg_sequences", "pg_am", "pg_database",
            "pg_roles", "pg_user", "pg_stat_activity", "pg_stat_gssapi", "pg_enum",
            "pg_trigger", "pg_depend", "pg_attrdef", "pg_locks",
            "pg_stat_user_tables", "pg_stat_user_indexes", "pg_prepared_xacts",
            "pg_statio_user_tables", "pg_stat_all_tables", "pg_tablespace",
            "pg_shdescription", "pg_collation", "pg_auth_members", "pg_inherits",
            "pg_policy", "pg_rewrite", "pg_event_trigger", "pg_foreign_data_wrapper",
            "pg_foreign_server", "pg_user_mapping", "pg_language", "pg_cast",
            "pg_operator", "pg_opclass", "pg_opfamily", "pg_aggregate",
            "pg_amop", "pg_amproc", "pg_foreign_table", "pg_timezone_names",
            "pg_timezone_abbrevs", "pg_sequence", "pg_authid", "pg_extension",
            "pg_stat_database", "pg_stat_all_indexes", "pg_statio_all_indexes",
            "pg_statio_user_indexes", "pg_statio_all_tables",
            "pg_stat_xact_user_tables", "pg_stat_xact_all_tables",
            "pg_cursors", "pg_prepared_statements", "pg_available_extensions",
            "pg_available_extension_versions", "pg_config", "pg_file_settings",
            "pg_hba_file_rules", "pg_shmem_allocations", "pg_stat_bgwriter",
            "pg_stat_checkpointer", "pg_stat_wal", "pg_stat_replication",
            "pg_stat_subscription", "pg_stat_progress_vacuum",
            "pg_stat_progress_create_index", "pg_stat_wal_receiver",
            "pg_publication", "pg_subscription", "pg_stat_ssl",
            "pg_matviews", "pg_rules", "pg_catalog", "pg_policies",
            "pg_seclabels", "pg_default_acl",
            // Listed in pg_class, so they must resolve by name too.
            "pg_stats", "pg_stats_ext", "pg_shadow", "pg_group", "pg_db_role_setting",
            "pg_user_mappings", "pg_publication_tables", "pg_replication_origin_status",
            "pg_stat_io", "pg_stat_archiver", "pg_stat_user_functions",
            "pg_stat_progress_analyze", "pg_stat_progress_cluster",
            "pg_stat_progress_basebackup", "pg_stat_progress_copy"
    );

    private static final Set<String> KNOWN_INFORMATION_SCHEMA_TABLES = Cols.setOf(
            "tables", "columns", "schemata", "table_constraints", "key_column_usage",
            "referential_constraints", "routines", "sequences", "views", "domains",
            "check_constraints", "constraint_column_usage", "constraint_table_usage",
            "parameters"
    );

    private final AstExecutor executor;

    CatalogMetadataFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "format_type": {
                Object typeOid = executor.evalExpr(fn.args().get(0), ctx);
                if (typeOid == null) return null;
                int oid = typeOidOf(typeOid);
                int typmod = -1;
                if (fn.args().size() > 1) {
                    Object modVal = executor.evalExpr(fn.args().get(1), ctx);
                    if (modVal != null) typmod = executor.toInt(modVal);
                }
                return formatTypeByOid(oid, typmod);
            }
            case "pg_get_constraintdef": {
                if (!fn.args().isEmpty()) {
                    Object oidVal = executor.evalExpr(fn.args().get(0), ctx);
                    if (oidVal != null) {
                        int coid = executor.toInt(oidVal);
                        for (Map.Entry<String, Schema> schemaEntry : executor.database.getSchemas().entrySet()) {
                            String schemaName = schemaEntry.getKey();
                            for (Table tbl : schemaEntry.getValue().getTables().values()) {
                                for (StoredConstraint sc : tbl.getConstraints()) {
                                    int scOid = executor.systemCatalog.getOid(
                                            CatalogConstraintBuilder.constraintKey(
                                                    schemaName, tbl.getName(), sc.getName()));
                                    if (scOid == coid) {
                                        return formatConstraintDef(sc, schemaName, tbl);
                                    }
                                }
                                // H16: NOT NULL constraints (contype 'n') — "NOT NULL <col>"
                                for (Column c : tbl.getColumns()) {
                                    if (c.isNullable()) continue;
                                    String conname = notNullConstraintName(tbl, c);
                                    int nnOid = executor.systemCatalog.getOid(
                                            CatalogConstraintBuilder.constraintKey(
                                                    schemaName, tbl.getName(), conname));
                                    if (nnOid == coid) {
                                        return "NOT NULL " + c.getName();
                                    }
                                }
                            }
                        }
                        // H16: domain constraints (CHECK and NOT NULL)
                        for (DomainType dom : executor.database.getDomains().values()) {
                            if (dom.getParsedCheck() != null) {
                                int dcOid = executor.systemCatalog.getOid(
                                        "con:domain:" + dom.getName() + "." + dom.getName() + "_check");
                                if (dcOid == coid) {
                                    return "CHECK ((" + stripOuterParens(
                                            CatalogHelper.renderDomainCheck(dom, dom.getParsedCheck())) + "))";
                                }
                            }
                            for (DomainType.NamedConstraint nc : dom.getNamedConstraints()) {
                                int ncOid = executor.systemCatalog.getOid(
                                        "con:domain:" + dom.getName() + "." + nc.name());
                                if (ncOid == coid) {
                                    return "CHECK ((" + stripOuterParens(
                                            CatalogHelper.renderDomainCheck(dom, nc.parsedCheck)) + "))";
                                }
                            }
                            if (dom.isNotNull()) {
                                int dnOid = executor.systemCatalog.getOid(
                                        "con:domain:" + dom.getName() + "." + dom.getName() + "_not_null");
                                if (dnOid == coid) {
                                    return "NOT NULL";
                                }
                            }
                        }
                    }
                }
                // Measured on PostgreSQL 18: an OID that names no constraint answers NULL, and so
                // does a NULL argument. The whole pg_get_*def family does. An empty string reads
                // like a constraint with no definition, which is a thing that cannot exist, and a
                // client concatenating the answer into DDL writes a broken statement out of it.
                return null;
            }
            case "pg_get_indexdef":
                return evalPgGetIndexdef(fn, ctx);
            case "pg_get_expr": {
                if (fn.args().size() > 0) {
                    Object expr = executor.evalExpr(fn.args().get(0), ctx);
                    if (expr == null) return null;
                    return expr.toString();
                }
                return null;
            }
            case "pg_get_triggerdef":
                return evalPgGetTriggerdef(fn, ctx);
            case "pg_get_ruledef": {
                if (fn.args().isEmpty()) return null;
                Object ruleOidVal = executor.evalExpr(fn.args().get(0), ctx);
                if (fn.args().size() > 1) executor.evalExpr(fn.args().get(1), ctx);
                if (ruleOidVal == null) return null;
                int ruleOid = executor.toInt(ruleOidVal);
                // M19: a view's implicit "_RETURN" rule reproduces the view query.
                for (Database.ViewDef vd : executor.database.getViews().values()) {
                    int rOid = executor.systemCatalog.getOid("rule:_RETURN_" + vd.name());
                    if (rOid == ruleOid && vd.query() != null) {
                        String vSchema = vd.schemaName() != null ? vd.schemaName() : "public";
                        String sql = vd.sourceSQL() != null ? vd.sourceSQL()
                                : SqlUnparser.toSql(vd.query());
                        String pretty = SqlUnparser.prettyViewDef(sql);
                        return "CREATE RULE \"_RETURN\" AS\n    ON SELECT TO "
                                + vSchema + "." + vd.name() + " DO INSTEAD " + pretty + ";";
                    }
                }
                // A rule written with CREATE RULE is stored as the text it deparses to.
                for (java.util.Map.Entry<String, String[]> entry
                        : executor.database.getRuleDefinitions().entrySet()) {
                    int rOid = executor.systemCatalog.getOid(
                            "rule:" + entry.getKey() + "_" + entry.getValue()[0]);
                    if (rOid == ruleOid) return entry.getValue()[1];
                }
                return null;
            }
            case "pg_get_function_sqlbody": {
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                return null;
            }
            case "pg_get_partkeydef": {
                if (fn.args().isEmpty()) return null;
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                int targetOid;
                if (arg instanceof RegclassValue) targetOid = ((RegclassValue) arg).oid();
                else if (arg instanceof Number) targetOid = ((Number) arg).intValue();
                else try { targetOid = Integer.parseInt(arg.toString().trim()); } catch (NumberFormatException e) { return null; }
                // Resolve OID to table, return partition key definition
                for (Map.Entry<String, Schema> schemaEntry : executor.database.getSchemas().entrySet()) {
                    for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                        Table t = tableEntry.getValue();
                        int tblOid = executor.systemCatalog.getOidMap()
                                .getOrDefault("rel:" + schemaEntry.getKey() + "." + t.getName(), -1);
                        if (tblOid == targetOid && t.getPartitionStrategy() != null) {
                            String col = t.getPartitionColumn();
                            return t.getPartitionStrategy().toUpperCase() + " (" + (col != null ? col : "") + ")";
                        }
                    }
                }
                return null;
            }
            case "pg_get_viewdef":
                return evalPgGetViewdef(fn, ctx);
            case "pg_relation_is_updatable": {
                String[] rel = relationOfArg(fn.args().get(0), ctx);
                if (rel == null) return 0;
                boolean includeTriggers = fn.args().size() > 1
                        && executor.isTruthy(executor.evalExpr(fn.args().get(1), ctx));
                return ViewUpdatability.relationEvents(executor.database, rel[0], rel[1], includeTriggers);
            }
            case "pg_column_is_updatable": {
                String[] rel = relationOfArg(fn.args().get(0), ctx);
                if (rel == null) return Boolean.FALSE;
                Object attArg = executor.evalExpr(fn.args().get(1), ctx);
                if (attArg == null) return null;
                int attnum = executor.toInt(attArg);
                boolean includeTriggers = fn.args().size() > 2
                        && executor.isTruthy(executor.evalExpr(fn.args().get(2), ctx));
                String column = columnNameAt(rel[0], rel[1], attnum);
                if (column == null) return Boolean.FALSE;
                return ViewUpdatability.columnIsUpdatable(executor.database, rel[0], rel[1],
                        column, includeTriggers) ? Boolean.TRUE : Boolean.FALSE;
            }
            case "pg_get_functiondef": {
                if (!fn.args().isEmpty()) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    Object[] row = builtinProcRow(arg);
                    if (row != null) return buildProcFunctionDef(row);
                    PgFunction func = resolveFunctionValue(arg);
                    if (func != null) {
                        return buildFunctionDef(func);
                    }
                    row = procRowOf(arg);
                    if (row != null) return buildProcFunctionDef(row);
                }
                // An OID with no pg_proc row behind it is an object PostgreSQL has nothing to
                // deparse, and it answers NULL rather than an empty definition.
                return null;
            }
            case "pg_get_function_arguments": {
                if (!fn.args().isEmpty()) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    Object[] row = builtinProcRow(arg);
                    if (row != null) return renderProcArguments(row, true);
                    PgFunction func = resolveFunctionValue(arg);
                    if (func != null) {
                        return buildFunctionArguments(func);
                    }
                    row = procRowOf(arg);
                    if (row != null) return renderProcArguments(row, true);
                }
                return null;
            }
            case "pg_get_function_identity_arguments": {
                if (!fn.args().isEmpty()) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    Object[] row = builtinProcRow(arg);
                    if (row != null) return renderProcArguments(row, false);
                    PgFunction func = resolveFunctionValue(arg);
                    if (func != null) {
                        return buildFunctionIdentityArguments(func);
                    }
                    row = procRowOf(arg);
                    if (row != null) return renderProcArguments(row, false);
                }
                return null;
            }
            case "pg_get_function_result": {
                if (!fn.args().isEmpty()) {
                    Object resultArg = executor.evalExpr(fn.args().get(0), ctx);
                    Object[] builtinRow = builtinProcRow(resultArg);
                    if (builtinRow != null) return renderProcResult(builtinRow);
                    PgFunction func = resolveFunctionValue(resultArg);
                    if (func != null) {
                        String rt = func.getReturnType();
                        if (rt == null || rt.isEmpty()) {
                            long outCount = func.getParams().stream()
                                    .filter(p -> "OUT".equalsIgnoreCase(p.mode()) || "INOUT".equalsIgnoreCase(p.mode()))
                                    .count();
                            return outCount > 1 ? "record" : (outCount == 1
                                    ? normalizePgTypeName(func.getParams().stream()
                                        .filter(p -> "OUT".equalsIgnoreCase(p.mode()) || "INOUT".equalsIgnoreCase(p.mode()))
                                        .findFirst().get().typeName())
                                    : "void");
                        }
                        return normalizePgTypeName(rt);
                    }
                    Object[] row = procRowOf(resultArg);
                    if (row != null) return renderProcResult(row);
                }
                return null;
            }
            case "pg_get_serial_sequence":
                return evalPgGetSerialSequence(fn, ctx);
            case "obj_description":
                return evalObjDescription(fn, ctx);
            case "col_description": {
                if (fn.args().size() < 2) return null;
                Object tableArg = executor.evalExpr(fn.args().get(0), ctx);
                Object colNumArg = executor.evalExpr(fn.args().get(1), ctx);
                if (tableArg == null || colNumArg == null) return null;
                int targetOid = oidOf(tableArg);
                if (targetOid < 0) return null;
                return describedBy(targetOid, executor.toInt(colNumArg), null);
            }
            case "pg_get_userbyid":
                return "memgres";
            case "pg_get_acl": {
                // PG 18 reads the object's ACL column and answers NULL when it is unset, which is
                // both what an object nobody has granted on looks like and what an object that is
                // not there at all looks like. memgres keeps grants outside the catalog rows, so
                // every object reads as the default ACL — the same NULL.
                for (Expression a : fn.args()) executor.evalExpr(a, ctx);
                return null;
            }
            case "pg_encoding_to_char":
                return "UTF8";
            case "shobj_description": {
                // The shared catalogs — databases, roles, tablespaces. Their comments are kept by
                // name rather than by schema, because nothing holds them.
                if (fn.args().size() < 2) {
                    for (Expression a : fn.args()) executor.evalExpr(a, ctx);
                    return null;
                }
                Object oidArg = executor.evalExpr(fn.args().get(0), ctx);
                Object catArg = executor.evalExpr(fn.args().get(1), ctx);
                if (oidArg == null || catArg == null) return null;
                String cat = String.valueOf(catArg).toLowerCase();
                String kind = cat.contains("database") ? "database"
                        : cat.contains("authid") || cat.contains("roles") ? "role"
                        : cat.contains("tablespace") ? "tablespace" : null;
                if (kind == null) return null;
                String named = nameForSharedOid(kind, oidOf(oidArg));
                return named == null ? null : executor.database.getComment(kind, named);
            }
            case "pg_describe_object": {
                if (fn.args().size() < 3) {
                    for (Expression a : fn.args()) executor.evalExpr(a, ctx);
                    return "";
                }
                Object classIdVal = executor.evalExpr(fn.args().get(0), ctx);
                Object objIdVal = executor.evalExpr(fn.args().get(1), ctx);
                Object objSubIdVal = executor.evalExpr(fn.args().get(2), ctx);
                if (classIdVal == null || objIdVal == null) return "";
                int classId = executor.toInt(classIdVal);
                int objId = executor.toInt(objIdVal);
                switch (classId) {
                    case 1255: { // pg_proc
                        // Look up function by OID
                        for (Map.Entry<String, Integer> entry : executor.systemCatalog.getOidMap().entrySet()) {
                            if (entry.getValue() == objId && entry.getKey().startsWith("proc:")) {
                                String funcName = entry.getKey().substring(5); // strip "proc:"
                                // Build argument type list from the actual function definition
                                PgFunction pgFunc = executor.database.getFunction(funcName);
                                if (pgFunc != null && pgFunc.getParams() != null && !pgFunc.getParams().isEmpty()) {
                                    StringBuilder sb = new StringBuilder("function ");
                                    sb.append(funcName).append("(");
                                    for (int i = 0; i < pgFunc.getParams().size(); i++) {
                                        if (i > 0) sb.append(", ");
                                        sb.append(pgFunc.getParams().get(i).typeName);
                                    }
                                    sb.append(")");
                                    return sb.toString();
                                }
                                return "function " + funcName + "()";
                            }
                        }
                        return "";
                    }
                    case 1259: { // pg_class
                        for (Map.Entry<String, Integer> entry : executor.systemCatalog.getOidMap().entrySet()) {
                            if (entry.getValue() == objId && entry.getKey().startsWith("rel:")) {
                                String fullKey = entry.getKey().substring(4); // strip "rel:"
                                // Extract table name from schema.table
                                String tableName = fullKey.contains(".") ? fullKey.substring(fullKey.lastIndexOf('.') + 1) : fullKey;
                                return "table " + tableName;
                            }
                        }
                        return "";
                    }
                    case 2615: { // pg_namespace
                        for (Map.Entry<String, Integer> entry : executor.systemCatalog.getOidMap().entrySet()) {
                            if (entry.getValue() == objId && entry.getKey().startsWith("ns:")) {
                                String schemaName = entry.getKey().substring(3); // strip "ns:"
                                return "schema " + schemaName;
                            }
                        }
                        return "";
                    }
                    default:
                        return "";
                }
            }
            case "_pg_expandarray": {
                // SRF: returns (x, n) for each element — x=value, n=1-based ordinal
                if (fn.args().isEmpty()) return null;
                Object arrVal = executor.evalExpr(fn.args().get(0), ctx);
                if (arrVal == null) return null;
                List<Object> result = new java.util.ArrayList<>();
                if (arrVal instanceof List<?>) {
                    List<?> list = (List<?>) arrVal;
                    for (int i = 0; i < list.size(); i++) {
                        result.add(Cols.listOf(list.get(i), i + 1));
                    }
                } else if (arrVal instanceof String) {
                    String s = ((String) arrVal).trim();
                    if (s.isEmpty()) return null;
                    String[] parts = s.split("\\s+");
                    for (int i = 0; i < parts.length; i++) {
                        try {
                            result.add(Cols.listOf(Integer.parseInt(parts[i]), i + 1));
                        } catch (NumberFormatException e) {
                            result.add(Cols.listOf(parts[i], i + 1));
                        }
                    }
                }
                return result.isEmpty() ? null : result;
            }
            case "_pg_char_max_length":
            case "_pg_char_octet_length":
            case "_pg_numeric_precision":
            case "_pg_numeric_precision_radix":
            case "_pg_numeric_scale":
            case "_pg_datetime_precision":
            case "_pg_interval_type":
            case "_pg_truetypid":
            case "_pg_truetypmod":
            case "_pg_index_position":
                return evalInformationSchemaHelper(name, fn, ctx);
            case "to_regclass":
                return evalToRegclass(fn, ctx);
            case "to_regtype":
                return evalToRegtype(fn, ctx);
            case "pg_switch_wal": {
                // Switches to a new write-ahead log file and answers with the position it wrote
                // up to. memgres keeps no log, so there is nothing to switch and the position is
                // the same one pg_current_wal_lsn gives -- but it is a pg_lsn, not the text it
                // is carried as, which is what a caller comparing two positions depends on.
                if (!fn.args().isEmpty()) return NOT_HANDLED;
                return ZERO_LSN;
            }
            case "pg_typeof": {
                // A write-ahead log position is carried as its own text, so the value says
                // nothing about the type that produced it and only the declaration can.
                Expression typeofArg = fn.args().isEmpty() ? null : fn.args().get(0);
                if (typeofArg instanceof FunctionCallExpr) {
                    String called = FunctionEvaluator.stripSchemaPrefix(
                            ((FunctionCallExpr) typeofArg).name().toLowerCase(Locale.ROOT));
                    if (LSN_FUNCTIONS.contains(called)) return "pg_lsn";
                    // A function reference is held as its name and its OID together, which is
                    // the same value for both of these; only the call says which type it is.
                    if ("to_regproc".equals(called)) return "regproc";
                    if ("to_regprocedure".equals(called)) return "regprocedure";
                }
                return NOT_HANDLED;
            }
            case "to_regprocedure":
            case "to_regproc": {
                // Neither raises: a name nothing answers to, a name several answer to and a name
                // in a schema that holds no such function are all no function at all.
                if (fn.args().isEmpty()) return null;
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                ProcLookup lookup = lookupProc(executor, arg.toString(),
                        "to_regprocedure".equals(name));
                if (lookup == null || lookup.ambiguous) return null;
                return new RegprocValue(lookup.oid, lookup.display);
            }
            case "pg_partition_root": {
                // pg_partition_root(regclass) → returns root partition table
                // In memgres, partitioning is limited; return the table itself as root
                if (fn.args().isEmpty()) return null;
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String tableName = arg.toString().trim();
                // Strip schema prefix if present
                if (tableName.contains(".")) {
                    tableName = tableName.substring(tableName.lastIndexOf('.') + 1);
                }
                // Remove surrounding quotes
                if (tableName.startsWith("\"") && tableName.endsWith("\"")) {
                    tableName = tableName.substring(1, tableName.length() - 1);
                }
                // Try to find the table in schemas; return table name as root
                for (Schema s : executor.database.getSchemas().values()) {
                    Table tbl = s.getTable(tableName);
                    if (tbl == null) tbl = s.getTable(tableName.toLowerCase());
                    if (tbl != null) return tableName;
                }
                return null;
            }
            default:
                return NOT_HANDLED;
        }
    }

    // ---- Complex case methods ----

    private Object evalPgGetIndexdef(FunctionCallExpr fn, RowContext ctx) {
        if (fn.args().isEmpty()) return null;
        Object arg = executor.evalExpr(fn.args().get(0), ctx);
        int colNo = 0;
        if (fn.args().size() >= 2) {
            Object colNoArg = executor.evalExpr(fn.args().get(1), ctx);
            colNo = colNoArg != null ? ((Number) colNoArg).intValue() : 0;
            if (fn.args().size() >= 3) executor.evalExpr(fn.args().get(2), ctx);
        }
        if (arg == null) return null;
        // Two schemas may each hold an index called i, so the OID — which names exactly one of
        // them — decides which definition to print. Reducing the argument to a bare name made
        // pg_get_indexdef answer with the other schema's index.
        String indexKey = null;
        if (arg instanceof RegclassValue) {
            indexKey = relationKeyForOid(((RegclassValue) arg).oid());
            if (indexKey == null) indexKey = ((RegclassValue) arg).name();
        } else if (arg instanceof Number) {
            indexKey = relationKeyForOid(((Number) arg).intValue());
        } else {
            indexKey = arg.toString();
        }
        if (indexKey == null) return null;
        String indexName = RelationNamespace.bareName(indexKey);
        int keyDot = indexKey.lastIndexOf('.');
        String indexSchema = keyDot > 0 ? indexKey.substring(0, keyDot) : null;
        List<String> cols = executor.database.getIndexColumns(indexKey);
        String constraintTableName = null;
        boolean constraintNullsNotDistinct = false;
        if (cols == null) {
            for (Map.Entry<String, Schema> schemaEntry : executor.database.getSchemas().entrySet()) {
                if (indexSchema != null && !indexSchema.equalsIgnoreCase(schemaEntry.getKey())) continue;
                for (Map.Entry<String, Table> tblEntry : schemaEntry.getValue().getTables().entrySet()) {
                    for (StoredConstraint sc : tblEntry.getValue().getConstraints()) {
                        if (sc.getName().equalsIgnoreCase(indexName) &&
                                (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE)) {
                            cols = sc.getColumns();
                            constraintTableName = schemaEntry.getKey() + "." + tblEntry.getKey();
                            constraintNullsNotDistinct = sc.isNullsNotDistinct();
                        }
                    }
                }
            }
        }
        if (colNo > 0 && cols != null) {
            return colNo <= cols.size() ? cols.get(colNo - 1) : "";
        }
        // A relation that is not an index has no index definition, and PostgreSQL answers NULL
        // rather than the empty string a client would read as an index with no columns.
        if (cols == null) return null;
        // H16: constraint-backed indexes (PK/UNIQUE) are always unique
        boolean unique = constraintTableName != null || executor.database.isUniqueIndex(indexKey);
        String tableName = constraintTableName != null
                ? constraintTableName
                : executor.database.getIndexTable(indexKey);
        if (tableName == null) {
            for (Map.Entry<String, Schema> schemaEntry : executor.database.getSchemas().entrySet()) {
                for (Map.Entry<String, Table> tblEntry : schemaEntry.getValue().getTables().entrySet()) {
                    boolean allFound = true;
                    for (String c : cols) {
                        if (tblEntry.getValue().getColumnIndex(c) < 0) { allFound = false; break; }
                    }
                    if (allFound) {
                        tableName = schemaEntry.getKey() + "." + tblEntry.getKey();
                        break;
                    }
                }
                if (tableName != null) break;
            }
        }
        if (tableName == null) return "";
        String idxMethod = executor.database.getIndexMethod(indexKey);
        if (idxMethod == null || idxMethod.isEmpty()) idxMethod = "btree";
        String whereClause = CatalogHelper.deparseIndexPredicate(executor.database, tableName,
                executor.database.getIndexWhereClause(indexKey));
        List<String> normalizedCols = CatalogHelper.deparseIndexColumns(executor.database, tableName, cols);
        List<String> columnOptions = executor.database.getIndexColumnOptions(indexKey);
        List<String> includeColumns = executor.database.getIndexIncludeColumns(indexKey);
        boolean nullsNotDistinct = executor.database.isIndexNullsNotDistinct(indexKey)
                || constraintNullsNotDistinct;
        return CatalogStubBuilder.buildIndexDef(indexName, tableName, unique, idxMethod,
                normalizedCols, columnOptions, includeColumns, nullsNotDistinct, whereClause);
    }

    /**
     * The {@code schema.name} an OID belongs to, or null when it names no relation. The OID is
     * what tells two same-named relations in different schemas apart.
     */
    private String relationKeyForOid(int targetOid) {
        for (Map.Entry<String, Integer> entry : executor.systemCatalog.getOidMap().entrySet()) {
            if (entry.getValue() == targetOid && entry.getKey().startsWith("rel:")) {
                return entry.getKey().substring(4);
            }
        }
        return null;
    }

    private Object evalPgGetTriggerdef(FunctionCallExpr fn, RowContext ctx) {
        Object trigOidVal = fn.args().isEmpty() ? null : executor.evalExpr(fn.args().get(0), ctx);
        if (fn.args().size() > 1) executor.evalExpr(fn.args().get(1), ctx);
        if (trigOidVal == null) return null;
        int trigOid = executor.toInt(trigOidVal);
        for (Map.Entry<String, List<PgTrigger>> trigEntry : executor.database.getAllTriggers().entrySet()) {
            Map<String, java.util.List<PgTrigger>> grouped = new java.util.LinkedHashMap<>();
            for (PgTrigger trig : trigEntry.getValue()) {
                String key = (trig.getTableName() == null ? "" : trig.getTableName().toLowerCase())
                        + "." + trig.getName().toLowerCase();
                grouped.computeIfAbsent(key, k -> new java.util.ArrayList<>()).add(trig);
            }
            for (Map.Entry<String, List<PgTrigger>> nameEntry : grouped.entrySet()) {
                PgTrigger first = nameEntry.getValue().get(0);
                String trigSchema = first.getSchemaName() != null ? first.getSchemaName() : "public";
                // The trigger's OID is the one pg_trigger gave it, which is keyed by the
                // relation it belongs to — a trigger name is only unique within its table.
                int tOid = executor.systemCatalog.getOid(
                        "trig:" + trigSchema + "." + first.getTableName() + "." + first.getName());
                if (tOid == trigOid) {
                    return buildTriggerDef(nameEntry.getValue(), trigSchema);
                }
            }
        }
        return null;
    }

    /** The events a trigger may fire on, in the order PostgreSQL prints them. */
    private static final PgTrigger.Event[] TRIGGER_EVENT_ORDER = {
            PgTrigger.Event.INSERT, PgTrigger.Event.DELETE,
            PgTrigger.Event.UPDATE, PgTrigger.Event.TRUNCATE};

    /**
     * The statement that would create this trigger, spelled the way PostgreSQL's
     * pg_get_triggerdef spells it: the relation qualified by its schema, the events in the
     * catalog's own order, and the WHEN condition when one was written.
     */
    private String buildTriggerDef(List<PgTrigger> triggers, String schema) {
        PgTrigger first = triggers.get(0);
        StringBuilder sb = new StringBuilder("CREATE TRIGGER ");
        sb.append(first.getName()).append(' ');
        sb.append(first.getTiming() == PgTrigger.Timing.INSTEAD_OF ? "INSTEAD OF"
                : first.getTiming().name()).append(' ');
        java.util.List<String> events = new java.util.ArrayList<>();
        for (PgTrigger.Event wanted : TRIGGER_EVENT_ORDER) {
            for (PgTrigger t : triggers) {
                if (t.getEvent() != wanted) continue;
                String ev = wanted.name();
                if (wanted == PgTrigger.Event.UPDATE && t.getUpdateColumns() != null
                        && !t.getUpdateColumns().isEmpty()) {
                    ev += " OF " + String.join(", ", t.getUpdateColumns());
                }
                if (!events.contains(ev)) events.add(ev);
            }
        }
        sb.append(String.join(" OR ", events));
        sb.append(" ON ").append(schema).append('.').append(first.getTableName());
        // The transition tables are what the trigger function reads its rows from; a definition
        // that leaves them out restores a trigger whose body cannot see the statement's work.
        String oldTable = first.getOldTransitionTable();
        String newTable = first.getNewTransitionTable();
        if (oldTable != null || newTable != null) {
            sb.append(" REFERENCING");
            if (oldTable != null) sb.append(" OLD TABLE AS ").append(oldTable);
            if (newTable != null) sb.append(" NEW TABLE AS ").append(newTable);
        }
        sb.append(" FOR EACH ").append(first.isForEachStatement() ? "STATEMENT" : "ROW");
        if (first.getWhenClause() != null && !first.getWhenClause().isEmpty()) {
            sb.append(" WHEN (").append(deparseTriggerWhen(first.getWhenClause())).append(')');
        }
        sb.append(" EXECUTE FUNCTION ").append(first.getFunctionName()).append('(');
        // The arguments belong in the definition: a dump that leaves them out restores a trigger
        // whose function sees TG_NARGS = 0.
        List<String> trigArgs = null;
        for (PgTrigger t : triggers) {
            if (t.getArgs() != null && !t.getArgs().isEmpty()) { trigArgs = t.getArgs(); break; }
        }
        if (trigArgs != null) {
            for (int i = 0; i < trigArgs.size(); i++) {
                if (i > 0) sb.append(", ");
                String arg = trigArgs.get(i) == null ? "" : trigArgs.get(i);
                sb.append('\'').append(arg.replace("'", "''")).append('\'');
            }
        }
        sb.append(')');
        return sb.toString();
    }

    /** {@code OLD} or {@code NEW} followed by a dot, however the statement spaced it. */
    private static final java.util.regex.Pattern TRIGGER_ROW_REF =
            java.util.regex.Pattern.compile("(?i)\\b(old|new)\\s*\\.\\s*");

    /**
     * A trigger's WHEN condition as pg_get_triggerdef prints it. PostgreSQL keeps the condition
     * as a parsed tree and prints it back from there, so what comes out is its own spelling and
     * not the statement's: the row references are lower case, a qualified name carries no spaces
     * around its dot, and the whole condition is parenthesised inside the WHEN's own parentheses.
     *
     * <p>What is stored here is the text as written, so only those three are corrected. A
     * condition whose operators PostgreSQL would re-bracket, or whose literals it would print
     * with an explicit cast, still comes back in the spelling it was written in.
     *
     * <p>The correction stops at a quote. {@code WHEN (NEW.v = 'Old. Faithful')} is a comparison
     * against a value, and a value is not a row reference however much it looks like one: lower
     * casing it and closing up the space after its dot produced a definition that restores a
     * trigger firing on a string nobody stored.
     */
    private static String deparseTriggerWhen(String condition) {
        String text = condition.trim();
        StringBuilder out = new StringBuilder("(");
        int i = 0;
        int plainFrom = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (c != '\'' && c != '"') { i++; continue; }
            appendRowRefsLowered(out, text.substring(plainFrom, i));
            int j = i + 1;
            while (j < text.length()) {
                if (text.charAt(j) == c) {
                    if (j + 1 < text.length() && text.charAt(j + 1) == c) { j += 2; continue; }
                    break;
                }
                j++;
            }
            // An unterminated quote runs to the end of the condition; copy what is there.
            int end = Math.min(j + 1, text.length());
            out.append(text, i, end);
            i = end;
            plainFrom = end;
        }
        appendRowRefsLowered(out, text.substring(plainFrom));
        return out.append(')').toString();
    }

    /** Copy a stretch of condition text with {@code OLD.}/{@code NEW.} in PostgreSQL's spelling. */
    private static void appendRowRefsLowered(StringBuilder out, String plain) {
        java.util.regex.Matcher m = TRIGGER_ROW_REF.matcher(plain);
        StringBuffer buf = new StringBuffer();
        while (m.find()) {
            m.appendReplacement(buf, m.group(1).toLowerCase(Locale.ROOT) + ".");
        }
        m.appendTail(buf);
        out.append(buf);
    }

    /**
     * The schema and name a regclass argument stands for, or null when it stands for nothing.
     *
     * <p>PostgreSQL's updatability functions take a regclass and answer 0 / false for an oid that
     * reaches no relation rather than raising, so a caller sweeping pg_class while another
     * session drops a relation gets an answer instead of a failed statement.</p>
     */
    private String[] relationOfArg(Expression arg, RowContext ctx) {
        Object value = executor.evalExpr(arg, ctx);
        if (value == null) return null;
        String name = null;
        if (value instanceof RegclassValue) {
            name = ((RegclassValue) value).name();
        } else if (value instanceof Number) {
            int targetOid = ((Number) value).intValue();
            for (Map.Entry<String, Integer> entry : executor.systemCatalog.getOidMap().entrySet()) {
                if (entry.getValue() == targetOid && entry.getKey().startsWith("rel:")) {
                    name = entry.getKey().substring("rel:".length());
                    break;
                }
            }
            if (name == null) return null;
        } else {
            name = value.toString();
        }
        if (name == null || name.isEmpty()) return null;
        int dot = name.lastIndexOf('.');
        if (dot >= 0) {
            return new String[]{name.substring(0, dot), name.substring(dot + 1)};
        }
        return new String[]{executor.defaultSchema(), name};
    }

    /** The name of the column at this attnum, or null when the relation has no such column. */
    private String columnNameAt(String schemaName, String relationName, int attnum) {
        if (attnum < 1) return null;
        Database.ViewDef vd = executor.database.getView(schemaName, relationName);
        List<Column> cols = null;
        if (vd != null) {
            cols = vd.cachedColumns();
        } else {
            Schema schema = executor.database.getSchema(schemaName);
            Table t = schema != null ? schema.getTable(relationName) : null;
            if (t == null) {
                for (Schema s : executor.database.getSchemas().values()) {
                    t = s.getTable(relationName);
                    if (t != null) break;
                }
            }
            if (t != null) cols = t.getColumns();
        }
        if (cols == null || attnum > cols.size()) return null;
        return cols.get(attnum - 1).getName();
    }

    private Object evalPgGetViewdef(FunctionCallExpr fn, RowContext ctx) {
        if (fn.args().isEmpty()) return null;
        Object arg = executor.evalExpr(fn.args().get(0), ctx);
        String viewName = null;
        if (arg instanceof RegclassValue) {
            RegclassValue rc = (RegclassValue) arg;
            viewName = rc.name();
        } else if (arg instanceof Number) {
            Number numArg = (Number) arg;
            int targetOid = numArg.intValue();
            for (Map.Entry<String, Integer> entry : executor.systemCatalog.getOidMap().entrySet()) {
                if (entry.getValue() == targetOid && entry.getKey().startsWith("rel:")) {
                    viewName = entry.getKey().substring(entry.getKey().lastIndexOf('.') + 1);
                    break;
                }
            }
        } else if (arg != null) {
            viewName = arg.toString();
        }
        // Every form of pg_get_viewdef lays the definition out over several lines. What the second
        // argument adds is parenthesis pruning: the one-argument form parenthesises every operator
        // the way a plain deparse does, and only an explicit true — or a wrap column, which
        // implies it — drops the pairs precedence makes unnecessary.
        boolean minimizeParens = false;
        int wrapColumn = 0;
        if (fn.args().size() >= 2) {
            Object prettyArg = executor.evalExpr(fn.args().get(1), ctx);
            if (prettyArg instanceof Boolean) {
                minimizeParens = ((Boolean) prettyArg);
            } else if (prettyArg instanceof Number) {
                minimizeParens = true;
                wrapColumn = ((Number) prettyArg).intValue();
            } else if (prettyArg != null) {
                String text = prettyArg.toString().trim();
                try {
                    wrapColumn = Integer.parseInt(text);
                    minimizeParens = true;
                } catch (NumberFormatException e) {
                    minimizeParens = Boolean.parseBoolean(text);
                }
            }
        }
        if (viewName != null) {
            if (viewName.contains(".")) {
                viewName = viewName.substring(viewName.lastIndexOf('.') + 1);
            }
            Database.ViewDef view = executor.database.getView(viewName);
            if (view != null && view.query() != null) {
                String sql = view.sourceSQL() != null ? view.sourceSQL()
                        : (minimizeParens ? SqlUnparser.toSqlPretty(view.query())
                                          : SqlUnparser.toSql(view.query()));
                return SqlUnparser.prettyViewDef(sql, wrapColumn) + ";";
            }
        }
        // An OID that names no view, or names a relation that is not one, has no definition:
        // PostgreSQL answers NULL, not an empty query text.
        return null;
    }

    private Object evalPgGetSerialSequence(FunctionCallExpr fn, RowContext ctx) {
        if (fn.args().size() < 2) return null;
        Object tblArg = executor.evalExpr(fn.args().get(0), ctx);
        Object colArg = executor.evalExpr(fn.args().get(1), ctx);
        if (tblArg == null || colArg == null) return null;
        String tblName = String.valueOf(tblArg);
        String colName = String.valueOf(colArg);
        String explicitSchema = null;
        if (tblName.contains(".")) {
            explicitSchema = tblName.substring(0, tblName.lastIndexOf('.'));
            tblName = tblName.substring(tblName.lastIndexOf('.') + 1);
        }
        boolean relationFound = false;
        boolean columnFound = false;
        for (java.util.Map.Entry<String, Schema> entry : executor.database.getSchemas().entrySet()) {
            String schemaName = entry.getKey();
            if (explicitSchema != null && !schemaName.equalsIgnoreCase(explicitSchema)) continue;
            Schema schema = entry.getValue();
            Table tbl = schema.getTable(tblName);
            if (tbl != null) {
                relationFound = true;
                for (Column col : tbl.getColumns()) {
                    if (col.getName().equalsIgnoreCase(colName)) {
                        columnFound = true;
                        String def = col.getDefaultValue();
                        if (def != null && def.toLowerCase().contains("nextval")) {
                            int q1 = def.indexOf('\'');
                            int q2 = def.indexOf('\'', q1 + 1);
                            if (q1 >= 0 && q2 > q1) {
                                return schemaName + "." + def.substring(q1 + 1, q2);
                            }
                        }
                        if (col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL
                                || col.getType() == DataType.SMALLSERIAL
                                || (def != null && def.contains("__identity__"))) {
                            String seqName = tblName + "_" + colName + "_seq";
                            if (executor.database.getSequence(schemaName, seqName) == null) {
                                Sequence seq = new Sequence(seqName, tbl.getSerialCounter(), 1L, null, null);
                                seq.setSchemaName(schemaName);
                                executor.database.addSequence(seq);
                            }
                            return schemaName + "." + seqName;
                        }
                    }
                }
            }
        }
        // A name that resolves to nothing is an error, not a NULL: a caller asking for the sequence
        // behind a column it misspelled would otherwise read the NULL as "column has no sequence".
        if (!relationFound && !executor.database.hasView(tblName)) {
            throw new MemgresException("relation \"" + tblName + "\" does not exist", "42P01");
        }
        if (relationFound && !columnFound) {
            throw new MemgresException(
                    "column \"" + colName + "\" of relation \"" + tblName + "\" does not exist", "42703");
        }
        // M20: fallback — check sequences with OWNED BY pointing to this table.column
        for (java.util.Map.Entry<String, Sequence> seqEntry : executor.database.getSequences().entrySet()) {
            Sequence seq = seqEntry.getValue();
            if (seq.getOwnedByTable() != null && seq.getOwnedByTable().equalsIgnoreCase(tblName)
                    && seq.getOwnedByColumn() != null && seq.getOwnedByColumn().equalsIgnoreCase(colName)) {
                return seq.getSchemaName() + "." + seq.getName();
            }
        }
        return null;
    }

    /**
     * {@code obj_description(oid[, catalog])}, which PostgreSQL defines as a read of
     * pg_description: the row whose objoid is the object, whose objsubid is 0, and whose classoid
     * is the catalog the object lives in. Reading the same table the catalog builds is what keeps
     * the answer and {@code \d+} in agreement whatever kind of object was commented on.
     */
    private Object evalObjDescription(FunctionCallExpr fn, RowContext ctx) {
        Object oidArg = executor.evalExpr(fn.args().get(0), ctx);
        String catalog = fn.args().size() > 1
                ? String.valueOf(executor.evalExpr(fn.args().get(1), ctx)) : null;
        if (oidArg == null) return null;
        int targetOid = oidOf(oidArg);
        if (targetOid < 0) return null;
        return describedBy(targetOid, 0, catalog);
    }

    /** The OID an argument to a description function carries, or -1. */
    private int oidOf(Object arg) {
        if (arg instanceof RegclassValue) return ((RegclassValue) arg).oid();
        if (arg instanceof RegtypeValue) return ((RegtypeValue) arg).oid();
        if (arg instanceof RegprocValue) return ((RegprocValue) arg).oid();
        if (arg instanceof RegnamespaceValue) return ((RegnamespaceValue) arg).oid();
        if (arg instanceof Number) return ((Number) arg).intValue();
        try {
            return Integer.parseInt(String.valueOf(arg).trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * The pg_description row for this object, or null. A null {@code catalog} takes any classoid,
     * which is the one-argument form's behaviour.
     */
    private Object describedBy(int objOid, int objSubId, String catalog) {
        Table descr;
        try {
            descr = executor.systemCatalog.resolve("pg_catalog", "pg_description");
        } catch (RuntimeException e) {
            return null;
        }
        if (descr == null) return null;
        int wantClass = -1;
        if (catalog != null && !catalog.trim().isEmpty()) {
            String bare = catalog.trim().toLowerCase();
            if (bare.startsWith("pg_catalog.")) bare = bare.substring("pg_catalog.".length());
            wantClass = executor.systemCatalog.getOid("rel:pg_catalog." + bare);
        }
        for (Object[] row : descr.getRows()) {
            if (executor.toInt(row[0]) != objOid) continue;
            if (executor.toInt(row[2]) != objSubId) continue;
            if (wantClass >= 0 && executor.toInt(row[1]) != wantClass) continue;
            return row[3];
        }
        return null;
    }

    /** The name of a database, role or tablespace of this OID, or null. */
    private String nameForSharedOid(String kind, int oid) {
        if (oid < 0) return null;
        String prefix = kind.equals("database") ? "db:" : kind.equals("role") ? "role:" : "ts:";
        for (Map.Entry<String, Integer> e : executor.systemCatalog.getOidMap().entrySet()) {
            if (e.getValue() != null && e.getValue() == oid && e.getKey().startsWith(prefix)) {
                return e.getKey().substring(prefix.length()).toLowerCase();
            }
        }
        return null;
    }

    // ------------------------------------------------------------------
    // information_schema's own helper functions
    // ------------------------------------------------------------------

    /**
     * The functions {@code information_schema}'s views are written in terms of.
     *
     * <p>They are ordinary SQL functions in PostgreSQL, declared in the {@code information_schema}
     * namespace and readable in {@code information_schema.sql}. memgres computes the same view
     * columns natively rather than by composing these, so the views agreed already — but the
     * helpers are also called directly, by ORMs and by anything that reads a column's declared
     * width or precision out of {@code pg_attribute} the way the views do, and every one of those
     * calls was a 42883.
     *
     * <p>Each answer below follows the reference server's own definition rather than memgres's
     * idea of the type, because the typmod arithmetic is what a caller is really asking about: a
     * varchar's typmod is its length plus four, a numeric's packs precision and scale into one
     * integer, and an interval's packs a field mask above its precision.
     */
    private Object evalInformationSchemaHelper(String name, FunctionCallExpr fn, RowContext ctx) {
        requireInformationSchemaVisible(fn, ctx);
        if (fn.args().size() != 2) {
            throw new MemgresException("function " + fn.name() + "(" + helperArgTypes(fn, ctx)
                    + ") does not exist\n  Hint: No function matches the given name and argument"
                    + " types. You might need to add explicit type casts.", "42883");
        }
        if ("_pg_truetypid".equals(name) || "_pg_truetypmod".equals(name)) {
            return evalTrueType(name, fn, ctx);
        }
        if ("_pg_index_position".equals(name)) {
            return evalIndexPosition(fn, ctx);
        }
        // Every remaining helper is declared RETURNS NULL ON NULL INPUT, so a NULL argument is
        // answered without the body running.
        Object typidVal = executor.evalExpr(fn.args().get(0), ctx);
        Object typmodVal = executor.evalExpr(fn.args().get(1), ctx);
        if (typidVal == null || typmodVal == null) return null;
        int typid = typeOidOf(typidVal);
        int typmod = executor.toInt(typmodVal);
        switch (name) {
            case "_pg_char_max_length":
                return pgCharMaxLength(typid, typmod);
            case "_pg_char_octet_length": {
                if (typid != TEXT_OID && typid != BPCHAR_OID && typid != VARCHAR_OID) return null;
                if (typmod == -1) return 1 << 30;
                Integer maxLength = pgCharMaxLength(typid, typmod);
                // The multiplier is pg_encoding_max_length of the database encoding; memgres
                // stores and serves text as UTF8, whose longest character is four bytes.
                return maxLength == null ? null : (Object) (maxLength * 4);
            }
            case "_pg_numeric_precision":
                switch (typid) {
                    case INT2_OID: return 16;
                    case INT4_OID: return 32;
                    case INT8_OID: return 64;
                    case FLOAT4_OID: return 24;
                    case FLOAT8_OID: return 53;
                    case NUMERIC_OID:
                        return typmod == -1 ? null : (Object) (((typmod - 4) >> 16) & 0xFFFF);
                    default: return null;
                }
            case "_pg_numeric_precision_radix":
                switch (typid) {
                    case INT2_OID: case INT4_OID: case INT8_OID:
                    case FLOAT4_OID: case FLOAT8_OID:
                        return 2;
                    case NUMERIC_OID:
                        return 10;
                    default: return null;
                }
            case "_pg_numeric_scale":
                switch (typid) {
                    case INT2_OID: case INT4_OID: case INT8_OID:
                        return 0;
                    case NUMERIC_OID:
                        return typmod == -1 ? null : (Object) ((typmod - 4) & 0xFFFF);
                    default: return null;
                }
            case "_pg_datetime_precision":
                if (typid == DATE_OID) return 0;
                if (typid == TIME_OID || typid == TIMETZ_OID
                        || typid == TIMESTAMP_OID || typid == TIMESTAMPTZ_OID) {
                    return typmod < 0 ? 6 : typmod;
                }
                if (typid == INTERVAL_OID) {
                    // An interval's typmod carries the field mask above its precision, and a
                    // precision of 0xFFFF means none was written.
                    return (typmod < 0 || (typmod & 0xFFFF) == 0xFFFF) ? 6 : (typmod & 0xFFFF);
                }
                return null;
            case "_pg_interval_type":
                return pgIntervalType(typid, typmod);
            default:
                return NOT_HANDLED;
        }
    }

    private static final int INT8_OID = 20;
    private static final int INT2_OID = 21;
    private static final int INT4_OID = 23;
    private static final int TEXT_OID = 25;
    private static final int FLOAT4_OID = 700;
    private static final int FLOAT8_OID = 701;
    private static final int BPCHAR_OID = 1042;
    private static final int VARCHAR_OID = 1043;
    private static final int DATE_OID = 1082;
    private static final int TIME_OID = 1083;
    private static final int TIMESTAMP_OID = 1114;
    private static final int TIMESTAMPTZ_OID = 1184;
    private static final int INTERVAL_OID = 1186;
    private static final int TIMETZ_OID = 1266;
    private static final int NUMERIC_OID = 1700;
    private static final int BIT_OID = 1560;
    private static final int VARBIT_OID = 1562;

    private static Integer pgCharMaxLength(int typid, int typmod) {
        if (typmod == -1) return null;
        if (typid == BPCHAR_OID || typid == VARCHAR_OID) return typmod - 4;
        if (typid == BIT_OID || typid == VARBIT_OID) return typmod;
        return null;
    }

    /**
     * The qualifier list of an interval type, upper-cased: {@code YEAR}, {@code DAY TO SECOND(3)}.
     *
     * <p>PostgreSQL reads it back out of {@code format_type}'s own rendering, keeping whatever
     * follows the type name and the space after it — which is why a plain {@code interval},
     * having no space and no qualifier, answers NULL rather than an empty string.
     */
    private Object pgIntervalType(int typid, int typmod) {
        if (typid != INTERVAL_OID) return null;
        String formatted = formatTypeByOid(INTERVAL_OID, typmod);
        if (formatted == null || !formatted.startsWith("interval")) return null;
        int at = "interval".length();
        while (at < formatted.length()) {
            char c = formatted.charAt(at);
            if (c == '(' || c == ')' || (c >= '0' && c <= '9')) at++;
            else break;
        }
        if (at >= formatted.length() || formatted.charAt(at) != ' ') return null;
        return formatted.substring(at + 1).toUpperCase(java.util.Locale.ROOT);
    }

    /**
     * {@code _pg_truetypid} and {@code _pg_truetypmod}, which see through a domain.
     *
     * <p>Both are declared over whole rows of {@code pg_attribute} and {@code pg_type} and are
     * called as {@code _pg_truetypid(a.*, t.*)}. memgres has no composite value carrying its
     * field names, so the fields are read off the row the star stands for rather than off the
     * value it produced.
     */
    private Object evalTrueType(String name, FunctionCallExpr fn, RowContext ctx) {
        Expression attArg = fn.args().get(0);
        Expression typArg = fn.args().get(1);
        Object typtype = rowField(typArg, ctx, "typtype");
        boolean domain = typtype != null && "d".equals(String.valueOf(typtype).trim());
        if (domain) {
            return "_pg_truetypid".equals(name)
                    ? rowField(typArg, ctx, "typbasetype")
                    : rowField(typArg, ctx, "typtypmod");
        }
        return "_pg_truetypid".equals(name)
                ? rowField(attArg, ctx, "atttypid")
                : rowField(attArg, ctx, "atttypmod");
    }

    /** One field of the row a whole-row reference stands for, or null when it is not one. */
    private Object rowField(Expression expr, RowContext ctx, String field) {
        if (ctx == null) return null;
        String qualifier = null;
        if (expr instanceof WildcardExpr) {
            qualifier = ((WildcardExpr) expr).table;
        } else if (expr instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) expr;
            // A bare name that matches a FROM entry is that row, not a column of it.
            if (ref.table() == null) qualifier = ref.column();
        }
        if (qualifier == null || ctx.getBinding(qualifier) == null) return null;
        try {
            return ctx.resolveColumn(qualifier, field);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Where an attribute sits in an index's key list, counting from one, or NULL when it is not
     * in it at all.
     */
    private Object evalIndexPosition(FunctionCallExpr fn, RowContext ctx) {
        Object indexArg = executor.evalExpr(fn.args().get(0), ctx);
        Object attArg = executor.evalExpr(fn.args().get(1), ctx);
        if (indexArg == null || attArg == null) return null;
        int indexOid;
        if (indexArg instanceof RegclassValue) indexOid = ((RegclassValue) indexArg).oid;
        else if (indexArg instanceof Number) indexOid = ((Number) indexArg).intValue();
        else {
            try {
                indexOid = Integer.parseInt(String.valueOf(indexArg).trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        int attnum = executor.toInt(attArg);
        Table pgIndex = executor.systemCatalog.resolve("pg_catalog", "pg_index");
        if (pgIndex == null) return null;
        int oidAt = pgIndex.getColumnIndex("indexrelid");
        int keyAt = pgIndex.getColumnIndex("indkey");
        if (oidAt < 0 || keyAt < 0) return null;
        for (Object[] row : pgIndex.getRows()) {
            if (oidAt >= row.length || keyAt >= row.length) continue;
            Object relid = row[oidAt];
            if (!(relid instanceof Number) || ((Number) relid).intValue() != indexOid) continue;
            List<Object> keys = FromFunctionResolver.toElementList(row[keyAt]);
            if (keys == null) return null;
            for (int i = 0; i < keys.size(); i++) {
                Object key = keys.get(i);
                if (key instanceof Number && ((Number) key).intValue() == attnum) return i + 1;
                if (key != null && String.valueOf(key).trim().equals(String.valueOf(attnum))) {
                    return i + 1;
                }
            }
            return null;
        }
        return null;
    }

    /**
     * Refuses a call that names a schema these helpers are not in.
     *
     * <p>They are declared in {@code information_schema}, so {@code pg_catalog._pg_truetypid(...)}
     * names a schema that does not hold them and the reference server refuses it. memgres strips
     * a {@code pg_catalog.} qualifier from every built-in call before dispatch, which would have
     * answered it.
     *
     * <p>An <em>unqualified</em> call is deliberately not refused, even though the reference
     * server refuses one under the default search path — {@code information_schema} is never
     * implicitly on it. PostgreSQL resolves a name once, when the statement carrying it is
     * written, and a view or a routine body created while the schema was on the path keeps
     * working after it comes off. memgres re-resolves stored definitions against the session's
     * current path, so refusing here refused a view that had been created legitimately (measured:
     * {@code SET search_path = public, information_schema; CREATE VIEW v AS SELECT
     * _pg_char_max_length(...); RESET search_path; SELECT * FROM v} answers 10 on the reference
     * server and 42883 with the check in place). Refusing SQL that works is the worse of the two
     * errors, so the unqualified spelling is answered and the divergence left where it is.
     */
    private void requireInformationSchemaVisible(FunctionCallExpr fn, RowContext ctx) {
        int dot = fn.name().lastIndexOf('.');
        if (dot < 0) return;
        String qualifier = fn.name().substring(0, dot);
        if ("information_schema".equalsIgnoreCase(qualifier)) return;
        throw new MemgresException("function " + fn.name() + "(" + helperArgTypes(fn, ctx)
                + ") does not exist\n  Hint: No function matches the given name and argument"
                + " types. You might need to add explicit type casts.", "42883");
    }

    /** The written arguments named the way PostgreSQL names them in a 42883. */
    private String helperArgTypes(FunctionCallExpr fn, RowContext ctx) {
        StringBuilder types = new StringBuilder();
        for (int i = 0; i < fn.args().size(); i++) {
            if (i > 0) types.append(", ");
            Expression arg = fn.args().get(i);
            if (arg instanceof Literal
                    && ((Literal) arg).literalType() == Literal.LiteralType.STRING) {
                types.append("unknown");
                continue;
            }
            Object value;
            try {
                value = executor.evalExpr(arg, ctx);
            } catch (RuntimeException e) {
                value = null;
            }
            types.append(value == null ? "unknown" : AstExecutor.pgTypeNameOf(value));
        }
        return types.toString();
    }

    private Object evalToRegclass(FunctionCallExpr fn, RowContext ctx) {
        if (fn.args().isEmpty()) return null;
        Object argValRc = executor.evalExpr(fn.args().get(0), ctx);
        if (argValRc == null) return null;
        String regclassName = String.valueOf(argValRc);
        Table foundRc = null;
        boolean foundIndexRc = false;
        String resolvedSchemaRc = null;
        if (regclassName.contains(".")) {
            int dotIdx = regclassName.indexOf('.');
            String schemaNameRc = regclassName.substring(0, dotIdx).toLowerCase();
            String tblNameRc = regclassName.substring(dotIdx + 1).toLowerCase();
            Schema schemaRc = executor.database.getSchema(schemaNameRc);
            if (schemaRc != null) foundRc = schemaRc.getTable(tblNameRc);
            if (foundRc == null) {
                boolean isKnownCatalog = ("pg_catalog".equals(schemaNameRc)
                                && KNOWN_PG_CATALOG_TABLES.contains(tblNameRc))
                        || ("information_schema".equals(schemaNameRc)
                                && KNOWN_INFORMATION_SCHEMA_TABLES.contains(tblNameRc));
                if (isKnownCatalog) {
                    Table sysCat = executor.systemCatalog.resolve(schemaNameRc, tblNameRc);
                    if (sysCat != null) foundRc = sysCat;
                }
            }
            // A sequence and a view are relations too, and to_regclass names them the same way.
            if (foundRc == null && executor.database.getSequence(schemaNameRc, tblNameRc) != null) {
                foundIndexRc = true;
            }
            if (foundRc == null && !foundIndexRc
                    && executor.database.getView(schemaNameRc, tblNameRc) != null) {
                foundIndexRc = true;
            }
            if (foundRc == null && !foundIndexRc && executor.database.hasIndex(tblNameRc)) {
                String idxTable = executor.database.getIndexTable(tblNameRc);
                if (idxTable != null) {
                    String idxTableSchema = null;
                    String idxTableName = idxTable;
                    if (idxTable.contains(".")) {
                        idxTableSchema = idxTable.substring(0, idxTable.indexOf('.')).toLowerCase();
                        idxTableName = idxTable.substring(idxTable.indexOf('.') + 1);
                    }
                    if (idxTableSchema != null && idxTableSchema.equals(schemaNameRc)) {
                        foundIndexRc = true;
                    } else if (schemaRc != null && schemaRc.getTable(idxTableName) != null) {
                        foundIndexRc = true;
                    }
                }
            }
            resolvedSchemaRc = schemaNameRc;
        } else {
            String effectiveSchemaRc = executor.session != null ? executor.session.getEffectiveSchema() : "public";
            for (Map.Entry<String, Schema> entry : executor.database.getSchemas().entrySet()) {
                foundRc = entry.getValue().getTable(regclassName);
                if (foundRc != null) { resolvedSchemaRc = entry.getKey(); break; }
            }
            if (foundRc == null) {
                for (String pathSchema : executor.relationSearchPath()) {
                    if (executor.database.getSequence(pathSchema, regclassName) != null
                            || executor.database.getView(pathSchema, regclassName) != null) {
                        foundIndexRc = true;
                        resolvedSchemaRc = pathSchema;
                        break;
                    }
                }
            }
            if (foundRc == null && !foundIndexRc
                    && executor.database.hasIndex(regclassName.toLowerCase())) {
                foundIndexRc = true;
                resolvedSchemaRc = effectiveSchemaRc;
            }
            if (foundRc == null && !foundIndexRc) {
                String lowerNameRc = regclassName.toLowerCase();
                if (KNOWN_PG_CATALOG_TABLES.contains(lowerNameRc)) {
                    Table sysCatalogRc = executor.systemCatalog.resolve("pg_catalog", lowerNameRc);
                    if (sysCatalogRc != null) {
                        foundRc = sysCatalogRc;
                        resolvedSchemaRc = "pg_catalog";
                    }
                }
            }
        }
        if (foundRc == null && !foundIndexRc) return null;
        String baseName = regclassName.contains(".")
                ? regclassName.substring(regclassName.indexOf('.') + 1)
                : regclassName;
        boolean schemaInSearchPath = resolvedSchemaRc == null || resolvedSchemaRc.equalsIgnoreCase("public");
        if (!schemaInSearchPath && executor.session != null) {
            String searchPathVal = executor.session.getGucSettings().get("search_path");
            if (searchPathVal != null) {
                for (String sp : searchPathVal.split(",")) {
                    String s = sp.trim().replace("\"", "").replace("'", "");
                    if (s.equalsIgnoreCase(resolvedSchemaRc)) {
                        schemaInSearchPath = true;
                        break;
                    }
                }
            }
        }
        if (resolvedSchemaRc != null && !schemaInSearchPath) {
            return resolvedSchemaRc + "." + baseName;
        }
        return baseName;
    }

    private Object evalToRegtype(FunctionCallExpr fn, RowContext ctx) {
        if (fn.args().isEmpty()) return null;
        Object arg = executor.evalExpr(fn.args().get(0), ctx);
        if (arg == null) return null;
        String written = arg.toString().trim().toLowerCase();
        String canonical = canonicalTypeName(executor.database, written);
        if (canonical == null) return null;
        // to_regtype answers a regtype, not the name of one: a caller writing
        // to_regtype('text[]')::oid is asking the value for its OID, and text cannot answer.
        return new RegtypeValue(typeOidOfName(written), canonical);
    }

    /** The OID of a type named as written, or 0 when nothing here carries one. */
    private int typeOidOfName(String typeName) {
        String bare = typeName;
        boolean array = bare.endsWith("[]");
        if (array) bare = bare.substring(0, bare.length() - 2).trim();
        DataType dt = DataType.fromPgName(bare);
        if (dt != null) {
            DataType resolved = array ? DataType.arrayOf(dt) : dt;
            if (resolved != null) return resolved.getOid();
        }
        int custom = executor.systemCatalog.getOid("type:" + bare);
        return array ? 0 : custom;
    }

    /**
     * The name PostgreSQL prints for a type spelled {@code typeName}, or null when no such type
     * exists. Shared with the privilege functions, which have to tell a real type from a typo.
     */
    static String canonicalTypeName(Database database, String typeName) {
        String canonical;
        // An array type is named after its element type, so it is the element that has to be
        // recognised: without this 'text[]' and 'bit[]' were no types at all.
        if (typeName.endsWith("[]")) {
            String element = canonicalTypeName(database,
                    typeName.substring(0, typeName.length() - 2).trim());
            return element == null ? null : element + "[]";
        }
        switch (typeName) {
            case "int4":
            case "integer":
            case "int":
                canonical = "integer";
                break;
            case "int8":
            case "bigint":
                canonical = "bigint";
                break;
            case "int2":
            case "smallint":
                canonical = "smallint";
                break;
            case "float4":
            case "real":
                canonical = "real";
                break;
            case "float8":
            case "double precision":
                canonical = "double precision";
                break;
            case "bool":
            case "boolean":
                canonical = "boolean";
                break;
            case "varchar":
            case "character varying":
                canonical = "character varying";
                break;
            case "char":
            case "character":
                canonical = "character";
                break;
            case "text":
                canonical = "text";
                break;
            case "numeric":
            case "decimal":
                canonical = "numeric";
                break;
            case "date":
                canonical = "date";
                break;
            case "timestamp":
            case "timestamp without time zone":
                canonical = "timestamp without time zone";
                break;
            case "timestamptz":
            case "timestamp with time zone":
                canonical = "timestamp with time zone";
                break;
            case "time":
            case "time without time zone":
                canonical = "time without time zone";
                break;
            case "timetz":
            case "time with time zone":
                canonical = "time with time zone";
                break;
            case "interval":
                canonical = "interval";
                break;
            case "json":
                canonical = "json";
                break;
            case "jsonb":
                canonical = "jsonb";
                break;
            case "uuid":
                canonical = "uuid";
                break;
            case "bytea":
                canonical = "bytea";
                break;
            case "inet":
                canonical = "inet";
                break;
            case "cidr":
                canonical = "cidr";
                break;
            case "macaddr":
                canonical = "macaddr";
                break;
            case "macaddr8":
                canonical = "macaddr8";
                break;
            case "xml":
                canonical = "xml";
                break;
            case "oid":
                canonical = "oid";
                break;
            case "name":
                canonical = "name";
                break;
            case "regclass":
                canonical = "regclass";
                break;
            case "regtype":
                canonical = "regtype";
                break;
            case "regproc":
                canonical = "regproc";
                break;
            case "regprocedure":
                canonical = "regprocedure";
                break;
            case "serial":
                canonical = "integer";
                break;
            case "bigserial":
                canonical = "bigint";
                break;
            case "bit":
                canonical = "bit";
                break;
            case "varbit":
            case "bit varying":
                canonical = "bit varying";
                break;
            case "point":
                canonical = "point";
                break;
            case "line":
                canonical = "line";
                break;
            case "lseg":
                canonical = "lseg";
                break;
            case "box":
                canonical = "box";
                break;
            case "path":
                canonical = "path";
                break;
            case "polygon":
                canonical = "polygon";
                break;
            case "circle":
                canonical = "circle";
                break;
            case "tsvector":
                canonical = "tsvector";
                break;
            case "tsquery":
                canonical = "tsquery";
                break;
            default:
                canonical = null;
                break;
        }
        if (canonical == null) {
            if (database.getCustomEnum(typeName) != null) canonical = typeName;
            else if (database.isDomain(typeName)) canonical = typeName;
        }
        return canonical;
    }

    // ========================================================================
    // Names written for the reg* types
    //
    // to_regproc, to_regprocedure and the two casts all read the same name and
    // differ only in what they do with the outcome: the casts raise where the
    // functions answer with nothing. So the reading is done once, here.
    // ========================================================================

    /** The function a bare or qualified name names, for the callers that want the definition. */
    private PgFunction resolveFunctionByName(String name) {
        int dot = name.indexOf('.');
        if (dot < 0) return executor.database.getFunction(name.toLowerCase(Locale.ROOT));
        String schema = name.substring(0, dot).toLowerCase(Locale.ROOT);
        String bare = name.substring(dot + 1).toLowerCase(Locale.ROOT);
        PgFunction found = executor.database.getFunction(schema, bare);
        return found != null ? found : executor.database.getFunction(bare);
    }

    /** A function name as a reg* type reads it: a schema, a name, and the signature if written. */
    private static final class ProcSpec {
        final String schema;
        final String name;
        /** Null where no parenthesis was written, so the name stands for every signature. */
        final List<String> argTypes;

        ProcSpec(String schema, String name, List<String> argTypes) {
            this.schema = schema;
            this.name = name;
            this.argTypes = argTypes;
        }
    }

    /** What a reg* name resolved to, or that several functions answer to it. */
    static final class ProcLookup {
        final int oid;
        final String display;
        final boolean ambiguous;

        private ProcLookup(int oid, String display, boolean ambiguous) {
            this.oid = oid;
            this.display = display;
            this.ambiguous = ambiguous;
        }

        static ProcLookup found(int oid, String display) { return new ProcLookup(oid, display, false); }

        static ProcLookup ambiguous() { return new ProcLookup(0, null, true); }
    }

    /** One function the catalog holds, whatever it was declared by. */
    private static final class ProcCandidate {
        final String schema;
        final String name;
        final List<String> argTypes;
        final int oid;

        ProcCandidate(String schema, String name, List<String> argTypes, int oid) {
            this.schema = schema;
            this.name = name;
            this.argTypes = argTypes;
            this.oid = oid;
        }
    }

    /**
     * The function a reg* name names, null when nothing does, or an ambiguous lookup when a bare
     * name is answered by more than one function in the first schema of the path that has it.
     *
     * @param requireSignature true for regprocedure, which is a signature and not a bare name
     */
    static ProcLookup lookupProc(AstExecutor executor, String written, boolean requireSignature) {
        ProcSpec spec = parseProcSpec(written);
        if (spec == null) return null;
        if (requireSignature && spec.argTypes == null) return null;
        if (!requireSignature && spec.argTypes != null) return null;

        for (String schema : candidateSchemas(executor, spec)) {
            List<ProcCandidate> matches = new ArrayList<ProcCandidate>();
            for (ProcCandidate candidate : proceduresIn(executor, schema, spec.name)) {
                if (spec.argTypes == null || spec.argTypes.equals(candidate.argTypes)) {
                    matches.add(candidate);
                }
            }
            if (matches.isEmpty()) continue;
            if (matches.size() > 1 && spec.argTypes == null) return ProcLookup.ambiguous();
            ProcCandidate match = matches.get(0);
            StringBuilder display = new StringBuilder();
            if (!schemaVisibleOnPath(executor, match.schema)) display.append(match.schema).append('.');
            display.append(match.name);
            if (requireSignature) {
                display.append('(');
                for (int i = 0; i < match.argTypes.size(); i++) {
                    if (i > 0) display.append(',');
                    display.append(match.argTypes.get(i));
                }
                display.append(')');
            }
            return ProcLookup.found(match.oid, display.toString());
        }
        return spec.argTypes == null ? lookupInPgProc(executor, spec) : null;
    }

    /**
     * The catalog's own row for a name nothing here carries a signature for -- the type input and
     * output functions, and an aggregate a query defined -- found by the name pg_proc holds.
     *
     * <p>Only a name the search path did not answer reaches this, and only where the database
     * holds no function of that name at all: a function it does hold has a schema of its own, so
     * one in a schema off the path has to stay unfound rather than be picked up here. A schema
     * written in front of the name still has to be the one the row is in.
     */
    private static ProcLookup lookupInPgProc(AstExecutor executor, ProcSpec spec) {
        if (!executor.database.getFunctionOverloads(spec.name).isEmpty()) return null;
        int wantedNamespace = 0;
        if (spec.schema != null) {
            wantedNamespace = namespaceOid(executor, spec.schema);
            if (wantedNamespace == 0) return null;
        }
        Table procs = executor.systemCatalog.resolve("pg_catalog", "pg_proc", executor.session);
        if (procs == null) return null;
        int oidIdx = procs.getColumnIndex("oid");
        int nameIdx = procs.getColumnIndex("proname");
        int nsIdx = procs.getColumnIndex("pronamespace");
        if (oidIdx < 0 || nameIdx < 0) return null;
        int oid = 0;
        int seen = 0;
        for (Object[] row : procs.getRows()) {
            Object rowName = nameIdx < row.length ? row[nameIdx] : null;
            if (rowName == null || !spec.name.equalsIgnoreCase(rowName.toString())) continue;
            if (wantedNamespace != 0) {
                Object ns = nsIdx >= 0 && nsIdx < row.length ? row[nsIdx] : null;
                if (!(ns instanceof Number) || ((Number) ns).intValue() != wantedNamespace) continue;
            }
            seen++;
            if (seen == 1 && oidIdx < row.length && row[oidIdx] instanceof Number) {
                oid = ((Number) row[oidIdx]).intValue();
            }
        }
        if (seen == 0) return null;
        String display = spec.schema != null && !schemaVisibleOnPath(executor, spec.schema)
                ? spec.schema + "." + spec.name : spec.name;
        return seen > 1 ? ProcLookup.ambiguous() : ProcLookup.found(oid, display);
    }

    /** The OID of a schema by name, or 0 when the database has no schema of that name. */
    private static int namespaceOid(AstExecutor executor, String schema) {
        Table namespaces = executor.systemCatalog.resolve("pg_catalog", "pg_namespace", executor.session);
        if (namespaces == null) return 0;
        int oidIdx = namespaces.getColumnIndex("oid");
        int nameIdx = namespaces.getColumnIndex("nspname");
        if (oidIdx < 0 || nameIdx < 0) return 0;
        for (Object[] row : namespaces.getRows()) {
            Object rowName = nameIdx < row.length ? row[nameIdx] : null;
            if (rowName == null || !schema.equalsIgnoreCase(rowName.toString())) continue;
            Object oid = oidIdx < row.length ? row[oidIdx] : null;
            if (oid instanceof Number) return ((Number) oid).intValue();
        }
        return 0;
    }

    /** Splits a written name into its schema, its name and the argument types it carries. */
    private static ProcSpec parseProcSpec(String written) {
        if (written == null) return null;
        String text = written.trim();
        if (text.isEmpty()) return null;
        List<String> args = null;
        int open = text.indexOf('(');
        if (open >= 0) {
            if (!text.endsWith(")")) return null;
            String inside = text.substring(open + 1, text.length() - 1).trim();
            args = new ArrayList<String>();
            if (!inside.isEmpty()) {
                for (String piece : inside.split(",")) {
                    String typeName = procTypeDisplay(piece);
                    if (typeName.isEmpty()) return null;
                    args.add(typeName);
                }
            }
            text = text.substring(0, open).trim();
        }
        int dot = lastDotOutsideQuotes(text);
        String schema = dot < 0 ? null : unquoteIdent(text.substring(0, dot));
        String name = unquoteIdent(dot < 0 ? text : text.substring(dot + 1));
        if (name.isEmpty()) return null;
        return new ProcSpec(schema, name, args);
    }

    private static int lastDotOutsideQuotes(String text) {
        boolean quoted = false;
        int found = -1;
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (ch == '"') quoted = !quoted;
            else if (ch == '.' && !quoted) found = i;
        }
        return found;
    }

    private static String unquoteIdent(String written) {
        String text = written.trim();
        if (text.length() >= 2 && text.startsWith("\"") && text.endsWith("\"")) {
            return text.substring(1, text.length() - 1).replace("\"\"", "\"");
        }
        return text.toLowerCase(Locale.ROOT);
    }

    /** The name PostgreSQL prints for a type, whichever of its spellings was written. */
    private static String procTypeDisplay(String written) {
        String text = written.trim();
        if (text.isEmpty()) return "";
        boolean array = text.endsWith("[]");
        String base = array ? text.substring(0, text.length() - 2).trim() : text;
        DataType type = DataType.fromPgName(base);
        String name = type != null ? CatalogHelper.pgTypeName(type) : base.toLowerCase(Locale.ROOT);
        return array ? name + "[]" : name;
    }

    private static String procTypeDisplayForOid(String oid) {
        DataType type = DataType.fromOid(Integer.parseInt(oid.trim()));
        return type == null ? "" : CatalogHelper.pgTypeName(type);
    }

    /** The schemas a name is looked for in: the one written, or the whole search path. */
    private static List<String> candidateSchemas(AstExecutor executor, ProcSpec spec) {
        if (spec.schema != null) return Cols.listOf(spec.schema);
        if (executor.session != null) return executor.session.getEffectiveSearchPath(true);
        return Cols.listOf("pg_catalog", "public");
    }

    private static boolean schemaVisibleOnPath(AstExecutor executor, String schema) {
        if (executor.session == null) return "pg_catalog".equals(schema) || "public".equals(schema);
        for (String onPath : executor.session.getEffectiveSearchPath(true)) {
            if (onPath.equalsIgnoreCase(schema)) return true;
        }
        return false;
    }

    /** Every function of this name in one schema: the database's own, and the built-in ones. */
    private static List<ProcCandidate> proceduresIn(AstExecutor executor, String schema, String name) {
        List<ProcCandidate> found = new ArrayList<ProcCandidate>();
        List<PgFunction> overloads = executor.database.getFunctionOverloads(name);
        for (int i = 0; i < overloads.size(); i++) {
            PgFunction fn = overloads.get(i);
            String fnSchema = fn.getSchemaName() == null ? "public" : fn.getSchemaName();
            if (!fnSchema.equalsIgnoreCase(schema)) continue;
            List<String> argTypes = new ArrayList<String>();
            if (fn.getParams() != null) {
                for (PgFunction.Param p : fn.getParams()) {
                    String mode = p.mode() == null ? "IN" : p.mode().toUpperCase(Locale.ROOT);
                    if (mode.equals("OUT") || mode.equals("TABLE")) continue;
                    argTypes.add(procTypeDisplay(p.typeName()));
                }
            }
            String key = i == 0 ? "proc:" + fn.getName() : "proc:" + fn.getName() + "#" + i;
            found.add(new ProcCandidate(fnSchema, fn.getName(), argTypes,
                    executor.systemCatalog.getOid(key)));
        }
        if (!"pg_catalog".equalsIgnoreCase(schema)) return found;

        // A few built-ins are registered as functions of their own as well, so that ALTER
        // FUNCTION reaches them; the signature table describes those same functions and must not
        // count them a second time -- one function twice over is not two functions.
        for (String[] signature : BuiltinFunctionSignatures.SIGNATURES) {
            if (!signature[0].equalsIgnoreCase(name)) continue;
            addUnlessPresent(found, builtinCandidate(executor, signature[0], signature[2]));
        }
        for (String[] aggregate : BuiltinAggregateSignatures.AGGREGATES) {
            if (!aggregate[0].equalsIgnoreCase(name)) continue;
            addUnlessPresent(found, builtinCandidate(executor, aggregate[0], aggregate[2]));
        }
        return found;
    }

    private static void addUnlessPresent(List<ProcCandidate> found, ProcCandidate candidate) {
        for (ProcCandidate seen : found) {
            if (seen.name.equalsIgnoreCase(candidate.name) && seen.argTypes.equals(candidate.argTypes)) {
                return;
            }
        }
        found.add(candidate);
    }

    private static ProcCandidate builtinCandidate(AstExecutor executor, String name, String argOids) {
        List<String> argTypes = new ArrayList<String>();
        String trimmed = argOids == null ? "" : argOids.trim();
        if (!trimmed.isEmpty()) {
            for (String oid : trimmed.split(" ")) argTypes.add(procTypeDisplayForOid(oid));
        }
        String bare = name.toLowerCase(Locale.ROOT);
        return new ProcCandidate("pg_catalog", bare, argTypes,
                builtinProcOid(executor, bare, trimmed));
    }

    /**
     * The OID pg_proc actually carries for one overload of a built-in.
     *
     * <p>An OID minted from the name alone is the same number for every overload of that name, so
     * a signature that names one of them resolved to whichever overload was registered first:
     * {@code lpad(text,integer,text)} came back as the two-argument lpad and
     * {@code generate_series(integer,integer)} as the numeric one, because pg_proc mints
     * {@code proc:lpad} and {@code proc:generate_series} for the first row of each name and a
     * distinct key for every row after it. The catalog is where the overloads are told apart, so
     * the row is found there — by the name and the argument types together, which is exactly what
     * a regprocedure signature names — and the minted OID is left as the answer only for a name
     * pg_proc has no row of that shape for.
     */
    private static int builtinProcOid(AstExecutor executor, String name, String argOids) {
        Table procs = executor.systemCatalog.resolve("pg_catalog", "pg_proc", executor.session);
        if (procs != null) {
            int oidIdx = procs.getColumnIndex("oid");
            int nameIdx = procs.getColumnIndex("proname");
            int argsIdx = procs.getColumnIndex("proargtypes");
            int nsIdx = procs.getColumnIndex("pronamespace");
            int pgCatalog = namespaceOid(executor, "pg_catalog");
            if (oidIdx >= 0 && nameIdx >= 0 && argsIdx >= 0) {
                for (Object[] row : procs.getRows()) {
                    Object rowName = nameIdx < row.length ? row[nameIdx] : null;
                    if (rowName == null || !name.equalsIgnoreCase(rowName.toString())) continue;
                    if (pgCatalog != 0 && nsIdx >= 0 && nsIdx < row.length) {
                        Object ns = row[nsIdx];
                        if (!(ns instanceof Number) || ((Number) ns).intValue() != pgCatalog) continue;
                    }
                    Object args = argsIdx < row.length ? row[argsIdx] : null;
                    String have = args == null ? "" : args.toString().trim();
                    if (!have.equals(argOids)) continue;
                    Object oid = oidIdx < row.length ? row[oidIdx] : null;
                    if (oid instanceof Number) return ((Number) oid).intValue();
                }
            }
        }
        return executor.systemCatalog.getOid("proc:" + name);
    }

    // ---- Helper methods ----

    private static String fkActionToSql(StoredConstraint.FkAction action) {
        switch (action) {
            case CASCADE:
                return "CASCADE";
            case SET_NULL:
                return "SET NULL";
            case SET_DEFAULT:
                return "SET DEFAULT";
            case RESTRICT:
                return "RESTRICT";
            case NO_ACTION:
                return "NO ACTION";
            default:
                throw new IllegalStateException("Unknown FK action: " + action);
        }
    }

    /**
     * NOT NULL constraint name for a column; partition children inherit the name
     * from the ancestor that first declared the column NOT NULL (L13).
     */
    private static String notNullConstraintName(Table t, Column col) {
        Table owner = t;
        Table parent = t.getPartitionParent();
        while (parent != null) {
            int idx = parent.getColumnIndex(col.getName());
            if (idx < 0 || parent.getColumns().get(idx).isNullable()) break;
            owner = parent;
            parent = parent.getPartitionParent();
        }
        String named = owner.notNullConstraintName(col.getName());
        return named != null ? named : owner.getName() + "_" + col.getName() + "_not_null";
    }

    /** Strip a single pair of balanced outer parentheses, if present. */
    private static String stripOuterParens(String s) {
        if (s == null) return "";
        String t = s.trim();
        while (t.startsWith("(") && t.endsWith(")")) {
            int depth = 0;
            boolean wraps = true;
            for (int i = 0; i < t.length(); i++) {
                char ch = t.charAt(i);
                if (ch == '(') depth++;
                else if (ch == ')') { depth--; if (depth == 0 && i < t.length() - 1) { wraps = false; break; } }
            }
            if (wraps) t = t.substring(1, t.length() - 1).trim();
            else break;
        }
        return t;
    }

    /**
     * True when the referenced table can be written without its schema — that is, when looking
     * the bare name up along the current search path finds that very table.
     *
     * <p>Being in the same schema as the constraint is not enough. PostgreSQL's deparser asks
     * only what the reader's search path would resolve the name to, so a foreign key inside a
     * schema nobody has on their path prints {@code REFERENCES q.par(id)} even though the
     * constraint and the table it names sit side by side. Treating a shared schema as visible
     * printed a definition that, pasted back in, would build the constraint against a different
     * table or fail outright.
     */
    private boolean schemaVisibleForRef(String refSchema, String refTable) {
        if (refSchema == null) return true;
        if (executor.session == null) return "public".equalsIgnoreCase(refSchema);
        for (String p : executor.session.getEffectiveSearchPath(false)) {
            Schema onPath = executor.database.getSchema(p);
            if (onPath == null) continue;
            // A schema earlier on the path that holds no table of this name hides nothing
            if (refTable != null && onPath.getTable(refTable) == null) continue;
            return refSchema.equalsIgnoreCase(p);
        }
        return false;
    }

    private String formatConstraintDef(StoredConstraint sc, String ownSchema, Table owner) {
        switch (sc.getType()) {
            case PRIMARY_KEY:
                return "PRIMARY KEY (" + String.join(", ", sc.getColumns()) + ")";
            case UNIQUE:
                // NULLS NOT DISTINCT is part of what the constraint says, so the definition it
                // reports back has to say it too.
                return "UNIQUE " + (sc.isNullsNotDistinct() ? "NULLS NOT DISTINCT " : "")
                        + "(" + String.join(", ", sc.getColumns()) + ")";
            case CHECK:
                return "CHECK (" + RuleDeparser.deparse(sc.getCheckExpr(), RuleDeparser.forTable(owner)) + ")";
            case FOREIGN_KEY: {
                StringBuilder sb = new StringBuilder("FOREIGN KEY (");
                sb.append(String.join(", ", sc.getColumns()));
                sb.append(") REFERENCES ");
                if (sc.getReferencesTable() != null) {
                    // H16: only schema-qualify the referenced table when its schema is
                    // NOT visible via the current search_path (matching PG).
                    if (!schemaVisibleForRef(sc.getReferencesSchema(), sc.getReferencesTable())) {
                        sb.append(sc.getReferencesSchema()).append(".");
                    }
                    sb.append(sc.getReferencesTable());
                }
                if (sc.getReferencesColumns() != null && !sc.getReferencesColumns().isEmpty()) {
                    sb.append("(").append(String.join(", ", sc.getReferencesColumns())).append(")");
                }
                if (sc.getOnUpdate() != null && sc.getOnUpdate() != StoredConstraint.FkAction.NO_ACTION) {
                    sb.append(" ON UPDATE ").append(fkActionToSql(sc.getOnUpdate()));
                }
                if (sc.getOnDelete() != null && sc.getOnDelete() != StoredConstraint.FkAction.NO_ACTION) {
                    sb.append(" ON DELETE ").append(fkActionToSql(sc.getOnDelete()));
                }
                return sb.toString();
            }
            case EXCLUDE: {
                // H16: reflect the actual backing index access method rather than
                // always printing gist.
                String am = executor.database.getIndexMethod(sc.getName());
                if (am == null || am.isEmpty()) am = "btree";
                StringBuilder sb = new StringBuilder("EXCLUDE USING ").append(am).append(" (");
                if (sc.getExcludeElements() != null) {
                    for (int i = 0; i < sc.getExcludeElements().size(); i++) {
                        if (i > 0) sb.append(", ");
                        StoredConstraint.ExcludeElement e = sc.getExcludeElements().get(i);
                        sb.append(e.column()).append(" WITH ").append(e.operator());
                    }
                }
                sb.append(")");
                return sb.toString();
            }
            default:
                throw new IllegalStateException("Unknown constraint type: " + sc.getType());
        }
    }

    private static final java.util.Map<Integer, String> EXTRA_TYPE_NAMES;
    static {
        java.util.Map<Integer, String> m = new java.util.HashMap<>();
        m.put(1033, "aclitem");
        m.put(1034, "aclitem[]");
        m.put(2249, "record");
        m.put(2287, "record[]");
        m.put(2278, "void");
        m.put(2276, "any");
        for (String polyName : PolymorphicTypes.names()) {
            m.put(PolymorphicTypes.oid(polyName), polyName);
        }
        m.put(22, "int2vector");
        m.put(30, "oidvector");
        m.put(18, "\"char\"");
        m.put(1002, "\"char\"[]");
        // The pseudo-types a routine's signature is declared over. A catalog function's argument
        // or result is one of these far more often than it is a table column's type — an input
        // function reads a cstring, a receive function is handed internal, a trigger function
        // answers trigger — so a renderer that does not know them prints "unknown" over most of
        // pg_proc. They carry no modifier, so each is simply its own name.
        m.put(2275, "cstring");
        m.put(2276, "\"any\"");        // reserved word: PG's format_type quotes it
        m.put(2279, "trigger");
        m.put(2281, "internal");
        m.put(3838, "event_trigger");
        m.put(2280, "language_handler");
        m.put(3115, "fdw_handler");
        m.put(325, "index_am_handler");
        m.put(269, "table_am_handler");
        m.put(3310, "tsm_handler");
        m.put(32, "pg_ddl_command");
        // The bootstrap types the catalogs are built out of, and their arrays. unknown is the
        // type an unadorned literal still has; refcursor is what a PL/pgSQL cursor variable is;
        // gtsvector is the GiST index representation tsvector's opclass stores.
        m.put(705, "unknown");
        m.put(1790, "refcursor");
        m.put(2201, "refcursor[]");
        m.put(3642, "gtsvector");
        m.put(3644, "gtsvector[]");
        // A typmod input function is handed the modifier's words as cstring[], so every
        // xxxtypmodin row in pg_proc names this type and nothing else does.
        m.put(1263, "cstring[]");
        EXTRA_TYPE_NAMES = java.util.Collections.unmodifiableMap(m);
    }

    /**
     * The OID a format_type argument names. A regtype value carries a type name rather than a
     * number — {@code 'varchar[]'::regtype} is the array type, not a number to be parsed — so the
     * name is looked up where one was written.
     */
    private int typeOidOf(Object value) {
        if (value instanceof Number) return ((Number) value).intValue();
        String text = value.toString().trim();
        try {
            return Integer.parseInt(text);
        } catch (NumberFormatException ignored) {
            // a type name, resolved below
        }
        boolean array = text.endsWith("[]");
        String bare = array ? text.substring(0, text.length() - 2).trim() : text;
        DataType dt = DataType.fromPgName(bare);
        if (dt != null) {
            DataType resolved = array ? DataType.arrayOf(dt) : dt;
            if (resolved != null) return resolved.getOid();
        }
        String userKey = TypeNamespace.resolve(executor.database, executor.session, bare);
        if (userKey != null) return executor.systemCatalog.getOid("type:" + userKey);
        return 0;
    }

    private String formatTypeByOid(int oid, int typmod) {
        // No type at all: PG prints a dash rather than naming whatever sits at OID zero.
        if (oid == 0) return "-";
        // PG quotes "char" so it is not read as the SQL type char; format_type is where a client
        // learns a catalog flag column is the single-byte type and not a bpchar.
        if (oid == 18) return "\"char\"";
        // An array is named after its element with the modifier applied to that element, so the
        // array's own row is answered by formatting the element and adding the brackets.
        DataType arrayType = null;
        for (DataType dt : DataType.values()) {
            if (dt.getOid() == oid && dt.getPgName().startsWith("_")) { arrayType = dt; break; }
        }
        if (arrayType != null) {
            DataType element = DataType.elementOf(arrayType);
            if (element != null) return CatalogHelper.formatType(element, null, typmod) + "[]";
        }
        for (DataType dt : DataType.values()) {
            if (dt.getOid() == oid) {
                switch (dt) {
                    case VARCHAR: case CHAR: case NUMERIC: case TIMESTAMP: case TIMESTAMPTZ:
                    case TIME: case TIMETZ: case INTERVAL: case BIT: case VARBIT:
                        return CatalogHelper.formatType(dt, null, typmod);
                    default:
                        break;
                }
                String base;
                switch (dt) {
                    case INTEGER:
                        base = "integer";
                        break;
                    case BIGINT:
                        base = "bigint";
                        break;
                    case SMALLINT:
                        base = "smallint";
                        break;
                    case TEXT:
                        base = "text";
                        break;
                    case VARCHAR: {
                        if (typmod > 0) base = "character varying(" + (typmod - 4) + ")";
                        else base = "character varying";
                        break;
                    }
                    case CHAR: {
                        if (typmod > 0) base = "character(" + (typmod - 4) + ")";
                        else base = "character";
                        break;
                    }
                    case BOOLEAN:
                        base = "boolean";
                        break;
                    case REAL:
                        base = "real";
                        break;
                    case DOUBLE_PRECISION:
                        base = "double precision";
                        break;
                    case NUMERIC: {
                        if (typmod > 0) {
                            int raw = typmod - 4;
                            int precision = (raw >> 16) & 0xFFFF;
                            int scale = raw & 0xFFFF;
                            base = "numeric(" + precision + "," + scale + ")";
                            break;
                        }
                        base = "numeric";
                        break;
                    }
                    case DATE:
                        base = "date";
                        break;
                    case TIMESTAMP:
                        base = "timestamp without time zone";
                        break;
                    case TIMESTAMPTZ:
                        base = "timestamp with time zone";
                        break;
                    case TIME:
                        base = "time without time zone";
                        break;
                    case INTERVAL:
                        base = "interval";
                        break;
                    case UUID:
                        base = "uuid";
                        break;
                    case BYTEA:
                        base = "bytea";
                        break;
                    case JSON:
                        base = "json";
                        break;
                    case JSONB:
                        base = "jsonb";
                        break;
                    case ACLITEM_ARRAY:
                        base = "aclitem[]";
                        break;
                    case TEXT_ARRAY:
                        base = "text[]";
                        break;
                    case INT4_ARRAY:
                        base = "integer[]";
                        break;
                    default:
                        // An array type is named "_elem" in pg_type but printed "elem[]": a
                        // client reading format_type sees a type name it could write in SQL.
                        DataType elem = DataType.elementOf(dt);
                        base = elem != null ? formatTypeByOid(elem.getOid(), -1) + "[]"
                                : dt.getPgName();
                        break;
                }
                return CatalogHelper.withPlainTypmod(dt, base, typmod);
            }
        }
        // A user-defined type prints under the name this session would write for it: bare where
        // the search path finds it, qualified where it would not.
        for (String typeKey : executor.database.typeKeys()) {
            if (executor.systemCatalog.getOid("type:" + typeKey) == oid) {
                return TypeNamespace.display(executor.database, executor.session, typeKey);
            }
        }
        String extra = EXTRA_TYPE_NAMES.get(oid);
        if (extra != null) return extra;
        String bootstrap = PgInternalTypes.nameForOid(oid);
        if (bootstrap != null) return bootstrap;
        String cataloged = catalogTypeName(oid, new java.util.HashSet<Integer>());
        if (cataloged != null) return cataloged;
        // An OID with no type behind it. PG prints "???" — deliberately not a name, so a caller
        // cannot mistake it for one. "unknown" was a real type's name and read as an answer.
        return "???";
    }

    /**
     * The name pg_type itself records for an OID, for the types no {@link DataType} models.
     *
     * <p>Every type memgres registers has a pg_type row, so the row is the one description that
     * cannot fall behind: a bootstrap type added to the catalog, a relation's composite row type
     * and the array beside it all name themselves there. Reading it is what keeps format_type
     * answering for types this class was never told about, instead of calling them "unknown".
     *
     * <p>An array is named after its element with the brackets added, the way PostgreSQL's own
     * format_type does it — and by the same test it uses: a type is an array when it points at an
     * element type and is not stored plain, which is why oidvector and int2vector, which do point
     * at an element, still print under their own names.
     *
     * @param seen the array types already followed, so a typelem cycle cannot recurse forever
     */
    private String catalogTypeName(int oid, Set<Integer> seen) {
        if (oid == 0) return null;
        Table types = executor.systemCatalog.resolve("pg_catalog", "pg_type", executor.session);
        if (types == null) return null;
        int oidAt = types.getColumnIndex("oid");
        int nameAt = types.getColumnIndex("typname");
        if (oidAt < 0 || nameAt < 0) return null;
        int elemAt = types.getColumnIndex("typelem");
        int storageAt = types.getColumnIndex("typstorage");
        for (Object[] row : types.getRows()) {
            Object o = oidAt < row.length ? row[oidAt] : null;
            if (!(o instanceof Number) || ((Number) o).intValue() != oid) continue;
            Object nm = nameAt < row.length ? row[nameAt] : null;
            if (nm == null) return null;
            Object elem = elemAt >= 0 && elemAt < row.length ? row[elemAt] : null;
            Object storage = storageAt >= 0 && storageAt < row.length ? row[storageAt] : null;
            int elemOid = elem instanceof Number ? ((Number) elem).intValue() : 0;
            boolean plain = storage != null && "p".equals(storage.toString());
            if (elemOid != 0 && !plain && seen.add(oid)) {
                String element = EXTRA_TYPE_NAMES.get(elemOid);
                if (element == null) element = PgInternalTypes.nameForOid(elemOid);
                if (element == null) {
                    DataType dt = dataTypeOf(elemOid);
                    if (dt != null) element = CatalogHelper.formatType(dt, null, -1);
                }
                if (element == null) element = catalogTypeName(elemOid, seen);
                if (element != null) return element + "[]";
            }
            return nm.toString();
        }
        return null;
    }

    /** The {@link DataType} an OID names, or null when no enum constant carries it. */
    private static DataType dataTypeOf(int oid) {
        for (DataType dt : DataType.values()) {
            if (dt.getOid() == oid && !dt.getPgName().startsWith("_")) return dt;
        }
        return null;
    }

    private static String buildFunctionDef(PgFunction func) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE OR REPLACE ");
        sb.append(func.isProcedure() ? "PROCEDURE " : "FUNCTION ");
        String funcSchema = func.getSchemaName() != null ? func.getSchemaName() : "public";
        sb.append(funcSchema).append(".").append(func.getName()).append("(");
        sb.append(buildFunctionArguments(func));
        sb.append(")\n");
        if (!func.isProcedure()) {
            sb.append(" RETURNS ").append(normalizePgTypeName(func.getReturnType())).append("\n");
        }
        sb.append(" LANGUAGE ").append(func.getLanguage()).append("\n");
        sb.append("AS $function$").append(func.getBody()).append("$function$\n");
        return sb.toString();
    }

    private static String buildFunctionArguments(PgFunction func) {
        if (func.getParams() == null || func.getParams().isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < func.getParams().size(); i++) {
            if (i > 0) sb.append(", ");
            PgFunction.Param p = func.getParams().get(i);
            if (p.mode() != null && !p.mode().equalsIgnoreCase("IN")) {
                sb.append(p.mode().toUpperCase()).append(" ");
            }
            if (p.name() != null && !p.name().isEmpty()) {
                sb.append(p.name()).append(" ");
            }
            sb.append(normalizePgTypeName(p.typeName()));
            if (p.defaultExpr() != null) {
                sb.append(" DEFAULT ").append(p.defaultExpr());
            }
        }
        return sb.toString();
    }

    /**
     * A written type name with its modifier removed: {@code varchar(10)} is {@code varchar},
     * {@code numeric(5,2)} is {@code numeric}, {@code interval day to second(2)} is
     * {@code interval}.
     *
     * <p>PostgreSQL records only a type OID for a function's arguments and result — pg_proc has
     * nowhere to put a typmod — so the qualifier a declaration wrote is genuinely gone by the
     * time pg_get_function_arguments is asked, and a client reading the signature back must see
     * the same bare type PostgreSQL shows it.
     */
    static String stripTypeModifier(String typeName) {
        if (typeName == null) return null;
        String s = typeName.trim();
        int open = s.indexOf('(');
        if (open >= 0) {
            int close = s.indexOf(')', open);
            s = close >= 0 ? (s.substring(0, open) + s.substring(close + 1)).trim()
                    : s.substring(0, open).trim();
        }
        if (DataType.intervalQualifier(s) != null) return "interval";
        return s;
    }

    static String normalizePgTypeName(String typeName) {
        if (typeName == null) return "void";
        typeName = stripTypeModifier(typeName);
        switch (typeName.toLowerCase().trim()) {
            case "int":
            case "int4":
                return "integer";
            case "int2":
            case "smallint":
                return "smallint";
            case "int8":
            case "bigint":
                return "bigint";
            case "float4":
            case "real":
                return "real";
            case "float8":
            case "double precision":
                return "double precision";
            case "bool":
                return "boolean";
            case "varchar":
                return "character varying";
            case "char":
                return "character";
            default:
                return typeName.toLowerCase();
        }
    }

    private PgFunction resolveFunction(Expression argExpr, RowContext ctx) {
        return resolveFunctionValue(executor.evalExpr(argExpr, ctx));
    }

    private PgFunction resolveFunctionValue(Object arg) {
        if (arg == null) return null;
        if (arg instanceof Number) {
            Number oid = (Number) arg;
            int oidVal = oid.intValue();
            Map<String, Integer> oidMap = executor.systemCatalog.getOidMap();
            for (Map.Entry<String, Integer> entry : oidMap.entrySet()) {
                if (entry.getValue() == oidVal && entry.getKey().startsWith("proc:")) {
                    String funcName = entry.getKey().substring(5);
                    PgFunction func = executor.database.getFunction(funcName);
                    if (func != null) return func;
                }
            }
        }
        String funcName = arg.toString();
        // Strip argument list if present, e.g. "cfmt.cfmt_fn(int)" -> "cfmt.cfmt_fn"
        int parenIdx = funcName.indexOf('(');
        if (parenIdx >= 0) {
            funcName = funcName.substring(0, parenIdx).trim();
        }
        return resolveFunctionByName(funcName);
    }

    private static String buildFunctionIdentityArguments(PgFunction func) {
        if (func.getParams() == null || func.getParams().isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (PgFunction.Param p : func.getParams()) {
            if ("OUT".equalsIgnoreCase(p.mode())) continue;
            if (!first) sb.append(", ");
            first = false;
            if (p.mode() != null && !"IN".equalsIgnoreCase(p.mode())) {
                sb.append(p.mode().toUpperCase()).append(" ");
            }
            if (p.name() != null && !p.name().isEmpty()) {
                sb.append(p.name()).append(" ");
            }
            sb.append(normalizePgTypeName(p.typeName()));
        }
        return sb.toString();
    }

    /**
     * The pg_proc row an OID names, or null when the argument is not an OID with a row behind it.
     *
     * <p>The three pg_get_function_* renderers are asked about built-in functions far more often
     * than about ones a session created — {@code SELECT pg_get_function_arguments(oid) FROM
     * pg_proc} is how a client lists what a server can do — and a built-in has no {@code
     * PgFunction} to read. The catalog row is where its signature is recorded, so that is what is
     * rendered; pg_proc is built once per statement and cached, so the lookup does not rebuild it
     * per row.
     */
    private Object[] procRowOf(Object arg) {
        int wanted;
        if (arg instanceof Number) wanted = ((Number) arg).intValue();
        else if (arg instanceof RegprocValue) wanted = ((RegprocValue) arg).oid();
        else return null;
        Table procs = executor.systemCatalog.resolve("pg_catalog", "pg_proc", executor.session);
        if (procs == null) return null;
        int oidIdx = procs.getColumnIndex("oid");
        if (oidIdx < 0) return null;
        for (Object[] row : procs.getRows()) {
            Object o = row[oidIdx];
            if (o instanceof Number && ((Number) o).intValue() == wanted) return row;
        }
        return null;
    }

    /**
     * The pg_proc row of a built-in, which is to say one whose namespace is pg_catalog.
     *
     * <p>A handful of built-ins are also carried as {@link PgFunction} stubs so that ALTER FUNCTION
     * can name them, and the stub knows less than the catalog row does: it invents an argument name
     * PostgreSQL does not record and its body is a comment rather than the C symbol. So for
     * anything in pg_catalog the row wins, and the stub is left to answer for what a user created.
     */
    private Object[] builtinProcRow(Object arg) {
        Object[] row = procRowOf(arg);
        if (row == null) return null;
        return "pg_catalog".equals(procNamespaceName(procCol(row, "pronamespace"))) ? row : null;
    }

    /** A pg_proc column by name, or null when the row is shorter than the catalog. */
    private Object procCol(Object[] row, String column) {
        Table procs = executor.systemCatalog.resolve("pg_catalog", "pg_proc", executor.session);
        if (procs == null) return null;
        int idx = procs.getColumnIndex(column);
        return idx < 0 || idx >= row.length ? null : row[idx];
    }

    /**
     * The argument list PostgreSQL prints for a pg_proc row: each argument as its mode, its name
     * when one is recorded, and its type — plus the defaults the tail arguments carry, which
     * {@code withDefaults} leaves off for the identity form.
     */
    private String renderProcArguments(Object[] row, boolean withDefaults) {
        List<Object> types = asList(procCol(row, "proargtypes"));
        List<Object> allTypes = asList(procCol(row, "proallargtypes"));
        List<Object> modes = asList(procCol(row, "proargmodes"));
        List<Object> names = asList(procCol(row, "proargnames"));
        if (!allTypes.isEmpty()) types = allTypes;
        Object variadicObj = procCol(row, "provariadic");
        int variadic = variadicObj instanceof Number ? ((Number) variadicObj).intValue() : 0;
        Object ndefObj = procCol(row, "pronargdefaults");
        int ndefaults = ndefObj instanceof Number ? ((Number) ndefObj).intValue() : 0;
        List<String> defaults = splitProcDefaults(procCol(row, "proargdefaults"));
        StringBuilder sb = new StringBuilder();
        int inputCount = 0;
        for (int i = 0; i < types.size(); i++) {
            String mode = i < modes.size() && modes.get(i) != null ? modes.get(i).toString() : "i";
            boolean isOut = "o".equals(mode);
            // The identity form names only the arguments a call passes, so OUT drops out of it.
            if (!withDefaults && isOut) continue;
            if (sb.length() > 0) sb.append(", ");
            if ("o".equals(mode)) sb.append("OUT ");
            else if ("b".equals(mode)) sb.append("INOUT ");
            else if ("v".equals(mode)) sb.append("VARIADIC ");
            else if (variadic != 0 && i == types.size() - 1) sb.append("VARIADIC ");
            if (i < names.size() && names.get(i) != null && !names.get(i).toString().isEmpty()) {
                sb.append(names.get(i));
                sb.append(' ');
            }
            Object t = types.get(i);
            sb.append(formatTypeByOid(t instanceof Number ? ((Number) t).intValue() : 0, -1));
            if (!isOut) {
                int defaultIdx = inputCount - (countInputArgs(types, modes) - ndefaults);
                if (withDefaults && defaultIdx >= 0 && defaultIdx < defaults.size()) {
                    sb.append(" DEFAULT ").append(defaults.get(defaultIdx));
                }
                inputCount++;
            }
        }
        return sb.toString();
    }

    /**
     * The {@code CREATE OR REPLACE FUNCTION} text PostgreSQL deparses a pg_proc row into.
     *
     * <p>Every part of it is read off the row rather than guessed, because the row is what a client
     * asking this question has already seen: the schema from pronamespace, the language from
     * prolang, the body from prosrc, and the attribute line from prokind, provolatile, proparallel,
     * proisstrict, prosecdef, proleakproof, procost and prorows in the order PostgreSQL prints
     * them. The defaults PostgreSQL leaves off are left off here too — VOLATILE, PARALLEL UNSAFE,
     * and a cost that is the language's own — so the text compares equal to PostgreSQL's for a
     * builtin whose row matches.
     *
     * <p>An aggregate has no function definition at all: PostgreSQL refuses with 42809 rather than
     * printing something a client could execute.
     */
    private String buildProcFunctionDef(Object[] row) {
        Object kind = procCol(row, "prokind");
        String prokind = kind == null ? "f" : kind.toString();
        String name = String.valueOf(procCol(row, "proname"));
        if ("a".equals(prokind)) {
            throw new MemgresException("\"" + name + "\" is an aggregate function", "42809");
        }
        boolean procedure = "p".equals(prokind);
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE OR REPLACE ").append(procedure ? "PROCEDURE " : "FUNCTION ");
        String schema = procNamespaceName(procCol(row, "pronamespace"));
        if (schema != null) sb.append(quoteProcIdent(schema)).append('.');
        sb.append(quoteProcIdent(name)).append('(');
        sb.append(renderProcArguments(row, true)).append(")\n");
        if (!procedure) {
            sb.append(" RETURNS ").append(renderProcResult(row)).append("\n");
        }
        String language = procLanguageName(procCol(row, "prolang"));
        sb.append(" LANGUAGE ").append(language == null ? "internal" : language).append("\n");
        StringBuilder attrs = new StringBuilder();
        if ("w".equals(prokind)) attrs.append(" WINDOW");
        Object volatility = procCol(row, "provolatile");
        if (volatility != null && "i".equals(volatility.toString())) attrs.append(" IMMUTABLE");
        else if (volatility != null && "s".equals(volatility.toString())) attrs.append(" STABLE");
        Object parallel = procCol(row, "proparallel");
        if (parallel != null && "s".equals(parallel.toString())) attrs.append(" PARALLEL SAFE");
        else if (parallel != null && "r".equals(parallel.toString())) attrs.append(" PARALLEL RESTRICTED");
        if (Boolean.TRUE.equals(procCol(row, "proisstrict"))) attrs.append(" STRICT");
        if (Boolean.TRUE.equals(procCol(row, "prosecdef"))) attrs.append(" SECURITY DEFINER");
        if (Boolean.TRUE.equals(procCol(row, "proleakproof"))) attrs.append(" LEAKPROOF");
        // A cost that is the language's own default is not printed, and neither is a row estimate
        // that is not a set-returning function's own.
        Object cost = procCol(row, "procost");
        if (cost instanceof Number) {
            double defaultCost = "internal".equals(language) || "c".equals(language) ? 1 : 100;
            double actual = ((Number) cost).doubleValue();
            if (actual > 0 && actual != defaultCost) attrs.append(" COST ").append(trimNumber(actual));
        }
        Object rows = procCol(row, "prorows");
        if (Boolean.TRUE.equals(procCol(row, "proretset")) && rows instanceof Number) {
            double actual = ((Number) rows).doubleValue();
            if (actual > 0 && actual != 1000) attrs.append(" ROWS ").append(trimNumber(actual));
        }
        if (attrs.length() > 0) sb.append(attrs).append("\n");
        Object src = procCol(row, "prosrc");
        sb.append("AS $function$").append(src == null ? "" : src).append("$function$\n");
        return sb.toString();
    }

    /** An identifier written the way a CREATE statement would have to write it to name it again. */
    private static String quoteProcIdent(String ident) {
        if (ident == null || ident.isEmpty()) return "\"\"";
        boolean plain = Character.isLowerCase(ident.charAt(0)) || ident.charAt(0) == '_';
        for (int i = 0; plain && i < ident.length(); i++) {
            char c = ident.charAt(i);
            plain = Character.isLowerCase(c) || Character.isDigit(c) || c == '_';
        }
        return plain ? ident : "\"" + ident.replace("\"", "\"\"") + "\"";
    }

    /** A cost or row estimate written the way PostgreSQL's %g writes it: 1 rather than 1.0. */
    private static String trimNumber(double value) {
        if (value == Math.rint(value) && !Double.isInfinite(value)) {
            return String.valueOf((long) value);
        }
        return String.valueOf(value);
    }

    /** The schema name behind a pronamespace OID, read from pg_namespace. */
    private String procNamespaceName(Object oidValue) {
        if (!(oidValue instanceof Number)) return null;
        int wanted = ((Number) oidValue).intValue();
        Table namespaces = executor.systemCatalog.resolve("pg_catalog", "pg_namespace", executor.session);
        if (namespaces == null) return null;
        int oidIdx = namespaces.getColumnIndex("oid");
        int nameIdx = namespaces.getColumnIndex("nspname");
        if (oidIdx < 0 || nameIdx < 0) return null;
        for (Object[] row : namespaces.getRows()) {
            Object o = row[oidIdx];
            if (o instanceof Number && ((Number) o).intValue() == wanted) {
                return row[nameIdx] == null ? null : row[nameIdx].toString();
            }
        }
        return null;
    }

    /** The language name behind a prolang OID, read from pg_language. */
    private String procLanguageName(Object oidValue) {
        if (!(oidValue instanceof Number)) return null;
        int wanted = ((Number) oidValue).intValue();
        Table languages = executor.systemCatalog.resolve("pg_catalog", "pg_language", executor.session);
        if (languages == null) return null;
        int oidIdx = languages.getColumnIndex("oid");
        int nameIdx = languages.getColumnIndex("lanname");
        if (oidIdx < 0 || nameIdx < 0) return null;
        for (Object[] row : languages.getRows()) {
            Object o = row[oidIdx];
            if (o instanceof Number && ((Number) o).intValue() == wanted) {
                return row[nameIdx] == null ? null : row[nameIdx].toString();
            }
        }
        return null;
    }

    /** The return type PostgreSQL prints for a pg_proc row, SETOF included. */
    private String renderProcResult(Object[] row) {
        Object rt = procCol(row, "prorettype");
        if (!(rt instanceof Number)) return "";
        Object retset = procCol(row, "proretset");
        String name = formatTypeByOid(((Number) rt).intValue(), -1);
        return Boolean.TRUE.equals(retset) ? "SETOF " + name : name;
    }

    private static int countInputArgs(List<Object> types, List<Object> modes) {
        int n = 0;
        for (int i = 0; i < types.size(); i++) {
            String mode = i < modes.size() && modes.get(i) != null ? modes.get(i).toString() : "i";
            if (!"o".equals(mode)) n++;
        }
        return n;
    }

    /** The default expressions a pg_proc row records, one per defaulted trailing argument. */
    private static List<String> splitProcDefaults(Object value) {
        if (value == null) return Cols.listOf();
        String text = value.toString().trim();
        if (text.isEmpty()) return Cols.listOf();
        List<String> out = new ArrayList<>();
        for (String part : text.split("\\|")) out.add(part.trim());
        return out;
    }

    private static List<Object> asList(Object value) {
        if (value instanceof List<?>) {
            List<Object> out = new ArrayList<>();
            for (Object o : (List<?>) value) out.add(o);
            return out;
        }
        return Cols.listOf();
    }

    private boolean isBuiltinFunction(String name) {
        switch (name.toLowerCase()) {
            case "now":
            case "current_timestamp":
            case "current_date":
            case "current_time":
            case "localtime":
            case "localtimestamp":
            case "clock_timestamp":
            case "statement_timestamp":
            case "transaction_timestamp":
            case "timeofday":
            case "age":
            case "date_trunc":
            case "date_part":
            case "extract":
            case "make_date":
            case "make_time":
            case "make_timestamp":
            case "make_timestamptz":
            case "make_interval":
            case "to_timestamp":
            case "to_date":
            case "to_char":
            case "to_number":
            case "abs":
            case "ceil":
            case "ceiling":
            case "floor":
            case "round":
            case "trunc":
            case "sign":
            case "sqrt":
            case "power":
            case "exp":
            case "ln":
            case "log":
            case "mod":
            case "div":
            case "pi":
            case "random":
            case "setseed":
            case "length":
            case "char_length":
            case "octet_length":
            case "bit_length":
            case "upper":
            case "lower":
            case "initcap":
            case "trim":
            case "ltrim":
            case "rtrim":
            case "btrim":
            case "lpad":
            case "rpad":
            case "substr":
            case "substring":
            case "left":
            case "right":
            case "reverse":
            case "replace":
            case "regexp_replace":
            case "regexp_match":
            case "regexp_matches":
            case "split_part":
            case "string_to_array":
            case "array_to_string":
            case "concat":
            case "concat_ws":
            case "format":
            case "quote_ident":
            case "quote_literal":
            case "chr":
            case "ascii":
            case "encode":
            case "decode":
            case "md5":
            case "sha256":
            case "coalesce":
            case "nullif":
            case "greatest":
            case "least":
            case "array_length":
            case "array_upper":
            case "array_lower":
            case "array_ndims":
            case "array_append":
            case "array_prepend":
            case "array_cat":
            case "array_remove":
            case "array_position":
            case "array_positions":
            case "jsonb_build_object":
            case "jsonb_build_array":
            case "jsonb_object":
            case "json_build_object":
            case "json_build_array":
            case "row_to_json":
            case "to_json":
            case "to_jsonb":
            case "jsonb_array_elements":
            case "jsonb_array_elements_text":
            case "jsonb_each":
            case "jsonb_each_text":
            case "jsonb_object_keys":
            case "jsonb_typeof":
            case "json_strip_nulls":
            case "json_populate_record":
            case "json_populate_recordset":
            case "jsonb_populate_record":
            case "jsonb_populate_recordset":
            case "jsonb_strip_nulls":
            case "jsonb_set":
            case "json_array_elements":
            case "json_each":
            case "json_object_keys":
            case "pg_typeof":
            case "pg_relation_size":
            case "pg_total_relation_size":
            case "pg_get_functiondef":
            case "pg_get_viewdef":
            case "pg_get_indexdef":
            case "pg_get_constraintdef":
            case "pg_get_triggerdef":
            case "pg_get_serial_sequence":
            case "pg_get_expr":
            case "pg_get_keywords":
            case "format_type":
            case "obj_description":
            case "col_description":
            case "has_schema_privilege":
            case "has_table_privilege":
            case "has_database_privilege":
            case "has_parameter_privilege":
            case "pg_has_role":
            case "acldefault":
            case "pg_table_is_visible":
            case "pg_function_is_visible":
            case "current_schema":
            case "current_schemas":
            case "current_user":
            case "session_user":
            case "current_database":
            case "version":
            case "pg_backend_pid":
            case "inet_server_addr":
            case "inet_server_port":
            case "gen_random_uuid":
            case "uuidv4":
            case "uuid_generate_v4":
            case "uuid_generate_v1":
            case "uuid_generate_v3":
            case "uuid_generate_v5":
            case "uuid_nil":
            case "uuid_ns_dns":
            case "uuid_ns_url":
            case "digest":
            case "hmac":
            case "gen_salt":
            case "show_trgm":
            case "similarity":
            case "levenshtein":
            case "soundex":
            case "unaccent":
            case "unicode_version":
            case "unicode_assigned":
            case "nextval":
            case "currval":
            case "setval":
            case "lastval":
            case "pg_sequence_last_value":
            case "txid_current":
            case "pg_current_xact_id":
            case "to_regclass":
            case "to_regtype":
            case "to_regproc":
            case "to_regprocedure":
            case "regclass":
            case "regtype":
            case "regproc":
            case "pg_advisory_lock":
            case "pg_advisory_unlock":
            case "pg_advisory_xact_lock":
            case "pg_advisory_xact_unlock":
            case "pg_sleep":
            case "pg_sleep_for":
            case "pg_sleep_until":
            case "unnest":
            case "generate_series":
            case "generate_subscripts":
            case "string_agg":
            case "array_agg":
            case "json_agg":
            case "jsonb_agg":
            case "count":
            case "sum":
            case "min":
            case "max":
            case "avg":
            case "row_number":
            case "rank":
            case "dense_rank":
            case "ntile":
            case "lag":
            case "lead":
            case "first_value":
            case "last_value":
            case "nth_value":
            case "bool_and":
            case "bool_or":
            case "bit_and":
            case "bit_or":
            case "bit_xor":
                return true;
            default:
                return false;
        }
    }
}
