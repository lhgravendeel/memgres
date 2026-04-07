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
            "pg_seclabels", "pg_default_acl"
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
                if (typeOid == null) return "unknown";
                int oid = executor.toInt(typeOid);
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
                        for (Schema schema : executor.database.getSchemas().values()) {
                            for (Table tbl : schema.getTables().values()) {
                                for (StoredConstraint sc : tbl.getConstraints()) {
                                    int scOid = executor.systemCatalog.getOid("con:" + tbl.getName() + "." + sc.getName());
                                    if (scOid == coid) {
                                        return formatConstraintDef(sc);
                                    }
                                }
                            }
                        }
                    }
                }
                return "";
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
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                if (fn.args().size() > 1) executor.evalExpr(fn.args().get(1), ctx);
                return "";
            }
            case "pg_get_function_sqlbody": {
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                return null;
            }
            case "pg_get_partkeydef": {
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                return null;
            }
            case "pg_get_viewdef":
                return evalPgGetViewdef(fn, ctx);
            case "pg_get_functiondef": {
                if (!fn.args().isEmpty()) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    String funcName = arg != null ? arg.toString() : "";
                    if (funcName.contains(".")) {
                        funcName = funcName.substring(funcName.lastIndexOf('.') + 1);
                    }
                    if (funcName.contains("(")) {
                        funcName = funcName.substring(0, funcName.indexOf('('));
                    }
                    PgFunction func = executor.database.getFunction(funcName);
                    if (func != null) {
                        return buildFunctionDef(func);
                    }
                }
                return "";
            }
            case "pg_get_function_arguments": {
                if (!fn.args().isEmpty()) {
                    PgFunction func = resolveFunction(fn.args().get(0), ctx);
                    if (func != null) {
                        return buildFunctionArguments(func);
                    }
                }
                return "";
            }
            case "pg_get_function_identity_arguments": {
                if (!fn.args().isEmpty()) {
                    PgFunction func = resolveFunction(fn.args().get(0), ctx);
                    if (func != null) {
                        return buildFunctionIdentityArguments(func);
                    }
                }
                return "";
            }
            case "pg_get_function_result": {
                if (!fn.args().isEmpty()) {
                    PgFunction func = resolveFunction(fn.args().get(0), ctx);
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
                }
                return "";
            }
            case "pg_get_serial_sequence":
                return evalPgGetSerialSequence(fn, ctx);
            case "obj_description":
                return evalObjDescription(fn, ctx);
            case "col_description": {
                if (fn.args().size() >= 2) {
                    Object tableArg = executor.evalExpr(fn.args().get(0), ctx);
                    Object colNumArg = executor.evalExpr(fn.args().get(1), ctx);
                    if (tableArg != null && colNumArg != null) {
                        int colNum = executor.toInt(colNumArg);
                        int targetOid = -1;
                        if (tableArg instanceof RegclassValue) {
                            RegclassValue rc = (RegclassValue) tableArg;
                            targetOid = rc.oid();
                        } else if (tableArg instanceof Number) {
                            Number n = (Number) tableArg;
                            targetOid = n.intValue();
                        }
                        if (targetOid >= 0) {
                            for (Schema schema : executor.database.getSchemas().values()) {
                                for (Table tbl : schema.getTables().values()) {
                                    int tblOid = executor.systemCatalog.getOid("rel:" + schema.getName() + "." + tbl.getName());
                                    if (tblOid == targetOid) {
                                        if (colNum >= 1 && colNum <= tbl.getColumns().size()) {
                                            String colName = tbl.getColumns().get(colNum - 1).getName();
                                            return executor.database.getComment("column", tbl.getName().toLowerCase() + "." + colName.toLowerCase());
                                        }
                                        return null;
                                    }
                                }
                            }
                        }
                    }
                }
                return null;
            }
            case "pg_get_userbyid":
                return "memgres";
            case "pg_encoding_to_char":
                return "UTF8";
            case "shobj_description":
                return null;
            case "pg_describe_object": {
                for (Expression a : fn.args()) executor.evalExpr(a, ctx);
                return "";
            }
            case "_pg_expandarray": {
                if (fn.args().isEmpty()) return null;
                Object arrVal = executor.evalExpr(fn.args().get(0), ctx);
                if (arrVal == null) return null;
                if (arrVal instanceof List<?>) {
                    List<?> list = (List<?>) arrVal;
                    if (list.isEmpty()) return null;
                    return Cols.listOf(list.get(0), 1);
                }
                if (arrVal instanceof String) {
                    String s = (String) arrVal;
                    String[] parts = s.trim().split("\\s+");
                    if (parts.length == 0) return null;
                    try {
                        return Cols.listOf(Integer.parseInt(parts[0]), 1);
                    } catch (NumberFormatException e) {
                        return Cols.listOf(parts[0], 1);
                    }
                }
                return null;
            }
            case "to_regclass":
                return evalToRegclass(fn, ctx);
            case "to_regtype":
                return evalToRegtype(fn, ctx);
            case "to_regprocedure": {
                if (fn.args().isEmpty()) return null;
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String procSig = arg.toString().trim();
                String procName = procSig;
                int parenIdx = procSig.indexOf('(');
                if (parenIdx >= 0) procName = procSig.substring(0, parenIdx).trim();
                if (resolveFunctionByName(procName) != null) return procSig;
                return null;
            }
            case "to_regproc": {
                if (fn.args().isEmpty()) return null;
                Object argRp = executor.evalExpr(fn.args().get(0), ctx);
                if (argRp == null) return null;
                String rpName = argRp.toString().trim();
                String rpFuncName = rpName.contains(".")
                        ? rpName.substring(rpName.indexOf('.') + 1).toLowerCase()
                        : rpName.toLowerCase();
                if (resolveFunctionByName(rpName) != null) return rpFuncName;
                if (isBuiltinFunction(rpFuncName)) return rpFuncName;
                return null;
            }
            default:
                return NOT_HANDLED;
        }
    }

    // ---- Complex case methods ----

    private Object evalPgGetIndexdef(FunctionCallExpr fn, RowContext ctx) {
        if (fn.args().isEmpty()) return "";
        Object arg = executor.evalExpr(fn.args().get(0), ctx);
        int colNo = 0;
        if (fn.args().size() >= 2) {
            Object colNoArg = executor.evalExpr(fn.args().get(1), ctx);
            colNo = colNoArg != null ? ((Number) colNoArg).intValue() : 0;
            if (fn.args().size() >= 3) executor.evalExpr(fn.args().get(2), ctx);
        }
        if (arg == null) return "";
        String indexName = null;
        if (arg instanceof RegclassValue) {
            RegclassValue rc = (RegclassValue) arg;
            indexName = rc.name();
        } else if (arg instanceof Number) {
            int targetOid = ((Number) arg).intValue();
            for (Map.Entry<String, Integer> entry : executor.systemCatalog.getOidMap().entrySet()) {
                if (entry.getValue() == targetOid && entry.getKey().startsWith("rel:")) {
                    indexName = entry.getKey().substring(entry.getKey().lastIndexOf('.') + 1);
                    break;
                }
            }
        } else {
            indexName = arg.toString();
        }
        if (indexName == null) return "";
        if (indexName.contains(".")) {
            indexName = indexName.substring(indexName.lastIndexOf('.') + 1);
        }
        List<String> cols = executor.database.getIndexColumns(indexName);
        String constraintTableName = null;
        if (cols == null) {
            for (Map.Entry<String, Schema> schemaEntry : executor.database.getSchemas().entrySet()) {
                for (Map.Entry<String, Table> tblEntry : schemaEntry.getValue().getTables().entrySet()) {
                    for (StoredConstraint sc : tblEntry.getValue().getConstraints()) {
                        if (sc.getName().equalsIgnoreCase(indexName) &&
                                (sc.getType() == StoredConstraint.Type.PRIMARY_KEY || sc.getType() == StoredConstraint.Type.UNIQUE)) {
                            cols = sc.getColumns();
                            constraintTableName = schemaEntry.getKey() + "." + tblEntry.getKey();
                        }
                    }
                }
            }
        }
        if (colNo > 0 && cols != null) {
            return colNo <= cols.size() ? cols.get(colNo - 1) : "";
        }
        if (cols == null) return "";
        boolean unique = executor.database.isUniqueIndex(indexName);
        String tableName = constraintTableName != null
                ? constraintTableName
                : executor.database.getIndexTable(indexName);
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
        String idxMethod = executor.database.getIndexMethod(indexName);
        if (idxMethod == null || idxMethod.isEmpty()) idxMethod = "btree";
        String whereClause = executor.database.getIndexWhereClause(indexName);
        List<String> normalizedCols = new java.util.ArrayList<>();
        for (String col : cols) {
            String nc = col.replaceAll("\\s+\\(", "(")
                           .replaceAll("\\(\\s+", "(")
                           .replaceAll("\\s+\\)", ")")
                           .replaceAll("\\s+,", ",")
                           .replaceAll(",\\s+", ", ");
            nc = nc.replaceAll("('(?:[^']*(?:'')*[^']*)*')(?!::)", "$1::text");
            nc = java.util.regex.Pattern.compile("\\b([a-z_][a-z0-9_]*)\\(")
                    .matcher(nc).replaceAll(m -> m.group(1).toUpperCase() + "(");
            if (nc.startsWith("(") && nc.endsWith(")")) {
                int depth = 0;
                boolean outerMatched = false;
                for (int ci = 0; ci < nc.length(); ci++) {
                    if (nc.charAt(ci) == '(') depth++;
                    else if (nc.charAt(ci) == ')') depth--;
                    if (depth == 0 && ci == nc.length() - 1) outerMatched = true;
                    if (depth == 0 && ci < nc.length() - 1) break;
                }
                if (outerMatched) {
                    nc = nc.substring(1, nc.length() - 1);
                }
            }
            normalizedCols.add(nc);
        }
        StringBuilder idxDef = new StringBuilder();
        idxDef.append("CREATE ").append(unique ? "UNIQUE " : "").append("INDEX ")
              .append(indexName).append(" ON ").append(tableName)
              .append(" USING ").append(idxMethod)
              .append(" (").append(String.join(", ", normalizedCols)).append(")");
        if (whereClause != null && !whereClause.isEmpty()) {
            idxDef.append(" WHERE ").append(whereClause);
        }
        return idxDef.toString();
    }

    private Object evalPgGetTriggerdef(FunctionCallExpr fn, RowContext ctx) {
        Object trigOidVal = fn.args().isEmpty() ? null : executor.evalExpr(fn.args().get(0), ctx);
        if (fn.args().size() > 1) executor.evalExpr(fn.args().get(1), ctx);
        if (trigOidVal == null) return "";
        int trigOid = executor.toInt(trigOidVal);
        for (Map.Entry<String, List<PgTrigger>> trigEntry : executor.database.getAllTriggers().entrySet()) {
            Map<String, java.util.List<PgTrigger>> grouped = new java.util.LinkedHashMap<>();
            for (PgTrigger trig : trigEntry.getValue()) {
                grouped.computeIfAbsent(trig.getName(), k -> new java.util.ArrayList<>()).add(trig);
            }
            for (Map.Entry<String, List<PgTrigger>> nameEntry : grouped.entrySet()) {
                int tOid = executor.systemCatalog.getOid("trig:" + nameEntry.getKey());
                if (tOid == trigOid) {
                    PgTrigger first = nameEntry.getValue().get(0);
                    StringBuilder sb = new StringBuilder("CREATE TRIGGER ");
                    sb.append(first.getName()).append(' ');
                    sb.append(first.getTiming().name()).append(' ');
                    java.util.List<String> events = new java.util.ArrayList<>();
                    for (PgTrigger t : nameEntry.getValue()) {
                        String ev = t.getEvent().name();
                        if (t.getEvent() == PgTrigger.Event.UPDATE && t.getUpdateColumns() != null && !t.getUpdateColumns().isEmpty()) {
                            ev += " OF " + String.join(", ", t.getUpdateColumns());
                        }
                        if (!events.contains(ev)) events.add(ev);
                    }
                    sb.append(String.join(" OR ", events));
                    sb.append(" ON ").append(first.getTableName());
                    sb.append(" FOR EACH ").append(first.isForEachStatement() ? "STATEMENT" : "ROW");
                    sb.append(" EXECUTE FUNCTION ").append(first.getFunctionName()).append("()");
                    return sb.toString();
                }
            }
        }
        return "";
    }

    private Object evalPgGetViewdef(FunctionCallExpr fn, RowContext ctx) {
        if (fn.args().isEmpty()) return "";
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
        boolean prettyPrint = false;
        if (fn.args().size() >= 2) {
            Object prettyArg = executor.evalExpr(fn.args().get(1), ctx);
            if (prettyArg instanceof Boolean) prettyPrint = ((Boolean) prettyArg);
            else if (prettyArg != null) prettyPrint = Boolean.parseBoolean(prettyArg.toString());
        }
        if (viewName != null) {
            if (viewName.contains(".")) {
                viewName = viewName.substring(viewName.lastIndexOf('.') + 1);
            }
            Database.ViewDef view = executor.database.getView(viewName);
            if (view != null && view.query() != null) {
                String sql = view.sourceSQL() != null ? view.sourceSQL()
                        : SqlUnparser.toSql(view.query());
                if (prettyPrint) {
                    java.util.regex.Matcher selectMatcher = java.util.regex.Pattern
                            .compile("(?i)^SELECT\\s+(.*?)\\s+FROM\\s+", java.util.regex.Pattern.DOTALL)
                            .matcher(sql);
                    if (selectMatcher.find()) {
                        String columnList = selectMatcher.group(1);
                        String[] columns = columnList.split("\\s*,\\s*");
                        StringBuilder formattedCols = new StringBuilder(columns[0].trim());
                        for (int ci = 1; ci < columns.length; ci++) {
                            formattedCols.append(",\n    ").append(columns[ci].trim());
                        }
                        sql = " SELECT " + formattedCols.toString()
                                + sql.substring(selectMatcher.end() - " FROM ".length());
                    }
                    sql = sql.replaceAll("(?i)\\s+FROM\\s+", "\n   FROM ")
                             .replaceAll("(?i)\\s+WHERE\\s+", "\n  WHERE ");
                }
                return sql + ";";
            }
        }
        return "";
    }

    private Object evalPgGetSerialSequence(FunctionCallExpr fn, RowContext ctx) {
        if (fn.args().size() < 2) return null;
        String tblName = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
        String colName = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
        if (tblName.contains(".")) {
            tblName = tblName.substring(tblName.lastIndexOf('.') + 1);
        }
        for (Schema schema : executor.database.getSchemas().values()) {
            Table tbl = schema.getTable(tblName);
            if (tbl != null) {
                for (Column col : tbl.getColumns()) {
                    if (col.getName().equalsIgnoreCase(colName)) {
                        String def = col.getDefaultValue();
                        if (def != null && def.toLowerCase().contains("nextval")) {
                            int q1 = def.indexOf('\'');
                            int q2 = def.indexOf('\'', q1 + 1);
                            if (q1 >= 0 && q2 > q1) {
                                return "public." + def.substring(q1 + 1, q2);
                            }
                        }
                        if (col.getType() == DataType.SERIAL || col.getType() == DataType.BIGSERIAL
                                || col.getType() == DataType.SMALLSERIAL
                                || (def != null && def.contains("__identity__"))) {
                            String seqName = tblName + "_" + colName + "_seq";
                            if (executor.database.getSequence(seqName) == null) {
                                Sequence seq = new Sequence(seqName, tbl.getSerialCounter(), 1L, null, null);
                                executor.database.addSequence(seq);
                            }
                            return "public." + seqName;
                        }
                    }
                }
            }
        }
        return null;
    }

    private Object evalObjDescription(FunctionCallExpr fn, RowContext ctx) {
        Object oidArg = executor.evalExpr(fn.args().get(0), ctx);
        String catalog = fn.args().size() > 1 ? String.valueOf(executor.evalExpr(fn.args().get(1), ctx)) : "pg_class";
        if (oidArg == null) return null;
        String comment = null;
        int targetOid = -1;
        if (oidArg instanceof RegclassValue) {
            RegclassValue rc = (RegclassValue) oidArg;
            targetOid = rc.oid();
        } else if (oidArg instanceof Number) {
            Number n = (Number) oidArg;
            targetOid = n.intValue();
        }
        if (targetOid >= 0) {
            for (Schema schema : executor.database.getSchemas().values()) {
                for (Table tbl : schema.getTables().values()) {
                    int tblOid = executor.systemCatalog.getOid("rel:" + schema.getName() + "." + tbl.getName());
                    if (tblOid == targetOid) {
                        comment = executor.database.getComment("table", tbl.getName().toLowerCase());
                        break;
                    }
                }
                if (comment != null) break;
            }
        }
        if (comment == null && targetOid >= 0) {
            for (Map.Entry<String, Integer> entry : executor.systemCatalog.getOidMap().entrySet()) {
                if (entry.getValue() == targetOid && entry.getKey().startsWith("rel:")) {
                    String indexName = entry.getKey().substring(entry.getKey().lastIndexOf('.') + 1);
                    comment = executor.database.getComment("index", indexName.toLowerCase());
                    if (comment != null) break;
                }
            }
        }
        if (comment == null) {
            String objName = oidArg.toString().toLowerCase();
            comment = executor.database.getComment("table", objName);
            if (comment == null) {
                comment = executor.database.getComment("index", objName);
            }
            if (comment == null && objName.contains(".")) {
                String bareName = objName.substring(objName.lastIndexOf('.') + 1);
                comment = executor.database.getComment("table", bareName);
                if (comment == null) {
                    comment = executor.database.getComment("index", bareName);
                }
            }
        }
        return comment;
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
            if (foundRc == null && executor.database.hasIndex(tblNameRc)) {
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
            if (foundRc == null && executor.database.hasIndex(regclassName.toLowerCase())) {
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
        String typeName = arg.toString().trim().toLowerCase();
        String canonical;
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
            if (executor.database.getCustomEnum(typeName) != null) canonical = typeName;
            else if (executor.database.isDomain(typeName)) canonical = typeName;
        }
        return canonical;
    }

    // ---- Shared helper for to_regproc / to_regprocedure ----

    private PgFunction resolveFunctionByName(String name) {
        String schema = null;
        String funcName = name;
        if (name.contains(".")) {
            int dotIdx = name.indexOf('.');
            schema = name.substring(0, dotIdx).toLowerCase();
            funcName = name.substring(dotIdx + 1).toLowerCase();
        } else {
            funcName = name.toLowerCase();
        }
        PgFunction found = null;
        if (schema != null) {
            found = executor.database.getFunction(schema + "." + funcName);
            if (found == null) found = executor.database.getFunction(funcName);
        } else {
            found = executor.database.getFunction(funcName);
        }
        return found;
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

    private String formatConstraintDef(StoredConstraint sc) {
        switch (sc.getType()) {
            case PRIMARY_KEY:
                return "PRIMARY KEY (" + String.join(", ", sc.getColumns()) + ")";
            case UNIQUE:
                return "UNIQUE (" + String.join(", ", sc.getColumns()) + ")";
            case CHECK:
                return "CHECK (" + (sc.getCheckExpr() != null ? SqlUnparser.exprToSql(sc.getCheckExpr()) : "true") + ")";
            case FOREIGN_KEY: {
                StringBuilder sb = new StringBuilder("FOREIGN KEY (");
                sb.append(String.join(", ", sc.getColumns()));
                sb.append(") REFERENCES ");
                if (sc.getReferencesTable() != null) sb.append(sc.getReferencesTable());
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
                StringBuilder sb = new StringBuilder("EXCLUDE USING gist (");
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

    private static final java.util.Map<Integer, String> EXTRA_TYPE_NAMES = Cols.mapOf(
            1033, "aclitem",
            1034, "aclitem[]",
            2249, "record",
            2287, "record[]",
            2278, "void",
            2276, "any",
            2277, "anyarray"
    );

    private String formatTypeByOid(int oid, int typmod) {
        for (DataType dt : DataType.values()) {
            if (dt.getOid() == oid) {
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
                        base = dt.getPgName();
                        break;
                }
                return base;
            }
        }
        for (CustomEnum ce : executor.database.getCustomEnums().values()) {
            int enumOid = executor.systemCatalog.getOid("type:" + ce.getName());
            if (enumOid == oid) return ce.getName();
        }
        for (DomainType domain : executor.database.getDomains().values()) {
            int domainOid = executor.systemCatalog.getOid("type:" + domain.getName());
            if (domainOid == oid) return domain.getName();
        }
        String extra = EXTRA_TYPE_NAMES.get(oid);
        if (extra != null) return extra;
        return "unknown";
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

    static String normalizePgTypeName(String typeName) {
        if (typeName == null) return "void";
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
        Object arg = executor.evalExpr(argExpr, ctx);
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
        return executor.database.getFunction(funcName);
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
            case "uuid_generate_v4":
            case "nextval":
            case "currval":
            case "setval":
            case "lastval":
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
