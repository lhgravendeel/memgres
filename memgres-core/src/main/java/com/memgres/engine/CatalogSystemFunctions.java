package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.math.BigDecimal;
import java.time.*;
import java.util.*;

/**
 * Evaluates catalog and system-information function calls (pg_typeof, current_database,
 * has_*_privilege, pg_sleep, etc.).  Metadata/introspection functions (pg_get_indexdef,
 * format_type, to_regclass, etc.) are delegated to CatalogMetadataFunctions.
 */
class CatalogSystemFunctions {

    static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    private final AstExecutor executor;
    private final CatalogMetadataFunctions metadataFunctions;

    CatalogSystemFunctions(AstExecutor executor) {
        this.executor = executor;
        this.metadataFunctions = new CatalogMetadataFunctions(executor);
    }

    private void requireArgs(FunctionCallExpr fn, int min) {
        if (fn.args().size() < min) {
            throw new MemgresException(
                "function " + fn.name() + "() does not exist" +
                (fn.args().isEmpty() ? "" : "\n  Hint: No function matches the given name and argument types."), "42883");
        }
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        // Try metadata/introspection functions first
        Object metaResult = metadataFunctions.eval(name, fn, ctx);
        if (metaResult != NOT_HANDLED) return metaResult;

        switch (name) {
            case "pg_typeof": {
                Expression rawExpr = fn.args().get(0);

                // Check if this is a column reference and use declared column type metadata
                if (rawExpr instanceof ColumnRef && ctx != null) {
                    ColumnRef colRef = (ColumnRef) rawExpr;
                    Column colDef = ctx.resolveColumnDef(colRef.table(), colRef.column());
                    if (colDef != null) {
                        if (colDef.getEnumTypeName() != null) return colDef.getEnumTypeName();
                        if (colDef.getCompositeTypeName() != null) return colDef.getCompositeTypeName();
                        if (colDef.getArrayElementType() != null)
                            return pgTypeDisplayName(colDef.getArrayElementType()) + "[]";
                        if (colDef.getType() == DataType.JSONB) return "jsonb";
                        if (colDef.getType() == DataType.JSON) return "json";
                        if (colDef.getType() != null && colDef.getType() != DataType.TEXT) {
                            return pgTypeDisplayName(colDef.getType());
                        }
                    }
                }

                if (rawExpr instanceof BinaryExpr && ((BinaryExpr) rawExpr).op() == BinaryExpr.BinOp.JSON_ARROW) {
                    BinaryExpr binExpr = (BinaryExpr) rawExpr;
                    return "jsonb";
                }

                if (rawExpr instanceof CastExpr) {
                    CastExpr cast = (CastExpr) rawExpr;
                    String tn = cast.typeName().toLowerCase().replaceAll("\\(.*\\)", "").trim();
                    if (tn.endsWith("[]")) {
                        String baseType = tn.substring(0, tn.length() - 2).trim();
                        try {
                            return pgTypeDisplayName(DataType.fromPgName(baseType)) + "[]";
                        } catch (Exception e) { return tn; }
                    }
                    try {
                        return pgTypeDisplayName(DataType.fromPgName(tn));
                    } catch (Exception e) { return tn; }
                }
                if (rawExpr instanceof ArrayExpr && !((ArrayExpr) rawExpr).isRow() && !((ArrayExpr) rawExpr).elements().isEmpty()) {
                    ArrayExpr arrExpr = (ArrayExpr) rawExpr;
                    Expression firstElem = arrExpr.elements().get(0);
                    if (firstElem instanceof CastExpr) {
                        CastExpr elemCast = (CastExpr) firstElem;
                        String elemTypeName = elemCast.typeName().toLowerCase().replaceAll("\\(.*\\)", "").trim();
                        if (executor.database.isCompositeType(elemTypeName)) {
                            return elemTypeName + "[]";
                        }
                        if (executor.database.isCustomEnum(elemTypeName)) {
                            return elemTypeName + "[]";
                        }
                    }
                }

                if (rawExpr instanceof Literal && ((Literal) rawExpr).literalType() == Literal.LiteralType.STRING) {
                    Literal lit = (Literal) rawExpr;
                    return "unknown";
                }

                if (rawExpr instanceof CaseExpr) {
                    CaseExpr caseExpr = (CaseExpr) rawExpr;
                    boolean hasInt = false, hasFloat = false, hasNull = false;
                    String nonNullType = null;
                    List<Expression> branches = new ArrayList<>();
                    for (CaseExpr.WhenClause wc : caseExpr.whenClauses()) branches.add(wc.result());
                    if (caseExpr.elseExpr() != null) branches.add(caseExpr.elseExpr());
                    for (Expression br : branches) {
                        if (br instanceof Literal) {
                            Literal bLit = (Literal) br;
                            if (bLit.literalType() == Literal.LiteralType.NULL) { hasNull = true; continue; }
                            if (bLit.literalType() == Literal.LiteralType.INTEGER) { hasInt = true; nonNullType = "integer"; }
                            else if (bLit.literalType() == Literal.LiteralType.FLOAT) { hasFloat = true; nonNullType = "numeric"; }
                        } else {
                            try {
                                Object brVal = executor.evalExpr(br, ctx);
                                if (brVal == null) { hasNull = true; }
                                else {
                                    DataType brDt = TypeCoercion.inferType(brVal);
                                    if (brDt != null) nonNullType = pgTypeDisplayName(brDt);
                                    if (brVal instanceof BigDecimal) hasFloat = true;
                                    else if (brVal instanceof Integer) hasInt = true;
                                }
                            } catch (Exception ignored) {}
                        }
                    }
                    if (hasInt && hasFloat) return "numeric";
                    if (hasNull && nonNullType != null) {
                        Object caseResult = executor.evalExpr(rawExpr, ctx);
                        if (caseResult == null) return nonNullType;
                    }
                }

                if (rawExpr instanceof FunctionCallExpr) {
                    FunctionCallExpr rawFn = (FunctionCallExpr) rawExpr;
                    String rawFnName = rawFn.name().toLowerCase();
                    if (rawFnName.equals("greatest") || rawFnName.equals("least")) {
                        boolean hasIntArg = false, hasFloatArg = false;
                        for (Expression a : rawFn.args()) {
                            if (a instanceof Literal) {
                                Literal lit2 = (Literal) a;
                                if (lit2.literalType() == Literal.LiteralType.INTEGER) hasIntArg = true;
                                else if (lit2.literalType() == Literal.LiteralType.FLOAT) hasFloatArg = true;
                            }
                        }
                        if (hasIntArg && hasFloatArg) return "numeric";
                    }
                }

                Object arg = executor.evalExpr(rawExpr, ctx);

                if (arg instanceof AstExecutor.PgRow) return "record";

                if (arg == null) return "unknown";

                if (arg instanceof java.util.List<?>) {
                    java.util.List<?> list = (java.util.List<?>) arg;
                    if (!list.isEmpty() && list.get(0) instanceof String && ((String) list.get(0)).startsWith("(")) {
                        String s0 = (String) list.get(0);
                        return "record";
                    }
                    DataType widest = inferListElementType(list);
                    String elemType = widest != null ? pgTypeDisplayName(widest) : "text";
                    return elemType + "[]";
                }
                if (arg instanceof String && ((String) arg).startsWith("{") && ((String) arg).endsWith("}")) {
                    String s = (String) arg;
                    if (rawExpr instanceof CastExpr && ((CastExpr) rawExpr).typeName().toLowerCase().endsWith("[]")) {
                        CastExpr cast2 = (CastExpr) rawExpr;
                        String base = cast2.typeName().toLowerCase().replace("[]", "").trim();
                        try { return pgTypeDisplayName(DataType.fromPgName(base)) + "[]"; } catch (Exception e) { /* fall through */ }
                    }
                    return "text[]";
                }
                if (arg instanceof BigDecimal) return "numeric";
                DataType dt = TypeCoercion.inferType(arg);
                return dt != null ? pgTypeDisplayName(dt) : arg.getClass().getSimpleName().toLowerCase();
            }
            case "current_database":
            case "current_catalog":
                return executor.session != null ? executor.session.getDatabaseName() : "memgres";
            case "current_schema":
                return executor.session != null ? executor.session.getEffectiveSchema() : "public";
            case "current_schemas": {
                boolean includeImplicit = false;
                if (!fn.args().isEmpty()) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    includeImplicit = arg instanceof Boolean ? ((Boolean) arg) : "true".equalsIgnoreCase(String.valueOf(arg));
                }
                if (executor.session != null) {
                    return new java.util.ArrayList<Object>(executor.session.getEffectiveSearchPath(includeImplicit));
                }
                List<Object> schemas = new java.util.ArrayList<>();
                if (includeImplicit) schemas.add("pg_catalog");
                schemas.add("public");
                return schemas;
            }
            case "current_user":
            case "current_role": {
                if (executor.session != null) {
                    GucSettings guc = executor.session.getGucSettings();
                    if (guc.hasSessionOverride("role")) {
                        String role = guc.get("role");
                        if (role != null && !role.equalsIgnoreCase("NONE") && !role.equalsIgnoreCase("DEFAULT")) {
                            return role;
                        }
                    }
                }
                return executor.sessionUser();
            }
            case "session_user":
                return executor.sessionUser();
            case "pg_backend_pid":
                if (executor.session != null) return executor.session.getPid();
                try {
                    return Integer.parseInt(java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
                } catch (Exception e) {
                    return 0;
                }
            case "inet_server_addr":
                return "127.0.0.1";
            case "inet_server_port":
                return 5432;
            case "inet_client_addr":
                return "127.0.0.1";
            case "inet_client_port":
                return 0;
            case "pg_conf_load_time":
                return OffsetDateTime.now();
            case "pg_postmaster_start_time":
                return OffsetDateTime.now();
            case "pg_is_in_recovery":
                return false;
            case "pg_is_wal_replay_paused":
                return false;
            case "pg_cancel_backend":
            case "pg_terminate_backend": {
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                return false;
            }
            case "pg_reload_conf":
                return true;
            case "pg_rotate_logfile":
                return false;
            case "pg_sleep": {
                if (!fn.args().isEmpty()) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    if (arg instanceof Number) {
                        Number n = (Number) arg;
                        long millis = (long) (n.doubleValue() * 1000);
                        if (millis > 0) {
                            try {
                                Thread.sleep(millis);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new MemgresException("canceling statement due to statement timeout", "57014");
                            }
                        }
                    }
                }
                return null;
            }
            case "pg_sleep_for": {
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                return null;
            }
            case "pg_sleep_until": {
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                return null;
            }
            case "pg_blocking_pids": {
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                return Cols.listOf();
            }
            case "pg_export_snapshot":
                return "00000001-00000001-1";
            case "pg_stat_clear_snapshot": {
                return null; 
            }
            case "pg_stat_reset": {
                return null; 
            }
            case "pg_stat_reset_shared": {
                return null; 
            }
            case "pg_stat_reset_single_table_counters":
            case "pg_stat_reset_single_function_counters": {
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                return null;
            }
            case "txid_current":
                return (long) (System.nanoTime() / 1000);
            case "pg_indexam_has_property": {
                Object amOid = fn.args().size() > 0 ? executor.evalExpr(fn.args().get(0), ctx) : 0;
                Object propArg = fn.args().size() > 1 ? executor.evalExpr(fn.args().get(1), ctx) : "";
                String prop = String.valueOf(propArg).toLowerCase();
                switch (prop) {
                    case "can_order":
                    case "can_unique":
                    case "can_multi_col":
                        return true;
                    default:
                        return false;
                }
            }
            case "pg_tablespace_location": {
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                return "";
            }
            case "current_setting": {
                String setting = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                String value = null;
                if (executor.session != null) {
                    value = executor.session.getGucSettings().get(setting);
                }
                if (value == null) {
                    value = new GucSettings().get(setting);
                }
                if (value == null) {
                    if (fn.args().size() > 1 && executor.isTruthy(executor.evalExpr(fn.args().get(1), ctx))) {
                        return null;
                    }
                    throw new MemgresException("unrecognized configuration parameter \"" + setting + "\"", "42704");
                }
                return value;
            }
            case "set_config": {
                if (fn.args().size() >= 2) {
                    String settingName = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                    if (settingName.equalsIgnoreCase("max_connections")) {
                        throw new MemgresException("parameter \"max_connections\" cannot be changed without restarting the server");
                    }
                    String settingValue = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                    if (executor.session != null) {
                        executor.session.getGucSettings().set(settingName, settingValue);
                    }
                    return settingValue;
                }
                return null;
            }
            case "pg_function_is_visible":
            case "pg_type_is_visible":
            case "pg_opclass_is_visible":
            case "pg_operator_is_visible":
            case "pg_collation_is_visible":
            case "pg_conversion_is_visible":
            case "pg_ts_config_is_visible":
            case "pg_ts_dict_is_visible":
            case "pg_ts_parser_is_visible":
            case "pg_ts_template_is_visible":
                return true;
            case "pg_table_is_visible": {
                Object tableOid = executor.evalExpr(fn.args().get(0), ctx);
                return true;
            }
            case "pg_database_size": {
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                return 8192L;
            }
            case "has_schema_privilege":
                return evalHasPrivilege(fn, ctx, "SCHEMA");
            case "has_table_privilege":
                return evalHasPrivilege(fn, ctx, "TABLE");
            case "has_function_privilege":
                return evalHasPrivilege(fn, ctx, "FUNCTION");
            case "has_database_privilege":
                return true;
            case "has_column_privilege": {
                String colPrivUser;
                String colPrivTable;
                String colPrivCol;
                String colPrivPriv;
                if (fn.args().size() >= 4) {
                    colPrivUser = String.valueOf(executor.evalExpr(fn.args().get(0), ctx)).toLowerCase();
                    colPrivTable = String.valueOf(executor.evalExpr(fn.args().get(1), ctx)).toLowerCase();
                    colPrivCol = String.valueOf(executor.evalExpr(fn.args().get(2), ctx)).toLowerCase();
                    colPrivPriv = String.valueOf(executor.evalExpr(fn.args().get(3), ctx)).toUpperCase().trim();
                } else {
                    colPrivUser = currentUserName();
                    colPrivTable = String.valueOf(executor.evalExpr(fn.args().get(0), ctx)).toLowerCase();
                    colPrivCol = String.valueOf(executor.evalExpr(fn.args().get(1), ctx)).toLowerCase();
                    colPrivPriv = String.valueOf(executor.evalExpr(fn.args().get(2), ctx)).toUpperCase().trim();
                }
                if (colPrivPriv.endsWith(" WITH GRANT OPTION")) {
                    colPrivPriv = colPrivPriv.substring(0, colPrivPriv.length() - " WITH GRANT OPTION".length()).trim();
                }
                String colPrivTableBare = colPrivTable.contains(".") ? colPrivTable.substring(colPrivTable.lastIndexOf('.') + 1) : colPrivTable;
                if (checkPrivilege(colPrivUser, colPrivPriv, "COLUMN", colPrivTableBare + "." + colPrivCol)) {
                    return true;
                }
                return checkPrivilege(colPrivUser, colPrivPriv, "TABLE", colPrivTableBare);
            }
            case "has_sequence_privilege":
                return true;
            case "has_server_privilege":
            case "has_tablespace_privilege":
            case "has_type_privilege":
            case "has_foreign_data_wrapper_privilege":
            case "has_language_privilege":
            case "has_parameter_privilege":
                return true;
            case "pg_has_role": {
                String pgHasRoleUser;
                String pgHasRoleRole;
                String pgHasRolePriv;
                if (fn.args().size() >= 3) {
                    pgHasRoleUser = String.valueOf(executor.evalExpr(fn.args().get(0), ctx)).toLowerCase();
                    pgHasRoleRole = String.valueOf(executor.evalExpr(fn.args().get(1), ctx)).toLowerCase();
                    pgHasRolePriv = String.valueOf(executor.evalExpr(fn.args().get(2), ctx)).toUpperCase();
                } else {
                    pgHasRoleUser = executor.session != null
                            ? executor.session.getGucSettings().hasSessionOverride("role")
                                ? executor.session.getGucSettings().get("role").toLowerCase()
                                : "memgres"
                            : "memgres";
                    pgHasRoleRole = String.valueOf(executor.evalExpr(fn.args().get(0), ctx)).toLowerCase();
                    pgHasRolePriv = String.valueOf(executor.evalExpr(fn.args().get(1), ctx)).toUpperCase();
                }
                if (pgHasRoleUser.equals(pgHasRoleRole)) { return true; }
                if (pgHasRolePriv.contains("ADMIN")) { return false; }
                Map<String, Set<String>> memberships = executor.database.getRoleMemberships();
                Set<String> visited = new HashSet<>();
                java.util.Queue<String> queue = new java.util.ArrayDeque<>();
                queue.add(pgHasRoleRole);
                visited.add(pgHasRoleRole);
                boolean pgHasRoleFound = false;
                while (!queue.isEmpty() && !pgHasRoleFound) {
                    String current = queue.poll();
                    Set<String> directMembers = memberships.get(current);
                    if (directMembers != null && directMembers.contains(pgHasRoleUser)) {
                        pgHasRoleFound = true;
                        break;
                    }
                    if (directMembers != null) {
                        for (String member : directMembers) {
                            if (!visited.contains(member)) {
                                visited.add(member);
                                queue.add(member);
                            }
                        }
                    }
                }
                return pgHasRoleFound;
            }
            case "pg_relation_size":
            case "pg_total_relation_size":
            case "pg_table_size":
            case "pg_indexes_size": {
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                return 8192L;
            }
            case "pg_column_size": {
                if (!fn.args().isEmpty()) {
                    Object val = executor.evalExpr(fn.args().get(0), ctx);
                    if (val == null) return 0;
                    return val.toString().length() + 4;
                }
                return 0;
            }
            case "pg_size_pretty": {
                if (!fn.args().isEmpty()) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    long bytes = arg instanceof Number ? ((Number) arg).longValue() : 0L;
                    if (bytes < 1024) return bytes + " bytes";
                    if (bytes < 1024 * 1024) return (bytes / 1024) + " kB";
                    if (bytes < 1024L * 1024 * 1024) return (bytes / (1024 * 1024)) + " MB";
                    return (bytes / (1024L * 1024 * 1024)) + " GB";
                }
                return "0 bytes";
            }
            case "pg_relation_filepath": {
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                return "base/16384/16385";
            }
            case "acldefault": {
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                if (fn.args().size() > 1) executor.evalExpr(fn.args().get(1), ctx);
                return null;
            }
            case "pg_current_wal_lsn":
            case "pg_current_wal_insert_lsn":
            case "pg_current_wal_flush_lsn":
                return "0/0";
            case "pg_last_wal_receive_lsn":
            case "pg_last_wal_replay_lsn":
                return null;
            case "pg_last_xact_replay_timestamp":
                return null;
            case "pg_wal_lsn_diff": {
                if (fn.args().size() >= 2) {
                    executor.evalExpr(fn.args().get(0), ctx);
                    executor.evalExpr(fn.args().get(1), ctx);
                }
                return java.math.BigDecimal.ZERO;
            }
            case "txid_current_snapshot":
                return "1:1:";
            case "txid_snapshot_xmin":
            case "txid_snapshot_xmax":
                return 1L;
            case "txid_snapshot_xip":
                return Cols.listOf();
            case "lo_creat":
            case "lo_create":
                return executor.database.getLargeObjectStore().loFromBytea(0, new byte[0]);
            case "lo_from_bytea": {
                long reqOid = 0;
                byte[] data = new byte[0];
                if (fn.args().size() >= 2) {
                    Object oidArg = executor.evalExpr(fn.args().get(0), ctx);
                    reqOid = ((Number) oidArg).longValue();
                    Object dataArg = executor.evalExpr(fn.args().get(1), ctx);
                    if (dataArg instanceof byte[]) data = (byte[]) dataArg;
                    else if (dataArg instanceof String) data = ((String) dataArg).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }
                return executor.database.getLargeObjectStore().loFromBytea(reqOid, data);
            }
            case "lo_import":
                return 1L;
            case "lo_export":
                return 1;
            case "lo_unlink": {
                requireArgs(fn, 1);
                Object oidArg = executor.evalExpr(fn.args().get(0), ctx);
                long loid = ((Number) oidArg).longValue();
                return executor.database.getLargeObjectStore().loUnlink(loid);
            }
            case "lo_get": {
                requireArgs(fn, 1);
                Object oidArg = executor.evalExpr(fn.args().get(0), ctx);
                long loid = ((Number) oidArg).longValue();
                if (fn.args().size() >= 3) {
                    Object offArg = executor.evalExpr(fn.args().get(1), ctx);
                    Object lenArg = executor.evalExpr(fn.args().get(2), ctx);
                    int offset = ((Number) offArg).intValue();
                    int length = ((Number) lenArg).intValue();
                    return executor.database.getLargeObjectStore().loGet(loid, offset, length);
                }
                return executor.database.getLargeObjectStore().loGet(loid);
            }
            case "lo_put": {
                requireArgs(fn, 3);
                Object oidArg = executor.evalExpr(fn.args().get(0), ctx);
                Object offArg = executor.evalExpr(fn.args().get(1), ctx);
                Object dataArg = executor.evalExpr(fn.args().get(2), ctx);
                long loid = ((Number) oidArg).longValue();
                int offset = ((Number) offArg).intValue();
                byte[] data;
                if (dataArg instanceof byte[]) data = (byte[]) dataArg;
                else if (dataArg instanceof String) data = ((String) dataArg).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                else data = new byte[0];
                executor.database.getLargeObjectStore().loPut(loid, offset, data);
                return null;
            }
            case "lo_open": {
                requireArgs(fn, 2);
                Object oidArg = executor.evalExpr(fn.args().get(0), ctx);
                Object modeArg = executor.evalExpr(fn.args().get(1), ctx);
                long loid = ((Number) oidArg).longValue();
                int mode = ((Number) modeArg).intValue();
                return executor.database.getLargeObjectStore().loOpen(loid, mode);
            }
            case "loread": {
                requireArgs(fn, 2);
                Object fdArg = executor.evalExpr(fn.args().get(0), ctx);
                Object lenArg = executor.evalExpr(fn.args().get(1), ctx);
                int fd = ((Number) fdArg).intValue();
                int len = ((Number) lenArg).intValue();
                return executor.database.getLargeObjectStore().loRead(fd, len);
            }
            case "lo_close": {
                requireArgs(fn, 1);
                Object fdArg = executor.evalExpr(fn.args().get(0), ctx);
                int fd = ((Number) fdArg).intValue();
                return executor.database.getLargeObjectStore().loClose(fd);
            }
            case "lo_lseek": {
                requireArgs(fn, 3);
                Object fdArg = executor.evalExpr(fn.args().get(0), ctx);
                Object offArg = executor.evalExpr(fn.args().get(1), ctx);
                Object whenceArg = executor.evalExpr(fn.args().get(2), ctx);
                int fd = ((Number) fdArg).intValue();
                int offset = ((Number) offArg).intValue();
                int whence = ((Number) whenceArg).intValue();
                return executor.database.getLargeObjectStore().loLseek(fd, offset, whence);
            }
            case "lo_tell": {
                requireArgs(fn, 1);
                Object fdArg = executor.evalExpr(fn.args().get(0), ctx);
                int fd = ((Number) fdArg).intValue();
                return executor.database.getLargeObjectStore().loTell(fd);
            }
            case "lo_truncate": {
                requireArgs(fn, 2);
                Object fdArg = executor.evalExpr(fn.args().get(0), ctx);
                Object lenArg = executor.evalExpr(fn.args().get(1), ctx);
                int fd = ((Number) fdArg).intValue();
                int len = ((Number) lenArg).intValue();
                return executor.database.getLargeObjectStore().loTruncate(fd, len);
            }
            case "lowrite": {
                requireArgs(fn, 2);
                Object fdArg = executor.evalExpr(fn.args().get(0), ctx);
                Object dataArg = executor.evalExpr(fn.args().get(1), ctx);
                int fd = ((Number) fdArg).intValue();
                byte[] data;
                if (dataArg instanceof byte[]) data = (byte[]) dataArg;
                else if (dataArg instanceof String) data = ((String) dataArg).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                else data = new byte[0];
                return executor.database.getLargeObjectStore().loWrite(fd, data);
            }
            case "pg_event_trigger_ddl_commands":
            case "pg_event_trigger_dropped_objects":
                return null;
            case "pg_event_trigger_table_rewrite_oid":
                return 0L;
            case "pg_event_trigger_table_rewrite_reason":
                return 0;
            default:
                return NOT_HANDLED;
        }
    }

    // ---- DRY helper for has_schema/table/function_privilege ----

    private boolean evalHasPrivilege(FunctionCallExpr fn, RowContext ctx, String objectType) {
        String user, objectName, priv;
        if (fn.args().size() >= 3) {
            user = String.valueOf(executor.evalExpr(fn.args().get(0), ctx)).toLowerCase();
            objectName = String.valueOf(executor.evalExpr(fn.args().get(1), ctx)).toLowerCase();
            priv = String.valueOf(executor.evalExpr(fn.args().get(2), ctx)).toUpperCase().trim();
        } else {
            user = currentUserName();
            objectName = String.valueOf(executor.evalExpr(fn.args().get(0), ctx)).toLowerCase();
            priv = String.valueOf(executor.evalExpr(fn.args().get(1), ctx)).toUpperCase().trim();
        }
        if (priv.endsWith(" WITH GRANT OPTION")) {
            priv = priv.substring(0, priv.length() - " WITH GRANT OPTION".length()).trim();
        }
        // Strip argument types for FUNCTION: "funcname(int, text)" -> "funcname"
        if ("FUNCTION".equals(objectType) && objectName.contains("(")) {
            objectName = objectName.substring(0, objectName.indexOf('(')).trim();
        }
        // Strip schema prefix for TABLE and FUNCTION
        if (("TABLE".equals(objectType) || "FUNCTION".equals(objectType)) && objectName.contains(".")) {
            objectName = objectName.substring(objectName.lastIndexOf('.') + 1);
        }
        return checkPrivilege(user, priv, objectType, objectName);
    }

    // ---- Privilege checking ----

    private boolean checkPrivilege(String roleName, String privilege, String objectType, String objectName) {
        String roleNameLower = roleName.toLowerCase();
        String objectNameLower = objectName.toLowerCase();

        // 1. Superusers have all privileges
        Map<String, String> roleAttrs = executor.database.getRoles().get(roleNameLower);
        if (roleAttrs != null && "true".equalsIgnoreCase(roleAttrs.get("SUPERUSER"))) {
            return true;
        }

        // 2. Owner has all privileges on their own objects
        String ownerKey = objectType.equalsIgnoreCase("TABLE")
                ? "table:public." + objectNameLower
                : objectType.equalsIgnoreCase("FUNCTION")
                    ? "function:" + objectNameLower
                    : objectType.equalsIgnoreCase("SCHEMA")
                        ? "schema:" + objectNameLower
                        : null;
        if (ownerKey != null) {
            String owner = executor.database.getObjectOwner(ownerKey);
            if (owner != null && owner.equalsIgnoreCase(roleNameLower)) {
                return true;
            }
        }

        // 3 & 4. Direct or inherited privilege check
        return checkPrivilegeDirectOrInherited(roleNameLower, privilege, objectType, objectNameLower,
                new HashSet<>());
    }

    private boolean checkPrivilegeDirectOrInherited(String roleName, String privilege,
            String objectType, String objectName, Set<String> visited) {
        if (visited.contains(roleName)) return false;
        visited.add(roleName);

        Set<String> privs = executor.database.getRolePrivileges(roleName);
        String objectNameLower = objectName.toLowerCase();
        String checkKey = privilege.toUpperCase() + ":" + objectType.toUpperCase() + ":" + objectNameLower;
        String allKey = "ALL:" + objectType.toUpperCase() + ":" + objectNameLower;
        if (privs.contains(checkKey) || privs.contains(allKey)) {
            return true;
        }

        Map<String, Set<String>> memberships = executor.database.getRoleMemberships();
        for (Map.Entry<String, Set<String>> entry : memberships.entrySet()) {
            String grantedRole = entry.getKey();
            Set<String> members = entry.getValue();
            if (members.contains(roleName) && !visited.contains(grantedRole)) {
                if (checkPrivilegeDirectOrInherited(grantedRole, privilege, objectType, objectName, visited)) {
                    return true;
                }
            }
        }

        return false;
    }

    // ---- Helper methods ----

    private String currentUserName() {
        if (executor.session != null) {
            GucSettings guc = executor.session.getGucSettings();
            if (guc.hasSessionOverride("role")) {
                String role = guc.get("role");
                if (role != null && !role.equalsIgnoreCase("NONE") && !role.equalsIgnoreCase("DEFAULT")) {
                    return role.toLowerCase();
                }
            }
            String sessionAuth = guc.get("session_authorization");
            if (sessionAuth != null) return sessionAuth.toLowerCase();
        }
        return "memgres";
    }

    static String pgTypeDisplayName(DataType dt) {
        switch (dt) {
            case INTEGER:
            case SERIAL:
                return "integer";
            case BIGINT:
            case BIGSERIAL:
                return "bigint";
            case SMALLINT:
            case SMALLSERIAL:
                return "smallint";
            case BOOLEAN:
                return "boolean";
            case DOUBLE_PRECISION:
                return "double precision";
            case REAL:
                return "real";
            case NUMERIC:
                return "numeric";
            case TEXT:
                return "text";
            case VARCHAR:
                return "character varying";
            case CHAR:
                return "character";
            case DATE:
                return "date";
            case TIME:
                return "time without time zone";
            case TIMESTAMP:
                return "timestamp without time zone";
            case TIMESTAMPTZ:
                return "timestamp with time zone";
            case INTERVAL:
                return "interval";
            case UUID:
                return "uuid";
            case JSON:
                return "json";
            case JSONB:
                return "jsonb";
            case BYTEA:
                return "bytea";
            case MONEY:
                return "money";
            case INET:
                return "inet";
            case CIDR:
                return "cidr";
            case MACADDR:
                return "macaddr";
            case XML:
                return "xml";
            case BIT:
                return "bit";
            case VARBIT:
                return "bit varying";
            case TSVECTOR:
                return "tsvector";
            case TSQUERY:
                return "tsquery";
            default:
                return dt.getPgName();
        }
    }

    private static DataType widenNumericType(DataType current, DataType next) {
        if (current == null) return next;
        if (next == null) return current;
        int curRank = numericRank(current);
        int nextRank = numericRank(next);
        return nextRank > curRank ? next : current;
    }

    private static int numericRank(DataType dt) {
        switch (dt) {
            case SMALLINT:
            case SMALLSERIAL:
                return 1;
            case INTEGER:
            case SERIAL:
                return 2;
            case BIGINT:
            case BIGSERIAL:
                return 3;
            case NUMERIC:
                return 4;
            case REAL:
                return 5;
            case DOUBLE_PRECISION:
                return 6;
            default:
                return 0;
        }
    }

    private static DataType inferListElementType(List<?> list) {
        DataType widest = null;
        for (Object elem : list) {
            if (elem == null) continue;
            if (elem instanceof List<?>) {
                DataType subType = inferListElementType((List<?>) elem);
                widest = widenNumericType(widest, subType);
            } else {
                DataType edt = TypeCoercion.inferType(elem);
                if (edt != null) {
                    widest = widenNumericType(widest, edt);
                }
            }
        }
        return widest;
    }
}
