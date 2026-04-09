package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Expression evaluation engine. Handles all eval* methods for AST expression nodes,
 * plus supporting operations (comparison, type inference, alias derivation, numeric ops).
 * Extracted from AstExecutor to separate expression evaluation from statement dispatch.
 */
class ExprEvaluator {

    final AstExecutor executor;

    ExprEvaluator(AstExecutor executor) {
        this.executor = executor;
    }

    // ---- Main expression dispatcher ----

    public Object evalExpr(Expression expr, RowContext ctx) {
        if (expr instanceof Literal) return evalLiteral(((Literal) expr));
        if (expr instanceof ColumnRef) return evalColumnRef(((ColumnRef) expr), ctx);
        if (expr instanceof BinaryExpr) return executor.binaryOpEvaluator.evalBinary(((BinaryExpr) expr), ctx);
        if (expr instanceof UnaryExpr) return evalUnaryValue(((UnaryExpr) expr).op(), evalExpr(((UnaryExpr) expr).operand(), ctx));
        if (expr instanceof FunctionCallExpr) return executor.functionEvaluator.evalFunction(((FunctionCallExpr) expr), ctx);
        if (expr instanceof CastExpr) return evalCast(((CastExpr) expr), ctx);
        if (expr instanceof IsNullExpr) return evalIsNull(((IsNullExpr) expr), ctx);
        if (expr instanceof IsJsonExpr) return evalIsJson(((IsJsonExpr) expr), ctx);
        if (expr instanceof JsonExistsExpr) return evalJsonExists(((JsonExistsExpr) expr), ctx);
        if (expr instanceof JsonValueExpr) return evalJsonValue(((JsonValueExpr) expr), ctx);
        if (expr instanceof JsonQueryExpr) return evalJsonQuery(((JsonQueryExpr) expr), ctx);
        if (expr instanceof InExpr) return evalIn(((InExpr) expr), ctx);
        if (expr instanceof BetweenExpr) return evalBetween(((BetweenExpr) expr), ctx);
        if (expr instanceof LikeExpr) return evalLike(((LikeExpr) expr), ctx);
        if (expr instanceof CaseExpr) return evalCase(((CaseExpr) expr), ctx);
        if (expr instanceof ParamRef) {
            ParamRef p = (ParamRef) expr;
            int idx = p.index() - 1; // $1 is index 0
            if (idx >= 0 && idx < executor.boundParameters.size()) {
                Object val = executor.boundParameters.get(idx);
                // Text-format parameters arrive as strings; try numeric coercion
                if (val instanceof String && !((String) val).isEmpty()) {
                    String s = (String) val;
                    try {
                        if (s.indexOf('.') >= 0 || s.indexOf('e') >= 0 || s.indexOf('E') >= 0) {
                            return new java.math.BigDecimal(s);
                        }
                        long lv = Long.parseLong(s);
                        if (lv >= Integer.MIN_VALUE && lv <= Integer.MAX_VALUE) {
                            return (int) lv;
                        }
                        return lv;
                    } catch (NumberFormatException ignored) {
                        // Not a number, keep as string
                    }
                }
                return val;
            }
            return null;
        }
        if (expr instanceof WildcardExpr) return null;
        if (expr instanceof SubqueryExpr) return evalSubquery(((SubqueryExpr) expr), ctx);
        if (expr instanceof ExistsExpr) return evalExists(((ExistsExpr) expr), ctx);
        if (expr instanceof AnyAllExpr) return evalAnyAll(((AnyAllExpr) expr), ctx);
        if (expr instanceof AnyAllArrayExpr) return evalAnyAllArray(((AnyAllArrayExpr) expr), ctx);
        if (expr instanceof ArrayExpr) return executor.arrayOperationHandler.evalArray(((ArrayExpr) expr), ctx);
        if (expr instanceof ArraySubqueryExpr) return executor.arrayOperationHandler.evalArraySubquery(((ArraySubqueryExpr) expr), ctx);
        if (expr instanceof AtTimeZoneExpr) {
            AtTimeZoneExpr attz = (AtTimeZoneExpr) expr;
            Object val = evalExpr(attz.expr(), ctx);
            Object zoneVal = evalExpr(attz.zone(), ctx);
            if (val == null) return null;
            String zoneName = zoneVal.toString();
            ZoneId zid;
            try {
                zid = ZoneId.of(zoneName);
            } catch (java.time.DateTimeException e) {
                throw new MemgresException("time zone \"" + zoneName + "\" not recognized", "22023");
            }
            if (val instanceof OffsetDateTime) {
                OffsetDateTime odt = (OffsetDateTime) val;
                // timestamptz -> timestamp (in that zone)
                return odt.atZoneSameInstant(zid).toLocalDateTime();
            } else if (val instanceof LocalDateTime) {
                LocalDateTime ldt = (LocalDateTime) val;
                // timestamp -> timestamptz (interpret as in that zone)
                return ldt.atZone(zid).toOffsetDateTime();
            } else if (val instanceof LocalTime) {
                LocalTime lt = (LocalTime) val;
                return lt;
            }
            return val;
        }
        if (expr instanceof WindowFuncExpr) {
            WindowFuncExpr wf = (WindowFuncExpr) expr;
            // Window functions should be evaluated by executeWindowSelect, not here
            return null;
        }
        if (expr instanceof NamedArgExpr) return evalExpr(((NamedArgExpr) expr).value(), ctx);
        if (expr instanceof IsBooleanExpr) {
            IsBooleanExpr ib = (IsBooleanExpr) expr;
            Object val = evalExpr(ib.expr(), ctx);
            Boolean b = val == null ? null : isTruthy(val);
            switch (ib.test()) {
                case IS_TRUE:
                    return b != null && b;
                case IS_NOT_TRUE:
                    return b == null || !b;
                case IS_FALSE:
                    return b != null && !b;
                case IS_NOT_FALSE:
                    return b == null || b;
                case IS_UNKNOWN:
                    return b == null;
                case IS_NOT_UNKNOWN:
                    return b != null;
                case IS_DOCUMENT: {
                    Object raw = evalExpr(ib.expr(), ctx);
                    return raw != null && XmlOperations.isDocument(raw.toString());
                }
                case IS_NOT_DOCUMENT: {
                    Object raw = evalExpr(ib.expr(), ctx);
                    return raw == null || !XmlOperations.isDocument(raw.toString());
                }
            }
        }
        if (expr instanceof FieldAccessExpr) {
            FieldAccessExpr fa = (FieldAccessExpr) expr;
            // Composite field access: (expr).field
            Object val = evalExpr(fa.expr(), ctx);
            if (val == null) return null;
            String fieldName = fa.field();

            if (val instanceof List<?>) {
                List<?> list = (List<?>) val;
                // If the result is a list (from _pg_expandarray), access by field name
                switch (fieldName.toLowerCase()) {
                    case "x":
                        return list.isEmpty() ? null : list.get(0);
                    case "n":
                        return list.isEmpty() ? null : list.get(1);
                    default:
                        return null;
                }
            }
            if (val instanceof Map<?,?>) {
                Map<?,?> map = (Map<?,?>) val;
                return map.get(fieldName);
            }

            // Determine the composite type from the inner expression
            String typeName = executor.resolveCompositeTypeName(fa.expr(), ctx);

            if (val instanceof AstExecutor.PgRow) {
                AstExecutor.PgRow row = (AstExecutor.PgRow) val;
                if (typeName != null) {
                    List<CreateTypeStmt.CompositeField> fields = executor.database.getCompositeType(typeName);
                    if (fields != null) {
                        for (int i = 0; i < fields.size(); i++) {
                            if (fields.get(i).name().equalsIgnoreCase(fieldName)) {
                                return i < row.values().size() ? row.values().get(i) : null;
                            }
                        }
                        // Field not found in the composite type
                        throw new MemgresException("column \"" + fieldName + "\" not found in data type " + typeName, "42703");
                    }
                }
                // Untyped ROW: PG does not allow field access on untyped records
                if (fieldName.matches("f\\d+")) {
                    throw new MemgresException("failed to find conversion function from unknown to text", "XX000");
                }
                throw new MemgresException("could not identify column \"" + fieldName + "\" in record data type", "42703");
            }

            // If the value is a String representation of a composite "(val1,val2,...)"
            if (val instanceof String && ((String) val).startsWith("(") && ((String) val).endsWith(")")) {
                String s = (String) val;
                if (typeName != null) {
                    List<CreateTypeStmt.CompositeField> fields = executor.database.getCompositeType(typeName);
                    if (fields != null) {
                        String[] parts = executor.splitCompositeString(s.substring(1, s.length() - 1));
                        for (int i = 0; i < fields.size(); i++) {
                            if (fields.get(i).name().equalsIgnoreCase(fieldName)) {
                                if (i < parts.length) {
                                    String part = parts[i];
                                    // Unquote if needed
                                    if (part.startsWith("\"") && part.endsWith("\"")) {
                                        part = part.substring(1, part.length() - 1);
                                    }
                                    // Coerce to the declared field type
                                    String fieldType = fields.get(i).typeName();
                                    if (executor.database.isCompositeType(fieldType)) {
                                        // Nested composite, return as PgRow for further chaining
                                        return executor.parseCompositeToRow(part, fieldType);
                                    }
                                    return executor.coerceFieldValue(part, fieldType);
                                }
                                return null;
                            }
                        }
                        throw new MemgresException("column \"" + fieldName + "\" not found in data type " + typeName, "42703");
                    }
                }
                // Untyped composite string
                throw new MemgresException("could not identify column \"" + fieldName + "\" in record data type", "42703");
            }

            // Scalar value; field access is invalid
            if (typeName != null) {
                throw new MemgresException("column \"" + fieldName + "\" not found in data type " + typeName, "42703");
            }
            if (fa.expr() instanceof FieldAccessExpr) {
                FieldAccessExpr innerFa = (FieldAccessExpr) fa.expr();
                String parentType = executor.resolveCompositeTypeName(innerFa.expr(), ctx);
                if (parentType != null) {
                    throw new MemgresException("type \"" + parentType + "\" does not exist", "42704");
                }
            }
            // Fallback: return the value itself (for backward compatibility)
            return val;
        }
        if (expr instanceof OrderedSetAggExpr) {
            OrderedSetAggExpr osa = (OrderedSetAggExpr) expr;
            // Ordered-set aggregate outside of aggregate context; evaluate over empty set
            return null;
        }
        if (expr instanceof ArraySliceExpr) return executor.arrayOperationHandler.evalArraySlice(((ArraySliceExpr) expr), ctx);
        if (expr instanceof CollateExpr) return evalExpr(((CollateExpr) expr).expr(), ctx);
        if (expr instanceof CompositeStarExpr) return evalExpr(((CompositeStarExpr) expr).expr(), ctx); // expansion handled by SelectExecutor
        if (expr instanceof QualifiedOperatorExpr) return evalQualifiedOperator(((QualifiedOperatorExpr) expr), ctx);
        throw new UnsupportedOperationException("Unsupported expression type: " + expr.getClass().getSimpleName());
    }

    // ---- Individual expression evaluators ----

    private Object evalLiteral(Literal lit) {
        switch (lit.literalType()) {
            case INTEGER: {
                try { return Integer.parseInt(lit.value()); }
                catch (NumberFormatException e) {
                    try { return Long.parseLong(lit.value()); }
                    catch (NumberFormatException e2) { return new java.math.BigDecimal(lit.value()); }
                }
            }
            case FLOAT:
                return new java.math.BigDecimal(lit.value());
            case STRING:
                return lit.value();
            case BIT_STRING: {
                // Validate bit digits: only 0 and 1 allowed
                for (char c : lit.value().toCharArray()) {
                    if (c != '0' && c != '1') {
                        throw new MemgresException("\"" + lit.value() + "\" is not a valid binary digit", "22P02");
                    }
                }
                return new AstExecutor.PgBitString(lit.value());
            }
            case BOOLEAN:
                return Boolean.parseBoolean(lit.value());
            case NULL:
                return null;
            case DEFAULT:
                throw new MemgresException("DEFAULT is not allowed in this context", "42601");
            default:
                throw new IllegalStateException("Unknown literal type: " + lit.literalType());
        }
    }

    private Object evalColumnRef(ColumnRef ref, RowContext ctx) {
        // pg_catalog.current_user etc.; treat as the system function directly
        if ("pg_catalog".equalsIgnoreCase(ref.table()) || "information_schema".equalsIgnoreCase(ref.table())) {
            String col = ref.column().toLowerCase();
            switch (col) {
                case "current_user":
                case "session_user":
                case "current_role": {
                    return "memgres"; 
                }
                case "current_database":
                case "current_catalog": {
                    return executor.session != null ? executor.session.getDatabaseName() : "memgres";
                }
                case "current_schema": {
                    return "public";
                }
                case "current_schemas": {
                    return new Object[]{"public"};
                }
                case "pg_backend_pid": {
                    return 12345;
                }
                case "inet_server_addr": {
                    return "127.0.0.1";
                }
            }
        }
        if (ctx == null) {
            // No row context, check for system columns
            String col = ref.column().toLowerCase();
            switch (col) {
                case "current_user":
                case "session_user":
                case "current_role":
                    return "memgres";
                case "current_database":
                case "current_catalog":
                    return executor.session != null ? executor.session.getDatabaseName() : "memgres";
                case "current_schema":
                    return "public";
                case "current_schemas":
                    return new Object[]{"public"};
                case "pg_backend_pid":
                    return 12345;
                default: {
                    // In strict mode (e.g., PREPARE), missing context means unresolvable column
                    if (executor.isStrictColumnRefs()) {
                        throw new MemgresException("column \"" + ref.column() + "\" does not exist", "42703");
                    }
                    // Try outer contexts (for LATERAL subqueries with no FROM clause)
                    if (!executor.outerContextStack.isEmpty()) {
                        for (Iterator<RowContext> it = executor.outerContextStack.descendingIterator(); it.hasNext(); ) {
                            RowContext outer = it.next();
                            try {
                                Object result = ref.table() != null
                                    ? outer.resolveColumn(ref.table(), ref.column())
                                    : outer.resolveColumn(null, ref.column());
                                if (result instanceof RowContext.TableoidRef) return resolveTableoidRef(result);
                                return result;
                            } catch (MemgresException e) {
                                if (!"42703".equals(e.getSqlState()) && !"42P01".equals(e.getSqlState())) throw e;
                            }
                        }
                    }
                    // Qualified reference with no context; table doesn't exist
                    if (ref.table() != null) {
                        throw new MemgresException("missing FROM-clause entry for table \"" + ref.table() + "\"", "42P01");
                    }
                    return ref.column(); // Return column name as string (for alias resolution)
                }
            }
        }
        if (ref.table() == null) {
            // Unqualified column reference; check current context first
            Object result = null;
            boolean foundInCurrent = false;
            try {
                result = ctx.resolveColumn(null, ref.column());
                foundInCurrent = true;
            } catch (MemgresException e) {
                if (!"42703".equals(e.getSqlState())) throw e;
                // column not in current context, will try outer contexts / special columns below
            }
            if (foundInCurrent) {
                if (result == null && ref.column().equalsIgnoreCase("tableoid")) {
                    return resolveTableoidRef(ctx.resolveColumn(null, "tableoid"));
                }
                if (result instanceof RowContext.TableoidRef) {
                    return resolveTableoidRef(result);
                }
                return result;
            }
            // Not found in current context, try special columns
            if (ref.column().equalsIgnoreCase("tableoid")) {
                try {
                    return resolveTableoidRef(ctx.resolveColumn(null, "tableoid"));
                } catch (MemgresException ignored) { /* fall through to outer contexts */ }
            }
            if (ref.column().equalsIgnoreCase("ctid")) {
                return "(0,0)"; // stub: return a fixed ctid value
            }
            // Check if the column name matches a table alias (whole-row reference, e.g. ROW_TO_JSON(row))
            {
                Object wholeRow = resolveWholeRowRef(ref.column(), ctx);
                if (wholeRow != null) return wholeRow;
                // Also check outer contexts
                for (Iterator<RowContext> it = executor.outerContextStack.descendingIterator(); it.hasNext(); ) {
                    wholeRow = resolveWholeRowRef(ref.column(), it.next());
                    if (wholeRow != null) return wholeRow;
                }
            }
            // Try outer contexts (for correlated subqueries)
            for (Iterator<RowContext> it = executor.outerContextStack.descendingIterator(); it.hasNext(); ) {
                RowContext outer = it.next();
                try {
                    result = outer.resolveColumn(null, ref.column());
                    if (result instanceof RowContext.TableoidRef) return resolveTableoidRef(result);
                    return result;
                } catch (MemgresException e) {
                    if (!"42703".equals(e.getSqlState())) throw e;
                    // not in this outer context either, continue
                }
            }
            throw new MemgresException("column \"" + ref.column() + "\" does not exist", "42703");
        } else {
            // Schema-qualified reference (schema.table.column); CTEs are not schema-qualified,
            // so a schema prefix means we're looking for a real table, not a CTE binding.
            if (ref.schema() != null) {
                throw new MemgresException(
                    "missing FROM-clause entry for table \"" + ref.table() + "\"", "42P01");
            }
            // Qualified column reference: table.column
            // The table may not be in the current context (e.g., LATERAL joins, correlated subqueries)
            // so we catch the "missing FROM-clause" error and fall through to outer contexts.
            Object result = null;
            boolean foundInCurrent = false;
            try {
                result = ctx.resolveColumn(ref.table(), ref.column());
                foundInCurrent = true;
            } catch (MemgresException e) {
                if (!"42P01".equals(e.getSqlState())) throw e;
                // table not in current context, will try outer contexts below
            }
            if (foundInCurrent) {
                if (result == null && ref.column().equalsIgnoreCase("tableoid")) {
                    return resolveTableoidRef(ctx.resolveColumn(ref.table(), "tableoid"));
                }
                if (result instanceof RowContext.TableoidRef) {
                    return resolveTableoidRef(result);
                }
                return result;
            }
            // Try outer contexts (for correlated subqueries / LATERAL joins)
            for (Iterator<RowContext> it = executor.outerContextStack.descendingIterator(); it.hasNext(); ) {
                RowContext outer = it.next();
                try {
                    result = outer.resolveColumn(ref.table(), ref.column());
                    if (result instanceof RowContext.TableoidRef) return resolveTableoidRef(result);
                    return result;
                } catch (MemgresException e) {
                    if (!"42P01".equals(e.getSqlState())) throw e;
                    // table not in this outer context either, continue searching
                }
            }
            // Not found in any context; throw the original error
            throw new MemgresException("missing FROM-clause entry for table \"" + ref.table() + "\"", "42P01");
        }
    }

    /**
     * If val is a TableoidRef, resolve it to the actual OID integer via SystemCatalog.
     */
    private Object resolveTableoidRef(Object val) {
        if (val instanceof RowContext.TableoidRef) {
            RowContext.TableoidRef ref = (RowContext.TableoidRef) val;
            Table sourceTable = ref.table();
            String tableName = sourceTable.getName();
            // System catalog tables live in pg_catalog
            if (tableName.startsWith("pg_") || tableName.startsWith("information_schema")) {
                return executor.systemCatalog.getOid("rel:pg_catalog." + tableName);
            }
            String schemaName = "public";
            // Check if the table belongs to a specific schema
            for (Map.Entry<String, Schema> schemaEntry : executor.database.getSchemas().entrySet()) {
                if (schemaEntry.getValue().getTable(tableName) == sourceTable) {
                    schemaName = schemaEntry.getKey();
                    break;
                }
            }
            return executor.systemCatalog.getOid("rel:" + schemaName + "." + tableName);
        }
        return val;
    }

    /**
     * Check if 'name' matches a table alias in the given context.
     * If so, return the entire row as a LinkedHashMap (whole-row reference).
     * Returns null if no matching binding is found.
     */
    private java.util.Map<String, Object> resolveWholeRowRef(String name, RowContext ctx) {
        RowContext.TableBinding b = ctx.getBinding(name);
        if (b == null) return null;
        java.util.Map<String, Object> record = new java.util.LinkedHashMap<>();
        for (int i = 0; i < b.table().getColumns().size(); i++) {
            record.put(b.table().getColumns().get(i).getName(), b.row()[i]);
        }
        return record;
    }

    /**
     * Evaluate a prefix-style qualified operator expression: OPERATOR(schema.op)(args).
     */
    private Object evalQualifiedOperator(QualifiedOperatorExpr qop, RowContext ctx) {
        // Check that search_path is valid because PG requires valid schemas for type resolution
        if (executor.session != null) {
            String searchPath = executor.session.getGucSettings().get("search_path");
            if (searchPath != null) {
                for (String sp : searchPath.split(",")) {
                    String s = sp.trim().replace("\"", "").replace("'", "");
                    if (s.isEmpty() || s.equals("$user")) continue;
                    if ("pg_catalog".equals(s) || "information_schema".equals(s)) continue;
                    if (executor.database.getSchema(s) == null) {
                        String qualifiedOp = (qop.schema() != null ? qop.schema() + "." : "") + qop.opSymbol();
                        throw new MemgresException(
                            "operator does not exist: " + qualifiedOp + " record", "42883");
                    }
                }
            }
        }
        return evalExpr(qop.inner(), ctx);
    }

    private Object evalCast(CastExpr cast, RowContext ctx) {
        Object val = evalExpr(cast.expr(), ctx);
        // If the inner expression is a set-returning function and produced a List,
        // cast each element individually to preserve the List for SRF expansion.
        if (val instanceof java.util.List<?> && cast.expr() instanceof FunctionCallExpr
                && SelectExecutor.SRF_FUNCTION_NAMES.contains(((FunctionCallExpr) cast.expr()).name().toLowerCase())) {
            java.util.List<?> list = (java.util.List<?>) val;
            FunctionCallExpr fn = (FunctionCallExpr) cast.expr();
            java.util.List<Object> castList = new java.util.ArrayList<>(list.size());
            for (Object elem : list) {
                castList.add(executor.castEvaluator.applyCast(elem, cast.typeName()));
            }
            return castList;
        }
        return executor.castEvaluator.applyCast(val, cast.typeName());
    }

    /**
     * Evaluate a unary operation on an already-evaluated value.
     */
    Object evalUnaryValue(UnaryExpr.UnaryOp op, Object val) {
        switch (op) {
            case NOT: {
                if (val == null) return null;
                return !isTruthy(val);
            }
            case NEGATE: {
                if (val == null) return null;
                if (val instanceof Integer) return -((Integer) val);
                if (val instanceof Long) return -((Long) val);
                if (val instanceof java.math.BigDecimal) return ((java.math.BigDecimal) val).negate();
                if (val instanceof Double) return -((Double) val);
                if (val instanceof Float) return -((Float) val);
                return val;
            }
            case POSITIVE:
                return val;
            case BIT_NOT: {
                if (val == null) return null;
                // Bit string NOT
                if (val instanceof AstExecutor.PgBitString) {
                    AstExecutor.PgBitString pbs = (AstExecutor.PgBitString) val;
                    StringBuilder sb = new StringBuilder(pbs.bits().length());
                    for (int i = 0; i < pbs.bits().length(); i++) {
                        sb.append(pbs.bits().charAt(i) == '0' ? '1' : '0');
                    }
                    return new AstExecutor.PgBitString(sb.toString());
                }
                if (val instanceof String && !((String) val).isEmpty() && ((String) val).chars().allMatch(c -> c == '0' || c == '1')) {
                    String s = (String) val;
                    StringBuilder sb = new StringBuilder(s.length());
                    for (int i = 0; i < s.length(); i++) {
                        sb.append(s.charAt(i) == '0' ? '1' : '0');
                    }
                    return new AstExecutor.PgBitString(sb.toString());
                }
                // inet bitwise NOT
                if (val instanceof String && ((String) val).contains(".")) {
                    String s = (String) val;
                    return NetworkOperations.bitwiseNot(s);
                }
                if (val instanceof Integer) return ~((Integer) val);
                if (val instanceof Long) return ~((Long) val);
                return ~toLong(val);
            }
            case ABS: {
                if (val == null) return null;
                if (val instanceof Integer) return Math.abs(((Integer) val));
                if (val instanceof Long) return Math.abs(((Long) val));
                if (val instanceof java.math.BigDecimal) return ((java.math.BigDecimal) val).abs();
                if (val instanceof Double) return Math.abs(((Double) val));
                if (val instanceof Float) return Math.abs(((Float) val));
                return val;
            }
            case SQRT: {
                if (val == null) return null;
                double d = toDouble(val);
                if (d < 0) throw new MemgresException("cannot take square root of a negative number", "2201F");
                double r = Math.sqrt(d);
                return r == Math.floor(r) && !Double.isInfinite(r) ? (long) r : r;
            }
            case CBRT: {
                if (val == null) return null;
                double r = Math.cbrt(toDouble(val));
                return r == Math.floor(r) && !Double.isInfinite(r) ? (long) r : r;
            }
            default:
                throw new IllegalStateException("Unknown unary op: " + op);
        }
    }

    private Object evalIsNull(IsNullExpr isn, RowContext ctx) {
        Object val = evalExpr(isn.expr(), ctx);
        // ROW IS NULL: true only if ALL fields are null
        // ROW IS NOT NULL: true only if ALL fields are non-null
        if (val instanceof AstExecutor.PgRow) {
            AstExecutor.PgRow pr = (AstExecutor.PgRow) val;
            if (isn.negated()) {
                return pr.values().stream().allMatch(v -> v != null);
            } else {
                return pr.values().stream().allMatch(v -> v == null);
            }
        }
        boolean isNull = val == null;
        return isn.negated() ? !isNull : isNull;
    }

    private Object evalIsJson(IsJsonExpr ij, RowContext ctx) {
        Object val = evalExpr(ij.expr(), ctx);
        if (val == null) return null; // SQL NULL IS JSON is NULL
        String s = val.toString().trim();
        boolean valid = isValidJson(s);
        if (valid && ij.jsonType() != null) {
            switch (ij.jsonType()) {
                case OBJECT: valid = s.startsWith("{"); break;
                case ARRAY: valid = s.startsWith("["); break;
                case SCALAR: valid = !s.startsWith("{") && !s.startsWith("["); break;
                case VALUE: break; // any JSON
            }
        }
        if (valid && ij.uniqueKeys()) {
            valid = hasUniqueKeys(s);
        }
        return ij.negated() ? !valid : valid;
    }

    static boolean isValidJson(String s) {
        if (s == null || s.isEmpty()) return false;
        s = s.trim();
        if (s.startsWith("{") || s.startsWith("[")) {
            try {
                String normalized = JsonOperations.normalizeJsonb(s);
                // Additional validation: check for nested braces without proper key structure
                // e.g. "{{invalid}}" would normalize to "{}" incorrectly
                if (s.startsWith("{") && s.length() > 2) {
                    String inner = s.substring(1, s.length() - 1).trim();
                    if (!inner.isEmpty() && !inner.contains("\"") && !inner.contains(":")) {
                        return false; // object must have quoted keys with colons
                    }
                }
                return true;
            } catch (Exception e) { return false; }
        }
        // scalar values
        if (s.equals("true") || s.equals("false") || s.equals("null")) return true;
        if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) return true;
        try { Double.parseDouble(s); return true; } catch (NumberFormatException e) {}
        return false;
    }

    private boolean hasUniqueKeys(String s) {
        s = s.trim();
        if (!s.startsWith("{")) return true;
        // Parse keys manually and check for duplicates
        Map<String, String> keys = JsonOperations.parseObjectKeys(s);
        // parseObjectKeys deduplicates, so count raw keys instead
        return countRawKeys(s) == keys.size();
    }

    private int countRawKeys(String s) {
        int count = 0;
        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        boolean expectKey = true;
        for (int i = 1; i < s.length() - 1; i++) {
            char c = s.charAt(i);
            if (escaped) { escaped = false; continue; }
            if (c == '\\') { escaped = true; continue; }
            if (c == '"') {
                if (!inString && depth == 0 && expectKey) {
                    // opening quote of a key
                    count++;
                }
                inString = !inString;
                continue;
            }
            if (inString) continue;
            if (c == '{' || c == '[') depth++;
            else if (c == '}' || c == ']') depth--;
            else if (c == ':' && depth == 0) expectKey = false;
            else if (c == ',' && depth == 0) expectKey = true;
        }
        return count;
    }

    // ---- SQL/JSON standard expression evaluation ----

    private Object evalJsonExists(JsonExistsExpr je, RowContext ctx) {
        Object inputVal = evalExpr(je.input(), ctx);
        if (inputVal == null) return null;
        Object pathVal = evalExpr(je.path(), ctx);
        if (pathVal == null) return null;
        String json = inputVal.toString();
        String path = pathVal.toString().trim();
        try {
            if (!isValidJson(json)) {
                if (je.onError() == JsonExistsExpr.OnBehavior.ERROR) throw new MemgresException("invalid input for JSON_EXISTS", "22032");
                return je.onError() == JsonExistsExpr.OnBehavior.TRUE_VAL ? true : false;
            }
            // Substitute PASSING variables into path
            if (je.passing() != null && !je.passing().isEmpty()) {
                for (Map.Entry<String, Expression> e : je.passing().entrySet()) {
                    Object v = evalExpr(e.getValue(), ctx);
                    if (v != null) path = path.replace("$" + e.getKey(), v.toString());
                }
            }
            List<String> results = executor.functionEvaluator.evaluateJsonPathAll(json, path);
            return !results.isEmpty();
        } catch (MemgresException e) {
            if (je.onError() == JsonExistsExpr.OnBehavior.ERROR) throw e;
            return je.onError() == JsonExistsExpr.OnBehavior.TRUE_VAL ? true : false;
        }
    }

    private Object evalJsonValue(JsonValueExpr jv, RowContext ctx) {
        Object inputVal = evalExpr(jv.input(), ctx);
        if (inputVal == null) return null;
        Object pathVal = evalExpr(jv.path(), ctx);
        if (pathVal == null) return null;
        String json = inputVal.toString();
        String path = pathVal.toString().trim();
        try {
            // Substitute PASSING variables
            if (jv.passing != null && !jv.passing.isEmpty()) {
                for (Map.Entry<String, Expression> e : jv.passing.entrySet()) {
                    Object v = evalExpr(e.getValue(), ctx);
                    if (v != null) path = path.replace("$" + e.getKey(), v.toString());
                }
            }
            List<String> results = executor.functionEvaluator.evaluateJsonPathAll(json, path);
            if (results.isEmpty()) {
                // ON EMPTY behavior
                if (jv.defaultOnEmpty != null) return evalExpr(jv.defaultOnEmpty, ctx);
                return null; // NULL ON EMPTY is default
            }
            String result = results.get(0);
            // JSON_VALUE extracts scalars only — objects/arrays are errors
            String trimmed = result.trim();
            if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
                if (jv.onError == JsonExistsExpr.OnBehavior.ERROR) {
                    throw new MemgresException("JSON_VALUE: non-scalar result", "22032");
                }
                if (jv.defaultOnError != null) return evalExpr(jv.defaultOnError, ctx);
                return null;
            }
            // Unquote strings
            if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
                trimmed = trimmed.substring(1, trimmed.length() - 1);
            }
            if (trimmed.equals("null")) return null;
            // RETURNING type cast
            if (jv.returningType != null) {
                return executor.castEvaluator.applyCast(trimmed, jv.returningType);
            }
            return trimmed;
        } catch (MemgresException e) {
            if (jv.onError == JsonExistsExpr.OnBehavior.ERROR) throw e;
            if (jv.defaultOnError != null) return evalExpr(jv.defaultOnError, ctx);
            return null;
        }
    }

    private Object evalJsonQuery(JsonQueryExpr jq, RowContext ctx) {
        Object inputVal = evalExpr(jq.input(), ctx);
        if (inputVal == null) return null;
        Object pathVal = evalExpr(jq.path(), ctx);
        if (pathVal == null) return null;
        String json = inputVal.toString();
        String path = pathVal.toString().trim();
        try {
            if (!isValidJson(json)) {
                if (jq.onError == JsonExistsExpr.OnBehavior.ERROR) throw new MemgresException("invalid input for JSON_QUERY", "22032");
                return handleJsonQueryOnEmpty(jq);
            }
            List<String> results = executor.functionEvaluator.evaluateJsonPathAll(json, path);
            if (results.isEmpty()) return handleJsonQueryOnEmpty(jq);
            String result = results.get(0);
            // Wrapper behavior
            if (jq.wrapper == JsonQueryExpr.WrapperBehavior.WITH_WRAPPER) {
                if (results.size() == 1) result = "[" + result + "]";
                else result = "[" + String.join(", ", results) + "]";
            } else if (jq.wrapper == JsonQueryExpr.WrapperBehavior.WITH_CONDITIONAL_WRAPPER) {
                String t = result.trim();
                if (!t.startsWith("{") && !t.startsWith("[")) {
                    result = "[" + result + "]";
                }
            }
            // Quotes behavior
            if (jq.quotes == JsonQueryExpr.QuotesBehavior.OMIT) {
                String t = result.trim();
                if (t.startsWith("\"") && t.endsWith("\"")) {
                    result = t.substring(1, t.length() - 1);
                }
            }
            return result;
        } catch (MemgresException e) {
            if (jq.onError == JsonExistsExpr.OnBehavior.ERROR) throw e;
            return null;
        }
    }

    private Object handleJsonQueryOnEmpty(JsonQueryExpr jq) {
        if (jq.onEmpty == JsonExistsExpr.OnBehavior.EMPTY_ARRAY) return "[]";
        if (jq.onEmpty == JsonExistsExpr.OnBehavior.EMPTY_OBJECT) return "{}";
        if (jq.onEmpty == JsonExistsExpr.OnBehavior.ERROR) throw new MemgresException("no SQL/JSON item found for specified path", "22034");
        return null;
    }

    private Object evalIn(InExpr in, RowContext ctx) {
        Object val = evalExpr(in.expr(), ctx);
        if (val == null) return null; // NULL IN (...) is NULL

        // Check for IN (subquery)
        if (in.values().size() == 1 && in.values().get(0) instanceof SubqueryExpr) {
            SubqueryExpr sq = (SubqueryExpr) in.values().get(0);
            if (ctx != null) executor.outerContextStack.push(ctx);
            QueryResult subResult;
            try {
                subResult = executor.executeStatement(sq.subquery());
            } finally {
                if (ctx != null) executor.outerContextStack.pop();
            }
            // Validate column count: (a, b) IN (SELECT c) is an error if c has fewer columns
            if (val instanceof List<?> || val instanceof AstExecutor.PgRow) {
                int expectedCols = val instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) val).values().size() : ((List<?>) val).size();
                if (!subResult.getColumns().isEmpty() && subResult.getColumns().size() < expectedCols) {
                    throw new MemgresException("subquery has too few columns", "42601");
                }
            }
            boolean found = false;
            boolean hasNull = false;
            for (Object[] row : subResult.getRows()) {
                List<?> rowVal = val instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) val).values() : (val instanceof List<?> ? (List<?>) val : null);
                if (rowVal != null && row.length > 1) {
                    // Row value IN (multi-column subquery): compare element by element
                    boolean allMatch = true;
                    for (int ri = 0; ri < Math.min(rowVal.size(), row.length); ri++) {
                        Object lv = rowVal.get(ri), rv = row[ri];
                        if (lv == null || rv == null) { allMatch = false; hasNull = true; break; }
                        if (!TypeCoercion.areEqual(lv, rv)) { allMatch = false; break; }
                    }
                    if (allMatch && rowVal.size() == row.length) { found = true; break; }
                } else {
                    Object elem = row.length > 0 ? row[0] : null;
                    if (elem == null) { hasNull = true; continue; }
                    if (TypeCoercion.areEqual(val, elem)) { found = true; break; }
                }
            }
            if (found) return !in.negated();
            if (hasNull) return null;
            return in.negated();
        }

        // Regular IN (value list)
        boolean found = false;
        boolean hasNull = false;
        for (Expression v : in.values()) {
            Object elem = evalExpr(v, ctx);
            if (elem == null) {
                hasNull = true;
                continue;
            }
            // Type mismatch validation: numeric IN (text), but allow PG array strings
            if (val instanceof Number && elem instanceof String && !((String) elem).isEmpty()
                    && !((String) elem).startsWith("{") && !((String) elem).startsWith("[")) {
                String se = (String) elem;
                try { new java.math.BigDecimal(se); } catch (NumberFormatException e) {
                    throw new MemgresException("operator does not exist: integer = text", "42883");
                }
            }
            // If both val and elem are Lists, compare as row values
            if (val instanceof List<?> && elem instanceof List<?>) {
                if (TypeCoercion.areEqual(val, elem)) { found = true; break; }
                continue;
            }
            // If elem is a List (from ANY(array_column)), check each element
            if (elem instanceof List<?>) {
                List<?> arrayElems = (List<?>) elem;
                for (Object ae : arrayElems) {
                    if (ae == null) { hasNull = true; continue; }
                    if (TypeCoercion.areEqual(val, ae)) { found = true; break; }
                }
                if (found) break;
                continue;
            }
            // If elem is a PG-format array string like {1,5,10} (from array parameters), parse and check
            if (elem instanceof String && ((String) elem).startsWith("{") && ((String) elem).endsWith("}")) {
                String s = (String) elem;
                List<Object> parsed = executor.arrayOperationHandler.parsePostgresArrayLiteral(s);
                for (Object ae : parsed) {
                    if (ae == null) { hasNull = true; continue; }
                    if (TypeCoercion.areEqual(val, ae)) { found = true; break; }
                }
                if (found) break;
                continue;
            }
            if (TypeCoercion.areEqual(val, elem)) {
                found = true;
                break;
            }
        }
        if (found) return !in.negated();
        if (hasNull) return null;
        return in.negated();
    }

    private Object evalBetween(BetweenExpr bet, RowContext ctx) {
        Object val = evalExpr(bet.expr(), ctx);
        Object low = evalExpr(bet.low(), ctx);
        Object high = evalExpr(bet.high(), ctx);
        if (val == null || low == null || high == null) return null;
        boolean inRange;
        if (bet.symmetric()) {
            boolean normalRange = compareValues(val, low) >= 0 && compareValues(val, high) <= 0;
            boolean swappedRange = compareValues(val, high) >= 0 && compareValues(val, low) <= 0;
            inRange = normalRange || swappedRange;
        } else {
            inRange = compareValues(val, low) >= 0 && compareValues(val, high) <= 0;
        }
        return bet.negated() ? !inRange : inRange;
    }

    private Object evalLike(LikeExpr like, RowContext ctx) {
        Object leftVal = evalExpr(like.left(), ctx);
        Object patternVal = evalExpr(like.pattern(), ctx);
        if (leftVal == null || patternVal == null) return null;
        // PG only allows LIKE on text-like types; reject integers, booleans, etc.
        if (leftVal instanceof Number || leftVal instanceof Boolean) {
            String typeName = leftVal instanceof Integer ? "integer" : leftVal instanceof Long ? "bigint" :
                    leftVal instanceof Boolean ? "boolean" : leftVal.getClass().getSimpleName().toLowerCase();
            throw new MemgresException("operator does not exist: " + typeName + " ~~ unknown", "42883");
        }
        String str = leftVal.toString();
        String pat = patternVal.toString();
        String esc = like.escape();
        if (esc != null && esc.length() > 1) {
            throw new MemgresException("invalid escape string", "22025");
        }

        // Build regex from LIKE pattern with escape character support
        StringBuilder regex = new StringBuilder();
        for (int i = 0; i < pat.length(); i++) {
            char ch = pat.charAt(i);
            if (esc != null && !esc.isEmpty() && ch == esc.charAt(0) && i + 1 < pat.length()) {
                // Next character is literal (escaped)
                i++;
                regex.append(java.util.regex.Pattern.quote(String.valueOf(pat.charAt(i))));
            } else if (ch == '%') {
                regex.append(".*");
            } else if (ch == '_') {
                regex.append(".");
            } else {
                regex.append(java.util.regex.Pattern.quote(String.valueOf(ch)));
            }
        }

        String regexStr = like.caseInsensitive() ? "(?i)" + regex : regex.toString();
        boolean matches = str.matches(regexStr);
        return like.negated() ? !matches : matches;
    }

    private Object evalCase(CaseExpr c, RowContext ctx) {
        // PG validates type compatibility at plan time (before short-circuit evaluation).
        validateCaseBranchTypes(c);

        if (c.operand() != null) {
            Object operand = evalExpr(c.operand(), ctx);
            for (CaseExpr.WhenClause when : c.whenClauses()) {
                Object whenVal = evalExpr(when.condition(), ctx);
                if (operand != null && whenVal != null && Objects.equals(operand, whenVal)) {
                    return evalExpr(when.result(), ctx);
                }
            }
        } else {
            for (CaseExpr.WhenClause when : c.whenClauses()) {
                if (isTruthy(evalExpr(when.condition(), ctx))) {
                    return evalExpr(when.result(), ctx);
                }
            }
        }
        return c.elseExpr() != null ? evalExpr(c.elseExpr(), ctx) : null;
    }

    /** Exposed for PREPARE-time validation by SessionExecutor. */
    void validateCaseBranchTypesForPrepare(CaseExpr c) {
        validateCaseBranchTypes(c);
    }

    /** PG validates CASE branch type compatibility at plan time. Reject obvious mismatches. */
    private void validateCaseBranchTypes(CaseExpr c) {
        // PG rejects CASE expressions where all branches return composite (user-defined) types
        if (!c.whenClauses().isEmpty()) {
            boolean allComposite = c.whenClauses().stream()
                    .map(CaseExpr.WhenClause::result)
                    .allMatch(e -> e instanceof CastExpr && executor.database.isCompositeType(((CastExpr) e).typeName().toLowerCase()))
                    && (c.elseExpr() == null
                        || (c.elseExpr() instanceof CastExpr && executor.database.isCompositeType(((CastExpr) c.elseExpr()).typeName().toLowerCase())));
            if (allComposite) {
                Expression firstResult = c.whenClauses().get(0).result();
                String typeName = firstResult instanceof CastExpr ? ((CastExpr) firstResult).typeName() : "record";
                throw new MemgresException(
                        "could not determine polymorphic type because input has type \"" + typeName + "\"", "42P18");
            }
        }
        boolean hasNumericLiteral = false;
        boolean hasNonNumericStringLiteral = false;
        String badValue = null;
        for (CaseExpr.WhenClause when : c.whenClauses()) {
            if (when.result() instanceof Literal) {
                Literal lit = (Literal) when.result();
                if (lit.literalType() == Literal.LiteralType.INTEGER || lit.literalType() == Literal.LiteralType.FLOAT) {
                    hasNumericLiteral = true;
                } else if (lit.literalType() == Literal.LiteralType.STRING) {
                    try { new java.math.BigDecimal(lit.value()); } catch (NumberFormatException e) {
                        hasNonNumericStringLiteral = true;
                        badValue = lit.value();
                    }
                }
            }
        }
        if (c.elseExpr() instanceof Literal) {
            Literal lit = (Literal) c.elseExpr();
            if (lit.literalType() == Literal.LiteralType.INTEGER || lit.literalType() == Literal.LiteralType.FLOAT) {
                hasNumericLiteral = true;
            } else if (lit.literalType() == Literal.LiteralType.STRING) {
                try { new java.math.BigDecimal(lit.value()); } catch (NumberFormatException e) {
                    hasNonNumericStringLiteral = true;
                    badValue = lit.value();
                }
            }
        }
        if (hasNumericLiteral && hasNonNumericStringLiteral) {
            throw new MemgresException("invalid input syntax for type integer: \"" + badValue + "\"", "22P02");
        }
        // Check for composite vs non-composite type mismatch across branches
        boolean hasComposite = false;
        boolean hasNonComposite = false;
        for (CaseExpr.WhenClause when : c.whenClauses()) {
            if (when.result() instanceof CastExpr && executor.database.isCompositeType(((CastExpr) when.result()).typeName().toLowerCase())) {
                CastExpr ce = (CastExpr) when.result();
                hasComposite = true;
            } else {
                hasNonComposite = true;
            }
        }
        if (c.elseExpr() != null) {
            if (c.elseExpr() instanceof CastExpr && executor.database.isCompositeType(((CastExpr) c.elseExpr()).typeName().toLowerCase())) {
                CastExpr ce = (CastExpr) c.elseExpr();
                hasComposite = true;
            } else {
                hasNonComposite = true;
            }
        }
        if (hasComposite && hasNonComposite) {
            throw new MemgresException(
                "CASE types record and text cannot be matched", "42804");
        }
    }

    // ---- Subquery evaluation ----

    private Object evalSubquery(SubqueryExpr sq, RowContext outerCtx) {
        if (outerCtx != null) executor.outerContextStack.push(outerCtx);
        try {
            QueryResult result = executor.executeStatement(sq.subquery());
            if (result.getRows().isEmpty()) return null;
            if (result.getRows().size() > 1) {
                throw new MemgresException("more than one row returned by a subquery used as an expression", "21000");
            }
            Object[] firstRow = result.getRows().get(0);
            if (firstRow.length > 1) {
                throw new MemgresException("subquery must return only one column", "42601");
            }
            if (firstRow.length > 0) return firstRow[0];
            return null;
        } finally {
            if (outerCtx != null) executor.outerContextStack.pop();
        }
    }

    private Object evalExists(ExistsExpr ex, RowContext outerCtx) {
        if (outerCtx != null) executor.outerContextStack.push(outerCtx);
        try {
            QueryResult result = executor.executeStatement(ex.subquery());
            return !result.getRows().isEmpty();
        } finally {
            if (outerCtx != null) executor.outerContextStack.pop();
        }
    }

    private Object evalAnyAll(AnyAllExpr aa, RowContext ctx) {
        Object leftVal = evalExpr(aa.left(), ctx);
        if (leftVal == null) return null;

        if (ctx != null) executor.outerContextStack.push(ctx);
        QueryResult subResult;
        try {
            subResult = executor.executeStatement(aa.subquery());
        } finally {
            if (ctx != null) executor.outerContextStack.pop();
        }

        // Type check: if left is numeric and subquery returns text, reject
        if (leftVal instanceof Number && !subResult.getRows().isEmpty()) {
            Object firstElem = subResult.getRows().get(0).length > 0 ? subResult.getRows().get(0)[0] : null;
            if (firstElem instanceof String && !((String) firstElem).isEmpty()) {
                String s = (String) firstElem;
                try { new java.math.BigDecimal(s); } catch (NumberFormatException e) {
                    String leftType = leftVal instanceof Integer ? "integer" : "bigint";
                    throw new MemgresException("operator does not exist: " + leftType + " " + aa.op().name().toLowerCase().replace("_", " ") + " text", "42883");
                }
            }
        }

        if (aa.isAll()) {
            boolean hasNull = false;
            for (Object[] row : subResult.getRows()) {
                Object elem = row.length > 0 ? row[0] : null;
                if (elem == null) { hasNull = true; continue; }
                if (!evalComparisonOp(aa.op(), leftVal, elem)) return false;
            }
            return hasNull ? null : true;
        } else {
            boolean hasNull = false;
            for (Object[] row : subResult.getRows()) {
                Object elem = row.length > 0 ? row[0] : null;
                if (elem == null) { hasNull = true; continue; }
                if (evalComparisonOp(aa.op(), leftVal, elem)) return true;
            }
            return hasNull ? null : false;
        }
    }

    private Object evalAnyAllArray(AnyAllArrayExpr aaa, RowContext ctx) {
        Object leftVal = evalExpr(aaa.left(), ctx);
        if (leftVal == null) return null;
        Object arrayVal = evalExpr(aaa.array(), ctx);
        if (arrayVal == null) return null;
        List<?> elements;
        if (arrayVal instanceof List<?>) elements = (List<?>) arrayVal;
        else if (arrayVal instanceof String && ((String) arrayVal).startsWith("{") && ((String) arrayVal).endsWith("}")) {
            String s = (String) arrayVal;
            String inner = s.substring(1, s.length() - 1).trim();
            elements = inner.isEmpty() ? Cols.listOf() : java.util.Arrays.asList(inner.split(","));
        } else {
            elements = Cols.listOf(arrayVal);
        }
        if (aaa.isAll()) {
            boolean hasNull = false;
            for (Object elem : elements) {
                if (elem == null) { hasNull = true; continue; }
                Object e = elem instanceof String ? ((String) elem).trim() : elem;
                if (!evalComparisonOp(aaa.op(), leftVal, e)) return false;
            }
            return hasNull ? null : true;
        } else {
            boolean hasNull = false;
            for (Object elem : elements) {
                if (elem == null) { hasNull = true; continue; }
                Object e = elem instanceof String ? ((String) elem).trim() : elem;
                if (evalComparisonOp(aaa.op(), leftVal, e)) return true;
            }
            return hasNull ? null : false;
        }
    }

    private boolean evalComparisonOp(BinaryExpr.BinOp op, Object left, Object right) {
        switch (op) {
            case EQUAL:
                return TypeCoercion.areEqual(left, right);
            case NOT_EQUAL:
                return !TypeCoercion.areEqual(left, right);
            case LESS_THAN:
                return compareValues(left, right) < 0;
            case GREATER_THAN:
                return compareValues(left, right) > 0;
            case LESS_EQUAL:
                return compareValues(left, right) <= 0;
            case GREATER_EQUAL:
                return compareValues(left, right) >= 0;
            default:
                throw new RuntimeException("Unsupported operator in ANY/ALL: " + op);
        }
    }

    // ---- Value comparison and truthiness ----

    @SuppressWarnings("unchecked")
    int compareValues(Object a, Object b) {
        // List (array) comparison: element-by-element, shorter list is "less" if prefix matches
        if (a instanceof List<?> && b instanceof List<?>) {
            List<?> la = (List<?>) a;
            List<?> lb = (List<?>) b;
            int minLen = Math.min(la.size(), lb.size());
            for (int i = 0; i < minLen; i++) {
                int cmp = compareValues(la.get(i), lb.get(i));
                if (cmp != 0) return cmp;
            }
            return Integer.compare(la.size(), lb.size());
        }
        // PgEnum values compare by ordinal position (creation order)
        if (a instanceof AstExecutor.PgEnum && b instanceof AstExecutor.PgEnum) {
            AstExecutor.PgEnum eb = (AstExecutor.PgEnum) b;
            AstExecutor.PgEnum ea = (AstExecutor.PgEnum) a;
            return ea.compareTo(eb);
        }
        // One PgEnum and one String (e.g., WHERE enum_col < 'value'); resolve the string to enum ordinal
        if (a instanceof AstExecutor.PgEnum && b instanceof String) {
            String sb = (String) b;
            AstExecutor.PgEnum ea = (AstExecutor.PgEnum) a;
            CustomEnum ce = executor.database.getCustomEnum(ea.typeName());
            if (ce != null && ce.isValidLabel(sb)) return Integer.compare(ea.ordinal(), ce.ordinal(sb));
        }
        if (b instanceof AstExecutor.PgEnum && a instanceof String) {
            String sa = (String) a;
            AstExecutor.PgEnum eb = (AstExecutor.PgEnum) b;
            CustomEnum ce = executor.database.getCustomEnum(eb.typeName());
            if (ce != null && ce.isValidLabel(sa)) return Integer.compare(ce.ordinal(sa), eb.ordinal());
        }
        return TypeCoercion.compare(a, b);
    }

    boolean isTruthy(Object val) {
        if (val == null) return false;
        if (val instanceof Boolean) return ((Boolean) val);
        if (val instanceof Number) return ((Number) val).doubleValue() != 0;
        if (val instanceof String) return !((String) val).isEmpty() && !((String) val).equalsIgnoreCase("false") && !((String) val).equals("0");
        return true;
    }

    /** Convert a SQL LIKE pattern to a Java regex, properly escaping regex special chars in literal parts. */
    static String likeToRegex(String likePattern) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < likePattern.length(); i++) {
            char c = likePattern.charAt(i);
            if (c == '%') {
                sb.append(".*");
            } else if (c == '_') {
                sb.append(".");
            } else {
                // Escape regex special characters
                if ("\\[]{}()^$.|*+?".indexOf(c) >= 0) {
                    sb.append('\\');
                }
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /** Strict truthiness check that distinguishes null from false. */
    boolean isTruthyStrict(Object val) {
        if (val == null) return false;
        return isTruthy(val);
    }

    // ---- Numeric operations ----

    Object numericOp(Object left, Object right,
                             java.util.function.BiFunction<Double, Double, Double> doubleOp,
                             java.util.function.BiFunction<Long, Long, Long> longOp) {
        return numericOp(left, right, doubleOp, longOp, null);
    }

    Object numericOp(Object left, Object right,
                             java.util.function.BiFunction<Double, Double, Double> doubleOp,
                             java.util.function.BiFunction<Long, Long, Long> longOp,
                             java.util.function.BiFunction<java.math.BigDecimal, java.math.BigDecimal, java.math.BigDecimal> bdOp) {
        if (left == null || right == null) return null;
        // Coerce strings to numeric before dispatch (PG implicit coercion)
        if (left instanceof String && right instanceof Number) {
            String s = (String) left;
            try { left = Integer.parseInt(s); } catch (NumberFormatException e1) {
                try { left = Long.parseLong(s); } catch (NumberFormatException e2) {
                    try { left = new java.math.BigDecimal(s); } catch (NumberFormatException e3) { /* leave as string */ }
                }
            }
        }
        if (right instanceof String && left instanceof Number) {
            String s = (String) right;
            try { right = Integer.parseInt(s); } catch (NumberFormatException e1) {
                try { right = Long.parseLong(s); } catch (NumberFormatException e2) {
                    try { right = new java.math.BigDecimal(s); } catch (NumberFormatException e3) { /* leave as string */ }
                }
            }
        }
        // BigDecimal arithmetic, preserve precision
        if (left instanceof java.math.BigDecimal || right instanceof java.math.BigDecimal) {
            java.math.BigDecimal l = TypeCoercion.toBigDecimal(left);
            java.math.BigDecimal r = TypeCoercion.toBigDecimal(right);
            if (bdOp != null) return bdOp.apply(l, r);
            return java.math.BigDecimal.valueOf(doubleOp.apply(l.doubleValue(), r.doubleValue()));
        }
        // Smallint arithmetic, check for overflow
        if (left instanceof Short && right instanceof Short) {
            try {
                long result = longOp.apply((long)(short)(Short)left, (long)(short)(Short)right);
                if (result >= Short.MIN_VALUE && result <= Short.MAX_VALUE) return (short) result;
                throw new MemgresException("smallint out of range", "22003");
            } catch (ArithmeticException e) {
                if (e.getMessage() != null && e.getMessage().contains("/ by zero"))
                    throw new MemgresException("division by zero", "22012");
                throw new MemgresException("smallint out of range", "22003");
            }
        }
        if (left instanceof Integer && right instanceof Integer) {
            try {
                long result = longOp.apply((long)(int)left, (long)(int)right);
                if (result >= Integer.MIN_VALUE && result <= Integer.MAX_VALUE) return (int) result;
                throw new MemgresException("integer out of range", "22003");
            } catch (ArithmeticException e) {
                if (e.getMessage() != null && e.getMessage().contains("/ by zero"))
                    throw new MemgresException("division by zero", "22012");
                throw new MemgresException("integer out of range", "22003");
            }
        }
        if ((left instanceof Integer || left instanceof Long) && (right instanceof Integer || right instanceof Long)) {
            try {
                return longOp.apply(toLong(left), toLong(right));
            } catch (ArithmeticException e) {
                if (e.getMessage() != null && e.getMessage().contains("/ by zero"))
                    throw new MemgresException("division by zero", "22012");
                throw new MemgresException("bigint out of range", "22003");
            }
        }
        double result = doubleOp.apply(toDouble(left), toDouble(right));
        // Check for overflow: non-infinite inputs producing infinite result
        if (Double.isInfinite(result) && !Double.isInfinite(toDouble(left)) && !Double.isInfinite(toDouble(right))) {
            throw new MemgresException("value out of range: overflow", "22003");
        }
        return result;
    }

    double toDouble(Object val) {
        if (val instanceof Number) return ((Number) val).doubleValue();
        if (val instanceof String) {
            String s = (String) val;
            try { return Double.parseDouble(s); }
            catch (NumberFormatException e) { throw new MemgresException("invalid input syntax for type double precision: \"" + s + "\"", "22P02"); }
        }
        return 0;
    }

    int toInt(Object val) {
        if (val instanceof Number) return ((Number) val).intValue();
        if (val instanceof String) {
            String s = (String) val;
            try { return Integer.parseInt(s); }
            catch (NumberFormatException e) { throw new MemgresException("invalid input syntax for type integer: \"" + s + "\"", "22P02"); }
        }
        return 0;
    }

    long toLong(Object val) {
        if (val == null) return 0;
        if (val instanceof Number) return ((Number) val).longValue();
        return Long.parseLong(val.toString());
    }

    // ---- Bit string operations ----

    /** Extract bit string content from PgBitString or a plain String of 0s and 1s, or return null. */
    static String toBitStringOrNull(Object val) {
        if (val instanceof AstExecutor.PgBitString) return ((AstExecutor.PgBitString) val).bits();
        if (val instanceof String && !((String) val).isEmpty() && ((String) val).chars().allMatch(c -> c == '0' || c == '1')) return ((String) val);
        return null;
    }

    /** Perform character-by-character bitwise operation on bit strings (0s and 1s). */
    static String bitwiseBitString(String a, String b, char op) {
        if (a.length() != b.length()) {
            String opName;
            switch (op) {
                case '&':
                    opName = "AND";
                    break;
                case '|':
                    opName = "OR";
                    break;
                case '#':
                    opName = "XOR";
                    break;
                default:
                    opName = "AND/OR/XOR";
                    break;
            }
            throw new MemgresException("cannot " + opName + " bit strings of different sizes", "22026");
        }
        int maxLen = a.length();
        StringBuilder sb = new StringBuilder(maxLen);
        for (int i = 0; i < maxLen; i++) {
            int ba = a.charAt(i) - '0';
            int bb = b.charAt(i) - '0';
            int result;
            switch (op) {
                case '&':
                    result = ba & bb;
                    break;
                case '|':
                    result = ba | bb;
                    break;
                case '#':
                    result = ba ^ bb;
                    break;
                default:
                    result = 0;
                    break;
            }
            sb.append(result);
        }
        return sb.toString();
    }

    // ---- Expression alias derivation ----

    String exprToAlias(Expression expr) {
        if (expr instanceof ColumnRef) return ((ColumnRef) expr).column();
        if (expr instanceof FunctionCallExpr) return ((FunctionCallExpr) expr).name();
        if (expr instanceof WindowFuncExpr) return ((WindowFuncExpr) expr).name();
        if (expr instanceof AtTimeZoneExpr) return "timezone";
        if (expr instanceof FieldAccessExpr) return ((FieldAccessExpr) expr).field();
        if (expr instanceof SubqueryExpr) {
            SubqueryExpr sq = (SubqueryExpr) expr;
            if (sq.subquery() instanceof SelectStmt && ((SelectStmt) sq.subquery()).targets() != null && !((SelectStmt) sq.subquery()).targets().isEmpty()) {
                SelectStmt sel = (SelectStmt) sq.subquery();
                SelectStmt.SelectTarget inner = sel.targets().get(0);
                if (inner.alias() != null) return inner.alias();
                return exprToAlias(inner.expr());
            }
            return "?column?";
        }
        if (expr instanceof ArraySubqueryExpr) return "array";
        if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            String inner = exprToAlias(cast.expr());
            return "?column?".equals(inner) ? castTypeToColumnName(cast.typeName()) : inner;
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            if (bin.op() == BinaryExpr.BinOp.JSON_ARROW && bin.left() instanceof ColumnRef) {
                ColumnRef cr = (ColumnRef) bin.left();
                return cr.column();
            }
            return "?column?";
        }
        if (expr instanceof CaseExpr) return "case";
        if (expr instanceof ArrayExpr) return ((ArrayExpr) expr).isRow() ? "row" : "array";
        if (expr instanceof ExistsExpr) return "exists";
        if (expr instanceof Literal) return "?column?";
        if (expr instanceof CollateExpr) return exprToAlias(((CollateExpr) expr).expr());
        if (expr instanceof CompositeStarExpr) return "?column?";
        if (expr instanceof ArraySliceExpr) return exprToAlias(((ArraySliceExpr) expr).array());
        return "?column?";
    }

    /** Map a cast type name to the PG column name (e.g. "int" -> "int4", "boolean" -> "bool"). */
    private String castTypeToColumnName(String typeName) {
        String base = typeName.replaceAll("\\[\\]", "").replaceAll("\\(.*\\)", "").trim();
        if (base.endsWith("[]")) base = base.substring(0, base.length() - 2).trim();
        String baseLower = base.toLowerCase();
        if (baseLower.equals("time with time zone") || baseLower.equals("timetz")) {
            return "timetz";
        }
        try {
            DataType dt = DataType.fromPgName(base);
            if (dt == null) return base;
            return dt.getPgName();
        } catch (Exception e) {
            return base;
        }
    }

    // ---- Type inference ----

    DataType inferTypeFromContext(Expression expr, List<RowContext.TableBinding> bindings) {
        if (expr instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) expr;
            for (RowContext.TableBinding b : bindings) {
                if (ref.table() != null) {
                    if (!ref.table().equalsIgnoreCase(b.alias()) &&
                            !ref.table().equalsIgnoreCase(b.table().getName())) continue;
                }
                int idx = b.table().getColumnIndex(ref.column());
                if (idx >= 0) return b.table().getColumns().get(idx).getType();
            }
            return DataType.TEXT;
        }
        if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            DataType dt = DataType.fromPgName(cast.typeName().replaceAll("\\(.*\\)", "").trim());
            return dt != null ? dt : DataType.TEXT;
        }
        if (expr instanceof Literal) {
            Literal lit = (Literal) expr;
            switch (lit.literalType()) {
                case INTEGER:
                    return DataType.INTEGER;
                case FLOAT:
                    return lit.value().contains("e") || lit.value().contains("E") ? DataType.DOUBLE_PRECISION : DataType.NUMERIC;
                case STRING:
                    return DataType.TEXT;
                case BOOLEAN:
                    return DataType.BOOLEAN;
                case BIT_STRING:
                    return DataType.BIT;
                case NULL:
                    return DataType.TEXT;
                case DEFAULT:
                    return DataType.TEXT;
            }
        }
        if (expr instanceof UnaryExpr) {
            UnaryExpr un = (UnaryExpr) expr;
            if (un.op() == UnaryExpr.UnaryOp.NEGATE) {
                DataType inner = inferTypeFromContext(un.operand(), bindings);
                return inner;
            }
            return DataType.TEXT;
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            // For arithmetic ops, infer from operands
            if (bin.op() == BinaryExpr.BinOp.ADD || bin.op() == BinaryExpr.BinOp.SUBTRACT
                    || bin.op() == BinaryExpr.BinOp.MULTIPLY || bin.op() == BinaryExpr.BinOp.DIVIDE
                    || bin.op() == BinaryExpr.BinOp.MODULO) {
                DataType leftType = inferTypeFromContext(bin.left(), bindings);
                DataType rightType = inferTypeFromContext(bin.right(), bindings);
                // If either is NUMERIC, result is NUMERIC (except double wins)
                if (leftType == DataType.DOUBLE_PRECISION || rightType == DataType.DOUBLE_PRECISION)
                    return DataType.DOUBLE_PRECISION;
                if (leftType == DataType.NUMERIC || rightType == DataType.NUMERIC)
                    return DataType.NUMERIC;
                if (leftType == DataType.BIGINT || rightType == DataType.BIGINT)
                    return DataType.BIGINT;
                return DataType.INTEGER;
            }
            // Comparison and logical ops return BOOLEAN
            if (bin.op() == BinaryExpr.BinOp.AND || bin.op() == BinaryExpr.BinOp.OR
                    || bin.op() == BinaryExpr.BinOp.EQUAL || bin.op() == BinaryExpr.BinOp.NOT_EQUAL
                    || bin.op() == BinaryExpr.BinOp.LESS_THAN || bin.op() == BinaryExpr.BinOp.GREATER_THAN
                    || bin.op() == BinaryExpr.BinOp.LESS_EQUAL || bin.op() == BinaryExpr.BinOp.GREATER_EQUAL) {
                return DataType.BOOLEAN;
            }
            // Concatenation: bytea || bytea returns bytea, otherwise text
            if (bin.op() == BinaryExpr.BinOp.CONCAT) {
                DataType lt = inferTypeFromContext(bin.left(), bindings);
                DataType rt = inferTypeFromContext(bin.right(), bindings);
                if (lt == DataType.BYTEA || rt == DataType.BYTEA) return DataType.BYTEA;
                return DataType.TEXT;
            }
            return DataType.TEXT;
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            String name = fn.name().toLowerCase();
            if (name.equals("count") || name.equals("length") || name.equals("char_length")
                    || name.equals("octet_length") || name.equals("bit_length")
                    || name.equals("position") || name.equals("strpos")
                    || name.equals("array_length") || name.equals("cardinality")
                    || name.equals("array_position")) return DataType.INTEGER;
            if (name.equals("sum") || name.equals("avg")) return DataType.NUMERIC;
            if (name.equals("max") || name.equals("min")) {
                if (!fn.args().isEmpty()) return inferTypeFromContext(fn.args().get(0), bindings);
                return DataType.TEXT;
            }
            if (name.equals("lower") || name.equals("upper") || name.equals("trim")
                    || name.equals("ltrim") || name.equals("rtrim") || name.equals("replace")
                    || name.equals("substring") || name.equals("concat")
                    || name.equals("concat_ws") || name.equals("left") || name.equals("right")
                    || name.equals("repeat") || name.equals("reverse")
                    || name.equals("md5") || name.equals("to_char") || name.equals("initcap")
                    || name.equals("translate") || name.equals("chr") || name.equals("format")
                    || name.equals("lpad") || name.equals("rpad") || name.equals("overlay")
                    || name.equals("string_agg") || name.equals("regexp_replace")
                    || name.equals("pg_typeof")) return DataType.TEXT;
            if (name.equals("now") || name.equals("current_timestamp")
                    || name.equals("statement_timestamp") || name.equals("clock_timestamp")
                    || name.equals("transaction_timestamp")) return DataType.TIMESTAMPTZ;
            if (name.equals("current_date") || name.equals("date_trunc")
                    || name.equals("to_date") || name.equals("make_date")) return DataType.DATE;
            if (name.equals("coalesce") || name.equals("nullif") || name.equals("greatest") || name.equals("least")) {
                if (!fn.args().isEmpty()) return inferTypeFromContext(fn.args().get(0), bindings);
                return DataType.TEXT;
            }
            if (name.equals("bool_and") || name.equals("bool_or") || name.equals("every")
                    || name.equals("has_database_privilege") || name.equals("has_schema_privilege")
                    || name.equals("has_table_privilege") || name.equals("has_column_privilege")
                    || name.equals("has_function_privilege") || name.equals("has_type_privilege")
                    || name.equals("has_sequence_privilege") || name.equals("has_any_column_privilege")
                    || name.equals("has_foreign_data_wrapper_privilege") || name.equals("has_server_privilege")
                    || name.equals("has_tablespace_privilege")
                    || name.equals("pg_is_in_recovery") || name.equals("pg_is_wal_replay_paused")
                    || name.equals("pg_has_role")) return DataType.BOOLEAN;
            if (name.equals("abs") || name.equals("ceil") || name.equals("floor")
                    || name.equals("round") || name.equals("trunc") || name.equals("sign")
                    || name.equals("mod") || name.equals("power") || name.equals("sqrt")
                    || name.equals("cbrt") || name.equals("exp") || name.equals("ln")
                    || name.equals("log") || name.equals("div")) return DataType.NUMERIC;
            if (name.equals("random") || name.equals("pi") || name.equals("degrees")
                    || name.equals("radians") || name.equals("sin") || name.equals("cos")
                    || name.equals("tan") || name.equals("asin") || name.equals("acos")
                    || name.equals("atan") || name.equals("atan2")) return DataType.DOUBLE_PRECISION;
            if (name.equals("array_agg")) return DataType.TEXT; // array type, but TEXT as fallback
            if (name.equals("row_number") || name.equals("rank") || name.equals("dense_rank")
                    || name.equals("ntile")) return DataType.BIGINT;
            if (name.equals("lag") || name.equals("lead") || name.equals("first_value")
                    || name.equals("last_value") || name.equals("nth_value")) {
                if (!fn.args().isEmpty()) return inferTypeFromContext(fn.args().get(0), bindings);
                return DataType.TEXT;
            }
            if (name.equals("uuid_generate_v4") || name.equals("gen_random_uuid")) return DataType.UUID;
            return DataType.TEXT;
        }
        if (expr instanceof IsNullExpr) return DataType.BOOLEAN;
        if (expr instanceof InExpr) return DataType.BOOLEAN;
        if (expr instanceof BetweenExpr) return DataType.BOOLEAN;
        if (expr instanceof LikeExpr) return DataType.BOOLEAN;
        if (expr instanceof ExistsExpr) return DataType.BOOLEAN;
        if (expr instanceof AnyAllExpr) return DataType.BOOLEAN;
        if (expr instanceof AnyAllArrayExpr) return DataType.BOOLEAN;
        if (expr instanceof IsBooleanExpr) return DataType.BOOLEAN;
        if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            if (!c.whenClauses().isEmpty()) {
                return inferTypeFromContext(c.whenClauses().get(0).result(), bindings);
            }
            if (c.elseExpr() != null) return inferTypeFromContext(c.elseExpr(), bindings);
            return DataType.TEXT;
        }
        if (expr instanceof SubqueryExpr) {
            SubqueryExpr sq = (SubqueryExpr) expr;
            if (sq.subquery() instanceof SelectStmt && ((SelectStmt) sq.subquery()).targets() != null && !((SelectStmt) sq.subquery()).targets().isEmpty()) {
                SelectStmt stmt = (SelectStmt) sq.subquery();
                return inferTypeFromContext(stmt.targets().get(0).expr(), bindings);
            }
            return DataType.TEXT;
        }
        if (expr instanceof ArrayExpr) return DataType.TEXT;
        return DataType.TEXT;
    }

    DataType inferExprType(Expression expr) {
        return inferTypeFromContext(expr, Cols.listOf());
    }

    // ---- JSON path parsing ----

    List<String> parseJsonPathArg(Object right) {
        if (right instanceof List<?>) {
            List<?> list = (List<?>) right;
            return list.stream().map(Object::toString).collect(Collectors.toList());
        }
        String s = right.toString().trim();
        if (s.startsWith("{") && s.endsWith("}")) {
            String inner = s.substring(1, s.length() - 1);
            return Arrays.asList(inner.split(","));
        }
        return Cols.listOf(s);
    }
}
