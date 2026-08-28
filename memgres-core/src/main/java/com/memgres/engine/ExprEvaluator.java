package com.memgres.engine;

import com.memgres.engine.util.Cols;
import com.memgres.engine.plpgsql.PlpgsqlExecutor;

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
    final SqlJsonEvaluator sqlJson;

    ExprEvaluator(AstExecutor executor) {
        this.executor = executor;
        this.sqlJson = new SqlJsonEvaluator(executor);
    }

    /**
     * Wraps an already-evaluated Java value so it can be threaded back through the normal
     * expression evaluation machinery (e.g. as a synthetic argument to {@link FunctionCallExpr}),
     * without re-parsing/re-evaluating it. Used by the attribute-notation fallback below.
     */
    static final class PrecomputedValueExpr implements Expression {
        private final Object value;
        /**
         * The type the replaced expression was declared to have, where the value alone cannot
         * say: a numeric NaN and a float8 NaN are the same Double. Null when unknown.
         */
        private final DataType declaredType;
        PrecomputedValueExpr(Object value) { this(value, null); }
        PrecomputedValueExpr(Object value, DataType declaredType) {
            this.value = value;
            this.declaredType = declaredType;
        }
        Object value() { return value; }
        DataType declaredType() { return declaredType; }
    }

    // ---- Main expression dispatcher ----

    /**
     * Countdown to the next cancellation poll. Every row of every scan passes through here, so
     * this is where a runaway query notices statement_timeout; polling on each expression would
     * cost more than it is worth, and a few thousand evaluations is still sub-millisecond.
     */
    private int cancelPollCountdown;

    /**
     * The calls in the expression being evaluated that were answered before evaluation began,
     * and what each of them answered. Identity-keyed: it is the node in this expression that was
     * computed, not any other node spelled the same.
     *
     * <p>An aggregate call is computed over a whole group and a window call over a whole
     * partition, so neither is computed from the row in front of it. What is left is the
     * expression the call is written inside, and that is an ordinary expression over the values
     * they came to. Evaluating it here rather than in a second evaluator written alongside is
     * what makes every place such a call may be written work the same: the evaluators written
     * alongside knew only the node types somebody had listed, so a call under BETWEEN, in an IN
     * list or inside ARRAY[] fell through to ordinary evaluation, where the call has no value,
     * and the row answered NULL -- or, in HAVING, no row at all.
     */
    private Map<Expression, PrecomputedValueExpr> foldedValues;

    /**
     * The calls written inside a nested query that belong to an enclosing query level, and what
     * each of them answered there.
     *
     * <p>PostgreSQL puts an aggregate at the query level its argument variables come from, which
     * is not always the level it is written at: {@code SELECT (SELECT count(a.v)) FROM t a}
     * counts the rows of {@code t}, once, because {@code a.v} comes from the outer query. Such a
     * call is answered over the outer group before the nested query runs at all, and the nested
     * query reads the answer.
     *
     * <p>Kept apart from {@link #foldedValues} because the nested query folds calls of its own as
     * it runs, and one belonging to a level above has to stay answered while it does.
     */
    private Map<Expression, PrecomputedValueExpr> levelFolded;

    /** Installs the enclosing level's answers, returning what was there to be put back. */
    Map<Expression, PrecomputedValueExpr> swapLevelFolded(
            Map<Expression, PrecomputedValueExpr> next) {
        Map<Expression, PrecomputedValueExpr> prior = levelFolded;
        levelFolded = next;
        return prior;
    }

    /** What an enclosing level answered for this call, or null where no level has. */
    PrecomputedValueExpr levelFoldedFor(Expression expr) {
        return levelFolded == null ? null : levelFolded.get(expr);
    }

    /** Every such answer standing at the moment, or null where there are none. */
    Map<Expression, PrecomputedValueExpr> levelFoldedAnswers() {
        return levelFolded;
    }

    /**
     * Evaluate an expression whose aggregate or window calls have already been answered.
     *
     * <p>A nested query may fold calls of its own while this one is part-way through a row, so
     * what was in hand is put back rather than dropped.
     */
    Object evalWithFolded(Map<Expression, PrecomputedValueExpr> folded, Expression expr,
                          RowContext ctx) {
        Map<Expression, PrecomputedValueExpr> prior = foldedValues;
        foldedValues = folded;
        try {
            return evalExpr(expr, ctx);
        } finally {
            foldedValues = prior;
        }
    }

    /**
     * What this call was folded to, or null where it was not folded.
     *
     * <p>A folded value carries the type the call it replaced was declared to have, which the
     * value alone cannot always say -- and which is the only answer left where the fold produced
     * NULL.
     */
    PrecomputedValueExpr foldedFor(Expression expr) {
        return foldedValues == null ? null : foldedValues.get(expr);
    }

    public Object evalExpr(Expression expr, RowContext ctx) {
        if (--cancelPollCountdown <= 0) {
            cancelPollCountdown = 4096;
            StatementCancel.check();
        }
        if (foldedValues != null) {
            PrecomputedValueExpr folded = foldedValues.get(expr);
            if (folded != null) return folded.value();
        }
        if (levelFolded != null) {
            PrecomputedValueExpr answered = levelFolded.get(expr);
            if (answered != null) return answered.value();
        }
        if (expr instanceof Literal) return evalLiteral(((Literal) expr));
        // A value the engine computed earlier stands for itself.
        if (expr instanceof ComputedValue) return ((ComputedValue) expr).value();
        if (expr instanceof PrecomputedValueExpr) return ((PrecomputedValueExpr) expr).value();
        // A node whose value the caller has already settled reads that value instead of being
        // computed again: an expanded set-returning call bound to one of its elements (see
        // SelectExecutor.findSrfCall / projectRows), or a window function's input over a grouped
        // query, bound to the value the group has for it. See RowContext.setBoundValue.
        if (ctx != null && ctx.hasBoundValue(expr)) return ctx.getBoundValue(expr);
        if (expr instanceof ColumnRef) return evalColumnRef(((ColumnRef) expr), ctx);
        if (expr instanceof BinaryExpr) {
            rejectUnequalRowArity((BinaryExpr) expr);
            if (isRowSubqueryComparison((BinaryExpr) expr)) {
                return evalRowSubqueryComparison((BinaryExpr) expr, ctx);
            }
            return executor.binaryOpEvaluator.evalBinary(((BinaryExpr) expr), ctx);
        }
        if (expr instanceof UnaryExpr) {
            UnaryExpr un = (UnaryExpr) expr;
            rejectUnresolvablePrefixOperator(un);
            return evalUnaryValue(un.op(), evalExpr(un.operand(), ctx));
        }
        if (expr instanceof FunctionCallExpr) {
            unifyVariadicArgumentTypes((FunctionCallExpr) expr);
            return executor.functionEvaluator.evalFunction(((FunctionCallExpr) expr), ctx);
        }
        if (expr instanceof CastExpr) return evalCast(((CastExpr) expr), ctx);
        if (expr instanceof IsNullExpr) return evalIsNull(((IsNullExpr) expr), ctx);
        if (expr instanceof IsJsonExpr) return evalIsJson(((IsJsonExpr) expr), ctx);
        if (expr instanceof JsonExistsExpr) return evalJsonExists(((JsonExistsExpr) expr), ctx);
        if (expr instanceof JsonValueExpr) return evalJsonValue(((JsonValueExpr) expr), ctx);
        if (expr instanceof JsonQueryExpr) return evalJsonQuery(((JsonQueryExpr) expr), ctx);
        if (expr instanceof RowElementExpr) return evalRowElement(((RowElementExpr) expr), ctx);
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
        if (expr instanceof WildcardExpr) {
            WildcardExpr wc = (WildcardExpr) expr;
            if (wc.table() != null && ctx != null) {
                RowContext.TableBinding b = ctx.getBinding(wc.table());
                // A star standing where a value is expected reads a whole row, and the row it
                // names may belong to a query this one is written inside: count(o.*) in a
                // subquery counts the outer row, because that is the row PostgreSQL resolves the
                // name against once the subquery's own FROM clause has not got it.
                for (Iterator<RowContext> it = executor.outerContextStack.descendingIterator();
                        b == null && it.hasNext(); ) {
                    b = it.next().getBinding(wc.table());
                }
                if (b != null) {
                    // Return the full row as a List so IS DISTINCT FROM can compare row tuples
                    java.util.List<Object> rowList = new java.util.ArrayList<>(b.row().length);
                    for (Object v : b.row()) rowList.add(v);
                    return rowList;
                }
            }
            return null;
        }
        if (expr instanceof SubqueryExpr) return evalSubquery(((SubqueryExpr) expr), ctx);
        if (expr instanceof ExistsExpr) return evalExists(((ExistsExpr) expr), ctx);
        if (expr instanceof AnyAllExpr) return evalAnyAll(((AnyAllExpr) expr), ctx);
        if (expr instanceof AnyAllArrayExpr) return evalAnyAllArray(((AnyAllArrayExpr) expr), ctx);
        if (expr instanceof ArrayExpr) return executor.arrayOperationHandler.evalArray(((ArrayExpr) expr), ctx);
        if (expr instanceof ArraySubqueryExpr) return executor.arrayOperationHandler.evalArraySubquery(((ArraySubqueryExpr) expr), ctx);
        if (expr instanceof AtTimeZoneExpr) {
            AtTimeZoneExpr attz = (AtTimeZoneExpr) expr;
            Object val = evalExpr(attz.expr(), ctx);
            ZoneId zid;
            if (attz.zone() == null) {
                // AT LOCAL (PG 17): the same conversion, against the session's TimeZone
                zid = TypeCoercion.sessionZone();
                if (val == null) return null;
            } else {
                Object zoneVal = evalExpr(attz.zone(), ctx);
                if (val == null) return null;
                String zoneName = zoneVal == null ? "null" : zoneVal.toString();
                try {
                    zid = ZoneId.of(zoneName);
                } catch (java.time.DateTimeException e) {
                    throw new MemgresException("time zone \"" + zoneName + "\" not recognized", "22023");
                }
            }
            if (val instanceof OffsetDateTime) {
                OffsetDateTime odt = (OffsetDateTime) val;
                // timestamptz -> timestamp (in that zone)
                return odt.atZoneSameInstant(zid).toLocalDateTime();
            } else if (val instanceof LocalDateTime) {
                LocalDateTime ldt = (LocalDateTime) val;
                // timestamp -> timestamptz (interpret as in that zone)
                return ldt.atZone(zid).toOffsetDateTime();
            } else if (val instanceof java.time.LocalDate) {
                // A date reaches the operator as a timestamptz at midnight, and converting it
                // back into the same zone lands on that same midnight.
                return ((java.time.LocalDate) val).atStartOfDay();
            } else if (val instanceof LocalTime || TypeCoercion.looksLikeTimeTz(val)) {
                // timetz keeps its instant and changes which offset it is written against; a
                // plain time reaches the operator having already taken the session's offset.
                return TypeCoercion.shiftTimeTzToZone(val, zid);
            }
            return val;
        }
        if (expr instanceof WindowFuncExpr) {
            // Answered above where a window pass computed this call; a window function has no
            // value of its own to compute from the row in front of it.
            return null;
        }
        if (expr instanceof NamedArgExpr) return evalExpr(((NamedArgExpr) expr).value(), ctx);
        if (expr instanceof IsBooleanExpr) {
            IsBooleanExpr ib = (IsBooleanExpr) expr;
            Object val = evalExpr(ib.expr(), ctx);
            // These tests read a three-valued boolean, so a value that is not one has no answer:
            // PG names the test and the type it was handed rather than coercing. Which of the two
            // errors it is depends on the declaration, not the value: a declared text is 42804
            // while an untyped literal is coerced to boolean and fails on its own input.
            if (isBooleanTest(ib.test())) {
                String declared = executor.binaryOpEvaluator.declaredTypeForResolution(ib.expr(), ctx);
                if (declared != null) {
                    String canonical = DataType.canonicalName(declared);
                    if (!"boolean".equals(canonical)) {
                        throw new MemgresException("argument of " + booleanTestName(ib.test())
                                + " must be type boolean, not type " + canonical, "42804");
                    }
                } else if (val instanceof String) {
                    TypeCoercion.toBoolean(val);
                }
            }
            if (val != null && isBooleanTest(ib.test())
                    && !(val instanceof Boolean) && !(val instanceof String)) {
                throw new MemgresException("argument of " + booleanTestName(ib.test())
                        + " must be type boolean, not type " + AstExecutor.pgTypeNameOf(val), "42804");
            }
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
                // Whether nothing is a document is not a question with a yes or a no.
                case IS_DOCUMENT: {
                    Object raw = evalExpr(ib.expr(), ctx);
                    return raw == null ? null
                            : Boolean.valueOf(XmlOperations.isDocument(raw.toString()));
                }
                case IS_NOT_DOCUMENT: {
                    Object raw = evalExpr(ib.expr(), ctx);
                    return raw == null ? null
                            : Boolean.valueOf(!XmlOperations.isDocument(raw.toString()));
                }
            }
        }
        if (expr instanceof FieldAccessExpr) {
            FieldAccessExpr fa = (FieldAccessExpr) expr;
            // A relation's own name standing where a value is wanted is the whole row, so both
            // (t).c and (t.*).c read column c of t. Reading the name as a column instead found
            // nothing to take a field from and the query failed on a shape PostgreSQL accepts.
            if (ctx != null) {
                String relation = null;
                if (fa.expr() instanceof ColumnRef && ((ColumnRef) fa.expr()).table() == null) {
                    relation = ((ColumnRef) fa.expr()).column();
                } else if (fa.expr() instanceof WildcardExpr) {
                    relation = ((WildcardExpr) fa.expr()).table();
                }
                if (relation != null && ctx.getBinding(relation) != null) {
                    return ctx.resolveColumn(relation, fa.field());
                }
            }
            // A function declared to return bare "record" carries no column names, so there is
            // nothing for (f()).x to match — PG says so rather than evaluating the call.
            if (fa.expr() instanceof FunctionCallExpr) {
                PgFunction pf = executor.database.getFunction(FunctionEvaluator.stripSchemaPrefix(
                        ((FunctionCallExpr) fa.expr()).name().toLowerCase()));
                if (pf != null && pf.declaresRecordResult() && !pf.hasOutParams()) {
                    throw new MemgresException("could not identify column \"" + fa.field()
                            + "\" in record data type", "42703");
                }
            }
            // Composite field access: (expr).field
            Object val = evalExpr(fa.expr(), ctx);
            if (val == null) return null;
            String fieldName = fa.field();

            if (val instanceof RecordValue) {
                // A record whose column names come from the call that built it, e.g. jsonb_each
                RecordValue record = (RecordValue) val;
                int idx = record.indexOf(fieldName);
                if (idx < 0) {
                    throw new MemgresException("could not identify column \"" + fieldName
                            + "\" in record data type", "42703");
                }
                return record.valueAt(idx);
            }
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
                    List<CreateTypeStmt.CompositeField> fields = executor.database.getRowType(typeName);
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
                // An untyped ROW names its fields by position -- f1 is the first value, f2 the
                // second -- so the number in the name says which value was asked for, and a name
                // that numbers no value of this row identifies no column.
                if (fieldName.length() > 1 && (fieldName.charAt(0) == 'f' || fieldName.charAt(0) == 'F')) {
                    int index;
                    try {
                        index = Integer.parseInt(fieldName.substring(1));
                    } catch (NumberFormatException e) {
                        index = -1; // not a position after all, so no field of that name
                    }
                    if (index >= 1 && index <= row.values().size()) {
                        // A field written as a bare string literal has no type of its own, and
                        // PostgreSQL has no output function to write an unknown out with.
                        if (fa.expr() instanceof ArrayExpr
                                && isUnknownLiteral(((ArrayExpr) fa.expr()).elements().get(index - 1))) {
                            throw new MemgresException(
                                    "failed to find conversion function from unknown to text", "XX000");
                        }
                        return row.values().get(index - 1);
                    }
                }
                throw new MemgresException("could not identify column \"" + fieldName + "\" in record data type", "42703");
            }

            // If the value is a String representation of a composite "(val1,val2,...)"
            if (val instanceof String && ((String) val).startsWith("(") && ((String) val).endsWith(")")) {
                String s = (String) val;
                if (typeName != null) {
                    List<CreateTypeStmt.CompositeField> fields = executor.database.getRowType(typeName);
                    if (fields != null) {
                        // The literal is read by the composite's own reader, which is the only
                        // thing that knows which quotes were structure and which were content.
                        List<RecordLiteral.Field> parts = RecordLiteral.parse(s);
                        for (int i = 0; i < fields.size(); i++) {
                            if (fields.get(i).name().equalsIgnoreCase(fieldName)) {
                                if (i < parts.size()) {
                                    RecordLiteral.Field part = parts.get(i);
                                    String fieldType = fields.get(i).typeName();
                                    if (executor.database.isCompositeType(fieldType)) {
                                        // Nested composite, return as PgRow for further chaining
                                        return executor.parseCompositeToRow(part.text, fieldType);
                                    }
                                    return executor.compositeTypeHandler.coerceFieldValue(
                                            part.text, fieldType, part.quoted);
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
        if (expr instanceof SubscriptExpr) return executor.subscriptEvaluator.eval((SubscriptExpr) expr, ctx);
        if (expr instanceof CollateExpr) {
            CollateExpr ce = (CollateExpr) expr;
            Object collated = evalExpr(ce.expr(), ctx);
            rejectUncollatableValue(collated);
            validateCollationAtRuntime(ce.collation());
            return collated;
        }
        if (expr instanceof CompositeStarExpr) return evalExpr(((CompositeStarExpr) expr).expr(), ctx); // expansion handled by SelectExecutor
        if (expr instanceof QualifiedOperatorExpr) return evalQualifiedOperator(((QualifiedOperatorExpr) expr), ctx);
        if (expr instanceof CustomOperatorExpr) return evalCustomOperator(((CustomOperatorExpr) expr), ctx);
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
            case FLOAT: {
                // INSERT ... SELECT re-wraps each computed value as a literal, so a numeric NaN
                // or infinity reaches here spelled out; BigDecimal has no form for any of them.
                Double special = NumericLimits.specialNumericOrNull(lit.value());
                if (special != null) return special;
                return new java.math.BigDecimal(lit.value());
            }
            case STRING:
                return lit.value();
            case BIT_STRING: {
                // The complaint is about the character that is not a digit, not about the string
                // it sits in: PostgreSQL quotes back the first one its reader could not take.
                for (char c : lit.value().toCharArray()) {
                    if (c != '0' && c != '1') {
                        throw new MemgresException("\"" + c + "\" is not a valid binary digit", "22P02");
                    }
                }
                return new AstExecutor.PgBitString(lit.value());
            }
            case BOOLEAN:
                return Boolean.parseBoolean(lit.value());
            case NULL:
                return null;
            case DEFAULT: {
                // PostgreSQL points at the keyword itself. The tree records no offset for it, so
                // the word is named and the protocol layer finds it in the statement text.
                MemgresException misplaced =
                        new MemgresException("DEFAULT is not allowed in this context", "42601");
                misplaced.setPositionToken("DEFAULT");
                throw misplaced;
            }
            default:
                throw new IllegalStateException("Unknown literal type: " + lit.literalType());
        }
    }

    private Object evalColumnRef(ColumnRef ref, RowContext ctx) {
        if (ref.catalog() != null) {
            rejectOtherCatalog(ref.catalog(),
                    ref.schema() + "." + ref.table() + "." + ref.column());
            ref = new ColumnRef(ref.schema(), ref.table(), ref.column());
        }
        // pg_catalog.current_user etc.; treat as the system function directly
        if ("pg_catalog".equalsIgnoreCase(ref.table()) || "information_schema".equalsIgnoreCase(ref.table())) {
            String col = ref.column().toLowerCase();
            switch (col) {
                case "current_user":
                case "current_role": {
                    // Respect SECURITY DEFINER role override via GUC
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
                case "session_user": {
                    return executor.sessionUser();
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
                case "current_role": {
                    // Respect SECURITY DEFINER role override via GUC
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
                                if (result instanceof RowContext.SystemColumnRef) return resolveSystemColumnRef(result);
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
                    // Nothing resolves this name. Returning it as text would let a typo become a
                    // plausible-looking value that defeats the column's declared type.
                    throw new MemgresException(
                            "column \"" + ref.column() + "\" does not exist", "42703");
                }
            }
        }
        if (ref.table() == null) {
            // Unqualified column reference; check current context first
            Object result = null;
            boolean foundInCurrent = false;
            // What this context said the name was, kept whole for the rethrow below: the
            // reason a name resolves to nothing is worked out where the relations are known,
            // and rebuilding the refusal here from the name alone threw that reason away.
            MemgresException notHere = null;
            try {
                result = ctx.resolveColumn(null, ref.column());
                foundInCurrent = true;
            } catch (MemgresException e) {
                if (!"42703".equals(e.getSqlState())) throw e;
                notHere = e;
                // column not in current context, will try outer contexts / special columns below
            }
            if (foundInCurrent) {
                if (result == null && ref.column().equalsIgnoreCase("tableoid")) {
                    return resolveTableoidRef(ctx.resolveColumn(null, "tableoid"));
                }
                if (result instanceof RowContext.TableoidRef) {
                    return resolveTableoidRef(result);
                }
                if (result instanceof RowContext.SystemColumnRef) {
                    return resolveSystemColumnRef(result);
                }
                return result;
            }
            // Not found in current context, try special columns
            if (ref.column().equalsIgnoreCase("tableoid")) {
                try {
                    return resolveTableoidRef(ctx.resolveColumn(null, "tableoid"));
                } catch (MemgresException ignored) { /* fall through to outer contexts */ }
            }
            // System columns: ctid, xmin, xmax, cmin, cmax are handled by RowContext.resolveColumn
            if (ref.column().equalsIgnoreCase("ctid") || ref.column().equalsIgnoreCase("xmin")
                    || ref.column().equalsIgnoreCase("xmax") || ref.column().equalsIgnoreCase("cmin")
                    || ref.column().equalsIgnoreCase("cmax")) {
                try {
                    Object val = ctx.resolveColumn(null, ref.column());
                    return resolveSystemColumnRef(val);
                } catch (MemgresException ignored) { /* fall through to outer contexts */ }
            }
            // For single-column SRF tables, resolve the alias to the scalar value
            // (e.g., SELECT elem::int FROM jsonb_array_elements(...) AS elem)
            {
                Object singleCol = resolveSingleColumnTableRef(ref.column(), ctx);
                if (singleCol != null) return singleCol == SINGLE_COL_NULL ? null : singleCol;
                for (Iterator<RowContext> it = executor.outerContextStack.descendingIterator(); it.hasNext(); ) {
                    singleCol = resolveSingleColumnTableRef(ref.column(), it.next());
                    if (singleCol != null) return singleCol == SINGLE_COL_NULL ? null : singleCol;
                }
            }
            // Check if the column name matches a table alias (whole-row reference, e.g. ROW_TO_JSON(row))
            {
                Object wholeRow = resolveWholeRowReference(ref.column(), ctx);
                if (wholeRow != null) return wholeRow;
                // Also check outer contexts
                for (Iterator<RowContext> it = executor.outerContextStack.descendingIterator(); it.hasNext(); ) {
                    wholeRow = resolveWholeRowReference(ref.column(), it.next());
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
            if (notHere != null) throw notHere;
            throw new MemgresException(
                    "column \"" + ref.column() + "\" does not exist", "42703");
        } else {
            // Schema-qualified reference (schema.table.column). PostgreSQL resolves one against
            // the FROM entry of that name when the entry is the relation the schema names; a CTE,
            // a subquery alias or a relation from another schema is a FROM entry the schema
            // prefix does not reach, and is reported as such rather than as a missing one.
            if (ref.schema() != null) {
                RowContext.TableBinding reached = schemaPrefixReaches(ctx, ref.schema(), ref.table());
                if (reached == null) {
                    for (Iterator<RowContext> it = executor.outerContextStack.descendingIterator(); it.hasNext(); ) {
                        reached = schemaPrefixReaches(it.next(), ref.schema(), ref.table());
                        if (reached != null) break;
                    }
                }
                if (reached != null) {
                    // Two schemas may hold a relation of the same name and both may be in the
                    // FROM clause, so a reference the schema pins to the second of them is
                    // answered from that one rather than from whichever the bare name finds first.
                    // What the bare name reaches, asked without judging it: the name may reach
                    // two entries and still be perfectly reachable through the schema.
                    List<RowContext.TableBinding> byName = ctx == null
                            ? Cols.<RowContext.TableBinding>listOf() : ctx.bindingsNamed(ref.table());
                    if (byName.size() != 1 || reached != byName.get(0)) {
                        int idx = reached.table().getColumnIndex(ref.column());
                        if (idx >= 0) return reached.row()[idx];
                    }
                    return evalColumnRef(new ColumnRef(null, ref.table(), ref.column()), ctx);
                }
                if (bindingNamed(ctx, ref.table())) {
                    // The same account PostgreSQL gives for a qualified star that reaches nothing:
                    // the entry is there, the schema written in front of it is not how it is
                    // reached. The message said so and the Detail beside it did not follow.
                    MemgresException ex = new MemgresException(
                        "invalid reference to FROM-clause entry for table \"" + ref.table() + "\"", "42P01");
                    ex.setDetail("There is an entry for table \"" + ref.table()
                            + "\", but it cannot be referenced from this part of the query.");
                    throw ex;
                }
                throw new MemgresException(
                    "missing FROM-clause entry for table \"" + ref.table() + "\"", "42P01");
            }
            // Qualified column reference: table.column
            // The table may not be in the current context (e.g., LATERAL joins, correlated subqueries)
            // so we catch the "missing FROM-clause" error and fall through to outer contexts.
            Object result = null;
            boolean foundInCurrent = false;
            MemgresException aliasHidingError = null;
            try {
                result = ctx.resolveColumn(ref.table(), ref.column());
                foundInCurrent = true;
            } catch (MemgresException e) {
                if ("42703".equals(e.getSqlState())) {
                    // Column resolution failed for a valid alias/table: PostgreSQL falls back to
                    // attribute notation here, i.e. alias.name ≡ name(alias) (e.g. gs.date ≡
                    // date(gs) when gs is bound to a single-column FROM-function result such as
                    // generate_series(...) AS gs(key)). Only attempt this when the qualifier really
                    // is bound in the current context; otherwise rethrow the original error.
                    Object fallback = tryAttributeNotationFallback(ctx, ref.table(), ref.column());
                    if (fallback != ATTRIBUTE_NOTATION_NOT_APPLICABLE) return fallback;
                    throw e;
                }
                if (!"42P01".equals(e.getSqlState())) throw e;
                // Save alias-hiding errors for later — outer contexts may resolve this (correlated subqueries)
                if (e.getMessage() != null && e.getMessage().startsWith("invalid reference")) {
                    aliasHidingError = e;
                }
                // table not in current context, will try outer contexts below
            }
            if (foundInCurrent) {
                if (result == null && ref.column().equalsIgnoreCase("tableoid")) {
                    return resolveTableoidRef(ctx.resolveColumn(ref.table(), "tableoid"));
                }
                if (result instanceof RowContext.TableoidRef) {
                    return resolveTableoidRef(result);
                }
                if (result instanceof RowContext.SystemColumnRef) {
                    return resolveSystemColumnRef(result);
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
            // Not found in any context; prefer alias-hiding error if detected
            if (aliasHidingError != null) throw aliasHidingError;
            throw new MemgresException("missing FROM-clause entry for table \"" + ref.table() + "\"", "42P01");
        }
    }

    /** True when {@code ctx} binds anything under {@code name}, whatever kind of thing it is. */
    private static boolean bindingNamed(RowContext ctx, String name) {
        return ctx != null && ctx.getBinding(name) != null;
    }

    /**
     * A name written with four parts names a catalog, and PostgreSQL reaches only the one it is
     * connected to: {@code memgrestest.public.t.id} is {@code public.t.id}, and any other catalog
     * is a cross-database reference it does not implement. The whole name is reported, which is
     * how PostgreSQL reports it.
     */
    void rejectOtherCatalog(String catalog, String rest) {
        if (catalog == null) return;
        String current = executor.session != null ? executor.session.getDatabaseName() : null;
        if (current != null && current.equalsIgnoreCase(catalog)) return;
        throw new MemgresException("cross-database references are not implemented: "
                + catalog + "." + rest, "0A000");
    }

    /**
     * The one FROM entry among {@code bindings} that {@code schema.table} names, or null. Shared
     * with the star expansion, which has the bindings in hand rather than a row context.
     */
    RowContext.TableBinding schemaPrefixReaches(List<RowContext.TableBinding> bindings,
                                                String schema, String table) {
        return schemaPrefixReaches(new RowContext(bindings), schema, table);
    }

    /**
     * Whether {@code schema.table} names the FROM entry {@code ctx} binds as {@code table}.
     *
     * <p>Only a relation can be reached through a schema: a WITH query and a subquery alias live
     * in the query, not in a schema, so {@code public.c.n} against {@code WITH c AS (...)} is an
     * invalid reference in PostgreSQL and not a resolution. A relation qualifies when the schema
     * really holds it — the same table object the binding was built from for a table, and the
     * recorded schema for a view or a system catalog, neither of which keeps its own table
     * object once its rows have been read.
     */
    private RowContext.TableBinding schemaPrefixReaches(RowContext ctx, String schema, String table) {
        if (ctx == null) return null;
        List<RowContext.TableBinding> named = new ArrayList<>();
        for (RowContext.TableBinding b : ctx.getBindings()) {
            String exposed = b.alias() != null ? b.alias() : b.table().getName();
            if (exposed.equalsIgnoreCase(table)) named.add(b);
        }
        if (named.isEmpty()) return null;
        // A WITH query shadows a relation of the same name and is not reachable through a schema.
        if (executor.selectExecutor.lookupCte(table) != null) return null;
        // A schema that does not exist reaches nothing, however the name reads.
        if (executor.database.getSchema(schema) == null && !"pg_temp".equalsIgnoreCase(schema)
                && !SystemCatalog.isSystemCatalog(schema, table)) {
            return null;
        }
        Table inSchema = null;
        try {
            inSchema = executor.resolveTable(schema, table, true);
        } catch (MemgresException ignored) {
            // Not a table of that schema; a view or a catalog may still answer to the name.
        }
        if (inSchema != null) {
            for (RowContext.TableBinding b : named) {
                if (b.table() == inSchema) return b;
            }
        }
        Database.ViewDef view = executor.database.getView(table);
        if (view != null && view.schemaName() != null && view.schemaName().equalsIgnoreCase(schema)) {
            return named.get(0);
        }
        if (SystemCatalog.isSystemCatalog(schema, table)) {
            // pg_catalog reaches its own relations, not a user table that happens to be in scope.
            try {
                executor.resolveTable(executor.defaultSchema(), table);
                return null;
            } catch (MemgresException notAUserTable) {
                return named.get(0);
            }
        }
        return null;
    }

    /** Sentinel returned by {@link #tryAttributeNotationFallback} when the fallback does not apply. */
    private static final Object ATTRIBUTE_NOTATION_NOT_APPLICABLE = new Object();

    /**
     * PostgreSQL's qualified-name resolution tries a column first, then falls back to attribute
     * notation: {@code alias.name} is read as {@code name(alias)} — calling the single-arg
     * cast/function {@code name} on the whole-row value bound to {@code alias}. This is how
     * {@code gs.date} resolves against {@code FROM generate_series(...) AS gs(key)}: the aliased
     * column is named {@code key}, so {@code gs.date} isn't a column, but {@code date(gs)} (cast
     * the row's single timestamp value to a date) is valid and is exactly what PostgreSQL returns.
     * <p>
     * Scoped to aliases bound to a single-column FROM-function (SRF) result table, as marked by
     * {@link FromFunctionResolver} via {@code Table.setFunctionResult(true)} — e.g.
     * {@code generate_series}/{@code unnest} virtual tables. It must NOT fire for ordinary
     * table/subquery/VALUES/CTE aliases: PostgreSQL's attribute notation there operates on the
     * composite row type ({@code date(t)} with {@code t} a record), never by casting the single
     * column's value, so {@code t.date} on a one-column table alias is a plain 42703 in PG and a
     * value-cast here would silently coerce typos into wrong results. Returns
     * {@link #ATTRIBUTE_NOTATION_NOT_APPLICABLE} when the qualifier isn't bound in {@code ctx},
     * isn't a function-result binding, doesn't have exactly one column, or {@code name} isn't a
     * recognized cast type name or registered function — callers should then raise the original
     * "column X.Y does not exist" error.
     */
    private Object tryAttributeNotationFallback(RowContext ctx, String tableQualifier, String funcOrCastName) {
        RowContext.TableBinding binding = ctx.getBinding(tableQualifier);
        if (binding == null) return ATTRIBUTE_NOTATION_NOT_APPLICABLE;
        if (!binding.table().isFunctionResult()) return ATTRIBUTE_NOTATION_NOT_APPLICABLE;
        List<Column> cols = binding.table().getColumns();
        if (cols.size() != 1) return ATTRIBUTE_NOTATION_NOT_APPLICABLE;
        Object rowValue = binding.row()[0];
        // Try funcOrCastName as a cast/type-name function: date(ts), text(x), numeric(x), timestamp(x), ...
        try {
            return executor.castEvaluator.applyCast(rowValue, funcOrCastName);
        } catch (MemgresException castFailed) {
            // Not a recognized type name; fall through to try a registered scalar function below.
        }
        // Try funcOrCastName as a regular single-arg user-defined function.
        PgFunction userFunc = executor.database.getFunction(funcOrCastName.toLowerCase());
        if (userFunc != null) {
            FunctionCallExpr synthetic = new FunctionCallExpr(
                    funcOrCastName, Cols.listOf(new PrecomputedValueExpr(rowValue)));
            return executor.functionEvaluator.evalFunction(synthetic, ctx);
        }
        return ATTRIBUTE_NOTATION_NOT_APPLICABLE;
    }

    /**
     * Type-inference counterpart of {@link #tryAttributeNotationFallback}, used by
     * {@link #inferTypeFromContext} (the RowDescription/Describe type-inference layer, see
     * {@code SelectExecutor.buildProjectedColumn}) so a qualified reference that will resolve via
     * the attribute-notation fallback at runtime advertises the *same* type it will actually
     * produce, instead of the generic {@code DataType.TEXT} default. Mirrors the runtime guard
     * exactly (SRF-provenance binding, exactly one column) and mirrors the two resolution
     * attempts: a cast/type name (via {@link DataType#fromPgName}, matching what
     * {@code CastEvaluator.applyCast} would resolve to) or a registered single-arg function's
     * declared return type. Returns {@code null} when the fallback doesn't apply or {@code
     * funcOrCastName} isn't a recognized cast/function name — callers then fall through to their
     * own default (TEXT).
     */
    private DataType inferAttributeNotationFallbackType(RowContext.TableBinding binding, String funcOrCastName) {
        if (!binding.table().isFunctionResult()) return null;
        if (binding.table().getColumns().size() != 1) return null;
        DataType castType = DataType.fromPgName(funcOrCastName);
        if (castType != null) return castType;
        if (executor != null && executor.database != null) {
            PgFunction userFunc = executor.database.getFunction(funcOrCastName.toLowerCase());
            if (userFunc != null && userFunc.getReturnType() != null) {
                DataType dt = DataType.fromPgName(userFunc.getReturnType().replaceAll("\\(.*\\)", "").trim());
                if (dt != null) return dt;
            }
        }
        return null;
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
     * Resolve a SystemColumnRef to its actual value (xmin/xmax/cmin/cmax).
     */
    private Object resolveSystemColumnRef(Object val) {
        if (val instanceof RowContext.SystemColumnRef) {
            RowContext.SystemColumnRef ref = (RowContext.SystemColumnRef) val;
            String schemaName = "public";
            for (Map.Entry<String, Schema> e : executor.database.getSchemas().entrySet()) {
                if (e.getValue().getTable(ref.table.getName()) == ref.table) {
                    schemaName = e.getKey();
                    break;
                }
            }
            String tableKey = schemaName + "." + ref.table.getName();
            Map<Object[], long[]> meta = executor.database.getRowMeta(tableKey);
            long[] rowMeta = meta.get(ref.row);
            // A row read out of a snapshot or a statement's image is a copy of the values and
            // nothing else, and where a row sits is a property of the row: the version being read
            // is still there, at the place it has always occupied, so that is the place
            // PostgreSQL answers ctid, xmin and the rest from.
            if (rowMeta == null && executor.session != null) {
                rowMeta = executor.session.tupleIdentityOfCopy(tableKey, ref.row);
            }
            switch (ref.column) {
                case "xmin": return rowMeta != null ? rowMeta[0] : 0L;
                case "xmax": return rowMeta != null ? rowMeta[1] : 0L;
                case "cmin": return rowMeta != null ? (int) rowMeta[2] : 0;
                // A tuple carries one command identifier, and PostgreSQL answers both cmin and
                // cmax from it, so a row always reports the same number for the two.
                case "cmax": return rowMeta != null ? (int) rowMeta[2] : 0;
                case "ctid": {
                    // Every row has a place of its own. Where no place was recorded — rows a
                    // CREATE TABLE AS or a COPY put there — the fallback was zero for all of
                    // them, so every row shared one ctid: DISTINCT ctid counted one row, and
                    // DELETE WHERE ctid = ... deleted the whole table.
                    if (rowMeta != null && rowMeta.length > 4 && rowMeta[4] != 0) {
                        return new PgTid(0, (int) rowMeta[4]);
                    }
                    java.util.List<Object[]> rows = ref.table.getRows();
                    for (int i = 0; i < rows.size(); i++) {
                        if (rows.get(i) == ref.row) return new PgTid(0, i + 1);
                    }
                    return new PgTid(0, 0);
                }
                default: return 0L;
            }
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

    private static final Object SINGLE_COL_NULL = new Object();

    /**
     * For single-column tables (e.g., SRF results like jsonb_array_elements),
     * when the alias matches the table name, return the scalar value.
     * This matches PG behavior for `SELECT elem::int FROM func() AS elem`.
     * Returns SINGLE_COL_NULL sentinel if found but value is null; returns null if no match.
     */
    private Object resolveSingleColumnTableRef(String name, RowContext ctx) {
        RowContext.TableBinding b = ctx.getBinding(name);
        if (b == null) return null;
        if (b.table().getColumns().size() == 1) {
            Object val = b.row()[0];
            return val != null ? val : SINGLE_COL_NULL;
        }
        return null;
    }

    /**
     * Evaluate a prefix-style qualified operator expression: OPERATOR(schema.op)(args).
     */
    private Object evalQualifiedOperator(QualifiedOperatorExpr qop, RowContext ctx) {
        // Validate explicit schema qualifier: OPERATOR(schema.op) — PG rejects if schema doesn't exist
        if (qop.schema() != null && !"pg_catalog".equals(qop.schema())
                && executor.database.getSchema(qop.schema()) == null) {
            throw new MemgresException(
                "schema \"" + qop.schema() + "\" does not exist", "3F000");
        }
        // A schema on the search path that does not exist is skipped, not complained about: it
        // is how an application names a schema it may or may not have created yet. Refusing the
        // operator instead made every statement fail for as long as the setting stood.
        return evalExpr(qop.inner(), ctx);
    }

    /**
     * Evaluate a user-defined operator expression (CustomOperatorExpr).
     * Looks up the PgOperator by name, resolves its backing function, and calls it.
     */
    private Object evalCustomOperator(CustomOperatorExpr cop, RowContext ctx) {
        // Validate explicit schema qualifier: reject if schema doesn't exist (3F000)
        if (cop.schema() != null && !"pg_catalog".equals(cop.schema())
                && executor.database.getSchema(cop.schema()) == null) {
            throw new MemgresException(
                "schema \"" + cop.schema() + "\" does not exist", "3F000");
        }

        Object leftVal = cop.left() != null ? evalExpr(cop.left(), ctx) : null;
        Object rightVal = evalExpr(cop.right(), ctx);

        // #= is hstore populate_record: record #= hstore → record
        if ("#=".equals(cop.opSymbol()) && cop.left() != null) {
            if (!executor.database.hasExtension("hstore"))
                throw new MemgresException("operator does not exist: record #= hstore", "42883");
            String typeName = executor.resolveCompositeTypeName(cop.left(), ctx);
            HstoreValue hs = (rightVal == null)
                    ? new HstoreValue(new java.util.LinkedHashMap<>())
                    : (rightVal instanceof HstoreValue)
                        ? (HstoreValue) rightVal : HstoreValue.parse(rightVal.toString());
            java.util.List<CreateTypeStmt.CompositeField> fields =
                    executor.compositeTypeHandler.resolveFieldsForType(typeName);
            if (fields == null)
                throw new MemgresException("operator does not exist: record #= hstore", "42883");
            return executor.compositeTypeHandler.populateFromHstore(leftVal, hs, fields);
        }

        // Built-in text operators that aren't registered as user PgOperator.
        // ^@ is PG 11+ starts-with on text (treated as STRICT).
        if ("^@".equals(cop.opSymbol()) && cop.left() != null) {
            // Only text starts with text. Reading both sides through toString() gave this
            // operator a meaning over every pair of values, so 1 ^@ 1 answered true.
            List<RowContext.TableBinding> bindings = ctx == null
                    ? java.util.Collections.<RowContext.TableBinding>emptyList()
                    : ctx.getBindings();
            DataType leftType = inferTypeFromContext(cop.left(), bindings);
            DataType rightType = inferTypeFromContext(cop.right(), bindings);
            if (!isTextLike(leftType) || !isTextLike(rightType)) {
                MemgresException e = new MemgresException("operator does not exist: "
                        + operandTypeName(cop.left(), leftVal, ctx) + " ^@ "
                        + operandTypeName(cop.right(), rightVal, ctx), "42883");
                e.setHint("No operator matches the given name and argument types. "
                        + "You might need to add explicit type casts.");
                throw e;
            }
            if (leftVal == null || rightVal == null) return null;
            return leftVal.toString().startsWith(rightVal.toString());
        }

        // Determine arg type names for operator lookup. A bare string constant has no type of
        // its own until something gives it one, so it is offered as unknown and takes whatever
        // the operator declares — which is how 'a' reaches an integer parameter and is refused
        // as one rather than reported as a missing text operator.
        String leftType = cop.left() != null ? operandTypeName(cop.left(), leftVal, ctx) : "NONE";
        String rightType = operandTypeName(cop.right(), rightVal, ctx);

        // Try to find matching operator in database
        PgOperator pgOp = resolveOperator(cop.schema(), cop.opSymbol(), leftType, rightType);
        if (pgOp == null) {
            // Error message matching PG format
            if (cop.isUnary()) {
                throw new MemgresException(
                    "operator does not exist: " + cop.opSymbol() + " " + rightType, "42883");
            }
            throw new MemgresException(
                "operator does not exist: " + leftType + " " + cop.opSymbol() + " " + rightType, "42883");
        }

        // Resolve the backing function
        String funcName = pgOp.getFunction();
        PgFunction func = executor.database.getFunction(funcName);
        if (func == null) {
            throw new MemgresException(
                "function " + funcName + " referenced by operator " + cop.opSymbol() + " does not exist", "42883");
        }

        // STRICT: return NULL if any arg is NULL
        if (func.isStrict()) {
            if (leftVal == null && cop.left() != null) return null;
            if (rightVal == null) return null;
        }

        // Build argument list and call. An operand the operator declares a type for arrives as
        // that type: a bare constant has none of its own, and handing it to the backing function
        // unconverted asked that function to multiply a string by a number.
        java.util.List<Object> args = new java.util.ArrayList<>();
        if (cop.left() != null) args.add(asDeclared(leftVal, pgOp.getLeftArg()));
        args.add(asDeclared(rightVal, pgOp.getRightArg()));

        PlpgsqlExecutor plExec = new PlpgsqlExecutor(executor, executor.database, executor.session);
        return plExec.executeFunction(func, args);
    }

    /**
     * Resolve a PgOperator by schema, name, and argument types.
     * Tries exact match first, then fuzzy matching on arg types.
     */
    /** An operand as the type the operator declares for it. */
    private Object asDeclared(Object value, String declaredType) {
        if (value == null || declaredType == null) return value;
        String bare = declaredType.trim().toLowerCase();
        if (bare.isEmpty() || bare.equals("any") || bare.startsWith("anyelement")
                || bare.equals("\"any\"")) {
            return value;
        }
        return executor.castEvaluator.applyCast(value, bare);
    }

    /**
     * The type an operand offers an operator.
     *
     * <p>What the expression was written as decides this, not what its value happens to be held
     * in: an integer column read as a Java long is still an integer, and offering it as a bigint
     * put it past every operator declared for integers.
     */
    private String operandTypeName(Expression expr, Object value, RowContext ctx) {
        if (isUntypedConstant(expr)) return "unknown";
        String declared = executor.binaryOpEvaluator.declaredTypeForResolution(expr, ctx);
        if (declared != null && !declared.isEmpty()) return declared.toLowerCase();
        return AstExecutor.pgTypeNameOf(value);
    }

    /** Whether this operand is a string constant nothing has yet given a type to. */
    private static boolean isUntypedConstant(Expression expr) {
        return expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.STRING;
    }

    PgOperator resolveOperator(String schema, String opSymbol, String leftType, String rightType) {
        // Search schemas: explicit schema, or search_path
        java.util.List<String> schemas = new java.util.ArrayList<>();
        if (schema != null) {
            schemas.add(schema.toLowerCase());
        } else {
            schemas.add("pg_catalog");
            schemas.add("public");
            if (executor.session != null) {
                String sp = executor.session.getGucSettings().get("search_path");
                if (sp != null) {
                    for (String s : sp.split(",")) {
                        String trimmed = s.trim().replace("\"", "").replace("'", "");
                        if (!trimmed.isEmpty() && !"$user".equals(trimmed)
                                && !schemas.contains(trimmed.toLowerCase())) {
                            schemas.add(trimmed.toLowerCase());
                        }
                    }
                }
            }
        }

        // Try to find operator with matching arg types
        java.util.List<PgOperator> candidates = executor.database.getOperatorsByName(opSymbol);
        if (candidates.isEmpty()) return null;

        // Exact schema + type match
        for (PgOperator op : candidates) {
            String opSchema = op.getSchemaName() != null ? op.getSchemaName().toLowerCase() : "public";
            if (!schemas.contains(opSchema)) continue;
            String opLeft = op.getLeftArg() != null ? op.getLeftArg().toLowerCase() : "NONE";
            String opRight = op.getRightArg() != null ? op.getRightArg().toLowerCase() : "NONE";
            if (typeMatches(opLeft, leftType.toLowerCase()) && typeMatches(opRight, rightType.toLowerCase())) {
                return op;
            }
        }

        // Fallback: match if operator uses polymorphic / "any" types
        for (PgOperator op : candidates) {
            String opSchema = op.getSchemaName() != null ? op.getSchemaName().toLowerCase() : "public";
            if (!schemas.contains(opSchema)) continue;
            String opLeft = op.getLeftArg() != null ? op.getLeftArg().toLowerCase() : "NONE";
            String opRight = op.getRightArg() != null ? op.getRightArg().toLowerCase() : "NONE";
            // For unary, check left is NONE
            if ("NONE".equalsIgnoreCase(leftType)) {
                if ("NONE".equalsIgnoreCase(opLeft) || opLeft.isEmpty()) {
                    if ("any".equalsIgnoreCase(opRight) || "anyelement".equalsIgnoreCase(opRight)
                            || typeMatches(opRight, rightType.toLowerCase())) {
                        return op;
                    }
                }
            } else {
                // Only match if at least one arg is polymorphic/"any"
                boolean leftOk = "any".equalsIgnoreCase(opLeft) || "anyelement".equalsIgnoreCase(opLeft);
                boolean rightOk = "any".equalsIgnoreCase(opRight) || "anyelement".equalsIgnoreCase(opRight);
                if (leftOk || rightOk) return op;
            }
        }

        return null;
    }

    /**
     * Check if operator declared type matches the actual value type,
     * with fuzzy matching for numeric types and "any" types.
     */
    private static boolean typeMatches(String declaredType, String actualType) {
        if (declaredType.equals(actualType)) return true;
        if ("NONE".equals(declaredType) && "NONE".equals(actualType)) return true;
        // "any" type matches anything
        if ("any".equals(declaredType) || "anyelement".equals(declaredType) || "\"any\"".equals(declaredType)) return true;
        // "unknown" is the type of NULL and of a literal nothing has typed yet — it takes the
        // type the operator declares.
        if ("unknown".equals(actualType)) return true;
        // A number reaches a parameter of a wider kind and not a narrower one: an integer is a
        // numeric without losing anything, a numeric is not an integer. Treating every numeric
        // type as interchangeable made an operator declared for two integers answer for two
        // numerics, which is an operator PostgreSQL says does not exist.
        int actual = numericWidth(actualType);
        int declared = numericWidth(declaredType);
        if (actual > 0 && declared > 0) return actual <= declared;
        // Text type aliases
        if (isTextType(declaredType) && isTextType(actualType)) return true;
        return false;
    }

    /** How wide a number is, so a narrower one may stand where a wider is declared. */
    private static int numericWidth(String t) {
        switch (t) {
            case "smallint": case "int2": return 1;
            case "integer": case "int": case "int4": return 2;
            case "bigint": case "int8": return 3;
            case "numeric": case "decimal": return 4;
            case "real": case "float4": return 5;
            case "double precision": case "float8": case "float": case "double": return 6;
            default: return 0;
        }
    }

    private static boolean isNumericType(String t) {
        return "integer".equals(t) || "int".equals(t) || "int4".equals(t)
            || "bigint".equals(t) || "int8".equals(t) || "smallint".equals(t) || "int2".equals(t)
            || "numeric".equals(t) || "decimal".equals(t) || "real".equals(t) || "float4".equals(t)
            || "double precision".equals(t) || "float8".equals(t) || "float".equals(t) || "double".equals(t);
    }

    private static boolean isTextType(String t) {
        return "text".equals(t) || "varchar".equals(t) || "character varying".equals(t) || "char".equals(t);
    }

    /** Nothing: the marker for "no user-defined cast applies", since null is a real answer. */
    private static final Object NO_USER_CAST = new Object();

    /**
     * The value as a cast created WITH FUNCTION produces it, or {@link #NO_USER_CAST}.
     *
     * <p>A cast the reader created is the one that applies between those two types; going through
     * the ordinary text conversion instead ignored the function that was written for it.
     */
    private Object userDefinedCast(CastExpr cast, Object val, RowContext ctx) {
        if (val == null) return NO_USER_CAST;
        String sourceName = executor.binaryOpEvaluator.declaredTypeForResolution(cast.expr(), ctx);
        if (sourceName == null) return NO_USER_CAST;
        int sourceOid = typeOidOf(stripTypeModifier(sourceName));
        int targetOid = typeOidOf(stripTypeModifier(cast.typeName()));
        if (sourceOid <= 0 || targetOid <= 0 || sourceOid == targetOid) return NO_USER_CAST;
        String function = executor.database.castFunctionFor(sourceOid, targetOid);
        if (function == null) return NO_USER_CAST;
        PgFunction func = executor.database.getFunction(function.contains(".")
                ? function.substring(function.lastIndexOf('.') + 1) : function);
        if (func == null) return NO_USER_CAST;
        PlpgsqlExecutor plExec = new PlpgsqlExecutor(executor, executor.database, executor.session);
        return plExec.executeFunction(func, java.util.Collections.singletonList(val));
    }

    /** The OID of a type, whether it is one the engine has built in or one the reader created. */
    private int typeOidOf(String typeName) {
        DataType built = DataType.fromPgName(typeName.toLowerCase());
        if (built != null) return built.getOid();
        String key = TypeNamespace.oidKeyFor(executor.database, typeName);
        return executor.systemCatalog.getOid(
                key != null ? key : TypeNamespace.oidKey(null, typeName));
    }

    /** A type name without the width or precision written after it. */
    private static String stripTypeModifier(String typeName) {
        int paren = typeName.indexOf('(');
        return (paren > 0 ? typeName.substring(0, paren) : typeName).trim();
    }

    /**
     * Refuse a cast PostgreSQL has no path for, before the value is read. Whether a cast exists is
     * a question about the two types, not about whether the source's text happens to parse as the
     * target — which is how {@code true::int8} used to answer 1.
     */
    private void rejectMissingCast(CastExpr cast, RowContext ctx) {
        String target = cast.typeName();
        if (target == null || target.contains("[]") || target.indexOf('.') >= 0) return;
        // A declared type is the only thing worth judging; where the source has none this rule
        // says nothing, which is what keeps an untyped literal castable to anything.
        String sourceName = executor.binaryOpEvaluator.declaredTypeForResolution(cast.expr(), ctx);
        if (sourceName == null) return;
        DataType source = DataType.fromPgName(stripTypeModifier(sourceName));
        DataType targetType = DataType.fromPgName(stripTypeModifier(target));
        if (source == null || targetType == null) return;
        if (executor.database != null
                && (executor.database.getCustomEnum(sourceName.toLowerCase()) != null
                    || executor.database.getCustomEnum(target.toLowerCase()) != null
                    || executor.database.isCompositeType(target.toLowerCase())
                    || executor.database.isDomain(target.toLowerCase())
                    || executor.database.isDomain(sourceName.toLowerCase()))) {
            return;
        }
        MemgresException refusal = CastLegality.refusalFor(source, targetType);
        if (refusal != null) throw refusal;
    }

    private Object evalCast(CastExpr cast, RowContext ctx) {
        rejectMissingCast(cast, ctx);
        Object val = evalExpr(cast.expr(), ctx);
        Object byFunction = userDefinedCast(cast, val, ctx);
        if (byFunction != NO_USER_CAST) return byFunction;
        // A blank-padded string is stored padded and read back trimmed: PostgreSQL's conversion
        // from bpchar to any other string type drops the blanks the declaration added, which is
        // why 'ab'::char(5)::text is two characters and not five.
        val = BlankPadding.readAsOtherString(val, cast.typeName(),
                executor.binaryOpEvaluator.declaredTypeForResolution(cast.expr(), ctx));
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
        // A JSON null is a value that means "no value", so casting it onward gives SQL NULL. The
        // word "null" arriving as plain text is not that -- it is text no type can read, which is
        // why the source has to say it came from json before this applies.
        if (isJsonValued(cast.expr()) && val instanceof String
                && ((String) val).trim().equals("null")) {
            String target = cast.typeName() == null ? "" : cast.typeName().toLowerCase().trim();
            if (!target.equals("json") && !target.equals("jsonb")
                    && !target.equals("text") && !target.equals("varchar")) {
                return null;
            }
        }
        // jsonb holds a value and not the text it prints as, so a cast out of it reads that value:
        // there is no number in a string to find, and a number that is not whole rounds.
        if (val instanceof String && BinaryOpEvaluator.isJsonbTypeName(
                executor.binaryOpEvaluator.declaredTypeForResolution(cast.expr(), ctx))) {
            Object payload = CastEvaluator.jsonbCastPayload((String) val, cast.typeName());
            if (payload != CastEvaluator.NOT_A_JSONB_CAST) {
                return payload == null ? null
                        : executor.castEvaluator.applyCast(payload, cast.typeName(), false);
            }
        }
        checkNumericSpecialToInteger(cast, val);
        boolean unknownLiteral = cast.expr() instanceof Literal
                && ((Literal) cast.expr()).literalType() == Literal.LiteralType.STRING;
        return executor.castEvaluator.applyCast(val, cast.typeName(), unknownLiteral);
    }

    /**
     * PG's numeric knows NaN and both infinities, and none of them has an integer form. It says
     * so outright, where the same value arriving from float8 is reported as a range error --
     * so the source's own type decides which error this is.
     */
    void checkNumericSpecialToInteger(CastExpr cast, Object val) {
        if (!NumericLimits.isSpecial(val)) return;
        String target = integerTypeName(cast.typeName());
        if (target == null) return;
        if (inferExprType(cast.expr()) != DataType.NUMERIC) return;
        double d = ((Number) val).doubleValue();
        throw new MemgresException(
                "cannot convert " + (Double.isNaN(d) ? "NaN" : "infinity") + " to " + target, "0A000");
    }

    /** The name PG uses for an integer cast target, or null when the target is not one. */
    private static String integerTypeName(String typeName) {
        if (typeName == null) return null;
        String t = typeName.toLowerCase().trim();
        if (t.equals("int") || t.equals("int4") || t.equals("integer")) return "integer";
        if (t.equals("int8") || t.equals("bigint")) return "bigint";
        if (t.equals("int2") || t.equals("smallint")) return "smallint";
        // PG's numeric-to-money conversion goes through bigint, and reports that type by name
        if (t.equals("money")) return "bigint";
        return null;
    }

    /** True when an expression is declared to produce json or jsonb. */
    private static boolean isJsonValued(Expression expr) {
        if (expr instanceof CastExpr) {
            String t = ((CastExpr) expr).typeName();
            return t != null && (t.equalsIgnoreCase("json") || t.equalsIgnoreCase("jsonb"));
        }
        return false;
    }

    /**
     * Evaluate a unary operation on an already-evaluated value.
     */
    /** The types every arithmetic prefix operator is declared over. */
    private static final Set<String> PREFIX_NUMERIC_OPERAND = Cols.setOf(
            "smallint", "integer", "bigint", "numeric", "real", "double precision");

    /**
     * An arithmetic prefix operator is resolved from the type its operand is written with, just as
     * a binary one is resolved from the types on both sides, and only then is a value read. The
     * operand was being read first and handed back unchanged whenever nothing knew what to do with
     * it, which answered {@code -'abc'::text} with abc and {@code @ '-10'::text} with -10 -- not
     * even the absolute value it was asked for. Only a type the query itself writes down counts:
     * a column's type here is whatever the engine settled on, and refusing an operator on the
     * strength of that would reject SQL PostgreSQL runs.
     */
    private void rejectUnresolvablePrefixOperator(UnaryExpr un) {
        String symbol;
        switch (un.op()) {
            case NEGATE: symbol = "-"; break;
            case POSITIVE: symbol = "+"; break;
            case ABS: symbol = "@"; break;
            case SQRT: symbol = "|/"; break;
            case CBRT: symbol = "||/"; break;
            default: return;
        }
        String written = typeWrittenInQuery(un.operand());
        if (written == null) {
            // An operand that says nothing about its type leaves the operator to be chosen from
            // the candidates alone. Every candidate but one is a number, so the numeric one wins
            // -- except for minus, which also negates an interval, and PostgreSQL will not choose
            // between the two.
            if (un.op() == UnaryExpr.UnaryOp.NEGATE && isUnknownConstant(un.operand())) {
                throw new MemgresException("operator is not unique: - unknown"
                        + "\n  Hint: Could not choose a best candidate operator."
                        + " You might need to add explicit type casts.", "42725");
            }
            return;
        }
        String operand = DataType.canonicalName(written);
        if (PREFIX_NUMERIC_OPERAND.contains(operand)) return;
        // Minus is the one prefix operator declared over a span of time as well as over a number,
        // and a time of day converts to a span implicitly, so it resolves against that one.
        if (un.op() == UnaryExpr.UnaryOp.NEGATE
                && ("interval".equals(operand) || "time without time zone".equals(operand))) {
            return;
        }
        // The advice follows from the message: an operator written in front of its one operand has
        // one argument to cast, and PostgreSQL says so in the singular.
        throw new MemgresException("operator does not exist: " + symbol + " " + operand
                + "\n  Hint: No operator matches the given name and argument type."
                + " You might need to add an explicit type cast.", "42883");
    }

    Object evalUnaryValue(UnaryExpr.UnaryOp op, Object val) {
        switch (op) {
            case NOT: {
                if (val == null) return null;
                return !isTruthy(val);
            }
            case NEGATE: {
                if (val == null) return null;
                Object negated = NumericLimits.negateExact(val);
                if (negated != null) return negated;
                if (val instanceof java.math.BigDecimal) return ((java.math.BigDecimal) val).negate();
                if (val instanceof Double) return -((Double) val);
                if (val instanceof Float) return -((Float) val);
                // Negating an interval flips every field, not just the leading one
                if (val instanceof PgInterval) return ((PgInterval) val).negate();
                if (val instanceof PgMoney) return new PgMoney(((PgMoney) val).getValue().negate());
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
                // inet/macaddr bitwise NOT
                if (val instanceof InetValue) return ((InetValue) val).bitwiseNot();
                if (val instanceof MacaddrValue) return ((MacaddrValue) val).bitwiseNot();
                if (val instanceof Macaddr8Value) return ((Macaddr8Value) val).bitwiseNot();
                if (val instanceof Integer) return ~((Integer) val);
                if (val instanceof Long) return ~((Long) val);
                return ~toLong(val);
            }
            case ABS: {
                if (val == null) return null;
                Object absolute = NumericLimits.absExact(val);
                return absolute != null ? absolute : val;
            }
            case SQRT: {
                if (val == null) return null;
                double d = toDouble(val);
                if (d < 0) throw new MemgresException("cannot take square root of a negative number", "2201F");
                // |/ is float8 only, so the answer stays a double rather than collapsing to an
                // integer whose text form (16331239353195370) is not what float8 prints.
                return Double.valueOf(Math.sqrt(d));
            }
            case CBRT: {
                if (val == null) return null;
                return Double.valueOf(Math.cbrt(toDouble(val)));
            }
            case GEO_IS_HORIZONTAL: {
                if (val == null) return null;
                Object geom = GeometricOperations.autoDetectPublic(val.toString());
                if (geom instanceof GeometricOperations.PgLseg) {
                    return GeometricOperations.isHorizontal((GeometricOperations.PgLseg) geom);
                }
                throw new MemgresException("operator does not exist: ?- " + GeometricOperations.pgTypeName(geom), "42883");
            }
            case GEO_IS_VERTICAL: {
                if (val == null) return null;
                Object geom = GeometricOperations.autoDetectPublic(val.toString());
                if (geom instanceof GeometricOperations.PgLseg) {
                    return GeometricOperations.isVertical((GeometricOperations.PgLseg) geom);
                }
                throw new MemgresException("operator does not exist: ?| " + GeometricOperations.pgTypeName(geom), "42883");
            }
            case GEO_CENTER: {
                if (val == null) return null;
                Object geom = GeometricOperations.autoDetectPublic(val.toString());
                return GeometricOperations.formatPoint(GeometricOperations.center(geom));
            }
            case GEO_LENGTH: {
                if (val == null) return null;
                Object geom = GeometricOperations.autoDetectPublic(val.toString());
                double len = GeometricOperations.length(geom);
                return (len == Math.floor(len) && !Double.isInfinite(len)) ? (Object) (long) len : (Object) len;
            }
            case GEO_NPOINTS: {
                if (val == null) return null;
                return GeometricOperations.npoints(val.toString());
            }
            case HSTORE_TO_ARRAY: {
                if (val == null) return null;
                HstoreValue h = (val instanceof HstoreValue) ? (HstoreValue) val : HstoreValue.parse(val.toString());
                java.util.List<Object> result = new java.util.ArrayList<>();
                for (java.util.Map.Entry<String, String> e : h.getData().entrySet()) {
                    result.add(e.getKey());
                    result.add(e.getValue());
                }
                return result;
            }
            case HSTORE_TO_MATRIX: {
                if (val == null) return null;
                HstoreValue h = (val instanceof HstoreValue) ? (HstoreValue) val : HstoreValue.parse(val.toString());
                java.util.List<Object> result = new java.util.ArrayList<>();
                for (java.util.Map.Entry<String, String> e : h.getData().entrySet()) {
                    java.util.List<Object> pair = new java.util.ArrayList<>();
                    pair.add(e.getKey());
                    pair.add(e.getValue());
                    result.add(pair);
                }
                return result;
            }
            default:
                throw new IllegalStateException("Unknown unary op: " + op);
        }
    }

    /**
     * Whole-row reference: a bare identifier naming a FROM item rather than a column,
     * as in SELECT t FROM t. Returns the row as a composite, or null when the name does
     * not match any binding.
     */
    private Object resolveWholeRowReference(String name, RowContext ctx) {
        if (name == null || ctx == null || ctx.getBindings() == null) return null;
        for (RowContext.TableBinding b : ctx.getBindings()) {
            boolean matches = (b.alias() != null && b.alias().equalsIgnoreCase(name))
                    || (b.table() != null && b.table().getName().equalsIgnoreCase(name));
            if (!matches) continue;
            List<Object> values = new ArrayList<>();
            Object[] row = b.row();
            int n = b.table() != null ? b.table().getColumns().size() : (row == null ? 0 : row.length);
            for (int i = 0; i < n; i++) {
                values.add(row != null && i < row.length ? row[i] : null);
            }
            return new AstExecutor.PgRow(values);
        }
        return null;
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

    /**
     * The IS JSON predicate, answered by the reader that reads json input rather than by looking at
     * the text's first character. What kind of value a document is, and whether it repeats a key,
     * are questions about the document; a text that opens with a brace is not thereby an object.
     */
    private Object evalIsJson(IsJsonExpr ij, RowContext ctx) {
        rejectIsJsonOperand(ij, ctx);
        Object val = evalExpr(ij.expr(), ctx);
        if (val == null) return null; // SQL NULL IS JSON is NULL
        // bytea carries the document as its bytes, which is the one form of it that is not text
        String text = val instanceof byte[]
                ? new String((byte[]) val, java.nio.charset.StandardCharsets.UTF_8)
                : val.toString();
        JsonParser.Shape shape = JsonParser.shapeOf(text.trim());
        boolean valid = shape != null && matchesJsonType(ij.jsonType(), shape.kind)
                // Uniqueness is asked of every object in the document, not only the outermost one
                && (!ij.uniqueKeys() || shape.uniqueKeys);
        return ij.negated() ? !valid : valid;
    }

    /**
     * The types IS JSON asks its question of: the string types, json and jsonb, and bytea, which
     * carries a document as its bytes. Anything else is refused rather than rendered to text and
     * read, which is how {@code 1 IS JSON} used to answer true.
     */
    private static final Set<String> IS_JSON_OPERAND_TYPES = Cols.setOf(
            "text", "varchar", "character varying", "char", "character", "bpchar", "name",
            "json", "jsonb", "bytea", "unknown");

    private void rejectIsJsonOperand(IsJsonExpr ij, RowContext ctx) {
        String declared = executor.binaryOpEvaluator.declaredTypeForResolution(ij.expr(), ctx);
        if (declared == null) {
            // A literal nothing has typed is PostgreSQL's unknown and reads as text. Anything else
            // has a type even where the query did not write one: "current_date IS JSON" asks the
            // question of a date.
            if (ij.expr() instanceof Literal) return;
            DataType inferred = inferExprType(ij.expr());
            if (inferred == null) return;
            declared = inferred.getPgName();
        }
        if (IS_JSON_OPERAND_TYPES.contains(stripTypeModifier(declared).toLowerCase())) return;
        throw new MemgresException("cannot use type " + declared + " in IS JSON predicate", "42804");
    }

    private static boolean matchesJsonType(IsJsonExpr.JsonType wanted, int kind) {
        if (wanted == null) return true;
        switch (wanted) {
            case OBJECT: return kind == JsonValue.OBJECT;
            case ARRAY: return kind == JsonValue.ARRAY;
            case SCALAR: return kind != JsonValue.OBJECT && kind != JsonValue.ARRAY;
            case BOOLEAN: return kind == JsonValue.BOOLEAN;
            case NULL: return kind == JsonValue.NULL;
            case STRING: return kind == JsonValue.STRING;
            case NUMBER: return kind == JsonValue.NUMBER;
            default: return true;   // VALUE: any document at all
        }
    }

    static boolean isValidJson(String s) {
        if (s == null) return false;
        // IS JSON asks exactly the question json input asks, so it is answered by the same reader
        // rather than by a second, looser one of its own. Judging a document by its opening
        // bracket called '[1,2' and '{"a":1}{"b":2}' JSON, neither of which json input would take.
        try {
            JsonParser.validate(s.trim());
            return true;
        } catch (MemgresException e) {
            return false;
        }
    }

    /**
     * The same reading, keeping the reader's complaint. A caller that only wants a yes or no asks
     * the question above; one that is about to refuse the text has to hand on the error the reader
     * raised, because only that names in its detail which rule the text broke.
     */
    static void requireJson(String s) {
        if (s == null) {
            throw new MemgresException("invalid input syntax for type json", "22P02");
        }
        // Not trimmed: the reader skips whitespace itself, and the text it was handed is the text
        // its complaint quotes back.
        JsonParser.validate(s);
    }

    // ---- SQL/JSON standard expression evaluation ----

    private Object evalJsonExists(JsonExistsExpr je, RowContext ctx) {
        return sqlJson.exists(je, ctx);
    }

    private Object evalJsonValue(JsonValueExpr jv, RowContext ctx) {
        return sqlJson.value(jv, ctx);
    }

    private Object evalJsonQuery(JsonQueryExpr jq, RowContext ctx) {
        return sqlJson.query(jq, ctx);
    }

    private Object evalIn(InExpr in, RowContext ctx) {
        // IN and "= ANY(array)" resolve the "=" of the operand type -- the parser turns the
        // second into this node. NOT IN is not the negation of that: PostgreSQL expands it to
        // "<> ALL", so it resolves "<>", which a point has even though it has no "=".
        if (in.values() != null && !in.values().isEmpty()) {
            Expression other = in.values().get(0);
            // "= ANY(<array>)" keeps the whole array as its single value, so the comparison is
            // against one element of it, not against the array. Where the element cannot be
            // named -- the array came out of a function, say -- there is nothing to resolve
            // against, and resolving against the array itself would refuse the comparison for
            // being between a type and an array of it.
            boolean resolvable = true;
            if (in.fromAny() && in.values().size() == 1) {
                Expression element = arrayElementOperand(other);
                if (element == null) element = elementByDeclaredType(other, ctx);
                if (element != null) {
                    other = element;
                } else if (executor.binaryOpEvaluator.declaredTypeForResolution(other, ctx) == null) {
                    // Nothing here says whether this is an array or a single value, so there is
                    // no pair of types to resolve an operator between.
                    resolvable = false;
                }
            } else if (in.fromAny()) {
                // A written ARRAY[...] arrives here as its elements. Where those elements are
                // themselves arrays the comparison is still against the innermost value, because
                // an array type in PostgreSQL says nothing about how many dimensions it has:
                // ARRAY[ARRAY[1],ARRAY[2]] is an integer[], and = ANY over it resolves against
                // integer rather than against integer[].
                other = innermostElementOperand(other);
            }
            if (resolvable) {
                BinaryExpr.BinOp op = in.negated()
                        ? BinaryExpr.BinOp.NOT_EQUAL : BinaryExpr.BinOp.EQUAL;
                executor.binaryOpEvaluator.rejectUnresolvableOperator(
                        new BinaryExpr(in.expr(), op, other), ctx);
                // Every entry of a written list is resolved against the left-hand side, not just
                // the first: PostgreSQL settles the whole list on one type before it searches it,
                // so 1 IN (1, true) is refused rather than answered true by the entry that matched
                // before the one that could not be compared was reached.
                if (!in.fromAny()) {
                    for (int i = 1; i < in.values().size(); i++) {
                        executor.binaryOpEvaluator.rejectUnresolvableOperator(
                                new BinaryExpr(in.expr(), op, in.values().get(i)), ctx);
                    }
                }
            }
        }
        Object val = evalExpr(in.expr(), ctx);
        // jsonb is a value and not the text it prints as, so membership is decided by reading
        // both sides as documents. Comparing their texts said 1 was not among (1.0).
        boolean jsonb = BinaryOpEvaluator.isJsonbTypeName(
                executor.binaryOpEvaluator.declaredTypeForResolution(in.expr(), ctx));

        // Check for IN (subquery). The ANY spelling over a written-out array is a list of
        // elements, each of them a value on its own -- so a subquery among them is not the
        // subquery form of IN and is judged as any other scalar element would be.
        if (!in.fromAny() && in.values().size() == 1 && in.values().get(0) instanceof SubqueryExpr) {
            SubqueryExpr sq = (SubqueryExpr) in.values().get(0);
            if (ctx != null) executor.outerContextStack.push(ctx);
            QueryResult subResult;
            try {
                subResult = executor.executeStatement(sq.subquery());
            } finally {
                if (ctx != null) executor.outerContextStack.pop();
            }
            // Both sides of IN have to be the same width. Only the narrow half was checked, so
            // 1 IN (SELECT 1, 2) compared against the first column and answered true where
            // PostgreSQL refuses the subquery outright. The width is a question about the two
            // select lists and not about the rows, so it is asked ahead of the null answer a null
            // left-hand side would otherwise short-circuit to.
            rejectSubqueryWidth(comparandWidth(in.expr(), val), subResult);
            // A set with nothing in it settles the answer without comparing anything, so there is
            // nothing left for a null on the left to be unknown about. Answering unknown first
            // dropped rows from a WHERE: "x NOT IN (SELECT 1 WHERE false)" kept every row but the
            // null one, where PostgreSQL keeps them all.
            if (subResult.getRows().isEmpty()) return in.negated();
            if (val == null) return null; // NULL IN (...) is NULL
            boolean found = false;
            boolean hasNull = false;
            for (Object[] row : subResult.getRows()) {
                List<?> rowVal = val instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) val).values() : (val instanceof List<?> ? (List<?>) val : null);
                if (rowVal != null && row.length > 1) {
                    // Row value IN (multi-column subquery): the rows are compared as rows, so a
                    // null member leaves the pair unknown rather than deciding it, and a member
                    // that differs settles the pair false however many others were unknown.
                    if (rowVal.size() != row.length) continue;
                    Object equal = BinaryOpEvaluator.rowEquality(rowVal, Arrays.asList(row), true);
                    if (equal == null) { hasNull = true; continue; }
                    if (Boolean.TRUE.equals(equal)) { found = true; break; }
                } else {
                    Object elem = row.length > 0 ? row[0] : null;
                    if (elem == null) { hasNull = true; continue; }
                    if (inEquals(jsonb, val, elem)) { found = true; break; }
                }
            }
            if (found) return !in.negated();
            if (hasNull) return null;
            return in.negated();
        }
        // Each entry of a written list stands where one value stands, so a subquery among them
        // may have only one column -- and PostgreSQL settles that from its select list, before
        // any entry is compared. Comparing left to right and stopping at the first match never
        // reached the entry that was at fault, so 1 IN (1, (SELECT 1, 2)) answered true.
        rejectWideSubqueryElements(in.values());
        rejectRowEntryMismatch(in);
        val = resolveInListTypes(in, val);
        if (val == null) {
            // "= ANY(<array>)" over an array holding nothing settles the answer without comparing
            // anything, so a null on the left leaves nothing to be unknown about: it is false, and
            // false is what PostgreSQL answers.
            if (in.fromAny() && in.values().size() == 1) {
                List<?> only = PgArray.from(evalExpr(in.values().get(0), ctx));
                if (only != null && only.isEmpty()) return in.negated();
            }
            return null; // NULL IN (...) is NULL
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
            // A written entry is one value, so an untyped one is read as the type it is being
            // compared against. It used to be spared that reading when its text was spelled the
            // way an array is spelled, which made 1 IN ('{1,2}') true.
            if (val instanceof Number && elem instanceof String && !((String) elem).isEmpty()) {
                String se = (String) elem;
                if (!in.fromAny() && v instanceof Literal
                        && ((Literal) v).literalType() == Literal.LiteralType.STRING) {
                    // A written entry is one value, read as the type it is compared against, and
                    // text spelled the way an array is spelled is not an exception to that.
                    elem = TypeCoercion.coerce(se, executor.exprEvaluator.inferExprType(in.expr()));
                } else if (!PgArray.looksLikeArrayText(se)) {
                    // An array written out beside a number is an array of text, and there is no
                    // operator between the two.
                    try { new java.math.BigDecimal(se); } catch (NumberFormatException e) {
                        throw new MemgresException("operator does not exist: integer = text", "42883");
                    }
                }
            }
            // Written out as IN, a row is compared as a row, so a null member leaves the pair
            // unknown instead of settling it not-equal. The ANY spelling over an array is not that
            // comparison: it is the ordinary "=" of the record type, which reads a null as a value
            // like any other, so ROW(1,NULL) = ANY (ARRAY[ROW(1,2)]) is false rather than unknown.
            // An array is one value and is not read this way either: two arrays holding a null in
            // the same place are the same array.
            if (!in.fromAny() && val instanceof AstExecutor.PgRow && elem instanceof AstExecutor.PgRow
                    && ((AstExecutor.PgRow) val).values().size()
                        == ((AstExecutor.PgRow) elem).values().size()) {
                Object equal = BinaryOpEvaluator.rowEquality(((AstExecutor.PgRow) val).values(),
                        ((AstExecutor.PgRow) elem).values(), true);
                if (equal == null) { hasNull = true; continue; }
                if (Boolean.TRUE.equals(equal)) { found = true; break; }
                continue;
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
                    if (inEquals(jsonb, val, ae)) { found = true; break; }
                }
                if (found) break;
                continue;
            }
            // An array parameter arrives as the text of its literal, and "= ANY(...)" over it is a
            // comparison against each element. A written IN list is not that: each entry there is
            // one value, and text spelled like an array is the text it is.
            // A jsonb object is written between braces too, and is one value rather than a list of
            // them: "= ANY(ARRAY['{\"a\":1}'::jsonb])" compares against the object, not against a
            // member of it.
            if (in.fromAny() && elem instanceof String && !jsonb
                    && ((String) elem).startsWith("{") && ((String) elem).endsWith("}")
                    && !RangeOperations.isMultirangeOrEmpty(((String) elem).trim())) {
                String s = (String) elem;
                List<Object> parsed = executor.arrayOperationHandler.parsePostgresArrayLiteral(s);
                for (Object ae : parsed) {
                    if (ae == null) { hasNull = true; continue; }
                    if (TypeCoercion.areEqual(val, ae)) { found = true; break; }
                }
                if (found) break;
                continue;
            }
            if (inEquals(jsonb, val, elem)) {
                found = true;
                break;
            }
        }
        if (found) return !in.negated();
        if (hasNull) return null;
        return in.negated();
    }

    /**
     * Whether a member of the set equals the value being looked for, reading both as documents
     * where that is what they are. jsonb values are held as their text, and two texts that differ
     * may still be one value.
     */
    private static boolean inEquals(boolean jsonb, Object val, Object elem) {
        if (jsonb && val instanceof String && elem instanceof String) {
            return JsonOperations.compareJsonb((String) val, (String) elem) == 0;
        }
        return TypeCoercion.areEqual(val, elem);
    }

    private Object evalBetween(BetweenExpr bet, RowContext ctx) {
        // BETWEEN is shorthand for a pair of comparisons, so PostgreSQL resolves ">=" and "<="
        // against the declared types and names the one that failed -- "text >= integer" -- before
        // it looks at a single value.
        executor.binaryOpEvaluator.rejectUnresolvableOperator(
                new BinaryExpr(bet.expr(), BinaryExpr.BinOp.GREATER_EQUAL, bet.low()), ctx);
        executor.binaryOpEvaluator.rejectUnresolvableOperator(
                new BinaryExpr(bet.expr(), BinaryExpr.BinOp.LESS_EQUAL, bet.high()), ctx);
        Object val = evalExpr(bet.expr(), ctx);
        Object low = evalExpr(bet.low(), ctx);
        Object high = evalExpr(bet.high(), ctx);
        // Being shorthand for that pair of comparisons is the whole of what BETWEEN is, so the two
        // are joined by the same AND every other pair is: a comparison that came out false settles
        // the answer whatever the other one did. Answering unknown as soon as any operand was null
        // made "1 BETWEEN 2 AND NULL" unknown, where 1 is below 2 whatever the upper bound is.
        Boolean inRange;
        if (bet.symmetric()) {
            inRange = or3(and3(atLeast(val, low), atMost(val, high)),
                    and3(atLeast(val, high), atMost(val, low)));
        } else {
            inRange = and3(atLeast(val, low), atMost(val, high));
        }
        if (inRange == null) return null;
        return bet.negated() ? Boolean.valueOf(!inRange.booleanValue()) : inRange;
    }

    /** {@code a >= b}, unknown where either side is. */
    private Boolean atLeast(Object a, Object b) {
        if (a == null || b == null) return null;
        return Boolean.valueOf(compareValues(a, b) >= 0);
    }

    /** {@code a <= b}, unknown where either side is. */
    private Boolean atMost(Object a, Object b) {
        if (a == null || b == null) return null;
        return Boolean.valueOf(compareValues(a, b) <= 0);
    }

    /** AND over the three values, where one false settles it without the other being known. */
    private static Boolean and3(Boolean a, Boolean b) {
        if (Boolean.FALSE.equals(a) || Boolean.FALSE.equals(b)) return Boolean.FALSE;
        return a == null || b == null ? null : Boolean.TRUE;
    }

    /** OR over the three values, where one true settles it without the other being known. */
    private static Boolean or3(Boolean a, Boolean b) {
        if (Boolean.TRUE.equals(a) || Boolean.TRUE.equals(b)) return Boolean.TRUE;
        return a == null || b == null ? null : Boolean.FALSE;
    }

    private Object evalLike(LikeExpr like, RowContext ctx) {
        Object leftVal = evalExpr(like.left(), ctx);
        Object patternVal = evalExpr(like.pattern(), ctx);
        if (leftVal == null || patternVal == null) return null;
        // PG only allows LIKE on text-like types; reject integers, booleans, json, etc.
        if (leftVal instanceof Number || leftVal instanceof Boolean) {
            String typeName = leftVal instanceof Integer ? "integer" : leftVal instanceof Long ? "bigint" :
                    leftVal instanceof Boolean ? "boolean" : leftVal.getClass().getSimpleName().toLowerCase();
            throw new MemgresException("operator does not exist: " + typeName + " ~~ unknown", "42883");
        }
        // Check if left operand comes from a json-returning function
        if (like.left() instanceof FunctionCallExpr) {
            String fnName = ((FunctionCallExpr) like.left()).name().toLowerCase();
            if (fnName.equals("row_to_json") || fnName.equals("to_json") || fnName.equals("json_build_object")
                    || fnName.equals("json_build_array") || fnName.equals("json_agg") || fnName.equals("json_object")) {
                throw new MemgresException("operator does not exist: json ~~ unknown", "42883");
            }
        }
        if (like.left() instanceof CastExpr) {
            String castType = ((CastExpr) like.left()).typeName().toLowerCase();
            if (castType.equals("json")) {
                throw new MemgresException("operator does not exist: json ~~ unknown", "42883");
            }
        }
        String str = leftVal.toString();
        String pat = patternVal.toString();
        String esc = null;
        if (like.escape() != null) {
            Object escVal = evalExpr(like.escape(), ctx);
            // The escape is an operand, and a comparison with an operand that is nothing has
            // nothing to answer.
            if (escVal == null) return null;
            esc = escVal.toString();
            if (esc.length() > 1) {
                // PostgreSQL states the rule the string broke rather than only calling it invalid.
                throw new MemgresException("invalid escape string"
                        + "\n  Hint: Escape string must be empty or one character.", "22025");
            }
        }
        boolean matches = likeMatch(str, pat, esc, like.caseInsensitive());
        return like.negated() ? !matches : matches;
    }

    /**
     * Equality, with an enum compared against a word by the enum's own reading of it.
     *
     * <p>A word the enum does not hold is not a value of that type at all, so asking whether a
     * value equals it is asking about something that does not exist. Comparing the two as text
     * answered false, which reads as "they are different" rather than "there is no such value".
     */
    private boolean enumEquals(Object left, Object right) {
        if ((left instanceof AstExecutor.PgEnum && right instanceof String)
                || (right instanceof AstExecutor.PgEnum && left instanceof String)) {
            return compareValues(left, right) == 0;
        }
        return TypeCoercion.areEqual(left, right);
    }

    /** Where a written label stands in an enum, refusing one the enum does not hold. */
    private static int enumOrdinal(CustomEnum ce, AstExecutor.PgEnum of, String label) {
        if (!ce.isValidLabel(label)) {
            String named = of.typeName();
            int dot = named.lastIndexOf('.');
            throw new MemgresException("invalid input value for enum "
                    + (dot < 0 ? named : named.substring(dot + 1)) + ": \"" + label + "\"",
                    "22P02");
        }
        return ce.ordinal(label);
    }

    /** Whether a type is one of the string types, which are the ones text operators take. */
    private static boolean isTextLike(DataType type) {
        return type == null || type == DataType.TEXT || type == DataType.VARCHAR
                || type == DataType.CHAR || type == DataType.NAME;
    }

    private Object evalCase(CaseExpr c, RowContext ctx) {
        // PG validates type compatibility at plan time (before short-circuit evaluation).
        validateCaseBranchTypes(c);

        if (c.operand() != null) {
            Object operand = evalExpr(c.operand(), ctx);
            for (CaseExpr.WhenClause when : c.whenClauses()) {
                Object whenVal = evalExpr(when.condition(), ctx);
                // Simple CASE is defined as "operand = whenVal" (PG docs), so use the same
                // equality semantics as the = operator (TypeCoercion.areEqual) rather than raw
                // Java equality. Raw Objects.equals silently failed for cross-representation
                // matches — most notably CASE <enum_col> WHEN 'label' (PgEnum vs String), which
                // made every WHEN miss and fall through to ELSE, e.g. turning the app's
                // "ORDER BY CASE type WHEN 'direct' THEN 0 ..." ranking into a constant.
                if (operand != null && whenVal != null && TypeCoercion.areEqual(operand, whenVal)) {
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
    /**
     * Validate a collation name at runtime, raising 42704 if unknown. Which names exist is one
     * question with one answer, so a COLLATE in an expression and a COLLATE in a column definition
     * ask it in the same place.
     */
    private void validateCollationAtRuntime(String collation) {
        DdlDefinitionChecks.requireCollationExists(executor.database, collation);
    }

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
        unifyResultTypes("CASE", caseResultBranches(c));
        resolveCaseOperandTypes(c);
    }

    /**
     * A simple CASE is written as {@code operand = value} for each WHEN, and PostgreSQL settles
     * both sides of that equality before it compares anything. The operand settles its own type
     * first -- an operand that says nothing about it is text, not whatever the first WHEN turns
     * out to be -- and each WHEN value is then read as that type, so a value from another family
     * has no equality to be tested with and a written-out one that will not read that way is an
     * input error. Comparing the values as they came out let {@code CASE 1 WHEN 'a'} answer.
     */
    private void resolveCaseOperandTypes(CaseExpr c) {
        if (c.operand() == null) return;
        String operandType = typeWrittenInQuery(c.operand());
        if (operandType == null) {
            if (!isUnknownConstant(c.operand())) return;
            operandType = "text";
        }
        ResultFamily operandFamily = familyOf(operandType);
        if (operandFamily == null) return;
        for (CaseExpr.WhenClause when : c.whenClauses()) {
            Expression value = when.condition();
            String valueType = typeWrittenInQuery(value);
            if (valueType == null) {
                // Reading an unknown as the settled type is what turns a mismatch into an input
                // error, and only the families memgres reads exactly as PostgreSQL does take part.
                if (isUntypedStringLiteral(value) && (operandFamily == ResultFamily.NUMERIC
                        || operandFamily == ResultFamily.BOOLEAN)) {
                    executor.castValue(((Literal) value).value(), operandType);
                }
                continue;
            }
            ResultFamily valueFamily = familyOf(valueType);
            if (valueFamily == null || valueFamily == operandFamily) continue;
            throw new MemgresException("operator does not exist: " + pgName(operandType) + " = "
                    + pgName(valueType)
                    + "\n  Hint: No operator matches the given name and argument types."
                    + " You might need to add explicit type casts.", "42883");
        }
    }

    // ---- Subquery evaluation ----

    private Object evalSubquery(SubqueryExpr sq, RowContext outerCtx) {
        // A subquery that reads nothing of the row it stands beside has one answer for the whole
        // statement, which is both what PostgreSQL gives it and one run of it instead of one per
        // row. Anything that might read the outer row is left to be run again each time.
        AstExecutor.ScalarSubqueryValue held = executor.scalarSubqueryValue(sq);
        if (held != null && held.answered) return held.value;
        Object answer = runSubquery(sq, outerCtx);
        if (held != null) {
            held.value = answer;
            held.answered = true;
        }
        return answer;
    }

    private Object runSubquery(SubqueryExpr sq, RowContext outerCtx) {
        if (outerCtx != null) executor.outerContextStack.push(outerCtx);
        try {
            QueryResult result = executor.executeStatement(sq.subquery());
            // How wide a scalar subquery is, is a property of its select list and not of the rows
            // it happens to return, so PostgreSQL refuses a second column before it looks at any
            // row -- including when there are none. Reading the width off the first row instead
            // let SELECT (SELECT 1, 2 WHERE false) answer NULL, and reported "more than one row"
            // for a query whose real fault was its width.
            rejectWideSubquery(sq.subquery(), result);
            if (result.getRows().isEmpty()) return null;
            if (result.getRows().size() > 1) {
                throw new MemgresException("more than one row returned by a subquery used as an expression", "21000");
            }
            Object[] firstRow = result.getRows().get(0);
            if (firstRow.length > 0) return firstRow[0];
            return null;
        } finally {
            if (outerCtx != null) executor.outerContextStack.pop();
        }
    }

    /**
     * A subquery standing where one value is expected may have only one column -- and must have
     * one. {@code SELECT} with an empty select list is a query PostgreSQL parses and gives no
     * columns at all, so {@code SELECT (SELECT)} is the same complaint as {@code SELECT (SELECT
     * 1, 2)}; answering NULL for it was reading a column that was not there. The empty list is
     * read off the statement rather than off the result, because a result the engine could not
     * describe also has no columns and refusing on that would refuse working SQL.
     */
    /**
     * Refuse a subquery of more than one column written where one value stands -- an entry of an
     * IN list or of an array constructor. The width is read off the select list rather than from
     * running the query, both because that is where PostgreSQL reads it and because an entry that
     * a comparison never reaches is one this engine has no result for.
     */
    /**
     * A row on the left of IN is compared against each entry of the list, so every entry has to be
     * a row of the same width: an entry of a different width has no comparison to make and one
     * that is not a row at all has no operator. Both were read entry by entry as far as they went,
     * so {@code ROW(1,2) IN (ROW(1,2,3))} answered false and {@code (a, b) IN ((1))} answered
     * nothing found.
     */
    private void rejectRowEntryMismatch(InExpr in) {
        int want = writtenRowWidth(in.expr());
        if (want <= 1) return;
        for (int i = 0; i < in.values().size(); i++) {
            Expression entry = in.values().get(i);
            int got = writtenRowWidth(entry);
            if (got > 0) {
                if (got != want) {
                    throw new MemgresException("unequal number of entries in row expressions", "42601");
                }
                continue;
            }
            if (isWrittenScalar(entry)) throw noRecordOperator(typeNameOf(entry), false);
        }
    }

    /**
     * The left side of an IN, read as the type the comparison settles on. Every entry is compared
     * against the left with the same operator, so PostgreSQL settles one type across the left and
     * the whole list before it compares anything: an entry written out but not readable as that
     * type is an error whether or not an earlier entry already matched, and an untyped left is
     * read as the list's type rather than kept as text. Reading them one pair at a time and
     * stopping at the first match let {@code 1 IN (1, 'x')} answer true and {@code 'x' IN (1)}
     * answer false.
     *
     * @return the left value, read as the settled type where the query left that open
     */
    private Object resolveInListTypes(InExpr in, Object val) {
        // "= ANY(<array>)" is one value on the right, not a list, and the array settles its own
        // element type.
        if (in.fromAny()) return val;
        String settled = typeWrittenInQuery(in.expr());
        for (int i = 0; settled == null && i < in.values().size(); i++) {
            settled = typeWrittenInQuery(in.values().get(i));
        }
        // Only the families memgres reads exactly as PostgreSQL does take part: a date or a string
        // accepts too much for a failure here to mean what PostgreSQL's failure means.
        ResultFamily family = settled == null ? null : familyOf(settled);
        if (family != ResultFamily.NUMERIC && family != ResultFamily.BOOLEAN) return val;
        for (int i = 0; i < in.values().size(); i++) {
            Expression entry = in.values().get(i);
            if (isUntypedStringLiteral(entry)) executor.castValue(((Literal) entry).value(), settled);
        }
        return isUntypedStringLiteral(in.expr())
                ? executor.castValue(((Literal) in.expr()).value(), settled) : val;
    }

    static void rejectWideSubqueryElements(List<Expression> elements) {
        if (elements == null) return;
        for (int i = 0; i < elements.size(); i++) {
            Expression e = elements.get(i);
            if (!(e instanceof SubqueryExpr)) continue;
            if (SelectStmt.writtenWidth(((SubqueryExpr) e).subquery()) > 1) {
                throw new MemgresException("subquery must return only one column", "42601");
            }
        }
    }

    static void rejectWideSubquery(Statement subquery, QueryResult result) {
        if (result.getColumns() != null && result.getColumns().size() > 1) {
            throw new MemgresException("subquery must return only one column", "42601");
        }
        if (subquery instanceof SelectStmt) {
            List<SelectStmt.SelectTarget> targets = ((SelectStmt) subquery).targets();
            if (targets != null && targets.isEmpty()) {
                throw new MemgresException("subquery must return only one column", "42601");
            }
        }
    }

    /**
     * How many columns the side of a comparison written as {@code expr} occupies, or -1 when that
     * cannot be told from the query text.
     *
     * <p>A row constructor occupies one column per element, and that is a property of how it is
     * written. Everything else occupies one column -- unless the value turns out to be a row, and
     * a value can be a row without a row constructor: {@code x} in {@code FROM t x} is the whole
     * row of t, and {@code x IN (SELECT y FROM t y)} compares one whole row against another. Such
     * a side is left unmeasured rather than counted as its element count, which would have made
     * the whole-row comparison look like a width clash and refused a query PostgreSQL runs.
     */
    private static int comparandWidth(Expression expr, Object value) {
        if (expr instanceof ArrayExpr && ((ArrayExpr) expr).isRow()) {
            return ((ArrayExpr) expr).elements().size();
        }
        if (value instanceof AstExecutor.PgRow || value instanceof List<?>) return -1;
        return 1;
    }

    /**
     * Both sides of a comparison against a subquery have to be the same width, and PostgreSQL
     * settles that from the select lists before it reads a row -- so a subquery of the wrong width
     * is refused whether or not it returns anything. Neither an unmeasurable side nor a subquery
     * with no column list at all (one the engine could not describe) is judged: refusing on the
     * strength of either would refuse comparisons PostgreSQL makes.
     */
    private static void rejectSubqueryWidth(int want, QueryResult sub) {
        if (want < 0 || sub.getColumns() == null || sub.getColumns().isEmpty()) return;
        int got = sub.getColumns().size();
        if (got < want) throw new MemgresException("subquery has too few columns", "42601");
        if (got > want) throw new MemgresException("subquery has too many columns", "42601");
    }

    /**
     * A row constructor compared against a subquery reads the whole subquery row, not just its
     * first column: {@code (1, 2) = (SELECT 1, 2)} is a row comparison and true. Sending the
     * subquery down the scalar path refused it as too wide, so a legitimate row comparison came
     * back as an error while a genuinely wide scalar subquery was let through.
     *
     * <p>Only the six ordered comparisons take this reading. IS DISTINCT FROM does not -- measured
     * against PostgreSQL 18, which refuses a wide subquery there -- and neither does a subquery
     * written on the left.
     */
    private static boolean isRowSubqueryComparison(BinaryExpr bin) {
        if (!(bin.right() instanceof SubqueryExpr)) return false;
        if (!(bin.left() instanceof ArrayExpr) || !((ArrayExpr) bin.left()).isRow()) return false;
        switch (bin.op()) {
            case EQUAL: case NOT_EQUAL:
            case LESS_THAN: case GREATER_THAN: case LESS_EQUAL: case GREATER_EQUAL:
                return true;
            default:
                return false;
        }
    }

    private Object evalRowSubqueryComparison(BinaryExpr bin, RowContext ctx) {
        ArrayExpr leftRow = (ArrayExpr) bin.left();
        SubqueryExpr sq = (SubqueryExpr) bin.right();
        if (ctx != null) executor.outerContextStack.push(ctx);
        QueryResult result;
        try {
            result = executor.executeStatement(sq.subquery());
        } finally {
            if (ctx != null) executor.outerContextStack.pop();
        }
        int want = leftRow.elements().size();
        int got = result.getColumns() == null ? 0 : result.getColumns().size();
        // The widths have to agree for the comparison to exist at all, so PostgreSQL settles them
        // before it looks at a row -- an empty subquery of the wrong width is still refused.
        if (got < want) throw new MemgresException("subquery has too few columns", "42601");
        if (got > want) throw new MemgresException("subquery has too many columns", "42601");
        rejectRowSubqueryTypeMismatch(bin, leftRow, result);
        if (result.getRows().isEmpty()) return null;
        if (result.getRows().size() > 1) {
            throw new MemgresException("more than one row returned by a subquery used as an expression", "21000");
        }
        Object leftVal = evalExpr(leftRow, ctx);
        List<Object> rightValues = new ArrayList<>(Arrays.asList(result.getRows().get(0)));
        return executor.binaryOpEvaluator.evalBinaryValues(bin.op(), leftVal,
                new AstExecutor.PgRow(rightValues));
    }

    /**
     * Entry by entry the comparison has to have an operator, and PostgreSQL resolves that from the
     * declared types before any row is read. Only a literal that carries its own type is judged
     * here: a bare string literal is untyped and takes whatever the other side is, and anything
     * the engine merely inferred a type for would refuse a comparison PostgreSQL makes.
     */
    private void rejectRowSubqueryTypeMismatch(BinaryExpr bin, ArrayExpr leftRow, QueryResult result) {
        for (int i = 0; i < leftRow.elements().size(); i++) {
            Expression element = leftRow.elements().get(i);
            if (!(element instanceof Literal)) continue;
            rejectUntypedTextEntry(element, result.getColumns().get(i).getType());
            Literal.LiteralType kind = ((Literal) element).literalType();
            if (kind != Literal.LiteralType.INTEGER && kind != Literal.LiteralType.FLOAT
                    && kind != Literal.LiteralType.BOOLEAN) {
                continue;
            }
            DataType leftType = executor.inferExprType(element);
            DataType rightType = result.getColumns().get(i).getType();
            if (leftType == null || rightType == null || leftType == rightType) continue;
            TypeCoercion.TypeCategory leftCat = TypeCoercion.categoryOf(leftType);
            TypeCoercion.TypeCategory rightCat = TypeCoercion.categoryOf(rightType);
            if (leftCat == null || rightCat == null || leftCat == rightCat) continue;
            throw new MemgresException("operator does not exist: " + leftType.toRegtypeDisplay()
                    + " " + BinaryOpEvaluator.opSymbol(bin.op()) + " " + rightType.toRegtypeDisplay(),
                    "42883");
        }
    }

    /**
     * One column of the row a multi-column UPDATE assignment takes its values from. The columns
     * of one assignment share the source node, so the row is read once per updated row: whichever
     * column is evaluated first binds it in that row's context and the rest read the binding. A
     * source that returns no row leaves every one of the columns null, which is what PostgreSQL
     * assigns.
     */
    private Object evalRowElement(RowElementExpr re, RowContext ctx) {
        Object row;
        if (ctx != null && ctx.hasBoundValue(re.source())) {
            row = ctx.getBoundValue(re.source());
        } else {
            row = evalRowElementSource(re, ctx);
            if (ctx != null) ctx.setBoundValue(re.source(), row);
        }
        if (!(row instanceof AstExecutor.PgRow)) return null;
        List<Object> values = ((AstExecutor.PgRow) row).values();
        return re.index() < values.size() ? values.get(re.index()) : null;
    }

    private Object evalRowElementSource(RowElementExpr re, RowContext ctx) {
        if (!(re.source() instanceof SubqueryExpr)) {
            return evalExpr(re.source(), ctx);
        }
        SubqueryExpr sq = (SubqueryExpr) re.source();
        if (ctx != null) executor.outerContextStack.push(ctx);
        QueryResult result;
        try {
            result = executor.executeStatement(sq.subquery());
        } finally {
            if (ctx != null) executor.outerContextStack.pop();
        }
        int got = result.getColumns() == null ? 0 : result.getColumns().size();
        if (got > 0 && got != re.width()) {
            throw new MemgresException("number of columns does not match number of values", "42601");
        }
        if (result.getRows().isEmpty()) return null;
        if (result.getRows().size() > 1) {
            throw new MemgresException("more than one row returned by a subquery used as an expression", "21000");
        }
        return new AstExecutor.PgRow(new ArrayList<>(Arrays.asList(result.getRows().get(0))));
    }

    /**
     * A string literal written in a row carries no type of its own, so PostgreSQL reads it as the
     * type the entry opposite has -- and the reading is what fails when the text is not a value of
     * that type. {@code (1, 'a') = (SELECT 1, 2)} is not a comparison that comes out false; it is
     * an 'a' that is not an integer. Comparing the text with the number instead answered false,
     * where the same row written with '1' answers true.
     */
    private void rejectUntypedTextEntry(Expression entry, DataType otherType) {
        if (!(entry instanceof Literal)) return;
        if (((Literal) entry).literalType() != Literal.LiteralType.STRING) return;
        if (otherType == null || otherType == DataType.TEXT || otherType == DataType.VARCHAR
                || otherType == DataType.CHAR) {
            return;
        }
        String text = ((Literal) entry).value();
        if (text == null) return;
        try {
            executor.castEvaluator.applyCast(text, otherType.getPgName());
        } catch (RuntimeException e) {
            throw new MemgresException("invalid input syntax for type "
                    + otherType.toRegtypeDisplay() + ": \"" + text + "\"", "22P02");
        }
    }

    private Object evalExists(ExistsExpr ex, RowContext outerCtx) {
        Boolean fromKeys = existsFromKeyIndex(ex, outerCtx);
        if (fromKeys != null) return fromKeys;
        if (outerCtx != null) executor.outerContextStack.push(outerCtx);
        try {
            QueryResult result = executor.executeStatement(ex.subquery());
            return !result.getRows().isEmpty();
        } finally {
            if (outerCtx != null) executor.outerContextStack.pop();
        }
    }

    /**
     * A correlated EXISTS that tests one column of one relation against a value from the outer
     * row is answered from that column's values, collected once for the statement. See
     * {@link ExistsKeyIndex}. Null means the subquery has to be run as written.
     */
    private Boolean existsFromKeyIndex(ExistsExpr ex, RowContext outerCtx) {
        if (outerCtx == null) return null;
        ExistsKeyIndex idx = executor.existsKeyIndex(ex);
        if (idx == ExistsKeyIndex.NOT_INDEXABLE) return null;
        Object outerValue;
        try {
            outerValue = evalExpr(idx.outerSide(), outerCtx);
        } catch (RuntimeException e) {
            return null;   // the value does not resolve here; let the subquery say so
        }
        // NULL is equal to nothing, so the subquery finds no row whatever the relation holds.
        if (outerValue == null) return Boolean.FALSE;
        return idx.contains(executor, outerValue);
    }

    /**
     * The comparison an ANY or ALL makes is the ordinary one between the left side and one value
     * of the subquery, and PostgreSQL settles both types from the query text before it runs
     * anything: a subquery hands its answer out as a value of some type, so an unknown in its
     * select list is text by the time the comparison is built. Waiting for the values meant a
     * subquery that produced no rows was never checked at all, and one that did was judged by
     * whether its first value happened to look like a number.
     */
    private void resolveAnyAllTypes(AnyAllExpr aa) {
        String rightType = subqueryOutputType(aa.subquery());
        if (rightType == null) return;
        ResultFamily rightFamily = familyOf(rightType);
        if (rightFamily == null) return;
        String leftType = typeWrittenInQuery(aa.left());
        if (leftType == null) {
            // Reading an unknown left side as the type it is compared against is what turns a
            // mismatch into an input error, and only the families read exactly as PostgreSQL
            // reads them take part.
            if (isUntypedStringLiteral(aa.left()) && (rightFamily == ResultFamily.NUMERIC
                    || rightFamily == ResultFamily.BOOLEAN)) {
                executor.castValue(((Literal) aa.left()).value(), rightType);
            }
            return;
        }
        ResultFamily leftFamily = familyOf(leftType);
        if (leftFamily == null || leftFamily == rightFamily) return;
        throw noAnyAllOperator(pgName(leftType), aa.op(), pgName(rightType));
    }

    /** PostgreSQL's complaint that a comparison has no operator between two types. */
    private static MemgresException noAnyAllOperator(String leftType, BinaryExpr.BinOp op,
            String rightType) {
        return new MemgresException("operator does not exist: " + leftType + " "
                + BinaryOpEvaluator.opSymbol(op) + " " + rightType
                + "\n  Hint: No operator matches the given name and argument types."
                + " You might need to add explicit type casts.", "42883");
    }

    private Object evalAnyAll(AnyAllExpr aa, RowContext ctx) {
        resolveAnyAllTypes(aa);
        Object leftVal = evalExpr(aa.left(), ctx);

        if (ctx != null) executor.outerContextStack.push(ctx);
        QueryResult subResult;
        try {
            subResult = executor.executeStatement(aa.subquery());
        } finally {
            if (ctx != null) executor.outerContextStack.pop();
        }

        // Where the query did not say what the subquery produces, the values it produced are the
        // only evidence there is: text on the right of an arithmetic comparison with a number on
        // the left has no operator, whatever else the column may hold.
        if (leftVal instanceof Number && !subResult.getRows().isEmpty()) {
            Object firstElem = subResult.getRows().get(0).length > 0 ? subResult.getRows().get(0)[0] : null;
            if (firstElem instanceof String && !((String) firstElem).isEmpty()) {
                String s = (String) firstElem;
                try { new java.math.BigDecimal(s); } catch (NumberFormatException e) {
                    throw noAnyAllOperator(AstExecutor.pgTypeNameOf(leftVal), aa.op(), "text");
                }
            }
        }

        // A row-constructor left side compares against the whole subquery row, not just
        // its first column: (1,2) = ANY (SELECT xi, yi FROM pts).
        int written = comparandWidth(aa.left(), leftVal);
        int leftWidth = leftVal instanceof AstExecutor.PgRow
                ? ((AstExecutor.PgRow) leftVal).values().size() : 1;
        // ... and one that is not a row constructor compares against one column, so a wider
        // subquery has no comparison to make. Reading only the first column of it answered
        // 1 = ANY (SELECT id, name FROM t) with true.
        rejectSubqueryWidth(written, subResult);

        // A set with nothing in it settles the answer without comparing anything: ALL holds over
        // nothing and ANY holds over nothing for no value at all, whether or not the left side is
        // known. Only once something is there to compare against is a null on the left unknown.
        if (subResult.getRows().isEmpty()) return aa.isAll();
        if (leftVal == null) return null;

        if (aa.isAll()) {
            boolean hasNull = false;
            for (Object[] row : subResult.getRows()) {
                Object elem = subqueryElement(row, leftWidth);
                if (elem == null) { hasNull = true; continue; }
                if (!evalComparisonOp(aa.op(), leftVal, elem)) return false;
            }
            return hasNull ? null : true;
        } else {
            boolean hasNull = false;
            for (Object[] row : subResult.getRows()) {
                Object elem = subqueryElement(row, leftWidth);
                if (elem == null) { hasNull = true; continue; }
                if (evalComparisonOp(aa.op(), leftVal, elem)) return true;
            }
            return hasNull ? null : false;
        }
    }

    /** One comparable value from a subquery row: a scalar, or a composite when the
     *  left-hand side is a row constructor. */
    private static Object subqueryElement(Object[] row, int width) {
        if (width <= 1) return row.length > 0 ? row[0] : null;
        List<Object> values = new ArrayList<>(width);
        for (int i = 0; i < width; i++) values.add(i < row.length ? row[i] : null);
        return new AstExecutor.PgRow(values);
    }

    /**
     * An expression standing for one element of the array an ANY/ALL compares against, so the
     * operator can be looked up against the element type. A constructor gives its first element; a
     * cast to an array type gives a cast to that array's element type.
     */
    /**
     * An operand standing for one element of an array whose type the query settles elsewhere.
     * A column names its array type as the catalogs spell it, {@code _text}, and a cast names it
     * as it was written, {@code text[]}; both mean the same array.
     */
    private Expression elementByDeclaredType(Expression array, RowContext ctx) {
        String declared = executor.binaryOpEvaluator.declaredTypeForResolution(array, ctx);
        if (declared == null) return null;
        declared = declared.trim();
        if (declared.endsWith("[]")) {
            return new CastExpr(array, declared.substring(0, declared.length() - 2).trim());
        }
        DataType type = DataType.fromPgName(declared.toLowerCase());
        if (type == null) return null;
        // oidvector and int2vector are arrays as far as a subscript or an ANY is concerned, even
        // though they are their own types: pg_index.indkey is written "= ANY(i.indkey)" by every
        // client that lists indexes.
        if (type == DataType.OIDVECTOR) return new CastExpr(array, DataType.OID.getPgName());
        if (type == DataType.INT2VECTOR) return new CastExpr(array, DataType.SMALLINT.getPgName());
        if (!DataType.isArrayType(type)) return null;
        DataType element = DataType.elementOf(type);
        return element == null ? null : new CastExpr(array, element.getPgName());
    }

    /**
     * The value an array's elements are, however deeply the constructor nests them. PostgreSQL's
     * array types carry no dimensionality — {@code ARRAY[ARRAY[1],ARRAY[2]]} is an
     * {@code integer[]} — so what {@code = ANY} compares against is the innermost value.
     */
    private static Expression innermostElementOperand(Expression element) {
        Expression current = element;
        while (current instanceof ArrayExpr && !((ArrayExpr) current).isRow()) {
            java.util.List<Expression> items = ((ArrayExpr) current).elements();
            if (items == null || items.isEmpty()) return current;
            current = items.get(0);
        }
        return current;
    }

    private static Expression arrayElementOperand(Expression array) {
        if (array instanceof ArrayExpr) {
            java.util.List<Expression> items = ((ArrayExpr) array).elements();
            return items.isEmpty() ? null : innermostElementOperand(items.get(0));
        }
        if (array instanceof CastExpr) {
            String type = ((CastExpr) array).typeName();
            if (type != null && type.trim().endsWith("[]")) {
                String element = type.trim().substring(0, type.trim().length() - 2).trim();
                return new CastExpr(((CastExpr) array).expr(), element);
            }
        }
        return null;
    }

    private Object evalAnyAllArray(AnyAllArrayExpr aaa, RowContext ctx) {
        // "= ALL(array)" resolves "=" the same way; "<> ANY" resolves "<>", which point does have
        Expression element = arrayElementOperand(aaa.array());
        if (element != null) {
            executor.binaryOpEvaluator.rejectUnresolvableOperator(
                    new BinaryExpr(aaa.left(), aaa.op(), element), ctx);
        }
        // x = ANY(ARRAY[...]) resolves the same "=" a plain comparison does, so a type that has
        // no "=" cannot be compared this way either. This path never reaches BinaryOpEvaluator,
        // so the rule is consulted directly against the left side and the array's first element.
        Object leftVal = evalExpr(aaa.left(), ctx);
        Object arrayVal = evalExpr(aaa.array(), ctx);
        if (arrayVal == null) return null;
        // The array is read by the one array reader, so a quoted element holding a comma stays
        // one element.
        List<?> elements = PgArray.from(arrayVal);
        if (elements == null) elements = Cols.listOf(arrayVal);
        // An array with nothing in it settles the answer without comparing anything, which is why
        // it answers even for a NULL on the left: there is nothing there to be unknown about.
        if (elements.isEmpty()) return aaa.isAll();
        if (leftVal == null) return null;
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
                return enumEquals(left, right);
            case NOT_EQUAL:
                return !enumEquals(left, right);
            case LESS_THAN:
                return compareValues(left, right) < 0;
            case GREATER_THAN:
                return compareValues(left, right) > 0;
            case LESS_EQUAL:
                return compareValues(left, right) <= 0;
            case GREATER_EQUAL:
                return compareValues(left, right) >= 0;
            default:
                // Every operator that answers a boolean can stand in front of ANY or ALL, not
                // only the six comparisons: PostgreSQL accepts LIKE ANY, ~ ANY and the rest.
                return executor.isTruthy(executor.binaryOpEvaluator.evalBinaryValues(op, left, right));
        }
    }

    // ---- Value comparison and truthiness ----

    @SuppressWarnings("unchecked")
    int compareValues(Object a, Object b) {
        // PgRow (record) comparison: element-by-element, like PG record comparison
        if (a instanceof AstExecutor.PgRow && b instanceof AstExecutor.PgRow) {
            List<Object> la = ((AstExecutor.PgRow) a).values;
            List<Object> lb = ((AstExecutor.PgRow) b).values;
            int minLen = Math.min(la.size(), lb.size());
            for (int i = 0; i < minLen; i++) {
                int cmp = compareValues(la.get(i), lb.get(i));
                if (cmp != 0) return cmp;
            }
            return Integer.compare(la.size(), lb.size());
        }
        // Two arrays are compared where every array is compared, by array_cmp's rules — including
        // the one this walk had wrong, that a null element sorts after every value.
        if (a instanceof List<?> && b instanceof List<?>) {
            return TypeCoercion.compare(a, b);
        }
        // A path is ordered by how many points it holds and by nothing else, so two paths of
        // one length stand in no order at all however far apart their points are.
        if (a instanceof String && b instanceof String) {
            Integer paths = GeometricOperations.comparePaths((String) a, (String) b);
            if (paths != null) return paths.intValue();
        }
        // Two documents are ordered as documents: by how many lexemes they hold, and then by
        // the lexemes as bytes. Comparing the text they print as put them in the order their
        // quotes and colons happened to fall in.
        if (a instanceof TsVector && b instanceof TsVector) {
            return TextSearchOperations.compareVectors((TsVector) a, (TsVector) b);
        }
        // PgEnum values compare by ordinal position (creation order)
        if (a instanceof AstExecutor.PgEnum && b instanceof AstExecutor.PgEnum) {
            AstExecutor.PgEnum eb = (AstExecutor.PgEnum) b;
            AstExecutor.PgEnum ea = (AstExecutor.PgEnum) a;
            return ea.compareTo(eb);
        }
        // One PgEnum and one String (e.g., WHERE enum_col < 'value'); the string is read by the
        // enum's own input function, and a word the enum does not hold is not a value of it --
        // falling through to a text comparison answered for a value that does not exist.
        if (a instanceof AstExecutor.PgEnum && b instanceof String) {
            AstExecutor.PgEnum ea = (AstExecutor.PgEnum) a;
            CustomEnum ce = executor.database.getCustomEnum(ea.typeName());
            if (ce != null) return Integer.compare(ea.ordinal(), enumOrdinal(ce, ea, (String) b));
        }
        if (b instanceof AstExecutor.PgEnum && a instanceof String) {
            AstExecutor.PgEnum eb = (AstExecutor.PgEnum) b;
            CustomEnum ce = executor.database.getCustomEnum(eb.typeName());
            if (ce != null) return Integer.compare(enumOrdinal(ce, eb, (String) a), eb.ordinal());
        }
        return TypeCoercion.compare(a, b);
    }

    boolean isTruthy(Object val) {
        if (val == null) return false;
        if (val instanceof Boolean) return ((Boolean) val);
        if (val instanceof Number) return ((Number) val).doubleValue() != 0;
        if (val instanceof String) {
            // A string standing where a condition does is read by boolean's input function, which
            // knows more words than "true" and "false": 'f', 'no', 'off' and 'n' are all false and
            // were all being read as true because they are neither empty nor the word "false".
            // A string that is no boolean word at all is left as it was — the clause-level checks
            // refuse the ones PostgreSQL refuses, and guessing here would refuse more than that.
            try {
                return Boolean.TRUE.equals(TypeCoercion.toBoolean(val));
            } catch (MemgresException e) {
                return !((String) val).isEmpty();
            }
        }
        return true;
    }

    /** Match a SQL LIKE pattern with backslash as the escape character. */
    static boolean likeMatch(String text, String pattern, boolean caseInsensitive) {
        return likeMatch(text, pattern, "\\", caseInsensitive);
    }

    /**
     * PostgreSQL's LIKE matcher, following like_match.c rather than a regex translation.
     * The difference that matters is when the pattern ends with the escape character:
     * PostgreSQL only complains once matching actually walks onto that escape, so
     * {@code 'abc' LIKE 'ab\'} is an error while {@code 'ab' LIKE 'ab\'} is plain false.
     *
     * @param escape the escape string; empty disables escaping, null means the default backslash
     */
    static boolean likeMatch(String text, String pattern, String escape, boolean caseInsensitive) {
        boolean noEscape = escape != null && escape.isEmpty();
        int esc = (escape == null || escape.isEmpty()) ? '\\' : escape.codePointAt(0);
        return matchLike(text, 0, pattern, 0, esc, noEscape, caseInsensitive);
    }

    /**
     * The walk itself, over characters rather than over the units they are stored in.
     *
     * <p>{@code _} stands for one character, and a character above U+FFFF is held in two units.
     * Stepping a unit at a time made {@code _} stand for half of one, so a pattern of a single
     * underscore did not match a single emoji and the two halves of it were compared separately.
     */
    private static boolean matchLike(String t, int ti, String p, int pi,
                                     int esc, boolean noEscape, boolean ci) {
        int tlen = t.length();
        int plen = p.length();
        while (ti < tlen && pi < plen) {
            int pc = p.codePointAt(pi);
            if (!noEscape && pc == esc) {
                pi += Character.charCount(pc);
                if (pi >= plen) throw patternEndsWithEscape();
                if (!sameChar(p.codePointAt(pi), t.codePointAt(ti), ci)) return false;
            } else if (pc == '%') {
                pi++;
                while (pi < plen && p.charAt(pi) == '%') pi++;
                if (pi >= plen) return true; // a trailing % swallows the rest
                // The remainder must start with a literal, an escape or _; knowing which
                // keeps the search from retrying every position of the text.
                int firstpat = 0;
                boolean literalFirst = true;
                if (!noEscape && p.codePointAt(pi) == esc) {
                    int after = pi + Character.charCount(esc);
                    if (after >= plen) throw patternEndsWithEscape();
                    firstpat = p.codePointAt(after);
                } else if (p.charAt(pi) == '_') {
                    literalFirst = false;
                } else {
                    firstpat = p.codePointAt(pi);
                }
                for (int k = ti; k < tlen; k += Character.charCount(t.codePointAt(k))) {
                    if (literalFirst && !sameChar(firstpat, t.codePointAt(k), ci)) continue;
                    if (matchLike(t, k, p, pi, esc, noEscape, ci)) return true;
                }
                return false;
            } else if (pc == '_') {
                ti += Character.charCount(t.codePointAt(ti));
                pi++;
                continue;
            } else if (!sameChar(pc, t.codePointAt(ti), ci)) {
                return false;
            }
            ti += Character.charCount(t.codePointAt(ti));
            pi += Character.charCount(pc);
        }
        if (ti < tlen) return false;
        // The text is spent; only a run of % can still match nothing
        while (pi < plen && p.charAt(pi) == '%') pi++;
        return pi >= plen;
    }

    private static boolean sameChar(int a, int b, boolean caseInsensitive) {
        if (a == b) return true;
        return caseInsensitive
                && Character.toLowerCase(a) == Character.toLowerCase(b);
    }

    private static MemgresException patternEndsWithEscape() {
        return new MemgresException("LIKE pattern must not end with escape character", "22025");
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
        // PgMoney arithmetic: unwrap to BigDecimal and re-wrap result as PgMoney
        boolean isMoney = left instanceof PgMoney || right instanceof PgMoney;
        if (isMoney) {
            java.math.BigDecimal l = TypeCoercion.toBigDecimal(left);
            java.math.BigDecimal r = TypeCoercion.toBigDecimal(right);
            java.math.BigDecimal result = bdOp != null ? bdOp.apply(l, r) : java.math.BigDecimal.valueOf(doubleOp.apply(l.doubleValue(), r.doubleValue()));
            return new PgMoney(result);
        }
        // BigDecimal arithmetic, preserve precision
        if (left instanceof java.math.BigDecimal || right instanceof java.math.BigDecimal) {
            java.math.BigDecimal l = TypeCoercion.toBigDecimal(left);
            java.math.BigDecimal r = TypeCoercion.toBigDecimal(right);
            if (bdOp != null) return bdOp.apply(l, r);
            return java.math.BigDecimal.valueOf(doubleOp.apply(l.doubleValue(), r.doubleValue()));
        }
        // Integer arithmetic is done in the wider of the two operand types and checked against
        // that type's own bounds: int2 with int4 is an int4, either with int8 is an int8. There
        // used to be an arm for two smallints and an arm for two integers and nothing for a mixed
        // pair, so smallint beside a wider integer fell through to the float8 path below and
        // 5::int2 / 2::int4 answered 2.5 where PostgreSQL answers 2.
        if (isIntegerValue(left) && isIntegerValue(right)) {
            DataType width = widerIntegerType(left, right);
            try {
                long result = longOp.apply(toLong(left), toLong(right));
                return narrowedInteger(result, width);
            } catch (ArithmeticException e) {
                if (e.getMessage() != null && e.getMessage().contains("/ by zero")) {
                    throw new MemgresException("division by zero", "22012");
                }
                throw integerOutOfRange(width);
            }
        }
        // PG's float4 operators return float4. Computing in double and keeping the wide result
        // would let a real column hold a value real cannot represent, so narrow back and check.
        if (left instanceof Float && right instanceof Float) {
            double lf = ((Float) left).doubleValue();
            double rf = ((Float) right).doubleValue();
            return NumericLimits.checkFloat4(doubleOp.apply(lf, rf), lf, rf);
        }
        double result = doubleOp.apply(toDouble(left), toDouble(right));
        // Check for overflow: non-infinite inputs producing infinite result
        if (Double.isInfinite(result) && !Double.isInfinite(toDouble(left)) && !Double.isInfinite(toDouble(right))) {
            throw new MemgresException("value out of range: overflow", "22003");
        }
        return result;
    }

    double toDouble(Object val) {
        if (val instanceof PgMoney) return ((PgMoney) val).getValue().doubleValue();
        if (val instanceof Number) return ((Number) val).doubleValue();
        if (val instanceof String) {
            String s = (String) val;
            try { return Double.parseDouble(s); }
            catch (NumberFormatException e) { throw new MemgresException("invalid input syntax for type double precision: \"" + s + "\"", "22P02"); }
        }
        return 0;
    }

    int toInt(Object val) {
        if (val instanceof PgMoney) return ((PgMoney) val).getValue().intValue();
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
        if (val instanceof PgMoney) return ((PgMoney) val).getValue().longValue();
        if (val instanceof Number) return ((Number) val).longValue();
        String s = val.toString();
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            // A value the type cannot read is what the client got wrong, not what the server did:
            // letting the parse failure out unhandled reported it as an internal error, which
            // says nothing about the value and nothing a client can act on.
            throw new MemgresException("invalid input syntax for type bigint: \"" + s + "\"", "22P02");
        }
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

    /**
     * The concrete result type of a polymorphic call, inferred from the argument expressions,
     * or null when the arguments do not determine it.
     */
    private String polymorphicReturnType(PgFunction userFunc, FunctionCallExpr fn,
                                         List<RowContext.TableBinding> bindings) {
        List<String> declared = new ArrayList<>();
        for (PgFunction.Param p : userFunc.getParams()) {
            if ("OUT".equalsIgnoreCase(p.mode())) continue;
            declared.add(p.typeName());
        }
        List<String> actual = new ArrayList<>();
        for (Expression arg : fn.args()) {
            actual.add(polymorphicArgTypeName(arg, bindings));
        }
        PolymorphicTypes.Binding binding = PolymorphicTypes.bind(declared, actual);
        if (binding == null) return null;
        return PolymorphicTypes.concreteType(
                userFunc.getReturnType().replaceAll("\\(.*\\)", "").trim(), binding);
    }

    /**
     * The type name of one call argument, for polymorphic binding. An ARRAY[...] constructor
     * needs spelling out because the general inference reports every array as text.
     */
    private String polymorphicArgTypeName(Expression arg, List<RowContext.TableBinding> bindings) {
        if (arg instanceof ArrayExpr && !((ArrayExpr) arg).isRow()) {
            List<Expression> elements = ((ArrayExpr) arg).elements();
            if (elements.isEmpty()) return null;
            DataType elemType = inferTypeFromContext(elements.get(0), bindings);
            return elemType != null ? PolymorphicTypes.typeName(elemType) + "[]" : null;
        }
        DataType dt = inferTypeFromContext(arg, bindings);
        return PolymorphicTypes.typeName(dt);
    }

    /**
     * Look up a user function for result-type inference. A qualified call names its schema
     * outright; an unqualified one takes the first search_path schema that defines the name.
     */
    private PgFunction resolveUserFunctionForTyping(String name, FunctionCallExpr fn,
                                                    List<RowContext.TableBinding> bindings) {
        List<PgFunction> candidates;
        int dot = name.lastIndexOf('.');
        if (dot >= 0) {
            candidates = executor.database.getFunctionOverloads(name.substring(0, dot), name.substring(dot + 1));
        } else {
            List<PgFunction> all = executor.database.getFunctionOverloads(name);
            if (all.isEmpty()) return executor.database.getFunction(name);
            candidates = new ArrayList<>();
            for (String schema : executor.searchPathSchemas()) {
                for (PgFunction f : all) {
                    if (Database.schemaOf(f).equalsIgnoreCase(schema)) candidates.add(f);
                }
            }
            if (candidates.isEmpty()) candidates = all;
        }
        if (candidates.isEmpty()) return null;
        // Overloads differ in result type, so the call's argument types decide which one is meant.
        List<String> hints = new ArrayList<>();
        for (Expression arg : fn.args()) hints.add(polymorphicArgTypeName(arg, bindings));
        PgFunction resolved = executor.database.resolveFunction(candidates, fn.args().size(), hints);
        return resolved != null ? resolved : candidates.get(0);
    }

    // ---- Expression alias derivation ----

    /**
     * The SQL/JSON constructors, whose column PostgreSQL names after the construct that was
     * written and not after the routine underneath it. Two of them have a name of their own here
     * because the construct's name is already an ordinary function's — {@code json_object(text[])}
     * takes an array of alternating keys and values and is not the same routine at all.
     */
    private static final Map<String, String> CONSTRUCTOR_LABELS;

    static {
        Map<String, String> labels = new HashMap<>();
        labels.put("json_object_constructor", "json_object");
        labels.put("json_array_constructor", "json_array");
        labels.put("json_array_subquery", "json_array");
        CONSTRUCTOR_LABELS = labels;
    }

    String exprToAlias(Expression expr) {
        if (expr instanceof ColumnRef) return ((ColumnRef) expr).column();
        if (expr instanceof FunctionCallExpr) {
            // PG labels the column with the bare routine name; the schema qualifier is not part of it.
            String fnName = ((FunctionCallExpr) expr).name();
            int dot = fnName.lastIndexOf('.');
            return CONSTRUCTOR_LABELS.getOrDefault(fnName,
                    dot >= 0 ? fnName.substring(dot + 1) : fnName);
        }
        if (expr instanceof WindowFuncExpr) return ((WindowFuncExpr) expr).name();
        if (expr instanceof AtTimeZoneExpr) return "timezone";
        if (expr instanceof FieldAccessExpr) return ((FieldAccessExpr) expr).field();
        if (expr instanceof SubqueryExpr) {
            SubqueryExpr sq = (SubqueryExpr) expr;
            if (sq.subquery() instanceof SelectStmt && ((SelectStmt) sq.subquery()).targets() != null && !((SelectStmt) sq.subquery()).targets().isEmpty()) {
                SelectStmt sel = (SelectStmt) sq.subquery();
                SelectStmt.SelectTarget inner = sel.targets().get(0);
                if (inner.alias() != null) return inner.alias();
                if (inner.expr() instanceof WildcardExpr) {
                    return starSubqueryAlias(sel, ((WildcardExpr) inner.expr()).table());
                }
                return exprToAlias(inner.expr());
            }
            return "?column?";
        }
        if (expr instanceof ArraySubqueryExpr) return "array";
        if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            // A cast is named after whatever it is a cast *of*, however many casts deep that is:
            // typbasetype::regtype::text is still the typbasetype column. Only when nothing
            // underneath has a name of its own does the type supply one, and then it is the type
            // this cast ends at — 1::int::oid is an oid column, not an int4 one. Asking the
            // expression directly underneath instead let the innermost type's name out.
            Expression beneath = cast.expr();
            while (beneath instanceof CastExpr) beneath = ((CastExpr) beneath).expr();
            String inner = exprToAlias(beneath);
            return "?column?".equals(inner) ? castTypeToColumnName(cast.typeName()) : inner;
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            if ((bin.op() == BinaryExpr.BinOp.JSON_ARROW || bin.op() == BinaryExpr.BinOp.JSON_SUBSCRIPT)
                    && bin.left() instanceof ColumnRef) {
                ColumnRef cr = (ColumnRef) bin.left();
                return cr.column();
            }
            return "?column?";
        }
        if (expr instanceof CustomOperatorExpr) {
            return "?column?";
        }
        if (expr instanceof OrderedSetAggExpr) return ((OrderedSetAggExpr) expr).funcName().toLowerCase();
        if (expr instanceof JsonValueExpr) return "json_value";
        if (expr instanceof JsonQueryExpr) return "json_query";
        if (expr instanceof JsonExistsExpr) return "json_exists";
        if (expr instanceof CaseExpr) return "case";
        if (expr instanceof ArrayExpr) return ((ArrayExpr) expr).isRow() ? "row" : "array";
        if (expr instanceof ExistsExpr) return "exists";
        if (expr instanceof Literal) return "?column?";
        if (expr instanceof CollateExpr) return exprToAlias(((CollateExpr) expr).expr());
        if (expr instanceof CompositeStarExpr) return "?column?";
        if (expr instanceof ArraySliceExpr) return exprToAlias(((ArraySliceExpr) expr).array());
        // A subscript keeps the name of what it reaches into, which is why ARRAY[...][1] is
        // labelled array rather than ?column?.
        if (expr instanceof SubscriptExpr) return exprToAlias(((SubscriptExpr) expr).base());
        return "?column?";
    }

    /**
     * The type of the single column a sub-query answers with, or null when it cannot be read.
     *
     * <p>Typing the target needs the names its own FROM supplies, which is what a shape is for,
     * and the names the enclosing query supplies after them: a sub-query may read a column of a
     * relation further out — {@code (SELECT sum(a.v))} is written over the enclosing {@code a} —
     * and a target whose columns did not resolve was typed from the call's fallback instead of
     * from what it was called on. Deliberately quiet: a sub-query this cannot describe leaves the
     * type unknown rather than guessing, and unknown is what the callers already handled.
     */
    private DataType subqueryColumnType(Statement subquery, List<RowContext.TableBinding> outer) {
        // A set operation answers with the types its first arm writes, so that is where the one
        // column's type is read from. Giving up on it reported the column as text, and ARRAY() over
        // it as an array of text, for a query whose arms are all integers.
        if (subquery instanceof SetOpStmt) {
            return subqueryColumnType(((SetOpStmt) subquery).left(), outer);
        }
        if (!(subquery instanceof SelectStmt)) return null;
        SelectStmt sel = (SelectStmt) subquery;
        if (sel.targets() == null || sel.targets().size() != 1) return null;
        Expression target = sel.targets().get(0).expr();
        try {
            List<RowContext.TableBinding> inner = new ArrayList<>();
            if (sel.from() != null) {
                for (SelectStmt.FromItem item : sel.from()) {
                    inner.addAll(executor.fromResolver.resolveItemShape(item));
                }
            }
            // A star stands for the columns the FROM item exposes, and a sub-query used as a
            // value has exactly one, so its type is that column's own. Only its own: a star
            // reaches no further out than the FROM it is written against.
            if (target instanceof WildcardExpr) {
                Column only = null;
                for (RowContext.TableBinding b : inner) {
                    for (Column c : b.table().getColumns()) {
                        if (only != null) return null;
                        only = c;
                    }
                }
                return only == null ? null : only.getType();
            }
            // Its own names first, so one both levels supply is read as its own.
            if (outer != null) inner.addAll(outer);
            return inferTypeFromContext(target, inner);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * The label a scalar sub-query written {@code (SELECT * FROM one_column_thing)} carries.
     *
     * <p>A star stands for the columns the FROM item exposes, and a sub-query used as a value has
     * exactly one, so the label is that column's own name — {@code (SELECT id FROM c)} and
     * {@code (SELECT * FROM c)} answer under the same name in PostgreSQL. Deliberately narrow: one
     * FROM item exposing exactly one column, and any difficulty describing it leaves the label as
     * the unnamed one it was.
     *
     * <p>A name written in front of the star picks one relation out of the FROM clause, so the
     * label is that relation's own column however many relations the clause holds — which is what
     * names {@code (SELECT b.* FROM x a, y b)} after b's column rather than leaving it unnamed.
     * A name the clause does not answer to is left to the rule above it.
     */
    private String starSubqueryAlias(SelectStmt sel, String qualifier) {
        try {
            RowContext.TableBinding named = null;
            if (qualifier != null && sel.from() != null) {
                for (SelectStmt.FromItem item : sel.from()) {
                    for (RowContext.TableBinding b : executor.fromResolver.resolveItemShape(item)) {
                        String exposed = b.alias() != null ? b.alias() : b.table().getName();
                        if (exposed != null && exposed.equalsIgnoreCase(qualifier)) {
                            named = b;
                            break;
                        }
                    }
                    if (named != null) break;
                }
            }
            if (named != null && named.table() != null) {
                List<Column> own = named.table().getColumns();
                return own.size() == 1 ? own.get(0).getName() : "?column?";
            }
            if (sel.from() == null || sel.from().size() != 1) return "?column?";
            List<Column> columns = new ArrayList<>();
            for (RowContext.TableBinding b : executor.fromResolver.resolveItemShape(sel.from().get(0))) {
                columns.addAll(b.table().getColumns());
            }
            return columns.size() == 1 ? columns.get(0).getName() : "?column?";
        } catch (RuntimeException e) {
            return "?column?";
        }
    }

    /** Map a cast type name to the PG column name (e.g. "int" -> "int4", "boolean" -> "bool"). */
    private String castTypeToColumnName(String typeName) {
        // The column a cast produces is named after the type the cast resolved to, and for
        // float(p) the modifier is what decides that type: float(24) is a float4 column.
        if (typeName.indexOf('(') > 0 && !typeName.contains("[]")) {
            DataType withModifier = DataType.fromPgName(typeName.toLowerCase().trim());
            if (withModifier == DataType.REAL || withModifier == DataType.DOUBLE_PRECISION) {
                return withModifier.getPgName();
            }
        }
        String base = typeName.replaceAll("\\[\\]", "").replaceAll("\\(.*\\)", "").trim();
        if (base.endsWith("[]")) base = base.substring(0, base.length() - 2).trim();
        // A column is named after the type, not after where the type lives, so a cast
        // written with the schema still reports the plain type name.
        int schemaDot = base.lastIndexOf('.');
        if (schemaDot > 0) base = base.substring(schemaDot + 1);
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

    /** True for a bare string literal, which PostgreSQL types from whatever sits opposite it. */
    private static boolean isUnknownLiteral(Expression expr) {
        return expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.STRING;
    }

    /** True for the range and multirange types, whose operators answer in their own type. */
    private static boolean isRangeType(DataType t) {
        switch (t) {
            case INT4RANGE: case INT8RANGE: case NUMRANGE:
            case DATERANGE: case TSRANGE: case TSTZRANGE:
            case INT4MULTIRANGE: case INT8MULTIRANGE: case NUMMULTIRANGE:
            case DATEMULTIRANGE: case TSMULTIRANGE: case TSTZMULTIRANGE:
                return true;
            default:
                return false;
        }
    }

    /**
     * {@code 42804} when COLLATE is written over a value of a type that carries no collation.
     * Only the string types have one, but the rule here is stated the other way round on purpose:
     * it fires on the Java classes that name one non-collatable PG type and nothing else, so a
     * value whose type the engine is unsure of is left alone. Refusing a COLLATE PostgreSQL
     * performs would break far more than accepting one it refuses.
     */
    /** The string types, which are the ones a collation applies to. */
    private static boolean isCollatableTypeName(String typeName) {
        return typeName.equals("text") || typeName.equals("character varying")
                || typeName.equals("character") || typeName.equals("name")
                || typeName.equals("citext") || typeName.equals("unknown");
    }

    private static void rejectUncollatableValue(Object value) {
        String typeName = null;
        if (value instanceof Short) typeName = "smallint";
        else if (value instanceof Integer) typeName = "integer";
        else if (value instanceof Long) typeName = "bigint";
        else if (value instanceof java.math.BigDecimal
                || value instanceof java.math.BigInteger) typeName = "numeric";
        else if (value instanceof Float) typeName = "real";
        else if (value instanceof Double) typeName = "double precision";
        else if (value instanceof Boolean) typeName = "boolean";
        else if (value instanceof java.time.LocalDate) typeName = "date";
        else if (value instanceof java.time.LocalTime) typeName = "time without time zone";
        else if (value instanceof java.time.OffsetTime) typeName = "time with time zone";
        else if (value instanceof java.time.LocalDateTime) typeName = "timestamp without time zone";
        else if (value instanceof java.time.OffsetDateTime) {
            typeName = "timestamp with time zone";
        } else if (value instanceof PgInterval) typeName = "interval";
        else if (value instanceof byte[]) typeName = "bytea";
        else if (value instanceof java.util.UUID) typeName = "uuid";
        else if (value instanceof AstExecutor.PgRow) typeName = "record";
        else if (value instanceof InetValue) typeName = "inet";
        else if (value instanceof java.util.List<?>) {
            // An array is collatable only where its elements are, and PostgreSQL names the array
            // type rather than the element type when it says so.
            for (Object element : PgArray.flatten((java.util.List<?>) value)) {
                if (element == null) continue;
                typeName = AstExecutor.pgTypeNameOf(element);
                if (typeName != null && !isCollatableTypeName(typeName)) {
                    typeName = typeName + "[]";
                } else {
                    typeName = null;
                }
                break;
            }
        }
        if (typeName != null) {
            throw PgErrors.datatypeMismatch("collations are not supported by type " + typeName);
        }
    }

    /**
     * The type an ordered-set aggregate answers in. {@code mode} and {@code percentile_disc} give
     * back one of the values they sorted, so they take the ORDER BY expression's own type; the
     * continuous percentile interpolates, which it can only do in float8 or in interval; and the
     * hypothetical-set four have fixed types of their own. Described as text the value reached the
     * client as a string, even though pg_typeof() already answered correctly.
     */
    private DataType orderedSetAggResultType(OrderedSetAggExpr osa,
                                             List<RowContext.TableBinding> bindings) {
        String name = osa.funcName() == null ? ""
                : FunctionEvaluator.stripSchemaPrefix(osa.funcName().toLowerCase());
        if (name.equals("rank") || name.equals("dense_rank")) return DataType.BIGINT;
        if (name.equals("percent_rank") || name.equals("cume_dist")) {
            return DataType.DOUBLE_PRECISION;
        }
        DataType sorted = null;
        if (osa.withinGroupOrderBy() != null && !osa.withinGroupOrderBy().isEmpty()) {
            sorted = inferTypeFromContext(osa.withinGroupOrderBy().get(0).expr(), bindings);
        }
        boolean multi = !osa.args().isEmpty() && osa.args().get(0) instanceof ArrayExpr
                && !((ArrayExpr) osa.args().get(0)).isRow();
        if (name.equals("percentile_cont")) {
            // An interval is the one non-float8 type PG can interpolate in; everything else it
            // accepts at all resolves through double precision.
            DataType elem = sorted == DataType.INTERVAL ? DataType.INTERVAL
                    : DataType.DOUBLE_PRECISION;
            if (!multi) return elem;
            DataType arr = DataType.arrayOf(elem);
            return arr != null ? arr : DataType.TEXT;
        }
        if (name.equals("mode") || name.equals("percentile_disc")) {
            if (sorted == null) return DataType.TEXT;
            if (!multi || name.equals("mode")) return sorted;
            DataType arr = DataType.arrayOf(sorted);
            return arr != null ? arr : DataType.TEXT;
        }
        return DataType.TEXT;
    }

    /**
     * The range type a constructor of that name builds, or null when the name is not one. Only the
     * six built-in range constructors and their multirange counterparts are listed: a user-defined
     * range type's constructor is resolved from the catalogue further down.
     */
    private static DataType rangeConstructorResultType(String name) {
        DataType dt = DataType.fromPgName(name);
        return dt != null && isRangeType(dt) ? dt : null;
    }

    /**
     * The shape a geometric constructor builds, or null when the name is not one.
     *
     * <p>{@code point(1,2)} is a point and {@code circle(p, r)} a circle, whichever spelling was
     * used to write them. memgres holds every geometric value as the text PostgreSQL prints for
     * it, so without this the column was described as text and a client that asked for a point
     * got a string instead.
     */
    private static DataType geometricConstructorResultType(String name, FunctionCallExpr fn) {
        if (fn.args() == null || fn.args().isEmpty()) return null;
        switch (name) {
            case "point": return DataType.POINT;
            case "box": return DataType.BOX;
            case "circle": return DataType.CIRCLE;
            case "lseg": return DataType.LSEG;
            case "line": return DataType.LINE;
            case "path": return DataType.PATH;
            case "polygon": return DataType.POLYGON;
            case "bound_box": return DataType.BOX;
            case "center": case "closest_point": return DataType.POINT;
            case "pclose": case "popen": return DataType.PATH;
            default: return null;
        }
    }

    /** The type a range is built over — what {@code lower()} and {@code upper()} answer in. */
    private static DataType rangeSubtypeOf(DataType t) {
        if (t == null) return null;
        switch (t) {
            case INT4RANGE: case INT4MULTIRANGE: return DataType.INTEGER;
            case INT8RANGE: case INT8MULTIRANGE: return DataType.BIGINT;
            case NUMRANGE: case NUMMULTIRANGE: return DataType.NUMERIC;
            case DATERANGE: case DATEMULTIRANGE: return DataType.DATE;
            case TSRANGE: case TSMULTIRANGE: return DataType.TIMESTAMP;
            case TSTZRANGE: case TSTZMULTIRANGE: return DataType.TIMESTAMPTZ;
            default: return null;
        }
    }

    /**
     * The type a function of the JSON family answers in, or null when the name is not one of them.
     * The two families are spelled alike but for the prefix, so a name is looked up once in its
     * bare form and the {@code jsonb_} spelling answers in jsonb where the bare one answers in
     * json. Only the document-valued ones differ that way; a length is an integer and a test is a
     * boolean in both.
     *
     * <p>Without this a call whose value is NULL had nothing to be described by and was reported
     * as text, so a client reading json_array_length(NULL) back was told it had asked for a string.
     */
    /** Bare names whose value is a document, so json answers in json and jsonb in jsonb. */
    private static final Set<String> JSON_FUNCTIONS_RETURNING_A_DOCUMENT = Cols.setOf(
            "to_json", "row_to_json", "array_to_json", "json_object", "json_object_agg",
            "json_agg_strict", "json_object_agg_strict", "json_object_agg_unique",
            "json_object_agg_unique_strict",
            "json_build_object", "json_build_array", "json_agg", "json_strip_nulls", "json_scalar",
            "json_extract_path", "json_object_field", "json_array_element",
            "json_set", "json_insert", "json_set_lax", "json_concat", "json_delete",
            "json_delete_path", "json_path_query", "json_path_query_first",
            "json_path_query_array",
            // The SQL/JSON constructors and aggregates answer in json unless a RETURNING says
            // otherwise, and a RETURNING is parsed as a cast around the call rather than as part
            // of it, so the call itself is always json.
            "json_array_constructor", "json_object_constructor",
            "json_arrayagg", "json_objectagg");

    /** Bare names that answer yes or no, which both families do alike. */
    private static final Set<String> JSON_FUNCTIONS_RETURNING_A_BOOLEAN = Cols.setOf(
            "json_path_exists", "json_path_match", "json_contains", "json_contained",
            "json_exists", "json_exists_any", "json_exists_all");

    /** Bare names that answer text, which both families do alike. */
    private static final Set<String> JSON_FUNCTIONS_RETURNING_TEXT = Cols.setOf(
            "json_typeof", "json_pretty", "json_extract_path_text", "json_object_field_text",
            "json_array_element_text", "json_object_keys");

    private static DataType jsonFunctionResultType(String name) {
        // The hstore conversions are named the other way round -- the family they answer in is
        // the end of the name rather than the start -- so they are read here rather than below.
        if (name.startsWith("hstore_to_json")) {
            return name.startsWith("hstore_to_jsonb") ? DataType.JSONB : DataType.JSON;
        }
        boolean b = name.startsWith("jsonb_") || name.equals("to_jsonb");
        String bare = name.startsWith("jsonb_") ? "json_" + name.substring(6)
                : name.equals("to_jsonb") ? "to_json" : name;
        if (JSON_FUNCTIONS_RETURNING_A_DOCUMENT.contains(bare)) {
            return b ? DataType.JSONB : DataType.JSON;
        }
        if (bare.equals("json_array_length")) return DataType.INTEGER;
        if (JSON_FUNCTIONS_RETURNING_A_BOOLEAN.contains(bare)) return DataType.BOOLEAN;
        if (JSON_FUNCTIONS_RETURNING_TEXT.contains(bare)) return DataType.TEXT;
        return null;
    }

    /** True for the network address types, whose operators answer in inet rather than in text. */
    private static boolean isInet(DataType t) {
        return t == DataType.INET || t == DataType.CIDR;
    }

    /**
     * The four shapes PostgreSQL declares {@code <<} and {@code >>} over. lseg, line and path have
     * no such operator at all, so they are deliberately left out: reading them as shapes here
     * would answer a question PostgreSQL refuses to be asked.
     */
    private static boolean isShiftableShape(DataType t) {
        if (t == null) return false;
        switch (t) {
            case POINT: case BOX: case POLYGON: case CIRCLE:
                return true;
            default:
                return false;
        }
    }

    /** True for the geometric types: an operator over one of them answers in a shape. */
    private static boolean isGeometric(DataType t) {
        if (t == null) return false;
        switch (t) {
            case POINT: case LINE: case LSEG: case BOX: case PATH: case POLYGON: case CIRCLE:
                return true;
            default:
                return false;
        }
    }

    /** True for the whole-number types, the only ones a date may be shifted by. */
    private static boolean isWholeNumber(DataType t) {
        if (t == null) return false;
        switch (t) {
            case SMALLINT: case INTEGER: case BIGINT:
            case SMALLSERIAL: case SERIAL: case BIGSERIAL:
                return true;
            default:
                return false;
        }
    }

    /**
     * Whichever of json and jsonb the pair is written in; the operators keep their own flavour.
     *
     * <p>hstore spells its lookup with the same arrow and is not a document: it maps text to text,
     * so reaching into one answers text, and reaching in with a list of keys answers a list of
     * them. Calling that jsonb handed pgjdbc a value it then tried to read as a document.
     */
    private static DataType jsonFlavour(DataType lt, DataType rt) {
        if (lt == DataType.HSTORE) {
            return rt != null && DataType.isArrayType(rt) ? DataType.TEXT_ARRAY : DataType.TEXT;
        }
        if (lt == DataType.JSON || rt == DataType.JSON) return DataType.JSON;
        return DataType.JSONB;
    }

    /**
     * The type an operator answers in, read off its operands' declared types the way PostgreSQL
     * reads it off the catalogue entry it resolved -- never off the value that comes back. Getting
     * this wrong is not cosmetic: pgjdbc decodes a column by the type the server declared, so a
     * {@code date} arriving under an {@code int4} descriptor makes getObject throw rather than
     * return the date the engine computed.
     *
     * <p>A null operand type is one the query never wrote down -- an untyped literal or a NULL --
     * which PostgreSQL resolves against the other side, so it does not drag the pair to text.
     * Returns null when this table has no answer, leaving the caller's text default in place.
     */
    private static DataType binaryResultType(BinaryExpr.BinOp op, DataType lt, DataType rt) {
        switch (op) {
            // Every one of these asks a yes/no question, whatever its operands are made of
            case EQUAL: case NOT_EQUAL: case LESS_THAN: case GREATER_THAN:
            case LESS_EQUAL: case GREATER_EQUAL: case AND: case OR:
            case IS_DISTINCT_FROM: case IS_NOT_DISTINCT_FROM:
            case LIKE: case ILIKE: case SIMILAR_TO:
            case REGEX_MATCH: case REGEX_IMATCH:
            case NOT_REGEX_MATCH: case NOT_REGEX_IMATCH:
            case CONTAINS: case CONTAINED_BY: case OVERLAP:
            case TS_MATCH: case RANGE_ADJACENT:
            case JSONB_EXISTS: case JSONB_EXISTS_ANY: case JSONB_EXISTS_ALL:
            case JSONB_PATH_EXISTS_OP:
            case INET_CONTAINS_EQUALS: case INET_CONTAINED_BY_EQUALS:
            case APPROX_EQUAL: case GEO_BELOW: case GEO_ABOVE:
            case GEO_NOT_EXTEND_RIGHT: case GEO_NOT_EXTEND_LEFT:
            case GEO_NOT_EXTEND_ABOVE: case GEO_NOT_EXTEND_BELOW:
            case GEO_INTERSECTS: case GEO_PARALLEL: case GEO_PERPENDICULAR:
            case GEO_HORIZONTAL: case GEO_VERTICAL:
                return DataType.BOOLEAN;
            case DISTANCE:
                return DataType.DOUBLE_PRECISION;
            case GEO_CLOSEST_POINT:
                return DataType.POINT;
            case JSON_ARROW: case JSON_HASH_ARROW: case JSON_SUBSCRIPT: case JSON_DELETE_PATH:
                return jsonFlavour(lt, rt);
            case JSON_ARROW_TEXT: case JSON_HASH_ARROW_TEXT:
                return DataType.TEXT;
            case SHIFT_LEFT: case SHIFT_RIGHT:
                // inet << inet asks whether one network sits inside the other; a range or a shape
                // is asked whether it lies wholly to one side of the other. Only on integers and
                // bit strings does the same spelling shift bits and keep the left operand's type.
                if (isInet(lt) || isInet(rt)) return DataType.BOOLEAN;
                if ((lt != null && isRangeType(lt)) || (rt != null && isRangeType(rt))) {
                    return DataType.BOOLEAN;
                }
                if (isShiftableShape(lt) || isShiftableShape(rt)) return DataType.BOOLEAN;
                return lt != null ? lt : rt;
            case BIT_AND: case BIT_OR: case BIT_XOR:
                if (isInet(lt) || isInet(rt)) return DataType.INET;
                if (lt == DataType.BIT || lt == DataType.VARBIT) return lt;
                if (rt == DataType.BIT || rt == DataType.VARBIT) return rt;
                return lt != null ? lt : rt;
            case POWER:
                // PostgreSQL has no integer ^, so a pair of integers resolves through float8
                if (lt == DataType.NUMERIC || rt == DataType.NUMERIC) return DataType.NUMERIC;
                return DataType.DOUBLE_PRECISION;
            case CONCAT:
                return concatResultType(lt, rt);
            case ADD: case SUBTRACT: case MULTIPLY: case DIVIDE: case MODULO:
                return arithmeticResultType(op, lt, rt);
            default:
                return null;
        }
    }

    /** The type {@code ||} answers in: an array, a document or a bit string keeps its own. */
    private static DataType concatResultType(DataType lt, DataType rt) {
        if (lt == DataType.BYTEA || rt == DataType.BYTEA) return DataType.BYTEA;
        if (DataType.isArrayType(lt)) return lt;
        if (DataType.isArrayType(rt)) return rt;
        if (lt == DataType.JSONB || rt == DataType.JSONB) return DataType.JSONB;
        // json has no || of its own. One beside anything else resolves through anynonarray||text
        // and answers with the two spellings run together; one beside another json resolves to
        // nothing at all, and is refused before the answer's type is asked for.
        if (lt == DataType.TSVECTOR || rt == DataType.TSVECTOR) return DataType.TSVECTOR;
        if (lt == DataType.TSQUERY || rt == DataType.TSQUERY) return DataType.TSQUERY;
        if (lt == DataType.HSTORE || rt == DataType.HSTORE) return DataType.HSTORE;
        if (lt == DataType.BIT || lt == DataType.VARBIT
                || rt == DataType.BIT || rt == DataType.VARBIT) return DataType.VARBIT;
        // A range concatenated with anything resolves through anynonarray||text, so it is text
        return DataType.TEXT;
    }

    /** The type {@code + - * / %} answer in, once the non-numeric families have had their say. */
    private static DataType arithmeticResultType(BinaryExpr.BinOp op, DataType lt, DataType rt) {
        // A range meets, joins or is cut by another range and stays that same range type.
        // Describing the column as an integer instead handed the driver a value it could not
        // read: "[5,10)" is no integer, so the row never arrived at all.
        if (lt != null && isRangeType(lt) && (rt == null || lt == rt)) return lt;
        if (rt != null && isRangeType(rt) && lt == null) return rt;

        boolean add = op == BinaryExpr.BinOp.ADD;
        boolean sub = op == BinaryExpr.BinOp.SUBTRACT;

        // jsonb minus a key or a path is the document with that part taken out
        if (sub && (lt == DataType.JSONB || lt == DataType.JSON)) return lt;

        // an inet counts the addresses between two of them, and moves by a whole number otherwise
        if (sub && isInet(lt) && isInet(rt)) return DataType.BIGINT;
        if ((add || sub) && isInet(lt) && !isInet(rt)) return DataType.INET;
        if (add && isInet(rt) && !isInet(lt)) return DataType.INET;

        // a shape translated or scaled by a point is that same shape
        if (isGeometric(lt)) return lt;
        if (isGeometric(rt)) return rt;

        if (lt == DataType.MONEY || rt == DataType.MONEY) {
            if (lt == DataType.MONEY && rt == DataType.MONEY && op == BinaryExpr.BinOp.DIVIDE) {
                return DataType.DOUBLE_PRECISION;
            }
            return DataType.MONEY;
        }

        DataType dt = dateTimeResultType(op, lt, rt);
        if (dt != null) return dt;

        // A range meets, joins or is cut by another range and stays that same range type.
        // Describing it as an integer handed the driver a value it could not read -- "[5,10)" is
        // no integer -- so the row never arrived at all.
        if (lt != null && isRangeType(lt) && (rt == null || rt == lt)) return lt;
        if (rt != null && isRangeType(rt) && lt == null) return rt;

        if (lt == DataType.DOUBLE_PRECISION || rt == DataType.DOUBLE_PRECISION)
            return DataType.DOUBLE_PRECISION;
        if (lt == DataType.NUMERIC || rt == DataType.NUMERIC)
            return DataType.NUMERIC;
        // real has no arithmetic of its own opposite another type; PostgreSQL widens to float8
        if (lt == DataType.REAL || rt == DataType.REAL) {
            return lt == rt ? DataType.REAL : DataType.DOUBLE_PRECISION;
        }
        if (lt == DataType.BIGINT || rt == DataType.BIGINT)
            return DataType.BIGINT;
        return DataType.INTEGER;
    }

    /**
     * Date and time arithmetic, whose result type is decided by the pair and never by the value:
     * a date shifted by days is a date, by an interval a timestamp, and the gap between two
     * timestamps is an interval. An operand type of null is an untyped literal, which PostgreSQL
     * resolves to the interval or the date the other side's operator asks for.
     */
    private static DataType dateTimeResultType(BinaryExpr.BinOp op, DataType lt, DataType rt) {
        boolean add = op == BinaryExpr.BinOp.ADD;
        boolean sub = op == BinaryExpr.BinOp.SUBTRACT;
        if (op == BinaryExpr.BinOp.MULTIPLY || op == BinaryExpr.BinOp.DIVIDE) {
            // an interval scaled by a number is still an interval
            return (lt == DataType.INTERVAL || rt == DataType.INTERVAL) ? DataType.INTERVAL : null;
        }
        if (!add && !sub) return null;
        if (lt == DataType.INTERVAL && (rt == DataType.INTERVAL || rt == null)) return DataType.INTERVAL;
        if (rt == DataType.INTERVAL && lt == null && add) return DataType.INTERVAL;
        if (sub) {
            if (lt == DataType.DATE && (rt == DataType.DATE || rt == null)) return DataType.INTEGER;
            if (lt == DataType.TIMESTAMP && (rt == DataType.TIMESTAMP || rt == DataType.TIMESTAMPTZ))
                return DataType.INTERVAL;
            if (lt == DataType.TIMESTAMPTZ && (rt == DataType.TIMESTAMP || rt == DataType.TIMESTAMPTZ))
                return DataType.INTERVAL;
            if (lt == DataType.TIME && rt == DataType.TIME) return DataType.INTERVAL;
        }
        if (lt == DataType.DATE) {
            if (rt == DataType.INTERVAL || rt == DataType.TIME) return DataType.TIMESTAMP;
            if (rt == DataType.TIMETZ) return DataType.TIMESTAMPTZ;
            if (isWholeNumber(rt)) return DataType.DATE;
            return null;
        }
        if (rt == DataType.DATE && add) {
            if (lt == DataType.INTERVAL || lt == DataType.TIME) return DataType.TIMESTAMP;
            if (lt == DataType.TIMETZ) return DataType.TIMESTAMPTZ;
            if (isWholeNumber(lt)) return DataType.DATE;
            return null;
        }
        // a moment shifted by an interval -- named or not yet named -- is a moment of the same kind
        if (isMoment(lt) && (rt == DataType.INTERVAL || rt == null)) return lt;
        if (isMoment(rt) && lt == null && add) return rt;
        if (isMoment(rt) && lt == DataType.INTERVAL && add) return rt;
        return null;
    }

    /**
     * The array type the array-building functions answer in. Each is declared over
     * {@code anyarray}/{@code anyelement}, so the answer is the array type of whichever argument
     * states one -- and an element argument states it just as well as the array does, which is how
     * {@code array_append('{1,2}', 3)} comes out as an integer array rather than as text.
     */
    private DataType arrayFunctionResultType(String name, FunctionCallExpr fn,
                                             List<RowContext.TableBinding> bindings) {
        List<Expression> args = fn.args();
        if (args == null || args.isEmpty()) return null;
        if (name.equals("string_to_array") || name.equals("regexp_split_to_array")
                || name.equals("tsvector_to_array") || name.equals("akeys") || name.equals("avals")) {
            return DataType.TEXT_ARRAY;
        }
        if (name.equals("unnest")) {
            DataType element = DataType.elementOf(inferTypeFromContext(args.get(0), bindings));
            return element;
        }
        if (name.equals("array_fill")) {
            return DataType.arrayOf(inferTypeFromContext(args.get(0), bindings));
        }
        int arrayArg;
        int elementArg;
        if (name.equals("array_append") || name.equals("array_remove")) {
            arrayArg = 0; elementArg = 1;
        } else if (name.equals("array_prepend")) {
            arrayArg = 1; elementArg = 0;
        } else if (name.equals("array_cat")) {
            arrayArg = 0; elementArg = -1;
        } else if (name.equals("array_replace")) {
            arrayArg = 0; elementArg = 1;
        } else if (name.equals("array_shuffle") || name.equals("array_sample")
                || name.equals("array_reverse") || name.equals("trim_array")) {
            arrayArg = 0; elementArg = -1;
        } else {
            return null;
        }
        if (arrayArg < args.size()) {
            DataType declared = inferTypeFromContext(args.get(arrayArg), bindings);
            if (DataType.isArrayType(declared)) return declared;
        }
        if (name.equals("array_cat") && args.size() > 1) {
            DataType other = inferTypeFromContext(args.get(1), bindings);
            if (DataType.isArrayType(other)) return other;
        }
        if (elementArg >= 0 && elementArg < args.size()) {
            DataType element = inferTypeFromContext(args.get(elementArg), bindings);
            DataType array = DataType.arrayOf(element);
            if (array != null) return array;
        }
        return DataType.TEXT_ARRAY;
    }

    /** The types that name a moment on the clock or the calendar, other than a bare date. */
    private static boolean isMoment(DataType t) {
        return t == DataType.TIMESTAMP || t == DataType.TIMESTAMPTZ
                || t == DataType.TIME || t == DataType.TIMETZ;
    }

    /**
     * The type an enclosing query level gives a column reference, walked in the same order the
     * runtime lookup walks. Null when no enclosing level holds the name, which leaves the caller's
     * own fallback in place.
     */
    Column columnFromOuterContexts(ColumnRef ref) {
        if (executor == null || ref.column() == null) return null;
        for (Iterator<RowContext> it = executor.outerContextStack.descendingIterator(); it.hasNext(); ) {
            for (RowContext.TableBinding b : it.next().getBindings()) {
                if (b.table() == null) continue;
                if (ref.table() != null) {
                    if (!ref.table().equalsIgnoreCase(b.alias())
                            && !ref.table().equalsIgnoreCase(b.table().getName())) continue;
                }
                int idx = b.table().getColumnIndex(ref.column());
                if (idx >= 0) return b.table().getColumns().get(idx);
            }
        }
        return null;
    }

    /** True when this level's own relations hold the name, so no enclosing one need be asked. */
    private static boolean resolvesHere(ColumnRef ref, List<RowContext.TableBinding> bindings) {
        for (RowContext.TableBinding b : bindings) {
            if (b.table() == null) continue;
            if (ref.table() != null) {
                if (!ref.table().equalsIgnoreCase(b.alias())
                        && !ref.table().equalsIgnoreCase(b.table().getName())) continue;
            }
            if (b.table().getColumnIndex(ref.column()) >= 0) return true;
        }
        return false;
    }

    /**
     * The type a SQL/JSON expression answers in: the one its RETURNING clause names, or the
     * reading's own default where the clause was not written. The modifier is stripped only after
     * the whole spelling has been offered, because for some types it names the type rather than a
     * width.
     */
    private static DataType returningType(String typeSpec, DataType unwritten) {
        if (typeSpec == null) return unwritten;
        DataType declared = DataType.fromPgName(typeSpec.trim());
        if (declared != null) return declared;
        DataType stripped = DataType.fromPgName(stripTypeModifier(typeSpec).trim());
        return stripped != null ? stripped : DataType.TEXT;
    }

    DataType inferTypeFromContext(Expression expr, List<RowContext.TableBinding> bindings) {
        // A subscript of an array is one element of it, and a range of one is another array of
        // the same type; a subscript of a json container is json. Answering jsonb for all of them
        // told a client the wrong type for every array column it read an element of.
        if (expr instanceof SubscriptExpr) {
            SubscriptExpr sub = (SubscriptExpr) expr;
            DataType base = inferTypeFromContext(sub.base(), bindings);
            if (base == null) return null;
            if (DataType.isArrayType(base)) {
                return sub.isSlice() ? base : DataType.elementOf(base);
            }
            if (base == DataType.INT2VECTOR) return DataType.SMALLINT;
            if (base == DataType.OIDVECTOR) return DataType.OID;
            // A point is subscripted into its coordinates; everything built out of points is
            // subscripted into those points.
            if (base == DataType.POINT) return DataType.DOUBLE_PRECISION;
            if (base == DataType.BOX || base == DataType.LSEG || base == DataType.PATH
                    || base == DataType.POLYGON) {
                return DataType.POINT;
            }
            if (base == DataType.JSON || base == DataType.JSONB || base == DataType.HSTORE) {
                return base == DataType.HSTORE ? DataType.TEXT : base;
            }
            return base == DataType.NAME ? DataType.TEXT : base;
        }
        if (expr instanceof PrecomputedValueExpr) {
            PrecomputedValueExpr pre = (PrecomputedValueExpr) expr;
            if (pre.declaredType() != null) return pre.declaredType();
            return pre.value() == null ? null : TypeCoercion.inferType(pre.value());
        }
        if (expr instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) expr;
            // A system column is none of the ones the relation declares, so the loop below never
            // found it and it fell through to text: a client asked for ctid was told it had read
            // a string, and every rule downstream that decides on the described type followed.
            DataType system = SystemColumns.resolve(ref, bindings);
            if (system != null) return system;
            for (RowContext.TableBinding b : bindings) {
                if (ref.table() != null) {
                    if (!ref.table().equalsIgnoreCase(b.alias()) &&
                            !ref.table().equalsIgnoreCase(b.table().getName())) continue;
                }
                int idx = b.table().getColumnIndex(ref.column());
                if (idx >= 0) return b.table().getColumns().get(idx).getType();
                // Column-wins semantics: a real column always takes precedence. Only once no
                // column named ref.column() exists on this qualified binding do we mirror
                // tryAttributeNotationFallback's runtime resolution (alias.name -> name(alias))
                // for type-inference purposes, so the projected Column's DataType matches what
                // will actually be produced at evaluation time (e.g. gs.date -> date(gs) -> DATE)
                // instead of silently defaulting to TEXT below.
                if (ref.table() != null) {
                    DataType fallbackType = inferAttributeNotationFallbackType(b, ref.column());
                    if (fallbackType != null) return fallbackType;
                }
            }
            // A name this level does not supply may come from an enclosing one: a LATERAL item
            // reads the relations to its left and a correlated sub-select reads the query around
            // it, and evaluation resolves both through outerContextStack. Reading the type from
            // the same place is what keeps a column a LATERAL exposes the type it actually has --
            // typed as text, abs() over one answered "function abs(text) does not exist".
            Column outer = columnFromOuterContexts(ref);
            if (outer != null) return outer.getType();
            return DataType.TEXT;
        }
        // The SQL/JSON expressions answer in the type their RETURNING clause names, and each has
        // its own default where none was written: JSON_EXISTS asks a question and so is a boolean,
        // JSON_QUERY hands back a document and JSON_VALUE the value inside one.
        if (expr instanceof JsonExistsExpr) return DataType.BOOLEAN;
        if (expr instanceof JsonValueExpr) {
            return returningType(((JsonValueExpr) expr).returningType, DataType.TEXT);
        }
        if (expr instanceof JsonQueryExpr) {
            return returningType(((JsonQueryExpr) expr).returningType, DataType.JSONB);
        }
        if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            // The modifier is part of what float(p) names — 24 is a real and 25 a double
            // precision — so it is offered whole first, and stripped only for the types whose
            // modifier says how wide a value may be rather than which type it is.
            DataType declared = DataType.fromPgName(cast.typeName().trim());
            if (declared != null) return declared;
            String typeName = cast.typeName().replaceAll("\\(.*\\)", "").trim();
            DataType dt = DataType.fromPgName(typeName);
            if (dt != null) return dt;
            // fromPgName only recognizes built-in PG type names; a cast to a registered custom
            // enum type (e.g. 'done'::my_status) infers ENUM, not TEXT, so a CASE/COALESCE built
            // from such casts advertises the real enum type via resolveEnumTypeName below instead
            // of the generic (OID-0) placeholder.
            if (executor != null && executor.database != null
                    && executor.database.getCustomEnum(typeName.toLowerCase()) != null) {
                return DataType.ENUM;
            }
            return DataType.TEXT;
        }
        if (expr instanceof Literal) {
            Literal lit = (Literal) expr;
            switch (lit.literalType()) {
                case INTEGER:
                    return DataType.INTEGER;
                case FLOAT:
                    // A written constant is numeric whether or not it carries an exponent —
                    // 1.0e0 is the same numeric one as 1.0, and only a cast makes it float8.
                    return DataType.NUMERIC;
                case STRING:
                    return DataType.TEXT;
                case BOOLEAN:
                    return DataType.BOOLEAN;
                case BIT_STRING:
                    return DataType.BIT;
                case NULL:
                    return null;
                case DEFAULT:
                    return DataType.TEXT;
            }
        }
        if (expr instanceof UnaryExpr) {
            UnaryExpr un = (UnaryExpr) expr;
            switch (un.op()) {
                case NEGATE:
                case POSITIVE:
                case ABS: {
                    // @ and the signs answer in the operand's own type: @ -10 is an integer and
                    // - interval '1 day' is an interval, not the text they were described as.
                    // An operand with no type of its own is an unknown literal, which PostgreSQL
                    // resolves through float8.
                    DataType inner = inferTypeFromContext(un.operand(), bindings);
                    return inner == null || inner == DataType.TEXT ? DataType.DOUBLE_PRECISION : inner;
                }
                case BIT_NOT: {
                    // ~ inet is an inet and ~ B'101' a bit string; only integers give an integer.
                    DataType inner = inferTypeFromContext(un.operand(), bindings);
                    if (inner == DataType.CIDR) return DataType.INET;
                    return inner;
                }
                case NOT:
                case GEO_IS_HORIZONTAL:
                case GEO_IS_VERTICAL:
                    return DataType.BOOLEAN;
                case SQRT:
                case CBRT:
                case GEO_LENGTH:
                    return DataType.DOUBLE_PRECISION;
                case GEO_NPOINTS:
                    return DataType.INTEGER;
                case GEO_CENTER:
                    return DataType.POINT;
                case HSTORE_TO_ARRAY:
                case HSTORE_TO_MATRIX:
                    return DataType.TEXT_ARRAY;
                default:
                    return DataType.TEXT;
            }
        }
        if (expr instanceof CustomOperatorExpr) {
            // Custom operators - can't infer return type without looking up the operator definition
            return DataType.TEXT;
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            DataType lt = inferTypeFromContext(bin.left(), bindings);
            DataType rt = inferTypeFromContext(bin.right(), bindings);
            // An untyped literal is PostgreSQL's unknown and takes the other operand's type,
            // so it must not drag the pair back to text.
            if (isUnknownLiteral(bin.left())) lt = null;
            if (isUnknownLiteral(bin.right())) rt = null;
            DataType resolved = binaryResultType(bin.op(), lt, rt);
            return resolved != null ? resolved : DataType.TEXT;
        }
        if (expr instanceof WindowFuncExpr) {
            // The type a window call answers in. The ranking functions have a type of their own;
            // the value-shifting ones answer in the type of the value they shift; and every other
            // window call is an aggregate over the frame, so it resolves exactly as the same
            // aggregate would. Without this a window column had no declared type and came out
            // as text.
            WindowFuncExpr wf = (WindowFuncExpr) expr;
            String wfName = wf.name() == null ? ""
                    : FunctionEvaluator.stripSchemaPrefix(wf.name().toLowerCase());
            if (wfName.equals("row_number") || wfName.equals("rank")
                    || wfName.equals("dense_rank") || wfName.equals("count")) {
                return DataType.BIGINT;
            }
            if (wfName.equals("ntile")) return DataType.INTEGER;
            if (wfName.equals("percent_rank") || wfName.equals("cume_dist")) {
                return DataType.DOUBLE_PRECISION;
            }
            if (wfName.equals("lag") || wfName.equals("lead") || wfName.equals("first_value")
                    || wfName.equals("last_value") || wfName.equals("nth_value")) {
                return wf.args().isEmpty() ? null : inferTypeFromContext(wf.args().get(0), bindings);
            }
            return inferTypeFromContext(
                    new FunctionCallExpr(wf.name(), wf.args(), wf.distinct(), wf.star()), bindings);
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            String name = FunctionEvaluator.stripSchemaPrefix(fn.name().toLowerCase());
            // count answers in bigint, which is what sum(count(*)) resolves against.
            if (name.equals("count")) return DataType.BIGINT;
            // A sequence counts in bigint, whichever of its functions is asked.
            if (name.equals("nextval") || name.equals("currval") || name.equals("lastval")
                    || name.equals("setval")) {
                return DataType.BIGINT;
            }
            if (name.equals("length") || name.equals("char_length")
                    || name.equals("octet_length") || name.equals("bit_length")
                    || name.equals("position") || name.equals("strpos")
                    || name.equals("array_length") || name.equals("cardinality")
                    || name.equals("array_ndims") || name.equals("array_upper")
                    || name.equals("array_lower")
                    || name.equals("num_nonnulls") || name.equals("num_nulls")
                    || name.equals("grouping")
                    || name.equals("get_byte") || name.equals("get_bit")
                    || name.equals("regexp_count") || name.equals("regexp_instr")
                    || name.equals("ascii") || name.equals("npoints")
                    || name.equals("masklen") || name.equals("family")
                    || name.equals("array_position")) return DataType.INTEGER;
            if (name.equals("array_positions")) return DataType.INT4_ARRAY;
            // The function spellings of @>, <@ and && answer the yes/no question the operators do.
            if (name.equals("arraycontains") || name.equals("arraycontained")
                    || name.equals("arrayoverlap")) return DataType.BOOLEAN;
            // The set-returning calls, whose row type is the type of the column the query gets.
            // Described as text they decoded as strings, though pg_typeof already answered right.
            if (name.equals("regexp_matches")) return DataType.TEXT_ARRAY;
            if (name.equals("json_array_elements")) return DataType.JSON;
            if (name.equals("jsonb_array_elements")) return DataType.JSONB;
            if (name.equals("json_each") || name.equals("jsonb_each")
                    || name.equals("json_each_text") || name.equals("jsonb_each_text")) {
                return DataType.RECORD;
            }
            if (name.equals("sum") || name.equals("avg")) {
                DataType dt = fn.args().isEmpty() ? null : inferTypeFromContext(fn.args().get(0), bindings);
                if (dt == DataType.DOUBLE_PRECISION) return DataType.DOUBLE_PRECISION;
                if (dt == DataType.REAL) {
                    // sum(real) stays real; avg(real) widens, which is where PG puts the line
                    return name.equals("sum") ? DataType.REAL : DataType.DOUBLE_PRECISION;
                }
                if (dt == DataType.INTERVAL) return DataType.INTERVAL;
                if (dt == DataType.MONEY) return DataType.MONEY;
                if (name.equals("sum") && (dt == DataType.SMALLINT || dt == DataType.INTEGER
                        || dt == DataType.SERIAL || dt == DataType.SMALLSERIAL)) return DataType.BIGINT;
                return DataType.NUMERIC;
            }
            if (name.equals("var_pop") || name.equals("var_samp") || name.equals("variance")
                    || name.equals("stddev") || name.equals("stddev_pop") || name.equals("stddev_samp")) {
                DataType dt = fn.args().isEmpty() ? null : inferTypeFromContext(fn.args().get(0), bindings);
                return dt == DataType.DOUBLE_PRECISION || dt == DataType.REAL
                        ? DataType.DOUBLE_PRECISION : DataType.NUMERIC;
            }
            // The bitwise aggregates answer in the type they were given: they are declared over
            // the three integer widths and over bit, and each form gives back its own.
            if (name.equals("bit_and") || name.equals("bit_or") || name.equals("bit_xor")) {
                DataType dt = fn.args().isEmpty() ? null
                        : inferTypeFromContext(fn.args().get(0), bindings);
                return dt == null ? DataType.INTEGER : dt;
            }
            // The two-argument statistical aggregates are declared over float8 alone. All but one
            // answer in it too: regr_count counts the pairs, and a count is a bigint.
            if (name.equals("regr_count")) return DataType.BIGINT;
            if (name.equals("corr") || name.equals("covar_pop") || name.equals("covar_samp")
                    || name.startsWith("regr_")) {
                return DataType.DOUBLE_PRECISION;
            }
            if (name.equals("max") || name.equals("min")) {
                if (!fn.args().isEmpty()) return inferTypeFromContext(fn.args().get(0), bindings);
                return DataType.TEXT;
            }
            // lower/upper of a range answer in the type the range is built over; over anything
            // else they are the string functions in the list just below.
            if ((name.equals("lower") || name.equals("upper")) && fn.args().size() == 1) {
                DataType sub = rangeSubtypeOf(inferTypeFromContext(fn.args().get(0), bindings));
                if (sub != null) return sub;
            }
            if (name.equals("lower") || name.equals("upper") || name.equals("trim")
                    || name.equals("ltrim") || name.equals("rtrim") || name.equals("replace")
                    || name.equals("substring") || name.equals("concat")
                    || name.equals("concat_ws") || name.equals("left") || name.equals("right")
                    || name.equals("repeat") || name.equals("reverse")
                    || name.equals("md5") || name.equals("to_char") || name.equals("initcap")
                    || name.equals("translate") || name.equals("chr") || name.equals("format")
                    || name.equals("lpad") || name.equals("rpad") || name.equals("overlay")
                    || name.equals("string_agg") || name.equals("regexp_replace")) return DataType.TEXT;
            // pg_typeof answers with a type, and PostgreSQL declares it as one
            if (name.equals("pg_typeof")) return DataType.REGTYPE;
            if (name.equals("now") || name.equals("current_timestamp")
                    || name.equals("statement_timestamp") || name.equals("clock_timestamp")
                    || name.equals("transaction_timestamp")) return DataType.TIMESTAMPTZ;
            if (name.equals("current_date")
                    || name.equals("to_date") || name.equals("make_date")) return DataType.DATE;
            if (name.equals("date_trunc")) {
                // date_trunc(text, timestamp[tz]|interval[, text timezone]) returns the same
                // type as its 2nd argument, never DATE. The previous blanket DataType.DATE was
                // itself wrong (surfaced while fixing mtask-8 Group 2: date_trunc(...) is the
                // generate_series start argument in ResultsMonthlyDao's months CTE, and a
                // timestamptz value advertised/decoded as DATE corrupts pgjdbc's binary decode).
                if (fn.args().size() >= 2) {
                    DataType dt = inferTypeFromContext(fn.args().get(1), bindings);
                    // PG has no date_trunc over date or time; a date resolves through the
                    // timestamptz form and a time through the interval one
                    if (dt == DataType.DATE) return DataType.TIMESTAMPTZ;
                    if (dt == DataType.TIME) return DataType.INTERVAL;
                    if (dt != null) return dt;
                }
                return DataType.TIMESTAMP;
            }
            if (name.equals("coalesce") || name.equals("nullif") || name.equals("greatest") || name.equals("least")) {
                // An untyped literal is read as whatever the other arguments settle on, so
                // GREATEST('10', 9) is an integer -- taking the first argument's type made it text
                for (Expression arg : fn.args()) {
                    if (isUnknownLiteral(arg)) continue;
                    DataType dt = inferTypeFromContext(arg, bindings);
                    if (dt != null) return dt;
                }
                for (Expression arg : fn.args()) {
                    DataType dt = inferTypeFromContext(arg, bindings);
                    if (dt != null) return dt;
                }
                return DataType.TEXT;
            }
            DataType arrayResult = arrayFunctionResultType(name, fn, bindings);
            if (arrayResult != null) return arrayResult;
            if (name.equals("bool_and") || name.equals("bool_or") || name.equals("every")
                    || name.equals("has_database_privilege") || name.equals("has_schema_privilege")
                    || name.equals("has_table_privilege") || name.equals("has_column_privilege")
                    || name.equals("has_function_privilege") || name.equals("has_type_privilege")
                    || name.equals("has_sequence_privilege") || name.equals("has_any_column_privilege")
                    || name.equals("has_foreign_data_wrapper_privilege") || name.equals("has_server_privilege")
                    || name.equals("has_tablespace_privilege") || name.equals("has_parameter_privilege")
                    || name.equals("has_language_privilege") || name.equals("has_largeobject_privilege")
                    || name.equals("pg_is_in_recovery") || name.equals("pg_is_wal_replay_paused")
                    || name.equals("pg_has_role")
                    || name.startsWith("pg_try_advisory_")
                    || name.equals("pg_advisory_unlock") || name.equals("pg_advisory_unlock_shared")
                    || name.equals("overlaps")) return DataType.BOOLEAN;
            // A function declared to return void answers with an empty value of type void, not a
            // NULL, and the difference is visible to anything that checks. Which functions those
            // are is read off the signature table rather than listed again here.
            if (BuiltinFunctionSignatures.returnsVoid(name)) {
                return DataType.VOID;
            }
            // abs and unary negation answer in the argument's own type; an untyped argument
            // resolves to float8, the preferred type of the numeric category.
            if (name.equals("abs")) {
                DataType dt = fn.args().isEmpty() ? null : inferTypeFromContext(fn.args().get(0), bindings);
                return dt == null || dt == DataType.TEXT ? DataType.DOUBLE_PRECISION : dt;
            }
            // These have a numeric form and a float8 form. A numeric argument keeps the answer
            // numeric; an integer, a real or an untyped literal all resolve to float8, which
            // is why round('2.5') rounds half to even and answers 2 rather than 3.
            if (name.equals("ceil") || name.equals("ceiling") || name.equals("floor")
                    || name.equals("round") || name.equals("trunc") || name.equals("sign")
                    || name.equals("power") || name.equals("pow") || name.equals("sqrt")
                    || name.equals("exp") || name.equals("ln")
                    || name.equals("log") || name.equals("log10")) {
                DataType dt = fn.args().isEmpty() ? null : inferTypeFromContext(fn.args().get(0), bindings);
                if (name.equals("trunc") && (dt == DataType.MACADDR || dt == DataType.MACADDR8)) return dt;
                // round(numeric, int), trunc(numeric, int) and log(numeric, numeric) are the
                // only two-argument forms, and all three are numeric only
                if (fn.args().size() > 1 && (name.equals("round") || name.equals("trunc")
                        || name.equals("log"))) {
                    return DataType.NUMERIC;
                }
                return dt == DataType.NUMERIC ? DataType.NUMERIC : DataType.DOUBLE_PRECISION;
            }
            if (name.equals("div")) return DataType.NUMERIC;
            if (name.equals("mod")) {
                DataType a = fn.args().isEmpty() ? null : inferTypeFromContext(fn.args().get(0), bindings);
                DataType b = fn.args().size() < 2 ? null : inferTypeFromContext(fn.args().get(1), bindings);
                if (a == null || a == DataType.TEXT) a = b;
                if (b == null || b == DataType.TEXT) b = a;
                if (a == null || b == null) return DataType.NUMERIC;
                return TypeCoercion.promoteNumeric(a, b);
            }
            if (name.equals("gcd") || name.equals("lcm")) {
                DataType a = fn.args().isEmpty() ? null : inferTypeFromContext(fn.args().get(0), bindings);
                DataType b = fn.args().size() < 2 ? null : inferTypeFromContext(fn.args().get(1), bindings);
                if (a == DataType.NUMERIC || b == DataType.NUMERIC) return DataType.NUMERIC;
                if (a == DataType.BIGINT || b == DataType.BIGINT) return DataType.BIGINT;
                if (a == DataType.SMALLINT && b == DataType.SMALLINT) return DataType.SMALLINT;
                return DataType.INTEGER;
            }
            if (name.equals("factorial") || name.equals("trim_scale")) return DataType.NUMERIC;
            if (name.equals("scale") || name.equals("min_scale")
                    || name.equals("width_bucket")) return DataType.INTEGER;
            if (name.equals("random") || name.equals("pi") || name.equals("degrees")
                    || name.equals("radians") || name.equals("cbrt")
                    || name.equals("sin") || name.equals("cos")
                    || name.equals("tan") || name.equals("cot") || name.equals("asin")
                    || name.equals("acos") || name.equals("atan") || name.equals("atan2")
                    || name.equals("sind") || name.equals("cosd") || name.equals("tand")
                    || name.equals("cotd") || name.equals("asind") || name.equals("acosd")
                    || name.equals("atand") || name.equals("atan2d")
                    || name.equals("sinh") || name.equals("cosh") || name.equals("tanh")
                    || name.equals("asinh") || name.equals("acosh") || name.equals("atanh")
                    || name.equals("random_normal")) return DataType.DOUBLE_PRECISION;
            // ts_rank/ts_rank_cd return float4 (OID 700), not text.
            if (name.equals("ts_rank") || name.equals("ts_rank_cd")) return DataType.REAL;
            // The text-search functions answer values of the text-search types. Left to the
            // fallback they were all described as text, so a client could not tell a document
            // from the characters it prints as, and no operator could be resolved over them.
            if (name.equals("to_tsvector") || name.equals("strip") || name.equals("setweight")
                    || name.equals("ts_delete") || name.equals("ts_filter")
                    || name.equals("array_to_tsvector") || name.equals("json_to_tsvector")
                    || name.equals("jsonb_to_tsvector") || name.equals("tsvector_concat")) {
                return DataType.TSVECTOR;
            }
            if (name.equals("to_tsquery") || name.equals("plainto_tsquery")
                    || name.equals("phraseto_tsquery") || name.equals("websearch_to_tsquery")
                    || name.equals("ts_rewrite") || name.equals("tsquery_phrase")
                    || name.equals("tsquery_and") || name.equals("tsquery_or")
                    || name.equals("tsquery_not")) {
                return DataType.TSQUERY;
            }
            if (name.equals("get_current_ts_config")) return DataType.REGCONFIG;
            // The XML constructors answer xml, whatever the characters they built happen to be.
            if (name.equals("xmlparse") || name.equals("xmlelement") || name.equals("xmlforest")
                    || name.equals("xmlconcat") || name.equals("xmlroot") || name.equals("xmlpi")
                    || name.equals("xmlcomment") || name.equals("xmlagg")) {
                return DataType.XML;
            }
            if (name.equals("numnode") || name.equals("tsvector_cmp")
                    || name.equals("tsquery_cmp")) {
                return DataType.INTEGER;
            }
            if (name.equals("ts_match_vq") || name.equals("ts_match_qv")
                    || name.equals("ts_match_tt") || name.equals("ts_match_tq")) {
                return DataType.BOOLEAN;
            }
            // The functions that answer bytes rather than the text of them. Left to the
            // fallback these were described as text, so a client reading the column's type saw
            // characters where the value is a string of bytes.
            if (name.equals("sha224") || name.equals("sha256") || name.equals("sha384")
                    || name.equals("sha512") || name.equals("decode") || name.equals("convert")
                    || name.equals("convert_to") || name.equals("set_byte")) {
                return DataType.BYTEA;
            }
            // set_bit is declared over both bytea and bit, and answers whichever it was given.
            if (name.equals("set_bit") && !fn.args().isEmpty()) {
                DataType given = inferTypeFromContext(fn.args().get(0), bindings);
                return given == DataType.BYTEA ? DataType.BYTEA : given;
            }
            // ts_headline hands back what it was given, with the matches marked inside it: a
            // document comes back a document. Read from the value alone an object came back as
            // the array its braces spell, so a client was handed a text[] of one element.
            if (name.equals("ts_headline")) {
                for (Expression arg : fn.args()) {
                    DataType given = inferTypeFromContext(arg, bindings);
                    if (given == DataType.JSON || given == DataType.JSONB) return given;
                }
                return DataType.TEXT;
            }
            if (name.equals("array_sample") || name.equals("array_shuffle")) {
                // Returns an array of the same type as the input
                if (!fn.args().isEmpty()) {
                    DataType argType = inferTypeFromContext(fn.args().get(0), bindings);
                    if (argType == DataType.INT4_ARRAY) return DataType.INT4_ARRAY;
                    if (argType == DataType.TEXT_ARRAY) return DataType.TEXT_ARRAY;
                    // If the argument is a cast to integer[], infer INT4_ARRAY
                    Expression arg0 = fn.args().get(0);
                    if (arg0 instanceof CastExpr) {
                        String targetType = ((CastExpr) arg0).typeName().toLowerCase();
                        if (targetType.equals("integer[]") || targetType.equals("int[]") || targetType.equals("int4[]")) return DataType.INT4_ARRAY;
                        if (targetType.equals("text[]") || targetType.equals("varchar[]")) return DataType.TEXT_ARRAY;
                    }
                    if (arg0 instanceof ArrayExpr) return DataType.INT4_ARRAY;
                }
                return DataType.TEXT;
            }
            if (name.equals("array_agg")) {
                // Derive the array type from the argument's element type. Advertising a fixed
                // _int4 (the old hardcoding) made pgjdbc parse text elements as int in text mode
                // ("Bad value for type int : tennet") and, worse, decode the payload as a binary
                // int4 array in binary mode — a garbage dimension count then triggers a giant
                // allocation (OutOfMemoryError at PgArray.readBinaryResultSet). Anything that
                // isn't int4-family is advertised as _text: elements travel as text, which pgjdbc
                // can always decode (enum-element arrays get their own array OID one level up,
                // in buildResultColumn, since a bare DataType can't carry the enum's identity).
                if (!fn.args().isEmpty()) {
                    DataType elem = inferTypeFromContext(fn.args().get(0), bindings);
                    if (elem != null) {
                        switch (elem) {
                            case INTEGER:
                            case SMALLINT:
                            case SERIAL:
                            case SMALLSERIAL:
                                return DataType.INT4_ARRAY;
                            // A document is not the text it prints as, and an array of them is
                            // read back as documents: array_agg over jsonb advertised as _text
                            // made every later reader of the array take its elements for strings,
                            // so to_jsonb of one quoted each of them.
                            case JSON:
                                return DataType.JSON_ARRAY;
                            case JSONB:
                                return DataType.JSONB_ARRAY;
                            default:
                                return DataType.TEXT_ARRAY;
                        }
                    }
                }
                return DataType.TEXT_ARRAY;
            }
            if (name.equals("row_number") || name.equals("rank") || name.equals("dense_rank")
                    || name.equals("ntile") || name.equals("txid_current")
                    || name.equals("pg_current_xact_id")
                    || name.equals("pg_current_xact_id_if_assigned")
                    || name.equals("txid_current_if_assigned")
                    || name.equals("pg_size_bytes")
                    || name.equals("pg_tablespace_size")) return DataType.BIGINT;
            if (name.equals("lag") || name.equals("lead") || name.equals("first_value")
                    || name.equals("last_value") || name.equals("nth_value")) {
                if (!fn.args().isEmpty()) return inferTypeFromContext(fn.args().get(0), bindings);
                return DataType.TEXT;
            }
            if (name.equals("generate_series")) {
                // The SRF's advertised element type is the type of its first (start) argument:
                // generate_series(int/bigint, ...) -> setof int/bigint,
                // generate_series(timestamp[tz], ..., interval) -> setof timestamp[tz].
                // Without this, a SELECT-list generate_series() (as opposed to one used in FROM,
                // which is typed by FromFunctionResolver from the actual runtime values) fell
                // through to the default DataType.TEXT below, so pgjdbc's strict getObject(col,
                // LocalDate.class)/getTimestamp rejected the RowDescription/value mismatch.
                if (!fn.args().isEmpty()) {
                    DataType dt = inferTypeFromContext(fn.args().get(0), bindings);
                    if (dt != null) return dt;
                }
                return DataType.TEXT;
            }
            // A backend's pid is an integer, and the calls that take one resolve on that. Read as
            // text it was an argument no signature of pg_blocking_pids accepts, so the call
            // settled on no declared result type at all.
            if (name.equals("pg_backend_pid")) return DataType.INTEGER;
            if (name.equals("uuid_generate_v4") || name.equals("gen_random_uuid") || name.equals("uuidv4")) return DataType.UUID;
            if (name.equals("json_serialize")) return DataType.TEXT;
            // date_part answers in double precision and extract in numeric, whatever unit either
            // was asked for. Described as text the value travelled as a string, so a client that
            // reads the column type saw a number as text and integer division took over
            // downstream, even though pg_typeof() already answered correctly.
            if (name.equals("date_part")) return DataType.DOUBLE_PRECISION;
            if (name.equals("extract")) return DataType.NUMERIC;
            // A range constructor answers in its own range type, not in the text its value
            // happens to be spelled as.
            DataType rangeCtor = rangeConstructorResultType(name);
            if (rangeCtor != null) return rangeCtor;
            // A geometric constructor answers in the shape it builds. Its value is held as the
            // text PostgreSQL prints, which is why the column used to be described as text --
            // and a client reading point(1,2) then got a string where PG hands it a point.
            DataType geometric = geometricConstructorResultType(name, fn);
            if (geometric != null) return geometric;
            // A type name written like a call is a cast, and the column has the type that was
            // written: int4('42') is an int4 column, jsonb('{}') a jsonb one.
            if (executor != null) {
                DataType coercion = executor.functionEvaluator.typeNameCoercionResultType(name, fn);
                if (coercion != null) return coercion;
            }
            // The JSON builders answer in json or jsonb by their own name.
            DataType jsonResult = jsonFunctionResultType(name);
            if (jsonResult != null) return jsonResult;
            // Check user-defined functions and aggregates for return type. A routine of the
            // database's own is filed under the name it was declared with, and the name of this
            // call has been folded once already where it was written -- so it is looked up as it
            // stands. Folding it a second time asked the catalog after a routine it does not
            // hold, and a call of a quoted name was then described as text whatever it returns.
            if (executor != null && executor.database != null) {
                String declaredName = FunctionEvaluator.stripSchemaPrefix(fn.name());
                PgFunction userFunc = resolveUserFunctionForTyping(declaredName, fn, bindings);
                if (userFunc != null && userFunc.getReturnType() != null) {
                    String declaredReturn = userFunc.getReturnType().replaceAll("\\(.*\\)", "").trim();
                    // SETOF t is a set of t, and a call that expands into rows carries one t per
                    // row -- so the column is t. Reading "SETOF int" as a type name found none
                    // and the expanded rows were described as text.
                    if (declaredReturn.regionMatches(true, 0, "SETOF ", 0, 6)) {
                        declaredReturn = declaredReturn.substring(6).trim();
                    }
                    if (PolymorphicTypes.isPolymorphic(declaredReturn)) {
                        // The result type of a polymorphic routine is whatever this call's
                        // arguments bind its slots to.
                        String concrete = polymorphicReturnType(userFunc, fn, bindings);
                        if (concrete != null) {
                            DataType dt = DataType.fromPgName(concrete);
                            if (dt != null) return dt;
                        }
                    }
                    DataType dt = DataType.fromPgName(declaredReturn);
                    if (dt != null) return dt;
                }
                PgAggregate userAgg = executor.database.getAggregate(declaredName);
                if (userAgg != null) {
                    // If aggregate has a finalfunc, use its return type
                    String ff = userAgg.getFinalfunc();
                    if (ff != null) {
                        PgFunction ffFunc = executor.database.getFunction(ff);
                        if (ffFunc != null && ffFunc.getReturnType() != null) {
                            DataType dt = DataType.fromPgName(ffFunc.getReturnType().replaceAll("\\(.*\\)", "").trim());
                            if (dt != null) return dt;
                        }
                    }
                    // Otherwise use the stype
                    if (userAgg.getStype() != null) {
                        DataType dt = DataType.fromPgName(userAgg.getStype().replaceAll("\\(.*\\)", "").trim());
                        if (dt != null) return dt;
                    }
                }
            }
            // Last of all, a name that is nothing but the implementation of an operator answers
            // exactly as the operator does, so the column is typed from the same expression
            // evaluation runs. Asked last, because a name the engine dispatches for itself has
            // already answered above — json_extract_path is the #> operator's function and is
            // also a function a query may call directly.
            Expression asOperator = FunctionEvaluator.operatorFunctionExpr(name, fn);
            if (asOperator != null) return inferTypeFromContext(asOperator, bindings);
            return DataType.TEXT;
        }
        if (expr instanceof AtTimeZoneExpr) {
            // PG semantics (mirrors the runtime AtTimeZoneExpr evaluation above): timestamptz AT
            // TIME ZONE z -> timestamp; timestamp AT TIME ZONE z -> timestamptz. Falling through
            // to the default DataType.TEXT here (as before this fix) is what let a SELECT-list
            // generate_series() whose start argument is a date_trunc(...) AT TIME ZONE ... (the
            // exact ResultsMonthlyDao months-CTE shape) advertise TEXT.
            DataType inner = inferTypeFromContext(((AtTimeZoneExpr) expr).expr(), bindings);
            if (inner == DataType.TIMESTAMPTZ) return DataType.TIMESTAMP;
            if (inner == DataType.TIMESTAMP) return DataType.TIMESTAMPTZ;
            // A date arrives as a timestamptz, so it comes back out as a timestamp; a time of
            // either kind comes back as timetz.
            if (inner == DataType.DATE) return DataType.TIMESTAMP;
            if (inner == DataType.TIME) return DataType.TIMETZ;
            return inner != null ? inner : DataType.TIMESTAMP;
        }
        if (expr instanceof OrderedSetAggExpr) {
            return orderedSetAggResultType((OrderedSetAggExpr) expr, bindings);
        }
        // COLLATE names how a string sorts; it never changes what the string is, so the column
        // keeps the type it had. Falling through to text made a varchar arrive as text.
        if (expr instanceof CollateExpr) {
            return inferTypeFromContext(((CollateExpr) expr).expr(), bindings);
        }
        if (expr instanceof IsNullExpr) return DataType.BOOLEAN;
        if (expr instanceof InExpr) return DataType.BOOLEAN;
        if (expr instanceof BetweenExpr) return DataType.BOOLEAN;
        if (expr instanceof LikeExpr) return DataType.BOOLEAN;
        if (expr instanceof ExistsExpr) return DataType.BOOLEAN;
        // A sub-query used as a value has the type of the one column it answers with, and
        // ARRAY(...) has the array of that type. Reporting either as text made the driver decode
        // integers as strings, even though pg_typeof already answered correctly.
        if (expr instanceof SubqueryExpr) {
            return subqueryColumnType(((SubqueryExpr) expr).subquery(), bindings);
        }
        if (expr instanceof ArraySubqueryExpr) {
            DataType element = subqueryColumnType(((ArraySubqueryExpr) expr).subquery(), bindings);
            DataType array = element == null ? null : DataType.arrayOf(element);
            return array != null ? array : DataType.TEXT_ARRAY;
        }
        if (expr instanceof AnyAllExpr) return DataType.BOOLEAN;
        if (expr instanceof AnyAllArrayExpr) return DataType.BOOLEAN;
        if (expr instanceof IsBooleanExpr) return DataType.BOOLEAN;
        if (expr instanceof IsJsonExpr) return DataType.BOOLEAN;
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
                // The subquery's own FROM tables must be visible when inferring its first
                // target's type — with only the outer bindings, a scalar subquery like
                // (SELECT array_agg(p.provider_id) FROM prov p ...) can't resolve p.provider_id
                // and falls back to TEXT regardless of the actual column type.
                return inferTypeFromContext(stmt.targets().get(0).expr(), subqueryScopedBindings(stmt, bindings));
            }
            return DataType.TEXT;
        }
        if (expr instanceof ArrayExpr) {
            ArrayExpr arr = (ArrayExpr) expr;
            // ROW(...) is a record, not an array, and has no single element type to speak of
            if (arr.isRow()) return DataType.RECORD;
            // An array of records is a record array; its elements have no one element type
            // either, so they are not asked for one.
            for (Expression element : arr.elements()) {
                if (element instanceof ArrayExpr && ((ArrayExpr) element).isRow()) {
                    return DataType.RECORD_ARRAY;
                }
            }
            for (Expression element : arr.elements()) {
                if (isUnknownLiteral(element)) continue;
                DataType array = DataType.arrayOf(inferTypeFromContext(element, bindings));
                if (array != null) return array;
            }
            return DataType.TEXT_ARRAY;
        }
        return DataType.TEXT;
    }

    /**
     * Bindings for inferring types inside a scalar subquery: the subquery's own plain-table FROM
     * items first (inner scope wins), then the caller's outer bindings (for correlated
     * references). Only simple {@code TableRef}s are resolved — join trees/nested subqueries in
     * FROM keep the previous behavior (fall through to the outer bindings / TEXT default).
     */
    private List<RowContext.TableBinding> subqueryScopedBindings(SelectStmt stmt, List<RowContext.TableBinding> outer) {
        if (stmt.from() == null || stmt.from().isEmpty() || executor == null) return outer;
        List<RowContext.TableBinding> result = new ArrayList<>();
        for (SelectStmt.FromItem fi : stmt.from()) {
            if (!(fi instanceof SelectStmt.TableRef)) continue;
            SelectStmt.TableRef tr = (SelectStmt.TableRef) fi;
            Table t;
            try {
                t = tr.schema() != null ? executor.resolveTable(tr.schema(), tr.table())
                        : executor.resolveTableSafe(tr.table());
            } catch (MemgresException e) {
                t = null;
            }
            if (t != null) {
                result.add(new RowContext.TableBinding(t, tr.alias() != null ? tr.alias() : tr.table(), null, null));
            }
        }
        if (result.isEmpty()) return outer;
        result.addAll(outer);
        return result;
    }

    DataType inferExprType(Expression expr) {
        return inferTypeFromContext(expr, Cols.listOf());
    }

    /**
     * When an expression's inferred type is {@link DataType#ENUM}, resolves the concrete enum
     * type name so callers can advertise the real per-type OID in RowDescription (see
     * {@code PgWireValueFormatter.columnTypeOid}, which falls back to the ENUM placeholder OID 0
     * -- and crashes pgjdbc -- whenever a column's type is ENUM but its enum type name is null).
     * {@link #inferTypeFromContext} already infers ENUM correctly for a plain column reference,
     * but discards *which* enum it is for any built expression (COALESCE, CASE, an explicit cast,
     * ...); this mirrors the same branches to recover that name wherever it's statically
     * determinable. Returns {@code null} when it can't be determined (e.g. a user-defined
     * function/aggregate return type, or divergent enum types on either side of a CASE/COALESCE)
     * -- callers should then advertise TEXT rather than ENUM-with-no-name.
     */
    String resolveEnumTypeName(Expression expr, List<RowContext.TableBinding> bindings) {
        if (expr instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) expr;
            for (RowContext.TableBinding b : bindings) {
                if (ref.table() != null) {
                    if (!ref.table().equalsIgnoreCase(b.alias()) &&
                            !ref.table().equalsIgnoreCase(b.table().getName())) continue;
                }
                int idx = b.table().getColumnIndex(ref.column());
                if (idx >= 0) return b.table().getColumns().get(idx).getEnumTypeName();
            }
            return null;
        }
        if (expr instanceof CastExpr) {
            String typeName = ((CastExpr) expr).typeName().replaceAll("\\(.*\\)", "").trim().toLowerCase();
            return executor.database.getCustomEnum(typeName) != null ? typeName : null;
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            String name = FunctionEvaluator.stripSchemaPrefix(fn.name().toLowerCase());
            // An enum's own name written like a call is a cast to it, so the column is that enum.
            if (fn.args().size() == 1 && executor != null) {
                String coerced = executor.functionEvaluator.coercibleTypeName(name);
                if (coerced != null && executor.database.getCustomEnum(coerced) != null) {
                    return coerced;
                }
            }
            if (name.equals("coalesce") || name.equals("nullif") || name.equals("greatest") || name.equals("least")
                    || name.equals("max") || name.equals("min") || name.equals("first_value")
                    || name.equals("last_value") || name.equals("nth_value") || name.equals("lag") || name.equals("lead")) {
                for (Expression arg : fn.args()) {
                    String n = resolveEnumTypeName(arg, bindings);
                    if (n != null) return n;
                }
            }
            return null;
        }
        if (expr instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) expr;
            for (CaseExpr.WhenClause wc : c.whenClauses()) {
                String n = resolveEnumTypeName(wc.result(), bindings);
                if (n != null) return n;
            }
            if (c.elseExpr() != null) return resolveEnumTypeName(c.elseExpr(), bindings);
            return null;
        }
        if (expr instanceof SubqueryExpr) {
            SubqueryExpr sq = (SubqueryExpr) expr;
            if (sq.subquery() instanceof SelectStmt && ((SelectStmt) sq.subquery()).targets() != null
                    && !((SelectStmt) sq.subquery()).targets().isEmpty()) {
                SelectStmt stmt = (SelectStmt) sq.subquery();
                return resolveEnumTypeName(stmt.targets().get(0).expr(), subqueryScopedBindings(stmt, bindings));
            }
            return null;
        }
        return null;
    }

    /**
     * Unwraps a projected expression to the {@code array_agg(...)} call it produces, if any:
     * either the expression is the call itself, or it is a scalar subquery whose single/first
     * target is the call. Returns {@code null} otherwise.
     */
    private static FunctionCallExpr findArrayAggCall(Expression expr) {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            return FunctionEvaluator.stripSchemaPrefix(fn.name().toLowerCase()).equals("array_agg") ? fn : null;
        }
        if (expr instanceof SubqueryExpr) {
            SubqueryExpr sq = (SubqueryExpr) expr;
            if (sq.subquery() instanceof SelectStmt && ((SelectStmt) sq.subquery()).targets() != null
                    && !((SelectStmt) sq.subquery()).targets().isEmpty()) {
                return findArrayAggCall(((SelectStmt) sq.subquery()).targets().get(0).expr());
            }
        }
        return null;
    }

    /**
     * Builds the result {@link Column} for a projected expression that isn't a plain resolved
     * column reference. Extends the plain type inference with the two cases a bare
     * {@link DataType} can't express:
     * <ul>
     *   <li>{@code array_agg} over a custom-enum element: the column must advertise the enum's
     *       own ARRAY type OID (the wave-4 pg_type machinery: {@code type=ENUM},
     *       {@code enumTypeName}, {@code arrayElementType=ENUM}), not {@code _int4}/{@code _text}
     *       — see {@code PgWireValueFormatter.columnTypeOid};</li>
     *   <li>a scalar-ENUM-typed expression (COALESCE/CASE/cast/...): the concrete enum type name
     *       is recovered via {@link #resolveEnumTypeName} (mtask-8 C1) or the column safely
     *       downgrades to TEXT — never an unnamed ENUM, whose placeholder OID 0 crashes
     *       pgjdbc.</li>
     * </ul>
     */
    Column buildResultColumn(String alias, Expression expr, List<RowContext.TableBinding> bindings) {
        // A column an enclosing level supplies is that column: a LATERAL that projects one keeps
        // everything the type is made of, not just the DataType — an int[] exposed through a
        // LATERAL described itself as _int4 while the same column read directly is integer[].
        if (expr instanceof ColumnRef && !resolvesHere((ColumnRef) expr, bindings)) {
            Column outer = columnFromOuterContexts((ColumnRef) expr);
            // What is copied is what the type is made of. A projection is not the relation's key
            // and carries neither its default nor its generation expression, so those are dropped.
            if (outer != null) {
                return new Column(alias, outer.getType(), true, false, null,
                        outer.getEnumTypeName(), outer.getPrecision(), outer.getScale(), null,
                        false, outer.getDomainTypeName(), outer.getCompositeTypeName(),
                        outer.getArrayElementType());
            }
        }
        FunctionCallExpr arrayAgg = findArrayAggCall(expr);
        if (arrayAgg != null && !arrayAgg.args().isEmpty()) {
            List<RowContext.TableBinding> scope = expr instanceof SubqueryExpr
                    && ((SubqueryExpr) expr).subquery() instanceof SelectStmt
                    ? subqueryScopedBindings((SelectStmt) ((SubqueryExpr) expr).subquery(), bindings)
                    : bindings;
            if (inferTypeFromContext(arrayAgg.args().get(0), scope) == DataType.ENUM) {
                String enumTypeName = resolveEnumTypeName(arrayAgg.args().get(0), scope);
                if (enumTypeName != null) {
                    return new Column(alias, DataType.ENUM, true, false, null, enumTypeName,
                            null, null, null, false, null, null, DataType.ENUM);
                }
            }
        }
        DataType targetType = inferTypeFromContext(expr, bindings);
        if (targetType == DataType.ENUM) {
            String enumTypeName = resolveEnumTypeName(expr, bindings);
            return enumTypeName != null
                    ? new Column(alias, DataType.ENUM, true, false, null, enumTypeName)
                    : new Column(alias, DataType.TEXT, true, false, null);
        }
        return new Column(alias, targetType, true, false, null);
    }

    // ---- JSON path parsing ----

    List<String> parseJsonPathArg(Object right) {
        return JsonOperations.parsePathArray(right);
    }

    /** True for the tests that read a three-valued boolean, so a non-boolean has no answer. */
    private static boolean isBooleanTest(IsBooleanExpr.BooleanTest test) {
        switch (test) {
            case IS_TRUE: case IS_NOT_TRUE: case IS_FALSE: case IS_NOT_FALSE:
            case IS_UNKNOWN: case IS_NOT_UNKNOWN:
                return true;
            default:
                return false;
        }
    }

    /** How PostgreSQL names each test in its error message. */
    private static String booleanTestName(IsBooleanExpr.BooleanTest test) {
        switch (test) {
            case IS_TRUE: return "IS TRUE";
            case IS_NOT_TRUE: return "IS NOT TRUE";
            case IS_FALSE: return "IS FALSE";
            case IS_NOT_FALSE: return "IS NOT FALSE";
            case IS_UNKNOWN: return "IS UNKNOWN";
            default: return "IS NOT UNKNOWN";
        }
    }

    // ---- Row comparison ----

    /**
     * A row comparison is defined entry by entry, so two row constructors of different lengths have
     * no comparison to make and PostgreSQL says so rather than answering. Reading the values
     * instead let {@code ROW(1,2) < ROW(1,2,3)} come out true on the strength of the entries the
     * two rows happen to share.
     */
    private void rejectUnequalRowArity(BinaryExpr bin) {
        if (!isRowComparison(bin.op())) return;
        int left = writtenRowWidth(bin.left());
        int right = writtenRowWidth(bin.right());
        if (left > 0 && right > 0) {
            if (left != right) {
                throw new MemgresException("unequal number of entries in row expressions", "42601");
            }
            List<Expression> leftEntries = ((ArrayExpr) bin.left()).elements();
            List<Expression> rightEntries = ((ArrayExpr) bin.right()).elements();
            for (int i = 0; i < left; i++) {
                rejectUntypedTextEntry(leftEntries.get(i), inferExprType(rightEntries.get(i)));
                rejectUntypedTextEntry(rightEntries.get(i), inferExprType(leftEntries.get(i)));
            }
            return;
        }
        // A row on one side and a single value on the other is not a comparison PostgreSQL has an
        // operator for, and it says so rather than reading the row's first entry. Only the two
        // distinctness operators are judged here: the six ordered comparisons settle a subquery of
        // the wrong width by its width, which is a different complaint and already made.
        if (bin.op() != BinaryExpr.BinOp.IS_DISTINCT_FROM
                && bin.op() != BinaryExpr.BinOp.IS_NOT_DISTINCT_FROM) {
            return;
        }
        if (left > 1 && isWrittenScalar(bin.right())) {
            throw noRecordOperator(typeNameOf(bin.right()), false);
        }
        if (right > 1 && isWrittenScalar(bin.left())) {
            throw noRecordOperator(typeNameOf(bin.left()), true);
        }
    }

    /** How many entries a row constructor is written with, or -1 when this is not one. */
    private static int writtenRowWidth(Expression expr) {
        if (expr instanceof ArrayExpr && ((ArrayExpr) expr).isRow()) {
            return ((ArrayExpr) expr).elements().size();
        }
        return -1;
    }

    /** True when the text fixes this side at one value: a literal, or a one-column subquery. */
    private static boolean isWrittenScalar(Expression expr) {
        if (expr instanceof Literal) return true;
        if (expr instanceof CastExpr) return isWrittenScalar(((CastExpr) expr).expr());
        return expr instanceof SubqueryExpr
                && SelectStmt.writtenWidth(((SubqueryExpr) expr).subquery()) == 1;
    }

    private String typeNameOf(Expression expr) {
        DataType type = inferExprType(expr);
        return type == null ? "unknown" : type.toRegtypeDisplay();
    }

    /** PostgreSQL's complaint that a row and a single value have no comparison between them. */
    static MemgresException noRecordOperator(String otherType, boolean otherOnLeft) {
        return new MemgresException("operator does not exist: "
                + (otherOnLeft ? otherType + " = record" : "record = " + otherType), "42883");
    }

    /** The operators that compare two rows entry by entry. */
    private static boolean isRowComparison(BinaryExpr.BinOp op) {
        switch (op) {
            case EQUAL: case NOT_EQUAL:
            case LESS_THAN: case GREATER_THAN: case LESS_EQUAL: case GREATER_EQUAL:
            case IS_DISTINCT_FROM: case IS_NOT_DISTINCT_FROM:
                return true;
            default:
                return false;
        }
    }

    // ---- Result type unification ----

    /** The families a branch list can be unified within; PostgreSQL never mixes two of them. */
    private enum ResultFamily { STRING, NUMERIC, BOOLEAN, DATETIME, ARRAY }

    /**
     * The type an expression is given by the query text itself -- a cast or a literal -- and
     * nothing inferred. A column's type is deliberately not consulted: a derived column out of a
     * subquery carries whatever type the engine defaulted it to, and refusing a query on the
     * strength of that rejects SQL PostgreSQL runs, which is worse than the permissiveness being
     * fixed. A bare string literal is PostgreSQL's {@code unknown} and returns null too.
     */
    private static String typeWrittenInQuery(Expression expr) {
        if (expr instanceof CastExpr) {
            String name = ((CastExpr) expr).typeName();
            return name == null ? null : name.toLowerCase().trim();
        }
        if (expr instanceof Literal) {
            switch (((Literal) expr).literalType()) {
                case INTEGER: return "integer";
                case FLOAT: return "numeric";
                case BOOLEAN: return "boolean";
                case BIT_STRING: return "bit";
                default: return null;
            }
        }
        if (expr instanceof ArrayExpr) {
            String element = arrayElementTypeWritten((ArrayExpr) expr);
            return element == null ? null : element + "[]";
        }
        if (expr instanceof SubqueryExpr) {
            return subqueryOutputType(((SubqueryExpr) expr).subquery());
        }
        if (expr instanceof BinaryExpr) return arithmeticResultTypeWritten((BinaryExpr) expr);
        return null;
    }

    /**
     * The element type an array constructor is written with. Every element takes part, because
     * PostgreSQL settles one element type across the whole constructor before it builds anything;
     * an element that says nothing about its type is read as the settled one, and a constructor
     * whose elements all say nothing is an array of text.
     */
    private static String arrayElementTypeWritten(ArrayExpr arr) {
        // A row is not an array: its fields keep their own types rather than settling on one.
        if (arr.isRow() || arr.elements() == null || arr.elements().isEmpty()) return null;
        String settled = null;
        for (Expression element : arr.elements()) {
            // Only one level: an array of arrays is one value of one type here, and deciding what
            // that type is takes rules this does not have.
            if (element instanceof ArrayExpr) return null;
            String name = typeWrittenInQuery(element);
            if (name == null) {
                if (!isUnknownConstant(element)) return null;
                continue;
            }
            ResultFamily family = familyOf(name);
            if (family == null) return null;
            if (settled == null) { settled = name; continue; }
            if (familyOf(settled) != family) return null;
            if (rankOf(name) > rankOf(settled)) settled = name;
        }
        // Nothing in the constructor said what it held, and PostgreSQL reads that as text.
        return settled != null ? pgName(settled) : "text";
    }

    /**
     * The type a scalar subquery is written to produce. Its select list settles it, and an unknown
     * there is settled too: a subquery hands its answer out as a value of some type, so there is
     * no unknown left to be read as whatever it is compared against.
     */
    private static String subqueryOutputType(Statement subquery) {
        if (!(subquery instanceof SelectStmt)) return null;
        SelectStmt stmt = (SelectStmt) subquery;
        if (stmt.targets() == null || stmt.targets().size() != 1) return null;
        Expression target = stmt.targets().get(0).expr();
        String name = typeWrittenInQuery(target);
        if (name != null) return name;
        return isUnknownConstant(target) ? "text" : null;
    }

    /** The type an arithmetic expression is written to produce: the wider of its two operands. */
    private static String arithmeticResultTypeWritten(BinaryExpr bin) {
        switch (bin.op()) {
            case ADD: case SUBTRACT: case MULTIPLY: case DIVIDE: case MODULO: break;
            default: return null;
        }
        String left = typeWrittenInQuery(bin.left());
        String right = typeWrittenInQuery(bin.right());
        if (left == null || right == null) return null;
        if (familyOf(left) != ResultFamily.NUMERIC || familyOf(right) != ResultFamily.NUMERIC) {
            return null;
        }
        return rankOf(right) > rankOf(left) ? right : left;
    }

    /**
     * True for a constant that says nothing about its type -- a bare string or a bare NULL -- both
     * of which PostgreSQL reads as {@code unknown} until something settles what they are.
     */
    private static boolean isUnknownConstant(Expression expr) {
        if (!(expr instanceof Literal)) return false;
        Literal.LiteralType type = ((Literal) expr).literalType();
        return type == Literal.LiteralType.STRING || type == Literal.LiteralType.NULL;
    }

    /** True for a string literal written without a cast, which is PostgreSQL's {@code unknown}. */
    private static boolean isUntypedStringLiteral(Expression expr) {
        return expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.STRING;
    }

    /**
     * PostgreSQL settles CASE, COALESCE, GREATEST and LEAST on a single result type before it runs
     * anything: it walks the branches keeping the widest declared type seen so far, and stops with
     * an error the moment a branch names a type from another family. Answering from whichever
     * branch happened to be taken let queries PostgreSQL refuses to plan return a value here.
     *
     * <p>Only a type the query writes down takes part. A bare string literal is {@code unknown} and
     * is read as the settled type once it is known, which is why {@code COALESCE(1::int, 'x')}
     * fails on the input {@code x} rather than on a type mismatch, while {@code '2'::text} beside
     * an integer really is one.
     *
     * @param context how PostgreSQL names the construct in the error: CASE, COALESCE, GREATEST, LEAST
     */
    private void unifyResultTypes(String context, List<Expression> branches) {
        String ptype = null;
        ResultFamily pfamily = null;
        int prank = 0;
        for (int i = 0; i < branches.size(); i++) {
            String name = typeWrittenInQuery(branches.get(i));
            if (name == null) continue;
            ResultFamily family = familyOf(name);
            // A family this rule does not judge (uuid, json, ranges, geometry, a domain, an enum)
            // carries its own coercion rules; guessing at them would reject SQL PostgreSQL accepts.
            if (family == null) return;
            if (pfamily == null) {
                ptype = name;
                pfamily = family;
                prank = rankOf(name);
                continue;
            }
            if (family != pfamily) {
                throw new MemgresException(context + " types " + pgName(ptype) + " and "
                        + pgName(name) + " cannot be matched", "42804");
            }
            if (family == ResultFamily.ARRAY) {
                String settledElement = elementOf(ptype);
                String element = elementOf(name);
                if (familyOf(element) != familyOf(settledElement)) {
                    // Two arrays are of one family whatever they hold, so the complaint is not
                    // that the types cannot be matched but that this one cannot be read as that.
                    throw new MemgresException(branchLabel(context) + " could not convert type "
                            + pgName(name) + " to " + pgName(ptype), "42846");
                }
                if (rankOf(element) > rankOf(settledElement)) {
                    ptype = name;
                    prank = rankOf(element);
                }
                continue;
            }
            int rank = rankOf(name);
            if (rank > prank) {
                ptype = name;
                prank = rank;
            }
        }
        // Reading an untyped literal as the settled type is what turns a mismatch into an input
        // error. Only the two families whose text form memgres reads exactly as PostgreSQL does
        // take part: a date or a string accepts too much for a failure here to mean the same thing.
        if (pfamily != ResultFamily.NUMERIC && pfamily != ResultFamily.BOOLEAN) return;
        for (int i = 0; i < branches.size(); i++) {
            Expression branch = branches.get(i);
            if (isUntypedStringLiteral(branch)) {
                executor.castValue(((Literal) branch).value(), ptype);
            }
        }
    }

    /** The element type of an array type name. */
    private static String elementOf(String arrayTypeName) {
        return arrayTypeName.endsWith("[]")
                ? arrayTypeName.substring(0, arrayTypeName.length() - 2) : arrayTypeName;
    }

    /**
     * How PostgreSQL names the construct when it is one branch that will not read as another,
     * rather than the construct as a whole: a CASE says which part of itself is at fault.
     */
    private static String branchLabel(String context) {
        return "CASE".equals(context) ? "CASE/WHEN" : context;
    }

    private static boolean isIntegerValue(Object v) {
        return v instanceof Short || v instanceof Integer || v instanceof Long;
    }

    /** The type an integer operation answers in: the wider of its two operands. */
    private static DataType widerIntegerType(Object left, Object right) {
        if (left instanceof Long || right instanceof Long) return DataType.BIGINT;
        if (left instanceof Integer || right instanceof Integer) return DataType.INTEGER;
        return DataType.SMALLINT;
    }

    /** The result as the width says it is, or the out-of-range that width raises. */
    private static Object narrowedInteger(long result, DataType width) {
        switch (width) {
            case SMALLINT:
                if (result < Short.MIN_VALUE || result > Short.MAX_VALUE) throw integerOutOfRange(width);
                return (short) result;
            case INTEGER:
                if (result < Integer.MIN_VALUE || result > Integer.MAX_VALUE) throw integerOutOfRange(width);
                return (int) result;
            default:
                return result;
        }
    }

    private static MemgresException integerOutOfRange(DataType width) {
        // PostgreSQL sends no datatype field with an integer overflow: the width is already in the
        // message, and the field is reserved for the declared type a domain or a cast named.
        String name = width == DataType.SMALLINT ? "smallint"
                : width == DataType.INTEGER ? "integer" : "bigint";
        return new MemgresException(name + " out of range", "22003");
    }

    /** The family a declared type belongs to, or null when it is one this rule leaves alone. */
    private static ResultFamily familyOf(String typeName) {
        String t = typeName.toLowerCase().trim();
        // An array is its own family: PostgreSQL matches one array against another by their
        // element types, and against anything else not at all.
        if (t.endsWith("[]")) {
            return familyOf(t.substring(0, t.length() - 2)) == null ? null : ResultFamily.ARRAY;
        }
        int paren = t.indexOf('(');
        if (paren > 0) t = t.substring(0, paren).trim();
        switch (t) {
            case "text": case "varchar": case "character varying": case "char":
            case "character": case "bpchar": case "name":
                return ResultFamily.STRING;
            case "smallint": case "integer": case "int": case "int2": case "int4": case "int8":
            case "bigint": case "numeric": case "decimal": case "real": case "double precision":
            case "float4": case "float8": case "float":
                return ResultFamily.NUMERIC;
            case "boolean": case "bool":
                return ResultFamily.BOOLEAN;
            case "date": case "timestamp": case "timestamptz":
            case "timestamp without time zone": case "timestamp with time zone":
                return ResultFamily.DATETIME;
            default:
                return null;
        }
    }

    /**
     * How wide a type is within its family. The widest wins, which is the same answer PostgreSQL
     * reaches by keeping whichever of two types the other converts to implicitly.
     */
    private static int rankOf(String typeName) {
        String t = typeName.toLowerCase().trim();
        int paren = t.indexOf('(');
        if (paren > 0) t = t.substring(0, paren).trim();
        switch (t) {
            case "char": case "character": case "bpchar": case "name": return 1;
            case "varchar": case "character varying": return 2;
            case "text": return 3;
            case "smallint": case "int2": return 1;
            case "integer": case "int": case "int4": return 2;
            case "bigint": case "int8": return 3;
            case "numeric": case "decimal": return 4;
            case "real": case "float4": return 5;
            case "double precision": case "float8": case "float": return 6;
            case "date": return 1;
            case "timestamp": case "timestamp without time zone": return 2;
            case "timestamptz": case "timestamp with time zone": return 3;
            default: return 0;
        }
    }

    /** The name PostgreSQL prints for a type in a mismatch message. */
    private static String pgName(String typeName) {
        String t = typeName.toLowerCase().trim();
        switch (t) {
            case "int": case "int4": return "integer";
            case "int2": return "smallint";
            case "int8": return "bigint";
            case "float8": case "float": return "double precision";
            case "float4": return "real";
            case "varchar": return "character varying";
            case "char": case "bpchar": return "character";
            case "bool": return "boolean";
            case "decimal": return "numeric";
            case "timestamp without time zone": return "timestamp";
            case "timestamp with time zone": return "timestamptz";
            default: return t;
        }
    }

    /**
     * COALESCE, GREATEST and LEAST settle on one type across all their arguments, exactly as CASE
     * does across its branches.
     */
    private void unifyVariadicArgumentTypes(FunctionCallExpr fn) {
        List<Expression> args = fn.args();
        if (args == null || args.size() < 2) return;
        String name = fn.name();
        if (name.equalsIgnoreCase("coalesce")) unifyResultTypes("COALESCE", args);
        else if (name.equalsIgnoreCase("greatest")) unifyResultTypes("GREATEST", args);
        else if (name.equalsIgnoreCase("least")) unifyResultTypes("LEAST", args);
    }

    /**
     * The CASE branches in the order PostgreSQL unifies them: the ELSE result first, then each
     * WHEN result. The order is what decides which of the two types a mismatch message names first.
     */
    private static List<Expression> caseResultBranches(CaseExpr c) {
        List<Expression> branches = new ArrayList<Expression>(c.whenClauses().size() + 1);
        if (c.elseExpr() != null) branches.add(c.elseExpr());
        for (CaseExpr.WhenClause when : c.whenClauses()) branches.add(when.result());
        return branches;
    }
}
