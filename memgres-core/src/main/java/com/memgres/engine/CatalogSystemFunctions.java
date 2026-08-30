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

    /** A function declared to return void answers with this, never with NULL. */
    static final Object VOID_RESULT = "";

    static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    private final AstExecutor executor;
    private final CatalogMetadataFunctions metadataFunctions;
    private final CatalogPrivilegeFunctions privilegeFunctions;

    CatalogSystemFunctions(AstExecutor executor) {
        this.executor = executor;
        this.metadataFunctions = new CatalogMetadataFunctions(executor);
        this.privilegeFunctions = new CatalogPrivilegeFunctions(executor);
    }

    /** A column's recorded type name as PostgreSQL spells it for this session. */
    private String typeDisplay(String stored) {
        return TypeNamespace.display(executor.database, executor.session, stored);
    }

    /**
     * The type the definition behind a relation settled for the column a reference names, or null
     * where the relation is not one built from a definition or its definition settled nothing.
     */
    private static String definedColumnType(RowContext ctx, ColumnRef ref) {
        if (ctx == null) return null;
        for (RowContext.TableBinding b : ctx.getBindings()) {
            Table table = b.table();
            if (table == null || !table.hasDefinedColumnTypes()) continue;
            String exposed = b.alias() != null ? b.alias() : table.getName();
            if (ref.table() != null && !ref.table().equalsIgnoreCase(exposed)) continue;
            int idx = table.getColumnIndex(ref.column());
            if (idx < 0) continue;
            return table.definedColumnType(idx);
        }
        return null;
    }

    /** True when the call names a user function whose declared result type is polymorphic. */
    private boolean isPolymorphicUserFunction(FunctionCallExpr fn) {
        String name = fn.name();
        int dot = name.lastIndexOf('.');
        if (dot >= 0) name = name.substring(dot + 1);
        for (PgFunction f : executor.database.getFunctionOverloads(name)) {
            if (PolymorphicTypes.isPolymorphic(f.getReturnType())) return true;
        }
        return false;
    }

    /**
     * The written arguments of a call named the way PostgreSQL names them in a 42883. An
     * unadorned string literal is "unknown": it has no type until the function it is handed to
     * gives it one, and a call that resolves to nothing never does.
     */
    private String writtenArgTypes(FunctionCallExpr fn, RowContext ctx) {
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

    private void requireArgs(FunctionCallExpr fn, int min) {
        if (fn.args().size() < min) {
            throw new MemgresException(
                "function " + fn.name() + "() does not exist" +
                (fn.args().isEmpty() ? "" : "\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts."), "42883");
        }
    }

    /**
     * The schema holding the relation this regclass names, or null when it cannot be told.
     */
    private String schemaOfRelation(Object regclass) {
        String written = regclass.toString();
        int dot = written.lastIndexOf('.');
        if (dot > 0) return written.substring(0, dot).replace("\"", "");
        Integer oid = null;
        if (regclass instanceof Number) oid = Integer.valueOf(((Number) regclass).intValue());
        else if (regclass instanceof RegclassValue) oid = Integer.valueOf(((RegclassValue) regclass).oid());
        if (oid != null) {
            for (java.util.Map.Entry<String, Integer> e
                    : executor.systemCatalog.getOidMap().entrySet()) {
                if (e.getValue().equals(oid) && e.getKey().startsWith("rel:")) {
                    String key = e.getKey().substring(4);
                    int at = key.lastIndexOf('.');
                    return at > 0 ? key.substring(0, at) : null;
                }
            }
        }
        return null;
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        // Try metadata/introspection functions first
        Object metaResult = metadataFunctions.eval(name, fn, ctx);
        if (metaResult != NOT_HANDLED) return metaResult;

        Object privResult = privilegeFunctions.eval(name, fn, ctx);
        if (privResult != NOT_HANDLED) return privResult;

        switch (name) {
            case "pg_collation_for": {
                // The collation an expression carries, written the way PostgreSQL writes it. A
                // literal with no type of its own is of type unknown, which has no collation at
                // all and answers NULL; anything else collatable carries the default.
                Expression arg = fn.args().isEmpty() ? null : fn.args().get(0);
                if (arg instanceof CollateExpr) {
                    return "\"" + ((CollateExpr) arg).collation() + "\"";
                }
                if (arg instanceof Literal) {
                    Literal.LiteralType kind = ((Literal) arg).literalType();
                    if (kind == Literal.LiteralType.STRING || kind == Literal.LiteralType.NULL) {
                        return null;
                    }
                }
                return "\"default\"";
            }
            case "pg_typeof": {
                Expression rawExpr = fn.args().get(0);
                // A call that was folded before this expression was reached is still written here
                // as the call it is, so what it came to is asked for rather than read off the node.
                ExprEvaluator.PrecomputedValueExpr alreadyFolded =
                        executor.exprEvaluator.foldedFor(rawExpr);
                if (alreadyFolded != null) rawExpr = alreadyFolded;
                // Whatever this method ends up reading the value for, it reads it once. Asking
                // twice ran a function with an effect twice: pg_typeof(lo_unlink(x)) unlinked the
                // object and then complained that no such object existed.
                OnceEvaluated value = new OnceEvaluated(rawExpr, ctx);

                // An expression already folded to a value -- a window call resolved over its
                // partition, an aggregate over its group -- carries the type its own expression
                // was declared to have. That declaration is exactly what pg_typeof asks for, and
                // it is the only answer available when the fold produced NULL.
                //
                // It answers only where the value cannot: a numeric NaN and a float8 NaN are the
                // same Double, so the declaration decides between them. Where the declaration
                // contradicts the value outright -- an aggregate this engine has no declared
                // result type for is guessed as text, while it computed a number -- the value is
                // the better witness and the guess is discarded.
                if (rawExpr instanceof ExprEvaluator.PrecomputedValueExpr) {
                    ExprEvaluator.PrecomputedValueExpr pre =
                            (ExprEvaluator.PrecomputedValueExpr) rawExpr;
                    DataType declared = pre.declaredType();
                    if (declared != null
                            && (pre.value() == null || agreesWithValue(declared, pre.value()))) {
                        return pgTypeDisplayName(declared);
                    }
                }

                // An LSN is carried as its own text -- "0/0" says nothing about the type that
                // produced it -- so the call's declared result type is the only witness.
                if (rawExpr instanceof FunctionCallExpr
                        && "pg_logical_emit_message".equals(FunctionEvaluator.stripSchemaPrefix(
                                ((FunctionCallExpr) rawExpr).name()
                                        .toLowerCase(java.util.Locale.ROOT)))) {
                    return "pg_lsn";
                }

                // CURRENT_TIME is a keyword and not a catalogued routine, and the timetz it
                // answers with is carried as its own printed text -- which says nothing but text.
                if (rawExpr instanceof FunctionCallExpr
                        && "current_time".equals(FunctionEvaluator.stripSchemaPrefix(
                                ((FunctionCallExpr) rawExpr).name()
                                        .toLowerCase(java.util.Locale.ROOT)))) {
                    return "time with time zone";
                }

                // Check if this is a system column reference (ctid, xmin, xmax, cmin, cmax, tableoid)
                if (rawExpr instanceof ColumnRef && ctx != null) {
                    ColumnRef colRef = (ColumnRef) rawExpr;
                    String colName = colRef.column().toLowerCase(java.util.Locale.ROOT);
                    switch (colName) {
                        case "ctid": return "tid";
                        case "xmin": case "xmax": return "xid";
                        case "cmin": case "cmax": return "cid";
                        case "tableoid": return "oid";
                    }
                    Column colDef = ctx.resolveColumnDef(colRef.table(), colRef.column());
                    if (colDef != null) {
                        // An array of a type the reader defined is a type of its own, and it is the
                        // one the column was declared with. Answering with the element's name said
                        // the column holds one value of that type where it holds a list of them.
                        String userElement =
                                CatalogHelper.arrayOfUserType(executor.database, colDef);
                        if (userElement != null) return typeDisplay(userElement) + "[]";
                        // A domain is a type of its own, and it is the type the column was
                        // declared with — so it is the name, not the base type it is built on and
                        // not the enum or composite that base may itself be.
                        if (colDef.getDomainTypeName() != null) return typeDisplay(colDef.getDomainTypeName());
                        if (colDef.getEnumTypeName() != null) return typeDisplay(colDef.getEnumTypeName());
                        if (colDef.getCompositeTypeName() != null) return typeDisplay(colDef.getCompositeTypeName());
                        if (colDef.getArrayElementType() != null)
                            return pgTypeDisplayName(colDef.getArrayElementType()) + "[]";
                        if (colDef.getType() == DataType.JSONB) return "jsonb";
                        if (colDef.getType() == DataType.JSON) return "json";
                        if (colDef.getType() != null && colDef.getType() != DataType.TEXT) {
                            return pgTypeDisplayName(colDef.getType());
                        }
                    }
                    // A relation built from a query carries what its own definition settled for
                    // each column, and that is the declaration pg_typeof asks for. Falling through
                    // to the value read the type off whatever the row happened to hold, so a
                    // column written NULL was called unknown -- which is a type no relation has.
                    String defined = definedColumnType(ctx, colRef);
                    if (defined != null && !defined.endsWith("[]")) return typeDisplay(defined);
                }

                // Subscripting an array yields one element of it, a range of one yields another
                // array, and only a json container yields json.
                if (rawExpr instanceof SubscriptExpr) {
                    SubscriptExpr sub = (SubscriptExpr) rawExpr;
                    if (ctx != null && sub.base() instanceof ColumnRef) {
                        ColumnRef base = (ColumnRef) sub.base();
                        Column baseDef = ctx.resolveColumnDef(base.table(), base.column());
                        // The element of an array of a type the reader defined is one value of
                        // that type, and a range of it is another array of it. Read off the
                        // representation the values are carried in, an element of an array of
                        // enums answered with the placeholder name "enum", which is a type
                        // nothing answers to.
                        String subElement = baseDef == null ? null
                                : CatalogHelper.arrayOfUserType(executor.database, baseDef);
                        if (subElement != null) {
                            return typeDisplay(subElement) + (sub.isSlice() ? "[]" : "");
                        }
                        if (baseDef != null && baseDef.getArrayElementType() != null) {
                            return pgTypeDisplayName(sub.isSlice()
                                    ? baseDef.getType() : baseDef.getArrayElementType());
                        }
                    }
                    String arrayType = executor.binaryOpEvaluator
                            .declaredTypeForResolution(sub.base(), ctx);
                    if (arrayType != null && arrayType.endsWith("[]")) {
                        if (sub.isSlice()) return arrayType;
                        DataType element = DataType.fromPgName(arrayType
                                .substring(0, arrayType.length() - 2)
                                .toLowerCase(java.util.Locale.ROOT).replaceAll("\\(.*\\)", "").trim());
                        if (element != null) return pgTypeDisplayName(element);
                    }
                    DataType inferred = executor.exprEvaluator.inferExprType(rawExpr);
                    if (inferred != null) return pgTypeDisplayName(inferred);
                }

                // Subscripting an array yields one element of it, so the answer is the array's
                // element type — jsonb only where the thing subscripted really was a jsonb.
                if (rawExpr instanceof BinaryExpr && ctx != null
                        && ((BinaryExpr) rawExpr).op() == BinaryExpr.BinOp.JSON_SUBSCRIPT
                        && ((BinaryExpr) rawExpr).left() instanceof ColumnRef) {
                    ColumnRef base = (ColumnRef) ((BinaryExpr) rawExpr).left();
                    Column baseDef = ctx.resolveColumnDef(base.table(), base.column());
                    if (baseDef != null && baseDef.getArrayElementType() != null) {
                        return pgTypeDisplayName(baseDef.getArrayElementType());
                    }
                }
                // The same for an array the statement built rather than a column of one: only a
                // column was read, so every other subscript answered jsonb whatever it was of.
                if (rawExpr instanceof BinaryExpr
                        && ((BinaryExpr) rawExpr).op() == BinaryExpr.BinOp.JSON_SUBSCRIPT) {
                    String arrayType = executor.binaryOpEvaluator
                            .declaredTypeForResolution(((BinaryExpr) rawExpr).left(), ctx);
                    if (arrayType != null && arrayType.endsWith("[]")) {
                        DataType element = DataType.fromPgName(arrayType
                                .substring(0, arrayType.length() - 2)
                                .toLowerCase(java.util.Locale.ROOT).replaceAll("\\(.*\\)", "").trim());
                        if (element != null) return pgTypeDisplayName(element);
                    }
                }
                if (rawExpr instanceof BinaryExpr
                        && (((BinaryExpr) rawExpr).op() == BinaryExpr.BinOp.JSON_ARROW
                            || ((BinaryExpr) rawExpr).op() == BinaryExpr.BinOp.JSON_SUBSCRIPT)) {
                    return "jsonb";
                }

                // A range the reader defined has no DataType of its own, and its constructor hands
                // back the text the range prints as, so the call's own name is the only thing left
                // that says which type produced the value.
                if (rawExpr instanceof FunctionCallExpr) {
                    String called = FunctionEvaluator.stripSchemaPrefix(
                            ((FunctionCallExpr) rawExpr).name().toLowerCase(java.util.Locale.ROOT));
                    if (executor.database.isRangeType(called)) return typeDisplay(called);
                }

                // A range, a shape, a document and a bit string are all carried as their own
                // text, so the value that comes back cannot say which type produced it -- and
                // pg_typeof is a question about the declaration in the first place. Only the
                // types whose value is indistinguishable from text are answered this way; the
                // rest keep the value-based reading, which knows a bigint from an integer.
                if (rawExpr instanceof BinaryExpr || rawExpr instanceof FunctionCallExpr) {
                    DataType inferred = executor.exprEvaluator.inferExprType(rawExpr);
                    if (isTextCarriedType(inferred)) {
                        DataType element = DataType.elementOf(inferred);
                        return element != null ? pgTypeDisplayName(element) + "[]"
                                : pgTypeDisplayName(inferred);
                    }
                }

                // An oid is carried as a plain number and a registry type as a plain name, so
                // neither value says which type produced it -- lo_creat answered a bigint and
                // to_regclass a text. The declaration is the only witness there is.
                if (rawExpr instanceof FunctionCallExpr) {
                    DataType declared = declaredResultType((FunctionCallExpr) rawExpr, ctx);
                    if (isOidCarriedType(declared)) return pgTypeDisplayName(declared);
                }

                // A routine the reader wrote answers with the type it was declared to answer
                // with, and the value cannot always say which that is: a character(n) is carried
                // as the padded text it prints as, which says only text.
                if (rawExpr instanceof FunctionCallExpr) {
                    PgFunction wrote = executor.database.getFunction(
                            FunctionEvaluator.stripSchemaPrefix(((FunctionCallExpr) rawExpr).name()
                                    .toLowerCase(java.util.Locale.ROOT)));
                    if (wrote != null && wrote.getReturnType() != null
                            && !wrote.isSetReturning()) {
                        String declaredName = wrote.getReturnType().trim();
                        if (!PolymorphicTypes.isPolymorphic(declaredName)) {
                            String bare = declaredName.replaceAll("\\(.*\\)", "").trim();
                            DataType named = DataType.fromPgName(bare);
                            return named != null ? pgTypeDisplayName(named) : typeDisplay(bare);
                        }
                    }
                }

                // A call that answered nothing has no value to read a type off, and its
                // signature is then the only witness there is: pg_column_toast_chunk_id answers
                // an oid whether or not the value it was asked about has one.
                if (rawExpr instanceof FunctionCallExpr) {
                    FunctionCallExpr call = (FunctionCallExpr) rawExpr;
                    String called = FunctionEvaluator.stripSchemaPrefix(
                            call.name().toLowerCase(java.util.Locale.ROOT));
                    if (BuiltinCallTypes.records(called) && value.get() == null) {
                        DataType declared = declaredResultType(call, ctx);
                        if (declared != null) return pgTypeDisplayName(declared);
                    }
                }

                // An empty array says nothing about what it would have held, so the call's own
                // declaration decides: pg_blocking_pids answers an integer array whether or not
                // anything is blocking.
                if (rawExpr instanceof FunctionCallExpr && value.get() instanceof java.util.List
                        && ((java.util.List<?>) value.get()).isEmpty()) {
                    DataType declared = declaredResultType((FunctionCallExpr) rawExpr, ctx);
                    if (declared != null && DataType.elementOf(declared) != null) {
                        return pgTypeDisplayName(declared);
                    }
                }

                if (rawExpr instanceof CastExpr) {
                    CastExpr cast = (CastExpr) rawExpr;
                    // float(p) names two different types depending on p, so the modifier is
                    // offered whole before it is stripped as a mere width.
                    DataType withModifier =
                            DataType.fromPgName(cast.typeName().toLowerCase(java.util.Locale.ROOT).trim());
                    if (withModifier != null && cast.typeName().indexOf('(') > 0) {
                        return pgTypeDisplayName(withModifier);
                    }
                    String tn = cast.typeName().toLowerCase(java.util.Locale.ROOT).replaceAll("\\(.*\\)", "").trim();
                    if (tn.endsWith("[]")) {
                        String baseType = tn.substring(0, tn.length() - 2).trim();
                        try {
                            return pgTypeDisplayName(DataType.fromPgName(baseType)) + "[]";
                        } catch (Exception e) { return tn; }
                    }
                    // A qualifier or a precision is part of the modifier, not of the type's
                    // name: INTERVAL '3' DAY is still just an interval.
                    if (IntervalTypmod.fromTypeSpec(tn) != null) return "interval";
                    try {
                        return pgTypeDisplayName(DataType.fromPgName(tn));
                    } catch (Exception e) { return tn; }
                }
                if (rawExpr instanceof AtTimeZoneExpr) {
                    // AT TIME ZONE / AT LOCAL swaps a value between the zoned and zoneless
                    // spelling of its type; a timetz is a formatted string, which the value-shape
                    // fallback below would otherwise report as text.
                    Object shifted = value.get();
                    if (shifted instanceof LocalDateTime) return "timestamp without time zone";
                    if (shifted instanceof OffsetDateTime) return "timestamp with time zone";
                    if (TypeCoercion.looksLikeTimeTz(shifted)) return "time with time zone";
                }
                if (rawExpr instanceof ArrayExpr && !((ArrayExpr) rawExpr).isRow() && !((ArrayExpr) rawExpr).elements().isEmpty()) {
                    ArrayExpr arrExpr = (ArrayExpr) rawExpr;
                    Expression firstElem = arrExpr.elements().get(0);
                    if (firstElem instanceof CastExpr) {
                        CastExpr elemCast = (CastExpr) firstElem;
                        String elemTypeName = elemCast.typeName().toLowerCase(java.util.Locale.ROOT).replaceAll("\\(.*\\)", "").trim();
                        if (executor.database.isCompositeType(elemTypeName)) {
                            return elemTypeName + "[]";
                        }
                        if (executor.database.isCustomEnum(elemTypeName)) {
                            return elemTypeName + "[]";
                        }
                    }
                    // An array is of whatever its elements are, where they agree on what that is.
                    // Reading only the composites and the enums left every other written element
                    // type answering text[]: an array of character was not one of character.
                    // Elements that disagree are left to the widening the values themselves show.
                    String arrayType = executor.binaryOpEvaluator
                            .declaredTypeForResolution(rawExpr, ctx);
                    if (arrayType != null && arrayType.endsWith("[]")) {
                        String elementName = arrayType.substring(0, arrayType.length() - 2)
                                .toLowerCase(java.util.Locale.ROOT).replaceAll("\\(.*\\)", "").trim();
                        DataType element = elementName.endsWith("[]") ? null
                                : DataType.fromPgName(elementName);
                        if (element != null && DataType.elementOf(element) == null) {
                            return pgTypeDisplayName(element) + "[]";
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
                        Object caseResult = value.get();
                        if (caseResult == null) return nonNullType;
                    }
                }

                if (rawExpr instanceof FunctionCallExpr) {
                    FunctionCallExpr rawFn = (FunctionCallExpr) rawExpr;
                    String rawFnName = rawFn.name().toLowerCase(java.util.Locale.ROOT);
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

                Object arg = value.get();

                if (arg instanceof AstExecutor.PgRow) return "record";

                if (arg == null) {
                    // A polymorphic routine's result type comes from its arguments, so it is known
                    // even when the call returned NULL and there is no value to read it off.
                    if (rawExpr instanceof FunctionCallExpr && isPolymorphicUserFunction((FunctionCallExpr) rawExpr)) {
                        DataType inferred = executor.exprEvaluator.inferTypeFromContext(
                                rawExpr, ctx != null ? ctx.getBindings()
                                        : new ArrayList<RowContext.TableBinding>());
                        if (inferred != null) return pgTypeDisplayName(inferred);
                    }
                    // abs(NULL::bigint) is still a bigint: the overload the call resolved to is
                    // what names the type, not the value it happened to produce.
                    if (rawExpr instanceof FunctionCallExpr) {
                        DataType inferred = executor.exprEvaluator.inferTypeFromContext(
                                rawExpr, ctx != null ? ctx.getBindings()
                                        : new ArrayList<RowContext.TableBinding>());
                        if (isNumericType(inferred)) return pgTypeDisplayName(inferred);
                    }
                    return "unknown";
                }

                // NaN and the infinities are carried as Doubles whatever type produced them, so
                // the expression's own type is what tells a numeric NaN from a float8 one.
                if (NumericLimits.isSpecial(arg)) {
                    DataType inferred = executor.exprEvaluator.inferTypeFromContext(
                            rawExpr, ctx != null ? ctx.getBindings()
                                    : new ArrayList<RowContext.TableBinding>());
                    if (inferred == DataType.NUMERIC) return "numeric";
                }

                // A JSON document is carried as its own text, so an object's braces read as an
                // array literal and nothing in the value tells json from jsonb. The expression's
                // declared type is the only witness there is; it overrules nothing else.
                {
                    DataType json = executor.exprEvaluator.inferTypeFromContext(
                            rawExpr, ctx != null ? ctx.getBindings()
                                    : new ArrayList<RowContext.TableBinding>());
                    if (json == DataType.JSON || json == DataType.JSONB) {
                        return pgTypeDisplayName(json);
                    }
                    // The name of a text search configuration is carried as that name, so the
                    // value cannot say it is one; only the call that produced it can.
                    if (json == DataType.REGCONFIG) return "regconfig";
                }
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
                    if (rawExpr instanceof CastExpr && ((CastExpr) rawExpr).typeName().toLowerCase(java.util.Locale.ROOT).endsWith("[]")) {
                        CastExpr cast2 = (CastExpr) rawExpr;
                        String base = cast2.typeName().toLowerCase(java.util.Locale.ROOT).replace("[]", "").trim();
                        try { return pgTypeDisplayName(DataType.fromPgName(base)) + "[]"; } catch (Exception e) { /* fall through */ }
                    }
                    return "text[]";
                }
                if (arg instanceof BigDecimal) return "numeric";
                DataType dt = TypeCoercion.inferType(arg);
                return dt != null ? pgTypeDisplayName(dt) : arg.getClass().getSimpleName().toLowerCase(java.util.Locale.ROOT);
            }
            case "current_database":
            case "current_catalog":
                return executor.session != null ? executor.session.getDatabaseName() : "memgres";
            case "current_schema":
                // The schema a name would resolve in, which is nothing at all when the path
                // names no schema that exists.
                return executor.session != null ? executor.session.getReportedSchema() : "public";
            case "current_schemas": {
                boolean includeImplicit = false;
                if (!fn.args().isEmpty()) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    includeImplicit = arg instanceof Boolean ? ((Boolean) arg) : "true".equalsIgnoreCase(String.valueOf(arg));
                }
                if (executor.session != null) {
                    return new java.util.ArrayList<Object>(
                            executor.session.getExistingSearchPath(includeImplicit));
                }
                List<Object> schemas = new java.util.ArrayList<>();
                if (includeImplicit) schemas.add("pg_catalog");
                schemas.add("public");
                return schemas;
            }
            case "user":
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
            case "pg_my_temp_schema": {
                // The OID of this session's temp namespace, or 0 when it has never made one
                if (executor.session == null) return 0;
                String tempSchema = executor.session.getTempSchemaName();
                if (executor.database.getSchema(tempSchema) == null) return 0;
                return executor.systemCatalog.getOid("ns:" + tempSchema);
            }
            case "pg_is_other_temp_schema": {
                if (fn.args().isEmpty()) return false;
                Object nsArg = executor.evalExpr(fn.args().get(0), ctx);
                if (nsArg == null) return null;
                int nsOid = executor.toInt(nsArg);
                if (nsOid == 0) return false;
                String mine = executor.session != null ? executor.session.getTempSchemaName() : null;
                for (Map.Entry<String, Integer> e : executor.systemCatalog.getOidMap().entrySet()) {
                    if (e.getValue() == null || e.getValue() != nsOid) continue;
                    if (!e.getKey().startsWith("ns:")) continue;
                    String ns = e.getKey().substring(3);
                    return ns.toLowerCase(java.util.Locale.ROOT).startsWith("pg_temp") && !ns.equalsIgnoreCase(mine);
                }
                return false;
            }
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
                return false;  // Return Java boolean so CASE WHEN evaluates correctly
            case "pg_is_wal_replay_paused":
                return false;
            case "pg_cancel_backend":
            case "pg_terminate_backend": {
                // memgres has no other backend to signal, so the answer is false whatever the
                // argument says — but a process ID is still an integer, and one that is not gets
                // the same refusal here as it would anywhere else.
                if (!fn.args().isEmpty()) executor.toInt(executor.evalExpr(fn.args().get(0), ctx));
                return false;
            }
            case "pg_drop_replication_slot": {
                if (!fn.args().isEmpty()) {
                    String slotName = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                    executor.database.removeReplicationSlot(slotName);
                }
                return null;  // void function
            }
            case "pg_reload_conf":
                return true;
            case "pg_rotate_logfile":
                return false;
            case "pg_sleep": {
                if (!fn.args().isEmpty()) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    if (arg != null) {
                        // How long to sleep for is a number of seconds. Sleeping for no time at
                        // all when it was not one accepted an argument the parameter cannot take.
                        long millis = (long) (executor.exprEvaluator.toDouble(arg) * 1000);
                        if (millis > 0) {
                            try {
                                Thread.sleep(millis);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                // The sleep was broken by a timeout or a cancel request; report whichever it was.
                                throw StatementCancel.canceled();
                            }
                        }
                    }
                }
                return VOID_RESULT;
            }
            case "pg_sleep_for": {
                if (!fn.args().isEmpty()) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    if (arg != null && !(arg instanceof PgInterval)) PgInterval.parse(arg.toString());
                }
                return VOID_RESULT;
            }
            case "pg_sleep_until": {
                if (!fn.args().isEmpty()) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    if (arg != null) executor.castEvaluator.applyCast(arg, "timestamptz");
                }
                return VOID_RESULT;
            }
            case "pg_blocking_pids": {
                if (!fn.args().isEmpty()) executor.toInt(executor.evalExpr(fn.args().get(0), ctx));
                return Cols.listOf();
            }
            case "pg_export_snapshot":
                return executor.database.exportSnapshot();
            case "pg_stat_clear_snapshot": {
                return VOID_RESULT;
            }
            case "pg_stat_reset": {
                return VOID_RESULT;
            }
            case "pg_stat_reset_shared": {
                if (!fn.args().isEmpty()) {
                    Object arg = executor.evalExpr(fn.args().get(0), ctx);
                    String target = arg != null ? arg.toString().toLowerCase(java.util.Locale.ROOT) : "";
                    java.util.Set<String> validTargets = new java.util.HashSet<>(java.util.Arrays.asList(
                            "archiver", "bgwriter", "checkpointer", "io", "recovery_prefetch",
                            "slru", "wal"));
                    if (!validTargets.contains(target)) {
                        MemgresException e = new MemgresException(
                                "unrecognized reset target: \"" + target + "\"", "22023");
                        e.setHint("Target must be \"archiver\", \"bgwriter\", \"checkpointer\","
                                + " \"io\", \"recovery_prefetch\", \"slru\", or \"wal\".");
                        throw e;
                    }
                }
                return VOID_RESULT;
            }
            case "pg_stat_reset_single_table_counters":
            case "pg_stat_reset_single_function_counters": {
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                return VOID_RESULT;
            }
            case "txid_current":
                return executor.session != null ? executor.session.getTransactionId() : (long) (System.nanoTime() / 1000);
            case "pg_indexam_has_property": {
                Object amOid = fn.args().size() > 0 ? executor.evalExpr(fn.args().get(0), ctx) : 0;
                Object propArg = fn.args().size() > 1 ? executor.evalExpr(fn.args().get(1), ctx) : "";
                String prop = String.valueOf(propArg).toLowerCase(java.util.Locale.ROOT);
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
                // A tablespace inside the data directory has no path of its own, which is the
                // empty string; an OID that names no tablespace has no directory to look in at
                // all, and PostgreSQL says so rather than answering as though it found one.
                if (fn.args().isEmpty()) return null;
                Object spcArg = executor.evalExpr(fn.args().get(0), ctx);
                if (spcArg == null) return null;
                int spcOid = executor.toInt(spcArg);
                if (spcOid != 1663 && spcOid != 1664) {
                    throw new MemgresException("could not stat file \"pg_tblspc/" + spcOid
                            + "\": No such file or directory", "58P01");
                }
                return "";
            }
            case "current_setting": {
                Object askedFor = executor.evalExpr(fn.args().get(0), ctx);
                // No parameter was named, so there is no setting to report — which is nothing,
                // not a parameter called "null".
                if (askedFor == null) return null;
                String setting = String.valueOf(askedFor);
                // current_setting reports the display form, the same text SHOW gives: a
                // parameter counted in a unit reads back as "4MB", not as its 4096 kB.
                String value = null;
                if (executor.session != null) {
                    value = executor.session.getGucSettings().getForDisplay(setting);
                }
                if (value == null) {
                    value = new GucSettings().getForDisplay(setting);
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
                    Object namedAs = executor.evalExpr(fn.args().get(0), ctx);
                    // A parameter that is nothing is no parameter to set, and PostgreSQL says so
                    // rather than looking up a parameter called "null".
                    if (namedAs == null) {
                        throw new MemgresException("SET requires parameter name", "22004");
                    }
                    String settingName = String.valueOf(namedAs);
                    // set_config is SET written as a function call, so it is judged by the same
                    // rules: an unrecognized parameter is refused rather than invented, and one
                    // that cannot be changed at run time says so with the same SQLSTATE.
                    GucSettings.requireKnown(settingName);
                    Object valueGiven = executor.evalExpr(fn.args().get(1), ctx);
                    // A value that is nothing puts the parameter back to its default, which is
                    // what RESET does; stringified to "null" it was refused as a bad value.
                    if (valueGiven == null) {
                        if (executor.session != null) {
                            executor.session.getGucSettings().reset(settingName);
                            return executor.session.getGucSettings().getForDisplay(settingName);
                        }
                        return null;
                    }
                    String settingValue = String.valueOf(valueGiven);
                    GucSettings.checkAssignable(settingName, settingValue);
                    // And by the same value checks: a time zone nobody has, an encoding that is
                    // not one and a role that does not exist are refused here as they are there.
                    executor.sessionExecutor.validateGucValue(settingName, settingValue);
                    boolean isLocal = fn.args().size() >= 3 && executor.isTruthy(executor.evalExpr(fn.args().get(2), ctx));
                    // set_config runs inside a query, so a transaction-scoped setting is subject
                    // to the same rules the SET statement is: the isolation level can no longer
                    // be chosen once this transaction has taken a snapshot, and a setting that
                    // belongs to a transaction does not stick when there is no transaction open.
                    if (executor.session != null && SessionExecutor.isTransactionScopedGuc(settingName)) {
                        String lower = settingName.toLowerCase(java.util.Locale.ROOT);
                        if (lower.equals("transaction_isolation") || lower.equals("transaction_deferrable")) {
                            throw new MemgresException("SET TRANSACTION "
                                    + (lower.equals("transaction_isolation") ? "ISOLATION LEVEL" : "[NOT] DEFERRABLE")
                                    + " must be called before any query", "25001");
                        }
                        if (!executor.session.isInTransaction()) {
                            executor.session.addNotice("WARNING", "25P01",
                                    "SET TRANSACTION can only be used in transaction blocks", null);
                            return settingValue;
                        }
                    }
                    if (executor.session != null) {
                        if (isLocal) {
                            // M13: set_config(..., true) is LOCAL — no-op outside txn
                            if (executor.session.isInTransaction()) {
                                executor.session.getGucSettings().setLocal(settingName, settingValue);
                            }
                            // Outside txn: silently ignored (PG behavior)
                        } else {
                            executor.session.getGucSettings().set(settingName, settingValue);
                        }
                        // What comes back is what the parameter now holds, which is not always
                        // what was written: a boolean set to "0" reads back as "off".
                        String stored =
                                executor.session.getGucSettings().getForDisplay(settingName);
                        if (stored != null) return stored;
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
                // Visible means reachable by its bare name, which is what the search path decides.
                // Answering true for everything told a client that two same-named tables in two
                // schemas were both reachable unqualified, which no search path can make true.
                Object tableOid = executor.evalExpr(fn.args().get(0), ctx);
                if (tableOid == null) return null;
                String schema = schemaOfRelation(tableOid);
                if (schema == null) return true;
                for (String onPath : executor.searchPathSchemas()) {
                    if (onPath.equalsIgnoreCase(schema)) return true;
                }
                return false;
            }
            case "pg_database_size": {
                // A size is asked about a database, so the database has to be there. Answering
                // with a size whatever the argument named let a typo report a database that
                // does not exist as one holding eight kilobytes.
                if (fn.args().isEmpty()) return null;
                Object dbArg = executor.evalExpr(fn.args().get(0), ctx);
                if (dbArg == null) return null;
                if (dbArg instanceof Number) {
                    int dbOid = ((Number) dbArg).intValue();
                    if (dbOid != executor.systemCatalog.getOid("db:memgres")) {
                        throw new MemgresException(
                                "database with OID " + dbOid + " does not exist", "42704");
                    }
                } else if (executor.session != null
                        && !String.valueOf(dbArg).equals(executor.session.getDatabaseName())) {
                    throw new MemgresException(
                            "database \"" + dbArg + "\" does not exist", "3D000");
                }
                return 8192L;
            }
            case "pg_relation_size":
            case "pg_total_relation_size":
            case "pg_table_size":
            case "pg_indexes_size": {
                // The argument is a regclass, so a name that no relation answers to is refused
                // where it is read; an OID that names nothing is not an error, it is no size.
                if (fn.args().isEmpty()) return null;
                Object relArg = executor.evalExpr(fn.args().get(0), ctx);
                if (relArg == null) return null;
                if (relArg instanceof Number) {
                    return executor.systemCatalog.keyForOid(((Number) relArg).intValue()) == null
                            ? null : 8192L;
                }
                executor.castEvaluator.applyCast(relArg, "regclass");
                return 8192L;
            }
            case "pg_column_toast_chunk_id": {
                // The chunk id of a value stored out of line, and null for one that is not.
                // memgres holds every value with its row, so no value has one -- which is also
                // PostgreSQL's answer for anything short enough not to have been moved out.
                requireArgs(fn, 1);
                executor.evalExpr(fn.args().get(0), ctx);
                return null;
            }
            case "pg_column_size": {
                if (!fn.args().isEmpty()) {
                    Object val = executor.evalExpr(fn.args().get(0), ctx);
                    if (val == null) return 0;
                    // Compute payload size in bytes + varlena header (4 bytes)
                    int payloadBytes;
                    if (val instanceof byte[]) {
                        payloadBytes = ((byte[]) val).length;
                    } else if (val instanceof String) {
                        try {
                            payloadBytes = ((String) val).getBytes("UTF-8").length;
                        } catch (java.io.UnsupportedEncodingException e) {
                            payloadBytes = ((String) val).length();
                        }
                    } else if (val instanceof Number) {
                        if (val instanceof Integer || val instanceof Short) payloadBytes = 4;
                        else if (val instanceof Long) payloadBytes = 8;
                        else if (val instanceof Float) payloadBytes = 4;
                        else if (val instanceof Double) payloadBytes = 8;
                        else payloadBytes = val.toString().length();
                    } else {
                        payloadBytes = val.toString().length();
                    }
                    return payloadBytes + 4;
                }
                return 0;
            }
            case "pg_size_pretty": {
                if (fn.args().isEmpty()) return "0 bytes";
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                return sizePretty(arg);
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
            case "pg_logical_emit_message": {
                // Writes a logical-decoding message into the WAL and answers with the LSN it was
                // written at. memgres keeps no WAL, so the call is accepted, its arguments are
                // evaluated for their own errors, and the answer is the same zero LSN
                // pg_current_wal_lsn gives. Both overloads -- a text payload and a bytea one --
                // and the optional trailing flush flag are the same call here.
                if (fn.args().size() < 3 || fn.args().size() > 4) {
                    throw new MemgresException("function " + fn.name()
                            + "(" + writtenArgTypes(fn, ctx) + ") does not exist"
                            + "\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
                }
                boolean anyNull = false;
                for (Expression emitArg : fn.args()) {
                    if (executor.evalExpr(emitArg, ctx) == null) anyNull = true;
                }
                // Declared strict, so a NULL anywhere in the call answers NULL.
                return anyNull ? null : "0/0";
            }
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
                if (!executor.database.getLargeObjectStore().exists(loid)) {
                    executor.database.getLargeObjectStore().loFromBytea(loid, new byte[0]);
                }
                executor.database.getLargeObjectStore().loPut(loid, offset, data);
                // Declared RETURNS void, which PG renders as an empty value and not as a NULL.
                return "";
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
            // The 64-bit spellings a driver reaches for on an object larger than two gigabytes.
            // Without them a client that asked for one was told the function did not exist.
            case "lo_tell64": {
                requireArgs(fn, 1);
                Object fdArg = executor.evalExpr(fn.args().get(0), ctx);
                return (long) executor.database.getLargeObjectStore()
                        .loTell(((Number) fdArg).intValue());
            }
            case "lo_lseek64": {
                requireArgs(fn, 3);
                Object fdArg = executor.evalExpr(fn.args().get(0), ctx);
                Object offArg = executor.evalExpr(fn.args().get(1), ctx);
                Object whenceArg = executor.evalExpr(fn.args().get(2), ctx);
                return (long) executor.database.getLargeObjectStore().loLseek(
                        ((Number) fdArg).intValue(), ((Number) offArg).intValue(),
                        ((Number) whenceArg).intValue());
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
            case "pg_client_encoding":
                return "UTF8";
            case "pg_current_xact_id": {
                // Returns a bigint transaction ID; stable within a transaction
                return executor.session != null ? executor.session.getTransactionId() : (long) (System.nanoTime() / 1000);
            }
            case "pg_current_xact_id_if_assigned":
            case "txid_current_if_assigned": {
                // Returns null if no transaction is currently active
                if (executor.session != null && executor.session.isInTransaction()) {
                    return executor.session.getTransactionId();
                }
                return null;
            }
            case "pg_current_snapshot": {
                // Returns a text snapshot string like '100:100:'
                long xid = System.nanoTime() / 1000;
                return xid + ":" + xid + ":";
            }
            case "txid_status":
            case "pg_xact_status": {
                // pg_xact_status(xid8) — PG requires xid8 type, rejects text
                requireArgs(fn, 1);
                Expression arg0 = fn.args().get(0);
                if (arg0 instanceof CastExpr) {
                    String castType = ((CastExpr) arg0).typeName().toLowerCase(java.util.Locale.ROOT);
                    if (castType.equals("text") || castType.equals("varchar") || castType.equals("character varying")) {
                        throw new MemgresException(
                                "function pg_xact_status(text) does not exist", "42883");
                    }
                }
                Object xidArg = executor.evalExpr(arg0, ctx);
                if (xidArg == null) return null;
                // If the xid matches the current transaction's xid, it's in progress.
                // This works both in explicit transactions and autocommit (where txid_current()
                // allocates a txid for the current statement).
                if (executor.session != null) {
                    long currentXid = executor.session.getTransactionId();
                    long queryXid;
                    if (xidArg instanceof Number) {
                        queryXid = ((Number) xidArg).longValue();
                    } else {
                        queryXid = Long.parseLong(xidArg.toString());
                    }
                    if (queryXid == currentXid) {
                        return "in progress";
                    }
                }
                return "committed";
            }
            case "pg_visible_in_snapshot": {
                // pg_visible_in_snapshot(xid, snapshot) → returns boolean
                requireArgs(fn, 2);
                Object xidArg = executor.evalExpr(fn.args().get(0), ctx);
                Object snapArg = executor.evalExpr(fn.args().get(1), ctx);
                if (xidArg == null || snapArg == null) return null;
                // Parse snapshot string 'xmin:xmax:xip_list'
                String snap = snapArg.toString();
                String[] parts = snap.split(":");
                long xid = ((Number) TypeCoercion.toLong(xidArg)).longValue();
                long xmin = Long.parseLong(parts[0].trim());
                long xmax = parts.length > 1 ? Long.parseLong(parts[1].trim()) : xmin;
                // xid is visible if it is < xmin (committed before snapshot)
                if (xid < xmin) return true;
                // xid is not visible if >= xmax (started after snapshot)
                if (xid >= xmax) return false;
                // Check if xid is in the in-progress list
                if (parts.length > 2 && !parts[2].isEmpty()) {
                    for (String xipStr : parts[2].split(",")) {
                        if (!xipStr.trim().isEmpty() && Long.parseLong(xipStr.trim()) == xid) {
                            return false; // in-progress, not visible
                        }
                    }
                }
                return true;
            }
            case "pg_snapshot_xmin": {
                // pg_snapshot_xmin(snapshot) → returns xmin from snapshot string
                requireArgs(fn, 1);
                Object snapArg = executor.evalExpr(fn.args().get(0), ctx);
                if (snapArg == null) return null;
                String[] parts = snapArg.toString().split(":");
                return Long.parseLong(parts[0].trim());
            }
            case "pg_snapshot_xmax": {
                // pg_snapshot_xmax(snapshot) → returns xmax from snapshot string
                requireArgs(fn, 1);
                Object snapArg = executor.evalExpr(fn.args().get(0), ctx);
                if (snapArg == null) return null;
                String[] parts = snapArg.toString().split(":");
                return parts.length > 1 ? Long.parseLong(parts[1].trim()) : Long.parseLong(parts[0].trim());
            }
            case "pg_snapshot_xip": {
                // pg_snapshot_xip(snapshot) → returns set of xids in progress
                requireArgs(fn, 1);
                Object snapArg = executor.evalExpr(fn.args().get(0), ctx);
                if (snapArg == null) return Cols.listOf();
                String snap = snapArg.toString();
                String[] parts = snap.split(":");
                List<Long> xips = new ArrayList<>();
                if (parts.length > 2 && !parts[2].isEmpty()) {
                    for (String xipStr : parts[2].split(",")) {
                        if (!xipStr.trim().isEmpty()) {
                            xips.add(Long.parseLong(xipStr.trim()));
                        }
                    }
                }
                return xips;
            }
            case "pg_size_bytes": {
                // pg_size_bytes(text) → parses size strings like '8 kB' to bytes
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String sizeStr = arg.toString().trim();
                return parseSizeBytes(sizeStr);
            }
            case "pg_tablespace_size": {
                // pg_tablespace_size(text|oid) → returns 0 (stub)
                if (!fn.args().isEmpty()) executor.evalExpr(fn.args().get(0), ctx);
                return 0L;
            }
            case "current_query": {
                // current_query() → returns the text of the currently executing query
                if (executor.session != null) {
                    String q = executor.session.getCurrentQuery();
                    if (q != null) return q;
                }
                return "";
            }
            case "pg_listening_channels": {
                // pg_listening_channels() → returns set of channels this session is listening on
                if (executor.session != null) {
                    return executor.database.getNotificationManager()
                            .getListeningChannels(executor.session);
                }
                return Cols.listOf();
            }
            case "pg_notification_queue_usage": {
                // pg_notification_queue_usage() → returns 0.0 (stub)
                return 0.0;
            }
            default:
                return NOT_HANDLED;
        }
    }

    // ---- pg_size_bytes parser ----

    /**
     * The bytes a size written as text stands for.
     *
     * <p>PostgreSQL reads the number with its own numeric scanner, so an exponent is a number
     * like any other, and it names the unit it could not read rather than only the whole string.
     * Matched against a hand-written pattern with no exponent, {@code 1e3} was a size nobody
     * could write, and a bad unit was reported without saying which unit was bad.
     */
    private static long parseSizeBytes(String sizeStr) {
        String text = sizeStr.trim();
        // The number runs while the characters could still be part of one; what is left is the
        // unit, which may be nothing at all.
        int at = 0;
        if (at < text.length() && (text.charAt(at) == '+' || text.charAt(at) == '-')) at++;
        int digitsFrom = at;
        while (at < text.length() && (Character.isDigit(text.charAt(at)) || text.charAt(at) == '.')) {
            at++;
        }
        if (at < text.length() && (text.charAt(at) == 'e' || text.charAt(at) == 'E')) {
            int exponentAt = at + 1;
            if (exponentAt < text.length()
                    && (text.charAt(exponentAt) == '+' || text.charAt(exponentAt) == '-')) {
                exponentAt++;
            }
            if (exponentAt < text.length() && Character.isDigit(text.charAt(exponentAt))) {
                at = exponentAt;
                while (at < text.length() && Character.isDigit(text.charAt(at))) at++;
            }
        }
        if (at == digitsFrom) {
            throw new MemgresException("invalid size: \"" + sizeStr + "\"", "22023");
        }
        java.math.BigDecimal number;
        try {
            number = new java.math.BigDecimal(text.substring(0, at));
        } catch (NumberFormatException notANumber) {
            throw new MemgresException("invalid size: \"" + sizeStr + "\"", "22023");
        }
        String unit = text.substring(at).trim();
        int shift = unitShift(unit);
        if (shift < 0) {
            MemgresException e = new MemgresException(
                    "invalid size: \"" + sizeStr + "\"", "22023");
            e.setDetail("Invalid size unit: \"" + unit + "\".");
            e.setHint("Valid units are \"bytes\", \"B\", \"kB\", \"MB\", \"GB\", \"TB\","
                    + " and \"PB\".");
            throw e;
        }
        return number.multiply(java.math.BigDecimal.valueOf(1L << shift))
                .setScale(0, java.math.RoundingMode.HALF_UP).longValueExact();
    }

    /** How many bits a unit multiplies by, or -1 for a unit PostgreSQL does not read. */
    private static int unitShift(String unit) {
        if (unit.isEmpty()) return 0;
        switch (unit.toLowerCase(java.util.Locale.ROOT)) {
            case "b":
            case "byte":
            case "bytes": return 0;
            case "kb": return 10;
            case "mb": return 20;
            case "gb": return 30;
            case "tb": return 40;
            case "pb": return 50;
            default: return -1;
        }
    }

    /** True for the types a math routine can resolve to, where inference is reliable. */
    private static boolean isNumericType(DataType dt) {
        if (dt == null) return false;
        switch (dt) {
            case SMALLINT: case INTEGER: case BIGINT:
            case REAL: case DOUBLE_PRECISION: case NUMERIC:
                return true;
            default:
                return false;
        }
    }

    /**
     * The types PostgreSQL stores as their own text, so an operator's result value cannot say
     * which of them produced it. For these -- and only these -- pg_typeof answers from the
     * declared type rather than from the value, which still knows a bigint from an integer.
     */
    /** An expression whose value is computed at most once, however many readers ask for it. */
    private final class OnceEvaluated {
        private final Expression expr;
        private final RowContext ctx;
        private boolean evaluated;
        private Object value;

        OnceEvaluated(Expression expr, RowContext ctx) {
            this.expr = expr;
            this.ctx = ctx;
        }

        Object get() {
            if (!evaluated) {
                value = executor.evalExpr(expr, ctx);
                evaluated = true;
            }
            return value;
        }
    }

    /** The declared result type of a built-in call, read against the arguments it was written with. */
    private DataType declaredResultType(FunctionCallExpr call, RowContext ctx) {
        String called = FunctionEvaluator.stripSchemaPrefix(
                call.name().toLowerCase(java.util.Locale.ROOT));
        if (!BuiltinCallTypes.records(called)) return null;
        java.util.List<Expression> args = call.args() == null
                ? java.util.Collections.<Expression>emptyList() : call.args();
        int[] written = new int[args.size()];
        for (int i = 0; i < args.size(); i++) {
            // What the call answers with depends on what it was passed: abs of a bigint is a
            // bigint, and abs of nothing in particular is a float8.
            DataType argType = executor.exprEvaluator.inferExprType(args.get(i));
            written[i] = argType == null ? 0 : argType.getOid();
        }
        DataType resolved = DataType.fromOid(BuiltinCallTypes.resultType(called, written));
        if (resolved != null) return resolved;
        // A name with one signature answers with that signature's type, whatever it was passed.
        return DataType.fromOid(BuiltinCallTypes.soleResultType(called));
    }

    /**
     * A type whose value says nothing about itself: an oid under its own name or another is a
     * bare number, and a name is a bare string. Only the declaration tells them from an integer
     * and a text.
     */
    private static boolean isOidCarriedType(DataType t) {
        if (t == null) return false;
        switch (t) {
            case OID: case REGPROC: case REGCLASS: case REGTYPE: case OIDVECTOR: case NAME:
                return true;
            default:
                return false;
        }
    }

    private static boolean isTextCarriedType(DataType t) {
        if (t == null) return false;
        if (DataType.isArrayType(t)) return true;
        switch (t) {
            case INT4RANGE: case INT8RANGE: case NUMRANGE:
            case DATERANGE: case TSRANGE: case TSTZRANGE:
            case INT4MULTIRANGE: case INT8MULTIRANGE: case NUMMULTIRANGE:
            case DATEMULTIRANGE: case TSMULTIRANGE: case TSTZMULTIRANGE:
            case POINT: case LINE: case LSEG: case BOX: case PATH: case POLYGON: case CIRCLE:
            case JSON: case JSONB: case TSVECTOR: case TSQUERY:
            case BIT: case VARBIT: case INET: case CIDR:
            // void comes back as the empty string, which says nothing about the type that
            // produced it; only the declaration can tell pg_typeof that it was void.
            case VOID:
                return true;
            default:
                return false;
        }
    }

    /**
     * Whether a declared type is believable next to the value that was actually computed. Only
     * the coarse division is asked about — a number, a string, a boolean — because the finer
     * distinctions are exactly what the declaration is there to settle.
     */
    private static boolean agreesWithValue(DataType declared, Object value) {
        DataType actual = TypeCoercion.inferType(value);
        if (actual == null) return true;
        return isNumericType(declared) == isNumericType(actual)
                && isStringType(declared) == isStringType(actual)
                && (declared == DataType.BOOLEAN) == (actual == DataType.BOOLEAN);
    }

    private static boolean isStringType(DataType dt) {
        return dt == DataType.TEXT || dt == DataType.VARCHAR || dt == DataType.CHAR;
    }

    /** A type written the way a message names it: integer, not int. */
    static String readableTypeName(String written) {
        String bare = written.trim();
        String suffix = "";
        while (bare.endsWith("[]")) {
            bare = bare.substring(0, bare.length() - 2).trim();
            suffix = suffix + "[]";
        }
        int paren = bare.indexOf('(');
        if (paren > 0) bare = bare.substring(0, paren).trim();
        DataType type = DataType.fromPgName(bare.toLowerCase(java.util.Locale.ROOT));
        return type == null ? written : pgTypeDisplayName(type) + suffix;
    }

    public static String pgTypeDisplayName(DataType dt) {
        // An array type is named after its element with brackets after it, not by the catalogue
        // spelling that puts an underscore in front: pg_typeof answers regtype[], not _regtype.
        DataType element = DataType.elementOf(dt);
        if (element != null) return pgTypeDisplayName(element) + "[]";
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
            case INTERNAL_CHAR:
                // The one type PostgreSQL names with quotes, because char without them is the
                // blank-padded string type. Printing it bare named the wrong type.
                return "\"char\"";
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

    /**
     * A size written the way a reader reads it.
     *
     * <p>PostgreSQL does not change unit at the unit's own size: it stays in bytes until ten
     * kilobytes, in kilobytes until twenty, and so on, so a kibibyte reads as "1024 bytes" and
     * not as "1 kB". And it rounds to the nearest rather than truncating, so a size a hair under
     * a unit does not read as one less than it is.
     *
     * <p>A numeric argument keeps its fraction while it is still in bytes.
     */
    static String sizePretty(Object arg) {
        String[] units = {"bytes", "kB", "MB", "GB", "TB", "PB"};
        if (arg instanceof java.math.BigDecimal || arg instanceof Double || arg instanceof Float) {
            java.math.BigDecimal size = new java.math.BigDecimal(arg.toString());
            java.math.BigDecimal limit = java.math.BigDecimal.valueOf(10L * 1024);
            int unit = 0;
            while (unit < units.length - 1 && size.abs().compareTo(limit) >= 0) {
                size = size.divide(java.math.BigDecimal.valueOf(1024), 0,
                        java.math.RoundingMode.HALF_UP);
                unit++;
            }
            return size.stripTrailingZeros().toPlainString() + " " + units[unit];
        }
        long size = arg instanceof Number ? ((Number) arg).longValue() : 0L;
        int unit = 0;
        // The unit changes at ten of the next one, not at one of it, and goes on changing while
        // ten of the next still fit: a mebibyte reads as 1024 kB and a kibibyte as 1024 bytes.
        while (unit < units.length - 1 && Math.abs(size) >= 10L * 1024) {
            // Rounded to the nearest, away from zero, which is what PostgreSQL does.
            size = (size + (size < 0 ? -512 : 512)) / 1024;
            unit++;
        }
        return size + " " + units[unit];
    }

}
