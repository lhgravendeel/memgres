package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Evaluates binary expressions, extracted from AstExecutor.
 */
class BinaryOpEvaluator {

    private final AstExecutor executor;

    BinaryOpEvaluator(AstExecutor executor) {
        this.executor = executor;
    }

    static String getRangeTypeName(Expression expr) {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            String name = fn.name().toLowerCase();
            switch (name) {
                case "int4range":
                case "int8range":
                case "numrange":
                case "daterange":
                case "tsrange":
                case "tstzrange":
                    return name;
                default:
                    return null;
            }
        }
        return null;
    }

    static boolean isCastToType(Expression expr, String typeName) {
        if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            return cast.typeName().equalsIgnoreCase(typeName);
        }
        return false;
    }

    private static boolean isCastToTextType(Expression expr) {
        if (expr instanceof CastExpr) {
            String tn = ((CastExpr) expr).typeName().toLowerCase();
            return tn.equals("text") || tn.equals("varchar") || tn.startsWith("character varying");
        }
        return false;
    }

    private static boolean isConcatExprWithTextCast(Expression expr) {
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            if (bin.op() == BinaryExpr.BinOp.CONCAT) {
                return isCastToTextType(bin.left()) || isCastToTextType(bin.right())
                        || isConcatExprWithTextCast(bin.left()) || isConcatExprWithTextCast(bin.right());
            }
        }
        return false;
    }

    Object evalBinary(BinaryExpr bin, RowContext ctx) {
        // Short-circuit for AND/OR with three-valued logic
        if (bin.op() == BinaryExpr.BinOp.AND) {
            Object left = executor.evalExpr(bin.left(), ctx);
            // PG rejects non-boolean operands for AND (42804)
            if (left != null && left instanceof Number && !(left instanceof Boolean)) {
                throw new MemgresException("argument of AND must be type boolean, not type " + AstExecutor.pgTypeNameOf(left), "42804");
            }
            if (Boolean.FALSE.equals(left)) return false; // FALSE AND x = FALSE
            Object right = executor.evalExpr(bin.right(), ctx);
            if (right != null && right instanceof Number && !(right instanceof Boolean)) {
                throw new MemgresException("argument of AND must be type boolean, not type " + AstExecutor.pgTypeNameOf(right), "42804");
            }
            if (Boolean.FALSE.equals(right)) return false; // x AND FALSE = FALSE
            if (left == null || right == null) return null; // NULL AND TRUE = NULL
            return executor.isTruthy(left) && executor.isTruthy(right);
        }
        if (bin.op() == BinaryExpr.BinOp.OR) {
            Object left = executor.evalExpr(bin.left(), ctx);
            // PG rejects non-boolean operands for OR (42804)
            if (left != null && left instanceof Number && !(left instanceof Boolean)) {
                throw new MemgresException("argument of OR must be type boolean, not type " + AstExecutor.pgTypeNameOf(left), "42804");
            }
            if (executor.isTruthyStrict(left)) return true; // TRUE OR x = TRUE
            Object right = executor.evalExpr(bin.right(), ctx);
            if (right != null && right instanceof Number && !(right instanceof Boolean)) {
                throw new MemgresException("argument of OR must be type boolean, not type " + AstExecutor.pgTypeNameOf(right), "42804");
            }
            if (executor.isTruthyStrict(right)) return true; // x OR TRUE = TRUE
            if (left == null || right == null) return null; // NULL OR FALSE = NULL
            return false;
        }

        Object left = executor.evalExpr(bin.left(), ctx);
        Object right = executor.evalExpr(bin.right(), ctx);

        // Collation-aware string comparison for EQUAL/NOT_EQUAL
        if ((bin.op() == BinaryExpr.BinOp.EQUAL || bin.op() == BinaryExpr.BinOp.NOT_EQUAL)
                && left instanceof String && right instanceof String) {
            String collationName = null;
            if (bin.left() instanceof CollateExpr) collationName = ((CollateExpr) bin.left()).collation();
            else if (bin.right() instanceof CollateExpr) collationName = ((CollateExpr) bin.right()).collation();
            if (collationName != null) {
                Database.CollationDef collDef = executor.database.getCollation(collationName);
                if (collDef != null && !collDef.deterministic) {
                    // Non-deterministic collation: use case-insensitive comparison
                    boolean eq = ((String) left).equalsIgnoreCase((String) right);
                    return bin.op() == BinaryExpr.BinOp.EQUAL ? eq : !eq;
                }
            }
        }

        // json type (not jsonb) does not support || or - operators
        if (bin.op() == BinaryExpr.BinOp.CONCAT || bin.op() == BinaryExpr.BinOp.SUBTRACT) {
            if (isCastToType(bin.left(), "json") || isCastToType(bin.right(), "json")) {
                String opSym = bin.op() == BinaryExpr.BinOp.CONCAT ? "||" : "-";
                throw new MemgresException("operator does not exist: json " + opSym + " json", "42883");
            }
        }

        // hstore - ::text → key deletion (explicit text cast means "delete key", not "subtract hstore")
        // Without explicit cast, PG resolves untyped literal as hstore (same-type preference)
        if (bin.op() == BinaryExpr.BinOp.SUBTRACT && left instanceof HstoreValue
                && right instanceof String && isCastToTextType(bin.right())) {
            return ((HstoreValue) left).deleteKey((String) right);
        }

        // json type does not support LIKE operator
        if (bin.op() == BinaryExpr.BinOp.LIKE || bin.op() == BinaryExpr.BinOp.ILIKE) {
            if (isCastToType(bin.left(), "json")) {
                throw new MemgresException("operator does not exist: json ~~ unknown", "42883");
            }
            if (bin.left() instanceof FunctionCallExpr) {
                String fnName = ((FunctionCallExpr) bin.left()).name().toLowerCase();
                if (fnName.equals("row_to_json") || fnName.equals("to_json") || fnName.equals("json_build_object")
                        || fnName.equals("json_build_array") || fnName.equals("json_agg") || fnName.equals("json_object")) {
                    throw new MemgresException("operator does not exist: json ~~ unknown", "42883");
                }
            }
        }

        // When || has an operand explicitly cast to text, force text concatenation
        // to avoid heuristic array detection on strings like "{1,2}"
        if (bin.op() == BinaryExpr.BinOp.CONCAT && left != null && right != null
                && !(left instanceof byte[]) && !(right instanceof byte[])
                && !(left instanceof List) && !(right instanceof List)
                && (isCastToTextType(bin.left()) || isCastToTextType(bin.right())
                    || isConcatExprWithTextCast(bin.left()) || isConcatExprWithTextCast(bin.right()))) {
            return left.toString() + right.toString();
        }

        // jsonb || jsonb: when an operand is explicitly cast to jsonb, both sides are
        // jsonb in PG. This handles scalar jsonb operands (e.g. '1'::jsonb || '2'::jsonb)
        // that the container-string heuristics in evalBuiltinBinary cannot recognize.
        // The other operand must itself be jsonb or an untyped literal PG would coerce;
        // there is no implicit cast from other types, so jsonb || 1 is 42883 in PG.
        if (bin.op() == BinaryExpr.BinOp.CONCAT
                && (isCastToType(bin.left(), "jsonb") || isCastToType(bin.right(), "jsonb"))
                && !(left instanceof List) && !(right instanceof List)
                && !(left instanceof HstoreValue) && !(right instanceof HstoreValue)) {
            if (left == null || right == null) return null;
            if (!isCastToType(bin.left(), "jsonb") && !(left instanceof String)) {
                throw new MemgresException("operator does not exist: "
                        + AstExecutor.pgTypeNameOf(left) + " || jsonb", "42883");
            }
            if (!isCastToType(bin.right(), "jsonb") && !(right instanceof String)) {
                throw new MemgresException("operator does not exist: jsonb || "
                        + AstExecutor.pgTypeNameOf(right), "42883");
            }
            return JsonOperations.concatenate(left.toString(), right.toString());
        }

        // Operator type mismatch validation (before coercion)
        executor.validateOperatorTypes(bin.op(), left, right);

        // Check for cross-type range operations (intersection via *)
        if (bin.op() == BinaryExpr.BinOp.MULTIPLY
                && left instanceof String && right instanceof String
                && RangeOperations.isRangeString(((String) left)) && RangeOperations.isRangeString(((String) right))) {
            String rs = (String) right;
            String ls = (String) left;
            String leftRangeType = getRangeTypeName(bin.left());
            String rightRangeType = getRangeTypeName(bin.right());
            if (leftRangeType != null && rightRangeType != null && !leftRangeType.equals(rightRangeType)) {
                throw new MemgresException(
                    "operator does not exist: " + leftRangeType + " * " + rightRangeType + "\n  Hint: No operator matches the given name and argument types. You might need to add explicit type casts.", "42883");
            }
        }

        // Try built-in operator handling; if it fails due to unsupported types,
        // fall back to user-defined operator lookup
        try {
            return evalBuiltinBinary(bin.op(), left, right);
        } catch (ClassCastException | NumberFormatException e) {
            // Built-in handling doesn't support these types — try user-defined operator
            String opSymbol = binOpToSymbol(bin.op());
            if (opSymbol != null) {
                Object result = tryUserDefinedOperator(opSymbol, left, right);
                if (result != null || (left == null || right == null)) return result;
            }
            throw e; // No user-defined operator either, rethrow
        } catch (MemgresException e) {
            // Only try fallback for "operator does not exist" errors
            if ("42883".equals(e.getSqlState())) {
                String opSymbol = binOpToSymbol(bin.op());
                if (opSymbol != null) {
                    Object result = tryUserDefinedOperator(opSymbol, left, right);
                    if (result != null || (left == null || right == null)) return result;
                }
            }
            throw e;
        }
    }

    private Object evalBuiltinBinary(BinaryExpr.BinOp op, Object left, Object right) {
        switch (op) {
            case ADD:
                // inet + integer arithmetic
                if (left instanceof InetValue && right instanceof Number) {
                    return ((InetValue) left).add(((Number) right).longValue());
                }
                if (left instanceof Number && right instanceof InetValue) {
                    return ((InetValue) right).add(((Number) left).longValue());
                }
                return executor.dateTimeAdd(left, right);
            case SUBTRACT:
                // inet - integer or inet - inet
                if (left instanceof InetValue && right instanceof Number) {
                    return ((InetValue) left).subtract(((Number) right).longValue());
                }
                if (left instanceof InetValue && right instanceof InetValue) {
                    return ((InetValue) left).subtract((InetValue) right);
                }
                return executor.dateTimeSubtract(left, right);
            case MULTIPLY:
                if (left instanceof InetValue || left instanceof MacaddrValue || left instanceof Macaddr8Value
                        || right instanceof InetValue || right instanceof MacaddrValue || right instanceof Macaddr8Value) {
                    String lt = ConstraintValidator.pgTypeNameOf(left);
                    String rt = ConstraintValidator.pgTypeNameOf(right);
                    throw new MemgresException("operator does not exist: " + lt + " * " + rt, "42883");
                }
                return executor.numericOrIntervalMul(left, right);
            case DIVIDE: {
                if (left instanceof String && right instanceof String
                        && GeometricOperations.isGeometricString(((String) left))) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return GeometricOperations.divide(ls, rs);
                }
                // money / money → float8 (PG behavior)
                if (left instanceof PgMoney && right instanceof PgMoney) {
                    return ((PgMoney) left).divideByMoney((PgMoney) right);
                }
                return executor.numericOp(left, right, (a, b) -> a / b, (a, b) -> a / b,
                    (a, b) -> a.divide(b, 20, java.math.RoundingMode.HALF_UP));
            }
            case MODULO:
                return executor.numericOp(left, right, (a, b) -> a % b, (a, b) -> a % b,
                    java.math.BigDecimal::remainder);
            case POWER: {
                if (left == null || right == null) return null;
                double result = Math.pow(executor.toDouble(left), executor.toDouble(right));
                // Numeric overflow: if either operand is BigDecimal, check for infinity
                if ((left instanceof java.math.BigDecimal || right instanceof java.math.BigDecimal) && Double.isInfinite(result)) {
                    throw new MemgresException("value overflows numeric format", "22003");
                }
                if (result == Math.floor(result) && !Double.isInfinite(result) && Math.abs(result) < Long.MAX_VALUE) {
                    return (long) result;
                }
                return result;
            }
            case BIT_AND: {
                if (left == null || right == null) return null;
                // Bit string AND: B'1010' & B'1100' -> '1000'
                String lBits = AstExecutor.toBitStringOrNull(left);
                String rBits = AstExecutor.toBitStringOrNull(right);
                if (lBits != null && rBits != null) {
                    return new AstExecutor.PgBitString(AstExecutor.bitwiseBitString(lBits, rBits, '&'));
                }
                // inet/macaddr bitwise AND
                if (left instanceof InetValue && right instanceof InetValue) {
                    return ((InetValue) left).bitwiseAnd((InetValue) right);
                }
                if (left instanceof MacaddrValue && right instanceof MacaddrValue) {
                    return ((MacaddrValue) left).bitwiseAnd((MacaddrValue) right);
                }
                if (left instanceof Macaddr8Value && right instanceof Macaddr8Value) {
                    return ((Macaddr8Value) left).bitwiseAnd((Macaddr8Value) right);
                }
                // intarray intersection: int[] & int[] -> intersection
                if (left instanceof java.util.List && right instanceof java.util.List) {
                    @SuppressWarnings("unchecked")
                    java.util.List<Object> la = (java.util.List<Object>) left;
                    @SuppressWarnings("unchecked")
                    java.util.List<Object> ra = (java.util.List<Object>) right;
                    java.util.Set<Object> rSet = new java.util.LinkedHashSet<>(ra);
                    java.util.List<Object> result = new java.util.ArrayList<>();
                    for (Object item : la) {
                        if (rSet.contains(item)) result.add(item);
                    }
                    return result;
                }
                { long r = executor.toLong(left) & executor.toLong(right);
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE) ? r : (int) r; }
            }
            case BIT_OR: {
                if (left == null || right == null) return null;
                // Bit string OR
                String lBitsOr = AstExecutor.toBitStringOrNull(left);
                String rBitsOr = AstExecutor.toBitStringOrNull(right);
                if (lBitsOr != null && rBitsOr != null) {
                    return new AstExecutor.PgBitString(AstExecutor.bitwiseBitString(lBitsOr, rBitsOr, '|'));
                }
                // inet/macaddr bitwise OR
                if (left instanceof InetValue && right instanceof InetValue) {
                    return ((InetValue) left).bitwiseOr((InetValue) right);
                }
                if (left instanceof MacaddrValue && right instanceof MacaddrValue) {
                    return ((MacaddrValue) left).bitwiseOr((MacaddrValue) right);
                }
                if (left instanceof Macaddr8Value && right instanceof Macaddr8Value) {
                    return ((Macaddr8Value) left).bitwiseOr((Macaddr8Value) right);
                }
                { long r = executor.toLong(left) | executor.toLong(right);
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE) ? r : (int) r; }
            }
            case BIT_XOR: {
                if (left == null || right == null) return null;
                // Bit string XOR
                String lBitsXor = AstExecutor.toBitStringOrNull(left);
                String rBitsXor = AstExecutor.toBitStringOrNull(right);
                if (lBitsXor != null && rBitsXor != null) {
                    return new AstExecutor.PgBitString(AstExecutor.bitwiseBitString(lBitsXor, rBitsXor, '#'));
                }
                // Geometric intersection: lseg # lseg, box # box
                if (left instanceof String && right instanceof String
                        && GeometricOperations.isGeometricString(((String) left))) {
                    String rs = (String) right;
                    String ls = (String) left;
                    Object result = GeometricOperations.intersectionGeneral(ls, rs);
                    return result != null ? GeometricOperations.format(result) : null;
                }
                { long r = executor.toLong(left) ^ executor.toLong(right);
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE) ? r : (int) r; }
            }
            case SHIFT_LEFT: {
                if (left == null || right == null) return null;
                // Bit string shift: B'1101' << 2 -> B'0100'
                if (left instanceof AstExecutor.PgBitString) {
                    String bits = ((AstExecutor.PgBitString) left).bits();
                    int shift = executor.toInt(right);
                    return new AstExecutor.PgBitString(shiftBitString(bits, shift, true));
                }
                // Check for geometric "strictly left of": box << box
                if (left instanceof String && right instanceof String
                        && GeometricOperations.isGeometricString(((String) left))) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return GeometricOperations.isStrictlyLeft(ls, rs);
                }
                // inet << inet: left is strictly contained in right
                if (left instanceof InetValue && right instanceof InetValue) {
                    return ((InetValue) right).contains((InetValue) left);
                }
                { long r = executor.toLong(left) << executor.toLong(right);
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE) ? r : (int) r; }
            }
            case SHIFT_RIGHT: {
                if (left == null || right == null) return null;
                // Bit string shift: B'1101' >> 2 -> B'0011'
                if (left instanceof AstExecutor.PgBitString) {
                    String bits = ((AstExecutor.PgBitString) left).bits();
                    int shift = executor.toInt(right);
                    return new AstExecutor.PgBitString(shiftBitString(bits, shift, false));
                }
                // Check for geometric "strictly right of": box >> box
                if (left instanceof String && right instanceof String
                        && GeometricOperations.isGeometricString(((String) left))) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return GeometricOperations.isStrictlyRight(ls, rs);
                }
                // inet >> inet: left strictly contains right
                if (left instanceof InetValue && right instanceof InetValue) {
                    return ((InetValue) left).contains((InetValue) right);
                }
                { long r = executor.toLong(left) >> executor.toLong(right);
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE) ? r : (int) r; }
            }
            case INET_CONTAINS_EQUALS: {
                if (left == null || right == null) return null;
                InetValue il = toInetValue(left), ir = toInetValue(right);
                return il.containsOrEquals(ir);
            }
            case INET_CONTAINED_BY_EQUALS: {
                if (left == null || right == null) return null;
                InetValue il = toInetValue(left), ir = toInetValue(right);
                return ir.containsOrEquals(il);
            }
            case EQUAL: {
                if (left == null || right == null) return null; // NULL = x is NULL
                // Handle array literal (String like "{1,2}") = List comparison
                boolean leftIsRow = left instanceof AstExecutor.PgRow || left instanceof List<?>;
                boolean rightIsRow = right instanceof AstExecutor.PgRow || right instanceof List<?>;
                if (leftIsRow != rightIsRow) {
                    // Allow comparing PG array literal string with an array/list
                    if (left instanceof String && ((String) left).startsWith("{") && ((String) left).endsWith("}") && rightIsRow) {
                        String s = (String) left;
                        return TypeCoercion.areEqual(left, right);
                    }
                    if (right instanceof String && ((String) right).startsWith("{") && ((String) right).endsWith("}") && leftIsRow) {
                        String s = (String) right;
                        return TypeCoercion.areEqual(left, right);
                    }
                    throw new MemgresException("operator does not exist: " +
                            (leftIsRow ? "record" : TypeCoercion.inferType(left) != null ? TypeCoercion.inferType(left).getPgName() : "unknown") +
                            " = " +
                            (rightIsRow ? "record" : TypeCoercion.inferType(right) != null ? TypeCoercion.inferType(right).getPgName() : "unknown"),
                            "42883");
                }
                // Handle ROW comparison (NULL propagates) vs ARRAY comparison (NULL=NULL is true)
                boolean leftIsArray = left instanceof List && !(left instanceof AstExecutor.PgRow);
                boolean rightIsArray = right instanceof List && !(right instanceof AstExecutor.PgRow);
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : left instanceof List ? (List<?>) left : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : right instanceof List ? (List<?>) right : null;
                if (lList != null && rList != null) {
                    if (lList.size() != rList.size()) {
                        if (leftIsArray && rightIsArray) return false;
                        throw new MemgresException("cannot compare row values of different sizes", "42601");
                    }
                    for (int ri = 0; ri < lList.size(); ri++) {
                        Object lv = lList.get(ri), rv = rList.get(ri);
                        if (lv == null || rv == null) {
                            if (leftIsArray && rightIsArray) {
                                // ARRAY: NULL=NULL is true, NULL!=non-NULL is false
                                if (lv == null && rv == null) continue;
                                return false;
                            }
                            return null; // ROW: NULL propagates
                        }
                        if (!TypeCoercion.areEqual(lv, rv)) return false;
                    }
                    return true;
                }
                // text vs integer comparison: coerce to compare numerically if possible,
                // otherwise treat as not-equal (catalog queries may compare text columns with OIDs)
                if ((left instanceof String && (right instanceof Integer || right instanceof Long))
                        || (right instanceof String && (left instanceof Integer || left instanceof Long))) {
                    String sVal = left instanceof String ? (String) left : (String) right;
                    try { Long.parseLong(sVal); } catch (NumberFormatException e) {
                        return false;
                    }
                }
                return TypeCoercion.areEqual(left, right);
            }
            case NOT_EQUAL: {
                if (left == null || right == null) return null; // NULL <> x is NULL
                // Handle ROW comparison
                List<?> lList2 = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : left instanceof List ? (List<?>) left : null;
                List<?> rList2 = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : right instanceof List ? (List<?>) right : null;
                if (lList2 != null && rList2 != null) {
                    if (lList2.size() != rList2.size()) throw new MemgresException("cannot compare row values of different sizes", "42601");
                    for (int ri = 0; ri < lList2.size(); ri++) {
                        Object lv = lList2.get(ri), rv = rList2.get(ri);
                        if (lv == null || rv == null) return null;
                        if (!TypeCoercion.areEqual(lv, rv)) return true;
                    }
                    return false;
                }
                return !TypeCoercion.areEqual(left, right);
            }
            case LESS_THAN: {
                if (left == null || right == null) return null;
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : left instanceof List ? (List<?>) left : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : right instanceof List ? (List<?>) right : null;
                if (lList != null && rList != null) {
                    int minLen = Math.min(lList.size(), rList.size());
                    for (int ri = 0; ri < minLen; ri++) {
                        Object lv = lList.get(ri), rv = rList.get(ri);
                        if (left instanceof AstExecutor.PgRow && (lv == null || rv == null)) return null;
                        int cmp = TypeCoercion.compare(lv, rv);
                        if (cmp != 0) return cmp < 0;
                    }
                    return lList.size() < rList.size();
                }
                return executor.compareValues(left, right) < 0;
            }
            case GREATER_THAN: {
                if (left == null || right == null) return null;
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : left instanceof List ? (List<?>) left : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : right instanceof List ? (List<?>) right : null;
                if (lList != null && rList != null) {
                    int minLen = Math.min(lList.size(), rList.size());
                    for (int ri = 0; ri < minLen; ri++) {
                        Object lv = lList.get(ri), rv = rList.get(ri);
                        if (left instanceof AstExecutor.PgRow && (lv == null || rv == null)) return null;
                        int cmp = TypeCoercion.compare(lv, rv);
                        if (cmp != 0) return cmp > 0;
                    }
                    return lList.size() > rList.size();
                }
                return executor.compareValues(left, right) > 0;
            }
            case LESS_EQUAL: {
                if (left == null || right == null) return null;
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : left instanceof List ? (List<?>) left : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : right instanceof List ? (List<?>) right : null;
                if (lList != null && rList != null) {
                    int minLen = Math.min(lList.size(), rList.size());
                    for (int ri = 0; ri < minLen; ri++) {
                        Object lv = lList.get(ri), rv = rList.get(ri);
                        if (left instanceof AstExecutor.PgRow && (lv == null || rv == null)) return null;
                        int cmp = TypeCoercion.compare(lv, rv);
                        if (cmp != 0) return cmp <= 0;
                    }
                    return lList.size() <= rList.size();
                }
                return executor.compareValues(left, right) <= 0;
            }
            case GREATER_EQUAL: {
                if (left == null || right == null) return null;
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : left instanceof List ? (List<?>) left : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : right instanceof List ? (List<?>) right : null;
                if (lList != null && rList != null) {
                    int minLen = Math.min(lList.size(), rList.size());
                    for (int ri = 0; ri < minLen; ri++) {
                        Object lv = lList.get(ri), rv = rList.get(ri);
                        if (left instanceof AstExecutor.PgRow && (lv == null || rv == null)) return null;
                        int cmp = TypeCoercion.compare(lv, rv);
                        if (cmp != 0) return cmp >= 0;
                    }
                    return lList.size() >= rList.size();
                }
                return executor.compareValues(left, right) >= 0;
            }
            case IS_DISTINCT_FROM: {
                // NULL-safe: both NULL -> false (not distinct), one NULL -> true
                if (left == null && right == null) return false;
                if (left == null || right == null) return true;
                return !TypeCoercion.areEqual(left, right);
            }
            case IS_NOT_DISTINCT_FROM: {
                if (left == null && right == null) return true;
                if (left == null || right == null) return false;
                return TypeCoercion.areEqual(left, right);
            }
            case CONCAT: {
                // hstore || hstore: merge
                if (left instanceof HstoreValue || right instanceof HstoreValue) {
                    if (left == null || right == null) return left != null ? left : right;
                    HstoreValue lh = left instanceof HstoreValue ? (HstoreValue) left : HstoreValue.parse(left.toString());
                    HstoreValue rh = right instanceof HstoreValue ? (HstoreValue) right : HstoreValue.parse(right.toString());
                    return lh.merge(rh);
                }
                // Array concat with NULL: NULL || array = array, array || NULL = array
                if (left == null && right instanceof List) return right;
                if (right == null && left instanceof List) return left;
                if (left == null || right == null) {
                    // Check if the non-null side is an array-like string
                    if (left == null && right != null) {
                        String rs = right.toString().trim();
                        if (rs.startsWith("{") && rs.endsWith("}")) return right;
                    }
                    if (right == null && left != null) {
                        String ls = left.toString().trim();
                        if (ls.startsWith("{") && ls.endsWith("}")) return left;
                    }
                    return null;
                }
                // Bytea (byte[]) concatenation
                if (left instanceof byte[] && right instanceof byte[]) {
                    byte[] lb = (byte[]) left;
                    byte[] rb = (byte[]) right;
                    byte[] result = new byte[lb.length + rb.length];
                    System.arraycopy(lb, 0, result, 0, lb.length);
                    System.arraycopy(rb, 0, result, lb.length, rb.length);
                    return result;
                }
                if (left instanceof byte[]) {
                    byte[] lb = (byte[]) left;
                    byte[] rb = TypeCoercion.toBytea(right);
                    if (rb != null) {
                        byte[] result = new byte[lb.length + rb.length];
                        System.arraycopy(lb, 0, result, 0, lb.length);
                        System.arraycopy(rb, 0, result, lb.length, rb.length);
                        return result;
                    }
                }
                if (right instanceof byte[]) {
                    byte[] rb = (byte[]) right;
                    byte[] lb = TypeCoercion.toBytea(left);
                    if (lb != null) {
                        byte[] result = new byte[lb.length + rb.length];
                        System.arraycopy(lb, 0, result, 0, lb.length);
                        System.arraycopy(rb, 0, result, lb.length, rb.length);
                        return result;
                    }
                }
                // TsQuery || TsQuery — OR of two queries
                if (left instanceof TsQuery && right instanceof TsQuery) return TsQuery.or((TsQuery) left, (TsQuery) right);
                if (left instanceof TsQuery) return TsQuery.or((TsQuery) left, TsQuery.parse(right.toString()));
                if (right instanceof TsQuery) return TsQuery.or(TsQuery.parse(left.toString()), (TsQuery) right);
                // TsVector || TsVector concatenation
                if (left instanceof TsVector && right instanceof TsVector) return ((TsVector) left).concat(((TsVector) right));
                if (left instanceof TsVector) return ((TsVector) left).concat(TsVector.fromText(right.toString()));
                if (right instanceof TsVector) return TsVector.fromText(left.toString()).concat(((TsVector) right));
                // Array concatenation: array || array, array || element, element || array
                if (left instanceof List && right instanceof List) {
                    List<?> ll = (List<?>) left;
                    List<?> rl = (List<?>) right;
                    List<Object> merged = new ArrayList<>(ll);
                    merged.addAll(rl);
                    return merged;
                }
                if (left instanceof List) {
                    List<?> ll = (List<?>) left;
                    List<Object> merged = new ArrayList<>(ll);
                    merged.add(right);
                    return merged;
                }
                if (right instanceof List) {
                    List<?> rl = (List<?>) right;
                    List<Object> merged = new ArrayList<>();
                    merged.add(left);
                    merged.addAll(rl);
                    return merged;
                }
                String ls = left.toString();
                String rs = right.toString();
                boolean looksLikePgArrayL = ls.trim().startsWith("{") && ls.trim().endsWith("}") && !ls.trim().startsWith("{\"");
                boolean looksLikePgArrayR = rs.trim().startsWith("{") && rs.trim().endsWith("}") && !rs.trim().startsWith("{\"");
                // PG does NOT support || for multirange types
                if (RangeOperations.isMultirangeOrEmpty(ls) || RangeOperations.isMultirangeOrEmpty(rs)) {
                    throw new MemgresException("operator does not exist: multirange || multirange", "42883");
                }
                // PG array string || scalar = array append
                if (looksLikePgArrayL && !looksLikePgArrayR) {
                    List<Object> arr = new ArrayList<>(FunctionEvaluator.parseSimplePgArray(ls));
                    arr.add(right instanceof Number ? right : rs);
                    return TypeCoercion.formatPgArray(arr);
                }
                // scalar || PG array string = array prepend
                if (looksLikePgArrayR && !looksLikePgArrayL) {
                    List<Object> arr = new ArrayList<>();
                    arr.add(left instanceof Number ? left : ls);
                    arr.addAll(FunctionEvaluator.parseSimplePgArray(rs));
                    return TypeCoercion.formatPgArray(arr);
                }
                // PG array string || PG array string = array concat
                if (looksLikePgArrayL && looksLikePgArrayR) {
                    List<Object> arr = new ArrayList<>(FunctionEvaluator.parseSimplePgArray(ls));
                    arr.addAll(FunctionEvaluator.parseSimplePgArray(rs));
                    return TypeCoercion.formatPgArray(arr);
                }
                if ((ls.trim().startsWith("{") || ls.trim().startsWith("["))) {
                    // Check for JSON concatenation
                    if ((rs.trim().startsWith("{") || rs.trim().startsWith("["))) {
                        return JsonOperations.concatenate(ls, rs);
                    }
                }
                return ls + rs;
            }
            case LIKE: {
                if (left == null || right == null) return null;
                if (left instanceof Number || left instanceof Boolean) {
                    String tn = left instanceof Integer ? "integer" : left instanceof Long ? "bigint" :
                            left instanceof Boolean ? "boolean" : left.getClass().getSimpleName().toLowerCase();
                    throw new MemgresException("operator does not exist: " + tn + " ~~ unknown", "42883");
                }
                String pattern = AstExecutor.likeToRegex(right.toString());
                return left.toString().matches("(?s)" + pattern);
            }
            case ILIKE: {
                if (left == null || right == null) return null;
                String pattern = AstExecutor.likeToRegex(right.toString());
                return left.toString().matches("(?si)" + pattern);
            }
            case SIMILAR_TO: {
                if (left == null || right == null) return null;
                // SIMILAR TO uses SQL regex with default backslash escape
                String pattern = similarToRegexForBinaryOp(right.toString(), "\\");
                return left.toString().matches("(?s)" + pattern);
            }
            case JSON_ARROW: {
                // json -> key or json -> index (returns JSON), also array subscript
                if (left == null || right == null) return null;
                // hstore -> text[]: extract multiple values by key array
                if (left instanceof HstoreValue && right instanceof List) {
                    HstoreValue h = (HstoreValue) left;
                    List<String> result = new java.util.ArrayList<>();
                    for (Object k : (List<?>) right) {
                        result.add(k != null ? h.get(k.toString()) : null);
                    }
                    return result;
                }
                // hstore -> text: extract value by key
                if (left instanceof HstoreValue) {
                    return ((HstoreValue) left).get(right.toString());
                }
                // Reject non-integer numeric types (e.g., jsonb -> 999999999999999999999)
                if (right instanceof java.math.BigDecimal) {
                    java.math.BigDecimal bd = (java.math.BigDecimal) right;
                    throw new MemgresException("operator does not exist: jsonb -> numeric", "42883");
                }
                if (right instanceof Number) {
                    Number n = (Number) right;
                    // Array index access
                    if (left instanceof PgVector) {
                        PgVector vec = (PgVector) left;
                        // int2vector/oidvector: 0-based subscript (PG convention)
                        int idx = n.intValue();
                        return (idx >= 0 && idx < vec.size()) ? vec.get(idx) : null;
                    }
                    if (left instanceof List) {
                        List<?> list = (List<?>) left;
                        int idx = n.intValue() - 1; // PG arrays are 1-based
                        return (idx >= 0 && idx < list.size()) ? list.get(idx) : null;
                    }
                    // Custom lower-bound array: "[lb:ub]={...}" format
                    if (left instanceof String && ((String) left).matches("\\[\\d+:\\d+\\]=\\{.*\\}")) {
                        String ls = (String) left;
                        int eqIdx = ls.indexOf('=');
                        String boundsStr = ls.substring(0, eqIdx);
                        String[] bparts = boundsStr.substring(1, boundsStr.length() - 1).split(":");
                        int lowerBound = Integer.parseInt(bparts[0].trim());
                        String content = ls.substring(eqIdx + 1);
                        List<Object> elements = FunctionEvaluator.parseSimplePgArray(content);
                        int idx = n.intValue() - lowerBound; // adjust for custom lower bound
                        return (idx >= 0 && idx < elements.size()) ? elements.get(idx) : null;
                    }
                    // JSON array string: [elem1, elem2, ...] with 0-based indexing for JSON ->
                    if (left instanceof String && ((String) left).trim().startsWith("[")) {
                        String ls = (String) left;
                        int idx = n.intValue(); // JSON arrays are 0-based in -> operator
                        // Parse JSON array
                        String inner = ls.trim();
                        inner = inner.substring(1, inner.length() - 1).trim();
                        if (!inner.isEmpty()) {
                            List<String> elements = new ArrayList<>();
                            int depth = 0; int start = 0;
                            for (int ci = 0; ci <= inner.length(); ci++) {
                                if (ci == inner.length() || (inner.charAt(ci) == ',' && depth == 0)) {
                                    elements.add(inner.substring(start, ci).trim());
                                    start = ci + 1;
                                } else if (inner.charAt(ci) == '{' || inner.charAt(ci) == '[') depth++;
                                else if (inner.charAt(ci) == '}' || inner.charAt(ci) == ']') depth--;
                            }
                            if (idx >= 0 && idx < elements.size()) return elements.get(idx);
                        }
                        return null;
                    }
                    // PG array string format: {elem1,elem2,...}
                    if (left instanceof String && ((String) left).startsWith("{") && ((String) left).endsWith("}") && !((String) left).startsWith("{\"")) {
                        String ls = (String) left;
                        String inner = ls.substring(1, ls.length() - 1);
                        if (!inner.isEmpty()) {
                            String[] parts = inner.split(",", -1);
                            int idx = n.intValue() - 1; // PG arrays are 1-based
                            if (idx >= 0 && idx < parts.length) {
                                String val = parts[idx].trim();
                                if (val.equalsIgnoreCase("NULL")) return null;
                                try { return Integer.parseInt(val); } catch (NumberFormatException e) {
                                    try { return Long.parseLong(val); } catch (NumberFormatException e2) { return val; }
                                }
                            }
                        }
                        return null;
                    }
                    // name-style subscript (pg_dump: typname[0] = '_'): PG's name type
                    // supports zero-based single-character access; out of range is NULL.
                    // Only plain strings land here — JSON/array containers were handled above.
                    if (left instanceof String) {
                        String plain = (String) left;
                        String trimmed = plain.trim();
                        if (!trimmed.startsWith("{") && !trimmed.startsWith("[") && !trimmed.startsWith("\"")) {
                            int idx = n.intValue();
                            return (idx >= 0 && idx < plain.length()) ? String.valueOf(plain.charAt(idx)) : null;
                        }
                    }
                }
                // Check for array subscript with non-integer key (e.g., b['x'])
                if (left instanceof List<?> || (left instanceof String && ((String) left).trim().startsWith("{") && !((String) left).trim().startsWith("{\"") && ((String) left).trim().endsWith("}"))) {
                    String ls2 = (String) left;
                    // Array subscripts must be integers
                    if (right instanceof String) {
                        String rs2 = (String) right;
                        try { Integer.parseInt(rs2); } catch (NumberFormatException e) {
                            throw new MemgresException("invalid input syntax for type integer: \"" + rs2 + "\"", "22P02");
                        }
                    }
                }
                // Object key access on JSON string
                String json = left.toString().trim();
                String key = right.toString();
                return executor.functionEvaluator.extractJsonKey(json, key);
            }
            case JSON_ARROW_TEXT: {
                // json ->> key (returns text, strips quotes from string values)
                if (left == null || right == null) return null;
                if (right instanceof Number) {
                    Number n = (Number) right;
                    if (left instanceof PgVector) {
                        PgVector vec = (PgVector) left;
                        int idx = n.intValue(); // 0-based for int2vector/oidvector
                        Object elem = (idx >= 0 && idx < vec.size()) ? vec.get(idx) : null;
                        return elem == null ? null : elem.toString();
                    }
                    if (left instanceof List) {
                        List<?> list = (List<?>) left;
                        // PG arrays are 1-based
                        int idx = n.intValue() - 1;
                        Object elem = (idx >= 0 && idx < list.size()) ? list.get(idx) : null;
                        return elem == null ? null : elem.toString();
                    }
                    // Left is a JSON array string from a previous -> operation
                    String leftJsonStr = left.toString().trim();
                    if (leftJsonStr.startsWith("[")) {
                        // ->> maps JSON null to SQL NULL and unquotes/unescapes strings
                        return JsonOperations.jsonValueToText(
                                JsonOperations.extractArrayElement(leftJsonStr, n.intValue()));
                    }
                }
                // Object key access on JSON string; ->> returns text (unquoted, unescaped),
                // and SQL NULL for JSON null values (unlike -> which returns "null")
                String json2 = left.toString().trim();
                String key2 = right.toString();
                return JsonOperations.jsonValueToText(executor.functionEvaluator.extractJsonKey(json2, key2));
            }
            case TS_MATCH: {
                // JSONB @@ jsonpath
                if (left != null && right != null) {
                    String ls = left.toString().trim();
                    if (ls.startsWith("{") || ls.startsWith("[")) {
                        String path = right.toString().trim();
                        return executor.functionEvaluator.evaluateJsonPathPredicate(ls, path);
                    }
                }
                // NULL @@ tsquery or tsvector @@ NULL → NULL (not false)
                if (left == null || right == null) return null;
                TsVector vec;
                if (left instanceof TsVector) { vec = (TsVector) left; }
                else { String s = left.toString(); TsVector p = TsVector.parseLiteral(s); vec = p != null ? p : TsVector.fromText(s); }
                TsQuery query = right instanceof TsQuery ? (TsQuery) right : TsQuery.parse(right.toString());
                return vec.matches(query);
            }
            case JSON_HASH_ARROW: {
                if (left == null || right == null) return null;
                // #> requires text[] path (must be an array or {}-formatted string)
                if (right instanceof String && !((String) right).trim().startsWith("{") && !(right instanceof List<?>)) {
                    String rs = (String) right;
                    throw new MemgresException("malformed array literal: \"" + right + "\"", "22P02");
                }
                String json = left.toString().trim();
                List<String> path = executor.parseJsonPathArg(right);
                return JsonOperations.extractPath(json, path);
            }
            case JSON_HASH_ARROW_TEXT: {
                if (left == null || right == null) return null;
                // #>> requires text[] path
                if (right instanceof String && !((String) right).trim().startsWith("{") && !(right instanceof List<?>)) {
                    String rs = (String) right;
                    throw new MemgresException("malformed array literal: \"" + right + "\"", "22P02");
                }
                String json = left.toString().trim();
                List<String> path = executor.parseJsonPathArg(right);
                return JsonOperations.extractPathText(json, path);
            }
            case CONTAINS: {
                if (left == null || right == null) return null;
                // hstore @> hstore containment
                if (left instanceof HstoreValue || right instanceof HstoreValue) {
                    HstoreValue lh = left instanceof HstoreValue ? (HstoreValue) left : HstoreValue.parse(left.toString());
                    HstoreValue rh = right instanceof HstoreValue ? (HstoreValue) right : HstoreValue.parse(right.toString());
                    return lh.containsAll(rh);
                }
                // Convert Java Lists to PG array format FIRST so arrays like ARRAY[1,5]
                // (which stringify as "[1, 5]") are never mistaken for range literals
                boolean lIsList = left instanceof List;
                boolean rIsList = right instanceof List;
                String ls = lIsList ? TypeCoercion.formatPgArray((List<?>) left) : left.toString().trim();
                String rs = rIsList ? TypeCoercion.formatPgArray((List<?>) right) : right.toString().trim();
                // Range/multirange semantics only apply when neither operand is a real array
                if (!lIsList && !rIsList) {
                    // Multirange containment: multirange @> value/range/multirange
                    if (RangeOperations.isMultirangeOrEmpty(ls)) {
                        if (RangeOperations.isMultirangeOrEmpty(rs)) return RangeOperations.multirangeContainsMultirange(ls, rs);
                        if (RangeOperations.isRangeString(rs)) return RangeOperations.multirangeContainsRange(ls, RangeOperations.parse(rs));
                        if (right instanceof Number) return RangeOperations.multirangeContains(ls, ((Number) right));
                        try { return RangeOperations.multirangeContains(ls, Long.parseLong(rs)); } catch (NumberFormatException ignore) {}
                        return false;
                    }
                    // Range containment: range @> value or range @> range or range @> multirange
                    if (RangeOperations.isRangeString(ls)) {
                        RangeOperations.PgRange range = RangeOperations.parse(ls);
                        if (right instanceof Number) return range.contains(((Number) right));
                        if (right instanceof java.time.LocalDateTime) return range.contains(((java.time.LocalDateTime) right).toEpochSecond(java.time.ZoneOffset.UTC));
                        if (right instanceof java.time.LocalDate) return range.contains(((java.time.LocalDate) right).toEpochDay());
                        // range @> multirange: true if range contains every sub-range
                        if (RangeOperations.isMultirangeOrEmpty(rs)) {
                            return RangeOperations.multirangeContainsMultirange("{" + ls + "}", rs);
                        }
                        if (RangeOperations.isRangeString(rs)) return range.containsRange(RangeOperations.parse(rs));
                        // PG: range @> non-range-non-number -> try parsing
                        try {
                            return range.contains(Long.parseLong(rs));
                        } catch (NumberFormatException e) {
                            // Try decimal (numrange @> numeric-as-string)
                            try { return range.contains(new java.math.BigDecimal(rs)); } catch (NumberFormatException ignored) {}
                            // Try timestamp
                            if (rs.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}.*")) {
                                try {
                                    java.time.LocalDateTime ldt = TypeCoercion.toLocalDateTime(rs);
                                    return range.contains(ldt.toEpochSecond(java.time.ZoneOffset.UTC));
                                } catch (Exception ignored) {}
                            }
                            // Try date
                            if (rs.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
                                return range.contains(java.time.LocalDate.parse(rs.substring(0, 10)).toEpochDay());
                            }
                            throw new MemgresException("malformed range literal: \"" + rs + "\"", "22P02");
                        }
                    }
                }
                // PG array containment: {1,2,3} @> {2,3}
                boolean lIsPgArray = lIsList || (ls.startsWith("{") && !ls.startsWith("{\""));
                boolean rIsPgArray = rIsList || (rs.startsWith("{") && !rs.startsWith("{\""));
                if (lIsPgArray && rIsPgArray) {
                    List<Object> la = FunctionEvaluator.parseSimplePgArray(ls);
                    List<Object> ra = FunctionEvaluator.parseSimplePgArray(rs);
                    return arrayContainsAll(la, ra);
                }
                if (lIsPgArray && !rIsPgArray) {
                    throw new MemgresException("malformed array literal: \"" + rs + "\"", "22P02");
                }
                if (rIsList && !lIsPgArray) {
                    if (!(left instanceof String)) {
                        throw new MemgresException("operator does not exist: " + AstExecutor.pgTypeNameOf(left) + " @> " + AstExecutor.pgTypeNameOf(right), "42883");
                    }
                    throw new MemgresException("malformed array literal: \"" + ls + "\"", "22P02");
                }
                // JSON containment
                if ((ls.startsWith("{") || ls.startsWith("[")) && (rs.startsWith("{") || rs.startsWith("["))) {
                    return JsonOperations.contains(ls, rs);
                }
                // jsonb containment with a scalar operand (e.g. '[1,2,3]'::jsonb @> '3'::jsonb,
                // '"foo"'::jsonb @> '"foo"'::jsonb). PG array strings were already handled
                // above, so a '['/'{' prefix here is JSON.
                if (JsonOperations.isJsonScalar(rs)
                        && (ls.startsWith("{") || ls.startsWith("[") || JsonOperations.isJsonScalar(ls))) {
                    return JsonOperations.contains(ls, rs);
                }
                if (GeometricOperations.isGeometricString(ls) || GeometricOperations.isGeometricString(rs)) {
                    return GeometricOperations.contains(ls, rs);
                }
                return false;
            }
            case CONTAINED_BY: {
                if (left == null || right == null) return null;
                // hstore <@ hstore: contained-by
                if (left instanceof HstoreValue || right instanceof HstoreValue) {
                    HstoreValue lh = left instanceof HstoreValue ? (HstoreValue) left : HstoreValue.parse(left.toString());
                    HstoreValue rh = right instanceof HstoreValue ? (HstoreValue) right : HstoreValue.parse(right.toString());
                    return lh.containedBy(rh);
                }
                // Convert Java Lists to PG array format FIRST so arrays like ARRAY[1,5]
                // (which stringify as "[1, 5]") are never mistaken for range literals
                boolean lIsList = left instanceof List;
                boolean rIsList = right instanceof List;
                String ls = lIsList ? TypeCoercion.formatPgArray((List<?>) left) : left.toString().trim();
                String rs = rIsList ? TypeCoercion.formatPgArray((List<?>) right) : right.toString().trim();
                // Range/multirange semantics only apply when neither operand is a real array
                if (!lIsList && !rIsList) {
                    // Multirange/range containment: a <@ b means b @> a
                    if (RangeOperations.isMultirangeOrEmpty(rs)) {
                        if (RangeOperations.isMultirangeOrEmpty(ls)) return RangeOperations.multirangeContainsMultirange(rs, ls);
                        if (RangeOperations.isRangeString(ls)) return RangeOperations.multirangeContainsRange(rs, RangeOperations.parse(ls));
                        if (left instanceof Number) return RangeOperations.multirangeContains(rs, ((Number) left));
                        try { return RangeOperations.multirangeContains(rs, Long.parseLong(ls)); } catch (NumberFormatException ignore) {}
                        return false;
                    }
                    if (RangeOperations.isRangeString(rs)) {
                        RangeOperations.PgRange range = RangeOperations.parse(rs);
                        // multirange <@ range: true if range contains every sub-range
                        if (RangeOperations.isMultirangeOrEmpty(ls)) {
                            return RangeOperations.multirangeContainsMultirange("{" + rs + "}", ls);
                        }
                        if (RangeOperations.isRangeString(ls)) return range.containsRange(RangeOperations.parse(ls));
                        if (left instanceof Number) return range.contains(((Number) left));
                        try { return range.contains(Long.parseLong(ls)); } catch (NumberFormatException ignore) {}
                        try { return range.contains(new java.math.BigDecimal(ls)); } catch (NumberFormatException ignore) {}
                    }
                }
                // PG array containment: {2,3} <@ {1,2,3}
                boolean lIsPgArray = lIsList || (ls.startsWith("{") && !ls.startsWith("{\""));
                boolean rIsPgArray = rIsList || (rs.startsWith("{") && !rs.startsWith("{\""));
                if (lIsPgArray && rIsPgArray) {
                    List<Object> la = FunctionEvaluator.parseSimplePgArray(ls);
                    List<Object> ra = FunctionEvaluator.parseSimplePgArray(rs);
                    return arrayContainsAll(ra, la);
                }
                if (lIsList && !rIsPgArray) {
                    if (!(right instanceof String)) {
                        throw new MemgresException("operator does not exist: " + AstExecutor.pgTypeNameOf(left) + " <@ " + AstExecutor.pgTypeNameOf(right), "42883");
                    }
                    throw new MemgresException("malformed array literal: \"" + rs + "\"", "22P02");
                }
                if (rIsList && !lIsPgArray) {
                    if (!(left instanceof String)) {
                        throw new MemgresException("operator does not exist: " + AstExecutor.pgTypeNameOf(left) + " <@ " + AstExecutor.pgTypeNameOf(right), "42883");
                    }
                    throw new MemgresException("malformed array literal: \"" + ls + "\"", "22P02");
                }
                if ((ls.startsWith("{") || ls.startsWith("[")) && (rs.startsWith("{") || rs.startsWith("["))) {
                    return JsonOperations.contains(rs, ls);
                }
                // jsonb scalar <@ jsonb container/scalar (e.g. '3'::jsonb <@ '[1,2,3]'::jsonb).
                // Range/multirange strings were already handled above.
                if (JsonOperations.isJsonScalar(ls)
                        && (rs.startsWith("{") || rs.startsWith("[") || JsonOperations.isJsonScalar(rs))) {
                    return JsonOperations.contains(rs, ls);
                }
                if (GeometricOperations.isGeometricString(ls) || GeometricOperations.isGeometricString(rs)) {
                    return GeometricOperations.contains(rs, ls);
                }
                return false;
            }
            case JSONB_EXISTS: {
                if (left == null || right == null) return null;
                if (left instanceof HstoreValue) return ((HstoreValue) left).getData().containsKey(right.toString());
                return JsonOperations.keyExists(left.toString(), right.toString());
            }
            case JSONB_EXISTS_ANY: {
                if (left == null || right == null) return null;
                if (left instanceof HstoreValue) {
                    List<String> hkeys = right instanceof List ? ((List<?>) right).stream().map(Object::toString).collect(Collectors.toList()) : Cols.listOf(right.toString());
                    HstoreValue h = (HstoreValue) left;
                    for (String k : hkeys) { if (h.getData().containsKey(k)) return true; }
                    return false;
                }
                // ?| requires text[] on right side
                if (right instanceof String && !((String) right).trim().startsWith("{") && !(right instanceof List<?>)) {
                    String rs = (String) right;
                    throw new MemgresException("malformed array literal: \"" + right + "\"", "22P02");
                }
                List<String> keys = right instanceof List ? ((List<?>) right).stream().map(Object::toString).collect(Collectors.toList()) : Cols.listOf(right.toString());
                return JsonOperations.anyKeyExists(left.toString(), keys);
            }
            case JSONB_EXISTS_ALL: {
                if (left == null || right == null) return null;
                if (left instanceof HstoreValue) {
                    List<String> hkeys = right instanceof List ? ((List<?>) right).stream().map(Object::toString).collect(Collectors.toList()) : Cols.listOf(right.toString());
                    HstoreValue h = (HstoreValue) left;
                    for (String k : hkeys) { if (!h.getData().containsKey(k)) return false; }
                    return true;
                }
                List<String> keys = right instanceof List ? ((List<?>) right).stream().map(Object::toString).collect(Collectors.toList()) : Cols.listOf(right.toString());
                return JsonOperations.allKeysExist(left.toString(), keys);
            }
            case JSONB_PATH_EXISTS_OP: {
                // @? operator, equivalent to jsonb_path_exists
                if (left == null || right == null) return null;
                return executor.functionEvaluator.evaluateJsonPathExists(left.toString().trim(), right.toString().trim());
            }
            case JSON_DELETE_PATH: {
                if (left == null || right == null) return null;
                List<String> path = executor.parseJsonPathArg(right);
                return JsonOperations.deletePath(left.toString(), path);
            }
            case OVERLAP: {
                if (left == null || right == null) return null;
                // inet && inet: overlap (networks share any addresses)
                if (left instanceof InetValue && right instanceof InetValue) {
                    InetValue il = (InetValue) left, ir = (InetValue) right;
                    return il.containsOrEquals(ir) || ir.containsOrEquals(il);
                }
                // TsQuery && TsQuery — AND of two queries
                if (left instanceof TsQuery && right instanceof TsQuery) {
                    return TsQuery.and((TsQuery) left, (TsQuery) right);
                }
                if (left instanceof TsQuery) return TsQuery.and((TsQuery) left, TsQuery.parse(right.toString()));
                if (right instanceof TsQuery) return TsQuery.and(TsQuery.parse(left.toString()), (TsQuery) right);
                // Convert Lists to PG format for uniform handling
                boolean oLIsList = left instanceof List;
                boolean oRIsList = right instanceof List;
                String oLs = oLIsList ? TypeCoercion.formatPgArray((List<?>) left) : left.toString().trim();
                String oRs = oRIsList ? TypeCoercion.formatPgArray((List<?>) right) : right.toString().trim();
                // Range/multirange semantics only apply when neither operand is a real array
                if (!oLIsList && !oRIsList) {
                    // Multirange overlap checks
                    if (RangeOperations.isMultirangeOrEmpty(oLs) && RangeOperations.isMultirangeOrEmpty(oRs)) {
                        return RangeOperations.multirangeOverlapsMultirange(oLs, oRs);
                    }
                    if (RangeOperations.isMultirangeOrEmpty(oLs) && RangeOperations.isRangeString(oRs)) {
                        return RangeOperations.multirangeOverlapsRange(oLs, RangeOperations.parse(oRs));
                    }
                    if (RangeOperations.isRangeString(oLs) && RangeOperations.isMultirangeOrEmpty(oRs)) {
                        return RangeOperations.multirangeOverlapsRange(oRs, RangeOperations.parse(oLs));
                    }
                    if (RangeOperations.isRangeString(oLs) && RangeOperations.isRangeString(oRs)) {
                        return RangeOperations.parse(oLs).overlaps(RangeOperations.parse(oRs));
                    }
                }
                // Array overlap: check if arrays share any element (NULL elements never match)
                boolean lArr = oLIsList || (oLs.startsWith("{") && !oLs.startsWith("{\""));
                boolean rArr = oRIsList || (oRs.startsWith("{") && !oRs.startsWith("{\""));
                if (lArr && rArr) {
                    List<Object> la = FunctionEvaluator.parseSimplePgArray(oLs);
                    List<Object> ra = FunctionEvaluator.parseSimplePgArray(oRs);
                    return arrayOverlaps(la, ra);
                }
                if (lArr != rArr && (oLIsList || oRIsList)) {
                    throw new MemgresException("malformed array literal: \"" + (lArr ? oRs : oLs) + "\"", "22P02");
                }
                return GeometricOperations.overlaps(oLs, oRs);
            }
            case DISTANCE: {
                if (left == null || right == null) return null;
                // TsQuery <-> TsQuery — phrase operator (distance 1)
                if (left instanceof TsQuery && right instanceof TsQuery) {
                    return TsQuery.phrase((TsQuery) left, (TsQuery) right, 1);
                }
                if (left instanceof TsQuery) return TsQuery.phrase((TsQuery) left, TsQuery.parse(right.toString()), 1);
                if (right instanceof TsQuery) return TsQuery.phrase(TsQuery.parse(left.toString()), (TsQuery) right, 1);
                // Try user-defined operator first (e.g., text <-> text)
                Object udResult = tryUserDefinedOperator("<->", left, right);
                if (udResult != null) return udResult;
                try {
                    return GeometricOperations.distance(left.toString(), right.toString());
                } catch (Exception e) {
                    throw new MemgresException("operator does not exist: " + AstExecutor.pgTypeNameOf(left) + " <-> " + AstExecutor.pgTypeNameOf(right), "42883");
                }
            }
            case APPROX_EQUAL: {
                if (left == null || right == null) return null;
                return GeometricOperations.sameAs(left.toString(), right.toString());
            }
            case GEO_BELOW: {
                if (left == null || right == null) return null;
                return GeometricOperations.isStrictlyBelow(left.toString(), right.toString());
            }
            case GEO_ABOVE: {
                if (left == null || right == null) return null;
                return GeometricOperations.isStrictlyAbove(left.toString(), right.toString());
            }
            case GEO_NOT_EXTEND_RIGHT: {
                if (left == null || right == null) return null;
                return GeometricOperations.doesNotExtendRight(left.toString(), right.toString());
            }
            case GEO_NOT_EXTEND_LEFT: {
                if (left == null || right == null) return null;
                return GeometricOperations.doesNotExtendLeft(left.toString(), right.toString());
            }
            case GEO_NOT_EXTEND_ABOVE: {
                if (left == null || right == null) return null;
                return GeometricOperations.doesNotExtendAbove(left.toString(), right.toString());
            }
            case GEO_NOT_EXTEND_BELOW: {
                if (left == null || right == null) return null;
                return GeometricOperations.doesNotExtendBelow(left.toString(), right.toString());
            }
            case GEO_INTERSECTS: {
                if (left == null || right == null) return null;
                return GeometricOperations.intersects(left.toString(), right.toString());
            }
            case GEO_CLOSEST_POINT: {
                if (left == null || right == null) return null;
                return GeometricOperations.formatPoint(GeometricOperations.closestPoint(left.toString(), right.toString()));
            }
            case GEO_PARALLEL: {
                if (left == null || right == null) return null;
                Object lObj = GeometricOperations.autoDetectPublic(left.toString());
                Object rObj = GeometricOperations.autoDetectPublic(right.toString());
                if (lObj instanceof GeometricOperations.PgLseg && rObj instanceof GeometricOperations.PgLseg) {
                    return GeometricOperations.isParallel((GeometricOperations.PgLseg) lObj, (GeometricOperations.PgLseg) rObj);
                }
                throw new MemgresException("operator ?|| not supported for these types", "42883");
            }
            case GEO_PERPENDICULAR: {
                if (left == null || right == null) return null;
                Object lObj = GeometricOperations.autoDetectPublic(left.toString());
                Object rObj = GeometricOperations.autoDetectPublic(right.toString());
                if (lObj instanceof GeometricOperations.PgLseg && rObj instanceof GeometricOperations.PgLseg) {
                    return GeometricOperations.isPerpendicular((GeometricOperations.PgLseg) lObj, (GeometricOperations.PgLseg) rObj);
                }
                throw new MemgresException("operator ?-| not supported for these types", "42883");
            }
            case REGEX_MATCH: {
                if (left == null || right == null) return null;
                try { return java.util.regex.Pattern.compile(pgRegexToJava(right.toString())).matcher(left.toString()).find(); }
                catch (java.util.regex.PatternSyntaxException e) { throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B"); }
            }
            case REGEX_IMATCH: {
                if (left == null || right == null) return null;
                try { return java.util.regex.Pattern.compile(pgRegexToJava(right.toString()), java.util.regex.Pattern.CASE_INSENSITIVE).matcher(left.toString()).find(); }
                catch (java.util.regex.PatternSyntaxException e) { throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B"); }
            }
            case NOT_REGEX_MATCH: {
                if (left == null || right == null) return null;
                try { return !java.util.regex.Pattern.compile(pgRegexToJava(right.toString())).matcher(left.toString()).find(); }
                catch (java.util.regex.PatternSyntaxException e) { throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B"); }
            }
            case NOT_REGEX_IMATCH: {
                if (left == null || right == null) return null;
                try { return !java.util.regex.Pattern.compile(pgRegexToJava(right.toString()), java.util.regex.Pattern.CASE_INSENSITIVE).matcher(left.toString()).find(); }
                catch (java.util.regex.PatternSyntaxException e) { throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B"); }
            }
            case RANGE_ADJACENT: {
                if (left == null || right == null) return null;
                String ls = left.toString().trim();
                String rs = right.toString().trim();
                if (RangeOperations.isMultirangeOrEmpty(ls) && RangeOperations.isMultirangeOrEmpty(rs)) {
                    return RangeOperations.multirangeAdjacentMultirange(ls, rs);
                }
                if (RangeOperations.isMultirangeOrEmpty(ls) && RangeOperations.isRangeString(rs)) {
                    return RangeOperations.multirangeAdjacentRange(ls, RangeOperations.parse(rs));
                }
                if (RangeOperations.isRangeString(ls) && RangeOperations.isMultirangeOrEmpty(rs)) {
                    return RangeOperations.multirangeAdjacentRange(rs, RangeOperations.parse(ls));
                }
                if (RangeOperations.isRangeString(ls) && RangeOperations.isRangeString(rs)) {
                    return RangeOperations.areAdjacent(RangeOperations.parse(ls), RangeOperations.parse(rs));
                }
                return false;
            }
            default:
                return null;
        }
    }

    /**
     * Try to dispatch a built-in operator token to a user-defined operator function.
     * This handles the case where a built-in operator symbol (like +) is overloaded
     * for custom types.
     */
    /** Recursively collect leaf elements of a (possibly nested) array into a flat list. */
    private static void collectLeafElements(List<?> list, List<Object> out) {
        for (Object o : list) {
            if (o instanceof List<?>) collectLeafElements((List<?>) o, out);
            else out.add(o);
        }
    }

    private static List<Object> leafElements(List<?> list) {
        List<Object> out = new ArrayList<>();
        collectLeafElements(list, out);
        return out;
    }

    /**
     * PG array containment (@>): every element of sub must equal some element of sup.
     * NULL elements never match anything, so a sub containing NULL is never contained.
     */
    private static boolean arrayContainsAll(List<?> sup, List<?> sub) {
        List<Object> supFlat = leafElements(sup);
        for (Object o : leafElements(sub)) {
            if (o == null) return false; // NULL never equals anything
            String os = o.toString();
            boolean found = false;
            for (Object s : supFlat) {
                if (s != null && s.toString().equals(os)) { found = true; break; }
            }
            if (!found) return false;
        }
        return true;
    }

    /**
     * PG array overlap (&&): true if the arrays share any non-NULL element.
     * NULL elements never match anything.
     */
    private static boolean arrayOverlaps(List<?> a, List<?> b) {
        Set<String> as = new HashSet<>();
        for (Object o : leafElements(a)) {
            if (o != null) as.add(o.toString());
        }
        for (Object o : leafElements(b)) {
            if (o != null && as.contains(o.toString())) return true;
        }
        return false;
    }

    Object tryUserDefinedOperator(String opSymbol, Object left, Object right) {
        String leftType = AstExecutor.pgTypeNameOf(left);
        String rightType = AstExecutor.pgTypeNameOf(right);
        ExprEvaluator exprEval = executor.exprEvaluator;
        PgOperator op = exprEval.resolveOperator(null, opSymbol, leftType, rightType);
        if (op == null) return null;

        PgFunction func = executor.database.getFunction(op.getFunction());
        if (func == null) return null;

        if (func.isStrict()) {
            if (left == null || right == null) return null;
        }

        java.util.List<Object> args = new java.util.ArrayList<>();
        args.add(left);
        args.add(right);

        com.memgres.engine.plpgsql.PlpgsqlExecutor plExec =
            new com.memgres.engine.plpgsql.PlpgsqlExecutor(executor, executor.database, executor.session);
        return plExec.executeFunction(func, args);
    }

    /**
     * Map a BinaryExpr.BinOp to its operator symbol string for user-defined operator lookup.
     */
    private static String binOpToSymbol(BinaryExpr.BinOp op) {
        switch (op) {
            case ADD: return "+";
            case SUBTRACT: return "-";
            case MULTIPLY: return "*";
            case DIVIDE: return "/";
            case MODULO: return "%";
            case POWER: return "^";
            case EQUAL: return "=";
            case NOT_EQUAL: return "<>";
            case LESS_THAN: return "<";
            case GREATER_THAN: return ">";
            case LESS_EQUAL: return "<=";
            case GREATER_EQUAL: return ">=";
            case CONCAT: return "||";
            case BIT_AND: return "&";
            case BIT_OR: return "|";
            case BIT_XOR: return "#";
            case SHIFT_LEFT: return "<<";
            case SHIFT_RIGHT: return ">>";
            case CONTAINS: return "@>";
            case CONTAINED_BY: return "<@";
            case OVERLAP: return "&&";
            default: return null;
        }
    }

    private static InetValue toInetValue(Object val) {
        if (val instanceof InetValue) return (InetValue) val;
        return InetValue.parse(val.toString());
    }

    /** Shift a bit string left or right, filling with zeros. Preserves length. */
    private static String shiftBitString(String bits, int shift, boolean leftShift) {
        int len = bits.length();
        if (shift >= len || shift <= -len) {
            // All bits shifted out
            char[] zeros = new char[len];
            java.util.Arrays.fill(zeros, '0');
            return new String(zeros);
        }
        if (shift < 0) {
            // Negative shift reverses direction
            return shiftBitString(bits, -shift, !leftShift);
        }
        if (shift == 0) return bits;
        StringBuilder sb = new StringBuilder(len);
        if (leftShift) {
            sb.append(bits, shift, len);
            for (int i = 0; i < shift; i++) sb.append('0');
        } else {
            for (int i = 0; i < shift; i++) sb.append('0');
            sb.append(bits, 0, len - shift);
        }
        return sb.toString();
    }

    Object evalBinaryValues(BinaryExpr.BinOp op, Object left, Object right) {
        // Apply type validation before computation
        executor.validateOperatorTypes(op, left, right);
        switch (op) {
            case ADD:
                if (left instanceof InetValue && right instanceof Number) {
                    return ((InetValue) left).add(((Number) right).longValue());
                }
                if (left instanceof Number && right instanceof InetValue) {
                    return ((InetValue) right).add(((Number) left).longValue());
                }
                return executor.dateTimeAdd(left, right);
            case SUBTRACT:
                if (left instanceof InetValue && right instanceof Number) {
                    return ((InetValue) left).subtract(((Number) right).longValue());
                }
                if (left instanceof InetValue && right instanceof InetValue) {
                    return ((InetValue) left).subtract((InetValue) right);
                }
                return executor.dateTimeSubtract(left, right);
            case MULTIPLY:
                if (left instanceof InetValue || left instanceof MacaddrValue || left instanceof Macaddr8Value
                        || right instanceof InetValue || right instanceof MacaddrValue || right instanceof Macaddr8Value) {
                    String lt = ConstraintValidator.pgTypeNameOf(left);
                    String rt = ConstraintValidator.pgTypeNameOf(right);
                    throw new MemgresException("operator does not exist: " + lt + " * " + rt, "42883");
                }
                return executor.numericOrIntervalMul(left, right);
            case DIVIDE: {
                if (left instanceof String && right instanceof String
                        && GeometricOperations.isGeometricString(((String) left))) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return GeometricOperations.divide(ls, rs);
                }
                // money / money → float8 (PG behavior)
                if (left instanceof PgMoney && right instanceof PgMoney) {
                    return ((PgMoney) left).divideByMoney((PgMoney) right);
                }
                return executor.numericOp(left, right, (a, b) -> a / b, (a, b) -> a / b,
                    (a, b) -> a.divide(b, 20, java.math.RoundingMode.HALF_UP));
            }
            case MODULO:
                return executor.numericOp(left, right, (a, b) -> a % b, (a, b) -> a % b,
                    java.math.BigDecimal::remainder);
            case POWER: {
                if (left == null || right == null) return null;
                double result = Math.pow(executor.toDouble(left), executor.toDouble(right));
                // Numeric overflow: if either operand is BigDecimal, check for infinity
                if ((left instanceof java.math.BigDecimal || right instanceof java.math.BigDecimal) && Double.isInfinite(result)) {
                    throw new MemgresException("value overflows numeric format", "22003");
                }
                if (result == Math.floor(result) && !Double.isInfinite(result) && Math.abs(result) < Long.MAX_VALUE) {
                    return (long) result;
                }
                return result;
            }
            case BIT_AND: {
                if (left == null || right == null) return null;
                // Bit string AND
                String lBits2 = AstExecutor.toBitStringOrNull(left);
                String rBits2 = AstExecutor.toBitStringOrNull(right);
                if (lBits2 != null && rBits2 != null) {
                    return new AstExecutor.PgBitString(AstExecutor.bitwiseBitString(lBits2, rBits2, '&'));
                }
                // inet/macaddr bitwise AND
                if (left instanceof InetValue && right instanceof InetValue) {
                    return ((InetValue) left).bitwiseAnd((InetValue) right);
                }
                if (left instanceof MacaddrValue && right instanceof MacaddrValue) {
                    return ((MacaddrValue) left).bitwiseAnd((MacaddrValue) right);
                }
                if (left instanceof Macaddr8Value && right instanceof Macaddr8Value) {
                    return ((Macaddr8Value) left).bitwiseAnd((Macaddr8Value) right);
                }
                { long r = executor.toLong(left) & executor.toLong(right);
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE) ? r : (int) r; }
            }
            case BIT_OR: {
                if (left == null || right == null) return null;
                // Bit string OR
                String lBitsOr2 = AstExecutor.toBitStringOrNull(left);
                String rBitsOr2 = AstExecutor.toBitStringOrNull(right);
                if (lBitsOr2 != null && rBitsOr2 != null) {
                    return new AstExecutor.PgBitString(AstExecutor.bitwiseBitString(lBitsOr2, rBitsOr2, '|'));
                }
                // inet/macaddr bitwise OR
                if (left instanceof InetValue && right instanceof InetValue) {
                    return ((InetValue) left).bitwiseOr((InetValue) right);
                }
                if (left instanceof MacaddrValue && right instanceof MacaddrValue) {
                    return ((MacaddrValue) left).bitwiseOr((MacaddrValue) right);
                }
                if (left instanceof Macaddr8Value && right instanceof Macaddr8Value) {
                    return ((Macaddr8Value) left).bitwiseOr((Macaddr8Value) right);
                }
                { long r = executor.toLong(left) | executor.toLong(right);
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE) ? r : (int) r; }
            }
            case BIT_XOR: {
                if (left == null || right == null) return null;
                // Bit string XOR
                String lBitsXor2 = AstExecutor.toBitStringOrNull(left);
                String rBitsXor2 = AstExecutor.toBitStringOrNull(right);
                if (lBitsXor2 != null && rBitsXor2 != null) {
                    return new AstExecutor.PgBitString(AstExecutor.bitwiseBitString(lBitsXor2, rBitsXor2, '#'));
                }
                // Geometric intersection: lseg # lseg, box # box
                if (left instanceof String && right instanceof String
                        && GeometricOperations.isGeometricString(((String) left))) {
                    String rs = (String) right;
                    String ls = (String) left;
                    Object result = GeometricOperations.intersectionGeneral(ls, rs);
                    return result != null ? GeometricOperations.format(result) : null;
                }
                { long r = executor.toLong(left) ^ executor.toLong(right);
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE) ? r : (int) r; }
            }
            case SHIFT_LEFT: {
                if (left == null || right == null) return null;
                // inet << inet: left is strictly contained in right
                if (left instanceof InetValue && right instanceof InetValue) {
                    return ((InetValue) right).contains((InetValue) left);
                }
                { long r = executor.toLong(left) << executor.toLong(right);
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE) ? r : (int) r; }
            }
            case SHIFT_RIGHT: {
                if (left == null || right == null) return null;
                // inet >> inet: left strictly contains right
                if (left instanceof InetValue && right instanceof InetValue) {
                    return ((InetValue) left).contains((InetValue) right);
                }
                { long r = executor.toLong(left) >> executor.toLong(right);
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE) ? r : (int) r; }
            }
            case INET_CONTAINS_EQUALS: {
                if (left == null || right == null) return null;
                InetValue il = toInetValue(left), ir = toInetValue(right);
                return il.containsOrEquals(ir);
            }
            case INET_CONTAINED_BY_EQUALS: {
                if (left == null || right == null) return null;
                InetValue il = toInetValue(left), ir = toInetValue(right);
                return ir.containsOrEquals(il);
            }
            case EQUAL: {
                if (left == null || right == null) return null;
                if (left instanceof List && right instanceof List) {
                    List<?> lList = (List<?>) left;
                    List<?> rList = (List<?>) right;
                    if (lList.size() != rList.size()) return false;
                    for (int ri = 0; ri < lList.size(); ri++) {
                        Object lv = lList.get(ri), rv = rList.get(ri);
                        if (lv == null || rv == null) return null;
                        if (!TypeCoercion.areEqual(lv, rv)) return false;
                    }
                    return true;
                }
                return TypeCoercion.areEqual(left, right);
            }
            case NOT_EQUAL: {
                if (left == null || right == null) return null;
                if (left instanceof List && right instanceof List) {
                    List<?> lList = (List<?>) left;
                    List<?> rList = (List<?>) right;
                    if (lList.size() != rList.size()) return true;
                    for (int ri = 0; ri < lList.size(); ri++) {
                        Object lv = lList.get(ri), rv = rList.get(ri);
                        if (lv == null || rv == null) return null;
                        if (!TypeCoercion.areEqual(lv, rv)) return true;
                    }
                    return false;
                }
                return !TypeCoercion.areEqual(left, right);
            }
            case LESS_THAN: {
                if (left == null || right == null) return null;
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : left instanceof List ? (List<?>) left : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : right instanceof List ? (List<?>) right : null;
                if (lList != null && rList != null) {
                    int minLen = Math.min(lList.size(), rList.size());
                    for (int ri = 0; ri < minLen; ri++) {
                        Object lv = lList.get(ri), rv = rList.get(ri);
                        if (left instanceof AstExecutor.PgRow && (lv == null || rv == null)) return null;
                        int cmp = TypeCoercion.compare(lv, rv);
                        if (cmp != 0) return cmp < 0;
                    }
                    return lList.size() < rList.size();
                }
                return executor.compareValues(left, right) < 0;
            }
            case GREATER_THAN: {
                if (left == null || right == null) return null;
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : left instanceof List ? (List<?>) left : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : right instanceof List ? (List<?>) right : null;
                if (lList != null && rList != null) {
                    int minLen = Math.min(lList.size(), rList.size());
                    for (int ri = 0; ri < minLen; ri++) {
                        Object lv = lList.get(ri), rv = rList.get(ri);
                        if (left instanceof AstExecutor.PgRow && (lv == null || rv == null)) return null;
                        int cmp = TypeCoercion.compare(lv, rv);
                        if (cmp != 0) return cmp > 0;
                    }
                    return lList.size() > rList.size();
                }
                return executor.compareValues(left, right) > 0;
            }
            case LESS_EQUAL: {
                if (left == null || right == null) return null;
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : left instanceof List ? (List<?>) left : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : right instanceof List ? (List<?>) right : null;
                if (lList != null && rList != null) {
                    int minLen = Math.min(lList.size(), rList.size());
                    for (int ri = 0; ri < minLen; ri++) {
                        Object lv = lList.get(ri), rv = rList.get(ri);
                        if (left instanceof AstExecutor.PgRow && (lv == null || rv == null)) return null;
                        int cmp = TypeCoercion.compare(lv, rv);
                        if (cmp != 0) return cmp <= 0;
                    }
                    return lList.size() <= rList.size();
                }
                return executor.compareValues(left, right) <= 0;
            }
            case GREATER_EQUAL: {
                if (left == null || right == null) return null;
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : left instanceof List ? (List<?>) left : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : right instanceof List ? (List<?>) right : null;
                if (lList != null && rList != null) {
                    int minLen = Math.min(lList.size(), rList.size());
                    for (int ri = 0; ri < minLen; ri++) {
                        Object lv = lList.get(ri), rv = rList.get(ri);
                        if (left instanceof AstExecutor.PgRow && (lv == null || rv == null)) return null;
                        int cmp = TypeCoercion.compare(lv, rv);
                        if (cmp != 0) return cmp >= 0;
                    }
                    return lList.size() >= rList.size();
                }
                return executor.compareValues(left, right) >= 0;
            }
            case CONCAT: {
                // hstore || hstore: merge
                if (left instanceof HstoreValue || right instanceof HstoreValue) {
                    if (left == null || right == null) return left != null ? left : right;
                    HstoreValue lh = left instanceof HstoreValue ? (HstoreValue) left : HstoreValue.parse(left.toString());
                    HstoreValue rh = right instanceof HstoreValue ? (HstoreValue) right : HstoreValue.parse(right.toString());
                    return lh.merge(rh);
                }
                // Array concat with NULL: NULL || array = array, array || NULL = array
                if (left == null && right instanceof List) return right;
                if (right == null && left instanceof List) return left;
                if (left == null || right == null) {
                    if (left == null && right != null) {
                        String rs = right.toString().trim();
                        if (rs.startsWith("{") && rs.endsWith("}")) return right;
                    }
                    if (right == null && left != null) {
                        String ls = left.toString().trim();
                        if (ls.startsWith("{") && ls.endsWith("}")) return left;
                    }
                    return null;
                }
                // Bytea (byte[]) concatenation
                if (left instanceof byte[] && right instanceof byte[]) {
                    byte[] lb = (byte[]) left;
                    byte[] rb = (byte[]) right;
                    byte[] result = new byte[lb.length + rb.length];
                    System.arraycopy(lb, 0, result, 0, lb.length);
                    System.arraycopy(rb, 0, result, lb.length, rb.length);
                    return result;
                }
                if (left instanceof byte[]) {
                    byte[] lb = (byte[]) left;
                    byte[] rb = TypeCoercion.toBytea(right);
                    if (rb != null) {
                        byte[] result = new byte[lb.length + rb.length];
                        System.arraycopy(lb, 0, result, 0, lb.length);
                        System.arraycopy(rb, 0, result, lb.length, rb.length);
                        return result;
                    }
                }
                if (right instanceof byte[]) {
                    byte[] rb = (byte[]) right;
                    byte[] lb = TypeCoercion.toBytea(left);
                    if (lb != null) {
                        byte[] result = new byte[lb.length + rb.length];
                        System.arraycopy(lb, 0, result, 0, lb.length);
                        System.arraycopy(rb, 0, result, lb.length, rb.length);
                        return result;
                    }
                }
                // TsQuery || TsQuery — OR of two queries
                if (left instanceof TsQuery && right instanceof TsQuery) return TsQuery.or((TsQuery) left, (TsQuery) right);
                if (left instanceof TsQuery) return TsQuery.or((TsQuery) left, TsQuery.parse(right.toString()));
                if (right instanceof TsQuery) return TsQuery.or(TsQuery.parse(left.toString()), (TsQuery) right);
                if (left instanceof TsVector && right instanceof TsVector) return ((TsVector) left).concat(((TsVector) right));
                if (left instanceof TsVector) return ((TsVector) left).concat(TsVector.fromText(right.toString()));
                if (right instanceof TsVector) return TsVector.fromText(left.toString()).concat(((TsVector) right));
                // Array concatenation: array || array, array || element, element || array
                if (left instanceof List && right instanceof List) {
                    List<?> ll = (List<?>) left;
                    List<?> rl = (List<?>) right;
                    List<Object> merged = new ArrayList<>(ll);
                    merged.addAll(rl);
                    return merged;
                }
                if (left instanceof List) {
                    List<?> ll = (List<?>) left;
                    List<Object> merged = new ArrayList<>(ll);
                    merged.add(right);
                    return merged;
                }
                if (right instanceof List) {
                    List<?> rl = (List<?>) right;
                    List<Object> merged = new ArrayList<>();
                    merged.add(left);
                    merged.addAll(rl);
                    return merged;
                }
                String ls = left.toString();
                String rs = right.toString();
                boolean looksLikePgArrayL = ls.trim().startsWith("{") && ls.trim().endsWith("}") && !ls.trim().startsWith("{\"");
                boolean looksLikePgArrayR = rs.trim().startsWith("{") && rs.trim().endsWith("}") && !rs.trim().startsWith("{\"");
                // PG array string || scalar = array append
                if (looksLikePgArrayL && !looksLikePgArrayR) {
                    List<Object> arr = new ArrayList<>(FunctionEvaluator.parseSimplePgArray(ls));
                    arr.add(right instanceof Number ? right : rs);
                    return TypeCoercion.formatPgArray(arr);
                }
                // scalar || PG array string = array prepend
                if (looksLikePgArrayR && !looksLikePgArrayL) {
                    List<Object> arr = new ArrayList<>();
                    arr.add(left instanceof Number ? left : ls);
                    arr.addAll(FunctionEvaluator.parseSimplePgArray(rs));
                    return TypeCoercion.formatPgArray(arr);
                }
                // PG array string || PG array string = array concat
                if (looksLikePgArrayL && looksLikePgArrayR) {
                    List<Object> arr = new ArrayList<>(FunctionEvaluator.parseSimplePgArray(ls));
                    arr.addAll(FunctionEvaluator.parseSimplePgArray(rs));
                    return TypeCoercion.formatPgArray(arr);
                }
                if ((ls.trim().startsWith("{") || ls.trim().startsWith("[")) &&
                    (rs.trim().startsWith("{") || rs.trim().startsWith("["))) {
                    return JsonOperations.concatenate(ls, rs);
                }
                return ls + rs;
            }
            case IS_DISTINCT_FROM: {
                if (left == null && right == null) return false;
                if (left == null || right == null) return true;
                return !TypeCoercion.areEqual(left, right);
            }
            case IS_NOT_DISTINCT_FROM: {
                if (left == null && right == null) return true;
                if (left == null || right == null) return false;
                return TypeCoercion.areEqual(left, right);
            }
            case TS_MATCH: {
                // JSONB @@ jsonpath
                if (left != null && right != null) {
                    String ls = left.toString().trim();
                    if (ls.startsWith("{") || ls.startsWith("[")) {
                        String path = right.toString().trim();
                        return executor.functionEvaluator.evaluateJsonPathPredicate(ls, path);
                    }
                }
                // NULL @@ tsquery or tsvector @@ NULL → NULL (not false)
                if (left == null || right == null) return null;
                TsVector vec;
                if (left instanceof TsVector) { vec = (TsVector) left; }
                else { String s = left.toString(); TsVector p = TsVector.parseLiteral(s); vec = p != null ? p : TsVector.fromText(s); }
                TsQuery query = right instanceof TsQuery ? (TsQuery) right : TsQuery.parse(right.toString());
                return vec.matches(query);
            }
            case JSON_HASH_ARROW: {
                if (left == null || right == null) return null;
                List<String> path = executor.parseJsonPathArg(right);
                return JsonOperations.extractPath(left.toString().trim(), path);
            }
            case JSON_HASH_ARROW_TEXT: {
                if (left == null || right == null) return null;
                List<String> path = executor.parseJsonPathArg(right);
                return JsonOperations.extractPathText(left.toString().trim(), path);
            }
            case CONTAINS: {
                if (left == null || right == null) return null;
                // hstore @> hstore containment
                if (left instanceof HstoreValue || right instanceof HstoreValue) {
                    HstoreValue lh = left instanceof HstoreValue ? (HstoreValue) left : HstoreValue.parse(left.toString());
                    HstoreValue rh = right instanceof HstoreValue ? (HstoreValue) right : HstoreValue.parse(right.toString());
                    return lh.containsAll(rh);
                }
                // Convert Java Lists to PG array format FIRST so arrays like ARRAY[1,5]
                // (which stringify as "[1, 5]") are never mistaken for range literals
                boolean lIsList = left instanceof List;
                boolean rIsList = right instanceof List;
                String lStr = lIsList ? TypeCoercion.formatPgArray((List<?>) left) : left.toString().trim();
                String rStr = rIsList ? TypeCoercion.formatPgArray((List<?>) right) : right.toString().trim();
                if (!lIsList && !rIsList) {
                    // Multirange containment
                    if (RangeOperations.isMultirangeOrEmpty(lStr)) {
                        if (right instanceof Number) return RangeOperations.multirangeContains(lStr, ((Number) right));
                        try { return RangeOperations.multirangeContains(lStr, Long.parseLong(rStr)); } catch (NumberFormatException ignore) {}
                        return false;
                    }
                    // Range containment
                    if (RangeOperations.isRangeString(lStr)) {
                        RangeOperations.PgRange range = RangeOperations.parse(lStr);
                        if (right instanceof Number) return range.contains(((Number) right));
                        if (right instanceof java.time.LocalDate) return range.contains(((java.time.LocalDate) right).toEpochDay());
                        // range @> multirange: true if range contains every sub-range
                        if (RangeOperations.isMultirangeOrEmpty(rStr)) {
                            return RangeOperations.multirangeContainsMultirange("{" + lStr + "}", rStr);
                        }
                        if (RangeOperations.isRangeString(rStr)) return range.containsRange(RangeOperations.parse(rStr));
                        if (rStr.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
                            return range.contains(java.time.LocalDate.parse(rStr.substring(0, 10)).toEpochDay());
                        }
                        try { return range.contains(Long.parseLong(rStr)); } catch (NumberFormatException ignore) {}
                        try { return range.contains(new java.math.BigDecimal(rStr)); } catch (NumberFormatException ignore) {}
                    }
                }
                // PG array containment (NULL elements never match)
                boolean lIsPgArray = lIsList || (lStr.startsWith("{") && !lStr.startsWith("{\""));
                boolean rIsPgArray = rIsList || (rStr.startsWith("{") && !rStr.startsWith("{\""));
                if (lIsPgArray && rIsPgArray) {
                    return arrayContainsAll(FunctionEvaluator.parseSimplePgArray(lStr), FunctionEvaluator.parseSimplePgArray(rStr));
                }
                if ((lStr.startsWith("{") || lStr.startsWith("[")) && (rStr.startsWith("{") || rStr.startsWith("["))) {
                    return JsonOperations.contains(lStr, rStr);
                }
                // jsonb containment with a scalar operand (e.g. '[1,2,3]'::jsonb @> '3'::jsonb)
                if (JsonOperations.isJsonScalar(rStr)
                        && (lStr.startsWith("{") || lStr.startsWith("[") || JsonOperations.isJsonScalar(lStr))) {
                    return JsonOperations.contains(lStr, rStr);
                }
                if (GeometricOperations.isGeometricString(lStr) || GeometricOperations.isGeometricString(rStr)) {
                    return GeometricOperations.contains(lStr, rStr);
                }
                return false;
            }
            case CONTAINED_BY: {
                if (left == null || right == null) return null;
                // hstore <@ hstore: contained-by
                if (left instanceof HstoreValue || right instanceof HstoreValue) {
                    HstoreValue lh = left instanceof HstoreValue ? (HstoreValue) left : HstoreValue.parse(left.toString());
                    HstoreValue rh = right instanceof HstoreValue ? (HstoreValue) right : HstoreValue.parse(right.toString());
                    return lh.containedBy(rh);
                }
                // Convert Java Lists to PG array format FIRST so arrays like ARRAY[1,5]
                // (which stringify as "[1, 5]") are never mistaken for range literals
                boolean lIsList = left instanceof List;
                boolean rIsList = right instanceof List;
                String lStr = lIsList ? TypeCoercion.formatPgArray((List<?>) left) : left.toString().trim();
                String rStr = rIsList ? TypeCoercion.formatPgArray((List<?>) right) : right.toString().trim();
                if (!lIsList && !rIsList) {
                    // Multirange/range containment: a <@ b means b @> a
                    if (RangeOperations.isMultirangeOrEmpty(rStr)) {
                        if (RangeOperations.isMultirangeOrEmpty(lStr)) return RangeOperations.multirangeContainsMultirange(rStr, lStr);
                        if (RangeOperations.isRangeString(lStr)) return RangeOperations.multirangeContainsRange(rStr, RangeOperations.parse(lStr));
                        if (left instanceof Number) return RangeOperations.multirangeContains(rStr, ((Number) left));
                        try { return RangeOperations.multirangeContains(rStr, Long.parseLong(lStr)); } catch (NumberFormatException ignore) {}
                        return false;
                    }
                    if (RangeOperations.isRangeString(rStr)) {
                        RangeOperations.PgRange range = RangeOperations.parse(rStr);
                        // multirange <@ range: true if range contains every sub-range
                        if (RangeOperations.isMultirangeOrEmpty(lStr)) {
                            return RangeOperations.multirangeContainsMultirange("{" + rStr + "}", lStr);
                        }
                        if (RangeOperations.isRangeString(lStr)) return range.containsRange(RangeOperations.parse(lStr));
                        if (left instanceof Number) return range.contains(((Number) left));
                        try { return range.contains(Long.parseLong(lStr)); } catch (NumberFormatException ignore) {}
                        try { return range.contains(new java.math.BigDecimal(lStr)); } catch (NumberFormatException ignore) {}
                    }
                }
                // PG array containment (NULL elements never match)
                boolean lIsPgArray = lIsList || (lStr.startsWith("{") && !lStr.startsWith("{\""));
                boolean rIsPgArray = rIsList || (rStr.startsWith("{") && !rStr.startsWith("{\""));
                if (lIsPgArray && rIsPgArray) {
                    return arrayContainsAll(FunctionEvaluator.parseSimplePgArray(rStr), FunctionEvaluator.parseSimplePgArray(lStr));
                }
                if ((lStr.startsWith("{") || lStr.startsWith("[")) && (rStr.startsWith("{") || rStr.startsWith("["))) {
                    return JsonOperations.contains(rStr, lStr);
                }
                // jsonb scalar <@ jsonb container/scalar (e.g. '3'::jsonb <@ '[1,2,3]'::jsonb)
                if (JsonOperations.isJsonScalar(lStr)
                        && (rStr.startsWith("{") || rStr.startsWith("[") || JsonOperations.isJsonScalar(rStr))) {
                    return JsonOperations.contains(rStr, lStr);
                }
                if (GeometricOperations.isGeometricString(lStr) || GeometricOperations.isGeometricString(rStr)) {
                    return GeometricOperations.contains(rStr, lStr);
                }
                return false;
            }
            case JSONB_EXISTS: {
                if (left == null || right == null) return null;
                if (left instanceof HstoreValue) return ((HstoreValue) left).getData().containsKey(right.toString());
                return JsonOperations.keyExists(left.toString(), right.toString());
            }
            case JSONB_EXISTS_ANY: {
                if (left == null || right == null) return null;
                if (left instanceof HstoreValue) {
                    List<String> hkeys = right instanceof List ? ((List<?>) right).stream().map(Object::toString).collect(Collectors.toList()) : Cols.listOf(right.toString());
                    HstoreValue h = (HstoreValue) left;
                    for (String k : hkeys) { if (h.getData().containsKey(k)) return true; }
                    return false;
                }
                // ?| requires text[] on right side
                if (right instanceof String && !((String) right).trim().startsWith("{") && !(right instanceof List<?>)) {
                    String rs = (String) right;
                    throw new MemgresException("malformed array literal: \"" + right + "\"", "22P02");
                }
                List<String> keys = right instanceof List ? ((List<?>) right).stream().map(Object::toString).collect(Collectors.toList()) : Cols.listOf(right.toString());
                return JsonOperations.anyKeyExists(left.toString(), keys);
            }
            case JSONB_EXISTS_ALL: {
                if (left == null || right == null) return null;
                if (left instanceof HstoreValue) {
                    List<String> hkeys = right instanceof List ? ((List<?>) right).stream().map(Object::toString).collect(Collectors.toList()) : Cols.listOf(right.toString());
                    HstoreValue h = (HstoreValue) left;
                    for (String k : hkeys) { if (!h.getData().containsKey(k)) return false; }
                    return true;
                }
                List<String> keys = right instanceof List ? ((List<?>) right).stream().map(Object::toString).collect(Collectors.toList()) : Cols.listOf(right.toString());
                return JsonOperations.allKeysExist(left.toString(), keys);
            }
            case JSONB_PATH_EXISTS_OP: {
                // @? operator, equivalent to jsonb_path_exists
                if (left == null || right == null) return null;
                return executor.functionEvaluator.evaluateJsonPathExists(left.toString().trim(), right.toString().trim());
            }
            case JSON_DELETE_PATH: {
                if (left == null || right == null) return null;
                List<String> path = executor.parseJsonPathArg(right);
                return JsonOperations.deletePath(left.toString(), path);
            }
            case OVERLAP: {
                if (left == null || right == null) return null;
                // inet && inet: overlap (networks share any addresses)
                if (left instanceof InetValue && right instanceof InetValue) {
                    InetValue il = (InetValue) left, ir = (InetValue) right;
                    return il.containsOrEquals(ir) || ir.containsOrEquals(il);
                }
                // TsQuery && TsQuery — AND of two queries
                if (left instanceof TsQuery && right instanceof TsQuery) {
                    return TsQuery.and((TsQuery) left, (TsQuery) right);
                }
                if (left instanceof TsQuery) return TsQuery.and((TsQuery) left, TsQuery.parse(right.toString()));
                if (right instanceof TsQuery) return TsQuery.and(TsQuery.parse(left.toString()), (TsQuery) right);
                // Convert Lists to PG format for uniform handling
                boolean oLIsList = left instanceof List;
                boolean oRIsList = right instanceof List;
                String oLs = oLIsList ? TypeCoercion.formatPgArray((List<?>) left) : left.toString().trim();
                String oRs = oRIsList ? TypeCoercion.formatPgArray((List<?>) right) : right.toString().trim();
                // Range/multirange semantics only apply when neither operand is a real array
                if (!oLIsList && !oRIsList) {
                    // Multirange overlap checks
                    if (RangeOperations.isMultirangeOrEmpty(oLs) && RangeOperations.isMultirangeOrEmpty(oRs)) {
                        return RangeOperations.multirangeOverlapsMultirange(oLs, oRs);
                    }
                    if (RangeOperations.isMultirangeOrEmpty(oLs) && RangeOperations.isRangeString(oRs)) {
                        return RangeOperations.multirangeOverlapsRange(oLs, RangeOperations.parse(oRs));
                    }
                    if (RangeOperations.isRangeString(oLs) && RangeOperations.isMultirangeOrEmpty(oRs)) {
                        return RangeOperations.multirangeOverlapsRange(oRs, RangeOperations.parse(oLs));
                    }
                    if (RangeOperations.isRangeString(oLs) && RangeOperations.isRangeString(oRs)) {
                        return RangeOperations.parse(oLs).overlaps(RangeOperations.parse(oRs));
                    }
                }
                // Array overlap: check if arrays share any element (NULL elements never match)
                boolean lArr = oLIsList || (oLs.startsWith("{") && !oLs.startsWith("{\""));
                boolean rArr = oRIsList || (oRs.startsWith("{") && !oRs.startsWith("{\""));
                if (lArr && rArr) {
                    List<Object> la = FunctionEvaluator.parseSimplePgArray(oLs);
                    List<Object> ra = FunctionEvaluator.parseSimplePgArray(oRs);
                    return arrayOverlaps(la, ra);
                }
                if (lArr != rArr && (oLIsList || oRIsList)) {
                    throw new MemgresException("malformed array literal: \"" + (lArr ? oRs : oLs) + "\"", "22P02");
                }
                return GeometricOperations.overlaps(oLs, oRs);
            }
            case DISTANCE: {
                if (left == null || right == null) return null;
                // TsQuery <-> TsQuery — phrase operator (distance 1)
                if (left instanceof TsQuery && right instanceof TsQuery) {
                    return TsQuery.phrase((TsQuery) left, (TsQuery) right, 1);
                }
                if (left instanceof TsQuery) return TsQuery.phrase((TsQuery) left, TsQuery.parse(right.toString()), 1);
                if (right instanceof TsQuery) return TsQuery.phrase(TsQuery.parse(left.toString()), (TsQuery) right, 1);
                // Try user-defined operator first (e.g., text <-> text)
                Object udResult = tryUserDefinedOperator("<->", left, right);
                if (udResult != null) return udResult;
                try {
                    return GeometricOperations.distance(left.toString(), right.toString());
                } catch (Exception e) {
                    throw new MemgresException("operator does not exist: " + AstExecutor.pgTypeNameOf(left) + " <-> " + AstExecutor.pgTypeNameOf(right), "42883");
                }
            }
            case APPROX_EQUAL: {
                if (left == null || right == null) return null;
                return GeometricOperations.sameAs(left.toString(), right.toString());
            }
            case GEO_BELOW: {
                if (left == null || right == null) return null;
                return GeometricOperations.isStrictlyBelow(left.toString(), right.toString());
            }
            case GEO_ABOVE: {
                if (left == null || right == null) return null;
                return GeometricOperations.isStrictlyAbove(left.toString(), right.toString());
            }
            case GEO_NOT_EXTEND_RIGHT: {
                if (left == null || right == null) return null;
                return GeometricOperations.doesNotExtendRight(left.toString(), right.toString());
            }
            case GEO_NOT_EXTEND_LEFT: {
                if (left == null || right == null) return null;
                return GeometricOperations.doesNotExtendLeft(left.toString(), right.toString());
            }
            case GEO_NOT_EXTEND_ABOVE: {
                if (left == null || right == null) return null;
                return GeometricOperations.doesNotExtendAbove(left.toString(), right.toString());
            }
            case GEO_NOT_EXTEND_BELOW: {
                if (left == null || right == null) return null;
                return GeometricOperations.doesNotExtendBelow(left.toString(), right.toString());
            }
            case GEO_INTERSECTS: {
                if (left == null || right == null) return null;
                return GeometricOperations.intersects(left.toString(), right.toString());
            }
            case GEO_CLOSEST_POINT: {
                if (left == null || right == null) return null;
                return GeometricOperations.formatPoint(GeometricOperations.closestPoint(left.toString(), right.toString()));
            }
            case GEO_PARALLEL: {
                if (left == null || right == null) return null;
                Object lObj = GeometricOperations.autoDetectPublic(left.toString());
                Object rObj = GeometricOperations.autoDetectPublic(right.toString());
                if (lObj instanceof GeometricOperations.PgLseg && rObj instanceof GeometricOperations.PgLseg) {
                    return GeometricOperations.isParallel((GeometricOperations.PgLseg) lObj, (GeometricOperations.PgLseg) rObj);
                }
                throw new MemgresException("operator ?|| not supported for these types", "42883");
            }
            case GEO_PERPENDICULAR: {
                if (left == null || right == null) return null;
                Object lObj = GeometricOperations.autoDetectPublic(left.toString());
                Object rObj = GeometricOperations.autoDetectPublic(right.toString());
                if (lObj instanceof GeometricOperations.PgLseg && rObj instanceof GeometricOperations.PgLseg) {
                    return GeometricOperations.isPerpendicular((GeometricOperations.PgLseg) lObj, (GeometricOperations.PgLseg) rObj);
                }
                throw new MemgresException("operator ?-| not supported for these types", "42883");
            }
            case REGEX_MATCH: {
                if (left == null || right == null) return null;
                try { return java.util.regex.Pattern.compile(pgRegexToJava(right.toString())).matcher(left.toString()).find(); }
                catch (java.util.regex.PatternSyntaxException e) { throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B"); }
            }
            case REGEX_IMATCH: {
                if (left == null || right == null) return null;
                try { return java.util.regex.Pattern.compile(pgRegexToJava(right.toString()), java.util.regex.Pattern.CASE_INSENSITIVE).matcher(left.toString()).find(); }
                catch (java.util.regex.PatternSyntaxException e) { throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B"); }
            }
            case NOT_REGEX_MATCH: {
                if (left == null || right == null) return null;
                try { return !java.util.regex.Pattern.compile(pgRegexToJava(right.toString())).matcher(left.toString()).find(); }
                catch (java.util.regex.PatternSyntaxException e) { throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B"); }
            }
            case NOT_REGEX_IMATCH: {
                if (left == null || right == null) return null;
                try { return !java.util.regex.Pattern.compile(pgRegexToJava(right.toString()), java.util.regex.Pattern.CASE_INSENSITIVE).matcher(left.toString()).find(); }
                catch (java.util.regex.PatternSyntaxException e) { throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B"); }
            }
            case RANGE_ADJACENT: {
                if (left == null || right == null) return null;
                String ls = left.toString().trim();
                String rs = right.toString().trim();
                if (RangeOperations.isMultirangeOrEmpty(ls) && RangeOperations.isMultirangeOrEmpty(rs)) {
                    return RangeOperations.multirangeAdjacentMultirange(ls, rs);
                }
                if (RangeOperations.isMultirangeOrEmpty(ls) && RangeOperations.isRangeString(rs)) {
                    return RangeOperations.multirangeAdjacentRange(ls, RangeOperations.parse(rs));
                }
                if (RangeOperations.isRangeString(ls) && RangeOperations.isMultirangeOrEmpty(rs)) {
                    return RangeOperations.multirangeAdjacentRange(rs, RangeOperations.parse(ls));
                }
                if (RangeOperations.isRangeString(ls) && RangeOperations.isRangeString(rs)) {
                    return RangeOperations.areAdjacent(RangeOperations.parse(ls), RangeOperations.parse(rs));
                }
                return false;
            }
            case LIKE: {
                if (left == null || right == null) return null;
                if (left instanceof Number || left instanceof Boolean) {
                    String tn = left instanceof Integer ? "integer" : left instanceof Long ? "bigint" :
                            left instanceof Boolean ? "boolean" : left.getClass().getSimpleName().toLowerCase();
                    throw new MemgresException("operator does not exist: " + tn + " ~~ unknown", "42883");
                }
                String likePattern = AstExecutor.likeToRegex(right.toString());
                return left.toString().matches("(?s)" + likePattern);
            }
            case ILIKE: {
                if (left == null || right == null) return null;
                String ilikePattern = AstExecutor.likeToRegex(right.toString());
                return left.toString().matches("(?si)" + ilikePattern);
            }
            case SIMILAR_TO: {
                if (left == null || right == null) return null;
                String simPattern = similarToRegexForBinaryOp(right.toString(), "\\");
                return left.toString().matches("(?s)" + simPattern);
            }
            case JSON_ARROW: {
                if (left == null || right == null) return null;
                if (left instanceof HstoreValue && right instanceof List) {
                    HstoreValue h = (HstoreValue) left;
                    java.util.List<String> result = new java.util.ArrayList<>();
                    for (Object k : (java.util.List<?>) right) {
                        result.add(k != null ? h.get(k.toString()) : null);
                    }
                    return result;
                }
                if (left instanceof HstoreValue) {
                    return ((HstoreValue) left).get(right.toString());
                }
                // Object key access on JSON string
                String json = left.toString().trim();
                String key = right.toString();
                return executor.functionEvaluator.extractJsonKey(json, key);
            }
            case JSON_ARROW_TEXT: {
                if (left == null || right == null) return null;
                if (left instanceof HstoreValue) {
                    return ((HstoreValue) left).get(right.toString());
                }
                // json ->> key (returns text; JSON null maps to SQL NULL,
                // strings are unquoted and unescaped)
                String jsonStr = left.toString().trim();
                Object extracted = executor.functionEvaluator.extractJsonKey(jsonStr, right.toString());
                return JsonOperations.jsonValueToText(extracted == null ? null : extracted.toString());
            }
            default:
                return null;
        }
    }

    /**
     * Convert PG-specific regex flags to Java equivalents.
     * PG (?n) = newline-sensitive (. excludes \n, ^ and $ match at line boundaries) → Java (?m)
     * PG (?x) = extended (ignore whitespace and # comments) → Java (?x)
     * PG (?s) = . matches everything including \n → Java (?s)
     * PG (?i) = case insensitive → Java (?i)
     */
    static String pgRegexToJava(String pattern) {
        // PG embedded flags: (?n) → Java (?m) (MULTILINE)
        pattern = pattern.replace("(?n)", "(?m)");
        // Convert POSIX bracket classes [[:digit:]] → \\p{Digit} etc.
        pattern = pattern.replace("[:alpha:]", "\\p{Alpha}");
        pattern = pattern.replace("[:digit:]", "\\p{Digit}");
        pattern = pattern.replace("[:alnum:]", "\\p{Alnum}");
        pattern = pattern.replace("[:upper:]", "\\p{Upper}");
        pattern = pattern.replace("[:lower:]", "\\p{Lower}");
        pattern = pattern.replace("[:space:]", "\\p{Space}");
        pattern = pattern.replace("[:print:]", "\\p{Print}");
        pattern = pattern.replace("[:punct:]", "\\p{Punct}");
        pattern = pattern.replace("[:cntrl:]", "\\p{Cntrl}");
        pattern = pattern.replace("[:xdigit:]", "\\p{XDigit}");
        pattern = pattern.replace("[:graph:]", "\\p{Graph}");
        pattern = pattern.replace("[:blank:]", "\\p{Blank}");
        // PG word boundaries: \m (begin), \M (end), \y (either) → Java \b
        pattern = pattern.replace("\\m", "\\b");
        pattern = pattern.replace("\\M", "\\b");
        pattern = pattern.replace("\\y", "\\b");
        // Fix $ anchor: Java $ matches before trailing \n even without MULTILINE.
        // PG $ matches only at end of string (unless (?n) is set, already converted to (?m)).
        // Replace bare $ with \z (absolute end) — skip $ inside [...] or preceded by \.
        if (pattern.contains("$") && !pattern.contains("(?m)")) {
            StringBuilder sb = new StringBuilder();
            boolean inCharClass = false;
            for (int i = 0; i < pattern.length(); i++) {
                char c = pattern.charAt(i);
                if (c == '\\' && i + 1 < pattern.length()) {
                    sb.append(c).append(pattern.charAt(i + 1));
                    i++;
                } else if (c == '[' && !inCharClass) {
                    inCharClass = true;
                    sb.append(c);
                } else if (c == ']' && inCharClass) {
                    inCharClass = false;
                    sb.append(c);
                } else if (c == '$' && !inCharClass) {
                    sb.append("\\z");
                } else {
                    sb.append(c);
                }
            }
            pattern = sb.toString();
        }
        return pattern;
    }

    /**
     * Convert a SQL SIMILAR TO pattern to a Java regex.
     * Handles: % -> .*, _ -> ., |, (), +, *, ?, [...] (including POSIX classes),
     * and escape character processing.
     */
    private static String similarToRegexForBinaryOp(String pattern, String escapeChar) {
        StringBuilder sb = new StringBuilder();
        String esc = escapeChar != null && !escapeChar.isEmpty() ? escapeChar : "\\";
        int i = 0;
        while (i < pattern.length()) {
            char ch = pattern.charAt(i);
            String chStr = String.valueOf(ch);
            if (chStr.equals(esc) && i + 1 < pattern.length()) {
                // Escaped character, treat next char as literal
                sb.append(java.util.regex.Pattern.quote(pattern.substring(i + 1, i + 2)));
                i += 2;
            } else if (ch == '%') {
                sb.append(".*");
                i++;
            } else if (ch == '_') {
                sb.append(".");
                i++;
            } else if (ch == '|' || ch == '(' || ch == ')' || ch == '+' || ch == '*' || ch == '?') {
                sb.append(ch);
                i++;
            } else if (ch == '{') {
                // Pass through bounded quantifier like {2}, {1,3} as-is
                int end = pattern.indexOf('}', i);
                if (end >= 0) {
                    sb.append(pattern, i, end + 1);
                    i = end + 1;
                } else {
                    sb.append(java.util.regex.Pattern.quote(chStr));
                    i++;
                }
            } else if (ch == '[') {
                // Pass character class through, converting POSIX classes to Java equivalents
                // Find closing ']' that isn't part of a POSIX class like [:alpha:]
                int end = -1;
                {
                    int depth = 0;
                    for (int j = i + 1; j < pattern.length(); j++) {
                        if (pattern.charAt(j) == '[' && j + 1 < pattern.length() && pattern.charAt(j + 1) == ':') {
                            depth++;
                        } else if (pattern.charAt(j) == ']') {
                            if (depth > 0 && j > 0 && pattern.charAt(j - 1) == ':') {
                                depth--;
                            } else {
                                end = j;
                                break;
                            }
                        }
                    }
                }
                if (end >= 0) {
                    String cls = pattern.substring(i, end + 1);
                    // Convert POSIX classes to Java regex equivalents
                    cls = cls.replace("[:alpha:]", "\\p{Alpha}");
                    cls = cls.replace("[:digit:]", "\\p{Digit}");
                    cls = cls.replace("[:alnum:]", "\\p{Alnum}");
                    cls = cls.replace("[:upper:]", "\\p{Upper}");
                    cls = cls.replace("[:lower:]", "\\p{Lower}");
                    cls = cls.replace("[:space:]", "\\p{Space}");
                    cls = cls.replace("[:print:]", "\\p{Print}");
                    cls = cls.replace("[:punct:]", "\\p{Punct}");
                    cls = cls.replace("[:cntrl:]", "\\p{Cntrl}");
                    cls = cls.replace("[:xdigit:]", "\\p{XDigit}");
                    cls = cls.replace("[:graph:]", "\\p{Graph}");
                    cls = cls.replace("[:blank:]", "\\p{Blank}");
                    sb.append(cls);
                    i = end + 1;
                } else {
                    sb.append(java.util.regex.Pattern.quote(chStr));
                    i++;
                }
            } else {
                sb.append(java.util.regex.Pattern.quote(chStr));
                i++;
            }
        }
        return sb.toString();
    }
}
