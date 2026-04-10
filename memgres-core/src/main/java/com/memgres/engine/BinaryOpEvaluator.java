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

        // json type (not jsonb) does not support || or - operators
        if (bin.op() == BinaryExpr.BinOp.CONCAT || bin.op() == BinaryExpr.BinOp.SUBTRACT) {
            if (isCastToType(bin.left(), "json") || isCastToType(bin.right(), "json")) {
                String opSym = bin.op() == BinaryExpr.BinOp.CONCAT ? "||" : "-";
                throw new MemgresException("operator does not exist: json " + opSym + " json", "42883");
            }
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
                return executor.dateTimeAdd(left, right);
            case SUBTRACT:
                return executor.dateTimeSubtract(left, right);
            case MULTIPLY:
                return executor.numericOrIntervalMul(left, right);
            case DIVIDE: {
                if (left instanceof String && right instanceof String
                        && GeometricOperations.isGeometricString(((String) left))) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return GeometricOperations.divide(ls, rs);
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
                // inet bitwise AND
                if (left instanceof String && right instanceof String && ((String) left).contains(".") && ((String) right).contains(".")) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return NetworkOperations.bitwiseAnd(ls, rs);
                }
                return (int)(executor.toLong(left) & executor.toLong(right));
            }
            case BIT_OR: {
                if (left == null || right == null) return null;
                // Bit string OR
                String lBitsOr = AstExecutor.toBitStringOrNull(left);
                String rBitsOr = AstExecutor.toBitStringOrNull(right);
                if (lBitsOr != null && rBitsOr != null) {
                    return new AstExecutor.PgBitString(AstExecutor.bitwiseBitString(lBitsOr, rBitsOr, '|'));
                }
                // inet bitwise OR
                if (left instanceof String && right instanceof String && ((String) left).contains(".") && ((String) right).contains(".")) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return NetworkOperations.bitwiseOr(ls, rs);
                }
                return (int)(executor.toLong(left) | executor.toLong(right));
            }
            case BIT_XOR: {
                if (left == null || right == null) return null;
                // Bit string XOR
                String lBitsXor = AstExecutor.toBitStringOrNull(left);
                String rBitsXor = AstExecutor.toBitStringOrNull(right);
                if (lBitsXor != null && rBitsXor != null) {
                    return new AstExecutor.PgBitString(AstExecutor.bitwiseBitString(lBitsXor, rBitsXor, '#'));
                }
                // Geometric intersection: lseg # lseg
                if (left instanceof String && right instanceof String
                        && GeometricOperations.isGeometricString(((String) left))) {
                    String rs = (String) right;
                    String ls = (String) left;
                    GeometricOperations.PgPoint pt = GeometricOperations.intersection(ls, rs);
                    return pt != null ? GeometricOperations.format(pt) : null;
                }
                return (int)(executor.toLong(left) ^ executor.toLong(right));
            }
            case SHIFT_LEFT: {
                if (left == null || right == null) return null;
                // Check for geometric "strictly left of": box << box
                if (left instanceof String && right instanceof String
                        && GeometricOperations.isGeometricString(((String) left))) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return GeometricOperations.isStrictlyLeft(ls, rs);
                }
                // Check for inet containment: inet << inet
                if (left instanceof String && right instanceof String && ((String) left).contains(".") && ((String) right).contains(".")) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return NetworkOperations.containedBy(ls, rs);
                }
                return (int)(executor.toLong(left) << executor.toLong(right));
            }
            case SHIFT_RIGHT: {
                if (left == null || right == null) return null;
                // Check for geometric "strictly right of": box >> box
                if (left instanceof String && right instanceof String
                        && GeometricOperations.isGeometricString(((String) left))) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return GeometricOperations.isStrictlyRight(ls, rs);
                }
                // Check for inet containment: inet >> inet
                if (left instanceof String && right instanceof String && ((String) left).contains(".") && ((String) right).contains(".")) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return NetworkOperations.contains(ls, rs);
                }
                return (int)(executor.toLong(left) >> executor.toLong(right));
            }
            case INET_CONTAINS_EQUALS: {
                if (left == null || right == null) return null;
                return NetworkOperations.containsOrEquals(left.toString(), right.toString());
            }
            case INET_CONTAINED_BY_EQUALS: {
                if (left == null || right == null) return null;
                return NetworkOperations.containedByOrEquals(left.toString(), right.toString());
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
                // Handle ROW comparison
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : left instanceof List ? (List<?>) left : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : right instanceof List ? (List<?>) right : null;
                if (lList != null && rList != null) {
                    if (lList.size() != rList.size()) throw new MemgresException("cannot compare row values of different sizes", "42601");
                    for (int ri = 0; ri < lList.size(); ri++) {
                        Object lv = lList.get(ri), rv = rList.get(ri);
                        if (lv == null || rv == null) return null;
                        if (!TypeCoercion.areEqual(lv, rv)) return false;
                    }
                    return true;
                }
                // PG rejects implicit text = integer comparison
                if ((left instanceof String && (right instanceof Integer || right instanceof Long))
                        || (right instanceof String && (left instanceof Integer || left instanceof Long))) {
                    // But allow if the string is a numeric literal (e.g., WHERE id = '5')
                    String sVal = left instanceof String ? (String) left : (String) right;
                    try { Long.parseLong(sVal); } catch (NumberFormatException e) {
                        throw new MemgresException("operator does not exist: text = integer", "42883");
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
                if (left == null || right == null) return null;
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
                // SIMILAR TO uses SQL regex: % -> .*, _ -> ., rest is standard regex
                String pattern = right.toString()
                        .replace("%", ".*")
                        .replace("_", ".");
                return left.toString().matches(pattern);
            }
            case JSON_ARROW: {
                // json -> key or json -> index (returns JSON), also array subscript
                if (left == null || right == null) return null;
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
                        String arrElem = JsonOperations.extractArrayElement(leftJsonStr, n.intValue());
                        if (arrElem != null) {
                            arrElem = arrElem.trim();
                            if (arrElem.startsWith("\"") && arrElem.endsWith("\"")) arrElem = arrElem.substring(1, arrElem.length() - 1);
                            if (arrElem.equals("null")) return null;
                            return arrElem;
                        }
                        return null;
                    }
                }
                // Object key access on JSON string; ->> returns text (unquoted)
                String json2 = left.toString().trim();
                String key2 = right.toString();
                String result2 = executor.functionEvaluator.extractJsonKey(json2, key2);
                if (result2 != null && result2.startsWith("\"") && result2.endsWith("\"")) {
                    result2 = result2.substring(1, result2.length() - 1);
                }
                // ->> returns SQL NULL for JSON null values (unlike -> which returns "null")
                if (result2 != null && result2.trim().equals("null")) {
                    return null;
                }
                return result2;
            }
            case TS_MATCH: {
                if (left == null || right == null) return false;
                // JSONB @@ jsonpath
                String ls = left.toString().trim();
                if (ls.startsWith("{") || ls.startsWith("[")) {
                    String path = right.toString().trim();
                    return executor.functionEvaluator.evaluateJsonPathPredicate(ls, path);
                }
                TsVector vec = left instanceof TsVector ? (TsVector) left : TsVector.fromText(left.toString());
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
                String ls = left.toString().trim();
                String rs = right.toString().trim();
                // Multirange containment: multirange @> value/range/multirange
                if (RangeOperations.isMultirangeOrEmpty(ls)) {
                    if (RangeOperations.isMultirangeOrEmpty(rs)) return RangeOperations.multirangeContainsMultirange(ls, rs);
                    if (RangeOperations.isRangeString(rs)) return RangeOperations.multirangeContainsRange(ls, RangeOperations.parse(rs));
                    if (right instanceof Number) return RangeOperations.multirangeContains(ls, ((Number) right));
                    try { return RangeOperations.multirangeContains(ls, Long.parseLong(rs)); } catch (NumberFormatException ignore) {}
                    return false;
                }
                // Range containment: range @> value or range @> range
                if (RangeOperations.isRangeString(ls)) {
                    RangeOperations.PgRange range = RangeOperations.parse(ls);
                    if (right instanceof Number) return range.contains(((Number) right));
                    if (right instanceof java.time.LocalDateTime) return range.contains(((java.time.LocalDateTime) right).toEpochSecond(java.time.ZoneOffset.UTC));
                    if (right instanceof java.time.LocalDate) return range.contains(((java.time.LocalDate) right).toEpochDay());
                    if (RangeOperations.isRangeString(rs)) return range.containsRange(RangeOperations.parse(rs));
                    // PG: range @> non-range-non-number -> try parsing
                    try {
                        return range.contains(Long.parseLong(rs));
                    } catch (NumberFormatException e) {
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
                // PG array containment: {1,2,3} @> {2,3}
                // Convert Lists to PG array strings for uniform handling
                if (left instanceof List) ls = TypeCoercion.formatPgArray((List<?>) left);
                if (right instanceof List) rs = TypeCoercion.formatPgArray((List<?>) right);
                boolean lIsPgArray = ls.startsWith("{") && !ls.startsWith("{\"");
                boolean rIsPgArray = rs.startsWith("{") && !rs.startsWith("{\"");
                if (lIsPgArray && rIsPgArray) {
                    List<Object> la = new ArrayList<>(FunctionEvaluator.parseSimplePgArray(ls));
                    List<Object> ra = new ArrayList<>(FunctionEvaluator.parseSimplePgArray(rs));
                    // String-based containsAll for PG arrays (compare as strings)
                    List<String> laStr = la.stream().map(o -> o == null ? "NULL" : o.toString()).collect(Collectors.toList());
                    List<String> raStr = ra.stream().map(o -> o == null ? "NULL" : o.toString()).collect(Collectors.toList());
                    return laStr.containsAll(raStr);
                }
                if (lIsPgArray && !rIsPgArray) {
                    throw new MemgresException("malformed array literal: \"" + rs + "\"", "22P02");
                }
                // JSON containment
                if ((ls.startsWith("{") || ls.startsWith("[")) && (rs.startsWith("{") || rs.startsWith("["))) {
                    return JsonOperations.contains(ls, rs);
                }
                if (left instanceof List && right instanceof List) {
                    return ((List<?>) left).containsAll((List<?>) right);
                }
                if (GeometricOperations.isGeometricString(ls) || GeometricOperations.isGeometricString(rs)) {
                    return GeometricOperations.contains(ls, rs);
                }
                return false;
            }
            case CONTAINED_BY: {
                if (left == null || right == null) return null;
                String ls = left.toString().trim();
                String rs = right.toString().trim();
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
                    if (RangeOperations.isRangeString(ls)) return range.containsRange(RangeOperations.parse(ls));
                    if (left instanceof Number) return range.contains(((Number) left));
                    try { return range.contains(Long.parseLong(ls)); } catch (NumberFormatException ignore) {}
                }
                if ((ls.startsWith("{") || ls.startsWith("[")) && (rs.startsWith("{") || rs.startsWith("["))) {
                    return JsonOperations.contains(rs, ls);
                }
                if (left instanceof List && right instanceof List) {
                    return ((List<?>) right).containsAll((List<?>) left);
                }
                if (GeometricOperations.isGeometricString(ls) || GeometricOperations.isGeometricString(rs)) {
                    return GeometricOperations.contains(rs, ls);
                }
                return false;
            }
            case JSONB_EXISTS: {
                if (left == null || right == null) return null;
                return JsonOperations.keyExists(left.toString(), right.toString());
            }
            case JSONB_EXISTS_ANY: {
                if (left == null || right == null) return null;
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
                // Convert Lists to PG format for uniform handling
                String oLs = (left instanceof List) ? TypeCoercion.formatPgArray((List<?>) left) : left.toString().trim();
                String oRs = (right instanceof List) ? TypeCoercion.formatPgArray((List<?>) right) : right.toString().trim();
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
                // Array overlap: check if arrays share any element
                boolean lArr = oLs.startsWith("{") && !oLs.startsWith("{\"");
                boolean rArr = oRs.startsWith("{") && !oRs.startsWith("{\"");
                if (lArr && rArr) {
                    List<Object> la = FunctionEvaluator.parseSimplePgArray(oLs);
                    List<Object> ra = FunctionEvaluator.parseSimplePgArray(oRs);
                    Set<String> laStrs = la.stream().map(o -> o == null ? "NULL" : o.toString()).collect(Collectors.toSet());
                    return ra.stream().anyMatch(o -> laStrs.contains(o == null ? "NULL" : o.toString()));
                }
                return GeometricOperations.overlaps(oLs, oRs);
            }
            case DISTANCE: {
                if (left == null || right == null) return null;
                return GeometricOperations.distance(left.toString(), right.toString());
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
            case REGEX_MATCH: {
                if (left == null || right == null) return null;
                try { return java.util.regex.Pattern.compile(right.toString()).matcher(left.toString()).find(); }
                catch (java.util.regex.PatternSyntaxException e) { throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B"); }
            }
            case REGEX_IMATCH: {
                if (left == null || right == null) return null;
                try { return java.util.regex.Pattern.compile(right.toString(), java.util.regex.Pattern.CASE_INSENSITIVE).matcher(left.toString()).find(); }
                catch (java.util.regex.PatternSyntaxException e) { throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B"); }
            }
            case NOT_REGEX_MATCH: {
                if (left == null || right == null) return null;
                try { return !java.util.regex.Pattern.compile(right.toString()).matcher(left.toString()).find(); }
                catch (java.util.regex.PatternSyntaxException e) { throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B"); }
            }
            case NOT_REGEX_IMATCH: {
                if (left == null || right == null) return null;
                try { return !java.util.regex.Pattern.compile(right.toString(), java.util.regex.Pattern.CASE_INSENSITIVE).matcher(left.toString()).find(); }
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

    Object evalBinaryValues(BinaryExpr.BinOp op, Object left, Object right) {
        // Apply type validation before computation
        executor.validateOperatorTypes(op, left, right);
        switch (op) {
            case ADD:
                return executor.dateTimeAdd(left, right);
            case SUBTRACT:
                return executor.dateTimeSubtract(left, right);
            case MULTIPLY:
                return executor.numericOrIntervalMul(left, right);
            case DIVIDE: {
                if (left instanceof String && right instanceof String
                        && GeometricOperations.isGeometricString(((String) left))) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return GeometricOperations.divide(ls, rs);
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
                // inet bitwise AND
                if (left instanceof String && right instanceof String && ((String) left).contains(".") && ((String) right).contains(".")) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return NetworkOperations.bitwiseAnd(ls, rs);
                }
                return (int)(executor.toLong(left) & executor.toLong(right));
            }
            case BIT_OR: {
                if (left == null || right == null) return null;
                // Bit string OR
                String lBitsOr2 = AstExecutor.toBitStringOrNull(left);
                String rBitsOr2 = AstExecutor.toBitStringOrNull(right);
                if (lBitsOr2 != null && rBitsOr2 != null) {
                    return new AstExecutor.PgBitString(AstExecutor.bitwiseBitString(lBitsOr2, rBitsOr2, '|'));
                }
                // inet bitwise OR
                if (left instanceof String && right instanceof String && ((String) left).contains(".") && ((String) right).contains(".")) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return NetworkOperations.bitwiseOr(ls, rs);
                }
                return (int)(executor.toLong(left) | executor.toLong(right));
            }
            case BIT_XOR: {
                if (left == null || right == null) return null;
                // Bit string XOR
                String lBitsXor2 = AstExecutor.toBitStringOrNull(left);
                String rBitsXor2 = AstExecutor.toBitStringOrNull(right);
                if (lBitsXor2 != null && rBitsXor2 != null) {
                    return new AstExecutor.PgBitString(AstExecutor.bitwiseBitString(lBitsXor2, rBitsXor2, '#'));
                }
                // Geometric intersection: lseg # lseg
                if (left instanceof String && right instanceof String
                        && GeometricOperations.isGeometricString(((String) left))) {
                    String rs = (String) right;
                    String ls = (String) left;
                    GeometricOperations.PgPoint pt = GeometricOperations.intersection(ls, rs);
                    return pt != null ? GeometricOperations.format(pt) : null;
                }
                return (int)(executor.toLong(left) ^ executor.toLong(right));
            }
            case SHIFT_LEFT: {
                if (left == null || right == null) return null;
                if (left instanceof String && right instanceof String && ((String) left).contains(".") && ((String) right).contains(".")) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return NetworkOperations.containedBy(ls, rs);
                }
                return (int)(executor.toLong(left) << executor.toLong(right));
            }
            case SHIFT_RIGHT: {
                if (left == null || right == null) return null;
                if (left instanceof String && right instanceof String && ((String) left).contains(".") && ((String) right).contains(".")) {
                    String rs = (String) right;
                    String ls = (String) left;
                    return NetworkOperations.contains(ls, rs);
                }
                return (int)(executor.toLong(left) >> executor.toLong(right));
            }
            case INET_CONTAINS_EQUALS: {
                if (left == null || right == null) return null;
                return NetworkOperations.containsOrEquals(left.toString(), right.toString());
            }
            case INET_CONTAINED_BY_EQUALS: {
                if (left == null || right == null) return null;
                return NetworkOperations.containedByOrEquals(left.toString(), right.toString());
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
                if (left == null || right == null) return null;
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
                if (left == null || right == null) return false;
                // JSONB @@ jsonpath
                String ls = left.toString().trim();
                if (ls.startsWith("{") || ls.startsWith("[")) {
                    String path = right.toString().trim();
                    return executor.functionEvaluator.evaluateJsonPathPredicate(ls, path);
                }
                TsVector vec = left instanceof TsVector ? (TsVector) left : TsVector.fromText(left.toString());
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
                String lStr = left.toString().trim();
                String rStr = right.toString().trim();
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
                    if (RangeOperations.isRangeString(rStr)) return range.containsRange(RangeOperations.parse(rStr));
                    if (rStr.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
                        return range.contains(java.time.LocalDate.parse(rStr.substring(0, 10)).toEpochDay());
                    }
                    try { return range.contains(Long.parseLong(rStr)); } catch (NumberFormatException ignore) {}
                }
                if ((lStr.startsWith("{") || lStr.startsWith("[")) && (rStr.startsWith("{") || rStr.startsWith("["))) {
                    return JsonOperations.contains(lStr, rStr);
                }
                if (left instanceof List && right instanceof List) {
                    return ((List<?>) left).containsAll((List<?>) right);
                }
                if (GeometricOperations.isGeometricString(lStr) || GeometricOperations.isGeometricString(rStr)) {
                    return GeometricOperations.contains(lStr, rStr);
                }
                return false;
            }
            case CONTAINED_BY: {
                if (left == null || right == null) return null;
                String lStr = left.toString().trim();
                String rStr = right.toString().trim();
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
                    if (RangeOperations.isRangeString(lStr)) return range.containsRange(RangeOperations.parse(lStr));
                    if (left instanceof Number) return range.contains(((Number) left));
                    try { return range.contains(Long.parseLong(lStr)); } catch (NumberFormatException ignore) {}
                }
                if ((lStr.startsWith("{") || lStr.startsWith("[")) && (rStr.startsWith("{") || rStr.startsWith("["))) {
                    return JsonOperations.contains(rStr, lStr);
                }
                if (left instanceof List && right instanceof List) {
                    return ((List<?>) right).containsAll((List<?>) left);
                }
                if (GeometricOperations.isGeometricString(lStr) || GeometricOperations.isGeometricString(rStr)) {
                    return GeometricOperations.contains(rStr, lStr);
                }
                return false;
            }
            case JSONB_EXISTS: {
                if (left == null || right == null) return null;
                return JsonOperations.keyExists(left.toString(), right.toString());
            }
            case JSONB_EXISTS_ANY: {
                if (left == null || right == null) return null;
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
                // Convert Lists to PG format for uniform handling
                String oLs = (left instanceof List) ? TypeCoercion.formatPgArray((List<?>) left) : left.toString().trim();
                String oRs = (right instanceof List) ? TypeCoercion.formatPgArray((List<?>) right) : right.toString().trim();
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
                // Array overlap: check if arrays share any element
                boolean lArr = oLs.startsWith("{") && !oLs.startsWith("{\"");
                boolean rArr = oRs.startsWith("{") && !oRs.startsWith("{\"");
                if (lArr && rArr) {
                    List<Object> la = FunctionEvaluator.parseSimplePgArray(oLs);
                    List<Object> ra = FunctionEvaluator.parseSimplePgArray(oRs);
                    Set<String> laStrs = la.stream().map(o -> o == null ? "NULL" : o.toString()).collect(java.util.stream.Collectors.toSet());
                    return ra.stream().anyMatch(o -> laStrs.contains(o == null ? "NULL" : o.toString()));
                }
                return GeometricOperations.overlaps(oLs, oRs);
            }
            case DISTANCE: {
                if (left == null || right == null) return null;
                return GeometricOperations.distance(left.toString(), right.toString());
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
            case REGEX_MATCH: {
                if (left == null || right == null) return null;
                try { return java.util.regex.Pattern.compile(right.toString()).matcher(left.toString()).find(); }
                catch (java.util.regex.PatternSyntaxException e) { throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B"); }
            }
            case REGEX_IMATCH: {
                if (left == null || right == null) return null;
                try { return java.util.regex.Pattern.compile(right.toString(), java.util.regex.Pattern.CASE_INSENSITIVE).matcher(left.toString()).find(); }
                catch (java.util.regex.PatternSyntaxException e) { throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B"); }
            }
            case NOT_REGEX_MATCH: {
                if (left == null || right == null) return null;
                try { return !java.util.regex.Pattern.compile(right.toString()).matcher(left.toString()).find(); }
                catch (java.util.regex.PatternSyntaxException e) { throw new MemgresException("invalid regular expression: " + e.getDescription(), "2201B"); }
            }
            case NOT_REGEX_IMATCH: {
                if (left == null || right == null) return null;
                try { return !java.util.regex.Pattern.compile(right.toString(), java.util.regex.Pattern.CASE_INSENSITIVE).matcher(left.toString()).find(); }
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
}
