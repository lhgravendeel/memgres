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

    /** Says that a pair of operands is not a log position and a byte count, so nothing was done. */
    private static final Object NOT_LSN_ARITHMETIC = new Object();

    /**
     * {@code pg_lsn + numeric} and {@code pg_lsn - numeric}, which answer with a log position.
     *
     * <p>PostgreSQL does the sum in numeric and converts the result back, so a fractional count
     * of bytes rounds once at the end: 256 - 0.5 is 255.5, which comes back as 256. A count only
     * adds to the right of a position — {@code integer - pg_lsn} is no operator at all — and a
     * position that would fall outside the 64 bits one occupies is out of range.
     */
    private Object lsnMovedByBytes(BinaryExpr bin, Object left, Object right) {
        boolean leftIsLsn = isPgLsnExpression(bin.left());
        boolean rightIsLsn = isPgLsnExpression(bin.right());
        if (leftIsLsn == rightIsLsn) return NOT_LSN_ARITHMETIC;
        boolean subtract = bin.op() == BinaryExpr.BinOp.SUBTRACT;
        if (!leftIsLsn && subtract) return NOT_LSN_ARITHMETIC;
        Object position = leftIsLsn ? left : right;
        Object count = leftIsLsn ? right : left;
        if (!(position instanceof String) || !(count instanceof Number)) return NOT_LSN_ARITHMETIC;
        if (left == null || right == null) return null;

        java.math.BigDecimal bytes;
        try {
            bytes = new java.math.BigDecimal(count.toString());
        } catch (NumberFormatException notANumber) {
            throw new MemgresException("cannot " + (subtract ? "subtract NaN from" : "add NaN to")
                    + " pg_lsn", "0A000");
        }
        java.math.BigInteger at = lsnAsBytes((String) position);
        java.math.BigDecimal moved = subtract
                ? new java.math.BigDecimal(at).subtract(bytes)
                : new java.math.BigDecimal(at).add(bytes);
        java.math.BigInteger result =
                moved.setScale(0, java.math.RoundingMode.HALF_UP).toBigIntegerExact();
        if (result.signum() < 0 || result.bitLength() > 64) {
            throw new MemgresException("pg_lsn out of range", "22023");
        }
        return lsnText(result);
    }

    /** Whether an expression is declared to be a log position rather than the text one looks like. */
    private static boolean isPgLsnExpression(Expression expr) {
        if (isCastToType(expr, "pg_lsn")) return true;
        if (expr instanceof FunctionCallExpr) {
            return CatalogMetadataFunctions.answersWithAnLsn(
                    FunctionEvaluator.stripSchemaPrefix(
                            ((FunctionCallExpr) expr).name().toLowerCase(java.util.Locale.ROOT)));
        }
        return false;
    }

    /** The byte offset a log position stands for, as the unsigned 64-bit number it is. */
    private static java.math.BigInteger lsnAsBytes(String lsn) {
        String text = lsn.trim();
        int slash = text.indexOf('/');
        if (slash < 0) throw new MemgresException("invalid input syntax for type pg_lsn: \""
                + lsn + "\"", "22P02");
        return new java.math.BigInteger(text.substring(0, slash), 16).shiftLeft(32)
                .add(new java.math.BigInteger(text.substring(slash + 1), 16));
    }

    /**
     * The type an addition or a subtraction over log positions answers with, or null when the
     * expression is not one: a position moved by a count of bytes is still a position, and the
     * difference of two positions is the count of bytes between them.
     */
    static String lsnArithmeticTypeName(Expression expr) {
        if (!(expr instanceof BinaryExpr)) return null;
        BinaryExpr bin = (BinaryExpr) expr;
        if (bin.op() != BinaryExpr.BinOp.ADD && bin.op() != BinaryExpr.BinOp.SUBTRACT) return null;
        boolean leftIsLsn = isPgLsnExpression(bin.left());
        boolean rightIsLsn = isPgLsnExpression(bin.right());
        if (leftIsLsn && rightIsLsn) {
            return bin.op() == BinaryExpr.BinOp.SUBTRACT ? "numeric" : null;
        }
        if (!leftIsLsn && !rightIsLsn) return null;
        if (!leftIsLsn && bin.op() == BinaryExpr.BinOp.SUBTRACT) return null;
        return "pg_lsn";
    }

    /** Two log positions ordered by how far along the log each one is. */
    static int compareLsnText(String left, String right) {
        return lsnAsBytes(left).compareTo(lsnAsBytes(right));
    }

    /** A byte offset written the way a log position is: two hexadecimal halves, upper case. */
    private static String lsnText(java.math.BigInteger bytes) {
        return bytes.shiftRight(32).toString(16).toUpperCase(java.util.Locale.ROOT)
                + "/" + bytes.and(java.math.BigInteger.valueOf(0xFFFFFFFFL))
                        .toString(16).toUpperCase(java.util.Locale.ROOT);
    }

    static String getRangeTypeName(Expression expr) {
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            String name = fn.name().toLowerCase(java.util.Locale.ROOT);
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

    /**
     * PG's operator set is narrower than the values suggest. An lseg contains nothing and money
     * compares only with money. The operand *values* cannot decide this -- a geometric value is
     * stored as text, so a value-level rule would break plain string comparison -- so the declared
     * type of each operand decides.
     */
    private void rejectPhantomOperator(BinaryExpr bin, RowContext ctx, Object left, Object right) {
        if (left == null || right == null) return;
        BinaryExpr.BinOp op = bin.op();

        // money compares only with money; PG has no cross-type comparison for it
        boolean moneyLeft = left instanceof PgMoney;
        boolean moneyRight = right instanceof PgMoney;
        if ((moneyLeft ^ moneyRight) && (isComparison(op) || resolvesThroughEquality(op))
                && (left instanceof Number || right instanceof Number)) {
            String other = AstExecutor.pgTypeNameOf(moneyLeft ? right : left);
            String sym = resolvedOperatorSymbol(op);
            throw new MemgresException("operator does not exist: "
                    + (moneyLeft ? "money " + sym + " " + other
                                 : other + " " + sym + " money"), "42883");
        }

        // A bare literal beside a shape is read as a shape of that same type, which is the
        // operator PostgreSQL resolves to: a box asked to hold '(1,1)' is asked to hold another
        // box and fails to read one, rather than quietly taking the literal for the point it
        // looks like.
        if (op == BinaryExpr.BinOp.CONTAINS || op == BinaryExpr.BinOp.CONTAINED_BY) {
            requireLiteralReadsAsOperandType(bin, ctx);
        }
        // an lseg has no interior, so it contains nothing
        if (op == BinaryExpr.BinOp.CONTAINS && "lseg".equals(declaredGeometricType(bin.left(), ctx))) {
            String rType = declaredGeometricType(bin.right(), ctx);
            throw new MemgresException("operator does not exist: lseg @> "
                    + (rType != null ? rType : "unknown")
                    + "\n  Hint: No operator matches the given name and argument types. You might need to add explicit type casts.", "42883");
        }
    }

    /**
     * The types that carry no {@code =} operator at all: a point and a polygon are compared with
     * {@code ~=}, which is a different question from equality and is spelled differently. Their
     * {@code <>} is not affected -- a point has one, which is why this rule names the lookup that
     * fails rather than the operator as written.
     */
    private static final Set<String> TYPES_WITHOUT_EQUALITY = Cols.setOf("point", "polygon");

    /**
     * The types that also have no {@code <>}. Measured from pg_operator: point does carry "<>"
     * (its equality is the separate {@code ~=}), so only polygon lacks both.
     */
    private static final Set<String> TYPES_WITHOUT_INEQUALITY = Cols.setOf("polygon");

    /**
     * PostgreSQL resolves {@code =} against the declared types before it looks at a value, so a
     * type that has no {@code =} cannot be compared even with a literal that would read as one.
     * The untyped literal is named {@code unknown}, the type it never got to resolve to.
     *
     * <p>Runs before the untyped literal is read as the other side's type: PG reports the missing
     * operator even when the literal is nothing that type could have parsed.
     */
    /** Also reached from IN and from ANY/ALL over an array, which resolve the same operator. */
    void rejectMissingEqualityOperator(BinaryExpr bin, RowContext ctx) {
        BinaryExpr.BinOp op = bin.op();
        boolean inequality = op == BinaryExpr.BinOp.NOT_EQUAL;
        if (op != BinaryExpr.BinOp.EQUAL && !inequality && !resolvesThroughEquality(op)) return;
        Set<String> missing = inequality ? TYPES_WITHOUT_INEQUALITY : TYPES_WITHOUT_EQUALITY;
        String lType = declaredGeometricType(bin.left(), ctx);
        String rType = declaredGeometricType(bin.right(), ctx);
        boolean lMissing = lType != null && missing.contains(lType);
        boolean rMissing = rType != null && missing.contains(rType);
        if (!lMissing && !rMissing) return;
        // Only an untyped literal could have resolved to the type opposite it. Any other operand
        // is a comparison between two different types, which is a lookup this rule cannot answer.
        String lName = lMissing ? lType : (isUntypedStringLiteral(bin.left()) ? "unknown" : null);
        String rName = rMissing ? rType : (isUntypedStringLiteral(bin.right()) ? "unknown" : null);
        if (lName == null || rName == null) return;
        throw new MemgresException("operator does not exist: "
                + lName + " " + resolvedOperatorSymbol(op) + " " + rName
                + "\n  Hint: No operator matches the given name and argument types. You might need to add explicit type casts.", "42883");
    }

    /**
     * True for the operators PostgreSQL answers by looking up the {@code =} of the operand type.
     * IS DISTINCT FROM is a NULL-safe equality, not an operator of its own, so a type pair that
     * has no {@code =} cannot be written with it either.
     */
    private static boolean resolvesThroughEquality(BinaryExpr.BinOp op) {
        return op == BinaryExpr.BinOp.IS_DISTINCT_FROM || op == BinaryExpr.BinOp.IS_NOT_DISTINCT_FROM;
    }

    /** The operator PostgreSQL names in the error, which is {@code =} for the DISTINCT tests. */
    private static String resolvedOperatorSymbol(BinaryExpr.BinOp op) {
        return resolvesThroughEquality(op) ? "=" : binOpToSymbol(op);
    }

    /**
     * The types whose operators take an untyped literal on the other side. Only types whose text
     * form is unambiguous are listed: resolving against text or a number would change how ordinary
     * string and arithmetic expressions behave, which is a far bigger blast radius than the bug.
     */
    private static final Set<String> LITERAL_RESOLVABLE_TYPES = Cols.setOf(
            "inet", "cidr", "macaddr", "macaddr8",
            "point", "line", "lseg", "box", "path", "polygon", "circle",
            "int4range", "int8range", "numrange", "daterange", "tsrange", "tstzrange",
            "int4multirange", "int8multirange", "nummultirange", "datemultirange",
            "tsmultirange", "tstzmultirange",
            "interval", "uuid", "tsvector", "tsquery");

    /**
     * The numeric types, which resolve a literal beside them where the operator is one two
     * numbers share. Left out, an untyped literal next to an integer was read as a double, so a
     * value that is not a number at all was complained about as a bad double rather than as a
     * bad integer; beside a comparison it stayed text and the two were compared as strings.
     */
    private static final Set<String> NUMERIC_LITERAL_RESOLVABLE = Cols.setOf(
            "smallint", "integer", "int", "bigint", "real", "double precision", "numeric",
            "int2", "int4", "int8", "float4", "float8", "float", "decimal", "double");

    /**
     * The type an untyped string literal on one side of {@code bin} should be read as, or null when
     * neither side is such a literal or the other side's type is not one that resolves it.
     */
    private String untypedLiteralTargetType(BinaryExpr bin, RowContext ctx) {
        boolean leftUntyped = isUntypedStringLiteral(bin.left());
        boolean rightUntyped = isUntypedStringLiteral(bin.right());
        // Two untyped literals resolve against each other in PG, which needs no help from here
        if (leftUntyped == rightUntyped) return null;
        Expression typed = leftUntyped ? bin.right() : bin.left();
        // A row constructor is a record, and a record carries no operator of its own for a
        // literal to be read as: PostgreSQL falls back to anynonarray || text and leaves the
        // literal the text it is.
        if (typed instanceof ArrayExpr && ((ArrayExpr) typed).isRow()) return null;
        String other = declaredOperandType(typed, ctx);
        if (other == null) return null;
        if (other.equals("record") || other.equals("record[]")) return null;
        if (other.endsWith("[]")) return other;
        // || is resolved within a type only where that type carries one of its own. A range, an
        // inet, a point or a record does not, so PostgreSQL falls back to anynonarray || text and
        // the literal stays the text it is -- reading it as a range instead refused a plain 'x'.
        if (bin.op() == BinaryExpr.BinOp.CONCAT
                && !other.equals("tsvector") && !other.equals("tsquery")) {
            return null;
        }
        // @@ never takes two of a kind: a literal beside a tsquery is the document being searched
        // and is read as a tsvector; a literal beside a tsvector is the query and is read as a
        // tsquery. Reading a literal as whatever the other side was made 'a b' -- a perfectly good
        // document -- a syntax error, and left tsvector @@ 'a' with no operator at all.
        if (bin.op() == BinaryExpr.BinOp.TS_MATCH) {
            if (other.equals("tsquery")) return rightUntyped ? "tsvector" : null;
            if (other.equals("tsvector")) return null;
        }
        // A number resolves a literal beside it only where the operator is one two numbers
        // share: the arithmetic and the comparisons. Containment and the rest belong to types
        // of their own, and reading the other operand as a number there cast a multirange to
        // an integer.
        if (NUMERIC_LITERAL_RESOLVABLE.contains(other)) {
            return resolvesAgainstNumbers(bin.op()) ? other : null;
        }
        return LITERAL_RESOLVABLE_TYPES.contains(other) ? other : null;
    }

    /** Whether two numbers share this operator, which is what lets one resolve a literal. */
    private static boolean resolvesAgainstNumbers(BinaryExpr.BinOp op) {
        switch (op) {
            case ADD:
            case SUBTRACT:
            case MULTIPLY:
            case DIVIDE:
            case MODULO:
            case POWER:
            case EQUAL:
            case NOT_EQUAL:
            case LESS_THAN:
            case LESS_EQUAL:
            case GREATER_THAN:
            case GREATER_EQUAL:
                return true;
            default:
                return false;
        }
    }

    /**
     * The type an untyped literal beside a date, time or interval operand is read as -- and the
     * two ambiguities PostgreSQL refuses to guess at rather than resolve.
     *
     * <p>PostgreSQL resolves {@code timestamp + unknown} to {@code timestamp + interval} and
     * {@code timestamp - unknown} to {@code timestamp - timestamp}, so the same literal is read
     * two different ways depending on the sign. {@code date + unknown} fits both {@code date +
     * integer} and {@code date + interval} equally well, so PostgreSQL stops with 42725 instead
     * of picking one. Reading the literal as a number regardless is what made
     * {@code date '2020-01-01' + '1'} answer 1 -- the date vanished entirely.
     */
    private String dateTimeLiteralTarget(BinaryExpr bin, RowContext ctx) {
        BinaryExpr.BinOp op = bin.op();
        boolean leftUntyped = isUntypedStringLiteral(bin.left());
        boolean rightUntyped = isUntypedStringLiteral(bin.right());
        if (leftUntyped == rightUntyped) return null;
        String declared = declaredTypeForResolution(leftUntyped ? bin.right() : bin.left(), ctx);
        String t = canonicalDateTimeType(declared);
        if (t == null) return null;
        boolean add = op == BinaryExpr.BinOp.ADD;
        boolean sub = op == BinaryExpr.BinOp.SUBTRACT;
        if (op == BinaryExpr.BinOp.MULTIPLY || op == BinaryExpr.BinOp.DIVIDE) {
            // an interval scaled by an untyped literal reads it as the number it is
            return t.equals("interval") ? "double precision" : null;
        }
        if (!add && !sub) return null;
        if (t.equals("interval")) return "interval";
        if (t.equals("date")) {
            if (add) throw notUniqueOperator(leftUntyped ? "unknown" : "date", op,
                    leftUntyped ? "date" : "unknown");
            // date - unknown has only date - date to resolve to, so the literal is read as a date
            return leftUntyped ? null : "date";
        }
        if (t.equals("timetz")) {
            throw notUniqueOperator(leftUntyped ? "unknown" : "time with time zone", op,
                    leftUntyped ? "time with time zone" : "unknown");
        }
        // timestamp, timestamptz and time: a literal added is an interval, one subtracted is
        // another moment of the same kind
        if (add) return "interval";
        return leftUntyped ? null : t;
    }

    /** The 42725 PostgreSQL raises when two operators fit a pair of types equally well. */
    private MemgresException notUniqueOperator(String lName, BinaryExpr.BinOp op, String rName) {
        return new MemgresException("operator is not unique: " + lName + " "
                + binOpToSymbol(op) + " " + rName
                + "\n  Hint: Could not choose a best candidate operator."
                + " You might need to add explicit type casts.", "42725");
    }

    /** The date/time type a declared name spells, or null when it names something else. */
    private static String canonicalDateTimeType(String declared) {
        if (declared == null) return null;
        String t = declared.toLowerCase(java.util.Locale.ROOT).trim();
        int paren = t.indexOf('(');
        if (paren > 0) t = t.substring(0, paren).trim();
        // "interval day to second" and the rest of the qualified spellings are still intervals
        if (t.equals("interval") || t.startsWith("interval ")) return "interval";
        switch (t) {
            case "date": return "date";
            case "timestamp": case "timestamp without time zone": return "timestamp";
            case "timestamptz": case "timestamp with time zone": return "timestamptz";
            case "time": case "time without time zone": return "time";
            case "timetz": case "time with time zone": return "timetz";
            default: return null;
        }
    }

    /** True for text spelled as an array, with or without the lower bounds an array may carry. */
    private static boolean isArrayText(String text) {
        String body = ArrayLiteral.body(text);
        return body.startsWith("{") && body.endsWith("}");
    }

    /** The range types, whose value is a pair of bounds written between brackets. */
    private static final Set<String> RANGE_TYPES = Cols.setOf(
            "int4range", "int8range", "numrange", "daterange", "tsrange", "tstzrange");

    /**
     * The element type each range is built over. A range's containment operators are declared over
     * this type and nothing else -- there is no {@code numrange @> integer} to fall back on, even
     * though an integer is a perfectly good numeric, so asking one that way is an error.
     */
    private static String rangeSubtype(String rangeType) {
        if (rangeType == null) return null;
        String base = rangeType.replace("multirange", "range");
        if (base.equals("int4range")) return "integer";
        if (base.equals("int8range")) return "bigint";
        if (base.equals("numrange")) return "numeric";
        if (base.equals("daterange")) return "date";
        if (base.equals("tsrange")) return "timestamp without time zone";
        if (base.equals("tstzrange")) return "timestamp with time zone";
        return null;
    }

    /**
     * A containment test between a range and a value of some other type than the range is built
     * over. Only a type the query states counts: a column's type in memgres is whatever the engine
     * settled on, and refusing an operator on the strength of that would reject working SQL.
     */
    private void rejectRangeElementTypeMismatch(BinaryExpr bin, RowContext ctx) {
        rejectCrossRangeFamily(bin, ctx);
        if (bin.op() != BinaryExpr.BinOp.CONTAINS && bin.op() != BinaryExpr.BinOp.CONTAINED_BY) {
            return;
        }
        String lDeclared = declaredOperandType(bin.left(), ctx);
        String rDeclared = declaredOperandType(bin.right(), ctx);
        boolean lRange = lDeclared != null
                && (RANGE_TYPES.contains(lDeclared) || MULTIRANGE_TYPES.contains(lDeclared));
        boolean rRange = rDeclared != null
                && (RANGE_TYPES.contains(rDeclared) || MULTIRANGE_TYPES.contains(rDeclared));
        if (lRange == rRange) return;
        String scalar = declaredTypeForResolution(lRange ? bin.right() : bin.left(), ctx);
        if (scalar == null || familyOf(scalar) == null) return;
        String subtype = rangeSubtype(lRange ? lDeclared : rDeclared);
        if (subtype == null || subtype.equals(pgName(scalar))) return;
        String lName = lRange ? lDeclared : pgName(scalar);
        String rName = lRange ? pgName(scalar) : rDeclared;
        throw new MemgresException("operator does not exist: " + lName + " "
                + binOpToSymbol(bin.op()) + " " + rName
                + "\n  Hint: No operator matches the given name and argument types. You might need to add explicit type casts.", "42883");
    }

    /**
     * An operator between two ranges is declared over one range type, so two different ones have
     * no operator between them however alike their shapes are. Reading the two texts and comparing
     * their bounds answered for pairs PostgreSQL has nothing to answer with.
     */
    private void rejectCrossRangeFamily(BinaryExpr bin, RowContext ctx) {
        String lDeclared = declaredOperandType(bin.left(), ctx);
        String rDeclared = declaredOperandType(bin.right(), ctx);
        if (lDeclared == null || rDeclared == null) return;
        boolean lRange = RANGE_TYPES.contains(lDeclared) || MULTIRANGE_TYPES.contains(lDeclared);
        boolean rRange = RANGE_TYPES.contains(rDeclared) || MULTIRANGE_TYPES.contains(rDeclared);
        if (!lRange || !rRange) return;
        String lSubtype = rangeSubtype(lDeclared);
        String rSubtype = rangeSubtype(rDeclared);
        if (lSubtype != null && rSubtype != null && lSubtype.equals(rSubtype)) return;
        if (lDeclared.equals(rDeclared)) return;
        throw new MemgresException("operator does not exist: " + lDeclared + " "
                + binOpToSymbol(bin.op()) + " " + rDeclared
                + "\n  Hint: No operator matches the given name and argument types. You might need to add explicit type casts.", "42883");
    }

    /** The multirange types, whose value is a brace-wrapped list of ranges. */
    private static final Set<String> MULTIRANGE_TYPES = Cols.setOf(
            "int4multirange", "int8multirange", "nummultirange", "datemultirange",
            "tsmultirange", "tstzmultirange");

    /**
     * A multirange operator takes a multirange on both sides, so an untyped literal beside one is
     * read as a multirange literal -- braces and all. A bare range literal is not one: PostgreSQL
     * refuses it rather than quietly wrapping it, which is what let a multirange {@code @>} answer
     * a question that was never asked of it.
     */
    private static void checkMultirangeLiteral(BinaryExpr.BinOp op, String targetType, Object value) {
        if (!MULTIRANGE_TYPES.contains(targetType) || value == null) return;
        if (!resolvesAgainstOperandType(op)) return;
        String text = value.toString().trim();
        if (text.startsWith("{") && text.endsWith("}")) return;
        // Reading it the way a multirange is read is what names the fault -- a literal that never
        // opened a brace is a different complaint from one that opened and never closed -- and it
        // names it in the same words wherever a multirange literal is read.
        RangeOperations.parseMultirangeLiteral(text);
        throw new MemgresException("malformed multirange literal: \"" + text + "\"", "22P02");
    }

    /**
     * True for the operators PostgreSQL resolves within one type, so an untyped literal beside
     * them has to be read as that type. Concatenation and pattern matching are deliberately left
     * out: they resolve to their text forms, which read any value at all.
     */
    private static boolean resolvesAgainstOperandType(BinaryExpr.BinOp op) {
        if (isComparison(op) || isArithmetic(op) || resolvesThroughEquality(op)) return true;
        switch (op) {
            case CONTAINS: case CONTAINED_BY: case OVERLAP:
            case SHIFT_LEFT: case SHIFT_RIGHT: case RANGE_ADJACENT:
                return true;
            default:
                return false;
        }
    }

    /** True for a string literal written without a cast, which is PostgreSQL's {@code unknown}. */
    private static boolean isUntypedStringLiteral(Expression expr) {
        return expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.STRING;
    }

    /** The types that make an operand of {@code @@} the document rather than the query. */
    private static final java.util.Set<String> TEXTUAL_TYPES = Cols.setOf(
            "text", "varchar", "character varying", "char", "character", "bpchar", "name", "citext");

    /**
     * What an operand of {@code @@} is, as far as resolution is concerned: {@code "tsvector"},
     * {@code "tsquery"}, {@code "text"} when it was written as text, or null when it is an
     * untyped literal that the other side has to settle.
     *
     * <p>Which operator was written is not something the values can answer on their own. A tsvector
     * beside a literal reads that literal as a query; a text beside one reads it as a phrase to
     * search for. Both arrive here as a Java string.
     */
    private String matchOperandType(Expression expr, RowContext ctx) {
        if (isUntypedStringLiteral(expr)) return null;
        String declared = declaredOperandType(expr, ctx);
        if ("tsvector".equals(declared) || "tsquery".equals(declared)) return declared;
        // Only a operand actually written as a string type reads its partner as a phrase; one
        // whose type this cannot work out is left for the values to settle, as before.
        return TEXTUAL_TYPES.contains(declared) ? "text" : null;
    }

    /** The type an operand is declared to have, from a cast or a column, or null. */
    private String declaredOperandType(Expression expr, RowContext ctx) {
        if (expr instanceof Literal) {
            Literal lit = (Literal) expr;
            switch (lit.literalType()) {
                case INTEGER: return "integer";
                case FLOAT: return "numeric";
                case BOOLEAN: return "boolean";
                // A string literal is PostgreSQL's unknown, which resolves against the other side
                default: return null;
            }
        }
        if (expr instanceof CastExpr) {
            String name = ((CastExpr) expr).typeName();
            return name == null ? null : name.toLowerCase(java.util.Locale.ROOT).trim();
        }
        if (expr instanceof ArrayExpr) {
            String element = arrayElementTypeName((ArrayExpr) expr, ctx);
            return element == null ? null : element + "[]";
        }
        if (expr instanceof FunctionCallExpr) {
            String range = getRangeTypeName(expr);
            if (range != null) return range;
        }
        if (expr instanceof ColumnRef && ctx != null) {
            ColumnRef ref = (ColumnRef) expr;
            for (RowContext.TableBinding b : ctx.getBindings()) {
                if (b.table() == null) continue;
                if (ref.table() != null && !ref.table().equalsIgnoreCase(b.alias())
                        && !ref.table().equalsIgnoreCase(b.table().getName())) continue;
                int idx = b.table().getColumnIndex(ref.column());
                if (idx < 0) continue;
                DataType t = b.table().getColumns().get(idx).getType();
                return t == null ? null : t.getPgName();
            }
        }
        return null;
    }

    /** The element type of an array constructor, decided from its first typed element. */
    private String arrayElementTypeName(ArrayExpr expr, RowContext ctx) {
        for (Expression element : expr.elements()) {
            if (element instanceof ArrayExpr) {
                String nested = arrayElementTypeName((ArrayExpr) element, ctx);
                if (nested != null) return nested;
                continue;
            }
            if (element instanceof Literal) {
                Literal lit = (Literal) element;
                if (lit.literalType() == Literal.LiteralType.INTEGER) return "integer";
                if (lit.literalType() == Literal.LiteralType.FLOAT) return "numeric";
                if (lit.literalType() == Literal.LiteralType.STRING) return "text";
                continue;
            }
            String declared = declaredOperandType(element, ctx);
            if (declared != null) return declared;
        }
        return null;
    }

    /**
     * The families an operator can be resolved within; PostgreSQL has no operator across two of
     * them. A date and a timestamp share one because PostgreSQL really does compare them; a time
     * and an interval do not, and neither does a uuid and the text that spells it.
     */
    private enum TypeFamily {
        STRING, NUMERIC, BOOLEAN, JSON, DATETIME, TIMEOFDAY, INTERVAL, UUID, NETWORK, BYTES
    }

    /**
     * PostgreSQL resolves an operator against the declared types of its operands, and there is no
     * {@code text = integer}, {@code text = date} or {@code uuid = text} to resolve to — a query
     * written that way is an error, not a comparison of a value with its spelling. Reading the
     * values instead let {@code '5'::text = 5} and {@code '2020-01-01'::text = date '2020-01-01'}
     * answer true, so a query that fails in production passed here.
     *
     * <p>Only operands whose type is actually declared take part: a bare literal is {@code unknown}
     * and PostgreSQL resolves it against the other side, so {@code '5' = 5} stays legal.
     */
    /**
     * Refuses a {@code ||} that names no operator, or more than one.
     *
     * <p>PostgreSQL declares eleven concatenations and no more, so a pair of types none of them
     * takes is a pair with nothing to run: neither {@code 1 || now()} nor {@code uuid || inet} is
     * a concatenation, however readily two values can be written one after the other. Reading the
     * values and running them together as strings answered for 772 pairs PostgreSQL has none for.
     *
     * <p>Only a pair whose types the statement settles is judged, which is the same benefit of the
     * doubt every other resolution rule here gives.
     */
    private void rejectUnresolvableConcat(BinaryExpr bin, RowContext ctx) {
        concatOutcome(bin, ctx);
    }

    /**
     * Which concatenation this {@code ||} means, refusing the pairs PostgreSQL refuses. Answers
     * UNDECIDED where the statement has not settled both types, which leaves the reading of the
     * two values to the rules that came before this one.
     */
    private ConcatResolution concatOutcome(BinaryExpr bin, RowContext ctx) {
        if (bin.op() != BinaryExpr.BinOp.CONCAT) return ConcatResolution.UNDECIDED;
        String lName = declaredTypeForResolution(bin.left(), ctx);
        String rName = declaredTypeForResolution(bin.right(), ctx);
        int lOid = operandOid(lName);
        int rOid = operandOid(rName);
        if (lOid < 0 || rOid < 0) return ConcatResolution.UNDECIDED;
        ConcatOperator.Resolution resolution = ConcatOperator.resolve(lOid, rOid);
        if (resolution.outcome == ConcatOperator.Outcome.NONE) {
            throw missingOperator(operandName(lName), BinaryExpr.BinOp.CONCAT, operandName(rName));
        }
        if (resolution.outcome == ConcatOperator.Outcome.AMBIGUOUS) {
            throw new MemgresException("operator is not unique: "
                    + operandName(lName) + " || " + operandName(rName)
                    + "\n  Hint: Could not choose a best candidate operator."
                    + " You might need to add explicit type casts.", "42725");
        }
        return new ConcatResolution(resolution, lOid, rOid);
    }

    /**
     * {@code text} where a {@code ||} resolves to the text concatenation and both its operands
     * say what they are, and null everywhere else -- the concatenations over arrays, bit strings
     * and the rest answer with types this does not work out, and a pair the query has not settled
     * is left unjudged as it is everywhere else here.
     */
    private String textConcatResultType(BinaryExpr bin, RowContext ctx) {
        if (bin.op() != BinaryExpr.BinOp.CONCAT) return null;
        String lName = declaredTypeForResolution(bin.left(), ctx);
        String rName = declaredTypeForResolution(bin.right(), ctx);
        if (lName == null && !isUntypedStringLiteral(bin.left())) return null;
        if (rName == null && !isUntypedStringLiteral(bin.right())) return null;
        int lOid = operandOid(lName);
        int rOid = operandOid(rName);
        if (lOid < 0 || rOid < 0) return null;
        return ConcatOperator.resolve(lOid, rOid).outcome == ConcatOperator.Outcome.TEXT_CONCAT
                ? "text" : null;
    }

    /** A resolved concatenation, with the types its two operands were written as. */
    private static final class ConcatResolution {
        static final ConcatResolution UNDECIDED = new ConcatResolution(null, 0, 0);

        final ConcatOperator.Resolution resolution;
        final int leftOid;
        final int rightOid;

        ConcatResolution(ConcatOperator.Resolution resolution, int leftOid, int rightOid) {
            this.resolution = resolution;
            this.leftOid = leftOid;
            this.rightOid = rightOid;
        }

        boolean is(ConcatOperator.Outcome outcome) {
            return resolution != null && resolution.outcome == outcome;
        }
    }

    /**
     * An operand read as text for a concatenation. A blank-padded string loses its padding on the
     * way — PostgreSQL stores a bpchar padded and reads it back trimmed, which is why
     * {@code 'a'::char(3) || 'b'} is {@code ab} and not {@code a  b}.
     */
    /** Whether a type is the blank-padded character type, or an array whose elements are. */
    private static boolean isBlankPaddedOrItsArray(int oid) {
        if (oid == DataType.CHAR.getOid()) return true;
        DataType type = DataType.fromOid(oid);
        DataType element = type == null ? null : DataType.elementOf(type);
        return element == DataType.CHAR;
    }

    private String concatOperandAsText(Object value, int oid) {
        String written = String.valueOf(executor.castValue(value, "text"));
        if (oid != DataType.CHAR.getOid()) return written;
        int end = written.length();
        while (end > 0 && written.charAt(end - 1) == ' ') end--;
        return written.substring(0, end);
    }

    /** The name PostgreSQL prints for an operand type, or "unknown" where none was written. */
    private static String operandName(String typeName) {
        if (typeName == null) return "unknown";
        String bare = canonicalOperandType(typeName);
        if (wasQuoted(typeName) && "char".equals(bare)) return "\"char\"";
        return pgName(bare);
    }

    /** The OID of a written operand type: 0 where the statement said nothing, -1 where this rule cannot tell. */
    private int operandOid(String typeName) {
        if (typeName == null) return 0;
        String t = canonicalOperandType(typeName);
        // Written with quotes it is PostgreSQL's own single byte; written without, char is the
        // blank-padded string type. They are different types and only one of them is category Z.
        if (wasQuoted(typeName)) return "char".equals(t) ? DataType.INTERNAL_CHAR.getOid() : -1;
        if (t.equals("char") || t.equals("character") || t.equals("bpchar")) {
            return DataType.CHAR.getOid();
        }
        if (t.endsWith("[]")) {
            DataType element = DataType.fromPgName(t.substring(0, t.length() - 2).trim());
            DataType array = element == null ? null : DataType.arrayOf(element);
            return array == null ? -1 : array.getOid();
        }
        DataType type = DataType.fromPgName(t);
        return type == null ? -1 : type.getOid();
    }

    /**
     * A written type name reduced to the type itself: without its modifier, and without the quotes
     * {@code "char"} has to be written with. Leaving those on found no type at all, so the one
     * pair PostgreSQL most often refuses went unjudged.
     */
    private static String canonicalOperandType(String typeName) {
        String t = typeName.toLowerCase(java.util.Locale.ROOT).trim();
        int paren = t.indexOf('(');
        if (paren > 0) {
            int close = t.lastIndexOf(')');
            String tail = close >= 0 && close + 1 < t.length() ? t.substring(close + 1) : "";
            t = (t.substring(0, paren) + tail).trim();
        }
        if (t.length() > 1 && t.charAt(0) == '"' && t.charAt(t.length() - 1) == '"') {
            t = t.substring(1, t.length() - 1);
        }
        return t;
    }

    private static boolean wasQuoted(String typeName) {
        String t = typeName.trim();
        return t.length() > 1 && t.charAt(0) == '"';
    }

    void rejectCrossCategoryOperator(BinaryExpr bin, RowContext ctx) {
        BinaryExpr.BinOp op = bin.op();
        boolean comparison = isComparison(op) || resolvesThroughEquality(op);
        boolean arithmetic = isArithmetic(op);
        if (!comparison && !arithmetic) return;
        String lName = declaredTypeForResolution(bin.left(), ctx);
        String rName = declaredTypeForResolution(bin.right(), ctx);
        TypeFamily lf = lName == null ? null : familyOf(lName);
        TypeFamily rf = rName == null ? null : familyOf(rName);

        if (lf == null || rf == null) return;

        if (arithmetic) {
            // Arithmetic lives in the numeric, date/time, network, geometric and range families.
            // Text, booleans and json carry none of it, so a pair with one of those on either
            // side has no operator to resolve to. Only pairs whose families are both known take
            // part: hstore, tsquery and the other types this rule does not judge do carry
            // arithmetic against text, and refusing those would reject SQL PostgreSQL runs.
            boolean lBad = lf == TypeFamily.STRING || lf == TypeFamily.BOOLEAN || lf == TypeFamily.JSON;
            boolean rBad = rf == TypeFamily.STRING || rf == TypeFamily.BOOLEAN || rf == TypeFamily.JSON;
            if (!lBad && !rBad) return;
            // pg_trgm declares a text % text of its own -- similarity, not a remainder -- so
            // once that extension is installed the pair really does resolve to an operator.
            if (op == BinaryExpr.BinOp.MODULO && lf == TypeFamily.STRING && rf == TypeFamily.STRING
                    && executor.database.hasExtension("pg_trgm")) {
                return;
            }
            throw missingOperator(pgName(lName), op, pgName(rName));
        }

        // json has no comparison operator of any kind, not even against another json
        boolean reject = (lf == TypeFamily.JSON || rf == TypeFamily.JSON) || lf != rf;
        if (!reject) return;
        throw missingOperator(pgName(lName), op, pgName(rName));
    }

    /** The 42883 PostgreSQL raises when no operator of that name takes that pair of types. */
    private MemgresException missingOperator(String lName, BinaryExpr.BinOp op, String rName) {
        return new MemgresException("operator does not exist: " + lName + " "
                + resolvedOperatorSymbol(op) + " " + rName
                + "\n  Hint: No operator matches the given name and argument types. You might need to add explicit type casts.", "42883");
    }

    /** Runs the declaration-only rules for an operator the parser spelled some other way. */
    void rejectUnresolvableOperator(BinaryExpr bin, RowContext ctx) {
        rejectMissingEqualityOperator(bin, ctx);
        rejectCrossCategoryOperator(bin, ctx);
        rejectUnresolvableConcat(bin, ctx);
        rejectOperatorWithNoEntry(bin, ctx);
    }

    /**
     * How an operator is written, for a message that names it. The bitwise operators are spelled
     * here and not in the catalogue lookup below, which reads an operator's spelling to decide
     * whether the operands have an entry for it: {@code <<} is a shift over numbers and a
     * containment test over networks, and one spelling would answer for both.
     */
    static String spellingOf(BinaryExpr.BinOp op) {
        return pgSpelling(op);
    }

    /** The spelling PostgreSQL's pg_operator uses for each of the operators written here. */
    private static String pgSpelling(BinaryExpr.BinOp op) {
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
            case LIKE: return "~~";
            case ILIKE: return "~~*";
            case REGEX_MATCH: return "~";
            case REGEX_IMATCH: return "~*";
            case NOT_REGEX_MATCH: return "!~";
            case NOT_REGEX_IMATCH: return "!~*";
            case CONTAINS: return "@>";
            case CONTAINED_BY: return "<@";
            case OVERLAP: return "&&";
            case TS_MATCH: return "@@";
            case CONCAT: return "||";
            // The operators that ask a document about its members, or build a new one from it,
            // are declared over jsonb alone. Spelling them lets the catalog rule see that json
            // has no entry for them, rather than answering as though it did.
            case JSONB_EXISTS: return "?";
            case JSONB_EXISTS_ANY: return "?|";
            case JSONB_EXISTS_ALL: return "?&";
            case JSONB_PATH_EXISTS_OP: return "@?";
            case JSON_DELETE_PATH: return "#-";
            // A path down into a document is written only over json and jsonb. The single-step
            // arrows are left unspelled because hstore uses them too and the catalogue does not
            // carry the extension's own entries; nothing but a document has a #> at all.
            case JSON_HASH_ARROW: return "#>";
            case JSON_HASH_ARROW_TEXT: return "#>>";
            // The bitwise operators are spelled here too, so the catalogue rule can see which
            // pairs of operands have an entry: PostgreSQL shifts an integer by an integer and a
            // bigint by an integer, and has nothing for an integer shifted by a bigint. The same
            // spellings belong to the network types, whose entries the catalogue also carries.
            case BIT_AND: return "&";
            case BIT_OR: return "|";
            case BIT_XOR: return "#";
            case SHIFT_LEFT: return "<<";
            case SHIFT_RIGHT: return ">>";
            default: return null;
        }
    }

    /**
     * Refuse an operator PostgreSQL has no pg_operator row for over these operand types. Choosing
     * an operator from the runtime classes of the two values instead let {@code 1 || 2},
     * {@code money + 1} and {@code date LIKE '2020%'} all run.
     */
    private void rejectOperatorWithNoEntry(BinaryExpr bin, RowContext ctx) {
        String spelling = pgSpelling(bin.op());
        if (spelling == null) return;
        String leftName = declaredTypeForResolution(bin.left(), ctx);
        String rightName = declaredTypeForResolution(bin.right(), ctx);
        int left = operandTypeOid(leftName, bin.left());
        int right = operandTypeOid(rightName, bin.right());
        if (left < 0 || right < 0) return;
        // An extension brings operators of its own, and the catalogue this rule reads carries
        // only the ones PostgreSQL ships: intarray declares & | - over integer arrays, so a
        // question about a pair of arrays is one that catalogue cannot answer.
        if (executor.database.hasExtension("intarray")
                && leftName != null && rightName != null
                && leftName.endsWith("[]") && rightName.endsWith("[]")) {
            return;
        }
        MemgresException refusal =
                OperatorResolution.refusalFor(spelling, left, right, leftName, rightName);
        if (refusal != null) throw refusal;
    }

    /**
     * The type OID an operand carries for resolution: unknown for an untyped literal, the declared
     * type where the query wrote one, and -1 where nothing can be said and the rule stands down.
     */
    private int operandTypeOid(String declared, Expression expr) {
        if (declared == null) {
            // Only a literal with no type of its own is PostgreSQL's "unknown"; anything else
            // whose type this engine could not work out is left unjudged. A bare NULL is unknown
            // too, which is why NULL + NULL has no operator to choose while NULL || NULL does.
            boolean untyped = isUntypedStringLiteral(expr)
                    || (expr instanceof Literal
                        && ((Literal) expr).literalType() == Literal.LiteralType.NULL);
            return untyped ? OperatorResolution.UNKNOWN : -1;
        }
        String bare = declared.toLowerCase(java.util.Locale.ROOT).trim();
        int paren = bare.indexOf('(');
        if (paren > 0) bare = bare.substring(0, paren).trim();
        if (executor.database != null
                && (executor.database.getCustomEnum(bare) != null
                    || executor.database.isCompositeType(bare)
                    || executor.database.isDomain(bare))) {
            return -1;
        }
        DataType type = DataType.fromPgName(bare);
        return type == null ? -1 : type.getOid();
    }

    /** The numeric types arithmetic widens through, narrowest first. */
    private static final DataType[] NUMERIC_LADDER = {DataType.SMALLINT, DataType.INTEGER,
            DataType.BIGINT, DataType.NUMERIC, DataType.REAL, DataType.DOUBLE_PRECISION};

    /**
     * What {@code a + b} over two numbers is declared to be: the wider of the two operand types.
     * Anything that is not plain arithmetic over two types on the ladder says nothing.
     */
    private String arithmeticResultType(BinaryExpr bin, RowContext ctx) {
        switch (bin.op()) {
            case ADD: case SUBTRACT: case MULTIPLY: case DIVIDE: case MODULO:
                break;
            default:
                return null;
        }
        int left = ladderRank(declaredTypeForResolution(bin.left(), ctx));
        int right = ladderRank(declaredTypeForResolution(bin.right(), ctx));
        if (left < 0 || right < 0) return null;
        return NUMERIC_LADDER[Math.max(left, right)].getPgName();
    }

    private static int ladderRank(String declared) {
        if (declared == null) return -1;
        DataType type = DataType.fromPgName(declared.toLowerCase(java.util.Locale.ROOT).trim());
        for (int i = 0; type != null && i < NUMERIC_LADDER.length; i++) {
            if (NUMERIC_LADDER[i] == type) return i;
        }
        return -1;
    }

    /**
     * A shift is done in the width of the value being shifted, and answers in that width.
     *
     * <p>Computing in long and widening whenever the answer did not fit an int gave results of a
     * type PostgreSQL never produces: {@code 1::int4 << 31} answered 2147483648 where the int4
     * result is -2147483648, and {@code 1 << 32} answered 4294967296 where the shift count is
     * taken as the low bits of the width, making it 1 again.
     */
    private Object integerShift(Object left, Object right, boolean leftwards) {
        long count = executor.toLong(right);
        if (left instanceof Long) {
            long value = executor.toLong(left);
            return Long.valueOf(leftwards ? value << count : value >> count);
        }
        if (left instanceof Short || left instanceof Byte) {
            int value = executor.toInt(left);
            int shifted = leftwards ? value << (count & 15) : value >> (count & 15);
            return Short.valueOf((short) shifted);
        }
        int value = executor.toInt(left);
        return Integer.valueOf(leftwards ? value << count : value >> count);
    }

    /** The operators that order or equate two values. */
    private static boolean isComparisonOp(BinaryExpr.BinOp op) {
        switch (op) {
            case EQUAL: case NOT_EQUAL: case LESS_THAN: case GREATER_THAN:
            case LESS_EQUAL: case GREATER_EQUAL:
            case IS_DISTINCT_FROM: case IS_NOT_DISTINCT_FROM:
                return true;
            default:
                return false;
        }
    }

    /**
     * The type an operand is declared to have for the purpose of resolving an operator: what the
     * query wrote down — a cast or a literal — or, for a column, the type its table declares.
     *
     * <p>A derived column is deliberately excluded. Out of a subquery, a CTE or a set-returning
     * function a column carries whatever type the engine defaulted it to, which is text far more
     * often than it should be; refusing an operator on the strength of that rejects SQL
     * PostgreSQL runs — {@code sub.rn >= 1} over a window function, say — and that is a worse
     * failure than the permissiveness this rule removes.
     */
    String declaredTypeForResolution(Expression expr, RowContext ctx) {
        // A call an enclosing query level has already answered is of the type it answered with.
        // Its arguments name that level's columns, which this one cannot read, so asking what it
        // would resolve to here asks about a call nothing here defines.
        ExprEvaluator.PrecomputedValueExpr answered = executor.exprEvaluator.levelFoldedFor(expr);
        if (answered != null) {
            return answered.declaredType() == null ? null : answered.declaredType().getPgName();
        }
        // A test answers yes or no, whatever it is a test of, so an operator written over one is
        // written over a boolean. Reading a test as saying nothing about its type left
        // 1 = (1 IN (1,2)) to be run as a comparison of an integer with a boolean, which
        // PostgreSQL has no operator for.
        if (isBooleanByConstruction(expr)) return "boolean";
        // A subscript is declared what one element of the thing subscripted is declared, and a
        // range of them is declared what the whole is: without that a bpchar element read as text
        // kept the blanks its declaration padded it to.
        if (expr instanceof SubscriptExpr) {
            SubscriptExpr sub = (SubscriptExpr) expr;
            String base = declaredTypeForResolution(sub.base(), ctx);
            if (base == null) {
                DataType inferred = executor.exprEvaluator.inferExprType(sub.base());
                base = inferred == null ? null : inferred.getPgName();
            }
            if (base == null) return null;
            if (sub.isSlice()) return base;
            if (base.endsWith("[]")) return base.substring(0, base.length() - 2);
            // The catalogs' two vectors are subscripted like arrays without being written like
            // one, so what a subscript of them is has to be said rather than derived.
            if (base.equals("int2vector")) return "smallint";
            if (base.equals("oidvector")) return "oid";
            if (base.equals("point")) return "double precision";
            if (base.equals("box") || base.equals("lseg") || base.equals("path")
                    || base.equals("polygon")) {
                return "point";
            }
            DataType baseType = DataType.fromPgName(base);
            DataType element = baseType == null ? null : DataType.elementOf(baseType);
            return element != null && element != baseType ? element.getPgName() : base;
        }
        if (expr instanceof CastExpr) {
            String name = ((CastExpr) expr).typeName();
            return name == null ? null : name.toLowerCase(java.util.Locale.ROOT).trim();
        }
        if (expr instanceof Literal) {
            switch (((Literal) expr).literalType()) {
                // A whole number is the narrowest of the three integer types that holds it, and
                // that is the type a call is resolved against: make_time is declared over integer
                // and a literal too wide for one reaches no signature it has.
                case INTEGER:
                    return ExprEvaluator.integerLiteralType((Literal) expr).toRegtypeDisplay();
                case FLOAT: return "numeric";
                case BOOLEAN: return "boolean";
                case BIT_STRING: return "bit";
                default: return null;
            }
        }
        if (expr instanceof BinaryExpr) {
            // Arithmetic over two numbers answers with the wider of them, so the operand of an
            // enclosing operator is a number and not something to be left unjudged: it is what
            // makes 1 || 2 + 3 a concatenation of two integers rather than one of unknown shape.
            // Anything else falls through to the operators below that name their own type.
            String arithmetic = arithmeticResultType((BinaryExpr) expr, ctx);
            if (arithmetic != null) return arithmetic;
            // Running two values together answers with text wherever the pair resolves to the
            // text concatenation, and that is as much part of the query as a cast is: read as
            // saying nothing, 1 = 1 || 'x' was compared as two numbers where PostgreSQL has no
            // operator between an integer and the text the concatenation gives.
            String concatenated = textConcatResultType((BinaryExpr) expr, ctx);
            if (concatenated != null) return concatenated;
        }
        if (expr instanceof UnaryExpr) {
            // A sign says nothing about the type: -4 is the integer 4 was, and reading it as
            // nothing in particular left the call it stood in to be resolved on a preferred type
            // instead — so abs(-4) was a double precision where abs(4) was an integer.
            UnaryExpr un = (UnaryExpr) expr;
            if (un.op == UnaryExpr.UnaryOp.NEGATE || un.op == UnaryExpr.UnaryOp.POSITIVE) {
                return declaredTypeForResolution(un.operand, ctx);
            }
            return null;
        }
        if (expr instanceof FunctionCallExpr) {
            // What a built-in answers with is part of what the query says, the same as a cast is:
            // now() is a timestamptz wherever it stands, so a literal beside it is read as one
            // rather than as a number. Only a call that settles on a single declared signature
            // says anything; the rest are left unresolved as before.
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            List<Expression> args = fn.args() == null ? new ArrayList<Expression>() : fn.args();
            int[] written = new int[args.size()];
            for (int i = 0; i < args.size(); i++) {
                String declared = declaredTypeForResolution(args.get(i), ctx);
                DataType t = declared == null ? null : DataType.fromPgName(declared);
                // A width is no part of which type an argument is: read whole, bit(4) named no
                // type at all and a call over it could not be resolved.
                if (t == null && declared != null && declared.indexOf('(') > 0) {
                    t = DataType.fromPgName(declared.substring(0, declared.indexOf('(')).trim());
                }
                written[i] = t == null ? 0 : t.getOid();
            }
            DataType result = DataType.fromOid(BuiltinCallTypes.resultType(fn.name(), written));
            if (result != null) return result.getPgName();
            // A routine the reader wrote says what it answers with in its own declaration, which
            // is as much part of the query as a built-in's is — and a character(n) result read as
            // text kept the blanks the declaration padded it to.
            PgFunction userFunc = executor.database.getFunction(
                    FunctionEvaluator.stripSchemaPrefix(fn.name().toLowerCase(java.util.Locale.ROOT)));
            if (userFunc != null && userFunc.getReturnType() != null
                    && !userFunc.isSetReturning()) {
                String declared = userFunc.getReturnType().trim();
                if (!PolymorphicTypes.isPolymorphic(declared)) return declared;
            }
            return null;
        }
        if (expr instanceof ArrayExpr && !((ArrayExpr) expr).isRow) {
            // ARRAY[2.5] is a numeric[], and a call taking an int[] does not take one. Reading the
            // constructor as saying nothing left every array argument unjudged.
            List<Expression> elements = ((ArrayExpr) expr).elements;
            if (elements == null || elements.isEmpty()) return null;
            String element = declaredTypeForResolution(elements.get(0), ctx);
            for (int i = 1; i < elements.size(); i++) {
                String other = declaredTypeForResolution(elements.get(i), ctx);
                if (element == null ? other != null : !element.equals(other)) return null;
            }
            // Elements that say nothing about themselves are read as text, the preferred type of
            // the category PostgreSQL settles an untyped literal to: ARRAY['a','b'] is a text[].
            if (element == null) {
                for (int i = 0; i < elements.size(); i++) {
                    if (!isUntypedStringLiteral(elements.get(i))) return null;
                }
                element = "text";
            }
            return element + "[]";
        }
        // Subscripting an array gives one element of it, which is of the array's element type.
        if (expr instanceof BinaryExpr
                && ((BinaryExpr) expr).op() == BinaryExpr.BinOp.JSON_SUBSCRIPT) {
            if (ctx != null && ((BinaryExpr) expr).left() instanceof ColumnRef) {
                ColumnRef base = (ColumnRef) ((BinaryExpr) expr).left();
                Column baseDef = ctx.resolveColumnDef(base.table(), base.column());
                if (baseDef != null && baseDef.getArrayElementType() != null) {
                    return baseDef.getArrayElementType().getPgName();
                }
            }
            String arrayType = declaredTypeForResolution(((BinaryExpr) expr).left(), ctx);
            return arrayType != null && arrayType.endsWith("[]")
                    ? arrayType.substring(0, arrayType.length() - 2) : null;
        }
        if (expr instanceof SubqueryExpr) {
            // A scalar subquery is of the type of the one column it answers with.
            return declaredTypeOfSingleTarget(((SubqueryExpr) expr).subquery(), ctx);
        }
        if (expr instanceof ColumnRef && ctx != null) {
            ColumnRef ref = (ColumnRef) expr;
            // A system column is typed by what it is, not by the relation's declarations, which
            // the loop below is all that reads: xmin > 0 ran where PostgreSQL has no
            // xid > integer, and ctid::bigint was judged against whatever the value looked like.
            DataType system = SystemColumns.resolve(ref, ctx.getBindings());
            if (system != null) return system.getPgName();
            for (RowContext.TableBinding b : ctx.getBindings()) {
                // The written name first: it costs a comparison, where deciding whether the
                // binding is a base table costs a walk of the schemas.
                if (ref.table() != null && !ref.table().equalsIgnoreCase(b.alias())
                        && (b.table() == null || !ref.table().equalsIgnoreCase(b.table().getName()))) continue;
                if (!readsItsColumnTypes(b.table())) continue;
                int idx = b.table().getColumnIndex(ref.column());
                if (idx < 0) continue;
                if (!columnTypeIsDeclared(b.table(), idx)) return null;
                // A column of a composite type is of that type, whatever it is stored as. The
                // stored type is text, and answering text said the row was a string.
                if (b.table().getColumns().get(idx).getCompositeTypeName() != null) return null;
                Column def = b.table().getColumns().get(idx);
                DataType t = def.getType();
                if (t == DataType.INTERNAL_CHAR) return "\"char\"";
                if (t == null) return null;
                // An array of an enum carries the enum as the column's own type, with the
                // element type beside it: read from the type alone the column was a single
                // value, so = ANY over it had no array to search and was refused.
                if (def.getArrayElementType() != null && !DataType.isArrayType(t)) {
                    String element = def.getEnumTypeName() != null
                            ? TypeNamespace.nameOfKey(def.getEnumTypeName())
                            : def.getArrayElementType().getPgName();
                    return element + "[]";
                }
                return t.getPgName();
            }
        }
        return null;
    }

    /** The operators whose answer is a truth value rather than a value of the operand's type. */
    private static final java.util.Set<BinaryExpr.BinOp> BOOLEAN_RESULT_OPS =
            java.util.Collections.unmodifiableSet(java.util.EnumSet.of(
                    BinaryExpr.BinOp.EQUAL, BinaryExpr.BinOp.NOT_EQUAL,
                    BinaryExpr.BinOp.LESS_THAN, BinaryExpr.BinOp.GREATER_THAN,
                    BinaryExpr.BinOp.LESS_EQUAL, BinaryExpr.BinOp.GREATER_EQUAL,
                    BinaryExpr.BinOp.AND, BinaryExpr.BinOp.OR,
                    BinaryExpr.BinOp.LIKE, BinaryExpr.BinOp.ILIKE, BinaryExpr.BinOp.SIMILAR_TO,
                    BinaryExpr.BinOp.REGEX_MATCH, BinaryExpr.BinOp.REGEX_IMATCH,
                    BinaryExpr.BinOp.NOT_REGEX_MATCH, BinaryExpr.BinOp.NOT_REGEX_IMATCH,
                    BinaryExpr.BinOp.IS_DISTINCT_FROM, BinaryExpr.BinOp.IS_NOT_DISTINCT_FROM));

    /** Whether an expression is a truth value by the shape it is written in. */
    private static boolean isBooleanByConstruction(Expression expr) {
        if (expr instanceof InExpr || expr instanceof BetweenExpr || expr instanceof LikeExpr
                || expr instanceof IsNullExpr || expr instanceof IsBooleanExpr
                || expr instanceof IsJsonExpr || expr instanceof ExistsExpr
                || expr instanceof AnyAllExpr || expr instanceof AnyAllArrayExpr) {
            return true;
        }
        if (expr instanceof UnaryExpr) return ((UnaryExpr) expr).op == UnaryExpr.UnaryOp.NOT;
        return expr instanceof BinaryExpr && BOOLEAN_RESULT_OPS.contains(((BinaryExpr) expr).op());
    }

    /**
     * True when a binding is a table the catalogue actually holds, so its column types are the
     * ones the user declared rather than ones the engine inferred while building a result.
     */
    private boolean isBaseTable(Table table) {
        if (table == null || table.isFunctionResult() || table.isViewProjection()) return false;
        return executor.baseTableNamed(table.getName()) == table;
    }

    /**
     * True when a binding's columns carry a type worth resolving on. A base table's are the ones
     * the user declared; a derived table's — a subquery in FROM, a CTE — are the ones its own
     * projection settled, which is as much a part of what the statement says. Only a set-returning
     * call's columns are left out: those are made up while the result is built.
     *
     * <p>Reading a derived column as saying nothing meant the same call was judged one way written
     * against a table and another written against a subquery over it.
     */
    private boolean readsItsColumnTypes(Table table) {
        return isBaseTable(table) || isDerivedProjection(table);
    }

    /**
     * True when a binding is a result the statement itself built — a subquery in FROM, a CTE —
     * whose columns were typed from its own projection.
     *
     * <p>Those types are as much a part of what the statement says as a declared column's are, but
     * only where the engine worked one out: a column it could not type is text, which is also the
     * type of every column that really is one. Reading that fallback as a declaration said a
     * number was a string, and the call or operator over it was resolved against the wrong type
     * — so text is the one answer this leaves alone, as it was left alone before.
     */
    private boolean isDerivedProjection(Table table) {
        if (table == null || table.isFunctionResult() || table.isViewProjection()) return false;
        return executor.baseTableNamed(table.getName()) != table;
    }

    /** Whether the column at {@code index} of {@code table} is one whose type may be read. */
    private boolean columnTypeIsDeclared(Table table, int index) {
        if (isBaseTable(table)) return true;
        DataType t = table.getColumns().get(index).getType();
        return t != null && t != DataType.TEXT;
    }

    /**
     * The type of the one column a subquery answers with, or null when it answers with more than
     * one or with something this rule cannot type.
     */
    private String declaredTypeOfSingleTarget(Statement stmt, RowContext ctx) {
        if (!(stmt instanceof SelectStmt)) return null;
        SelectStmt select = (SelectStmt) stmt;
        List<SelectStmt.SelectTarget> targets = select.targets;
        if (targets == null || targets.size() != 1) return null;
        Expression target = targets.get(0).expr();
        if (target instanceof ColumnRef && select.from != null && select.from.size() == 1) {
            // The column of a single named table, which is the only FROM this rule reads.
            ColumnRef ref = (ColumnRef) target;
            SelectStmt.FromItem source = select.from.get(0);
            if (!(source instanceof SelectStmt.TableRef)) return null;
            Table table = executor.baseTableNamed(((SelectStmt.TableRef) source).table);
            if (table == null) return null;
            int idx = table.getColumnIndex(ref.column());
            if (idx < 0) return null;
            DataType t = table.getColumns().get(idx).getType();
            return t == null ? null : t.getPgName();
        }
        // Anything else is read only where it says what it is on its own. Reading it against the
        // context outside the subquery types its columns as nothing in particular, and a call
        // resolved on that guesses -- max(k) over an unknown k reads as the preferred type of the
        // one category any candidate takes, which is text, for a column that is an integer.
        if (target instanceof CastExpr || target instanceof Literal) {
            return declaredTypeForResolution(target, null);
        }
        return null;
    }

    private static boolean isArithmetic(BinaryExpr.BinOp op) {
        switch (op) {
            case ADD: case SUBTRACT: case MULTIPLY: case DIVIDE: case MODULO: case POWER:
                return true;
            default:
                return false;
        }
    }

    /** The family a declared type belongs to, or null when it is one this rule leaves alone. */
    private static TypeFamily familyOf(String typeName) {
        String t = typeName.toLowerCase(java.util.Locale.ROOT).trim();
        int paren = t.indexOf('(');
        if (paren > 0) t = t.substring(0, paren).trim();
        if (t.endsWith("[]")) return null;
        switch (t) {
            case "text": case "varchar": case "character varying": case "char":
            case "character": case "bpchar": case "name":
                return TypeFamily.STRING;
            case "smallint": case "integer": case "int": case "int2": case "int4": case "int8":
            case "bigint": case "numeric": case "decimal": case "real": case "double precision":
            case "float4": case "float8":
            case "serial": case "bigserial": case "smallserial":
                return TypeFamily.NUMERIC;
            case "boolean": case "bool":
                return TypeFamily.BOOLEAN;
            case "json":
                // json alone: it carries no comparison operators at all, not even equality.
                // jsonb does have them, so it is deliberately left out of this rule.
                return TypeFamily.JSON;
            case "date": case "timestamp": case "timestamptz":
            case "timestamp without time zone": case "timestamp with time zone":
                // PostgreSQL really does compare a date with a timestamp, so they share a family
                return TypeFamily.DATETIME;
            case "time": case "timetz":
            case "time without time zone": case "time with time zone":
                return TypeFamily.TIMEOFDAY;
            case "interval":
                return TypeFamily.INTERVAL;
            case "uuid":
                return TypeFamily.UUID;
            case "inet": case "cidr":
                return TypeFamily.NETWORK;
            case "bytea":
                return TypeFamily.BYTES;
            default:
                // Arrays, geometry, ranges, enums, domains and the rest have their own operator
                // sets and cross-type rules; guessing at them would reject SQL PostgreSQL accepts.
                return null;
        }
    }

    /** The name PostgreSQL prints for a type in an operator error. */
    private static String pgName(String typeName) {
        String t = typeName.toLowerCase(java.util.Locale.ROOT).trim();
        switch (t) {
            case "int": case "int4": case "serial": return "integer";
            case "int2": case "smallserial": return "smallint";
            case "int8": case "bigserial": return "bigint";
            case "float8": return "double precision";
            case "float4": return "real";
            case "varchar": return "character varying";
            case "char": case "bpchar": return "character";
            case "bool": return "boolean";
            case "decimal": return "numeric";
            case "timestamp": return "timestamp without time zone";
            case "timestamptz": return "timestamp with time zone";
            case "time": return "time without time zone";
            case "timetz": return "time with time zone";
            case "\"char\"": return "\"char\"";
            case "varbit": return "bit varying";
            case "citext": return "citext";
            default: return t;
        }
    }

    /** The answer a comparison operator gives for a comparison result. */
    private static Boolean compareOp(BinaryExpr.BinOp op, int cmp) {
        switch (op) {
            case LESS_THAN: return cmp < 0;
            case LESS_EQUAL: return cmp <= 0;
            case GREATER_THAN: return cmp > 0;
            case GREATER_EQUAL: return cmp >= 0;
            case EQUAL: return cmp == 0;
            case NOT_EQUAL: return cmp != 0;
            default: return null;
        }
    }

    private static boolean isComparison(BinaryExpr.BinOp op) {
        switch (op) {
            case EQUAL: case NOT_EQUAL: case LESS_THAN: case GREATER_THAN:
            case LESS_EQUAL: case GREATER_EQUAL:
                return true;
            default:
                return false;
        }
    }

    /** The geometric type an operand is declared to have, or null when it carries no declaration. */
    /**
     * Refuse a bare literal that will not read as the shape the operator wants.
     *
     * <p>An operator between two shapes is resolved by the side whose type is known, and the
     * literal beside it is then read with the reader of that type. Reading it as whatever it
     * happens to look like resolved a different operator from the one PostgreSQL resolves.
     */
    private void requireLiteralReadsAsOperandType(BinaryExpr bin, RowContext ctx) {
        String lType = declaredGeometricType(bin.left(), ctx);
        String rType = declaredGeometricType(bin.right(), ctx);
        if (lType != null && rType == null && isBareStringLiteral(bin.right())) {
            GeometricOperations.parseAs(lType, ((Literal) bin.right()).value());
        } else if (rType != null && lType == null && isBareStringLiteral(bin.left())) {
            GeometricOperations.parseAs(rType, ((Literal) bin.left()).value());
        }
    }

    /** A value written as a quoted string and nothing else, so its type is still open. */
    private static boolean isBareStringLiteral(Expression expr) {
        return expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.STRING;
    }

    private String declaredGeometricType(Expression expr, RowContext ctx) {
        if (expr instanceof CastExpr) {
            String name = ((CastExpr) expr).typeName();
            return name == null ? null : geometricTypeName(name.toLowerCase(java.util.Locale.ROOT).trim());
        }
        if (expr instanceof ColumnRef && ctx != null) {
            ColumnRef ref = (ColumnRef) expr;
            for (RowContext.TableBinding b : ctx.getBindings()) {
                if (b.table() == null) continue;
                if (ref.table() != null && !ref.table().equalsIgnoreCase(b.alias())
                        && !ref.table().equalsIgnoreCase(b.table().getName())) continue;
                int idx = b.table().getColumnIndex(ref.column());
                if (idx < 0) continue;
                DataType t = b.table().getColumns().get(idx).getType();
                return t == null ? null : geometricTypeName(t.getPgName());
            }
        }
        return null;
    }

    private static String geometricTypeName(String name) {
        switch (name) {
            case "point": case "line": case "lseg":
            case "box": case "path": case "polygon": case "circle":
                return name;
            default:
                return null;
        }
    }

    static boolean isCastToType(Expression expr, String typeName) {
        if (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            return cast.typeName().equalsIgnoreCase(typeName);
        }
        return false;
    }

    /**
     * Check if an expression resolves to jsonb type — covers explicit casts AND column references
     * to jsonb-typed columns. Used for || operator to distinguish jsonb concat from text concat.
     */
    private boolean isJsonbExpression(Expression expr, RowContext ctx) {
        if (isCastToType(expr, "jsonb")) return true;
        if (expr instanceof ColumnRef && ctx != null) {
            ColumnRef ref = (ColumnRef) expr;
            for (RowContext.TableBinding b : ctx.getBindings()) {
                if (ref.table != null) {
                    String bindName = b.alias() != null ? b.alias() : b.table().getName();
                    if (!bindName.equalsIgnoreCase(ref.table)) continue;
                }
                int idx = b.table().getColumnIndex(ref.column);
                if (idx >= 0 && b.table().getColumns().get(idx).getType() == DataType.JSONB) {
                    return true;
                }
            }
        }
        return false;
    }

    /** True for the name a jsonb operand is declared with. */
    static boolean isJsonbTypeName(String typeName) {
        return typeName != null && typeName.equalsIgnoreCase("jsonb");
    }

    private static boolean isCastToTextType(Expression expr) {
        if (expr instanceof CastExpr) {
            String tn = ((CastExpr) expr).typeName().toLowerCase(java.util.Locale.ROOT);
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

        // Resolved from the declared types alone, so it is decided before an untyped literal is
        // read: PostgreSQL reports the operator it could not find even when that literal is text
        // the type could never have parsed.
        rejectMissingEqualityOperator(bin, ctx);
        rejectCrossCategoryOperator(bin, ctx);
        rejectOperatorWithNoEntry(bin, ctx);
        ConcatResolution concat = concatOutcome(bin, ctx);

        Object left = executor.evalExpr(bin.left(), ctx);
        Object right = executor.evalExpr(bin.right(), ctx);

        // A bpchar ignores trailing blanks when it is compared, and no other string type does.
        // Where one side is declared bpchar the pair is compared that way; everywhere else the
        // blanks count, so 'abc ' = 'abc' is false as PostgreSQL says it is.
        if (isComparisonOp(bin.op()) && left instanceof String && right instanceof String
                && (BlankPadding.isBlankPadded(declaredTypeForResolution(bin.left(), ctx))
                    || BlankPadding.isBlankPadded(declaredTypeForResolution(bin.right(), ctx)))) {
            left = BlankPadding.trimmed(left);
            right = BlankPadding.trimmed(right);
        }

        // jsonb is a value and not the text it prints as, so two documents are compared as
        // documents. Comparing their texts said 1 and 1.0 were different values and ordered
        // containers by their first character, which put [1, 2] before [3].
        if (isComparisonOp(bin.op()) && left instanceof String && right instanceof String
                && (isJsonbTypeName(declaredTypeForResolution(bin.left(), ctx))
                    || isJsonbTypeName(declaredTypeForResolution(bin.right(), ctx)))) {
            int cmp = JsonOperations.compareJsonb((String) left, (String) right);
            if (bin.op() == BinaryExpr.BinOp.IS_DISTINCT_FROM) return cmp != 0;
            if (bin.op() == BinaryExpr.BinOp.IS_NOT_DISTINCT_FROM) return cmp == 0;
            return compareOp(bin.op(), cmp);
        }

        // The two concatenations that take a text on one side read the other as text too, and
        // read it with that type's own output function. Running the stored values together
        // instead printed whatever Java made of them — a timestamp as 2020-01-01T00:00, a real as
        // 1.0 — and a bytea beside a text came back a bytea rather than the text it prints as.
        if (concat.is(ConcatOperator.Outcome.TEXT_CONCAT)) {
            if (left == null || right == null) return null;
            return concatOperandAsText(left, concat.leftOid)
                    + concatOperandAsText(right, concat.rightOid);
        }
        // An operator declared over one type reads both operands as it, so an operand the
        // statement left untyped is that type's input to parse — and 'a' is no more a jsonb than
        // it is a binary digit. Running the two values together accepted both.
        if (concat.is(ConcatOperator.Outcome.SAME_TYPE) && concat.resolution.sameType != null) {
            if (concat.leftOid == 0) left = executor.castValue(left, concat.resolution.sameType);
            if (concat.rightOid == 0) right = executor.castValue(right, concat.resolution.sameType);
        }
        // An array joined to a NULL element gains the element; joined to a NULL array it is
        // unchanged. Which of the two was written is in the declared types, not in the values:
        // reading only the values dropped the element and left the array a member short.
        if (concat.is(ConcatOperator.Outcome.ARRAY) && (left == null || right == null)
                && (left == null ? concat.leftOid : concat.rightOid) != 0) {
            boolean nullIsArray = DataType.isArrayType(
                    DataType.fromOid(left == null ? concat.leftOid : concat.rightOid));
            Object present = left == null ? right : left;
            if (nullIsArray) return present;
            PgArray array = PgArray.from(present);
            if (array != null) {
                java.util.List<Object> merged = new ArrayList<>();
                if (left == null) merged.add(null);
                merged.addAll(array);
                if (right == null) merged.add(null);
                return array.resized(merged);
            }
        }
        // An array concatenation takes its element type from the left operand, so an element
        // joined on the right is read as that type — and a blank-padded string loses its padding
        // going in. Written on the left it is the element type, and keeps it.
        if (concat.is(ConcatOperator.Outcome.ARRAY) && right != null
                && concat.rightOid == DataType.CHAR.getOid()
                && !isBlankPaddedOrItsArray(concat.leftOid)) {
            right = concatOperandAsText(right, concat.rightOid);
        }

        // A bare string literal has no type of its own. PostgreSQL resolves it against the other
        // operand; guessing from its shape instead picks a type the query never mentioned, which
        // is how '192.168.1.1' next to an inet came out as a point.
        String resolveTo = dateTimeLiteralTarget(bin, ctx);
        if (resolveTo == null) resolveTo = untypedLiteralTargetType(bin, ctx);
        if (resolveTo != null) {
            if (isUntypedStringLiteral(bin.left())) {
                checkMultirangeLiteral(bin.op(), resolveTo, left);
                left = executor.castValue(left, resolveTo);
            } else {
                checkMultirangeLiteral(bin.op(), resolveTo, right);
                right = executor.castValue(right, resolveTo);
            }
        }

        rejectRangeElementTypeMismatch(bin, ctx);
        rejectPhantomOperator(bin, ctx, left, right);

        // Two ranges are equal when their bounds are, whatever scale each bound was written with:
        // a numrange bound spelled 1.50 is the same bound as one spelled 1.5. Only a pair the
        // query declares to be ranges is read that way, because a point is written with the same
        // parentheses a range is.
        if ((bin.op() == BinaryExpr.BinOp.EQUAL || bin.op() == BinaryExpr.BinOp.NOT_EQUAL)
                && left instanceof String && right instanceof String
                && declaredRangeTypeStrict(bin.left(), ctx) != null
                && declaredRangeTypeStrict(bin.right(), ctx) != null) {
            boolean same = TypeCoercion.compare(left, right) == 0;
            return bin.op() == BinaryExpr.BinOp.EQUAL ? same : !same;
        }

        // Two paths added are joined, not translated: the second operand is a path and not the
        // point a value-level reading took it for. Only the declared types can say which of the
        // two operators was written, since both shapes are stored as parenthesised point lists.
        if (bin.op() == BinaryExpr.BinOp.ADD && "path".equals(declaredOperandType(bin.left(), ctx))
                && ("path".equals(declaredOperandType(bin.right(), ctx))
                    || isUntypedStringLiteral(bin.right()))) {
            if (left == null || right == null) return null;
            return GeometricOperations.pathConcat(left.toString(), right.toString());
        }

        // A range has no || of its own, so PostgreSQL resolves the one written beside it to
        // anynonarray || text and answers with the two spellings run together. Two declared
        // ranges leave nothing for that rule to reach, and PostgreSQL says so.
        if (bin.op() == BinaryExpr.BinOp.CONCAT) {
            String lRange = declaredRangeTypeStrict(bin.left(), ctx);
            String rRange = declaredRangeTypeStrict(bin.right(), ctx);
            if (lRange != null && rRange != null) {
                throw new MemgresException("operator does not exist: " + lRange + " || " + rRange
                        + "\n  Hint: No operator matches the given name and argument types. You might need to add explicit type casts.", "42883");
            }
            if (lRange != null || rRange != null) {
                if (left == null || right == null) return null;
                return left.toString() + right.toString();
            }
        }

        // A range is stored as its text, so the values alone cannot tell one from a pair of dotted
        // numbers -- which is how numrange * numrange came out as inet * inet. Where the query
        // declares a range type the operator is resolved from that declaration instead.
        if (bin.op() == BinaryExpr.BinOp.MULTIPLY || bin.op() == BinaryExpr.BinOp.ADD
                || bin.op() == BinaryExpr.BinOp.SUBTRACT) {
            Object combined = declaredRangeArithmetic(bin, ctx, left, right);
            if (combined != NOT_A_RANGE_OPERATION) return combined;
        }

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

        // A write-ahead log position and a count of bytes: the position moves by that many
        // bytes and stays a position. Carried as text, it reached the arithmetic as a string
        // and was refused as "text + integer".
        if (bin.op() == BinaryExpr.BinOp.ADD || bin.op() == BinaryExpr.BinOp.SUBTRACT) {
            Object moved = lsnMovedByBytes(bin, left, right);
            if (moved != NOT_LSN_ARITHMETIC) return moved;
        }

        // json type (not jsonb) does not support || or - operators
        if (bin.op() == BinaryExpr.BinOp.CONCAT || bin.op() == BinaryExpr.BinOp.SUBTRACT) {
            if (isCastToType(bin.left(), "json") || isCastToType(bin.right(), "json")) {
                String opSym = bin.op() == BinaryExpr.BinOp.CONCAT ? "||" : "-";
                throw new MemgresException("operator does not exist: json " + opSym + " json", "42883");
            }
        }

        // A json array is written the way a range is, so a document reaching containment was
        // read as a range and '[2,3]' was said to lie inside '[1,5]'. The declared type says
        // which operator was written, and a document is never a range.
        if ((bin.op() == BinaryExpr.BinOp.CONTAINS || bin.op() == BinaryExpr.BinOp.CONTAINED_BY)
                && left != null && right != null
                && (isJsonbExpression(bin.left(), ctx) || isJsonbExpression(bin.right(), ctx))) {
            String container = bin.op() == BinaryExpr.BinOp.CONTAINS
                    ? left.toString() : right.toString();
            String held = bin.op() == BinaryExpr.BinOp.CONTAINS
                    ? right.toString() : left.toString();
            return JsonOperations.contains(container, held);
        }

        // A jsonb scalar reaches @@ looking like a tsvector would; the declared type says which
        // operator was written, and a jsonpath predicate is never a tsquery.
        if (bin.op() == BinaryExpr.BinOp.TS_MATCH && left != null && right != null
                && isJsonbExpression(bin.left(), ctx)) {
            return executor.functionEvaluator.evaluateJsonPathMatchSilent(
                    left.toString().trim(), right.toString().trim());
        }

        // PostgreSQL declares four @@ operators and they disagree on how to read a bare string,
        // so the answer depends on which one was written. Only here are both operands still
        // expressions, so this is where that can be told.
        if (bin.op() == BinaryExpr.BinOp.TS_MATCH && left != null && right != null) {
            return TextSearchOperations.matches(left, right,
                    matchOperandType(bin.left(), ctx), matchOperandType(bin.right(), ctx));
        }

        // A jsonb scalar is stored as its own text, so only the declared type tells
        // '"x"'::jsonb - 'a' apart from subtracting one text value from another. PG has nothing
        // to delete from a scalar and says so instead of falling back to arithmetic.
        if (bin.op() == BinaryExpr.BinOp.SUBTRACT && left instanceof String && right != null
                && isJsonbExpression(bin.left(), ctx) && JsonOperations.isScalarValue((String) left)) {
            throw new MemgresException("cannot delete from scalar", "22023");
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
                String fnName = ((FunctionCallExpr) bin.left()).name().toLowerCase(java.util.Locale.ROOT);
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

        // jsonb || jsonb: when an operand is jsonb (explicit cast or jsonb column), both
        // sides are jsonb in PG. This handles scalar jsonb operands (e.g. '1'::jsonb || '2'::jsonb)
        // that the container-string heuristics in evalBuiltinBinary cannot recognize.
        // The other operand must itself be jsonb or an untyped literal PG would coerce;
        // there is no implicit cast from other types, so jsonb || 1 is 42883 in PG.
        if (bin.op() == BinaryExpr.BinOp.CONCAT
                && (isJsonbExpression(bin.left(), ctx) || isJsonbExpression(bin.right(), ctx))
                && !(left instanceof List) && !(right instanceof List)
                && !(left instanceof HstoreValue) && !(right instanceof HstoreValue)) {
            if (left == null || right == null) return null;
            if (!isJsonbExpression(bin.left(), ctx) && !(left instanceof String)) {
                throw new MemgresException("operator does not exist: "
                        + AstExecutor.pgTypeNameOf(left) + " || jsonb", "42883");
            }
            if (!isJsonbExpression(bin.right(), ctx) && !(right instanceof String)) {
                throw new MemgresException("operator does not exist: jsonb || "
                        + AstExecutor.pgTypeNameOf(right), "42883");
            }
            return JsonOperations.concatenate(left.toString(), right.toString());
        }

        // text -> int / text[i] is not supported in PG; reject if left is explicitly text-typed
        if ((bin.op() == BinaryExpr.BinOp.JSON_ARROW || bin.op() == BinaryExpr.BinOp.JSON_ARROW_TEXT
                || bin.op() == BinaryExpr.BinOp.JSON_SUBSCRIPT)
                && isCastToTextType(bin.left()) && left instanceof String) {
            throw new MemgresException("operator does not exist: text -> integer", "42883");
        }

        // A range beside anything that is not a range has no << or >> to resolve to. Naming the
        // operands from what the query wrote down is what makes the error read as PostgreSQL's.
        if (bin.op() == BinaryExpr.BinOp.SHIFT_LEFT || bin.op() == BinaryExpr.BinOp.SHIFT_RIGHT) {
            // A range the query names as one counts whatever its value looks like: "empty" and
            // the empty multirange "{}" are ranges that no text form gives away.
            boolean lRange = isRangeTypeName(declaredOperandType(bin.left(), ctx))
                    || isRangeText(left);
            boolean rRange = isRangeTypeName(declaredOperandType(bin.right(), ctx))
                    || isRangeText(right);
            if (lRange != rRange) {
                throw noSuchShiftOperator(
                        operandTypeName(bin.left(), left, ctx),
                        bin.op() == BinaryExpr.BinOp.SHIFT_LEFT,
                        operandTypeName(bin.right(), right, ctx));
            }
        }

        // One date taken from another answers with a count of days, and no count of days reaches
        // an endless date. The same two written as timestamps do have an answer — an endless
        // interval — so which it is turns on the declaration, not on the value.
        if (bin.op() == BinaryExpr.BinOp.SUBTRACT
                && (TypeCoercion.isDateTimeInfinity(left) || TypeCoercion.isDateTimeInfinity(right))
                && isDateTypeName(declaredOperandType(bin.left(), ctx))
                && isDateTypeName(declaredOperandType(bin.right(), ctx))) {
            throw new MemgresException("cannot subtract infinite dates", "22008");
        }

        // Operator type mismatch validation (before coercion)
        // A bare quoted literal beside a bit string is read as a bit string: B'101' = '101' asks
        // whether two bit strings are the same, and reading the literal as text found no operator
        // between the two types.
        if (bin.op() == BinaryExpr.BinOp.EQUAL) {
            if (left instanceof AstExecutor.PgBitString && right instanceof String
                    && isBareStringLiteral(bin.right())) {
                right = new AstExecutor.PgBitString((String) right);
            } else if (right instanceof AstExecutor.PgBitString && left instanceof String
                    && isBareStringLiteral(bin.left())) {
                left = new AstExecutor.PgBitString((String) left);
            }
        }
        // A value of a composite type is kept as the text it prints as, and a ROW written in the
        // query is a row of values: set beside one another as they stand, a column of a composite
        // type was a string against a record and named no operator. An operand declared to be of
        // a composite type is read apart into its fields before the two are compared.
        if (isComparison(bin.op()) || resolvesThroughEquality(bin.op())) {
            String lComposite = declaredCompositeType(bin.left(), ctx);
            if (lComposite != null && left instanceof String
                    && right instanceof AstExecutor.PgRow) {
                left = executor.parseCompositeToRow((String) left, lComposite);
            }
            String rComposite = declaredCompositeType(bin.right(), ctx);
            if (rComposite != null && right instanceof String
                    && left instanceof AstExecutor.PgRow) {
                right = executor.parseCompositeToRow((String) right, rComposite);
            }
            refuseRowAgainstScalar(bin, left, right, lComposite, rComposite, ctx);
        }

        executor.validateOperatorTypes(bin.op(), left, right);

        // Try built-in operator handling; if it fails due to unsupported types,
        // fall back to user-defined operator lookup
        try {
            return evalBuiltinBinary(bin.op(), left, right,
                    isJsonOperand(bin.left(), ctx) || isJsonOperand(bin.right(), ctx));
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
        return evalBuiltinBinary(op, left, right, false);
    }

    /**
     * The same, told whether the query declared either operand to be a json document. A jsonb
     * array is written with the brackets a range is written with, so without being told the
     * containment operators asked a range question of a document and answered it.
     */
    private Object evalBuiltinBinary(BinaryExpr.BinOp op, Object left, Object right,
            boolean jsonContainment) {
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
                // interval / number scales every field; without this the interval coerces to 0
                if (left instanceof PgInterval && right instanceof Number) {
                    double divisor = ((Number) right).doubleValue();
                    if (divisor == 0) throw new MemgresException("division by zero", "22012");
                    return ((PgInterval) left).multiply(1.0 / divisor);
                }
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
                rejectFloatDivisionByZero(left, right);
                return executor.numericOp(left, right, (a, b) -> a / b, BinaryOpEvaluator::divideExact,
                    (a, b) -> a.divide(b, NumericMath.divisionScale(a, b),
                            java.math.RoundingMode.HALF_UP));
            }
            case MODULO:
                return executor.numericOp(left, right, (a, b) -> a % b, (a, b) -> a % b,
                    java.math.BigDecimal::remainder);
            case POWER: {
                if (left == null || right == null) return null;
                // PG has numeric^numeric and float8^float8 only: a numeric operand keeps the
                // result numeric, computed in numeric so its last digit is numeric's rather
                // than a double's; everything else answers in float8.
                boolean numericPower = left instanceof java.math.BigDecimal
                        || right instanceof java.math.BigDecimal;
                if (numericPower && !NumericLimits.isSpecial(left) && !NumericLimits.isSpecial(right)) {
                    return NumericMath.power(TypeCoercion.toBigDecimal(left),
                            TypeCoercion.toBigDecimal(right));
                }
                double powBase = executor.toDouble(left);
                double powExp = executor.toDouble(right);
                NumericLimits.checkPowerDomain(powBase, powExp);
                double result = Math.pow(powBase, powExp);
                if (numericPower && Double.isInfinite(result)) {
                    throw NumericLimits.valueOverflowsNumeric();
                }
                if (numericPower) return numericPowerScale(result);
                // Two finite operands whose power is not finite have overflowed the type, which
                // PostgreSQL reports rather than answering with an infinity nobody wrote.
                if (Double.isInfinite(result) && !Double.isInfinite(powBase)
                        && !Double.isInfinite(powExp)) {
                    throw NumericLimits.floatOverflow();
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
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE)
                        ? (Object) Long.valueOf(r) : (Object) Integer.valueOf((int) r); }
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
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE)
                        ? (Object) Long.valueOf(r) : (Object) Integer.valueOf((int) r); }
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
                // What is left is the integer form, and text is not a number: PostgreSQL has no
                // # between two strings, and reading them as numbers refused the second one for
                // its spelling rather than saying there is no such operator.
                if (left instanceof String && right instanceof String) {
                    throw new MemgresException("operator does not exist: text # text"
                            + "\n  Hint: No operator matches the given name and argument types."
                            + " You might need to add explicit type casts.", "42883");
                }
                { long r = executor.toLong(left) ^ executor.toLong(right);
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE)
                        ? (Object) Long.valueOf(r) : (Object) Integer.valueOf((int) r); }
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
                { Boolean rangeCmp = rangeShift(left, right, true);
                if (rangeCmp != null) return rangeCmp; }
                rejectRangeShiftMismatch(left, right, true);
                return integerShift(left, right, true);
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
                { Boolean rangeCmp = rangeShift(left, right, false);
                if (rangeCmp != null) return rangeCmp; }
                rejectRangeShiftMismatch(left, right, false);
                return integerShift(left, right, false);
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
                    // Allow comparing PG array literal string with an array/list. An array's lower
                    // bounds are part of its shape, so one written with a bound other than 1 is not
                    // equal to the same elements written without one.
                    String literal = rightIsRow && left instanceof String ? (String) left
                            : (leftIsRow && right instanceof String ? (String) right : null);
                    if (literal != null && isArrayText(literal)) {
                        if (ArrayLiteral.statedBounds(literal) != null) return false;
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
                // Two arrays are ordered by array_cmp, which puts a null element after every
                // value; a record is ordered field by field and is unknown as soon as a field is.
                if (left instanceof List<?> && right instanceof List<?>) {
                    // Equality asks a stricter question than ordering: two arrays holding the same
                    // elements at different subscripts are ordered together but are not equal.
                    if (op == BinaryExpr.BinOp.EQUAL) return TypeCoercion.areEqual(left, right);
                    if (op == BinaryExpr.BinOp.NOT_EQUAL) return !TypeCoercion.areEqual(left, right);
                    return compareOp(op, TypeCoercion.compare(left, right));
                }
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : null;
                if (lList != null && rList != null) {
                    if (leftIsArray && rightIsArray) return arraysEqual(lList, rList);
                    if (lList.size() != rList.size()) {
                        throw new MemgresException("cannot compare row values of different sizes", "42601");
                    }
                    return rowEquality(lList, rList, true);
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
                requireEnumLabels(left, right);
                return TypeCoercion.areEqual(left, right);
            }
            case NOT_EQUAL: {
                if (left == null || right == null) return null; // NULL <> x is NULL
                requireEnumLabels(left, right);
                // Handle ROW comparison
                boolean leftIsArrayNe = left instanceof List && !(left instanceof AstExecutor.PgRow);
                boolean rightIsArrayNe = right instanceof List && !(right instanceof AstExecutor.PgRow);
                List<?> lList2 = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : left instanceof List ? (List<?>) left : null;
                List<?> rList2 = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : right instanceof List ? (List<?>) right : null;
                if (lList2 != null && rList2 != null) {
                    // An array is one value, and two arrays that hold NULL in the same place are
                    // that same value -- so <> answers false where a row's <> answers unknown.
                    if (leftIsArrayNe && rightIsArrayNe) return !arraysEqual(lList2, rList2);
                    if (lList2.size() != rList2.size()) throw new MemgresException("cannot compare row values of different sizes", "42601");
                    return rowEquality(lList2, rList2, false);
                }
                return !TypeCoercion.areEqual(left, right);
            }
            case LESS_THAN: {
                if (left == null || right == null) return null;
                // Two arrays are ordered by array_cmp, which puts a null element after every
                // value; a record is ordered field by field and is unknown as soon as a field is.
                if (left instanceof List<?> && right instanceof List<?>) {
                    // Equality asks a stricter question than ordering: two arrays holding the same
                    // elements at different subscripts are ordered together but are not equal.
                    if (op == BinaryExpr.BinOp.EQUAL) return TypeCoercion.areEqual(left, right);
                    if (op == BinaryExpr.BinOp.NOT_EQUAL) return !TypeCoercion.areEqual(left, right);
                    return compareOp(op, TypeCoercion.compare(left, right));
                }
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : null;
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
                // Two arrays are ordered by array_cmp, which puts a null element after every
                // value; a record is ordered field by field and is unknown as soon as a field is.
                if (left instanceof List<?> && right instanceof List<?>) {
                    // Equality asks a stricter question than ordering: two arrays holding the same
                    // elements at different subscripts are ordered together but are not equal.
                    if (op == BinaryExpr.BinOp.EQUAL) return TypeCoercion.areEqual(left, right);
                    if (op == BinaryExpr.BinOp.NOT_EQUAL) return !TypeCoercion.areEqual(left, right);
                    return compareOp(op, TypeCoercion.compare(left, right));
                }
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : null;
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
                // Two arrays are ordered by array_cmp, which puts a null element after every
                // value; a record is ordered field by field and is unknown as soon as a field is.
                if (left instanceof List<?> && right instanceof List<?>) {
                    // Equality asks a stricter question than ordering: two arrays holding the same
                    // elements at different subscripts are ordered together but are not equal.
                    if (op == BinaryExpr.BinOp.EQUAL) return TypeCoercion.areEqual(left, right);
                    if (op == BinaryExpr.BinOp.NOT_EQUAL) return !TypeCoercion.areEqual(left, right);
                    return compareOp(op, TypeCoercion.compare(left, right));
                }
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : null;
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
                // Two arrays are ordered by array_cmp, which puts a null element after every
                // value; a record is ordered field by field and is unknown as soon as a field is.
                if (left instanceof List<?> && right instanceof List<?>) {
                    // Equality asks a stricter question than ordering: two arrays holding the same
                    // elements at different subscripts are ordered together but are not equal.
                    if (op == BinaryExpr.BinOp.EQUAL) return TypeCoercion.areEqual(left, right);
                    if (op == BinaryExpr.BinOp.NOT_EQUAL) return !TypeCoercion.areEqual(left, right);
                    return compareOp(op, TypeCoercion.compare(left, right));
                }
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : null;
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
                    return ArrayOperationHandler.concatArrays((List<?>) left, (List<?>) right);
                }
                if (left instanceof List) {
                    // An element added to the end lands after the last subscript the array had,
                    // so the array still begins where it began.
                    List<?> ll = (List<?>) left;
                    List<Object> merged = new ArrayList<>(ll);
                    merged.add(right);
                    return ArrayOperationHandler.keepingLowerBounds(ll, merged);
                }
                if (right instanceof List) {
                    List<?> rl = (List<?>) right;
                    List<Object> merged = new ArrayList<>();
                    merged.add(left);
                    merged.addAll(rl);
                    return merged;
                }
                String ls = TypeCoercion.toString(left);
                String rs = TypeCoercion.toString(right);
                // PG does NOT support || for multirange types
                if (RangeOperations.isMultirangeOrEmpty(ls) || RangeOperations.isMultirangeOrEmpty(rs)) {
                    throw new MemgresException("operator does not exist: multirange || multirange", "42883");
                }
                // Text that is spelled the way a document is spelled is still text, and two of
                // them are run together rather than merged. Which || was written is settled from
                // the operands' types further up; by here they are two strings, and the only
                // concatenation left that takes two strings is the one over text.
                return ls + rs;
            }
            case LIKE: {
                if (left == null || right == null) return null;
                if (left instanceof Number || left instanceof Boolean) {
                    String tn = left instanceof Integer ? "integer" : left instanceof Long ? "bigint" :
                            left instanceof Boolean ? "boolean" : left.getClass().getSimpleName().toLowerCase(java.util.Locale.ROOT);
                    throw new MemgresException("operator does not exist: " + tn + " ~~ unknown", "42883");
                }
                return AstExecutor.likeMatch(likeOperand(left), likeOperand(right), false);
            }
            case ILIKE: {
                if (left == null || right == null) return null;
                return AstExecutor.likeMatch(likeOperand(left), likeOperand(right), true);
            }
            case SIMILAR_TO: {
                if (left == null || right == null) return null;
                return similarToMatches(likeOperand(left), likeOperand(right), "\\");
            }
            case JSON_ARROW:
            case JSON_SUBSCRIPT: {
                // json -> key or json -> index (returns JSON), also array/container subscript a[i]
                boolean isArrow = op == BinaryExpr.BinOp.JSON_ARROW;
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
                    throw new MemgresException("operator does not exist: jsonb -> numeric", "42883");
                }
                // The arrow is declared over int4 and text, so a wider integer beside it names no
                // operator at all rather than being narrowed to one.
                if (right instanceof Long) {
                    throw new MemgresException("operator does not exist: jsonb -> bigint"
                            + "\n  Hint: No operator matches the given name and argument types. You might need to add explicit type casts.", "42883");
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
                            // A negative subscript counts back from the end, so -1 is the last
                            // element; one that reaches past the front is simply not there.
                            if (idx < 0) idx += elements.size();
                            if (idx >= 0 && idx < elements.size()) return elements.get(idx);
                        }
                        return null;
                    }
                    // PG array string format: {elem1,elem2,...}; a quoted element may hold a
                    // comma of its own, so the split has to respect the quoting
                    if (left instanceof String && isPgArrayText((String) left)) {
                        List<Object> elements = FunctionEvaluator.parseSimplePgArray((String) left);
                        int idx = n.intValue() - 1; // PG arrays are 1-based
                        return (idx >= 0 && idx < elements.size()) ? elements.get(idx) : null;
                    }
                    // jsonb string scalar ("...") accessed with an integer: the -> operator
                    // treats the scalar as a one-element array (index 0 echoes the scalar,
                    // any other index is NULL); the [] subscript operator is always NULL.
                    if (left instanceof String && ((String) left).trim().startsWith("\"")) {
                        return (isArrow && n.intValue() == 0) ? ((String) left).trim() : null;
                    }
                    // name-style subscript (pg_dump: typname[0] = '_'): PG's name type
                    // supports zero-based single-character access; out of range is NULL.
                    // Only plain strings land here — JSON/array containers were handled above.
                    if (left instanceof String) {
                        String plain = (String) left;
                        String trimmed = plain.trim();
                        if (!trimmed.startsWith("{") && !trimmed.startsWith("[") && !trimmed.startsWith("\"")) {
                            // jsonb number/bool/null scalar: same one-element-array rule for ->.
                            if (trimmed.equals("null") || trimmed.equals("true") || trimmed.equals("false")
                                    || trimmed.matches("-?\\d+(\\.\\d+)?([eE][+-]?\\d+)?")) {
                                return (isArrow && n.intValue() == 0) ? trimmed : null;
                            }
                            int idx = n.intValue();
                            return (idx >= 0 && idx < plain.length()) ? String.valueOf(plain.charAt(idx)) : null;
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
                        return executor.functionEvaluator.evaluateJsonPathMatchSilent(ls, path);
                    }
                }
                // NULL @@ tsquery or tsvector @@ NULL → NULL (not false)
                return TextSearchOperations.matches(left, right);
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
                // One query holds another when it names every lexeme the other does.
                if (left instanceof TsQuery && right instanceof TsQuery) {
                    return TextSearchOperations.queryContains((TsQuery) left, (TsQuery) right);
                }
                // hstore @> hstore containment
                if (left instanceof HstoreValue || right instanceof HstoreValue) {
                    HstoreValue lh = left instanceof HstoreValue ? (HstoreValue) left : HstoreValue.parse(left.toString());
                    HstoreValue rh = right instanceof HstoreValue ? (HstoreValue) right : HstoreValue.parse(right.toString());
                    return lh.containsAll(rh);
                }
                // Geometric containment check BEFORE range/array to avoid misrouting
                {
                    String ls0 = left.toString().trim();
                    String rs0 = right.toString().trim();
                    if (GeometricOperations.isGeometricString(ls0) && GeometricOperations.isGeometricString(rs0)) {
                        // GeometricOperations.contains encodes PG's @> operator set and rejects
                        // the pairs PG has no operator for
                        return GeometricOperations.contains(ls0, rs0);
                    }
                }
                // Convert Java Lists to PG array format FIRST so arrays like ARRAY[1,5]
                // (which stringify as "[1, 5]") are never mistaken for range literals
                boolean lIsList = left instanceof List;
                boolean rIsList = right instanceof List;
                String ls = lIsList ? TypeCoercion.formatPgArray((List<?>) left) : left.toString().trim();
                String rs = rIsList ? TypeCoercion.formatPgArray((List<?>) right) : right.toString().trim();
                // Range/multirange semantics only apply when neither operand is a real array,
                // and never to a json document: a jsonb array is written with the brackets a
                // range is written with, and asking a range question of it answered for a
                // containment PostgreSQL decides quite differently.
                if (!lIsList && !rIsList && !jsonContainment) {
                    // Multirange containment: multirange @> value/range/multirange
                    if (RangeOperations.isMultirangeOrEmpty(ls)) {
                        if (RangeOperations.isMultirangeOrEmpty(rs)) return RangeOperations.multirangeContainsMultirange(ls, rs);
                        if (RangeOperations.isRangeString(rs)) return RangeOperations.multirangeContainsRange(ls, RangeOperations.parse(rs));
                        return RangeOperations.multirangeContainsValue(ls, right);
                    }
                    // Range containment: range @> value or range @> range or range @> multirange
                    if (RangeOperations.isRangeString(ls)) {
                        RangeOperations.PgRange range = RangeOperations.parse(ls);
                        // range @> multirange: true if range contains every sub-range
                        if (!(right instanceof Number) && RangeOperations.isMultirangeOrEmpty(rs)) {
                            return RangeOperations.multirangeContainsMultirange("{" + ls + "}", rs);
                        }
                        if (!(right instanceof Number) && RangeOperations.isRangeString(rs)) {
                            return range.containsRange(RangeOperations.parse(rs));
                        }
                        // range @> value: the probe is read as a value of the element type
                        Boolean held = range.containsValue(right);
                        if (held == null) {
                            throw new MemgresException("malformed range literal: \"" + rs + "\"", "22P02");
                        }
                        return held;
                    }
                }
                // PG array containment: {1,2,3} @> {2,3}
                boolean lIsPgArray = lIsList || (ls.startsWith("{") && !ls.startsWith("{\""));
                boolean rIsPgArray = rIsList || (rs.startsWith("{") && !rs.startsWith("{\""));
                if (lIsPgArray && rIsPgArray) {
                    List<?> la = lIsList ? (List<?>) left : PgArray.from(ls);
                    List<?> ra = rIsList ? (List<?>) right : PgArray.from(rs);
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
                // One query holds another when it names every lexeme the other does.
                if (left instanceof TsQuery && right instanceof TsQuery) {
                    return TextSearchOperations.queryContains((TsQuery) right, (TsQuery) left);
                }
                // hstore <@ hstore: contained-by
                if (left instanceof HstoreValue || right instanceof HstoreValue) {
                    HstoreValue lh = left instanceof HstoreValue ? (HstoreValue) left : HstoreValue.parse(left.toString());
                    HstoreValue rh = right instanceof HstoreValue ? (HstoreValue) right : HstoreValue.parse(right.toString());
                    return lh.containedBy(rh);
                }
                // Geometric containment check BEFORE range/array to avoid misrouting
                {
                    String ls0 = left.toString().trim();
                    String rs0 = right.toString().trim();
                    if (GeometricOperations.isGeometricString(ls0) && GeometricOperations.isGeometricString(rs0)) {
                        return GeometricOperations.containedBy(ls0, rs0); // a <@ b (not simply b @> a)
                    }
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
                        return RangeOperations.multirangeContainsValue(rs, left);
                    }
                    if (RangeOperations.isRangeString(rs)) {
                        RangeOperations.PgRange range = RangeOperations.parse(rs);
                        // multirange <@ range: true if range contains every sub-range
                        if (!(left instanceof Number) && RangeOperations.isMultirangeOrEmpty(ls)) {
                            return RangeOperations.multirangeContainsMultirange("{" + rs + "}", ls);
                        }
                        if (!(left instanceof Number) && RangeOperations.isRangeString(ls)) {
                            return range.containsRange(RangeOperations.parse(ls));
                        }
                        Boolean held = range.containsValue(left);
                        if (held != null) return held;
                    }
                }
                // PG array containment: {2,3} <@ {1,2,3}
                boolean lIsPgArray = lIsList || (ls.startsWith("{") && !ls.startsWith("{\""));
                boolean rIsPgArray = rIsList || (rs.startsWith("{") && !rs.startsWith("{\""));
                if (lIsPgArray && rIsPgArray) {
                    List<?> la = lIsList ? (List<?>) left : FunctionEvaluator.parseSimplePgArray(ls);
                    List<?> ra = rIsList ? (List<?>) right : FunctionEvaluator.parseSimplePgArray(rs);
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
                // ?| is also the geometric "vertically aligned" operator: point ?| point -> boolean
                if (!(left instanceof List) && !(right instanceof List)
                        && GeometricOperations.isPointString(left.toString())
                        && GeometricOperations.isPointString(right.toString())) {
                    return GeometricOperations.pointsVerticallyAligned(left.toString(), right.toString());
                }
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
                return executor.functionEvaluator.evaluateJsonPathExistsSilent(left.toString().trim(), right.toString().trim());
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
                    // An array that is already an array keeps its values: writing it out and
                    // reading it back made every element a string, so a numeric 1.00 stopped
                    // being the 1.000 it equals.
                    List<?> la = oLIsList ? (List<?>) left
                            : FunctionEvaluator.parseSimplePgArray(oLs);
                    List<?> ra = oRIsList ? (List<?>) right
                            : FunctionEvaluator.parseSimplePgArray(oRs);
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
            case GEO_BELOW_EQ: {
                if (left == null || right == null) return null;
                return GeometricOperations.isBelowByType(
                        GeometricOperations.autoDetectPublic(left.toString()),
                        GeometricOperations.autoDetectPublic(right.toString()));
            }
            case GEO_ABOVE_EQ: {
                if (left == null || right == null) return null;
                return GeometricOperations.isAboveByType(
                        GeometricOperations.autoDetectPublic(left.toString()),
                        GeometricOperations.autoDetectPublic(right.toString()));
            }
            case GEO_ABOVE: {
                if (left == null || right == null) return null;
                return GeometricOperations.isStrictlyAbove(left.toString(), right.toString());
            }
            case GEO_NOT_EXTEND_RIGHT: {
                if (left == null || right == null) return null;
                // The ranges spell this operator the same way the geometric types do, and mean
                // something else by it: neither operand reaching past the other's end.
                Boolean overlapLeft = rangeNotExtend(left, right, true);
                if (overlapLeft != null) return overlapLeft;
                return GeometricOperations.doesNotExtendRight(left.toString(), right.toString());
            }
            case GEO_NOT_EXTEND_LEFT: {
                if (left == null || right == null) return null;
                Boolean overlapRight = rangeNotExtend(left, right, false);
                if (overlapRight != null) return overlapRight;
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
                if (lObj instanceof GeometricOperations.PgLine && rObj instanceof GeometricOperations.PgLine) {
                    return GeometricOperations.isParallel((GeometricOperations.PgLine) lObj, (GeometricOperations.PgLine) rObj);
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
                // Two lines meet at a right angle when their normals do.
                if (lObj instanceof GeometricOperations.PgLine && rObj instanceof GeometricOperations.PgLine) {
                    GeometricOperations.PgLine ll = (GeometricOperations.PgLine) lObj;
                    GeometricOperations.PgLine rl = (GeometricOperations.PgLine) rObj;
                    return ll.a() * rl.a() + ll.b() * rl.b() == 0.0;
                }
                throw new MemgresException("operator ?-| not supported for these types", "42883");
            }
            case GEO_HORIZONTAL: {
                // point ?- point : horizontally aligned (same y) -> boolean
                if (left == null || right == null) return null;
                return GeometricOperations.pointsHorizontallyAligned(left.toString(), right.toString());
            }
            case REGEX_MATCH: {
                if (left == null || right == null) return null;
                return PgRegex.compile(right.toString()).matcher(left.toString()).find();
            }
            case REGEX_IMATCH: {
                if (left == null || right == null) return null;
                return PgRegex.compile(right.toString(), PgRegex.caseInsensitive()).matcher(left.toString()).find();
            }
            case NOT_REGEX_MATCH: {
                if (left == null || right == null) return null;
                return !PgRegex.compile(right.toString()).matcher(left.toString()).find();
            }
            case NOT_REGEX_IMATCH: {
                if (left == null || right == null) return null;
                return !PgRegex.compile(right.toString(), PgRegex.caseInsensitive()).matcher(left.toString()).find();
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
    static boolean arrayContainsAll(List<?> sup, List<?> sub) {
        List<Object> supFlat = leafElements(sup);
        for (Object o : leafElements(sub)) {
            if (o == null) return false; // NULL never equals anything
            boolean found = false;
            for (Object s : supFlat) {
                // Elements are matched by the element type's =, not by their spelling: comparing
                // the written text made a numeric 1.0 fail to match the same value written 1.00.
                if (s != null && TypeCoercion.areEqual(s, o)) { found = true; break; }
            }
            if (!found) return false;
        }
        return true;
    }

    /**
     * PG array overlap (&&): true if the arrays share any non-NULL element.
     * NULL elements never match anything.
     */
    static boolean arrayOverlaps(List<?> a, List<?> b) {
        List<Object> left = leafElements(a);
        for (Object o : leafElements(b)) {
            if (o == null) continue;
            for (Object s : left) {
                if (s != null && TypeCoercion.areEqual(s, o)) return true;
            }
        }
        return false;
    }

    /**
     * Refuses a comparison between a row and something that is not one.
     *
     * <p>A record is compared by an operator declared over two records, so the other side has to
     * become one: a constant with no type of its own is asked to read itself as a record, which
     * PostgreSQL cannot do for a row whose type has no name, and anything already of another type
     * names no operator at all. Comparing the row against the value as it stood answered for
     * pairs a real server refuses.
     */
    private void refuseRowAgainstScalar(BinaryExpr bin, Object left, Object right,
            String lComposite, String rComposite, RowContext ctx) {
        boolean lRow = left instanceof AstExecutor.PgRow;
        boolean rRow = right instanceof AstExecutor.PgRow;
        if (lRow == rRow) return;
        Expression other = lRow ? bin.right() : bin.left();
        if (isUntypedStringLiteral(other)) {
            throw new MemgresException(
                    "input of anonymous composite types is not implemented", "0A000");
        }
        String otherName = declaredTypeForResolution(other, ctx);
        if (otherName == null || familyOf(otherName) == null) return;
        String rowName = (lRow ? lComposite : rComposite) == null
                ? "record" : (lRow ? lComposite : rComposite);
        throw missingOperator(lRow ? rowName : pgName(otherName), bin.op(),
                lRow ? pgName(otherName) : rowName);
    }

    /**
     * The composite type an operand is declared to be of, or {@code null} where it is declared to
     * be of anything else. Only the declaration counts: what a value looks like says nothing, and
     * a text operand really written as text has no operator against a record whatever it holds.
     */
    private String declaredCompositeType(Expression expr, RowContext ctx) {
        if (expr instanceof CastExpr) {
            String written = ((CastExpr) expr).typeName();
            if (written == null) return null;
            String name = Quoting.nameAsRead(written.trim());
            return executor.database.isCompositeType(name) ? name : null;
        }
        if (expr instanceof ColumnRef && ctx != null) {
            ColumnRef ref = (ColumnRef) expr;
            Column def = ctx.resolveColumnDef(ref.table(), ref.column());
            return def == null ? null : def.getCompositeTypeName();
        }
        return null;
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
    /** The members of a value that is a row or an array, or null when it is neither. */
    private static List<?> rowOrArrayValues(Object v) {
        if (v instanceof AstExecutor.PgRow) return ((AstExecutor.PgRow) v).values();
        if (v instanceof List) return (List<?>) v;
        return null;
    }

    private static boolean isArrayValue(Object v) {
        return v instanceof List && !(v instanceof AstExecutor.PgRow);
    }

    /**
     * Two rows are equal when every pair of members is non-null and equal, unequal when some pair
     * is non-null and unequal, and unknown otherwise. A NULL member does not settle the answer by
     * itself and a pair that differs settles it even when another pair is NULL, so answering
     * unknown at the first NULL was wrong in both directions: {@code (NULL, 1) = (2, 3)} is false
     * to PostgreSQL, not unknown.
     *
     * @param equal true for {@code =}, false for {@code <>}
     */
    static Object rowEquality(List<?> l, List<?> r, boolean equal) {
        boolean unknown = false;
        for (int i = 0; i < l.size(); i++) {
            Object lv = l.get(i), rv = r.get(i);
            if (lv == null || rv == null) { unknown = true; continue; }
            if (!TypeCoercion.areEqual(lv, rv)) return equal ? Boolean.FALSE : Boolean.TRUE;
        }
        if (unknown) return null;
        return equal ? Boolean.TRUE : Boolean.FALSE;
    }

    /** An array is one value: NULL in the same place on both sides is the same value. */
    private static boolean arraysEqual(List<?> l, List<?> r) {
        if (l.size() != r.size()) return false;
        for (int i = 0; i < l.size(); i++) {
            Object lv = l.get(i), rv = r.get(i);
            if (lv == null || rv == null) {
                if (lv == null && rv == null) continue;
                return false;
            }
            if (!TypeCoercion.areEqual(lv, rv)) return false;
        }
        return true;
    }

    static String opSymbol(BinaryExpr.BinOp op) {
        return binOpToSymbol(op);
    }

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
    /**
     * {@code 42883} when only one side of {@code <<} or {@code >>} is a range. PostgreSQL declares
     * the operator over a pair of ranges and nothing else, so a range beside an integer matches no
     * candidate; reading the range's own text as a number instead threw a NumberFormatException
     * that reached the client as an internal XX000.
     */
    private static void rejectRangeShiftMismatch(Object left, Object right, boolean leftward) {
        boolean lBase = isRangeText(left);
        boolean rBase = isRangeText(right);
        // "empty" and "{}" only read as ranges beside one, so the other side decides them
        boolean lRange = lBase || (rBase && isAmbiguousEmptyRange(left));
        boolean rRange = rBase || (lBase && isAmbiguousEmptyRange(right));
        if (lRange == rRange) return;
        throw noSuchShiftOperator(AstExecutor.pgTypeNameOf(left), leftward,
                AstExecutor.pgTypeNameOf(right));
    }

    /** The two empty spellings a range shares with an empty array and a plain word. */
    private static boolean isAmbiguousEmptyRange(Object value) {
        if (!(value instanceof String)) return false;
        String s = ((String) value).trim();
        return s.equals("{}") || s.equalsIgnoreCase("empty");
    }

    /** True when a declared type name is one of the range or multirange types. */
    /** Whether an operand was declared to be a date, as against a timestamp of either kind. */
    private static boolean isDateTypeName(String declared) {
        return declared != null && declared.trim().equalsIgnoreCase("date");
    }

    private static boolean isRangeTypeName(String declared) {
        if (declared == null) return false;
        String t = declared.toLowerCase(java.util.Locale.ROOT).trim();
        return RANGE_TYPES.contains(t) || RANGE_TYPES.contains(t.replace("multirange", "range"));
    }

    private static MemgresException noSuchShiftOperator(String lName, boolean leftward,
                                                        String rName) {
        return new MemgresException("operator does not exist: "
                + lName + (leftward ? " << " : " >> ") + rName
                + "\n  Hint: No operator matches the given name and argument types. You might need to add explicit type casts.", "42883");
    }

    /** The type an operand was written as, falling back on the one its value carries. */
    private String operandTypeName(Expression expr, Object value, RowContext ctx) {
        String declared = declaredOperandType(expr, ctx);
        return declared != null ? declared : AstExecutor.pgTypeNameOf(value);
    }

    /** True for a value spelled as a range or a multirange. */
    private static boolean isRangeText(Object value) {
        if (!(value instanceof String)) return false;
        String s = ((String) value).trim();
        return RangeOperations.isMultirangeString(s) || RangeOperations.isRangeString(s);
    }

    /**
     * Range and multirange "strictly left/right of". Both spellings arrive as text, so they are
     * recognised here before the operands fall through to integer bit-shifting.
     */
    private static Boolean rangeShift(Object left, Object right, boolean leftward) {
        if (!(left instanceof String) || !(right instanceof String)) return null;
        String ls = ((String) left).trim(), rs = ((String) right).trim();
        boolean lMulti = RangeOperations.isMultirangeString(ls);
        boolean rMulti = RangeOperations.isMultirangeString(rs);
        boolean lRange = lMulti || RangeOperations.isRangeString(ls);
        boolean rRange = rMulti || RangeOperations.isRangeString(rs);
        // "{}" on its own is ambiguous with an empty array, so it only counts as an empty
        // multirange when the other operand is unambiguously a range.
        boolean lEmptyMulti = !lRange && rRange && ls.equals("{}");
        boolean rEmptyMulti = !rRange && lRange && rs.equals("{}");
        if (!(lRange || lEmptyMulti) || !(rRange || rEmptyMulti)) return null;
        java.util.List<RangeOperations.PgRange> a = rangeParts(ls, lMulti, lEmptyMulti);
        java.util.List<RangeOperations.PgRange> b = rangeParts(rs, rMulti, rEmptyMulti);
        return leftward
                ? RangeOperations.multirangeStrictlyLeftOf(a, b)
                : RangeOperations.multirangeStrictlyLeftOf(b, a);
    }

    private static java.util.List<RangeOperations.PgRange> rangeParts(
            String s, boolean multi, boolean emptyMulti) {
        if (emptyMulti) return java.util.Collections.emptyList();
        if (multi) return RangeOperations.parseMultirange(s);
        return java.util.Collections.singletonList(RangeOperations.parse(s));
    }

    /**
     * A range's {@code &<} and {@code &>}: whether one does not reach past the other's end. Empty
     * ranges are outside every comparison and answer false, as PostgreSQL's do.
     */
    private static Boolean rangeNotExtend(Object left, Object right, boolean leftward) {
        if (!(left instanceof String) || !(right instanceof String)) return null;
        String ls = ((String) left).trim(), rs = ((String) right).trim();
        boolean lMulti = RangeOperations.isMultirangeString(ls);
        boolean rMulti = RangeOperations.isMultirangeString(rs);
        boolean lRange = lMulti || RangeOperations.isRangeString(ls);
        boolean rRange = rMulti || RangeOperations.isRangeString(rs);
        if (!lRange || !rRange) return null;
        java.util.List<RangeOperations.PgRange> a = rangeParts(ls, lMulti, false);
        java.util.List<RangeOperations.PgRange> b = rangeParts(rs, rMulti, false);
        return leftward ? RangeOperations.multirangeDoesNotExtendRight(a, b)
                : RangeOperations.multirangeDoesNotExtendRight(b, a);
    }

    /** True when the query says this operand is a json document rather than a range. */
    private boolean isJsonOperand(Expression expr, RowContext ctx) {
        String declared = declaredTypeForResolution(expr, ctx);
        return "json".equals(declared) || "jsonb".equals(declared);
    }

    /** Handed back when this rule does not decide the operator, leaving the old path in charge. */
    private static final Object NOT_A_RANGE_OPERATION = new Object();

    /**
     * The meet, join or difference of two operands the query declares to be ranges. PostgreSQL
     * resolves {@code *}, {@code +} and {@code -} from those declarations -- a range against a
     * range and a multirange against a multirange -- and answers with the bounds that were written,
     * not with a whole number the text was rounded to, which is why the bounds are chosen rather
     * than recomputed.
     */
    private Object declaredRangeArithmetic(BinaryExpr bin, RowContext ctx, Object left, Object right) {
        BinaryExpr.BinOp op = bin.op();
        String lType = declaredRangeType(bin.left(), bin.right(), ctx);
        String rType = declaredRangeType(bin.right(), bin.left(), ctx);
        if (lType == null || rType == null) return NOT_A_RANGE_OPERATION;
        // PostgreSQL declares these three over two ranges or over two multiranges, and over
        // nothing in between: a range added to a multirange is a pair it has no operator for.
        if (MULTIRANGE_TYPES.contains(lType) != MULTIRANGE_TYPES.contains(rType)) {
            throw new MemgresException("operator does not exist: " + lType + " " + binOpToSymbol(op)
                    + " " + rType
                    + "\n  Hint: No operator matches the given name and argument types. You might need to add explicit type casts.", "42883");
        }
        if (!lType.equals(rType)) {
            throw new MemgresException("operator does not exist: " + lType + " " + binOpToSymbol(op)
                    + " " + rType
                    + "\n  Hint: No operator matches the given name and argument types. You might need to add explicit type casts.", "42883");
        }
        if (left == null || right == null) return null;
        if (!(left instanceof String) || !(right instanceof String)) return NOT_A_RANGE_OPERATION;
        if (MULTIRANGE_TYPES.contains(lType)) {
            if (op == BinaryExpr.BinOp.ADD) {
                return RangeOperations.multirangeUnion((String) left, (String) right, lType);
            }
            if (op == BinaryExpr.BinOp.SUBTRACT) {
                return RangeOperations.multirangeSubtract((String) left, (String) right, lType);
            }
            java.util.List<RangeOperations.PgRange> parts = new ArrayList<>();
            for (RangeOperations.PgRange a : RangeOperations.parseMultirange((String) left, lType)) {
                for (RangeOperations.PgRange b : RangeOperations.parseMultirange((String) right, lType)) {
                    RangeOperations.PgRange part = intersectRanges(a, b);
                    if (!part.isEmpty()) parts.add(part);
                }
            }
            StringBuilder sb = new StringBuilder("{");
            for (int i = 0; i < parts.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(parts.get(i).toString());
            }
            return sb.append('}').toString();
        }
        // Both operands are read as the type the query declared them, so a numrange whose bounds
        // happen to be whole numbers is not canonicalised as though it were an integer range.
        RangeOperations.PgRange a = RangeOperations.parse((String) left, lType);
        RangeOperations.PgRange b = RangeOperations.parse((String) right, lType);
        if (op == BinaryExpr.BinOp.ADD) return unionRanges(a, b).toString();
        if (op == BinaryExpr.BinOp.SUBTRACT) return differenceRanges(a, b).toString();
        return intersectRanges(a, b).toString();
    }

    /**
     * The join of two ranges. A range type holds one pair of bounds and nothing else, so two ranges
     * with a gap between them have no union PostgreSQL can express -- it says so rather than
     * quietly handing back a range that covers the gap as well.
     */
    private static RangeOperations.PgRange unionRanges(
            RangeOperations.PgRange a, RangeOperations.PgRange b) {
        return RangeOperations.union(a, b);
    }

    /**
     * What is left of the first range once the second is taken out of it. Cutting a hole in the
     * middle leaves two pieces, which is one more than a range can hold.
     */
    private static RangeOperations.PgRange differenceRanges(
            RangeOperations.PgRange a, RangeOperations.PgRange b) {
        return RangeOperations.subtract(a, b);
    }

    /**
     * The range or multirange type an operand is declared to have. An untyped literal has already
     * been read as the type opposite it by the time this runs, so it counts as that type too.
     */
    /** The range type an operand states outright, with no resolution from the other side. */
    private String declaredRangeTypeStrict(Expression expr, RowContext ctx) {
        String declared = declaredOperandType(expr, ctx);
        if (declared == null) return null;
        return RANGE_TYPES.contains(declared) || MULTIRANGE_TYPES.contains(declared) ? declared : null;
    }

    private String declaredRangeType(Expression expr, Expression other, RowContext ctx) {
        String declared = declaredOperandType(expr, ctx);
        if (declared == null && isUntypedStringLiteral(expr)) declared = declaredOperandType(other, ctx);
        if (declared == null) return null;
        return RANGE_TYPES.contains(declared) || MULTIRANGE_TYPES.contains(declared) ? declared : null;
    }

    /**
     * The overlap of two ranges: the later of the two lower bounds and the earlier of the two
     * upper bounds, each kept exactly as its own range wrote it.
     */
    private static RangeOperations.PgRange intersectRanges(
            RangeOperations.PgRange a, RangeOperations.PgRange b) {
        return RangeOperations.intersection(a, b);
    }

    private static java.math.BigDecimal toBigDecimal(Number n) {
        if (n instanceof java.math.BigDecimal) return (java.math.BigDecimal) n;
        if (n instanceof Double || n instanceof Float) return new java.math.BigDecimal(n.toString());
        return java.math.BigDecimal.valueOf(n.longValue());
    }

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
                // interval / number scales every field; without this the interval coerces to 0
                if (left instanceof PgInterval && right instanceof Number) {
                    double divisor = ((Number) right).doubleValue();
                    if (divisor == 0) throw new MemgresException("division by zero", "22012");
                    return ((PgInterval) left).multiply(1.0 / divisor);
                }
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
                rejectFloatDivisionByZero(left, right);
                return executor.numericOp(left, right, (a, b) -> a / b, BinaryOpEvaluator::divideExact,
                    (a, b) -> a.divide(b, NumericMath.divisionScale(a, b),
                            java.math.RoundingMode.HALF_UP));
            }
            case MODULO:
                return executor.numericOp(left, right, (a, b) -> a % b, (a, b) -> a % b,
                    java.math.BigDecimal::remainder);
            case POWER: {
                if (left == null || right == null) return null;
                // PG has numeric^numeric and float8^float8 only: a numeric operand keeps the
                // result numeric, computed in numeric so its last digit is numeric's rather
                // than a double's; everything else answers in float8.
                boolean numericPower = left instanceof java.math.BigDecimal
                        || right instanceof java.math.BigDecimal;
                if (numericPower && !NumericLimits.isSpecial(left) && !NumericLimits.isSpecial(right)) {
                    return NumericMath.power(TypeCoercion.toBigDecimal(left),
                            TypeCoercion.toBigDecimal(right));
                }
                double powBase = executor.toDouble(left);
                double powExp = executor.toDouble(right);
                NumericLimits.checkPowerDomain(powBase, powExp);
                double result = Math.pow(powBase, powExp);
                if (numericPower && Double.isInfinite(result)) {
                    throw NumericLimits.valueOverflowsNumeric();
                }
                if (numericPower) return numericPowerScale(result);
                // Two finite operands whose power is not finite have overflowed the type, which
                // PostgreSQL reports rather than answering with an infinity nobody wrote.
                if (Double.isInfinite(result) && !Double.isInfinite(powBase)
                        && !Double.isInfinite(powExp)) {
                    throw NumericLimits.floatOverflow();
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
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE)
                        ? (Object) Long.valueOf(r) : (Object) Integer.valueOf((int) r); }
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
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE)
                        ? (Object) Long.valueOf(r) : (Object) Integer.valueOf((int) r); }
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
                // What is left is the integer form, and text is not a number: PostgreSQL has no
                // # between two strings, and reading them as numbers refused the second one for
                // its spelling rather than saying there is no such operator.
                if (left instanceof String && right instanceof String) {
                    throw new MemgresException("operator does not exist: text # text"
                            + "\n  Hint: No operator matches the given name and argument types."
                            + " You might need to add explicit type casts.", "42883");
                }
                { long r = executor.toLong(left) ^ executor.toLong(right);
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE)
                        ? (Object) Long.valueOf(r) : (Object) Integer.valueOf((int) r); }
            }
            case SHIFT_LEFT: {
                if (left == null || right == null) return null;
                // inet << inet: left is strictly contained in right
                if (left instanceof InetValue && right instanceof InetValue) {
                    return ((InetValue) right).contains((InetValue) left);
                }
                { Boolean rangeCmp = rangeShift(left, right, true);
                if (rangeCmp != null) return rangeCmp; }
                rejectRangeShiftMismatch(left, right, true);
                { long r = executor.toLong(left) << executor.toLong(right);
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE)
                        ? (Object) Long.valueOf(r) : (Object) Integer.valueOf((int) r); }
            }
            case SHIFT_RIGHT: {
                if (left == null || right == null) return null;
                // inet >> inet: left strictly contains right
                if (left instanceof InetValue && right instanceof InetValue) {
                    return ((InetValue) left).contains((InetValue) right);
                }
                { Boolean rangeCmp = rangeShift(left, right, false);
                if (rangeCmp != null) return rangeCmp; }
                rejectRangeShiftMismatch(left, right, false);
                { long r = executor.toLong(left) >> executor.toLong(right);
                return (left instanceof Long || right instanceof Long || r < Integer.MIN_VALUE || r > Integer.MAX_VALUE)
                        ? (Object) Long.valueOf(r) : (Object) Integer.valueOf((int) r); }
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
                // A ROW value is not a java List, so a row that reached here -- the one a row
                // constructor compared against a subquery builds, among others -- missed the list
                // branch entirely and was compared as its own printed text: (1,NULL) = (1,2) came
                // back false because "(1,)" and "(1,2)" are different strings, and
                // (1,NULL) = (1,NULL) came back true because they are the same one.
                List<?> lEq = rowOrArrayValues(left);
                List<?> rEq = rowOrArrayValues(right);
                if (lEq != null && rEq != null) {
                    if (isArrayValue(left) && isArrayValue(right)) return arraysEqual(lEq, rEq);
                    if (lEq.size() != rEq.size()) return false;
                    return rowEquality(lEq, rEq, true);
                }
                return TypeCoercion.areEqual(left, right);
            }
            case NOT_EQUAL: {
                if (left == null || right == null) return null;
                List<?> lNe = rowOrArrayValues(left);
                List<?> rNe = rowOrArrayValues(right);
                if (lNe != null && rNe != null) {
                    if (isArrayValue(left) && isArrayValue(right)) return !arraysEqual(lNe, rNe);
                    if (lNe.size() != rNe.size()) return true;
                    return rowEquality(lNe, rNe, false);
                }
                return !TypeCoercion.areEqual(left, right);
            }
            case LESS_THAN: {
                if (left == null || right == null) return null;
                // Two arrays are ordered by array_cmp, which puts a null element after every
                // value; a record is ordered field by field and is unknown as soon as a field is.
                if (left instanceof List<?> && right instanceof List<?>) {
                    // Equality asks a stricter question than ordering: two arrays holding the same
                    // elements at different subscripts are ordered together but are not equal.
                    if (op == BinaryExpr.BinOp.EQUAL) return TypeCoercion.areEqual(left, right);
                    if (op == BinaryExpr.BinOp.NOT_EQUAL) return !TypeCoercion.areEqual(left, right);
                    return compareOp(op, TypeCoercion.compare(left, right));
                }
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : null;
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
                // Two arrays are ordered by array_cmp, which puts a null element after every
                // value; a record is ordered field by field and is unknown as soon as a field is.
                if (left instanceof List<?> && right instanceof List<?>) {
                    // Equality asks a stricter question than ordering: two arrays holding the same
                    // elements at different subscripts are ordered together but are not equal.
                    if (op == BinaryExpr.BinOp.EQUAL) return TypeCoercion.areEqual(left, right);
                    if (op == BinaryExpr.BinOp.NOT_EQUAL) return !TypeCoercion.areEqual(left, right);
                    return compareOp(op, TypeCoercion.compare(left, right));
                }
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : null;
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
                // Two arrays are ordered by array_cmp, which puts a null element after every
                // value; a record is ordered field by field and is unknown as soon as a field is.
                if (left instanceof List<?> && right instanceof List<?>) {
                    // Equality asks a stricter question than ordering: two arrays holding the same
                    // elements at different subscripts are ordered together but are not equal.
                    if (op == BinaryExpr.BinOp.EQUAL) return TypeCoercion.areEqual(left, right);
                    if (op == BinaryExpr.BinOp.NOT_EQUAL) return !TypeCoercion.areEqual(left, right);
                    return compareOp(op, TypeCoercion.compare(left, right));
                }
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : null;
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
                // Two arrays are ordered by array_cmp, which puts a null element after every
                // value; a record is ordered field by field and is unknown as soon as a field is.
                if (left instanceof List<?> && right instanceof List<?>) {
                    // Equality asks a stricter question than ordering: two arrays holding the same
                    // elements at different subscripts are ordered together but are not equal.
                    if (op == BinaryExpr.BinOp.EQUAL) return TypeCoercion.areEqual(left, right);
                    if (op == BinaryExpr.BinOp.NOT_EQUAL) return !TypeCoercion.areEqual(left, right);
                    return compareOp(op, TypeCoercion.compare(left, right));
                }
                List<?> lList = left instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) left).values() : null;
                List<?> rList = right instanceof AstExecutor.PgRow ? ((AstExecutor.PgRow) right).values() : null;
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
                    return ArrayOperationHandler.concatArrays((List<?>) left, (List<?>) right);
                }
                if (left instanceof List) {
                    // An element added to the end lands after the last subscript the array had,
                    // so the array still begins where it began.
                    List<?> ll = (List<?>) left;
                    List<Object> merged = new ArrayList<>(ll);
                    merged.add(right);
                    return ArrayOperationHandler.keepingLowerBounds(ll, merged);
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
                        return executor.functionEvaluator.evaluateJsonPathMatchSilent(ls, path);
                    }
                }
                // NULL @@ tsquery or tsvector @@ NULL → NULL (not false)
                return TextSearchOperations.matches(left, right);
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
                // One query holds another when it names every lexeme the other does.
                if (left instanceof TsQuery && right instanceof TsQuery) {
                    return TextSearchOperations.queryContains((TsQuery) left, (TsQuery) right);
                }
                // hstore @> hstore containment
                if (left instanceof HstoreValue || right instanceof HstoreValue) {
                    HstoreValue lh = left instanceof HstoreValue ? (HstoreValue) left : HstoreValue.parse(left.toString());
                    HstoreValue rh = right instanceof HstoreValue ? (HstoreValue) right : HstoreValue.parse(right.toString());
                    return lh.containsAll(rh);
                }
                // Geometric containment check BEFORE range/array to avoid misrouting
                {
                    String ls0 = left.toString().trim();
                    String rs0 = right.toString().trim();
                    if (GeometricOperations.isGeometricString(ls0) && GeometricOperations.isGeometricString(rs0)) {
                        return GeometricOperations.contains(ls0, rs0);
                    }
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
                        return RangeOperations.multirangeContainsValue(lStr, right);
                    }
                    // Range containment
                    if (RangeOperations.isRangeString(lStr)) {
                        RangeOperations.PgRange range = RangeOperations.parse(lStr);
                        // range @> multirange: true if range contains every sub-range
                        if (!(right instanceof Number) && RangeOperations.isMultirangeOrEmpty(rStr)) {
                            return RangeOperations.multirangeContainsMultirange("{" + lStr + "}", rStr);
                        }
                        if (!(right instanceof Number) && RangeOperations.isRangeString(rStr)) {
                            return range.containsRange(RangeOperations.parse(rStr));
                        }
                        Boolean held = range.containsValue(right);
                        if (held != null) return held;
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
                // One query holds another when it names every lexeme the other does.
                if (left instanceof TsQuery && right instanceof TsQuery) {
                    return TextSearchOperations.queryContains((TsQuery) right, (TsQuery) left);
                }
                // hstore <@ hstore: contained-by
                if (left instanceof HstoreValue || right instanceof HstoreValue) {
                    HstoreValue lh = left instanceof HstoreValue ? (HstoreValue) left : HstoreValue.parse(left.toString());
                    HstoreValue rh = right instanceof HstoreValue ? (HstoreValue) right : HstoreValue.parse(right.toString());
                    return lh.containedBy(rh);
                }
                // Geometric containment check BEFORE range/array to avoid misrouting
                {
                    String ls0 = left.toString().trim();
                    String rs0 = right.toString().trim();
                    if (GeometricOperations.isGeometricString(ls0) && GeometricOperations.isGeometricString(rs0)) {
                        return GeometricOperations.containedBy(ls0, rs0);
                    }
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
                        return RangeOperations.multirangeContainsValue(rStr, left);
                    }
                    if (RangeOperations.isRangeString(rStr)) {
                        RangeOperations.PgRange range = RangeOperations.parse(rStr);
                        // multirange <@ range: true if range contains every sub-range
                        if (!(left instanceof Number) && RangeOperations.isMultirangeOrEmpty(lStr)) {
                            return RangeOperations.multirangeContainsMultirange("{" + rStr + "}", lStr);
                        }
                        if (!(left instanceof Number) && RangeOperations.isRangeString(lStr)) {
                            return range.containsRange(RangeOperations.parse(lStr));
                        }
                        Boolean held = range.containsValue(left);
                        if (held != null) return held;
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
                // ?| is also the geometric "vertically aligned" operator: point ?| point -> boolean
                if (!(left instanceof List) && !(right instanceof List)
                        && GeometricOperations.isPointString(left.toString())
                        && GeometricOperations.isPointString(right.toString())) {
                    return GeometricOperations.pointsVerticallyAligned(left.toString(), right.toString());
                }
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
                return executor.functionEvaluator.evaluateJsonPathExistsSilent(left.toString().trim(), right.toString().trim());
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
                    // An array that is already an array keeps its values: writing it out and
                    // reading it back made every element a string, so a numeric 1.00 stopped
                    // being the 1.000 it equals.
                    List<?> la = oLIsList ? (List<?>) left
                            : FunctionEvaluator.parseSimplePgArray(oLs);
                    List<?> ra = oRIsList ? (List<?>) right
                            : FunctionEvaluator.parseSimplePgArray(oRs);
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
                if (lObj instanceof GeometricOperations.PgLine && rObj instanceof GeometricOperations.PgLine) {
                    return GeometricOperations.isParallel((GeometricOperations.PgLine) lObj, (GeometricOperations.PgLine) rObj);
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
                // Two lines meet at a right angle when their normals do.
                if (lObj instanceof GeometricOperations.PgLine && rObj instanceof GeometricOperations.PgLine) {
                    GeometricOperations.PgLine ll = (GeometricOperations.PgLine) lObj;
                    GeometricOperations.PgLine rl = (GeometricOperations.PgLine) rObj;
                    return ll.a() * rl.a() + ll.b() * rl.b() == 0.0;
                }
                throw new MemgresException("operator ?-| not supported for these types", "42883");
            }
            case GEO_HORIZONTAL: {
                // point ?- point : horizontally aligned (same y) -> boolean
                if (left == null || right == null) return null;
                return GeometricOperations.pointsHorizontallyAligned(left.toString(), right.toString());
            }
            case REGEX_MATCH: {
                if (left == null || right == null) return null;
                return PgRegex.compile(right.toString()).matcher(left.toString()).find();
            }
            case REGEX_IMATCH: {
                if (left == null || right == null) return null;
                return PgRegex.compile(right.toString(), PgRegex.caseInsensitive()).matcher(left.toString()).find();
            }
            case NOT_REGEX_MATCH: {
                if (left == null || right == null) return null;
                return !PgRegex.compile(right.toString()).matcher(left.toString()).find();
            }
            case NOT_REGEX_IMATCH: {
                if (left == null || right == null) return null;
                return !PgRegex.compile(right.toString(), PgRegex.caseInsensitive()).matcher(left.toString()).find();
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
                            left instanceof Boolean ? "boolean" : left.getClass().getSimpleName().toLowerCase(java.util.Locale.ROOT);
                    throw new MemgresException("operator does not exist: " + tn + " ~~ unknown", "42883");
                }
                return AstExecutor.likeMatch(likeOperand(left), likeOperand(right), false);
            }
            case ILIKE: {
                if (left == null || right == null) return null;
                return AstExecutor.likeMatch(likeOperand(left), likeOperand(right), true);
            }
            case SIMILAR_TO: {
                if (left == null || right == null) return null;
                return similarToMatches(likeOperand(left), likeOperand(right), "\\");
            }
            case JSON_ARROW:
            case JSON_SUBSCRIPT: {
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
     * True for text holding a PG array literal such as {@code {a,b}} or {@code {"a b"}}.
     * A JSON object opens the same way, so a quoted first token followed by a colon is
     * read as an object rather than an array.
     */
    /**
     * One side of a pattern match, written by its own type's output function. Reading the Java
     * object instead compared a bytea's identity hash against the pattern rather than its bytes.
     */
    private static String likeOperand(Object value) {
        if (value instanceof byte[]) {
            return new String((byte[]) value, java.nio.charset.StandardCharsets.ISO_8859_1);
        }
        return TypeCoercion.toString(value);
    }

    private static boolean isPgArrayText(String s) {
        if (!s.startsWith("{") || !s.endsWith("}")) return false;
        int i = 1;
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        if (i >= s.length() || s.charAt(i) != '"') return true;
        i++;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '\\') { i += 2; continue; }
            i++;
            if (c == '"') break;
        }
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        return i >= s.length() || s.charAt(i) != ':';
    }

    /**
     * Convert a SQL SIMILAR TO pattern to a Java regex.
     * Handles: % -> .*, _ -> ., |, (), +, *, ?, [...] (including POSIX classes),
     * and escape character processing.
     */
    /**
     * Does this value match this SIMILAR TO pattern?
     *
     * <p>The pattern used to be handed to {@code String.matches} after translation, so a pattern
     * java.util.regex would not compile reached the client as an internal error rather than as the
     * invalid_regular_expression PostgreSQL raises. The two shapes PostgreSQL names in its own
     * words are checked first; anything else the engine refuses is reported with its description.
     */
    static boolean similarToMatches(String value, String pattern, String escapeChar) {
        String esc = escapeChar != null && !escapeChar.isEmpty() ? escapeChar : "\\";
        rejectMalformedSimilarPattern(pattern, esc);
        // The pattern becomes an ordinary regular expression and is then compiled by the one
        // compiler this engine has, so that its classes and escapes are the same ones.
        return PgRegex.compile(PgRegex.fromSimilarTo(pattern, Character.valueOf(esc.charAt(0))))
                .matcher(value).matches();
    }

    /**
     * Check that a word compared against an enum is one of that enum's labels.
     *
     * <p>A word the enum does not hold names no value of that type, so the comparison has
     * nothing to be about. Comparing the two as text answered false, which a reader takes to
     * mean the two values differ rather than that one of them does not exist.
     */
    private void requireEnumLabels(Object left, Object right) {
        AstExecutor.PgEnum held = left instanceof AstExecutor.PgEnum
                ? (AstExecutor.PgEnum) left
                : right instanceof AstExecutor.PgEnum ? (AstExecutor.PgEnum) right : null;
        if (held == null) return;
        Object other = left instanceof AstExecutor.PgEnum ? right : left;
        if (!(other instanceof String)) return;
        CustomEnum ce = executor.database.getCustomEnum(held.typeName());
        if (ce == null || ce.isValidLabel((String) other)) return;
        String named = held.typeName();
        int dot = named.lastIndexOf('.');
        throw new MemgresException("invalid input value for enum "
                + (dot < 0 ? named : named.substring(dot + 1)) + ": \"" + other + "\"", "22P02");
    }

    /** The two malformations PostgreSQL names specifically, checked on the pattern as written. */
    private static void rejectMalformedSimilarPattern(String pattern, String esc) {
        int depth = 0;
        boolean operandAvailable = false;
        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);
            if (esc.length() == 1 && ch == esc.charAt(0)) {
                i++;
                operandAvailable = true;
                continue;
            }
            if (ch == '(') { depth++; operandAvailable = false; continue; }
            if (ch == ')') {
                depth--;
                if (depth < 0) {
                    throw new MemgresException(
                            "invalid regular expression: parentheses () not balanced", "2201B");
                }
                operandAvailable = true;
                continue;
            }
            if (ch == '|') { operandAvailable = false; continue; }
            if (ch == '*' || ch == '+' || ch == '?'
                    || (ch == '{' && boundedQuantifierEnd(pattern, i) >= 0)) {
                if (!operandAvailable) {
                    throw new MemgresException(
                            "invalid regular expression: quantifier operand invalid", "2201B");
                }
                if (ch == '{') i = boundedQuantifierEnd(pattern, i);
                continue;
            }
            operandAvailable = true;
        }
        if (depth != 0) {
            throw new MemgresException(
                    "invalid regular expression: parentheses () not balanced", "2201B");
        }
    }

    /** The index of the closing brace when a bounded quantifier starts here, else -1. */
    private static int boundedQuantifierEnd(String pattern, int open) {
        int i = open + 1;
        int digits = 0;
        while (i < pattern.length() && Character.isDigit(pattern.charAt(i))) { i++; digits++; }
        if (digits == 0) return -1;
        if (i < pattern.length() && pattern.charAt(i) == ',') {
            i++;
            while (i < pattern.length() && Character.isDigit(pattern.charAt(i))) i++;
        }
        return i < pattern.length() && pattern.charAt(i) == '}' ? i : -1;
    }

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
            } else if (chStr.equals(esc)) {
                // An escape with nothing left to escape is dropped, as PG's similar_escape does
                i++;
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
                // A bounded quantifier is {n}, {n,} or {n,m} and nothing else. Passing anything
                // between braces through left java.util.regex to reject it, where PostgreSQL
                // reads a brace that begins no quantifier as the character it is.
                int end = boundedQuantifierEnd(pattern, i);
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

    /**
     * PG's numeric power pads the result to 17 significant digits, so 10.0^3 prints as
     * 1000.0000000000000 rather than 1000.
     */
    /**
     * PG's float division reports a zero divisor rather than handing back an infinity. Only a
     * NaN dividend escapes it, which is why the check looks at the dividend at all.
     */
    private static void rejectFloatDivisionByZero(Object left, Object right) {
        boolean isFloat = left instanceof Double || left instanceof Float
                || right instanceof Double || right instanceof Float;
        if (!isFloat || !(right instanceof Number) || ((Number) right).doubleValue() != 0) return;
        if (left instanceof Number && Double.isNaN(((Number) left).doubleValue())) return;
        throw new MemgresException("division by zero", "22012");
    }

    private static java.math.BigDecimal numericPowerScale(double value) {
        java.math.BigDecimal bd = new java.math.BigDecimal(Double.toString(value));
        int intDigits = bd.abs().compareTo(java.math.BigDecimal.ONE) < 0
                ? 1 : bd.abs().toBigInteger().toString().length();
        int scale = Math.max(0, 16 - (intDigits - 1));
        return bd.setScale(scale, java.math.RoundingMode.HALF_UP);
    }




    /**
     * Integer division has exactly one overflow: the most negative value divided by -1 has no
     * positive counterpart. PG reports it rather than wrapping back to itself.
     */
    private static Long divideExact(Long a, Long b) {
        if (a != null && b != null && a == Long.MIN_VALUE && b == -1L) {
            throw new MemgresException("bigint out of range", "22003");
        }
        return a / b;
    }

}
