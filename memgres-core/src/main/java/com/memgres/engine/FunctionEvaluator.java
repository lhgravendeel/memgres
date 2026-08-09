package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import com.memgres.engine.plpgsql.PlpgsqlExecutor;
import com.memgres.engine.util.Strs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.*;
import java.util.*;

/**
 * Evaluates SQL function calls. Extracted from AstExecutor to reduce class size.
 */
class FunctionEvaluator {

    /** PG's NOTIFY payload must stay under 8000 bytes. */
    private static final int NOTIFY_PAYLOAD_LIMIT = 8000;


    private static final Logger LOG = LoggerFactory.getLogger(FunctionEvaluator.class);

    // CRC-32C (Castagnoli) lookup table — bit-reversed polynomial 0x82F63B78
    private static final int[] CRC32C_TABLE = new int[256];
    static {
        for (int i = 0; i < 256; i++) {
            int crc = i;
            for (int j = 0; j < 8; j++)
                crc = (crc >>> 1) ^ ((crc & 1) != 0 ? 0x82F63B78 : 0);
            CRC32C_TABLE[i] = crc;
        }
    }

    private static long crc32c(byte[] data) {
        int crc = 0xFFFFFFFF;
        for (byte b : data)
            crc = (crc >>> 8) ^ CRC32C_TABLE[(crc ^ b) & 0xFF];
        return (~crc) & 0xFFFFFFFFL;
    }

    static final Object NOT_HANDLED = new Object();


    private final AstExecutor executor;
    private final MathFunctions mathFunctions;
    private final StringFunctions stringFunctions;
    private final CatalogSystemFunctions catalogSystemFunctions;
    final JsonFunctions jsonFunctions;
    private final TextSearchFunctions textSearchFunctions;
    private final DateTimeFunctions dateTimeFunctions;
    private final XmlFunctions xmlFunctions;
    private final GeometricFunctions geometricFunctions;
    private final RangeFunctions rangeFunctions;
    private final NetworkFunctions networkFunctions;
    private final ByteaFunctions byteaFunctions;
    private final AdvisoryLockFunctions advisoryLockFunctions;

    FunctionEvaluator(AstExecutor executor) {
        this.executor = executor;
        this.mathFunctions = new MathFunctions(executor);
        this.stringFunctions = new StringFunctions(executor);
        this.catalogSystemFunctions = new CatalogSystemFunctions(executor);
        this.jsonFunctions = new JsonFunctions(executor);
        this.textSearchFunctions = new TextSearchFunctions(executor);
        this.dateTimeFunctions = new DateTimeFunctions(executor);
        this.xmlFunctions = new XmlFunctions(executor);
        this.geometricFunctions = new GeometricFunctions(executor);
        this.rangeFunctions = new RangeFunctions(executor);
        this.networkFunctions = new NetworkFunctions(executor);
        this.byteaFunctions = new ByteaFunctions(executor);
        this.advisoryLockFunctions = new AdvisoryLockFunctions(executor);
    }

    private static java.nio.charset.Charset pgEncodingToCharset(String enc) {
        String upper = enc.toUpperCase().replace("-", "").replace("_", "");
        switch (upper) {
            case "UTF8": case "UTF88": return java.nio.charset.StandardCharsets.UTF_8;
            case "LATIN1": case "ISO88591": return java.nio.charset.StandardCharsets.ISO_8859_1;
            case "LATIN2": case "ISO88592": return java.nio.charset.Charset.forName("ISO-8859-2");
            case "WIN1252": return java.nio.charset.Charset.forName("windows-1252");
            case "SQLASCII": case "ASCII": return java.nio.charset.StandardCharsets.US_ASCII;
            default: return java.nio.charset.Charset.forName(enc);
        }
    }

    private static HstoreValue toHstore(Object val) {
        if (val instanceof HstoreValue) return (HstoreValue) val;
        return HstoreValue.parse(val.toString());
    }

    /** Loose JSON: numeric values are unquoted, NULLs are JSON null. PG does NOT unquote booleans. */
    private static String hstoreToJsonLooseString(HstoreValue h) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (java.util.Map.Entry<String, String> e : h.getData().entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append("\"").append(e.getKey().replace("\\", "\\\\").replace("\"", "\\\"")).append("\": ");
            String v = e.getValue();
            if (v == null) {
                sb.append("null");
            } else {
                try {
                    new java.math.BigDecimal(v);
                    sb.append(v); // valid number — unquoted
                } catch (NumberFormatException ex) {
                    sb.append("\"").append(v.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
                }
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static String hstoreToJsonString(HstoreValue h) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (java.util.Map.Entry<String, String> e : h.getData().entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append("\"").append(e.getKey().replace("\\", "\\\\").replace("\"", "\\\"")).append("\": ");
            if (e.getValue() == null) {
                sb.append("null");
            } else {
                sb.append("\"").append(e.getValue().replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * The names whose written argument count settles nothing at all, so no arity rule may read it.
     *
     * <p>Two kinds. A <b>type constructor</b> — {@code varchar(n)}, {@code numeric(p,s)},
     * {@code point(x,y)} — is cast syntax the grammar spells like a call, and the arguments a
     * writer gives it are a typmod rather than the parameters PostgreSQL's pg_proc row declares.
     * A <b>syntax form</b> — {@code trim}, {@code position}, {@code extract}, {@code overlay},
     * {@code coalesce}, {@code greatest} — is desugared into a call whose shape is memgres's own
     * and need not match the row PostgreSQL keeps for it, where it keeps one at all.
     * {@code x OVERLAPS y} is the plainest case: PostgreSQL declares it over four endpoints and
     * memgres desugars it to two row constructors, so reading the count would refuse the operator.
     *
     * <p>The variadic names used to be here too, because a table recording only fixed argument
     * lists could not express "and any number more". {@link BuiltinFunctionSignatures} records the
     * variadic ones as such now, so they are judged like everything else — which is what turned
     * {@code format()} from an internal error into the 42883 PostgreSQL answers.
     */
    private static final Set<String> ANY_ARITY = Cols.setOf(
            "jsonb_delete", "tsquery_phrase", "coalesce", "greatest", "least",
            "grouping", "normalize", "position", "extract", "overlay", "trim",
            "overlaps",
            "current_user", "session_user", "merge_action",
            "varchar", "bit", "numeric", "char", "bpchar", "decimal", "timestamp", "timestamptz",
            "time", "timetz", "interval", "box", "point", "polygon", "lseg", "circle", "path",
            "line", "int4multirange", "int8multirange", "nummultirange", "tsmultirange",
            "tstzmultirange", "datemultirange");

    /**
     * Refuses a call whose qualifier names no schema.
     *
     * <p>A qualified call is looked for in one schema and nowhere else, so the qualifier is
     * resolved first: {@code nosuchschema.f(1)} is 3F000 "schema does not exist", not 42883. Only
     * a single unquoted qualifier is judged, and only when nothing at all answers to it — a schema
     * of the user's, one of the two the catalog supplies, or the session's temp schema.
     */
    private void rejectMissingSchemaQualifier(String name) {
        int dot = name.indexOf('.');
        if (dot <= 0 || name.indexOf('.', dot + 1) >= 0) return;
        String qualifier = name.substring(0, dot);
        if ("pg_catalog".equals(qualifier) || "information_schema".equals(qualifier)) return;
        if (executor.database.getSchema(qualifier) != null) return;
        if (executor.session != null
                && qualifier.equals(executor.session.getTempSchemaName())) {
            return;
        }
        throw new MemgresException("schema \"" + qualifier + "\" does not exist", "3F000");
    }

    /** Whether this name's written argument count settles nothing; see {@link #ANY_ARITY}. */
    static boolean acceptsAnyArity(String name) {
        return name != null && ANY_ARITY.contains(name.toLowerCase(java.util.Locale.ROOT));
    }

    /**
     * Refuses a call to a built-in with a number of arguments no signature of that name has.
     *
     * <p>PostgreSQL resolves a call by name and argument list together: there is no signature that
     * quietly ignores an argument and none that supplies one that was not written, so
     * {@code upper('a','b')} is not upper applied to the first of them and {@code lpad('a')} is
     * not lpad with a length of its own choosing. Both are a function that does not exist (42883).
     * memgres read as many arguments as each implementation wanted, which turned a mistyped call
     * into a plausible answer one way and into an internal error — an index off the end of the
     * argument list, reported to the client as XX000 — the other.
     *
     * <p>The rule reads in both directions now because {@link BuiltinFunctionSignatures} is
     * complete: every signature PostgreSQL declares for a name is recorded with the fewest
     * arguments it takes, so "too few" is as sound a reading as "too many". It used to record
     * several names only in the long form PostgreSQL keeps internally, which is why the rule was
     * one-sided and a short call reached the implementation.
     *
     * <p>A name the table does not list is not judged at all, which is also why no aggregate is:
     * they carry no row there. A name a user has declared a function for decides its own arity,
     * and so do the names in {@link #ANY_ARITY}.
     */
    private void rejectWrongArity(String name, FunctionCallExpr fn, RowContext ctx) {
        if (name == null || ANY_ARITY.contains(name)) return;
        if (fn.star()) return;
        if (executor.database.getFunction(name) != null) return;
        // unnest names its own too-many-arguments case, and names the argument types better.
        if ("unnest".equals(name)) return;
        for (Expression arg : fn.args()) {
            // A named or variadic argument binds to a parameter by name rather than by position,
            // so counting the written arguments is not what decides whether the call resolves.
            if (arg instanceof NamedArgExpr) return;
            // A query written where an argument goes is not an argument list at all — PostgreSQL
            // refuses the syntax before it counts anything.
            if (AstWalk.anyMatch(arg, n -> n instanceof Statement)) return;
        }
        // A type name written like a call of one argument is a cast, not a call: numrange(NULL) is
        // CAST(NULL AS numrange), which PostgreSQL runs and no pg_proc row of that name describes.
        if (fn.args().size() == 1 && DataType.fromPgName(name) != null) return;
        if (!BuiltinFunctionSignatures.recordsSignature(name)) return;
        if (BuiltinFunctionSignatures.acceptsArity(name, fn.args().size())) return;
        throw new MemgresException("function " + fn.name() + "(" + argTypeNames(fn, ctx)
                + ") does not exist\n  Hint: No function matches the given name and argument"
                + " types. You might need to add explicit type casts.", "42883");
    }

    /** The argument types of a call, named the way PostgreSQL names them in a 42883. */
    private String argTypeNames(FunctionCallExpr fn, RowContext ctx) {
        StringBuilder types = new StringBuilder();
        for (int i = 0; i < fn.args().size(); i++) {
            if (i > 0) types.append(", ");
            Expression arg = fn.args().get(i);
            // An unadorned string literal is PostgreSQL's "unknown": it has no type until the
            // function it is handed to gives it one, and a call that resolves to nothing never does.
            if (arg instanceof Literal
                    && ((Literal) arg).literalType() == Literal.LiteralType.STRING) {
                types.append("unknown");
                continue;
            }
            // A call is named by what it returns, which is not always what its value looks like:
            // int4range(1,5) is a range, and the string it is held as would be reported as text.
            String declared = declaredReturnTypeName(arg);
            if (declared != null) {
                types.append(declared);
                continue;
            }
            // The arguments are transformed before the function is resolved, so an argument that
            // is wrong in itself is what PostgreSQL reports -- a query written where one goes is a
            // syntax error long before anything counts them.
            Object value = executor.evalExpr(arg, ctx);
            types.append(value == null ? "unknown" : AstExecutor.pgTypeNameOf(value));
        }
        return types.toString();
    }

    /**
     * The type a nested call returns, when every signature PostgreSQL declares for its name returns
     * the same one, and null otherwise. A name with overloads that differ, a set-returning one and
     * one this database has its own function for are all left to the value.
     */
    private String declaredReturnTypeName(Expression arg) {
        if (!(arg instanceof FunctionCallExpr)) return null;
        FunctionCallExpr call = (FunctionCallExpr) arg;
        if (call.filter() != null || call.distinct || call.star()) return null;
        String bare = stripSchemaPrefix(call.name().toLowerCase(java.util.Locale.ROOT));
        if (executor.database != null && executor.database.getFunction(bare) != null) return null;
        DataType found = null;
        for (String[] signature : BuiltinFunctionSignatures.SIGNATURES) {
            if (!signature[0].equalsIgnoreCase(bare)) continue;
            if (signature[3] == null || signature[3].isEmpty()) return null;
            if (signature[3].charAt(0) == 't') return null;
            DataType returned = DataType.fromOid(Integer.parseInt(signature[1]));
            if (returned == null) return null;
            if (found != null && found != returned) return null;
            found = returned;
        }
        return found == null ? null : CatalogHelper.pgTypeName(found);
    }

    // ---- A type name written like a call ----

    /**
     * The type names PostgreSQL's own grammar reads as a type rather than as a function name.
     *
     * <p>{@code SELECT numeric(42)} is a syntax error there (42601), not a call and not a
     * coercion, because the parser has already decided the word starts a type before it reaches
     * the parenthesis. Every one of these is a col_name_keyword; a spelling that is not —
     * {@code int4}, {@code bpchar}, {@code timestamptz}, {@code varbit} — is an ordinary
     * identifier and does resolve as a coercion. Enabling these here would make memgres accept
     * SQL PostgreSQL rejects, so the coercion path refuses to look at them.
     */
    private static final Set<String> TYPE_NAME_KEYWORDS = Cols.setOf(
            "numeric", "decimal", "dec", "varchar", "character varying", "boolean", "integer",
            "int", "bigint", "smallint", "real", "float", "double precision", "double",
            "char", "character", "nchar", "bit", "bit varying", "time", "timestamp",
            "interval", "serial", "bigserial", "smallserial", "setof", "national character",
            "citext", "hstore");

    /**
     * Type names PostgreSQL keeps that memgres reads through a cast without carrying a
     * {@link DataType} for them, so {@link DataType#fromPgName} cannot be what recognises them.
     */
    private static final Set<String> EXTRA_COERCIBLE_TYPE_NAMES = Cols.setOf(
            "regnamespace", "regconfig", "regdictionary", "regoperator", "regoper", "regrole",
            "regprocedure", "regcollation", "pg_lsn", "tid", "jsonpath", "xid", "cid",
            "aclitem", "pg_snapshot", "txid_snapshot", "cstring", "unknown");

    /**
     * The pseudo-types whose name is not a coercion PostgreSQL will perform. A polymorphic
     * placeholder stands for a type rather than being one, and an anonymous composite has no
     * input function, so PostgreSQL answers 42883 or 0A000 rather than converting anything.
     */
    private static final Set<String> UNCOERCIBLE_TYPE_NAMES = Cols.setOf(
            "record", "anyarray", "anyelement", "anynonarray", "anyenum", "anyrange",
            "anymultirange", "anycompatible", "anycompatiblearray", "anycompatiblenonarray",
            "anycompatiblerange", "anycompatiblemultirange", "any", "internal", "trigger",
            "event_trigger", "language_handler", "fdw_handler", "index_am_handler",
            "tsm_handler", "table_am_handler", "gtsvector", "opaque", "pg_ddl_command",
            "enum", "refcursor");

    /**
     * Whether this name can stand for a type in a call written like a function.
     *
     * <p>Answered for the bare name, with any {@code pg_catalog.} qualifier already stripped: a
     * built-in type, one of the names memgres only casts to, a domain the user declared or an
     * enum the user declared. A schema qualifier that is not pg_catalog's leaves the name alone,
     * because {@code public.int4(1)} names nothing in either engine.
     */
    String coercibleTypeName(String name) {
        if (name == null) return null;
        String bare = name;
        if (bare.indexOf('.') >= 0) {
            // A schema qualifier is part of the call. pg_catalog's is stripped before this point,
            // so what is left names a schema of the user's, and only a type really in it answers.
            if (!bare.startsWith("public.") || bare.indexOf('.', 7) >= 0) return null;
            bare = bare.substring(7);
            if (executor.database == null) return null;
            if (executor.database.getDomain(bare) == null
                    && executor.database.getCustomEnum(bare) == null) {
                return null;
            }
            return bare;
        }
        if (TYPE_NAME_KEYWORDS.contains(bare) || UNCOERCIBLE_TYPE_NAMES.contains(bare)) return null;
        if (bare.endsWith("[]") || bare.startsWith("_")) return null;
        // A type this database was told about answers first: the name is then its own, whatever
        // PostgreSQL happens to keep under the same spelling.
        if (executor.database != null
                && (executor.database.getDomain(bare) != null
                    || executor.database.getCustomEnum(bare) != null)) {
            return bare;
        }
        if (EXTRA_COERCIBLE_TYPE_NAMES.contains(bare)) return bare;
        return DataType.fromPgName(bare) != null ? bare : null;
    }

    /**
     * The type whose conversions decide whether the call resolves. A domain is its base type for
     * that purpose — {@code adomain(1)} converts an integer exactly as the integer it is built on
     * does — while every other name decides for itself.
     */
    private String coercionTargetOf(String typeName) {
        if (executor.database == null) return typeName;
        DomainType domain = executor.database.getDomain(typeName);
        if (domain == null || domain.getBaseType() == null) return typeName;
        return domain.getBaseType().getPgName();
    }

    /** The types whose values PostgreSQL reads with the target type's input function. */
    private static boolean isStringCategory(DataType t) {
        return t == DataType.TEXT || t == DataType.VARCHAR || t == DataType.CHAR
                || t == DataType.NAME;
    }

    private static boolean isNumberType(DataType t) {
        return t == DataType.SMALLINT || t == DataType.INTEGER || t == DataType.BIGINT
                || t == DataType.REAL || t == DataType.DOUBLE_PRECISION || t == DataType.NUMERIC
                || t == DataType.SERIAL || t == DataType.BIGSERIAL || t == DataType.SMALLSERIAL;
    }

    private static boolean isOidType(DataType t) {
        return t == DataType.OID || t == DataType.REGCLASS || t == DataType.REGTYPE
                || t == DataType.REGPROC;
    }

    /**
     * Whether PostgreSQL has a conversion from the argument's type to the named one, measured
     * against PG 18 rather than derived. A source with no type of its own — an unadorned literal
     * or a NULL — is read by the target's input function and so always converts; a source in the
     * string category likewise. Everything else needs a cast PostgreSQL actually declares, which
     * is why {@code date(42)}, {@code uuid(42)} and {@code int4(point '(1,2)')} are a function
     * that does not exist rather than a conversion that fails.
     *
     * <p>Where the conversion PostgreSQL performs is not the one memgres's cast would perform —
     * {@code bytea(42)} writes the integer's bytes, {@code int4(jsonb)} reads a JSON number — the
     * pair is left out, so the call is refused rather than answered wrongly.
     */
    private static boolean coercionAdmitted(String target, DataType src) {
        if (src == null || isStringCategory(src)) return true;
        DataType targetType = DataType.fromPgName(target);
        if (targetType != null && targetType == src) return true;
        switch (target) {
            case "int2": case "int4": case "int8": case "float4": case "float8":
                if (isNumberType(src)) return true;
                if (isOidType(src)) return target.equals("int4") || target.equals("int8");
                return src == DataType.BOOLEAN && target.equals("int4");
            case "bool":
                return src == DataType.SMALLINT || src == DataType.INTEGER;
            case "oid":
                return src == DataType.SMALLINT || src == DataType.INTEGER
                        || src == DataType.BIGINT || isOidType(src);
            case "money":
                return isNumberType(src);
            case "date": case "timestamptz":
                return src == DataType.DATE || src == DataType.TIMESTAMP
                        || src == DataType.TIMESTAMPTZ;
            case "timetz":
                return src == DataType.TIME || src == DataType.TIMETZ
                        || src == DataType.TIMESTAMP || src == DataType.TIMESTAMPTZ;
            case "inet": case "cidr":
                return src == DataType.INET || src == DataType.CIDR;
            case "macaddr": case "macaddr8":
                return src == DataType.MACADDR || src == DataType.MACADDR8;
            case "json": case "jsonb":
                return src == DataType.JSON || src == DataType.JSONB;
            case "name":
                return true;
            case "bpchar":
                return src != DataType.INET && src != DataType.CIDR;
            case "regclass": case "regtype": case "regproc": case "regnamespace":
            case "regconfig": case "regdictionary": case "regoperator": case "regoper":
            case "regprocedure": case "regcollation": case "regrole":
                return src == DataType.INTEGER || isOidType(src);
            default:
                return false;
        }
    }

    /** The type an argument expression carries, or null when it is an unadorned literal. */
    private DataType staticArgType(Expression arg, RowContext ctx) {
        if (arg instanceof Literal) {
            Literal.LiteralType lt = ((Literal) arg).literalType();
            if (lt == Literal.LiteralType.STRING || lt == Literal.LiteralType.NULL) return null;
        }
        List<RowContext.TableBinding> bindings = ctx == null
                ? java.util.Collections.<RowContext.TableBinding>emptyList() : ctx.getBindings();
        try {
            return executor.exprEvaluator.inferTypeFromContext(arg, bindings);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** The way PostgreSQL names an argument type in a 42883 raised by a coercion that has none. */
    private String coercionArgTypeName(Expression arg, RowContext ctx) {
        DataType t = staticArgType(arg, ctx);
        if (t == null) return "unknown";
        if (t == DataType.ENUM) {
            List<RowContext.TableBinding> bindings = ctx == null
                    ? java.util.Collections.<RowContext.TableBinding>emptyList() : ctx.getBindings();
            String enumName = executor.exprEvaluator.resolveEnumTypeName(arg, bindings);
            if (enumName != null) return enumName;
        }
        return CatalogHelper.pgTypeName(t);
    }

    /**
     * The value a type name written like a call produces, or {@link #NOT_HANDLED} when the name
     * is not a type or the call is not one PostgreSQL reads as a coercion.
     *
     * <p>One written argument is a coercion of it. Two or three are the typmod-applying rows
     * PostgreSQL keeps in pg_proc, and those are reachable only through the {@code pg_catalog.}
     * spelling, because the bare name of every type that has one is a keyword.
     */
    private Object typeNameCoercion(String name, FunctionCallExpr fn, RowContext ctx) {
        if (fn.star() || fn.distinct()) return NOT_HANDLED;
        for (Expression a : fn.args()) {
            if (a instanceof NamedArgExpr) return NOT_HANDLED;
        }
        boolean qualified = fn.name() != null
                && fn.name().toLowerCase(java.util.Locale.ROOT).startsWith("pg_catalog.");
        if (qualified && fn.args().size() >= 2 && fn.args().size() <= 3) {
            return typmodCoercion(name, fn, ctx);
        }
        if (fn.args().size() != 1) return NOT_HANDLED;
        String typeName = coercibleTypeName(name);
        if (typeName == null) return NOT_HANDLED;
        Expression arg = fn.args().get(0);
        if (!coercionAdmitted(coercionTargetOf(typeName), staticArgType(arg, ctx))) {
            throw new MemgresException("function " + fn.name() + "("
                    + coercionArgTypeName(arg, ctx) + ") does not exist\n"
                    + "  Hint: No function matches the given name and argument types."
                    + " You might need to add explicit type casts.", "42883");
        }
        Object value = executor.evalExpr(arg, ctx);
        // void carries no value at all: whatever is handed to it, PostgreSQL prints nothing.
        if (typeName.equals("void")) return value == null ? null : "";
        return executor.castEvaluator.applyCast(value, typeName,
                arg instanceof Literal
                        && ((Literal) arg).literalType() == Literal.LiteralType.STRING);
    }

    /**
     * {@code pg_catalog.varchar(v, typmod, isExplicit)} and its relatives, which apply a stored
     * type modifier to a value that already has the type. The modifier is the packed one the
     * catalogs hold, so a length carries four bytes of header with it and anything below that is
     * "no modifier at all" — which is why {@code pg_catalog.varchar('abcdef', 3, true)} answers
     * abcdef and {@code pg_catalog.varchar('abcdef', 7, true)} answers abc.
     */
    private Object typmodCoercion(String name, FunctionCallExpr fn, RowContext ctx) {
        boolean lengthTyped = name.equals("varchar") || name.equals("bpchar");
        if (!lengthTyped && !name.equals("numeric")) return NOT_HANDLED;
        if (lengthTyped && fn.args().size() != 3) return NOT_HANDLED;
        if (!lengthTyped && fn.args().size() != 2) return NOT_HANDLED;
        Object value = executor.evalExpr(fn.args().get(0), ctx);
        Object typmodArg = executor.evalExpr(fn.args().get(1), ctx);
        if (value == null || typmodArg == null) return null;
        if (!(typmodArg instanceof Number)) return NOT_HANDLED;
        int typmod = ((Number) typmodArg).intValue();
        if (typmod < 4) return value;
        if (lengthTyped) {
            String s = value.toString();
            int n = typmod - 4;
            return s.length() > n ? s.substring(0, n) : s;
        }
        int bits = typmod - 4;
        int precision = (bits >> 16) & 0xffff;
        int scale = bits & 0xffff;
        return executor.castEvaluator.applyCast(value, "numeric(" + precision + "," + scale + ")");
    }

    /**
     * The type a call that is really a coercion answers in, for the layer that describes a column
     * before any row is produced. Null when the call is not one.
     */
    DataType typeNameCoercionResultType(String name, FunctionCallExpr fn) {
        if (fn.args().size() != 1 || fn.star() || fn.distinct()) {
            boolean qualified = fn.name() != null
                    && fn.name().toLowerCase(java.util.Locale.ROOT).startsWith("pg_catalog.");
            if (!qualified || fn.args().size() < 2 || fn.args().size() > 3) return null;
            if (!name.equals("varchar") && !name.equals("bpchar") && !name.equals("numeric")) {
                return null;
            }
            return DataType.fromPgName(name);
        }
        String typeName = coercibleTypeName(name);
        if (typeName == null) return null;
        DataType dt = DataType.fromPgName(typeName);
        if (dt != null) return dt;
        if (executor.database != null) {
            DomainType domain = executor.database.getDomain(typeName);
            if (domain != null) return domain.getBaseType();
            if (executor.database.getCustomEnum(typeName) != null) return DataType.ENUM;
        }
        return null;
    }

    // ---- The functions behind the operators ----

    /**
     * The operator spelling of each name pg_operator records as its implementation, or null where
     * two operators share a name and the spelling therefore settles nothing.
     *
     * <p>Empty now that {@code ##} agrees with PostgreSQL for every shape it is written over.
     * close_lseg and close_sb sat here because two segments that do not meet were no point at all
     * in PostgreSQL and a point in memgres, and a segment crossing a box gave the crossing point
     * there and a corner here; both were measured against the live server and fixed, so the two
     * names are callable again. Exposing a name that returns a wrong answer is worse than leaving
     * it uncallable, which is what this list is for when the next one turns up.
     */
    private static final Set<String> ANSWERS_WRONGLY = new HashSet<String>();

    private static final Map<String, String[]> OPERATOR_BY_FUNCTION = buildOperatorByFunction();

    private static Map<String, String[]> buildOperatorByFunction() {
        Map<String, String[]> byFunction = new HashMap<String, String[]>();
        Set<String> ambiguous = new HashSet<String>();
        for (Object[] row : PgOperatorTable.OPERATORS) {
            String symbol = (String) row[0];
            String kind = (String) row[1];
            String function = (String) row[5];
            if (function == null || function.isEmpty()) continue;
            String[] seen = byFunction.get(function);
            if (seen == null) {
                byFunction.put(function, new String[]{symbol, kind});
            } else if (!seen[0].equals(symbol) || !seen[1].equals(kind)) {
                ambiguous.add(function);
            }
        }
        for (String function : ambiguous) byFunction.remove(function);
        return byFunction;
    }

    /** The binary operator each spelling denotes, for the spellings memgres evaluates. */
    private static final Map<String, BinaryExpr.BinOp> BINARY_BY_SYMBOL = buildBinaryBySymbol();

    private static Map<String, BinaryExpr.BinOp> buildBinaryBySymbol() {
        Map<String, BinaryExpr.BinOp> map = new HashMap<String, BinaryExpr.BinOp>();
        map.put("+", BinaryExpr.BinOp.ADD);
        map.put("-", BinaryExpr.BinOp.SUBTRACT);
        map.put("*", BinaryExpr.BinOp.MULTIPLY);
        map.put("/", BinaryExpr.BinOp.DIVIDE);
        map.put("%", BinaryExpr.BinOp.MODULO);
        map.put("^", BinaryExpr.BinOp.POWER);
        map.put("=", BinaryExpr.BinOp.EQUAL);
        map.put("<>", BinaryExpr.BinOp.NOT_EQUAL);
        map.put("<", BinaryExpr.BinOp.LESS_THAN);
        map.put(">", BinaryExpr.BinOp.GREATER_THAN);
        map.put("<=", BinaryExpr.BinOp.LESS_EQUAL);
        map.put(">=", BinaryExpr.BinOp.GREATER_EQUAL);
        map.put("||", BinaryExpr.BinOp.CONCAT);
        map.put("&", BinaryExpr.BinOp.BIT_AND);
        map.put("|", BinaryExpr.BinOp.BIT_OR);
        map.put("#", BinaryExpr.BinOp.BIT_XOR);
        map.put("<<", BinaryExpr.BinOp.SHIFT_LEFT);
        map.put(">>", BinaryExpr.BinOp.SHIFT_RIGHT);
        map.put("@>", BinaryExpr.BinOp.CONTAINS);
        map.put("<@", BinaryExpr.BinOp.CONTAINED_BY);
        map.put("&&", BinaryExpr.BinOp.OVERLAP);
        map.put("->", BinaryExpr.BinOp.JSON_ARROW);
        map.put("->>", BinaryExpr.BinOp.JSON_ARROW_TEXT);
        map.put("#>", BinaryExpr.BinOp.JSON_HASH_ARROW);
        map.put("#>>", BinaryExpr.BinOp.JSON_HASH_ARROW_TEXT);
        map.put("#-", BinaryExpr.BinOp.JSON_DELETE_PATH);
        map.put("@@", BinaryExpr.BinOp.TS_MATCH);
        map.put("?", BinaryExpr.BinOp.JSONB_EXISTS);
        map.put("?|", BinaryExpr.BinOp.JSONB_EXISTS_ANY);
        map.put("?&", BinaryExpr.BinOp.JSONB_EXISTS_ALL);
        map.put("@?", BinaryExpr.BinOp.JSONB_PATH_EXISTS_OP);
        map.put("~", BinaryExpr.BinOp.REGEX_MATCH);
        map.put("~*", BinaryExpr.BinOp.REGEX_IMATCH);
        map.put("!~", BinaryExpr.BinOp.NOT_REGEX_MATCH);
        map.put("!~*", BinaryExpr.BinOp.NOT_REGEX_IMATCH);
        map.put("~~", BinaryExpr.BinOp.LIKE);
        map.put("~~*", BinaryExpr.BinOp.ILIKE);
        map.put("<->", BinaryExpr.BinOp.DISTANCE);
        map.put("~=", BinaryExpr.BinOp.APPROX_EQUAL);
        map.put("<<|", BinaryExpr.BinOp.GEO_BELOW);
        map.put("|>>", BinaryExpr.BinOp.GEO_ABOVE);
        map.put("&<", BinaryExpr.BinOp.GEO_NOT_EXTEND_RIGHT);
        map.put("&>", BinaryExpr.BinOp.GEO_NOT_EXTEND_LEFT);
        map.put("&<|", BinaryExpr.BinOp.GEO_NOT_EXTEND_ABOVE);
        map.put("|&>", BinaryExpr.BinOp.GEO_NOT_EXTEND_BELOW);
        map.put("?#", BinaryExpr.BinOp.GEO_INTERSECTS);
        map.put("##", BinaryExpr.BinOp.GEO_CLOSEST_POINT);
        map.put("?||", BinaryExpr.BinOp.GEO_PARALLEL);
        map.put("?-|", BinaryExpr.BinOp.GEO_PERPENDICULAR);
        map.put("?-", BinaryExpr.BinOp.GEO_HORIZONTAL);
        map.put("-|-", BinaryExpr.BinOp.RANGE_ADJACENT);
        map.put(">>=", BinaryExpr.BinOp.INET_CONTAINS_EQUALS);
        map.put("<<=", BinaryExpr.BinOp.INET_CONTAINED_BY_EQUALS);
        return map;
    }

    /** The prefix operator each spelling denotes, for the spellings memgres evaluates. */
    private static final Map<String, UnaryExpr.UnaryOp> UNARY_BY_SYMBOL = buildUnaryBySymbol();

    private static Map<String, UnaryExpr.UnaryOp> buildUnaryBySymbol() {
        Map<String, UnaryExpr.UnaryOp> map = new HashMap<String, UnaryExpr.UnaryOp>();
        map.put("-", UnaryExpr.UnaryOp.NEGATE);
        map.put("+", UnaryExpr.UnaryOp.POSITIVE);
        map.put("~", UnaryExpr.UnaryOp.BIT_NOT);
        map.put("@", UnaryExpr.UnaryOp.ABS);
        map.put("|/", UnaryExpr.UnaryOp.SQRT);
        map.put("||/", UnaryExpr.UnaryOp.CBRT);
        map.put("@@", UnaryExpr.UnaryOp.GEO_CENTER);
        map.put("@-@", UnaryExpr.UnaryOp.GEO_LENGTH);
        map.put("#", UnaryExpr.UnaryOp.GEO_NPOINTS);
        map.put("?-", UnaryExpr.UnaryOp.GEO_IS_HORIZONTAL);
        map.put("?|", UnaryExpr.UnaryOp.GEO_IS_VERTICAL);
        return map;
    }

    /**
     * Whether this name is one PostgreSQL keeps as the implementation of an operator, and memgres
     * can therefore call by evaluating the operator. Read by the placement checks, which refuse a
     * name the engine cannot dispatch before they look at anything else about the call.
     */
    static boolean isOperatorFunction(String name) {
        if (name == null) return false;
        String[] operator = OPERATOR_BY_FUNCTION.get(name.toLowerCase(java.util.Locale.ROOT));
        if (operator == null) return false;
        return "b".equals(operator[1]) ? BINARY_BY_SYMBOL.containsKey(operator[0])
                : UNARY_BY_SYMBOL.containsKey(operator[0]);
    }

    /**
     * The value the operator behind this name produces, or {@link #NOT_HANDLED} when the name
     * backs no operator memgres evaluates or the call was not written with the arguments the
     * operator takes.
     */
    private Object operatorFunctionCall(String name, FunctionCallExpr fn, RowContext ctx) {
        Expression rewritten = operatorFunctionExpr(name, fn);
        if (rewritten == null) return NOT_HANDLED;
        try {
            return executor.evalExpr(rewritten, ctx);
        } catch (MemgresException e) {
            // The query wrote a function, so an argument the operator cannot take is that function
            // resolving to nothing -- naming the operator would name something nobody wrote.
            if ("42883".equals(e.getSqlState()) && e.getMessage() != null
                    && e.getMessage().startsWith("operator does not exist")) {
                throw new MemgresException("function " + fn.name() + "("
                        + argTypeNames(fn, ctx) + ") does not exist"
                        + "\n  Hint: No function matches the given name and argument types."
                        + " You might need to add explicit type casts.", "42883");
            }
            throw e;
        }
    }

    /**
     * The call written as the operator it implements, or null when it is not one. Both evaluation
     * and the layer that describes the column read it, so the two cannot disagree about what the
     * call answers.
     */
    static Expression operatorFunctionExpr(String name, FunctionCallExpr fn) {
        if (fn.star() || fn.distinct()) return null;
        for (Expression a : fn.args()) {
            if (a instanceof NamedArgExpr) return null;
        }
        String[] operator = OPERATOR_BY_FUNCTION.get(name);
        if (operator == null) return null;
        if ("b".equals(operator[1]) && fn.args().size() == 2) {
            BinaryExpr.BinOp op = BINARY_BY_SYMBOL.get(operator[0]);
            return op == null ? null
                    : new BinaryExpr(fn.args().get(0), op, fn.args().get(1));
        }
        if ("l".equals(operator[1]) && fn.args().size() == 1) {
            UnaryExpr.UnaryOp op = UNARY_BY_SYMBOL.get(operator[0]);
            return op == null ? null : new UnaryExpr(op, fn.args().get(0));
        }
        return null;
    }

    private void requireArgs(FunctionCallExpr fn, int min) {
        if (fn.args().size() < min) {
            throw new MemgresException(
                "function " + fn.name() + "() does not exist" +
                (fn.args().isEmpty() ? "" : "\n  Hint: No function matches the given name and argument types."), "42883");
        }
    }

    /**
     * Checks that a required extension is installed. PG 18 requires CREATE EXTENSION
     * before extension functions become available. Throws 42883 if not installed.
     */
    private void requireExtension(String extensionName, String functionName, int argCount) {
        if (!executor.database.hasExtension(extensionName)) {
            String sig = functionName + "(" + String.join(", ",
                    java.util.Collections.nCopies(argCount, "unknown")) + ")";
            throw new MemgresException(
                    "function " + sig + " does not exist\n" +
                    "  Hint: No function matches the given name and argument types. " +
                    "You might need to add explicit type casts.", "42883");
        }
    }

    /** True when the expression is jsonb, which stores a parsed value rather than its text. */
    private boolean isJsonbTyped(Expression expr, RowContext ctx) {
        if (expr instanceof CastExpr) {
            String name = ((CastExpr) expr).typeName();
            return name != null && "jsonb".equalsIgnoreCase(name.trim());
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
                return t != null && "jsonb".equalsIgnoreCase(t.getPgName());
            }
        }
        return false;
    }

    /**
     * The declared array type of a column, or null when the expression does not name one. PG
     * spells array types with a leading underscore in the catalog, so that is what identifies one.
     */
    private DataType arrayColumnType(Expression expr, RowContext ctx) {
        if (expr instanceof CastExpr) {
            String name = ((CastExpr) expr).typeName();
            if (name == null) return null;
            String trimmed = name.trim();
            if (!trimmed.endsWith("[]")) return null;
            return DataType.fromPgName("_" + trimmed.substring(0, trimmed.length() - 2).trim());
        }
        if (!(expr instanceof ColumnRef) || ctx == null) return null;
        ColumnRef ref = (ColumnRef) expr;
        for (RowContext.TableBinding b : ctx.getBindings()) {
            if (b.table() == null) continue;
            if (ref.table() != null && !ref.table().equalsIgnoreCase(b.alias())
                    && !ref.table().equalsIgnoreCase(b.table().getName())) continue;
            int idx = b.table().getColumnIndex(ref.column());
            if (idx < 0) continue;
            DataType t = b.table().getColumns().get(idx).getType();
            if (t != null && t.getPgName() != null && t.getPgName().startsWith("_")) return t;
            return null;
        }
        return null;
    }

    /** {@code _int4} holds int4 elements; drop the underscore to name what goes in it. */
    private DataType elementTypeOf(DataType arrayType) {
        if (arrayType == null || arrayType.getPgName() == null) return null;
        String name = arrayType.getPgName();
        if (!name.startsWith("_")) return null;
        return DataType.fromPgName(name.substring(1));
    }

    /**
     * The array-mutating functions are declared over {@code anyarray}, so an untyped literal
     * handed to one is read as an array of whatever element type the call settles on -- not left
     * as the piece of text it looks like. Dropping it instead is what made
     * {@code array_cat('{1,2}','{3}')} answer {@code {}}: both operands were text, neither was a
     * List, and the function quietly concatenated nothing at all.
     *
     * @param elementTypeName the element type the other argument states, or null for text
     */
    private Object readArrayOperand(Object value, String elementTypeName) {
        if (!(value instanceof String)) return value;
        String body = ArrayLiteral.body((String) value);
        if (!body.startsWith("{") || !body.endsWith("}")) return value;
        String element = elementTypeName != null ? elementTypeName : "text";
        try {
            return executor.applyCast(value, element + "[]");
        } catch (MemgresException e) {
            // A literal the settled element type cannot read stays as it was, so the existing
            // error surfaces from the operation rather than from this resolution step.
            return value;
        }
    }

    /** The type name an already-typed value carries, which an untyped literal beside it takes. */
    private static String elementTypeNameOf(Object value) {
        if (value == null) return null;
        if (value instanceof String) return null;
        return AstExecutor.pgTypeNameOf(value);
    }

    /** An operand of the array containment functions, which take arrays and nothing else. */
    private static List<?> asElementList(Object value, String functionName) {
        if (value instanceof List<?>) return (List<?>) value;
        throw new MemgresException("function " + functionName + "("
                + AstExecutor.pgTypeNameOf(value) + ", " + AstExecutor.pgTypeNameOf(value)
                + ") does not exist\n"
                + "  Hint: No function matches the given name and argument types. "
                + "You might need to add explicit type casts.", "42883");
    }

    /** The element type of an argument that is already a proper array, or null. */
    private static String elementTypeNameOfArray(Object value) {
        if (!(value instanceof List<?>)) return null;
        for (Object element : (List<?>) value) {
            String name = elementTypeNameOf(element);
            if (name != null) return name;
        }
        return null;
    }

    Object evalFunction(FunctionCallExpr fn, RowContext ctx) {
        String name = foldedName(fn.name());
        // Strip a schema prefix that names the schema the function is really in
        name = stripCallableSchemaPrefix(name);
        // A qualifier is resolved to a schema before anything is looked for inside it, so a
        // qualifier that names no schema is reported as the missing schema rather than as a
        // function that does not exist in it.
        rejectMissingSchemaQualifier(name);

        // Reject DEFAULT as a function argument; PG gives 42601 (syntax error)
        for (Expression arg : fn.args()) {
            if (arg instanceof Literal && ((Literal) arg).literalType() == Literal.LiteralType.DEFAULT) {
                Literal lit = (Literal) arg;
                throw new MemgresException("DEFAULT is not allowed in this context", "42601");
            }
        }

        // Expand VARIADIC args: NamedArgExpr("__variadic__", arrayExpr) → expand array to individual args
        boolean callUsedVariadic = fn.args().stream().anyMatch(a -> a instanceof NamedArgExpr && ((NamedArgExpr) a).name().equals("__variadic__"));
        // PG has no concat_ws(text) signature: a separator alone with no value arguments and no
        // VARIADIC part fails function resolution (42883). A VARIADIC argument satisfies the
        // signature even when the array is empty, so gate on the pre-expansion argument shape.
        if ("concat_ws".equals(name) && fn.args().size() < 2 && !callUsedVariadic) {
            throw new MemgresException("function concat_ws(unknown) does not exist\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts.", "42883");
        }
        if (callUsedVariadic) {
            List<Expression> expandedArgs = new ArrayList<>();
            for (Expression arg : fn.args()) {
                if (arg instanceof NamedArgExpr && ((NamedArgExpr) arg).name().equals("__variadic__")) {
                    NamedArgExpr na = (NamedArgExpr) arg;
                    // Evaluate the array and expand into individual literal args
                    Object arrVal = executor.evalExpr(na.value(), ctx);
                    if (arrVal != null) {
                        List<Object> elements = FromFunctionResolver.toElementList(arrVal);
                        for (Object elem : elements) {
                            if (elem == null) expandedArgs.add(Literal.ofNull());
                            else expandedArgs.add(Literal.ofString(elem.toString()));
                        }
                    }
                } else {
                    expandedArgs.add(arg);
                }
            }
            fn = new FunctionCallExpr(fn.name(), expandedArgs, fn.distinct(), fn.star());
        }

        // Validate named args: check for duplicates and positional-after-named
        boolean seenNamed = false;
        Set<String> namedArgNames = new java.util.HashSet<>();
        for (Expression arg : fn.args()) {
            if (arg instanceof NamedArgExpr && !((NamedArgExpr) arg).name().equals("__variadic__")) {
                NamedArgExpr na = (NamedArgExpr) arg;
                seenNamed = true;
                if (!namedArgNames.add(na.name())) {
                    throw new MemgresException("argument name \"" + na.name() + "\" used more than once", "42601");
                }
            } else if (seenNamed && !(arg instanceof NamedArgExpr)) {
                throw new MemgresException("positional argument cannot follow named argument", "42601");
            }
        }

        // Ordered-set aggregates require WITHIN GROUP clause; without it, PG gives 42601
        if (name.equals("percentile_cont") || name.equals("percentile_disc") || name.equals("mode")) {
            throw new MemgresException(
                "function " + name + " requires WITHIN GROUP (ORDER BY ...) syntax", "42601");
        }

        // VALUES is not a function; using it as a function argument is a syntax error
        if (name.equals("values")) {
            throw new MemgresException("syntax error at or near \"VALUES\"", "42601");
        }

        // A function is resolved by its name and its argument list together, so a call with a
        // number of arguments no signature of that name has resolves to nothing at all. memgres
        // read the arguments it wanted and ignored the rest, which made upper('a','b') answer 'A'
        // and now(1) answer the time.
        // A VARIADIC argument was one written argument before it was expanded, and an empty array
        // expands to none at all, so the count in hand is not the count the writer gave.
        if (!callUsedVariadic) rejectWrongArity(name, fn, ctx);
        // A routine memgres answers without reading its arguments is still strict, so a NULL
        // argument is the whole call. Left to the implementation, the answer was whatever it was
        // going to be anyway: false, the empty array, the one role, the empty string a void
        // function prints -- none of which the client asked about.
        if (!callUsedVariadic && !fn.args().isEmpty()
                && BuiltinFunctionSignatures.nullArgumentMakesTheCallNull(name, fn.args().size())
                && !userDeclaredFunction(executor, name)) {
            for (Expression arg : fn.args()) {
                if (executor.evalExpr(arg, ctx) == null) return null;
            }
        }

        // PostgreSQL reports a refused call the way the statement wrote it, and it writes the
        // grammar-spelled forms schema-qualified: the same missing routine is pg_catalog.substring
        // written substring(s FROM n) and substring written substring(s, n).
        if (!callUsedVariadic) {
            rejectAmbiguousBuiltin(executor, name,
                    fn.spelledInGrammar ? "pg_catalog." + fn.name() : fn.name(), fn.args(), ctx);
        }

        // A routine declared over text takes a bpchar argument through the conversion that drops
        // the blanks its declaration added, so upper('ab'::char(5)) is AB and not "AB   ". The
        // argument is read once here and handed on already trimmed; only the ones whose type it
        // was written with are touched, so nothing else about the call is read differently.
        fn = withBlanksDropped(fn, ctx, name);

        // Delegate to category handlers
        Object delegated;
        delegated = mathFunctions.eval(name, fn, ctx);
        if (delegated != NOT_HANDLED) return delegated;
        delegated = stringFunctions.eval(name, fn, ctx);
        if (delegated != NOT_HANDLED) return delegated;
        delegated = catalogSystemFunctions.eval(name, fn, ctx);
        if (delegated != NOT_HANDLED) return delegated;

        switch (name) {
            case "merge_action": {
                // merge_action() returns 'INSERT', 'UPDATE', or 'DELETE' in MERGE RETURNING (PG 17+)
                if (executor.currentMergeAction == null) {
                    throw new MemgresException("merge_action() can only be used in a MERGE RETURNING clause", "42P20");
                }
                return executor.currentMergeAction;
            }
            case "row": {
                // ROW(a, b, c) -> List of evaluated values
                List<Object> row = new ArrayList<>();
                for (Expression arg : fn.args()) {
                    row.add(executor.evalExpr(arg, ctx));
                }
                return row;
            }
            case "now":
            case "current_timestamp": {
                // now()/current_timestamp must be stable within a transaction (transaction timestamp)
                if (executor.session != null && executor.session.getTransactionTimestamp() != null) {
                    return executor.session.getTransactionTimestamp();
                }
                return executor.currentStatementTimestamp != null ? executor.currentStatementTimestamp : OffsetDateTime.now();
            }
            case "current_date":
                return executor.currentInstant()
                        .atZoneSameInstant(TypeCoercion.sessionZone()).toLocalDate();
            case "current_time":
            case "localtime":
                return executor.currentInstant()
                        .atZoneSameInstant(TypeCoercion.sessionZone()).toLocalTime();
            case "localtimestamp":
                return executor.currentInstant()
                        .atZoneSameInstant(TypeCoercion.sessionZone()).toLocalDateTime();
            case "version":
                return "PostgreSQL 18.0";
            case "gen_random_uuid":
                return java.util.UUID.randomUUID();
            case "uuidv4":
                return java.util.UUID.randomUUID();
            case "uuid_generate_v4":
                requireExtension("uuid-ossp", name, fn.args().size());
                return java.util.UUID.randomUUID();
            case "uuid_generate_v1": {
                requireExtension("uuid-ossp", name, fn.args().size());
                // UUID v1: timestamp + node (MAC) based
                // Use current time since UUID epoch (Oct 15, 1582) in 100-ns intervals
                long uuidEpochOffset = 122192928000000000L; // 100-ns intervals between UUID epoch and Unix epoch
                long timestamp = System.currentTimeMillis() * 10000L + uuidEpochOffset;
                long timeLow = timestamp & 0xFFFFFFFFL;
                long timeMid = (timestamp >>> 32) & 0xFFFFL;
                long timeHi = (timestamp >>> 48) & 0x0FFFL;
                long msb = (timeLow << 32) | (timeMid << 16) | 0x1000L | timeHi; // version 1
                // Clock sequence (random) and node (random, multicast bit set)
                java.security.SecureRandom sr = new java.security.SecureRandom();
                int clockSeq = sr.nextInt(0x3FFF);
                long node = sr.nextLong() & 0xFFFFFFFFFFL | 0x010000000000L; // set multicast bit
                long lsb = ((long)(0x80 | ((clockSeq >>> 8) & 0x3F)) << 56)
                         | ((long)(clockSeq & 0xFF) << 48)
                         | node;
                return new java.util.UUID(msb, lsb);
            }
            case "uuid_generate_v3": {
                requireExtension("uuid-ossp", name, fn.args().size());
                // UUID v3: MD5-based namespace UUID
                requireArgs(fn, 2);
                Object nsArg = executor.evalExpr(fn.args().get(0), ctx);
                Object nameArg = executor.evalExpr(fn.args().get(1), ctx);
                if (nsArg == null || nameArg == null) return null;
                java.util.UUID namespace = nsArg instanceof java.util.UUID ? (java.util.UUID) nsArg : java.util.UUID.fromString(nsArg.toString());
                return uuid3(namespace, nameArg.toString());
            }
            case "uuid_generate_v5": {
                requireExtension("uuid-ossp", name, fn.args().size());
                // UUID v5: SHA-1-based namespace UUID
                requireArgs(fn, 2);
                Object nsArg = executor.evalExpr(fn.args().get(0), ctx);
                Object nameArg = executor.evalExpr(fn.args().get(1), ctx);
                if (nsArg == null || nameArg == null) return null;
                java.util.UUID namespace = nsArg instanceof java.util.UUID ? (java.util.UUID) nsArg : java.util.UUID.fromString(nsArg.toString());
                return uuid5(namespace, nameArg.toString());
            }
            case "uuid_nil":
                requireExtension("uuid-ossp", name, fn.args().size());
                return java.util.UUID.fromString("00000000-0000-0000-0000-000000000000");
            case "uuid_ns_dns":
                requireExtension("uuid-ossp", name, fn.args().size());
                return java.util.UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
            case "uuid_ns_url":
                requireExtension("uuid-ossp", name, fn.args().size());
                return java.util.UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8");
            case "uuidv7": {
                if (!fn.args().isEmpty()) {
                    throw new MemgresException("function uuidv7(" + fn.args().stream()
                            .map(a -> { Object v = executor.evalExpr(a, ctx); return v instanceof Integer ? "integer" : "unknown"; })
                            .collect(java.util.stream.Collectors.joining(", ")) + ") does not exist", "42883");
                }
                // UUID v7: time-ordered UUID (PG18-specific)
                long timestamp = System.currentTimeMillis();
                long msb = (timestamp << 16) | 0x7000L | (long)(Math.random() * 0x0FFF);
                long lsb = 0x8000000000000000L | (long)(Math.random() * 0x3FFFFFFFFFFFFFFFL);
                return new java.util.UUID(msb, lsb);
            }
            // The version is the four bits after the third dash; v7 additionally carries the
            // millisecond timestamp it was minted from in its leading 48 bits.
            case "uuid_extract_version": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String uuid = arg.toString();
                java.util.UUID parsed = java.util.UUID.fromString(uuid);
                return parsed.version();
            }
            case "uuid_extract_timestamp": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                java.util.UUID parsed = java.util.UUID.fromString(arg.toString());
                if (parsed.version() != 7) return null;
                long millis = parsed.getMostSignificantBits() >>> 16;
                return java.time.OffsetDateTime.ofInstant(
                        java.time.Instant.ofEpochMilli(millis), java.time.ZoneOffset.UTC);
            }
            // json(text) is the constructor behind JSON '...'; it validates and yields a json value.
            case "json": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String text = arg.toString();
                if (!ExprEvaluator.isValidJson(text)) {
                    throw new MemgresException("invalid input syntax for type json", "22P02");
                }
                return text;
            }
            case "crc32": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof Number) throw new MemgresException("function crc32(integer) does not exist", "42883");
                java.util.zip.CRC32 crc = new java.util.zip.CRC32();
                crc.update(byteaOf(arg));
                return crc.getValue();
            }
            case "crc32c": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof Number) throw new MemgresException("function crc32c(integer) does not exist", "42883");
                return crc32c(byteaOf(arg));
            }
            case "gen_random_bytes": {
                requireArgs(fn, 1);
                int size = TypeCoercion.toInteger(executor.evalExpr(fn.args().get(0), ctx));
                byte[] bytes = new byte[size];
                new java.security.SecureRandom().nextBytes(bytes);
                return bytes;
            }
            case "digest": {
                requireExtension("pgcrypto", name, fn.args().size());
                // pgcrypto: digest(data, type) → bytea hash
                requireArgs(fn, 2);
                Object dataArg = executor.evalExpr(fn.args().get(0), ctx);
                Object typeArg = executor.evalExpr(fn.args().get(1), ctx);
                if (dataArg == null || typeArg == null) return null;
                byte[] data;
                if (dataArg instanceof byte[]) {
                    data = (byte[]) dataArg;
                } else {
                    data = dataArg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }
                String algo = typeArg.toString().toLowerCase().trim();
                String javaAlgo;
                switch (algo) {
                    case "md5": javaAlgo = "MD5"; break;
                    case "sha1": javaAlgo = "SHA-1"; break;
                    case "sha224": javaAlgo = "SHA-224"; break;
                    case "sha256": javaAlgo = "SHA-256"; break;
                    case "sha384": javaAlgo = "SHA-384"; break;
                    case "sha512": javaAlgo = "SHA-512"; break;
                    default: throw new MemgresException("Cannot use \"" + algo + "\": No such hash algorithm", "22023");
                }
                try {
                    java.security.MessageDigest md = java.security.MessageDigest.getInstance(javaAlgo);
                    return md.digest(data);
                } catch (java.security.NoSuchAlgorithmException e) {
                    throw new MemgresException("Cannot use \"" + algo + "\": No such hash algorithm", "22023");
                }
            }
            case "hmac": {
                requireExtension("pgcrypto", name, fn.args().size());
                // pgcrypto: hmac(data, key, type) → bytea HMAC
                requireArgs(fn, 3);
                Object dataArg = executor.evalExpr(fn.args().get(0), ctx);
                Object keyArg = executor.evalExpr(fn.args().get(1), ctx);
                Object typeArg = executor.evalExpr(fn.args().get(2), ctx);
                if (dataArg == null || keyArg == null || typeArg == null) return null;
                byte[] data;
                if (dataArg instanceof byte[]) {
                    data = (byte[]) dataArg;
                } else {
                    data = dataArg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }
                byte[] key;
                if (keyArg instanceof byte[]) {
                    key = (byte[]) keyArg;
                } else {
                    key = keyArg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }
                String algo = typeArg.toString().toLowerCase().trim();
                String hmacAlgo;
                switch (algo) {
                    case "md5": hmacAlgo = "HmacMD5"; break;
                    case "sha1": hmacAlgo = "HmacSHA1"; break;
                    case "sha224": hmacAlgo = "HmacSHA224"; break;
                    case "sha256": hmacAlgo = "HmacSHA256"; break;
                    case "sha384": hmacAlgo = "HmacSHA384"; break;
                    case "sha512": hmacAlgo = "HmacSHA512"; break;
                    default: throw new MemgresException("Cannot use \"" + algo + "\": No such hash algorithm", "22023");
                }
                try {
                    javax.crypto.Mac mac = javax.crypto.Mac.getInstance(hmacAlgo);
                    mac.init(new javax.crypto.spec.SecretKeySpec(key, hmacAlgo));
                    return mac.doFinal(data);
                } catch (java.security.NoSuchAlgorithmException | java.security.InvalidKeyException e) {
                    throw new MemgresException("Cannot use \"" + algo + "\": " + e.getMessage(), "22023");
                }
            }
            case "gen_salt": {
                requireExtension("pgcrypto", name, fn.args().size());
                // pgcrypto: gen_salt(type [, iter_count]) → text salt string
                requireArgs(fn, 1);
                Object typeArg = executor.evalExpr(fn.args().get(0), ctx);
                if (typeArg == null) return null;
                String saltType = typeArg.toString().toLowerCase().trim();
                java.security.SecureRandom sr = new java.security.SecureRandom();
                switch (saltType) {
                    case "bf": {
                        int rounds = fn.args().size() > 1 ? TypeCoercion.toInteger(executor.evalExpr(fn.args().get(1), ctx)) : 8;
                        byte[] saltBytes = new byte[16];
                        sr.nextBytes(saltBytes);
                        String encoded = java.util.Base64.getEncoder().encodeToString(saltBytes).substring(0, 22);
                        return "$2a$" + String.format("%02d", rounds) + "$" + encoded;
                    }
                    case "md5": {
                        byte[] saltBytes = new byte[6];
                        sr.nextBytes(saltBytes);
                        StringBuilder sb = new StringBuilder("$1$");
                        for (byte b : saltBytes) sb.append(String.format("%02x", b & 0xFF));
                        return sb.toString();
                    }
                    case "des": {
                        byte[] saltBytes = new byte[2];
                        sr.nextBytes(saltBytes);
                        String chars = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
                        return "" + chars.charAt((saltBytes[0] & 0xFF) % 64) + chars.charAt((saltBytes[1] & 0xFF) % 64);
                    }
                    case "xdes": {
                        byte[] saltBytes = new byte[3];
                        sr.nextBytes(saltBytes);
                        String chars = "./0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
                        return "_" + chars.charAt((saltBytes[0] & 0xFF) % 64) + chars.charAt((saltBytes[1] & 0xFF) % 64) + chars.charAt((saltBytes[2] & 0xFF) % 64);
                    }
                    default:
                        throw new MemgresException("Unknown salt type: " + saltType, "22023");
                }
            }
            case "pg_notify": {
                // pg_notify(channel, payload): sends notification, returns void
                // Respects transaction boundaries: deferred until COMMIT, discarded on ROLLBACK
                requireArgs(fn, 2);
                Object channel = executor.evalExpr(fn.args().get(0), ctx);
                Object payload = executor.evalExpr(fn.args().get(1), ctx);
                // A notification travels through a fixed-size queue slot, so PG bounds both
                // the channel name and the payload rather than truncating either
                if (channel == null || channel.toString().trim().isEmpty()) {
                    throw new MemgresException("channel name cannot be empty", "22023");
                }
                if (payload != null && payload.toString().length() >= NOTIFY_PAYLOAD_LIMIT) {
                    throw new MemgresException("payload string too long", "22023");
                }
                if (channel != null) {
                    if (executor.session != null) {
                        executor.session.queueNotification(
                                channel.toString(), payload != null ? payload.toString() : "");
                    } else {
                        executor.database.getNotificationManager().notify(
                                channel.toString(), payload != null ? payload.toString() : "", 0);
                    }
                }
                // pg_notify returns void, which renders as an empty string over the wire --
                // so `pg_notify(...) IS NULL` is false, as it is in PG
                return "";
            }
            // Set-returning, and PostgreSQL lets a set-returning call stand in the select list as
            // readily as in FROM. Both were reachable only through FROM, so writing one in the
            // select list answered that the function does not exist.
            case "string_to_table":
                return FromFunctionResolver.stringToTableValues(evaluatedArgs(fn, ctx));
            case "regexp_split_to_table":
                return FromFunctionResolver.regexpSplitToTableValues(evaluatedArgs(fn, ctx));
            case "generate_series": {
                if (fn.args().size() < 2) {
                    throw new MemgresException("function generate_series() does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                Object startObj = executor.evalExpr(fn.args().get(0), ctx);
                Object stopObj = executor.evalExpr(fn.args().get(1), ctx);
                Object stepObj = fn.args().size() > 2 ? executor.evalExpr(fn.args().get(2), ctx) : null;
                // generate_series is a strict function: a NULL start/stop bound (e.g. a
                // parameter that is unknown/NULL at Describe time, or a NULL computed via
                // date_trunc/AT TIME ZONE of a NULL timestamp) yields an empty set, not an
                // error. Without this guard the date/timestamp overload below unconditionally
                // calls TypeCoercion.toLocalDateTime(null), which NPEs on "val.toString()".
                if (startObj == null || stopObj == null) {
                    return new ArrayList<>();
                }
                // OffsetDateTime (timestamptz) overload
                if (startObj instanceof java.time.OffsetDateTime) {
                    java.time.OffsetDateTime tzStart = (java.time.OffsetDateTime) startObj;
                    java.time.OffsetDateTime tzStop = stopObj instanceof java.time.OffsetDateTime ? (java.time.OffsetDateTime) stopObj
                            : TypeCoercion.toOffsetDateTime(stopObj);
                    PgInterval step = stepObj != null ? TypeCoercion.toInterval(stepObj) : new PgInterval(0, 1, 0);
                    boolean ascending = !tzStart.isAfter(tzStop);
                    List<Object> result = new ArrayList<>();
                    java.time.OffsetDateTime cur = tzStart;
                    for (int i = 0; i < 10000; i++) {
                        if (ascending ? cur.isAfter(tzStop) : cur.isBefore(tzStop)) break;
                        result.add(cur);
                        java.time.OffsetDateTime next = step.addTo(cur);
                        if (next.isEqual(cur)) break;
                        cur = next;
                    }
                    return result;
                }
                // Date/timestamp overload: generate_series(start_date, end_date, interval)
                if (startObj instanceof LocalDate || startObj instanceof LocalDateTime
                        || (stepObj instanceof PgInterval)) {
                    boolean dateInput = startObj instanceof LocalDate;
                    LocalDateTime start = startObj instanceof LocalDate ? ((LocalDate) startObj).atStartOfDay() : TypeCoercion.toLocalDateTime(startObj);
                    LocalDateTime stop = stopObj instanceof LocalDate ? ((LocalDate) stopObj).atStartOfDay() : TypeCoercion.toLocalDateTime(stopObj);
                    PgInterval step = stepObj != null ? TypeCoercion.toInterval(stepObj) : new PgInterval(0, 1, 0);
                    boolean ascending = !start.isAfter(stop);
                    List<Object> result = new ArrayList<>();
                    LocalDateTime cur = start;
                    for (int i = 0; i < 10000; i++) {
                        if (ascending ? cur.isAfter(stop) : cur.isBefore(stop)) break;
                        result.add(dateInput ? cur.toLocalDate() : cur);
                        LocalDateTime next = step.addTo(cur);
                        if (next.isEqual(cur)) break;
                        cur = next;
                    }
                    return result;
                }
                // Numeric overload, reject non-numeric args
                if (startObj instanceof String && !((String) startObj).isEmpty() && !Character.isDigit(((String) startObj).charAt(0)) && ((String) startObj).charAt(0) != '-') {
                    String s = (String) startObj;
                    throw new MemgresException("function generate_series(unknown, unknown) is not unique", "42725");
                }
                // A bound or a step with a fraction is the numeric form, and truncating it to a
                // bigint answered a series of whole numbers for a series that has none: the same
                // call written in FROM already answered 1.5, 2.5, 3.5.
                if (startObj instanceof java.math.BigDecimal || stopObj instanceof java.math.BigDecimal
                        || stepObj instanceof java.math.BigDecimal) {
                    java.math.BigDecimal nStart = TypeCoercion.toBigDecimal(startObj);
                    java.math.BigDecimal nStop = TypeCoercion.toBigDecimal(stopObj);
                    java.math.BigDecimal nStep = stepObj != null
                            ? TypeCoercion.toBigDecimal(stepObj) : java.math.BigDecimal.ONE;
                    List<Object> numeric = new ArrayList<>();
                    if (nStep.signum() != 0) {
                        boolean up = nStep.signum() > 0;
                        // Each value is the start plus so many steps, which carries the scale that
                        // adding the step that many times carries.
                        for (int i = 0; i < 10000; i++) {
                            java.math.BigDecimal v = i == 0 ? nStart
                                    : nStart.add(nStep.multiply(java.math.BigDecimal.valueOf(i)));
                            if (up ? v.compareTo(nStop) > 0 : v.compareTo(nStop) < 0) break;
                            numeric.add(v);
                        }
                    }
                    return numeric;
                }
                long start = executor.toLong(startObj);
                long stop = executor.toLong(stopObj);
                long step = stepObj != null ? executor.toLong(stepObj) : 1;
                List<Object> result = new ArrayList<>();
                if (step > 0) {
                    for (long v = start; v <= stop; v += step) {
                        result.add((v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) ? (int) v : v);
                    }
                } else if (step < 0) {
                    for (long v = start; v >= stop; v += step) {
                        result.add((v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) ? (int) v : v);
                    }
                }
                return result;
            }
            case "generate_subscripts": {
                // generate_subscripts(anyarray, dim [, reverse]) → set of integer subscripts
                if (fn.args().size() < 2) {
                    throw new MemgresException("function generate_subscripts() does not exist", "42883");
                }
                Object arrObj = executor.evalExpr(fn.args().get(0), ctx);
                int dim = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                boolean reverse = fn.args().size() > 2 && executor.isTruthy(executor.evalExpr(fn.args().get(2), ctx));
                int lo = 1;
                int hi;
                // Handle custom lower-bound arrays: "[lb:ub]={...}" format
                if (arrObj instanceof String && ((String) arrObj).matches("\\[\\d+:\\d+\\]=\\{.*\\}")) {
                    String s = (String) arrObj;
                    int eqIdx = s.indexOf('=');
                    String boundsStr = s.substring(0, eqIdx);
                    String[] parts = boundsStr.substring(1, boundsStr.length() - 1).split(":");
                    lo = Integer.parseInt(parts[0].trim());
                    hi = Integer.parseInt(parts[1].trim());
                } else if (arrObj instanceof List<?>) {
                    List<?> list = (List<?>) arrObj;
                    if (dim == 2 && !list.isEmpty() && list.get(0) instanceof List<?>) {
                        List<?> sub = (List<?>) list.get(0);
                        hi = lo + sub.size() - 1;
                    } else if (dim == 1) {
                        hi = lo + list.size() - 1;
                    } else {
                        return Cols.listOf(); // dimension doesn't exist
                    }
                } else if (arrObj instanceof String && ((String) arrObj).startsWith("{") && ((String) arrObj).endsWith("}")) {
                    String s = (String) arrObj;
                    String inner = s.substring(1, s.length() - 1);
                    if (inner.isEmpty()) return Cols.listOf();
                    // Detect multi-dimensional arrays: inner content starts with '{'
                    if (inner.trim().startsWith("{")) {
                        // Multi-dimensional array like {{1,2},{3,4}}
                        // Count top-level sub-arrays for dim calculation
                        List<String> topLevel = splitTopLevelSubArrays(inner);
                        if (dim == 1) {
                            hi = lo + topLevel.size() - 1;
                        } else if (dim == 2 && !topLevel.isEmpty()) {
                            // Parse the first sub-array to get dimension 2 size
                            String firstSub = topLevel.get(0).trim();
                            if (firstSub.startsWith("{") && firstSub.endsWith("}")) {
                                String subInner = firstSub.substring(1, firstSub.length() - 1);
                                // Check for further nesting
                                if (subInner.trim().startsWith("{")) {
                                    List<String> subLevel = splitTopLevelSubArrays(subInner);
                                    hi = lo + subLevel.size() - 1;
                                } else {
                                    List<Object> subElems = parseSimplePgArray(firstSub);
                                    hi = lo + subElems.size() - 1;
                                }
                            } else {
                                return Cols.listOf();
                            }
                        } else if (dim > 2 && !topLevel.isEmpty()) {
                            // Navigate deeper dimensions
                            String current = topLevel.get(0).trim();
                            for (int d = 2; d < dim; d++) {
                                if (current.startsWith("{") && current.endsWith("}")) {
                                    String ci = current.substring(1, current.length() - 1);
                                    if (ci.trim().startsWith("{")) {
                                        List<String> sub = splitTopLevelSubArrays(ci);
                                        if (sub.isEmpty()) { return Cols.listOf(); }
                                        current = sub.get(0).trim();
                                    } else {
                                        return Cols.listOf(); // dimension doesn't exist
                                    }
                                } else {
                                    return Cols.listOf();
                                }
                            }
                            // current should be an array at the target dimension
                            if (current.startsWith("{") && current.endsWith("}")) {
                                String ci = current.substring(1, current.length() - 1);
                                if (ci.trim().startsWith("{")) {
                                    List<String> sub = splitTopLevelSubArrays(ci);
                                    hi = lo + sub.size() - 1;
                                } else {
                                    List<Object> subElems = parseSimplePgArray(current);
                                    hi = lo + subElems.size() - 1;
                                }
                            } else {
                                return Cols.listOf();
                            }
                        } else {
                            return Cols.listOf();
                        }
                    } else {
                        // 1D array
                        List<Object> elems = parseSimplePgArray(s);
                        if (dim == 1) {
                            hi = lo + elems.size() - 1;
                        } else {
                            return Cols.listOf();
                        }
                    }
                } else {
                    return Cols.listOf();
                }
                List<Object> result = new ArrayList<>();
                if (lo <= hi) {
                    if (reverse) {
                        for (int i = hi; i >= lo; i--) result.add(i);
                    } else {
                        for (int i = lo; i <= hi; i++) result.add(i);
                    }
                }
                return result;
            }
            case "nextval": {
                Object seqArg = executor.evalExpr(fn.args().get(0), ctx);
                String seqName;
                if (seqArg instanceof RegclassValue) {
                    RegclassValue rc = (RegclassValue) seqArg;
                    seqName = rc.name();
                } else if (seqArg instanceof Number) {
                    // OID from ::regclass, look up sequence by trying all sequences
                    int targetOid = ((Number) seqArg).intValue();
                    seqName = null;
                    for (Map.Entry<String, Sequence> entry : executor.database.getSequences().entrySet()) {
                        int seqOid = executor.systemCatalog.getOid("rel:" + entry.getKey());
                        if (seqOid == targetOid) { seqName = entry.getKey(); break; }
                    }
                    if (seqName == null) seqName = String.valueOf(seqArg); // fallback
                } else {
                    seqName = String.valueOf(seqArg);
                }
                // A qualifier names one schema's sequence and no other's: stripping it made
                // nextval('other.s') advance whichever sequence of that name was found first.
                Sequence seq = resolveSequence(seqName);
                if (seq == null) throw new MemgresException("relation \"" + seqName + "\" does not exist", "42P01");
                long nv;
                if (executor.session != null && seq.getCache() > 1) {
                    nv = executor.session.nextvalCached(seq);
                } else {
                    nv = seq.nextVal();
                }
                executor.lastSequenceValue = nv;
                executor.sessionSequenceValues.put(seq.qualifiedName().toLowerCase(), nv);
                return nv;
            }
            case "currval": {
                String seqName = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                Sequence seq = resolveSequence(seqName);
                if (seq == null) throw new MemgresException("relation \"" + seqName + "\" does not exist", "42P01");
                Long drawn = executor.sessionSequenceValues.get(seq.qualifiedName().toLowerCase());
                if (drawn == null) {
                    throw new MemgresException("currval of sequence \"" + seq.getName()
                            + "\" is not yet defined in this session", "55000");
                }
                return drawn;
            }
            case "lastval": {
                if (executor.lastSequenceValue == null) {
                    throw new MemgresException(
                            "lastval is not yet defined in this session", "55000");
                }
                return executor.lastSequenceValue;
            }
            case "pg_sequence_last_value": {
                // Returns the last value allocated by the sequence, or NULL if never used.
                Object seqArg = executor.evalExpr(fn.args().get(0), ctx);
                String seqName;
                if (seqArg instanceof RegclassValue) {
                    seqName = ((RegclassValue) seqArg).name();
                } else if (seqArg instanceof Number) {
                    int targetOid = ((Number) seqArg).intValue();
                    seqName = null;
                    for (Map.Entry<String, Sequence> entry : executor.database.getSequences().entrySet()) {
                        int seqOid = executor.systemCatalog.getOid("rel:" + entry.getKey());
                        if (seqOid == targetOid) { seqName = entry.getKey(); break; }
                    }
                    if (seqName == null) seqName = String.valueOf(seqArg);
                } else {
                    seqName = String.valueOf(seqArg);
                }
                Sequence seq = resolveSequence(seqName);
                if (seq == null) throw new MemgresException("relation \"" + seqName + "\" does not exist", "42P01");
                try { return seq.currVal(); }
                catch (Exception e) { return null; } // never been used -> null
            }
            case "setval": {
                String seqName = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                Sequence seq = resolveSequence(seqName);
                if (seq == null) throw new MemgresException("relation \"" + seqName + "\" does not exist", "42P01");
                Object rawVal = executor.evalExpr(fn.args().get(1), ctx);
                if (rawVal == null) return null; // PG treats setval(seq, NULL) as a no-op returning NULL
                long val = executor.toLong(rawVal);
                long result;
                boolean marksCurrval = true;
                if (fn.args().size() > 2) {
                    boolean isCalled = executor.isTruthy(executor.evalExpr(fn.args().get(2), ctx));
                    result = seq.setVal(val, isCalled);
                    marksCurrval = isCalled;
                } else {
                    result = seq.setVal(val);
                }
                // setval defines currval for the session that called it, but only when the
                // value counts as already used.
                if (marksCurrval) {
                    executor.sessionSequenceValues.put(seq.qualifiedName().toLowerCase(), val);
                }
                // Also sync the table's serial counter if this sequence matches tableName_colName_seq
                // This ensures GENERATED ALWAYS AS IDENTITY / SERIAL columns pick up the new value
                if (seqName.contains("_seq")) {
                    String prefix = seqName.substring(0, seqName.lastIndexOf("_seq"));
                    int lastUnderscore = prefix.lastIndexOf('_');
                    if (lastUnderscore > 0) {
                        String tblName = prefix.substring(0, lastUnderscore);
                        for (Schema schema : executor.database.getSchemas().values()) {
                            Table tbl = schema.getTable(tblName);
                            if (tbl != null) {
                                tbl.resetSerialCounter(val + 1); // +1 because setval sets the last returned value
                                break;
                            }
                        }
                    }
                }
                return result;
            }
            case "coalesce": {
                // PG validates type compatibility at plan time before short-circuit evaluation.
                // Check for json vs jsonb type mismatch (PG rejects mixing these)
                boolean hasJsonFunc = false, hasJsonbCast = false;
                for (Expression arg : fn.args()) {
                    if (arg instanceof FunctionCallExpr) {
                        String fname = ((FunctionCallExpr) arg).name().toLowerCase();
                        if (fname.equals("json_arrayagg") || fname.equals("json_objectagg")
                                || fname.equals("json_array_constructor") || fname.equals("json_object_constructor")) {
                            hasJsonFunc = true;
                        }
                    }
                    if (arg instanceof CastExpr) {
                        String targetType = ((CastExpr) arg).typeName().toLowerCase();
                        if (targetType.equals("jsonb")) hasJsonbCast = true;
                    }
                }
                if (hasJsonFunc && hasJsonbCast) {
                    throw new MemgresException("could not convert type jsonb to json", "42846");
                }
                // Check for obvious mismatches: numeric literal mixed with non-numeric string literal.
                boolean hasNum = false, hasBadStr = false;
                String badVal = null;
                for (Expression arg : fn.args()) {
                    if (arg instanceof Literal) {
                        Literal lit = (Literal) arg;
                        if (lit.literalType() == Literal.LiteralType.INTEGER || lit.literalType() == Literal.LiteralType.FLOAT) {
                            hasNum = true;
                        } else if (lit.literalType() == Literal.LiteralType.STRING) {
                            try { new java.math.BigDecimal(lit.value()); } catch (NumberFormatException e) {
                                hasBadStr = true; badVal = lit.value();
                            }
                        }
                    }
                }
                if (hasNum && hasBadStr) {
                    throw new MemgresException("invalid input syntax for type integer: \"" + badVal + "\"", "22P02");
                }
                // PG uses short-circuit evaluation: stop at first non-null, don't evaluate remaining args.
                // This means `COALESCE(1, 1/0)` returns 1, not a division-by-zero error.
                Object result = null;
                for (Expression arg : fn.args()) {
                    Object v = executor.evalExpr(arg, ctx);
                    if (v != null) { result = v; break; }
                }
                return result;
            }
            case "nullif": {
                // NULLIF is an "=" written another way, so the operator is resolved from the
                // declared types before anything is evaluated: NULLIF(1, '2'::text) has no
                // integer = text to resolve to and is an error, not the 1 it used to return.
                executor.binaryOpEvaluator.rejectUnresolvableOperator(
                        new BinaryExpr(fn.args().get(0), BinaryExpr.BinOp.EQUAL, fn.args().get(1)), ctx);
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                // NULL is never equal to anything, so PG hands the first argument straight back.
                // Standing in an empty string for the missing value instead made NULLIF(1, NULL)
                // read "" as an integer and fail on input the query never contained.
                if (a == null) return null;
                if (b == null) return a;
                validateHomogeneousTypes(Cols.listOf(a, b), "NULLIF");
                Object left = a, right = b;
                // NULLIF is defined as a "=" between its arguments, and PG resolves that operator
                // from the declared types: a bare literal is unknown and is read as the other
                // side's type. Comparing the raw values instead left NULLIF(1, '1') at 1.
                if (isUntypedStringLiteral(fn.args().get(1)) && !isUntypedStringLiteral(fn.args().get(0))) {
                    right = readUntypedLiteralAs(right, left);
                } else if (isUntypedStringLiteral(fn.args().get(0)) && !isUntypedStringLiteral(fn.args().get(1))) {
                    left = readUntypedLiteralAs(left, right);
                }
                if (left instanceof Number && right instanceof Number) {
                    // The numeric types share one comparison in PG, so 1 and 1.0 are equal there.
                    return TypeCoercion.compare(left, right) == 0 ? null : a;
                }
                return Objects.equals(left, right) ? null : a;
            }
            case "greatest": {
                if (fn.args().isEmpty()) throw new MemgresException("syntax error at or near \")\"", "42601");
                List<Object> vals = new ArrayList<>();
                for (Expression arg : fn.args()) vals.add(executor.evalExpr(arg, ctx));
                validateHomogeneousTypes(vals, "GREATEST");
                readUntypedLiteralsAsSettledType(fn.args(), vals);
                Object max = null;
                for (Object val : vals) {
                    if (val != null && (max == null || executor.compareValues(val, max) > 0)) max = val;
                }
                return max;
            }
            case "least": {
                if (fn.args().isEmpty()) throw new MemgresException("syntax error at or near \")\"", "42601");
                List<Object> vals = new ArrayList<>();
                for (Expression arg : fn.args()) vals.add(executor.evalExpr(arg, ctx));
                validateHomogeneousTypes(vals, "LEAST");
                readUntypedLiteralsAsSettledType(fn.args(), vals);
                Object min = null;
                for (Object val : vals) {
                    if (val != null && (min == null || executor.compareValues(val, min) < 0)) min = val;
                }
                return min;
            }
            case "num_nulls": {
                int count = 0;
                for (Expression arg : fn.args()) {
                    if (executor.evalExpr(arg, ctx) == null) count++;
                }
                return count;
            }
            case "num_nonnulls": {
                int count = 0;
                for (Expression arg : fn.args()) {
                    if (executor.evalExpr(arg, ctx) != null) count++;
                }
                return count;
            }
            case "age":
            case "date_part":
            case "extract":
            case "date_trunc":
            case "make_date":
            case "make_time":
            case "make_timestamp":
            case "make_timestamptz":
            case "make_interval":
            case "clock_timestamp":
            case "statement_timestamp":
            case "transaction_timestamp":
            case "timeofday":
            case "to_char":
            case "to_date":
            case "to_timestamp":
            case "to_number":
            case "justify_hours":
            case "justify_days":
            case "justify_interval":
            case "isfinite":
            case "date_bin":
                return dateTimeFunctions.eval(name, fn, ctx);
            case "timezone": {
                // timezone(zone, timestamp) → applies timezone conversion
                // Equivalent to: timestamp AT TIME ZONE zone
                requireArgs(fn, 2);
                Object zoneArg = executor.evalExpr(fn.args().get(0), ctx);
                Object tsArg = executor.evalExpr(fn.args().get(1), ctx);
                if (tsArg == null) return null;
                String zoneName = zoneArg != null ? zoneArg.toString() : "UTC";
                java.time.ZoneId zid;
                try {
                    zid = java.time.ZoneId.of(zoneName);
                } catch (java.time.DateTimeException e) {
                    throw new MemgresException("time zone \"" + zoneName + "\" not recognized", "22023");
                }
                if (tsArg instanceof OffsetDateTime) {
                    return ((OffsetDateTime) tsArg).atZoneSameInstant(zid).toLocalDateTime();
                } else if (tsArg instanceof LocalDateTime) {
                    return ((LocalDateTime) tsArg).atZone(zid).toOffsetDateTime();
                } else if (tsArg instanceof LocalTime) {
                    return tsArg;
                }
                return tsArg;
            }
            case "array_length": {
                requireDeclaredArrayArg(fn.args().get(0));
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr == null) return null;
                if (!(arr instanceof List<?>) && !isPgArrayText(arr)) {
                    String typeName = arr instanceof Number ? "integer" : "text";
                    throw new MemgresException("function array_length(" + typeName + ", integer) does not exist", "42883");
                }
                int dim = fn.args().size() > 1 ? executor.toInt(executor.evalExpr(fn.args().get(1), ctx)) : 1;
                if (dim < 1) return null; // dimension 0 doesn't exist
                // A 0-based vector always has one dimension, even with nothing in it: PG stores
                // an empty oidvector as [0:-1] rather than as a dimensionless empty array, so
                // array_length answers 0 where a plain empty array answers NULL.
                if (arr instanceof PgVector) return dim == 1 ? (Object) ((PgVector) arr).size() : null;
                if (arr instanceof List<?>) {
                    List<?> list = (List<?>) arr;
                    if (dim == 1) return list.isEmpty() ? null : list.size();
                    // Dimension 2+: check if elements are sub-arrays
                    if (dim == 2 && !list.isEmpty() && list.get(0) instanceof List<?>) return ((List<?>) list.get(0)).size();
                    return null; // dimension doesn't exist for this array
                }
                // Handle PostgreSQL array string format: {val1,val2,...}, with the optional
                // "[lb:ub]=" prefix an array with a non-default lower bound is written with.
                if (isPgArrayText(arr)) {
                    String s = pgArrayBody(arr.toString());
                    if (dim != 1) return null;
                    String inner = s.substring(1, s.length() - 1).trim();
                    if (inner.isEmpty()) return null; // PG returns NULL for empty arrays
                    return countArrayElements(inner);
                }
                return null;
            }
            case "array_upper": {
                requireDeclaredArrayArg(fn.args().get(0));
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                int dim2 = fn.args().size() > 1 ? executor.toInt(executor.evalExpr(fn.args().get(1), ctx)) : 1;
                if (dim2 < 1) return null;
                int[][] upperBounds = arr instanceof String ? ArrayLiteral.statedBounds((String) arr) : null;
                if (upperBounds != null) {
                    return dim2 <= upperBounds[1].length ? (Object) upperBounds[1][dim2 - 1] : null;
                }
                // A 0-based vector runs from 0, so its upper bound is one less than its length —
                // and -1 when it is empty, which is the bound PG reports for an empty oidvector.
                if (arr instanceof PgVector) return dim2 == 1 ? (Object) (((PgVector) arr).size() - 1) : null;
                if (arr instanceof List<?>) {
                    List<?> list = (List<?>) arr;
                    // PG returns NULL for empty arrays (they have no dimensions)
                    if (list.isEmpty()) return null;
                    if (dim2 == 1) return list.size();
                    if (dim2 == 2 && list.get(0) instanceof List<?>) return ((List<?>) list.get(0)).size();
                    return null;
                }
                if (arr instanceof String && ((String) arr).startsWith("{") && ((String) arr).endsWith("}")) {
                    String s = (String) arr;
                    if (dim2 != 1) return null;
                    String inner = s.substring(1, s.length() - 1).trim();
                    if (inner.isEmpty()) return null; // PG returns NULL for empty arrays
                    return countArrayElements(inner);
                }
                return null;
            }
            case "array_lower": {
                requireDeclaredArrayArg(fn.args().get(0));
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                int dim2 = fn.args().size() > 1 ? executor.toInt(executor.evalExpr(fn.args().get(1), ctx)) : 1;
                if (dim2 < 1) return null;
                int[][] lowerBounds = arr instanceof String ? ArrayLiteral.statedBounds((String) arr) : null;
                if (lowerBounds != null) {
                    return dim2 <= lowerBounds[0].length ? (Object) lowerBounds[0][dim2 - 1] : null;
                }
                // int2vector and oidvector are subscripted from 0, empty or not.
                if (arr instanceof PgVector) return dim2 == 1 ? (Object) 0 : null;
                if (arr instanceof List<?> && !((List<?>) arr).isEmpty()) {
                    List<?> list = (List<?>) arr;
                    if (dim2 == 1) return 1;
                    if (dim2 == 2 && !list.isEmpty() && list.get(0) instanceof List<?>) return 1;
                    return null;
                }
                if (arr instanceof String && ((String) arr).startsWith("{") && ((String) arr).endsWith("}")) {
                    String s = (String) arr;
                    if (dim2 != 1) return null;
                    String inner = s.substring(1, s.length() - 1).trim();
                    if (!inner.isEmpty()) return 1;
                    return null;
                }
                return null;
            }
            case "array_ndims": {
                requireDeclaredArrayArg(fn.args().get(0));
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr == null) return null;
                int[][] ndimsBounds = arr instanceof String ? ArrayLiteral.statedBounds((String) arr) : null;
                if (ndimsBounds != null) return ndimsBounds[0].length;
                String s = arr instanceof String ? (String) arr : TypeCoercion.formatPgArray(arr instanceof List<?> ? (List<?>) arr : Cols.listOf(arr));
                if (!s.startsWith("{")) return null;
                int dims = 0;
                for (int ci = 0; ci < s.length(); ci++) {
                    if (s.charAt(ci) == '{') dims++;
                    else break;
                }
                return dims;
            }
            case "array_fill": {
                // array_fill is polymorphic in its first argument, so that argument has to have a
                // type of its own -- a bare literal or NULL names none and PG cannot pick one.
                if (fn.args().get(0) instanceof Literal) {
                    Literal.LiteralType lt = ((Literal) fn.args().get(0)).literalType();
                    if (lt == Literal.LiteralType.STRING || lt == Literal.LiteralType.NULL) {
                        throw new MemgresException("could not determine polymorphic type because"
                                + " input has type unknown", "42804");
                    }
                }
                Object fillVal = executor.evalExpr(fn.args().get(0), ctx);
                Object dimsArg = executor.evalExpr(fn.args().get(1), ctx);
                // Neither the dimensions nor the lower bounds are strict arguments: a null one is
                // an error rather than a null result, because there is no array to build from it.
                if (dimsArg == null) throw new MemgresException(
                        "dimension array or low bound array cannot be null", "22004");
                List<?> dimsList = arrayFillBounds(dimsArg);
                if (dimsList == null) return null;
                List<?> lbList = null;
                if (fn.args().size() > 2) {
                    Object lbArg = executor.evalExpr(fn.args().get(2), ctx);
                    if (lbArg == null) throw new MemgresException(
                            "dimension array or low bound array cannot be null", "22004");
                    lbList = arrayFillBounds(lbArg);
                    if (lbList == null) return null;
                    if (lbList.size() != dimsList.size()) {
                        throw new MemgresException("wrong number of array subscripts", "2202E");
                    }
                }
                if (dimsList.size() > 6) {
                    throw new MemgresException("number of array dimensions (" + dimsList.size()
                            + ") exceeds the maximum allowed (6)", "54000");
                }
                for (int di = 0; di < dimsList.size(); di++) {
                    if (dimsList.get(di) == null || (lbList != null && lbList.get(di) == null)) {
                        throw new MemgresException("dimension values cannot be null", "22004");
                    }
                    // A negative extent is not an empty array: PG computes the element count from
                    // it and reports the size check that count fails.
                    if (((Number) dimsList.get(di)).intValue() < 0) {
                        throw new MemgresException(
                                "array size exceeds the maximum allowed (134217727)", "54000");
                    }
                }
                if (dimsList.isEmpty()) return "{}";
                // An array has a size past which PostgreSQL will not build one, and the extents
                // are asked for rather than accumulated: a request for four hundred million
                // elements is refused before anything is allocated for it, instead of taking the
                // heap with it.
                long requested = 1;
                for (int di = 0; di < dimsList.size(); di++) {
                    requested *= ((Number) dimsList.get(di)).intValue();
                    if (requested > MAX_ARRAY_ELEMENTS) {
                        throw new MemgresException("array size exceeds the maximum allowed ("
                                + MAX_ARRAY_ELEMENTS + ")", "54000");
                    }
                }
                String filled = buildFilledArray(fillVal, dimsList, 0);
                StringBuilder prefix = new StringBuilder();
                boolean customBounds = false;
                for (int di = 0; di < dimsList.size(); di++) {
                    int lb = lbList == null ? 1 : ((Number) lbList.get(di)).intValue();
                    if (lb != 1) customBounds = true;
                    int ub = lb + ((Number) dimsList.get(di)).intValue() - 1;
                    prefix.append("[").append(lb).append(":").append(ub).append("]");
                }
                // The bounds prefix is only written when a bound is not the default 1
                return customBounds ? prefix.append("=").append(filled).toString() : filled;
            }
            case "trim_array": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                Object nObj = executor.evalExpr(fn.args().get(1), ctx);
                if (arr == null || nObj == null) return null;
                int n = ((Number) nObj).intValue();
                List<Object> list;
                if (arr instanceof List<?>) list = new java.util.ArrayList<>((List<?>) arr);
                else if (arr instanceof String && ((String) arr).startsWith("{")) list = new java.util.ArrayList<>(parseSimplePgArray((String) arr));
                else return arr;
                if (n < 0 || n > list.size()) throw new MemgresException("number of elements to trim must be between 0 and " + list.size(), "2202E");
                list = list.subList(0, list.size() - n);
                return TypeCoercion.formatPgArray(list);
            }
            case "array_dims": {
                requireDeclaredArrayArg(fn.args().get(0));
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr == null) return null;
                int[][] statedDims = arr instanceof String ? ArrayLiteral.statedBounds((String) arr) : null;
                if (statedDims != null) {
                    StringBuilder stated = new StringBuilder();
                    for (int di = 0; di < statedDims[0].length; di++) {
                        stated.append('[').append(statedDims[0][di]).append(':')
                                .append(statedDims[1][di]).append(']');
                    }
                    return stated.toString();
                }
                List<?> list = null;
                if (arr instanceof List<?>) list = (List<?>) arr;
                else if (arr instanceof String && ((String) arr).startsWith("{")) {
                    String s = (String) arr;
                    list = parseSimplePgArray(s);
                }
                if (list == null || list.isEmpty()) return null;
                StringBuilder dims = new StringBuilder("[1:" + list.size() + "]");
                // Check for multi-dimensional
                if (!list.isEmpty() && list.get(0) instanceof List<?>) {
                    dims.append("[1:").append(((List<?>) list.get(0)).size()).append("]");
                }
                return dims.toString();
            }
            case "array_sort": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr == null) return null;
                if (arr instanceof Number || arr instanceof Boolean) {
                    throw new MemgresException("function array_sort(integer) does not exist", "42883");
                }
                List<Object> list;
                if (arr instanceof List<?>) list = new ArrayList<>((List<?>) arr);
                else if (arr instanceof String && ((String) arr).startsWith("{")) list = new ArrayList<>(parseSimplePgArray(((String) arr)));
                else return arr;
                // array_sort(a, descending, nulls_first): the second argument says which way
                // round, and the third where the nulls go -- by default the way ORDER BY puts
                // them, last when ascending and first when descending. Reading neither sorted
                // every call ascending, whatever it was asked for.
                final boolean descending = fn.args().size() > 1
                        && executor.isTruthy(executor.evalExpr(fn.args().get(1), ctx));
                final boolean nullsFirst = fn.args().size() > 2
                        ? executor.isTruthy(executor.evalExpr(fn.args().get(2), ctx)) : descending;
                list.sort(new java.util.Comparator<Object>() {
                    @Override
                    public int compare(Object a, Object b) {
                        if (a == null || b == null) {
                            if (a == null && b == null) return 0;
                            return (a == null) == nullsFirst ? -1 : 1;
                        }
                        int cmp = TypeCoercion.compare(a, b);
                        return descending ? -cmp : cmp;
                    }
                });
                return TypeCoercion.formatPgArray(list);
            }
            case "array_reverse": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr == null) return null;
                List<Object> list;
                if (arr instanceof List<?>) list = new ArrayList<>((List<?>) arr);
                else if (arr instanceof String && ((String) arr).startsWith("{")) list = new ArrayList<>(parseSimplePgArray(((String) arr)));
                else return arr;
                java.util.Collections.reverse(list);
                return TypeCoercion.formatPgArray(list);
            }
            case "array_to_string": {
                requireDeclaredArrayArg(fn.args().get(0));
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                Object delim = executor.evalExpr(fn.args().get(1), ctx);
                // PG is strict on the delimiter: a NULL there makes the whole call NULL.
                if (delim == null) return null;
                // Handle string-formatted arrays like {a,b,c} (quote-aware, nested arrays flattened)
                if (arr instanceof String && ((String) arr).startsWith("{") && ((String) arr).endsWith("}")) {
                    String s = (String) arr;
                    String inner = s.substring(1, s.length() - 1);
                    if (inner.isEmpty()) return "";
                    arr = flattenArray(parseSimplePgArray(s));
                }
                if (arr instanceof List<?>) {
                    List<?> list = (List<?>) arr;
                    Object nullStr = fn.args().size() > 2
                            ? executor.evalExpr(fn.args().get(2), ctx) : null;
                    // A NULL element is skipped entirely — separator included — unless a
                    // replacement text was supplied.
                    StringBuilder sb = new StringBuilder();
                    boolean first = true;
                    for (Object elem : list) {
                        String rendered;
                        if (elem != null) {
                            // An element is written out as it is held. Trimming it dropped the
                            // spaces a text element really has, and the blanks a bpchar element
                            // is declared with — neither of which the array stopped holding.
                            rendered = elem.toString();
                        } else if (nullStr != null) {
                            rendered = nullStr.toString();
                        } else {
                            continue;
                        }
                        if (!first) sb.append(delim);
                        first = false;
                        sb.append(rendered);
                    }
                    return sb.toString();
                }
                return null;
            }
            case "cardinality": {
                requireDeclaredArrayArg(fn.args().get(0));
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr instanceof List<?>) return countLeafElements((List<?>) arr);
                if (arr != null) {
                    String s = pgArrayBody(arr.toString());
                    if (s.equals("{}")) return 0;
                    if (s.startsWith("{") && s.endsWith("}")) {
                        // Quote-aware count (commas inside quoted elements are not separators)
                        return countLeafElements(parseSimplePgArray(s));
                    }
                }
                return null;
            }
            // The function spellings of the ?| and ?& operators. PG exposes both, and code that
            // builds SQL from a query builder tends to write the function rather than an operator
            // the driver's parameter placeholder syntax collides with.
            case "jsonb_exists_any":
            case "jsonb_exists_all": {
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                Object keysArg = executor.evalExpr(fn.args().get(1), ctx);
                if (json == null || keysArg == null) return null;
                List<String> keys = jsonbKeyArg(keysArg);
                if (json instanceof HstoreValue) {
                    Map<String, String> data = ((HstoreValue) json).getData();
                    for (String key : keys) {
                        boolean present = key != null && data.containsKey(key);
                        if (name.endsWith("any") && present) return true;
                        if (name.endsWith("all") && !present) return false;
                    }
                    return !name.endsWith("any");
                }
                return name.endsWith("any")
                        ? JsonOperations.anyKeyExists(json.toString(), keys)
                        : JsonOperations.allKeysExist(json.toString(), keys);
            }
            case "unnest": {
                if (fn.args().isEmpty()) {
                    throw new MemgresException("function unnest() does not exist\n  Hint: No function matches the given name and argument types.", "42883");
                }
                // The many-argument form of unnest exists only as a FROM item -- it produces a
                // row of several columns, which a select-list expression has no room for, and
                // PostgreSQL has no such function to call there. Dropping the extra arguments
                // answered the one-argument call to a query that did not write one.
                if (fn.args().size() > 1) throw noMultiArgUnnest(fn, ctx);
                // unnest returns set; expand array into individual elements as a List
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr instanceof TsVector) {
                    TsVector tv = (TsVector) arr;
                    List<Object[]> rows = TextSearchOperations.unnestTsVector(tv);
                    StringBuilder sb = new StringBuilder();
                    for (Object[] row : rows) {
                        if (sb.length() > 0) sb.append(",");
                        sb.append("(").append(row[0]).append(",").append(row[1]).append(",").append(row[2]).append(")");
                    }
                    return "(" + sb + ")";
                }
                if (arr instanceof List<?>) {
                    // PG unnest fully flattens multidimensional arrays into scalar elements
                    return flattenArray((List<?>) arr); // Return as List for SRF expansion
                }
                // Multirange unnest: expand into individual range strings
                if (arr instanceof String && RangeOperations.isMultirangeOrEmpty(((String) arr))) {
                    java.util.List<RangeOperations.PgRange> ranges = RangeOperations.parseMultirange(((String) arr));
                    List<Object> result = new ArrayList<>();
                    for (RangeOperations.PgRange r : ranges) {
                        result.add(r.toString());
                    }
                    return result;
                }
                if (arr instanceof String && ((String) arr).startsWith("{") && ((String) arr).endsWith("}")) {
                    String s = (String) arr;
                    List<Object> parsed = flattenArray(parseSimplePgArray(s));
                    // If this is an enum array, wrap elements as PgEnum for ordinal-based sorting
                    String enumTypeName = resolveEnumTypeFromArrayArg(fn.args().get(0), ctx);
                    if (enumTypeName != null) {
                        CustomEnum ce = executor.database.getCustomEnum(enumTypeName);
                        if (ce != null) {
                            List<Object> enumList = new ArrayList<>();
                            for (Object o : parsed) {
                                String label = o.toString();
                                if (ce.isValidLabel(label)) {
                                    enumList.add(new AstExecutor.PgEnum(label, enumTypeName, ce.ordinal(label)));
                                } else {
                                    enumList.add(o);
                                }
                            }
                            return enumList;
                        }
                    }
                    return parsed;
                }
                // unnest on non-array type should error
                if (arr instanceof Number || arr instanceof Boolean) {
                    throw new MemgresException("function unnest(" +
                            (arr instanceof Integer ? "integer" : arr instanceof Long ? "bigint" : "unknown") +
                            ") does not exist", "42883");
                }
                return arr;
            }
            case "aclexplode": {
                // aclexplode(aclitem[]) -> SETOF record(grantor oid, grantee oid, privilege_type text, is_grantable boolean)
                // Returns empty set for NULL input (which is the common case since Memgres doesn't implement column-level ACLs)
                Object acl = executor.evalExpr(fn.args().get(0), ctx);
                if (acl == null) {
                    return new ArrayList<>(); // NULL ACL = no privileges = 0 rows
                }
                // Parse ACL strings like "{postgres=arwdDxt/postgres,=r/postgres}"
                List<Object> rows = new ArrayList<>();
                String aclStr = acl.toString().trim();
                if (aclStr.startsWith("{")) aclStr = aclStr.substring(1);
                if (aclStr.endsWith("}")) aclStr = aclStr.substring(0, aclStr.length() - 1);
                if (aclStr.isEmpty()) {
                    return rows;
                }
                for (String item : aclStr.split(",")) {
                    item = item.trim();
                    if (item.isEmpty()) continue;
                    // Format: grantee=privs/grantor  (empty grantee = PUBLIC)
                    int eqIdx = item.indexOf('=');
                    int slashIdx = item.indexOf('/');
                    if (eqIdx < 0 || slashIdx < 0) continue;
                    String granteeStr = item.substring(0, eqIdx);
                    String privs = item.substring(eqIdx + 1, slashIdx);
                    String grantorStr = item.substring(slashIdx + 1);
                    long grantorOid = 10; // default superuser OID
                    long granteeOid = granteeStr.isEmpty() ? 0 : 10;
                    for (int i = 0; i < privs.length(); i++) {
                        char c = privs.charAt(i);
                        boolean grantable = (i + 1 < privs.length() && privs.charAt(i + 1) == '*');
                        String privType;
                        switch (c) {
                            case 'r':
                                privType = "SELECT";
                                break;
                            case 'w':
                                privType = "UPDATE";
                                break;
                            case 'a':
                                privType = "INSERT";
                                break;
                            case 'd':
                                privType = "DELETE";
                                break;
                            case 'D':
                                privType = "TRUNCATE";
                                break;
                            case 'x':
                                privType = "REFERENCES";
                                break;
                            case 't':
                                privType = "TRIGGER";
                                break;
                            case 'X':
                                privType = "EXECUTE";
                                break;
                            case 'U':
                                privType = "USAGE";
                                break;
                            case 'C':
                                privType = "CREATE";
                                break;
                            case 'c':
                                privType = "CONNECT";
                                break;
                            case 'T':
                                privType = "TEMPORARY";
                                break;
                            case '*':
                                privType = null;
                                break;
                            default:
                                privType = null;
                                break;
                        }
                        if (privType != null) {
                            // Return as composite row: (grantor, grantee, privilege_type, is_grantable)
                            rows.add("(" + grantorOid + "," + granteeOid + "," + privType + "," + grantable + ")");
                        }
                    }
                }
                return rows;
            }
            // UPDATE t SET a[lo:hi] = value — a slice only ever names part of an array.
            case "__array_assign_slice__": {
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                int lo = executor.toInt(executor.evalExpr(fn.args().get(1), ctx));
                int hi = executor.toInt(executor.evalExpr(fn.args().get(2), ctx));
                Object value = executor.evalExpr(fn.args().get(3), ctx);
                return executor.arrayOperationHandler.assignSlice(arr, lo, hi, value);
            }
            // UPDATE t SET a[k] = value — an array index or a JSON key, told apart by the
            // column's declared type rather than by how the subscript happens to be written.
            case "__subscript_assign__": {
                Expression target = fn.args().get(0);
                DataType arrayType = arrayColumnType(target, ctx);
                if (arrayType != null) {
                    Object arr = executor.evalExpr(target, ctx);
                    Object value = executor.evalExpr(fn.args().get(3), ctx);
                    Object keyVal = executor.evalExpr(fn.args().get(4), ctx);
                    // The element still has to fit the array's element type, as it would on insert.
                    DataType elemType = elementTypeOf(arrayType);
                    if (elemType != null) value = TypeCoercion.coerce(value, elemType);
                    return executor.arrayOperationHandler.assignElement(
                            arr, executor.toInt(keyVal), value);
                }
                return evalFunction(new FunctionCallExpr("jsonb_set",
                        Cols.listOf(target, fn.args().get(1), fn.args().get(2))), ctx);
            }
            case "array_cat": {
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                Object aArr = readArrayOperand(a, elementTypeNameOfArray(b));
                Object bArr = readArrayOperand(b, elementTypeNameOfArray(a));
                List<Object> result = new ArrayList<>();
                if (aArr instanceof List<?>) result.addAll((List<?>) aArr);
                if (bArr instanceof List<?>) result.addAll((List<?>) bArr);
                return result;
            }
            case "array_append": {
                Object arr = readArrayOperand(executor.evalExpr(fn.args().get(0), ctx),
                        elementTypeNameOf(executor.evalExpr(fn.args().get(1), ctx)));
                Object elem = executor.evalExpr(fn.args().get(1), ctx);
                // Type compatibility check: if array has numeric elements, reject text element
                if (arr instanceof List<?> && !((List<?>) arr).isEmpty() && elem != null) {
                    List<?> la = (List<?>) arr;
                    Object first = la.stream().filter(java.util.Objects::nonNull).findFirst().orElse(null);
                    if (first instanceof Number && elem instanceof String && !(elem instanceof Number)) {
                        try { Double.parseDouble(elem.toString()); } catch (NumberFormatException e) {
                            throw new MemgresException("invalid input syntax for type integer: \"" + elem + "\"", "22P02");
                        }
                    }
                }
                List<Object> result = new ArrayList<>();
                if (arr instanceof List<?>) result.addAll((List<?>) arr);
                result.add(elem);
                return result;
            }
            case "array_prepend": {
                Object elem = executor.evalExpr(fn.args().get(0), ctx);
                Object arr = readArrayOperand(executor.evalExpr(fn.args().get(1), ctx),
                        elementTypeNameOf(elem));
                List<Object> result = new ArrayList<>();
                result.add(elem);
                if (arr instanceof List<?>) result.addAll((List<?>) arr);
                return result;
            }
            case "array_remove": {
                Object elem = executor.evalExpr(fn.args().get(1), ctx);
                Object arr = readArrayOperand(executor.evalExpr(fn.args().get(0), ctx),
                        elementTypeNameOf(elem));
                if (arr == null) return null;
                List<Object> result = new ArrayList<>();
                if (arr instanceof List<?>) {
                    for (Object o : (List<?>) arr) {
                        if (!TypeCoercion.areEqual(o, elem)) result.add(o);
                    }
                }
                return result;
            }
            case "array_position": {
                Object elem = executor.evalExpr(fn.args().get(1), ctx);
                Object arr = readArrayOperand(executor.evalExpr(fn.args().get(0), ctx),
                        elementTypeNameOf(elem));
                if (arr == null) return null;
                int startPos = 1;
                if (fn.args().size() > 2) {
                    Object startArg = executor.evalExpr(fn.args().get(2), ctx);
                    if (startArg != null) startPos = ((Number) startArg).intValue();
                }
                if (arr instanceof List<?>) {
                    List<?> la = (List<?>) arr;
                    for (int ai = Math.max(startPos - 1, 0); ai < la.size(); ai++) {
                        if (TypeCoercion.areEqual(la.get(ai), elem)) return ai + 1; // 1-based
                    }
                }
                return null;
            }
            case "array_positions": {
                Object elem = executor.evalExpr(fn.args().get(1), ctx);
                Object arr = readArrayOperand(executor.evalExpr(fn.args().get(0), ctx),
                        elementTypeNameOf(elem));
                if (arr == null) return null;
                List<Object> positions = new ArrayList<>();
                if (arr instanceof List<?>) {
                    List<?> la = (List<?>) arr;
                    for (int ai = 0; ai < la.size(); ai++) {
                        if (TypeCoercion.areEqual(la.get(ai), elem)) positions.add(ai + 1);
                    }
                }
                return positions;
            }
            case "array_replace": {
                Object oldVal = executor.evalExpr(fn.args().get(1), ctx);
                Object arr = readArrayOperand(executor.evalExpr(fn.args().get(0), ctx),
                        elementTypeNameOf(oldVal));
                if (arr == null) return null;
                Object newVal = executor.evalExpr(fn.args().get(2), ctx);
                List<Object> result = new ArrayList<>();
                if (arr instanceof List<?>) {
                    for (Object o : (List<?>) arr) {
                        result.add(TypeCoercion.areEqual(o, oldVal) ? newVal : o);
                    }
                }
                return result;
            }
            case "arraycontains":
            case "arraycontained":
            case "arrayoverlap": {
                requireArgs(fn, 2);
                Object leftVal = executor.evalExpr(fn.args().get(0), ctx);
                Object rightVal = executor.evalExpr(fn.args().get(1), ctx);
                Object left = readArrayOperand(leftVal, elementTypeNameOfArray(rightVal));
                Object right = readArrayOperand(rightVal, elementTypeNameOfArray(leftVal));
                if (left == null || right == null) return null;
                List<?> la = asElementList(left, name);
                List<?> ra = asElementList(right, name);
                if (name.equals("arrayoverlap")) return BinaryOpEvaluator.arrayOverlaps(la, ra);
                return name.equals("arraycontains")
                        ? BinaryOpEvaluator.arrayContainsAll(la, ra)
                        : BinaryOpEvaluator.arrayContainsAll(ra, la);
            }
            case "__similar_to_escape__": {
                // SIMILAR TO with ESCAPE: __similar_to_escape__(str, pattern, escape_char)
                requireArgs(fn, 3);
                Object strVal = executor.evalExpr(fn.args().get(0), ctx);
                Object patVal = executor.evalExpr(fn.args().get(1), ctx);
                Object escVal = executor.evalExpr(fn.args().get(2), ctx);
                if (strVal == null || patVal == null) return null;
                String str = strVal.toString();
                String pat = patVal.toString();
                String esc = escVal != null ? escVal.toString() : "";
                // If escape is empty, disable escaping; convert SIMILAR TO pattern to regex
                String regex;
                if (esc.isEmpty()) {
                    // No escape character: convert % and _ but quote everything else for regex
                    StringBuilder sbNoEsc = new StringBuilder();
                    for (int ci = 0; ci < pat.length(); ci++) {
                        char ch = pat.charAt(ci);
                        if (ch == '%') {
                            sbNoEsc.append(".*");
                        } else if (ch == '_') {
                            sbNoEsc.append(".");
                        } else {
                            sbNoEsc.append(java.util.regex.Pattern.quote(String.valueOf(ch)));
                        }
                    }
                    regex = sbNoEsc.toString();
                } else {
                    char escChar = esc.charAt(0);
                    StringBuilder sb = new StringBuilder();
                    for (int ci = 0; ci < pat.length(); ci++) {
                        char ch = pat.charAt(ci);
                        if (ch == escChar && ci + 1 < pat.length()) {
                            char next = pat.charAt(ci + 1);
                            // Escaped character: emit literal
                            sb.append(java.util.regex.Pattern.quote(String.valueOf(next)));
                            ci++; // skip next
                        } else if (ch == escChar) {
                            // An escape with nothing left to escape is dropped, as PG does
                        } else if (ch == '%') {
                            sb.append(".*");
                        } else if (ch == '_') {
                            sb.append(".");
                        } else {
                            sb.append(ch);
                        }
                    }
                    regex = sb.toString();
                }
                return str.matches("(?s)" + regex);
            }
            case "overlaps": {
                // SQL OVERLAPS: (start1, end1_or_interval) OVERLAPS (start2, end2_or_interval) -> boolean
                // Each argument is a row constructor (ArrayExpr) with 2 elements
                if (fn.args().size() != 2)
                    throw new MemgresException("OVERLAPS requires exactly two range arguments", "42804");
                Expression leftExpr = fn.args().get(0);
                Expression rightExpr = fn.args().get(1);
                Object leftVal = executor.evalExpr(leftExpr, ctx);
                Object rightVal = executor.evalExpr(rightExpr, ctx);
                // Extract start/end from each side (PgRow or List). Any endpoint may be NULL,
                // which makes that end of the period unknown rather than absent.
                List<?> lv = extractRowValues(leftVal);
                List<?> rv = extractRowValues(rightVal);
                java.time.temporal.Temporal s1 = lv.get(0) == null ? null : toTemporal(lv.get(0));
                java.time.temporal.Temporal e1 = resolveOverlapEnd(s1, lv.get(1));
                java.time.temporal.Temporal s2 = rv.get(0) == null ? null : toTemporal(rv.get(0));
                java.time.temporal.Temporal e2 = resolveOverlapEnd(s2, rv.get(1));

                // Take the non-null endpoint as the start when only one is known; otherwise
                // order the pair. A period with neither endpoint known says nothing at all.
                if (s1 == null) {
                    if (e1 == null) return null;
                    s1 = e1;
                    e1 = null;
                } else if (e1 != null && compareTemporal(s1, e1) > 0) {
                    java.time.temporal.Temporal tmp = s1; s1 = e1; e1 = tmp;
                }
                if (s2 == null) {
                    if (e2 == null) return null;
                    s2 = e2;
                    e2 = null;
                } else if (e2 != null && compareTemporal(s2, e2) > 0) {
                    java.time.temporal.Temporal tmp = s2; s2 = e2; e2 = tmp;
                }

                // Both starts are now known, so they can be compared.
                int startCmp = compareTemporal(s1, s2);
                if (startCmp > 0) {
                    // The later-starting period overlaps only if it begins before the other ends.
                    if (e2 == null) return null;
                    if (compareTemporal(s1, e2) < 0) return true;
                    return e1 == null ? null : Boolean.FALSE;
                } else if (startCmp < 0) {
                    if (e1 == null) return null;
                    if (compareTemporal(s2, e1) < 0) return true;
                    return e2 == null ? null : Boolean.FALSE;
                } else {
                    // Equal starts overlap whenever both periods are fully known.
                    if (e1 == null || e2 == null) return null;
                    return true;
                }
            }
            case "array_sample": {
                // array_sample(arr, n) - returns n random elements from arr (PG 16+)
                // PG 18 only supports 2-arg form; 3-arg with seed does not exist
                if (fn.args().size() >= 3) {
                    throw new MemgresException(
                            "function array_sample(integer[], integer, integer) does not exist", "42883");
                }
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                Object nObj = executor.evalExpr(fn.args().get(1), ctx);
                if (arr == null) return null;
                int n = executor.toInt(nObj);
                List<Object> elements;
                if (arr instanceof List<?>) {
                    elements = new ArrayList<>((List<?>) arr);
                } else {
                    elements = new ArrayList<>(parseSimplePgArray(arr.toString()));
                }
                if (n <= 0) return TypeCoercion.formatPgArray(new ArrayList<>());
                if (n >= elements.size()) n = elements.size();
                java.util.Random rng = new java.util.Random();
                java.util.Collections.shuffle(elements, rng);
                return TypeCoercion.formatPgArray(new ArrayList<>(elements.subList(0, n)));
            }
            case "array_shuffle": {
                // array_shuffle(arr) - returns arr with elements in random order (PG 16+)
                Object arr = executor.evalExpr(fn.args().get(0), ctx);
                if (arr == null) return null;
                List<Object> elements;
                if (arr instanceof List<?>) {
                    elements = new ArrayList<>((List<?>) arr);
                } else {
                    elements = new ArrayList<>(parseSimplePgArray(arr.toString()));
                }
                java.util.Collections.shuffle(elements, new java.util.Random());
                return TypeCoercion.formatPgArray(elements);
            }
            case "enum_first": {
                String enumType = resolveEnumTypeFromArg(fn.args().get(0), ctx);
                if (enumType == null) return null;
                CustomEnum ce = executor.database.getCustomEnum(enumType);
                return ce == null ? null : ce.getLabels().get(0);
            }
            case "enum_last": {
                String enumType = resolveEnumTypeFromArg(fn.args().get(0), ctx);
                if (enumType == null) return null;
                CustomEnum ce = executor.database.getCustomEnum(enumType);
                return ce == null ? null : ce.getLabels().get(ce.getLabels().size() - 1);
            }
            case "enum_range": {
                if (fn.args().size() >= 2) {
                    // Bounded form: enum_range(from, to)
                    String enumType = resolveEnumTypeFromArg(fn.args().get(0), ctx);
                    if (enumType == null) enumType = resolveEnumTypeFromArg(fn.args().get(1), ctx);
                    if (enumType == null) return null;
                    CustomEnum ce = executor.database.getCustomEnum(enumType);
                    if (ce == null) return null;
                    Object fromVal = executor.evalExpr(fn.args().get(0), ctx);
                    Object toVal = executor.evalExpr(fn.args().get(1), ctx);
                    String fromStr = fromVal == null ? null : fromVal.toString();
                    String toStr = toVal == null ? null : toVal.toString();
                    java.util.List<String> labels = ce.getLabels();
                    int startIdx = fromStr == null ? 0 : labels.indexOf(fromStr);
                    int endIdx = toStr == null ? labels.size() - 1 : labels.indexOf(toStr);
                    if (startIdx < 0) startIdx = 0;
                    if (endIdx < 0) endIdx = labels.size() - 1;
                    java.util.List<String> range = labels.subList(startIdx, endIdx + 1);
                    return "{" + String.join(",", range) + "}";
                }
                // Unbounded form: enum_range(NULL::type)
                String enumType = resolveEnumTypeFromArg(fn.args().get(0), ctx);
                if (enumType == null) return null;
                CustomEnum ce = executor.database.getCustomEnum(enumType);
                if (ce == null) return null;
                return "{" + String.join(",", ce.getLabels()) + "}";
            }
            case "json_scalar": {
                if (fn.args().isEmpty()) throw new MemgresException("function json_scalar requires one argument", "42883");
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                if (val == null) return "null";
                if (val instanceof Number) return val.toString();
                if (val instanceof Boolean) return val.toString();
                // strings get quoted
                return "\"" + val.toString().replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
            }
            case "json_serialize": {
                if (fn.args().isEmpty()) throw new MemgresException("function json_serialize requires one argument", "42883");
                Expression arg = fn.args().get(0);
                Object val = executor.evalExpr(arg, ctx);
                if (val == null) return null;
                String jsonStr = val.toString();
                // JSON_SERIALIZE without RETURNING returns text (SQL standard default).
                // jsonb has already thrown the original spelling away, so it prints in its own
                // normalized form; json keeps the text it was given and must be handed back as is.
                String trimmed = jsonStr.trim();
                if (isJsonbTyped(arg, ctx) && (trimmed.startsWith("{") || trimmed.startsWith("["))) {
                    return JsonOperations.normalizeJsonb(trimmed);
                }
                return jsonStr;
            }
            case "json_array_subquery": {
                // JSON_ARRAY(SELECT ...) — execute subquery and build JSON array from results
                if (fn.args().isEmpty()) throw new MemgresException("json_array_subquery requires a subquery argument", "42883");
                Expression subqExpr = fn.args().get(0);
                if (!(subqExpr instanceof SubqueryExpr)) {
                    throw new MemgresException("json_array_subquery requires a subquery argument", "42883");
                }
                SubqueryExpr sq = (SubqueryExpr) subqExpr;
                QueryResult result = executor.executeStatement(sq.subquery());
                // PG: JSON_ARRAY from empty subquery returns NULL
                if (result.getRows().isEmpty()) return null;
                StringBuilder sb = new StringBuilder("[");
                boolean first = true;
                for (Object[] row : result.getRows()) {
                    if (row.length > 0 && row[0] != null) {
                        if (!first) sb.append(", ");
                        first = false;
                        appendJsonValue(sb, row[0]);
                    }
                }
                sb.append("]");
                return sb.toString();
            }
            case "json_array_constructor": {
                // Last arg is the null behavior flag: "absent_on_null" or "null_on_null"
                int argCount = fn.args().size();
                String nullBehavior = "absent"; // default: ABSENT ON NULL
                if (argCount > 0) {
                    Expression lastArg = fn.args().get(argCount - 1);
                    if (lastArg instanceof Literal) {
                        String flag = ((Literal) lastArg).value();
                        if ("absent_on_null".equals(flag) || "null_on_null".equals(flag)) {
                            nullBehavior = flag.startsWith("null") ? "null" : "absent";
                            argCount--;
                        }
                    }
                }
                StringBuilder sb = new StringBuilder("[");
                boolean first = true;
                for (int i = 0; i < argCount; i++) {
                    Object val = executor.evalExpr(fn.args().get(i), ctx);
                    if (val == null && "absent".equals(nullBehavior)) continue;
                    if (!first) sb.append(", ");
                    first = false;
                    appendJsonValue(sb, val);
                }
                sb.append("]");
                return sb.toString();
            }
            case "json_object_constructor": {
                // Args are key-value pairs, last two args are flags: nullBehavior, uniqueKeys
                int argCount = fn.args().size();
                String nullBehavior = "absent";
                boolean uniqueKeys = false;
                // Parse trailing flags (packed as "absent_on_null"/"null_on_null" and "unique_keys"/"no_unique_keys")
                if (argCount >= 2) {
                    Expression lastArg = fn.args().get(argCount - 1);
                    Expression secondLastArg = fn.args().get(argCount - 2);
                    if (lastArg instanceof Literal && secondLastArg instanceof Literal) {
                        String f1 = ((Literal) secondLastArg).value();
                        String f2 = ((Literal) lastArg).value();
                        if (("absent_on_null".equals(f1) || "null_on_null".equals(f1)) &&
                            ("unique_keys".equals(f2) || "no_unique_keys".equals(f2))) {
                            nullBehavior = f1.startsWith("null") ? "null" : "absent";
                            uniqueKeys = "unique_keys".equals(f2);
                            argCount -= 2;
                        }
                    }
                }
                StringBuilder sb = new StringBuilder("{");
                boolean first = true;
                Set<String> seenKeys = uniqueKeys ? new HashSet<>() : null;
                for (int i = 0; i + 1 < argCount; i += 2) {
                    Object key = executor.evalExpr(fn.args().get(i), ctx);
                    Object val = executor.evalExpr(fn.args().get(i + 1), ctx);
                    if (key == null) throw new MemgresException("null value not allowed for object key", "22023");
                    if (val == null && "absent".equals(nullBehavior)) continue;
                    String keyStr = key.toString();
                    if (uniqueKeys && seenKeys != null) {
                        if (!seenKeys.add(keyStr)) {
                            throw new MemgresException("duplicate JSON object key value", "22030");
                        }
                    }
                    if (!first) sb.append(", ");
                    first = false;
                    sb.append("\"").append(keyStr.replace("\\", "\\\\").replace("\"", "\\\"")).append("\" : ");
                    appendJsonValue(sb, val);
                }
                sb.append("}");
                return sb.toString();
            }
            case "text": {
                if (fn.args().size() != 1) {
                    throw new MemgresException("function text() requires exactly one argument", "42883");
                }
                Expression argExpr = fn.args().get(0);
                Object val = executor.evalExpr(argExpr, ctx);
                if (val == null) return null;
                // text(inet)/text(cidr) uses the network_show representation, which always
                // includes the prefix length (e.g. text('1.2.3.4'::inet) -> '1.2.3.4/32').
                if (val instanceof InetValue) {
                    return ((InetValue) val).text();
                }
                // Check if the argument comes from an inet/cidr column and use network text representation
                if (argExpr instanceof ColumnRef && ctx != null) {
                    ColumnRef ref = (ColumnRef) argExpr;
                    Column colDef = ctx.resolveColumnDef(ref.table(), ref.column());
                    if (colDef != null && (colDef.getType() == DataType.INET || colDef.getType() == DataType.CIDR)) {
                        return NetworkOperations.text(val.toString());
                    }
                }
                return val.toString();
            }
            // ---- pg_trgm extension ----
            case "show_trgm": {
                requireExtension("pg_trgm", name, fn.args().size());
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString().toLowerCase();
                Set<String> trgmSet = new java.util.TreeSet<>();
                // Pad with two spaces on each side (PG convention)
                String padded = "  " + s + " ";
                for (int i = 0; i <= padded.length() - 3; i++) {
                    trgmSet.add(padded.substring(i, i + 3));
                }
                return new ArrayList<>(trgmSet);
            }
            case "similarity": {
                requireExtension("pg_trgm", name, fn.args().size());
                requireArgs(fn, 2);
                Object arg1 = executor.evalExpr(fn.args().get(0), ctx);
                Object arg2 = executor.evalExpr(fn.args().get(1), ctx);
                if (arg1 == null || arg2 == null) return 0.0;
                String s1 = arg1.toString().toLowerCase();
                String s2 = arg2.toString().toLowerCase();
                Set<String> trgm1 = trigramSet(s1);
                Set<String> trgm2 = trigramSet(s2);
                if (trgm1.isEmpty() && trgm2.isEmpty()) return 1.0;
                Set<String> intersection = new HashSet<>(trgm1);
                intersection.retainAll(trgm2);
                Set<String> union = new HashSet<>(trgm1);
                union.addAll(trgm2);
                if (union.isEmpty()) return 0.0;
                return (double) intersection.size() / (double) union.size();
            }
            // ---- cube extension ----
            case "cube": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof java.util.List<?>) {
                    java.util.List<?> list = (java.util.List<?>) arg;
                    double[] coords = new double[list.size()];
                    for (int ci = 0; ci < list.size(); ci++) {
                        Object elem = list.get(ci);
                        if (elem instanceof Number) {
                            coords[ci] = ((Number) elem).doubleValue();
                        } else {
                            coords[ci] = Double.parseDouble(elem.toString());
                        }
                    }
                    return new CubeValue(coords);
                }
                // Single scalar → 1D cube
                double v = (arg instanceof Number) ? ((Number) arg).doubleValue() : Double.parseDouble(arg.toString());
                return new CubeValue(new double[]{v});
            }
            case "cube_dim": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                if (arg instanceof CubeValue) {
                    return ((CubeValue) arg).dim();
                }
                throw new MemgresException("function cube_dim(cube) does not exist", "42883");
            }
            // ---- fuzzystrmatch extension ----
            case "levenshtein": {
                requireExtension("fuzzystrmatch", name, fn.args().size());
                requireArgs(fn, 2);
                Object arg1 = executor.evalExpr(fn.args().get(0), ctx);
                Object arg2 = executor.evalExpr(fn.args().get(1), ctx);
                if (arg1 == null || arg2 == null) return null;
                return levenshteinDistance(arg1.toString(), arg2.toString());
            }
            case "soundex": {
                requireExtension("fuzzystrmatch", name, fn.args().size());
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                return computeSoundex(arg.toString());
            }
            // ---- unaccent extension ----
            case "unaccent": {
                requireExtension("unaccent", name, fn.args().size());
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString();
                // Use Java's Normalizer to decompose, then strip combining marks
                String normalized = java.text.Normalizer.normalize(s, java.text.Normalizer.Form.NFD);
                return normalized.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
            }
            // ---- unicode functions ----
            case "unicode_version":
                return Character.UnicodeScript.of('A').toString().isEmpty() ? "0.0" : System.getProperty("java.version").startsWith("1") ? "6.2" : "15.0";
            case "unicode_assigned": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String text = arg.toString();
                if (text.isEmpty()) return false;
                int codepoint = text.codePointAt(0);
                return Character.isDefined(codepoint);
            }
            case "__is_normalized__": {
                // IS [NOT] [NFC|NFD|NFKC|NFKD] NORMALIZED
                Object textVal = executor.evalExpr(fn.args().get(0), ctx);
                if (textVal == null) return null;
                String form = executor.evalExpr(fn.args().get(1), ctx).toString();
                boolean expectTrue = executor.isTruthy(executor.evalExpr(fn.args().get(2), ctx));
                java.text.Normalizer.Form nf;
                switch (form.toUpperCase()) {
                    case "NFD": nf = java.text.Normalizer.Form.NFD; break;
                    case "NFKC": nf = java.text.Normalizer.Form.NFKC; break;
                    case "NFKD": nf = java.text.Normalizer.Form.NFKD; break;
                    default: nf = java.text.Normalizer.Form.NFC; break;
                }
                boolean normalized = java.text.Normalizer.isNormalized(textVal.toString(), nf);
                boolean result = expectTrue ? normalized : !normalized;
                return result;
            }

            // ---- server file access stubs ----
            case "pg_read_file": {
                // pg_read_file(text) / pg_read_file(text, int, int) — stub, returns empty string
                return "";
            }
            case "pg_read_binary_file": {
                // pg_read_binary_file(text) / pg_read_binary_file(text, int, int) — stub, returns empty bytea
                return new byte[0];
            }
            case "pg_stat_file": {
                // pg_stat_file(text) → record (size, access, modification, change, creation, isdir)
                // Stub: returns a record with zeroed/null fields
                List<Object> record = new ArrayList<>();
                record.add(0L);          // size
                record.add(null);        // access
                record.add(null);        // modification
                record.add(null);        // change
                record.add(null);        // creation
                record.add(false);       // isdir
                return record;
            }
            case "pg_ls_dir": {
                // pg_ls_dir(text) → set of text — stub, returns empty set
                return new ArrayList<>();
            }
            case "pg_ls_logdir":
            case "pg_ls_waldir":
            case "pg_ls_tmpdir":
            case "pg_ls_archive_statusdir": {
                // These all return set of record — stub, return empty set
                return new ArrayList<>();
            }
            case "pg_current_logfile": {
                // pg_current_logfile() → text — stub, returns empty string
                return "";
            }
            case "pg_log_backend_memory_contexts": {
                // pg_log_backend_memory_contexts(int) → boolean — stub, returns true
                return true;
            }
            case "pg_promote": {
                // pg_promote(boolean, integer) → boolean — only valid on a standby server
                throw new MemgresException("recovery is not in progress", "55000");
            }
            case "pg_wal_replay_pause":
            case "pg_wal_replay_resume": {
                // Recovery control, and this server is not recovering — the same 55000 PostgreSQL
                // gives on a primary. The name was listed in pg_proc and answered 42883, which told
                // a monitoring tool the function was missing rather than that it did not apply.
                throw new MemgresException("recovery is not in progress", "55000");
            }
            case "pg_switch_wal": {
                // Forces a WAL segment switch and answers with the LSN it ended at. memgres keeps
                // no WAL, so nothing moves and the answer is the same zero LSN pg_current_wal_lsn
                // gives.
                return "0/0";
            }
            case "pg_create_restore_point": {
                // Names a point in the WAL to recover to, and answers with its LSN. The name is
                // required and NULL propagates, because the function is declared strict.
                requireArgs(fn, 1);
                Object pointName = executor.evalExpr(fn.args().get(0), ctx);
                return pointName == null ? null : "0/0";
            }
            case "pg_safe_snapshot_blocking_pids": {
                // pg_safe_snapshot_blocking_pids(int) → int[] — stub, returns empty int array
                return "{}";
            }
            case "pg_partition_ancestors": {
                // pg_partition_ancestors(regclass) → set of regclass — returns the table itself
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return new ArrayList<>();
                List<Object> result = new ArrayList<>();
                result.add(arg);
                return result;
            }
            case "pg_partition_tree": {
                // pg_partition_tree(regclass) → set of (relid, parentrelid, isleaf, level)
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return new ArrayList<>();
                // Return the table itself as a single-entry partition tree
                List<Object> row = new ArrayList<>();
                row.add(arg);   // relid
                row.add(null);  // parentrelid (root has no parent)
                row.add(true);  // isleaf
                row.add(0);     // level
                List<List<Object>> result = new ArrayList<>();
                result.add(row);
                return result;
            }
            case "pg_stat_statements_reset": {
                throw new MemgresException("pg_stat_statements must be loaded via \"shared_preload_libraries\"", "55000");
            }

            // ---- sha224 standalone function ----
            case "sha224": {
                requireArgs(fn, 1);
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                byte[] data;
                if (arg instanceof byte[]) {
                    data = (byte[]) arg;
                } else {
                    data = arg.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }
                try {
                    java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-224");
                    return md.digest(data);
                } catch (java.security.NoSuchAlgorithmException e) {
                    throw new MemgresException("SHA-224 not available", "XX000");
                }
            }

            // ---- encoding conversion stub ----
            case "convert": {
                // convert(bytea, src_encoding, dest_encoding) → bytea — encoding conversion
                requireArgs(fn, 3);
                Object input = executor.evalExpr(fn.args().get(0), ctx);
                if (input == null) return null;
                Object srcEncObj = executor.evalExpr(fn.args().get(1), ctx);
                Object dstEncObj = executor.evalExpr(fn.args().get(2), ctx);
                String srcEnc = srcEncObj != null ? srcEncObj.toString() : "UTF8";
                String dstEnc = dstEncObj != null ? dstEncObj.toString() : "UTF8";
                byte[] data;
                if (input instanceof byte[]) {
                    data = (byte[]) input;
                } else {
                    data = input.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                }
                try {
                    java.nio.charset.Charset srcCharset = pgEncodingToCharset(srcEnc);
                    java.nio.charset.Charset dstCharset = pgEncodingToCharset(dstEnc);
                    String decoded = new String(data, srcCharset);
                    return decoded.getBytes(dstCharset);
                } catch (Exception e) {
                    return data;
                }
            }

            // ---- enum comparison ----
            case "enum_cmp": {
                requireArgs(fn, 2);
                Object a = executor.evalExpr(fn.args().get(0), ctx);
                Object b = executor.evalExpr(fn.args().get(1), ctx);
                if (a == null || b == null) return null;
                // Two enum values carry their own position, so they compare by it without
                // anything having to name the type they belong to.
                if (a instanceof AstExecutor.PgEnum && b instanceof AstExecutor.PgEnum) {
                    return Integer.signum(((AstExecutor.PgEnum) a).compareTo((AstExecutor.PgEnum) b));
                }
                // Compare by position in the enum type
                // Try to resolve the enum type from argument casts
                String enumType = null;
                for (Expression arg : fn.args()) {
                    if (arg instanceof CastExpr) {
                        enumType = ((CastExpr) arg).typeName();
                        break;
                    }
                }
                if (enumType != null) {
                    CustomEnum ce = executor.database.getCustomEnum(enumType);
                    if (ce != null) {
                        List<String> labels = ce.getLabels();
                        int posA = labels.indexOf(a.toString());
                        int posB = labels.indexOf(b.toString());
                        return Integer.compare(posA, posB);
                    }
                }
                // Fallback: lexicographic comparison. A comparison function answers with the
                // sign of the difference, not the difference itself.
                return Integer.signum(a.toString().compareTo(b.toString()));
            }

            // ---- ICU unicode version ----
            case "icu_unicode_version": {
                // Returns the ICU unicode version string — stub
                return "15.1";
            }

            // =================================================================
            // hstore extension functions
            // =================================================================
            case "akeys": {
                requireExtension("hstore", name, fn.args().size());
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                if (val == null) return null;
                HstoreValue h = toHstore(val);
                return h.keys();
            }
            case "avals": {
                requireExtension("hstore", name, fn.args().size());
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                if (val == null) return null;
                HstoreValue h = toHstore(val);
                return h.values();
            }
            case "skeys":
            case "svals":
            case "each": {
                // These are set-returning functions — they work in FROM clauses via FromFunctionResolver,
                // and in SELECT target list via SRF expansion (returning a List).
                requireExtension("hstore", name, fn.args().size());
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                if (val == null) return null;
                HstoreValue h = toHstore(val);
                if (name.equals("skeys")) {
                    return new ArrayList<>(h.keys());
                } else if (name.equals("svals")) {
                    return new ArrayList<>(h.values());
                } else {
                    // each: list of key-value pairs
                    List<String> pairs = new ArrayList<>();
                    for (java.util.Map.Entry<String, String> e : h.getData().entrySet()) {
                        pairs.add("(" + e.getKey() + "," + (e.getValue() != null ? e.getValue() : "") + ")");
                    }
                    return pairs;
                }
            }
            case "exist":
            case "isexists": {
                requireExtension("hstore", name, fn.args().size());
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                Object key = executor.evalExpr(fn.args().get(1), ctx);
                if (val == null || key == null) return null;
                HstoreValue h = toHstore(val);
                return h.containsKey(key.toString());
            }
            case "defined":
            case "isdefined": {
                requireExtension("hstore", name, fn.args().size());
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                Object key = executor.evalExpr(fn.args().get(1), ctx);
                if (val == null || key == null) return null;
                HstoreValue h = toHstore(val);
                return h.defined(key.toString());
            }
            case "delete": {
                requireExtension("hstore", name, fn.args().size());
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                Object key = executor.evalExpr(fn.args().get(1), ctx);
                if (val == null) return null;
                HstoreValue h = toHstore(val);
                if (key == null) return h;
                if (key instanceof java.util.List) {
                    java.util.List<String> keys = new java.util.ArrayList<>();
                    for (Object k : (java.util.List<?>) key) keys.add(k != null ? k.toString() : null);
                    return h.deleteKeys(keys);
                }
                if (key instanceof HstoreValue) {
                    // delete(hstore, hstore) — remove matching key/value pairs
                    HstoreValue rh = (HstoreValue) key;
                    java.util.Map<String, String> result = new java.util.LinkedHashMap<>(h.getData());
                    for (java.util.Map.Entry<String, String> e : rh.getData().entrySet()) {
                        String v = result.get(e.getKey());
                        if (v != null && v.equals(e.getValue())) result.remove(e.getKey());
                        else if (v == null && e.getValue() == null && result.containsKey(e.getKey())) result.remove(e.getKey());
                    }
                    return new HstoreValue(result);
                }
                return h.deleteKey(key.toString());
            }
            case "slice": {
                requireExtension("hstore", name, fn.args().size());
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                Object keysObj = executor.evalExpr(fn.args().get(1), ctx);
                if (val == null) return null;
                HstoreValue h = toHstore(val);
                List<String> keys = new ArrayList<>();
                if (keysObj instanceof List) {
                    for (Object k : (List<?>) keysObj) keys.add(k != null ? k.toString() : null);
                }
                return h.slice(keys);
            }
            case "populate_record": {
                requireExtension("hstore", name, fn.args().size());
                if (fn.args().size() != 2)
                    throw new MemgresException("function populate_record requires 2 arguments", "42883");
                String typeName = executor.resolveCompositeTypeName(fn.args().get(0), ctx);
                Object baseArg = executor.evalExpr(fn.args().get(0), ctx);
                Object hstoreArg = executor.evalExpr(fn.args().get(1), ctx);
                HstoreValue hs = (hstoreArg == null)
                        ? new HstoreValue(new java.util.LinkedHashMap<>()) : toHstore(hstoreArg);
                java.util.List<CreateTypeStmt.CompositeField> fields =
                        executor.compositeTypeHandler.resolveFieldsForType(typeName);
                if (fields == null)
                    throw new MemgresException("first argument of populate_record must be a row type", "42846");
                return executor.compositeTypeHandler.populateFromHstore(baseArg, hs, fields);
            }
            case "hstore": {
                requireExtension("hstore", name, fn.args().size());
                if (fn.args().size() == 2) {
                    // hstore(keys text[], vals text[]) or hstore(key text, val text)
                    Object keysObj = executor.evalExpr(fn.args().get(0), ctx);
                    Object valsObj = executor.evalExpr(fn.args().get(1), ctx);
                    if (keysObj instanceof List && valsObj instanceof List) {
                        List<?> keysList = (List<?>) keysObj;
                        List<?> valsList = (List<?>) valsObj;
                        java.util.Map<String, String> map = new java.util.LinkedHashMap<>();
                        for (int i = 0; i < keysList.size(); i++) {
                            String k = keysList.get(i) != null ? keysList.get(i).toString() : null;
                            String v = i < valsList.size() && valsList.get(i) != null ? valsList.get(i).toString() : null;
                            if (k != null) map.put(k, v);
                        }
                        return new HstoreValue(map);
                    }
                    // hstore(key text, val text) — single pair
                    java.util.Map<String, String> map = new java.util.LinkedHashMap<>();
                    if (keysObj != null) map.put(keysObj.toString(), valsObj != null ? valsObj.toString() : null);
                    return new HstoreValue(map);
                }
                if (fn.args().size() == 1) {
                    // hstore(text) or hstore(hstore) or hstore(text[]) — parse, pass through, or build from array
                    Object rec = executor.evalExpr(fn.args().get(0), ctx);
                    if (rec == null) return null;
                    if (rec instanceof HstoreValue) return rec;
                    // hstore(record) — convert composite type to hstore
                    if (rec instanceof java.util.Map) {
                        java.util.Map<String, String> hmap = new java.util.LinkedHashMap<>();
                        for (java.util.Map.Entry<?, ?> e : ((java.util.Map<?, ?>) rec).entrySet()) {
                            hmap.put(e.getKey().toString(), e.getValue() != null ? e.getValue().toString() : null);
                        }
                        return new HstoreValue(hmap);
                    }
                    if (rec instanceof AstExecutor.PgRow) {
                        String typeName = executor.resolveCompositeTypeName(fn.args().get(0), ctx);
                        java.util.List<CreateTypeStmt.CompositeField> fields =
                                typeName != null ? executor.compositeTypeHandler.resolveFieldsForType(typeName) : null;
                        if (fields != null) {
                            AstExecutor.PgRow row = (AstExecutor.PgRow) rec;
                            java.util.Map<String, String> hmap = new java.util.LinkedHashMap<>();
                            for (int i = 0; i < fields.size() && i < row.values().size(); i++) {
                                Object v = row.values().get(i);
                                hmap.put(fields.get(i).name(), v != null ? v.toString() : null);
                            }
                            return new HstoreValue(hmap);
                        }
                        throw new MemgresException("could not determine composite type for hstore(record)", "42804");
                    }
                    if (rec instanceof List) {
                        List<?> arr = (List<?>) rec;
                        java.util.Map<String, String> map = new java.util.LinkedHashMap<>();
                        if (!arr.isEmpty() && arr.get(0) instanceof List) {
                            // 2D array: [[k1,v1],[k2,v2],...]
                            for (Object row : arr) {
                                List<?> pair = (List<?>) row;
                                if (pair.size() >= 2) {
                                    String k = pair.get(0) != null ? pair.get(0).toString() : null;
                                    String v = pair.get(1) != null ? pair.get(1).toString() : null;
                                    if (k != null) map.put(k, v);
                                }
                            }
                        } else {
                            // Flat alternating array: [k1,v1,k2,v2,...]
                            for (int i = 0; i + 1 < arr.size(); i += 2) {
                                String k = arr.get(i) != null ? arr.get(i).toString() : null;
                                String v = arr.get(i + 1) != null ? arr.get(i + 1).toString() : null;
                                if (k != null) map.put(k, v);
                            }
                        }
                        return new HstoreValue(map);
                    }
                    return HstoreValue.parse(rec.toString());
                }
                throw new MemgresException("function hstore() requires 1 or 2 arguments", "42883");
            }
            case "hstore_to_json": {
                requireExtension("hstore", name, fn.args().size());
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                if (val == null) return null;
                HstoreValue h = toHstore(val);
                return hstoreToJsonString(h);
            }
            case "hstore_to_jsonb": {
                requireExtension("hstore", name, fn.args().size());
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                if (val == null) return null;
                HstoreValue h = toHstore(val);
                return hstoreToJsonString(h);
            }
            case "hstore_to_json_loose":
            case "hstore_to_jsonb_loose": {
                requireExtension("hstore", name, fn.args().size());
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                if (val == null) return null;
                HstoreValue h = toHstore(val);
                return hstoreToJsonLooseString(h);
            }
            case "hstore_to_array": {
                requireExtension("hstore", name, fn.args().size());
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                if (val == null) return null;
                HstoreValue h = toHstore(val);
                // Returns flat array: {k1, v1, k2, v2, ...}
                List<String> result = new ArrayList<>();
                for (java.util.Map.Entry<String, String> e : h.getData().entrySet()) {
                    result.add(e.getKey());
                    result.add(e.getValue());
                }
                return result;
            }
            case "hstore_to_matrix": {
                requireExtension("hstore", name, fn.args().size());
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                if (val == null) return null;
                HstoreValue h = toHstore(val);
                // Returns 2D array: {{k1,v1},{k2,v2},...}
                List<List<String>> result = new ArrayList<>();
                for (java.util.Map.Entry<String, String> e : h.getData().entrySet()) {
                    List<String> pair = new ArrayList<>();
                    pair.add(e.getKey());
                    pair.add(e.getValue());
                    result.add(pair);
                }
                return result;
            }

            default: {
                // Delegate to domain-specific function handlers
                Object delegated2;
                delegated2 = jsonFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;
                delegated2 = textSearchFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;
                delegated2 = xmlFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;
                delegated2 = geometricFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;
                delegated2 = rangeFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;
                delegated2 = networkFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;
                delegated2 = byteaFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;
                delegated2 = advisoryLockFunctions.eval(name, fn, ctx);
                if (delegated2 != NOT_HANDLED) return delegated2;

                // Try user-defined function; resolve overloads by argument types
                PgFunction userFunc;
                {
                    // Handle schema-qualified function names (e.g., lib.helper)
                    String lookupName = name;
                    String qualifiedSchema = null;
                    if (name.contains(".")) {
                        int dot = name.indexOf('.');
                        qualifiedSchema = name.substring(0, dot);
                        lookupName = name.substring(dot + 1);
                    }
                    List<PgFunction> overloads = executor.database.getFunctionOverloads(lookupName);
                    // Filter by explicit schema when schema-qualified
                    if (qualifiedSchema != null) {
                        String qs = qualifiedSchema;
                        List<PgFunction> filtered = new ArrayList<>();
                        for (PgFunction f : overloads) {
                            String fSchema = f.getSchemaName() != null ? f.getSchemaName().toLowerCase() : "public";
                            if (qs.equalsIgnoreCase(fSchema)) filtered.add(f);
                        }
                        // For pg_catalog qualification, fall back to unfiltered (built-ins lack schema)
                        if (!filtered.isEmpty() || !"pg_catalog".equalsIgnoreCase(qs)) {
                            overloads = filtered;
                        }
                    } else if (!name.contains(".") && executor.session != null) {
                        List<String> visibleSchemas = new ArrayList<>();
                        visibleSchemas.add("pg_catalog");
                        String searchPath = executor.session.getGucSettings().get("search_path");
                        if (searchPath != null) {
                            for (String sp : searchPath.split(",")) {
                                String sc = sp.trim().toLowerCase();
                                if (!visibleSchemas.contains(sc)) visibleSchemas.add(sc);
                            }
                        }
                        if (!visibleSchemas.contains("public")) visibleSchemas.add("public");
                        // Group by search_path position so the earliest schema holding this name wins,
                        // the way PG resolves an unqualified routine reference.
                        List<PgFunction> filtered = new ArrayList<>();
                        for (String schema : visibleSchemas) {
                            for (PgFunction f : overloads) {
                                if (Database.schemaOf(f).equalsIgnoreCase(schema)) filtered.add(f);
                            }
                        }
                        // Apply filter. Fall back to unfiltered only if ALL functions have null schema (built-ins)
                        if (!filtered.isEmpty()) {
                            overloads = filtered;
                        } else {
                            boolean anyHasSchema = overloads.stream().anyMatch(f -> f.getSchemaName() != null);
                            if (anyHasSchema) {
                                overloads = filtered; // empty — function exists but not in search_path
                            }
                            // else: all have null schema (built-ins) — keep unfiltered
                        }
                    }
                    if (overloads.size() >= 1) {
                        // Resolve by evaluating argument types
                        // When named args are present, type hints are in call order which
                        // may differ from param order, so skip hints to avoid false mismatches.
                        boolean callHasNamedArgs = fn.args().stream()
                                .anyMatch(a -> a instanceof NamedArgExpr && !((NamedArgExpr) a).name().equals("__variadic__"));
                        List<String> argTypeHints = new ArrayList<>();
                        if (!callHasNamedArgs) {
                            for (Expression arg : fn.args()) {
                                // Check for explicit cast (e.g., ROW(...)::typename) - use cast type as hint
                                if (arg instanceof CastExpr) {
                                    argTypeHints.add(((CastExpr) arg).typeName());
                                    continue;
                                }
                                try {
                                    Object v = executor.evalExpr(arg, ctx);
                                    if (v instanceof Integer) argTypeHints.add("integer");
                                    else if (v instanceof Long) argTypeHints.add("bigint");
                                    else if (v instanceof String) argTypeHints.add("text");
                                    else if (v instanceof Boolean) argTypeHints.add("boolean");
                                    else if (v instanceof Double || v instanceof Float) argTypeHints.add("double precision");
                                    else argTypeHints.add(null);
                                } catch (Exception e) {
                                    argTypeHints.add(null);
                                }
                            }
                        }
                        // Pick among same-arity candidates the way PG's function resolution does,
                        // before falling back to the arity-and-hint match below.
                        List<PgFunction> best = narrowByDeclaredArgTypes(overloads, fn, callHasNamedArgs);
                        if (best != null) overloads = best;
                        userFunc = overloads.isEmpty() ? null
                                : executor.database.resolveFunction(overloads, fn.args().size(), argTypeHints);
                        // When explicit VARIADIC was used and the array was empty,
                        // expansion yields 0 variadic args. Resolution rejects this
                        // because it looks like no variadic args were provided.
                        // Retry with argCount+1 to simulate the empty array as one arg.
                        if (userFunc == null && callUsedVariadic && fn.args().isEmpty()) {
                            userFunc = executor.database.resolveFunction(overloads, 1, argTypeHints);
                        }
                    } else {
                        userFunc = null;
                    }
                }
                if (userFunc != null) {
                    // Procedures cannot be called via SELECT; must use CALL
                    if (userFunc.isProcedure()) {
                        throw new MemgresException(name + " is a procedure\nHint: To call a procedure, use CALL.", "42809");
                    }
                    // Collect input params (excluding OUT)
                    List<PgFunction.Param> inputParams = new ArrayList<>();
                    boolean funcHasVariadic = false;
                    PgFunction.Param variadicParam = null;
                    for (PgFunction.Param p : userFunc.getParams()) {
                        if ("VARIADIC".equalsIgnoreCase(p.mode())) {
                            funcHasVariadic = true;
                            variadicParam = p;
                            inputParams.add(p);
                            continue;
                        }
                        if (!"OUT".equalsIgnoreCase(p.mode())) {
                            inputParams.add(p);
                        }
                    }
                    int requiredParams = 0;
                    for (PgFunction.Param p : inputParams) {
                        if (p.defaultExpr() == null) requiredParams++;
                    }

                    // Check if any arg uses named notation
                    boolean hasNamedNotation = fn.args().stream().anyMatch(a -> a instanceof NamedArgExpr);

                    if (hasNamedNotation) {
                        // Resolve named parameters: reorder args to match parameter positions
                        List<Expression> positionalArgs = new ArrayList<>();
                        Map<String, Expression> namedMap = new LinkedHashMap<>();
                        for (Expression arg : fn.args()) {
                            if (arg instanceof NamedArgExpr) {
                                NamedArgExpr na = (NamedArgExpr) arg;
                                namedMap.put(na.name().toLowerCase(), na.value());
                            } else {
                                positionalArgs.add(arg);
                            }
                        }

                        // Check for positional arg conflicting with named arg
                        for (int i = 0; i < positionalArgs.size(); i++) {
                            if (i < inputParams.size()) {
                                String paramName = inputParams.get(i).name() != null ? inputParams.get(i).name().toLowerCase() : null;
                                if (paramName != null && namedMap.containsKey(paramName)) {
                                    throw new MemgresException("function " + name + "(" +
                                            buildNamedArgTypeList(fn.args(), ctx) +
                                            ") does not exist", "42883");
                                }
                            }
                        }

                        // Check that all named args refer to valid parameter names
                        for (String argName : namedMap.keySet()) {
                            boolean found = false;
                            for (PgFunction.Param p : inputParams) {
                                if (p.name() != null && p.name().equalsIgnoreCase(argName)) {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                throw new MemgresException("function " + name + "(" +
                                        buildNamedArgTypeList(fn.args(), ctx) +
                                        ") does not exist", "42883");
                            }
                        }

                        // Build reordered arg list matching parameter positions
                        List<Object> args = new ArrayList<>();
                        for (int i = 0; i < inputParams.size(); i++) {
                            PgFunction.Param p = inputParams.get(i);
                            String pName = p.name() != null ? p.name().toLowerCase() : null;

                            if (i < positionalArgs.size()) {
                                args.add(executor.evalExpr(positionalArgs.get(i), ctx));
                            } else if (pName != null && namedMap.containsKey(pName)) {
                                args.add(executor.evalExpr(namedMap.get(pName), ctx));
                            } else if (p.defaultExpr() != null) {
                                // Evaluate default expression
                                try {
                                    QueryResult defaultResult = executor.execute("SELECT " + p.defaultExpr());
                                    Object defaultVal = (!defaultResult.getRows().isEmpty() && defaultResult.getRows().get(0).length > 0)
                                            ? defaultResult.getRows().get(0)[0] : null;
                                    args.add(defaultVal);
                                } catch (Exception e) {
                                    args.add(null);
                                }
                            } else {
                                // Required parameter missing
                                throw new MemgresException("function " + name + "(" +
                                        buildNamedArgTypeList(fn.args(), ctx) +
                                        ") does not exist", "42883");
                            }
                        }

                        // STRICT: return NULL immediately if any argument is NULL
                        // For set-returning functions, return empty set instead of NULL
                        if (userFunc.isStrict()) {
                            for (Object arg : args) {
                                if (arg == null) {
                                    String rt = userFunc.getReturnType();
                                    if (rt != null && (rt.toUpperCase().startsWith("SETOF") || rt.toUpperCase().startsWith("TABLE"))) {
                                        return new java.util.ArrayList<>();
                                    }
                                    return null;
                                }
                            }
                        }
                        userFunc.incrementCallCount();
                        PlpgsqlExecutor plExec = new PlpgsqlExecutor(executor, executor.database, executor.session);
                        Object result = plExec.executeFunction(userFunc, args);
                        if (result instanceof List<?>) return (List<?>) result;
                        return result;
                    }

                    // Positional-only args: validate arity
                    int actualArgs = fn.args().size();
                    if (!funcHasVariadic && (actualArgs < requiredParams || actualArgs > inputParams.size())) {
                        throw new MemgresException("function " + name + "(" +
                                fn.args().stream().map(a -> {
                                    Object v = executor.evalExpr(a, ctx);
                                    return v instanceof Integer ? "integer" : v instanceof Long ? "bigint" :
                                           v instanceof String ? "text" : "unknown";
                                }).collect(java.util.stream.Collectors.joining(", ")) +
                                ") does not exist", "42883");
                    }
                    List<Object> args = new ArrayList<>();
                    for (int i = 0; i < fn.args().size(); i++) {
                        Object val = executor.evalExpr(fn.args().get(i), ctx);
                        // Coerce argument to declared parameter type (skip VARIADIC array type)
                        if (val != null && i < inputParams.size()
                                && !"VARIADIC".equalsIgnoreCase(inputParams.get(i).mode())) {
                            String declaredType = inputParams.get(i).typeName();
                            if (declaredType != null) {
                                val = executor.castEvaluator.applyCast(val, declaredType);
                            }
                        }
                        args.add(val);
                    }
                    checkPolymorphicArgs(inputParams, args, fn.args(), ctx, name);
                    // STRICT: return NULL immediately if any argument is NULL
                    // For set-returning functions, return empty set instead of NULL
                    if (userFunc.isStrict()) {
                        for (Object arg : args) {
                            if (arg == null) {
                                String rt = userFunc.getReturnType();
                                if (rt != null && (rt.toUpperCase().startsWith("SETOF") || rt.toUpperCase().startsWith("TABLE"))) {
                                    return new java.util.ArrayList<>();
                                }
                                return null;
                            }
                        }
                    }
                    userFunc.incrementCallCount();
                    PlpgsqlExecutor plExec = new PlpgsqlExecutor(executor, executor.database, executor.session);
                    Object result = plExec.executeFunction(userFunc, args);
                    // Handle set-returning functions
                    if (result instanceof List<?>) {
                        // When called in scalar context, return the list as-is for FROM processing
                        return (List<?>) result;
                    }
                    return result;
                }
                // Check if it's a known aggregate function; if so, return null
                // (aggregates are handled by SelectExecutor, not FunctionEvaluator)
                Set<String> AGGREGATES = Cols.setOf("count", "sum", "avg", "min", "max",
                        "string_agg", "array_agg", "bool_and", "bool_or", "every",
                        "bit_and", "bit_or", "json_agg", "jsonb_agg",
                        "json_object_agg", "jsonb_object_agg", "xmlagg",
                        "json_arrayagg", "json_objectagg", "any_value");
                if (AGGREGATES.contains(name)) {
                    return null; // Will be handled by aggregate executor
                }
                // grouping() only makes sense over a grouping expression of this query level
                if (name.equals("grouping")) {
                    throw new MemgresException(
                            "arguments to GROUPING must be grouping expressions of the associated query level",
                            "42803");
                }
                // "open" is not a SQL function; PG gives 42704 (undefined_object)
                if (name.equals("open") || name.equals("close")) {
                    throw new MemgresException("type \"" + name + "\" does not exist", "42704");
                }
                // A type name written like a call is a cast, not a call. No pg_proc row answers
                // to it and none needs to: PostgreSQL resolves the call as a coercion, so
                // int4('42'), bool('t'), uuid(...) and a user's own domain or enum name all run.
                Object coerced = typeNameCoercion(name, fn, ctx);
                if (coerced != NOT_HANDLED) return coerced;
                // A name the catalog advertises as the implementation of an operator is callable
                // in PostgreSQL exactly as the operator is: int4pl(1,2) is 1 + 2 and texteq('a',
                // 'a') is 'a' = 'a'. memgres listed every one of them in pg_proc and could
                // dispatch none, so a tool that resolved a name through the catalog wrote a call
                // the server then refused.
                Object viaOperator = operatorFunctionCall(name, fn, ctx);
                if (viaOperator != NOT_HANDLED) return viaOperator;
                // Unknown function; build argument type list for error message
                StringBuilder argTypes = new StringBuilder();
                for (int ai = 0; ai < fn.args().size(); ai++) {
                    if (ai > 0) argTypes.append(", ");
                    try {
                        Object av = executor.evalExpr(fn.args().get(ai), ctx);
                        argTypes.append(av == null ? "unknown" :
                                av instanceof Integer ? "integer" :
                                av instanceof Long ? "bigint" :
                                av instanceof Double ? "double precision" :
                                av instanceof Boolean ? "boolean" :
                                av instanceof java.math.BigDecimal ? "numeric" :
                                "text");
                    } catch (Exception e) {
                        argTypes.append("unknown");
                    }
                }
                throw new MemgresException(
                        "function " + fn.name() + "(" + argTypes + ") does not exist", "42883");
            }
        }
    }

    // ---- JSON path delegation (used by BinaryOpEvaluator, FromResolver) ----

    String extractJsonKey(String json, String key) {
        return jsonFunctions.extractJsonKey(json, key);
    }

    private void appendJsonValue(StringBuilder sb, Object val) {
        if (val == null) { sb.append("null"); return; }
        if (val instanceof Number || val instanceof Boolean) { sb.append(val); return; }
        String s = val.toString().trim();
        if ((s.startsWith("{") && s.endsWith("}")) || (s.startsWith("[") && s.endsWith("]"))) {
            sb.append(s); // already JSON
        } else {
            sb.append("\"").append(s.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
        }
    }

    List<String> evaluateJsonPathAll(String json, String path) {
        return jsonFunctions.evaluateJsonPathAll(json, path);
    }

    boolean evaluateJsonPathExists(String json, String path) {
        return jsonFunctions.evaluateJsonPathExists(json, path);
    }

    /**
     * The @? and @@ operators take no silent argument because they are always silent: a document
     * that does not have the shape the path asks for makes them NULL rather than an error.
     */
    Boolean evaluateJsonPathExistsSilent(String json, String path) {
        json = JsonOperations.normalizeJsonb(json);
        try {
            return jsonFunctions.evaluateJsonPathExists(json, path);
        } catch (MemgresException e) {
            if (!JsonFunctions.isSuppressible(e)) throw e;
            return null;
        }
    }

    /**
     * The @@ operator is jsonb_path_match with the silent flag always on, so a path that does not
     * produce a single boolean makes it NULL rather than raising.
     */
    Boolean evaluateJsonPathMatchSilent(String json, String path) {
        json = JsonOperations.normalizeJsonb(json);
        try {
            Object result = jsonFunctions.evalPathMatch(json, path);
            return result instanceof Boolean ? (Boolean) result : null;
        } catch (MemgresException e) {
            if (!JsonFunctions.isSuppressible(e)) throw e;
            return null;
        }
    }

    // ---- Kept utility methods ----

    /**
     * Build an argument type list string for named-arg function call error messages.
     */
    private String buildNamedArgTypeList(List<Expression> args, RowContext ctx) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) sb.append(", ");
            Expression arg = args.get(i);
            if (arg instanceof NamedArgExpr) {
                NamedArgExpr na = (NamedArgExpr) arg;
                sb.append(na.name()).append(" => ");
                try {
                    Object v = executor.evalExpr(na.value(), ctx);
                    sb.append(inferArgType(v));
                } catch (Exception e) {
                    sb.append("unknown");
                }
            } else {
                try {
                    Object v = executor.evalExpr(arg, ctx);
                    sb.append(inferArgType(v));
                } catch (Exception e) {
                    sb.append("unknown");
                }
            }
        }
        return sb.toString();
    }

    private static String inferArgType(Object v) {
        if (v == null) return "unknown";
        if (v instanceof Integer) return "integer";
        if (v instanceof Long) return "bigint";
        if (v instanceof Double) return "double precision";
        if (v instanceof Boolean) return "boolean";
        if (v instanceof java.math.BigDecimal) return "numeric";
        return "text";
    }

    // ---- Enum helpers ----

    private String resolveEnumTypeFromArrayArg(Expression arg, RowContext ctx) {
        if (arg instanceof ColumnRef && ctx != null) {
            ColumnRef colRef = (ColumnRef) arg;
            Column colDef = ctx.resolveColumnDef(colRef.table(), colRef.column());
            if (colDef != null && colDef.getEnumTypeName() != null) {
                return colDef.getEnumTypeName();
            }
        }
        return null;
    }

    private String resolveEnumTypeFromArg(Expression arg, RowContext ctx) {
        if (arg instanceof CastExpr) {
            // enum_range(NULL::e) reads the e the search path names, not some other schema's.
            CastExpr cast = (CastExpr) arg;
            return executor.castEvaluator.qualifyUserType(cast.typeName());
        }
        Object val = executor.evalExpr(arg, ctx);
        return val == null ? null : val.toString();
    }

    /**
     * Resolve a sequence by name, checking session temp schema first, then global.
     */
    private Sequence resolveSequence(String seqName) {
        int dot = seqName.indexOf('.');
        String writtenSchema = dot > 0 ? seqName.substring(0, dot) : null;
        String bare = dot > 0 ? seqName.substring(dot + 1) : seqName;
        // A name that says which schema is answered by that schema alone; a bare one walks the
        // search path, with this session's temporary schema implicitly ahead of it. pg_temp is
        // not a schema of its own but the alias this session's temporary one answers to.
        List<String> path = new ArrayList<>();
        if (writtenSchema != null) {
            path.add(SchemaQualifier.resolveAlias(executor.session, writtenSchema));
        } else {
            if (executor.session != null) path.add(executor.session.getTempSchemaName());
            path.addAll(executor.searchPathSchemas());
        }
        for (String schema : path) {
            Sequence seq = executor.database.getSequence(schema, bare);
            // A sequence another session created in a transaction that is still open may never
            // have existed; it is not there to draw a value from yet.
            if (seq != null && executor.database.isObjectVisibleTo(seq, executor.session)) return seq;
        }

        // PG creates implicit sequences for SERIAL columns (tablename_colname_seq).
        // Memgres uses an internal counter instead, so auto-create the sequence
        // on first reference to maintain PG-compatible setval/nextval/currval behavior.
        if (bare.endsWith("_seq")) {
            String prefix = bare.substring(0, bare.length() - 4);
            int lastUnderscore = prefix.lastIndexOf('_');
            if (lastUnderscore > 0) {
                String tblName = prefix.substring(0, lastUnderscore);
                String colName = prefix.substring(lastUnderscore + 1);
                for (String schemaName : path) {
                    Schema schema = executor.database.getSchema(schemaName);
                    if (schema == null) continue;
                    Table tbl = schema.getTable(tblName);
                    if (tbl != null) {
                        int colIdx = tbl.getColumnIndex(colName);
                        if (colIdx >= 0 && (tbl.getColumns().get(colIdx).getType() == DataType.SERIAL
                                || tbl.getColumns().get(colIdx).getType() == DataType.BIGSERIAL
                                || tbl.getColumns().get(colIdx).getType() == DataType.SMALLSERIAL)) {
                            Sequence implicitSeq = new Sequence(bare, null, null, null, null);
                            implicitSeq.setSchemaName(schema.getName());
                            long currentVal = tbl.getSerialCounter() - 1;
                            if (currentVal >= 1) implicitSeq.setVal(currentVal);
                            executor.database.addSequence(implicitSeq);
                            return implicitSeq;
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Validate that all non-null values in a list have compatible types.
     */
    private void validateHomogeneousTypes(List<Object> values, String funcName) {
        Object firstNonNull = null;
        for (Object v : values) { if (v != null) { firstNonNull = v; break; } }
        if (firstNonNull == null) return;
        boolean firstIsNumeric = firstNonNull instanceof Number;
        for (Object v : values) {
            if (v == null) continue;
            if (firstIsNumeric && v instanceof String) {
                String s = (String) v;
                try { new java.math.BigDecimal(s); } catch (NumberFormatException e) {
                    throw new MemgresException("invalid input syntax for type integer: \"" + s + "\"", "22P02");
                }
            } else if (firstNonNull instanceof String && v instanceof Number) {
                // First was string, second is number; PG would infer text type, numbers coerce to text, that's OK
            }
        }
    }

    // ---- Overload resolution over the types the call was written with ----

    /** The type categories PostgreSQL resolves an overloaded call within. */
    private enum ArgCategory { NUMERIC, STRING, BOOLEAN }

    /**
     * How far a numeric type sits along the chain PostgreSQL casts implicitly: a value converts
     * without being asked only towards a wider type, so numeric reaches float8 but not the other
     * way round. This is what makes f(int)/f(float8) called with 6.0 resolve to the float8 one.
     */
    private static int numericRank(String type) {
        switch (type) {
            case "smallint": return 1;
            case "integer": return 2;
            case "bigint": return 3;
            case "numeric": return 4;
            case "real": return 5;
            case "double precision": return 6;
            default: return -1;
        }
    }

    private static ArgCategory categoryOf(String type) {
        if (numericRank(type) > 0) return ArgCategory.NUMERIC;
        switch (type) {
            case "text": case "character varying": case "character": case "name":
                return ArgCategory.STRING;
            case "boolean":
                return ArgCategory.BOOLEAN;
            default:
                return null;
        }
    }

    /** The type PostgreSQL falls back on within a category when nothing else decides. */
    private static String preferredTypeOf(ArgCategory category) {
        switch (category) {
            case NUMERIC: return "double precision";
            case STRING: return "text";
            default: return "boolean";
        }
    }

    /** The spelling PostgreSQL stores a type under, so that int, int4 and integer compare equal. */
    private static String canonicalTypeName(String type) {
        if (type == null) return null;
        String t = type.toLowerCase().trim();
        int paren = t.indexOf('(');
        if (paren > 0) t = t.substring(0, paren).trim();
        switch (t) {
            case "int": case "int4": return "integer";
            case "int2": return "smallint";
            case "int8": return "bigint";
            case "float4": return "real";
            case "float8": case "float": return "double precision";
            case "decimal": return "numeric";
            case "varchar": return "character varying";
            case "char": case "bpchar": return "character";
            case "bool": return "boolean";
            default: return t;
        }
    }

    /**
     * Refuses a call whose name, for the arguments as written, means more than one thing.
     *
     * <p>An untyped literal is of no type until a signature says what it is, and a name declared
     * over several types in several categories says nothing: {@code sum} is declared over numbers
     * and over intervals, so {@code sum('1')} names neither and PostgreSQL refuses it. Reading the
     * literal as a number regardless answered a call PostgreSQL does not have, and answered it
     * with whichever overload memgres happened to reach first.
     *
     * <p>Only a call every one of whose arguments is either written with a type or written as an
     * untyped literal is judged. An argument whose type this cannot read — a column of a
     * subquery, a call this says nothing about — leaves the whole call alone, because a call
     * refused on a guess is worse than a call resolved on one. A name a user has declared a
     * function for is left alone too: their declaration is part of the answer.
     */
    static void rejectAmbiguousBuiltin(AstExecutor executor, String name, List<Expression> args,
                                       RowContext ctx) {
        rejectAmbiguousBuiltin(executor, name, name, args, ctx);
    }

    /** As above, reporting the call by the name the statement wrote rather than the one it means. */
    static void rejectAmbiguousBuiltin(AstExecutor executor, String name, String writtenName,
                                       List<Expression> args, RowContext ctx) {
        if (name == null || args == null || args.isEmpty()) return;
        if (!BuiltinCallTypes.records(name)) return;
        if (executor.database.getFunctions().containsKey(name.toLowerCase())) return;
        // A type name written as a call is a cast to that type, whatever else PostgreSQL happens
        // to declare under the same spelling: bytea('x') is 'x'::bytea and takes one argument of
        // whatever was written, so there is no overload to choose between.
        if (args.size() == 1 && executor.functionEvaluator.coercibleTypeName(name) != null) return;
        args = flattenRowArguments(args);
        int[] written = new int[args.size()];
        for (int i = 0; i < args.size(); i++) {
            Expression arg = args.get(i);
            String declared = executor.binaryOpEvaluator.declaredTypeForResolution(arg, ctx);
            if (declared == null) {
                if (!isUntypedLiteral(arg)) return;   // no opinion about this call
                written[i] = BuiltinCallTypes.UNKNOWN;
                continue;
            }
            DataType type = DataType.fromPgName(canonicalTypeName(declared));
            if (type == null) return;
            written[i] = type.getOid();
        }
        BuiltinCallTypes.requireCallable(name, writtenName, written);
        BuiltinCallTypes.requireReachable(name, writtenName, written);
        // A call PostgreSQL cannot choose between is refused whether or not an argument was
        // written without a type: to_hex(int2) reaches both to_hex(int4) and to_hex(int8) and is
        // neither, so it is not a call at all. Asking only where an argument was untyped let
        // every such call through and picked one of the two.
        BuiltinCallTypes.requireResolvable(name, writtenName, written);
    }

    /**
     * The call with every bpchar argument replaced by the text it is read as.
     *
     * <p>Only an argument the statement wrote a bpchar type for is touched, and only for the
     * routines that read one as a text — so an argument judged on the shape it was written with,
     * a count or a pattern, still arrives as the expression it was.
     */
    private FunctionCallExpr withBlanksDropped(FunctionCallExpr fn, RowContext ctx, String name) {
        if (fn.args() == null || fn.args().isEmpty()) return fn;
        if (!BlankPadding.readsItAsText(name)) return fn;
        List<Expression> replaced = null;
        for (int i = 0; i < fn.args().size(); i++) {
            Expression arg = fn.args().get(i);
            if (!BlankPadding.isBlankPadded(
                    executor.binaryOpEvaluator.declaredTypeForResolution(arg, ctx))) {
                continue;
            }
            Object trimmed = BlankPadding.trimmed(executor.evalExpr(arg, ctx));
            if (replaced == null) replaced = new ArrayList<>(fn.args());
            replaced.set(i, new ExprEvaluator.PrecomputedValueExpr(trimmed, DataType.TEXT));
        }
        if (replaced == null) return fn;
        FunctionCallExpr rebuilt = new FunctionCallExpr(fn.name(), replaced, fn.distinct(),
                fn.star(), fn.orderBy(), fn.filter());
        rebuilt.spelledInGrammar = fn.spelledInGrammar;
        return rebuilt;
    }

    /**
     * Whether a user has declared a function of this name. A few built-ins are carried as
     * {@link PgFunction} stubs so that ALTER FUNCTION can name them, and those are memgres's own
     * — they live in pg_catalog, and they are still the built-in.
     */
    private static boolean userDeclaredFunction(AstExecutor executor, String name) {
        for (PgFunction f : executor.database.getFunctionOverloads(name.toLowerCase())) {
            if (!"pg_catalog".equals(f.getSchemaName())) return true;
        }
        PgFunction single = executor.database.getFunctions().get(name.toLowerCase());
        return single != null && !"pg_catalog".equals(single.getSchemaName());
    }

    /**
     * The arguments a call really passes. {@code (a, b) OVERLAPS (c, d)} is written as two pairs
     * and declared as four arguments, so the pairs are read apart before the signature is looked
     * up; otherwise the call is judged against an arity nothing declares.
     */
    private static List<Expression> flattenRowArguments(List<Expression> args) {
        boolean anyRow = false;
        for (Expression arg : args) {
            if (arg instanceof ArrayExpr && ((ArrayExpr) arg).isRow()) { anyRow = true; break; }
        }
        if (!anyRow) return args;
        List<Expression> flat = new ArrayList<>();
        for (Expression arg : args) {
            if (arg instanceof ArrayExpr && ((ArrayExpr) arg).isRow()) {
                flat.addAll(((ArrayExpr) arg).elements());
            } else {
                flat.add(arg);
            }
        }
        return flat;
    }

    /**
     * The most elements PostgreSQL will build an array of: what fits in the largest allocation it
     * makes, one pointer per element.
     */
    private static final long MAX_ARRAY_ELEMENTS = 134217727L;

    /**
     * The bytes a bytea value carries.
     *
     * <p>memgres holds one as a {@code byte[]}, and reading it through {@code toString} handed the
     * hash and the reverse of a Java array identity -- {@code [B@54c26376} -- which is neither the
     * value nor anything the user wrote. A value that arrived as text is read as its own bytes,
     * which is what the cast from text to bytea does with it.
     */
    static byte[] byteaOf(Object value) {
        if (value instanceof byte[]) return (byte[]) value;
        return value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /** A literal of no type: a string, or a bare NULL. */    /** A literal of no type: a string, or a bare NULL. */
    private static boolean isUntypedLiteral(Expression expr) {
        if (!(expr instanceof Literal)) return false;
        Literal.LiteralType type = ((Literal) expr).literalType();
        return type == Literal.LiteralType.STRING || type == Literal.LiteralType.NULL;
    }

    /**
     * The type the query text itself gives an argument, or null for PostgreSQL's {@code unknown}.
     * Only a cast or a literal counts. A column or a nested call is deliberately left out: the
     * engine's runtime value carries whatever Java class it was stored as, and narrowing a call
     * on the strength of that would refuse overloads PostgreSQL resolves.
     */
    private static String writtenArgType(Expression expr) {
        if (expr instanceof CastExpr) return canonicalTypeName(((CastExpr) expr).typeName());
        if (expr instanceof Literal) {
            switch (((Literal) expr).literalType()) {
                case INTEGER: return "integer";
                case FLOAT: return "numeric";
                case BOOLEAN: return "boolean";
                default: return null; // a string or NULL literal is unknown
            }
        }
        return null;
    }

    /** True when an argument of one type reaches a parameter of another without an explicit cast. */
    private static boolean acceptsImplicitly(String argType, String paramType) {
        if (argType.equals(paramType)) return true;
        ArgCategory ac = categoryOf(argType), pc = categoryOf(paramType);
        if (ac == null || ac != pc) return false;
        if (ac == ArgCategory.NUMERIC) return numericRank(argType) <= numericRank(paramType);
        return ac == ArgCategory.STRING; // the string types all convert to each other
    }

    /**
     * Narrows overloaded candidates to the one PostgreSQL would call, or returns null to leave the
     * choice to the arity-and-hint match. PG resolves a call from the types written in the query:
     * it keeps the candidates every argument reaches, prefers exact matches, then prefers the
     * category's preferred type, and finally reads an untyped literal as the string type when a
     * candidate takes one. Choosing the first same-arity candidate instead made f(int)/f(float8)
     * answer with the int one for f('6') and f(6.0), which PostgreSQL sends to float8.
     *
     * <p>The rule engages only when every argument's type is written in the query and every
     * candidate is a plain list of IN parameters of types this understands. Anything else — a
     * column argument, a default, a VARIADIC, a domain or a composite parameter — is left to the
     * existing path, because a wrong narrowing here rejects SQL PostgreSQL runs.
     */
    private List<PgFunction> narrowByDeclaredArgTypes(List<PgFunction> overloads,
                                                      FunctionCallExpr fn, boolean callHasNamedArgs) {
        if (callHasNamedArgs || overloads.size() < 2) return null;
        List<String> argTypes = new ArrayList<>();
        boolean anyUnknown = false;
        for (Expression arg : fn.args()) {
            String t = writtenArgType(arg);
            if (t == null) {
                if (!(arg instanceof Literal)) return null; // a column or a call: no opinion
                anyUnknown = true;
            } else if (categoryOf(t) == null) {
                return null; // a cast to a type these rules say nothing about
            }
            argTypes.add(t);
        }

        List<PgFunction> candidates = new ArrayList<>();
        for (PgFunction f : overloads) {
            List<String> params = plainInputParamTypes(f);
            // One candidate of a shape these rules do not model leaves the whole call undecided:
            // discarding it could send the call to an overload PostgreSQL would not have chosen.
            if (params == null) return null;
            if (params.size() != argTypes.size()) continue;
            boolean reachable = true;
            for (int i = 0; i < params.size(); i++) {
                if (argTypes.get(i) == null) continue; // unknown reaches anything
                if (!acceptsImplicitly(argTypes.get(i), params.get(i))) { reachable = false; break; }
            }
            if (reachable) candidates.add(f);
        }
        if (candidates.isEmpty()) return new ArrayList<>(); // nothing matches: no such function
        if (candidates.size() == 1) return candidates;

        candidates = keepBest(candidates, argTypes, true);
        if (candidates.size() == 1) return candidates;
        candidates = keepBest(candidates, argTypes, false);
        if (candidates.size() == 1) return candidates;
        candidates = narrowUnknownArgs(candidates, argTypes);
        if (candidates == null) return null;
        if (candidates.size() == 1) return candidates;
        if (!anyUnknown) return null; // ambiguous only among typed args: leave it to the old path
        throw new MemgresException("function " + fn.name() + "(" + unknownSignature(argTypes)
                + ") is not unique\n  Hint: Could not choose a best candidate function."
                + " You might need to add explicit type casts.", "42725");
    }

    /**
     * Keeps the candidates matching the most argument positions exactly, or — when
     * {@code exact} is false — on the most preferred types of the arguments' categories.
     */
    private List<PgFunction> keepBest(List<PgFunction> candidates, List<String> argTypes, boolean exact) {
        int bestScore = -1;
        List<PgFunction> best = new ArrayList<>();
        for (PgFunction f : candidates) {
            List<String> params = plainInputParamTypes(f);
            int score = 0;
            for (int i = 0; i < argTypes.size(); i++) {
                String arg = argTypes.get(i);
                if (arg == null) continue;
                String param = params.get(i);
                if (exact ? arg.equals(param)
                        : param.equals(preferredTypeOf(categoryOf(arg)))) score++;
            }
            if (score > bestScore) { bestScore = score; best.clear(); }
            if (score == bestScore) best.add(f);
        }
        return best;
    }

    /**
     * Applies PostgreSQL's last rule: at each argument the query left untyped, a candidate taking
     * a string type wins because an untyped literal looks like one; failing that all candidates
     * must agree on a category, and the category's preferred type wins within it. Returns null
     * when a candidate's parameter falls outside these categories, where guessing is unsafe.
     */
    private List<PgFunction> narrowUnknownArgs(List<PgFunction> candidates, List<String> argTypes) {
        for (int i = 0; i < argTypes.size() && candidates.size() > 1; i++) {
            if (argTypes.get(i) != null) continue;
            Set<ArgCategory> categories = new LinkedHashSet<>();
            for (PgFunction f : candidates) {
                ArgCategory c = categoryOf(plainInputParamTypes(f).get(i));
                if (c == null) return null;
                categories.add(c);
            }
            ArgCategory chosen = categories.contains(ArgCategory.STRING) ? ArgCategory.STRING
                    : categories.size() == 1 ? categories.iterator().next() : null;
            if (chosen == null) continue; // no clue at this position; PG reports it as ambiguous
            List<PgFunction> kept = new ArrayList<>();
            for (PgFunction f : candidates) {
                if (categoryOf(plainInputParamTypes(f).get(i)) == chosen) kept.add(f);
            }
            String preferred = preferredTypeOf(chosen);
            List<PgFunction> preferredOnly = new ArrayList<>();
            for (PgFunction f : kept) {
                if (plainInputParamTypes(f).get(i).equals(preferred)) preferredOnly.add(f);
            }
            candidates = preferredOnly.isEmpty() ? kept : preferredOnly;
        }
        return candidates;
    }

    /** The signature PostgreSQL prints for a call it could not resolve. */
    private static String unknownSignature(List<String> argTypes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < argTypes.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(argTypes.get(i) == null ? "unknown" : argTypes.get(i));
        }
        return sb.toString();
    }

    /**
     * The canonical types of a function's parameters when it takes nothing but plain IN
     * parameters of types these rules model, or null when it does not.
     */
    private static List<String> plainInputParamTypes(PgFunction f) {
        List<String> types = new ArrayList<>();
        for (PgFunction.Param p : f.getParams()) {
            String mode = p.mode();
            if (mode != null && !"IN".equalsIgnoreCase(mode)) return null;
            if (p.defaultExpr() != null) return null;
            String t = canonicalTypeName(p.typeName());
            if (t == null || categoryOf(t) == null) return null;
            types.add(t);
        }
        return types;
    }

    /** True for a string literal written without a cast, which is PostgreSQL's {@code unknown}. */
    private static boolean isUntypedStringLiteral(Expression expr) {
        return expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.STRING;
    }

    /**
     * Reads an untyped literal as the type of the value it is being compared with. Only a numeric
     * counterpart is handled: every other type either already compares as text or has a spelling
     * this would have to guess at, and guessing rejects input PostgreSQL accepts.
     */
    /**
     * GREATEST and LEAST settle on one type across their arguments before comparing anything, so
     * an untyped literal beside an integer is read as an integer: {@code GREATEST('10', 9)} is
     * the number 10, not the text '10' that sorts before '9'. Answering from the raw literal made
     * the value's type disagree with the one the column advertises.
     */
    private static void readUntypedLiteralsAsSettledType(List<Expression> args, List<Object> vals) {
        Object typed = null;
        for (int i = 0; i < args.size() && i < vals.size(); i++) {
            if (!isUntypedStringLiteral(args.get(i)) && vals.get(i) != null) {
                typed = vals.get(i);
                break;
            }
        }
        if (typed == null) return;
        for (int i = 0; i < args.size() && i < vals.size(); i++) {
            if (isUntypedStringLiteral(args.get(i))) {
                vals.set(i, readUntypedLiteralAs(vals.get(i), typed));
            }
        }
    }

    private static Object readUntypedLiteralAs(Object literalValue, Object typedValue) {
        if (!(literalValue instanceof String) || !(typedValue instanceof Number)) return literalValue;
        String text = ((String) literalValue).trim();
        try {
            BigDecimal read = new BigDecimal(text);
            // The literal takes the settled type, not a wider one: GREATEST('10', 9) is an
            // integer 10 and pg_typeof says so, where a BigDecimal would have said numeric.
            if (typedValue instanceof Integer || typedValue instanceof Short) {
                return read.intValueExact();
            }
            if (typedValue instanceof Long) return read.longValueExact();
            if (typedValue instanceof Double || typedValue instanceof Float) {
                return read.doubleValue();
            }
            return read;
        } catch (ArithmeticException e) {
            throw new MemgresException("invalid input syntax for type "
                    + numericTypeNameOf(typedValue) + ": \"" + literalValue + "\"", "22P02");
        } catch (NumberFormatException e) {
            throw new MemgresException("invalid input syntax for type "
                    + numericTypeNameOf(typedValue) + ": \"" + literalValue + "\"", "22P02");
        }
    }

    /** The type name PostgreSQL prints when an untyped literal will not read as this number. */
    private static String numericTypeNameOf(Object value) {
        if (value instanceof Integer || value instanceof Short) return "integer";
        if (value instanceof Long) return "bigint";
        if (value instanceof Double || value instanceof Float) return "double precision";
        return "numeric";
    }

    /**
     * PostgreSQL binds an {@code anyarray} parameter from the type its argument is declared to
     * have, and a bare literal has none — {@code array_length('{1,2,3}', 1)} is an error there
     * rather than an array. Only what the query wrote is inspected, so a cast, an ARRAY
     * constructor, a column and a nested function call all carry a type and keep working.
     */
    private static void requireDeclaredArrayArg(Expression arg) {
        if (!(arg instanceof Literal)) return;
        Literal.LiteralType type = ((Literal) arg).literalType();
        if (type == Literal.LiteralType.STRING || type == Literal.LiteralType.NULL) {
            throw new MemgresException(
                    "could not determine polymorphic type because input has type unknown", "42804");
        }
    }

    /**
     * True when a value is an array in PostgreSQL's text form, either {@code {a,b}} or the
     * {@code [0:2]={a,b,c}} form an array with a non-default lower bound is written with.
     */
    private static boolean isPgArrayText(Object value) {
        if (!(value instanceof String)) return false;
        String body = pgArrayBody((String) value);
        return body.startsWith("{") && body.endsWith("}");
    }

    /** The {@code {...}} part of an array's text form, with any {@code [lb:ub]=} prefix removed. */
    private static String pgArrayBody(String text) {
        return ArrayLiteral.body(text);
    }

    /** The keys an {@code ?|}/{@code ?&} style argument names, from a text[] value or its text form. */
    private List<String> jsonbKeyArg(Object value) {
        if (value instanceof List<?>) {
            List<String> keys = new ArrayList<>();
            for (Object o : (List<?>) value) keys.add(o == null ? null : o.toString());
            return keys;
        }
        String s = value.toString().trim();
        if (s.startsWith("{") && s.endsWith("}")) {
            List<String> keys = new ArrayList<>();
            for (Object o : parseSimplePgArray(s)) keys.add(o == null ? null : o.toString());
            return keys;
        }
        throw new MemgresException("malformed array literal: \"" + s + "\"", "22P02");
    }

    /** Returns true if the string can be parsed as a number. */
    static boolean isNumericString(String s) {
        if (s == null || Strs.isBlank(s)) return false;
        String t = s.trim();
        if (t.equalsIgnoreCase("infinity") || t.equalsIgnoreCase("-infinity") || t.equalsIgnoreCase("nan")) return true;
        try { Double.parseDouble(t); return true; } catch (NumberFormatException e) { return false; }
    }

    /** Recursively flatten a (possibly nested) array into a flat list of scalar elements. */
    static List<Object> flattenArray(List<?> list) {
        List<Object> out = new ArrayList<>();
        flattenArrayInto(list, out);
        return out;
    }

    private static void flattenArrayInto(List<?> list, List<Object> out) {
        for (Object o : list) {
            if (o instanceof List<?>) flattenArrayInto((List<?>) o, out);
            else out.add(o);
        }
    }

    /** Recursively count all leaf elements in a nested list. */
    private static int countLeafElements(List<?> list) {
        int count = 0;
        for (Object elem : list) {
            if (elem instanceof List<?>) count += countLeafElements((List<?>) elem);
            else count++;
        }
        return count;
    }

    /** Build a filled multi-dimensional array string. */
    private String buildFilledArray(Object fillVal, List<?> dims, int dimIdx) {
        if (dimIdx >= dims.size()) {
            return fillVal == null ? "NULL" : fillVal.toString();
        }
        int size = ((Number) dims.get(dimIdx)).intValue();
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < size; i++) {
            if (i > 0) sb.append(",");
            sb.append(buildFilledArray(fillVal, dims, dimIdx + 1));
        }
        sb.append("}");
        return sb.toString();
    }

    /** An array_fill dimension or lower-bound argument as a list, or null when it is not one. */
    private static List<?> arrayFillBounds(Object arg) {
        if (arg instanceof List<?>) return (List<?>) arg;
        if (arg instanceof String && ((String) arg).startsWith("{")) {
            return parseSimplePgArray((String) arg);
        }
        return null;
    }

    /** Count elements in a PG array inner string, respecting quoted strings and braces. */
    static int countArrayElements(String inner) {
        if (inner.isEmpty()) return 0;
        int count = 1;
        boolean inQuote = false;
        int braceDepth = 0;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (inQuote) {
                if (c == '\\' && i + 1 < inner.length()) { i++; continue; }
                if (c == '"') inQuote = false;
            } else {
                if (c == '"') inQuote = true;
                else if (c == '{') braceDepth++;
                else if (c == '}') braceDepth--;
                else if (c == ',' && braceDepth == 0) count++;
            }
        }
        return count;
    }

    /**
     * Parse a PG array literal like {a,"b,c",NULL,{1,2}} into a List.
     * Honors PG array literal syntax: double-quoted elements (commas/braces inside
     * quotes are literal, backslash escapes both inside and outside quotes),
     * unquoted NULL keyword becomes SQL null while quoted "NULL" stays a string,
     * and nested braces produce nested Lists.
     */
    static List<Object> parseSimplePgArray(String s) {
        if (s == null) return Cols.listOf();
        // An array with a lower bound other than 1 carries it in front of the braces; the elements
        // themselves are written the same way, so the bounds are simply skipped here.
        s = pgArrayBody(s);
        if (!s.startsWith("{") || !s.endsWith("}")) return Cols.listOf();
        int[] pos = {1};
        return parsePgArrayBody(s, pos);
    }

    /** Parse array body starting just after an opening brace; consumes the matching closing brace. */
    private static List<Object> parsePgArrayBody(String s, int[] pos) {
        List<Object> result = new ArrayList<>();
        skipArrayWhitespace(s, pos);
        if (pos[0] < s.length() && s.charAt(pos[0]) == '}') {
            pos[0]++;
            return result;
        }
        while (pos[0] < s.length()) {
            skipArrayWhitespace(s, pos);
            char c = pos[0] < s.length() ? s.charAt(pos[0]) : '}';
            if (c == '{') {
                pos[0]++;
                result.add(parsePgArrayBody(s, pos));
            } else if (c == '"') {
                // Quoted element: commas/braces are literal; \x escapes to x
                pos[0]++;
                StringBuilder sb = new StringBuilder();
                while (pos[0] < s.length()) {
                    char q = s.charAt(pos[0]);
                    if (q == '\\' && pos[0] + 1 < s.length()) {
                        sb.append(s.charAt(pos[0] + 1));
                        pos[0] += 2;
                    } else if (q == '"') {
                        pos[0]++;
                        break;
                    } else {
                        sb.append(q);
                        pos[0]++;
                    }
                }
                result.add(sb.toString());
            } else {
                // Unquoted token until top-level ',' or '}'
                StringBuilder sb = new StringBuilder();
                while (pos[0] < s.length() && s.charAt(pos[0]) != ',' && s.charAt(pos[0]) != '}') {
                    char u = s.charAt(pos[0]);
                    if (u == '\\' && pos[0] + 1 < s.length()) {
                        sb.append(s.charAt(pos[0] + 1));
                        pos[0] += 2;
                    } else {
                        sb.append(u);
                        pos[0]++;
                    }
                }
                result.add(parseUnquotedArrayElement(sb.toString().trim()));
            }
            skipArrayWhitespace(s, pos);
            if (pos[0] < s.length() && s.charAt(pos[0]) == ',') {
                pos[0]++;
                continue;
            }
            if (pos[0] < s.length() && s.charAt(pos[0]) == '}') {
                pos[0]++;
            }
            break;
        }
        return result;
    }

    private static void skipArrayWhitespace(String s, int[] pos) {
        while (pos[0] < s.length() && Character.isWhitespace(s.charAt(pos[0]))) pos[0]++;
    }

    /** Convert an unquoted array element: NULL keyword → null, numeric → Integer/Long, else text. */
    private static Object parseUnquotedArrayElement(String t) {
        if (t.equalsIgnoreCase("NULL")) return null;
        try { return Integer.valueOf(t); } catch (NumberFormatException e) { /* not int */ }
        try { return Long.valueOf(t); } catch (NumberFormatException e) { /* not long */ }
        return t;
    }

    /**
     * Split a string containing top-level comma-separated sub-arrays, respecting brace nesting.
     */
    static List<String> splitTopLevelSubArrays(String s) {
        List<String> result = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '{') depth++;
            else if (c == '}') depth--;
            else if (c == ',' && depth == 0) {
                result.add(s.substring(start, i).trim());
                start = i + 1;
            }
        }
        String last = s.substring(start).trim();
        if (!last.isEmpty()) result.add(last);
        return result;
    }

    // ---- OVERLAPS helper methods ----

    private List<?> extractRowValues(Object val) {
        if (val instanceof AstExecutor.PgRow) return ((AstExecutor.PgRow) val).values();
        if (val instanceof List<?>) return (List<?>) val;
        throw new MemgresException("OVERLAPS arguments must be row constructors", "42804");
    }

    private java.time.temporal.Temporal toTemporal(Object val) {
        if (val instanceof java.time.LocalDate) return (java.time.LocalDate) val;
        if (val instanceof java.time.LocalDateTime) return (java.time.LocalDateTime) val;
        if (val instanceof java.time.OffsetDateTime) return (java.time.OffsetDateTime) val;
        if (val instanceof java.time.LocalTime) return ((java.time.LocalTime) val).atDate(java.time.LocalDate.of(1970, 1, 1));
        String s = val.toString().trim();
        try { return java.time.LocalDate.parse(s); } catch (Exception ignored) {}
        try { return java.time.LocalDateTime.parse(s.replace(" ", "T")); } catch (Exception ignored) {}
        try { return java.time.OffsetDateTime.parse(s); } catch (Exception ignored) {}
        throw new MemgresException("cannot convert value to temporal type for OVERLAPS: " + s, "42804");
    }

    /**
     * Resolve the second element of an OVERLAPS pair, which is either an endpoint or a length.
     * Returns null when the endpoint is unknown — either because it is NULL, or because it is a
     * length measured from a start that is itself unknown.
     */
    private java.time.temporal.Temporal resolveOverlapEnd(java.time.temporal.Temporal start, Object endOrInterval) {
        if (endOrInterval == null) return null;
        if (endOrInterval instanceof java.time.LocalDate || endOrInterval instanceof java.time.LocalDateTime
                || endOrInterval instanceof java.time.OffsetDateTime) {
            return toTemporal(endOrInterval);
        }
        if (start == null) return null;
        // Handle PgInterval: add interval to start date/timestamp
        if (endOrInterval instanceof PgInterval) {
            PgInterval iv = (PgInterval) endOrInterval;
            if (start instanceof java.time.LocalDate) return iv.addTo((java.time.LocalDate) start);
            if (start instanceof java.time.LocalDateTime) return iv.addTo((java.time.LocalDateTime) start);
            if (start instanceof java.time.OffsetDateTime) return iv.addTo((java.time.OffsetDateTime) start);
        }
        // Try as a date/timestamp string
        String s = endOrInterval.toString().trim();
        try { return toTemporal(endOrInterval); } catch (Exception ignored) {}
        // Try parsing as interval string and add to start
        PgInterval iv = PgInterval.parse(s);
        if (start instanceof java.time.LocalDate) return iv.addTo((java.time.LocalDate) start);
        if (start instanceof java.time.LocalDateTime) return iv.addTo((java.time.LocalDateTime) start);
        if (start instanceof java.time.OffsetDateTime) return iv.addTo((java.time.OffsetDateTime) start);
        throw new MemgresException("unsupported temporal type for OVERLAPS", "42804");
    }

    @SuppressWarnings("unchecked")
    private int compareTemporal(java.time.temporal.Temporal a, java.time.temporal.Temporal b) {
        if (a instanceof java.time.LocalDate && b instanceof java.time.LocalDate) {
            return ((java.time.LocalDate) a).compareTo((java.time.LocalDate) b);
        }
        if (a instanceof java.time.LocalDateTime && b instanceof java.time.LocalDateTime) {
            return ((java.time.LocalDateTime) a).compareTo((java.time.LocalDateTime) b);
        }
        if (a instanceof java.time.OffsetDateTime && b instanceof java.time.OffsetDateTime) {
            return ((java.time.OffsetDateTime) a).compareTo((java.time.OffsetDateTime) b);
        }
        // Mixed types: convert both to LocalDateTime for comparison
        java.time.LocalDateTime la = toLocalDateTime(a);
        java.time.LocalDateTime lb = toLocalDateTime(b);
        return la.compareTo(lb);
    }

    private java.time.LocalDateTime toLocalDateTime(java.time.temporal.Temporal t) {
        if (t instanceof java.time.LocalDateTime) return (java.time.LocalDateTime) t;
        if (t instanceof java.time.LocalDate) return ((java.time.LocalDate) t).atStartOfDay();
        if (t instanceof java.time.OffsetDateTime) return ((java.time.OffsetDateTime) t).toLocalDateTime();
        throw new MemgresException("cannot convert to LocalDateTime for comparison", "42804");
    }

    /**
     * A polymorphic signature only accepts arguments that bind its slots consistently — an
     * anyarray slot needs an array, an anynonarray slot needs a scalar, and every anyelement
     * slot must land on the same type. PG reports a call that cannot bind as no such function.
     */
    private void checkPolymorphicArgs(List<PgFunction.Param> inputParams, List<Object> args,
                                      List<Expression> argExprs, RowContext ctx, String name) {
        List<String> declared = new ArrayList<>();
        boolean anyPolymorphic = false;
        for (PgFunction.Param p : inputParams) {
            declared.add(p.typeName());
            if (PolymorphicTypes.isPolymorphic(p.typeName())) anyPolymorphic = true;
        }
        if (!anyPolymorphic) return;
        List<String> actual = new ArrayList<>();
        for (int i = 0; i < args.size(); i++) {
            // A NULL value still carries a type when the expression has one (a cast or a column);
            // only a bare NULL literal is the "unknown" PG refuses to resolve a polymorph from.
            String t = PolymorphicTypes.actualTypeName(args.get(i));
            if (t == null && i < argExprs.size()) t = declaredTypeOf(argExprs.get(i), ctx);
            actual.add(t);
        }
        PolymorphicTypes.Binding binding = PolymorphicTypes.bind(declared, actual);
        if (binding == null) {
            StringBuilder argTypes = new StringBuilder();
            for (int i = 0; i < actual.size(); i++) {
                if (i > 0) argTypes.append(", ");
                argTypes.append(actual.get(i) == null ? "unknown" : actual.get(i));
            }
            throw new MemgresException("function " + name + "(" + argTypes + ") does not exist", "42883");
        }
        for (String d : declared) {
            if (PolymorphicTypes.concreteType(d, binding) == null) {
                throw new MemgresException(
                        "could not determine polymorphic type because input has type unknown", "42804");
            }
        }
    }

    /** The static type of an argument expression, used when its runtime value is NULL. */
    private String declaredTypeOf(Expression expr, RowContext ctx) {
        try {
            return PolymorphicTypes.typeName(executor.exprEvaluator.inferTypeFromContext(
                    expr, ctx != null ? ctx.getBindings() : new ArrayList<RowContext.TableBinding>()));
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** "function unnest(integer[], text[]) does not exist", named after the arguments written. */
    private MemgresException noMultiArgUnnest(FunctionCallExpr fn, RowContext ctx) {
        StringBuilder types = new StringBuilder();
        for (Expression arg : fn.args()) {
            if (types.length() > 0) types.append(", ");
            DataType t = executor.exprEvaluator.inferTypeFromContext(arg,
                    ctx != null ? ctx.getBindings() : new ArrayList<RowContext.TableBinding>());
            types.append(t == null ? "unknown" : t.toRegtypeDisplay());
        }
        MemgresException e = new MemgresException(
                "function unnest(" + types + ") does not exist", "42883");
        e.setHint("No function matches the given name and argument types."
                + " You might need to add explicit type casts.");
        e.setPositionToken("unnest");
        return e;
    }

    /** Every argument of a call, evaluated left to right. */
    private List<Object> evaluatedArgs(FunctionCallExpr fn, RowContext ctx) {
        List<Object> values = new ArrayList<>(fn.args().size());
        for (Expression arg : fn.args()) values.add(executor.evalExpr(arg, ctx));
        return values;
    }

    /** Strip well-known schema prefixes (pg_catalog., information_schema.) from a function name. */
    static String stripSchemaPrefix(String name) {
        if (name.startsWith("pg_catalog.")) {
            return name.substring("pg_catalog.".length());
        }
        if (name.startsWith("information_schema.")) {
            return name.substring("information_schema.".length());
        }
        return name;
    }

    /**
     * The name to resolve a call by, with a qualifier removed only where it names the schema the
     * function is really in.
     *
     * <p>A qualifier is part of the call, not decoration on it: {@code pg_catalog.abs(1)} is abs
     * because abs lives in pg_catalog, and {@code information_schema.abs(1)} is a function that
     * does not exist, which is what PostgreSQL says. information_schema holds only its own
     * helpers, every one of them named {@code _pg_*}, so those keep resolving through it and
     * nothing else does.
     *
     * <p>Separate from {@link #stripSchemaPrefix}, which the placement checks use to classify a
     * name rather than to resolve one, and which must keep answering for both prefixes.
     */
    /**
     * The name a call resolves by. The lexer folds an unquoted identifier and leaves a quoted one
     * as written, so a name that still carries a capital was written in quotes: {@code "ABS"} is
     * not abs, and PostgreSQL finds no function of that name at all. Folding it anyway answered a
     * call PostgreSQL refuses. A name the user declared is looked for as written first, so a
     * function created under a quoted mixed-case name keeps working.
     */
    private String foldedName(String written) {
        String folded = written.toLowerCase(Locale.ROOT);
        if (written.equals(folded)) return folded;
        if (executor.database.getFunction(written) != null) return folded;
        if (executor.database.getFunction(folded) != null) return folded;
        return written;
    }

    static String stripCallableSchemaPrefix(String name) {
        if (name.startsWith("pg_catalog.")) {
            return name.substring("pg_catalog.".length());
        }
        if (name.startsWith("information_schema._pg_")) {
            return name.substring("information_schema.".length());
        }
        return name;
    }

    // ---- UUID v3/v5 helpers ----

    private static java.util.UUID uuid3(java.util.UUID namespace, String name) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            md.update(uuidToBytes(namespace));
            md.update(name.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            byte[] hash = md.digest();
            hash[6] = (byte) ((hash[6] & 0x0F) | 0x30); // version 3
            hash[8] = (byte) ((hash[8] & 0x3F) | 0x80); // variant RFC4122
            return bytesToUuid(hash);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new MemgresException("MD5 not available", "XX000");
        }
    }

    private static java.util.UUID uuid5(java.util.UUID namespace, String name) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-1");
            md.update(uuidToBytes(namespace));
            md.update(name.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            byte[] hash = md.digest();
            hash[6] = (byte) ((hash[6] & 0x0F) | 0x50); // version 5
            hash[8] = (byte) ((hash[8] & 0x3F) | 0x80); // variant RFC4122
            return bytesToUuid(hash);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new MemgresException("SHA-1 not available", "XX000");
        }
    }

    private static byte[] uuidToBytes(java.util.UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        byte[] out = new byte[16];
        for (int i = 0; i < 8; i++) out[i] = (byte) (msb >>> (56 - i * 8));
        for (int i = 0; i < 8; i++) out[8 + i] = (byte) (lsb >>> (56 - i * 8));
        return out;
    }

    private static java.util.UUID bytesToUuid(byte[] bytes) {
        long msb = 0, lsb = 0;
        for (int i = 0; i < 8; i++) msb = (msb << 8) | (bytes[i] & 0xFF);
        for (int i = 8; i < 16; i++) lsb = (lsb << 8) | (bytes[i] & 0xFF);
        return new java.util.UUID(msb, lsb);
    }

    // ---- Trigram helper ----

    private static Set<String> trigramSet(String s) {
        Set<String> trgms = new HashSet<>();
        String padded = "  " + s + " ";
        for (int i = 0; i <= padded.length() - 3; i++) {
            trgms.add(padded.substring(i, i + 3));
        }
        return trgms;
    }

    // ---- Levenshtein distance ----

    private static int levenshteinDistance(String s, String t) {
        int m = s.length(), n = t.length();
        int[] prev = new int[n + 1], curr = new int[n + 1];
        for (int j = 0; j <= n; j++) prev[j] = j;
        for (int i = 1; i <= m; i++) {
            curr[0] = i;
            for (int j = 1; j <= n; j++) {
                int cost = s.charAt(i - 1) == t.charAt(j - 1) ? 0 : 1;
                curr[j] = Math.min(Math.min(curr[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
            }
            int[] tmp = prev; prev = curr; curr = tmp;
        }
        return prev[n];
    }

    // ---- Soundex ----

    private static String computeSoundex(String s) {
        if (s == null || s.isEmpty()) return "0000";
        s = s.toUpperCase();
        // Strip non-alpha
        StringBuilder alpha = new StringBuilder();
        for (char c : s.toCharArray()) {
            if (c >= 'A' && c <= 'Z') alpha.append(c);
        }
        if (alpha.length() == 0) return "0000";
        char first = alpha.charAt(0);
        String map = "01230120022455012623010202"; // A=0, B=1, C=2, ...
        StringBuilder code = new StringBuilder();
        code.append(first);
        char lastCode = map.charAt(first - 'A');
        for (int i = 1; i < alpha.length() && code.length() < 4; i++) {
            char c = alpha.charAt(i);
            char mc = map.charAt(c - 'A');
            if (mc != '0' && mc != lastCode) {
                code.append(mc);
            }
            lastCode = mc;
        }
        while (code.length() < 4) code.append('0');
        return code.toString();
    }
}
