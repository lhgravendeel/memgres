package com.memgres.engine;

import com.memgres.engine.parser.ast.ArraySubqueryExpr;
import com.memgres.engine.parser.ast.BinaryExpr;
import com.memgres.engine.parser.ast.CastExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.ExistsExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.SubqueryExpr;
import com.memgres.engine.parser.ast.TableConstraint;
import com.memgres.engine.parser.ast.UnaryExpr;
import com.memgres.engine.util.Cols;

import java.util.Set;

/**
 * Definition-time checks PostgreSQL makes before it will record a table or column definition.
 *
 * <p>These all reject definitions PostgreSQL considers incoherent — a column that shadows a
 * system column, a DEFAULT that can never be evaluated, a CHECK that is not a predicate, an
 * identity column whose type has no sequence behind it. Accepting them stores a definition
 * PostgreSQL could not have produced, and the contradiction surfaces later at some unrelated
 * statement (an INSERT, a catalog read) that then looks like the culprit.
 */
public final class DdlDefinitionChecks {

    private DdlDefinitionChecks() {
    }

    /**
     * The system columns every table carries. A user column of the same name would shadow the
     * system one, so anything reading {@code ctid} or {@code xmin} from that table would get the
     * user's data instead.
     */
    private static final Set<String> SYSTEM_COLUMN_NAMES = Cols.setOf(
            "ctid", "xmin", "cmin", "xmax", "cmax", "tableoid");

    /** Types whose values are varlena, and so can be stored out of line or compressed. */
    private static final Set<DataType> VARLENA_TYPES = Cols.setOf(
            DataType.NUMERIC, DataType.VARCHAR, DataType.CHAR, DataType.TEXT,
            DataType.INET, DataType.CIDR, DataType.BYTEA, DataType.JSON, DataType.JSONB,
            DataType.TSVECTOR, DataType.PATH, DataType.POLYGON, DataType.XML,
            DataType.BIT, DataType.VARBIT, DataType.HSTORE, DataType.RECORD,
            DataType.INT4RANGE, DataType.INT8RANGE, DataType.NUMRANGE, DataType.DATERANGE,
            DataType.TSRANGE, DataType.TSTZRANGE,
            DataType.INT4MULTIRANGE, DataType.INT8MULTIRANGE, DataType.NUMMULTIRANGE,
            DataType.DATEMULTIRANGE, DataType.TSMULTIRANGE, DataType.TSTZMULTIRANGE);

    /** Types that carry a collation. Everything else rejects a COLLATE clause outright. */
    private static final Set<DataType> COLLATABLE_TYPES = Cols.setOf(
            DataType.TEXT, DataType.VARCHAR, DataType.CHAR, DataType.NAME,
            DataType.TEXT_ARRAY, DataType.VARCHAR_ARRAY, DataType.CHAR_ARRAY, DataType.NAME_ARRAY);

    // ---- range subtypes ----

    /**
     * Types PostgreSQL can put no two values of in order: it has no default btree operator class
     * for them and none for any type they are binary-coercible to either. Measured against
     * PostgreSQL 18 rather than read off its opclass list, because the list is not the whole rule —
     * a varchar borrows text's class and a regclass borrows oid's, so neither belongs here even
     * though neither has a class of its own.
     */
    private static final Set<DataType> UNORDERED_TYPES = Cols.setOf(
            DataType.JSON, DataType.XML, DataType.XID,
            DataType.POINT, DataType.LINE, DataType.LSEG, DataType.BOX,
            DataType.PATH, DataType.POLYGON, DataType.CIRCLE);

    /**
     * A range keeps its bounds in order, and it is the subtype's default btree operator class that
     * puts them there — so a subtype nothing can order is one no range can be built over.
     * PostgreSQL says so when the type is defined, not when a value of it is first written.
     */
    public static void requireOrderableRangeSubtype(DataType subtype) {
        if (subtype == null || !UNORDERED_TYPES.contains(subtype)) return;
        throw new MemgresException("data type " + CatalogHelper.pgTypeName(subtype)
                + " has no default operator class for access method \"btree\""
                + "\n  Hint: You must specify an operator class for the range type or define a"
                + " default operator class for the subtype.", "42704");
    }

    // ---- column names ----

    /**
     * {@code 42701} when the name would shadow one of the system columns. The comparison is
     * case-sensitive on purpose: unquoted names have already been folded to lower case, so a
     * name that still has capitals was quoted and is a different identifier from {@code xmax}.
     */
    public static void rejectSystemColumnName(String name) {
        if (name != null && SYSTEM_COLUMN_NAMES.contains(name)) {
            throw new MemgresException("column name \"" + name
                    + "\" conflicts with a system column name", "42701");
        }
    }

    // ---- DEFAULT expressions ----

    /**
     * A DEFAULT is evaluated with no row and no query in scope, so a subquery or a column
     * reference in one can never produce a value. PostgreSQL refuses the definition rather than
     * storing a default that would fail at every insert.
     */
    public static void validateDefaultExpression(Expression expr) {
        if (expr == null) return;
        if (AstWalk.anyMatch(expr, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                return n instanceof SubqueryExpr || n instanceof ExistsExpr
                        || n instanceof ArraySubqueryExpr || n instanceof SelectStmt;
            }
        })) {
            throw PgErrors.notImplemented("cannot use subquery in DEFAULT expression");
        }
        if (AstWalk.anyMatch(expr, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) { return n instanceof ColumnRef; }
        })) {
            throw PgErrors.notImplemented("cannot use column reference in DEFAULT expression");
        }
    }

    /** Functions that change something when called, so calling one to inspect it is not free. */
    private static final Set<String> SIDE_EFFECTING_FUNCTIONS = Cols.setOf("nextval", "setval");

    /**
     * Whether a DEFAULT can be evaluated now purely to see what it produces. Most expressions can:
     * running {@code now()} to find it is a timestamp costs nothing. {@code nextval} is different
     * — evaluating it consumes a sequence value, so the first row inserted would come out with the
     * second number. PostgreSQL type-checks a default without ever running it, and this is the one
     * place where the difference between checking and running is visible.
     */
    public static boolean isEvaluableAtDefinitionTime(Expression expr) {
        if (expr == null) return false;
        return !AstWalk.anyMatch(expr, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                return n instanceof FunctionCallExpr
                        && ((FunctionCallExpr) n).name != null
                        && SIDE_EFFECTING_FUNCTIONS.contains(
                                ((FunctionCallExpr) n).name.toLowerCase(java.util.Locale.ROOT));
            }
        });
    }

    // ---- CHECK / POLICY predicates ----

    /**
     * {@code 42804} when the expression is plainly of some non-boolean type, and {@code 22P02}
     * when a bare string literal stands where the condition does and is not a word boolean's
     * input function knows. PostgreSQL names the context ({@code CHECK}, {@code POLICY}) and the
     * type it found. Expressions whose type this engine cannot decide statically are left alone
     * rather than guessed at, and so are the conditions written inside this one.
     */
    public static void requireBooleanPredicate(Expression expr, Table table, String context) {
        BooleanContext.check(expr, context, BooleanContext.Types.of(table));
    }

    /**
     * The type a DEFAULT expression plainly has, where that is decidable without a full type
     * resolver: a cast names its type, and a literal that is not the untyped string kind carries
     * one of its own. An integer literal's type follows its magnitude, the way PostgreSQL's lexer
     * settles it — {@code 2147483648} is a bigint, not an integer.
     *
     * <p>Returns null when the type is not decidable here, which leaves the caller to judge the
     * expression by what it evaluates to rather than guessing.
     */
    public static String defaultExpressionTypeName(Expression expr, Object value) {
        if (expr instanceof CastExpr) {
            DataType dt = DataType.fromPgName(baseTypeName(((CastExpr) expr).typeName()));
            boolean isArray = ((CastExpr) expr).typeName() != null
                    && ((CastExpr) expr).typeName().replaceAll("\\(.*\\)", "").trim().endsWith("[]");
            if (dt == null) return null;
            // An array is a type in its own right, and PostgreSQL names it as one where a column
            // cannot take it: a default of integer[] on an integer column is a mismatch and not a
            // value to be read.
            return isArray ? dt.toRegtypeDisplay() + "[]" : dt.toRegtypeDisplay();
        }
        if (expr instanceof UnaryExpr && ((UnaryExpr) expr).op() == UnaryExpr.UnaryOp.NEGATE) {
            return defaultExpressionTypeName(((UnaryExpr) expr).operand(), value);
        }
        if (expr instanceof Literal) {
            switch (((Literal) expr).literalType()) {
                case INTEGER: return runtimeTypeName(value);
                case FLOAT: return "numeric";
                case BOOLEAN: return "boolean";
                default: return null; // a bare string literal is still of type unknown
            }
        }
        return null;
    }

    /**
     * True for a bare string literal, which is still of type {@code unknown}. PostgreSQL reports
     * a bad one as invalid input for the column's type; anything else already has a type of its
     * own, so the same failure is a type mismatch instead.
     */
    public static boolean isUntypedLiteral(Expression expr) {
        return expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.STRING;
    }

    /**
     * The PostgreSQL type name for an evaluated value, used to say what type a default expression
     * turned out to be. Returns null for values whose type this mapping does not cover.
     */
    public static String runtimeTypeName(Object value) {
        if (value instanceof java.time.OffsetDateTime) return "timestamp with time zone";
        if (value instanceof java.time.LocalDateTime) return "timestamp without time zone";
        if (value instanceof java.time.LocalDate) return "date";
        if (value instanceof java.time.OffsetTime) return "time with time zone";
        if (value instanceof java.time.LocalTime) return "time without time zone";
        if (value instanceof PgInterval) return "interval";
        if (value instanceof String) return "text";
        if (value instanceof Boolean) return "boolean";
        if (value instanceof java.util.UUID) return "uuid";
        if (value instanceof byte[]) return "bytea";
        if (value instanceof java.math.BigDecimal) return "numeric";
        if (value instanceof Double || value instanceof Float) return "double precision";
        if (value instanceof Long) return "bigint";
        if (value instanceof Integer || value instanceof Short) return "integer";
        return null;
    }

    // ---- COLLATE ----

    /**
     * {@code 42804} when a COLLATE clause names a type that carries no collation.
     *
     * <p>This is the last of the three questions one COLLATE clause raises, and PostgreSQL asks it
     * last: what the written type is, whether the collation named exists, and only then whether the
     * two go together. So the caller resolves the type and the collation first and this judges what
     * came out -- a clause written on a type that does not exist, or naming a collation that does
     * not, is reported as that rather than as a type with no collation.
     *
     * <p>A domain is a type of its own wherever PostgreSQL prints one: whether it carries a
     * collation is its base type's answer, given under the domain's own name. The serial shorthand
     * is named by the integer type it stands for, which is the type the column would have had. An
     * enum, a composite and a range are types of their own too, and none of them carries a
     * collation at all -- so the answer is theirs and not that of the representation this engine
     * stores their values in, under which an enum reads as a label and a composite or a range as
     * the text it prints as, all three of them collatable.
     */
    public static void rejectUncollatableType(String typeName, DdlExecutor.ResolvedType resolved,
                                              String collation) {
        if (collation == null) return;
        boolean isArray = typeName != null
                && typeName.replaceAll("\\(.*\\)", "").trim().endsWith("[]");
        if (resolved != null && resolved.domainTypeName() != null) {
            DataType base = integerBehindSerial(resolved.dataType());
            if (base == null || COLLATABLE_TYPES.contains(base)) return;
            throw PgErrors.datatypeMismatch("collations are not supported by type "
                    + resolved.domainDisplayName() + (isArray ? "[]" : ""));
        }
        if (resolved != null && resolved.userTypeDisplayName() != null) {
            throw PgErrors.datatypeMismatch("collations are not supported by type "
                    + resolved.userTypeDisplayName() + (isArray ? "[]" : ""));
        }
        DataType dt = integerBehindSerial(DataType.fromPgName(baseTypeName(typeName)));
        if (dt == null || COLLATABLE_TYPES.contains(dt)) return;
        throw PgErrors.datatypeMismatch("collations are not supported by type "
                + dt.toRegtypeDisplay() + (isArray ? "[]" : ""));
    }

    /**
     * The integer type a serial column is really of. The shorthand is not a type, so it is not one
     * PostgreSQL ever names: a complaint about a serial column names smallint, integer or bigint.
     */
    private static DataType integerBehindSerial(DataType dt) {
        if (dt == DataType.SMALLSERIAL) return DataType.SMALLINT;
        if (dt == DataType.SERIAL) return DataType.INTEGER;
        if (dt == DataType.BIGSERIAL) return DataType.BIGINT;
        return dt;
    }

    // ---- identity ----

    /** {@code 22023} — identity is fed by a sequence, so only the integer types can carry it. */
    public static void requireIdentityType(DataType dt) {
        if (dt == DataType.SMALLINT || dt == DataType.INTEGER || dt == DataType.BIGINT
                || dt == DataType.SMALLSERIAL || dt == DataType.SERIAL || dt == DataType.BIGSERIAL) {
            return;
        }
        throw PgErrors.invalidParameter(
                "identity column type must be smallint, integer, or bigint");
    }

    // ---- storage and compression ----

    /**
     * Translate a SET STORAGE option to its {@code pg_attribute.attstorage} code, rejecting both
     * an unknown option ({@code 22023}) and a real one the column's type cannot hold
     * ({@code 0A000}) — a fixed-length value is never stored out of line.
     */
    public static String storageCode(String storageType, Column col) {
        String code;
        switch (storageType.toUpperCase(java.util.Locale.ROOT)) {
            case "PLAIN": code = "p"; break;
            case "EXTERNAL": code = "e"; break;
            case "EXTENDED": code = "x"; break;
            case "MAIN": code = "m"; break;
            case "DEFAULT": code = null; break;
            default:
                throw PgErrors.invalidParameter("invalid storage type \"" + storageType + "\"");
        }
        if (!"p".equals(code) && !isVarlena(col)) {
            throw PgErrors.notImplemented("column data type " + typeDisplay(col)
                    + " can only have storage PLAIN");
        }
        return code;
    }

    /**
     * Translate a SET COMPRESSION method to its {@code pg_attribute.attcompression} code.
     * Compression only applies to varlena values, and only the two built-in methods exist.
     */
    public static String compressionCode(String method, Column col) {
        String code;
        if ("pglz".equals(method)) code = "p";
        else if ("lz4".equals(method)) code = "l";
        else if ("default".equals(method)) code = "";
        else throw PgErrors.invalidParameter("invalid compression method \"" + method + "\"");
        if (!isVarlena(col)) {
            throw PgErrors.notImplemented("column data type " + typeDisplay(col)
                    + " does not support compression");
        }
        return code;
    }

    /** True when the column holds varlena values: an array, or one of the varlena base types. */
    private static boolean isVarlena(Column col) {
        if (col.getArrayElementType() != null) return true;
        DataType dt = col.getType();
        if (dt == null) return false;
        if (dt.getPgName().startsWith("_")) return true;
        return VARLENA_TYPES.contains(dt);
    }

    /** The column's type as PostgreSQL spells it in an error about that type. */
    private static String typeDisplay(Column col) {
        if (col.getEnumTypeName() != null) return TypeNamespace.nameOfKey(col.getEnumTypeName());
        if (col.getDomainTypeName() != null) return TypeNamespace.nameOfKey(col.getDomainTypeName());
        DataType dt = col.getType();
        if (dt == null) return "unknown";
        String base = dt.toRegtypeDisplay();
        return col.getArrayElementType() != null ? base + "[]" : base;
    }

    /** Strip typmod and array brackets, leaving the bare type name. */
    private static String baseTypeName(String typeName) {
        if (typeName == null) return "";
        return typeName.replaceAll("\\(.*\\)", "").replace("[]", "").trim();
    }

    // ---- key column lists ----

    /**
     * The columns a key names have to be the relation's own, and each may be named once.
     * PostgreSQL settles both before it stores the constraint; a key over a column that is not
     * there was stored with an attribute number nothing answers to, and enforced nothing at all.
     *
     * @param kind how PostgreSQL words the constraint in the duplicate message: "primary key",
     *     "unique" or "exclusion"
     */
    public static void validateKeyColumns(Table table, java.util.List<String> columns, String kind) {
        if (columns == null || table == null) return;
        Set<String> seen = new java.util.HashSet<String>();
        for (String written : columns) {
            if (written == null || isExpressionKeyElement(written)) continue;
            rejectSystemKeyColumn(written);
            if (table.getColumnIndex(written) < 0) {
                throw new MemgresException("column \"" + written
                        + "\" named in key does not exist", "42703");
            }
            if (!seen.add(written.toLowerCase(java.util.Locale.ROOT))) {
                throw new MemgresException("column \"" + written + "\" appears twice in "
                        + kind + " constraint", "42701");
            }
        }
    }

    /**
     * A key is a btree index, so each column it names must have a btree operator class. json has
     * none: it defines no ordering, and not even an equality, because one document may be written
     * down in more ways than one. jsonb has both and is allowed.
     */
    public static void requireKeyColumnOpclass(Table table, java.util.List<String> columns) {
        if (columns == null || table == null) return;
        String am = "btree";
        for (String written : columns) {
            if (written == null || isExpressionKeyElement(written)) continue;
            int at = table.getColumnIndex(written);
            if (at < 0) continue;   // a column that is not there is reported by the caller
            String typeName = DdlIndexValidator.indexedTypeName(table.getColumns().get(at));
            // A type this engine cannot name is left alone: refusing a valid key is the worse
            // failure of the two.
            if (typeName == null || DdlIndexValidator.defaultOpclass(am, typeName) != null) {
                continue;
            }
            MemgresException e = new MemgresException("data type " + typeName
                    + " has no default operator class for access method \"" + am + "\"", "42704");
            e.setHint("You must specify an operator class for the index or define a default"
                    + " operator class for the data type.");
            throw e;
        }
    }

    /**
     * The existence half alone, for the payload columns of an {@code INCLUDE} clause: those are
     * carried in the index rather than compared, so naming one twice is not a key collision.
     */
    public static void requireKeyColumnsExist(Table table, java.util.List<String> columns) {
        if (columns == null || table == null) return;
        for (String written : columns) {
            if (written == null || isExpressionKeyElement(written)) continue;
            rejectSystemKeyColumn(written);
            if (table.getColumnIndex(written) < 0) {
                throw new MemgresException("column \"" + written
                        + "\" named in key does not exist", "42703");
            }
        }
    }

    /**
     * A system column written where an index column belongs.
     *
     * <p>What PostgreSQL says depends on the column's type rather than on its being a system one:
     * {@code xid} and {@code cid} have no btree operator class to build an index with and are
     * refused for that, where {@code tid} and {@code oid} have one and so reach the rule that no
     * index may be built over a system column at all.
     */
    public static void rejectSystemKeyColumn(String column) {
        if (!isSystemColumnName(column)) return;
        String lower = column.toLowerCase(java.util.Locale.ROOT);
        String unindexable = lower.equals("xmin") || lower.equals("xmax") ? "xid"
                : lower.equals("cmin") || lower.equals("cmax") ? "cid" : null;
        if (unindexable == null) throw indexOnSystemColumn();
        MemgresException e = new MemgresException("data type " + unindexable
                + " has no default operator class for access method \"btree\"", "42704");
        e.setHint("You must specify an operator class for the index or define a default"
                + " operator class for the data type.");
        throw e;
    }

    /** What an index over a system column is refused as, wherever the column was written. */
    public static MemgresException indexOnSystemColumn() {
        return new MemgresException(
                "index creation on system columns is not supported", "0A000");
    }

    /**
     * True when the key element is an expression rather than a bare column name. An expression key
     * is resolved when it is evaluated, so there is no name here to look up.
     */
    static boolean isExpressionKeyElement(String written) {
        return written.startsWith("__using_index__:")
                || written.indexOf('(') >= 0 || written.indexOf(' ') >= 0
                || written.indexOf('+') >= 0 || written.indexOf('-') >= 0
                || written.indexOf('*') >= 0 || written.indexOf('/') >= 0;
    }

    /**
     * An exclusion constraint is enforced by an index that can answer "does any stored value stand
     * in this relation to the new one", and only some access methods can. PostgreSQL names the
     * method rather than storing a constraint that would never fire.
     */
    public static void requireExclusionCapableAccessMethod(String method) {
        if (method == null) return;
        String m = method.toLowerCase(java.util.Locale.ROOT);
        if (m.equals("btree") || m.equals("gist") || m.equals("spgist")) return;
        throw PgErrors.notImplemented("access method \"" + method
                + "\" does not support exclusion constraints");
    }

    /**
     * {@code serial} is CREATE TABLE shorthand for an integer column with a sequence behind it,
     * not a type: no type of that name exists, so naming one where a type is expected is 42704.
     * The column-definition paths keep accepting it, which is the one place the shorthand means
     * something.
     */
    public static void rejectSerialPseudotype(String written) {
        if (written == null) return;
        String bare = baseTypeName(written);
        String lower = bare.toLowerCase(java.util.Locale.ROOT);
        if (lower.equals("serial") || lower.equals("bigserial") || lower.equals("smallserial")
                || lower.equals("serial2") || lower.equals("serial4") || lower.equals("serial8")) {
            throw PgErrors.undefinedObject("type", bare);
        }
    }

    /**
     * A system column's value is settled by the write itself, so a CHECK cannot be evaluated
     * against one: PostgreSQL refuses the constraint rather than storing one that would read a
     * column the row does not carry yet.
     */
    /** True for a name every relation carries whether or not anybody declared it. */
    static boolean isSystemColumnName(String name) {
        return name != null && SYSTEM_COLUMN_NAMES.contains(name.toLowerCase(java.util.Locale.ROOT));
    }

    public static void rejectSystemColumnInCheck(Expression expr) {
        if (expr == null) return;
        Object found = AstWalk.findFirst(expr, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                return n instanceof ColumnRef
                        && ((ColumnRef) n).column() != null
                        && SYSTEM_COLUMN_NAMES.contains(((ColumnRef) n).column().toLowerCase(java.util.Locale.ROOT));
            }
        });
        if (found != null) {
            throw new MemgresException("system column \"" + ((ColumnRef) found).column()
                    + "\" reference in check constraint is invalid", "42P10");
        }
    }

    /**
     * A generated column is settled from the row's own values while the row is being written, and
     * a system column has none of its values yet: where the tuple goes and which transaction wrote
     * it are decided by the write this expression is part of.
     */
    public static void rejectSystemColumnInGeneration(Expression expr) {
        if (expr == null) return;
        Object found = AstWalk.findFirst(expr, new java.util.function.Predicate<Object>() {
            @Override public boolean test(Object n) {
                return n instanceof ColumnRef && isSystemColumnName(((ColumnRef) n).column());
            }
        });
        if (found != null) {
            throw new MemgresException("cannot use system column \""
                    + ((ColumnRef) found).column().toLowerCase(java.util.Locale.ROOT)
                    + "\" in column generation expression", "42P10");
        }
    }

    /** The collations every database has, whatever encoding it was created with. */
    private static final Set<String> BUILTIN_COLLATIONS = Cols.setOf(
            "c", "posix", "default", "ucs_basic", "unicode", "icu_root", "pg_c_utf8",
            "c.utf-8", "c.utf8");

    /**
     * {@code 42704} when a COLLATE clause names a collation the database does not hold. PostgreSQL
     * resolves the name where it is written -- in a column definition, in a domain -- rather than
     * at the first comparison that would have used it, so nothing is ever created carrying a
     * collation that nothing could be sorted by.
     */
    public static void requireCollationExists(Database db, String collation) {
        if (collation == null) return;
        String name = collation.toLowerCase(java.util.Locale.ROOT).replace("\"", "");
        if (name.startsWith("pg_catalog.")) name = name.substring("pg_catalog.".length());
        if (BUILTIN_COLLATIONS.contains(name)) return;
        if (db != null && db.getCollation(name) != null) return;
        throw new MemgresException("collation \"" + collation
                + "\" for encoding \"UTF8\" does not exist", "42704");
    }

    /** The operators that are their own commutator, and so the ones an exclusion may compare with. */
    private static final Set<String> COMMUTATIVE_EXCLUSION_OPERATORS =
            Cols.setOf("=", "<>", "!=", "&&", "~=");

    /**
     * An exclusion constraint asks whether a stored value and a new one stand in some relation, and
     * the answer has to be the same whichever of the two is written first: the index may compare
     * the pair either way round. PostgreSQL takes only an operator that is its own commutator, and
     * names the operator with the types it would have compared. An element whose key is an
     * expression is left alone -- there is no column there whose type could be named.
     */
    public static void requireCommutativeExclusionOperators(
            Table table, java.util.List<TableConstraint.ExcludeElement> elements) {
        if (elements == null || table == null) return;
        for (TableConstraint.ExcludeElement element : elements) {
            String op = element.operator();
            if (op == null || COMMUTATIVE_EXCLUSION_OPERATORS.contains(op)) continue;
            int idx = element.column() == null ? -1 : table.getColumnIndex(element.column());
            if (idx < 0) continue;
            DataType dt = table.getColumns().get(idx).getType();
            if (dt == null) continue;
            String type = dt.toRegtypeDisplay();
            MemgresException e = new MemgresException(
                    "operator " + op + "(" + type + "," + type + ") is not commutative", "42809");
            e.setDetail("Only commutative operators can be used in exclusion constraints.");
            throw e;
        }
    }

    /**
     * An exclusion constraint is enforced by an index, so the operator it compares with has to be
     * one the index's operator class knows about: PostgreSQL looks the operator up in that class's
     * operator family and refuses the constraint when it is not a member. The default access
     * method, taken when no USING is written, is btree, whose only search operators are the five
     * ordering ones — so {@code <>} is refused there although a gist class over the same type
     * carries it.
     *
     * <p>Only {@code <>} is judged. An operator the type does not have at all is a different
     * complaint (there is no operator to look up), and every ordering operator has already been
     * refused as not commutative before this runs.
     */
    public static void requireExclusionOperatorInFamily(
            Table table, java.util.List<TableConstraint.ExcludeElement> elements, String method) {
        if (elements == null || table == null) return;
        if (method != null && !"btree".equalsIgnoreCase(method)) return;
        for (TableConstraint.ExcludeElement element : elements) {
            String op = element.operator();
            if (op == null || !(op.equals("<>") || op.equals("!="))) continue;
            int idx = element.column() == null ? -1 : table.getColumnIndex(element.column());
            if (idx < 0) continue;
            Column col = table.getColumns().get(idx);
            if (col.getEnumTypeName() != null || col.getDomainTypeName() != null
                    || col.getCompositeTypeName() != null || col.getArrayElementType() != null) {
                continue;
            }
            String[] family = btreeOperatorFamily(col.getType());
            if (family == null) continue;
            String operand = family[1] != null ? family[1] : col.getType().toRegtypeDisplay();
            // PostgreSQL names the operator by its own name, so a written != is reported as <>.
            MemgresException e = new MemgresException("operator <>(" + operand + "," + operand
                    + ") is not a member of operator family \"" + family[0] + "\"", "42809");
            e.setDetail("The exclusion operator must be related to the index operator class"
                    + " for the constraint.");
            throw e;
        }
    }

    /**
     * The btree operator family a column type's default operator class belongs to, and the type
     * whose operators that class holds where it is not the column's own — a varchar borrows text's
     * class, so the operator PostgreSQL resolves and names is text's. Null for a type whose class
     * this engine does not model, which leaves the constraint unjudged rather than wrongly refused.
     */
    private static String[] btreeOperatorFamily(DataType dt) {
        if (dt == null) return null;
        switch (dt) {
            case SMALLINT: case INTEGER: case BIGINT: return new String[]{"integer_ops", null};
            case NUMERIC: return new String[]{"numeric_ops", null};
            case REAL: case DOUBLE_PRECISION: return new String[]{"float_ops", null};
            case BOOLEAN: return new String[]{"bool_ops", null};
            case TEXT: return new String[]{"text_ops", null};
            case VARCHAR: return new String[]{"text_ops", "text"};
            case CHAR: return new String[]{"bpchar_ops", null};
            case BYTEA: return new String[]{"bytea_ops", null};
            case UUID: return new String[]{"uuid_ops", null};
            case DATE: case TIMESTAMP: case TIMESTAMPTZ:
                return new String[]{"datetime_ops", null};
            case TIME: return new String[]{"time_ops", null};
            case TIMETZ: return new String[]{"timetz_ops", null};
            case INTERVAL: return new String[]{"interval_ops", null};
            case INET: case CIDR: return new String[]{"network_ops", null};
            case MACADDR: return new String[]{"macaddr_ops", null};
            case MACADDR8: return new String[]{"macaddr8_ops", null};
            case MONEY: return new String[]{"money_ops", null};
            case OID: return new String[]{"oid_ops", null};
            case JSONB: return new String[]{"jsonb_ops", null};
            default: return null;
        }
    }

    /**
     * A bare string literal standing where a value of the column's type is expected is read with
     * that type's input function, so one the type cannot read is refused when the definition is
     * written rather than at the first row that relies on it. Only the numeric types are judged
     * here, which is as far as the DEFAULT path judges one.
     */
    public static void requireUntypedLiteralReadableAs(Expression expr, DataType dataType) {
        if (dataType == null || !isUntypedLiteral(expr)) return;
        if (TypeCoercion.categoryOf(dataType) != TypeCoercion.TypeCategory.NUMERIC) return;
        String written = ((Literal) expr).value();
        if (written == null) return;
        try {
            new java.math.BigDecimal(written);
        } catch (NumberFormatException e) {
            throw new MemgresException("invalid input syntax for type "
                    + dataType.toRegtypeDisplay() + ": \"" + written + "\"", "22P02");
        }
    }

    /**
     * Whether the type a column was declared with holds an array. A type of the reader's own has no
     * array type in this engine's own list -- an array of an enum is recorded as the enum with an
     * element type beside it -- so it is the element that answers rather than the type's name.
     */
    private static boolean holdsAnArray(DdlExecutor.ResolvedType resolved) {
        return resolved.arrayElementType() != null || DataType.isArrayType(resolved.dataType());
    }

    /**
     * A bare string literal standing for an array is read by array input, which settles the shape
     * of the value before it looks at anything in it: text that does not begin an array is refused
     * outright, and what the braces do hold is then read by the element type's own input function,
     * as far as this engine reads one. Read as a single value of the element type instead, 'x' on
     * a text[] column was nobody's complaint and the column kept a default no row could take.
     */
    private static void requireUntypedLiteralReadableAsArray(Expression expr,
                                                             DdlExecutor.ResolvedType resolved) {
        if (!isUntypedLiteral(expr)) return;
        String written = ((Literal) expr).value();
        if (written == null) return;
        DataType element = resolved.arrayElementType() != null ? resolved.arrayElementType()
                : DataType.elementOf(resolved.dataType());
        requireArrayElementsReadableAs(ArrayLiteral.parse(written).elements(), element);
    }

    /** Every element of a literal already read as an array, however many dimensions deep. */
    private static void requireArrayElementsReadableAs(java.util.List<?> elements,
                                                       DataType elementType) {
        if (elementType == null
                || TypeCoercion.categoryOf(elementType) != TypeCoercion.TypeCategory.NUMERIC) {
            return;
        }
        for (Object element : elements) {
            if (element instanceof java.util.List<?>) {
                requireArrayElementsReadableAs((java.util.List<?>) element, elementType);
            } else if (element != null) {
                requireNumberReadableAs((String) element, elementType);
            }
        }
    }

    /**
     * One number read the way its own type reads one. The inexact types also read the three values
     * that are not numbers at all, which is the whole of what tells them from an integer type here.
     */
    private static void requireNumberReadableAs(String written, DataType type) {
        if ((type == DataType.NUMERIC || type == DataType.REAL
                || type == DataType.DOUBLE_PRECISION)
                && NumericLimits.specialNumericOrNull(written) != null) {
            return;
        }
        try {
            new java.math.BigDecimal(written);
        } catch (NumberFormatException e) {
            throw new MemgresException("invalid input syntax for type "
                    + type.toRegtypeDisplay() + ": \"" + written + "\"", "22P02");
        }
    }

    /**
     * A generation expression has to produce a value the column can hold, and PostgreSQL settles
     * that where the column is defined rather than at the first row: it coerces the expression to
     * the column's type in assignment context, and a pair with no such cast is refused. The
     * complaint is the one a DEFAULT of the wrong type gets, down to the words "default
     * expression", because it is the same code that stores both.
     *
     * <p>Only a type this engine can read straight off the expression is judged -- a cast names
     * its own, a literal that is not the untyped string kind carries one, and a reference to a
     * column already declared on this table has that column's. Everything else is left to the row
     * that first relies on it, and so is a column whose type is an enum, a composite or an array,
     * which PostgreSQL names in its own terms rather than in its base type's. A column declared
     * with a domain is judged: PostgreSQL asks for a cast to the type the domain is built over,
     * and names the domain when there is none.
     */
    public static void requireGenerationExprFits(Expression expr, DdlExecutor.ResolvedType resolved,
                                                 String columnName,
                                                 java.util.List<Column> declared) {
        requireStoredExprFits(expr, resolved, columnName, declared);
    }

    /**
     * A DEFAULT written in a column definition is judged the same way, and by the same rule: the
     * expression is coerced to the column's type in assignment context, and a pair with no such
     * cast is refused where the column is defined.
     *
     * <p>Left unjudged, the definition is stored and every INSERT that leaves the column out fails
     * on a value the statement never wrote -- a table nobody can insert into without naming the
     * column, which is not a state PostgreSQL lets a CREATE TABLE reach.
     */
    public static void requireDefaultExprFits(Expression expr, DdlExecutor.ResolvedType resolved,
                                              String columnName,
                                              java.util.List<Column> declared) {
        requireStoredExprFits(expr, resolved, columnName, declared);
    }

    /** The shared rule: what a stored expression produces has to be what the column can hold. */
    private static void requireStoredExprFits(Expression expr, DdlExecutor.ResolvedType resolved,
                                              String columnName,
                                              java.util.List<Column> declared) {
        if (resolved == null) return;
        // Which input function reads a bare literal is the column's type to say, and an array's
        // reads the value's shape before it reads anything in it. Reading one as a single value of
        // the element type asked the wrong reader: it had no objection to 'x' on a text[] column,
        // and it took the whole of '{{1},{a}}' for one number on an int[][] one.
        if (holdsAnArray(resolved)) {
            requireUntypedLiteralReadableAsArray(expr, resolved);
        } else {
            requireUntypedLiteralReadableAs(expr, resolved.dataType());
        }
        // An array is a type of its own, and PostgreSQL asks the same question of it as of any
        // other: there is a coercion from one array to another exactly where there is one between
        // their element types, and none at all from a lone value to an array. Skipping every
        // column that held an array left integer[] taking a DEFAULT of 1.
        String arrayType = builtinArrayTypeName(resolved.dataType());
        if (expr == null || resolved.dataType() == null || resolved.enumTypeName() != null
                || resolved.compositeTypeName() != null
                || (resolved.arrayElementType() != null && arrayType == null)) {
            return;
        }
        String exprType = plainExpressionTypeName(expr, declared);
        if (exprType == null || TypeCoercion.assignableFrom(exprType, resolved.dataType())) return;
        // A domain is the type the column has, so it is the name PostgreSQL puts in the complaint,
        // even though the cast it looked for was one to the type the domain is built over. Saying
        // the base type there names a type the writer never wrote down. It is spelled the way the
        // reader would have spelled it -- bare where the search path reaches the domain, qualified
        // where it does not -- because that is how PostgreSQL prints any type name.
        String columnType = resolved.domainTypeName() != null ? resolved.domainDisplayName()
                : arrayType != null ? arrayType
                : resolved.dataType().toRegtypeDisplay();
        MemgresException e = PgErrors.datatypeMismatch("column \"" + columnName + "\" is of type "
                + columnType
                + " but default expression is of type " + exprType);
        e.setHint("You will need to rewrite or cast the expression.");
        throw e;
    }

    /**
     * An array type spelled the way PostgreSQL spells it in a message -- the element type's own
     * name with brackets after it -- and null for anything that is not one of the array types this
     * engine models. The name pg_type carries for an array is its internal one, {@code _int4},
     * which is not what a complaint about the column would say; and the declared width is left
     * off, because PostgreSQL names the array type rather than the column's modifier.
     */
    private static String builtinArrayTypeName(DataType arrayType) {
        DataType element = DataType.elementOf(arrayType);
        return element == null ? null : element.toRegtypeDisplay() + "[]";
    }

    /**
     * The type an expression plainly has, with a column of the same table resolved against the
     * columns declared before it. Null wherever the type is not decidable from the expression
     * alone, which leaves the caller to judge nothing.
     */
    private static String plainExpressionTypeName(Expression expr, java.util.List<Column> declared) {
        if (expr instanceof UnaryExpr && ((UnaryExpr) expr).op() == UnaryExpr.UnaryOp.NEGATE) {
            return plainExpressionTypeName(((UnaryExpr) expr).operand(), declared);
        }
        if (expr instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) expr;
            if (declared == null) return null;
            for (Column c : declared) {
                if (!c.getName().equalsIgnoreCase(ref.column())) continue;
                if (c.getEnumTypeName() != null || c.getDomainTypeName() != null
                        || c.getCompositeTypeName() != null || c.getArrayElementType() != null
                        || c.getType() == null) {
                    return null;
                }
                return c.getType().toRegtypeDisplay();
            }
            return null;
        }
        if (expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.INTEGER) {
            // An integer literal's type follows its magnitude, the way PostgreSQL's lexer settles
            // it: 2147483648 is a bigint and not an integer.
            String written = ((Literal) expr).value();
            if (written == null) return null;
            try {
                java.math.BigInteger n = new java.math.BigInteger(written.trim());
                if (n.bitLength() < 32) return "integer";
                return n.bitLength() < 64 ? "bigint" : "numeric";
            } catch (NumberFormatException notANumber) {
                return null;
            }
        }
        // Beyond a column of the table being defined, the answer is the one the boolean contexts
        // already settle on: the same literals, operators and calls decide what a DEFAULT produces
        // as decide whether a WHERE is a condition, so one reading of an expression serves both.
        // It stays one-sided -- silence where the type is not certain leaves the definition
        // standing, which is what a wrong answer here would not.
        String settled = BooleanContext.typeOf(expr, BooleanContext.Types.none());
        return settled != null ? settled : untypedOperandsTypeName(expr);
    }

    /**
     * The type PostgreSQL settles on where nothing an expression is written over carries one.
     *
     * <p>A string literal written bare is of type {@code unknown}, so an operator or a call over
     * nothing but those has no argument to take its type from; PostgreSQL resolves each of them to
     * text, the preferred type of the string category. That is why {@code 'a' || 'b'} and
     * {@code coalesce('a','b')} are text rather than untyped, and why an integer column will take
     * neither of them. Where one operand does carry a type the answer is that type's business:
     * {@code 'a' || '{1}'::int[]} reads the literal as an array instead of making the pair text.
     */
    private static String untypedOperandsTypeName(Expression expr) {
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            if (bin.op() != BinaryExpr.BinOp.CONCAT) return null;
            return isUntypedLiteral(bin.left()) && isUntypedLiteral(bin.right()) ? "text" : null;
        }
        if (!(expr instanceof FunctionCallExpr)) return null;
        FunctionCallExpr call = (FunctionCallExpr) expr;
        String name = call.name() == null ? "" : call.name().toLowerCase(java.util.Locale.ROOT);
        if (!name.equals("coalesce") && !name.equals("greatest") && !name.equals("least")) {
            return null;
        }
        if (call.args() == null || call.args().isEmpty()) return null;
        for (Expression arg : call.args()) {
            if (!isUntypedLiteral(arg)) return null;
        }
        return "text";
    }

    /**
     * LIKE copies the shape of a relation that has one. A sequence and an index are relations too
     * and PostgreSQL finds them by name, then refuses them: there are no columns there to copy.
     */
    public static void requireLikeableSource(Database db, String schemaName, String written) {
        if (db == null || written == null) return;
        int dot = written.indexOf('.');
        String schema = dot > 0 ? written.substring(0, dot) : schemaName;
        String bare = dot > 0 ? written.substring(dot + 1) : written;
        String kind = RelationNamespace.kindOf(db, schema, bare);
        if (!RelationNamespace.SEQUENCE.equals(kind) && !RelationNamespace.INDEX.equals(kind)) {
            return;
        }
        MemgresException e = new MemgresException(
                "relation \"" + bare + "\" is invalid in LIKE clause", "42809");
        e.setDetail("This operation is not supported for "
                + RelationNamespace.pluralKind(kind) + ".");
        throw e;
    }

    /**
     * A relation has at most one primary key. A second one written in the same CREATE TABLE, or
     * added to a table that already has one, is refused as a fault in the definition rather than
     * stored as a second constraint nothing would ever consult.
     */
    static void rejectSecondPrimaryKey(Table table, String tableName) {
        int keys = 0;
        for (StoredConstraint sc : table.getConstraints()) {
            if (sc.getType() == StoredConstraint.Type.PRIMARY_KEY) keys++;
        }
        if (keys > 1) {
            throw new MemgresException("multiple primary keys for table \"" + tableName
                    + "\" are not allowed", "42P16");
        }
    }
}
