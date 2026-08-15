package com.memgres.engine;

import com.memgres.engine.parser.ast.AnyAllArrayExpr;
import com.memgres.engine.parser.ast.AnyAllExpr;
import com.memgres.engine.parser.ast.BetweenExpr;
import com.memgres.engine.parser.ast.BinaryExpr;
import com.memgres.engine.parser.ast.CaseExpr;
import com.memgres.engine.parser.ast.CastExpr;
import com.memgres.engine.parser.ast.CollateExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.ExistsExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.InExpr;
import com.memgres.engine.parser.ast.IsBooleanExpr;
import com.memgres.engine.parser.ast.IsJsonExpr;
import com.memgres.engine.parser.ast.IsNullExpr;
import com.memgres.engine.parser.ast.JsonExistsExpr;
import com.memgres.engine.parser.ast.LikeExpr;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.Statement;
import com.memgres.engine.parser.ast.SubqueryExpr;
import com.memgres.engine.parser.ast.UnaryExpr;
import com.memgres.engine.parser.ast.WindowFuncExpr;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The places PostgreSQL requires a boolean, and what it says when it is handed something else.
 *
 * <p>A condition is not "anything that can be read as true or false". PostgreSQL coerces the
 * expression to boolean while it transforms the clause the expression stands in, and a type with
 * no coercion to boolean is refused there and then, naming the clause:
 * {@code argument of WHERE must be type boolean, not type integer} (42804). The clauses that do
 * this are WHERE, HAVING, a JOIN's ON, a searched CASE's WHEN, FILTER, AND, OR, NOT, a CHECK
 * constraint, a policy expression, a partial index's predicate, a rule's qualification, a
 * trigger's WHEN and a MERGE action's AND.
 *
 * <p>A bare string literal is the one exception, and it is the reason this cannot be done by
 * looking at values. {@code WHERE 't'} is accepted: an unadorned literal is of type
 * {@code unknown}, so the context resolves it and boolean's input function reads it. The same
 * literal spelled {@code 'zzz'} is 22P02 rather than 42804, because it is that input function
 * complaining and not the type system. Which of the two errors a statement gets is decided by how
 * the expression was written, never by what it evaluates to — {@code 'x'::text} is 42804 while a
 * bare {@code 'x'} is 22P02.
 *
 * <h3>Why the answer is one-sided</h3>
 *
 * <p>memgres has no parse-analysis phase, so the type of an expression is generally not known
 * until it has been evaluated. {@link #typeOf} therefore answers only where it is certain — a
 * typed literal, a column of a base table, a cast, an operator whose result type does not depend
 * on resolution, a call every signature of which returns one type — and answers null everywhere
 * else. Null means "say nothing", so an expression this cannot type is accepted exactly as before.
 * That asymmetry is deliberate: being silent leaves memgres's old, laxer behaviour, while being
 * wrong would refuse SQL PostgreSQL runs.
 */
final class BooleanContext {

    private BooleanContext() {
    }

    /**
     * What the names in an expression are known to be.
     *
     * <p>Two kinds of relation answer. One is a relation the catalogue holds, whose columns carry
     * the types the user declared. The other is one built from a query — a derived table, a WITH
     * item, a view, a VALUES list, a function in FROM — whose columns carry whatever type the
     * builder read off a value, which is a guess and often wrong; for those the answer comes from
     * {@link DefinedTypes}, which works the type out from the definition instead and says nothing
     * where the definition does not settle it. A column no relation types is a column this says
     * nothing about, because refusing a condition on the strength of a guess rejects valid SQL.
     */
    static final class Types {

        private final Table table;
        private final Set<String> aliases;
        private final List<RowContext.TableBinding> bindings;
        private final AstExecutor executor;

        private Types(Table table, Set<String> aliases,
                      List<RowContext.TableBinding> bindings, AstExecutor executor) {
            this.table = table;
            this.aliases = aliases;
            this.bindings = bindings;
            this.executor = executor;
        }

        /** Nothing is known: only what the expression itself writes down carries a type. */
        static Types none() {
            return new Types(null, null, null, null);
        }

        /** The columns of one relation, for a definition stored against it. */
        static Types of(Table table) {
            Set<String> named = new HashSet<String>();
            if (table != null && table.getName() != null) {
                named.add(table.getName().toLowerCase(Locale.ROOT));
            }
            return new Types(table, named, null, null);
        }

        /** The same, for a definition whose rows answer to names of their own (NEW, OLD). */
        static Types of(Table table, Set<String> rowAliases) {
            Set<String> named = new HashSet<String>();
            for (String alias : rowAliases) named.add(alias.toLowerCase(Locale.ROOT));
            return new Types(table, named, null, null);
        }

        /** The relations a query level has resolved, described rather than read. */
        static Types of(AstExecutor executor, List<RowContext.TableBinding> bindings) {
            return new Types(null, null, bindings, executor);
        }

        String columnType(ColumnRef ref) {
            String column = ref.column();
            if (column == null || "*".equals(column)) return null;
            if (table != null) {
                String qualifier = ref.table();
                if (qualifier != null && !aliases.contains(qualifier.toLowerCase(Locale.ROOT))) {
                    return null;
                }
                return declaredType(table, null, column);
            }
            if (bindings == null || executor == null) return null;
            String found = supplied(bindings, ref, column);
            if (found != null) return found;
            // A name this level DOES supply is this level's, even where its type could not be
            // read -- a derived table, a CTE and a VALUES list all expose columns whose type was
            // inferred rather than declared. Reaching past one of those to an enclosing level
            // types the wrong column, and an inner WHERE over a boolean was refused because the
            // outer relation happened to have an integer of the same name.
            if (exposes(bindings, ref, column)) return null;
            // A name this level does not supply is a correlated reference to an enclosing one,
            // which PostgreSQL resolves and types exactly as it does one of its own.
            for (RowContext outer : executor.outerContextStack) {
                found = supplied(outer.getBindings(), ref, column);
                if (found != null) return found;
                if (exposes(outer.getBindings(), ref, column)) return null;
            }
            return null;
        }

        /** Whether these relations expose the column at all, whatever type it turned out to be. */
        private boolean exposes(List<RowContext.TableBinding> from, ColumnRef ref, String column) {
            if (from == null) return false;
            for (RowContext.TableBinding b : from) {
                if (b.table() == null) continue;
                String exposed = b.alias() != null ? b.alias() : nameOf(b.table());
                if (ref.table() != null
                        && (exposed == null || !ref.table().equalsIgnoreCase(exposed))) {
                    continue;
                }
                if (b.table().getColumnIndex(column) >= 0) return true;
            }
            return false;
        }

        private String supplied(List<RowContext.TableBinding> from, ColumnRef ref, String column) {
            if (from == null) return null;
            String found = null;
            for (RowContext.TableBinding b : from) {
                String type = declaredType(b.table(), ref.table(), column,
                        b.alias() != null ? b.alias() : nameOf(b.table()));
                if (type == null) continue;
                // Two relations answering to one name is an ambiguity, and PostgreSQL says so
                // rather than typing it — even where both of them declare the same type.
                if (found != null) return null;
                found = type;
            }
            return found;
        }

        private static String nameOf(Table table) {
            return table == null ? null : table.getName();
        }

        private String declaredType(Table table, String qualifier, String column) {
            return declaredType(table, qualifier, column, nameOf(table));
        }

        private String declaredType(Table table, String qualifier, String column, String exposed) {
            if (table == null) return null;
            if (qualifier != null && (exposed == null || !qualifier.equalsIgnoreCase(exposed))) {
                return null;
            }
            // A relation built from a query carries the type its definition settles, where the
            // definition settles one; the type the builder read off a value is not that type.
            if (table.hasDefinedColumnTypes()) {
                return table.definedColumnType(table.getColumnIndex(column));
            }
            // Only a relation the catalogue actually holds carries the types the user declared.
            if (bindings != null && executor.baseTableNamed(table.getName()) != table) return null;
            int idx = table.getColumnIndex(column);
            if (idx < 0) return null;
            Column col = table.getColumns().get(idx);
            // An enum or a domain answers to a name of its own, which this does not know.
            if (col.getEnumTypeName() != null || col.getDomainTypeName() != null
                    || col.getCompositeTypeName() != null) {
                return null;
            }
            if (col.getType() == null) return null;
            if (col.getArrayElementType() != null) {
                return col.getArrayElementType().toRegtypeDisplay() + "[]";
            }
            return col.getType().toRegtypeDisplay();
        }

        boolean isUserFunction(String bareName) {
            return executor != null && executor.database.getFunction(bareName) != null;
        }
    }

    // ---- The checks ----

    /**
     * Refuses {@code expr} when the clause it heads requires a boolean and it is certainly not
     * one, then the same for every boolean context written inside it.
     *
     * @param clause the clause name PostgreSQL puts in the message ("WHERE", "HAVING", "JOIN/ON")
     */
    static void check(Expression expr, String clause, Types types) {
        if (expr == null) return;
        require(expr, clause, types);
        scan(expr, types);
    }

    /**
     * The boolean contexts written inside an expression, without requiring the expression itself
     * to be one. This is what a select list, an ORDER BY key or a SET list is put through: nothing
     * there has to be a condition, but a CASE, a FILTER or an AND written there still does.
     */
    static void scan(Object node, Types types) {
        if (node == null || node instanceof Statement) return;
        if (node instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) node;
            String op = bin.op() == BinaryExpr.BinOp.AND ? "AND"
                    : bin.op() == BinaryExpr.BinOp.OR ? "OR" : null;
            if (op != null) {
                // Left to right, the order PostgreSQL transforms the operands in.
                require(bin.left(), op, types);
                scan(bin.left(), types);
                require(bin.right(), op, types);
                scan(bin.right(), types);
                return;
            }
        }
        if (node instanceof UnaryExpr && ((UnaryExpr) node).op() == UnaryExpr.UnaryOp.NOT) {
            Expression operand = ((UnaryExpr) node).operand();
            require(operand, "NOT", types);
            scan(operand, types);
            return;
        }
        if (node instanceof CaseExpr) {
            CaseExpr caseExpr = (CaseExpr) node;
            // A simple CASE compares its WHEN values with the operand, so they are not conditions.
            boolean searched = caseExpr.operand() == null;
            scan(caseExpr.operand(), types);
            if (caseExpr.whenClauses() != null) {
                for (CaseExpr.WhenClause when : caseExpr.whenClauses()) {
                    if (searched) require(when.condition(), "CASE/WHEN", types);
                    scan(when.condition(), types);
                    scan(when.result(), types);
                }
            }
            scan(caseExpr.elseExpr(), types);
            return;
        }
        Expression filter = filterOf(node);
        if (filter != null) {
            // The arguments are transformed before the FILTER predicate is.
            for (Expression arg : argsOf(node)) scan(arg, types);
            require(filter, "FILTER", types);
            scan(filter, types);
            return;
        }
        AstWalk.forEachChild(node, child -> scan(child, types));
    }

    /** Refuses one expression that has to be a condition, saying nothing when it may be one. */
    static void require(Expression expr, String clause, Types types) {
        if (expr == null) return;
        if (isUntypedLiteral(expr)) {
            // Of type unknown, so the context resolves it: boolean's input function reads it, and
            // a word that is not one of the words it knows is its complaint rather than a mismatch.
            TypeCoercion.toBoolean(((Literal) expr).value());
            return;
        }
        String type = typeOf(expr, types);
        if (type == null || "boolean".equals(type)) return;
        throw PgErrors.datatypeMismatch(
                "argument of " + clause + " must be type boolean, not type " + type);
    }

    /** True for a bare string literal, which PostgreSQL still leaves of type {@code unknown}. */
    private static boolean isUntypedLiteral(Expression expr) {
        return expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.STRING
                && ((Literal) expr).value() != null;
    }

    // ---- Typing ----

    /**
     * The PostgreSQL type name this expression certainly has, or null when that cannot be settled
     * without evaluating it.
     */
    static String typeOf(Expression expr, Types types) {
        if (expr == null) return null;
        if (expr instanceof Literal) {
            switch (((Literal) expr).literalType()) {
                case INTEGER: return integerLiteralType(((Literal) expr).value());
                case FLOAT: return "numeric";
                case BOOLEAN: return "boolean";
                // A bare string and a bare NULL are both of type unknown.
                default: return null;
            }
        }
        if (expr instanceof ColumnRef) return types.columnType((ColumnRef) expr);
        if (expr instanceof CastExpr) return castTypeName(((CastExpr) expr).typeName());
        if (expr instanceof CollateExpr) return typeOf(((CollateExpr) expr).expr(), types);
        if (expr instanceof IsNullExpr || expr instanceof IsBooleanExpr
                || expr instanceof IsJsonExpr || expr instanceof LikeExpr
                || expr instanceof BetweenExpr || expr instanceof InExpr
                || expr instanceof ExistsExpr || expr instanceof AnyAllExpr
                || expr instanceof AnyAllArrayExpr || expr instanceof JsonExistsExpr) {
            return "boolean";
        }
        if (expr instanceof UnaryExpr) {
            UnaryExpr unary = (UnaryExpr) expr;
            if (unary.op() == UnaryExpr.UnaryOp.NOT) return "boolean";
            if (unary.op() == UnaryExpr.UnaryOp.NEGATE || unary.op() == UnaryExpr.UnaryOp.POSITIVE) {
                String inner = typeOf(unary.operand(), types);
                return numericRank(inner) > 0 ? inner : null;
            }
            return null;
        }
        if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            if (isBooleanOp(bin.op())) return "boolean";
            if (isArithmetic(bin.op())) {
                return arithmeticResult(typeOf(bin.left(), types), typeOf(bin.right(), types));
            }
            if (isBitwise(bin.op())) {
                return bitwiseResult(bin.op(), typeOf(bin.left(), types), typeOf(bin.right(), types));
            }
            if (bin.op() == BinaryExpr.BinOp.CONCAT) {
                // || over a string is text whatever the other side is written as; over an array,
                // a jsonb or a bit string it is that instead, so only a known string decides it.
                return isStringType(typeOf(bin.left(), types))
                        || isStringType(typeOf(bin.right(), types)) ? "text" : null;
            }
            return null;
        }
        if (expr instanceof CaseExpr) return caseType((CaseExpr) expr, types);
        if (expr instanceof SubqueryExpr) return subqueryType((SubqueryExpr) expr);
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) expr;
            return callType(fn.name(), fn.args(), types);
        }
        if (expr instanceof WindowFuncExpr) {
            WindowFuncExpr wf = (WindowFuncExpr) expr;
            return callType(wf.name(), wf.args(), types);
        }
        return null;
    }

    private static boolean isStringType(String type) {
        return "text".equals(type) || "character varying".equals(type)
                || "character".equals(type) || "name".equals(type);
    }

    /** PostgreSQL's lexer settles an integer literal's type by its magnitude. */
    private static String integerLiteralType(String value) {
        if (value == null) return null;
        try {
            BigInteger n = new BigInteger(value.trim());
            if (n.bitLength() < 32) return "integer";
            if (n.bitLength() < 64) return "bigint";
            return "numeric";
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** The type a cast names, or null when it names one this does not recognise. */
    private static String castTypeName(String written) {
        if (written == null) return null;
        String name = written.trim().toLowerCase(Locale.ROOT);
        int paren = name.indexOf('(');
        if (paren > 0) name = name.substring(0, paren).trim();
        boolean array = name.endsWith("[]");
        if (array) name = name.substring(0, name.length() - 2).trim();
        DataType dt = DataType.fromPgName(name);
        if (dt == null) return null;
        return array ? dt.toRegtypeDisplay() + "[]" : dt.toRegtypeDisplay();
    }

    /** A CASE is typed only when every branch agrees, which is the one case needing no promotion. */
    private static String caseType(CaseExpr expr, Types types) {
        String found = null;
        if (expr.whenClauses() == null || expr.whenClauses().isEmpty()) return null;
        for (CaseExpr.WhenClause when : expr.whenClauses()) {
            String branch = typeOf(when.result(), types);
            if (branch == null) return null;
            if (found != null && !found.equals(branch)) return null;
            found = branch;
        }
        if (expr.elseExpr() != null) {
            // A bare NULL takes the branch type rather than deciding it.
            boolean nullElse = expr.elseExpr() instanceof Literal
                    && ((Literal) expr.elseExpr()).literalType() == Literal.LiteralType.NULL;
            if (!nullElse) {
                String elseType = typeOf(expr.elseExpr(), types);
                if (elseType == null || !elseType.equals(found)) return null;
            }
        }
        return found;
    }

    /**
     * A scalar sub-query is typed by its single output column, and only where that column carries
     * a type of its own — the sub-query's relations are a scope this does not have.
     */
    private static String subqueryType(SubqueryExpr expr) {
        if (!(expr.subquery() instanceof SelectStmt)) return null;
        SelectStmt select = (SelectStmt) expr.subquery();
        if (select.targets() == null || select.targets().size() != 1) return null;
        if (select.from() != null && !select.from().isEmpty()) return null;
        return typeOf(select.targets().get(0).expr(), Types.none());
    }

    /**
     * The type a call returns.
     *
     * <p>Two ways, in order. When every argument's type is certain, the signature whose parameter
     * types are exactly those is the one PostgreSQL resolves to, and its result type is the
     * answer: {@code abs(int)} is an integer where {@code abs} on its own could be six things.
     * Failing that, a name every recorded signature of which returns one and the same concrete
     * type is answered from the name alone. Anything less certain, and anything the user declared
     * — whose body this cannot read — is left untyped.
     */
    private static String callType(String written, List<Expression> args, Types types) {
        if (written == null) return null;
        String bare = FunctionEvaluator.stripSchemaPrefix(written.toLowerCase(Locale.ROOT));
        if (types.isUserFunction(bare)) return null;
        String exact = exactSignatureType(bare, args, types);
        if (exact != null) return exact;
        String value = VALUE_FUNCTION_TYPES.get(bare);
        return value != null ? value : SINGLE_RETURN_TYPES.get(bare);
    }

    /**
     * The SQL value functions the grammar spells as bare keywords. Each of them is a call, but not
     * every one is a call to something pg_catalog holds under that name — there is a now() to look
     * up and no current_timestamp() — so the ones the signature list cannot answer for are named
     * here. Each type was read off PostgreSQL rather than assumed: CURRENT_TIME carries a zone
     * where LOCALTIME does not, and that is the whole of the difference between the two.
     */
    private static final java.util.Map<String, String> VALUE_FUNCTION_TYPES = valueFunctionTypes();

    private static java.util.Map<String, String> valueFunctionTypes() {
        java.util.Map<String, String> types = new java.util.HashMap<String, String>();
        types.put("current_timestamp", DataType.TIMESTAMPTZ.toRegtypeDisplay());
        types.put("localtimestamp", DataType.TIMESTAMP.toRegtypeDisplay());
        types.put("current_date", DataType.DATE.toRegtypeDisplay());
        types.put("current_time", DataType.TIMETZ.toRegtypeDisplay());
        types.put("localtime", DataType.TIME.toRegtypeDisplay());
        types.put("current_role", DataType.NAME.toRegtypeDisplay());
        types.put("current_catalog", DataType.NAME.toRegtypeDisplay());
        return types;
    }

    /**
     * The result type of the signatures whose parameters are exactly these argument types.
     *
     * <p>An unadorned string literal has no type of its own — PostgreSQL calls it {@code unknown}
     * and settles it from the call it stands in — and where the name has a signature taking text
     * there, that is the one PostgreSQL resolves to: {@code upper('a')} is upper(text) and returns
     * text, not the {@code anyelement} its range overloads return. Reading the literal as text is
     * that preference and nothing more; a name with no text signature of that shape stays untyped.
     */
    private static String exactSignatureType(String bare, List<Expression> args, Types types) {
        if (args == null || args.isEmpty()) return null;
        int[] oids = new int[args.size()];
        for (int i = 0; i < args.size(); i++) {
            Expression arg = args.get(i);
            DataType declared = scalarType(typeOf(arg, types));
            if (declared == null && isUntypedStringLiteral(arg)) declared = DataType.TEXT;
            if (declared == null) return null;
            oids[i] = declared.getOid();
        }
        String found = null;
        for (String[][] table : new String[][][]{
                BuiltinAggregateSignatures.AGGREGATES, BuiltinFunctionSignatures.SIGNATURES}) {
            for (String[] sig : table) {
                if (!sig[0].equalsIgnoreCase(bare)) continue;
                // A row memgres wrote for itself names the types it happened to write down, so an
                // argument list matching one settles nothing about which form was resolved to.
                if (sig.length > 4 && !BuiltinFunctionSignatures.isPostgresSignature(sig)) {
                    return null;
                }
                String[] params = sig[2].isEmpty() ? new String[0] : sig[2].split(" ");
                if (params.length != oids.length) continue;
                boolean same = true;
                for (int i = 0; i < params.length; i++) {
                    if (!params[i].equals(String.valueOf(oids[i]))) {
                        same = false;
                        break;
                    }
                }
                if (!same) continue;
                DataType returns = DataType.fromOid(parseOid(sig[1]));
                if (returns == null) return null;
                String display = returns.toRegtypeDisplay();
                if (found != null && !found.equals(display)) return null;
                found = display;
            }
        }
        return found;
    }

    /** A string literal written without a type on it, which PostgreSQL leaves as unknown. */
    private static boolean isUntypedStringLiteral(Expression expr) {
        return expr instanceof Literal
                && ((Literal) expr).literalType() == Literal.LiteralType.STRING;
    }

    private static int parseOid(String oid) {
        try {
            return Integer.parseInt(oid);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /** The scalar type a display name stands for; null for an array or a name this cannot read. */
    private static DataType scalarType(String display) {
        if (display == null || display.endsWith("[]")) return null;
        return DataType.fromPgName(display);
    }

    /** Names every recorded signature of which returns one and the same concrete type. */
    private static final java.util.Map<String, String> SINGLE_RETURN_TYPES = buildReturnTypes();

    private static java.util.Map<String, String> buildReturnTypes() {
        java.util.Map<String, String> single = new java.util.HashMap<String, String>();
        Set<String> conflicting = new HashSet<String>();
        for (String[] sig : BuiltinAggregateSignatures.AGGREGATES) {
            record(single, conflicting, sig[0], sig[1], false);
        }
        for (String[] sig : BuiltinFunctionSignatures.SIGNATURES) {
            // A set-returning call is not a scalar of that type, so it is left alone.
            boolean returnsSet = sig.length > 3 && sig[3] != null && sig[3].startsWith("t");
            record(single, conflicting, sig[0], sig[1], returnsSet);
        }
        for (String name : conflicting) single.remove(name);
        return single;
    }

    private static void record(java.util.Map<String, String> single, Set<String> conflicting,
                               String name, String rettype, boolean returnsSet) {
        String key = name.toLowerCase(Locale.ROOT);
        if (returnsSet) {
            conflicting.add(key);
            return;
        }
        int oid;
        try {
            oid = Integer.parseInt(rettype);
        } catch (NumberFormatException e) {
            conflicting.add(key);
            return;
        }
        DataType dt = DataType.fromOid(oid);
        // A polymorphic or pseudo result is decided by the arguments, which this does not resolve.
        if (dt == null) {
            conflicting.add(key);
            return;
        }
        String display = dt.toRegtypeDisplay();
        String previous = single.put(key, display);
        if (previous != null && !previous.equals(display)) conflicting.add(key);
    }

    // ---- Operators ----

    private static boolean isBooleanOp(BinaryExpr.BinOp op) {
        switch (op) {
            case EQUAL:
            case NOT_EQUAL:
            case LESS_THAN:
            case GREATER_THAN:
            case LESS_EQUAL:
            case GREATER_EQUAL:
            case AND:
            case OR:
            case LIKE:
            case ILIKE:
            case SIMILAR_TO:
            case IS_DISTINCT_FROM:
            case IS_NOT_DISTINCT_FROM:
            case REGEX_MATCH:
            case REGEX_IMATCH:
            case NOT_REGEX_MATCH:
            case NOT_REGEX_IMATCH:
                return true;
            default:
                return false;
        }
    }

    /**
     * The operators PostgreSQL spells the same way for an integer and for something else.
     *
     * <p>{@code >>} shifts an integer, and it also asks whether one network address contains
     * another and whether one range lies wholly to the right of another -- and those answer with a
     * boolean. Which one a query means is settled by what it hands them, so nothing follows from
     * the operator alone.
     */
    private static boolean isBitwise(BinaryExpr.BinOp op) {
        switch (op) {
            case BIT_AND:
            case BIT_OR:
            case BIT_XOR:
            case SHIFT_LEFT:
            case SHIFT_RIGHT:
                return true;
            default:
                return false;
        }
    }

    /**
     * What one of those produces when both sides are certainly integers, and null otherwise.
     *
     * <p>A shift keeps the width of the value being shifted -- the other side is a distance, not a
     * second operand -- so {@code int8 >> int4} is bigint and {@code int4 >> int4} is integer.
     * PostgreSQL declares the bitwise operators only over two integers of the same width, so a
     * mixed pair resolves to nothing this can name.
     */
    private static String bitwiseResult(BinaryExpr.BinOp op, String left, String right) {
        if (!isIntegerType(left) || !isIntegerType(right)) return null;
        if (op == BinaryExpr.BinOp.SHIFT_LEFT || op == BinaryExpr.BinOp.SHIFT_RIGHT) return left;
        return left.equals(right) ? left : null;
    }

    private static boolean isIntegerType(String type) {
        return "smallint".equals(type) || "integer".equals(type) || "bigint".equals(type);
    }

    private static boolean isArithmetic(BinaryExpr.BinOp op) {
        switch (op) {
            case ADD:
            case SUBTRACT:
            case MULTIPLY:
            case DIVIDE:
            case MODULO:
                return true;
            default:
                return false;
        }
    }

    /**
     * What arithmetic over two numeric types yields. Only the numeric family is answered for:
     * {@code +} over a date or an interval means something else, and this says nothing about it.
     */
    private static String arithmeticResult(String left, String right) {
        int l = numericRank(left);
        int r = numericRank(right);
        if (l == 0 || r == 0) return null;
        // PostgreSQL has no operator mixing numeric with a float, so both are cast up to float8.
        if ((l == RANK_NUMERIC && r == RANK_REAL) || (l == RANK_REAL && r == RANK_NUMERIC)) {
            return "double precision";
        }
        return RANKED[Math.max(l, r)];
    }

    private static final int RANK_NUMERIC = 4;
    private static final int RANK_REAL = 5;
    private static final String[] RANKED = {
            null, "smallint", "integer", "bigint", "numeric", "real", "double precision"};

    private static int numericRank(String type) {
        if (type == null) return 0;
        for (int i = 1; i < RANKED.length; i++) {
            if (RANKED[i].equals(type)) return i;
        }
        return 0;
    }

    // ---- Calls carrying a FILTER ----

    private static Expression filterOf(Object node) {
        if (node instanceof FunctionCallExpr) return ((FunctionCallExpr) node).filter();
        if (node instanceof WindowFuncExpr) return ((WindowFuncExpr) node).filter();
        return null;
    }

    private static List<Expression> argsOf(Object node) {
        if (node instanceof FunctionCallExpr) return ((FunctionCallExpr) node).args();
        if (node instanceof WindowFuncExpr) return ((WindowFuncExpr) node).args();
        return java.util.Collections.emptyList();
    }
}
