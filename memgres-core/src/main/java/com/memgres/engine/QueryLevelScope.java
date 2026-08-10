package com.memgres.engine;

import com.memgres.engine.parser.ast.CastExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.Statement;
import com.memgres.engine.parser.ast.WindowFuncExpr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * What one query level supplies, for the checks PostgreSQL makes before it judges a clause.
 *
 * <p>PostgreSQL analyses a statement in a fixed order and that order decides which fault a
 * statement with several of them reports. The range table is built first, so a relation that is
 * not there outranks everything; then each clause is transformed against it, and inside a single
 * call the arguments are transformed, then the FILTER expression — coerced to boolean — and only
 * then is the function itself resolved. A call that turns out to be an ordinary function carrying
 * FILTER, DISTINCT or OVER is the last thing complained about, not the first.
 *
 * <p>memgres resolves names while it runs, so the earlier faults are found later. This class is
 * what the clause-level refusals consult before they fire: given the relations this query level
 * has already resolved, it answers whether a column reference resolves, whether a call resolves,
 * and whether a FILTER predicate is a boolean. Each answer is deliberately one-sided — anything it
 * cannot settle from the bindings it says nothing about — because it only ever runs on the path to
 * a refusal that was going to be raised anyway. Being silent leaves the old, later message; being
 * wrong could only ever swap one error for another, never refuse a statement PostgreSQL runs.
 */
final class QueryLevelScope {

    private final SelectExecutor select;
    private final List<RowContext.TableBinding> bindings;
    private final List<RowContext.OutCol> output;
    private final SelectStmt stmt;
    /** Whether the bindings are every relation the FROM clause names, or only the ones read. */
    private final boolean whole;

    QueryLevelScope(SelectExecutor select, List<RowContext.TableBinding> bindings,
                    List<RowContext.OutCol> output, SelectStmt stmt) {
        this(select, bindings, output, stmt, true);
    }

    QueryLevelScope(SelectExecutor select, List<RowContext.TableBinding> bindings,
                    List<RowContext.OutCol> output, SelectStmt stmt, boolean whole) {
        this.select = select;
        this.bindings = bindings;
        this.output = output;
        this.stmt = stmt;
        this.whole = whole;
    }

    List<RowContext.TableBinding> bindings() {
        return bindings;
    }

    // ---- Columns ----

    /**
     * Refuses the first column reference in {@code node}'s own scope that this level cannot supply.
     *
     * <p>Silent when the scope is not knowable: an enclosing query may supply a name this one does
     * not, and a FROM-function's single column answers to names it does not hold through attribute
     * notation ({@code gs.date} meaning {@code date(gs)}), so neither can be judged from the
     * bindings alone.
     */
    void rejectUnresolvedColumns(Object node) {
        if (node == null || bindings == null || bindings.isEmpty() || !whole) return;
        if (!select.executor.outerContextStack.isEmpty()) return;
        for (RowContext.TableBinding b : bindings) {
            if (b.table() == null) return;
            if (b.table().isFunctionResult()) return;
        }
        List<ColumnRef> refs = new ArrayList<ColumnRef>();
        collectLocalColumnRefs(node, refs);
        for (ColumnRef ref : refs) {
            String column = ref.column();
            if (column == null || "*".equals(column) || SelectExecutor.isSystemColumn(column)) {
                continue;
            }
            if (ref.table() == null) {
                if (suppliesUnqualified(column)) continue;
                MemgresException e = new MemgresException(
                        "column \"" + column + "\" does not exist", "42703");
                String hint = RowContext.suggestClosestColumn(column, bindings);
                if (hint != null) e.setHint(hint);
                throw e;
            }
            List<RowContext.TableBinding> named = bindingsNamed(ref.table());
            // A qualifier this level does not expose names something else — an enclosing relation,
            // a composite value, a type being cast to — so it is not this check's business.
            if (named.isEmpty()) continue;
            boolean found = false;
            for (RowContext.TableBinding b : named) {
                if (b.table().getColumnIndex(column) >= 0) {
                    found = true;
                    break;
                }
            }
            if (found) continue;
            MemgresException e = new MemgresException(
                    "column " + ref.table() + "." + column + " does not exist", "42703");
            String hint = RowContext.suggestClosestColumn(column, named);
            if (hint != null) e.setHint(hint);
            throw e;
        }
    }

    private boolean suppliesUnqualified(String column) {
        if (output != null) {
            for (RowContext.OutCol oc : output) {
                if (oc.name != null && oc.name.equalsIgnoreCase(column)) return true;
            }
        }
        for (RowContext.TableBinding b : bindings) {
            if (b.table().getColumnIndex(column) >= 0) return true;
            // A bare name matching a FROM item is a whole-row reference.
            if (b.alias() != null && b.alias().equalsIgnoreCase(column)) return true;
            if (b.table().getName() != null && b.table().getName().equalsIgnoreCase(column)) {
                return true;
            }
        }
        // ORDER BY and GROUP BY read the select list's output names, so a name defined there is a
        // name this level supplies even though no relation holds it.
        if (stmt != null && stmt.targets() != null) {
            for (SelectStmt.SelectTarget target : stmt.targets()) {
                if (target.alias() != null && target.alias().equalsIgnoreCase(column)) return true;
            }
        }
        return false;
    }

    private List<RowContext.TableBinding> bindingsNamed(String qualifier) {
        List<RowContext.TableBinding> named = new ArrayList<RowContext.TableBinding>();
        for (RowContext.TableBinding b : bindings) {
            // An alias hides the relation's own name, as PostgreSQL scopes it.
            String exposed = b.alias() != null ? b.alias() : b.table().getName();
            if (exposed != null && exposed.equalsIgnoreCase(qualifier)) named.add(b);
        }
        return named;
    }

    /** Every column reference written in this node's own query level. */
    private static void collectLocalColumnRefs(Object node, List<ColumnRef> out) {
        if (node == null || node instanceof Statement) return;
        if (node instanceof ColumnRef) {
            out.add((ColumnRef) node);
            return;
        }
        AstWalk.forEachChild(node, child -> collectLocalColumnRefs(child, out));
    }

    // ---- Types ----

    /**
     * The type an expression certainly has here, or null when this cannot settle it. Only a typed
     * literal and a column of a relation this level supplies are certain; a string literal is not,
     * because PostgreSQL leaves an unadorned one as {@code unknown} and resolves it from context.
     */
    DataType certainTypeOf(Expression expr) {
        if (expr instanceof Literal) {
            Literal lit = (Literal) expr;
            if (lit.literalType() == Literal.LiteralType.INTEGER) return DataType.INTEGER;
            if (lit.literalType() == Literal.LiteralType.FLOAT) return DataType.NUMERIC;
            if (lit.literalType() == Literal.LiteralType.BOOLEAN) return DataType.BOOLEAN;
            return null;
        }
        // A cast says what the value is, whatever it was written as, so its type is certain.
        if (expr instanceof CastExpr) {
            String typeName = ((CastExpr) expr).typeName();
            if (typeName == null || typeName.trim().endsWith("[]")) return null;
            return DataType.fromPgName(typeName.trim().toLowerCase(Locale.ROOT));
        }
        if (!(expr instanceof ColumnRef)) return null;
        if (bindings == null || bindings.isEmpty() || !whole) return null;
        if (!select.executor.outerContextStack.isEmpty()) return null;
        ColumnRef ref = (ColumnRef) expr;
        String column = ref.column();
        if (column == null || "*".equals(column)) return null;
        List<RowContext.TableBinding> candidates =
                ref.table() == null ? bindings : bindingsNamed(ref.table());
        DataType found = null;
        for (RowContext.TableBinding b : candidates) {
            if (b.table() == null || b.table().isFunctionResult()) return null;
            int idx = b.table().getColumnIndex(column);
            if (idx < 0) continue;
            DataType type = certainTypeIn(b.table(), idx);
            // A relation built from a query carries the type its builder read off a value, which
            // is a guess: refusing a statement because one of those was guessed wrong is how an
            // ordinary join on a boolean column came to be rejected. Only the type the relation's
            // definition settles is certain, and there is not always one.
            if (type == null) return null;
            if (found != null && found != type) return null;
            found = type;
        }
        return found;
    }

    /** The type a relation's column certainly has, or null where nothing settles it. */
    private static DataType certainTypeIn(Table table, int index) {
        if (!table.hasDefinedColumnTypes()) return table.getColumns().get(index).getType();
        String defined = table.definedColumnType(index);
        // An array is named "integer[]" and stands for no one DataType here, so it is left alone.
        return defined == null || defined.endsWith("[]") ? null : DataType.fromPgName(defined);
    }

    /**
     * Refuses a FILTER predicate that is certainly not a boolean.
     *
     * <p>FILTER says which rows a call accumulates, so its argument is a condition; PostgreSQL
     * coerces it to boolean while it transforms the call, which is before it resolves the function
     * the FILTER hangs off.
     */
    void rejectNonBooleanFilter(Expression filter) {
        BooleanContext.require(filter, "FILTER",
                BooleanContext.Types.of(select.executor, bindings));
    }

    // ---- Function resolution ----

    /**
     * Refuses a call that resolves to no function: an unknown name, an argument count no signature
     * of that name takes, or argument types no signature of it accepts.
     *
     * <p>PostgreSQL resolves a function by name <em>and</em> argument list before it judges the
     * clause the call stands in, so {@code abs(txt)} is a function that does not exist rather than
     * abs applied to something odd, {@code lpad('a')} is a function that does not exist rather
     * than lpad with a length of its own choosing, and neither ever reaches the complaint about a
     * FILTER, an OVER or a column further along the select list.
     *
     * <p>Each of the three reads a table that has to be complete before it may be read at all.
     * The name is judged against {@link BuiltinFunctionNames#isCallable}, which is the register of
     * everything the engine dispatches rather than the shorter list the catalog reports; the count
     * against {@link BuiltinFunctionSignatures}, which now records every signature PostgreSQL
     * declares and how few arguments each takes; the types against the same table, counting a type
     * as accepted whenever it is in the same family as the parameter, because PostgreSQL casts
     * within a family without being asked.
     *
     * <p>Still one-sided where it has to be. A name a user declared decides for itself, a name the
     * signature table does not record is not judged on its count, and an argument whose type this
     * level cannot settle leaves the call to resolve at evaluation time as before — where the same
     * three questions are asked again of values rather than of bindings.
     *
     * <p>{@code writtenTypesOnly} narrows the last of the three to calls whose arguments say what
     * they are, which is what the unconditional walk asks for. A column's type is the type memgres
     * gave it, and where the two servers spell a catalog column differently the comparison is
     * against the wrong thing: memgres keeps a stored expression as text where PostgreSQL declares
     * pg_node_tree, and numbers a transaction with an integer where PostgreSQL declares xid, so
     * {@code pg_get_expr(prqual, prrelid)} and {@code age(transactionid)} — both of which
     * PostgreSQL runs — resolve to nothing when read that way. A literal and a cast carry a type
     * of their own and are safe to read; on the path to a refusal, where a wrong answer can only
     * exchange one error for another, the check still reads everything.
     */
    void rejectUnresolvableCall(Object call, String bareName) {
        rejectUnresolvableCall(call, bareName, false);
    }

    void rejectUnresolvableCall(Object call, String bareName, boolean writtenTypesOnly) {
        List<Expression> args = argsOf(call);
        if (args == null) return;
        // A qualifier that is not pg_catalog names a schema of the user's, and what is in one is
        // the database's business rather than the register's: {@link #bareName} leaves such a
        // qualifier on, so a name still carrying one is left to resolve where the schema is read.
        if (bareName.indexOf('.') >= 0) return;
        if (select.hasUserFunction(bareName)) return;
        // A type the user declared is callable like a function -- PostgreSQL reads
        // {@code sometype(x)} as a cast -- and declaring a range or a composite mints a
        // constructor of that name besides. None of them is in the register, which holds what the
        // engine ships with rather than what this database has been told about.
        if (namesADeclaredType(bareName)) return;
        // A type name written like a call of one argument is a cast, not a call:
        // {@code numrange(NULL)} is CAST(NULL AS numrange), which PostgreSQL runs and no pg_proc
        // row of that name describes. Reading the row as the whole story refused it.
        if (args.size() == 1 && DataType.fromPgName(bareName) != null) return;
        // The complaint names the argument types, so it is only made where they can be named. An
        // argument this level cannot type is one evaluation will type from its value, and the same
        // three questions are asked there.
        if (!canNameEveryArgument(args)) return;
        // A name nothing answers to resolves to no function whatever it was handed, so this is
        // settled before the arguments are looked at -- as PostgreSQL settles it.
        if (!BuiltinFunctionNames.isCallable(bareName)
                && !select.isAggregateFunction(bareName)
                && !PlacementCheck.isWindowFunctionName(bareName)
                // PostgreSQL keeps the implementation of an operator as an ordinary function, and
                // the catalog lists it. memgres dispatches those by evaluating the operator behind
                // the name, which is a name the register does not carry -- so a check that read the
                // register alone refused calls the engine can answer.
                && !FunctionEvaluator.isOperatorFunction(bareName)) {
            throw noSuchFunction(call, args);
        }
        if (!FunctionEvaluator.acceptsAnyArity(bareName)) {
            if (BuiltinFunctionSignatures.recordsSignature(bareName)
                    && !BuiltinFunctionSignatures.acceptsArity(bareName, args.size())) {
                throw noSuchFunction(call, args);
            }
            if (BuiltinFunctionSignatures.windowCallCannotResolve(bareName, args.size())) {
                throw noSuchFunction(call, args);
            }
        }
        int[] argOids = new int[args.size()];
        for (int i = 0; i < args.size(); i++) {
            Expression arg = args.get(i);
            if (writtenTypesOnly && !(arg instanceof Literal) && !(arg instanceof CastExpr)) return;
            DataType type = certainTypeOf(arg);
            if (type == null) return;
            argOids[i] = type.getOid();
        }
        boolean comparable = false;
        for (String[] signature : BuiltinFunctionSignatures.SIGNATURES) {
            if (!signature[0].equalsIgnoreCase(bareName)) continue;
            // A row memgres wrote for itself is not the whole story about that name, so nothing
            // follows from an argument list failing to match it.
            if (!BuiltinFunctionSignatures.isPostgresSignature(signature)) return;
            String[] params = signature[2].isEmpty() ? new String[0] : signature[2].split(" ");
            boolean variadic = signature.length > 4 && signature[4].contains("+");
            // A variadic signature records its last parameter as the array PostgreSQL collects the
            // arguments into, and a call writes the elements: jsonb_extract_path is declared over
            // (jsonb, text[]) and called with (jsonb, text, text). Reading the recorded type as the
            // one the call must write refused every such call.
            if (variadic ? params.length == 0 || argOids.length < params.length - 1
                         : params.length != argOids.length) {
                continue;
            }
            comparable = true;
            boolean accepts = true;
            int fixed = variadic ? params.length - 1 : params.length;
            for (int i = 0; i < fixed; i++) {
                if (!accepts(argOids[i], Integer.parseInt(params[i]))) {
                    accepts = false;
                    break;
                }
            }
            if (accepts && variadic) {
                int declared = Integer.parseInt(params[params.length - 1]);
                int element = CatalogCoreBuilder.arrayElementOid(declared);
                for (int i = fixed; i < argOids.length; i++) {
                    // VARIADIC t[] may also be handed the array itself.
                    if (accepts(argOids[i], declared)) continue;
                    if (element == 0 || !accepts(argOids[i], element)) {
                        accepts = false;
                        break;
                    }
                }
            }
            if (accepts) return;
        }
        if (!comparable) return;
        throw noSuchFunction(call, args);
    }

    /** Whether this database has been told about a type of that name. */
    private boolean namesADeclaredType(String bareName) {
        Database database = select.executor.database;
        if (database == null) return false;
        return database.isCustomEnum(bareName)
                || database.getDomain(bareName) != null
                || database.getCompositeType(bareName) != null
                || database.isRangeType(bareName)
                || database.isShellType(bareName);
    }

    /**
     * Whether every argument can be given the name PostgreSQL would give it in a 42883: a type
     * this level is certain of, or {@code unknown} for the literals PostgreSQL leaves untyped.
     */
    private boolean canNameEveryArgument(List<Expression> args) {
        for (Expression arg : args) {
            if (certainTypeOf(arg) != null) continue;
            if (!(arg instanceof Literal)) return false;
            Literal.LiteralType kind = ((Literal) arg).literalType();
            if (kind != Literal.LiteralType.STRING && kind != Literal.LiteralType.NULL) {
                return false;
            }
        }
        return true;
    }

    /**
     * "function abs(text) does not exist", named after the arguments as PostgreSQL names them.
     *
     * <p>An unadorned string literal is PostgreSQL's {@code unknown}: it has no type until the
     * function it is handed to gives it one, and a call that resolves to nothing never does. An
     * argument this level cannot type at all is named the same way, which is as much as can be
     * said about it here.
     */
    private MemgresException noSuchFunction(Object call, List<Expression> args) {
        StringBuilder types = new StringBuilder();
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) types.append(", ");
            DataType type = certainTypeOf(args.get(i));
            types.append(type == null ? "unknown" : CatalogHelper.pgTypeName(type));
        }
        MemgresException e = new MemgresException(
                "function " + nameOf(call) + "(" + types + ") does not exist", "42883");
        e.setHint("No function matches the given name and argument types. You might need to add explicit type casts.");
        return e;
    }

    private static List<Expression> argsOf(Object call) {
        if (call instanceof FunctionCallExpr) return ((FunctionCallExpr) call).args();
        if (call instanceof WindowFuncExpr) return ((WindowFuncExpr) call).args();
        return null;
    }

    private static String nameOf(Object call) {
        if (call instanceof FunctionCallExpr) return ((FunctionCallExpr) call).name();
        if (call instanceof WindowFuncExpr) return ((WindowFuncExpr) call).name();
        return null;
    }

    /** The type OIDs PostgreSQL casts between without being asked, grouped by family. */
    private static final int[][] FAMILIES = {
            {21, 23, 20, 1700, 700, 701},                 // numeric
            {25, 1042, 1043, 19},                         // string
            {1082, 1114, 1184},                           // date and timestamp
            {1083, 1266},                                 // time
    };

    /**
     * Where memgres spells a catalog column differently from PostgreSQL, the types it will take.
     *
     * <p>A column's type is the type memgres gave it, and a parameter's is the one PostgreSQL
     * declared, so a catalog the two servers spell differently makes the comparison against the
     * wrong thing: memgres keeps a stored expression as text where PostgreSQL declares
     * {@code pg_node_tree}, and numbers a transaction with an integer where it declares
     * {@code xid}. {@code pg_get_expr(prqual, prrelid)} is a query pg_dump sends, and reading the
     * declared type as the one the column must carry refused it.
     */
    private static final Map<Integer, int[]> SPELLED_DIFFERENTLY = new HashMap<Integer, int[]>();

    static {
        SPELLED_DIFFERENTLY.put(194, new int[]{25, 1042, 1043});      // pg_node_tree <- text
        SPELLED_DIFFERENTLY.put(28, new int[]{21, 23, 20});           // xid <- an integer
        SPELLED_DIFFERENTLY.put(5069, new int[]{21, 23, 20});         // xid8 <- an integer
        SPELLED_DIFFERENTLY.put(29, new int[]{21, 23, 20});           // cid <- an integer
    }

    private static final Set<Integer> ACCEPTS_ANYTHING = new HashSet<Integer>();

    static {
        // A pseudo-type parameter is resolved from the actual argument, so it accepts one.
        for (String name : PolymorphicTypes.names()) ACCEPTS_ANYTHING.add(PolymorphicTypes.oid(name));
        ACCEPTS_ANYTHING.add(2276); // any
        ACCEPTS_ANYTHING.add(2249); // record
        ACCEPTS_ANYTHING.add(705);  // unknown
        ACCEPTS_ANYTHING.add(2281); // internal
    }

    /**
     * The conversions PostgreSQL performs without being asked, read off {@link PgCastTable}.
     *
     * <p>A parameter takes an argument of another type when pg_cast records an implicit entry
     * between them, which is the rule PostgreSQL resolves a call by. Reading the cast table rather
     * than a list written here keeps the two from disagreeing: it is what says
     * {@code pg_get_viewdef(regclass)} is pg_get_viewdef(oid), and that
     * {@code format_type(1043, 104)} hands two integers to a function declared over oid and int4.
     */
    private static final Set<Long> IMPLICIT_CASTS = implicitCasts();

    private static Set<Long> implicitCasts() {
        Set<Long> pairs = new HashSet<Long>();
        for (Object[] cast : PgCastTable.CASTS) {
            if (!"i".equals(cast[3])) continue;
            long source = ((Number) cast[0]).longValue();
            long target = ((Number) cast[1]).longValue();
            pairs.add(Long.valueOf((source << 32) | target));
        }
        return pairs;
    }

    private static boolean accepts(int argOid, int paramOid) {
        if (argOid == paramOid) return true;
        if (ACCEPTS_ANYTHING.contains(paramOid)) return true;
        // A parameter of a type memgres does not model cannot be compared with anything: the
        // column it would be handed carries whatever type memgres gave it instead, which is not
        // the type PostgreSQL declared. pg_get_expr takes pg_node_tree and memgres keeps the
        // expression as text; age takes xid and memgres numbers transactions with an integer.
        if (DataType.fromOid(paramOid) == null) return true;
        int[] alsoTaken = SPELLED_DIFFERENTLY.get(Integer.valueOf(paramOid));
        if (alsoTaken != null) {
            for (int oid : alsoTaken) {
                if (oid == argOid) return true;
            }
        }
        if (IMPLICIT_CASTS.contains(Long.valueOf(((long) argOid << 32) | paramOid))) return true;
        for (int[] family : FAMILIES) {
            boolean hasArg = false;
            boolean hasParam = false;
            for (int oid : family) {
                if (oid == argOid) hasArg = true;
                if (oid == paramOid) hasParam = true;
            }
            if (hasArg && hasParam) return true;
        }
        return false;
    }

    // ---- Scope membership ----

    /**
     * True when {@code target} is written in {@code root}'s own query level — reachable without
     * crossing into a nested statement, which has a FROM clause of its own and so a scope these
     * bindings say nothing about.
     */
    static boolean isOwnLevel(Object root, Object target) {
        if (root == target) return true;
        if (root == null) return false;
        boolean[] hit = new boolean[1];
        AstWalk.forEachChild(root, child -> {
            if (!hit[0] && !(child instanceof Statement)) hit[0] = isOwnLevel(child, target);
        });
        return hit[0];
    }

    /** The name as PostgreSQL folds it: unquoted parts are already lower case when parsed. */
    static String bareName(String name) {
        return FunctionEvaluator.stripSchemaPrefix(name.toLowerCase(Locale.ROOT));
    }
}
