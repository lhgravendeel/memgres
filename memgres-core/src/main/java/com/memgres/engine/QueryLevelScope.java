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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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

    QueryLevelScope(SelectExecutor select, List<RowContext.TableBinding> bindings,
                    List<RowContext.OutCol> output, SelectStmt stmt) {
        this.select = select;
        this.bindings = bindings;
        this.output = output;
        this.stmt = stmt;
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
        if (node == null || bindings == null || bindings.isEmpty()) return;
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
        if (bindings == null || bindings.isEmpty()) return null;
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
            DataType type = b.table().getColumns().get(idx).getType();
            if (found != null && found != type) return null;
            found = type;
        }
        return found;
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
     * Refuses a call whose argument types no signature of that name accepts.
     *
     * <p>PostgreSQL resolves a function by name <em>and</em> argument types, so {@code abs(txt)}
     * is a function that does not exist rather than abs applied to something odd — and a call that
     * resolves to nothing never reaches the complaint about its FILTER clause.
     *
     * <p>Deliberately narrow. Every argument's type has to be certain, the name has to have a
     * signature of that arity to compare against, and a type is counted as accepted whenever it is
     * in the same family as the parameter, because PostgreSQL casts within a family implicitly.
     * Anything less certain is left to resolve at evaluation time as before.
     */
    void rejectUnresolvableCall(Object call, String bareName) {
        List<Expression> args = argsOf(call);
        if (args == null) return;
        int[] argOids = new int[args.size()];
        String[] argNames = new String[args.size()];
        for (int i = 0; i < args.size(); i++) {
            DataType type = certainTypeOf(args.get(i));
            if (type == null) return;
            argOids[i] = type.getOid();
            argNames[i] = CatalogHelper.pgTypeName(type);
        }
        boolean recorded = false;
        boolean comparable = false;
        for (String[] signature : BuiltinFunctionSignatures.SIGNATURES) {
            if (!signature[0].equalsIgnoreCase(bareName)) continue;
            recorded = true;
            String[] params = signature[2].isEmpty() ? new String[0] : signature[2].split(" ");
            if (params.length != argOids.length) continue;
            comparable = true;
            boolean accepts = true;
            for (int i = 0; i < params.length; i++) {
                if (!accepts(argOids[i], Integer.parseInt(params[i]))) {
                    accepts = false;
                    break;
                }
            }
            if (accepts) return;
        }
        // A name with no recorded signature says nothing about what it takes. When one is
        // recorded but not at this length, only a call longer than every form of it is judged:
        // the table under-records the short forms, so "too few" would refuse working SQL, while
        // the longest form recorded is the longest memgres implements. See
        // {@link FunctionEvaluator#rejectWrongArity}.
        if (!recorded) return;
        if (!comparable) {
            if (FunctionEvaluator.acceptsAnyArity(bareName)) return;
            int longest = 0;
            for (String[] signature : BuiltinFunctionSignatures.SIGNATURES) {
                if (!signature[0].equalsIgnoreCase(bareName)) continue;
                int params = signature[2].isEmpty() ? 0 : signature[2].split(" ").length;
                if (params > longest) longest = params;
            }
            if (argOids.length <= longest) return;
        }
        StringBuilder types = new StringBuilder();
        for (int i = 0; i < argNames.length; i++) {
            if (i > 0) types.append(", ");
            types.append(argNames[i]);
        }
        throw new MemgresException(
                "function " + nameOf(call) + "(" + types + ") does not exist", "42883");
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

    private static final Set<Integer> ACCEPTS_ANYTHING = new HashSet<Integer>();

    static {
        // A pseudo-type parameter is resolved from the actual argument, so it accepts one.
        for (String name : PolymorphicTypes.names()) ACCEPTS_ANYTHING.add(PolymorphicTypes.oid(name));
        ACCEPTS_ANYTHING.add(2276); // any
        ACCEPTS_ANYTHING.add(2249); // record
        ACCEPTS_ANYTHING.add(705);  // unknown
        ACCEPTS_ANYTHING.add(2281); // internal
    }

    private static boolean accepts(int argOid, int paramOid) {
        if (argOid == paramOid) return true;
        if (ACCEPTS_ANYTHING.contains(paramOid)) return true;
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
