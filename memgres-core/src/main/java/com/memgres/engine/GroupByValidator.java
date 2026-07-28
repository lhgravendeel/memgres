package com.memgres.engine;

import com.memgres.engine.parser.ast.ArrayExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.OrderedSetAggExpr;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.Statement;
import com.memgres.engine.parser.ast.UnaryExpr;
import com.memgres.engine.parser.ast.WildcardExpr;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * PostgreSQL's grouping rules for a query that aggregates.
 *
 * <p>Once a query groups, every expression outside an aggregate has to be one PostgreSQL can
 * evaluate per group: either it is (a subexpression of) something the query grouped by, or it
 * reads a column the grouping determines. Anything else has one value per input row and no
 * single value per group, so PostgreSQL rejects it rather than picking an arbitrary row.
 *
 * <p>Two parts of that rule are easy to get wrong and both matter here:
 *
 * <ul>
 *   <li><b>Grouping by an expression licenses that expression, not its columns.</b>
 *       {@code GROUP BY a + 0} makes {@code a + 0} available but leaves {@code a} ungrouped;
 *       matching has to be structural, over whole subexpressions, not "the query mentions a
 *       GROUP BY so anything goes".</li>
 *   <li><b>Grouping by a primary key licenses the whole row.</b> The key determines every other
 *       column of its table, so {@code SELECT id, other FROM t GROUP BY id} is valid SQL and
 *       common in application and ORM queries. The dependency comes from the table's declared
 *       PRIMARY KEY only — a UNIQUE NOT NULL constraint does not grant it (verified against
 *       PostgreSQL 18), and neither does a view, a sub-select or a CTE, none of which carry the
 *       constraint.</li>
 * </ul>
 *
 * <p>With GROUPING SETS, ROLLUP or CUBE the two lists diverge: an expression is grouped if it
 * appears in <em>any</em> set (the row is NULL-extended for the sets that omit it), but a
 * functional dependency may only be read off the columns present in <em>every</em> set, since
 * otherwise there is a set in which the key is not grouped at all. That is why
 * {@code GROUP BY GROUPING SETS ((id))} accepts {@code other} and {@code GROUP BY ROLLUP (id)},
 * whose sets are {@code (id)} and {@code ()}, does not.
 */
final class GroupByValidator {

    private final SelectExecutor select;

    GroupByValidator(SelectExecutor select) {
        this.select = select;
    }

    /**
     * Rejects the grouped query if any expression outside an aggregate is neither grouped nor
     * functionally determined by the grouping. {@code targets} must already have star targets
     * expanded, because GROUP BY ordinals count output columns.
     */
    void validate(SelectStmt stmt, List<SelectStmt.SelectTarget> targets,
                  List<RowContext.TableBinding> bindings) {
        // Resolving happens once for the whole grouping specification: ordinals become the
        // target they point at and bare names become the FROM column they name, so everything
        // downstream compares like with like.
        List<Expression> grouped = resolveItems(stmt.groupBy(), targets, bindings);
        for (Expression g : grouped) {
            if (select.containsAggregate(g)) {
                throw new MemgresException("aggregate functions are not allowed in GROUP BY", "42803");
            }
        }

        List<Expression> determining = grouped;
        if (stmt.groupingSets() != null && !stmt.groupingSets().isEmpty()) {
            determining = commonToEverySet(stmt.groupingSets(), targets, bindings);
        }

        Set<String> groupedForms = new LinkedHashSet<String>();
        for (Expression g : grouped) groupedForms.add(canon(g, bindings));

        Check check = new Check(groupedForms, determining, bindings);
        for (SelectStmt.SelectTarget t : targets) check.walk(t.expr());
        if (stmt.having() != null) check.walk(stmt.having());
        if (stmt.windowDefs() != null) {
            // A WINDOW clause entry reads the grouped rows like any window specification, and is
            // judged even when nothing names it — as PostgreSQL judges it.
            for (SelectStmt.WindowDef def : stmt.windowDefs()) {
                if (def.partitionBy() != null) {
                    for (Expression p : def.partitionBy()) check.walk(p);
                }
                if (def.orderBy() != null) {
                    for (SelectStmt.OrderByItem item : def.orderBy()) check.walk(item.expr());
                }
            }
        }
        if (stmt.orderBy() != null) {
            // ORDER BY may name an output column; resolve those before judging them, so that
            // ORDER BY over an aggregate's alias stays legal.
            for (SelectStmt.OrderByItem item : select.resolveOrderBy(stmt.orderBy(), targets)) {
                check.walk(item.expr());
            }
        }
    }

    // ---- GROUP BY item resolution ----

    /**
     * Turns the written GROUP BY items into the expressions they stand for, raising the errors
     * PostgreSQL raises for an item that cannot stand for one.
     */
    private List<Expression> resolveItems(List<Expression> items,
                                          List<SelectStmt.SelectTarget> targets,
                                          List<RowContext.TableBinding> bindings) {
        List<Expression> out = new ArrayList<Expression>();
        if (items == null) return out;
        for (Expression item : items) {
            addResolved(item, targets, bindings, out);
        }
        return out;
    }

    private void addResolved(Expression item, List<SelectStmt.SelectTarget> targets,
                             List<RowContext.TableBinding> bindings, List<Expression> out) {
        Integer position = integerConstant(item);
        if (position != null) {
            if (position < 1 || position > targets.size()) {
                throw new MemgresException(
                        "GROUP BY position " + position + " is not in select list", "42P10");
            }
            out.add(targets.get(position - 1).expr());
            return;
        }
        if (item instanceof Literal) {
            // Only an integer constant means "the Nth output column"; every other bare constant
            // is a value PostgreSQL refuses to group by at all.
            throw new MemgresException("non-integer constant in GROUP BY", "42601");
        }
        if (item instanceof ArrayExpr && ((ArrayExpr) item).isRow()) {
            // GROUP BY (a, b) groups by each member, exactly as if they were written apart.
            out.add(item);
            for (Expression element : ((ArrayExpr) item).elements()) {
                addResolved(element, targets, bindings, out);
            }
            return;
        }
        out.add(resolveName(item, targets, bindings));
    }

    /**
     * A bare name in GROUP BY is a FROM column if one has that name, and only otherwise an
     * output alias — SQL99 gives the input column priority, so {@code SELECT b AS a ... GROUP BY a}
     * groups by {@code a} and leaves {@code b} ungrouped.
     */
    private Expression resolveName(Expression item, List<SelectStmt.SelectTarget> targets,
                                   List<RowContext.TableBinding> bindings) {
        if (!(item instanceof ColumnRef) || ((ColumnRef) item).table() != null) return item;
        ColumnRef ref = (ColumnRef) item;
        if (findBinding(ref, bindings) != null) return item;
        for (SelectStmt.SelectTarget t : targets) {
            if (t.alias() != null && t.alias().equalsIgnoreCase(ref.column())) return t.expr();
        }
        return item;
    }

    /** The grouping expressions every grouping set has, the only ones a dependency may rest on. */
    private List<Expression> commonToEverySet(List<List<Expression>> sets,
                                              List<SelectStmt.SelectTarget> targets,
                                              List<RowContext.TableBinding> bindings) {
        List<List<Expression>> resolved = new ArrayList<List<Expression>>();
        for (List<Expression> set : sets) resolved.add(resolveItems(set, targets, bindings));
        List<Expression> common = new ArrayList<Expression>();
        if (resolved.isEmpty()) return common;
        for (Expression candidate : resolved.get(0)) {
            String form = canon(candidate, bindings);
            boolean inEverySet = true;
            for (int i = 1; i < resolved.size() && inEverySet; i++) {
                inEverySet = false;
                for (Expression other : resolved.get(i)) {
                    if (form.equals(canon(other, bindings))) { inEverySet = true; break; }
                }
            }
            if (inEverySet) common.add(candidate);
        }
        return common;
    }

    /**
     * Adds the (lowercased) names of every column the grouping determines through a primary key.
     * A grouping set that groups a table's whole key produces one row per key value, so the other
     * columns of that table have a single value in the group and must not be masked out.
     */
    static void addFunctionallyDeterminedColumns(List<Expression> groupExprs,
                                                 List<RowContext.TableBinding> bindings,
                                                 Set<String> out) {
        if (bindings == null || groupExprs == null) return;
        for (RowContext.TableBinding binding : bindings) {
            List<String> key = primaryKeyColumns(binding.table());
            if (key == null || key.isEmpty()) continue;
            boolean wholeKeyGrouped = true;
            for (String keyColumn : key) {
                boolean grouped = false;
                for (Expression g : groupExprs) {
                    if (!(g instanceof ColumnRef)) continue;
                    ColumnRef ref = (ColumnRef) g;
                    if (ref.column().equalsIgnoreCase(keyColumn)
                            && findBinding(ref, bindings) == binding) { grouped = true; break; }
                }
                if (!grouped) { wholeKeyGrouped = false; break; }
            }
            if (!wholeKeyGrouped) continue;
            for (Column column : binding.table().getColumns()) {
                out.add(column.getName().toLowerCase());
            }
        }
    }

    /** The integer position an item denotes, or null when it is not an integer constant. */
    private static Integer integerConstant(Expression item) {
        int sign = 1;
        Expression node = item;
        while (node instanceof UnaryExpr) {
            UnaryExpr unary = (UnaryExpr) node;
            if (unary.op() == UnaryExpr.UnaryOp.NEGATE) sign = -sign;
            else if (unary.op() != UnaryExpr.UnaryOp.POSITIVE) return null;
            node = unary.operand();
        }
        if (!(node instanceof Literal)) return null;
        Literal literal = (Literal) node;
        if (literal.literalType() != Literal.LiteralType.INTEGER) return null;
        try {
            return sign * Integer.parseInt(literal.value().trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    // ---- The ungrouped-expression walk ----

    private final class Check {
        private final Set<String> groupedForms;
        private final List<Expression> determining;
        private final List<RowContext.TableBinding> bindings;

        Check(Set<String> groupedForms, List<Expression> determining,
              List<RowContext.TableBinding> bindings) {
            this.groupedForms = groupedForms;
            this.determining = determining;
            this.bindings = bindings;
        }

        void walk(Object node) {
            if (node == null) return;
            if (node instanceof Literal || node instanceof WildcardExpr) return;
            // A nested query has its own FROM and its own grouping rules.
            if (node instanceof Statement) return;
            if (node instanceof Expression) {
                Expression expr = (Expression) node;
                // GROUPING reports whether the current grouping set includes the expression it
                // names, so what it names has to be one the query groups by — in a plain
                // GROUP BY just as much as under GROUPING SETS, where it always answers 0.
                if (isGroupingCall(expr)) {
                    for (Expression arg : ((com.memgres.engine.parser.ast.FunctionCallExpr) expr).args()) {
                        if (!groupedForms.contains(canon(arg, bindings))) {
                            throw new MemgresException("arguments to GROUPING must be grouping "
                                    + "expressions of the associated query level", "42803");
                        }
                    }
                    return;
                }
                // An aggregate consumes whatever it reads, including its FILTER and ORDER BY.
                if (isAggregateCall(expr)) return;
                if (groupedForms.contains(canon(expr, bindings))) return;
                if (expr instanceof ColumnRef) {
                    ColumnRef ref = (ColumnRef) expr;
                    RowContext.TableBinding binding = findBinding(ref, bindings);
                    // A name that is not a column at all is an undefined-column error, which
                    // normal evaluation reports; do not pre-empt it here.
                    if (binding == null) return;
                    if (determinedByGrouping(ref, binding)) return;
                    throw new MemgresException("column \"" + qualify(ref, binding)
                            + "\" must appear in the GROUP BY clause or be used in an aggregate function",
                            "42803");
                }
            }
            AstWalk.forEachChild(node, this::walk);
        }

        /** True when the grouping covers the column's whole primary key. */
        private boolean determinedByGrouping(ColumnRef ref, RowContext.TableBinding binding) {
            List<String> key = primaryKeyColumns(binding.table());
            if (key == null || key.isEmpty()) return false;
            for (String keyColumn : key) {
                boolean grouped = false;
                for (Expression g : determining) {
                    if (!(g instanceof ColumnRef)) continue;
                    ColumnRef groupRef = (ColumnRef) g;
                    if (!groupRef.column().equalsIgnoreCase(keyColumn)) continue;
                    if (findBinding(groupRef, bindings) == binding) { grouped = true; break; }
                }
                if (!grouped) return false;
            }
            return true;
        }
    }

    /** True when a GROUPING(...) call stands anywhere in this query level's own tree. */
    static boolean containsGroupingCall(Object node) {
        if (node == null) return false;
        // A nested query does its own grouping, and its GROUPING belongs to it.
        if (node instanceof Statement) return false;
        if (node instanceof Expression && isGroupingCall((Expression) node)) return true;
        final boolean[] found = {false};
        AstWalk.forEachChild(node, new java.util.function.Consumer<Object>() {
            public void accept(Object child) {
                if (!found[0] && containsGroupingCall(child)) found[0] = true;
            }
        });
        return found[0];
    }

    /** True for a call to GROUPING(...), whichever schema it is written with. */
    static boolean isGroupingCall(Expression expr) {
        if (!(expr instanceof com.memgres.engine.parser.ast.FunctionCallExpr)) return false;
        String name = ((com.memgres.engine.parser.ast.FunctionCallExpr) expr).name();
        return name != null && "grouping".equals(FunctionEvaluator.stripSchemaPrefix(name.toLowerCase()));
    }

    private boolean isAggregateCall(Expression expr) {
        if (expr instanceof OrderedSetAggExpr) return true;
        if (expr instanceof com.memgres.engine.parser.ast.FunctionCallExpr) {
            return select.isAggregateFunction(((com.memgres.engine.parser.ast.FunctionCallExpr) expr).name());
        }
        return false;
    }

    // ---- Relation and column resolution ----

    /**
     * The declared primary key of a base table, or null for anything without one.
     *
     * <p>Read from the table's constraints rather than the columns' primary-key flags: a view,
     * a sub-select and a CTE are each backed by a fresh relation that reuses the underlying
     * {@link Column} objects, flags and all, but carries no constraints — which is exactly
     * PostgreSQL's rule, since only a real relation's PRIMARY KEY can determine a row.
     */
    private static List<String> primaryKeyColumns(Table table) {
        if (table == null) return null;
        for (StoredConstraint constraint : table.getConstraints()) {
            if (constraint.getType() == StoredConstraint.Type.PRIMARY_KEY) {
                return constraint.getColumns();
            }
        }
        return null;
    }

    /** The FROM item a column reference reads, or null when it reads none of them. */
    private static RowContext.TableBinding findBinding(ColumnRef ref,
                                                       List<RowContext.TableBinding> bindings) {
        if (bindings == null || ref.column() == null) return null;
        for (RowContext.TableBinding binding : bindings) {
            Table table = binding.table();
            if (table == null) continue;
            if (ref.table() != null
                    && !ref.table().equalsIgnoreCase(binding.alias())
                    && !ref.table().equalsIgnoreCase(table.getName())) {
                continue;
            }
            if (table.getColumnIndex(ref.column()) >= 0) return binding;
        }
        return null;
    }

    private static String qualify(ColumnRef ref, RowContext.TableBinding binding) {
        String relation = binding.alias() != null ? binding.alias() : binding.table().getName();
        return relation + "." + ref.column();
    }

    // ---- Structural identity ----

    /**
     * A comparable form of an expression in which column references stand for the FROM column
     * they resolve to, so {@code t.a} and a bare {@code a} of the same relation compare equal
     * and {@code abs(a)} does not compare equal to {@code abs(b)}. Reflective rather than a
     * per-node-type visitor: the set of expression classes grows, and a visitor that stops
     * covering one silently starts accepting ungrouped columns inside it.
     */
    static String canon(Object node, List<RowContext.TableBinding> bindings) {
        if (node == null) return "~";
        if (node instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) node;
            RowContext.TableBinding binding = findBinding(ref, bindings);
            String relation = binding == null
                    ? (ref.table() == null ? "?" : ref.table())
                    : (binding.alias() != null ? binding.alias() : binding.table().getName());
            return "col:" + relation.toLowerCase() + "." + ref.column().toLowerCase();
        }
        if (node instanceof Enum) return ((Enum<?>) node).name();
        if (node instanceof Iterable) {
            StringBuilder sb = new StringBuilder("[");
            for (Object element : (Iterable<?>) node) sb.append(canon(element, bindings)).append(',');
            return sb.append(']').toString();
        }
        if (!isAstNode(node)) return String.valueOf(node).toLowerCase();
        StringBuilder sb = new StringBuilder(node.getClass().getSimpleName()).append('(');
        for (Field field : node.getClass().getFields()) {
            if (Modifier.isStatic(field.getModifiers())) continue;
            Object value;
            try {
                value = field.get(node);
            } catch (IllegalAccessException e) {
                continue;
            }
            sb.append(field.getName()).append('=').append(canon(value, bindings)).append(';');
        }
        return sb.append(')').toString();
    }

    private static boolean isAstNode(Object node) {
        Class<?> c = node.getClass();
        while (c != null && c.getEnclosingClass() != null) c = c.getEnclosingClass();
        String pkg = c == null || c.getPackage() == null ? "" : c.getPackage().getName();
        return pkg.startsWith("com.memgres.engine.parser.ast");
    }
}
