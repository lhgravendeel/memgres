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
        // PostgreSQL transforms the sort clause before the grouping one, so an ORDER BY that
        // names no output column is reported before anything the grouping has to say — even
        // before an unresolvable GROUP BY item.
        List<SelectStmt.OrderByItem> orderBy = select.resolveOrderBy(stmt.orderBy(), targets);

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

        // A HAVING whose operators or functions do not resolve is a type error, and PostgreSQL
        // reports it while it analyses HAVING — before it judges the query's grouping.
        checkHavingTypes(stmt, bindings);
        rejectDistinctSortKeysOutsideSelectList(stmt, targets, orderBy);

        Set<String> groupedForms = new LinkedHashSet<String>();
        for (Expression g : grouped) groupedForms.add(canon(g, bindings));

        Check check = new Check(groupedForms, determining, bindings);
        for (SelectStmt.SelectTarget t : targets) check.walk(t.expr());
        if (stmt.distinctOn() != null) {
            // DISTINCT ON keys are output expressions of the grouped result like any other, so
            // they are judged even though they need not appear in the select list.
            for (Expression on : stmt.distinctOn()) check.walk(on);
        }
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
        if (orderBy != null) {
            // ORDER BY may name an output column; those were resolved above, so that ORDER BY
            // over an aggregate's alias stays legal.
            for (SelectStmt.OrderByItem item : orderBy) {
                check.walk(item.expr());
            }
        }
    }

    /**
     * SELECT DISTINCT sorts the distinct rows, so every sort key has to be one of the columns
     * that survives the DISTINCT. An aggregate the select list does not carry is as unavailable
     * to it as a window function is, and PostgreSQL refuses both with the same message.
     */
    private void rejectDistinctSortKeysOutsideSelectList(SelectStmt stmt,
                                                         List<SelectStmt.SelectTarget> targets,
                                                         List<SelectStmt.OrderByItem> orderBy) {
        if (!stmt.distinct() || orderBy == null || orderBy.isEmpty()) return;
        if (stmt.distinctOn() != null && !stmt.distinctOn().isEmpty()) return;
        for (SelectStmt.OrderByItem item : orderBy) {
            if (select.resolveOrderByToColumnIndex(item.expr(), targets) < 0) {
                throw new MemgresException(
                        "for SELECT DISTINCT, ORDER BY expressions must appear in select list", "42P10");
            }
        }
    }

    // ---- HAVING type resolution ----

    /**
     * The type errors PostgreSQL raises while analysing HAVING, which it does before it decides
     * whether the query's expressions are grouped. Without this, {@code SELECT a, b ... GROUP BY a
     * HAVING sum(b) > 1} blames the ungrouped {@code b} in the select list rather than the
     * aggregate that has no meaning over a text column.
     */
    private void checkHavingTypes(SelectStmt stmt, List<RowContext.TableBinding> bindings) {
        if (stmt.having() == null || bindings == null || bindings.size() != 1) return;
        // Only a declared column has a type this may be judged against. A sub-select, a CTE, a
        // view or a set-returning function is backed by a relation whose column types were
        // inferred from a first result, and a derived column typed by inference — a window
        // function's, say — would be refused for a type it does not really have.
        if (!readsOneBaseTable(stmt)) return;
        rejectUnresolvableTypes(stmt.having(), bindings);
        select.executor.validateWhereTypesAgainstTable(stmt.having(), bindings.get(0).table());
    }

    /** True when the FROM clause is one plain table of the database and nothing else. */
    private boolean readsOneBaseTable(SelectStmt stmt) {
        if (stmt.from() == null || stmt.from().size() != 1) return false;
        if (!(stmt.from().get(0) instanceof SelectStmt.TableRef)) return false;
        SelectStmt.TableRef ref = (SelectStmt.TableRef) stmt.from().get(0);
        if (select.lookupCte(ref.table()) != null) return false;
        if (select.executor.database.getView(ref.table()) != null) return false;
        try {
            return select.executor.resolveTable(
                    ref.schema() != null ? ref.schema() : select.executor.defaultSchema(),
                    ref.table()) != null;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * The two type errors a HAVING can carry that need no row to see: {@code sum} and {@code avg}
     * exist for numbers only, and a count compares as the bigint it is.
     */
    private void rejectUnresolvableTypes(Object node, List<RowContext.TableBinding> bindings) {
        if (node == null) return;
        if (node instanceof Statement) return;
        if (node instanceof com.memgres.engine.parser.ast.FunctionCallExpr) {
            com.memgres.engine.parser.ast.FunctionCallExpr call =
                    (com.memgres.engine.parser.ast.FunctionCallExpr) node;
            String name = call.name() == null ? "" : call.name().toLowerCase();
            if (("sum".equals(name) || "avg".equals(name)) && call.args().size() == 1
                    && call.args().get(0) instanceof ColumnRef) {
                ColumnRef ref = (ColumnRef) call.args().get(0);
                RowContext.TableBinding binding = findBinding(ref, bindings);
                DataType type = binding == null ? null : columnType(binding, ref);
                if (type == DataType.TEXT || type == DataType.VARCHAR || type == DataType.CHAR
                        || type == DataType.BOOLEAN) {
                    MemgresException e = new MemgresException(
                            "function " + name + "(" + type.getPgName() + ") does not exist", "42883");
                    e.setHint("No function matches the given name and argument types.");
                    throw e;
                }
            }
        }
        if (node instanceof com.memgres.engine.parser.ast.BinaryExpr) {
            com.memgres.engine.parser.ast.BinaryExpr bin =
                    (com.memgres.engine.parser.ast.BinaryExpr) node;
            rejectNonBigintCountComparison(bin.left(), bin.right());
            rejectNonBigintCountComparison(bin.right(), bin.left());
        }
        AstWalk.forEachChild(node, new java.util.function.Consumer<Object>() {
            @Override public void accept(Object child) { rejectUnresolvableTypes(child, bindings); }
        });
    }

    /** {@code count(...) > 'x'} reads the literal as the bigint the count is, and fails there. */
    private static void rejectNonBigintCountComparison(Expression maybeCount, Expression other) {
        if (!(maybeCount instanceof com.memgres.engine.parser.ast.FunctionCallExpr)) return;
        if (!"count".equalsIgnoreCase(
                ((com.memgres.engine.parser.ast.FunctionCallExpr) maybeCount).name())) return;
        if (!(other instanceof Literal)) return;
        Literal literal = (Literal) other;
        if (literal.literalType() != Literal.LiteralType.STRING) return;
        try {
            Long.parseLong(literal.value().trim());
        } catch (NumberFormatException e) {
            throw new MemgresException(
                    "invalid input syntax for type bigint: \"" + literal.value() + "\"", "22P02");
        }
    }

    private static DataType columnType(RowContext.TableBinding binding, ColumnRef ref) {
        int index = binding.table().getColumnIndex(ref.column());
        if (index < 0) return null;
        return binding.table().getColumns().get(index).getType();
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
        if (!(item instanceof ColumnRef)) return item;
        ColumnRef ref = (ColumnRef) item;
        if (findBinding(ref, bindings) != null) return item;
        if (ref.table() == null) {
            for (SelectStmt.SelectTarget t : targets) {
                if (t.alias() != null && t.alias().equalsIgnoreCase(ref.column())) return t.expr();
            }
        }
        rejectUnresolvableGroupItem(ref, bindings);
        return item;
    }

    /**
     * A GROUP BY item that names nothing is that error and no other. Left to fall through, it
     * silently groups by nothing and the query is then blamed for the ungrouped columns the
     * item was meant to license — which is neither the error PostgreSQL reports nor a useful
     * one, since the name the query got wrong never appears in it.
     */
    private void rejectUnresolvableGroupItem(ColumnRef ref,
                                             List<RowContext.TableBinding> bindings) {
        if (bindings == null || bindings.isEmpty() || ref.column() == null) return;
        if (SelectExecutor.isSystemColumn(ref.column())) return;
        // A correlated reference to an enclosing query is resolved where that query's rows are,
        // not here, so nothing in this FROM clause proves it wrong.
        if (resolvesInEnclosingQuery(ref)) return;
        if (ref.table() == null) {
            // A bare name matching a FROM item is a whole-row reference, not a column.
            for (RowContext.TableBinding binding : bindings) {
                if (namesRelation(binding, ref.column())) return;
            }
            MemgresException e = new MemgresException(
                    "column \"" + ref.column() + "\" does not exist", "42703");
            for (RowContext.TableBinding binding : bindings) {
                String hint = RowContext.suggestClosestColumn(ref.column(), binding.table());
                if (hint != null) { e.setHint(hint); break; }
            }
            throw e;
        }
        String hiddenByAlias = null;
        for (RowContext.TableBinding binding : bindings) {
            if (binding.table() == null) continue;
            boolean byAlias = ref.table().equalsIgnoreCase(binding.alias());
            if (!byAlias && ref.table().equalsIgnoreCase(binding.table().getName())
                    && binding.alias() != null
                    && !binding.alias().equalsIgnoreCase(binding.table().getName())) {
                hiddenByAlias = binding.alias();
                continue;
            }
            if (byAlias || ref.table().equalsIgnoreCase(binding.table().getName())) {
                // The relation is in the FROM clause; the column it was asked for is not.
                throw new MemgresException(
                        "column " + ref.table() + "." + ref.column() + " does not exist", "42703");
            }
        }
        if (hiddenByAlias != null) {
            MemgresException e = new MemgresException(
                    "invalid reference to FROM-clause entry for table \"" + ref.table() + "\"", "42P01");
            e.setHint("Perhaps you meant to reference the table alias \"" + hiddenByAlias + "\".");
            throw e;
        }
        throw new MemgresException(
                "missing FROM-clause entry for table \"" + ref.table() + "\"", "42P01");
    }

    private static boolean namesRelation(RowContext.TableBinding binding, String name) {
        if (binding.alias() != null && binding.alias().equalsIgnoreCase(name)) return true;
        return binding.table() != null && binding.table().getName().equalsIgnoreCase(name);
    }

    /** True when an enclosing query's rows can answer for this name. */
    private boolean resolvesInEnclosingQuery(ColumnRef ref) {
        for (RowContext outer : select.executor.outerContextStack) {
            if (findBinding(ref, outer.getBindings()) != null) return true;
            for (RowContext.TableBinding binding : outer.getBindings()) {
                if (ref.table() != null && namesRelation(binding, ref.table())) return true;
            }
        }
        return false;
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
    static Integer integerConstant(Expression item) {
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
            if (node instanceof SelectStmt) {
                // A nested query has its own FROM and its own grouping rules, but a column of
                // ours that it reads is still one value per input row of ours, so it is ours
                // to judge — PostgreSQL says so in as many words.
                walkSubquery((SelectStmt) node, new ArrayList<Scope>());
                return;
            }
            if (node instanceof Statement) return;
            if (node instanceof Expression) {
                Expression expr = (Expression) node;
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

        /**
         * Judges the outer columns a nested query reads. Everything the nested query's own FROM
         * can answer for belongs to it; what is left resolves against our FROM, and then has to
         * be grouped exactly as it would in our own select list.
         *
         * <p>When the nested FROM cannot be resolved without running something — a CTE, a view,
         * another sub-select — the nested query is left alone rather than guessed at: a name
         * mistaken for an outer column would be reported as an error the query does not have.
         */
        private void walkSubquery(SelectStmt inner, List<Scope> enclosing) {
            List<Scope> scopes = new ArrayList<Scope>(enclosing);
            scopes.add(subqueryScope(inner));
            final List<Scope> pass = scopes;
            AstWalk.forEachChild(inner, new java.util.function.Consumer<Object>() {
                @Override public void accept(Object child) { walkInSubquery(child, pass); }
            });
        }

        private void walkInSubquery(Object node, List<Scope> scopes) {
            if (node == null) return;
            if (node instanceof Literal || node instanceof WildcardExpr) return;
            if (node instanceof SelectStmt) {
                walkSubquery((SelectStmt) node, scopes);
                return;
            }
            if (node instanceof Statement) return;
            if (node instanceof Expression) {
                Expression expr = (Expression) node;
                // An aggregate over an outer column belongs to the outer query, where the
                // column is one of the rows being aggregated, so it is allowed.
                if (isAggregateCall(expr)) return;
                if (expr instanceof ColumnRef) {
                    ColumnRef ref = (ColumnRef) expr;
                    for (Scope scope : scopes) {
                        if (ref.table() != null) {
                            if (scope.names(ref.table())) return;
                        } else {
                            if (findBinding(ref, scope.resolved) != null) return;
                            if (scope.opaque || scope.names(ref.column())) return;
                        }
                    }
                    RowContext.TableBinding binding = findBinding(ref, bindings);
                    if (binding == null) return;
                    if (groupedForms.contains(canon(ref, bindings))) return;
                    if (determinedByGrouping(ref, binding)) return;
                    throw new MemgresException("subquery uses ungrouped column \""
                            + qualify(ref, binding) + "\" from outer query", "42803");
                }
            }
            final List<Scope> pass = scopes;
            AstWalk.forEachChild(node, new java.util.function.Consumer<Object>() {
                @Override public void accept(Object child) { walkInSubquery(child, pass); }
            });
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

    private boolean isAggregateCall(Expression expr) {
        if (expr instanceof OrderedSetAggExpr) return true;
        if (expr instanceof com.memgres.engine.parser.ast.FunctionCallExpr) {
            return select.isAggregateFunction(((com.memgres.engine.parser.ast.FunctionCallExpr) expr).name());
        }
        return false;
    }

    // ---- Nested query scopes ----

    /**
     * What a nested query's own FROM clause brings into scope. A relation whose columns can be
     * read off the catalog answers for the names it has; one that would have to be run first —
     * a CTE, a view, a sub-select, a function — answers only for its own name, and leaves the
     * scope opaque so that an unqualified name inside the nested query is left alone rather
     * than guessed at.
     */
    private static final class Scope {
        final List<RowContext.TableBinding> resolved = new ArrayList<RowContext.TableBinding>();
        final List<String> names = new ArrayList<String>();
        boolean opaque;

        boolean names(String name) {
            if (name == null) return false;
            for (String candidate : names) {
                if (name.equalsIgnoreCase(candidate)) return true;
            }
            return false;
        }
    }

    private Scope subqueryScope(SelectStmt inner) {
        Scope scope = new Scope();
        if (inner.from() == null || inner.from().isEmpty()) return scope;
        for (SelectStmt.FromItem item : inner.from()) {
            addScopeItem(item, scope);
        }
        return scope;
    }

    private void addScopeItem(SelectStmt.FromItem item, Scope scope) {
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            addScopeItem(join.left(), scope);
            addScopeItem(join.right(), scope);
            return;
        }
        if (item instanceof SelectStmt.SubqueryFrom) {
            scope.opaque = true;
            scope.names.add(((SelectStmt.SubqueryFrom) item).alias());
            return;
        }
        if (item instanceof SelectStmt.FunctionFrom) {
            SelectStmt.FunctionFrom function = (SelectStmt.FunctionFrom) item;
            scope.opaque = true;
            scope.names.add(function.alias() != null ? function.alias() : function.functionName());
            return;
        }
        if (!(item instanceof SelectStmt.TableRef)) {
            scope.opaque = true;
            return;
        }
        SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
        String alias = ref.alias() != null ? ref.alias() : ref.table();
        scope.names.add(alias);
        if (select.lookupCte(ref.table()) != null
                || select.executor.database.getView(ref.table()) != null) {
            scope.opaque = true;
            return;
        }
        Table table;
        try {
            table = select.executor.resolveTable(
                    ref.schema() != null ? ref.schema() : select.executor.defaultSchema(), ref.table());
        } catch (RuntimeException e) {
            scope.opaque = true;
            return;
        }
        if (table == null) {
            scope.opaque = true;
            return;
        }
        scope.resolved.add(new RowContext.TableBinding(table, alias, new Object[table.getColumns().size()]));
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
    private static String canon(Object node, List<RowContext.TableBinding> bindings) {
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
