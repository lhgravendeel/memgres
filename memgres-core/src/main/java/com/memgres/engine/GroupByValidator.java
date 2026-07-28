package com.memgres.engine;

import com.memgres.engine.parser.ast.ArrayExpr;
import com.memgres.engine.parser.ast.CastExpr;
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

    /** The names every relation answers for without declaring them as columns. */
    private static final Set<String> SYSTEM_COLUMNS = new java.util.HashSet<String>(
            java.util.Arrays.asList("ctid", "xmin", "xmax", "cmin", "cmax", "tableoid", "oid"));

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
            // The groups are formed by reading this expression per input row, so nothing that
            // only has a value once the groups exist may stand in it — and PostgreSQL names
            // which kind of call that was.
            select.placementCheck.reject(g, "GROUP BY");
        }

        List<Expression> determining = grouped;
        if (stmt.groupingSets() != null && !stmt.groupingSets().isEmpty()) {
            determining = commonToEverySet(stmt.groupingSets(), targets, bindings);
        }

        // A HAVING whose operators or functions do not resolve is a type error, and PostgreSQL
        // reports it while it analyses HAVING — before it judges the query's grouping.
        checkHavingTypes(stmt, bindings);
        rejectDistinctSortKeysOutsideSelectList(stmt, targets, orderBy, bindings);

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
     *
     * <p>What counts as "one of them" is the column, not the spelling. PostgreSQL matches the
     * sort key against the select list after both have been resolved, so {@code ORDER BY d.s}
     * and a select list carrying a bare {@code s} of that same relation are the same column and
     * the query is accepted — while {@code ORDER BY u.a} against a select list carrying
     * {@code t.a} names a different column and is still refused.
     */
    private void rejectDistinctSortKeysOutsideSelectList(SelectStmt stmt,
                                                         List<SelectStmt.SelectTarget> targets,
                                                         List<SelectStmt.OrderByItem> orderBy,
                                                         List<RowContext.TableBinding> bindings) {
        if (!stmt.distinct() || orderBy == null || orderBy.isEmpty()) return;
        if (stmt.distinctOn() != null && !stmt.distinctOn().isEmpty()) return;
        for (SelectStmt.OrderByItem item : orderBy) {
            if (select.resolveOrderByToColumnIndex(item.expr(), targets) >= 0) continue;
            if (namesSelectListExpression(item.expr(), targets, bindings)) continue;
            throw new MemgresException(
                    "for SELECT DISTINCT, ORDER BY expressions must appear in select list", "42P10");
        }
    }

    /** True when the select list carries this expression, however either of them was written. */
    private boolean namesSelectListExpression(Expression sortKey,
                                              List<SelectStmt.SelectTarget> targets,
                                              List<RowContext.TableBinding> bindings) {
        String key = canon(sortKey, bindings);
        for (SelectStmt.SelectTarget target : targets) {
            if (key.equals(canon(target.expr(), bindings))) return true;
        }
        return false;
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
                    // PostgreSQL names the type the way SQL spells it -- character varying, not
                    // the catalog's varchar -- which is what toRegtypeDisplay renders.
                    MemgresException e = new MemgresException(
                            "function " + name + "(" + type.toRegtypeDisplay() + ") does not exist",
                            "42883");
                    e.setHint("No function matches the given name and argument types.");
                    throw e;
                }
            }
        }
        if (node instanceof com.memgres.engine.parser.ast.BinaryExpr) {
            com.memgres.engine.parser.ast.BinaryExpr bin =
                    (com.memgres.engine.parser.ast.BinaryExpr) node;
            if (readsLiteralAsBigint(bin.op())) {
                rejectNonBigintCountComparison(bin.left(), bin.right());
                rejectNonBigintCountComparison(bin.right(), bin.left());
            }
        }
        AstWalk.forEachChild(node, new java.util.function.Consumer<Object>() {
            @Override public void accept(Object child) { rejectUnresolvableTypes(child, bindings); }
        });
    }

    /**
     * The operators that read a bare string literal as the bigint the count beside it is.
     *
     * <p>A comparison and the four arithmetic operators do; nothing else does.
     * {@code count(*) || 'x'} concatenates a bigint with a string and is ordinary SQL that
     * PostgreSQL runs — as are LIKE, the JSON operators and every other operator that has its
     * own reading of a string operand. Firing on any binary expression at all, as this once did,
     * refused those working queries.
     */
    private static boolean readsLiteralAsBigint(com.memgres.engine.parser.ast.BinaryExpr.BinOp op) {
        switch (op) {
            case EQUAL:
            case NOT_EQUAL:
            case LESS_THAN:
            case LESS_EQUAL:
            case GREATER_THAN:
            case GREATER_EQUAL:
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
        item = eraseNoOpCast(item, bindings);
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

    // ---- Casts the column already satisfies ----

    /**
     * A cast asking for the type its operand column already has, dropped.
     *
     * <p>PostgreSQL erases such a cast while it analyses the query, so {@code GROUP BY a::int}
     * over an {@code int} column <em>is</em> {@code GROUP BY a}: it licenses a bare {@code a} in
     * the select list, and a primary key grouped that way still determines its row. A cast to
     * any other type — {@code b::varchar} over {@code text}, {@code n::numeric} over
     * {@code numeric(10,2)} — is a real coercion producing a different value, so grouping by it
     * leaves the column itself ungrouped, exactly as PostgreSQL has it.
     *
     * <p>Only a cast written directly over a column of the FROM clause is erased. That is the
     * one operand whose type is declared rather than inferred, so nothing here has to guess a
     * type and wrongly let an ungrouped column through.
     */
    private static Expression eraseNoOpCast(Expression expr, List<RowContext.TableBinding> bindings) {
        while (expr instanceof CastExpr) {
            CastExpr cast = (CastExpr) expr;
            // a::int::int is two casts over the column, and PostgreSQL erases both.
            Expression operand = eraseNoOpCast(cast.expr(), bindings);
            if (!(operand instanceof ColumnRef)) return expr;
            ColumnRef ref = (ColumnRef) operand;
            RowContext.TableBinding binding = findBinding(ref, bindings);
            if (binding == null) return expr;
            int index = binding.table().getColumnIndex(ref.column());
            if (index < 0) return expr;
            if (!castsToOwnType(cast.typeName(), binding.table().getColumns().get(index))) return expr;
            expr = ref;
        }
        return expr;
    }

    /** True when the cast names the column's own declared type, length or precision and all. */
    private static boolean castsToOwnType(String typeName, Column column) {
        if (typeName == null || column == null) return false;
        String written = typeName.trim();
        // A domain and an enum are named types of their own: naming a column's own domain or
        // enum is the no-op cast, and naming the type underneath it is a conversion out of it.
        if (column.getDomainTypeName() != null) {
            return written.equalsIgnoreCase(column.getDomainTypeName());
        }
        if (column.getEnumTypeName() != null) {
            return written.equalsIgnoreCase(column.getEnumTypeName());
        }
        if (column.getCompositeTypeName() != null || column.getArrayElementType() != null) {
            return false;
        }
        String name = written.replaceAll("\\s+", " ");
        Integer precision = null;
        Integer scale = null;
        int paren = name.indexOf('(');
        if (paren >= 0) {
            if (!name.endsWith(")")) return false;
            String[] args = name.substring(paren + 1, name.length() - 1).split(",");
            name = name.substring(0, paren).trim();
            if (args.length > 2) return false;
            try {
                precision = Integer.valueOf(args[0].trim());
                if (args.length > 1) scale = Integer.valueOf(args[1].trim());
            } catch (NumberFormatException e) {
                return false;
            }
        }
        DataType cast = builtInType(name);
        if (cast == null || cast != declaredType(column.getType())) return false;
        return sameModifier(precision, column.getPrecision())
                && sameModifier(scale, column.getScale());
    }

    private static boolean sameModifier(Integer a, Integer b) {
        return a == null ? b == null : a.equals(b);
    }

    /** A serial column is the integer type it is stored as, which is what a cast can name. */
    private static DataType declaredType(DataType type) {
        if (type == DataType.SERIAL) return DataType.INTEGER;
        if (type == DataType.BIGSERIAL) return DataType.BIGINT;
        if (type == DataType.SMALLSERIAL) return DataType.SMALLINT;
        return type;
    }

    /**
     * The built-in type a cast's type name names, or null for anything else.
     *
     * <p>Deliberately not {@link DataType#fromPgName}: that maps {@code citext} and the
     * {@code reg*} aliases onto the type they are stored as, and a cast to one of those is a
     * conversion PostgreSQL does not erase. A name this does not know simply keeps its cast.
     */
    private static DataType builtInType(String name) {
        String lower = name.toLowerCase();
        if ("smallint".equals(lower) || "int2".equals(lower)) return DataType.SMALLINT;
        if ("integer".equals(lower) || "int".equals(lower) || "int4".equals(lower)) return DataType.INTEGER;
        if ("bigint".equals(lower) || "int8".equals(lower)) return DataType.BIGINT;
        if ("real".equals(lower) || "float4".equals(lower)) return DataType.REAL;
        if ("double precision".equals(lower) || "float8".equals(lower)) return DataType.DOUBLE_PRECISION;
        if ("numeric".equals(lower) || "decimal".equals(lower)) return DataType.NUMERIC;
        if ("varchar".equals(lower) || "character varying".equals(lower)) return DataType.VARCHAR;
        if ("char".equals(lower) || "character".equals(lower) || "bpchar".equals(lower)) return DataType.CHAR;
        if ("text".equals(lower)) return DataType.TEXT;
        if ("name".equals(lower)) return DataType.NAME;
        if ("boolean".equals(lower) || "bool".equals(lower)) return DataType.BOOLEAN;
        if ("date".equals(lower)) return DataType.DATE;
        if ("timestamp".equals(lower) || "timestamp without time zone".equals(lower)) return DataType.TIMESTAMP;
        if ("timestamptz".equals(lower) || "timestamp with time zone".equals(lower)) return DataType.TIMESTAMPTZ;
        if ("time".equals(lower) || "time without time zone".equals(lower)) return DataType.TIME;
        if ("timetz".equals(lower) || "time with time zone".equals(lower)) return DataType.TIMETZ;
        if ("interval".equals(lower)) return DataType.INTERVAL;
        if ("bytea".equals(lower)) return DataType.BYTEA;
        if ("uuid".equals(lower)) return DataType.UUID;
        if ("json".equals(lower)) return DataType.JSON;
        if ("jsonb".equals(lower)) return DataType.JSONB;
        if ("inet".equals(lower)) return DataType.INET;
        if ("cidr".equals(lower)) return DataType.CIDR;
        if ("macaddr".equals(lower)) return DataType.MACADDR;
        if ("macaddr8".equals(lower)) return DataType.MACADDR8;
        if ("money".equals(lower)) return DataType.MONEY;
        if ("xml".equals(lower)) return DataType.XML;
        if ("tsvector".equals(lower)) return DataType.TSVECTOR;
        if ("tsquery".equals(lower)) return DataType.TSQUERY;
        if ("bit".equals(lower)) return DataType.BIT;
        if ("varbit".equals(lower) || "bit varying".equals(lower)) return DataType.VARBIT;
        if ("oid".equals(lower)) return DataType.OID;
        return null;
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
                // GROUPING reports whether the current grouping set includes the expression it
                // names, so what it names has to be one the query groups by — in a plain
                // GROUP BY just as much as under GROUPING SETS, where it always answers 0.
                if (isGroupingCall(expr)) {
                    for (Expression arg : ((com.memgres.engine.parser.ast.FunctionCallExpr) expr).args()) {
                        if (groupedForms.contains(canon(arg, bindings))) continue;
                        // A name is resolved before it is judged as a grouping expression, and a
                        // select-list alias is not a name an expression can use: GROUPING(k) over
                        // "SELECT s AS k" is an undefined column, not a misplaced GROUPING.
                        rejectUnknownColumn(arg);
                        throw new MemgresException("arguments to GROUPING must be grouping "
                                + "expressions of the associated query level", "42803");
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

        /**
         * A GROUPING argument that is a bare name no relation in the FROM answers for is an
         * undefined column, which PostgreSQL reports before it asks whether the query groups by
         * it. Only an unqualified name over relations whose columns are all known is judged: a
         * qualified one, a system column and anything reached through a relation this walk could
         * not resolve are left to the grouping-expression message.
         */
        private void rejectUnknownColumn(Expression arg) {
            if (!(arg instanceof ColumnRef)) return;
            ColumnRef ref = (ColumnRef) arg;
            if (ref.table() != null || ref.column() == null) return;
            if (SYSTEM_COLUMNS.contains(ref.column().toLowerCase())) return;
            if (bindings == null || bindings.isEmpty()) return;
            for (RowContext.TableBinding binding : bindings) {
                if (binding.table() == null) return;
            }
            if (findBinding(ref, bindings) != null) return;
            throw new MemgresException("column \"" + ref.column() + "\" does not exist", "42703");
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
    static String canon(Object node, List<RowContext.TableBinding> bindings) {
        if (node == null) return "~";
        if (node instanceof CastExpr) {
            // A cast to the column's own type is not part of the expression PostgreSQL compares,
            // wherever in the expression it stands: GROUP BY a::int + 1 licenses a + 1.
            Expression erased = eraseNoOpCast((CastExpr) node, bindings);
            if (erased != node) return canon(erased, bindings);
        }
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
