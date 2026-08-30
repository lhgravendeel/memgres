package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;

/**
 * Resolves FROM clauses and JOIN operations for SELECT, UPDATE, and DELETE statements.
 * Delegates to FromFunctionResolver for set-returning functions and FromJoinExecutor for joins.
 */
class FromResolver {
    final AstExecutor executor;
    final FromFunctionResolver functionResolver;
    final FromJoinExecutor joinExecutor;
    final CrossProductPairer pairer;
    // Track the last resolved table info for LEFT JOIN null-padding when right side is empty
    Table lastResolvedRightTable;
    String lastResolvedRightAlias;
    /**
     * The WHERE of the query whose FROM is being resolved. A full join reads it: a WHERE that
     * discards the null-padded rows makes the join an inner one, which lifts the restriction on
     * what its condition may be. See {@link FullJoinAdmissibility}.
     */
    Expression enclosingWhere;
    /** The HAVING of that query; the clauses of it with no aggregate are WHERE clauses too. */
    Expression enclosingHaving;
    /** The ON conditions of the joins above the one being resolved that filter its rows. */
    final List<Expression> joinQualsAbove = new ArrayList<>();
    /** The query whose FROM clause is being resolved, or null when it is not a plain SELECT. */
    Statement currentQuery;
    /**
     * The statement the client sent, when that statement is a SELECT. Everything else — a
     * subquery, a WITH query, a view body, an arm of a set operation, the source of a writing
     * statement, a function body — is a query PostgreSQL may still rescue a full join in from
     * above, so it is never the one whose joins are judged. See {@link FullJoinAdmissibility}.
     */
    Statement outermostQuery;
    /**
     * The query whose FROM clause a relation built from another query is being read for. It is
     * that query that decides what such a relation has to work out, so it is remembered while the
     * FROM clause is described as well as while it is read.
     */
    SelectStmt qualifyingQuery;
    /**
     * The name a relation built from a query answers to in the query reading it -- the alias of a
     * derived table, of a WITH item or of a view -- and the parts of that query's qualification
     * which speak about that relation alone, for as long as the relation is being built.
     *
     * <p>PostgreSQL pulls such a relation up into the query that reads it, so that query's WHERE
     * becomes a qualification of the scan underneath rather than a filter on what the scan
     * produced. A VIRTUAL generated column is worked out where the reference to it stands, so a
     * row the enclosing WHERE discards never reaches the generation expression -- which is what
     * leaves a relation whose expression raises for one of its rows readable through a view.
     */
    String derivedRelation;
    List<Expression> derivedQualification;
    /**
     * The names the FROM item wrote for that relation's columns, when it wrote an alias list.
     *
     * <p>An alias list renames every column the relation exposes, so the query above it writes the
     * new names and the query underneath still answers to its own. Both the demand that query is
     * read under and the qualification pushed into it have to be read through the list, or they
     * speak about columns nothing supplies.
     */
    List<String> derivedColumnAliases;
    /**
     * Whether the query being read as a relation is one arm of a set operation.
     *
     * <p>An arm answers for some of the relation's rows and not for the rest, so a column the
     * relation could carry generated would be worked out for every arm's rows from an expression
     * only one arm's relation supplies. Each arm therefore works its own out.
     */
    boolean derivedSetOperation;
    /**
     * Whether the relation the query being read is built into names its own columns.
     *
     * <p>A WITH item may name the columns it exposes, and from then on those are the relation's
     * names. A column the query could otherwise leave for that relation to work out would arrive
     * there under a name its generation expression is not written in, so the query works it out
     * itself.
     */
    boolean derivedRenamesColumns;
    /**
     * Whether the query just read as a relation left a VIRTUAL generated column out of a row
     * because the qualification pushed into it discarded that row.
     */
    boolean derivedQualificationApplied;
    /**
     * The qualification the query whose FROM clause is being resolved has itself taken from the
     * query above it.
     *
     * <p>PostgreSQL pulls a whole chain up at once: a view whose body reads a WITH item, or a WITH
     * item built from another, ends up as one query with one set of qualifications over it. So a
     * query that has taken a qualification hands it on to the relation it is built from.
     */
    List<Expression> qualifyingPushed;
    /** The query each WITH item now in scope was declared in, and the references it has there. */
    private final Map<SelectStmt.CommonTableExpr, Object> cteOwners = new IdentityHashMap<>();
    private final Map<SelectStmt.CommonTableExpr, Integer> cteReferences = new IdentityHashMap<>();

    final FullJoinAdmissibility fullJoinCheck;

    FromResolver(AstExecutor executor) {
        this.executor = executor;
        this.functionResolver = new FromFunctionResolver(executor);
        this.joinExecutor = new FromJoinExecutor(this);
        this.pairer = new CrossProductPairer(executor);
        this.fullJoinCheck = new FullJoinAdmissibility(this);
    }

    /** Whether the FROM clause being resolved is the one the client's own statement wrote. */
    boolean judgingOutermostQuery() {
        return outermostQuery != null && currentQuery == outermostQuery;
    }

    /**
     * Build a relation from a query, offering that query the qualification the query above imposes
     * on it. Only the query being read is offered it: a query nested inside that one is a query of
     * its own and takes the qualification of whatever reads it, so the offer is taken once.
     */
    private <T> T readAsDerivedRelation(String alias, java.util.function.Supplier<T> build) {
        return readAsDerivedRelation(alias, null, build);
    }

    /** The same, for a FROM item that gave the relation's columns names of its own. */
    private <T> T readAsDerivedRelation(String alias, List<String> columnAliases,
                                        java.util.function.Supplier<T> build) {
        String priorRelation = derivedRelation;
        List<Expression> priorQualification = derivedQualification;
        List<String> priorColumnAliases = derivedColumnAliases;
        boolean priorForNoRow = readForNoRow;
        derivedRelation = alias;
        derivedQualification = qualificationOn(alias);
        derivedColumnAliases = columnAliases;
        // A relation the query above answers without asking for a row is a relation PostgreSQL
        // never reads, and nothing it is built from is read either.
        readForNoRow = readForNoRow || admitsNoRow(alias, qualifyingQuery, qualifyingPushed);
        try {
            return build.get();
        } finally {
            derivedRelation = priorRelation;
            derivedQualification = priorQualification;
            derivedColumnAliases = priorColumnAliases;
            readForNoRow = priorForNoRow;
        }
    }

    /**
     * Whether what is being read now is being built for a query PostgreSQL never asks for a row.
     *
     * <p>A qualification decided against before a row is read, and a LIMIT of none, leave the query
     * above answering without asking anything below it for anything. A WITH item that query keeps
     * apart is computed only when it is asked, so it does not run -- and neither does an item read
     * by the query inside it, however that query's own qualification reads.
     */
    private boolean readForNoRow;

    /** The name a LATERAL sub-select answers to in the query reading it. */
    static String lateralAlias(SelectStmt.SubqueryFrom item) {
        return item.alias() != null ? item.alias() : "subquery";
    }

    /**
     * Run a LATERAL sub-select as a relation of the query reading it.
     *
     * <p>PostgreSQL pulls one up into that query just as it pulls a plain derived table up, so a
     * reference to a VIRTUAL generated column of a relation underneath stands in the query above
     * and its expression is evaluated over the rows that query's joins and its WHERE kept. Run
     * without saying so, the sub-select worked every one of them out for every row it produced,
     * and a row the query above discards then raised an error it never asked for.
     */
    QueryResult readLateralSubquery(SelectStmt.SubqueryFrom item) {
        // An item PostgreSQL keeps apart is computed before the query reading it is planned, so
        // none of that query's qualification reaches the relation underneath and every row the
        // item holds reaches a VIRTUAL generated column's expression.
        String readAs = runsPerOuterRow(item) ? null : lateralAlias(item);
        return readAsDerivedRelation(readAs, item.columnAliases(), () -> {
            if (item.subquery() instanceof SelectStmt) {
                return executor.executeSelect((SelectStmt) item.subquery());
            }
            return executor.executeStatement(item.subquery());
        });
    }

    /**
     * Whether the query reading a LATERAL item never asks it for a row beside this one.
     *
     * <p>An item PostgreSQL keeps apart is a query of its own, run once per row of the relation
     * beside it -- and only for the rows that relation's own scan kept, because a restriction the
     * query puts on that relation is read before the item is reached. A row the query discards
     * therefore never has the item run for it at all, so nothing the item would answer with is
     * worked out: not a VIRTUAL generated column of the relation underneath, and not an aggregate,
     * a grouping or a select list of the item's own that reads one. That is what leaves {@code o
     * LEFT JOIN LATERAL (SELECT * FROM t z WHERE z.a = o.a LIMIT 1) s ON s.g = 2 WHERE o.note =
     * 'x'} readable where {@code 10/a} raises for the row a is zero in, while the same item without
     * the LIMIT is not: pulled up into the query, an item is a relation of that query and its scan
     * is narrowed only by what a restriction carries into it.
     *
     * <p>Leaving the pairing out altogether answers what PostgreSQL answers, because every row it
     * could have been part of is discarded by the very part that decided against this one.
     */
    boolean lateralItemUnasked(SelectStmt.SubqueryFrom item, RowContext outer) {
        return runsPerOuterRow(item) && discardsOuterRow(outer);
    }

    /**
     * Read the whole relation a LATERAL item PostgreSQL pulls up stands in front of, under what the
     * query reading the item says about it.
     *
     * <p>Pulled up, the item is no longer a query of its own: the relation underneath is one of the
     * reading query's own relations and is scanned once, so a qualification speaking of the item
     * alone is a filter on that scan and is read for every row the scan visits. The item's own
     * comparison with the row beside it is not such a filter -- it says which rows pair, not which
     * rows there are -- so the scan is not narrowed by it, and {@code o LEFT JOIN LATERAL (SELECT *
     * FROM t z WHERE z.a = o.a) s ON s.g = 2} reaches {@code 10/a} for the row a is zero in however
     * few rows o holds. Run once per row of o, it reached that row only where o held a row that
     * paired with it, so what the query answered depended on how many rows o happened to have.
     *
     * <p>What does narrow the scan narrows it here: a constant an equivalence class carries onto the
     * relation, the item's own comparisons with anything but the row beside it, and a join
     * qualification about the item alone -- which chooses the rows of the scan as well, because a
     * row it is false of pairs with nothing and is carried by no row the query answers with.
     */
    void readLateralItemWhole(SelectStmt.SubqueryFrom item, String alias, Expression joined) {
        if (runsPerOuterRow(item) || !(item.subquery() instanceof SelectStmt)) return;
        SelectStmt inner = (SelectStmt) item.subquery();
        if (inner.fromValues() || inner.joinExpression()) return;
        if (inner.from() == null || inner.from().size() != 1 || qualifyingQuery == null) return;
        // Only a relation that has a VIRTUAL generated column has anything to be worked out over,
        // and only one a name reaches on its own can be asked what it holds without reading it.
        if (!hasGeneratedColumn(storedRelationColumns(inner.from().get(0)))) return;
        Set<String> under = exposedNamesOf(inner.from());
        Set<String> beside = exposedNamesOf(qualifyingQuery.from());
        List<Expression> ownParts = new ArrayList<>();
        boolean pairs = false;
        for (Expression part : conjunctsOf(inner.where())) {
            if (pairsWithARowBesideIt(part, under, beside)) {
                pairs = true;
            } else {
                ownParts.add(part);
            }
        }
        if (!pairs) return;
        final SelectStmt whole = new SelectStmt(inner.distinct(), inner.distinctOn(),
                inner.targets(), inner.from(), allOf(ownParts), inner.groupBy(), inner.having(),
                inner.windowDefs(), inner.orderBy(), inner.limit(), inner.offset(),
                inner.withClauses(), inner.groupingSets(), inner.lockClause(), inner.withTies());
        QueryResult scanned = readAsDerivedRelation(alias, item.columnAliases(),
                () -> executor.executeSelect(whole));
        Table relation = new Table(alias, FromFunctionResolver.applyColumnAliases(
                new ArrayList<>(scanned.getColumns()), item.columnAliases()));
        renamesColumnsOf(relation, alias, scanned.getColumns(), item.columnAliases());
        if (!executor.dmlExecutor.hasVirtualColumns(relation)) return;
        List<Expression> pairing = executor.dmlExecutor.decidableQualification(relation, alias,
                conjunctsOf(joined), false, beside);
        for (Object[] row : scanned.getRows()) {
            if (executor.dmlExecutor.qualificationRejects(relation, alias, row, pairing)) continue;
            executor.dmlExecutor.computeVirtualColumns(relation, alias, row);
        }
    }

    /**
     * Whether one part of an item's own qualification speaks of a relation the query reading the
     * item holds beside it. Such a part is read where the two are paired; a part naming a relation
     * of a query further out is read against a row that stands still while the scan runs, which is
     * a restriction on the scan like any other.
     */
    private static boolean pairsWithARowBesideIt(Expression part, Set<String> under,
                                                 Set<String> beside) {
        return AstWalk.anyMatch(part, node -> {
            if (!(node instanceof ColumnRef)) return false;
            String named = ((ColumnRef) node).table();
            return named != null && !under.contains(named.toLowerCase(java.util.Locale.ROOT))
                    && beside.contains(named.toLowerCase(java.util.Locale.ROOT));
        });
    }

    private static boolean hasGeneratedColumn(List<Column> columns) {
        if (columns == null) return false;
        for (Column col : columns) {
            if (col.isVirtual()) return true;
        }
        return false;
    }

    /** The parts that must all hold, written back as one qualification. */
    private static Expression allOf(List<Expression> parts) {
        Expression all = null;
        for (Expression part : parts) {
            all = all == null ? part : new BinaryExpr(all, BinaryExpr.BinOp.AND, part);
        }
        return all;
    }

    /**
     * Whether PostgreSQL keeps a LATERAL item apart from the query reading it rather than pulling
     * it up, which is what makes the item a query run once per row of the relation beside it.
     *
     * <p>An item is pulled up only where what it holds is left for the query above to settle.
     * Anything that settles which of its rows there are, or in what order, before that query is
     * planned keeps the two apart: a set operation, a LIMIT or an OFFSET, a sort, a DISTINCT, a
     * grouping, a HAVING, a locking clause, a WITH item of its own, and an aggregate, a window
     * call, a set-returning call or a volatile one in its select list. A pulled-up item is not run
     * per row of anything, and it is the same test that decides whether the reading query's
     * qualification reaches the relation the item stands in front of.
     */
    private boolean runsPerOuterRow(SelectStmt.SubqueryFrom item) {
        // A set operation answers with the rows of two queries put together, and which rows that
        // leaves is settled between them rather than by anything above.
        if (!(item.subquery() instanceof SelectStmt)) return true;
        SelectStmt inner = (SelectStmt) item.subquery();
        if (inner.limit() != null || inner.offset() != null) return true;
        if (inner.distinct() || holdsAny(inner.distinctOn())) return true;
        if (holdsAny(inner.orderBy()) || holdsAny(inner.groupBy())) return true;
        if (holdsAny(inner.groupingSets()) || inner.having() != null) return true;
        if (holdsAny(inner.withClauses()) || inner.lockClause() != null) return true;
        if (holdsAny(inner.windowDefs())) return true;
        if (inner.targets() == null) return false;
        return executor.selectExecutor.hasAggregateInTargets(inner.targets())
                || settlesItsOwnRows(inner.targets());
    }

    /**
     * Whether a select list settles what the item answers with rather than leaving that to the
     * query above: an aggregate reads the rows the item holds all at once, a window call is
     * numbered over them, a set-returning call multiplies them, and a volatile one answers
     * differently each time the item is read.
     */
    private boolean settlesItsOwnRows(List<SelectStmt.SelectTarget> targets) {
        for (SelectStmt.SelectTarget target : targets) {
            boolean settles = AstWalk.anyMatch(target.expr(), node -> {
                if (node instanceof WindowFuncExpr || node instanceof OrderedSetAggExpr) return true;
                if (!(node instanceof FunctionCallExpr)) return false;
                FunctionCallExpr call = (FunctionCallExpr) node;
                if (call.name() == null) return false;
                return isVolatileCall(call) || executor.selectExecutor.isSetReturningCall(call);
            });
            if (settles) return true;
        }
        return false;
    }

    /**
     * Whether the query whose FROM clause is being resolved has already decided against a row of it.
     *
     * <p>Only a part of that query's WHERE that must hold on its own counts, and only one that can
     * be decided from the row as it stands: every name in it is one of the relations the row
     * carries, and nothing in it is a query, a call or a window, which may answer differently here
     * than where it was written. A part like that being false of the row means every row the query
     * could build from it is discarded by that same part -- through an outer join as well, because
     * a row padded with nulls does not carry this one at all. A part that cannot be decided decides
     * nothing, and so does one that raises while it is read.
     */
    private boolean discardsOuterRow(RowContext outer) {
        if (outer == null || qualifyingQuery == null || qualifyingQuery.where() == null) {
            return false;
        }
        Set<String> carried = new HashSet<>();
        for (RowContext.TableBinding binding : outer.getBindings()) {
            if (binding.alias() != null) carried.add(binding.alias().toLowerCase(java.util.Locale.ROOT));
        }
        if (carried.isEmpty()) return false;
        for (Expression part : conjunctsOf(qualifyingQuery.where())) {
            if (!decidableFromRow(part, carried)) continue;
            try {
                if (!executor.isTruthy(executor.evalExpr(part, outer))) return true;
            } catch (RuntimeException undecided) {
                // Nothing was settled about the row, so nothing is settled against it.
            }
        }
        return false;
    }

    /** Whether one part of a qualification can be decided from a row, and decided the same twice. */
    private static boolean decidableFromRow(Expression part, Set<String> carried) {
        final boolean[] usable = {true};
        AstWalk.forEach(part, node -> {
            if (node instanceof Statement || node instanceof FunctionCallExpr
                    || node instanceof WindowFuncExpr || node instanceof OrderedSetAggExpr) {
                usable[0] = false;
                return;
            }
            if (!(node instanceof ColumnRef)) return;
            ColumnRef ref = (ColumnRef) node;
            if (ref.column() == null || ref.table() == null
                    || !carried.contains(ref.table().toLowerCase(java.util.Locale.ROOT))) {
                usable[0] = false;
            }
        });
        return usable[0];
    }

    /**
     * Read a WITH item as a relation of the query above it.
     *
     * <p>PostgreSQL plans a WITH item it pulls up again at every reference, so a qualification
     * pushed into one holds for the reference that pushed it and for no other. The rows this
     * reference was answered are therefore not kept for the next reference, which would otherwise
     * be answered with the first one's qualification. A WITH item that writes runs once however
     * often it is named, so its rows are always kept.
     */
    private QueryResult readCte(SelectStmt.CommonTableExpr cte, String alias,
                                List<String> columnAliases) {
        // Refused before the item is computed, so the write it would have applied does not happen.
        StatementAnalyzer.requireReturning(cte);
        boolean priorApplied = derivedQualificationApplied;
        derivedQualificationApplied = false;
        boolean priorRenamed = derivedRenamesColumns;
        derivedRenamesColumns = cte.columnNames() != null && !cte.columnNames().isEmpty();
        final boolean inlined = inlinesCte(cte);
        // An item kept apart is computed when the query above first asks it for a row, so a query
        // that answers without asking leaves it holding nothing at all. An item that writes is
        // computed however the query above is answered, because PostgreSQL applies its write once
        // per statement whether or not anything reads what it returns.
        final boolean unasked = !inlined && !writesAnywhere(cte.query())
                && answersWithoutReading(alias);
        try {
            // An item PostgreSQL keeps apart is computed before the query reading it is planned,
            // so none of that query's qualification reaches the relation underneath and every row
            // the item holds reaches a VIRTUAL generated column's expression.
            QueryResult result = readAsDerivedRelation(inlined ? alias : null,
                    namesAbove(cte, columnAliases),
                    () -> readCteBody(inlined, unasked, cte));
            if ((derivedQualificationApplied || unasked) && !writesRows(cte.query())) {
                executor.cteResultCache.remove(cte);
            }
            return result;
        } finally {
            derivedQualificationApplied = priorApplied;
            derivedRenamesColumns = priorRenamed;
        }
    }

    /**
     * Compute a WITH item, under the demand of the query reading it where PostgreSQL pulls the item
     * up into that query and under none where it keeps the item apart.
     *
     * <p>An item kept apart is computed in full before the query reading it is planned, so what it
     * holds is settled by its own select list rather than by what that query goes on to read of it:
     * a {@code *} there stands for every column of the relation underneath, a VIRTUAL generated one
     * included, and PostgreSQL works the generation expression out for every row the item holds.
     * Read under the demand of the query above, a column that query never names was left
     * unworked-out, and {@code WITH c AS MATERIALIZED (SELECT * FROM t) SELECT count(*) FROM c}
     * answered a count where PostgreSQL raises the error {@code 10/a} raises for a row of t. An
     * item that names its columns is a different matter: what it does not name it does not hold.
     */
    private QueryResult readCteBody(boolean inlined, boolean unasked,
                                    SelectStmt.CommonTableExpr cte) {
        if (inlined) return executor.selectExecutor.executeCte(cte);
        executor.dmlExecutor.enterColumnDemand(
                () -> unasked ? Collections.<String>emptySet()
                        : executor.selectExecutor.columnsHeldAlone(cte.query()), () -> null);
        try {
            return executor.selectExecutor.executeCte(cte);
        } finally {
            executor.dmlExecutor.exitColumnDemand();
        }
    }

    /**
     * Whether the query now reading a relation of this name answers without reading a row of it.
     *
     * <p>A WITH item PostgreSQL keeps apart is computed when the query above first asks it for a
     * row. A qualification that is false before any row is read, and a LIMIT of none, mean it is
     * never asked at all: the item does not run, and a VIRTUAL generated column of the relation
     * under it is never worked out. That is what leaves {@code WITH c AS MATERIALIZED (SELECT *
     * FROM t) SELECT count(*) FROM c WHERE false} a count of no rows over a relation whose
     * {@code 10/a} raises for one of them.
     *
     * <p>Whether the item is asked is settled by the whole qualification rather than by any one
     * part of it. The parts must all hold, so one of them false before a row is read leaves the
     * rest nothing to decide and {@code WHERE false AND c.a = 5} asks the item for a row exactly as
     * little as {@code WHERE false} does. What stands under an OR is not such a part: what it says
     * is read beside what the other side says, row by row.
     */
    private boolean answersWithoutReading(String alias) {
        // An item read by a query that is itself never asked for a row is never asked either.
        if (readForNoRow) return true;
        if (alias == null || qualifyingQuery == null) return false;
        if (!exposedNamesOf(qualifyingQuery.from()).contains(alias.toLowerCase(java.util.Locale.ROOT))) return false;
        return admitsNoRow(alias, qualifyingQuery, qualifyingPushed);
    }

    /**
     * Whether a query's qualification is decided against before a row of anything is read.
     *
     * <p>Only a part written out of constants settles the matter that early. One that names a
     * column is decided row by row, which is a row asked for, and one holding a query or a call may
     * answer differently where it was written than it answers here. A qualification the query above
     * pushed down is read with the query's own, because PostgreSQL pulls the two up into one query
     * and reads every part of it before a row is fetched. So is a HAVING clause, which drops the
     * groups a query answers with as surely as a WHERE drops the rows they are made of.
     */
    private boolean admitsNoRow(String alias, SelectStmt query, List<Expression> pushed) {
        if (query == null) return false;
        if (writtenAsNoRows(query.limit())) return true;
        List<Expression> parts = conjunctsOf(query.where());
        collectConjuncts(query.having(), parts);
        joinsNeverAsking(query.from(), alias, parts);
        if (pushed != null) parts.addAll(pushed);
        for (Expression part : parts) {
            if (settledFalse(part)) return true;
        }
        return false;
    }

    /**
     * The conditions of a query's joins that settle, before a row is read, that a relation of it is
     * never asked for one.
     *
     * <p>An inner join answers the pairs its condition holds of and no others, so a condition false
     * before a row is read leaves the whole clause empty and nothing under it is asked anything. An
     * outer join answers a row of the side it preserves whatever its condition says, so that side
     * and everything below it is asked just the same; a row of the other side the condition rejects
     * is paired with nothing and never answered at all, which is why the condition settles the
     * matter for the relations that side holds.
     */
    private static void joinsNeverAsking(List<SelectStmt.FromItem> fromItems, String alias,
                                         List<Expression> out) {
        if (fromItems == null) return;
        for (SelectStmt.FromItem item : fromItems) joinsNeverAsking(item, alias, out);
    }

    private static void joinsNeverAsking(SelectStmt.FromItem item, String alias,
                                         List<Expression> out) {
        if (!(item instanceof SelectStmt.JoinFrom)) return;
        SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
        SelectStmt.JoinType type = join.joinType();
        if (type == SelectStmt.JoinType.INNER || type == SelectStmt.JoinType.CROSS) {
            collectConjuncts(join.on(), out);
            joinsNeverAsking(join.left(), alias, out);
            joinsNeverAsking(join.right(), alias, out);
            return;
        }
        if (alias == null) return;
        SelectStmt.FromItem gated = null;
        if (type == SelectStmt.JoinType.LEFT || type == SelectStmt.JoinType.NATURAL_LEFT) {
            gated = join.right();
        } else if (type == SelectStmt.JoinType.RIGHT
                || type == SelectStmt.JoinType.NATURAL_RIGHT) {
            gated = join.left();
        }
        if (gated == null) return;
        Set<String> notPreserved = new HashSet<>();
        collectExposedName(gated, notPreserved);
        if (!notPreserved.contains(alias.toLowerCase(java.util.Locale.ROOT))) return;
        collectConjuncts(join.on(), out);
        joinsNeverAsking(gated, alias, out);
    }

    /**
     * Whether a qualification reads false out of what is written in it alone.
     *
     * <p>Such a qualification is false for every row there is, so a scan reading it decides against
     * a row without reading anything of the row at all.
     *
     * <p>PostgreSQL settles a whole written expression that way, not only the parts an AND is
     * written out in. The parts of an AND must all hold, so one of them false settles the whole of
     * it; a branch of an OR holds on its own, so an OR is settled only where every branch is. That
     * is what leaves {@code WHERE false AND c.a = 5 OR false} a qualification nothing is read for,
     * while {@code WHERE false OR c.a = 5} is one that is read row by row.
     */
    boolean settledFalse(Expression part) {
        if (part instanceof BinaryExpr) {
            BinaryExpr sides = (BinaryExpr) part;
            if (sides.op() == BinaryExpr.BinOp.AND) {
                return settledFalse(sides.left()) || settledFalse(sides.right());
            }
            if (sides.op() == BinaryExpr.BinOp.OR) {
                return settledFalse(sides.left()) && settledFalse(sides.right());
            }
        }
        if (!settledBeforeARowIsRead(part)) return false;
        // A query written in a qualification is read here rather than where it stands, so the offer
        // standing for the relation now being built is kept back from it: that offer is the
        // relation's, and a query of no relations at all would take it and leave the relation none.
        String priorRelation = derivedRelation;
        List<Expression> priorQualification = derivedQualification;
        List<String> priorColumnAliases = derivedColumnAliases;
        boolean priorArm = derivedSetOperation;
        boolean priorRenamed = derivedRenamesColumns;
        derivedRelation = null;
        derivedQualification = null;
        derivedColumnAliases = null;
        derivedSetOperation = false;
        derivedRenamesColumns = false;
        try {
            return !executor.isTruthy(executor.evalExpr(part, null));
        } catch (RuntimeException undecided) {
            // Nothing was settled about the query, so nothing is settled about the item.
            return false;
        } finally {
            derivedRelation = priorRelation;
            derivedQualification = priorQualification;
            derivedColumnAliases = priorColumnAliases;
            derivedSetOperation = priorArm;
            derivedRenamesColumns = priorRenamed;
        }
    }

    /**
     * Whether everything a qualification is written out of answers before a row is read.
     *
     * <p>A column is a row read, and a window call and an ordered-set aggregate are the rows around
     * one. A call answers here what it answers where it stands only where PostgreSQL declares it
     * IMMUTABLE, which is the rule PostgreSQL folds one by while it plans: {@code lower('A')} is
     * folded away, while {@code now()}, which answers one thing for this statement and another for
     * the next, and {@code random()} and {@code nextval}, which answer differently every time and
     * leave something behind as they do, are left standing where they were written.
     */
    private boolean settledBeforeARowIsRead(Expression part) {
        final boolean[] settled = {true};
        AstWalk.forEach(part, node -> {
            if (node instanceof ColumnRef || node instanceof WindowFuncExpr
                    || node instanceof OrderedSetAggExpr) {
                settled[0] = false;
            } else if (node instanceof FunctionCallExpr) {
                if (!foldsWhereItStands((FunctionCallExpr) node)) settled[0] = false;
            } else if (node instanceof Statement) {
                if (!answersBeforeARowIsRead((Statement) node)) settled[0] = false;
            }
        });
        return settled[0];
    }

    /**
     * Whether a query written inside a qualification answers before a row of anything is read.
     *
     * <p>A sub-select with no FROM clause of its own reads no relation, and a reference reaching out
     * of one into the query around it is a column like any other and is refused where it stands --
     * so what such a query answers is one answer for the whole statement rather than one per row.
     * PostgreSQL reads such a qualification once, before the scan under it is asked for anything at
     * all, which is what leaves {@code WHERE (SELECT false)} a qualification a relation is never
     * read for. A query that names a relation, that carries a WITH item, or that is a set operation
     * reads something, and reading it here would be reading it at a moment PostgreSQL does not.
     */
    private static boolean answersBeforeARowIsRead(Statement query) {
        if (!(query instanceof SelectStmt)) return false;
        SelectStmt select = (SelectStmt) query;
        return !holdsAny(select.from()) && !holdsAny(select.withClauses());
    }

    /**
     * Whether PostgreSQL folds this call away while it plans, which it does for a call it declares
     * IMMUTABLE for the number of arguments the call was written with. A name the user has declared
     * a function of their own for need not be the built-in at all, and is left alone.
     */
    private boolean foldsWhereItStands(FunctionCallExpr call) {
        String name = call.name();
        if (name == null || call.args() == null || name.indexOf('.') >= 0) return false;
        if (executor.database.getFunction(name) != null) return false;
        return ImmutableCalls.BY_NAME_AND_ARITY.contains(
                name.toLowerCase(Locale.ROOT) + "/" + call.args().size());
    }

    /**
     * The calls PostgreSQL folds, read off the signatures recorded for it rather than written out
     * again so that the two cannot drift apart.
     *
     * <p>A name is folded only for the argument counts its every signature is immutable at:
     * {@code length(text)} is immutable and {@code length(bytea, name)} reads the encoding, so a
     * call of one argument folds and a call of two does not. A variadic signature answers for
     * counts that cannot be written down, so nothing is claimed about a name that has one. Held in
     * a class of its own so the reading is done the first time a qualification holds a call.
     */
    private static final class ImmutableCalls {

        static final Set<String> BY_NAME_AND_ARITY = read();

        private static Set<String> read() {
            Set<String> immutable = new HashSet<>();
            Set<String> otherwise = new HashSet<>();
            Set<String> unbounded = new HashSet<>();
            for (String[] signature : BuiltinFunctionSignatures.SIGNATURES) {
                String name = signature[0].toLowerCase(Locale.ROOT);
                int params = signature[2].isEmpty() ? 0 : signature[2].split(" ").length;
                String fewest = signature.length > 4 ? signature[4] : String.valueOf(params);
                if (fewest.endsWith("*")) fewest = fewest.substring(0, fewest.length() - 1);
                if (fewest.endsWith("+")) {
                    unbounded.add(name);
                    fewest = fewest.substring(0, fewest.length() - 1);
                }
                int least;
                try {
                    least = Integer.parseInt(fewest);
                } catch (NumberFormatException notWritten) {
                    least = params;
                }
                boolean folds = BuiltinFunctionSignatures.isPostgresSignature(signature)
                        && "fi".equals(signature[3]);
                for (int count = least; count <= Math.max(params, least); count++) {
                    (folds ? immutable : otherwise).add(name + "/" + count);
                }
            }
            immutable.removeAll(otherwise);
            Set<String> folded = new HashSet<>();
            for (String key : immutable) {
                if (!unbounded.contains(key.substring(0, key.lastIndexOf('/')))) folded.add(key);
            }
            return Collections.unmodifiableSet(folded);
        }
    }

    /**
     * Read the relations a writing statement brings in beside the one it writes, under the
     * qualification that statement puts on them.
     *
     * <p>PostgreSQL settles a qualification written out of constants before it reads a row of
     * anything, so a statement no row can satisfy neither writes nor reads: the relations it named
     * are asked for nothing, a WITH item it keeps apart is never computed, and no VIRTUAL generated
     * column of anything underneath is worked out. That is what leaves {@code WITH c AS
     * MATERIALIZED (SELECT * FROM t) UPDATE o SET note = 'z' FROM c WHERE false} a statement that
     * writes nothing over a relation whose {@code 10/a} raises for one of its rows.
     */
    private <T> T readForWritingStatement(Expression qualification,
                                          List<SelectStmt.FromItem> fromItems,
                                          java.util.function.Supplier<T> read) {
        if (readForNoRow || !admitsNoRow(qualification, fromItems)) return read.get();
        readForNoRow = true;
        executor.dmlExecutor.enterColumnDemand(() -> Collections.<String>emptySet(),
                () -> Collections.<String>emptySet());
        try {
            return read.get();
        } finally {
            executor.dmlExecutor.exitColumnDemand();
            readForNoRow = false;
        }
    }

    /**
     * Whether a writing statement's own qualification is decided against before a row is read.
     *
     * <p>Its parts must all hold, so one of them settled false settles the statement. A join
     * written in the clause is read with them: an inner join answers the pairs its condition holds
     * of and no others, so a condition false before a row is read leaves the whole clause empty.
     * An outer join answers a row of the side it preserves whatever its condition says, so nothing
     * is taken from one.
     */
    private boolean admitsNoRow(Expression qualification, List<SelectStmt.FromItem> fromItems) {
        List<Expression> parts = conjunctsOf(qualification);
        joinsNeverAsking(fromItems, null, parts);
        for (Expression part : parts) {
            if (settledFalse(part)) return true;
        }
        return false;
    }

    /** Whether a row count was written out as none at all. */
    private static boolean writtenAsNoRows(Expression count) {
        return count instanceof Literal
                && ((Literal) count).literalType() == Literal.LiteralType.INTEGER
                && "0".equals(((Literal) count).value().trim());
    }

    /**
     * The names the query above answers to a WITH item's columns by: the alias list its FROM item
     * wrote as far as that reaches, then the item's own column list, then the names the query
     * underneath gave them.
     *
     * <p>Both lists rename the same columns from the left, so one simply stands over the other --
     * and what the query above writes is what its qualification and its demand are written in.
     */
    private static List<String> namesAbove(SelectStmt.CommonTableExpr cte,
                                           List<String> columnAliases) {
        List<String> own = cte.columnNames();
        if (own == null || own.isEmpty()) return columnAliases;
        if (columnAliases == null || columnAliases.isEmpty()) return own;
        List<String> above = new ArrayList<>(columnAliases);
        for (int i = columnAliases.size(); i < own.size(); i++) above.add(own.get(i));
        return above;
    }

    private static boolean writesRows(Statement query) {
        return query instanceof InsertStmt || query instanceof UpdateStmt
                || query instanceof DeleteStmt || query instanceof MergeStmt;
    }

    /**
     * The built-in calls PostgreSQL declares VOLATILE. {@code now()} and the other bare-keyword
     * value functions are STABLE, not volatile, and do not stop an item being pulled up.
     */
    private static final Set<String> VOLATILE_CALLS = Cols.setOf(
            "random", "random_normal", "clock_timestamp", "timeofday", "gen_random_uuid",
            "uuidv4", "uuidv7", "nextval", "setval", "currval", "lastval", "txid_current",
            "pg_sleep", "pg_sleep_for", "pg_sleep_until", "statement_timestamp");

    /**
     * Whether PostgreSQL would pull this WITH item up into the query that reads it.
     *
     * <p>A pulled-up item is planned as part of that query, so the query's qualification becomes a
     * qualification of the relation underneath and a VIRTUAL generated column is worked out only
     * for the rows it keeps. An item kept apart is computed first, on its own, with none of that
     * qualification, so every row it holds reaches the generation expression. MATERIALIZED asks for
     * it to be kept apart and NOT MATERIALIZED for it to be pulled up; written neither way, an item
     * is pulled up when the query names it once. A recursion, a body that writes and a volatile
     * call each keep it apart however it was written, because computing it in two places would not
     * answer the same thing twice.
     */
    private boolean inlinesCte(SelectStmt.CommonTableExpr cte) {
        if (Boolean.TRUE.equals(cte.materialized())) return false;
        if (writesAnywhere(cte.query())) return false;
        // Declaring RECURSIVE does not make an item recursive; naming itself does.
        if (cte.recursive() && RecursiveCteCheck.selfReferencing(cte)) return false;
        if (callsVolatile(cte.query())) return false;
        if (Boolean.FALSE.equals(cte.materialized())) return true;
        return referencesTo(cte) <= 1;
    }

    private static boolean writesAnywhere(Statement query) {
        return AstWalk.anyMatch(query, node -> node instanceof InsertStmt
                || node instanceof UpdateStmt || node instanceof DeleteStmt
                || node instanceof MergeStmt);
    }

    private static boolean callsVolatile(Statement query) {
        return AstWalk.anyMatch(query, node -> node instanceof FunctionCallExpr
                && isVolatileCall((FunctionCallExpr) node));
    }

    private static boolean isVolatileCall(FunctionCallExpr call) {
        String name = call.name();
        if (name == null) return false;
        String bare = name.toLowerCase(java.util.Locale.ROOT);
        int dot = bare.lastIndexOf('.');
        if (dot >= 0) bare = bare.substring(dot + 1);
        return VOLATILE_CALLS.contains(bare);
    }

    /** How many times the query that declared this WITH item names it. */
    private int referencesTo(SelectStmt.CommonTableExpr cte) {
        Integer known = cteReferences.get(cte);
        if (known != null) return known;
        Object owner = cteOwners.get(cte);
        // An item whose declaring query this did not run -- one reached through a view's body from
        // elsewhere, or one a writing statement carries -- is counted as named once, which is what
        // nearly every item is; the alternative would be to keep it apart on no evidence.
        if (owner == null) return 1;
        final Map<String, int[]> counts = new HashMap<>();
        for (Map.Entry<SelectStmt.CommonTableExpr, Object> entry : cteOwners.entrySet()) {
            if (entry.getValue() == owner) {
                counts.put(entry.getKey().name().toLowerCase(java.util.Locale.ROOT), new int[1]);
            }
        }
        AstWalk.forEach(owner, node -> {
            if (!(node instanceof SelectStmt.TableRef)) return;
            SelectStmt.TableRef ref = (SelectStmt.TableRef) node;
            if (ref.schema() != null || ref.table() == null) return;
            int[] seen = counts.get(ref.table().toLowerCase(java.util.Locale.ROOT));
            if (seen != null) seen[0]++;
        });
        for (Map.Entry<SelectStmt.CommonTableExpr, Object> entry : cteOwners.entrySet()) {
            if (entry.getValue() != owner) continue;
            int[] seen = counts.get(entry.getKey().name().toLowerCase(java.util.Locale.ROOT));
            cteReferences.put(entry.getKey(), seen == null ? 1 : seen[0]);
        }
        Integer found = cteReferences.get(cte);
        return found == null ? 1 : found;
    }

    /**
     * Remember which query declared these WITH items. For a set operation it is the whole
     * operation: every arm reads the items, so an item one arm names twice is named twice.
     */
    void noteCteScope(List<SelectStmt.CommonTableExpr> items, Object owner) {
        if (items == null) return;
        for (SelectStmt.CommonTableExpr item : items) cteOwners.put(item, owner);
    }

    void forgetCteScope(List<SelectStmt.CommonTableExpr> items) {
        if (items == null) return;
        for (SelectStmt.CommonTableExpr item : items) {
            cteOwners.remove(item);
            cteReferences.remove(item);
        }
    }

    /**
     * The parts of the enclosing query's qualification that speak about one relation alone.
     *
     * <p>A part naming another relation cannot be decided from this one's rows, and a part written
     * with no relation at all can only be this one's when the query reads nothing else. A join
     * condition counts where the join is an inner one: an outer join's condition settles which
     * rows are paired, not which rows there are. Everything left out simply stays where it was
     * written, and the relation is read as it was read before.
     */
    private List<Expression> qualificationOn(String alias) {
        if (alias == null || qualifyingQuery == null) return null;
        Set<String> read = new HashSet<>();
        exposedNames(qualifyingQuery.from(), read);
        // The name has to be one that query reads, or that query is not the one reading it.
        if (!read.contains(alias.toLowerCase(java.util.Locale.ROOT))) return null;
        List<Expression> parts = new ArrayList<>();
        collectConjuncts(qualifyingQuery.where(), parts);
        collectJoinConjuncts(qualifyingQuery.from(), alias, parts);
        // The query doing the reading may itself be a relation of the query above, in which case it
        // has already taken that query's qualification onto its own scan; PostgreSQL pulls the
        // whole chain up at once, so the qualification carries on down. Only a part written with no
        // relation name survives the next step, which is what it has to be to mean anything here.
        if (qualifyingPushed != null) parts.addAll(qualifyingPushed);
        // A sub-select the statement above reads as one join with itself is restricted by what that
        // statement's equalities say about the column the two are joined on, and that restriction
        // reaches the relation this query is built from exactly as the query's own does.
        parts.addAll(restrictionsPushedInto(qualifyingQuery));
        // What the query's equalities say between them: a relation compared with another that is
        // itself compared with a constant is compared with that constant, and PostgreSQL puts the
        // derived restriction on the relation's own scan. It is what makes a relation reachable
        // through a join alone -- nothing written about it directly says anything about it. A
        // LATERAL item's own comparison with the row beside it is one of them: PostgreSQL pulls
        // the item up into this query, so what the item says inside stands here too, and so does
        // what the statement holding this query has already settled about the row beside it.
        List<Expression> equated = new ArrayList<>(parts);
        lateralCorrelations(qualifyingQuery.from(), equated);
        equated.addAll(enclosingEqualities(read));
        parts.addAll(DmlExecutor.impliedEqualities(equated));
        boolean paired = readAsJoinAbove(qualifyingQuery);
        List<Expression> applicable = new ArrayList<>();
        for (Expression part : parts) {
            if (speaksOnlyOf(part, alias, read.size() == 1, read, paired)) applicable.add(part);
        }
        return applicable.isEmpty() ? null : applicable;
    }

    /**
     * The qualifications of the statements now running, the innermost last.
     *
     * <p>What is kept is the qualification itself rather than the restrictions it puts on the
     * sub-selects written in it, because working those out costs a walk of it and there is nothing
     * for them to decide unless a relation has a VIRTUAL generated column, which nearly none has.
     */
    private final List<Expression> qualifying = new ArrayList<>();

    /** The name each of them writes its columns without a relation for, where there is only one. */
    private final List<String> qualifyingLone = new ArrayList<>();

    /** Read a qualification, with the sub-selects written in it standing under it. */
    void enterQualification(Expression qualification) {
        enterQualification(qualification, null);
    }

    /** The same, for a statement whose columns a name written on its own can only be one of. */
    void enterQualification(Expression qualification, String lone) {
        qualifying.add(qualification);
        qualifyingLone.add(lone);
    }

    void exitQualification() {
        qualifying.remove(qualifying.size() - 1);
        qualifyingLone.remove(qualifyingLone.size() - 1);
    }

    /**
     * The equalities the statement holding this sub-select has already settled, as they read here.
     *
     * <p>PostgreSQL reads a sub-select written among the parts of a qualification as one query with
     * the statement holding it, so what that statement says of the row standing beside this scan
     * stands in the same class as what this query says of its own: {@code o.a = 5} written there,
     * beside {@code s.a = o.a} written here, says {@code s.a = 5} as surely as it says either, and
     * that is a restriction on s's own scan. The parts of a qualification are read cheapest first
     * and a part holding a query is the dearest there is, so every plain part of the statement
     * above held for the row this sub-select is being run for.
     *
     * <p>A name this query's own FROM clause answers to is this query's, whatever the statement
     * above called its relations, so a part naming one says nothing here. A name written without a
     * relation is the one relation that statement reads, where it reads only one, and nothing at
     * all where it reads more.
     */
    List<Expression> enclosingEqualities(Set<String> here) {
        if (qualifying.isEmpty()) return Collections.emptyList();
        String lone = qualifyingLone.get(qualifyingLone.size() - 1);
        List<Expression> outer = new ArrayList<>();
        for (Expression part : conjunctsOf(qualifying.get(qualifying.size() - 1))) {
            Expression written = writtenOutside(part, here, lone);
            if (written != null) outer.add(written);
        }
        return outer;
    }

    /** One part of that qualification as it reads here, or null where it says nothing here. */
    private static Expression writtenOutside(Expression part, Set<String> here, String lone) {
        if (!(part instanceof BinaryExpr)) return null;
        BinaryExpr equality = (BinaryExpr) part;
        if (equality.op() != BinaryExpr.BinOp.EQUAL) return null;
        Expression left = sideOutside(equality.left(), here, lone);
        Expression right = sideOutside(equality.right(), here, lone);
        if (left == null || right == null) return null;
        return left == equality.left() && right == equality.right()
                ? part : new BinaryExpr(left, BinaryExpr.BinOp.EQUAL, right);
    }

    private static Expression sideOutside(Expression side, Set<String> here, String lone) {
        if (side instanceof Literal) return side;
        if (!(side instanceof ColumnRef)) return null;
        ColumnRef ref = (ColumnRef) side;
        if (ref.column() == null) return null;
        String named = ref.table() != null ? ref.table() : lone;
        if (named == null || here.contains(named.toLowerCase(java.util.Locale.ROOT))) return null;
        return ref.table() != null ? ref : new ColumnRef(named, ref.column());
    }

    /**
     * Whether the statement holding this sub-select reads it as one join with itself.
     *
     * <p>PostgreSQL pulls a sub-select written as EXISTS, NOT EXISTS, IN or {@code = ANY} among the
     * parts that must all hold up into the statement holding it, and reads the two as one join.
     * What the sub-select then says about the row it is compared with is the condition of that
     * join, read where the two are paired and above the scan's own filter, so it settles which
     * rows pair rather than which rows there are -- and the generation expression of a row it would
     * have rejected is reached just the same. Written anywhere else, the sub-select stays a plan of
     * its own, run once for each row of the statement above with that row's values standing in it,
     * and what it says about them is a filter on its own scan like any other.
     */
    boolean readAsJoinAbove(SelectStmt sub) {
        if (qualifying.isEmpty()) return false;
        for (Expression part : conjunctsOf(qualifying.get(qualifying.size() - 1))) {
            if (readAsJoin(part, sub)) return true;
        }
        return false;
    }

    private static boolean readAsJoin(Expression part, SelectStmt sub) {
        if (part instanceof UnaryExpr && ((UnaryExpr) part).op() == UnaryExpr.UnaryOp.NOT) {
            Expression held = ((UnaryExpr) part).operand();
            return held instanceof ExistsExpr && ((ExistsExpr) held).subquery() == sub;
        }
        if (part instanceof ExistsExpr) return ((ExistsExpr) part).subquery() == sub;
        if (part instanceof InExpr) {
            InExpr in = (InExpr) part;
            if (in.negated() || in.fromAny() || in.values() == null || in.values().size() != 1) {
                return false;
            }
            Expression only = in.values().get(0);
            return only instanceof SubqueryExpr && ((SubqueryExpr) only).subquery() == sub;
        }
        if (part instanceof AnyAllExpr) {
            AnyAllExpr any = (AnyAllExpr) part;
            return !any.isAll() && any.subquery() == sub;
        }
        return false;
    }

    /**
     * What one row of this sub-select has to hold for what the statement above asked it to be
     * settled, or null where no single row settles it.
     *
     * <p>PostgreSQL stops reading a sub-select the moment a row settles the answer: EXISTS and NOT
     * EXISTS the moment one row is answered at all, IN and {@code = ANY} the moment one equals the
     * value they are compared with, NOT IN and ALL the moment one fails the comparison. The rows
     * behind it are never read, so a VIRTUAL generated column of theirs is never worked out --
     * which is what leaves {@code WHERE o.a = 5 AND EXISTS (SELECT 1 FROM t s WHERE s.g = 2)}
     * readable over a relation whose {@code 10/a} raises for a row standing behind one the
     * sub-select's own qualification holds of.
     *
     * <p>Only a sub-select whose rows are answered one at a time is read that way. One that groups
     * its rows, sorts them, sets them apart or takes them from a place among them reads every row
     * it holds before it answers with any; so does one written among the branches of an OR, which
     * PostgreSQL answers by reading the whole of it into a table of values first. A second relation
     * beside it is the same matter one level down: a row of this one settles nothing until what it
     * pairs with is known.
     */
    List<Expression> settledByOneRowOf(SelectStmt sub) {
        if (qualifying.isEmpty() || !answersOneRowAtATime(sub)) return null;
        for (Expression part : conjunctsOf(qualifying.get(qualifying.size() - 1))) {
            List<Expression> compared = settlingComparison(part, sub);
            if (compared == null) continue;
            List<Expression> settles = new ArrayList<>(conjunctsOf(sub.where()));
            settles.addAll(compared);
            return settles;
        }
        return null;
    }

    /**
     * What a row has to compare as, beside holding the sub-select's own qualification, for the part
     * of the statement above to be settled by it -- nothing at all for EXISTS, which any row the
     * sub-select answers with settles.
     */
    private static List<Expression> settlingComparison(Expression part, SelectStmt sub) {
        if (part instanceof UnaryExpr && ((UnaryExpr) part).op() == UnaryExpr.UnaryOp.NOT) {
            Expression held = ((UnaryExpr) part).operand();
            return held instanceof ExistsExpr && ((ExistsExpr) held).subquery() == sub
                    ? Collections.<Expression>emptyList() : null;
        }
        if (part instanceof ExistsExpr) {
            return ((ExistsExpr) part).subquery() == sub
                    ? Collections.<Expression>emptyList() : null;
        }
        // A comparison of ANY -- which is what IN is, and what NOT IN is under a NOT -- is answered
        // out of a table of values PostgreSQL reads every row of the sub-select into, so no one row
        // settles it and the rows behind the one that matches are read just the same.
        if (part instanceof AnyAllExpr) {
            AnyAllExpr any = (AnyAllExpr) part;
            if (!any.isAll() || any.subquery() != sub) return null;
            Expression answered = answersWithOne(sub);
            if (answered == null) return null;
            // ALL is settled by a row the comparison does not hold of. A row it reads neither way
            // settles nothing, which is what a null compares as.
            return Collections.<Expression>singletonList(new UnaryExpr(UnaryExpr.UnaryOp.NOT,
                    new BinaryExpr(any.left(), any.op(), answered)));
        }
        return null;
    }

    /** The one expression a sub-select answers with, where what it answers with is one. */
    private static Expression answersWithOne(SelectStmt sub) {
        if (sub.targets() == null || sub.targets().size() != 1) return null;
        Expression only = sub.targets().get(0).expr();
        return only instanceof WildcardExpr || only instanceof CompositeStarExpr ? null : only;
    }

    /** Whether a sub-select answers with its rows one at a time, as they are read. */
    private boolean answersOneRowAtATime(SelectStmt sub) {
        if (sub.fromValues() || sub.joinExpression()) return false;
        if (sub.from() == null || sub.from().size() != 1) return false;
        if (sub.from().get(0) instanceof SelectStmt.JoinFrom) return false;
        // A place counted from among the rows is settled over the rows before it, so the row that
        // settles the answer stands behind however many of them that place passed over.
        if (sub.offset() != null) return false;
        if (sub.distinct() || holdsAny(sub.distinctOn())) return false;
        if (holdsAny(sub.orderBy()) || holdsAny(sub.groupBy())) return false;
        if (holdsAny(sub.groupingSets()) || sub.having() != null) return false;
        if (holdsAny(sub.withClauses()) || sub.lockClause() != null) return false;
        if (holdsAny(sub.windowDefs())) return false;
        return sub.targets() != null
                && !executor.selectExecutor.hasAggregateInTargets(sub.targets());
    }

    /**
     * The restriction the statement that holds this sub-select puts on the scan underneath it.
     *
     * <p>PostgreSQL pulls a sub-select written as {@code x IN (SELECT c FROM t)} up into the
     * statement holding it and reads the two as one join, which puts c and x in one class:
     * {@code x = 5} beside it says {@code c = 5} as surely as it says either of them, and that is
     * the restriction PostgreSQL puts on t's own scan. It matters because a restriction on the
     * scan decides a row before anything costly is read of it, so a VIRTUAL generated column of a
     * row the join could never have kept is not worked out -- which is what leaves {@code WHERE
     * o.a IN (SELECT s.a FROM t s WHERE s.g = 2) AND o.a = 5} a query that answers over a relation
     * whose {@code 10/a} raises where a is zero.
     *
     * <p>Only a sub-select the statement reads as a join carries the restriction, and that is what
     * {@code IN} and {@code = ANY} written among the parts that must all hold are: a row of the
     * sub-select equal to anything but x decides nothing, so PostgreSQL never reads one. A
     * sub-select under NOT or under OR, one compared any other way, and one that groups its rows,
     * orders and cuts them or holds a set operation are all worked out in full, because which rows
     * such a one answers with is settled before the comparison is made at all.
     */
    List<Expression> restrictionsPushedInto(SelectStmt subquery) {
        for (int i = qualifying.size() - 1; i >= 0; i--) {
            List<Expression> parts = conjunctsOf(qualifying.get(i));
            Expression compared = null;
            for (Expression part : parts) {
                compared = semiJoinedWith(part, subquery);
                if (compared != null) break;
            }
            if (compared == null) continue;
            Expression answered = restrictableOutput(subquery);
            if (answered == null) return Collections.emptyList();
            Expression value = constantFor(compared, parts);
            if (value == null) return Collections.emptyList();
            return Collections.<Expression>singletonList(
                    new BinaryExpr(answered, BinaryExpr.BinOp.EQUAL, value));
        }
        return Collections.emptyList();
    }

    /** What one part of a qualification compares this sub-select's rows with, if it is one. */
    private static Expression semiJoinedWith(Expression part, SelectStmt subquery) {
        if (part instanceof InExpr) {
            InExpr in = (InExpr) part;
            if (in.negated() || in.fromAny() || in.values() == null || in.values().size() != 1) {
                return null;
            }
            Expression only = in.values().get(0);
            return only instanceof SubqueryExpr
                    && answersWithRowsOf(((SubqueryExpr) only).subquery(), subquery)
                    ? in.expr() : null;
        }
        if (part instanceof AnyAllExpr) {
            AnyAllExpr any = (AnyAllExpr) part;
            return !any.isAll() && any.op() == BinaryExpr.BinOp.EQUAL
                    && answersWithRowsOf(any.subquery(), subquery) ? any.left() : null;
        }
        return null;
    }

    /**
     * Whether what a sub-select written in a qualification answers with is the rows this query
     * holds.
     *
     * <p>A set operation answers with the rows of the queries it is written out of, so a
     * restriction on what it answers with is a restriction on each of them, and that is where
     * PostgreSQL puts it. EXCEPT is not such a one: a row taken out of what the right side answers
     * with is a row left in what the operation answers with, so a restriction read there says the
     * opposite of what it says above. Neither is an operation that takes its rows from a place
     * among them, which is settled before the restriction is read at all.
     */
    private static boolean answersWithRowsOf(Statement written, SelectStmt subquery) {
        if (written == subquery) return true;
        if (!(written instanceof SetOpStmt)) return false;
        SetOpStmt operation = (SetOpStmt) written;
        if (operation.limit() != null || operation.offset() != null) return false;
        if (operation.op() == SetOpStmt.SetOpType.EXCEPT) return false;
        return answersWithRowsOf(operation.left(), subquery)
                || answersWithRowsOf(operation.right(), subquery);
    }

    /**
     * The one expression such a sub-select answers with, where a restriction on it is a restriction
     * on the relation underneath. A sub-select that cuts its rows down to a number of them, that
     * answers one of a group by a rule the group's other rows take part in, or that locks what it
     * read settles which rows it holds above the scan, and a restriction below that is a different
     * question -- so PostgreSQL leaves such a one to be worked out as it was written.
     */
    private static Expression restrictableOutput(SelectStmt subquery) {
        if (subquery.targets() == null || subquery.targets().size() != 1) return null;
        if (subquery.limit() != null || subquery.offset() != null || subquery.having() != null
                || subquery.lockClause() != null) return null;
        if (holdsAny(subquery.distinctOn()) || holdsAny(subquery.windowDefs())
                || holdsAny(subquery.withClauses()) || holdsAny(subquery.groupingSets())) {
            return null;
        }
        Expression only = subquery.targets().get(0).expr();
        if (only instanceof WildcardExpr || only instanceof CompositeStarExpr) return null;
        // A grouped sub-select answers one row of each group, named by what it grouped on, so a
        // restriction on that name holds of every row the group could have been made from.
        if (holdsAny(subquery.groupBy()) && !subquery.groupBy().contains(only)) return null;
        return only;
    }

    private static boolean holdsAny(List<?> written) {
        return written != null && !written.isEmpty();
    }

    /** What the parts that must all hold say this expression is, where they say it is a constant. */
    private static Expression constantFor(Expression compared, List<Expression> parts) {
        for (Expression part : parts) {
            if (!(part instanceof BinaryExpr)) continue;
            BinaryExpr equality = (BinaryExpr) part;
            if (equality.op() != BinaryExpr.BinOp.EQUAL) continue;
            if (equality.right() instanceof Literal && compared.equals(equality.left())) {
                return equality.right();
            }
            if (equality.left() instanceof Literal && compared.equals(equality.right())) {
                return equality.left();
            }
        }
        // A name compared with another that is itself compared with a constant is compared with
        // that constant, so the restriction survives a join between the statement's own relations.
        if (!(compared instanceof ColumnRef)) return null;
        ColumnRef ref = (ColumnRef) compared;
        for (Expression implied : DmlExecutor.impliedEqualities(parts)) {
            BinaryExpr equality = (BinaryExpr) implied;
            ColumnRef named = (ColumnRef) equality.left();
            if (ref.table() != null && named.table() != null && ref.column() != null
                    && named.column() != null
                    && ref.table().equalsIgnoreCase(named.table())
                    && ref.column().equalsIgnoreCase(named.column())) {
                return equality.right();
            }
        }
        return null;
    }

    /**
     * What a LATERAL item's own comparisons say about the relation the query above reads it as.
     *
     * <p>PostgreSQL pulls a lateral item up into the query reading it, so a comparison written
     * inside the item stands beside that query's own: {@code (SELECT * FROM t z WHERE z.a = o.a)}
     * read as {@code s} says {@code s.a = o.a}, and beside {@code o.a = 5} that says {@code s.a =
     * 5}. That is the restriction PostgreSQL puts on the item's own scan, and a row it rejects is
     * decided before the generation expression of a VIRTUAL column of that row is ever reached --
     * which is what leaves {@code o LEFT JOIN LATERAL (SELECT * FROM t z WHERE z.a = o.a) s ON s.g
     * = 2 WHERE o.a = 5} readable where {@code 10/a} raises for the row a is zero in.
     *
     * <p>Only an item that answers with its relation's columns can be read this way: a select list
     * that computes anything answers with something no column of the relation stands behind. What
     * the relation above calls a column is read off the place that column stands in, so a select
     * list that renames one and an alias list over the item are both read back. A name written
     * without its relation is the item's own wherever the item has a column of that name, an
     * unqualified name reaching the relation nearest it before anything the query above holds; and
     * where the item has no such column, what the comparison derives speaks of a column the
     * relation above has not got either, and decides nothing about any of its rows.
     */
    void lateralCorrelations(List<SelectStmt.FromItem> fromItems, List<Expression> out) {
        if (fromItems == null) return;
        for (SelectStmt.FromItem item : fromItems) collectLateralCorrelations(item, out);
    }

    private void collectLateralCorrelations(SelectStmt.FromItem item, List<Expression> out) {
        if (item instanceof SelectStmt.JoinFrom) {
            collectLateralCorrelations(((SelectStmt.JoinFrom) item).left(), out);
            collectLateralCorrelations(((SelectStmt.JoinFrom) item).right(), out);
            return;
        }
        if (!(item instanceof SelectStmt.SubqueryFrom)) return;
        SelectStmt.SubqueryFrom sub = (SelectStmt.SubqueryFrom) item;
        if (!sub.lateral()) return;
        if (!(sub.subquery() instanceof SelectStmt)) return;
        SelectStmt inner = (SelectStmt) sub.subquery();
        if (inner.where() == null || inner.from() == null || inner.from().size() != 1) return;
        SelectStmt.FromItem only = inner.from().get(0);
        if (only instanceof SelectStmt.JoinFrom) return;
        String under = SelectExecutor.exposedNameOf(only);
        String alias = lateralAlias(sub);
        if (under == null || !answersWithItsRelationsColumns(inner, under)) return;
        List<String> aliases = sub.columnAliases();
        for (Expression part : conjunctsOf(inner.where())) {
            if (!(part instanceof BinaryExpr)) continue;
            BinaryExpr equality = (BinaryExpr) part;
            if (equality.op() != BinaryExpr.BinOp.EQUAL) continue;
            Expression left = asWrittenAbove(equality.left(), inner, only, under, alias, aliases);
            Expression right = asWrittenAbove(equality.right(), inner, only, under, alias, aliases);
            if (left == null || right == null) continue;
            out.add(new BinaryExpr(left, BinaryExpr.BinOp.EQUAL, right));
        }
    }

    /**
     * Whether a query answers with its one relation's columns, whatever it calls them.
     *
     * <p>A column the select list renamed still stands where it stood, and the name the relation
     * above answers to it by is read off that place rather than off the name it had underneath.
     */
    private static boolean answersWithItsRelationsColumns(SelectStmt inner, String under) {
        if (inner.targets() == null || inner.targets().isEmpty()) return false;
        for (SelectStmt.SelectTarget target : inner.targets()) {
            if (target.expr() instanceof WildcardExpr) {
                String qualifier = ((WildcardExpr) target.expr()).table();
                if (target.alias() != null
                        || !(qualifier == null || qualifier.equalsIgnoreCase(under))) {
                    return false;
                }
                continue;
            }
            if (!(target.expr() instanceof ColumnRef)) return false;
            ColumnRef ref = (ColumnRef) target.expr();
            if (ref.column() == null) return false;
            if (ref.table() != null && !ref.table().equalsIgnoreCase(under)) return false;
        }
        return true;
    }

    /** One side of such a comparison as the query above writes it, or null where it cannot. */
    private Expression asWrittenAbove(Expression side, SelectStmt inner, SelectStmt.FromItem only,
                                      String under, String alias, List<String> aliases) {
        if (side instanceof Literal) return side;
        if (!(side instanceof ColumnRef)) return null;
        ColumnRef ref = (ColumnRef) side;
        if (ref.column() == null) return null;
        // A name qualified with the item's own relation is the relation above's, under the name the
        // item answers to, and so is one written with no relation at all: an unqualified name is
        // resolved against the relation nearest it, which is the item's own. Any other qualifier
        // names a relation of the query above, which reads there exactly as it reads inside.
        if (ref.table() != null && !ref.table().equalsIgnoreCase(under)) return ref;
        String above = holdsAny(aliases)
                ? nameInTheList(inner, only, ref.column(), aliases)
                : nameWrittenInTheList(inner, ref.column());
        return above == null ? null : new ColumnRef(alias, above);
    }

    /**
     * The name the item's select list gives the column a comparison inside it speaks of, where no
     * alias list stands over it.
     *
     * <p>A target that names the column answers above to whatever the target was written as, so a
     * comparison with {@code z.a} speaks above of what {@code z.a AS aa} exposes, which is
     * {@code aa}. A {@code *} answers to the column's own name, and so does a column no target
     * names at all -- which the relation above has not got, and which decides nothing about it.
     */
    private static String nameWrittenInTheList(SelectStmt inner, String column) {
        for (SelectStmt.SelectTarget target : inner.targets()) {
            if (!(target.expr() instanceof ColumnRef)) continue;
            String named = ((ColumnRef) target.expr()).column();
            if (named == null || !named.equalsIgnoreCase(column)) continue;
            return target.alias() != null ? target.alias() : named;
        }
        return column;
    }

    /**
     * The name an alias list on the item gives the column a comparison inside it speaks of.
     *
     * <p>The list renames the columns the item exposes one for one from the left, so the name to
     * write above is the one standing where that column stands in what the item answers with --
     * whatever the select list called it there -- and a column past the end of the list keeps that
     * name. A {@code *} stands for the columns of the relation underneath, in the order that
     * relation has them, which is something this can read only where the name reaches a relation
     * whose columns are recorded; a WITH item is built from a query, and finding out what one
     * exposes means reading it. A column the item exposes twice stands in two places under two
     * names of the list, and the two carry one value, so what is written of the first place is
     * written of the second as well.
     */
    private String nameInTheList(SelectStmt inner, SelectStmt.FromItem only, String column,
                                 List<String> aliases) {
        List<Column> underColumns = null;
        int position = 0;
        int found = -1;
        String own = null;
        for (SelectStmt.SelectTarget target : inner.targets()) {
            if (target.expr() instanceof WildcardExpr) {
                if (underColumns == null) {
                    underColumns = namedRelationColumns(only);
                    if (underColumns == null) return null;
                }
                for (Column col : underColumns) {
                    if (found < 0 && col.getName().equalsIgnoreCase(column)) {
                        found = position;
                        own = col.getName();
                    }
                    position++;
                }
                continue;
            }
            if (!(target.expr() instanceof ColumnRef)) return null;
            String named = ((ColumnRef) target.expr()).column();
            if (found < 0 && named != null && named.equalsIgnoreCase(column)) {
                found = position;
                own = target.alias() != null ? target.alias() : named;
            }
            position++;
        }
        if (found < 0) return null;
        return found < aliases.size() ? aliases.get(found) : own;
    }

    /**
     * The columns a FROM item exposes where they can be read off a relation rather than worked out
     * by reading a query: a stored relation reached by a name of its own, with nothing renaming it.
     */
    private List<Column> storedRelationColumns(SelectStmt.FromItem only) {
        if (!(only instanceof SelectStmt.TableRef)) return null;
        SelectStmt.TableRef ref = (SelectStmt.TableRef) only;
        if (holdsAny(ref.columnAliases())) return null;
        if (lookupCteFor(ref) != null || viewFor(ref) != null) return null;
        try {
            Table table = executor.resolveTable(
                    ref.schema() != null ? ref.schema() : executor.defaultSchema(), ref.table());
            return table == null ? null : table.getColumns();
        } catch (RuntimeException unreachable) {
            return null;
        }
    }

    /**
     * The same, for a relation whose columns are written down rather than stored.
     *
     * <p>A view keeps the columns its body answered with from the moment it was defined, and where
     * that body is one relation's columns the definition says what they are and in what order. Both
     * are read off what is recorded, so a {@code *} over a view can be counted through without the
     * view being read. Without it a view was the one relation a column standing behind an alias
     * list could not be found in.
     */
    private List<Column> namedRelationColumns(SelectStmt.FromItem only) {
        List<Column> stored = storedRelationColumns(only);
        if (stored != null) return stored;
        if (!(only instanceof SelectStmt.TableRef)) return null;
        SelectStmt.TableRef ref = (SelectStmt.TableRef) only;
        if (holdsAny(ref.columnAliases()) || lookupCteFor(ref) != null) return null;
        Database.ViewDef view = viewFor(ref);
        if (view == null) return null;
        if (holdsAny(view.cachedColumns())) return view.cachedColumns();
        return bodyColumns(view.query());
    }

    /**
     * The columns a view's body answers with, where each of them is a column of the one stored
     * relation the body reads. A body that computes anything, that reads more than one relation or
     * that reads something built from a query answers with columns of its own, and what those are
     * is settled by running it.
     */
    private List<Column> bodyColumns(Statement query) {
        if (!(query instanceof SelectStmt)) return null;
        SelectStmt body = (SelectStmt) query;
        if (body.from() == null || body.from().size() != 1) return null;
        if (body.targets() == null || body.targets().isEmpty()) return null;
        SelectStmt.FromItem only = body.from().get(0);
        List<Column> under = storedRelationColumns(only);
        if (under == null) return null;
        String named = SelectExecutor.exposedNameOf(only);
        List<Column> answered = new ArrayList<>();
        for (SelectStmt.SelectTarget target : body.targets()) {
            if (target.alias() != null && target.expr() instanceof WildcardExpr) return null;
            if (target.expr() instanceof WildcardExpr) {
                String qualifier = ((WildcardExpr) target.expr()).table();
                if (qualifier != null && !qualifier.equalsIgnoreCase(named)) return null;
                answered.addAll(under);
                continue;
            }
            if (!(target.expr() instanceof ColumnRef)) return null;
            ColumnRef read = (ColumnRef) target.expr();
            if (read.column() == null) return null;
            if (read.table() != null && !read.table().equalsIgnoreCase(named)) return null;
            Column behind = null;
            for (Column candidate : under) {
                if (candidate.getName().equalsIgnoreCase(read.column())) {
                    behind = candidate;
                    break;
                }
            }
            if (behind == null) return null;
            answered.add(target.alias() == null ? behind : behind.withName(target.alias()));
        }
        return answered;
    }

    /** The names a FROM clause answers to: one per item, and both sides of every join. */
    private static void exposedNames(List<SelectStmt.FromItem> fromItems, Set<String> out) {
        if (fromItems == null) return;
        for (SelectStmt.FromItem item : fromItems) collectExposedName(item, out);
    }

    private static void collectExposedName(SelectStmt.FromItem item, Set<String> out) {
        if (item instanceof SelectStmt.JoinFrom) {
            collectExposedName(((SelectStmt.JoinFrom) item).left(), out);
            collectExposedName(((SelectStmt.JoinFrom) item).right(), out);
            return;
        }
        String name = SelectExecutor.exposedNameOf(item);
        if (name != null) out.add(name.toLowerCase(java.util.Locale.ROOT));
    }

    private static void collectConjuncts(Expression expr, List<Expression> out) {
        if (expr == null) return;
        if (expr instanceof BinaryExpr && ((BinaryExpr) expr).op() == BinaryExpr.BinOp.AND) {
            collectConjuncts(((BinaryExpr) expr).left(), out);
            collectConjuncts(((BinaryExpr) expr).right(), out);
            return;
        }
        out.add(expr);
    }

    private static void collectJoinConjuncts(List<SelectStmt.FromItem> fromItems, String alias,
                                             List<Expression> out) {
        if (fromItems == null) return;
        for (SelectStmt.FromItem item : fromItems) collectJoinConjuncts(item, alias, out);
    }

    private static void collectJoinConjuncts(SelectStmt.FromItem item, String alias,
                                             List<Expression> out) {
        if (!(item instanceof SelectStmt.JoinFrom)) return;
        SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
        collectJoinConjuncts(join.left(), alias, out);
        collectJoinConjuncts(join.right(), alias, out);
        SelectStmt.JoinType type = join.joinType();
        if (type == SelectStmt.JoinType.INNER || type == SelectStmt.JoinType.CROSS) {
            collectConjuncts(join.on(), out);
            return;
        }
        // An outer join's condition settles which rows are paired, not which rows there are, for
        // the side it preserves: a row of that side the condition rejects is still answered, padded
        // with nulls. The other side is not preserved, so one of ITS rows the condition rejects is
        // paired with nothing and never answered at all -- which makes the condition a restriction
        // on that side's scan, exactly as a WHERE clause is.
        Set<String> notPreserved = new HashSet<>();
        if (type == SelectStmt.JoinType.LEFT || type == SelectStmt.JoinType.NATURAL_LEFT) {
            collectExposedName(join.right(), notPreserved);
        } else if (type == SelectStmt.JoinType.RIGHT || type == SelectStmt.JoinType.NATURAL_RIGHT) {
            collectExposedName(join.left(), notPreserved);
        }
        if (alias != null && notPreserved.contains(alias.toLowerCase(java.util.Locale.ROOT))) {
            collectConjuncts(join.on(), out);
        }
    }

    /** The parts of a qualification that must all hold, in the order they were written. */
    static List<Expression> conjunctsOf(Expression qualification) {
        List<Expression> parts = new ArrayList<>();
        collectConjuncts(qualification, parts);
        return parts;
    }

    /** The names a FROM clause answers to, one per relation and both sides of every join. */
    static Set<String> exposedNamesOf(List<SelectStmt.FromItem> fromItems) {
        Set<String> names = new HashSet<>();
        exposedNames(fromItems, names);
        return names;
    }

    /**
     * Whether a qualification can be decided from one relation's row, and would be decided the
     * same way twice: every name in it is that relation's or an enclosing query's, and nothing in
     * it is a query, a call or a window -- a call may read something other than the row it is
     * handed, and deciding it here as well as above would then be deciding two different things.
     *
     * <p>A name the reading query's own FROM clause does not answer to is a row an enclosing query
     * holds, and that row stands still while this relation is read, so the part reads here what it
     * reads where it was written and a row it rejects is a row the qualification rejects too. That
     * is what leaves the relation under {@code EXISTS (SELECT 1 FROM (SELECT * FROM t) s WHERE s.a
     * = o.a AND s.g = 2)} readable when the generation expression raises for a row this outer row
     * never pairs with. A name the query does read is another of its relations, and what that one
     * says depends on which row it paired with, which is not settled here.
     */
    private static boolean speaksOnlyOf(Expression expr, String alias, boolean lone,
                                        Set<String> read, boolean paired) {
        final boolean[] usable = {true};
        final boolean[] named = {false};
        AstWalk.forEach(expr, node -> {
            if (node instanceof Statement || node instanceof FunctionCallExpr
                    || node instanceof WindowFuncExpr || node instanceof OrderedSetAggExpr) {
                usable[0] = false;
                return;
            }
            if (!(node instanceof ColumnRef)) return;
            ColumnRef ref = (ColumnRef) node;
            if (ref.column() == null) {
                usable[0] = false;
            } else if (ref.table() == null) {
                if (lone) named[0] = true; else usable[0] = false;
            } else if (ref.table().equalsIgnoreCase(alias)) {
                named[0] = true;
            } else if (read.contains(ref.table().toLowerCase(java.util.Locale.ROOT)) || paired) {
                // A relation this query does not read is one the statement above holds a row of,
                // and that row stands still while this scan runs -- but where that statement reads
                // this query as one join with itself, what compares the two is the condition of
                // that join and settles which rows pair rather than which rows there are.
                usable[0] = false;
            }
        });
        return usable[0] && named[0];
    }

    /**
     * The view a FROM item names. A qualified reference reaches the view in the schema it names
     * and nothing else: another schema's view of the same name is a different relation, and a
     * table in the named schema is what the reference means when that schema holds no such view.
     */
    Database.ViewDef viewFor(SelectStmt.TableRef tableRef) {
        if (tableRef.schema() != null) {
            return executor.database.getView(tableRef.schema(), tableRef.table());
        }
        return executor.database.getView(tableRef.table());
    }

    // ---- Table Bindings (column structure without data) ----

    List<RowContext.TableBinding> resolveTableBindings(List<SelectStmt.FromItem> fromItems) {
        List<RowContext.TableBinding> bindings = new ArrayList<>();
        for (SelectStmt.FromItem item : fromItems) {
            // A comma between two FROM items exposes the left one to the right the same way an
            // explicit join does, so a LATERAL written after a comma reads the names before it and
            // has to be described with them in scope. Without this the item could not be described
            // at all and the names it exposes went missing from the query level's scope.
            if (readsItemsToItsLeft(item) && !bindings.isEmpty()) {
                executor.outerContextStack.push(new RowContext(new ArrayList<>(bindings)));
                try {
                    resolveTableBindingsFromItem(item, bindings);
                } finally {
                    executor.outerContextStack.pop();
                }
            } else {
                resolveTableBindingsFromItem(item, bindings);
            }
        }
        return bindings;
    }

    /**
     * How many relations a FROM clause names, counting a join as the items on either side of it.
     *
     * <p>{@link #resolveTableBindings} adds exactly one binding per named relation and silently
     * adds none for one it cannot describe — a sequence, a WITH item still being defined, a
     * lateral it cannot read — so a count that comes up short is a description that is missing a
     * relation. Anything reading the bindings as <em>everything</em> this query level supplies has
     * to know that, or a column the missing relation holds looks like a column nothing holds.
     */
    static int relationCount(List<SelectStmt.FromItem> fromItems) {
        if (fromItems == null) return 0;
        int n = 0;
        for (SelectStmt.FromItem item : fromItems) n += relationCount(item);
        return n;
    }

    private static int relationCount(SelectStmt.FromItem item) {
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            return relationCount(join.left()) + relationCount(join.right());
        }
        return 1;
    }

    /** A LATERAL sub-select, and a function in FROM, which is lateral whether or not it says so. */
    private static boolean readsItemsToItsLeft(SelectStmt.FromItem item) {
        if (item instanceof SelectStmt.FunctionFrom) return true;
        return item instanceof SelectStmt.SubqueryFrom
                && ((SelectStmt.SubqueryFrom) item).lateral();
    }

    /**
     * The names and columns a FROM item exposes, with every value null.
     *
     * <p>An outer join has to name-pad the side that contributed nothing: the whole point of
     * {@code t1 RIGHT JOIN t2} is to answer with NULLs where {@code t1} has no match, and those
     * rows must still answer to {@code t1}'s name. Taking the shape from the first row of the
     * side only works while the side has a row; when it is empty — because the relation is empty,
     * because a subquery filtered everything away, or because the condition is never true — the
     * shape has to come from the FROM item itself.
     */
    List<RowContext.TableBinding> resolveItemShape(SelectStmt.FromItem item) {
        List<RowContext.TableBinding> bindings = new ArrayList<>();
        try {
            resolveTableBindingsFromItem(item, bindings);
        } catch (RuntimeException e) {
            // The shape is a convenience, not the answer: a FROM item that cannot be described
            // (an unreadable lateral, say) leaves the padding as it was before.
            return Cols.listOf();
        }
        return bindings;
    }

    /**
     * The columns a FROM item exposes, in order, when it produced no rows to read them from.
     *
     * <p>Every column of every relation, except where a USING or NATURAL join merged two of them
     * into one — which also decides where in the list they sit. A join whose clause cannot be
     * satisfied is described as though it had none: a query that answers with no rows still has
     * to say what its columns are, and it is the executed join, not this, that refuses it.
     *
     * @param bindings the same item's bindings, which the returned columns index into
     */
    List<RowContext.OutCol> resolveItemOutput(SelectStmt.FromItem item,
                                              List<RowContext.TableBinding> bindings) {
        try {
            Described described = describe(item);
            return described.bindings.size() == bindings.size() ? described.output
                    : RowContext.defaultOutput(bindings);
        } catch (RuntimeException e) {
            return RowContext.defaultOutput(bindings);
        }
    }

    /** The columns a whole FROM clause exposes, its items placed side by side. */
    List<RowContext.OutCol> resolveClauseOutput(List<SelectStmt.FromItem> fromItems,
                                                List<RowContext.TableBinding> bindings) {
        if (fromItems == null || fromItems.isEmpty()) return RowContext.defaultOutput(bindings);
        List<RowContext.OutCol> out = new ArrayList<>();
        int offset = 0;
        try {
            List<RowContext.TableBinding> soFar = new ArrayList<>();
            for (SelectStmt.FromItem item : fromItems) {
                Described described = describeReadingLeft(item, soFar);
                for (RowContext.OutCol oc : described.output) out.add(oc.shift(offset));
                offset += described.bindings.size();
                soFar.addAll(described.bindings);
            }
        } catch (RuntimeException e) {
            return RowContext.defaultOutput(bindings);
        }
        return offset == bindings.size() ? out : RowContext.defaultOutput(bindings);
    }

    /** A FROM item's relations and the columns it exposes over them, described together. */
    private static final class Described {
        final List<RowContext.TableBinding> bindings;
        final List<RowContext.OutCol> output;

        Described(List<RowContext.TableBinding> bindings, List<RowContext.OutCol> output) {
            this.bindings = bindings;
            this.output = output;
        }
    }

    /** {@link #describe} with the FROM items written before this one in scope, for a LATERAL. */
    private Described describeReadingLeft(SelectStmt.FromItem item,
                                          List<RowContext.TableBinding> soFar) {
        if (!readsItemsToItsLeft(item) || soFar.isEmpty()) return describe(item);
        executor.outerContextStack.push(new RowContext(new ArrayList<>(soFar)));
        try {
            return describe(item);
        } finally {
            executor.outerContextStack.pop();
        }
    }

    private Described describe(SelectStmt.FromItem item) {
        if (!(item instanceof SelectStmt.JoinFrom)) {
            List<RowContext.TableBinding> bindings = new ArrayList<>();
            resolveTableBindingsFromItem(item, bindings);
            return new Described(bindings, RowContext.defaultOutput(bindings));
        }
        SelectStmt.JoinFrom jf = (SelectStmt.JoinFrom) item;
        Described left = describe(jf.left());
        Described right;
        // A LATERAL item reads the names to its left, so describing it needs those names in
        // scope even when they carry no row, exactly as resolveTableBindingsFromItem has it.
        if (readsItemsToItsLeft(jf.right())) {
            executor.outerContextStack.push(new RowContext(new ArrayList<>(left.bindings)));
            try {
                right = describe(jf.right());
            } finally {
                executor.outerContextStack.pop();
            }
        } else {
            right = describe(jf.right());
        }
        List<RowContext.TableBinding> bindings = new ArrayList<>(left.bindings);
        bindings.addAll(right.bindings);
        List<String> using = jf.using();
        if (FromJoinExecutor.isNatural(jf.joinType())) {
            using = FromJoinExecutor.naturalNames(left.output, right.output);
        }
        return new Described(bindings, FromJoinExecutor.shapeOfJoin(
                left.output, left.bindings.size(), right.output, using, jf.joinType()).output);
    }

    private static void addBinding(List<RowContext.TableBinding> bindings, Table table,
                                   String alias) {
        bindings.add(new RowContext.TableBinding(table, alias,
                new Object[table.getColumns().size()]));
    }

    /**
     * The relation as this FROM item exposes it.
     *
     * <p>An alias list renames the columns for this query level and nothing else about them: the
     * types behind the new names are the relation's own. A description that kept the stored names
     * would answer for names the query cannot write and refuse the ones it can, so every relation
     * a name can reach — a stored table, a WITH item, a view, a catalog — is renamed here.
     */
    private static Table asWritten(Table table, String alias, List<String> columnAliases) {
        if (columnAliases == null || columnAliases.isEmpty()) return table;
        List<Column> renamed = FromFunctionResolver.applyColumnAliases(
                new ArrayList<>(table.getColumns()), columnAliases);
        Table exposed = new Table(alias, renamed);
        String[] defined = new String[renamed.size()];
        for (int i = 0; i < renamed.size(); i++) defined[i] = DefinedTypes.typeIn(table, i);
        exposed.setDefinedColumnTypes(defined);
        exposed.setFunctionResult(table.isFunctionResult());
        // Renaming the columns for this query level says nothing about where the rows come from,
        // so a stored relation written under an alias list still has its rows stored.
        exposed.setStoresRows(table.storesRows());
        return exposed;
    }

    private void resolveTableBindingsFromItem(SelectStmt.FromItem item, List<RowContext.TableBinding> bindings) {
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef tableRef = (SelectStmt.TableRef) item;
            String schemaName = tableRef.schema() != null ? tableRef.schema() : executor.defaultSchema();
            String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
            // Check CTEs first
            SelectStmt.CommonTableExpr cte = lookupCteFor(tableRef);
            if (cte != null) {
                QueryResult cteResult = readCte(cte, alias, tableRef.columnAliases());
                Table virtualTable = new Table(alias, cteResult.getColumns());
                defineFromCte(virtualTable, cte);
                addBinding(bindings, asWritten(virtualTable, alias, tableRef.columnAliases()), alias);
                return;
            }
            // Check views
            Database.ViewDef view = viewFor(tableRef);
            if (view != null) {
                // Materialized views know their columns without re-running the query
                // (and an unpopulated matview must still be describable).
                if (view.materialized() && view.cachedColumns() != null && !view.cachedColumns().isEmpty()) {
                    Table mv = new Table(alias, view.cachedColumns());
                    addBinding(bindings, asWritten(mv, alias, tableRef.columnAliases()), alias);
                    return;
                }
                try {
                    QueryResult vr = readAsDerivedRelation(alias, tableRef.columnAliases(),
                            () -> executor.executeViewQuery(tableRef.table(), view.query()));
                    if (!vr.getColumns().isEmpty()) {
                        Table virtualTable = new Table(alias, vr.getColumns());
                        defineFromView(virtualTable, view);
                        addBinding(bindings,
                                asWritten(virtualTable, alias, tableRef.columnAliases()), alias);
                    }
                } catch (Exception e) { /* skip */ }
                return;
            }
            // Check system catalogs
            boolean userTableExists = false;
            try { executor.resolveTable(schemaName, tableRef.table()); userTableExists = true; } catch (MemgresException ignored) {}
            if (!userTableExists && SystemCatalog.isSystemCatalog(tableRef.schema(), tableRef.table())) {
                recordCatalogReadLock(tableRef.schema(), tableRef.table());
                Table catalogTable = executor.systemCatalog.resolve(tableRef.schema(), tableRef.table(), executor.session);
                if (catalogTable != null) {
                    addBinding(bindings,
                            asWritten(catalogTable, alias, tableRef.columnAliases()), alias);
                    return;
                }
            }
            // Regular table
            try {
                Table table = executor.resolveTable(schemaName, tableRef.table());
                addBinding(bindings, asWritten(table, alias, tableRef.columnAliases()), alias);
            } catch (MemgresException e) { /* table not found, skip */ }
        } else if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom joinFrom = (SelectStmt.JoinFrom) item;
            resolveTableBindingsFromItem(joinFrom.left(), bindings);
            // A LATERAL item reads the names to its left, so describing it needs those names in
            // scope even when they carry no row — otherwise the describe fails and the lateral
            // alias goes missing from a query that answers with no rows at all.
            if (readsItemsToItsLeft(joinFrom.right())) {
                executor.outerContextStack.push(new RowContext(new ArrayList<>(bindings)));
                try {
                    resolveTableBindingsFromItem(joinFrom.right(), bindings);
                } finally {
                    executor.outerContextStack.pop();
                }
            } else {
                resolveTableBindingsFromItem(joinFrom.right(), bindings);
            }
            // A USING or NATURAL join still names both relations and both keep all their columns;
            // what changes is which of them the join exposes, and that is the output shape's
            // business (see resolveItemOutput), not the bindings'.
        } else if (item instanceof SelectStmt.SubqueryFrom) {
            SelectStmt.SubqueryFrom subqFrom = (SelectStmt.SubqueryFrom) item;
            if (subqFrom.alias() != null) {
                try {
                    QueryResult sqResult = readAsDerivedRelation(subqFrom.alias(),
                            subqFrom.columnAliases(),
                            () -> executor.executeStatement(subqFrom.subquery()));
                    if (!sqResult.getColumns().isEmpty()) {
                        List<Column> columns = FromFunctionResolver.applyColumnAliases(
                                new ArrayList<>(sqResult.getColumns()), subqFrom.columnAliases());
                        String sqAlias = subqFrom.alias();
                        Table virtualTable = new Table(sqAlias, columns);
                        defineFromQuery(virtualTable, subqFrom.subquery());
                        bindings.add(new RowContext.TableBinding(virtualTable, sqAlias,
                                new Object[columns.size()]));
                    }
                } catch (Exception e) { /* skip, can't resolve */ }
            }
        } else if (item instanceof SelectStmt.FunctionFrom) {
            SelectStmt.FunctionFrom funcFrom = (SelectStmt.FunctionFrom) item;
            String alias = funcFrom.alias() != null ? funcFrom.alias() : funcFrom.functionName();
            // XMLTABLE: extract column definitions from encoded args
            if (funcFrom.functionName().equals("__xmltable__")) {
                List<Column> cols = new ArrayList<>();
                for (int i = 2; i < funcFrom.args().size(); i++) {
                    Expression arg = funcFrom.args().get(i);
                    String def = arg instanceof Literal ? ((Literal) arg).value() : arg.toString();
                    String[] parts = FromFunctionResolver.splitColumnDefinition(def);
                    DataType dt = parts[1] == null ? null : DataType.fromPgName(parts[1]);
                    cols.add(new Column(parts[0], dt != null ? dt : DataType.TEXT, true, false, null));
                }
                Table virtualTable = new Table(alias, cols);
                // The columns above fall back to text wherever the definition list named a type
                // this does not read, so none of them is a type to be trusted.
                virtualTable.setDefinedColumnTypes(new String[cols.size()]);
                bindings.add(new RowContext.TableBinding(virtualTable, alias, new Object[cols.size()]));
            }
            // JSON_TABLE: extract column definitions from the JsonTableExpr
            else if (funcFrom.functionName().equals("__json_table__") && !funcFrom.args().isEmpty()
                    && funcFrom.args().get(0) instanceof JsonTableExpr) {
                JsonTableExpr jt = (JsonTableExpr) funcFrom.args().get(0);
                List<Column> cols = new ArrayList<>();
                List<String> types = new ArrayList<>();
                FromFunctionResolver.collectJsonTableColumns(jt.columns, cols, types);
                cols = FromFunctionResolver.applyColumnAliases(
                        cols, FromFunctionResolver.stripColTypes(funcFrom.columnAliases()), null);
                Table virtualTable = new Table(alias, cols);
                // The COLUMNS list names each column's type, so these are types to be trusted.
                virtualTable.setDefinedColumnTypes(types.toArray(new String[0]));
                bindings.add(new RowContext.TableBinding(virtualTable, alias, new Object[cols.size()]));
            } else {
                // The shape describes an item that produced no row, so the type has to come from
                // the call rather than from a value; text is only the fallback.
                DataType known = functionResolver.singleColumnType(funcFrom);
                DataType colType = known != null ? known : DataType.TEXT;
                List<String> ca = funcFrom.columnAliases();
                List<Column> cols = new ArrayList<>();
                if (ca != null && !ca.isEmpty()) {
                    for (String colName : ca) {
                        cols.add(new Column(FromFunctionResolver.stripColType(colName), colType, true, false, null));
                    }
                } else {
                    cols.add(new Column(alias, colType, true, false, null));
                }
                // WITH ORDINALITY's column is a bigint whether or not the item produced a row to
                // read the type off, and it is the last one however the aliases named them.
                if (funcFrom.withOrdinality()) {
                    if (ca == null || ca.size() < 2) {
                        cols.add(new Column("ordinality", DataType.BIGINT, true, false, null));
                    } else {
                        Column last = cols.get(cols.size() - 1);
                        cols.set(cols.size() - 1,
                                new Column(last.getName(), DataType.BIGINT, true, false, null));
                    }
                }
                Table virtualTable = new Table(alias, cols);
                defineFromFunction(virtualTable, funcFrom);
                // What a call in FROM actually produces is decided by running it: a record with
                // no column definition list holds whatever fields it holds, and a single column
                // answers to names it does not carry through attribute notation. The columns above
                // are a shape to pad rows with, not the register of what this relation supplies,
                // and marking the provenance is what keeps the analysis from reading them as one --
                // exactly as FromFunctionResolver marks the relation it builds from real rows.
                virtualTable.setFunctionResult(true);
                bindings.add(new RowContext.TableBinding(virtualTable, alias, new Object[cols.size()]));
            }
        }
    }

    // ---- What a relation built from a definition may be trusted about ----
    //
    // Every relation below is built by running something and describing whatever came back, so its
    // columns carry the type the builder read off a value rather than the type the definition
    // gives them. Each is marked as such, along with the types its definition does settle, so that
    // a check reading a column's type knows which of the two it has. See {@link DefinedTypes}.

    /** A derived table, a VALUES list, or a parenthesized join given an alias. */
    private void defineFromQuery(Table virtualTable, Statement query) {
        virtualTable.setDefinedColumnTypes(
                executor.definedTypes.ofQuery(query, virtualTable.getColumns().size()));
    }

    private void defineFromCte(Table virtualTable, SelectStmt.CommonTableExpr cte) {
        virtualTable.setDefinedColumnTypes(
                executor.definedTypes.ofCte(cte, virtualTable.getColumns().size()));
    }

    private void defineFromView(Table virtualTable, Database.ViewDef view) {
        virtualTable.setDefinedColumnTypes(
                executor.definedTypes.ofView(view, virtualTable.getColumns().size()));
    }

    /** A stored relation rebuilt behind an alias list: its own declared types, under new names. */
    private static void defineAsDeclared(Table renamed, Table declared) {
        String[] types = new String[renamed.getColumns().size()];
        for (int i = 0; i < types.length; i++) types[i] = DefinedTypes.typeIn(declared, i);
        renamed.setDefinedColumnTypes(types);
    }

    private void defineFromFunction(Table virtualTable, SelectStmt.FunctionFrom funcFrom) {
        virtualTable.setDefinedColumnTypes(
                executor.definedTypes.ofFunction(funcFrom, virtualTable.getColumns().size()));
    }

    // ---- FROM Clause Resolution ----

    /**
     * Resolve a SELECT's FROM clause, with its WHERE pushed down for early filtering during the
     * cross-product, and recording which query the clause belongs to. A full join is judged only
     * in the query the client sent, and a join below the ones this resolves belongs to whatever
     * subquery, WITH query or view it was written in.
     */
    List<RowContext> resolveFromClause(List<SelectStmt.FromItem> fromItems, Expression where,
                                       Expression having, Statement owner) {
        Expression priorWhere = enclosingWhere;
        Expression priorHaving = enclosingHaving;
        Statement priorQuery = currentQuery;
        List<Expression> priorJoinQuals = new ArrayList<>(joinQualsAbove);
        enclosingWhere = where;
        enclosingHaving = having;
        currentQuery = owner;
        joinQualsAbove.clear();
        try {
            List<RowContext> resolved = resolveFromClauseInner(fromItems, where);
            stampCoveredNames(fromItems, resolved);
            stampJoinedNames(fromItems, resolved);
            return resolved;
        } finally {
            enclosingWhere = priorWhere;
            enclosingHaving = priorHaving;
            currentQuery = priorQuery;
            joinQualsAbove.clear();
            joinQualsAbove.addAll(priorJoinQuals);
        }
    }

    /**
     * Tells each row which relations the FROM clause holds but does not answer to, so that a
     * qualifier naming one of them is reported as out of reach rather than as missing.
     *
     * <p>{@code (a JOIN b) AS j} exposes {@code j} and nothing else — {@code a} is written in the
     * query and cannot be referenced, which is a different mistake from writing a relation that is
     * not there at all, and PostgreSQL words the two differently.
     */
    private static void stampCoveredNames(List<SelectStmt.FromItem> fromItems,
                                          List<RowContext> resolved) {
        if (fromItems == null || resolved == null || resolved.isEmpty()) return;
        Set<String> covered = new LinkedHashSet<>();
        for (SelectStmt.FromItem item : fromItems) collectCoveredNames(item, covered);
        if (covered.isEmpty()) return;
        for (RowContext ctx : resolved) {
            for (RowContext.TableBinding b : ctx.getBindings()) {
                covered.remove(b.alias() != null ? b.alias().toLowerCase(java.util.Locale.ROOT)
                        : b.table().getName().toLowerCase(java.util.Locale.ROOT));
            }
            break;
        }
        if (covered.isEmpty()) return;
        for (RowContext ctx : resolved) ctx.setCoveredNames(covered);
    }

    /**
     * Tells each row which relations the FROM clause reaches only through a join, so that a system
     * column written unqualified is not read off one of them.
     *
     * <p>A join is one FROM item and exposes one row, made of its sides' ordinary columns. The
     * system columns are not among them: {@code SELECT ctid FROM a JOIN b ON ...} names nothing,
     * where over {@code FROM a, b} it names something both sides have and is ambiguous. See
     * {@code RowContext.joinedNames}.
     */
    private static void stampJoinedNames(List<SelectStmt.FromItem> fromItems,
                                         List<RowContext> resolved) {
        if (fromItems == null || resolved == null || resolved.isEmpty()) return;
        Set<String> joined = new LinkedHashSet<>();
        for (SelectStmt.FromItem item : fromItems) {
            if (item instanceof SelectStmt.JoinFrom) collectCoveredNames(item, joined);
        }
        if (joined.isEmpty()) return;
        for (RowContext ctx : resolved) ctx.setJoinedNames(joined);
    }

    /** Every relation name written anywhere in a FROM tree, however it was later renamed. */
    static void collectCoveredNames(SelectStmt.FromItem item, Set<String> out) {
        if (item instanceof SelectStmt.JoinFrom) {
            collectCoveredNames(((SelectStmt.JoinFrom) item).left(), out);
            collectCoveredNames(((SelectStmt.JoinFrom) item).right(), out);
            return;
        }
        if (item instanceof SelectStmt.TableRef) {
            SelectStmt.TableRef ref = (SelectStmt.TableRef) item;
            out.add(ref.alias() != null ? ref.alias().toLowerCase(java.util.Locale.ROOT) : ref.table().toLowerCase(java.util.Locale.ROOT));
            return;
        }
        // A parenthesized join given an alias is carried as a sub-query over the join itself; the
        // relations under it are written in the query and covered by that alias.
        //
        // A sub-SELECT is not that. Its FROM clause belongs to a query of its own and is no part
        // of this one's range table, so a qualifier naming a relation in it is a relation this
        // query does not have — missing rather than out of reach.
        if (item instanceof SelectStmt.SubqueryFrom
                && ((SelectStmt.SubqueryFrom) item).subquery() instanceof SelectStmt) {
            SelectStmt inner = (SelectStmt) ((SelectStmt.SubqueryFrom) item).subquery();
            if (inner.joinExpression() && inner.from() != null) {
                for (SelectStmt.FromItem f : inner.from()) collectCoveredNames(f, out);
            }
        }
    }

    /**
     * A FROM item that is not LATERAL cannot read a relation entered beside it.
     *
     * <p>The relations of one FROM clause are computed side by side, so a sub-select written as one
     * of them has no row of its neighbour to read — LATERAL is the word that says to compute it
     * once per such row instead. PostgreSQL says the entry is there but out of reach and names the
     * word that would bring it into reach; reporting the entry as missing sent the reader looking
     * for a relation they had written.
     *
     * <p>Which entries count is decided by position and nothing else. PostgreSQL builds the range
     * table left to right — a join's left side before its right, and a nested join in the place it
     * stands — and a sub-select is transformed against the entries made <em>before</em> it. So
     * {@code FROM t a, (SELECT a.v) b} names an entry that is there and out of reach, while
     * {@code FROM (SELECT a.v) b, t a} names one that has not been entered yet and is simply
     * missing. Whether a comma or a JOIN stands between them makes no difference: both enter the
     * relations in the order they are written, which is why this walks the whole clause rather
     * than its top-level items.
     *
     * <p>An entry answers both to the name it exposes and, where an alias renamed it, to the
     * relation's own name — PostgreSQL finds it either way, reports the name that was written and
     * names the entry in the detail.
     */
    private void rejectSiblingReferenceWithoutLateral(List<SelectStmt.FromItem> fromItems) {
        if (fromItems == null || fromItems.isEmpty()) return;
        Map<String, String> entered = new LinkedHashMap<>();
        for (SelectStmt.FromItem item : fromItems) enterRangeTableItem(item, entered, true);
    }

    /**
     * Whether a query level above this one supplies a relation of that name.
     *
     * <p>A sub-select written beside a relation cannot read it, but it can read the levels this
     * whole query is nested inside — that is an ordinary correlated reference and needs no LATERAL.
     * When both could be meant PostgreSQL takes the outer one, because the sibling is not in scope
     * at all: {@code SELECT 1 FROM t WHERE EXISTS (SELECT 1 FROM t z, (SELECT t.v) q)} reads the
     * outermost {@code t} and runs. So a name something above supplies is never this rule's.
     */
    private boolean enclosingLevelSupplies(String written) {
        if (written == null || executor == null) return false;
        for (RowContext outer : executor.outerContextStack) {
            List<RowContext.TableBinding> bindings = outer == null ? null : outer.getBindings();
            if (bindings == null) continue;
            for (RowContext.TableBinding b : bindings) {
                if (written.equalsIgnoreCase(b.alias())) return true;
                if (b.table() != null && written.equalsIgnoreCase(b.table().getName())) return true;
            }
        }
        return false;
    }

    /**
     * Walks one FROM tree in the order its relations enter the range table, judging each.
     *
     * @param lateralWouldHelp whether writing LATERAL on the item would be legal where it stands.
     *                         On the nullable side of a RIGHT or FULL join it would not — the rows
     *                         it would read are not determined when it is evaluated — so
     *                         PostgreSQL leaves the advice off there.
     */
    private void enterRangeTableItem(SelectStmt.FromItem item, Map<String, String> entered,
                                     boolean lateralWouldHelp) {
        if (item instanceof SelectStmt.JoinFrom) {
            SelectStmt.JoinFrom join = (SelectStmt.JoinFrom) item;
            enterRangeTableItem(join.left(), entered, lateralWouldHelp);
            enterRangeTableItem(join.right(), entered,
                    lateralWouldHelp && !nullsTheLeftSide(join.joinType()));
            return;
        }
        if (item instanceof SelectStmt.SubqueryFrom
                && !((SelectStmt.SubqueryFrom) item).lateral()
                && !entered.isEmpty()) {
            String written = SelectExecutor.firstReferenceTo(
                    ((SelectStmt.SubqueryFrom) item).subquery(), entered.keySet());
            if (written != null && enclosingLevelSupplies(written)) written = null;
            if (written != null) {
                String exposed = entered.get(written.toLowerCase(java.util.Locale.ROOT));
                MemgresException e = new MemgresException(
                        "invalid reference to FROM-clause entry for table \"" + written + "\"",
                        "42P01");
                e.setDetail("There is an entry for table \"" + (exposed == null ? written : exposed)
                        + "\", but it cannot be referenced from this part of the query.");
                if (lateralWouldHelp) {
                    e.setHint("To reference that table, you must mark this subquery with LATERAL.");
                }
                throw e;
            }
        }
        String exposed = SelectExecutor.exposedNameOf(item);
        if (exposed == null) return;
        rememberEntry(entered, exposed, exposed);
        if (item instanceof SelectStmt.TableRef) {
            rememberEntry(entered, ((SelectStmt.TableRef) item).table(), exposed);
        }
    }

    /** A join whose right side may be read before the left is known, so no LATERAL may stand there. */
    private static boolean nullsTheLeftSide(SelectStmt.JoinType type) {
        return type == SelectStmt.JoinType.RIGHT || type == SelectStmt.JoinType.FULL
                || type == SelectStmt.JoinType.NATURAL_RIGHT
                || type == SelectStmt.JoinType.NATURAL_FULL;
    }

    private static void rememberEntry(Map<String, String> entered, String name, String exposed) {
        if (name == null) return;
        String key = name.toLowerCase(Locale.ROOT);
        if (!entered.containsKey(key)) entered.put(key, exposed);
    }

    /**
     * Resolve the FROM of an UPDATE or the USING of a DELETE. The statement's WHERE is not pushed
     * into the scan: it also names the table being written, which is not one of these relations.
     * It does settle whether they are read at all, a statement no row can satisfy asking them for
     * nothing.
     */
    List<RowContext> resolveWrittenFromClause(List<SelectStmt.FromItem> fromItems,
                                              Expression qualification) {
        return readForWritingStatement(qualification, fromItems,
                () -> resolveFromClauseInner(fromItems, null));
    }

    /**
     * Resolve the source a MERGE reads beside its target, under the condition pairing the two where
     * that condition settles whether the source is read at all.
     *
     * <p>A source row that paired with no target row is answered by an arm written WHEN NOT
     * MATCHED, so PostgreSQL preserves the source side of the join and reads every row of it
     * whatever the condition says. With no such arm to answer for one, a condition decided against
     * before a row is read pairs nothing, and the source is never asked.
     */
    List<RowContext> resolveMergeSource(SelectStmt.FromItem source, Expression pairing) {
        return readForWritingStatement(pairing, null, () -> resolveFromItem(source));
    }

    /**
     * Whether one of the schemas PostgreSQL searches implicitly answers to this bare name.
     *
     * <p>pg_temp and pg_catalog are searched whether or not search_path names them, so an empty
     * path hides the user's schemas and neither of those. A name this says yes to is left to the
     * ordinary resolution below, which reads the catalog or the temp schema and still refuses a
     * name neither of them has.
     */
    /**
     * Reports a relation the statement names that does not exist, before any clause of the
     * statement is judged.
     *
     * <p>PostgreSQL builds the range table first and transforms the rest of the query against it,
     * so a query that both names a missing relation and misuses a clause is reported as the
     * missing relation. memgres has no such phase — resolving a FROM item here means running it,
     * and running it is observable: a WITH item that writes would be applied, and a fault inside a
     * derived table would surface, for a statement PostgreSQL refuses outright. So this asks only
     * whether each name resolves to <em>something</em>, along the same ladder
     * {@link #resolveTableRef} walks, and reads nothing.
     *
     * <p>It can only report a name the ordinary lookup would also have reported: every branch that
     * finds anything at all returns without complaint, and the refusal itself is raised by that
     * same lookup so the message, hint and SQLSTATE are the ones it gives.
     */
    void checkRelationNamesExist(Object statement) {
        final Set<String> withNames = new HashSet<String>();
        AstWalk.forEach(statement, node -> {
            if (node instanceof SelectStmt.CommonTableExpr) {
                String n = ((SelectStmt.CommonTableExpr) node).name();
                if (n != null) withNames.add(n.toLowerCase(Locale.ROOT));
            }
        });
        AstWalk.forEach(statement, node -> {
            // The relation a data-modifying statement writes goes into the range table before the
            // ones it reads, so it is the one reported when neither of them exists. It is held as
            // a schema and a name rather than as a reference, so it is made into one here.
            SelectStmt.TableRef written = writtenRelationOf(node);
            if (written != null) checkOneRelationName(written, withNames);
            if (node instanceof SelectStmt.TableRef) {
                checkOneRelationName((SelectStmt.TableRef) node, withNames);
            }
        });
    }

    /** The relation a data-modifying statement writes, as a reference this can resolve. */
    static SelectStmt.TableRef writtenRelationOf(Object node) {
        if (node instanceof InsertStmt) {
            InsertStmt s = (InsertStmt) node;
            return new SelectStmt.TableRef(s.schema(), s.table(), null, false);
        }
        if (node instanceof UpdateStmt) {
            UpdateStmt s = (UpdateStmt) node;
            return new SelectStmt.TableRef(s.schema(), s.table(), null, false);
        }
        if (node instanceof DeleteStmt) {
            DeleteStmt s = (DeleteStmt) node;
            return new SelectStmt.TableRef(s.schema(), s.table(), null, false);
        }
        if (node instanceof MergeStmt) {
            MergeStmt s = (MergeStmt) node;
            return new SelectStmt.TableRef(s.schema(), s.targetTable(), null, false);
        }
        return null;
    }

    private void checkOneRelationName(SelectStmt.TableRef tableRef, Set<String> withNames) {
        if (tableRef.table() == null) return;
        // A WITH item is not a stored relation. Its name is taken from anywhere in the statement
        // rather than from the scope stack, because a name defined at another query level is
        // still not a table, and refusing a query PostgreSQL runs is the worse mistake.
        if (tableRef.schema() == null
                && withNames.contains(tableRef.table().toLowerCase(Locale.ROOT))) {
            return;
        }
        if (lookupCteFor(tableRef) != null) return;
        if (viewFor(tableRef) != null) return;

        String schemaName = tableRef.schema() != null ? tableRef.schema() : executor.defaultSchema();
        boolean userQualified = tableRef.schema() != null;
        refuseForeignTable(schemaName, tableRef.table());
        try {
            executor.resolveTable(schemaName, tableRef.table(), userQualified);
            return;
        } catch (MemgresException e) {
            // Anything other than "no such relation" means the name found something and the
            // ordinary lookup has a better answer than this pass does.
            if (!"42P01".equals(e.getSqlState())) return;
        }
        if (SystemCatalog.isSystemCatalog(tableRef.schema(), tableRef.table())) {
            recordCatalogReadLock(tableRef.schema(), tableRef.table());
            if (executor.systemCatalog.resolve(
                    tableRef.schema(), tableRef.table(), executor.session) != null) {
                return;
            }
        }
        // Nothing answers to the name. Raise it the way the ordinary lookup does, so the message
        // and the "there is a WITH item" hint are identical.
        executor.resolveTable(schemaName, tableRef.table(), userQualified);
    }

    /**
     * A FROM item that names a foreign table is refused by its wrapper, not by the name lookup.
     * The relation is there — it is in pg_class and information_schema, and a query that called
     * it missing would send a reader looking for a name that is plainly listed.
     */
    private void refuseForeignTable(String schemaName, String name) {
        Database.FdwForeignTable ft = ForeignTables.lookup(executor.database, schemaName, name);
        if (ft != null) throw ForeignTables.noHandler(executor.database, ft);
    }

    private boolean implicitlySearchedSchemaHolds(String name) {
        Schema pgTemp = executor.database.getSchema(executor.session.getTempSchemaName());
        if (pgTemp != null && pgTemp.getTable(name) != null) return true;
        return SystemCatalog.isSystemCatalog(null, name)
                && executor.systemCatalog.resolve(null, name, executor.session) != null;
    }

    private List<RowContext> resolveFromClauseInner(List<SelectStmt.FromItem> fromItems, Expression where) {
        rejectSiblingReferenceWithoutLateral(fromItems);
        if (fromItems.size() == 1) {
            // A single relation with a WHERE is the one shape an index can narrow. The clause is
            // handed to the same resolver the sequential scan uses, which decides for itself
            // whether an index may answer it; every check the scan makes is made either way.
            if (where != null && fromItems.get(0) instanceof SelectStmt.TableRef) {
                return resolveTableRef((SelectStmt.TableRef) fromItems.get(0), where);
            }
            return resolveFromItem(fromItems.get(0));
        }

        // A LATERAL subquery or a function call in FROM is run once per row of what stands to its
        // left. Parentheses do not stop that: an item written inside a join reads what is left of
        // the whole join, so the clause is resolved item by item whenever one of them holds such
        // an item anywhere rather than only when one is written at the top of the list.
        boolean hasLateral = false;
        for (SelectStmt.FromItem item : fromItems) {
            if (FromJoinExecutor.holdsLateralItem(item)) {
                hasLateral = true;
                break;
            }
        }

        if (hasLateral) {
            return resolveFromClauseWithLateral(fromItems, where);
        }

        // Multiple FROM items = implicit cross join (no LATERAL)
        if (where != null) {
            return crossProductWithEarlyFilter(fromItems, where);
        }
        List<List<RowContext>> resolvedItems = new ArrayList<>();
        for (SelectStmt.FromItem fromItem : fromItems) {
            resolvedItems.add(resolveFromItem(fromItem));
        }
        return crossProductContexts(resolvedItems);
    }

    /**
     * Process FROM items sequentially when LATERAL subqueries are present.
     */
    private List<RowContext> resolveFromClauseWithLateral(List<SelectStmt.FromItem> fromItems, Expression where) {
        List<RowContext> accumulated = null;
        List<Expression> wherePredicates = where != null ? flattenAndPredicates(where) : Cols.listOf();
        Set<Integer> appliedPredicates = new HashSet<>();

        for (int itemIdx = 0; itemIdx < fromItems.size(); itemIdx++) {
            SelectStmt.FromItem fromItem = fromItems.get(itemIdx);
            boolean isLateralSubquery = fromItem instanceof SelectStmt.SubqueryFrom && ((SelectStmt.SubqueryFrom) fromItem).lateral();
            boolean isFunctionFrom = fromItem instanceof SelectStmt.FunctionFrom;

            if (isLateralSubquery) {
                SelectStmt.SubqueryFrom sqf = (SelectStmt.SubqueryFrom) fromItem;
                if (accumulated == null || accumulated.isEmpty()) {
                    accumulated = resolveFromItem(fromItem);
                    continue;
                }

                List<RowContext> newAccumulated = new ArrayList<>();
                // The definition is the same text for every left row, so what it settles about
                // the sub-query's columns is worked out once rather than once per row.
                String[] settled = null;
                boolean readWhole = false;
                for (RowContext leftCtx : accumulated) {
                    // An item PostgreSQL keeps apart is run once per row of the relation beside it,
                    // and only for the rows that relation's own scan kept, so it is not run at all
                    // for a row the query has already discarded.
                    if (lateralItemUnasked(sqf, leftCtx)) continue;
                    executor.outerContextStack.push(leftCtx);
                    try {
                        QueryResult subResult = readLateralSubquery(sqf);
                        String alias = lateralAlias(sqf);
                        List<Column> columns = FromFunctionResolver.applyColumnAliases(
                                new ArrayList<>(subResult.getColumns()), sqf.columnAliases());
                        Table virtualTable = new Table(alias, columns);
                        renamesColumnsOf(virtualTable, alias, subResult.getColumns(),
                                sqf.columnAliases());
                        if (settled == null || settled.length != columns.size()) {
                            settled = executor.definedTypes.ofQuery(sqf.subquery(), columns.size());
                        }
                        virtualTable.setDefinedColumnTypes(settled);

                        // Pulled up, the item is not a query of its own: the relation underneath
                        // is one of this query's and is scanned once, whatever the item's own
                        // comparison with the row beside it keeps out of the pairing.
                        if (!readWhole) {
                            readWhole = true;
                            readLateralItemWhole(sqf, alias, null);
                        }
                        if (subResult.getRows().isEmpty()) {
                            // Implicit INNER JOIN semantics, skip
                        } else {
                            // A VIRTUAL generated column the sub-select left for this relation to
                            // work out is worked out here, because the query reading it may
                            // qualify on it -- and a qualification decides which rows there are
                            // before anything is read of them.
                            boolean lateralVirtual =
                                    executor.dmlExecutor.hasVirtualColumns(virtualTable);
                            for (Object[] row : subResult.getRows()) {
                                RowContext rightCtx = new RowContext(virtualTable, alias,
                                        lateralVirtual ? executor.dmlExecutor.computeVirtualColumns(
                                                virtualTable, alias, row) : row);
                                newAccumulated.add(joinExecutor.mergeContexts(leftCtx, rightCtx));
                            }
                        }
                    } finally {
                        executor.outerContextStack.pop();
                    }
                }
                accumulated = newAccumulated;
            } else if (isFunctionFrom && accumulated != null) {
                // Implicit LATERAL for functions-in-FROM. Note this branch must also own
                // the zero-left-rows case: with no rows to iterate the function is never
                // evaluated (PG semantics). Falling through to the generic branch would
                // evaluate its arguments without any row context, so a column-ref arg
                // like pg_get_sequence_data(seqrelid) degrades to a bare string and
                // crashes — pg_dump hits exactly that on a database with no sequences.
                SelectStmt.FunctionFrom funcFrom = (SelectStmt.FunctionFrom) fromItem;
                List<RowContext> newAccumulated = new ArrayList<>();
                for (RowContext leftCtx : accumulated) {
                    executor.outerContextStack.push(leftCtx);
                    try {
                        // A comma between two FROM items is an inner join, so a function that
                        // produces no rows for this left row removes it — the same as the
                        // lateral-subquery branch above, which skips it. Padding it with NULLs
                        // instead answered LEFT JOIN LATERAL to a query that did not write one.
                        for (RowContext rightCtx : resolveFunctionItem(funcFrom)) {
                            newAccumulated.add(joinExecutor.mergeContexts(leftCtx, rightCtx));
                        }
                    } finally {
                        executor.outerContextStack.pop();
                    }
                }
                accumulated = newAccumulated;
            } else if (accumulated != null && !accumulated.isEmpty()
                    && FromJoinExecutor.readsNamesOf(fromItem, accumulated.get(0).getBindings())) {
                // A join written in parentheses whose own lateral item reads a relation standing
                // to the left of those parentheses. The item is inside the join, so the join is
                // what has to be resolved once per left row for it to have that row to read.
                List<RowContext> newAccumulated = new ArrayList<>();
                for (RowContext leftCtx : accumulated) {
                    executor.outerContextStack.push(leftCtx);
                    try {
                        for (RowContext rightCtx : resolveFromItem(fromItem)) {
                            newAccumulated.add(joinExecutor.mergeContexts(leftCtx, rightCtx));
                        }
                    } finally {
                        executor.outerContextStack.pop();
                    }
                }
                accumulated = newAccumulated;
            } else {
                List<RowContext> resolved = resolveFromItem(fromItem);
                if (accumulated == null) {
                    accumulated = resolved;
                } else {
                    List<Expression> applicablePredicates = new ArrayList<>();
                    for (int pi = 0; pi < wherePredicates.size(); pi++) {
                        if (!appliedPredicates.contains(pi)) {
                            Expression pred = wherePredicates.get(pi);
                            if (!accumulated.isEmpty() && !resolved.isEmpty()) {
                                RowContext sample = joinExecutor.mergeContexts(accumulated.get(0), resolved.get(0));
                                if (canEvaluatePredicate(pred, sample)) {
                                    applicablePredicates.add(pred);
                                    appliedPredicates.add(pi);
                                }
                            }
                        }
                    }

                    accumulated = pairer.pair(accumulated, resolved, applicablePredicates,
                            (l, r) -> joinExecutor.mergeContexts(l, r));
                }
            }
        }

        return accumulated != null ? accumulated : Cols.listOf();
    }

    // ---- Resolve single FROM item ----

    List<RowContext> resolveFromItem(SelectStmt.FromItem fromItem) {
        if (fromItem instanceof SelectStmt.TableRef) return resolveTableRef(((SelectStmt.TableRef) fromItem));
        if (fromItem instanceof SelectStmt.SubqueryFrom) return resolveSubquery(((SelectStmt.SubqueryFrom) fromItem));
        if (fromItem instanceof SelectStmt.FunctionFrom) return resolveFunctionItem(((SelectStmt.FunctionFrom) fromItem));
        if (fromItem instanceof SelectStmt.JoinFrom) return joinExecutor.executeJoin(((SelectStmt.JoinFrom) fromItem));
        throw new IllegalArgumentException("Unknown FromItem type: " + fromItem.getClass().getSimpleName());
    }

    /**
     * A set-returning call in FROM, with a relation of a composite type given that composite's own
     * columns.
     *
     * <p>PostgreSQL types {@code unnest(cs)} over an array of a composite as returning the
     * composite itself, and a FROM item whose rows are of a composite type supplies one column per
     * field -- which is what lets {@code u.a} name a field of the record. The call builds a
     * relation of one column holding the record as it is written, so the values are already right
     * and only the shape of the relation in front of them is wrong. An alias list or WITH
     * ORDINALITY names the columns itself, so neither is re-shaped here.
     */
    private List<RowContext> resolveFunctionItem(SelectStmt.FunctionFrom funcFrom) {
        List<RowContext> contexts = functionResolver.resolveFunctionFrom(funcFrom);
        if (contexts.isEmpty() || funcFrom.withOrdinality()) return contexts;
        if (!"unnest".equalsIgnoreCase(funcFrom.functionName())) return contexts;
        if (funcFrom.columnAliases() != null && !funcFrom.columnAliases().isEmpty()) return contexts;
        if (funcFrom.args() == null || funcFrom.args().size() != 1) return contexts;
        String typeName = executor.compositeTypeHandler.arrayElementCompositeType(
                funcFrom.args().get(0), executor.outerContextStack.peek());
        if (typeName == null) return contexts;
        List<CreateTypeStmt.CompositeField> fields =
                executor.compositeTypeHandler.resolveFieldsForType(typeName);
        if (fields == null || fields.isEmpty()) return contexts;
        String alias = funcFrom.alias() != null ? funcFrom.alias() : funcFrom.functionName();
        List<Column> cols = new ArrayList<>();
        String[] declared = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            DataType fieldType = DataType.fromPgName(fields.get(i).typeName());
            cols.add(new Column(fields.get(i).name(),
                    fieldType != null ? fieldType : DataType.TEXT, true, false, null));
            declared[i] = fieldType != null ? fieldType.getPgName() : fields.get(i).typeName();
        }
        Table expanded = new Table(alias, cols);
        expanded.setFunctionResult(true);
        expanded.setDefinedColumnTypes(declared);
        List<RowContext> byField = new ArrayList<>(contexts.size());
        for (RowContext ctx : contexts) {
            List<RowContext.TableBinding> bound = ctx.getBindings();
            if (bound.size() != 1 || bound.get(0).row().length != 1) return contexts;
            Object element = bound.get(0).row()[0];
            Object[] row = new Object[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                row[i] = executor.extractCompositeField(element, fields.get(i).name(), typeName);
            }
            expanded.insertRow(row);
            byField.add(new RowContext(expanded, alias, row));
        }
        return byField;
    }

    /**
     * Note the AccessShareLock a read of a catalog relation takes, under the name pg_locks reports
     * the relation by. A catalog reference written without a schema is in pg_catalog.
     */
    private void recordCatalogReadLock(String writtenSchema, String relation) {
        if (executor.session == null) return;
        String schema = writtenSchema != null ? writtenSchema.toLowerCase(java.util.Locale.ROOT) : "pg_catalog";
        executor.session.recordRelationLock(
                schema + "." + relation.toLowerCase(java.util.Locale.ROOT), "AccessShareLock");
    }

    /**
     * A catalog relation's rows, without those describing a relation this transaction's snapshot
     * did not hold.
     *
     * <p>PostgreSQL snapshots the database rather than the relations a transaction happens to read,
     * and the catalogs are snapshotted with it: a relation another session created and committed
     * after a REPEATABLE READ transaction took its snapshot is not in that transaction's pg_class
     * either. The catalog here is derived from the live database at every reference, so what the
     * snapshot did not hold has to be taken back out of it. A relation this transaction made
     * itself is its own to see, as it is everywhere else.
     */
    private List<Object[]> snapshotVisibleCatalogRows(Table catalogTable, String catalogName) {
        Session session = executor.session;
        if (session == null || !session.isRRSnapshotTaken()) return catalogTable.getRows();
        // Both relations carry the OID of the relation they describe; the column it is written in
        // is all that differs.
        String relationColumn = "pg_class".equals(catalogName) ? "oid"
                : "pg_attribute".equals(catalogName) ? "attrelid" : null;
        if (relationColumn == null) return catalogTable.getRows();
        int oidIndex = catalogTable.getColumnIndex(relationColumn);
        if (oidIndex < 0) return catalogTable.getRows();
        Set<Integer> unseen = new HashSet<>();
        for (Map.Entry<String, Schema> schemaEntry : executor.database.getSchemas().entrySet()) {
            for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                Table candidate = tableEntry.getValue();
                if (session.hasRRSnapshot(schemaEntry.getKey() + "." + tableEntry.getKey())
                        || executor.database.wasCreatedBy(candidate, session)) continue;
                unseen.add(executor.systemCatalog.getOid(
                        "rel:" + schemaEntry.getKey() + "." + candidate.getName()));
            }
        }
        if (unseen.isEmpty()) return catalogTable.getRows();
        List<Object[]> shown = new ArrayList<>();
        for (Object[] row : catalogTable.getRows()) {
            if (row[oidIndex] instanceof Number
                    && unseen.contains(((Number) row[oidIndex]).intValue())) continue;
            shown.add(row);
        }
        return shown;
    }

    /**
     * The WITH item a FROM reference names, or null.
     *
     * <p>A WITH item lives in no schema, so a schema-qualified name can never reach one: it is
     * always the stored relation of that name, and "does not exist" when there is none.
     */
    private SelectStmt.CommonTableExpr lookupCteFor(SelectStmt.TableRef tableRef) {
        if (tableRef.schema() != null) return null;
        return executor.selectExecutor.lookupCte(tableRef.table());
    }

    /**
     * The columns a FROM item's alias list renames, as far as the list reaches.
     *
     * <p>{@code t AS z(m)} calls the relation's first column m and leaves the rest alone; naming
     * more columns than the relation has is an error PostgreSQL raises before reading a row.
     */
    private static List<Column> renameColumns(String alias, List<Column> columns,
                                              List<String> aliases) {
        if (aliases == null || aliases.isEmpty()) return columns;
        if (aliases.size() > columns.size()) {
            throw new MemgresException("table \"" + alias + "\" has " + columns.size()
                    + " columns available but " + aliases.size() + " columns specified", "42P10");
        }
        List<Column> renamed = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            Column c = columns.get(i);
            renamed.add(i < aliases.size()
                    ? new Column(aliases.get(i), c.getType(), c.isNullable(), c.isPrimaryKey(),
                            c.getDefaultValue(), c.getEnumTypeName(), c.getPrecision(), c.getScale(),
                            c.getGeneratedExpr(), c.isVirtual(), c.getDomainTypeName(),
                            c.getCompositeTypeName(), c.getArrayElementType())
                    : c);
        }
        return renamed;
    }

    /**
     * Record the relation whose columns a FROM item's alias list renamed.
     *
     * <p>PostgreSQL renames the references to a relation's columns and not the expressions stored
     * with them, so a VIRTUAL generated column reached through such a list still carries a
     * generation expression written in the names the relation underneath answers to. The rows and
     * the column positions are that relation's own, which is what lets the expression be worked out
     * against it while the query above writes the new names. Without it the column read as one
     * nothing had ever written.
     */
    static void renamesColumnsOf(Table exposed, String alias, List<Column> own,
                                 List<String> aliases) {
        if (aliases == null || aliases.isEmpty()) return;
        exposed.setColumnsRenamedFrom(new Table(alias, own));
    }

    /**
     * What PostgreSQL calls a FROM item whose alias list names more columns than it has.
     *
     * <p>A parenthesised join stays a join there rather than becoming a relation, so it is a join
     * expression and the refusal says so. Everything else that can wear an alias list -- a stored
     * relation, a view, a query, a WITH item, a call -- PostgreSQL calls a table.
     */
    static String aliasedItemNoun(SelectStmt.FromItem item) {
        if (!(item instanceof SelectStmt.SubqueryFrom)) return "table";
        Statement under = ((SelectStmt.SubqueryFrom) item).subquery();
        return under instanceof SelectStmt && ((SelectStmt) under).joinExpression()
                ? "join expression" : "table";
    }

    private List<RowContext> resolveTableRef(SelectStmt.TableRef tableRef) {
        return resolveTableRef(tableRef, null);
    }

    /**
     * Resolve a relation named in a FROM clause.
     *
     * <p>{@code indexWhere} is the query's WHERE, offered so that an equality on an indexed column
     * can be answered from the index instead of by reading every row. It is an optimisation and
     * nothing more: the caller applies the WHERE again to whatever comes back, so a probe is
     * allowed to return rows the clause will later discard but never to leave one out. Everything
     * that decides which rows a reader may see at all — the SELECT privilege, the ACCESS SHARE
     * lock, the repeatable-read snapshot, uncommitted work in other sessions and row-level
     * security — is settled below on the one path, so an index scan and a sequential scan cannot
     * disagree about it.
     */
    private List<RowContext> resolveTableRef(SelectStmt.TableRef tableRef, Expression indexWhere) {
        // Check CTEs first
        SelectStmt.CommonTableExpr cte = lookupCteFor(tableRef);
        if (cte != null) {
            String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
            QueryResult cteResult = readCte(cte, alias, tableRef.columnAliases());
            Table virtualTable = new Table(alias,
                    renameColumns(alias, cteResult.getColumns(), tableRef.columnAliases()));
            renamesColumnsOf(virtualTable, alias, cteResult.getColumns(),
                    tableRef.columnAliases());
            defineFromCte(virtualTable, cte);
            lastResolvedRightTable = virtualTable;
            lastResolvedRightAlias = alias;
            for (Object[] row : cteResult.getRows()) {
                virtualTable.insertRow(row);
            }
            return derivedContexts(virtualTable, alias);
        }

        // Check views
        Database.ViewDef view = viewFor(tableRef);
        if (view != null) {
            if (view.materialized() && !view.populated()) {
                MemgresException ex = new MemgresException(
                        "materialized view \"" + view.name() + "\" has not been populated", "55000");
                ex.setHint("Use the REFRESH MATERIALIZED VIEW command.");
                throw ex;
            }
            String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
            List<Column> cols;
            List<Object[]> rows;
            if (view.materialized() && view.cachedColumns() != null) {
                cols = view.cachedColumns();
                rows = view.cachedRows();
            } else {
                // The reader needs SELECT on the view; the body then runs with the view
                // owner's rights, as PG does.
                String viewSchema = tableRef.schema() != null ? tableRef.schema() : executor.defaultSchema();
                executor.checkTablePrivilege("SELECT", viewSchema, tableRef.table());
                String priorViewOwner = executor.viewOwnerRole;
                String viewOwner = executor.database.getObjectOwner(
                        "view:" + viewSchema.toLowerCase(java.util.Locale.ROOT) + "." + tableRef.table().toLowerCase(java.util.Locale.ROOT));
                if (viewOwner != null) executor.viewOwnerRole = viewOwner;
                QueryResult viewResult;
                try {
                    viewResult = readAsDerivedRelation(alias, tableRef.columnAliases(),
                            () -> executor.executeViewQuery(tableRef.table(), view.query()));
                } finally {
                    executor.viewOwnerRole = priorViewOwner;
                }
                cols = viewResult.getColumns();
                rows = viewResult.getRows();
            }
            // An alias list renames every column the relation exposes, whatever kind of relation
            // it is: the query above writes the new names and the body under it still answers to
            // its own. Without this a view was the one relation that kept answering to the names
            // the list had renamed away.
            Table virtualTable = new Table(alias,
                    renameColumns(alias, cols, tableRef.columnAliases()));
            renamesColumnsOf(virtualTable, alias, cols, tableRef.columnAliases());
            // A materialized view keeps its rows where an ordinary view composes them each time it
            // is read, so its rows sit somewhere and have the system columns that says. See
            // Table.storesRows.
            virtualTable.setStoresRows(view.materialized() && view.cachedColumns() != null);
            defineFromView(virtualTable, view);
            lastResolvedRightTable = virtualTable;
            lastResolvedRightAlias = alias;
            for (Object[] row : rows) {
                virtualTable.insertRow(row);
            }
            return derivedContexts(virtualTable, alias);
        }

        // Check system catalogs
        String schemaName = tableRef.schema() != null ? tableRef.schema() : executor.defaultSchema();
        // H35: pass userQualified=true when user explicitly wrote schema.table
        boolean userQualified = tableRef.schema() != null;
        refuseForeignTable(schemaName, tableRef.table());
        // An empty search_path leaves an unqualified name nowhere to be found -- except in the
        // two schemas PostgreSQL searches whether or not the path names them. pg_temp is one and
        // was already allowed for here; pg_catalog is the other, and leaving it out is why
        // pg_dump could not read this server at all. pg_dump clears the search_path on purpose,
        // so that a schema planted by someone else cannot answer to a name it means to read as
        // the catalog's, and then reads pg_settings by its bare name -- which PostgreSQL answers
        // and this refused.
        if (!userQualified && executor.session != null) {
            String sp = executor.session.getGucSettings().get("search_path");
            if (sp != null) {
                boolean hasEntries = false;
                for (String part : sp.split(",")) {
                    String s = part.trim().replace("\"", "").replace("'", "");
                    if (!s.isEmpty() && !s.equals("$user")) { hasEntries = true; break; }
                }
                if (!hasEntries && !implicitlySearchedSchemaHolds(tableRef.table())) {
                    throw new MemgresException("relation \"" + tableRef.table() + "\" does not exist", "42P01");
                }
            }
        }
        boolean userTableExists = false;
        try { executor.resolveTable(schemaName, tableRef.table(), userQualified); userTableExists = true; } catch (MemgresException ignored) {}
        if (!userTableExists && SystemCatalog.isSystemCatalog(tableRef.schema(), tableRef.table())) {
            // A catalog relation named in a FROM clause is read like any other, and the read
            // takes an AccessShareLock the transaction holds. Noted before the relation is
            // built, because the relation being built may be pg_locks itself: a query asking
            // what it holds saw every lock but the one it was taking to ask.
            recordCatalogReadLock(tableRef.schema(), tableRef.table());
            Table catalogTable = executor.systemCatalog.resolve(tableRef.schema(), tableRef.table(), executor.session);
            if (catalogTable != null) {
                String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
                lastResolvedRightTable = catalogTable;
                lastResolvedRightAlias = alias;
                List<RowContext> contexts = new ArrayList<>();
                for (Object[] row : snapshotVisibleCatalogRows(catalogTable,
                        tableRef.table().toLowerCase(java.util.Locale.ROOT))) {
                    contexts.add(new RowContext(catalogTable, alias, row));
                }
                return contexts;
            }
        }
        Table table = executor.resolveTable(schemaName, tableRef.table(), userQualified);
        // C6: Enforce SELECT privilege on user tables
        executor.checkTablePrivilege("SELECT", schemaName, tableRef.table());
        // A reader inside an explicit transaction holds ACCESS SHARE until it ends, which is
        // what makes a concurrent TRUNCATE or ALTER wait instead of yanking the table away.
        // An autocommit reader does not take it: PostgreSQL would, but a lock wait with no
        // deadline turns any session that is left idle in a transaction into one that hangs
        // every later reader, and what that lock protects here is answered by hiding the
        // uncommitted shape instead.
        if (executor.session != null && executor.session.isInTransaction()) {
            executor.database.acquireTableLock(schemaName + "." + tableRef.table(),
                    "AccessShareLock", executor.session, false);
        }
        String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
        // An alias list renames the columns the query will see. The rows are the stored table's
        // own, so only the description in front of them is rebuilt.
        if (tableRef.columnAliases() != null && !tableRef.columnAliases().isEmpty()) {
            Table renamed = new Table(alias,
                    renameColumns(alias, table.getColumns(), tableRef.columnAliases()));
            // Renaming a column does not retype it, and the relation this stands in front of
            // declared every one of them, so the types stay as certain as they were.
            defineAsDeclared(renamed, table);
            renamed.setColumnsRenamedFrom(table);
            for (Object[] row : table.getAllRows()) renamed.insertRow(row);
            table = renamed;
        }
        lastResolvedRightTable = table;
        lastResolvedRightAlias = alias;

        // MVCC: Check for REPEATABLE READ snapshot
        String schemaTableKey = schemaName + "." + tableRef.table();
        Session currentSession = executor.session;
        if (currentSession != null && currentSession.hasRRSnapshot(schemaTableKey)) {
            // ONLY reads what the relation stores itself, so it reads the part of the snapshot
            // that came from there rather than the rows its partitions and children hold for it.
            List<Object[]> snapshot = tableRef.only()
                    ? currentSession.getRRSnapshotOwnRows(schemaTableKey)
                    : currentSession.getRRSnapshot(schemaTableKey);
            boolean snapshotHasVirtual = executor.dmlExecutor.hasVirtualColumns(table);
            List<RowContext> contexts = new ArrayList<>();
            for (Object[] row : snapshot) {
                Object[] r = snapshotHasVirtual
                        ? executor.dmlExecutor.computeVirtualColumns(table, alias, row) : row;
                contexts.add(snapshotContext(table, alias, schemaTableKey, row, r, currentSession));
            }
            return contexts;
        }

        // Use getAllRowsWithSource for inheritance/partitioning
        boolean hasVirtual = executor.dmlExecutor.hasVirtualColumns(table);
        List<RowContext> contexts = new ArrayList<>();
        List<Object[]> probed = indexWhere == null ? null
                : indexProbe(tableRef, table, indexWhere, schemaTableKey, currentSession);
        if (probed != null) {
            for (Object[] row : probed) {
                Object[] r = hasVirtual
                        ? executor.dmlExecutor.computeVirtualColumns(table, alias, row) : row;
                contexts.add(new RowContext(table, alias, r));
            }
        } else if (tableRef.only()) {
            for (Object[] row : table.getRows()) {
                Object[] r = hasVirtual
                        ? executor.dmlExecutor.computeVirtualColumns(table, alias, row) : row;
                contexts.add(new RowContext(table, alias, r));
            }
        } else {
            for (Table.RowWithSource rws : table.getAllRowsWithSource()) {
                Object[] r = hasVirtual
                        ? executor.dmlExecutor.computeVirtualColumns(table, alias, rws.row())
                        : rws.row();
                contexts.add(new RowContext(Cols.listOf(
                        new RowContext.TableBinding(table, alias, r, rws.source(), rws.stored()))));
            }
        }

        // MVCC: Filter out uncommitted changes from other sessions
        if (currentSession != null) {
            contexts = applyMvccVisibility(contexts, table, alias, schemaTableKey, currentSession,
                    tableRef.only());
        }

        // Apply Row-Level Security filtering (default-deny: even with no policies, non-owner sees nothing)
        if (table.isRlsEnabled()) {
            contexts = applyRlsFiltering(contexts, table, schemaName);
        }

        return contexts;
    }

    /**
     * The rows an index can answer an equality with, or null to read the relation in full.
     *
     * <p>An index holds every stored row of the relation under the value of its columns, so a
     * {@code WHERE col = value} that names all of an index's columns can be answered by one lookup
     * instead of a pass over the table. A 10,000-row relation read 5,000 times by primary key is
     * 50 million row visits done as 5,000 hash lookups.
     *
     * <p>Returning null is always safe — it means read everything — and every doubt is settled
     * that way. The probe is refused unless the value written in the query is the same value the
     * relation stores, because the index compares stored values and the query does not: a function
     * or a cast over the column, a value of another type, a domain or an enum with its own storage
     * rules, all send the query back to a sequential scan rather than risk a row that answers the
     * clause not being found under the key that was looked up. {@code = NULL} matches nothing in
     * SQL, so it is not a key to look anything up under either.
     *
     * <p>The rows come from the index and are therefore the relation as it stands now. That is the
     * right answer only when now is what the reader is entitled to see, so a transaction reading
     * from a repeatable-read snapshot, and a reader with another session's uncommitted work on the
     * relation to account for, both read it in full: the snapshot is the whole relation and cannot
     * be rebuilt from a lookup, and an uncommitted update to an indexed column has already moved
     * the row to its new key while the reader is still owed its old one.
     */
    private List<Object[]> indexProbe(SelectStmt.TableRef tableRef, Table table, Expression where,
                                      String schemaTableKey, Session session) {
        if (table.getIndexes().isEmpty()) return null;
        // ONLY, inheritance and partitioning spread the relation's rows over tables of their own,
        // each with an index of its own; one index answers for one of them.
        if (tableRef.only()) return null;
        if (!table.getPartitions().isEmpty() || !table.getChildren().isEmpty()
                || table.getParentTable() != null) return null;
        if (session != null && !indexReflectsWhatSessionMaySee(schemaTableKey, session)) return null;

        Map<String, Object> equalities = collectEqualityKeys(tableRef, table, where);
        if (equalities == null || equalities.isEmpty()) return null;

        // A unique index answers with at most one row, so it is preferred where both would do.
        List<Object[]> found = probeIndexes(table, equalities, true);
        if (found == null) found = probeIndexes(table, equalities, false);
        return found;
    }

    /** Look up {@code equalities} in the table's unique or non-unique indexes, or null for none. */
    private List<Object[]> probeIndexes(Table table, Map<String, Object> equalities, boolean unique) {
        List<Column> columns = table.getColumns();
        for (TableIndex idx : table.getIndexes().values()) {
            if (idx.isUnique() != unique) continue;
            int[] colIndices = idx.getColumnIndices();
            if (colIndices.length == 0) continue;
            Object[] keyValues = new Object[colIndices.length];
            boolean complete = true;
            for (int i = 0; i < colIndices.length; i++) {
                if (colIndices[i] < 0 || colIndices[i] >= columns.size()) { complete = false; break; }
                String name = columns.get(colIndices[i]).getName().toLowerCase(Locale.ROOT);
                if (!equalities.containsKey(name)) { complete = false; break; }
                keyValues[i] = equalities.get(name);
            }
            if (!complete) continue;
            return idx.findAll(keyValues);
        }
        return null;
    }

    /**
     * Whether what the index holds now is what this session is entitled to read.
     *
     * <p>A repeatable-read transaction reads the relation as it stood when it first looked, which
     * is a list of rows the session keeps and the index knows nothing of. Building that list from
     * a probe would fix the snapshot at the few rows one lookup returned, and every later
     * statement in the transaction would read the relation as if it held only those — so a
     * transaction at that isolation level reads in full whether it has taken its snapshot yet or
     * not.
     *
     * <p>Another session's uncommitted work is undone for this reader row by row, which needs the
     * rows: an update it has not committed has already moved its row to the new key, and this
     * reader is owed the row under the old one.
     */
    private boolean indexReflectsWhatSessionMaySee(String schemaTableKey, Session session) {
        if (session.hasRRSnapshot(schemaTableKey)) return false;
        if (session.isInTransaction()) {
            String isolation = session.getEffectiveIsolationLevel();
            if ("repeatable read".equals(isolation) || "serializable".equals(isolation)) return false;
        }
        for (Session other : executor.database.getActiveSessions()) {
            if (other == session) continue;
            if (!other.isInTransaction()) continue;
            if (!other.getUncommittedInserts(schemaTableKey).isEmpty()) return false;
            if (!other.getUncommittedUpdates(schemaTableKey).isEmpty()) return false;
            if (!other.getUncommittedDeletes(schemaTableKey).isEmpty()) return false;
        }
        return true;
    }

    /**
     * The column-name-to-stored-value pairs an index may be looked up under, from the ANDed
     * equalities of a WHERE. Null when the clause holds none usable.
     *
     * <p>Only a bare column against a written value counts. A cast or a function over the column
     * is a different value from the one the index holds, and a qualifier naming something other
     * than this relation is not this relation's column at all.
     */
    private Map<String, Object> collectEqualityKeys(SelectStmt.TableRef tableRef, Table table,
                                                    Expression where) {
        String alias = tableRef.alias() != null ? tableRef.alias() : tableRef.table();
        Map<String, Object> equalities = null;
        for (Expression pred : flattenAndPredicates(where)) {
            if (!(pred instanceof BinaryExpr)) continue;
            BinaryExpr bin = (BinaryExpr) pred;
            if (bin.op() != BinaryExpr.BinOp.EQUAL) continue;
            ColumnRef ref = null;
            Expression valueExpr = null;
            if (bin.left() instanceof ColumnRef && isWrittenValue(bin.right())) {
                ref = (ColumnRef) bin.left();
                valueExpr = bin.right();
            } else if (bin.right() instanceof ColumnRef && isWrittenValue(bin.left())) {
                ref = (ColumnRef) bin.right();
                valueExpr = bin.left();
            }
            if (ref == null || ref.column() == null) continue;
            if (ref.table() != null && !ref.table().equalsIgnoreCase(alias)
                    && !ref.table().equalsIgnoreCase(tableRef.table())) continue;
            int colIdx = table.getColumnIndex(ref.column());
            if (colIdx < 0) continue;
            Column column = table.getColumns().get(colIdx);
            Object key = storedFormOf(valueExpr, column);
            if (key == null) continue;
            String name = column.getName().toLowerCase(Locale.ROOT);
            if (equalities == null) equalities = new LinkedHashMap<String, Object>();
            Object prior = equalities.put(name, key);
            // Two equalities on one column with different values match nothing; there is no one
            // key to look that up under, so the clause is left to the scan.
            if (prior != null
                    && !Objects.equals(TableIndex.normalize(prior), TableIndex.normalize(key))) {
                return null;
            }
        }
        return equalities;
    }

    /** A value written in the query itself, which is the same for every row and reads no column. */
    private boolean isWrittenValue(Expression expr) {
        return expr instanceof Literal || expr instanceof ParamRef;
    }

    /**
     * The value as this column stores it, or null when the query's value cannot be turned into a
     * key the index would have filed the row under.
     *
     * <p>Stored values were put through {@link TypeCoercion#coerceForStorage} on their way in, so
     * a lookup value has to make the same trip — CHAR(n) pads, NUMERIC(p,s) rounds — or a row
     * that answers the clause sits under a key the probe never asks for. The trip is only taken
     * for types where it lands somewhere the index compares the way SQL does: an enum, a domain,
     * an array, a composite, JSON, a network address and a geometric value each have an equality
     * of their own that comparing stored forms does not reproduce, and a value of the wrong type
     * altogether is a question for the scan, which raises whatever PostgreSQL raises for it.
     */
    private Object storedFormOf(Expression valueExpr, Column column) {
        if (!hasProbeableStorage(column)) return null;
        Object raw;
        try {
            raw = executor.evalExpr(valueExpr, null);
        } catch (RuntimeException e) {
            return null;
        }
        // = NULL is unknown for every row, never a key.
        if (raw == null) return null;
        if (!valueMatchesColumnType(raw, column.getType())) return null;
        try {
            return TypeCoercion.coerceForStorage(raw, column);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** Whether this column's stored values are ones an index lookup compares the way SQL does. */
    private static boolean hasProbeableStorage(Column column) {
        if (column.isVirtual()) return false;
        if (column.getEnumTypeName() != null) return false;
        if (column.getDomainTypeName() != null) return false;
        if (column.getCompositeTypeName() != null) return false;
        if (column.getArrayElementType() != null) return false;
        switch (column.getType()) {
            case SMALLINT: case INTEGER: case BIGINT:
            case SMALLSERIAL: case SERIAL: case BIGSERIAL:
            case NUMERIC: case REAL: case DOUBLE_PRECISION:
            case TEXT: case VARCHAR: case CHAR: case NAME:
            case BOOLEAN:
            case DATE: case TIME: case TIMESTAMP: case TIMESTAMPTZ:
            case UUID:
                return true;
            default:
                return false;
        }
    }

    /**
     * Whether the query's value is of the type this column holds.
     *
     * <p>A quoted value has no type of its own in PostgreSQL and takes the column's, so a string
     * is accepted for a date or a UUID. Anything else crossing type families — a number against a
     * text column, a word against an integer one — is either an error PostgreSQL reports or a
     * comparison with a rule of its own, and neither is settled by a hash lookup.
     */
    private static boolean valueMatchesColumnType(Object value, DataType type) {
        switch (type) {
            case SMALLINT: case INTEGER: case BIGINT:
            case SMALLSERIAL: case SERIAL: case BIGSERIAL:
            case NUMERIC: case REAL: case DOUBLE_PRECISION:
                return value instanceof Number;
            case TEXT: case VARCHAR: case CHAR: case NAME:
                return value instanceof String;
            case BOOLEAN:
                return value instanceof Boolean;
            case DATE:
                return value instanceof String || value instanceof java.time.LocalDate;
            case TIME:
                return value instanceof String || value instanceof java.time.LocalTime;
            case TIMESTAMP:
                return value instanceof String || value instanceof java.time.LocalDateTime;
            case TIMESTAMPTZ:
                return value instanceof String || value instanceof java.time.OffsetDateTime;
            case UUID:
                return value instanceof String || value instanceof java.util.UUID;
            default:
                return false;
        }
    }

    private List<RowContext> applyRlsFiltering(List<RowContext> contexts, Table table, String schemaName) {
        if (executor.shouldBypassRls(table, schemaName)) return contexts;
        return filterByRlsUsing(contexts, table, "SELECT");
    }

    /** Filter rows by RLS USING policies for the given command. Shared by SELECT/UPDATE/DELETE. */
    List<RowContext> filterByRlsUsing(List<RowContext> contexts, Table table, String command) {
        String effectiveRole = executor.currentRole();
        List<RlsPolicy> permissivePolicies = new ArrayList<>();
        List<RlsPolicy> restrictivePolicies = new ArrayList<>();
        for (RlsPolicy p : table.getRlsPolicies()) {
            if (p.appliesTo(command) && p.getUsingExpr() != null
                    && p.appliesToRole(effectiveRole)) {
                if (p.isRestrictive()) {
                    restrictivePolicies.add(p);
                } else {
                    permissivePolicies.add(p);
                }
            }
        }
        // Default-deny: no applicable policies → 0 rows
        if (permissivePolicies.isEmpty() && restrictivePolicies.isEmpty()) {
            return new ArrayList<>();
        }
        List<RowContext> filtered = new ArrayList<>();
        for (RowContext ctx : contexts) {
            // PG semantics: row must pass ALL restrictive policies
            // AND at least ONE permissive policy (if any exist).
            // PostgreSQL evaluates a policy as part of the query, so whatever the expression raises
            // is reported with its own SQLSTATE. Dropping the row instead left the query looking
            // like it had succeeded while quietly hiding the row that raised.
            boolean passesPermissive = false;
            for (RlsPolicy policy : permissivePolicies) {
                Object result = executor.evalExpr(policy.getUsingExpr(), ctx);
                if (Boolean.TRUE.equals(result)) {
                    passesPermissive = true;
                    break;
                }
            }
            boolean passesRestrictive = true;
            for (RlsPolicy policy : restrictivePolicies) {
                Object result = executor.evalExpr(policy.getUsingExpr(), ctx);
                if (!Boolean.TRUE.equals(result)) {
                    passesRestrictive = false;
                    break;
                }
            }
            if (passesPermissive && passesRestrictive) {
                filtered.add(ctx);
            }
        }
        return filtered;
    }

    private List<RowContext> resolveSubquery(SelectStmt.SubqueryFrom subqFrom) {
        String alias = subqFrom.alias() != null ? subqFrom.alias() : "subquery";
        QueryResult subResult = readAsDerivedRelation(alias, subqFrom.columnAliases(), () -> {
            if (subqFrom.subquery() instanceof SelectStmt) {
                return executor.executeSelect((SelectStmt) subqFrom.subquery());
            }
            return executor.executeStatement(subqFrom.subquery());
        });
        List<Column> columns = FromFunctionResolver.applyColumnAliases(
                new ArrayList<>(subResult.getColumns()), subqFrom.columnAliases(), alias,
                aliasedItemNoun(subqFrom));
        Table virtualTable = new Table(alias, columns);
        renamesColumnsOf(virtualTable, alias, subResult.getColumns(), subqFrom.columnAliases());
        defineFromQuery(virtualTable, subqFrom.subquery());
        for (Object[] row : subResult.getRows()) {
            virtualTable.insertRow(row);
        }
        return derivedContexts(virtualTable, alias);
    }

    /**
     * The rows a relation built from a query offers the query reading it.
     *
     * <p>A VIRTUAL generated column the query underneath left for this relation to work out is
     * worked out here for every row, the same way a stored relation's is, because the query reading
     * it may qualify on it -- and a qualification decides which rows there are before anything is
     * read of them. Which columns that is, and for which rows, is the reading query's business:
     * {@link DmlExecutor#computeVirtualColumns(Table, String, Object[])} asks it.
     */
    private List<RowContext> derivedContexts(Table relation, String alias) {
        boolean hasVirtual = executor.dmlExecutor.hasVirtualColumns(relation);
        List<RowContext> contexts = new ArrayList<>();
        for (Object[] row : relation.getRows()) {
            contexts.add(new RowContext(relation, alias, hasVirtual
                    ? executor.dmlExecutor.computeVirtualColumns(relation, alias, row) : row));
        }
        return contexts;
    }

    // ---- Cross-product ----

    private List<RowContext> crossProductContexts(List<List<RowContext>> items) {
        List<RowContext> result = new ArrayList<>();
        if (items.isEmpty()) return result;
        if (items.size() == 1) return items.get(0);

        result = new ArrayList<>(items.get(0));
        for (int i = 1; i < items.size(); i++) {
            List<RowContext> right = items.get(i);
            List<RowContext> newResult = new ArrayList<>();
            for (RowContext leftCtx : result) {
                for (RowContext rightCtx : right) {
                    newResult.add(leftCtx.merge(rightCtx));
                }
            }
            result = newResult;
        }
        return result;
    }

    /**
     * Cross-product FROM items with early WHERE predicate application.
     */
    private List<RowContext> crossProductWithEarlyFilter(List<SelectStmt.FromItem> fromItems, Expression where) {
        List<Expression> predicates = flattenAndPredicates(where);
        Set<Integer> appliedPredicates = new HashSet<>();

        List<RowContext> accumulated = null;
        for (SelectStmt.FromItem fromItem : fromItems) {
            List<RowContext> resolved = resolveFromItem(fromItem);
            if (accumulated == null) {
                accumulated = resolved;
            } else {
                List<Expression> applicablePredicates = new ArrayList<>();
                if (!accumulated.isEmpty() && !resolved.isEmpty()) {
                    RowContext sample = accumulated.get(0).merge(resolved.get(0));
                    for (int pi = 0; pi < predicates.size(); pi++) {
                        if (!appliedPredicates.contains(pi) && canEvaluatePredicate(predicates.get(pi), sample)) {
                            applicablePredicates.add(predicates.get(pi));
                            appliedPredicates.add(pi);
                        }
                    }
                }

                accumulated = pairer.pair(accumulated, resolved, applicablePredicates,
                        (l, r) -> l.merge(r));
            }
        }
        return accumulated != null ? accumulated : Cols.listOf();
    }

    // ---- WHERE predicate pushdown helpers ----

    private List<Expression> flattenAndPredicates(Expression expr) {
        List<Expression> predicates = new ArrayList<>();
        flattenAndPredicatesHelper(expr, predicates);
        return predicates;
    }

    private void flattenAndPredicatesHelper(Expression expr, List<Expression> result) {
        if (expr instanceof BinaryExpr && ((BinaryExpr) expr).op() == BinaryExpr.BinOp.AND) {
            BinaryExpr bin = (BinaryExpr) expr;
            flattenAndPredicatesHelper(bin.left(), result);
            flattenAndPredicatesHelper(bin.right(), result);
        } else {
            result.add(expr);
        }
    }

    /**
     * Whether every column a conjunct names is one this row has, and so whether it can be
     * decided here at all.
     *
     * <p>Answering yes wrongly does not lose rows: a conjunct that then fails to evaluate leaves
     * its rows alone, and the query's own WHERE runs over the finished rows regardless. It only
     * costs the chance to have filtered earlier.
     */
    static boolean canEvaluatePredicate(Expression pred, RowContext ctx) {
        try {
            collectColumnRefs(pred, ctx);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void collectColumnRefs(Expression expr, RowContext ctx) {
        if (expr instanceof ColumnRef) {
            ColumnRef cr = (ColumnRef) expr;
            if (ctx.resolveColumnDef(cr.table(), cr.column()) == null) {
                throw new RuntimeException("unresolvable");
            }
        } else if (expr instanceof BinaryExpr) {
            BinaryExpr bin = (BinaryExpr) expr;
            collectColumnRefs(bin.left(), ctx);
            collectColumnRefs(bin.right(), ctx);
        } else if (expr instanceof CustomOperatorExpr) {
            CustomOperatorExpr cop = (CustomOperatorExpr) expr;
            if (cop.left() != null) collectColumnRefs(cop.left(), ctx);
            collectColumnRefs(cop.right(), ctx);
        } else if (expr instanceof UnaryExpr) {
            UnaryExpr ue = (UnaryExpr) expr;
            collectColumnRefs(ue.operand(), ctx);
        } else if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr fce = (FunctionCallExpr) expr;
            if (fce.args() != null) {
                for (Expression arg : fce.args()) {
                    collectColumnRefs(arg, ctx);
                }
            }
        } else if (expr instanceof CastExpr) {
            CastExpr ce = (CastExpr) expr;
            collectColumnRefs(ce.expr(), ctx);
        } else if (expr instanceof IsNullExpr) {
            IsNullExpr ine = (IsNullExpr) expr;
            collectColumnRefs(ine.expr(), ctx);
        } else if (expr instanceof InExpr) {
            InExpr ie = (InExpr) expr;
            collectColumnRefs(ie.expr(), ctx);
            if (ie.values() != null) {
                for (Expression v : ie.values()) collectColumnRefs(v, ctx);
            }
        } else if (expr instanceof CaseExpr) {
            CaseExpr caseExpr = (CaseExpr) expr;
            if (caseExpr.operand() != null) collectColumnRefs(caseExpr.operand(), ctx);
            if (caseExpr.whenClauses() != null) {
                for (CaseExpr.WhenClause wc : caseExpr.whenClauses()) {
                    collectColumnRefs(wc.condition(), ctx);
                    collectColumnRefs(wc.result(), ctx);
                }
            }
            if (caseExpr.elseExpr() != null) collectColumnRefs(caseExpr.elseExpr(), ctx);
        }
        // Subqueries, ExistsExpr, Literals, and other types: skip or always resolve
    }

    // ---- MVCC Visibility ----

    /**
     * One row of a snapshot, bound to the relation that stores it.
     *
     * <p>A row a partitioned table's snapshot or an inheritance parent's holds is a row of one of
     * the relations below, and which relation a row belongs to and where in it the row lives are
     * properties of that relation. PostgreSQL answers tableoid and ctid from it however the row
     * was reached, so the binding says where the row came from and which tuple it stands for.
     */
    private RowContext snapshotContext(Table table, String alias, String schemaTableKey,
                                       Object[] row, Object[] values, Session session) {
        Table storage = session.snapshotRowStorage(schemaTableKey, row);
        if (storage == null) return new RowContext(table, alias, values);
        Object[] tuple = session.snapshotRowTuple(schemaTableKey, row);
        return new RowContext(Cols.listOf(new RowContext.TableBinding(
                table, alias, values, storage, tuple != null ? tuple : row)));
    }

    private List<RowContext> applyMvccVisibility(List<RowContext> contexts, Table table, String alias,
                                                  String schemaTableKey, Session currentSession,
                                                  boolean onlyOwnRows) {
        Database db = executor.database;

        // SSI: track that this serializable transaction read from this table
        currentSession.trackSsiRead(schemaTableKey);

        Set<Object[]> otherUncommittedInserts = Collections.newSetFromMap(new IdentityHashMap<>());
        Map<Object[], Object[]> otherUncommittedUpdates = new IdentityHashMap<>();
        List<Object[]> otherUncommittedDeletes = new ArrayList<>();

        for (Session otherSession : db.getActiveSessions()) {
            if (otherSession == currentSession) continue;
            if (!otherSession.isInTransaction()) continue;

            Set<Object[]> inserts = otherSession.getUncommittedInserts(schemaTableKey);
            otherUncommittedInserts.addAll(inserts);

            Map<Object[], Object[]> updates = otherSession.getUncommittedUpdates(schemaTableKey);
            otherUncommittedUpdates.putAll(updates);

            List<Object[]> deletes = otherSession.getUncommittedDeletes(schemaTableKey);
            otherUncommittedDeletes.addAll(deletes);
        }

        if (otherUncommittedInserts.isEmpty() && otherUncommittedUpdates.isEmpty() && otherUncommittedDeletes.isEmpty()) {
            if (currentSession.isInTransaction()) {
                String isolation = currentSession.getEffectiveIsolationLevel();
                if ("repeatable read".equals(isolation) || "serializable".equals(isolation)) {
                    List<Object[]> visibleRows = new ArrayList<>();
                    for (RowContext ctx : contexts) {
                        visibleRows.add(getFirstRow(ctx));
                    }
                    List<Object[]> snapshot = currentSession.getOrCreateRRSnapshot(
                            schemaTableKey, visibleRows, table, contexts);
                    if (snapshot != null) {
                        List<RowContext> snapshotContexts = new ArrayList<>();
                        for (Object[] row : snapshot) {
                            snapshotContexts.add(
                                    snapshotContext(table, alias, schemaTableKey, row, row, currentSession));
                        }
                        return snapshotContexts;
                    }
                }
            }
            return contexts;
        }

        List<RowContext> filtered = new ArrayList<>();
        for (RowContext ctx : contexts) {
            RowContext.TableBinding binding = firstBinding(ctx);
            Object[] row = binding == null ? null : binding.row();
            // A row this relation reads through a partition or an inheritance child is a copy of
            // the row rearranged to this relation's columns. Another session's work on it is
            // recorded against the row where it lives, which is the one the copy was made from.
            Object[] stored = binding == null || binding.storedRow() == null
                    ? row : binding.storedRow();

            if (otherUncommittedInserts.contains(stored)) {
                continue;
            }

            Object[] oldValues = otherUncommittedUpdates.get(stored);
            if (oldValues != null) {
                filtered.add(committedContext(table, alias, binding.sourceTable(), oldValues));
                continue;
            }

            filtered.add(ctx);
        }

        for (Object[] deletedRow : otherUncommittedDeletes) {
            if (!otherUncommittedInserts.contains(deletedRow)) {
                Table home = currentSession.relationStoringRow(table, deletedRow);
                // ONLY reads what the relation stores itself. A statement naming the relation may
                // have deleted a row one of its inheritance children holds, and that row was never
                // one of the rows ONLY reads.
                if (onlyOwnRows && home != table) continue;
                // If the row was updated before being deleted, the committed
                // state is the pre-update old values, not its current contents.
                Object[] oldValues = otherUncommittedUpdates.get(deletedRow);
                filtered.add(committedContext(table, alias, home,
                        oldValues != null ? oldValues : deletedRow));
            }
        }

        if (currentSession.isInTransaction()) {
            String isolation = currentSession.getEffectiveIsolationLevel();
            if ("repeatable read".equals(isolation) || "serializable".equals(isolation)) {
                List<Object[]> visibleRows = new ArrayList<>();
                for (RowContext ctx : filtered) {
                    visibleRows.add(getFirstRow(ctx));
                }
                List<Object[]> snapshot = currentSession.getOrCreateRRSnapshot(
                        schemaTableKey, visibleRows, table, filtered);
                if (snapshot != null) {
                    List<RowContext> snapshotContexts = new ArrayList<>();
                    for (Object[] row : snapshot) {
                        snapshotContexts.add(
                                snapshotContext(table, alias, schemaTableKey, row, row, currentSession));
                    }
                    return snapshotContexts;
                }
            }
        }

        return filtered;
    }

    private static Object[] getFirstRow(RowContext ctx) {
        RowContext.TableBinding binding = firstBinding(ctx);
        return binding == null ? null : binding.row();
    }

    private static RowContext.TableBinding firstBinding(RowContext ctx) {
        List<RowContext.TableBinding> bindings = ctx.getBindings();
        return bindings.isEmpty() ? null : bindings.get(0);
    }

    /**
     * The version of a row this session is entitled to see, read as {@code table} reads it.
     *
     * <p>What another session's uncommitted write replaced, and what it has deleted without
     * committing, are rows of the relation that stores them: a partition or an inheritance child
     * holds its own columns and the relation above reads them rearranged to its own. Where the row
     * lives and which relation it belongs to stay with the stored row, so ctid and tableoid go on
     * answering from there and not from the relation the reader happened to name.
     */
    private static RowContext committedContext(Table table, String alias, Table storage,
                                               Object[] stored) {
        if (storage == null || storage == table) return new RowContext(table, alias, stored);
        return new RowContext(Cols.listOf(new RowContext.TableBinding(
                table, alias, rowAsRelationAboveReadsIt(storage, table, stored), storage, stored)));
    }

    /**
     * One of {@code storage}'s rows as {@code above} reads it, rearranged through every relation
     * between the two. A partition may order its columns differently from the table it partitions
     * and an inheritance child may carry columns its parent never declared.
     */
    private static Object[] rowAsRelationAboveReadsIt(Table storage, Table above, Object[] row) {
        Object[] mapped = row;
        Table below = storage;
        while (below != null && below != above) {
            Table next = below.getPartitionParent();
            if (next != null) mapped = below.rowToParent(mapped);
            else next = below.getInheritParents().isEmpty() ? null : below.getInheritParents().get(0);
            if (next == null) break;
            below = next;
        }
        int width = above.getColumns().size();
        return mapped.length == width ? mapped : Arrays.copyOf(mapped, width);
    }
}
