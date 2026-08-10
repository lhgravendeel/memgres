package com.memgres.engine;

import com.memgres.engine.parser.ast.*;

import java.util.List;

/**
 * Decides whether a view accepts INSERT, UPDATE and DELETE, and says why when it does not.
 *
 * <p>PostgreSQL's rule for an automatically updatable view is exact, and this is the one place
 * that states it. The executor asks it before rewriting a write on a view into a write on the
 * base table; {@code information_schema.views}, {@code information_schema.tables},
 * {@code information_schema.columns}, {@code pg_relation_is_updatable} and
 * {@code pg_column_is_updatable} all answer from the same code. A tool that asks the catalog
 * before writing therefore gets the answer the write will actually give it — the two disagreeing
 * is worse than either being wrong, because the caller has no way to find out which to believe.</p>
 *
 * <p>The reasons are worded the way PostgreSQL words the {@code Detail} line of the error, and
 * they are tested in the order PostgreSQL tests them, so a view that breaks two of the rules is
 * refused for the same one PostgreSQL names.</p>
 */
final class ViewUpdatability {

    private ViewUpdatability() {
    }

    /**
     * The event bits {@code pg_relation_is_updatable} reports. PostgreSQL builds them as
     * {@code 1 << CMD_UPDATE}, {@code 1 << CMD_INSERT} and {@code 1 << CMD_DELETE}, which is why
     * a plain table answers 28 rather than 7.
     */
    static final int UPDATE = 4;
    static final int INSERT = 8;
    static final int DELETE = 16;
    static final int ALL_EVENTS = UPDATE | INSERT | DELETE;

    static final String DETAIL_DISTINCT =
            "Views containing DISTINCT are not automatically updatable.";
    static final String DETAIL_GROUP_BY =
            "Views containing GROUP BY are not automatically updatable.";
    static final String DETAIL_HAVING =
            "Views containing HAVING are not automatically updatable.";
    static final String DETAIL_SET_OP =
            "Views containing UNION, INTERSECT, or EXCEPT are not automatically updatable.";
    static final String DETAIL_WITH =
            "Views containing WITH are not automatically updatable.";
    static final String DETAIL_LIMIT =
            "Views containing LIMIT or OFFSET are not automatically updatable.";
    static final String DETAIL_AGGREGATE =
            "Views that return aggregate functions are not automatically updatable.";
    static final String DETAIL_WINDOW =
            "Views that return window functions are not automatically updatable.";
    static final String DETAIL_SRF =
            "Views that return set-returning functions are not automatically updatable.";
    static final String DETAIL_NOT_SINGLE_RELATION =
            "Views that do not select from a single table or view are not automatically updatable.";
    static final String DETAIL_NOT_COLUMN =
            "View columns that are not columns of their base relation are not updatable.";

    /** How deep a chain of views over views is followed before it is called a loop. */
    private static final int MAX_VIEW_DEPTH = 100;

    /** Why a write on a view is refused: the relation to blame and the sentence PostgreSQL uses. */
    static final class Reason {
        /** The relation the rule was broken by — the innermost view of a chain of views. */
        final String relation;
        final String detail;

        Reason(String relation, String detail) {
            this.relation = relation;
            this.detail = detail;
        }
    }

    /**
     * Why this view is not automatically updatable, or null when it is. A view over another view
     * is only as updatable as the view underneath it, so the reason reported is the innermost
     * one, blamed on the innermost view — which is the view PostgreSQL names in the error.
     */
    static Reason notAutoUpdatable(Database db, Database.ViewDef vd) {
        return notAutoUpdatable(db, vd, 0);
    }

    /** The {@code Detail} sentence alone, or null when the view is automatically updatable. */
    static String notAutoUpdatableReason(Database db, Database.ViewDef vd) {
        Reason reason = notAutoUpdatable(db, vd, 0);
        return reason == null ? null : reason.detail;
    }

    private static Reason notAutoUpdatable(Database db, Database.ViewDef vd, int depth) {
        if (vd == null) return new Reason(null, DETAIL_NOT_SINGLE_RELATION);
        if (vd.materialized()) return new Reason(vd.name(), DETAIL_NOT_SINGLE_RELATION);
        if (depth > MAX_VIEW_DEPTH) return new Reason(vd.name(), DETAIL_NOT_SINGLE_RELATION);
        Statement query = vd.query();
        if (!(query instanceof SelectStmt)) {
            // A view whose body is a set operation has no single relation to write back to, and
            // PostgreSQL names the set operation rather than the missing relation.
            if (query instanceof SetOpStmt) return new Reason(vd.name(), DETAIL_SET_OP);
            return new Reason(vd.name(), DETAIL_NOT_SINGLE_RELATION);
        }
        SelectStmt sel = (SelectStmt) query;
        String shape = queryShapeReason(db, sel);
        if (shape != null) return new Reason(vd.name(), shape);
        // The body selects from exactly one relation; if that relation is itself a view, the
        // write has to reach a table through it too.
        SelectStmt.TableRef ref = (SelectStmt.TableRef) sel.from().get(0);
        Database.ViewDef inner = ref.schema() != null
                ? db.getView(ref.schema(), ref.table()) : db.getView(ref.table());
        if (inner != null) {
            // An unconditional INSTEAD rule replaces the write on the inner view outright, so
            // the shape of that view no longer decides anything. A trigger does not count here:
            // PostgreSQL reports a view over a trigger-updatable view as not auto-updatable and
            // only counts the trigger when the caller asks it to.
            if (insteadRuleEvents(db, inner.name()) == ALL_EVENTS) return null;
            return notAutoUpdatable(db, inner, depth + 1);
        }
        return null;
    }

    /**
     * Why the body of a view is not of an automatically updatable shape, or null when it is.
     * The clauses are tested in PostgreSQL's own order so that a view breaking more than one
     * rule is reported against the same rule PostgreSQL reports it against.
     */
    private static String queryShapeReason(Database db, SelectStmt sel) {
        if (sel.distinct() || (sel.distinctOn() != null && !sel.distinctOn().isEmpty())) {
            return DETAIL_DISTINCT;
        }
        if ((sel.groupBy() != null && !sel.groupBy().isEmpty())
                || (sel.groupingSets() != null && !sel.groupingSets().isEmpty())) {
            return DETAIL_GROUP_BY;
        }
        if (sel.having() != null) return DETAIL_HAVING;
        if (sel.withClauses() != null && !sel.withClauses().isEmpty()) return DETAIL_WITH;
        if (sel.limit() != null || sel.offset() != null) return DETAIL_LIMIT;
        if (sel.targets() != null) {
            for (SelectStmt.SelectTarget target : sel.targets()) {
                if (containsAggregate(target.expr())) return DETAIL_AGGREGATE;
            }
            for (SelectStmt.SelectTarget target : sel.targets()) {
                if (containsWindowFunc(target.expr())) return DETAIL_WINDOW;
            }
            for (SelectStmt.SelectTarget target : sel.targets()) {
                if (containsSetReturningCall(db, target.expr())) return DETAIL_SRF;
            }
        }
        // A join, a subquery in FROM, a function in FROM or a VALUES list all leave the write
        // with no single relation to reach.
        if (sel.from() == null || sel.from().size() != 1) return DETAIL_NOT_SINGLE_RELATION;
        if (!(sel.from().get(0) instanceof SelectStmt.TableRef)) return DETAIL_NOT_SINGLE_RELATION;
        return null;
    }

    /** The events an INSTEAD OF trigger on this relation covers, as event bits. */
    static int insteadOfEvents(Database db, String relation) {
        int events = 0;
        List<PgTrigger> triggers = db.getTriggersForTable(relation);
        if (triggers == null) return 0;
        for (PgTrigger t : triggers) {
            if (t.getTiming() != PgTrigger.Timing.INSTEAD_OF) continue;
            if (t.getEvent() == PgTrigger.Event.INSERT) events |= INSERT;
            else if (t.getEvent() == PgTrigger.Event.UPDATE) events |= UPDATE;
            else if (t.getEvent() == PgTrigger.Event.DELETE) events |= DELETE;
        }
        return events;
    }

    /** The events an unconditional INSTEAD rule on this relation covers, as event bits. */
    static int insteadRuleEvents(Database db, String relation) {
        int events = 0;
        if (isUnconditionalInsteadRule(db, relation, "INSERT")) events |= INSERT;
        if (isUnconditionalInsteadRule(db, relation, "UPDATE")) events |= UPDATE;
        if (isUnconditionalInsteadRule(db, relation, "DELETE")) events |= DELETE;
        return events;
    }

    private static boolean isUnconditionalInsteadRule(Database db, String relation, String event) {
        String rule = db.getRule(relation, event);
        if (rule == null) return false;
        if (!rule.startsWith("INSTEAD") && !"INSTEAD_NOTHING".equals(rule)) return false;
        // A rule with a WHERE of its own only replaces the write for the rows it matches, so
        // PostgreSQL does not count it as making the view updatable.
        return db.getRuleQualification(relation, event) == null;
    }

    /**
     * What {@code pg_relation_is_updatable} answers for a relation: the events it accepts, as
     * event bits. A table accepts all three. A view accepts what its shape allows, plus — when
     * the caller asked for triggers to be counted — whatever an INSTEAD OF trigger covers.
     */
    static int relationEvents(Database db, String schema, String relation, boolean includeTriggers) {
        return relationEvents(db, schema, relation, includeTriggers, 0);
    }

    private static int relationEvents(Database db, String schema, String relation,
                                      boolean includeTriggers, int depth) {
        Database.ViewDef vd = schema != null ? db.getView(schema, relation) : db.getView(relation);
        if (vd == null) {
            // A table, or a name that reaches nothing: PostgreSQL answers 0 for an oid it cannot
            // resolve rather than raising, so an absent relation and a read-only one look alike.
            Table t = findTable(db, schema, relation);
            return t != null ? ALL_EVENTS : 0;
        }
        if (vd.materialized() || depth > MAX_VIEW_DEPTH) return 0;
        int events = insteadRuleEvents(db, relation);
        if (includeTriggers) events |= insteadOfEvents(db, relation);
        // A view of an updatable shape is only as updatable as what it selects from: a view over
        // a view that nothing can write to accepts nothing either, and a view over one that only
        // a trigger makes writable accepts only what that trigger covers.
        if (vd.query() instanceof SelectStmt) {
            SelectStmt sel = (SelectStmt) vd.query();
            if (queryShapeReason(db, sel) == null) {
                SelectStmt.TableRef ref = (SelectStmt.TableRef) sel.from().get(0);
                String baseSchema = ref.schema() != null ? ref.schema() : schema;
                events |= relationEvents(db, baseSchema, ref.table(), includeTriggers, depth + 1);
            }
        }
        return events;
    }

    /**
     * What {@code pg_column_is_updatable} answers: whether an UPDATE may assign to this column.
     * A column of an updatable view is only updatable when it is a plain reference to a column
     * of the base relation — an expression has nothing to assign back to.
     */
    static boolean columnIsUpdatable(Database db, String schema, String relation, String column,
                                     boolean includeTriggers) {
        Database.ViewDef vd = schema != null ? db.getView(schema, relation) : db.getView(relation);
        if (vd == null) {
            return findTable(db, schema, relation) != null;
        }
        int events = relationEvents(db, schema, relation, includeTriggers);
        if ((events & (UPDATE | DELETE)) != (UPDATE | DELETE)) return false;
        // An INSTEAD OF trigger takes the whole row, so every column of the view is assignable.
        if (notAutoUpdatableReason(db, vd) != null) return true;
        return !isExpressionColumn(vd, column);
    }

    /** True when this view column is computed rather than read straight out of the base relation. */
    static boolean isExpressionColumn(Database.ViewDef vd, String column) {
        if (vd == null || column == null) return false;
        if (!(vd.query() instanceof SelectStmt)) return false;
        SelectStmt sel = (SelectStmt) vd.query();
        if (sel.targets() == null) return false;
        for (SelectStmt.SelectTarget target : sel.targets()) {
            String name = target.alias();
            if (name == null && target.expr() instanceof ColumnRef) {
                name = ((ColumnRef) target.expr()).column();
            }
            if (name == null || !name.equalsIgnoreCase(column)) continue;
            return !(target.expr() instanceof ColumnRef);
        }
        return false;
    }

    private static Table findTable(Database db, String schema, String relation) {
        if (schema != null) {
            Schema s = db.getSchema(schema);
            if (s != null && s.getTable(relation) != null) return s.getTable(relation);
        }
        for (Schema s : db.getSchemas().values()) {
            Table t = s.getTable(relation);
            if (t != null) return t;
        }
        return null;
    }

    /** True if the expression contains an aggregate call anywhere inside it. */
    static boolean containsAggregate(Object node) {
        if (node == null || node instanceof Statement) return false;
        if (node instanceof WindowFuncExpr) {
            // A window call over an aggregate is a window function, not an aggregate.
            return false;
        }
        if (node instanceof FunctionCallExpr) {
            String name = ((FunctionCallExpr) node).name().toLowerCase();
            if (name.indexOf('.') >= 0) name = name.substring(name.lastIndexOf('.') + 1);
            if (SelectExecutor.AGGREGATE_FUNCTIONS.contains(name)) return true;
        }
        final boolean[] found = {false};
        AstWalk.forEachChild(node, child -> {
            if (!found[0] && containsAggregate(child)) found[0] = true;
        });
        return found[0];
    }

    /** True if the expression contains an OVER clause anywhere inside it. */
    static boolean containsWindowFunc(Object node) {
        if (node == null || node instanceof Statement) return false;
        if (node instanceof WindowFuncExpr) return true;
        final boolean[] found = {false};
        AstWalk.forEachChild(node, child -> {
            if (!found[0] && containsWindowFunc(child)) found[0] = true;
        });
        return found[0];
    }

    /**
     * True if the expression calls a set-returning function anywhere inside it. The names are the
     * ones {@link SelectExecutor} recognises plus anything this database was told returns a set,
     * so a view over a user-declared {@code RETURNS SETOF} function is judged the same way.
     */
    static boolean containsSetReturningCall(Database db, Object node) {
        if (node == null || node instanceof Statement) return false;
        if (node instanceof FunctionCallExpr) {
            FunctionCallExpr call = (FunctionCallExpr) node;
            String name = FunctionEvaluator.stripSchemaPrefix(call.name().toLowerCase());
            if (SelectExecutor.SRF_FUNCTION_NAMES.contains(name)) return true;
            if (db != null) {
                PgFunction declared = db.getFunction(name);
                if (declared != null && declared.isSetReturning() && !declared.declaresRecordResult()) {
                    return true;
                }
            }
        }
        final boolean[] found = {false};
        AstWalk.forEachChild(node, child -> {
            if (!found[0] && containsSetReturningCall(db, child)) found[0] = true;
        });
        return found[0];
    }

    /**
     * PostgreSQL's Hint for a write refused on a non-updatable view: it names the trigger and the
     * rule that would make the write possible, so a caller learns what to create rather than only
     * that it cannot write. A MERGE is the one write no rule can stand in for, so its Hint offers
     * the trigger alone and says which command it is speaking about.
     */
    static String hintFor(String verb, boolean byMerge) {
        String doing = "update".equals(verb) ? "updating the view"
                : "delete from".equals(verb) ? "deleting from the view" : "inserting into the view";
        String event = "update".equals(verb) ? "UPDATE"
                : "delete from".equals(verb) ? "DELETE" : "INSERT";
        if (byMerge) {
            return "To enable " + doing + " using MERGE, provide an INSTEAD OF "
                    + event + " trigger.";
        }
        return "To enable " + doing + ", provide an INSTEAD OF " + event + " trigger"
                + " or an unconditional ON " + event + " DO INSTEAD rule.";
    }

    /** The 55000 a write on a non-updatable view raises, with PostgreSQL's Detail and Hint. */
    static MemgresException cannotWrite(String verb, String viewName, String detail,
                                        boolean byMerge) {
        MemgresException ex = new MemgresException(
                "cannot " + verb + " view \"" + viewName + "\"", "55000");
        ex.setDetail(detail);
        ex.setHint(hintFor(verb, byMerge));
        return ex;
    }
}
