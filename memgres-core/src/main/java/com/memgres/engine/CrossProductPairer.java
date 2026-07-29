package com.memgres.engine;

import com.memgres.engine.parser.ast.BinaryExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;

/**
 * Puts one FROM item beside the ones already resolved, using the WHERE conjuncts that reach them.
 *
 * <p>A comma between FROM items is an inner join, so every conjunct of the query's top-level AND
 * has to hold of the finished row. That makes three shortcuts sound, and this class takes them in
 * order:
 *
 * <ol>
 *   <li>a conjunct that names only the relations on one side decides that side's rows on its own,
 *       so it is applied there before any pairing — a restriction like {@code c.relname = 't'}
 *       cuts a catalog of hundreds of rows to one before it is ever paired with anything;</li>
 *   <li>an equality between one column of each side pairs the rows by key instead of by trying
 *       every combination;</li>
 *   <li>whatever is left is evaluated on each row the first two produced.</li>
 * </ol>
 *
 * <p>None of this is allowed to change the answer, so:
 *
 * <ul>
 *   <li>a conjunct is pushed to a side only when it resolves against that side <em>and does not
 *       resolve against the other</em> — a bare column name both sides hold is ambiguous, and the
 *       query has to say so rather than quietly read one of them;</li>
 *   <li>a conjunct that cannot be evaluated where it was placed leaves its rows alone, exactly as
 *       the pair-wise filter did; the query's own WHERE still runs over the finished rows;</li>
 *   <li>keying rows is a way of proposing pairs, never of accepting them: every pair the keys
 *       propose is still put through all the conjuncts. So a key that collides costs time and
 *       nothing else, and only a key that <em>misses</em> could lose a row. That is why the keys
 *       are built from a small set of column types whose values key exactly, and why anything
 *       unexpected — a value of the wrong class, a null, an expression that will not evaluate —
 *       abandons the keys and pairs the rows the plain way;</li>
 *   <li>under {@code =} a null matches nothing, so a null key is never put in the table and never
 *       probes it.</li>
 * </ul>
 *
 * <p>The rows come out in the order the plain double loop produced them: the left side in its own
 * order, and within each left row the matching right rows in theirs.
 */
final class CrossProductPairer {

    /** Below this many combinations the plain double loop is quicker than building a key table. */
    private static final int HASH_THRESHOLD = 2048;

    private final AstExecutor executor;

    CrossProductPairer(AstExecutor executor) {
        this.executor = executor;
    }

    /**
     * The rows of {@code left} and {@code right} side by side, keeping those the conjuncts admit.
     *
     * @param predicates conjuncts of the query's WHERE that reach both sides together
     * @param merge      how a left row and a right row are joined into one
     */
    List<RowContext> pair(List<RowContext> left, List<RowContext> right,
                          List<Expression> predicates, BinaryOperator<RowContext> merge) {
        if (left.isEmpty() || right.isEmpty()) return new ArrayList<>();
        if (predicates.isEmpty()) return nestedLoop(left, right, predicates, merge);

        RowContext leftSample = left.get(0);
        RowContext rightSample = right.get(0);
        List<Expression> leftOnly = new ArrayList<>();
        List<Expression> rightOnly = new ArrayList<>();
        List<Expression> shared = new ArrayList<>();
        for (Expression pred : predicates) {
            boolean inLeft = resolvesAgainst(pred, leftSample);
            boolean inRight = resolvesAgainst(pred, rightSample);
            if (inLeft && !inRight) {
                leftOnly.add(pred);
            } else if (inRight && !inLeft) {
                rightOnly.add(pred);
            } else {
                shared.add(pred);
            }
        }

        List<RowContext> l = restrict(left, leftOnly);
        List<RowContext> r = restrict(right, rightOnly);
        if (l.isEmpty() || r.isEmpty()) return new ArrayList<>();

        if ((long) l.size() * r.size() >= HASH_THRESHOLD) {
            HashPlan plan = planFor(shared, l.get(0), r.get(0));
            if (plan != null) {
                List<RowContext> keyed = hashJoin(l, r, shared, plan, merge);
                if (keyed != null) return keyed;
            }
        }
        return nestedLoop(l, r, shared, merge);
    }

    // ---- The plain pairing ----

    private List<RowContext> nestedLoop(List<RowContext> left, List<RowContext> right,
                                        List<Expression> predicates,
                                        BinaryOperator<RowContext> merge) {
        List<RowContext> out = new ArrayList<>();
        for (RowContext leftCtx : left) {
            for (RowContext rightCtx : right) {
                RowContext merged = merge.apply(leftCtx, rightCtx);
                if (holds(merged, predicates)) out.add(merged);
            }
        }
        return out;
    }

    // ---- Pairing by key ----

    /** One equality between a column of each side, and which family its values key as. */
    private static final class HashPlan {
        final Expression leftKey;
        final Expression rightKey;
        /** True when the keys are text read as themselves, false when they are numbers. */
        final boolean text;

        HashPlan(Expression leftKey, Expression rightKey, boolean text) {
            this.leftKey = leftKey;
            this.rightKey = rightKey;
            this.text = text;
        }
    }

    /** The rows the plan's keys pair, or null when the values would not key exactly. */
    private List<RowContext> hashJoin(List<RowContext> left, List<RowContext> right,
                                      List<Expression> predicates, HashPlan plan,
                                      BinaryOperator<RowContext> merge) {
        Object[] rightKeys = keysOf(right, plan.rightKey, plan.text);
        if (rightKeys == null) return null;
        Object[] leftKeys = keysOf(left, plan.leftKey, plan.text);
        if (leftKeys == null) return null;

        Map<Object, List<RowContext>> buckets = new HashMap<>();
        for (int i = 0; i < right.size(); i++) {
            Object key = rightKeys[i];
            if (key == null) continue; // a null key matches nothing under =
            List<RowContext> bucket = buckets.get(key);
            if (bucket == null) {
                bucket = new ArrayList<>();
                buckets.put(key, bucket);
            }
            bucket.add(right.get(i));
        }

        List<RowContext> out = new ArrayList<>();
        for (int i = 0; i < left.size(); i++) {
            Object key = leftKeys[i];
            if (key == null) continue;
            List<RowContext> bucket = buckets.get(key);
            if (bucket == null) continue;
            RowContext leftCtx = left.get(i);
            for (RowContext rightCtx : bucket) {
                RowContext merged = merge.apply(leftCtx, rightCtx);
                // The key only proposed this pair; the conjuncts, including the one that
                // supplied the key, still decide it.
                if (holds(merged, predicates)) out.add(merged);
            }
        }
        return out;
    }

    /**
     * The key of every row, null where the row's key is null, or null for the whole array when
     * some value will not key exactly and the rows have to be paired the plain way instead.
     */
    private Object[] keysOf(List<RowContext> rows, Expression keyExpr, boolean text) {
        Object[] keys = new Object[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            Object value;
            try {
                value = executor.evalExpr(keyExpr, rows.get(i));
            } catch (Exception e) {
                return null;
            }
            if (value == null) continue;
            if (text) {
                if (!(value instanceof String)) return null;
                String s = (String) value;
                // Two texts that read as times of day compare as times, not as text, so their
                // written forms cannot stand in for them.
                if (TypeCoercion.isTimeTzString(s.trim())) return null;
                keys[i] = s;
            } else {
                if (!(value instanceof Number)) return null;
                double d = ((Number) value).doubleValue();
                if (Double.isNaN(d) || Double.isInfinite(d)) return null;
                keys[i] = Double.valueOf(d == 0.0 ? 0.0 : d); // -0.0 and 0.0 are one value
            }
        }
        return keys;
    }

    /** An equality this pairing can key on, or null when it has none. */
    private static HashPlan planFor(List<Expression> predicates, RowContext left, RowContext right) {
        for (Expression pred : predicates) {
            if (!(pred instanceof BinaryExpr)) continue;
            BinaryExpr bin = (BinaryExpr) pred;
            if (bin.op() != BinaryExpr.BinOp.EQUAL) continue;
            if (!(bin.left() instanceof ColumnRef) || !(bin.right() instanceof ColumnRef)) continue;
            ColumnRef a = (ColumnRef) bin.left();
            ColumnRef b = (ColumnRef) bin.right();
            HashPlan plan = planFor(a, b, bin.left(), bin.right(), left, right);
            if (plan == null) plan = planFor(b, a, bin.right(), bin.left(), left, right);
            if (plan != null) return plan;
        }
        return null;
    }

    private static HashPlan planFor(ColumnRef onLeft, ColumnRef onRight,
                                    Expression leftKey, Expression rightKey,
                                    RowContext left, RowContext right) {
        Column lc = keyColumn(onLeft, left, right);
        if (lc == null) return null;
        Column rc = keyColumn(onRight, right, left);
        if (rc == null) return null;
        if (!keyable(lc) || !keyable(rc)) return null;
        if (integral(lc.getType()) && integral(rc.getType())) {
            return new HashPlan(leftKey, rightKey, false);
        }
        if (plainText(lc.getType()) && plainText(rc.getType())) {
            return new HashPlan(leftKey, rightKey, true);
        }
        return null;
    }

    /**
     * The one column of {@code own} a reference names, when {@code other} has no column of that
     * name at all — a name both sides answer to is ambiguous, and reading either of them here
     * would answer a query PostgreSQL refuses.
     */
    private static Column keyColumn(ColumnRef ref, RowContext own, RowContext other) {
        if (ref.catalog() != null || ref.schema() != null) return null;
        String column = ref.column();
        if (column == null || "*".equals(column)) return null;
        Column resolved;
        try {
            resolved = own.resolveColumnDef(ref.table(), column);
            if (resolved == null) return null;
            if (other.resolveColumnDef(ref.table(), column) != null) return null;
        } catch (RuntimeException e) {
            return null;
        }
        // The reference has to reach one stored column, and the same one: a pseudo-column, a
        // column a join merged, or a name two relations of this side both hold is not keyable.
        Column found = null;
        int hits = 0;
        for (RowContext.TableBinding binding : own.getBindings()) {
            if (ref.table() != null) {
                String name = binding.alias() != null ? binding.alias() : binding.table().getName();
                if (!name.equalsIgnoreCase(ref.table())) continue;
            }
            int idx = binding.table().getColumnIndex(column);
            if (idx < 0) continue;
            hits++;
            found = binding.table().getColumns().get(idx);
        }
        return hits == 1 && found == resolved ? found : null;
    }

    /** A column whose values are its own type's, with nothing layered over them. */
    private static boolean keyable(Column column) {
        return column.getEnumTypeName() == null && column.getDomainTypeName() == null
                && column.getCompositeTypeName() == null && column.getArrayElementType() == null;
    }

    /** The whole-number types, whose values are equal exactly when their magnitudes are. */
    private static boolean integral(DataType type) {
        return type == DataType.SMALLINT || type == DataType.INTEGER || type == DataType.BIGINT
                || type == DataType.OID || type == DataType.SMALLSERIAL
                || type == DataType.SERIAL || type == DataType.BIGSERIAL;
    }

    /** The text types compared as written — not bpchar, whose trailing blanks do not count. */
    private static boolean plainText(DataType type) {
        return type == DataType.TEXT || type == DataType.VARCHAR || type == DataType.NAME;
    }

    // ---- Applying conjuncts ----

    private List<RowContext> restrict(List<RowContext> rows, List<Expression> predicates) {
        if (predicates.isEmpty()) return rows;
        List<RowContext> kept = new ArrayList<>();
        for (RowContext ctx : rows) {
            if (holds(ctx, predicates)) kept.add(ctx);
        }
        return kept;
    }

    private boolean holds(RowContext ctx, List<Expression> predicates) {
        for (Expression pred : predicates) {
            try {
                if (!executor.isTruthy(executor.evalExpr(pred, ctx))) return false;
            } catch (Exception e) {
                // Not decidable here; the query's own WHERE decides it over the finished row.
            }
        }
        return true;
    }

    /** Whether every column a conjunct names is one this row has. */
    private static boolean resolvesAgainst(Expression pred, RowContext ctx) {
        return FromResolver.canEvaluatePredicate(pred, ctx);
    }
}
