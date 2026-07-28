package com.memgres.engine;

import com.memgres.engine.parser.ast.CastExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.SetOpStmt;
import com.memgres.engine.parser.ast.Statement;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The shape PostgreSQL insists a {@code WITH RECURSIVE} item has before it will run it.
 *
 * <p>A recursive WITH item is only recursive because its body names itself, and PostgreSQL can
 * only evaluate that self-reference in one place: a single scan of the rows the previous
 * iteration produced, sitting at the top of the recursive term's FROM clause. Anything that would
 * need the whole result at once — a second scan of it, an aggregate over it, the null-extended
 * side of an outer join, a sub-select — has no meaning while the result is still being built, so
 * PostgreSQL refuses the query (42P19) rather than answer a different question. A few clauses it
 * simply has not implemented for a recursive query at all — ORDER BY, LIMIT, OFFSET, FOR UPDATE —
 * are 0A000, and a recursive term whose column type differs from the non-recursive term's is
 * 42804.
 *
 * <p>Every rule here fires only on a WITH item that both declares RECURSIVE and actually names
 * itself; ordinary recursion — one self-reference in the FROM clause, joined to anything, under
 * WHERE, GROUP BY, DISTINCT, a window function, or an aggregate in the <em>non</em>-recursive
 * term — is left alone.
 */
final class RecursiveCteCheck {

    private RecursiveCteCheck() { }

    /** True when a WITH item's body names the item itself, so it really does recurse. */
    static boolean selfReferencing(SelectStmt.CommonTableExpr cte) {
        return !selfReferences(cte.query(), cte.name()).isEmpty();
    }

    /**
     * Applies every structural rule to a WITH item that declares RECURSIVE and names itself.
     * Called before the fixed-point loop starts, so a refused query produces no rows at all.
     */
    static void validate(SelectExecutor select, SelectStmt.CommonTableExpr cte) {
        String name = cte.name();

        // The only shape PG can evaluate is "non-recursive term UNION [ALL] recursive term".
        if (!(cte.query() instanceof SetOpStmt)
                || ((SetOpStmt) cte.query()).op() != SetOpStmt.SetOpType.UNION) {
            throw new MemgresException("recursive query \"" + name
                    + "\" does not have the form non-recursive-term UNION [ALL] recursive-term",
                    "42P19");
        }
        SetOpStmt setOp = (SetOpStmt) cte.query();

        // The seed has to be computable without the result it seeds.
        if (!selfReferences(setOp.left(), name).isEmpty()) {
            throw new MemgresException("recursive reference to query \"" + name
                    + "\" must not appear within its non-recursive term", "42P19");
        }

        List<Boolean> refs = selfReferences(setOp.right(), name);
        if (refs.isEmpty()) return;
        if (refs.size() > 1) {
            throw new MemgresException("recursive reference to query \"" + name
                    + "\" must not appear more than once", "42P19");
        }
        if (refs.get(0)) {
            throw new MemgresException("recursive reference to query \"" + name
                    + "\" must not appear within a subquery", "42P19");
        }
        if (refUnderOuterJoin(setOp.right(), name)) {
            throw new MemgresException("recursive reference to query \"" + name
                    + "\" must not appear within an outer join", "42P19");
        }

        // Clauses PG has not implemented for a recursive query. These sit on the set operation
        // itself; the same words inside a parenthesised arm or a subquery belong to that arm and
        // are allowed, which is why only the top node is looked at.
        if (setOp.orderBy() != null && !setOp.orderBy().isEmpty()) {
            throw new MemgresException("ORDER BY in a recursive query is not implemented", "0A000");
        }
        if (setOp.offset() != null) {
            throw new MemgresException("OFFSET in a recursive query is not implemented", "0A000");
        }
        if (setOp.limit() != null) {
            throw new MemgresException("LIMIT in a recursive query is not implemented", "0A000");
        }
        if (hasLockClause(setOp.left()) || hasLockClause(setOp.right())) {
            throw new MemgresException("FOR UPDATE/SHARE in a recursive query is not implemented",
                    "0A000");
        }

        // An aggregate reads the whole recursive term at once, which is exactly what the
        // iteration cannot supply. Only this query level counts: an aggregate inside a FROM
        // subquery or a scalar sub-select of the recursive term is over that query, not this one.
        if (recursiveTermHasAggregate(select, setOp.right())) {
            throw new MemgresException(
                    "aggregate functions are not allowed in a recursive query's recursive term",
                    "42P19");
        }
    }

    /**
     * The column types the two terms agree on must be the non-recursive term's own types: PG
     * resolves the union's output type from both arms and then insists the seed already had it.
     * Only an explicit cast in the recursive term is read here, because that is the one spelling
     * that states a different type outright.
     */
    static void checkColumnTypes(String cteName, List<Column> baseColumns, Statement recursiveTerm) {
        if (!(recursiveTerm instanceof SelectStmt)) return;
        List<SelectStmt.SelectTarget> targets = ((SelectStmt) recursiveTerm).targets();
        if (targets == null) return;
        int n = Math.min(baseColumns.size(), targets.size());
        for (int i = 0; i < n; i++) {
            DataType baseType = baseColumns.get(i).getType();
            int baseRank = numericRank(baseType);
            if (baseRank == 0) continue;
            Expression expr = targets.get(i).expr();
            if (!(expr instanceof CastExpr)) continue;
            String castName = ((CastExpr) expr).typeName();
            int castRank = numericRank(castName);
            if (castRank > baseRank) {
                throw new MemgresException("recursive query \"" + cteName + "\" column " + (i + 1)
                        + " has type " + displayName(baseType) + " in non-recursive term but type "
                        + displayName(castRank) + " overall", "42804");
            }
        }
    }

    // ---- self reference discovery ----

    /**
     * One entry per unqualified FROM reference to {@code name} inside {@code node}; the value
     * says whether the reference sits inside a sub-select of an expression (EXISTS, IN, a scalar
     * subquery) rather than in a FROM clause. A schema-qualified name is never a WITH item, so it
     * is not counted.
     */
    private static List<Boolean> selfReferences(Object node, String name) {
        List<Boolean> found = new ArrayList<>();
        String lcName = name.toLowerCase();
        Deque<Object> nodes = new ArrayDeque<>();
        Deque<Boolean> viaExpr = new ArrayDeque<>();
        Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        nodes.push(node);
        viaExpr.push(Boolean.FALSE);
        while (!nodes.isEmpty()) {
            Object cur = nodes.pop();
            boolean inExpr = viaExpr.pop();
            if (cur == null || !seen.add(cur)) continue;
            if (cur instanceof SelectStmt.TableRef) {
                SelectStmt.TableRef ref = (SelectStmt.TableRef) cur;
                if (ref.schema() == null && ref.table() != null
                        && ref.table().toLowerCase().equals(lcName)) {
                    found.add(inExpr);
                }
                continue;
            }
            if (cur instanceof String || cur instanceof Number || cur instanceof Boolean
                    || cur instanceof Character || cur instanceof Enum) continue;
            // Once inside an expression, everything below it is inside a sub-select of that
            // expression — the FROM-clause subqueries PG allows are reached through FromItems.
            boolean childInExpr = inExpr || cur instanceof Expression;
            if (cur instanceof Collection) {
                for (Object item : (Collection<?>) cur) { nodes.push(item); viaExpr.push(childInExpr); }
                continue;
            }
            if (cur instanceof Map) {
                for (Object item : ((Map<?, ?>) cur).values()) { nodes.push(item); viaExpr.push(childInExpr); }
                continue;
            }
            Class<?> cls = cur.getClass();
            Package pkg = cls.getPackage();
            if (pkg == null || !pkg.getName().startsWith("com.memgres")) continue;
            for (java.lang.reflect.Field f : cls.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(f.getModifiers())) continue;
                try {
                    f.setAccessible(true);
                    Object v = f.get(cur);
                    if (v != null) { nodes.push(v); viaExpr.push(childInExpr); }
                } catch (IllegalAccessException | RuntimeException ignored) { }
            }
        }
        return found;
    }

    /** True when the self-reference sits on a side of a join that the join may null-extend. */
    private static boolean refUnderOuterJoin(Object node, String name) {
        for (Object join : collect(node, SelectStmt.JoinFrom.class)) {
            SelectStmt.JoinFrom j = (SelectStmt.JoinFrom) join;
            SelectStmt.JoinType type = j.joinType();
            boolean rightNullable = type == SelectStmt.JoinType.LEFT
                    || type == SelectStmt.JoinType.NATURAL_LEFT
                    || type == SelectStmt.JoinType.FULL
                    || type == SelectStmt.JoinType.NATURAL_FULL;
            boolean leftNullable = type == SelectStmt.JoinType.RIGHT
                    || type == SelectStmt.JoinType.NATURAL_RIGHT
                    || type == SelectStmt.JoinType.FULL
                    || type == SelectStmt.JoinType.NATURAL_FULL;
            if (rightNullable && !selfReferences(j.right(), name).isEmpty()) return true;
            if (leftNullable && !selfReferences(j.left(), name).isEmpty()) return true;
        }
        return false;
    }

    /** Every node of the given class reachable from {@code node}. */
    private static List<Object> collect(Object node, Class<?> wanted) {
        List<Object> found = new ArrayList<>();
        Deque<Object> stack = new ArrayDeque<>();
        Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        stack.push(node);
        while (!stack.isEmpty()) {
            Object cur = stack.pop();
            if (cur == null || !seen.add(cur)) continue;
            if (wanted.isInstance(cur)) found.add(cur);
            if (cur instanceof String || cur instanceof Number || cur instanceof Boolean
                    || cur instanceof Character || cur instanceof Enum) continue;
            if (cur instanceof Collection) {
                for (Object item : (Collection<?>) cur) stack.push(item);
                continue;
            }
            if (cur instanceof Map) {
                for (Object item : ((Map<?, ?>) cur).values()) stack.push(item);
                continue;
            }
            Class<?> cls = cur.getClass();
            Package pkg = cls.getPackage();
            if (pkg == null || !pkg.getName().startsWith("com.memgres")) continue;
            for (java.lang.reflect.Field f : cls.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(f.getModifiers())) continue;
                try {
                    f.setAccessible(true);
                    Object v = f.get(cur);
                    if (v != null) stack.push(v);
                } catch (IllegalAccessException | RuntimeException ignored) { }
            }
        }
        return found;
    }

    // ---- clause inspection ----

    private static boolean hasLockClause(Statement stmt) {
        return stmt instanceof SelectStmt && ((SelectStmt) stmt).lockClause() != null;
    }

    /** An aggregate written at the recursive term's own query level. */
    private static boolean recursiveTermHasAggregate(SelectExecutor select, Statement stmt) {
        if (!(stmt instanceof SelectStmt)) return false;
        SelectStmt sel = (SelectStmt) stmt;
        if (sel.targets() != null) {
            for (SelectStmt.SelectTarget target : sel.targets()) {
                if (target.expr() != null && select.containsAggregate(target.expr())) return true;
            }
        }
        if (sel.where() != null && select.containsAggregate(sel.where())) return true;
        if (sel.having() != null && select.containsAggregate(sel.having())) return true;
        if (sel.groupBy() != null) {
            for (Expression e : sel.groupBy()) {
                if (e != null && select.containsAggregate(e)) return true;
            }
        }
        return false;
    }

    // ---- numeric widening ----

    private static int numericRank(DataType type) {
        if (type == null) return 0;
        switch (type) {
            case SMALLINT: case SMALLSERIAL: return 1;
            case INTEGER: case SERIAL: return 2;
            case BIGINT: case BIGSERIAL: return 3;
            case NUMERIC: return 4;
            case REAL: return 5;
            case DOUBLE_PRECISION: return 6;
            default: return 0;
        }
    }

    private static int numericRank(String typeName) {
        if (typeName == null) return 0;
        String t = typeName.trim().toLowerCase();
        int paren = t.indexOf('(');
        if (paren >= 0) t = t.substring(0, paren).trim();
        if (t.equals("smallint") || t.equals("int2")) return 1;
        if (t.equals("integer") || t.equals("int") || t.equals("int4")) return 2;
        if (t.equals("bigint") || t.equals("int8")) return 3;
        if (t.equals("numeric") || t.equals("decimal")) return 4;
        if (t.equals("real") || t.equals("float4")) return 5;
        if (t.equals("double precision") || t.equals("float8")) return 6;
        return 0;
    }

    private static String displayName(DataType type) {
        return displayName(numericRank(type));
    }

    private static String displayName(int rank) {
        switch (rank) {
            case 1: return "smallint";
            case 2: return "integer";
            case 3: return "bigint";
            case 4: return "numeric";
            case 5: return "real";
            case 6: return "double precision";
            default: return "unknown";
        }
    }
}
