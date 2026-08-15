package com.memgres.engine;

import com.memgres.engine.parser.ast.CreateTableAsStmt;
import com.memgres.engine.parser.ast.DeclareCursorStmt;
import com.memgres.engine.parser.ast.DeleteStmt;
import com.memgres.engine.parser.ast.ExplainStmt;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.InsertStmt;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.MergeStmt;
import com.memgres.engine.parser.ast.SelectStmt;
import com.memgres.engine.parser.ast.SetOpStmt;
import com.memgres.engine.parser.ast.UpdateStmt;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * The DEFAULT keyword written where it does not ask a column for its default.
 *
 * <p>The keyword is not a value. It may only be the whole of a value an INSERT supplies for a
 * column or the whole of what an assignment writes; parentheses around it change nothing, and one
 * element of a multi-column assignment counts as the whole of what that element writes. Anywhere
 * else PostgreSQL refuses the statement while it reads it, before a single row is fetched.
 *
 * <p>Which keyword the complaint names is a question of the order the statement is read in, and
 * that order is not the order the text spells: a query's WITH items and FROM items are read before
 * anything it selects, and an UPDATE's WHERE and RETURNING before the values it assigns. So
 * {@code UPDATE t SET a = DEFAULT || 'x' WHERE b = DEFAULT} is reported against the keyword in the
 * WHERE clause, the second one in the text. Within one clause the reading is left to right, which
 * is the order the text has.
 */
final class MisplacedDefault {

    private MisplacedDefault() {}

    /** Whether a node is the keyword rather than a value. */
    static boolean isKeyword(Object node) {
        return node instanceof Literal
                && ((Literal) node).literalType() == Literal.LiteralType.DEFAULT;
    }

    /** Refuse the keyword if it stands anywhere in this part of a statement. */
    static void reject(Object part, int textOffset) {
        if (part == null) return;
        if (AstWalk.findFirst(part, node -> isKeyword(node)) == null) return;
        Set<Object> standing = standingPlaces(part);
        Literal anywhere = anywhere(part, standing);
        if (anywhere == null) return;
        Literal inReadingOrder = first(part, standing);
        throw error(inReadingOrder != null ? inReadingOrder : anywhere, textOffset);
    }

    /** The places the keyword may stand, gathered by identity rather than by what they spell. */
    static Set<Object> standingPlaces(Object root) {
        final Set<Object> standing =
                Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
        AstWalk.forEach(root, node -> collectStanding(node, standing));
        return standing;
    }

    private static void collectStanding(Object node, Set<Object> standing) {
        if (node instanceof InsertStmt.SetClause) {
            Expression value = ((InsertStmt.SetClause) node).value();
            if (value != null) standing.add(value);
        } else if (node instanceof InsertStmt) {
            List<List<Expression>> rows = ((InsertStmt) node).values();
            if (rows != null) {
                for (List<Expression> row : rows) standing.addAll(row);
            }
        } else if (node instanceof MergeStmt.WhenNotMatched) {
            List<Expression> values = ((MergeStmt.WhenNotMatched) node).values();
            if (values != null) standing.addAll(values);
        }
    }

    /**
     * The misplaced keyword standing earliest in the text, wherever in the statement it stands.
     *
     * <p>This is what answers for a shape {@link #first} does not know the reading order of, so
     * that a keyword is never let through for want of somewhere to report it.
     */
    static Literal anywhere(Object root, final Set<Object> standing) {
        final Literal[] earliest = new Literal[1];
        AstWalk.forEach(root, node -> {
            if (!isKeyword(node) || standing.contains(node)) return;
            Literal keyword = (Literal) node;
            if (earliest[0] == null || writtenAt(keyword) < writtenAt(earliest[0])) {
                earliest[0] = keyword;
            }
        });
        return earliest[0];
    }

    /** The misplaced keyword PostgreSQL reaches first, or null when this part holds none. */
    static Literal first(Object node, Set<Object> standing) {
        if (node == null) return null;
        if (node instanceof List) {
            for (Object item : (List<?>) node) {
                Literal found = first(item, standing);
                if (found != null) return found;
            }
            return null;
        }
        Object[] clauses = clausesInReadingOrder(node);
        if (clauses == null) return anywhere(node, standing);
        for (int i = 0; i < clauses.length; i++) {
            Literal found = first(clauses[i], standing);
            if (found != null) return found;
        }
        // A part of the statement whose place in the reading is not written down here still holds
        // a keyword that has to be refused, so the text decides between whatever is left.
        return anywhere(node, standing);
    }

    /** The complaint, pointing at the keyword the client wrote. */
    static MemgresException error(Literal keyword, int textOffset) {
        MemgresException misplaced =
                new MemgresException("DEFAULT is not allowed in this context", "42601");
        if (keyword != null && keyword.offset() >= 0) {
            misplaced.setPosition(keyword.offset() + textOffset + 1);
        } else {
            // A node that was not read from the statement's own text has nowhere of its own, so
            // the word is named instead and the protocol layer looks for it.
            misplaced.setPositionToken("DEFAULT");
        }
        return misplaced;
    }

    /** Where the keyword was written, with one the parser did not read sorting last. */
    private static int writtenAt(Literal keyword) {
        return keyword.offset() < 0 ? Integer.MAX_VALUE : keyword.offset();
    }

    /**
     * A statement's parts in the order PostgreSQL reads them, or null for a node whose shape is
     * not written down here — for which the text is what decides.
     */
    private static Object[] clausesInReadingOrder(Object node) {
        if (node instanceof SelectStmt) {
            SelectStmt sel = (SelectStmt) node;
            return new Object[]{sel.withClauses, sel.from, sel.targets, sel.where, sel.having,
                    sel.windowDefs, sel.orderBy, sel.groupBy, sel.groupingSets, sel.distinctOn,
                    sel.limit, sel.offset};
        }
        if (node instanceof SetOpStmt) {
            SetOpStmt set = (SetOpStmt) node;
            return new Object[]{set.left, set.right, set.orderBy, set.limit, set.offset};
        }
        if (node instanceof InsertStmt) {
            InsertStmt ins = (InsertStmt) node;
            return new Object[]{ins.withClauses, ins.values, ins.selectStmt, ins.onConflict,
                    ins.returning};
        }
        if (node instanceof UpdateStmt) {
            UpdateStmt upd = (UpdateStmt) node;
            return new Object[]{upd.withClauses, upd.from, upd.where, upd.returning,
                    upd.setClauses};
        }
        if (node instanceof DeleteStmt) {
            DeleteStmt del = (DeleteStmt) node;
            return new Object[]{del.withClauses, del.using, del.where, del.returning};
        }
        if (node instanceof MergeStmt) {
            MergeStmt merge = (MergeStmt) node;
            return new Object[]{merge.withClauses, merge.source, merge.onCondition,
                    merge.whenClauses, merge.returning};
        }
        if (node instanceof SelectStmt.CommonTableExpr) {
            return new Object[]{((SelectStmt.CommonTableExpr) node).query};
        }
        if (node instanceof ExplainStmt) {
            return new Object[]{((ExplainStmt) node).statement};
        }
        if (node instanceof DeclareCursorStmt) {
            return new Object[]{((DeclareCursorStmt) node).query()};
        }
        if (node instanceof CreateTableAsStmt) {
            return new Object[]{((CreateTableAsStmt) node).query()};
        }
        return null;
    }
}
