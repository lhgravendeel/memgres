package com.memgres.engine;

import com.memgres.engine.parser.Parser;
import com.memgres.engine.parser.ast.AnyAllArrayExpr;
import com.memgres.engine.parser.ast.ArrayExpr;
import com.memgres.engine.parser.ast.BinaryExpr;
import com.memgres.engine.parser.ast.CaseExpr;
import com.memgres.engine.parser.ast.CastExpr;
import com.memgres.engine.parser.ast.CollateExpr;
import com.memgres.engine.parser.ast.ColumnRef;
import com.memgres.engine.parser.ast.CustomOperatorExpr;
import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;
import com.memgres.engine.parser.ast.InExpr;
import com.memgres.engine.parser.ast.IsBooleanExpr;
import com.memgres.engine.parser.ast.IsNullExpr;
import com.memgres.engine.parser.ast.Literal;
import com.memgres.engine.parser.ast.SubscriptExpr;
import com.memgres.engine.parser.ast.UnaryExpr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The key a duplicate-key error names, as PostgreSQL writes it: <em>Key (a, b)=(1, 2)</em>.
 *
 * <p>Two errors carry that key and they are different sentences. A write refused because the key
 * is in the index already ends <em>already exists.</em>; a unique index that could not be built
 * over the rows the table already held ends <em>is duplicated.</em>, because nothing was written
 * there and what is being reported is the state the table was found in.
 *
 * <p>The list holds the index's key columns and nothing else: an INCLUDE column, a COLLATE clause,
 * an operator class and a sort direction are all left out, none of them being part of what the two
 * rows collided on. PostgreSQL writes the keys with its pretty-printer rather than with the
 * deparser {@code pg_get_indexdef} uses, and the two disagree about parentheses -- the pretty
 * printer leaves out every pair the expression's own operator precedence already implies -- so an
 * index over {@code (a+1)} is named {@code (a + 1)} here where its definition reads
 * {@code ((a + 1))}.
 */
final class IndexKeyDescription {

    private IndexKeyDescription() {
    }

    /** The DETAIL of a write refused because the key is in the index already. */
    static String alreadyExists(Table table, List<String> keys, Object[] values) {
        return detail(table, keys, values, " already exists.");
    }

    /** The DETAIL of a unique index that rows already in the table kept from being built. */
    static String duplicated(Table table, List<String> keys, Object[] values) {
        return detail(table, keys, values, " is duplicated.");
    }

    private static String detail(Table table, List<String> keys, Object[] values, String ending) {
        StringBuilder sb = new StringBuilder("Key (");
        sb.append(keyList(table, keys));
        sb.append(")=(");
        for (int i = 0; values != null && i < values.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(ErrorValueText.of(values[i]));
        }
        return sb.append(')').append(ending).toString();
    }

    /** The key columns, comma separated: what stands between {@code Key (} and {@code )}. */
    static String keyList(Table table, List<String> keys) {
        RuleDeparser.ColumnTypes types = RuleDeparser.forTable(table);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; keys != null && i < keys.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(element(table, types, keys.get(i)));
        }
        return sb.toString();
    }

    /**
     * One key. A key that names a column is recognised by asking the relation rather than by
     * reading the text, because a quoted name may be anything at all, and it is written back with
     * whatever quoting it needs; anything else is an expression, and is deparsed.
     */
    private static String element(Table table, RuleDeparser.ColumnTypes types, String key) {
        if (key == null) return "";
        String text = key.trim();
        String column = columnNamed(table, text);
        if (column != null) return column;
        Expression expr;
        try {
            expr = Parser.parseExpression(text);
        } catch (RuntimeException e) {
            return text;
        }
        // A collation is a property of the comparison, not of the key, so it is left out here even
        // when it was written into the key itself.
        while (expr instanceof CollateExpr) expr = ((CollateExpr) expr).expr();
        if (expr instanceof ColumnRef && ((ColumnRef) expr).table() == null) {
            column = columnNamed(table, ((ColumnRef) expr).column());
            if (column != null) return column;
        }
        String plain = RuleDeparser.deparse(expr, types);
        // Whether the key needs parentheses around it is settled by what it is rather than by how
        // it prints: PostgreSQL writes a bare function call as it stands and wraps everything else.
        boolean wrapped = !RuleDeparser.deparseIndexElement(expr, types).equals(plain);
        String body = plain;
        try {
            body = pretty(Parser.parseExpression(plain), types);
        } catch (RuntimeException e) {
            // Nothing here can be left out safely, so the fully parenthesised form stands.
        }
        return wrapped ? "(" + body + ")" : body;
    }

    /** The relation's own spelling of a column of this name, quoted as it needs, or null. */
    private static String columnNamed(Table table, String name) {
        if (table == null || name == null) return null;
        int idx = table.getColumnIndex(name);
        return idx < 0 ? null : RuleDeparser.quoteIdentifier(table.getColumns().get(idx).getName());
    }

    /** Raised when an expression holds something no parenthesis can safely be taken out of. */
    private static final class Unsupported extends RuntimeException {
        Unsupported() {
            super(null, null, false, false);
        }
    }

    /**
     * The expression without the parentheses PostgreSQL's pretty-printer leaves out.
     *
     * <p>The work is done over the deparsed text rather than by writing the expression out again,
     * so that every literal, function name and CASE line stays exactly as the deparser wrote it and
     * only parentheses move. A node whose text cannot be lined up with its children is refused
     * outright, which leaves the caller with the fully parenthesised form -- one pair too many, but
     * never a mangled expression.
     */
    private static String pretty(Expression e, RuleDeparser.ColumnTypes types) {
        String plain = RuleDeparser.deparse(e, types);
        if (e instanceof ColumnRef || e instanceof Literal) return plain;
        if (e instanceof CastExpr) {
            // A constant prints its own type label, with no parentheses to take out.
            if (plain.isEmpty() || plain.charAt(0) != '(') return plain;
            int close = matchingClose(plain, 0);
            if (close < 0) throw new Unsupported();
            Expression arg = ((CastExpr) e).expr();
            if (!RuleDeparser.deparse(arg, types).equals(plain.substring(1, close))) {
                throw new Unsupported();
            }
            return operand(arg, types, e, 0) + plain.substring(close + 1);
        }
        List<Expression> children = children(e);
        String text = plain;
        // A node PostgreSQL parenthesises as a whole is recognised by its own text: the deparser
        // wrote that pair, and dropping it again is the whole business of the pretty-printer.
        if (!plain.isEmpty() && plain.charAt(0) == '('
                && matchingClose(plain, 0) == plain.length() - 1) {
            text = plain.substring(1, plain.length() - 1);
        }
        StringBuilder out = new StringBuilder();
        int at = 0;
        for (int i = 0; i < children.size(); i++) {
            Expression child = children.get(i);
            String childText = RuleDeparser.deparse(child, types);
            int found = text.indexOf(childText, at);
            if (found < 0) throw new Unsupported();
            out.append(text, at, found);
            out.append(operand(child, types, e, i));
            at = found + childText.length();
        }
        return out.append(text, at, text.length()).toString();
    }

    /** One child, with the parentheses its parent makes necessary and no others. */
    private static String operand(Expression child, RuleDeparser.ColumnTypes types,
                                  Expression parent, int index) {
        String body = pretty(child, types);
        return simple(child, parent, index, types) ? body : "(" + body + ")";
    }

    /**
     * Whether the child reads correctly under its parent without parentheses of its own.
     *
     * <p>PostgreSQL does not weigh full operator precedence here. It knows only that {@code *},
     * {@code /} and {@code %} bind tighter than {@code +} and {@code -}, that an operator under one
     * of its own precedence keeps its parentheses unless it is the left operand, and that anything
     * else -- a comparison, a concatenation, an exponentiation -- keeps them. That is why
     * {@code (a + b) = c} prints its parentheses while {@code a % b + c} does not.
     */
    private static boolean simple(Expression child, Expression parent, int index,
                                  RuleDeparser.ColumnTypes types) {
        // A parent that keeps its parts apart with a name and brackets of its own writes each of
        // them as it stands: a function's arguments, an array's elements, CASE's branches.
        if (writesChildBare(parent, index)) return true;
        if (child instanceof ColumnRef || child instanceof Literal) return true;
        if (child instanceof FunctionCallExpr || child instanceof ArrayExpr
                || child instanceof CaseExpr || child instanceof SubscriptExpr) {
            return true;
        }
        if (child instanceof CastExpr) {
            CastExpr cast = (CastExpr) child;
            // A cast a function carries out is a call like any other and needs no parentheses. One
            // PostgreSQL performs by writing the value out as text and reading it back in is not a
            // call, and reads exactly as simply as the value it casts.
            if (!castReadsThroughText(cast, types)) return true;
            return simple(cast.expr(), cast, 0, types);
        }
        int childClass = operatorClass(child);
        int parentClass = operatorClass(parent);
        if (childClass >= 0 && parentClass >= 0) {
            if (childClass == ADDITIVE) return parentClass == ADDITIVE && index == 0;
            if (childClass == MULTIPLICATIVE) {
                return parentClass == ADDITIVE || (parentClass == MULTIPLICATIVE && index == 0);
            }
            return false;
        }
        if (!operatorLike(child)) return false;
        // An operator with one operand, or a test such as IS NULL, keeps its parentheses under any
        // operator at all; under AND, OR or NOT it does not need them.
        return parentClass < 0 && booleanConnective(parent);
    }

    /** True for the nodes PostgreSQL writes out with no parenthesis decision to make about them. */
    private static boolean writesChildBare(Expression parent, int index) {
        if (parent instanceof FunctionCallExpr || parent instanceof CaseExpr
                || parent instanceof ArrayExpr || parent instanceof SubscriptExpr) {
            return true;
        }
        // The left-hand side is an operand like any other; the array it is compared against sits
        // inside parentheses of the construct's own.
        if (parent instanceof AnyAllArrayExpr || parent instanceof InExpr) return index > 0;
        return false;
    }

    private static boolean operatorLike(Expression e) {
        if (e instanceof BinaryExpr) return !booleanConnective(e);
        if (e instanceof UnaryExpr) return ((UnaryExpr) e).op() != UnaryExpr.UnaryOp.NOT;
        return e instanceof CustomOperatorExpr || e instanceof IsNullExpr
                || e instanceof IsBooleanExpr;
    }

    private static boolean booleanConnective(Expression e) {
        if (e instanceof BinaryExpr) {
            BinaryExpr b = (BinaryExpr) e;
            return b.op() == BinaryExpr.BinOp.AND || b.op() == BinaryExpr.BinOp.OR;
        }
        return e instanceof UnaryExpr && ((UnaryExpr) e).op() == UnaryExpr.UnaryOp.NOT;
    }

    private static final int OTHER_OPERATOR = 0;
    private static final int ADDITIVE = 1;
    private static final int MULTIPLICATIVE = 2;

    /** The precedence class of a two-operand operator node, or -1 when the node is not one. */
    private static int operatorClass(Expression e) {
        if (e instanceof BinaryExpr) {
            switch (((BinaryExpr) e).op()) {
                case AND:
                case OR:
                    return -1;   // a boolean connective, not an operator
                case ADD:
                case SUBTRACT:
                    return ADDITIVE;
                case MULTIPLY:
                case DIVIDE:
                case MODULO:
                    return MULTIPLICATIVE;
                default:
                    return OTHER_OPERATOR;
            }
        }
        if (e instanceof CustomOperatorExpr && ((CustomOperatorExpr) e).left() != null) {
            String symbol = ((CustomOperatorExpr) e).opSymbol();
            if (symbol != null && !symbol.isEmpty()) {
                char first = symbol.charAt(0);
                if (first == '+' || first == '-') return ADDITIVE;
                if (first == '*' || first == '/' || first == '%') return MULTIPLICATIVE;
            }
            return OTHER_OPERATOR;
        }
        return -1;
    }

    /** The types a cast to or from which PostgreSQL performs by writing the value out as text. */
    private static final Set<String> TEXT_FAMILY = new HashSet<String>(Arrays.asList(
            "text", "varchar", "character varying", "char", "character", "bpchar", "name",
            "\"char\"", "unknown"));

    /**
     * Whether the cast goes through the text form of the value. Such a cast is a node of its own in
     * PostgreSQL's tree rather than a function call, which is what decides whether it may be
     * written without parentheses inside a larger expression.
     */
    private static boolean castReadsThroughText(CastExpr cast, RuleDeparser.ColumnTypes types) {
        if (isTextFamily(cast.typeName())) return true;
        RuleDeparser.PgType source = RuleDeparser.typeOf(cast.expr(), types);
        // A type nothing here can resolve is treated as one that does, so that an expression whose
        // shape is unclear keeps the parentheses it was written with.
        if (source == null || source.dt == null) return true;
        return isTextFamily(source.dt.name());
    }

    private static boolean isTextFamily(String typeName) {
        if (typeName == null) return true;
        String name = typeName.trim().toLowerCase(Locale.ROOT);
        int paren = name.indexOf('(');
        if (paren >= 0) name = name.substring(0, paren).trim();
        int dot = name.lastIndexOf('.');
        if (dot >= 0 && !name.endsWith("\"")) name = name.substring(dot + 1).trim();
        name = name.replaceAll("\\s+", " ");
        return TEXT_FAMILY.contains(name);
    }

    /** The subexpressions the deparser wrote out inside this node, in the order it wrote them. */
    private static List<Expression> children(Expression e) {
        List<Expression> out = new ArrayList<Expression>();
        if (e instanceof BinaryExpr) {
            BinaryExpr b = (BinaryExpr) e;
            if (booleanConnective(b)) {
                // A chain of one connective is written flat, so it has to be read flat.
                flatten(b, b.op(), out);
            } else {
                out.add(b.left());
                out.add(b.right());
            }
            return out;
        }
        if (e instanceof UnaryExpr) {
            out.add(((UnaryExpr) e).operand());
            return out;
        }
        if (e instanceof IsNullExpr) {
            out.add(((IsNullExpr) e).expr());
            return out;
        }
        if (e instanceof IsBooleanExpr) {
            out.add(((IsBooleanExpr) e).expr());
            return out;
        }
        if (e instanceof CollateExpr) {
            out.add(((CollateExpr) e).expr());
            return out;
        }
        if (e instanceof CustomOperatorExpr) {
            CustomOperatorExpr c = (CustomOperatorExpr) e;
            if (c.left() != null) out.add(c.left());
            out.add(c.right());
            return out;
        }
        if (e instanceof AnyAllArrayExpr) {
            AnyAllArrayExpr a = (AnyAllArrayExpr) e;
            out.add(a.left());
            out.add(a.array());
            return out;
        }
        if (e instanceof InExpr) {
            InExpr in = (InExpr) e;
            if (in.values() == null) throw new Unsupported();   // IN (SELECT ...) is not a key
            out.add(in.expr());
            out.addAll(in.values());
            return out;
        }
        if (e instanceof ArrayExpr) {
            out.addAll(((ArrayExpr) e).elements());
            return out;
        }
        if (e instanceof FunctionCallExpr) {
            FunctionCallExpr fn = (FunctionCallExpr) e;
            if (!fn.star()) out.addAll(fn.args());
            return out;
        }
        if (e instanceof CaseExpr) {
            CaseExpr c = (CaseExpr) e;
            if (c.operand() != null) out.add(c.operand());
            for (CaseExpr.WhenClause w : c.whenClauses()) {
                out.add(w.condition());
                out.add(w.result());
            }
            if (c.elseExpr() != null) out.add(c.elseExpr());
            return out;
        }
        throw new Unsupported();
    }

    private static void flatten(Expression e, BinaryExpr.BinOp op, List<Expression> out) {
        if (e instanceof BinaryExpr && ((BinaryExpr) e).op() == op) {
            flatten(((BinaryExpr) e).left(), op, out);
            flatten(((BinaryExpr) e).right(), op, out);
        } else {
            out.add(e);
        }
    }

    /** The position of the parenthesis closing the one at {@code open}, or -1. */
    private static int matchingClose(String text, int open) {
        int depth = 0;
        boolean inString = false;
        for (int i = open; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'') {
                inString = !inString;
            } else if (!inString) {
                if (c == '(') {
                    depth++;
                } else if (c == ')' && --depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }
}
