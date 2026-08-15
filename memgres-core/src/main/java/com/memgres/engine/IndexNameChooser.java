package com.memgres.engine;

import com.memgres.engine.parser.Parser;
import com.memgres.engine.parser.ast.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The name PostgreSQL gives an index nobody named, and the name it gives a partition's copy of an
 * index declared on the partitioned table above it. Both follow one rule -- the relation, what
 * each indexed column is worth as a name, and {@code idx}, numbered when a relation of that name
 * already lives in the schema -- so both ask here.
 *
 * <p>The name is not only for the catalogue: a unique index reports it as the constraint a
 * duplicate key violates, and a partition reports the name of its own copy rather than the
 * partitioned table's.
 */
final class IndexNameChooser {

    /**
     * A relation name lives in a fixed 64-byte field, so PostgreSQL composes a derived name to fit
     * rather than cutting the finished name off at the end: a long relation and a long column each
     * give up characters until the whole fits, which is why the two end up sharing the room evenly
     * instead of the columns disappearing.
     */
    private static final int NAME_DATA_LEN = 64;

    private IndexNameChooser() {}

    /**
     * The name for an index on {@code relationName} reading {@code keys}, free in {@code
     * schemaName}. {@code relation} is the relation being indexed, consulted so that a key naming
     * a column is recognised as one; null when the target is something this cannot open, in which
     * case the key is read as an expression.
     */
    static String choose(Database database, String schemaName, String relationName, Table relation,
                         List<String> keys, List<String> includeColumns) {
        String columns = join(columnNames(relation, keys, includeColumns));
        String label = "idx";
        for (int pass = 1; ; pass++) {
            String candidate = compose(relationName, columns, label);
            if (RelationNamespace.kindOf(database, schemaName, candidate) == null) return candidate;
            label = "idx" + pass;
        }
    }

    /**
     * What each indexed column contributes, in order. A name repeated inside one index is numbered
     * from 1 on its second occurrence, so an index over (a, a) is named a_a1 and one over three
     * expressions expr_expr1_expr2. The columns an INCLUDE clause carries take part in the name
     * exactly as the key columns do.
     */
    private static List<String> columnNames(Table relation, List<String> keys,
                                            List<String> includeColumns) {
        List<String> all = new ArrayList<String>();
        if (keys != null) all.addAll(keys);
        if (includeColumns != null) all.addAll(includeColumns);
        List<String> chosen = new ArrayList<String>();
        for (String key : all) {
            String origin = columnName(relation, key);
            String candidate = origin;
            for (int n = 1; chosen.contains(candidate); n++) candidate = origin + n;
            chosen.add(candidate);
        }
        return chosen;
    }

    /**
     * What one index key is worth as a name. A key that names a column is recognised by asking the
     * relation rather than by reading the text, because a quoted name may be anything at all --
     * {@code "with space"} is one column, not two words.
     */
    private static String columnName(Table relation, String key) {
        if (key == null) return "expr";
        String text = key.trim();
        if (relation != null && relation.getColumnIndex(text) >= 0) return text;
        Expression expr;
        try {
            expr = Parser.parseExpression(text);
        } catch (RuntimeException e) {
            return "expr";
        }
        String[] found = new String[1];
        return figureName(expr, found) == 0 || found[0] == null ? "expr" : found[0];
    }

    /**
     * The name an expression carries, and how firmly it carries it. A column or a call names
     * itself outright; a cast or a CASE has a name only where nothing better was found underneath,
     * and PostgreSQL keeps the two apart so that a CASE whose ELSE is a cast is still called
     * {@code case} while a CASE whose ELSE calls a function is called after that function. Zero
     * means the expression has no name of its own, which is the answer PostgreSQL writes as
     * {@code expr}.
     */
    private static int figureName(Expression expr, String[] name) {
        if (expr == null) return 0;
        if (expr instanceof ColumnRef) {
            name[0] = ((ColumnRef) expr).column();
            return 2;
        }
        if (expr instanceof FunctionCallExpr) {
            name[0] = lastPart(((FunctionCallExpr) expr).name());
            return 2;
        }
        if (expr instanceof ArrayExpr) {
            name[0] = ((ArrayExpr) expr).isRow() ? "row" : "array";
            return 2;
        }
        if (expr instanceof FieldAccessExpr) {
            name[0] = ((FieldAccessExpr) expr).field();
            return 2;
        }
        if (expr instanceof CollateExpr) {
            return figureName(((CollateExpr) expr).expr(), name);
        }
        if (expr instanceof SubscriptExpr) {
            return figureName(((SubscriptExpr) expr).base(), name);
        }
        if (expr instanceof CastExpr) {
            int strength = figureName(((CastExpr) expr).expr(), name);
            if (strength > 1) return strength;
            String type = castTypeName(((CastExpr) expr).typeName());
            if (type == null) return strength;
            name[0] = type;
            return 1;
        }
        if (expr instanceof CaseExpr) {
            int strength = figureName(((CaseExpr) expr).elseExpr(), name);
            if (strength > 1) return strength;
            name[0] = "case";
            return 1;
        }
        return 0;
    }

    /**
     * The name the catalogue holds a type under, from the way the cast wrote it. PostgreSQL's
     * grammar rewrites the spellings the standard prescribes into its own names long before an
     * index name is derived, so a cast to bigint contributes int8 and one to character varying
     * contributes varchar. A length or precision is no part of the name, and neither is the schema.
     */
    private static String castTypeName(String typeName) {
        if (typeName == null) return null;
        String text = typeName.trim().toLowerCase();
        int paren = text.indexOf('(');
        if (paren >= 0) text = text.substring(0, paren).trim();
        int dot = text.lastIndexOf('.');
        if (dot >= 0) text = text.substring(dot + 1).trim();
        text = text.replaceAll("\\s+", " ");
        if (text.isEmpty()) return null;
        if (text.equals("int") || text.equals("integer")) return "int4";
        if (text.equals("bigint")) return "int8";
        if (text.equals("smallint")) return "int2";
        if (text.equals("real")) return "float4";
        if (text.equals("float") || text.equals("double precision")) return "float8";
        if (text.equals("decimal")) return "numeric";
        if (text.equals("boolean")) return "bool";
        if (text.equals("char") || text.equals("character")) return "bpchar";
        if (text.equals("character varying")) return "varchar";
        return text;
    }

    /** A qualified name is worth its last part, the way a function is named after itself. */
    private static String lastPart(String name) {
        if (name == null) return null;
        int dot = name.lastIndexOf('.');
        return dot >= 0 ? name.substring(dot + 1) : name;
    }

    private static String join(List<String> parts) {
        StringBuilder out = new StringBuilder();
        for (String part : parts) {
            if (out.length() > 0) out.append('_');
            out.append(part);
        }
        return out.toString();
    }

    /**
     * Relation, columns and label joined by underscores, cut back to fit a relation name. Room is
     * taken from whichever of the relation and the columns is currently longer, one character at a
     * time, so both survive in a name that has to lose something.
     */
    private static String compose(String name1, String name2, String label) {
        int overhead = 0;
        int name1len = byteLength(name1);
        int name2len = 0;
        if (name2 != null) {
            name2len = byteLength(name2);
            overhead++;
        }
        if (label != null) overhead += byteLength(label) + 1;
        int available = NAME_DATA_LEN - 1 - overhead;
        while (name1len + name2len > available && (name1len > 0 || name2len > 0)) {
            if (name1len > name2len) name1len--;
            else name2len--;
        }
        StringBuilder out = new StringBuilder(clip(name1, name1len));
        if (name2 != null) out.append('_').append(clip(name2, name2len));
        if (label != null) out.append('_').append(label);
        return out.toString();
    }

    private static int byteLength(String text) {
        return text == null ? 0 : text.getBytes(StandardCharsets.UTF_8).length;
    }

    /** The longest prefix of {@code text} fitting in {@code maxBytes}, never splitting a character. */
    private static String clip(String text, int maxBytes) {
        if (text == null) return "";
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= maxBytes) return text;
        if (maxBytes <= 0) return "";
        int end = maxBytes;
        while (end > 0 && (bytes[end] & 0xC0) == 0x80) end--;
        return new String(bytes, 0, end, StandardCharsets.UTF_8);
    }
}
