package com.memgres.engine;

import java.util.ArrayList;
import java.util.List;

/**
 * One row of a composite (record) result whose column names come from the function that produced
 * it rather than from a declared type -- what {@code jsonb_each} returns when it is called in the
 * SELECT list instead of the FROM clause.
 *
 * <p>Printed on its own the value takes PG's composite literal form, {@code (a,1)}: fields are
 * separated by commas, a NULL field is empty, and a field is double-quoted when leaving it bare
 * would change where the commas fall.
 */
public final class RecordValue {

    private final List<String> names;
    private final List<Object> values;

    public RecordValue(List<String> names, List<Object> values) {
        this.names = names;
        this.values = values;
    }

    /** A two-field record, the shape every member of the json_each family returns. */
    static RecordValue of(String n1, Object v1, String n2, Object v2) {
        List<String> n = new ArrayList<String>(2);
        n.add(n1);
        n.add(n2);
        List<Object> v = new ArrayList<Object>(2);
        v.add(v1);
        v.add(v2);
        return new RecordValue(n, v);
    }

    public List<String> names() {
        return names;
    }

    public int size() {
        return values.size();
    }

    public Object valueAt(int index) {
        return index >= 0 && index < values.size() ? values.get(index) : null;
    }

    /** The index of the named field, or -1 when the record has no such field. */
    public int indexOf(String name) {
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).equalsIgnoreCase(name)) return i;
        }
        return -1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(',');
            appendField(sb, values.get(i));
        }
        return sb.append(')').toString();
    }

    /** A NULL field prints as nothing at all, which is what tells it apart from an empty string. */
    private static void appendField(StringBuilder sb, Object value) {
        if (value == null) return;
        String text = value.toString();
        if (!needsQuoting(text)) {
            sb.append(text);
            return;
        }
        sb.append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"' || c == '\\') sb.append(c);
            sb.append(c);
        }
        sb.append('"');
    }

    private static boolean needsQuoting(String text) {
        if (text.isEmpty()) return true;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"' || c == '\\' || c == ',' || c == '(' || c == ')' || Character.isWhitespace(c)) {
                return true;
            }
        }
        return false;
    }
}
