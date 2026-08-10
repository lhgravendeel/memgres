package com.memgres.engine;

import java.util.ArrayList;
import java.util.List;

/**
 * The text form of a composite, read the way PostgreSQL's {@code record_in} reads it.
 *
 * <p>The writer beside it doubles a quote and escapes a backslash. The reader memgres had did
 * neither: it split on commas, stripped one pair of quotes and handed the rest on, so
 * {@code ("a""b",z)} came back as {@code a""b} and a field holding a comma was two fields. Writer
 * and reader are inverse of each other only if both know the same rules, which is why the reading
 * lives here beside nothing else.
 */
final class RecordLiteral {

    /** One field of the literal: its text, and whether that text was written inside quotes. */
    static final class Field {
        final String text;
        final boolean quoted;

        Field(String text, boolean quoted) {
            this.text = text;
            this.quoted = quoted;
        }
    }

    private RecordLiteral() {
    }

    /**
     * The fields of a composite literal. An unquoted field with nothing in it is the SQL null,
     * which is why the quoting has to be reported alongside the text rather than folded into it.
     */
    static List<Field> parse(String literal) {
        String s = literal.trim();
        int p = 0;
        if (p < s.length() && s.charAt(p) == '(') p++;
        int end = s.length();
        while (end > p && Character.isWhitespace(s.charAt(end - 1))) end--;
        if (end > p && s.charAt(end - 1) == ')') end--;

        List<Field> fields = new ArrayList<Field>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        boolean inQuotes = false;
        int kept = 0;
        boolean leadingSpace = true;
        while (p < end) {
            char c = s.charAt(p);
            if (inQuotes) {
                if (c == '"') {
                    // A doubled quote inside the quotes is one quote; a single one closes them.
                    if (p + 1 < end && s.charAt(p + 1) == '"') {
                        current.append('"');
                        p += 2;
                    } else {
                        inQuotes = false;
                        p++;
                    }
                    kept = current.length();
                } else if (c == '\\' && p + 1 < end) {
                    current.append(s.charAt(p + 1));
                    p += 2;
                    kept = current.length();
                } else {
                    current.append(c);
                    p++;
                    kept = current.length();
                }
                continue;
            }
            if (c == '"') {
                inQuotes = true;
                quoted = true;
                leadingSpace = false;
                p++;
            } else if (c == '\\' && p + 1 < end) {
                current.append(s.charAt(p + 1));
                p += 2;
                quoted = true;
                leadingSpace = false;
                kept = current.length();
            } else if (c == ',') {
                fields.add(new Field(current.substring(0, kept), quoted));
                current.setLength(0);
                kept = 0;
                quoted = false;
                leadingSpace = true;
                p++;
            } else if (Character.isWhitespace(c)) {
                // Whitespace around an unquoted field falls away; whitespace inside one stays.
                if (!leadingSpace) current.append(c);
                p++;
            } else {
                current.append(c);
                p++;
                leadingSpace = false;
                kept = current.length();
            }
        }
        fields.add(new Field(current.substring(0, kept), quoted));
        return fields;
    }

    /** True when the text could be a composite literal at all. */
    static boolean looksLikeRecord(String text) {
        String s = text.trim();
        return s.length() >= 2 && s.charAt(0) == '(' && s.charAt(s.length() - 1) == ')';
    }
}
