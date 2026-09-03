package com.memgres.engine;

import com.memgres.engine.parser.PgKeywords;

/**
 * Writing a value or a name so that reading it back gives what was written.
 *
 * <p>There were three of these: {@code quote_literal}, {@code quote_nullable} and
 * {@code format}'s {@code %L}, each quoting on its own rules, so the same string came out three
 * ways and only two of them could be read again. There is one now, because the guarantee is one
 * guarantee.
 */
final class Quoting {

    private Quoting() {
    }

    /**
     * A value as an SQL string literal. A backslash means nothing inside an ordinary literal and
     * everything inside an escape one, so a value carrying a backslash is written as an
     * {@code E'...'} string with the backslash doubled, exactly as {@code quote_literal} does.
     */
    static String literal(String text) {
        String escaped = text.replace("'", "''").replace("\\", "\\\\");
        return text.indexOf('\\') >= 0 ? "E'" + escaped + "'" : "'" + escaped + "'";
    }

    /** A value as a literal, or the word NULL when there is no value. */
    static String nullableLiteral(Object value) {
        return value == null ? "NULL" : literal(TypeCoercion.toString(value));
    }

    /**
     * A name written so that it reads back as the same name. A word that is spelled the way an
     * unquoted identifier is spelled and names nothing in the grammar stands as it is; everything
     * else is quoted, with any quote inside it doubled.
     */
    static String identifier(String name) {
        if (!needsQuoting(name)) return name;
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }


    /**
     * A name read the way PostgreSQL reads one written as text: the double quotes around a part of
     * it are identifier quoting rather than characters of the name, and a part written without them
     * is folded to lower case. It is this name, and not the text it was written as, that a
     * complaint about a missing object quotes back.
     */
    static String nameAsRead(String written) {
        if (written == null) return null;
        String text = written.trim();
        StringBuilder out = new StringBuilder();
        StringBuilder part = new StringBuilder();
        boolean inQuotes = false;
        boolean quotedPart = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < text.length() && text.charAt(i + 1) == '"') {
                    part.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                    quotedPart = true;
                }
            } else if (c == '.' && !inQuotes) {
                out.append(quotedPart ? part.toString()
                        : part.toString().trim().toLowerCase(java.util.Locale.ROOT));
                out.append('.');
                part.setLength(0);
                quotedPart = false;
            } else {
                part.append(c);
            }
        }
        out.append(quotedPart ? part.toString()
                : part.toString().trim().toLowerCase(java.util.Locale.ROOT));
        return out.toString();
    }

    private static boolean needsQuoting(String name) {
        if (name.isEmpty()) return true;
        char first = name.charAt(0);
        if (!(first >= 'a' && first <= 'z') && first != '_') return true;
        for (int i = 1; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') && c != '_') return true;
        }
        // A word the grammar knows has to be quoted even though it is spelled like a name, or the
        // text reads as that construct instead. The hand-written list this used to consult had
        // sixty words in it, so quote_ident('trim') came back as an identifier the parser refused.
        return PgKeywords.isKeywordOrReserved(name);
    }
}
