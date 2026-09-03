package com.memgres.engine;

import java.util.ArrayList;
import java.util.List;

/**
 * Cutting a routine's body into the statements it is written as.
 *
 * <p>A semicolon separates statements only where it stands as itself. Inside a string, an
 * identifier's quotes, a comment or a dollar-quoted body it is one more character of that thing --
 * and a body that was cut on every semicolon it held ended a statement in the middle of a comment,
 * so {@code SELECT 1 -- one; two} was two statements and the second was the word two.
 */
public final class SqlPieces {

    private SqlPieces() {
    }

    /** The statements a body is written as, in order, with the empty ones left out. */
    public static List<String> statementsIn(String body) {
        List<String> result = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        int i = 0;
        while (i < body.length()) {
            char c = body.charAt(i);
            if (c == ';') {
                String statement = current.toString().trim();
                if (!statement.isEmpty()) result.add(statement);
                current.setLength(0);
                i++;
                continue;
            }
            int past = endOfSpan(body, i);
            if (past > i) {
                current.append(body, i, past);
                i = past;
                continue;
            }
            current.append(c);
            i++;
        }
        String last = current.toString().trim();
        if (!last.isEmpty()) result.add(last);
        return result;
    }

    /**
     * Where the thing beginning at {@code i} ends, or {@code i} itself when nothing begins there:
     * a string, a quoted name, a comment or a dollar-quoted body. An unterminated one runs to the
     * end of the text, which is where the parser will report it.
     */
    private static int endOfSpan(String body, int i) {
        char c = body.charAt(i);
        if (c == '\'' || c == '"') return endOfQuoted(body, i, c);
        if (c == '-' && i + 1 < body.length() && body.charAt(i + 1) == '-') {
            int end = body.indexOf('\n', i);
            return end < 0 ? body.length() : end;
        }
        if (c == '/' && i + 1 < body.length() && body.charAt(i + 1) == '*') {
            return endOfBlockComment(body, i);
        }
        if (c == '$') {
            String tag = dollarTagAt(body, i);
            if (tag != null) {
                int end = body.indexOf(tag, i + tag.length());
                return end < 0 ? body.length() : end + tag.length();
            }
        }
        return i;
    }

    private static int endOfQuoted(String body, int i, char quote) {
        int at = i + 1;
        while (at < body.length()) {
            if (body.charAt(at) == quote) {
                // A doubled quote stands for one of itself and the string goes on.
                if (at + 1 < body.length() && body.charAt(at + 1) == quote) {
                    at += 2;
                    continue;
                }
                return at + 1;
            }
            at++;
        }
        return body.length();
    }

    /** A block comment holds block comments, so the outer one ends where the last of them does. */
    private static int endOfBlockComment(String body, int i) {
        int depth = 0;
        int at = i;
        while (at < body.length() - 1) {
            if (body.charAt(at) == '/' && body.charAt(at + 1) == '*') {
                depth++;
                at += 2;
            } else if (body.charAt(at) == '*' && body.charAt(at + 1) == '/') {
                depth--;
                at += 2;
                if (depth == 0) return at;
            } else {
                at++;
            }
        }
        return body.length();
    }

    /** The opening tag of a dollar-quoted string written here, or null when none is. */
    private static String dollarTagAt(String body, int i) {
        int at = i + 1;
        while (at < body.length() && body.charAt(at) != '$') {
            char c = body.charAt(at);
            boolean partOfATag = Character.isLetterOrDigit(c) || c == '_';
            if (!partOfATag) return null;
            at++;
        }
        return at < body.length() ? body.substring(i, at + 1) : null;
    }
}
