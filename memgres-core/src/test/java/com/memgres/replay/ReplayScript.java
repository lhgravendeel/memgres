package com.memgres.replay;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * One reproducer from the investigation reports, with the answer PostgreSQL gave for each of its
 * statements written above it.
 *
 * <p>The reports record what memgres gets wrong. Read back as scripts and replayed statement by
 * statement, they are the only measure of how much of that record is actually closed — which is
 * why they are checked in rather than sampled: a corpus written to assert what was just fixed can
 * only ever agree with itself.
 */
public final class ReplayScript {

    /** What PostgreSQL answered for one statement. */
    public static final class Expectation {
        public final String sql;
        /** The column list, as {@code name:type} pairs, or null when the statement returned none. */
        public final List<String> columns;
        public final List<String> rows;
        /** The row count PostgreSQL reported for a statement that returned no result set. */
        public final Integer updateCount;
        public final String sqlState;
        public final String messageLike;
        /** Set when this statement is allowed to differ, and why. */
        public final String divergence;

        Expectation(String sql, List<String> columns, List<String> rows, Integer updateCount,
                    String sqlState, String messageLike, String divergence) {
            this.sql = sql;
            this.columns = columns;
            this.rows = rows;
            this.updateCount = updateCount;
            this.sqlState = sqlState;
            this.messageLike = messageLike;
            this.divergence = divergence;
        }

        public boolean expectsError() {
            return sqlState != null;
        }
    }

    public final String name;
    public final String source;
    public final String finding;
    public final String title;
    /** Set when the report wrote this reproducer abbreviated, so it cannot be replayed as written. */
    public final String unrunnable;
    public final List<Expectation> statements;

    private ReplayScript(String name, String source, String finding, String title,
                         String unrunnable, List<Expectation> statements) {
        this.name = name;
        this.source = source;
        this.finding = finding;
        this.title = title;
        this.unrunnable = unrunnable;
        this.statements = statements;
    }

    public static ReplayScript parse(String name, Path file) throws IOException {
        return parse(name, new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
    }

    public static ReplayScript parse(String name, String text) {
        String source = null, finding = null, title = null, unrunnable = null;
        List<Expectation> out = new ArrayList<>();

        List<String> columns = null;
        List<String> rows = new ArrayList<>();
        Integer updateCount = null;
        String sqlState = null, messageLike = null, divergence = null;
        StringBuilder sql = new StringBuilder();

        for (String raw : text.split("\n", -1)) {
            String line = raw.trim();
            if (sql.length() == 0 && line.startsWith("--")) {
                if (line.startsWith("-- source:")) source = after(line, "-- source:");
                else if (line.startsWith("-- finding:")) finding = after(line, "-- finding:");
                else if (line.startsWith("-- title:")) title = after(line, "-- title:");
                else if (line.startsWith("-- unrunnable:")) unrunnable = after(line, "-- unrunnable:");
                else if (line.startsWith("-- columns:")) columns = splitCells(after(line, "-- columns:"));
                else if (line.startsWith("-- row:")) rows.add(after(line, "-- row:"));
                else if (line.startsWith("-- rowcount:")) { /* the row list already says */ }
                else if (line.startsWith("-- ok:")) updateCount = Integer.valueOf(after(line, "-- ok:"));
                else if (line.startsWith("-- sqlstate:")) sqlState = after(line, "-- sqlstate:");
                else if (line.startsWith("-- message-like:")) messageLike = after(line, "-- message-like:");
                else if (line.startsWith("-- divergence:")) divergence = after(line, "-- divergence:");
                continue;
            }
            if (line.isEmpty() && sql.length() == 0) continue;
            sql.append(raw).append('\n');
            if (endsStatement(sql.toString())) {
                out.add(new Expectation(sql.toString().trim(), columns, new ArrayList<>(rows),
                        updateCount, sqlState, messageLike, divergence));
                sql.setLength(0);
                columns = null;
                rows = new ArrayList<>();
                updateCount = null;
                sqlState = null;
                messageLike = null;
                divergence = null;
            }
        }
        return new ReplayScript(name, source, finding, title, unrunnable, out);
    }

    private static String after(String line, String tag) {
        return line.substring(tag.length()).trim();
    }

    private static List<String> splitCells(String s) {
        List<String> out = new ArrayList<>();
        for (String p : s.split("\\|")) out.add(p.trim());
        return out;
    }

    /**
     * Whether the text so far is a whole statement: a semicolon that is not inside a literal, an
     * identifier or a dollar-quoted body. A function body holds semicolons of its own, and cutting
     * on the first one turns one statement into several that do not parse.
     */
    static boolean endsStatement(String text) {
        boolean inSingle = false, inDouble = false, lineComment = false;
        String dollar = null;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (lineComment) {
                if (c == '\n') lineComment = false;
                continue;
            }
            if (dollar != null) {
                if (text.startsWith(dollar, i)) { i += dollar.length() - 1; dollar = null; }
                continue;
            }
            if (inSingle) {
                if (c == '\'') {
                    if (i + 1 < text.length() && text.charAt(i + 1) == '\'') i++;
                    else inSingle = false;
                }
                continue;
            }
            if (inDouble) {
                if (c == '"') inDouble = false;
                continue;
            }
            if (c == '-' && i + 1 < text.length() && text.charAt(i + 1) == '-') { lineComment = true; i++; continue; }
            if (c == '\'') { inSingle = true; continue; }
            if (c == '"') { inDouble = true; continue; }
            if (c == '$') {
                int close = text.indexOf('$', i + 1);
                if (close > i && text.substring(i + 1, close).matches("[A-Za-z_0-9]*")) {
                    dollar = text.substring(i, close + 1);
                    i = close;
                }
                continue;
            }
            if (c == ';') return true;
        }
        return false;
    }
}
