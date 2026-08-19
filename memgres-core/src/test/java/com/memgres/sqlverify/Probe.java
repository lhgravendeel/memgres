package com.memgres.sqlverify;

import java.sql.*;
import java.util.*;

/** Shared statement runner for the PgFields / MemFields probes. */
final class Probe {
    private Probe() {}

    /**
     * How the probes send statements. Simple query mode by default; the extended protocol when
     * {@code -Dprobe.extended} is given, because the two are read differently — Parse analyses a
     * statement before anything runs it, and simple query does not.
     */
    static final String QUERY_MODE =
            System.getProperty("probe.extended") != null ? "" : "?preferQueryMode=simple";

    static void runScript(Connection c, String script) {
        for (String stmt : split(script)) {
            String trimmed = stmt.trim();
            if (trimmed.isEmpty()) continue;
            System.out.println("### " + trimmed.replace('\n', ' '));
            run(c, trimmed);
            System.out.println();
        }
    }

    private static void run(Connection c, String sql) {
        try (Statement s = c.createStatement()) {
            boolean hasRs = s.execute(sql);
            if (!hasRs) {
                System.out.println("OK " + s.getUpdateCount());
                return;
            }
            try (ResultSet rs = s.getResultSet()) {
                ResultSetMetaData md = rs.getMetaData();
                int n = md.getColumnCount();
                StringBuilder head = new StringBuilder("columns: ");
                for (int i = 1; i <= n; i++) {
                    if (i > 1) head.append(" | ");
                    head.append(md.getColumnName(i)).append(':').append(md.getColumnTypeName(i));
                }
                System.out.println(head);
                int rows = 0;
                while (rs.next()) {
                    StringBuilder b = new StringBuilder("row: ");
                    for (int i = 1; i <= n; i++) {
                        if (i > 1) b.append(" | ");
                        String v = rs.getString(i);
                        b.append(rs.wasNull() ? "NULL" : v);
                    }
                    System.out.println(b);
                    rows++;
                }
                System.out.println("(" + rows + " rows)");
            }
        } catch (SQLException e) {
            System.out.println("ERR[" + e.getSQLState() + "] " + e.getMessage());
            if (e instanceof org.postgresql.util.PSQLException) {
                org.postgresql.util.ServerErrorMessage m =
                        ((org.postgresql.util.PSQLException) e).getServerErrorMessage();
                if (m != null) {
                    if (m.getDetail() != null) System.out.println("  DETAIL: " + m.getDetail());
                    if (m.getHint() != null) System.out.println("  HINT: " + m.getHint());
                    if (m.getConstraint() != null) System.out.println("  CONSTRAINT: " + m.getConstraint());
                    if (m.getSchema() != null) System.out.println("  SCHEMA: " + m.getSchema());
                    if (m.getTable() != null) System.out.println("  TABLE: " + m.getTable());
                    if (m.getColumn() != null) System.out.println("  COLUMN: " + m.getColumn());
                    if (m.getDatatype() != null) System.out.println("  DATATYPE: " + m.getDatatype());
                }
            }
            rollbackQuietly(c);
        }
    }

    private static void rollbackQuietly(Connection c) {
        try (Statement s = c.createStatement()) {
            s.execute("ROLLBACK");
        } catch (SQLException ignored) {
            // autocommit connections have nothing to roll back
        }
    }

    /** Split on ';' at end of line, respecting quotes, dollar quotes and comments. */
    private static List<String> split(String sql) {
        List<String> out = new ArrayList<String>();
        StringBuilder cur = new StringBuilder();
        int i = 0, n = sql.length();
        while (i < n) {
            char ch = sql.charAt(i);
            if (ch == '\'' || ch == '"') {
                int j = i + 1;
                while (j < n) {
                    if (sql.charAt(j) == ch) {
                        if (j + 1 < n && sql.charAt(j + 1) == ch) { j += 2; continue; }
                        break;
                    }
                    j++;
                }
                cur.append(sql, i, Math.min(j + 1, n));
                i = j + 1;
                continue;
            }
            if (ch == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                int j = sql.indexOf('\n', i);
                if (j < 0) j = n;
                cur.append(sql, i, j);
                i = j;
                continue;
            }
            if (ch == '$') {
                int close = sql.indexOf('$', i + 1);
                if (close > 0 && isTag(sql, i + 1, close)) {
                    String tag = sql.substring(i, close + 1);
                    int end = sql.indexOf(tag, close + 1);
                    if (end < 0) end = n - tag.length();
                    cur.append(sql, i, Math.min(end + tag.length(), n));
                    i = end + tag.length();
                    continue;
                }
            }
            if (ch == ';') {
                out.add(cur.toString());
                cur.setLength(0);
                i++;
                continue;
            }
            cur.append(ch);
            i++;
        }
        out.add(cur.toString());
        return out;
    }

    private static boolean isTag(String s, int from, int to) {
        for (int i = from; i < to; i++) {
            char c = s.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_') return false;
        }
        return true;
    }
}
