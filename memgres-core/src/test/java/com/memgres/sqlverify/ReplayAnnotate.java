package com.memgres.sqlverify;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/**
 * Record what PostgreSQL answers for every statement of every replay script.
 *
 * <p>Each script gets a database of its own, so what one reproducer leaves behind cannot be read
 * as another one's answer. The answers are written above the statement that produced them, in the
 * annotation vocabulary the feature-comparison corpus already uses, so a reader can see what is
 * expected without a PostgreSQL to hand and CI can check memgres against it with no server at all.
 */
public class ReplayAnnotate {

    public static void main(String[] args) throws Exception {
        Path root = Paths.get(args[0]);
        List<String> manifest = Files.readAllLines(root.resolve("MANIFEST.txt"));
        int done = 0, statements = 0;
        for (String rel : manifest) {
            rel = rel.trim();
            if (rel.isEmpty()) continue;
            Path f = root.resolve(rel);
            if (!Files.exists(f)) continue;
            statements += annotate(f);
            if (++done % 100 == 0) System.out.println("  " + done + " / " + manifest.size());
        }
        System.out.println("annotated " + done + " scripts, " + statements + " statements");
    }

    private static int annotate(Path f) throws IOException {
        List<String> lines = Files.readAllLines(f, StandardCharsets.UTF_8);
        List<String> header = new ArrayList<>();
        StringBuilder body = new StringBuilder();
        // Run twice over a script that already carries answers, the answers would be read as part
        // of the script and recorded again inside themselves. Everything this tool wrote last time
        // comes off first, so annotating is the same operation however often it is done.
        boolean inRecorded = false;
        for (String l : lines) {
            String t = l.trim();
            if (t.equals("-- begin-expected") || t.equals("-- begin-expected-error")) {
                inRecorded = true;
                continue;
            }
            if (t.equals("-- end-expected") || t.equals("-- end-expected-error")) {
                inRecorded = false;
                continue;
            }
            if (inRecorded) continue;
            if (body.length() == 0 && t.startsWith("--") && !t.startsWith("-- replay:")) {
                header.add(l);
            } else {
                body.append(l).append('\n');
            }
        }
        boolean unrunnable = false;
        for (String h : header) if (h.startsWith("-- unrunnable:")) unrunnable = true;

        List<String> stmts = SqlVerifyHarness.splitStatements(body.toString());
        StringBuilder out = new StringBuilder();
        for (String h : header) out.append(h).append('\n');

        if (unrunnable) {
            for (String s : stmts) out.append(s.trim()).append(";\n");
            Files.write(f, out.toString().getBytes(StandardCharsets.UTF_8));
            return 0;
        }

        ReplayDb.recreate();
        int n = 0;
        try (Connection c = ReplayDb.connect()) {
            try (Statement g = c.createStatement()) {
                g.execute("SET statement_timeout = 5000");
            } catch (SQLException ignored) {
                // a server that will not be told to give up is left to its own devices
            }
            for (String raw : stmts) {
                String s = raw.trim();
                if (s.isEmpty()) continue;
                // The reports write comments between the statements of a reproducer, and a split
                // hands the comment back attached to the statement after it. Read as a comment
                // and set aside whole, the statement went unrecorded — every statement that
                // happened to follow a note in the report was replayed against no expectation.
                int cut = endOfLeadingComments(s);
                String lead = s.substring(0, cut);
                String sql = s.substring(cut).trim();
                if (!lead.trim().isEmpty()) out.append(lead.endsWith("\n") ? lead : lead + "\n");
                if (sql.isEmpty()) continue;
                out.append(record(c, sql));
                out.append(sql.endsWith(";") ? sql : sql + ";").append('\n');
                n++;
            }
        } catch (SQLException e) {
            out.append("-- unrunnable: could not connect: ").append(e.getMessage()).append('\n');
        }
        Files.write(f, out.toString().getBytes(StandardCharsets.UTF_8));
        return n;
    }

    /** Where the comment lines a split chunk opens with end and its statement begins. */
    private static int endOfLeadingComments(String chunk) {
        int at = 0;
        while (at < chunk.length()) {
            int nl = chunk.indexOf('\n', at);
            String line = (nl < 0 ? chunk.substring(at) : chunk.substring(at, nl)).trim();
            if (!line.startsWith("--") && !line.isEmpty()) return at;
            if (nl < 0) return chunk.length();
            at = nl + 1;
        }
        return at;
    }

    /** The answer to one statement, written as the annotation that asserts it. */
    static String record(Connection c, String sql) {
        StringBuilder a = new StringBuilder();
        try (Statement s = c.createStatement()) {
            boolean hasRs = s.execute(sql);
            if (!hasRs) {
                a.append("-- begin-expected\n-- ok: ").append(s.getUpdateCount())
                        .append("\n-- end-expected\n");
                return a.toString();
            }
            try (ResultSet rs = s.getResultSet()) {
                ResultSetMetaData md = rs.getMetaData();
                int cols = md.getColumnCount();
                StringBuilder head = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) head.append(" | ");
                    head.append(md.getColumnName(i)).append(':').append(md.getColumnTypeName(i));
                }
                List<String> rows = new ArrayList<>();
                while (rs.next()) {
                    StringBuilder r = new StringBuilder();
                    for (int i = 1; i <= cols; i++) {
                        if (i > 1) r.append(" | ");
                        String v = rs.getString(i);
                        r.append(rs.wasNull() ? "NULL" : v.replace("\n", "\\n"));
                    }
                    rows.add(r.toString());
                }
                a.append("-- begin-expected\n-- columns: ").append(head).append('\n');
                for (String r : rows) a.append("-- row: ").append(r).append('\n');
                a.append("-- rowcount: ").append(rows.size()).append("\n-- end-expected\n");
            }
        } catch (SQLException e) {
            a.setLength(0);
            a.append("-- begin-expected-error\n-- sqlstate: ").append(e.getSQLState()).append('\n');
            String m = e.getMessage();
            if (m != null) {
                int nl = m.indexOf('\n');
                if (nl > 0) m = m.substring(0, nl);
                a.append("-- message-like: ").append(m.replace("ERROR: ", "").trim()).append('\n');
            }
            a.append("-- end-expected-error\n");
            ReplayDb.rollback(c);
        }
        return a.toString();
    }
}
