package com.memgres.replay;

import com.memgres.core.Memgres;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/** Replays the corpus against memgres and says, per statement, whether it answered as PG does. */
public final class ReplayRunner {

    public static Path corpusDir() {
        try {
            java.net.URL url = ReplayRunner.class.getClassLoader().getResource("replay/MANIFEST.txt");
            if (url == null) throw new IllegalStateException("the replay corpus is not on the classpath");
            return Paths.get(url.toURI()).getParent();
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    public static List<String> manifest(Path dir) throws IOException {
        return Files.readAllLines(dir.resolve("MANIFEST.txt")).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    /**
     * Run every script, each against a memgres of its own.
     *
     * <p>A server apiece is what makes the answer trustworthy: sharing one, a reproducer that
     * leaves a role or a table behind changes what the next one is told, and the difference is
     * read as this script's when it belongs to the one before it.
     */
    public static ReplayLedger run(Path dir, List<String> names) throws Exception {
        ReplayLedger ledger = new ReplayLedger();
        for (String rel : names) {
            Path f = dir.resolve(rel);
            if (!Files.exists(f)) continue;
            ReplayScript script = ReplayScript.parse(rel, f);
            ledger.scripts++;
            if (script.unrunnable != null) {
                ledger.unrunnable++;
                continue;
            }
            runOne(script, ledger);
        }
        return ledger;
    }

    /**
     * The name the reference database has, given to memgres too.
     *
     * <p>PostgreSQL names the database in several of its messages — "permission denied for
     * database x" — and a server called something else answers with its own name, which is a
     * difference between two harnesses rather than between two engines.
     */
    static final String DATABASE = "memgrestest";

    private static void runOne(ReplayScript script, ReplayLedger ledger) throws Exception {
        Memgres m = Memgres.builder().port(0).defaultDatabaseName(DATABASE).build().start();
        try (Connection c = DriverManager.getConnection(
                m.getJdbcUrl() + "?preferQueryMode=simple", m.getUser(), m.getPassword())) {
            for (ReplayScript.Expectation e : script.statements) {
                String actual = execute(c, e.sql);
                String expected = expected(e);
                if (expected == null) {          // nothing was recorded for this statement
                    ledger.agree();
                } else if (matches(expected, actual)) {
                    ledger.agree();
                } else if (e.divergence != null) {
                    ledger.allow();
                } else {
                    ledger.differ(script.name, script.finding, e.sql, expected, actual);
                }
            }
        } finally {
            m.close();
        }
    }

    /** What PostgreSQL answered, rendered the same way memgres's answer is rendered. */
    static String expected(ReplayScript.Expectation e) {
        if (e.expectsError()) {
            return "ERR[" + e.sqlState + "] " + (e.messageLike == null ? "" : e.messageLike);
        }
        if (e.updateCount != null) return "OK " + e.updateCount;
        if (e.columns == null) return null;
        StringBuilder b = new StringBuilder("columns: ")
                .append(String.join(" | ", e.columns)).append('\n');
        for (String r : e.rows) b.append("row: ").append(r).append('\n');
        b.append("(").append(e.rows.size()).append(" rows)");
        return b.toString();
    }

    static String execute(Connection c, String sql) {
        try (Statement s = c.createStatement()) {
            boolean hasRs = s.execute(sql);
            if (!hasRs) return "OK " + s.getUpdateCount();
            try (ResultSet rs = s.getResultSet()) {
                ResultSetMetaData md = rs.getMetaData();
                int cols = md.getColumnCount();
                StringBuilder head = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) head.append(" | ");
                    head.append(md.getColumnName(i)).append(':').append(md.getColumnTypeName(i));
                }
                StringBuilder b = new StringBuilder("columns: ").append(head).append('\n');
                int n = 0;
                while (rs.next()) {
                    b.append("row: ");
                    for (int i = 1; i <= cols; i++) {
                        if (i > 1) b.append(" | ");
                        String v = rs.getString(i);
                        b.append(rs.wasNull() ? "NULL" : v.replace("\n", "\\n"));
                    }
                    b.append('\n');
                    n++;
                }
                return b.append("(").append(n).append(" rows)").toString();
            }
        } catch (SQLException e) {
            rollback(c);
            String msg = e.getMessage() == null ? "" : e.getMessage();
            int nl = msg.indexOf('\n');
            if (nl > 0) msg = msg.substring(0, nl);
            return "ERR[" + e.getSQLState() + "] " + msg.replace("ERROR: ", "").trim();
        }
    }

    private static void rollback(Connection c) {
        try (Statement s = c.createStatement()) {
            s.execute("ROLLBACK");
        } catch (SQLException ignored) {
            // an autocommit connection has nothing to roll back
        }
    }

    /**
     * Whether the two answers say the same thing.
     *
     * <p>Values that cannot repeat between two servers -- a timestamp, an OID, a generated name --
     * are compared as their shape rather than their text, because a corpus that fails on the clock
     * is a corpus nobody keeps green.
     */
    static boolean matches(String expected, String actual) {
        return normalise(expected).equals(normalise(actual));
    }

    private static String normalise(String s) {
        String t = s;
        t = t.replaceAll("\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?([+-]\\d{2}(:\\d{2})?)?",
                "<timestamp>");
        t = t.replaceAll("\\b1[0-9]{4,}\\b", "<oid>");
        t = t.replaceAll("\\b0/[0-9A-F]{6,}\\b", "<lsn>");
        // A plan that was run reports how long it took and how much it allocated. Those are
        // measurements of the machine, not of the answer: PostgreSQL does not repeat them
        // between two runs of its own, so holding memgres to the recorded ones would be holding
        // it to a stopwatch reading. The shape of the line still has to match.
        t = t.replaceAll("time=[0-9.]+\\.\\.[0-9.]+", "time=<elapsed>");
        t = t.replaceAll("(Planning|Execution) Time: [0-9.]+ ms", "$1 Time: <elapsed> ms");
        t = t.replaceAll("used=[0-9]+kB allocated=[0-9]+kB", "used=<memory> allocated=<memory>");
        return t.trim();
    }
}
