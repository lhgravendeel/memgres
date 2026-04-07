package com.memgres;

import com.memgres.engine.util.IO;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import com.memgres.engine.util.Strs;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test runner for the pg18_sample_sql_5 suite.
 *
 * Parses SQL files with embedded expectation comments:
 *   {@code -- begin-expected} / {@code -- end-expected}       (expected result rows)
 *   {@code -- begin-expected-error} / {@code -- end-expected-error}  (expected error)
 *
 * Statements without expectations are executed silently (DDL/DML setup).
 * Reports per-file pass/fail counts and fails the test if any expectation is violated.
 *
 * .md files are skipped (scenario design docs, not executable SQL).
 */
class Pg18SampleSql5Test {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    void runAllSqlFiles() throws Exception {
        Path suiteDir = findSuiteDir();
        List<Path> sqlFiles = Files.list(suiteDir)
                .filter(p -> p.toString().endsWith(".sql"))
                .sorted()
                .collect(Collectors.toList());

        assertTrue(sqlFiles.size() > 0, "No SQL files found in " + suiteDir);

        int totalPass = 0, totalFail = 0, totalSkipped = 0;
        List<String> failures = new ArrayList<>();

        for (Path sqlFile : sqlFiles) {
            String fileName = sqlFile.getFileName().toString();
            String content = IO.readString(sqlFile);
            List<ParsedBlock> blocks = parseFile(content);

            int filePass = 0, fileFail = 0, fileSkipped = 0;

            for (int bi = 0; bi < blocks.size(); bi++) {
                ParsedBlock block = blocks.get(bi);
                String stmtLabel = fileName + " stmt " + (bi + 1);

                if (block.sql.trim().isEmpty()) continue;

                try {
                    if (block.expectation == null) {
                        // No expectation, just execute
                        try (Statement stmt = conn.createStatement()) {
                            stmt.execute(block.sql);
                        } catch (SQLException e) {
                            // Unexpected error on setup statement
                            fileFail++;
                            failures.add(stmtLabel + ": UNEXPECTED ERROR: " + e.getMessage()
                                    + "\n    SQL: " + truncate(block.sql, 120));
                            safeRollback();
                        }
                    } else if (block.expectation instanceof ExpectedError ee) {
                        // Expect an error
                        try (Statement stmt = conn.createStatement()) {
                            stmt.execute(block.sql);
                            // If we get here, expected error didn't happen
                            fileFail++;
                            failures.add(stmtLabel + ": EXPECTED ERROR containing \""
                                    + ee.messageLike + "\" but statement SUCCEEDED"
                                    + "\n    SQL: " + truncate(block.sql, 120));
                        } catch (SQLException e) {
                            String msg = e.getMessage().toLowerCase();
                            if (msg.contains(ee.messageLike.toLowerCase())) {
                                filePass++;
                            } else {
                                fileFail++;
                                failures.add(stmtLabel + ": ERROR message mismatch"
                                        + "\n    Expected substring: " + ee.messageLike
                                        + "\n    Actual: " + e.getMessage()
                                        + "\n    SQL: " + truncate(block.sql, 120));
                            }
                            safeRollback();
                        }
                    } else if (block.expectation instanceof ExpectedResult er) {
                        // Expect specific result
                        try (Statement stmt = conn.createStatement()) {
                            boolean hasRs = stmt.execute(block.sql);
                            if (!hasRs) {
                                fileFail++;
                                failures.add(stmtLabel + ": EXPECTED result set but got update count"
                                        + "\n    SQL: " + truncate(block.sql, 120));
                                continue;
                            }
                            ResultSet rs = stmt.getResultSet();
                            ResultSetMetaData meta = rs.getMetaData();
                            int colCount = meta.getColumnCount();

                            // Check columns
                            List<String> actualCols = new ArrayList<>();
                            for (int c = 1; c <= colCount; c++) {
                                actualCols.add(meta.getColumnLabel(c).toLowerCase());
                            }
                            List<String> expectedCols = er.columns.stream()
                                    .map(String::toLowerCase).collect(Collectors.toList());

                            if (!actualCols.equals(expectedCols)) {
                                fileFail++;
                                failures.add(stmtLabel + ": COLUMN MISMATCH"
                                        + "\n    Expected: " + String.join("|", expectedCols)
                                        + "\n    Actual:   " + String.join("|", actualCols)
                                        + "\n    SQL: " + truncate(block.sql, 120));
                                // drain result set
                                while (rs.next()) {}
                                continue;
                            }

                            // Check rows
                            List<String> actualRows = new ArrayList<>();
                            while (rs.next()) {
                                StringBuilder sb = new StringBuilder();
                                for (int c = 1; c <= colCount; c++) {
                                    if (c > 1) sb.append("|");
                                    String val = rs.getString(c);
                                    sb.append(val == null ? "NULL" : val);
                                }
                                actualRows.add(sb.toString());
                            }

                            if (actualRows.size() != er.rows.size()) {
                                fileFail++;
                                failures.add(stmtLabel + ": ROW COUNT MISMATCH"
                                        + "\n    Expected " + er.rows.size() + " rows, got " + actualRows.size()
                                        + "\n    Expected rows: " + er.rows
                                        + "\n    Actual rows:   " + actualRows
                                        + "\n    SQL: " + truncate(block.sql, 120));
                                continue;
                            }

                            boolean rowsMatch = true;
                            StringBuilder rowDiff = new StringBuilder();
                            for (int r = 0; r < er.rows.size(); r++) {
                                String expected = er.rows.get(r);
                                String actual = actualRows.get(r);
                                if (!valuesMatch(expected, actual)) {
                                    rowsMatch = false;
                                    rowDiff.append("\n    Row ").append(r + 1)
                                            .append(": expected [").append(expected)
                                            .append("] got [").append(actual).append("]");
                                }
                            }
                            if (rowsMatch) {
                                filePass++;
                            } else {
                                fileFail++;
                                failures.add(stmtLabel + ": ROW DATA MISMATCH" + rowDiff
                                        + "\n    SQL: " + truncate(block.sql, 120));
                            }
                        } catch (SQLException e) {
                            fileFail++;
                            failures.add(stmtLabel + ": UNEXPECTED ERROR (expected result)"
                                    + "\n    Error: " + e.getMessage()
                                    + "\n    SQL: " + truncate(block.sql, 120));
                            safeRollback();
                        }
                    }
                } catch (Exception e) {
                    fileFail++;
                    failures.add(stmtLabel + ": EXCEPTION: " + e.getClass().getSimpleName()
                            + ": " + e.getMessage()
                            + "\n    SQL: " + truncate(block.sql, 120));
                    safeRollback();
                }
            }

            totalPass += filePass;
            totalFail += fileFail;
            totalSkipped += fileSkipped;
            String status = fileFail == 0 ? "PASS" : "FAIL";
            System.out.printf("[%s] %-55s  pass=%d  fail=%d%n", status, fileName, filePass, fileFail);
        }

        System.out.println();
        System.out.println(Strs.repeat("=", 70));
        System.out.printf("TOTAL: %d passed, %d failed, %d files%n", totalPass, totalFail, sqlFiles.size());
        System.out.println(Strs.repeat("=", 70));

        if (!failures.isEmpty()) {
            System.out.println();
            System.out.println("FAILURES:");
            System.out.println(Strs.repeat("-", 70));
            for (int i = 0; i < failures.size(); i++) {
                System.out.printf("  %d) %s%n", i + 1, failures.get(i));
            }
            System.out.println(Strs.repeat("-", 70));
        }

        // Fail the test if there are any failures, with a summary message
        if (totalFail > 0) {
            fail(String.format("%d of %d expectations failed across %d files. See stdout for details.",
                    totalFail, totalPass + totalFail, sqlFiles.size()));
        }
    }

    // =========================================================================
    // Parsing
    // =========================================================================

    /** A parsed block: optionally an expectation followed by a SQL statement. */
        public static final class ParsedBlock {
        public final String sql;
        public final Object expectation;

        public ParsedBlock(String sql, Object expectation) {
            this.sql = sql;
            this.expectation = expectation;
        }

        public String sql() { return sql; }
        public Object expectation() { return expectation; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ParsedBlock that = (ParsedBlock) o;
            return java.util.Objects.equals(sql, that.sql)
                && java.util.Objects.equals(expectation, that.expectation);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(sql, expectation);
        }

        @Override
        public String toString() {
            return "ParsedBlock[sql=" + sql + ", " + "expectation=" + expectation + "]";
        }
    }

        public static final class ExpectedResult {
        public final List<String> columns;
        public final List<String> rows;

        public ExpectedResult(List<String> columns, List<String> rows) {
            this.columns = columns;
            this.rows = rows;
        }

        public List<String> columns() { return columns; }
        public List<String> rows() { return rows; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExpectedResult that = (ExpectedResult) o;
            return java.util.Objects.equals(columns, that.columns)
                && java.util.Objects.equals(rows, that.rows);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(columns, rows);
        }

        @Override
        public String toString() {
            return "ExpectedResult[columns=" + columns + ", " + "rows=" + rows + "]";
        }
    }
        public static final class ExpectedError {
        public final String messageLike;

        public ExpectedError(String messageLike) {
            this.messageLike = messageLike;
        }

        public String messageLike() { return messageLike; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExpectedError that = (ExpectedError) o;
            return java.util.Objects.equals(messageLike, that.messageLike);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(messageLike);
        }

        @Override
        public String toString() {
            return "ExpectedError[messageLike=" + messageLike + "]";
        }
    }

    /**
     * Parse a SQL file into blocks. Each block is a SQL statement optionally
     * preceded by an expectation comment block.
     */
    static List<ParsedBlock> parseFile(String content) {
        List<ParsedBlock> blocks = new ArrayList<>();
        List<String> lines = Strs.lines(content).collect(Collectors.toList());

        int i = 0;
        Object pendingExpectation = null;

        while (i < lines.size()) {
            String line = lines.get(i).trim();

            // Parse expectation blocks
            if (line.equals("-- begin-expected")) {
                List<String> columns = null;
                List<String> rows = new ArrayList<>();
                i++;
                while (i < lines.size()) {
                    String el = lines.get(i).trim();
                    if (el.equals("-- end-expected")) { i++; break; }
                    if (el.startsWith("-- columns:")) {
                        String colStr = el.substring("-- columns:".length()).trim();
                        // Support both | and , as column separators
                        String colSep = colStr.contains("|") ? "\\|" : ",";
                        columns = Arrays.stream(colStr.split(colSep))
                                .map(String::trim).collect(Collectors.toList());
                    } else if (el.startsWith("-- row:")) {
                        rows.add(el.substring("-- row:".length()).trim());
                    }
                    i++;
                }
                pendingExpectation = new ExpectedResult(columns != null ? columns : Cols.listOf(), rows);
                continue;
            }

            if (line.equals("-- begin-expected-error")) {
                String messageLike = "";
                i++;
                while (i < lines.size()) {
                    String el = lines.get(i).trim();
                    if (el.equals("-- end-expected-error")) { i++; break; }
                    if (el.startsWith("-- message-like:")) {
                        messageLike = el.substring("-- message-like:".length()).trim();
                    }
                    i++;
                }
                pendingExpectation = new ExpectedError(messageLike);
                continue;
            }

            // Skip pure comment lines and blank lines (don't consume pending expectation)
            if (line.startsWith("--") || line.isEmpty()) {
                i++;
                continue;
            }

            // Collect SQL statement (may span multiple lines, ends with ;)
            StringBuilder sqlBuf = new StringBuilder();
            boolean inSingle = false, inDouble = false;
            String dollarTag = null;
            boolean foundSemicolon = false;

            while (i < lines.size() && !foundSemicolon) {
                String rawLine = lines.get(i);
                // Strip line comments (outside quotes)
                String processedLine = rawLine;
                int commentPos = findLineCommentPos(rawLine, inSingle, inDouble, dollarTag);
                if (commentPos >= 0) {
                    processedLine = rawLine.substring(0, commentPos);
                }

                for (int ci = 0; ci < processedLine.length(); ci++) {
                    char c = processedLine.charAt(ci);

                    if (dollarTag != null) {
                        sqlBuf.append(c);
                        if (c == '$') {
                            int tagEnd = processedLine.indexOf(dollarTag, ci);
                            if (tagEnd == ci) {
                                sqlBuf.append(dollarTag.substring(1));
                                ci += dollarTag.length() - 1;
                                dollarTag = null;
                            }
                        }
                        continue;
                    }
                    if (inSingle) {
                        sqlBuf.append(c);
                        if (c == '\'' && (ci + 1 >= processedLine.length() || processedLine.charAt(ci + 1) != '\'')) {
                            inSingle = false;
                        } else if (c == '\'' && ci + 1 < processedLine.length() && processedLine.charAt(ci + 1) == '\'') {
                            sqlBuf.append('\'');
                            ci++;
                        }
                        continue;
                    }
                    if (inDouble) {
                        sqlBuf.append(c);
                        if (c == '"') inDouble = false;
                        continue;
                    }

                    if (c == '$') {
                        int dEnd = processedLine.indexOf('$', ci + 1);
                        if (dEnd > ci) {
                            String tag = processedLine.substring(ci, dEnd + 1);
                            if (tag.matches("\\$[a-zA-Z0-9_]*\\$")) {
                                dollarTag = tag;
                                sqlBuf.append(tag);
                                ci = dEnd;
                                continue;
                            }
                        }
                    }
                    if (c == '\'') { inSingle = true; sqlBuf.append(c); continue; }
                    if (c == '"')  { inDouble = true; sqlBuf.append(c); continue; }
                    if (c == ';') {
                        foundSemicolon = true;
                        break;
                    }
                    sqlBuf.append(c);
                }
                if (!foundSemicolon) sqlBuf.append('\n');
                i++;
            }

            String sql = sqlBuf.toString().trim();
            if (!sql.isEmpty()) {
                blocks.add(new ParsedBlock(sql, pendingExpectation));
                pendingExpectation = null;
            }
        }

        return blocks;
    }

    /** Find position of a line comment (--) outside of quotes. Returns -1 if none. */
    static int findLineCommentPos(String line, boolean inSingle, boolean inDouble, String dollarTag) {
        if (dollarTag != null || inSingle || inDouble) return -1;
        boolean sq = false, dq = false;
        String dt = null;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (dt != null) {
                if (c == '$' && line.indexOf(dt, i) == i) {
                    i += dt.length() - 1;
                    dt = null;
                }
                continue;
            }
            if (sq) { if (c == '\'') sq = false; continue; }
            if (dq) { if (c == '"') dq = false; continue; }
            if (c == '$') {
                int dEnd = line.indexOf('$', i + 1);
                if (dEnd > i) {
                    String tag = line.substring(i, dEnd + 1);
                    if (tag.matches("\\$[a-zA-Z0-9_]*\\$")) { dt = tag; i = dEnd; continue; }
                }
            }
            if (c == '\'') { sq = true; continue; }
            if (c == '"') { dq = true; continue; }
            if (c == '-' && i + 1 < line.length() && line.charAt(i + 1) == '-') {
                return i;
            }
        }
        return -1;
    }

    /**
     * Compare expected and actual row values with tolerance for numeric formatting.
     * E.g. "1.20" matches "1.2", and "4 days" matches "4 days".
     */
    static boolean valuesMatch(String expected, String actual) {
        if (expected.equals(actual)) return true;
        // Compare cell by cell
        String[] eParts = expected.split("\\|", -1);
        String[] aParts = actual.split("\\|", -1);
        if (eParts.length != aParts.length) return false;
        for (int i = 0; i < eParts.length; i++) {
            if (!cellMatches(eParts[i].trim(), aParts[i].trim())) return false;
        }
        return true;
    }

    static boolean cellMatches(String expected, String actual) {
        if (expected.equals(actual)) return true;
        // In the test file format, empty string between pipes means NULL
        if (expected.isEmpty() && actual.equalsIgnoreCase("NULL")) return true;
        if (expected.equalsIgnoreCase("NULL") && actual.isEmpty()) return true;
        if (expected.equalsIgnoreCase("NULL") && actual.equalsIgnoreCase("NULL")) return true;
        // Try numeric comparison with tolerance
        try {
            double e = Double.parseDouble(expected);
            double a = Double.parseDouble(actual);
            if (Math.abs(e - a) < 0.001) return true;
            // Also try BigDecimal for exact comparison (e.g. "1.20" vs "1.2")
            java.math.BigDecimal be = new java.math.BigDecimal(expected).stripTrailingZeros();
            java.math.BigDecimal ba = new java.math.BigDecimal(actual).stripTrailingZeros();
            if (be.compareTo(ba) == 0) return true;
        } catch (NumberFormatException ignored) {}
        // Boolean comparison: t/true, f/false
        if (isBoolMatch(expected, actual)) return true;
        return false;
    }

    static boolean isBoolMatch(String a, String b) {
        Set<String> trues = Cols.setOf("t", "true", "TRUE");
        Set<String> falses = Cols.setOf("f", "false", "FALSE");
        if (trues.contains(a) && trues.contains(b)) return true;
        if (falses.contains(a) && falses.contains(b)) return true;
        return false;
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private void safeRollback() {
        try {
            if (!conn.getAutoCommit()) conn.rollback();
        } catch (SQLException ignored) {}
    }

    private static String truncate(String s, int max) {
        String oneLine = s.replaceAll("\\s+", " ").trim();
        return oneLine.length() <= max ? oneLine : oneLine.substring(0, max) + "...";
    }

    private static Path findSuiteDir() {
        try {
            java.net.URL url = Pg18SampleSql5Test.class.getClassLoader().getResource("pg18-sql-5");
            if (url != null) return Path.of(url.toURI());
        } catch (Exception ignored) {}
        throw new RuntimeException("Cannot find pg18-sql-5 resource directory");
    }
}
