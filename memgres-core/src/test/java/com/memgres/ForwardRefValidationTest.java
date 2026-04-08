package com.memgres;

import com.memgres.engine.util.IO;
import com.memgres.engine.util.Cols;
import com.memgres.core.Memgres;
import com.memgres.engine.util.Strs;
import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test runner for the function-validations suite.
 *
 * Validates PL/pgSQL forward-reference behavior: deferred body validation,
 * SQL-language eager validation, declaration-level checks, dynamic SQL,
 * triggers, procedures, DO blocks, and signature dependencies.
 *
 * Supports expectation comment formats:
 *   {@code -- expect-error}            (expect any error)
 *   {@code -- expect-error: SQLSTATE}  (expect specific SQLSTATE code)
 *   {@code -- begin-expected} / {@code -- end-expected}  (expected result rows)
 *
 * Statements without expectations are executed silently (DDL/DML setup).
 * Reports per-file pass/fail counts and fails the test if any expectation is violated.
 */
class ForwardRefValidationTest {

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
                .filter(p -> !p.getFileName().toString().contains("manual_two_session"))
                .sorted()
                .collect(Collectors.toList());

        assertTrue(sqlFiles.size() > 0, "No SQL files found in " + suiteDir);

        int totalPass = 0, totalFail = 0;
        List<String> failures = new ArrayList<>();

        for (Path sqlFile : sqlFiles) {
            String fileName = sqlFile.getFileName().toString();
            String content = IO.readString(sqlFile);
            List<ParsedBlock> blocks = parseFile(content);

            int filePass = 0, fileFail = 0;

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
                            fileFail++;
                            failures.add(stmtLabel + ": UNEXPECTED ERROR: " + e.getSQLState()
                                    + " " + e.getMessage()
                                    + "\n    SQL: " + truncate(block.sql, 150));
                            safeRollback();
                        }
                    } else if (block.expectation instanceof ExpectedError ee) {
                        try (Statement stmt = conn.createStatement()) {
                            stmt.execute(block.sql);
                            fileFail++;
                            failures.add(stmtLabel + ": EXPECTED ERROR"
                                    + (ee.sqlState != null ? " with SQLSTATE " + ee.sqlState : "")
                                    + " but statement SUCCEEDED"
                                    + "\n    SQL: " + truncate(block.sql, 150));
                        } catch (SQLException e) {
                            if (ee.sqlState != null) {
                                if (ee.sqlState.equals(e.getSQLState())) {
                                    filePass++;
                                } else {
                                    fileFail++;
                                    failures.add(stmtLabel + ": SQLSTATE MISMATCH"
                                            + "\n    Expected: " + ee.sqlState
                                            + "\n    Actual:   " + e.getSQLState()
                                            + " (" + e.getMessage() + ")"
                                            + "\n    SQL: " + truncate(block.sql, 150));
                                }
                            } else {
                                // Any error is fine
                                filePass++;
                            }
                            safeRollback();
                        }
                    } else if (block.expectation instanceof ExpectedResult er) {
                        try (Statement stmt = conn.createStatement()) {
                            boolean hasRs = stmt.execute(block.sql);
                            if (!hasRs) {
                                fileFail++;
                                failures.add(stmtLabel + ": EXPECTED result set but got update count"
                                        + "\n    SQL: " + truncate(block.sql, 150));
                                continue;
                            }
                            ResultSet rs = stmt.getResultSet();
                            ResultSetMetaData meta = rs.getMetaData();
                            int colCount = meta.getColumnCount();

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
                                        + "\n    SQL: " + truncate(block.sql, 150));
                                while (rs.next()) {}
                                continue;
                            }

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
                                        + "\n    SQL: " + truncate(block.sql, 150));
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
                                        + "\n    SQL: " + truncate(block.sql, 150));
                            }
                        } catch (SQLException e) {
                            fileFail++;
                            failures.add(stmtLabel + ": UNEXPECTED ERROR (expected result)"
                                    + "\n    Error: " + e.getSQLState() + " " + e.getMessage()
                                    + "\n    SQL: " + truncate(block.sql, 150));
                            safeRollback();
                        }
                    }
                } catch (Exception e) {
                    fileFail++;
                    failures.add(stmtLabel + ": EXCEPTION: " + e.getClass().getSimpleName()
                            + ": " + e.getMessage()
                            + "\n    SQL: " + truncate(block.sql, 150));
                    safeRollback();
                }
            }

            totalPass += filePass;
            totalFail += fileFail;
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

        if (totalFail > 0) {
            fail(String.format("%d of %d expectations failed across %d files. See stdout for details.",
                    totalFail, totalPass + totalFail, sqlFiles.size()));
        }
    }

    // =========================================================================
    // Parsing
    // =========================================================================

    static final class ParsedBlock {
        final String sql;
        final Object expectation;

        ParsedBlock(String sql, Object expectation) {
            this.sql = sql;
            this.expectation = expectation;
        }
    }

    static final class ExpectedResult {
        final List<String> columns;
        final List<String> rows;

        ExpectedResult(List<String> columns, List<String> rows) {
            this.columns = columns;
            this.rows = rows;
        }
    }

    static final class ExpectedError {
        final String sqlState; // null means any error

        ExpectedError(String sqlState) {
            this.sqlState = sqlState;
        }
    }

    /**
     * Parse a SQL file into blocks. Supports:
     * - {@code -- expect-error} and {@code -- expect-error: SQLSTATE} (inline)
     * - {@code -- begin-expected} / {@code -- columns:} / {@code -- row:} / {@code -- end-expected}
     */
    static List<ParsedBlock> parseFile(String content) {
        List<ParsedBlock> blocks = new ArrayList<>();
        List<String> lines = Strs.lines(content).collect(Collectors.toList());

        int i = 0;
        Object pendingExpectation = null;

        while (i < lines.size()) {
            String line = lines.get(i).trim();

            // Parse inline expect-error
            if (line.startsWith("-- expect-error")) {
                String rest = line.substring("-- expect-error".length()).trim();
                String sqlState = null;
                if (rest.startsWith(":")) {
                    sqlState = rest.substring(1).trim();
                    if (sqlState.isEmpty()) sqlState = null;
                }
                pendingExpectation = new ExpectedError(sqlState);
                i++;
                continue;
            }

            // Parse begin-expected block
            if (line.equals("-- begin-expected")) {
                List<String> columns = null;
                List<String> rows = new ArrayList<>();
                i++;
                while (i < lines.size()) {
                    String el = lines.get(i).trim();
                    if (el.equals("-- end-expected")) { i++; break; }
                    if (el.startsWith("-- columns:")) {
                        String colStr = el.substring("-- columns:".length()).trim();
                        // Support both pipe and comma separators
                        String sep = colStr.contains("|") ? "\\|" : ",";
                        columns = Arrays.stream(colStr.split(sep))
                                .map(String::trim).collect(Collectors.toList());
                    } else if (el.startsWith("-- row:")) {
                        rows.add(el.substring("-- row:".length()).trim());
                    }
                    i++;
                }
                pendingExpectation = new ExpectedResult(columns != null ? columns : Cols.listOf(), rows);
                continue;
            }

            // Skip pure comment lines and blank lines
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
     */
    static boolean valuesMatch(String expected, String actual) {
        if (expected.equals(actual)) return true;

        // Split by pipe and compare cell-by-cell
        String[] expCells = expected.split("\\|", -1);
        String[] actCells = actual.split("\\|", -1);
        if (expCells.length != actCells.length) return false;

        for (int i = 0; i < expCells.length; i++) {
            String e = expCells[i].trim();
            String a = actCells[i].trim();
            if (e.equals(a)) continue;

            // NULL matching
            if ((e.isEmpty() || e.equalsIgnoreCase("null"))
                    && (a.isEmpty() || a.equalsIgnoreCase("null"))) continue;

            // Boolean matching
            if (isBoolean(e) && isBoolean(a)
                    && toBool(e) == toBool(a)) continue;

            // Numeric matching
            try {
                java.math.BigDecimal be = new java.math.BigDecimal(e).stripTrailingZeros();
                java.math.BigDecimal ba = new java.math.BigDecimal(a).stripTrailingZeros();
                if (be.compareTo(ba) == 0) continue;
            } catch (NumberFormatException ignored) {}

            return false;
        }
        return true;
    }

    private static boolean isBoolean(String s) {
        return s.equalsIgnoreCase("t") || s.equalsIgnoreCase("f")
                || s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false");
    }

    private static boolean toBool(String s) {
        return s.equalsIgnoreCase("t") || s.equalsIgnoreCase("true");
    }

    private void safeRollback() {
        try {
            if (!conn.getAutoCommit()) conn.rollback();
            conn.setAutoCommit(true);
        } catch (SQLException ignored) {}
    }

    private static String truncate(String s, int max) {
        String oneLine = s.replaceAll("\\s+", " ").trim();
        return oneLine.length() <= max ? oneLine : oneLine.substring(0, max) + "...";
    }

    private static Path findSuiteDir() {
        try {
            java.net.URL url = ForwardRefValidationTest.class.getClassLoader()
                    .getResource("function-validations");
            if (url != null) return Path.of(url.toURI());
        } catch (Exception ignored) {}
        throw new RuntimeException("Cannot find function-validations resource directory");
    }
}
