package com.memgres.sqlverify;

import com.memgres.engine.util.IO;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import com.memgres.engine.util.Strs;

/**
 * Shared logic for running SQL verification files and collecting results.
 *
 * Each SQL file is split into statements. Each statement is executed and
 * the result is captured as either:
 *   - SUCCESS with column names + row data (for SELECT / RETURNING)
 *   - SUCCESS with update count (for INSERT / UPDATE / DELETE / DDL)
 *   - ERROR with the error message
 *
 * Results are stored in a JSON-like text format that can be compared
 * between PG and memgres to find behavioral differences.
 */
public class SqlVerifyHarness {

        public static final class StatementResult {
        public final String sql;
        public final boolean success;
        public final String errorMessage;
        public final String errorState;
        public final List<String> columns;
        public final List<List<String>> rows;
        public final int updateCount;

        public StatementResult(String sql, boolean success, String errorMessage, String errorState,
                               List<String> columns, List<List<String>> rows, int updateCount) {
            this.sql = sql;
            this.success = success;
            this.errorMessage = errorMessage;
            this.errorState = errorState;
            this.columns = columns;
            this.rows = rows;
            this.updateCount = updateCount;
        }

        public String sql() { return sql; }
        public boolean success() { return success; }
        public String errorMessage() { return errorMessage; }
        public String errorState() { return errorState; }
        public List<String> columns() { return columns; }
        public List<List<String>> rows() { return rows; }
        public int updateCount() { return updateCount; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StatementResult that = (StatementResult) o;
            return java.util.Objects.equals(sql, that.sql)
                && success == that.success
                && java.util.Objects.equals(errorMessage, that.errorMessage)
                && java.util.Objects.equals(errorState, that.errorState)
                && java.util.Objects.equals(columns, that.columns)
                && java.util.Objects.equals(rows, that.rows)
                && java.util.Objects.equals(updateCount, that.updateCount);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(sql, success, errorMessage, errorState, columns, rows, updateCount);
        }

        @Override
        public String toString() {
            return "StatementResult[sql=" + sql + ", " + "success=" + success + ", " + "errorMessage=" + errorMessage + ", " + "errorState=" + errorState + ", " + "columns=" + columns + ", " + "rows=" + rows + ", " + "updateCount=" + updateCount + "]";
        }
    }

        public static final class FileResult {
        public final String fileName;
        public final List<StatementResult> results;

        public FileResult(String fileName, List<StatementResult> results) {
            this.fileName = fileName;
            this.results = results;
        }

        public String fileName() { return fileName; }
        public List<StatementResult> results() { return results; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FileResult that = (FileResult) o;
            return java.util.Objects.equals(fileName, that.fileName)
                && java.util.Objects.equals(results, that.results);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(fileName, results);
        }

        @Override
        public String toString() {
            return "FileResult[fileName=" + fileName + ", " + "results=" + results + "]";
        }
    }

    /**
     * Run all SQL files from the sql-verify resource directory against the given connection.
     */
    public static List<FileResult> runAll(Connection conn) throws Exception {
        return runAll(conn, findResourceDir());
    }

    /**
     * Run all SQL files from the given resource directory against the given connection.
     */
    public static List<FileResult> runAll(Connection conn, Path resourceDir) throws Exception {
        conn.setAutoCommit(true);
        List<FileResult> allResults = new ArrayList<>();

        List<Path> sqlFiles;
        try (var stream = Files.list(resourceDir)) {
            sqlFiles = stream.filter(p -> p.toString().endsWith(".sql")).sorted().collect(Collectors.toList());
        }

        for (Path sqlFile : sqlFiles) {
            String fileName = sqlFile.getFileName().toString();
            System.out.print("  Running: " + fileName + " ... ");
            FileResult result = runFile(conn, sqlFile);
            allResults.add(result);
            long ok = result.results().stream().filter(StatementResult::success).count();
            long fail = result.results().size() - ok;
            System.out.println(ok + " ok, " + fail + " errors, " + result.results().size() + " total");
        }

        return allResults;
    }

    /**
     * Run a single SQL file and collect results for each statement.
     */
    public static FileResult runFile(Connection conn, Path sqlFile) throws IOException {
        String sql = IO.readString(sqlFile, StandardCharsets.UTF_8);
        // Strip psql backslash commands (\echo, \pset, \set, etc.) since they are not valid SQL
        sql = stripPsqlCommands(sql);
        List<String> statements = splitStatements(sql);
        List<StatementResult> results = new ArrayList<>();

        for (String rawStmt : statements) {
            String stmt = rawStmt.trim();
            if (stmt.isEmpty()) continue;
            // Skip pure comments
            if (isCommentOnly(stmt)) continue;

            results.add(executeStatement(conn, stmt));
        }

        return new FileResult(sqlFile.getFileName().toString(), results);
    }

    /**
     * Strip psql backslash meta-commands (lines starting with \) from SQL text.
     * These are psql-specific directives, not valid SQL.
     */
    static String stripPsqlCommands(String sql) {
        StringBuilder sb = new StringBuilder();
        for (String line : sql.split("\n", -1)) {
            if (Strs.stripLeading(line).startsWith("\\")) continue;
            sb.append(line).append('\n');
        }
        return sb.toString();
    }

    /**
     * Execute a single statement and capture the result.
     */
    static StatementResult executeStatement(Connection conn, String sql) {
        try (Statement s = conn.createStatement()) {
            boolean hasResultSet = s.execute(sql);

            if (hasResultSet) {
                try (ResultSet rs = s.getResultSet()) {
                    ResultSetMetaData md = rs.getMetaData();
                    int colCount = md.getColumnCount();
                    List<String> columns = new ArrayList<>();
                    for (int i = 1; i <= colCount; i++) {
                        columns.add(md.getColumnName(i));
                    }
                    List<List<String>> rows = new ArrayList<>();
                    while (rs.next()) {
                        List<String> row = new ArrayList<>();
                        for (int i = 1; i <= colCount; i++) {
                            String val = rs.getString(i);
                            row.add(val); // null stays as null
                        }
                        rows.add(row);
                    }
                    return new StatementResult(sql, true, null, null, columns, rows, -1);
                }
            } else {
                int updateCount = s.getUpdateCount();
                return new StatementResult(sql, true, null, null, null, null, updateCount);
            }
        } catch (SQLException e) {
            // Normalize the error message by stripping position info and trailing whitespace
            String msg = normalizeError(e.getMessage());
            String state = e.getSQLState();
            // Try to reset connection state after error
            try { conn.createStatement().execute("ROLLBACK"); } catch (SQLException ignored) {}
            return new StatementResult(sql, false, msg, state, null, null, -1);
        } catch (Exception e) {
            // Catch any other exception (protocol errors, etc.) and treat as error
            String msg = "Internal error: " + e.getClass().getSimpleName() + ": " + e.getMessage();
            try { conn.createStatement().execute("ROLLBACK"); } catch (Exception ignored) {}
            return new StatementResult(sql, false, msg, "XX000", null, null, -1);
        }
    }

    /**
     * Normalize error messages for comparison. Strip PG-specific position info
     * and line/column references that differ between implementations.
     */
    static String normalizeError(String msg) {
        if (msg == null) return null;
        // Strip trailing whitespace and newlines
        msg = Strs.strip(msg);
        // Take only the first line (PG often adds Detail/Hint on subsequent lines)
        int nl = msg.indexOf('\n');
        if (nl > 0) msg = Strs.strip(msg.substring(0, nl));
        // Strip "  Position: NNN" suffix (PG adds this, memgres may not)
        msg = msg.replaceAll("\\s+Position:\\s*\\d+$", "");
        return msg;
    }

    /**
     * Save results to a file for later comparison.
     */
    public static void saveResults(List<FileResult> results, Path outputPath) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (FileResult fr : results) {
            sb.append("=== FILE: ").append(fr.fileName()).append(" ===\n");
            for (int i = 0; i < fr.results().size(); i++) {
                StatementResult sr = fr.results().get(i);
                sb.append("--- STMT ").append(i + 1).append(" ---\n");
                // Write SQL (first 200 chars)
                String sqlPreview = Strs.strip(sr.sql().replaceAll("\\s+", " "));
                if (sqlPreview.length() > 200) sqlPreview = sqlPreview.substring(0, 200) + "...";
                sb.append("SQL: ").append(sqlPreview).append("\n");

                if (sr.success()) {
                    if (sr.columns() != null) {
                        sb.append("RESULT: ").append(sr.columns().size()).append(" columns, ").append(sr.rows().size()).append(" rows\n");
                        sb.append("COLUMNS: ").append(String.join(", ", sr.columns())).append("\n");
                        for (List<String> row : sr.rows()) {
                            sb.append("ROW: ").append(formatRow(row)).append("\n");
                        }
                    } else {
                        sb.append("OK: ").append(sr.updateCount()).append(" rows affected\n");
                    }
                } else {
                    sb.append("ERROR [").append(sr.errorState()).append("]: ").append(sr.errorMessage()).append("\n");
                }
            }
            sb.append("\n");
        }
        IO.writeString(outputPath, sb.toString());
    }

    /**
     * Load previously saved results for comparison.
     */
    public static List<FileResult> loadResults(Path inputPath) throws IOException {
        List<FileResult> results = new ArrayList<>();
        String content = IO.readString(inputPath);
        String[] fileSections = content.split("=== FILE: ");

        for (String section : fileSections) {
            if (Strs.isBlank(section)) continue;
            int nameEnd = section.indexOf(" ===\n");
            if (nameEnd < 0) continue;
            String fileName = section.substring(0, nameEnd);
            List<StatementResult> stmts = new ArrayList<>();

            String[] stmtSections = section.split("--- STMT \\d+ ---\n");
            for (String stmtSection : stmtSections) {
                if (Strs.isBlank(stmtSection) || !stmtSection.contains("SQL: ")) continue;
                stmts.add(parseStatementResult(stmtSection));
            }

            results.add(new FileResult(fileName, stmts));
        }
        return results;
    }

    private static StatementResult parseStatementResult(String section) {
        String[] lines = section.split("\n");
        String sql = "";
        boolean success = false;
        String errorMessage = null;
        String errorState = null;
        List<String> columns = null;
        List<List<String>> rows = null;
        int updateCount = -1;

        boolean lastWasRow = false;
        for (String line : lines) {
            if (line.startsWith("SQL: ")) {
                sql = line.substring(5);
                lastWasRow = false;
            } else if (line.startsWith("RESULT: ")) {
                success = true;
                columns = new ArrayList<>();
                rows = new ArrayList<>();
                lastWasRow = false;
            } else if (line.startsWith("COLUMNS: ")) {
                String colStr = line.substring(9).trim();
                columns = colStr.isEmpty() ? new ArrayList<>() : new ArrayList<>(Arrays.asList(colStr.split(", ")));
                lastWasRow = false;
            } else if (line.startsWith("ROW: ")) {
                rows.add(parseRow(line.substring(5)));
                lastWasRow = true;
            } else if (line.startsWith("OK: ")) {
                success = true;
                String countStr = line.substring(4).replace(" rows affected", "");
                updateCount = Integer.parseInt(countStr);
                lastWasRow = false;
            } else if (line.startsWith("ERROR [")) {
                success = false;
                int stateEnd = line.indexOf("]: ");
                errorState = line.substring(7, stateEnd);
                errorMessage = line.substring(stateEnd + 3);
                lastWasRow = false;
            } else if (lastWasRow && rows != null && !rows.isEmpty()) {
                // Continuation of a multiline ROW value; append to previous row's last cell
                List<String> lastRow = rows.get(rows.size() - 1);
                String lastVal = lastRow.get(lastRow.size() - 1);
                lastRow.set(lastRow.size() - 1, lastVal + "\n" + line);
            }
        }

        return new StatementResult(sql, success, errorMessage, errorState, columns, rows, updateCount);
    }

    static String formatRow(List<String> row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.size(); i++) {
            if (i > 0) sb.append(" | ");
            // Escape newlines and backslashes to keep rows on a single line
            String val = row.get(i) == null ? "NULL" : row.get(i)
                    .replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
            sb.append(val);
        }
        return sb.toString();
    }

    private static List<String> parseRow(String line) {
        if (line.isEmpty()) return new ArrayList<>(); // 0-column row
        String[] parts = line.split(" \\| ", -1);
        List<String> row = new ArrayList<>();
        for (String part : parts) {
            if ("NULL".equals(part)) {
                row.add(null);
            } else {
                // Unescape newlines and backslashes
                row.add(part.replace("\\n", "\n").replace("\\r", "\r").replace("\\\\", "\\"));
            }
        }
        return row;
    }

    // === SQL Splitting ===

    static List<String> splitStatements(String sql) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        char stringChar = 0;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            // Dollar quoting
            if (!inString && c == '$') {
                int j = i + 1;
                while (j < sql.length() && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) j++;
                if (j < sql.length() && sql.charAt(j) == '$') {
                    String delimiter = sql.substring(i, j + 1);
                    current.append(delimiter);
                    i = j + 1;
                    int close = sql.indexOf(delimiter, i);
                    if (close >= 0) {
                        current.append(sql, i, close + delimiter.length());
                        i = close + delimiter.length() - 1;
                    } else {
                        current.append(sql.substring(i));
                        i = sql.length() - 1;
                    }
                    continue;
                }
                current.append(c);
                continue;
            }

            if (inString) {
                current.append(c);
                if (c == stringChar) {
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == stringChar) {
                        current.append(sql.charAt(++i));
                    } else {
                        inString = false;
                    }
                }
            } else if (c == '\'' || c == '"') {
                inString = true;
                stringChar = c;
                current.append(c);
            } else if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                int eol = sql.indexOf('\n', i);
                if (eol < 0) eol = sql.length();
                current.append(sql, i, Math.min(eol + 1, sql.length()));
                i = eol;
            } else if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
                int close = sql.indexOf("*/", i + 2);
                if (close >= 0) {
                    current.append(sql, i, close + 2);
                    i = close + 1;
                } else {
                    current.append(sql.substring(i));
                    i = sql.length() - 1;
                }
            } else if (c == ';') {
                String stmt = current.toString().trim();
                if (!stmt.isEmpty()) result.add(stmt);
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        String last = current.toString().trim();
        if (!last.isEmpty()) result.add(last);
        return result;
    }

    static boolean isCommentOnly(String stmt) {
        return Strs.lines(stmt).allMatch(line -> {
            String trimmed = line.trim();
            return trimmed.isEmpty() || trimmed.startsWith("--") || trimmed.startsWith("/*");
        });
    }

    static Path findResourceDir() {
        return findResourceDir("sql-verify");
    }

    static Path findResourceDir(String name) {
        Path dir = Path.of("memgres-core/src/test/resources/" + name);
        if (Files.isDirectory(dir)) return dir;
        dir = Path.of("src/test/resources/" + name);
        if (Files.isDirectory(dir)) return dir;
        dir = Path.of(name);
        if (Files.isDirectory(dir)) return dir;
        throw new RuntimeException("Cannot find " + name + " resource directory");
    }

    /**
     * Compare two result sets and return a list of differences.
     */
    public static List<String> compare(List<FileResult> expected, List<FileResult> actual) {
        List<String> diffs = new ArrayList<>();

        Map<String, FileResult> expectedMap = new LinkedHashMap<>();
        for (FileResult fr : expected) expectedMap.put(fr.fileName(), fr);
        Map<String, FileResult> actualMap = new LinkedHashMap<>();
        for (FileResult fr : actual) actualMap.put(fr.fileName(), fr);

        for (String fileName : expectedMap.keySet()) {
            if (!actualMap.containsKey(fileName)) {
                diffs.add("MISSING FILE: " + fileName);
                continue;
            }
            FileResult exp = expectedMap.get(fileName);
            FileResult act = actualMap.get(fileName);

            int maxStmts = Math.max(exp.results().size(), act.results().size());
            for (int i = 0; i < maxStmts; i++) {
                if (i >= exp.results().size()) {
                    diffs.add(fileName + " stmt " + (i + 1) + ": EXTRA statement in actual");
                    continue;
                }
                if (i >= act.results().size()) {
                    diffs.add(fileName + " stmt " + (i + 1) + ": MISSING statement in actual");
                    continue;
                }

                StatementResult e = exp.results().get(i);
                StatementResult a = act.results().get(i);

                String prefix = fileName + " stmt " + (i + 1);
                String sqlHint = e.sql().length() > 80 ? e.sql().substring(0, 80) + "..." : e.sql();

                // Compare success/failure
                if (e.success() != a.success()) {
                    if (e.success()) {
                        diffs.add(prefix + ": EXPECTED success but GOT error: " + a.errorMessage() + " | SQL: " + sqlHint);
                    } else {
                        diffs.add(prefix + ": EXPECTED error but GOT success | SQL: " + sqlHint);
                    }
                    continue;
                }

                if (!e.success()) {
                    // Both errors; compare SQLSTATE (not the message text, which varies)
                    if (e.errorState() != null && a.errorState() != null && !e.errorState().equals(a.errorState())) {
                        diffs.add(prefix + ": ERROR STATE differs: expected " + e.errorState() + " got " + a.errorState() + " | SQL: " + sqlHint);
                    }
                    continue;
                }

                // Both success; compare result structure
                if (e.columns() != null && a.columns() == null) {
                    diffs.add(prefix + ": EXPECTED result set but got update count | SQL: " + sqlHint);
                    continue;
                }
                if (e.columns() == null && a.columns() != null) {
                    diffs.add(prefix + ": EXPECTED update count but got result set | SQL: " + sqlHint);
                    continue;
                }

                if (e.columns() != null) {
                    // Compare column count
                    if (e.columns().size() != a.columns().size()) {
                        diffs.add(prefix + ": COLUMN COUNT differs: expected " + e.columns().size() + " got " + a.columns().size() + " | SQL: " + sqlHint);
                        continue;
                    }
                    // Compare column names
                    if (!e.columns().equals(a.columns())) {
                        diffs.add(prefix + ": COLUMN NAMES differ: expected " + e.columns() + " got " + a.columns() + " | SQL: " + sqlHint);
                    }
                    // Compare row count
                    if (e.rows().size() != a.rows().size()) {
                        diffs.add(prefix + ": ROW COUNT differs: expected " + e.rows().size() + " got " + a.rows().size() + " | SQL: " + sqlHint);
                        continue;
                    }
                    // Compare row data (with normalization for non-deterministic values)
                    boolean skipRowData = isFullyNonDeterministic(e.sql());
                    if (!skipRowData) {
                        boolean unordered = !hasOrderBy(e.sql()) && e.rows().size() > 1;
                        if (unordered) {
                            // Set-based comparison for unordered results
                            List<List<String>> eNorm = e.rows().stream().map(SqlVerifyHarness::normalizeRow).sorted(Comparator.comparing(Object::toString)).collect(Collectors.toList());
                            List<List<String>> aNorm = a.rows().stream().map(SqlVerifyHarness::normalizeRow).sorted(Comparator.comparing(Object::toString)).collect(Collectors.toList());
                            if (!Objects.equals(eNorm, aNorm)) {
                                diffs.add(prefix + ": UNORDERED DATA differs | SQL: " + sqlHint);
                            }
                        } else {
                            for (int r = 0; r < e.rows().size(); r++) {
                                List<String> eRow = normalizeRow(e.rows().get(r));
                                List<String> aRow = normalizeRow(a.rows().get(r));
                                if (!Objects.equals(eRow, aRow)) {
                                    // Double-check with raw string comparison (avoids false positives from pipe-in-value parsing)
                                    String eRaw = normalizeValue(formatRow(e.rows().get(r)));
                                    String aRaw = normalizeValue(formatRow(a.rows().get(r)));
                                    if (!Objects.equals(eRaw, aRaw)) {
                                        diffs.add(prefix + " row " + (r + 1) + ": DATA differs: expected " + formatRow(e.rows().get(r)) + " got " + formatRow(a.rows().get(r)) + " | SQL: " + sqlHint);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Compare update counts
                    if (e.updateCount() != a.updateCount()) {
                        diffs.add(prefix + ": UPDATE COUNT differs: expected " + e.updateCount() + " got " + a.updateCount() + " | SQL: " + sqlHint);
                    }
                }
            }
        }

        return diffs;
    }

    /**
     * Returns true if the SQL statement produces entirely non-deterministic output
     * that should not be compared at the row-data level (e.g. SELECT RANDOM(),
     * SELECT CURRENT_USER). Statements that mix deterministic and non-deterministic
     * columns are handled by value-level normalization instead.
     */
    /** Check if a SQL query has an ORDER BY clause at the outermost level. */
    private static boolean hasOrderBy(String sql) {
        String upper = sql.toUpperCase().replaceAll("\\s+", " ");
        // Find ORDER BY that's NOT inside parentheses (subquery or OVER clause)
        int depth = 0;
        for (int i = 0; i < upper.length(); i++) {
            char c = upper.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && i + 8 <= upper.length() && upper.substring(i, i + 8).equals("ORDER BY")) {
                return true;
            }
        }
        return false;
    }

    private static boolean isFullyNonDeterministic(String sql) {
        String upper = sql.toUpperCase().replaceAll("\\s+", " ").trim();
        // Strip leading SQL comments (may be on same line after collapsing whitespace)
        upper = upper.replaceAll("--[^\\n]*?(?=\\bSELECT\\b)", "").trim();
        while (upper.startsWith("--")) {
            int nl = upper.indexOf('\n');
            if (nl < 0) return false;
            upper = upper.substring(nl + 1).trim();
        }
        // Fully non-deterministic: output differs every run
        return upper.matches("SELECT\\s+RANDOM\\(\\).*")
                || upper.matches("SELECT\\s+CURRENT_USER.*")
                || upper.matches("SELECT\\s+CURRENT_DATABASE\\(\\).*")
                || upper.matches("SELECT\\s+GEN_RANDOM_UUID\\(\\).*")
                || upper.startsWith("EXPLAIN ANALYZE ")
                || upper.contains("TABLESAMPLE ");
    }

    // ---- Value normalization for comparison ----

    private static final java.util.regex.Pattern TIMESTAMP_PATTERN = java.util.regex.Pattern.compile(
            "\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?(([+-]\\d{2}(:\\d{2})?)|Z)?");
    private static final java.util.regex.Pattern TIME_ONLY_PATTERN = java.util.regex.Pattern.compile(
            "^\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?(([+-]\\d{2}(:\\d{2})?)|Z)?$");
    private static final java.util.regex.Pattern UUID_PATTERN = java.util.regex.Pattern.compile(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    /**
     * Normalize a row for comparison: replace non-deterministic values with placeholders.
     * This allows comparing structure and deterministic values while ignoring
     * timestamps, UUIDs, and random numbers that inherently differ between runs.
     */
    private static List<String> normalizeRow(List<String> row) {
        if (row == null) return null;
        List<String> normalized = new ArrayList<>(row.size());
        for (String val : row) {
            normalized.add(normalizeValue(val));
        }
        return normalized;
    }

    private static final java.util.regex.Pattern NUMERIC_PATTERN = java.util.regex.Pattern.compile(
            "^-?\\d+\\.\\d{8,}$");
    private static final java.util.regex.Pattern DATE_ONLY_PATTERN = java.util.regex.Pattern.compile(
            "^\\d{4}-\\d{2}-\\d{2}$");

    private static String normalizeValue(String val) {
        if (val == null) return null;

        // Replace full UUIDs with placeholder
        val = UUID_PATTERN.matcher(val).replaceAll("<UUID>");

        // Replace timestamps with placeholder
        val = TIMESTAMP_PATTERN.matcher(val).replaceAll("<TIMESTAMP>");

        // Replace standalone date values (YYYY-MM-DD from CURRENT_DATE etc.)
        if (DATE_ONLY_PATTERN.matcher(val).matches()) {
            return "<DATE>";
        }

        // Replace standalone time values (HH:MM:SS.nnn+TZ)
        if (TIME_ONLY_PATTERN.matcher(val).matches()) {
            return "<TIME>";
        }

        // Normalize high-precision numerics: truncate to 12 significant digits
        // PG's numeric precision varies by computation; 12 sig figs is a safe match threshold
        if (NUMERIC_PATTERN.matcher(val).matches()) {
            try {
                java.math.BigDecimal bd = new java.math.BigDecimal(val);
                bd = bd.round(new java.math.MathContext(12));
                return bd.toPlainString();
            } catch (Exception e) { /* not a number */ }
        }
        // Normalize whole-number floats: "12.0" → "12" (JDBC driver may add .0 based on column type)
        if (val.matches("-?\\d+\\.0+$")) {
            return val.replaceAll("\\.0+$", "");
        }

        // Normalize multiline values (newlines in data break the line-based comparison format)
        if (val.contains("\n") || val.contains("\r")) {
            val = val.replace("\r\n", "<NL>").replace("\n", "<NL>").replace("\r", "<NL>");
        }

        return val;
    }
}
