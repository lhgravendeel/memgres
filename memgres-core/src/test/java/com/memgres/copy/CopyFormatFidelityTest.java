package com.memgres.copy;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Format-fidelity tests for COPY TO STDOUT / COPY FROM STDIN over the wire protocol.
 *
 * Covers PostgreSQL-exact semantics for:
 *  - text-format NULL marker matching against RAW (pre-unescape) input:
 *    {@code \N} is NULL, {@code \\N} is the literal two-char string {@code \N}
 *  - the full text-format escape set: \b \f \n \r \t \v \\ , octal and hex escapes,
 *    unknown escapes taken literally, data ending in a lone backslash
 *  - empty input lines as data rows (text: one empty-string field; CSV: one NULL field),
 *    with a trailing final newline not creating a phantom row
 *  - non-default DELIMITER being escaped on text output and unescaped on input
 *  - CSV QUOTE/ESCAPE options honored symmetrically on input and output
 *  - the \. end-of-data marker mid-stream (text and CSV)
 *  - multi-byte UTF-8 data round-trips
 *  - wrong-column-count rows raising 22P04
 *  - HEADER / HEADER MATCH on COPY FROM
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CopyFormatFidelityTest {

    static Memgres memgres;
    static Connection conn;
    static CopyManager cm;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        cm = new CopyManager(conn.unwrap(BaseConnection.class));
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String copyOut(String sql) throws SQLException, IOException {
        StringWriter sw = new StringWriter();
        cm.copyOut(sql, sw);
        return sw.toString();
    }

    private long copyIn(String sql, String data) throws SQLException, IOException {
        return cm.copyIn(sql, new StringReader(data));
    }

    private String singleValue(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private int rowCount(String table) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    // ========================================================================
    // 1. Text-format NULL marker: raw comparison before unescaping
    // ========================================================================

    @Test @Order(10)
    void text_backslashN_escaped_isLiteralData_notNull() throws Exception {
        exec("CREATE TABLE ff_bsn(id int, val text)");
        // Input field \\N (escaped backslash + N) is the literal 2-char string \N,
        // while bare \N is NULL. PG matches the null marker against the RAW field.
        copyIn("COPY ff_bsn FROM STDIN", "1\t\\\\N\n2\t\\N\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ff_bsn ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("\\N", rs.getString("val"), "\\\\N must be the literal string \\N, not NULL");
            assertTrue(rs.next());
            assertNull(rs.getString("val"), "bare \\N must be NULL");
        }
        exec("DROP TABLE ff_bsn");
    }

    @Test @Order(11)
    void text_literalBackslashN_roundTrips() throws Exception {
        exec("CREATE TABLE ff_rt1(id int, val text)");
        copyIn("COPY ff_rt1 FROM STDIN", "1\t\\\\N\n2\t\\N\n");

        String out = copyOut("COPY ff_rt1 TO STDOUT");
        assertEquals("1\t\\\\N\n2\t\\N\n", out,
                "literal \\N must export as \\\\N; NULL must export as \\N");

        exec("CREATE TABLE ff_rt1b(id int, val text)");
        copyIn("COPY ff_rt1b FROM STDIN", out);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ff_rt1b ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("\\N", rs.getString("val"), "literal \\N string must survive a full round-trip");
            assertTrue(rs.next());
            assertNull(rs.getString("val"), "NULL must survive a full round-trip");
        }
        exec("DROP TABLE ff_rt1");
        exec("DROP TABLE ff_rt1b");
    }

    @Test @Order(12)
    void text_customNullMarker_rawComparison() throws Exception {
        exec("CREATE TABLE ff_cnull(id int, val text)");
        // NULL marker 'NA': raw field NA -> NULL; \x4EA unescapes to 'NA' but raw differs -> data
        copyIn("COPY ff_cnull FROM STDIN WITH (NULL 'NA')", "1\tNA\n2\t\\x4EA\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ff_cnull ORDER BY id")) {
            assertTrue(rs.next());
            assertNull(rs.getString("val"), "raw NA must match custom NULL marker");
            assertTrue(rs.next());
            assertEquals("NA", rs.getString("val"),
                    "field whose RAW text differs from the marker must stay data even if it unescapes to the marker");
        }
        exec("DROP TABLE ff_cnull");
    }

    // ========================================================================
    // 2. Full text-format escape set, both directions
    // ========================================================================

    @Test @Order(20)
    void text_fullEscapeSet_input() throws Exception {
        exec("CREATE TABLE ff_esc(id int, val text)");
        copyIn("COPY ff_esc FROM STDIN",
                "1\ta\\bb\n" +      // \b backspace
                "2\ta\\fb\n" +      // \f form feed
                "3\ta\\nb\n" +      // \n newline
                "4\ta\\rb\n" +      // \r carriage return
                "5\ta\\tb\n" +      // \t tab
                "6\ta\\vb\n" +      // \v vertical tab
                "7\ta\\\\b\n");     // \\ backslash
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ff_esc ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("a\bb", rs.getString(2));
            assertTrue(rs.next()); assertEquals("a\fb", rs.getString(2));
            assertTrue(rs.next()); assertEquals("a\nb", rs.getString(2));
            assertTrue(rs.next()); assertEquals("a\rb", rs.getString(2));
            assertTrue(rs.next()); assertEquals("a\tb", rs.getString(2));
            assertTrue(rs.next()); assertEquals("ab", rs.getString(2));
            assertTrue(rs.next()); assertEquals("a\\b", rs.getString(2));
        }
        exec("DROP TABLE ff_esc");
    }

    @Test @Order(21)
    void text_octalAndHexEscapes_input() throws Exception {
        exec("CREATE TABLE ff_oct(id int, val text)");
        copyIn("COPY ff_oct FROM STDIN",
                "1\t\\101BC\n" +   // octal 101 = 'A'
                "2\t\\x41BC\n" +   // hex 41 = 'A', then literal 'BC' (max 2 hex digits)
                "3\t\\7Q\n" +      // single octal digit
                "4\t\\xZ\n");      // \x with no hex digit: literal 'x'
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ff_oct ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("ABC", rs.getString(2));
            assertTrue(rs.next()); assertEquals("ABC", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Q", rs.getString(2));
            assertTrue(rs.next()); assertEquals("xZ", rs.getString(2));
        }
        exec("DROP TABLE ff_oct");
    }

    @Test @Order(22)
    void text_unknownEscape_backslashDropped() throws Exception {
        exec("CREATE TABLE ff_unk(id int, val text)");
        // \q is not a known escape: the backslash is dropped and 'q' kept.
        // \N in the MIDDLE of data (raw field != null marker) unescapes to plain 'N'.
        copyIn("COPY ff_unk FROM STDIN", "1\ta\\qb\n2\tx\\Ny\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ff_unk ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("aqb", rs.getString(2));
            assertTrue(rs.next()); assertEquals("xNy", rs.getString(2));
        }
        exec("DROP TABLE ff_unk");
    }

    @Test @Order(23)
    void text_loneTrailingBackslash_keptLiterally() throws Exception {
        exec("CREATE TABLE ff_lb(id int, val text)");
        copyIn("COPY ff_lb FROM STDIN", "1\tabc\\\n");
        assertEquals("abc\\", singleValue("SELECT val FROM ff_lb"));
        exec("DROP TABLE ff_lb");
    }

    @Test @Order(24)
    void text_controlChars_escapedOnOutput_andRoundTrip() throws Exception {
        exec("CREATE TABLE ff_out(id int, val text)");
        exec("INSERT INTO ff_out VALUES (1, 'a'||chr(8)||'b'||chr(12)||'c'||chr(10)||'d'||chr(13)" +
                "||'e'||chr(9)||'f'||chr(11)||'g'||chr(92)||'h')");
        String out = copyOut("COPY ff_out TO STDOUT");
        assertEquals("1\ta\\bb\\fc\\nd\\re\\tf\\vg\\\\h\n", out,
                "output must escape \\b \\f \\n \\r \\t \\v and backslash");

        exec("CREATE TABLE ff_out2(id int, val text)");
        copyIn("COPY ff_out2 FROM STDIN", out);
        assertEquals("a\bb\fc\nd\re\tfg\\h", singleValue("SELECT val FROM ff_out2"),
                "control characters must survive a full round-trip");
        exec("DROP TABLE ff_out");
        exec("DROP TABLE ff_out2");
    }

    // ========================================================================
    // 3. Empty lines are data rows
    // ========================================================================

    @Test @Order(30)
    void text_emptyLine_singleColumn_isEmptyStringRow() throws Exception {
        exec("CREATE TABLE ff_el1(val text)");
        long rows = copyIn("COPY ff_el1 FROM STDIN", "a\n\nb\n");
        assertEquals(3, rows, "an empty line in text format is a valid one-field row");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM ff_el1")) {
            int empties = 0, data = 0;
            while (rs.next()) {
                String v = rs.getString(1);
                assertNotNull(v, "empty text field is '' (default NULL marker is \\N), not NULL");
                if (v.isEmpty()) empties++; else data++;
            }
            assertEquals(1, empties);
            assertEquals(2, data);
        }
        exec("DROP TABLE ff_el1");
    }

    @Test @Order(31)
    void text_emptyLine_withEmptyNullMarker_isNullRow() throws Exception {
        exec("CREATE TABLE ff_el2(val text)");
        copyIn("COPY ff_el2 FROM STDIN WITH (NULL '')", "\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM ff_el2")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1), "with NULL '' an empty field is NULL");
        }
        exec("DROP TABLE ff_el2");
    }

    @Test @Order(32)
    void text_emptyLine_multiColumn_raises22P04() throws Exception {
        exec("CREATE TABLE ff_el3(id int, val text)");
        SQLException ex = assertThrows(SQLException.class,
                () -> copyIn("COPY ff_el3 FROM STDIN", "1\ta\n\n"));
        assertEquals("22P04", ex.getSQLState(), "missing data must raise 22P04");
        assertTrue(ex.getMessage().contains("missing data for column"),
                "PG-like message expected, got: " + ex.getMessage());
        exec("DROP TABLE ff_el3");
    }

    @Test @Order(33)
    void csv_emptyLine_isNullRow() throws Exception {
        exec("CREATE TABLE ff_el4(val text)");
        long rows = copyIn("COPY ff_el4 FROM STDIN WITH (FORMAT csv)", "a\n\nb\n");
        assertEquals(3, rows, "an empty CSV line is a one-field row");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM ff_el4 WHERE val IS NULL")) {
            rs.next();
            assertEquals(1, rs.getInt(1), "the empty CSV line is a single NULL field");
        }
        exec("DROP TABLE ff_el4");
    }

    @Test @Order(34)
    void csv_emptyLine_multiColumn_raises22P04() throws Exception {
        exec("CREATE TABLE ff_el5(id int, val text)");
        SQLException ex = assertThrows(SQLException.class,
                () -> copyIn("COPY ff_el5 FROM STDIN WITH (FORMAT csv)", "1,a\n\n"));
        assertEquals("22P04", ex.getSQLState());
        exec("DROP TABLE ff_el5");
    }

    @Test @Order(35)
    void trailingNewline_noPhantomRow_bothFormats() throws Exception {
        exec("CREATE TABLE ff_tn(val text)");
        assertEquals(2, copyIn("COPY ff_tn FROM STDIN", "a\nb\n"),
                "text: trailing terminator must not create a phantom row");
        assertEquals(2, copyIn("COPY ff_tn FROM STDIN", "a\nb"),
                "text: last line without newline still counts");
        assertEquals(2, copyIn("COPY ff_tn FROM STDIN WITH (FORMAT csv)", "a\nb\n"),
                "csv: trailing terminator must not create a phantom row");
        assertEquals(2, copyIn("COPY ff_tn FROM STDIN WITH (FORMAT csv)", "a\nb"),
                "csv: last line without newline still counts");
        assertEquals(0, copyIn("COPY ff_tn FROM STDIN", ""), "empty input has no rows");
        assertEquals(0, copyIn("COPY ff_tn FROM STDIN WITH (FORMAT csv)", ""), "empty csv input has no rows");
        exec("DROP TABLE ff_tn");
    }

    // ========================================================================
    // 4. Custom DELIMITER escaping in text format
    // ========================================================================

    @Test @Order(40)
    void text_customDelimiter_escapedOnOutput_roundTrips() throws Exception {
        exec("CREATE TABLE ff_dl(id int, val text)");
        exec("INSERT INTO ff_dl VALUES (1, 'a|b'), (2, 'plain')");
        String out = copyOut("COPY ff_dl TO STDOUT WITH (DELIMITER '|')");
        assertEquals("1|a\\|b\n2|plain\n", out,
                "the active delimiter must be backslash-escaped in output");

        exec("CREATE TABLE ff_dl2(id int, val text)");
        copyIn("COPY ff_dl2 FROM STDIN WITH (DELIMITER '|')", out);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ff_dl2 ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("a|b", rs.getString(2));
            assertTrue(rs.next()); assertEquals("plain", rs.getString(2));
        }
        exec("DROP TABLE ff_dl");
        exec("DROP TABLE ff_dl2");
    }

    @Test @Order(41)
    void text_escapedDelimiter_onInput_isLiteral() throws Exception {
        exec("CREATE TABLE ff_dl3(id int, val text)");
        copyIn("COPY ff_dl3 FROM STDIN WITH (DELIMITER '|')", "1|x\\|y\n");
        assertEquals("x|y", singleValue("SELECT val FROM ff_dl3"));
        exec("DROP TABLE ff_dl3");
    }

    // ========================================================================
    // 5. CSV QUOTE / ESCAPE options on input (symmetric with output)
    // ========================================================================

    @Test @Order(50)
    void csv_customQuote_doubled_roundTrips() throws Exception {
        exec("CREATE TABLE ff_q1(id int, val text)");
        exec("INSERT INTO ff_q1 VALUES (1, 'a$b'), (2, 'x,y')");
        String out = copyOut("COPY ff_q1 TO STDOUT WITH (FORMAT csv, QUOTE '$')");
        assertEquals("1,$a$$b$\n2,$x,y$\n", out,
                "custom quote char must be used and doubled when embedded");

        exec("CREATE TABLE ff_q2(id int, val text)");
        copyIn("COPY ff_q2 FROM STDIN WITH (FORMAT csv, QUOTE '$')", out);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ff_q2 ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("a$b", rs.getString(2));
            assertTrue(rs.next()); assertEquals("x,y", rs.getString(2));
        }
        exec("DROP TABLE ff_q1");
        exec("DROP TABLE ff_q2");
    }

    @Test @Order(51)
    void csv_customEscape_distinctFromQuote_roundTrips() throws Exception {
        exec("CREATE TABLE ff_e1(id int, val text)");
        exec("INSERT INTO ff_e1 VALUES (1, 'has\"quote'), (2, E'has\\\\backslash')");
        String out = copyOut("COPY ff_e1 TO STDOUT WITH (FORMAT csv, ESCAPE '\\')");
        // PG only quotes fields containing the QUOTE char, not the ESCAPE char alone.
        // "has\"quote" is quoted (contains "), "has\backslash" is NOT quoted (only contains \).
        assertEquals("1,\"has\\\"quote\"\n2,has\\backslash\n", out,
                "with ESCAPE distinct from QUOTE, only embedded quote char triggers quoting");

        exec("CREATE TABLE ff_e2(id int, val text)");
        copyIn("COPY ff_e2 FROM STDIN WITH (FORMAT csv, ESCAPE '\\')", out);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ff_e2 ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("has\"quote", rs.getString(2));
            assertTrue(rs.next()); assertEquals("has\\backslash", rs.getString(2));
        }
        exec("DROP TABLE ff_e1");
        exec("DROP TABLE ff_e2");
    }

    @Test @Order(52)
    void csv_quotedEmpty_vs_unquotedEmpty() throws Exception {
        exec("CREATE TABLE ff_qe(id int, val text)");
        copyIn("COPY ff_qe FROM STDIN WITH (FORMAT csv)", "1,\n2,\"\"\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ff_qe ORDER BY id")) {
            assertTrue(rs.next());
            assertNull(rs.getString(2), "unquoted empty CSV field is NULL");
            assertTrue(rs.next());
            assertEquals("", rs.getString(2), "quoted empty CSV field is the empty string");
        }
        exec("DROP TABLE ff_qe");
    }

    @Test @Order(53)
    void csv_quotedEmpty_customQuote() throws Exception {
        exec("CREATE TABLE ff_qec(id int, val text)");
        copyIn("COPY ff_qec FROM STDIN WITH (FORMAT csv, QUOTE '$')", "1,\n2,$$\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ff_qec ORDER BY id")) {
            assertTrue(rs.next());
            assertNull(rs.getString(2));
            assertTrue(rs.next());
            assertEquals("", rs.getString(2), "custom-quoted empty field is the empty string");
        }
        exec("DROP TABLE ff_qec");
    }

    @Test @Order(54)
    void csv_embeddedNewline_customQuote_roundTrips() throws Exception {
        exec("CREATE TABLE ff_nl(id int, val text)");
        exec("INSERT INTO ff_nl VALUES (1, E'line1\\nline2'), (2, 'simple')");
        String out = copyOut("COPY ff_nl TO STDOUT WITH (FORMAT csv, QUOTE '$')");
        assertTrue(out.contains("$line1\nline2$"), "embedded newline must be inside custom quotes: " + out);

        exec("CREATE TABLE ff_nl2(id int, val text)");
        copyIn("COPY ff_nl2 FROM STDIN WITH (FORMAT csv, QUOTE '$')", out);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ff_nl2 ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("line1\nline2", rs.getString(2), "embedded newline must survive with custom quote");
            assertTrue(rs.next());
            assertEquals("simple", rs.getString(2));
        }
        exec("DROP TABLE ff_nl");
        exec("DROP TABLE ff_nl2");
    }

    @Test @Order(55)
    void csv_valueEqualToNullMarker_isQuotedOnOutput() throws Exception {
        exec("CREATE TABLE ff_nm(id int, val text)");
        exec("INSERT INTO ff_nm VALUES (1, 'NA'), (2, NULL)");
        String out = copyOut("COPY ff_nm TO STDOUT WITH (FORMAT csv, NULL 'NA')");
        assertEquals("1,\"NA\"\n2,NA\n", out,
                "data equal to the NULL marker must be quoted so it stays distinguishable");

        exec("CREATE TABLE ff_nm2(id int, val text)");
        copyIn("COPY ff_nm2 FROM STDIN WITH (FORMAT csv, NULL 'NA')", out);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ff_nm2 ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("NA", rs.getString(2));
            assertTrue(rs.next()); assertNull(rs.getString(2));
        }
        exec("DROP TABLE ff_nm");
        exec("DROP TABLE ff_nm2");
    }

    @Test @Order(56)
    void csv_trailingDelimiter_withCustomNull_isEmptyString() throws Exception {
        exec("CREATE TABLE ff_td(id int, val text)");
        // With NULL 'NA', an empty unquoted field is the empty string, not NULL.
        copyIn("COPY ff_td FROM STDIN WITH (FORMAT csv, NULL 'NA')", "1,\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM ff_td")) {
            assertTrue(rs.next());
            assertEquals("", rs.getString(1),
                    "empty field is only NULL when it matches the NULL marker");
        }
        exec("DROP TABLE ff_td");
    }

    // ========================================================================
    // 6. End-of-data marker \.
    // ========================================================================

    @Test @Order(60)
    void text_endOfDataMarker_stopsMidStream() throws Exception {
        exec("CREATE TABLE ff_eod(id int, val text)");
        long rows = copyIn("COPY ff_eod FROM STDIN", "1\ta\n\\.\n2\tb\n");
        assertEquals(1, rows, "rows after \\. must be ignored");
        assertEquals(1, rowCount("ff_eod"));
        exec("DROP TABLE ff_eod");
    }

    @Test @Order(61)
    void csv_endOfDataMarker_stopsMidStream() throws Exception {
        exec("CREATE TABLE ff_eodc(id int, val text)");
        long rows = copyIn("COPY ff_eodc FROM STDIN WITH (FORMAT csv)", "1,a\n\\.\n2,b\n");
        assertEquals(1, rows, "rows after \\. must be ignored in CSV too");
        assertEquals(1, rowCount("ff_eodc"));
        exec("DROP TABLE ff_eodc");
    }

    @Test @Order(62)
    void csv_singleColumn_backslashDotData_roundTrips() throws Exception {
        exec("CREATE TABLE ff_eodd(val text)");
        exec("INSERT INTO ff_eodd VALUES (E'\\\\.')");
        String out = copyOut("COPY ff_eodd TO STDOUT WITH (FORMAT csv)");
        assertEquals("\"\\.\"\n", out,
                "a lone \\. field must be quoted so it does not read back as end-of-data");

        exec("CREATE TABLE ff_eodd2(val text)");
        long rows = copyIn("COPY ff_eodd2 FROM STDIN WITH (FORMAT csv)", out);
        assertEquals(1, rows);
        assertEquals("\\.", singleValue("SELECT val FROM ff_eodd2"));
        exec("DROP TABLE ff_eodd");
        exec("DROP TABLE ff_eodd2");
    }

    // ========================================================================
    // 7. UTF-8 data
    // ========================================================================

    @Test @Order(70)
    void utf8_multiByteData_textFormat_roundTrips() throws Exception {
        exec("CREATE TABLE ff_u8(id int, val text)");
        copyIn("COPY ff_u8 FROM STDIN", "1\tcafé\n2\t日本語テスト\n3\t🎉🚀 emoji\n");
        String out = copyOut("COPY ff_u8 TO STDOUT");
        assertEquals("1\tcafé\n2\t日本語テスト\n3\t🎉🚀 emoji\n", out,
                "multi-byte UTF-8 must survive text-format round-trip");
        exec("DROP TABLE ff_u8");
    }

    @Test @Order(71)
    void utf8_largePayload_multipleCopyDataChunks() throws Exception {
        // Large payload forces the driver to split CopyData messages; multi-byte
        // chars must not be corrupted at buffer boundaries (decode happens after
        // full buffering).
        exec("CREATE TABLE ff_u8b(id int, val text)");
        StringBuilder data = new StringBuilder();
        StringBuilder val = new StringBuilder();
        for (int i = 0; i < 3000; i++) val.append("héllo日本🎉");
        for (int i = 1; i <= 5; i++) data.append(i).append('\t').append(val).append('\n');
        // Send as raw UTF-8 bytes: the byte stream is chunked into multiple CopyData
        // messages by the driver, so multi-byte sequences can straddle chunk borders.
        long rows = cm.copyIn("COPY ff_u8b FROM STDIN",
                new ByteArrayInputStream(data.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertEquals(5, rows);
        assertEquals(val.toString(), singleValue("SELECT val FROM ff_u8b WHERE id = 3"),
                "multi-byte data split across CopyData chunks must round-trip intact");
        exec("DROP TABLE ff_u8b");
    }

    // ========================================================================
    // 8. Wrong column count => 22P04
    // ========================================================================

    @Test @Order(80)
    void wrongColumnCount_errors_22P04_withPgLikeMessages() throws Exception {
        exec("CREATE TABLE ff_cc(id int, name text, city text)");
        SQLException tooFew = assertThrows(SQLException.class,
                () -> copyIn("COPY ff_cc FROM STDIN", "1\talice\n"));
        assertEquals("22P04", tooFew.getSQLState());
        assertTrue(tooFew.getMessage().contains("missing data for column \"city\""),
                "expected PG-like missing-data message, got: " + tooFew.getMessage());

        SQLException tooMany = assertThrows(SQLException.class,
                () -> copyIn("COPY ff_cc FROM STDIN", "1\talice\tNYC\textra\n"));
        assertEquals("22P04", tooMany.getSQLState());
        assertTrue(tooMany.getMessage().contains("extra data after last expected column"),
                "expected PG-like extra-data message, got: " + tooMany.getMessage());
        exec("DROP TABLE ff_cc");
    }

    // ========================================================================
    // 9. HEADER option on COPY FROM
    // ========================================================================

    @Test @Order(90)
    void header_textFormat_skipped() throws Exception {
        exec("CREATE TABLE ff_h1(id int, name text)");
        long rows = copyIn("COPY ff_h1 FROM STDIN WITH (HEADER)", "id\tname\n1\talice\n");
        assertEquals(1, rows, "header line must be skipped, not inserted");
        assertEquals("alice", singleValue("SELECT name FROM ff_h1"));
        exec("DROP TABLE ff_h1");
    }

    @Test @Order(91)
    void headerMatch_validatesAgainstTableColumns() throws Exception {
        exec("CREATE TABLE ff_h2(id int, name text)");
        // matching header (no explicit column list: table columns are the reference)
        long rows = copyIn("COPY ff_h2 FROM STDIN WITH (FORMAT csv, HEADER MATCH)", "id,name\n1,alice\n");
        assertEquals(1, rows);

        SQLException ex = assertThrows(SQLException.class,
                () -> copyIn("COPY ff_h2 FROM STDIN WITH (FORMAT csv, HEADER MATCH)", "id,wrong\n2,bob\n"));
        assertEquals("22P04", ex.getSQLState(), "HEADER MATCH mismatch must raise 22P04");
        exec("DROP TABLE ff_h2");
    }

    @Test @Order(92)
    void headerMatch_withColumnList() throws Exception {
        exec("CREATE TABLE ff_h3(id int, name text, city text)");
        long rows = copyIn("COPY ff_h3(name, city) FROM STDIN WITH (FORMAT csv, HEADER MATCH)",
                "name,city\nalice,NYC\n");
        assertEquals(1, rows);

        assertThrows(SQLException.class,
                () -> copyIn("COPY ff_h3(name, city) FROM STDIN WITH (FORMAT csv, HEADER MATCH)",
                        "city,name\nNYC,alice\n"),
                "column order mismatch must fail HEADER MATCH");
        exec("DROP TABLE ff_h3");
    }

    // ========================================================================
    // 10. CRLF and mixed terminators
    // ========================================================================

    @Test @Order(100)
    void crlf_terminators_bothFormats() throws Exception {
        exec("CREATE TABLE ff_crlf(id int, val text)");
        assertEquals(2, copyIn("COPY ff_crlf FROM STDIN", "1\ta\r\n2\tb\r\n"));
        assertEquals(2, copyIn("COPY ff_crlf FROM STDIN WITH (FORMAT csv)", "3,c\r\n4,d\r\n"));
        assertEquals(4, rowCount("ff_crlf"));
        exec("DROP TABLE ff_crlf");
    }

    @Test @Order(101)
    void csv_quotedCarriageReturn_preserved() throws Exception {
        exec("CREATE TABLE ff_qcr(id int, val text)");
        // \r inside a quoted CSV field is data, not a line terminator
        copyIn("COPY ff_qcr FROM STDIN WITH (FORMAT csv)", "1,\"a\rb\"\n");
        assertEquals("a\rb", singleValue("SELECT val FROM ff_qcr"));
        exec("DROP TABLE ff_qcr");
    }

    // ========================================================================
    // 11. DEFAULT marker interaction with escaping (text format)
    // ========================================================================

    @Test @Order(110)
    void text_defaultMarker_rawComparison() throws Exception {
        exec("CREATE TABLE ff_def(id int, val text DEFAULT 'DFLT')");
        copyIn("COPY ff_def(id, val) FROM STDIN WITH (DEFAULT '\\D')", "1\thello\n2\t\\D\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ff_def ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("hello", rs.getString(2));
            assertTrue(rs.next()); assertEquals("DFLT", rs.getString(2),
                    "the raw DEFAULT marker must substitute the column default");
        }
        exec("DROP TABLE ff_def");
    }
}
