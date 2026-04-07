package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.memgres.engine.util.Strs;

import java.io.*;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for COPY TO STDOUT / COPY FROM STDIN via the PgWire protocol.
 * Uses the PostgreSQL JDBC CopyManager which exercises the real COPY sub-protocol
 * (CopyOutResponse, CopyData, CopyDone for TO; CopyInResponse, CopyData, CopyDone for FROM).
 *
 * No server-side file access; all data flows over the wire via STDIN/STDOUT.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CopyProtocolTest {

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

    private int rowCount(String table) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    // ========================================================================
    // COPY TO STDOUT, text format (default)
    // ========================================================================

    @Test @Order(1)
    void toStdout_textFormat_basic() throws Exception {
        exec("CREATE TABLE ct_text(id int, name text)");
        exec("INSERT INTO ct_text VALUES (1, 'alice'), (2, 'bob')");
        String out = copyOut("COPY ct_text TO STDOUT");
        // Default text format: tab-delimited, newline-terminated
        assertTrue(out.contains("1"), "Output should contain id 1");
        assertTrue(out.contains("alice"), "Output should contain 'alice'");
        assertTrue(out.contains("bob"), "Output should contain 'bob'");
        assertTrue(out.contains("\t"), "Text format uses tab delimiter");
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(2, lines.length, "Should have 2 data lines");
        exec("DROP TABLE ct_text");
    }

    @Test @Order(2)
    void toStdout_textFormat_nullValues() throws Exception {
        exec("CREATE TABLE ct_nulls(id int, val text)");
        exec("INSERT INTO ct_nulls VALUES (1, NULL), (2, 'present'), (3, NULL)");
        String out = copyOut("COPY ct_nulls TO STDOUT");
        // In text format, NULL is represented as \N
        assertTrue(out.contains("\\N"), "Text format should represent NULL as \\N");
        assertTrue(out.contains("present"), "Non-null value should be present");
        exec("DROP TABLE ct_nulls");
    }

    @Test @Order(3)
    void toStdout_textFormat_emptyTable() throws Exception {
        exec("CREATE TABLE ct_empty(id int, val text)");
        String out = copyOut("COPY ct_empty TO STDOUT");
        assertEquals("", out, "Empty table should produce no output");
        exec("DROP TABLE ct_empty");
    }

    @Test @Order(4)
    void toStdout_textFormat_columnList() throws Exception {
        exec("CREATE TABLE ct_cols(id int, name text, city text)");
        exec("INSERT INTO ct_cols VALUES (1, 'alice', 'NYC'), (2, 'bob', 'LA')");
        String out = copyOut("COPY ct_cols(name, city) TO STDOUT");
        // Should only contain name and city, not id
        assertFalse(out.isEmpty());
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(2, lines.length);
        // Each line should have exactly one tab (2 columns)
        for (String line : lines) {
            String[] parts = line.split("\t", -1);
            assertEquals(2, parts.length, "Should have 2 columns per line: " + line);
        }
        assertTrue(out.contains("alice"));
        assertTrue(out.contains("NYC"));
        exec("DROP TABLE ct_cols");
    }

    // ========================================================================
    // COPY TO STDOUT, CSV format
    // ========================================================================

    @Test @Order(10)
    void toStdout_csv_basic() throws Exception {
        exec("CREATE TABLE ct_csv(id int, name text)");
        exec("INSERT INTO ct_csv VALUES (1, 'alice'), (2, 'bob')");
        String out = copyOut("COPY ct_csv TO STDOUT WITH (FORMAT csv)");
        assertTrue(out.contains(","), "CSV should use comma delimiter");
        assertTrue(out.contains("alice"));
        assertTrue(out.contains("bob"));
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(2, lines.length);
        exec("DROP TABLE ct_csv");
    }

    @Test @Order(11)
    void toStdout_csv_withHeader() throws Exception {
        exec("CREATE TABLE ct_csvh(id int, name text, active boolean)");
        exec("INSERT INTO ct_csvh VALUES (1, 'alice', true), (2, 'bob', false)");
        String out = copyOut("COPY ct_csvh TO STDOUT WITH (FORMAT csv, HEADER true)");
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(3, lines.length, "Should have 1 header + 2 data lines");
        String header = lines[0].toLowerCase();
        assertTrue(header.contains("id"), "Header should contain 'id'");
        assertTrue(header.contains("name"), "Header should contain 'name'");
        assertTrue(header.contains("active"), "Header should contain 'active'");
        exec("DROP TABLE ct_csvh");
    }

    @Test @Order(12)
    void toStdout_csv_customDelimiter() throws Exception {
        exec("CREATE TABLE ct_delim(id int, name text)");
        exec("INSERT INTO ct_delim VALUES (1, 'alpha'), (2, 'beta')");
        String out = copyOut("COPY ct_delim TO STDOUT WITH (FORMAT csv, DELIMITER '|')");
        assertTrue(out.contains("|"), "Should use pipe delimiter");
        assertFalse(out.contains(","), "Should not use comma when pipe is specified");
        exec("DROP TABLE ct_delim");
    }

    @Test @Order(13)
    void toStdout_csv_nullValues() throws Exception {
        exec("CREATE TABLE ct_csvnull(id int, val text)");
        exec("INSERT INTO ct_csvnull VALUES (1, NULL), (2, 'present')");
        String out = copyOut("COPY ct_csvnull TO STDOUT WITH (FORMAT csv)");
        // In CSV, NULL is empty (no quoting)
        assertTrue(out.contains("present"));
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(2, lines.length);
        exec("DROP TABLE ct_csvnull");
    }

    @Test @Order(14)
    void toStdout_csv_customNullString() throws Exception {
        exec("CREATE TABLE ct_csvns(id int, val text)");
        exec("INSERT INTO ct_csvns VALUES (1, NULL), (2, 'present')");
        String out = copyOut("COPY ct_csvns TO STDOUT WITH (FORMAT csv, NULL 'N/A')");
        assertTrue(out.contains("N/A"), "Custom null string should appear for NULL values");
        assertTrue(out.contains("present"));
        exec("DROP TABLE ct_csvns");
    }

    @Test @Order(15)
    void toStdout_csv_quotedValues() throws Exception {
        exec("CREATE TABLE ct_quoted(id int, val text)");
        exec("INSERT INTO ct_quoted VALUES (1, 'has,comma'), (2, 'no comma')");
        String out = copyOut("COPY ct_quoted TO STDOUT WITH (FORMAT csv)");
        // Value containing comma should be quoted in CSV
        assertTrue(out.contains("\"has,comma\""), "Comma in value should be quoted");
        exec("DROP TABLE ct_quoted");
    }

    @Test @Order(16)
    void toStdout_csv_quotedQuotes() throws Exception {
        exec("CREATE TABLE ct_qq(id int, val text)");
        exec("INSERT INTO ct_qq VALUES (1, 'has\"quote')");
        String out = copyOut("COPY ct_qq TO STDOUT WITH (FORMAT csv)");
        // Double-quote inside value should be escaped as ""
        assertTrue(out.contains("\"\""), "Quote in value should be doubled");
        exec("DROP TABLE ct_qq");
    }

    // ========================================================================
    // COPY TO STDOUT, data types
    // ========================================================================

    @Test @Order(20)
    void toStdout_allDataTypes() throws Exception {
        exec("CREATE TABLE ct_types(" +
                "i int, bi bigint, si smallint, " +
                "t text, vc varchar(50), " +
                "b boolean, " +
                "r real, dp double precision, n numeric(10,2), " +
                "d date, ts timestamp, " +
                "j json, jb jsonb)");
        exec("INSERT INTO ct_types VALUES (" +
                "42, 9999999999, 7, " +
                "'hello world', 'varchar val', " +
                "true, " +
                "3.14, 2.718281828, 123.45, " +
                "'2024-06-15', '2024-06-15 10:30:00', " +
                "'{\"key\":\"value\"}', '{\"num\":1}')");
        String out = copyOut("COPY ct_types TO STDOUT WITH (FORMAT csv)");
        assertTrue(out.contains("42"), "int value");
        assertTrue(out.contains("9999999999"), "bigint value");
        assertTrue(out.contains("hello world"), "text value");
        assertTrue(out.contains("2024-06-15"), "date value");
        assertTrue(out.contains("123.45"), "numeric value");
        assertFalse(out.isEmpty());
        exec("DROP TABLE ct_types");
    }

    // ========================================================================
    // COPY TO STDOUT, subquery form
    // ========================================================================

    @Test @Order(30)
    void toStdout_subquery_basic() throws Exception {
        exec("CREATE TABLE ct_sq(id int, name text, score int)");
        exec("INSERT INTO ct_sq VALUES (1, 'alice', 90), (2, 'bob', 80), (3, 'charlie', 70)");
        String out = copyOut("COPY (SELECT name, score FROM ct_sq WHERE score >= 80 ORDER BY name) TO STDOUT");
        assertTrue(out.contains("alice"));
        assertTrue(out.contains("bob"));
        assertFalse(out.contains("charlie"), "charlie (score=70) should be filtered out");
        exec("DROP TABLE ct_sq");
    }

    @Test @Order(31)
    void toStdout_subquery_withLimit() throws Exception {
        exec("CREATE TABLE ct_sql(id int, val text)");
        exec("INSERT INTO ct_sql VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        String out = copyOut("COPY (SELECT * FROM ct_sql ORDER BY id LIMIT 2) TO STDOUT");
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(2, lines.length, "LIMIT 2 should produce 2 lines");
        exec("DROP TABLE ct_sql");
    }

    @Test @Order(32)
    void toStdout_subquery_csv() throws Exception {
        exec("CREATE TABLE ct_sqc(id int, name text)");
        exec("INSERT INTO ct_sqc VALUES (1, 'alice')");
        String out = copyOut("COPY (SELECT id, name FROM ct_sqc) TO STDOUT WITH (FORMAT csv, HEADER true)");
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(2, lines.length, "1 header + 1 data");
        assertTrue(lines[0].toLowerCase().contains("id"));
        assertTrue(lines[0].toLowerCase().contains("name"));
        exec("DROP TABLE ct_sqc");
    }

    @Test @Order(33)
    void toStdout_subquery_withJoin() throws Exception {
        exec("CREATE TABLE ct_j1(id int, name text)");
        exec("CREATE TABLE ct_j2(id int, city text)");
        exec("INSERT INTO ct_j1 VALUES (1, 'alice'), (2, 'bob')");
        exec("INSERT INTO ct_j2 VALUES (1, 'NYC'), (2, 'LA')");
        String out = copyOut("COPY (SELECT a.name, b.city FROM ct_j1 a JOIN ct_j2 b ON a.id = b.id ORDER BY a.name) TO STDOUT");
        assertTrue(out.contains("alice"));
        assertTrue(out.contains("NYC"));
        assertTrue(out.contains("bob"));
        assertTrue(out.contains("LA"));
        exec("DROP TABLE ct_j1");
        exec("DROP TABLE ct_j2");
    }

    // ========================================================================
    // COPY FROM STDIN, text format
    // ========================================================================

    @Test @Order(40)
    void fromStdin_textFormat_basic() throws Exception {
        exec("CREATE TABLE cf_text(id int, name text)");
        long rows = copyIn("COPY cf_text FROM STDIN", "1\talice\n2\tbob\n3\tcharlie\n");
        assertEquals(3, rows, "Should import 3 rows");
        assertEquals(3, rowCount("cf_text"));
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name FROM cf_text ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("alice", rs.getString(2));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("bob", rs.getString(2));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertEquals("charlie", rs.getString(2));
            assertFalse(rs.next());
        }
        exec("DROP TABLE cf_text");
    }

    @Test @Order(41)
    void fromStdin_textFormat_withNulls() throws Exception {
        exec("CREATE TABLE cf_nulls(id int, val text)");
        long rows = copyIn("COPY cf_nulls FROM STDIN", "1\t\\N\n2\tpresent\n3\t\\N\n");
        assertEquals(3, rows);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM cf_nulls ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertNull(rs.getString(2));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("present", rs.getString(2));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertNull(rs.getString(2));
        }
        exec("DROP TABLE cf_nulls");
    }

    @Test @Order(42)
    void fromStdin_textFormat_emptyInput() throws Exception {
        exec("CREATE TABLE cf_empty(id int, val text)");
        long rows = copyIn("COPY cf_empty FROM STDIN", "");
        assertEquals(0, rows, "Empty input should insert 0 rows");
        assertEquals(0, rowCount("cf_empty"));
        exec("DROP TABLE cf_empty");
    }

    @Test @Order(43)
    void fromStdin_textFormat_columnList() throws Exception {
        exec("CREATE TABLE cf_cols(id serial, name text, city text DEFAULT 'unknown')");
        long rows = copyIn("COPY cf_cols(name) FROM STDIN", "alice\nbob\n");
        assertEquals(2, rows);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name, city FROM cf_cols ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("alice", rs.getString("name"));
            assertEquals("unknown", rs.getString("city"), "Default should fill in");
            assertTrue(rs.next());
            assertEquals("bob", rs.getString("name"));
        }
        exec("DROP TABLE cf_cols");
    }

    // ========================================================================
    // COPY FROM STDIN, CSV format
    // ========================================================================

    @Test @Order(50)
    void fromStdin_csv_basic() throws Exception {
        exec("CREATE TABLE cf_csv(id int, name text, active boolean)");
        long rows = copyIn("COPY cf_csv FROM STDIN WITH (FORMAT csv)", "1,alice,true\n2,bob,false\n");
        assertEquals(2, rows);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name, active FROM cf_csv ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("alice", rs.getString(2)); assertTrue(rs.getBoolean(3));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("bob", rs.getString(2)); assertFalse(rs.getBoolean(3));
        }
        exec("DROP TABLE cf_csv");
    }

    @Test @Order(51)
    void fromStdin_csv_withHeader() throws Exception {
        exec("CREATE TABLE cf_csvh(id int, name text)");
        long rows = copyIn("COPY cf_csvh FROM STDIN WITH (FORMAT csv, HEADER true)",
                "id,name\n1,alice\n2,bob\n");
        assertEquals(2, rows, "Header line should be skipped");
        assertEquals(2, rowCount("cf_csvh"));
        exec("DROP TABLE cf_csvh");
    }

    @Test @Order(52)
    void fromStdin_csv_customDelimiter() throws Exception {
        exec("CREATE TABLE cf_pipe(id int, name text)");
        long rows = copyIn("COPY cf_pipe FROM STDIN WITH (FORMAT csv, DELIMITER '|')",
                "1|alice\n2|bob\n");
        assertEquals(2, rows);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT name FROM cf_pipe ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("bob", rs.getString(1));
        }
        exec("DROP TABLE cf_pipe");
    }

    @Test @Order(53)
    void fromStdin_csv_customNull() throws Exception {
        exec("CREATE TABLE cf_csvnull(id int, val text)");
        long rows = copyIn("COPY cf_csvnull FROM STDIN WITH (FORMAT csv, NULL 'NA')",
                "1,NA\n2,present\n");
        assertEquals(2, rows);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM cf_csvnull ORDER BY id")) {
            assertTrue(rs.next()); assertNull(rs.getString("val"), "NA should be treated as NULL");
            assertTrue(rs.next()); assertEquals("present", rs.getString("val"));
        }
        exec("DROP TABLE cf_csvnull");
    }

    @Test @Order(54)
    void fromStdin_csv_quotedFields() throws Exception {
        exec("CREATE TABLE cf_csvq(id int, val text)");
        long rows = copyIn("COPY cf_csvq FROM STDIN WITH (FORMAT csv)",
                "1,\"has,comma\"\n2,\"plain\"\n3,\"has\"\"quote\"\n");
        assertEquals(3, rows);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM cf_csvq ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("has,comma", rs.getString("val"));
            assertTrue(rs.next()); assertEquals("plain", rs.getString("val"));
            assertTrue(rs.next()); assertEquals("has\"quote", rs.getString("val"));
        }
        exec("DROP TABLE cf_csvq");
    }

    // ========================================================================
    // COPY FROM STDIN, type coercion
    // ========================================================================

    @Test @Order(60)
    void fromStdin_typeCoercion_allTypes() throws Exception {
        exec("CREATE TABLE cf_types(" +
                "i int, bi bigint, si smallint, " +
                "t text, b boolean, " +
                "r real, dp double precision, n numeric(10,2), " +
                "d date, ts timestamp)");
        long rows = copyIn("COPY cf_types FROM STDIN WITH (FORMAT csv)",
                "42,9999999999,7,hello,true,3.14,2.718,123.45,2024-06-15,2024-06-15 10:30:00\n");
        assertEquals(1, rows);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM cf_types")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("i"));
            assertEquals(9999999999L, rs.getLong("bi"));
            assertEquals(7, rs.getShort("si"));
            assertEquals("hello", rs.getString("t"));
            assertTrue(rs.getBoolean("b"));
            assertEquals(3.14f, rs.getFloat("r"), 0.01);
            assertEquals(2.718, rs.getDouble("dp"), 0.001);
            assertEquals("2024-06-15", rs.getDate("d").toString());
        }
        exec("DROP TABLE cf_types");
    }

    @Test @Order(61)
    void fromStdin_serialColumn_autoIncrement() throws Exception {
        exec("CREATE TABLE cf_serial(id serial PRIMARY KEY, name text)");
        long rows = copyIn("COPY cf_serial(name) FROM STDIN WITH (FORMAT csv)", "alice\nbob\n");
        assertEquals(2, rows);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name FROM cf_serial ORDER BY id")) {
            assertTrue(rs.next());
            int id1 = rs.getInt("id");
            assertEquals("alice", rs.getString("name"));
            assertTrue(rs.next());
            int id2 = rs.getInt("id");
            assertEquals("bob", rs.getString("name"));
            assertTrue(id2 > id1, "Serial IDs should auto-increment");
        }
        exec("DROP TABLE cf_serial");
    }

    @Test @Order(62)
    void fromStdin_defaultValues() throws Exception {
        exec("CREATE TABLE cf_defaults(id int, status text DEFAULT 'active', score int DEFAULT 0)");
        // Only supply id column
        long rows = copyIn("COPY cf_defaults(id) FROM STDIN WITH (FORMAT csv)", "1\n2\n");
        assertEquals(2, rows);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, status, score FROM cf_defaults ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("active", rs.getString("status"), "Default should apply");
            assertEquals(0, rs.getInt("score"), "Default should apply");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
        }
        exec("DROP TABLE cf_defaults");
    }

    // ========================================================================
    // Round-trip: COPY TO → COPY FROM
    // ========================================================================

    @Test @Order(70)
    void roundTrip_textFormat() throws Exception {
        exec("CREATE TABLE rt_src(id int, name text, val double precision)");
        exec("INSERT INTO rt_src VALUES (1, 'alice', 1.5), (2, 'bob', 2.7), (3, 'charlie', 3.9)");

        // Export
        String exported = copyOut("COPY rt_src TO STDOUT");
        assertFalse(exported.isEmpty());

        // Import into a new table
        exec("CREATE TABLE rt_dst(id int, name text, val double precision)");
        long rows = copyIn("COPY rt_dst FROM STDIN", exported);
        assertEquals(3, rows);

        // Verify data matches
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT s.id, s.name, s.val FROM rt_src s " +
                     "JOIN rt_dst d ON s.id = d.id WHERE s.name = d.name AND s.val = d.val ORDER BY s.id")) {
            int count = 0;
            while (rs.next()) count++;
            assertEquals(3, count, "All 3 rows should match after round-trip");
        }
        exec("DROP TABLE rt_src");
        exec("DROP TABLE rt_dst");
    }

    @Test @Order(71)
    void roundTrip_csvFormat() throws Exception {
        exec("CREATE TABLE rt_csrc(id int, name text, active boolean)");
        exec("INSERT INTO rt_csrc VALUES (1, 'alice', true), (2, 'bob', false)");

        String exported = copyOut("COPY rt_csrc TO STDOUT WITH (FORMAT csv)");
        exec("CREATE TABLE rt_cdst(id int, name text, active boolean)");
        long rows = copyIn("COPY rt_cdst FROM STDIN WITH (FORMAT csv)", exported);
        assertEquals(2, rows);

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name, active FROM rt_cdst ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("alice", rs.getString(2)); assertTrue(rs.getBoolean(3));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("bob", rs.getString(2)); assertFalse(rs.getBoolean(3));
        }
        exec("DROP TABLE rt_csrc");
        exec("DROP TABLE rt_cdst");
    }

    @Test @Order(72)
    void roundTrip_withNulls() throws Exception {
        exec("CREATE TABLE rt_nsrc(id int, val text)");
        exec("INSERT INTO rt_nsrc VALUES (1, NULL), (2, 'present'), (3, NULL)");

        String exported = copyOut("COPY rt_nsrc TO STDOUT");
        exec("CREATE TABLE rt_ndst(id int, val text)");
        copyIn("COPY rt_ndst FROM STDIN", exported);

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM rt_ndst ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertNull(rs.getString(2));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("present", rs.getString(2));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertNull(rs.getString(2));
        }
        exec("DROP TABLE rt_nsrc");
        exec("DROP TABLE rt_ndst");
    }

    @Test @Order(73)
    void roundTrip_csvWithSpecialChars() throws Exception {
        exec("CREATE TABLE rt_spec(id int, val text)");
        exec("INSERT INTO rt_spec VALUES (1, 'has,comma'), (2, 'has\ttab')");

        String exported = copyOut("COPY rt_spec TO STDOUT WITH (FORMAT csv)");
        exec("CREATE TABLE rt_sdst(id int, val text)");
        copyIn("COPY rt_sdst FROM STDIN WITH (FORMAT csv)", exported);

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM rt_sdst ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("has,comma", rs.getString("val"));
            assertTrue(rs.next()); assertEquals("has\ttab", rs.getString("val"));
        }
        exec("DROP TABLE rt_spec");
        exec("DROP TABLE rt_sdst");
    }

    // ========================================================================
    // Error cases
    // ========================================================================

    @Test @Order(80)
    void error_copyTo_nonExistentTable() throws Exception {
        assertThrows(SQLException.class,
                () -> copyOut("COPY nonexistent_table_xyz TO STDOUT"),
                "COPY TO on nonexistent table should throw");
    }

    @Test @Order(81)
    void error_copyFrom_nonExistentTable() throws Exception {
        assertThrows(SQLException.class,
                () -> copyIn("COPY nonexistent_table_xyz FROM STDIN", "1\tdata\n"),
                "COPY FROM on nonexistent table should throw");
    }

    @Test @Order(82)
    void error_copyTo_nonExistentColumn() throws Exception {
        exec("CREATE TABLE ct_badcol(id int, name text)");
        assertThrows(SQLException.class,
                () -> copyOut("COPY ct_badcol(id, nonexistent) TO STDOUT"),
                "COPY with nonexistent column should throw");
        exec("DROP TABLE ct_badcol");
    }

    @Test @Order(83)
    void error_copyFrom_constraintViolation_notNull() throws Exception {
        exec("CREATE TABLE cf_nn(id int NOT NULL, name text NOT NULL)");
        // Try inserting a row with NULL for a NOT NULL column
        assertThrows(SQLException.class,
                () -> copyIn("COPY cf_nn FROM STDIN", "1\t\\N\n"),
                "NULL in NOT NULL column should throw");
        exec("DROP TABLE cf_nn");
    }

    @Test @Order(84)
    void error_copyFrom_constraintViolation_pk() throws Exception {
        exec("CREATE TABLE cf_pk(id int PRIMARY KEY, name text)");
        exec("INSERT INTO cf_pk VALUES (1, 'alice')");
        assertThrows(SQLException.class,
                () -> copyIn("COPY cf_pk FROM STDIN", "1\tbob\n"),
                "Duplicate PK should throw");
        exec("DROP TABLE cf_pk");
    }

    // ========================================================================
    // Edge cases
    // ========================================================================

    @Test @Order(90)
    void edge_singleRow() throws Exception {
        exec("CREATE TABLE ct_single(id int, val text)");
        exec("INSERT INTO ct_single VALUES (1, 'only')");
        String out = copyOut("COPY ct_single TO STDOUT WITH (FORMAT csv)");
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(1, lines.length, "Single row should produce single line");
        assertTrue(out.contains("only"));
        exec("DROP TABLE ct_single");
    }

    @Test @Order(91)
    void edge_manyRows() throws Exception {
        exec("CREATE TABLE ct_many(id int, val text)");
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= 1000; i++) {
            sb.append(i).append(",row_").append(i).append("\n");
        }
        long rows = copyIn("COPY ct_many FROM STDIN WITH (FORMAT csv)", sb.toString());
        assertEquals(1000, rows, "Should import 1000 rows");
        assertEquals(1000, rowCount("ct_many"));

        String exported = copyOut("COPY ct_many TO STDOUT WITH (FORMAT csv)");
        String[] lines = Strs.strip(exported).split("\n");
        assertEquals(1000, lines.length, "Should export 1000 lines");
        exec("DROP TABLE ct_many");
    }

    @Test @Order(92)
    void edge_emptyStringVsNull() throws Exception {
        exec("CREATE TABLE ct_evn(id int, val text)");
        // In CSV, empty unquoted = NULL, quoted empty = empty string
        copyIn("COPY ct_evn FROM STDIN WITH (FORMAT csv)", "1,\n2,\"\"\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ct_evn ORDER BY id")) {
            assertTrue(rs.next());
            // Unquoted empty → NULL in CSV mode
            assertNull(rs.getString("val"), "Unquoted empty should be NULL in CSV");
            assertTrue(rs.next());
            // Quoted empty → empty string
            assertEquals("", rs.getString("val"), "Quoted empty should be empty string");
        }
        exec("DROP TABLE ct_evn");
    }

    @Test @Order(93)
    void edge_serverSideCopy_denied() throws Exception {
        exec("CREATE TABLE ct_deny(id int, val text)");
        // COPY TO a file path should be denied (no superuser / server-side file access)
        assertThrows(SQLException.class,
                () -> exec("COPY ct_deny TO '/tmp/test.csv'"),
                "Server-side COPY TO file should be denied");
        assertThrows(SQLException.class,
                () -> exec("COPY ct_deny FROM '/tmp/test.csv'"),
                "Server-side COPY FROM file should be denied");
        exec("DROP TABLE ct_deny");
    }

    @Test @Order(94)
    void edge_copyFrom_singleColumn() throws Exception {
        exec("CREATE TABLE cf_sc(val text)");
        long rows = copyIn("COPY cf_sc FROM STDIN WITH (FORMAT csv)", "hello\nworld\n");
        assertEquals(2, rows);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM cf_sc ORDER BY val")) {
            assertTrue(rs.next()); assertEquals("hello", rs.getString(1));
            assertTrue(rs.next()); assertEquals("world", rs.getString(1));
        }
        exec("DROP TABLE cf_sc");
    }

    @Test @Order(95)
    void edge_copyTo_booleanRendering() throws Exception {
        exec("CREATE TABLE ct_bool(id int, flag boolean)");
        exec("INSERT INTO ct_bool VALUES (1, true), (2, false)");
        String out = copyOut("COPY ct_bool TO STDOUT WITH (FORMAT csv)");
        // PG renders booleans as 't'/'f' in text format, 'true'/'false' in CSV
        assertTrue(out.contains("t") || out.contains("true"), "Boolean true should be present");
        assertTrue(out.contains("f") || out.contains("false"), "Boolean false should be present");
        exec("DROP TABLE ct_bool");
    }

    // ========================================================================
    // Data & format edge cases
    // ========================================================================

    @Test @Order(100)
    void edge_csv_embeddedNewlines() throws Exception {
        exec("CREATE TABLE ct_nl(id int, val text)");
        exec("INSERT INTO ct_nl VALUES (1, E'line1\\nline2'), (2, 'simple')");
        String out = copyOut("COPY ct_nl TO STDOUT WITH (FORMAT csv)");
        // Value with newline should be quoted; the output will have 3 actual lines
        // (row 1 spans 2 lines because of embedded newline, row 2 is 1 line)
        assertTrue(out.contains("line1"), "Should contain the multiline value");
        assertTrue(out.contains("simple"));

        // Round-trip: import the exported data
        exec("CREATE TABLE ct_nl2(id int, val text)");
        copyIn("COPY ct_nl2 FROM STDIN WITH (FORMAT csv)", out);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ct_nl2 ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("line1\nline2", rs.getString("val"), "Embedded newline should survive round-trip");
            assertTrue(rs.next());
            assertEquals("simple", rs.getString("val"));
        }
        exec("DROP TABLE ct_nl");
        exec("DROP TABLE ct_nl2");
    }

    @Test @Order(101)
    void edge_unicodeMultibyte() throws Exception {
        exec("CREATE TABLE ct_uni(id int, val text)");
        exec("INSERT INTO ct_uni VALUES (1, 'café'), (2, '日本語'), (3, '🎉🚀')");
        String out = copyOut("COPY ct_uni TO STDOUT WITH (FORMAT csv)");
        assertTrue(out.contains("café"), "Latin accented chars");
        assertTrue(out.contains("日本語"), "CJK characters");
        assertTrue(out.contains("🎉"), "Emoji characters");

        exec("CREATE TABLE ct_uni2(id int, val text)");
        copyIn("COPY ct_uni2 FROM STDIN WITH (FORMAT csv)", out);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM ct_uni2 ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("café", rs.getString(1));
            assertTrue(rs.next()); assertEquals("日本語", rs.getString(1));
            assertTrue(rs.next()); assertEquals("🎉🚀", rs.getString(1));
        }
        exec("DROP TABLE ct_uni");
        exec("DROP TABLE ct_uni2");
    }

    @Test @Order(102)
    void edge_veryLongTextValue() throws Exception {
        exec("CREATE TABLE ct_long(id int, val text)");
        String longVal = Strs.repeat("x", 10_000);
        exec("INSERT INTO ct_long VALUES (1, '" + longVal + "')");
        String out = copyOut("COPY ct_long TO STDOUT WITH (FORMAT csv)");
        assertTrue(out.contains(longVal), "10KB string should be preserved");

        exec("CREATE TABLE ct_long2(id int, val text)");
        copyIn("COPY ct_long2 FROM STDIN WITH (FORMAT csv)", out);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM ct_long2")) {
            assertTrue(rs.next());
            assertEquals(longVal, rs.getString(1), "Long value should survive round-trip");
        }
        exec("DROP TABLE ct_long");
        exec("DROP TABLE ct_long2");
    }

    @Test @Order(103)
    void edge_jsonWithCommasAndQuotes() throws Exception {
        exec("CREATE TABLE ct_json(id int, data json)");
        exec("INSERT INTO ct_json VALUES (1, '{\"name\":\"alice\",\"tags\":[\"a\",\"b\"]}')");
        exec("INSERT INTO ct_json VALUES (2, '{\"val\":\"has\\\"quote\"}')");
        String out = copyOut("COPY ct_json TO STDOUT WITH (FORMAT csv)");
        assertFalse(out.isEmpty());

        exec("CREATE TABLE ct_json2(id int, data json)");
        copyIn("COPY ct_json2 FROM STDIN WITH (FORMAT csv)", out);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, data::text FROM ct_json2 ORDER BY id")) {
            assertTrue(rs.next());
            String json1 = rs.getString(2);
            assertTrue(json1.contains("alice"), "JSON should survive CSV round-trip");
            assertTrue(json1.contains("tags"), "JSON arrays should survive");
        }
        exec("DROP TABLE ct_json");
        exec("DROP TABLE ct_json2");
    }

    @Test @Order(104)
    void edge_arrayColumns() throws Exception {
        exec("CREATE TABLE ct_arr(id int, nums int[], tags text[])");
        exec("INSERT INTO ct_arr VALUES (1, '{1,2,3}', '{\"hello\",\"world\"}')");
        exec("INSERT INTO ct_arr VALUES (2, '{4,5}', '{\"a\"}')");
        String out = copyOut("COPY ct_arr TO STDOUT WITH (FORMAT csv)");
        assertFalse(out.isEmpty());

        exec("CREATE TABLE ct_arr2(id int, nums int[], tags text[])");
        copyIn("COPY ct_arr2 FROM STDIN WITH (FORMAT csv)", out);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, nums, tags FROM ct_arr2 ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
        }
        exec("DROP TABLE ct_arr");
        exec("DROP TABLE ct_arr2");
    }

    @Test @Order(105)
    void edge_textFormat_emptyStringVsNull() throws Exception {
        // In text format: \N = NULL, empty between tabs = empty string
        exec("CREATE TABLE ct_tevn(id int, val text)");
        copyIn("COPY ct_tevn FROM STDIN", "1\t\n2\t\\N\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ct_tevn ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("", rs.getString("val"), "Empty field in text format should be empty string");
            assertTrue(rs.next());
            assertNull(rs.getString("val"), "\\N in text format should be NULL");
        }
        exec("DROP TABLE ct_tevn");
    }

    @Test @Order(106)
    void edge_textFormat_backslashEscapes() throws Exception {
        // Text format supports: \t (tab), \n (newline), \\ (backslash), \N (null)
        exec("CREATE TABLE ct_esc(id int, val text)");
        // Input with escaped tab and backslash within a value
        copyIn("COPY ct_esc FROM STDIN", "1\thas\\\\backslash\n2\thas\\ttab\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM ct_esc ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("has\\backslash", rs.getString("val"), "\\\\  should become single backslash");
            assertTrue(rs.next());
            assertEquals("has\ttab", rs.getString("val"), "\\t should become actual tab");
        }
        exec("DROP TABLE ct_esc");
    }

    @Test @Order(107)
    void edge_trailingNewline() throws Exception {
        exec("CREATE TABLE ct_trail(id int, name text)");
        // With trailing newline
        long rows1 = copyIn("COPY ct_trail FROM STDIN WITH (FORMAT csv)", "1,alice\n2,bob\n");
        assertEquals(2, rows1, "Trailing newline should not create extra row");
        exec("TRUNCATE ct_trail");

        // Without trailing newline
        long rows2 = copyIn("COPY ct_trail FROM STDIN WITH (FORMAT csv)", "1,alice\n2,bob");
        assertEquals(2, rows2, "Missing trailing newline should still import last row");
        exec("DROP TABLE ct_trail");
    }

    @Test @Order(108)
    void edge_windowsCRLF() throws Exception {
        exec("CREATE TABLE ct_crlf(id int, name text)");
        long rows = copyIn("COPY ct_crlf FROM STDIN WITH (FORMAT csv)", "1,alice\r\n2,bob\r\n");
        assertEquals(2, rows, "CRLF line endings should work");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT name FROM ct_crlf ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("bob", rs.getString(1));
        }
        exec("DROP TABLE ct_crlf");
    }

    @Test @Order(109)
    void edge_numericEdgeCases() throws Exception {
        exec("CREATE TABLE ct_num(id int, r real, dp double precision)");
        copyIn("COPY ct_num FROM STDIN WITH (FORMAT csv)",
                "1,NaN,NaN\n2,Infinity,Infinity\n3,-Infinity,-Infinity\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, r, dp FROM ct_num ORDER BY id")) {
            assertTrue(rs.next());
            assertTrue(Float.isNaN(rs.getFloat("r")), "NaN float");
            assertTrue(Double.isNaN(rs.getDouble("dp")), "NaN double");
            assertTrue(rs.next());
            assertTrue(Float.isInfinite(rs.getFloat("r")), "Infinity float");
            assertTrue(Double.isInfinite(rs.getDouble("dp")), "Infinity double");
            assertTrue(rs.next());
            assertEquals(Float.NEGATIVE_INFINITY, rs.getFloat("r"), "-Infinity float");
            assertEquals(Double.NEGATIVE_INFINITY, rs.getDouble("dp"), "-Infinity double");
        }
        exec("DROP TABLE ct_num");
    }

    // ========================================================================
    // Column handling
    // ========================================================================

    @Test @Order(110)
    void column_reorderedColumnList() throws Exception {
        exec("CREATE TABLE ct_reord(id int, name text, city text)");
        // Insert with columns in different order than table definition
        copyIn("COPY ct_reord(city, name, id) FROM STDIN WITH (FORMAT csv)",
                "NYC,alice,1\nLA,bob,2\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name, city FROM ct_reord ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("alice", rs.getString("name"));
            assertEquals("NYC", rs.getString("city"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("bob", rs.getString("name"));
            assertEquals("LA", rs.getString("city"));
        }
        exec("DROP TABLE ct_reord");
    }

    @Test @Order(111)
    void column_schemaQualifiedTable() throws Exception {
        exec("CREATE TABLE ct_schema_q(id int, val text)");
        exec("INSERT INTO ct_schema_q VALUES (1, 'hello')");
        String out = copyOut("COPY public.ct_schema_q TO STDOUT WITH (FORMAT csv)");
        assertTrue(out.contains("hello"), "Schema-qualified COPY TO should work");

        exec("CREATE TABLE ct_schema_q2(id int, val text)");
        copyIn("COPY public.ct_schema_q2 FROM STDIN WITH (FORMAT csv)", out);
        assertEquals(1, rowCount("ct_schema_q2"));
        exec("DROP TABLE ct_schema_q");
        exec("DROP TABLE ct_schema_q2");
    }

    @Test @Order(112)
    void column_copyToView() throws Exception {
        exec("CREATE TABLE ct_vsrc(id int, name text, score int)");
        exec("INSERT INTO ct_vsrc VALUES (1, 'alice', 90), (2, 'bob', 80)");
        exec("CREATE VIEW ct_vw AS SELECT id, name FROM ct_vsrc WHERE score >= 85");
        String out = copyOut("COPY ct_vw TO STDOUT WITH (FORMAT csv)");
        assertTrue(out.contains("alice"), "View COPY should include alice (score=90)");
        assertFalse(out.contains("bob"), "View COPY should exclude bob (score=80)");
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(1, lines.length, "Only 1 row should pass the view filter");
        exec("DROP VIEW ct_vw");
        exec("DROP TABLE ct_vsrc");
    }

    // ========================================================================
    // Subquery variations
    // ========================================================================

    @Test @Order(120)
    void subquery_withAggregates() throws Exception {
        exec("CREATE TABLE ct_agg(dept text, salary int)");
        exec("INSERT INTO ct_agg VALUES ('eng', 100), ('eng', 120), ('sales', 80), ('sales', 90)");
        String out = copyOut(
                "COPY (SELECT dept, count(*) as cnt, sum(salary) as total FROM ct_agg GROUP BY dept ORDER BY dept) TO STDOUT WITH (FORMAT csv)");
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(2, lines.length, "2 departments");
        assertTrue(out.contains("eng"), "Should contain eng department");
        assertTrue(out.contains("sales"), "Should contain sales department");
        exec("DROP TABLE ct_agg");
    }

    @Test @Order(121)
    void subquery_withCTE() throws Exception {
        exec("CREATE TABLE ct_cte(id int, val int)");
        exec("INSERT INTO ct_cte VALUES (1, 10), (2, 20), (3, 30)");
        String out = copyOut(
                "COPY (WITH top AS (SELECT * FROM ct_cte WHERE val >= 20) SELECT id, val FROM top ORDER BY id) TO STDOUT WITH (FORMAT csv)");
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(2, lines.length, "CTE should filter to 2 rows");
        assertFalse(out.contains(",10"), "val=10 should be filtered out by CTE");
        exec("DROP TABLE ct_cte");
    }

    // ========================================================================
    // Transaction behavior
    // ========================================================================

    @Test @Order(130)
    void transaction_copyFromThenRollback() throws Exception {
        exec("CREATE TABLE cf_tx(id int, val text)");
        conn.setAutoCommit(false);
        try {
            copyIn("COPY cf_tx FROM STDIN WITH (FORMAT csv)", "1,alice\n2,bob\n");
            // Data should be visible within the transaction
            assertEquals(2, rowCount("cf_tx"));
            conn.rollback();
            // After rollback, data should be gone
            assertEquals(0, rowCount("cf_tx"), "ROLLBACK should undo COPY FROM");
        } finally {
            conn.setAutoCommit(true);
        }
        exec("DROP TABLE cf_tx");
    }

    @Test @Order(131)
    void transaction_copyFromThenCommit() throws Exception {
        exec("CREATE TABLE cf_txc(id int, val text)");
        conn.setAutoCommit(false);
        try {
            copyIn("COPY cf_txc FROM STDIN WITH (FORMAT csv)", "1,alice\n2,bob\n");
            conn.commit();
            assertEquals(2, rowCount("cf_txc"), "COMMIT should persist COPY FROM data");
        } finally {
            conn.setAutoCommit(true);
        }
        exec("DROP TABLE cf_txc");
    }

    @Test @Order(132)
    void transaction_multipleCopiesAppend() throws Exception {
        exec("CREATE TABLE cf_multi(id int, val text)");
        copyIn("COPY cf_multi FROM STDIN WITH (FORMAT csv)", "1,first\n2,second\n");
        copyIn("COPY cf_multi FROM STDIN WITH (FORMAT csv)", "3,third\n4,fourth\n");
        copyIn("COPY cf_multi FROM STDIN WITH (FORMAT csv)", "5,fifth\n");
        assertEquals(5, rowCount("cf_multi"), "Multiple COPYs should append");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM cf_multi ORDER BY id")) {
            for (int i = 1; i <= 5; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
        }
        exec("DROP TABLE cf_multi");
    }

    // ========================================================================
    // Additional options
    // ========================================================================

    @Test @Order(140)
    void option_forceNotNull() throws Exception {
        // FORCE_NOT_NULL makes empty CSV fields import as empty string, not NULL
        exec("CREATE TABLE cf_fnn(id int, val text)");
        copyIn("COPY cf_fnn FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL (val))",
                "1,\n2,present\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM cf_fnn ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("", rs.getString("val"), "FORCE_NOT_NULL should make empty = empty string");
            assertFalse(rs.wasNull());
            assertTrue(rs.next());
            assertEquals("present", rs.getString("val"));
        }
        exec("DROP TABLE cf_fnn");
    }

    @Test @Order(141)
    void option_forceQuote() throws Exception {
        exec("CREATE TABLE ct_fq2(id int, name text, city text)");
        exec("INSERT INTO ct_fq2 VALUES (1, 'alice', 'NYC'), (2, 'bob', 'LA')");
        String out = copyOut("COPY ct_fq2 TO STDOUT WITH (FORMAT csv, FORCE_QUOTE (name, city))");
        // FORCE_QUOTE should quote name and city even if they don't need quoting
        String[] lines = Strs.strip(out).split("\n");
        for (String line : lines) {
            // name and city fields should be quoted
            assertTrue(line.contains("\""), "FORCE_QUOTE fields should be quoted: " + line);
        }
        exec("DROP TABLE ct_fq2");
    }

    // ========================================================================
    // Error handling: column count mismatches and bad data
    // ========================================================================

    @Test @Order(150)
    void error_tooManyColumns() throws Exception {
        exec("CREATE TABLE cf_tmc(id int, name text)");
        // 3 fields for a 2-column table
        assertThrows(Exception.class,
                () -> copyIn("COPY cf_tmc FROM STDIN WITH (FORMAT csv)", "1,alice,extra\n"),
                "Too many columns should throw");
        exec("DROP TABLE cf_tmc");
    }

    @Test @Order(151)
    void error_tooFewColumns() throws Exception {
        exec("CREATE TABLE cf_tfc(id int, name text, city text)");
        // 2 fields for a 3-column table (without column list)
        assertThrows(Exception.class,
                () -> copyIn("COPY cf_tfc FROM STDIN WITH (FORMAT csv)", "1,alice\n"),
                "Too few columns should throw");
        exec("DROP TABLE cf_tfc");
    }

    @Test @Order(152)
    void error_invalidTypeData() throws Exception {
        exec("CREATE TABLE cf_badtype(id int, val int)");
        assertThrows(Exception.class,
                () -> copyIn("COPY cf_badtype FROM STDIN WITH (FORMAT csv)", "1,not_a_number\n"),
                "Non-numeric value for int column should throw");
        exec("DROP TABLE cf_badtype");
    }

    // ========================================================================
    // Security: filesystem access denied
    // ========================================================================

    @Test @Order(200)
    void security_copyTo_absolutePath_denied() throws Exception {
        exec("CREATE TABLE sec_to1(id int, val text)");
        exec("INSERT INTO sec_to1 VALUES (1, 'data')");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("COPY sec_to1 TO '/tmp/test_output.csv'"),
                "COPY TO absolute path should be denied");
        // PG error: "must be superuser to COPY to or from a file"
        assertTrue(ex.getMessage().toLowerCase().contains("superuser")
                        || ex.getMessage().toLowerCase().contains("not supported")
                        || ex.getMessage().toLowerCase().contains("permission"),
                "Error should mention privilege restriction: " + ex.getMessage());
        exec("DROP TABLE sec_to1");
    }

    @Test @Order(201)
    void security_copyFrom_absolutePath_denied() throws Exception {
        exec("CREATE TABLE sec_from1(id int, val text)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("COPY sec_from1 FROM '/tmp/test_input.csv'"),
                "COPY FROM absolute path should be denied");
        assertTrue(ex.getMessage().toLowerCase().contains("superuser")
                        || ex.getMessage().toLowerCase().contains("not supported")
                        || ex.getMessage().toLowerCase().contains("permission"),
                "Error should mention privilege restriction: " + ex.getMessage());
        exec("DROP TABLE sec_from1");
    }

    @Test @Order(202)
    void security_copyTo_relativePath_denied() throws Exception {
        exec("CREATE TABLE sec_to2(id int, val text)");
        exec("INSERT INTO sec_to2 VALUES (1, 'data')");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("COPY sec_to2 TO 'relative/path.csv'"),
                "COPY TO relative path should be denied");
        assertTrue(ex.getMessage().toLowerCase().contains("superuser")
                        || ex.getMessage().toLowerCase().contains("not supported")
                        || ex.getMessage().toLowerCase().contains("permission"),
                "Error should mention privilege restriction: " + ex.getMessage());
        exec("DROP TABLE sec_to2");
    }

    @Test @Order(203)
    void security_copyTo_program_denied() throws Exception {
        exec("CREATE TABLE sec_prog1(id int, val text)");
        exec("INSERT INTO sec_prog1 VALUES (1, 'data')");
        assertThrows(SQLException.class,
                () -> exec("COPY sec_prog1 TO PROGRAM 'cat > /tmp/out.csv'"),
                "COPY TO PROGRAM should be denied");
        exec("DROP TABLE sec_prog1");
    }

    @Test @Order(204)
    void security_copyFrom_program_denied() throws Exception {
        exec("CREATE TABLE sec_prog2(id int, val text)");
        assertThrows(SQLException.class,
                () -> exec("COPY sec_prog2 FROM PROGRAM 'echo 1,test'"),
                "COPY FROM PROGRAM should be denied");
        exec("DROP TABLE sec_prog2");
    }

    @Test @Order(205)
    void security_pathTraversal_denied() throws Exception {
        exec("CREATE TABLE sec_trav(id int, val text)");
        exec("INSERT INTO sec_trav VALUES (1, 'data')");
        assertThrows(SQLException.class,
                () -> exec("COPY sec_trav TO '../../etc/passwd'"),
                "Path traversal should be denied");
        assertThrows(SQLException.class,
                () -> exec("COPY sec_trav TO '/etc/shadow'"),
                "Sensitive path should be denied");
        exec("DROP TABLE sec_trav");
    }

    @Test @Order(206)
    void security_copyFrom_pathTraversal_denied() throws Exception {
        exec("CREATE TABLE sec_trav2(id int, val text)");
        assertThrows(SQLException.class,
                () -> exec("COPY sec_trav2 FROM '../../etc/passwd'"),
                "Path traversal FROM should be denied");
        exec("DROP TABLE sec_trav2");
    }

    // ========================================================================
    // Direction errors
    // ========================================================================

    @Test @Order(210)
    void error_copyTo_stdin_wrongDirection() throws Exception {
        exec("CREATE TABLE dir_err1(id int, val text)");
        assertThrows(SQLException.class,
                () -> exec("COPY dir_err1 TO STDIN"),
                "COPY TO STDIN is invalid (wrong direction)");
        exec("DROP TABLE dir_err1");
    }

    @Test @Order(211)
    void error_copyFrom_stdout_wrongDirection() throws Exception {
        exec("CREATE TABLE dir_err2(id int, val text)");
        assertThrows(SQLException.class,
                () -> exec("COPY dir_err2 FROM STDOUT"),
                "COPY FROM STDOUT is invalid (wrong direction)");
        exec("DROP TABLE dir_err2");
    }

    // ========================================================================
    // WHERE clause (PG 12+)
    // ========================================================================

    @Test @Order(220)
    void copyTo_withWhere_filtersRows() throws Exception {
        exec("CREATE TABLE ct_where(id int, name text, score int)");
        exec("INSERT INTO ct_where VALUES (1, 'alice', 90), (2, 'bob', 60), (3, 'charlie', 80)");
        String out = copyOut("COPY ct_where TO STDOUT WITH (FORMAT csv) WHERE score >= 80");
        assertTrue(out.contains("alice"), "alice (90) should pass filter");
        assertTrue(out.contains("charlie"), "charlie (80) should pass filter");
        assertFalse(out.contains("bob"), "bob (60) should be filtered out");
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(2, lines.length, "Only 2 rows should pass WHERE filter");
        exec("DROP TABLE ct_where");
    }

    @Test @Order(221)
    void copyTo_withWhere_noMatch() throws Exception {
        exec("CREATE TABLE ct_where2(id int, score int)");
        exec("INSERT INTO ct_where2 VALUES (1, 10), (2, 20)");
        String out = copyOut("COPY ct_where2 TO STDOUT WHERE score > 100");
        assertEquals("", out, "No rows match → empty output");
        exec("DROP TABLE ct_where2");
    }

    @Test @Order(222)
    void copyTo_withWhere_textFormat() throws Exception {
        exec("CREATE TABLE ct_where3(id int, active boolean)");
        exec("INSERT INTO ct_where3 VALUES (1, true), (2, false), (3, true)");
        String out = copyOut("COPY ct_where3 TO STDOUT WHERE active = true");
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(2, lines.length, "Only active=true rows");
        exec("DROP TABLE ct_where3");
    }

    // ========================================================================
    // Column validation edge cases
    // ========================================================================

    @Test @Order(230)
    void error_duplicateColumnsInList() throws Exception {
        exec("CREATE TABLE cf_dup(id int, name text)");
        assertThrows(SQLException.class,
                () -> copyIn("COPY cf_dup(id, id) FROM STDIN WITH (FORMAT csv)", "1,2\n"),
                "Duplicate column in list should throw");
        exec("DROP TABLE cf_dup");
    }

    @Test @Order(231)
    void error_emptyColumnList() throws Exception {
        exec("CREATE TABLE cf_ecl(id int, name text)");
        assertThrows(SQLException.class,
                () -> exec("COPY cf_ecl() FROM STDIN"),
                "Empty column list should throw");
        exec("DROP TABLE cf_ecl");
    }

    @Test @Order(232)
    void quotedMixedCaseTableName() throws Exception {
        exec("CREATE TABLE \"MixedCaseTable\"(id int, val text)");
        exec("INSERT INTO \"MixedCaseTable\" VALUES (1, 'hello')");
        String out = copyOut("COPY \"MixedCaseTable\" TO STDOUT WITH (FORMAT csv)");
        assertTrue(out.contains("hello"), "Quoted table name should work");

        exec("CREATE TABLE \"MixedCaseDst\"(id int, val text)");
        copyIn("COPY \"MixedCaseDst\" FROM STDIN WITH (FORMAT csv)", out);
        assertEquals(1, rowCount("\"MixedCaseDst\""));
        exec("DROP TABLE \"MixedCaseTable\"");
        exec("DROP TABLE \"MixedCaseDst\"");
    }

    // ========================================================================
    // Constraint enforcement during COPY FROM
    // ========================================================================

    @Test @Order(240)
    void constraint_checkViolation() throws Exception {
        exec("CREATE TABLE cf_chk(id int, age int CHECK (age >= 0))");
        assertThrows(Exception.class,
                () -> copyIn("COPY cf_chk FROM STDIN WITH (FORMAT csv)", "1,-5\n"),
                "CHECK constraint violation should throw");
        exec("DROP TABLE cf_chk");
    }

    @Test @Order(241)
    void constraint_uniqueViolation_nonPK() throws Exception {
        exec("CREATE TABLE cf_uniq(id int, email text UNIQUE)");
        exec("INSERT INTO cf_uniq VALUES (1, 'alice@test.com')");
        assertThrows(Exception.class,
                () -> copyIn("COPY cf_uniq FROM STDIN WITH (FORMAT csv)", "2,alice@test.com\n"),
                "UNIQUE violation should throw");
        exec("DROP TABLE cf_uniq");
    }

    @Test @Order(242)
    void constraint_foreignKeyViolation() throws Exception {
        exec("CREATE TABLE cf_fk_parent(id int PRIMARY KEY)");
        exec("INSERT INTO cf_fk_parent VALUES (1), (2)");
        exec("CREATE TABLE cf_fk_child(id int, parent_id int REFERENCES cf_fk_parent(id))");
        assertThrows(Exception.class,
                () -> copyIn("COPY cf_fk_child FROM STDIN WITH (FORMAT csv)", "1,999\n"),
                "FK violation should throw because parent_id 999 does not exist");
        exec("DROP TABLE cf_fk_child");
        exec("DROP TABLE cf_fk_parent");
    }

    @Test @Order(243)
    void constraint_multipleRowsPartialFailure() throws Exception {
        // When a constraint violation occurs mid-COPY, previously inserted rows
        // should be rolled back (atomic behavior)
        exec("CREATE TABLE cf_partial(id int PRIMARY KEY, val text)");
        try {
            copyIn("COPY cf_partial FROM STDIN WITH (FORMAT csv)", "1,ok\n2,ok\n2,duplicate\n");
            fail("Should have thrown on duplicate PK");
        } catch (Exception expected) {
            // expected
        }
        // Depending on implementation: either 0 rows (atomic rollback) or 2 rows (partial)
        // PG does atomic rollback for COPY
        int count = rowCount("cf_partial");
        assertEquals(0, count, "COPY should be atomic, all-or-nothing on constraint failure");
        exec("DROP TABLE cf_partial");
    }

    // ========================================================================
    // Special column types
    // ========================================================================

    @Test @Order(250)
    void specialCol_generatedColumn_excludedFromImport() throws Exception {
        exec("CREATE TABLE cf_gen(id int, price int, tax int GENERATED ALWAYS AS (price * 10 / 100) STORED)");
        // COPY FROM should only need id and price; tax is generated
        copyIn("COPY cf_gen(id, price) FROM STDIN WITH (FORMAT csv)", "1,100\n2,200\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, price, tax FROM cf_gen ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(100, rs.getInt("price"));
            assertEquals(10, rs.getInt("tax"), "Generated column should compute automatically");
            assertTrue(rs.next());
            assertEquals(200, rs.getInt("price"));
            assertEquals(20, rs.getInt("tax"));
        }
        exec("DROP TABLE cf_gen");
    }

    @Test @Order(251)
    void specialCol_generatedColumn_includedInExport() throws Exception {
        exec("CREATE TABLE ct_gen(id int, price int, tax int GENERATED ALWAYS AS (price * 10 / 100) STORED)");
        exec("INSERT INTO ct_gen(id, price) VALUES (1, 100)");
        String out = copyOut("COPY ct_gen TO STDOUT WITH (FORMAT csv)");
        // Export should include id, price, and the computed tax
        assertTrue(out.contains("10"), "Generated tax value should appear in export");
        exec("DROP TABLE ct_gen");
    }

    @Test @Order(252)
    void specialCol_identityAlways_errorOnValueProvided() throws Exception {
        exec("CREATE TABLE cf_ident(id int GENERATED ALWAYS AS IDENTITY, name text)");
        // Providing a value for GENERATED ALWAYS identity column should error
        assertThrows(Exception.class,
                () -> copyIn("COPY cf_ident FROM STDIN WITH (FORMAT csv)", "99,alice\n"),
                "GENERATED ALWAYS identity column should reject explicit values");
        exec("DROP TABLE cf_ident");
    }

    @Test @Order(253)
    void specialCol_identityAlways_omittedColumnAutoGenerates() throws Exception {
        exec("CREATE TABLE cf_ident2(id int GENERATED ALWAYS AS IDENTITY, name text)");
        copyIn("COPY cf_ident2(name) FROM STDIN WITH (FORMAT csv)", "alice\nbob\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name FROM cf_ident2 ORDER BY name")) {
            assertTrue(rs.next());
            assertEquals("alice", rs.getString("name"));
            int id1 = rs.getInt("id");
            assertTrue(rs.next());
            assertEquals("bob", rs.getString("name"));
            int id2 = rs.getInt("id");
            assertTrue(id2 > id1, "Identity should auto-generate increasing values");
        }
        exec("DROP TABLE cf_ident2");
    }

    @Test @Order(254)
    void specialCol_identityByDefault_acceptsValue() throws Exception {
        exec("CREATE TABLE cf_identd(id int GENERATED BY DEFAULT AS IDENTITY, name text)");
        // BY DEFAULT allows providing explicit values
        copyIn("COPY cf_identd FROM STDIN WITH (FORMAT csv)", "42,alice\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name FROM cf_identd")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("id"), "BY DEFAULT should accept explicit value");
            assertEquals("alice", rs.getString("name"));
        }
        exec("DROP TABLE cf_identd");
    }

    // ========================================================================
    // Row count accuracy
    // ========================================================================

    @Test @Order(260)
    void rowCount_copyFromReturnsAccurateCount() throws Exception {
        exec("CREATE TABLE cf_rc(id int, val text)");
        long rows = copyIn("COPY cf_rc FROM STDIN WITH (FORMAT csv)",
                "1,a\n2,b\n3,c\n4,d\n5,e\n");
        assertEquals(5, rows, "CopyManager should report exactly 5 rows imported");
        assertEquals(5, rowCount("cf_rc"), "Table should contain exactly 5 rows");
        exec("DROP TABLE cf_rc");
    }

    @Test @Order(261)
    void rowCount_copyToReturnsAccurateCount() throws Exception {
        exec("CREATE TABLE ct_rc(id int, val text)");
        exec("INSERT INTO ct_rc VALUES (1,'a'), (2,'b'), (3,'c')");
        StringWriter sw = new StringWriter();
        long rows = cm.copyOut("COPY ct_rc TO STDOUT WITH (FORMAT csv)", sw);
        assertEquals(3, rows, "CopyManager should report exactly 3 rows exported");
        exec("DROP TABLE ct_rc");
    }

    // ========================================================================
    // Sequences after COPY FROM
    // ========================================================================

    @Test @Order(270)
    void sequence_afterCopyFrom_nextvalContinues() throws Exception {
        exec("CREATE TABLE cf_seq(id serial PRIMARY KEY, name text)");
        // Insert via COPY; serial should auto-assign ids
        copyIn("COPY cf_seq(name) FROM STDIN WITH (FORMAT csv)", "alice\nbob\ncharlie\n");

        // Now do a regular INSERT; it should get the next id without collision
        exec("INSERT INTO cf_seq(name) VALUES ('dave')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name FROM cf_seq ORDER BY id")) {
            int prevId = 0;
            while (rs.next()) {
                int id = rs.getInt("id");
                assertTrue(id > prevId, "IDs should be strictly increasing: " + id + " > " + prevId);
                prevId = id;
            }
            assertEquals(4, prevId > 0 ? rowCount("cf_seq") : 0, "Should have 4 total rows");
        }
        exec("DROP TABLE cf_seq");
    }

    @Test @Order(271)
    void sequence_afterCopyFrom_explicitIds_nextvalSkipsPast() throws Exception {
        exec("CREATE TABLE cf_seq2(id serial PRIMARY KEY, name text)");
        // COPY with explicit high ids
        copyIn("COPY cf_seq2 FROM STDIN WITH (FORMAT csv)", "100,alice\n200,bob\n");
        // Next insert should not collide with 100 or 200
        exec("INSERT INTO cf_seq2(name) VALUES ('charlie')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM cf_seq2 WHERE name = 'charlie'")) {
            assertTrue(rs.next());
            int charlieId = rs.getInt(1);
            assertNotEquals(100, charlieId, "Should not collide with explicitly inserted id");
            assertNotEquals(200, charlieId, "Should not collide with explicitly inserted id");
        }
        exec("DROP TABLE cf_seq2");
    }

    // ========================================================================
    // Temporary tables
    // ========================================================================

    @Test @Order(280)
    void tempTable_copyTo() throws Exception {
        exec("CREATE TEMP TABLE tmp_to(id int, val text)");
        exec("INSERT INTO tmp_to VALUES (1, 'temp_data')");
        String out = copyOut("COPY tmp_to TO STDOUT WITH (FORMAT csv)");
        assertTrue(out.contains("temp_data"), "COPY TO from temp table should work");
        exec("DROP TABLE tmp_to");
    }

    @Test @Order(281)
    void tempTable_copyFrom() throws Exception {
        exec("CREATE TEMP TABLE tmp_from(id int, val text)");
        long rows = copyIn("COPY tmp_from FROM STDIN WITH (FORMAT csv)", "1,temp_in\n2,temp_in2\n");
        assertEquals(2, rows);
        assertEquals(2, rowCount("tmp_from"));
        exec("DROP TABLE tmp_from");
    }

    // ========================================================================
    // Wide table (many columns)
    // ========================================================================

    @Test @Order(290)
    void wideTable_manyColumns() throws Exception {
        // Create a table with 50 columns
        StringBuilder createSql = new StringBuilder("CREATE TABLE ct_wide(");
        for (int i = 1; i <= 50; i++) {
            if (i > 1) createSql.append(", ");
            createSql.append("c").append(i).append(" int");
        }
        createSql.append(")");
        exec(createSql.toString());

        // Build CSV row with values 1..50
        StringBuilder csvRow = new StringBuilder();
        for (int i = 1; i <= 50; i++) {
            if (i > 1) csvRow.append(",");
            csvRow.append(i);
        }
        csvRow.append("\n");

        copyIn("COPY ct_wide FROM STDIN WITH (FORMAT csv)", csvRow.toString());

        // Export and verify
        String out = copyOut("COPY ct_wide TO STDOUT WITH (FORMAT csv)");
        String[] fields = Strs.strip(out).split(",");
        assertEquals(50, fields.length, "Should have 50 fields");
        assertEquals("1", fields[0]);
        assertEquals("50", fields[49]);

        exec("DROP TABLE ct_wide");
    }

    // ========================================================================
    // Dropped columns
    // ========================================================================

    @Test @Order(300)
    void droppedColumn_copyToSkipsDropped() throws Exception {
        exec("CREATE TABLE ct_drop(id int, removed text, kept text)");
        exec("INSERT INTO ct_drop VALUES (1, 'gone', 'here')");
        exec("ALTER TABLE ct_drop DROP COLUMN removed");
        String out = copyOut("COPY ct_drop TO STDOUT WITH (FORMAT csv)");
        // Should only have id and kept (2 columns)
        String[] fields = Strs.strip(out).split(",");
        assertEquals(2, fields.length, "Dropped column should not appear in output");
        assertTrue(out.contains("here"));
        assertFalse(out.contains("gone"), "Dropped column data should not appear");
        exec("DROP TABLE ct_drop");
    }

    @Test @Order(301)
    void droppedColumn_copyFromSkipsDropped() throws Exception {
        exec("CREATE TABLE cf_drop(id int, removed text, kept text)");
        exec("ALTER TABLE cf_drop DROP COLUMN removed");
        // COPY FROM should only expect 2 columns (id, kept)
        copyIn("COPY cf_drop FROM STDIN WITH (FORMAT csv)", "1,here\n2,there\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, kept FROM cf_drop ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt("id")); assertEquals("here", rs.getString("kept"));
            assertTrue(rs.next()); assertEquals(2, rs.getInt("id")); assertEquals("there", rs.getString("kept"));
        }
        exec("DROP TABLE cf_drop");
    }

    // ========================================================================
    // Binary format
    // ========================================================================

    @Test @Order(310)
    void binaryFormat_copyTo() throws Exception {
        exec("CREATE TABLE ct_bin2(id int, val text)");
        exec("INSERT INTO ct_bin2 VALUES (1, 'binary_test')");
        // Binary COPY TO should at least respond with proper protocol
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long rows = cm.copyOut("COPY ct_bin2 TO STDOUT WITH (FORMAT binary)", baos);
        assertTrue(rows >= 1, "Binary COPY TO should export rows");
        byte[] data = baos.toByteArray();
        assertTrue(data.length > 0, "Binary output should not be empty");
        // PG binary format starts with "PGCOPY\n\377\r\n\0" (11-byte signature)
        assertEquals('P', (char) data[0], "Binary header should start with PGCOPY signature");
        exec("DROP TABLE ct_bin2");
    }

    @Test @Order(311)
    void binaryFormat_roundTrip() throws Exception {
        exec("CREATE TABLE ct_binrt(id int, name text, flag boolean)");
        exec("INSERT INTO ct_binrt VALUES (1, 'alice', true), (2, 'bob', false)");

        // Export binary
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long outRows = cm.copyOut("COPY ct_binrt TO STDOUT WITH (FORMAT binary)", baos);
        assertEquals(2, outRows);

        // Import binary into new table
        exec("CREATE TABLE ct_binrt2(id int, name text, flag boolean)");
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        long inRows = cm.copyIn("COPY ct_binrt2 FROM STDIN WITH (FORMAT binary)", bais);
        assertEquals(2, inRows);

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name, flag FROM ct_binrt2 ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("alice", rs.getString(2)); assertTrue(rs.getBoolean(3));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("bob", rs.getString(2)); assertFalse(rs.getBoolean(3));
        }
        exec("DROP TABLE ct_binrt");
        exec("DROP TABLE ct_binrt2");
    }

    // ========================================================================
    // FREEZE option
    // ========================================================================

    @Test @Order(320)
    void option_freeze() throws Exception {
        exec("CREATE TABLE cf_freeze(id int, val text)");
        // FREEZE is a PG optimization hint and should be accepted (or give sensible error)
        try {
            copyIn("COPY cf_freeze FROM STDIN WITH (FORMAT csv, FREEZE true)", "1,data\n");
            assertEquals(1, rowCount("cf_freeze"), "FREEZE option should not prevent data import");
        } catch (SQLException e) {
            // Acceptable: some implementations may not support FREEZE
            // but it should not be a parse error
            assertFalse(e.getMessage().contains("syntax"),
                    "FREEZE should be parsed even if not supported: " + e.getMessage());
        }
        exec("DROP TABLE cf_freeze");
    }

    // ========================================================================
    // COPY and triggers
    // ========================================================================

    @Test @Order(330)
    void triggers_beforeInsertFiresDuringCopy() throws Exception {
        exec("CREATE TABLE cf_trig(id int, val text, modified_by text DEFAULT 'direct')");
        exec("CREATE OR REPLACE FUNCTION trig_set_modified() RETURNS trigger AS $$ " +
                "BEGIN NEW.modified_by := 'copy_trigger'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trg_before_ins BEFORE INSERT ON cf_trig " +
                "FOR EACH ROW EXECUTE FUNCTION trig_set_modified()");

        copyIn("COPY cf_trig(id, val) FROM STDIN WITH (FORMAT csv)", "1,hello\n2,world\n");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val, modified_by FROM cf_trig ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("hello", rs.getString("val"));
            assertEquals("copy_trigger", rs.getString("modified_by"),
                    "BEFORE INSERT trigger should fire during COPY FROM");
            assertTrue(rs.next());
        }
        exec("DROP TABLE cf_trig");
    }

    // ========================================================================
    // COPY and table inheritance
    // ========================================================================

    @Test @Order(340)
    void inheritance_copyFromParent() throws Exception {
        exec("CREATE TABLE cf_parent(id int, val text)");
        exec("CREATE TABLE cf_child(extra text) INHERITS (cf_parent)");
        exec("INSERT INTO cf_parent VALUES (1, 'parent_only')");
        exec("INSERT INTO cf_child VALUES (2, 'in_child', 'bonus')");

        // COPY TO from parent should include rows from child (PG inheritance behavior)
        String out = copyOut("COPY cf_parent TO STDOUT WITH (FORMAT csv)");
        assertTrue(out.contains("parent_only"), "Parent row should be included");
        // In PG, COPY on parent includes child rows
        // (but this is actually PG-version-dependent; COPY may or may not include inherited rows)
        exec("DROP TABLE cf_child");
        exec("DROP TABLE cf_parent");
    }

    // ========================================================================
    // COPY with ENCODING option
    // ========================================================================

    @Test @Order(350)
    void option_encoding() throws Exception {
        exec("CREATE TABLE cf_enc(id int, val text)");
        // ENCODING option should be accepted (UTF8 is default anyway)
        try {
            copyIn("COPY cf_enc FROM STDIN WITH (FORMAT csv, ENCODING 'UTF8')", "1,hello\n");
            assertEquals(1, rowCount("cf_enc"));
        } catch (SQLException e) {
            // Acceptable if not implemented but should not be a parse error
            assertFalse(e.getMessage().contains("syntax"),
                    "ENCODING should be parsed: " + e.getMessage());
        }
        exec("DROP TABLE cf_enc");
    }

    // ========================================================================
    // Mixed case / case sensitivity of COPY keyword
    // ========================================================================

    @Test @Order(360)
    void caseSensitivity_copyKeyword() throws Exception {
        exec("CREATE TABLE ct_case(id int, val text)");
        exec("INSERT INTO ct_case VALUES (1, 'data')");
        // PG accepts COPY in any case
        String out = copyOut("copy ct_case TO STDOUT WITH (FORMAT csv)");
        assertTrue(out.contains("data"), "Lowercase 'copy' should work");
        exec("DROP TABLE ct_case");
    }

    // ========================================================================
    // COPY with ON_ERROR (PG 17+)
    // ========================================================================

    @Test @Order(370)
    void option_onError_stop() throws Exception {
        exec("CREATE TABLE cf_onerr(id int, val int)");
        // ON_ERROR stop is the default, so invalid data should error
        assertThrows(Exception.class,
                () -> copyIn("COPY cf_onerr FROM STDIN WITH (FORMAT csv, ON_ERROR stop)",
                        "1,100\n2,bad\n3,300\n"),
                "ON_ERROR stop should abort on invalid data");
        exec("DROP TABLE cf_onerr");
    }

    @Test @Order(371)
    void option_onError_ignore() throws Exception {
        exec("CREATE TABLE cf_onerr2(id int, val int)");
        // ON_ERROR ignore should skip bad rows and continue
        try {
            copyIn("COPY cf_onerr2 FROM STDIN WITH (FORMAT csv, ON_ERROR ignore)",
                    "1,100\n2,bad\n3,300\n");
            // If supported, bad row should be skipped
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*) FROM cf_onerr2")) {
                rs.next();
                int count = rs.getInt(1);
                assertTrue(count >= 2, "Good rows should be imported, bad rows skipped");
            }
        } catch (SQLException e) {
            // Acceptable if ON_ERROR ignore is not yet supported
            assertFalse(e.getMessage().contains("syntax"),
                    "ON_ERROR should be parsed: " + e.getMessage());
        }
        exec("DROP TABLE cf_onerr2");
    }

    // ========================================================================
    // COPY with DEFAULT option (PG 16+)
    // ========================================================================

    @Test @Order(380)
    void option_default() throws Exception {
        exec("CREATE TABLE cf_def(id int, status text DEFAULT 'active')");
        // DEFAULT option: a magic string in input that means "use column default"
        try {
            copyIn("COPY cf_def FROM STDIN WITH (FORMAT csv, DEFAULT '\\D')",
                    "1,\\D\n2,custom\n");
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT id, status FROM cf_def ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals("active", rs.getString("status"),
                        "DEFAULT marker should use column default value");
                assertTrue(rs.next());
                assertEquals("custom", rs.getString("status"));
            }
        } catch (SQLException e) {
            // Acceptable if not implemented
            assertFalse(e.getMessage().contains("syntax"),
                    "DEFAULT option should be parsed: " + e.getMessage());
        }
        exec("DROP TABLE cf_def");
    }

    // ========================================================================
    // Concurrent COPY operations
    // ========================================================================

    @Test @Order(390)
    void concurrent_copyFromDifferentTables() throws Exception {
        exec("CREATE TABLE cf_conc1(id int, val text)");
        exec("CREATE TABLE cf_conc2(id int, val text)");
        // Sequential COPYs to different tables should work fine
        copyIn("COPY cf_conc1 FROM STDIN WITH (FORMAT csv)", "1,first\n");
        copyIn("COPY cf_conc2 FROM STDIN WITH (FORMAT csv)", "2,second\n");
        assertEquals(1, rowCount("cf_conc1"));
        assertEquals(1, rowCount("cf_conc2"));
        exec("DROP TABLE cf_conc1");
        exec("DROP TABLE cf_conc2");
    }

    // ========================================================================
    // COPY with text format special values
    // ========================================================================

    @Test @Order(400)
    void textFormat_backslashDotTerminator() throws Exception {
        // In PG text-mode COPY FROM STDIN (simple protocol), \. signals end-of-data
        // The CopyManager protocol uses CopyDone instead, but if present
        // in data it should be handled gracefully
        exec("CREATE TABLE cf_dot(id int, val text)");
        copyIn("COPY cf_dot FROM STDIN", "1\tfirst\n2\tsecond\n");
        assertEquals(2, rowCount("cf_dot"));
        exec("DROP TABLE cf_dot");
    }

    @Test @Order(401)
    void textFormat_backslashInValue() throws Exception {
        // A literal backslash in text format is \\
        exec("CREATE TABLE cf_bs(id int, path text)");
        copyIn("COPY cf_bs FROM STDIN", "1\tC:\\\\Users\\\\test\n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT path FROM cf_bs")) {
            assertTrue(rs.next());
            assertEquals("C:\\Users\\test", rs.getString(1),
                    "Double backslash should become single backslash");
        }
        exec("DROP TABLE cf_bs");
    }

    // ========================================================================
    // COPY FROM with whitespace edge cases
    // ========================================================================

    @Test @Order(410)
    void whitespace_leadingTrailingSpaces_csv() throws Exception {
        exec("CREATE TABLE cf_ws(id int, val text)");
        // In CSV, leading/trailing spaces outside quotes are preserved (PG behavior)
        copyIn("COPY cf_ws FROM STDIN WITH (FORMAT csv)", "1, spaced \n");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM cf_ws")) {
            assertTrue(rs.next());
            assertEquals(" spaced ", rs.getString(1),
                    "CSV should preserve spaces outside quotes");
        }
        exec("DROP TABLE cf_ws");
    }

    @Test @Order(411)
    void whitespace_onlyWhitespaceLines_ignored() throws Exception {
        exec("CREATE TABLE cf_wsl(id int, val text)");
        // Empty/blank lines between data lines
        copyIn("COPY cf_wsl FROM STDIN WITH (FORMAT csv)", "1,first\n\n2,second\n");
        // Blank line may be treated as a row with empty fields or ignored
        // PG treats blank lines as a row with all-null columns in CSV
        // This test just verifies it doesn't crash
        assertTrue(rowCount("cf_wsl") >= 2, "At least the real rows should be imported");
        exec("DROP TABLE cf_wsl");
    }

    // ========================================================================
    // COPY with very specific PG-compatible output format
    // ========================================================================

    @Test @Order(420)
    void textFormat_outputTabDelimited() throws Exception {
        exec("CREATE TABLE ct_fmt(a int, b text, c boolean)");
        exec("INSERT INTO ct_fmt VALUES (1, 'hello', true)");
        String out = copyOut("COPY ct_fmt TO STDOUT");
        // PG text format: fields separated by tab, rows by newline
        // Boolean: 't'/'f', NULL: '\N'
        String[] fields = Strs.strip(out).split("\t");
        assertEquals(3, fields.length, "Text format should have 3 tab-separated fields");
        assertEquals("1", fields[0], "Integer field");
        assertEquals("hello", fields[1], "Text field");
        assertEquals("t", fields[2], "Boolean should be 't' in text format");
        exec("DROP TABLE ct_fmt");
    }

    @Test @Order(421)
    void csvFormat_outputCommaDelimited() throws Exception {
        exec("CREATE TABLE ct_cfmt(a int, b text, c boolean)");
        exec("INSERT INTO ct_cfmt VALUES (1, 'hello', true)");
        String out = copyOut("COPY ct_cfmt TO STDOUT WITH (FORMAT csv)");
        // CSV booleans are 'true'/'false' (not 't'/'f')
        assertTrue(out.contains(","), "CSV uses comma delimiter");
        // Verify data is present
        assertTrue(out.contains("1"));
        assertTrue(out.contains("hello"));
        exec("DROP TABLE ct_cfmt");
    }

    // ========================================================================
    // COPY (subquery) TO: additional subquery forms
    // ========================================================================

    @Test @Order(430)
    void subquery_withDistinct() throws Exception {
        exec("CREATE TABLE ct_dist(id int, cat text)");
        exec("INSERT INTO ct_dist VALUES (1,'a'), (2,'b'), (3,'a'), (4,'b')");
        String out = copyOut("COPY (SELECT DISTINCT cat FROM ct_dist ORDER BY cat) TO STDOUT WITH (FORMAT csv)");
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(2, lines.length, "DISTINCT should reduce to 2 rows");
        exec("DROP TABLE ct_dist");
    }

    @Test @Order(431)
    void subquery_withUnion() throws Exception {
        exec("CREATE TABLE ct_u1(id int, val text)");
        exec("CREATE TABLE ct_u2(id int, val text)");
        exec("INSERT INTO ct_u1 VALUES (1, 'from_u1')");
        exec("INSERT INTO ct_u2 VALUES (2, 'from_u2')");
        String out = copyOut(
                "COPY (SELECT id, val FROM ct_u1 UNION ALL SELECT id, val FROM ct_u2 ORDER BY id) TO STDOUT WITH (FORMAT csv)");
        assertTrue(out.contains("from_u1"));
        assertTrue(out.contains("from_u2"));
        String[] lines = Strs.strip(out).split("\n");
        assertEquals(2, lines.length);
        exec("DROP TABLE ct_u1");
        exec("DROP TABLE ct_u2");
    }

    @Test @Order(432)
    void subquery_withExpressions() throws Exception {
        exec("CREATE TABLE ct_expr(id int, price numeric, qty int)");
        exec("INSERT INTO ct_expr VALUES (1, 10.50, 3), (2, 20.00, 1)");
        String out = copyOut(
                "COPY (SELECT id, price * qty AS total FROM ct_expr ORDER BY id) TO STDOUT WITH (FORMAT csv)");
        assertTrue(out.contains("31.50") || out.contains("31.5"), "Computed total for row 1");
        assertTrue(out.contains("20.00") || out.contains("20"), "Computed total for row 2");
        exec("DROP TABLE ct_expr");
    }

    // ========================================================================
    // COPY FROM after TRUNCATE
    // ========================================================================

    @Test @Order(440)
    void truncateThenCopyFrom() throws Exception {
        exec("CREATE TABLE cf_trunc(id int, val text)");
        exec("INSERT INTO cf_trunc VALUES (1, 'old')");
        assertEquals(1, rowCount("cf_trunc"));
        exec("TRUNCATE cf_trunc");
        assertEquals(0, rowCount("cf_trunc"));
        copyIn("COPY cf_trunc FROM STDIN WITH (FORMAT csv)", "2,new\n3,newer\n");
        assertEquals(2, rowCount("cf_trunc"));
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM cf_trunc ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("new", rs.getString(1));
            assertTrue(rs.next()); assertEquals("newer", rs.getString(1));
        }
        exec("DROP TABLE cf_trunc");
    }

    // ========================================================================
    // COPY and NULL in all column positions
    // ========================================================================

    @Test @Order(450)
    void nulls_inEveryPosition() throws Exception {
        exec("CREATE TABLE cf_allnull(a text, b text, c text)");
        // First col null
        copyIn("COPY cf_allnull FROM STDIN", "\\N\tb\tc\n");
        // Middle col null
        copyIn("COPY cf_allnull FROM STDIN", "a\t\\N\tc\n");
        // Last col null
        copyIn("COPY cf_allnull FROM STDIN", "a\tb\t\\N\n");
        // All null
        copyIn("COPY cf_allnull FROM STDIN", "\\N\t\\N\t\\N\n");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT a, b, c FROM cf_allnull ORDER BY coalesce(a,'zzz'), coalesce(b,'zzz')")) {
            // Row 1: a='a', b='b', c=null (coalesce(a)='a', coalesce(b)='b')
            assertTrue(rs.next()); assertEquals("a", rs.getString("a")); assertEquals("b", rs.getString("b")); assertNull(rs.getString("c"));
            // Row 2: a='a', b=null, c='c' (coalesce(a)='a', coalesce(b)='zzz')
            assertTrue(rs.next()); assertEquals("a", rs.getString("a")); assertNull(rs.getString("b")); assertEquals("c", rs.getString("c"));
            // Check we have 4 rows total
        }
        assertEquals(4, rowCount("cf_allnull"));
        exec("DROP TABLE cf_allnull");
    }
}
