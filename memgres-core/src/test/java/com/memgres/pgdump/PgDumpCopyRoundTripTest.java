package com.memgres.pgdump;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.StringReader;
import java.io.StringWriter;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3: COPY Data Round-Trip Integrity.
 *
 * Verifies that COPY TO STDOUT → COPY FROM STDIN preserves data exactly for all types.
 * Each test inserts data, exports via COPY TO, loads into a fresh table via COPY FROM,
 * then compares the SELECT results row-by-row.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PgDumpCopyRoundTripTest {

    static Memgres memgres;
    static Connection conn;
    static CopyManager cm;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        cm = new CopyManager(conn.unwrap(BaseConnection.class));
        exec("SET timezone = 'UTC'");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String copyOut(String sql) throws Exception {
        StringWriter sw = new StringWriter();
        cm.copyOut(sql, sw);
        return sw.toString();
    }

    static void copyIn(String sql, String data) throws Exception {
        cm.copyIn(sql, new StringReader(data));
    }

    /** Compare all rows between two tables. */
    static void assertTablesEqual(String srcTable, String dstTable, String orderBy) throws SQLException {
        String srcSql = "SELECT * FROM " + srcTable + " ORDER BY " + orderBy;
        String dstSql = "SELECT * FROM " + dstTable + " ORDER BY " + orderBy;
        try (Statement s1 = conn.createStatement(); Statement s2 = conn.createStatement();
             ResultSet rs1 = s1.executeQuery(srcSql); ResultSet rs2 = s2.executeQuery(dstSql)) {
            ResultSetMetaData md = rs1.getMetaData();
            int cols = md.getColumnCount();
            int row = 0;
            while (rs1.next()) {
                assertTrue(rs2.next(), "dst table has fewer rows than src at row " + (row + 1));
                row++;
                for (int c = 1; c <= cols; c++) {
                    String colName = md.getColumnName(c);
                    String v1 = rs1.getString(c);
                    String v2 = rs2.getString(c);
                    assertEquals(v1, v2, "Row " + row + ", column '" + colName + "' differs");
                }
            }
            assertFalse(rs2.next(), "dst table has more rows than src after row " + row);
        }
    }

    /** Round-trip helper: COPY TO from src, COPY FROM into dst, compare. */
    static void roundTrip(String srcTable, String dstTable, String orderBy) throws Exception {
        String data = copyOut("COPY " + srcTable + " TO STDOUT");
        assertFalse(data.isEmpty(), "COPY TO produced no data");
        copyIn("COPY " + dstTable + " FROM STDIN", data);
        assertTablesEqual(srcTable, dstTable, orderBy);
    }

    // === Integer types ===

    @Test @Order(1)
    void roundTrip_integers() throws Exception {
        exec("CREATE TABLE rt_int_src (a smallint, b integer, c bigint, d serial)");
        exec("CREATE TABLE rt_int_dst (a smallint, b integer, c bigint, d serial)");
        exec("INSERT INTO rt_int_src (a, b, c) VALUES (1, 100, 1000000000000)");
        exec("INSERT INTO rt_int_src (a, b, c) VALUES (-32768, -2147483648, -9223372036854775808)");
        exec("INSERT INTO rt_int_src (a, b, c) VALUES (32767, 2147483647, 9223372036854775807)");
        exec("INSERT INTO rt_int_src (a, b, c) VALUES (0, 0, 0)");
        exec("INSERT INTO rt_int_src (a, b, c) VALUES (NULL, NULL, NULL)");

        roundTrip("rt_int_src", "rt_int_dst", "d");
    }

    // === Float / Numeric types ===

    @Test @Order(2)
    void roundTrip_floats() throws Exception {
        exec("CREATE TABLE rt_float_src (a real, b double precision, c numeric(10,4), d numeric)");
        exec("CREATE TABLE rt_float_dst (a real, b double precision, c numeric(10,4), d numeric)");
        exec("INSERT INTO rt_float_src VALUES (1.5, 3.14159265358979, 12345.6789, 99999999999999999.123456789)");
        exec("INSERT INTO rt_float_src VALUES (-0.0, -0.0, -0.0001, -1)");
        exec("INSERT INTO rt_float_src VALUES ('Infinity', 'Infinity', NULL, NULL)");
        exec("INSERT INTO rt_float_src VALUES ('-Infinity', '-Infinity', NULL, NULL)");
        exec("INSERT INTO rt_float_src VALUES ('NaN', 'NaN', NULL, NULL)");
        exec("INSERT INTO rt_float_src VALUES (0, 0, 0.0000, 0)");
        exec("INSERT INTO rt_float_src VALUES (NULL, NULL, NULL, NULL)");

        roundTrip("rt_float_src", "rt_float_dst", "a, b NULLS LAST");
    }

    // === String types ===

    @Test @Order(3)
    void roundTrip_strings() throws Exception {
        exec("CREATE TABLE rt_str_src (a text, b varchar(100), c char(10))");
        exec("CREATE TABLE rt_str_dst (a text, b varchar(100), c char(10))");
        exec("INSERT INTO rt_str_src VALUES ('hello', 'world', 'abc')");
        exec("INSERT INTO rt_str_src VALUES ('', '', '          ')"); // char(10) pads
        exec("INSERT INTO rt_str_src VALUES ('line1\nline2', 'tab\there', 'back\\slash')");
        exec("INSERT INTO rt_str_src VALUES (NULL, NULL, NULL)");

        roundTrip("rt_str_src", "rt_str_dst", "a NULLS LAST");
    }

    // === Bytea ===

    @Test @Order(4)
    void roundTrip_binary() throws Exception {
        exec("CREATE TABLE rt_bytea_src (a bytea)");
        exec("CREATE TABLE rt_bytea_dst (a bytea)");
        exec("INSERT INTO rt_bytea_src VALUES ('\\xDEADBEEF')");
        exec("INSERT INTO rt_bytea_src VALUES ('\\x00FF00FF')");
        exec("INSERT INTO rt_bytea_src VALUES ('\\x')"); // empty bytea
        exec("INSERT INTO rt_bytea_src VALUES (NULL)");

        roundTrip("rt_bytea_src", "rt_bytea_dst", "a NULLS LAST");
    }

    // === Temporal types ===

    @Test @Order(5)
    void roundTrip_temporal() throws Exception {
        exec("CREATE TABLE rt_time_src (a date, b time, c timestamp, d timestamptz, e interval)");
        exec("CREATE TABLE rt_time_dst (a date, b time, c timestamp, d timestamptz, e interval)");
        exec("INSERT INTO rt_time_src VALUES ('2024-01-15', '10:30:00', '2024-01-15 10:30:00', '2024-01-15 10:30:00+00', '1 year 2 mons 3 days 04:05:06')");
        exec("INSERT INTO rt_time_src VALUES ('1970-01-01', '00:00:00', '1970-01-01 00:00:00', '1970-01-01 00:00:00+00', '0')");
        exec("INSERT INTO rt_time_src VALUES ('2099-12-31', '23:59:59.999999', '2099-12-31 23:59:59.999999', '2099-12-31 23:59:59.999999+00', '-1 days -02:03:04.567')");
        exec("INSERT INTO rt_time_src VALUES ('0001-01-01', '12:00:00', '0001-01-01 00:00:00', '0001-01-01 00:00:00+00', '100 years')");
        exec("INSERT INTO rt_time_src VALUES (NULL, NULL, NULL, NULL, NULL)");

        roundTrip("rt_time_src", "rt_time_dst", "a NULLS LAST");
    }

    // === Boolean ===

    @Test @Order(6)
    void roundTrip_boolean() throws Exception {
        exec("CREATE TABLE rt_bool_src (a boolean)");
        exec("CREATE TABLE rt_bool_dst (a boolean)");
        exec("INSERT INTO rt_bool_src VALUES (true)");
        exec("INSERT INTO rt_bool_src VALUES (false)");
        exec("INSERT INTO rt_bool_src VALUES (NULL)");

        roundTrip("rt_bool_src", "rt_bool_dst", "a NULLS LAST");
    }

    // === UUID ===

    @Test @Order(7)
    void roundTrip_uuid() throws Exception {
        exec("CREATE TABLE rt_uuid_src (a uuid)");
        exec("CREATE TABLE rt_uuid_dst (a uuid)");
        exec("INSERT INTO rt_uuid_src VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        exec("INSERT INTO rt_uuid_src VALUES ('00000000-0000-0000-0000-000000000000')");
        exec("INSERT INTO rt_uuid_src VALUES ('ffffffff-ffff-ffff-ffff-ffffffffffff')");
        exec("INSERT INTO rt_uuid_src VALUES (NULL)");

        roundTrip("rt_uuid_src", "rt_uuid_dst", "a NULLS LAST");
    }

    // === JSON / JSONB ===

    @Test @Order(8)
    void roundTrip_json() throws Exception {
        exec("CREATE TABLE rt_json_src (a json, b jsonb)");
        exec("CREATE TABLE rt_json_dst (a json, b jsonb)");
        exec("INSERT INTO rt_json_src VALUES ('{\"key\": \"value\"}', '{\"key\": \"value\"}')");
        exec("INSERT INTO rt_json_src VALUES ('[1, 2, 3]', '[1, 2, 3]')");
        exec("INSERT INTO rt_json_src VALUES ('null', 'null')");
        exec("INSERT INTO rt_json_src VALUES ('\"string\"', '\"string\"')");
        exec("INSERT INTO rt_json_src VALUES ('42', '42')");
        exec("INSERT INTO rt_json_src VALUES ('{\"nested\": {\"deep\": [true, false, null]}}', '{\"nested\": {\"deep\": [true, false, null]}}')");
        exec("INSERT INTO rt_json_src VALUES (NULL, NULL)");

        roundTrip("rt_json_src", "rt_json_dst", "a::text NULLS LAST");
    }

    // === Arrays ===

    @Test @Order(9)
    void roundTrip_arrays() throws Exception {
        exec("CREATE TABLE rt_arr_src (a int[], b text[], c boolean[])");
        exec("CREATE TABLE rt_arr_dst (a int[], b text[], c boolean[])");
        exec("INSERT INTO rt_arr_src VALUES ('{1,2,3}', '{hello,world}', '{t,f,t}')");
        exec("INSERT INTO rt_arr_src VALUES ('{}', '{}', '{}')"); // empty arrays
        exec("INSERT INTO rt_arr_src VALUES ('{NULL,1,NULL}', '{NULL,\"has,comma\",NULL}', '{NULL,true,NULL}')");
        exec("INSERT INTO rt_arr_src VALUES (NULL, NULL, NULL)");

        roundTrip("rt_arr_src", "rt_arr_dst", "a::text NULLS LAST");
    }

    // === Enums ===

    @Test @Order(10)
    void roundTrip_enums() throws Exception {
        exec("CREATE TYPE rt_mood AS ENUM ('happy', 'sad', 'neutral')");
        exec("CREATE TABLE rt_enum_src (a rt_mood)");
        exec("CREATE TABLE rt_enum_dst (a rt_mood)");
        exec("INSERT INTO rt_enum_src VALUES ('happy')");
        exec("INSERT INTO rt_enum_src VALUES ('sad')");
        exec("INSERT INTO rt_enum_src VALUES ('neutral')");
        exec("INSERT INTO rt_enum_src VALUES (NULL)");

        roundTrip("rt_enum_src", "rt_enum_dst", "a NULLS LAST");
    }

    // === Domains ===

    @Test @Order(11)
    void roundTrip_domains() throws Exception {
        exec("CREATE DOMAIN rt_posint AS integer CHECK (VALUE > 0)");
        exec("CREATE DOMAIN rt_email AS text CHECK (VALUE LIKE '%@%')");
        exec("CREATE TABLE rt_dom_src (a rt_posint, b rt_email)");
        exec("CREATE TABLE rt_dom_dst (a rt_posint, b rt_email)");
        exec("INSERT INTO rt_dom_src VALUES (1, 'test@example.com')");
        exec("INSERT INTO rt_dom_src VALUES (999, 'admin@corp.org')");
        exec("INSERT INTO rt_dom_src VALUES (NULL, NULL)");

        roundTrip("rt_dom_src", "rt_dom_dst", "a NULLS LAST");
    }

    // === NULLs in all positions ===

    @Test @Order(12)
    void roundTrip_nullsInAllPositions() throws Exception {
        exec("CREATE TABLE rt_nulls_src (a text, b int, c boolean, d jsonb, e text[], f uuid)");
        exec("CREATE TABLE rt_nulls_dst (a text, b int, c boolean, d jsonb, e text[], f uuid)");
        // All NULL
        exec("INSERT INTO rt_nulls_src VALUES (NULL, NULL, NULL, NULL, NULL, NULL)");
        // NULL in each position
        exec("INSERT INTO rt_nulls_src VALUES (NULL, 1, true, '{}', '{a}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        exec("INSERT INTO rt_nulls_src VALUES ('x', NULL, true, '{}', '{a}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        exec("INSERT INTO rt_nulls_src VALUES ('x', 1, NULL, '{}', '{a}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        exec("INSERT INTO rt_nulls_src VALUES ('x', 1, true, NULL, '{a}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        exec("INSERT INTO rt_nulls_src VALUES ('x', 1, true, '{}', NULL, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        exec("INSERT INTO rt_nulls_src VALUES ('x', 1, true, '{}', '{a}', NULL)");

        roundTrip("rt_nulls_src", "rt_nulls_dst", "a NULLS FIRST, b NULLS FIRST, c NULLS FIRST, d::text NULLS FIRST, e::text NULLS FIRST, f NULLS FIRST");
    }

    // === Special characters ===

    @Test @Order(13)
    void roundTrip_specialCharacters() throws Exception {
        exec("CREATE TABLE rt_spec_src (a text, b text)");
        exec("CREATE TABLE rt_spec_dst (a text, b text)");
        exec("INSERT INTO rt_spec_src VALUES ('tab\there', 'newline\nhere')");
        exec("INSERT INTO rt_spec_src VALUES ('backslash\\here', 'quote''here')");
        exec("INSERT INTO rt_spec_src VALUES ('carriage\rreturn', 'mixed\t\n\r\\all')");
        exec("INSERT INTO rt_spec_src VALUES ('double\"quote', 'pipe|bar')");

        roundTrip("rt_spec_src", "rt_spec_dst", "a");
    }

    // === Empty strings ===

    @Test @Order(14)
    void roundTrip_emptyStrings() throws Exception {
        exec("CREATE TABLE rt_empty_src (a text, b varchar(50), c text)");
        exec("CREATE TABLE rt_empty_dst (a text, b varchar(50), c text)");
        exec("INSERT INTO rt_empty_src VALUES ('', '', '')");
        exec("INSERT INTO rt_empty_src VALUES ('', 'notempty', '')");
        exec("INSERT INTO rt_empty_src VALUES ('notempty', '', 'notempty')");
        exec("INSERT INTO rt_empty_src VALUES (NULL, '', NULL)");

        roundTrip("rt_empty_src", "rt_empty_dst", "a NULLS FIRST, b, c NULLS FIRST");
    }

    // === Unicode ===

    @Test @Order(15)
    void roundTrip_unicodeValues() throws Exception {
        exec("CREATE TABLE rt_uni_src (a text, b text)");
        exec("CREATE TABLE rt_uni_dst (a text, b text)");
        exec("INSERT INTO rt_uni_src VALUES ('emoji: \uD83D\uDE00\uD83C\uDF0D', 'CJK: \u4e16\u754c')");
        exec("INSERT INTO rt_uni_src VALUES ('accents: \u00E9\u00E8\u00EA\u00EB', 'cyrillic: \u041F\u0440\u0438\u0432\u0435\u0442')");
        exec("INSERT INTO rt_uni_src VALUES ('combining: e\u0301', 'zero-width: a\u200Bb')"); // combining acute, ZWS
        exec("INSERT INTO rt_uni_src VALUES ('arabic: \u0645\u0631\u062D\u0628\u0627', 'hebrew: \u05E9\u05DC\u05D5\u05DD')");

        roundTrip("rt_uni_src", "rt_uni_dst", "a");
    }

    // === Numeric edge cases ===

    @Test @Order(16)
    void roundTrip_numericEdgeCases() throws Exception {
        exec("CREATE TABLE rt_numedge_src (a real, b double precision, c numeric)");
        exec("CREATE TABLE rt_numedge_dst (a real, b double precision, c numeric)");
        exec("INSERT INTO rt_numedge_src VALUES ('NaN', 'NaN', NULL)");
        exec("INSERT INTO rt_numedge_src VALUES ('Infinity', 'Infinity', NULL)");
        exec("INSERT INTO rt_numedge_src VALUES ('-Infinity', '-Infinity', NULL)");
        exec("INSERT INTO rt_numedge_src VALUES (1e-37, 1e-307, 0.000000000000000000000000000001)");
        exec("INSERT INTO rt_numedge_src VALUES (1e37, 1e307, 99999999999999999999999999999999999999)");
        exec("INSERT INTO rt_numedge_src VALUES (0, 0, 0)");
        exec("INSERT INTO rt_numedge_src VALUES (-0.0, -0.0, -0.00)");

        roundTrip("rt_numedge_src", "rt_numedge_dst", "a NULLS LAST, b NULLS LAST");
    }

    // === CSV format round-trip ===

    @Test @Order(17)
    void roundTrip_csvFormat() throws Exception {
        exec("CREATE TABLE rt_csv_src (a text, b int, c boolean, d text)");
        exec("CREATE TABLE rt_csv_dst (a text, b int, c boolean, d text)");
        exec("INSERT INTO rt_csv_src VALUES ('hello', 1, true, 'world')");
        exec("INSERT INTO rt_csv_src VALUES ('has,comma', 2, false, 'has\"quote')");
        exec("INSERT INTO rt_csv_src VALUES ('', 0, true, '')");
        exec("INSERT INTO rt_csv_src VALUES (NULL, NULL, NULL, NULL)");

        String data = copyOut("COPY rt_csv_src TO STDOUT (FORMAT csv)");
        assertFalse(data.isEmpty());
        copyIn("COPY rt_csv_dst FROM STDIN (FORMAT csv)", data);
        assertTablesEqual("rt_csv_src", "rt_csv_dst", "b NULLS LAST");
    }

    // === Binary format round-trip ===

    @Test @Order(18)
    void roundTrip_binaryFormat() throws Exception {
        exec("CREATE TABLE rt_bin_src (a int, b text, c boolean, d numeric(8,2))");
        exec("CREATE TABLE rt_bin_dst (a int, b text, c boolean, d numeric(8,2))");
        exec("INSERT INTO rt_bin_src VALUES (1, 'alice', true, 95.50)");
        exec("INSERT INTO rt_bin_src VALUES (2, 'bob', false, 0.00)");
        exec("INSERT INTO rt_bin_src VALUES (NULL, NULL, NULL, NULL)");

        // Binary round-trip via byte streams
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        cm.copyOut("COPY rt_bin_src TO STDOUT (FORMAT binary)", baos);
        java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(baos.toByteArray());
        cm.copyIn("COPY rt_bin_dst FROM STDIN (FORMAT binary)", bais);
        assertTablesEqual("rt_bin_src", "rt_bin_dst", "a NULLS LAST");
    }

    // === Network types round-trip ===

    @Test @Order(19)
    void roundTrip_networkTypes() throws Exception {
        exec("CREATE TABLE rt_net_src (a inet, b cidr)");
        exec("CREATE TABLE rt_net_dst (a inet, b cidr)");
        exec("INSERT INTO rt_net_src VALUES ('192.168.1.1', '10.0.0.0/8')");
        exec("INSERT INTO rt_net_src VALUES ('::1', '::ffff:0:0/96')");
        exec("INSERT INTO rt_net_src VALUES (NULL, NULL)");

        roundTrip("rt_net_src", "rt_net_dst", "a NULLS LAST");
    }

    // === Column list round-trip ===

    @Test @Order(20)
    void roundTrip_columnList() throws Exception {
        exec("CREATE TABLE rt_collist_src (id serial, name text, score int DEFAULT 0)");
        exec("CREATE TABLE rt_collist_dst (id serial, name text, score int DEFAULT 0)");
        exec("INSERT INTO rt_collist_src (name, score) VALUES ('alice', 100)");
        exec("INSERT INTO rt_collist_src (name, score) VALUES ('bob', 200)");

        // COPY only specific columns
        String data = copyOut("COPY rt_collist_src (name, score) TO STDOUT");
        copyIn("COPY rt_collist_dst (name, score) FROM STDIN", data);
        // Compare only the copied columns (id will differ due to serial)
        try (Statement s1 = conn.createStatement(); Statement s2 = conn.createStatement();
             ResultSet rs1 = s1.executeQuery("SELECT name, score FROM rt_collist_src ORDER BY name");
             ResultSet rs2 = s2.executeQuery("SELECT name, score FROM rt_collist_dst ORDER BY name")) {
            while (rs1.next()) {
                assertTrue(rs2.next());
                assertEquals(rs1.getString(1), rs2.getString(1));
                assertEquals(rs1.getInt(2), rs2.getInt(2));
            }
            assertFalse(rs2.next());
        }
    }

    // === Large row count ===

    @Test @Order(21)
    void roundTrip_largeRowCount() throws Exception {
        exec("CREATE TABLE rt_large_src (id int, val text)");
        exec("CREATE TABLE rt_large_dst (id int, val text)");

        // Insert 1000 rows using generate_series
        exec("INSERT INTO rt_large_src SELECT g, 'row_' || g FROM generate_series(1, 1000) g");

        roundTrip("rt_large_src", "rt_large_dst", "id");

        // Verify count
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM rt_large_dst")) {
            rs.next();
            assertEquals(1000, rs.getInt(1));
        }
    }

    // === Mixed-type pg_dump-style table ===

    @Test @Order(22)
    void roundTrip_pgDumpStyleTable() throws Exception {
        exec("CREATE TYPE rt_status AS ENUM ('pending', 'active', 'closed')");
        exec("""
            CREATE TABLE rt_dump_src (
                id serial PRIMARY KEY,
                name text NOT NULL,
                email varchar(255),
                status rt_status DEFAULT 'pending',
                score numeric(5,2),
                active boolean DEFAULT true,
                tags text[],
                metadata jsonb DEFAULT '{}',
                uid uuid,
                created_at timestamptz DEFAULT now()
            )""");
        exec("""
            CREATE TABLE rt_dump_dst (
                id serial PRIMARY KEY,
                name text NOT NULL,
                email varchar(255),
                status rt_status DEFAULT 'pending',
                score numeric(5,2),
                active boolean DEFAULT true,
                tags text[],
                metadata jsonb DEFAULT '{}',
                uid uuid,
                created_at timestamptz DEFAULT now()
            )""");

        exec("INSERT INTO rt_dump_src (name, email, status, score, active, tags, metadata, uid) " +
             "VALUES ('Alice', 'alice@example.com', 'active', 98.50, true, '{loyal,vip}', " +
             "'{\"tier\": \"gold\"}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
        exec("INSERT INTO rt_dump_src (name, email, status, score, active, tags, metadata, uid) " +
             "VALUES ('Bob', 'bob@example.com', 'pending', 75.25, true, '{new}', " +
             "'{\"tier\": \"silver\"}', 'b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22')");
        exec("INSERT INTO rt_dump_src (name, email, status, score, active, tags, metadata, uid) " +
             "VALUES ('Carol', NULL, 'closed', NULL, false, NULL, NULL, NULL)");

        roundTrip("rt_dump_src", "rt_dump_dst", "id");
    }

    // === COPY TO matches COPY FROM exactly (text preserved) ===

    @Test @Order(23)
    void roundTrip_copyOutputPreserved() throws Exception {
        exec("CREATE TABLE rt_pres_src (a text, b int, c boolean)");
        exec("CREATE TABLE rt_pres_dst (a text, b int, c boolean)");
        exec("INSERT INTO rt_pres_src VALUES ('hello', 1, true)");
        exec("INSERT INTO rt_pres_src VALUES (NULL, NULL, NULL)");

        String out1 = copyOut("COPY rt_pres_src TO STDOUT");
        copyIn("COPY rt_pres_dst FROM STDIN", out1);
        String out2 = copyOut("COPY rt_pres_dst TO STDOUT");

        assertEquals(out1, out2, "COPY output changed after round-trip");
    }

    // === Multiple round-trips preserve data ===

    @Test @Order(24)
    void roundTrip_multipleIterations() throws Exception {
        exec("CREATE TABLE rt_multi_a (a text, b numeric, c boolean, d timestamptz)");
        exec("CREATE TABLE rt_multi_b (a text, b numeric, c boolean, d timestamptz)");
        exec("CREATE TABLE rt_multi_c (a text, b numeric, c boolean, d timestamptz)");
        exec("INSERT INTO rt_multi_a VALUES ('test', 123.456, true, '2024-06-15 10:30:00+00')");
        exec("INSERT INTO rt_multi_a VALUES (NULL, NULL, NULL, NULL)");

        // a → b → c, verify c matches a
        String d1 = copyOut("COPY rt_multi_a TO STDOUT");
        copyIn("COPY rt_multi_b FROM STDIN", d1);
        String d2 = copyOut("COPY rt_multi_b TO STDOUT");
        copyIn("COPY rt_multi_c FROM STDIN", d2);
        String d3 = copyOut("COPY rt_multi_c TO STDOUT");

        assertEquals(d1, d2, "Data changed after first round-trip");
        assertEquals(d2, d3, "Data changed after second round-trip");
        assertTablesEqual("rt_multi_a", "rt_multi_c", "a NULLS LAST");
    }

    // === Custom delimiter round-trip ===

    @Test @Order(25)
    void roundTrip_customDelimiter() throws Exception {
        exec("CREATE TABLE rt_delim_src (a text, b int, c text)");
        exec("CREATE TABLE rt_delim_dst (a text, b int, c text)");
        exec("INSERT INTO rt_delim_src VALUES ('hello', 1, 'world')");
        exec("INSERT INTO rt_delim_src VALUES ('no-pipe-here', 2, 'ok')");
        exec("INSERT INTO rt_delim_src VALUES (NULL, NULL, NULL)");

        String data = copyOut("COPY rt_delim_src TO STDOUT (DELIMITER '|')");
        copyIn("COPY rt_delim_dst FROM STDIN (DELIMITER '|')", data);
        assertTablesEqual("rt_delim_src", "rt_delim_dst", "b NULLS LAST");
    }

    // === Interval edge cases ===

    @Test @Order(26)
    void roundTrip_intervalEdgeCases() throws Exception {
        exec("CREATE TABLE rt_iv_src (a interval)");
        exec("CREATE TABLE rt_iv_dst (a interval)");
        exec("INSERT INTO rt_iv_src VALUES ('1 year 2 mons 3 days 04:05:06')");
        exec("INSERT INTO rt_iv_src VALUES ('-1 year -2 mons -3 days -04:05:06')");
        exec("INSERT INTO rt_iv_src VALUES ('0')");
        exec("INSERT INTO rt_iv_src VALUES ('1 day')");
        exec("INSERT INTO rt_iv_src VALUES ('-1 days')");
        exec("INSERT INTO rt_iv_src VALUES ('00:00:00.000001')"); // 1 microsecond
        exec("INSERT INTO rt_iv_src VALUES ('178000000 years')"); // near max
        exec("INSERT INTO rt_iv_src VALUES (NULL)");

        roundTrip("rt_iv_src", "rt_iv_dst", "a NULLS LAST");
    }
}
