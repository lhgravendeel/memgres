package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category F: Array / JSON / XML edge cases.
 *
 * Covers:
 *  - Multi-dim literal '{{1,2},{3,4}}'::int[][] parses into a 2D array
 *  - Array equality with NULL elements propagates element-wise
 *  - jsonb_populate_record / jsonb_populate_recordset
 *  - xmlserialize DOCUMENT mode enforces single-root, INDENT option
 */
class Round16ArrayJsonXmlEdgeTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // F1. Multi-dim array literal
    // =========================================================================

    @Test
    void two_dim_int_array_literal_round_trip() throws SQLException {
        // array_dims = '[1:2][1:2]'
        String dims = scalarString("SELECT array_dims('{{1,2},{3,4}}'::int[][])");
        assertEquals("[1:2][1:2]", dims,
                "'{{1,2},{3,4}}'::int[][] must have array_dims = [1:2][1:2]");
    }

    @Test
    void two_dim_int_array_element_access() throws SQLException {
        int v = scalarInt("SELECT ('{{1,2},{3,4}}'::int[][])[2][1]");
        assertEquals(3, v, "(2,1) element of {{1,2},{3,4}} must be 3");
    }

    @Test
    void two_dim_int_array_cardinality() throws SQLException {
        int n = scalarInt("SELECT cardinality('{{1,2},{3,4}}'::int[][])");
        assertEquals(4, n, "cardinality of a 2x2 int[][] must be 4 (total elements)");
    }

    // =========================================================================
    // F2. Array equality element-wise NULL handling
    // =========================================================================

    @Test
    void array_with_null_equality_is_null_not_true() throws SQLException {
        // PG 18: ARRAY[1, NULL] = ARRAY[1, NULL] propagates NULL → (bool) NULL
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT (ARRAY[1, NULL]::int[] = ARRAY[1, NULL]::int[])::text")) {
            assertTrue(rs.next());
            Object o = rs.getObject(1);
            assertNull(o,
                    "ARRAY[1,NULL] = ARRAY[1,NULL] must propagate NULL element-wise → NULL; got: " + o);
        }
    }

    @Test
    void array_of_non_nulls_equal_is_true() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ARRAY[1,2,3] = ARRAY[1,2,3]")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // =========================================================================
    // F3. jsonb_populate_record
    // =========================================================================

    @Test
    void jsonb_populate_record_fills_row_type() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS r16_jp (a int, b text)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT a, b FROM jsonb_populate_record("
                             + "NULL::r16_jp, '{\"a\": 42, \"b\": \"hi\"}'::jsonb)")) {
            assertTrue(rs.next());
            assertEquals(42,   rs.getInt("a"));
            assertEquals("hi", rs.getString("b"));
        }
    }

    @Test
    void jsonb_populate_recordset_fills_multiple_rows() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS r16_jp2 (a int, b text)");
        int n = scalarInt(
                "SELECT count(*)::int FROM jsonb_populate_recordset("
                        + "NULL::r16_jp2, '[{\"a\":1,\"b\":\"x\"},{\"a\":2,\"b\":\"y\"}]'::jsonb)");
        assertEquals(2, n, "jsonb_populate_recordset must emit 2 rows from 2-element array");
    }

    // =========================================================================
    // F4. xmlserialize DOCUMENT mode — single root enforcement
    // =========================================================================

    @Test
    void xmlserialize_document_rejects_multi_root() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT xmlserialize(DOCUMENT '<a/><b/>'::xml AS text)")) {
            if (rs.next()) {
                String got = rs.getString(1);
                fail("xmlserialize DOCUMENT multi-root must raise an error; got " + got);
            }
        } catch (SQLException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    void xmlserialize_with_indent_produces_indented_output() throws SQLException {
        String v = scalarString(
                "SELECT xmlserialize(DOCUMENT '<a><b/></a>'::xml AS text INDENT)");
        assertNotNull(v);
        // INDENT should introduce newlines between nested elements
        assertTrue(v.contains("\n"),
                "xmlserialize … INDENT must emit multi-line indented text; got: " + v);
    }
}
