package com.memgres.types;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for composite type rendering, ROW expressions, and unnest on typed arrays.
 *
 * PG renders composite values as (val1,val2), not PgRow[values=[...]].
 * unnest on enum arrays should return individual elements, not the whole array.
 * Array slice column naming: b[2:3] should keep column name 'b', not '?column?'.
 */
class CompositeTypeAndRowTest {

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

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static List<String> column(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            List<String> vals = new ArrayList<>();
            while (rs.next()) vals.add(rs.getString(1));
            return vals;
        }
    }

    static List<String> columnNames(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> cols = new ArrayList<>();
            for (int i = 1; i <= md.getColumnCount(); i++) cols.add(md.getColumnName(i));
            return cols;
        }
    }

    // ========================================================================
    // Composite type rendering
    // ========================================================================

    @Test
    void nested_composite_renders_as_parenthesized_text() throws SQLException {
        exec("CREATE TYPE test_addr AS (street text, zip int)");
        exec("CREATE TYPE test_contact AS (name text, home test_addr)");
        exec("CREATE TABLE test_people(id int PRIMARY KEY, info test_contact)");
        try {
            exec("INSERT INTO test_people VALUES (1, ROW('Ann', ROW('Main', 12345)))");
            String val = scalar("SELECT info FROM test_people WHERE id = 1");
            // PG renders this as: (Ann,"(Main,12345)")
            assertNotNull(val);
            assertTrue(val.contains("Ann"), "Should contain name");
            assertFalse(val.contains("PgRow"), "Should NOT render as PgRow[values=[...]]");
            assertTrue(val.startsWith("(") && val.endsWith(")"),
                    "Composite value should be rendered in parenthesized form: " + val);
        } finally {
            exec("DROP TABLE IF EXISTS test_people");
            exec("DROP TYPE IF EXISTS test_contact");
            exec("DROP TYPE IF EXISTS test_addr");
        }
    }

    @Test
    void row_expression_renders_as_parenthesized() throws SQLException {
        String val = scalar("SELECT ROW(1, 'hello', true)");
        assertNotNull(val);
        assertTrue(val.startsWith("(") && val.endsWith(")"),
                "ROW expression should render as (1,hello,t): " + val);
        assertFalse(val.contains("PgRow"), "Should not contain PgRow");
    }

    @Test
    void composite_in_array_renders_correctly() throws SQLException {
        exec("CREATE TYPE test_pair AS (x int, y text)");
        try {
            String val = scalar("SELECT ARRAY[ROW(1,'a')::test_pair, ROW(2,'b')::test_pair]");
            assertNotNull(val);
            // PG renders as: {"(1,a)","(2,b)"}
            assertTrue(val.contains("(1,a)") || val.contains("(1, a)"),
                    "Array of composites should render elements in parenthesized form: " + val);
        } finally {
            exec("DROP TYPE IF EXISTS test_pair");
        }
    }

    // ========================================================================
    // unnest on enum arrays
    // ========================================================================

    @Test
    void unnest_enum_array_returns_individual_elements() throws SQLException {
        exec("CREATE TYPE test_mood AS ENUM ('sad', 'ok', 'happy')");
        exec("CREATE TABLE test_moods(id int, moods test_mood[])");
        try {
            exec("INSERT INTO test_moods VALUES (1, ARRAY['ok'::test_mood, 'happy'::test_mood])");
            exec("INSERT INTO test_moods VALUES (2, ARRAY['sad'::test_mood])");

            List<String> vals = column("SELECT unnest(moods) FROM test_moods ORDER BY 1");
            // Should be 3 individual values: happy, ok, sad (sorted)
            assertEquals(3, vals.size(), "unnest should produce 3 individual enum values, not 2 arrays");
            assertTrue(vals.contains("sad"), "Should contain 'sad'");
            assertTrue(vals.contains("ok"), "Should contain 'ok'");
            assertTrue(vals.contains("happy"), "Should contain 'happy'");
        } finally {
            exec("DROP TABLE IF EXISTS test_moods");
            exec("DROP TYPE IF EXISTS test_mood");
        }
    }

    @Test
    void unnest_integer_array() throws SQLException {
        List<String> vals = column("SELECT unnest(ARRAY[3,1,2]) ORDER BY 1");
        assertEquals(Cols.listOf("1", "2", "3"), vals);
    }

    // ========================================================================
    // Array slice column naming
    // ========================================================================

    @Test
    void array_subscript_preserves_column_name() throws SQLException {
        exec("CREATE TABLE arr_t(id int, b int[])");
        try {
            exec("INSERT INTO arr_t VALUES (1, ARRAY[10,20,30])");
            List<String> cols = columnNames("SELECT b[1] FROM arr_t");
            assertEquals("b", cols.get(0),
                    "Array subscript b[1] should keep column name 'b', not '?column?'");
        } finally {
            exec("DROP TABLE IF EXISTS arr_t");
        }
    }

    @Test
    void array_slice_preserves_column_name() throws SQLException {
        exec("CREATE TABLE arr_t2(id int, b int[])");
        try {
            exec("INSERT INTO arr_t2 VALUES (1, ARRAY[10,20,30])");
            List<String> cols = columnNames("SELECT b[2:3] FROM arr_t2");
            assertEquals("b", cols.get(0),
                    "Array slice b[2:3] should keep column name 'b', not '?column?'");
        } finally {
            exec("DROP TABLE IF EXISTS arr_t2");
        }
    }

    // ========================================================================
    // ROW comparison and field access
    // ========================================================================

    @Test
    void row_equality_comparison() throws SQLException {
        String val = scalar("SELECT ROW(1,'x') = ROW(1,'x')");
        assertEquals("t", val);
    }

    @Test
    void row_less_than_comparison() throws SQLException {
        String val = scalar("SELECT ROW(1,'x') < ROW(2,'a')");
        assertEquals("t", val);
    }

    @Test
    void row_field_access_gives_proper_error() {
        // (SELECT ROW(1,2)).x should fail with 42703 (undefined_column)
        try {
            exec("SELECT (SELECT ROW(1,2)).x");
            fail("Should fail because anonymous ROW has no named fields");
        } catch (SQLException e) {
            assertEquals("42703", e.getSQLState(),
                    "Expected 42703 (undefined_column) for field access on anonymous ROW");
        }
    }

    @Test
    void composite_field_expansion_via_dot_star() throws SQLException {
        exec("CREATE TYPE test_addr2 AS (street text, zip int)");
        exec("CREATE TABLE test_p2(id int, home test_addr2)");
        try {
            exec("INSERT INTO test_p2 VALUES (1, ROW('Main', 12345))");
            List<String> cols = columnNames("SELECT (home).* FROM test_p2");
            assertEquals(2, cols.size(), "composite.* should expand to 2 columns");
        } finally {
            exec("DROP TABLE IF EXISTS test_p2");
            exec("DROP TYPE IF EXISTS test_addr2");
        }
    }
}
