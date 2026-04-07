package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for jsonb_path_query (SRF), jsonb_path_match, generate_subscripts,
 * ARRAY[ROW(...)::record], and domain array constraints.
 *
 * Key PG behaviors:
 * - jsonb_path_query is a set-returning function: one row per match
 * - jsonb_path_match evaluates to boolean, supports exists() in jsonpath
 * - generate_subscripts on multidimensional arrays returns per-dimension subscripts
 * - Domain constraints on array types enforce check on the whole array
 */
class JsonPathAndArrayTest {

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

    static int countRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = 0; while (rs.next()) n++; return n;
        }
    }

    static List<String> column(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            List<String> vals = new ArrayList<>();
            while (rs.next()) vals.add(rs.getString(1));
            return vals;
        }
    }

    // ========================================================================
    // jsonb_path_query as set-returning function
    // ========================================================================

    @Test
    void jsonb_path_query_array_elements() throws SQLException {
        int count = countRows("SELECT jsonb_path_query('{\"a\":[1,2,3]}'::jsonb, '$.a[*]')");
        assertEquals(3, count, "jsonb_path_query on 3-element array should return 3 rows");
    }

    @Test
    void jsonb_path_query_nested_objects() throws SQLException {
        int count = countRows(
                "SELECT jsonb_path_query('{\"items\":[{\"x\":1},{\"x\":2}]}'::jsonb, '$.items[*].x')");
        assertEquals(2, count, "jsonb_path_query should extract 2 nested values");
    }

    @Test
    void jsonb_path_query_single_value() throws SQLException {
        String val = scalar("SELECT jsonb_path_query('{\"a\":42}'::jsonb, '$.a')");
        assertEquals("42", val);
    }

    @Test
    void jsonb_path_query_no_match_returns_empty() throws SQLException {
        int count = countRows("SELECT jsonb_path_query('{\"a\":1}'::jsonb, '$.b')");
        assertEquals(0, count, "Non-matching path should return 0 rows");
    }

    // ========================================================================
    // jsonb_path_match
    // ========================================================================

    @Test
    void jsonb_path_match_simple_predicate() throws SQLException {
        String val = scalar("SELECT jsonb_path_match('{\"a\":2}'::jsonb, '$.a == 2')");
        assertEquals("t", val);
    }

    @Test
    void jsonb_path_match_with_exists() throws SQLException {
        String val = scalar("SELECT jsonb_path_match('{\"a\":2}'::jsonb, 'exists($.a ? (@ == 2))')");
        assertEquals("t", val, "jsonb_path_match with exists() should return true");
    }

    @Test
    void jsonb_path_match_false_predicate() throws SQLException {
        String val = scalar("SELECT jsonb_path_match('{\"a\":2}'::jsonb, '$.a == 99')");
        assertEquals("f", val);
    }

    // ========================================================================
    // generate_subscripts on multidimensional arrays
    // ========================================================================

    @Test
    void generate_subscripts_2d_array() throws SQLException {
        // ARRAY[[1,2],[3,4]] has dimension 1 with 2 elements and dimension 2 with 2 elements
        List<String> dim1 = column("SELECT generate_subscripts(ARRAY[[1,2],[3,4]], 1)");
        assertEquals(2, dim1.size(), "Dimension 1 should have 2 subscripts");
        assertEquals("1", dim1.get(0));
        assertEquals("2", dim1.get(1));

        List<String> dim2 = column("SELECT generate_subscripts(ARRAY[[1,2],[3,4]], 2)");
        assertEquals(2, dim2.size(), "Dimension 2 should have 2 subscripts");
    }

    @Test
    void generate_subscripts_produces_cross_product_in_from() throws SQLException {
        // When used in FROM twice for different dimensions, should produce cross product
        int count = countRows("""
            SELECT a.i, b.j
            FROM generate_subscripts(ARRAY[[1,2],[3,4]], 1) AS a(i),
                 generate_subscripts(ARRAY[[1,2],[3,4]], 2) AS b(j)
            """);
        assertEquals(4, count, "Cross product of 2x2 subscripts should be 4 rows");
    }

    // ========================================================================
    // Domain with array type constraint
    // ========================================================================

    @Test
    void domain_array_check_constraint() throws SQLException {
        exec("CREATE DOMAIN posint_arr AS int[] CHECK (array_length(VALUE, 1) > 0)");
        exec("CREATE TABLE darr_t(id int PRIMARY KEY, vals posint_arr)");
        try {
            // Valid: non-empty array
            exec("INSERT INTO darr_t VALUES (1, ARRAY[1,2,3])");

            // PG18: array_length(ARRAY[]::int[], 1) returns NULL, and NULL > 0 is NULL,
            // which does NOT violate CHECK (only explicit false violates). So empty array is allowed.
            exec("INSERT INTO darr_t VALUES (2, ARRAY[]::int[])");
        } finally {
            exec("DROP TABLE IF EXISTS darr_t");
            exec("DROP DOMAIN IF EXISTS posint_arr");
        }
    }

    // ========================================================================
    // ARRAY[ROW(...)::record]
    // ========================================================================

    @Test
    void array_of_record_type() throws SQLException {
        // PG supports ARRAY[ROW(...)::record]; casting to record is a no-op pass-through
        String val = scalar("SELECT ARRAY[ROW(1,'a')::record, ROW(2,'b')::record]");
        assertNotNull(val, "ARRAY of record should succeed");
    }

    // ========================================================================
    // ts_rank precision
    // ========================================================================

    @Test
    void ts_rank_returns_reasonable_value() throws SQLException {
        String val = scalar(
                "SELECT ts_rank(to_tsvector('english', 'The quick brown fox'), to_tsquery('english', 'fox'))");
        assertNotNull(val);
        double rank = Double.parseDouble(val);
        assertTrue(rank > 0 && rank < 1, "ts_rank should be between 0 and 1: " + rank);
        // PG returns approximately 0.0607927 for this specific case
        assertTrue(rank > 0.05 && rank < 0.07,
                "ts_rank for 'fox' in 'The quick brown fox' should be ~0.06, got " + rank);
    }

    // ========================================================================
    // setval should fail on GENERATED ALWAYS AS IDENTITY owned sequence
    // ========================================================================

    @Test
    void setval_on_owned_sequence() throws SQLException {
        exec("CREATE TABLE sv_t(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY)");
        try {
            // Find the sequence name
            String seqName = scalar("""
                SELECT pg_get_serial_sequence('sv_t', 'id')
                """);
            if (seqName != null) {
                // setval should work (it's a low-level operation)
                exec("SELECT setval('" + seqName + "', 1000, true)");
                exec("INSERT INTO sv_t DEFAULT VALUES");
                String id = scalar("SELECT id FROM sv_t");
                assertEquals("1001", id, "After setval(1000), next identity should be 1001");
            }
        } finally {
            exec("DROP TABLE IF EXISTS sv_t CASCADE");
        }
    }
}
