package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for server-side PREPARE / EXECUTE / DEALLOCATE statements.
 *
 * PG supports named prepared statements at the SQL level with $1, $2 parameters.
 * These are distinct from JDBC PreparedStatement (which uses the extended query protocol).
 * Key behaviors:
 * - PREPARE name AS sql: creates a named prepared statement
 * - EXECUTE name(val1, val2): runs it with parameter values
 * - DEALLOCATE name: drops the prepared statement
 * - Type inference for $1 from context (COALESCE, CASE, operators)
 * - Parameter count validation
 */
class PrepareExecuteTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE prep_t(id int PRIMARY KEY, a int, b text, flag boolean)");
        exec("INSERT INTO prep_t(id, a, b, flag) VALUES (1, 10, 'x', true), (2, NULL, 'y', false), (3, 30, NULL, NULL)");
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

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    static int countRows(String sql) throws SQLException {
        return query(sql).size();
    }

    // ========================================================================
    // Basic PREPARE / EXECUTE / DEALLOCATE
    // ========================================================================

    @Test
    void prepare_and_execute_with_typed_param() throws SQLException {
        exec("PREPARE p_cast AS SELECT $1::int + 1, pg_typeof($1::int)");
        try {
            List<List<String>> rows = query("EXECUTE p_cast('41')");
            assertEquals(1, rows.size());
            assertEquals("42", rows.get(0).get(0));
            assertEquals("integer", rows.get(0).get(1));
        } finally {
            exec("DEALLOCATE p_cast");
        }
    }

    @Test
    void prepare_with_explicit_param_types() throws SQLException {
        exec("PREPARE p_limit(int) AS SELECT id, a FROM prep_t ORDER BY id LIMIT $1");
        try {
            assertEquals(2, countRows("EXECUTE p_limit(2)"));
            assertEquals(1, countRows("EXECUTE p_limit(1)"));
            assertEquals(3, countRows("EXECUTE p_limit(3)"));
        } finally {
            exec("DEALLOCATE p_limit");
        }
    }

    @Test
    void prepare_with_two_params() throws SQLException {
        exec("PREPARE p_offset(int, int) AS SELECT id, a FROM prep_t ORDER BY id LIMIT $1 OFFSET $2");
        try {
            List<List<String>> rows = query("EXECUTE p_offset(2, 1)");
            assertEquals(2, rows.size());
            assertEquals("2", rows.get(0).get(0), "First row after OFFSET 1 should be id=2");
        } finally {
            exec("DEALLOCATE p_offset");
        }
    }

    @Test
    void prepare_with_array_param() throws SQLException {
        exec("PREPARE p_any(int[]) AS SELECT 2 = ANY($1), 4 = ANY($1)");
        try {
            List<List<String>> rows = query("EXECUTE p_any(ARRAY[1,2,3])");
            assertEquals("t", rows.get(0).get(0));
            assertEquals("f", rows.get(0).get(1));
        } finally {
            exec("DEALLOCATE p_any");
        }
    }

    // ========================================================================
    // Type inference for untyped $1
    // ========================================================================

    @Test
    void prepare_null_param_with_is_null() throws SQLException {
        // PG 18+ rejects bare $1 without type context at PREPARE time
        try {
            exec("PREPARE p_null AS SELECT $1 IS NULL, pg_typeof($1)");
            // If it succeeds, clean up and fail
            exec("DEALLOCATE p_null");
            fail("PG 18+ rejects bare $1 without type context at PREPARE time");
        } catch (SQLException e) {
            // Expected: PG fails here with "could not determine data type of parameter $1"
        }
    }

    @Test
    void prepare_inferred_type_from_coalesce() throws SQLException {
        exec("PREPARE p_inf AS SELECT COALESCE($1, 'fallback'), pg_typeof(COALESCE($1, 'fallback'))");
        try {
            List<List<String>> rows = query("EXECUTE p_inf(NULL)");
            assertEquals("fallback", rows.get(0).get(0));

            rows = query("EXECUTE p_inf('abc')");
            assertEquals("abc", rows.get(0).get(0));
        } finally {
            exec("DEALLOCATE p_inf");
        }
    }

    @Test
    void prepare_boolean_param_in_case() throws SQLException {
        exec("PREPARE p_case AS SELECT CASE WHEN $1 THEN 'yes' ELSE 'no' END");
        try {
            assertEquals("yes", scalar("EXECUTE p_case(true)"));
            assertEquals("no", scalar("EXECUTE p_case(false)"));
        } finally {
            exec("DEALLOCATE p_case");
        }
    }

    // ========================================================================
    // PREPARE with DML
    // ========================================================================

    @Test
    void prepare_insert_with_returning() throws SQLException {
        exec("PREPARE p_ins AS INSERT INTO prep_t(id, a, b, flag) VALUES ($1, $2, $3, $4) RETURNING id, a, b, flag");
        try {
            List<List<String>> rows = query("EXECUTE p_ins(10, 100, 'ps', true)");
            assertEquals(1, rows.size());
            assertEquals("10", rows.get(0).get(0));
            assertEquals("100", rows.get(0).get(1));
        } finally {
            exec("DELETE FROM prep_t WHERE id = 10");
            exec("DEALLOCATE p_ins");
        }
    }

    // ========================================================================
    // Re-execute with different values
    // ========================================================================

    @Test
    void reuse_prepared_statement_with_different_values() throws SQLException {
        exec("PREPARE p_reuse(int[]) AS SELECT 2 = ANY($1), 4 = ANY($1)");
        try {
            List<List<String>> r1 = query("EXECUTE p_reuse(ARRAY[1,2,3])");
            assertEquals("t", r1.get(0).get(0));
            assertEquals("f", r1.get(0).get(1));

            List<List<String>> r2 = query("EXECUTE p_reuse(ARRAY[4,5,6])");
            assertEquals("f", r2.get(0).get(0));
            assertEquals("t", r2.get(0).get(1));
        } finally {
            exec("DEALLOCATE p_reuse");
        }
    }

    // ========================================================================
    // DEALLOCATE
    // ========================================================================

    @Test
    void deallocate_removes_prepared_statement() throws SQLException {
        exec("PREPARE p_dealloc AS SELECT 1");
        exec("DEALLOCATE p_dealloc");
        assertThrows(SQLException.class,
                () -> exec("EXECUTE p_dealloc"),
                "EXECUTE after DEALLOCATE should fail");
    }

    @Test
    void deallocate_nonexistent_fails() {
        assertThrows(SQLException.class,
                () -> exec("DEALLOCATE no_such_statement"),
                "DEALLOCATE nonexistent should fail");
    }

    // ========================================================================
    // Error cases
    // ========================================================================

    @Test
    void execute_with_wrong_param_count_fails() throws SQLException {
        exec("PREPARE p_bad(int) AS SELECT * FROM prep_t WHERE id = $1 AND a = $2");
        try {
            // $2 is referenced but only 1 param type declared; PG rejects at PREPARE time
            // or at EXECUTE time with wrong param count
            assertThrows(SQLException.class,
                    () -> exec("EXECUTE p_bad(1)"),
                    "Should fail with wrong parameter count");
        } finally {
            try { exec("DEALLOCATE p_bad"); } catch (SQLException ignored) {}
        }
    }

    @Test
    void prepare_with_like_pattern() throws SQLException {
        exec("PREPARE p_like(text) AS SELECT id, b FROM prep_t WHERE coalesce(b, '') LIKE $1 ORDER BY id");
        try {
            assertEquals(1, countRows("EXECUTE p_like('x%')"));
            assertEquals(3, countRows("EXECUTE p_like('%')"));
        } finally {
            exec("DEALLOCATE p_like");
        }
    }

    @Test
    void prepare_with_timestamp() throws SQLException {
        exec("PREPARE p_ts AS SELECT $1::timestamptz AT TIME ZONE 'UTC'");
        try {
            String val = scalar("EXECUTE p_ts('2024-01-01 12:00:00+00')");
            assertNotNull(val);
            assertTrue(val.contains("2024-01-01"), "Should contain the date");
        } finally {
            exec("DEALLOCATE p_ts");
        }
    }

    @Test
    void prepare_with_row_and_typeof() throws SQLException {
        exec("PREPARE p_row AS SELECT ROW($1::int, $2::text), pg_typeof(ROW($1::int, $2::text))");
        try {
            List<List<String>> rows = query("EXECUTE p_row(5, 'five')");
            assertEquals(1, rows.size());
            assertNotNull(rows.get(0).get(0));
        } finally {
            exec("DEALLOCATE p_row");
        }
    }
}
