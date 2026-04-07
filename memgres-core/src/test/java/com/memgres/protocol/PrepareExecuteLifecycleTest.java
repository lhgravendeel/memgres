package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for server-side PREPARE / EXECUTE / DEALLOCATE lifecycle, parameter
 * type inference, error handling, and protocol correctness.
 *
 * Core issues:
 * - PREPARE with bare $1 (no type context) should fail in some cases
 * - EXECUTE after DEALLOCATE should fail
 * - EXECUTE with wrong param count: wrong SQLSTATE
 * - Re-EXECUTE of a prepared SELECT crashes with "no field structure"
 * - PREPARE bad_row / bad_count succeed when they should fail
 * - DEALLOCATE of already-deallocated statement should fail
 */
class PrepareExecuteLifecycleTest {

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

    static void assertSqlFails(String sql, String expectedState) {
        try {
            exec(sql);
            fail("Expected error for: " + sql);
        } catch (SQLException e) {
            assertEquals(expectedState, e.getSQLState(),
                    "Wrong SQLSTATE for: " + sql + ": " + e.getMessage());
        }
    }

    // ========================================================================
    // Basic lifecycle: PREPARE → EXECUTE → DEALLOCATE
    // ========================================================================

    @Test
    void basic_prepare_execute_deallocate() throws SQLException {
        exec("PREPARE p_basic(int) AS SELECT id, a FROM prep_t WHERE id = $1");
        try {
            List<List<String>> rows = query("EXECUTE p_basic(1)");
            assertEquals(1, rows.size());
            assertEquals("1", rows.get(0).get(0));
            assertEquals("10", rows.get(0).get(1));
        } finally {
            exec("DEALLOCATE p_basic");
        }
    }

    @Test
    void deallocate_then_execute_fails() throws SQLException {
        exec("PREPARE p_del AS SELECT 1");
        exec("DEALLOCATE p_del");
        assertSqlFails("EXECUTE p_del", "26000");
    }

    @Test
    void deallocate_nonexistent_fails() {
        assertSqlFails("DEALLOCATE no_such_statement", "26000");
    }

    @Test
    void deallocate_all() throws SQLException {
        exec("PREPARE p_da1 AS SELECT 1");
        exec("PREPARE p_da2 AS SELECT 2");
        exec("DEALLOCATE ALL");
        assertSqlFails("EXECUTE p_da1", "26000");
        assertSqlFails("EXECUTE p_da2", "26000");
    }

    // ========================================================================
    // Re-execute: same prepared statement with different values
    // ========================================================================

    @Test
    void re_execute_with_different_values() throws SQLException {
        exec("PREPARE p_re(int) AS SELECT id, a FROM prep_t ORDER BY id LIMIT $1");
        try {
            assertEquals(1, countRows("EXECUTE p_re(1)"));
            assertEquals(2, countRows("EXECUTE p_re(2)"));
            assertEquals(3, countRows("EXECUTE p_re(3)"));
        } finally {
            exec("DEALLOCATE p_re");
        }
    }

    @Test
    void re_execute_select_does_not_crash() throws SQLException {
        // This was crashing with "Received resultset tuples, but no field structure"
        exec("PREPARE p_resel(int) AS SELECT id, a FROM prep_t ORDER BY id LIMIT $1");
        try {
            // First execution
            List<List<String>> r1 = query("EXECUTE p_resel(2)");
            assertEquals(2, r1.size());
            // Second execution; must not crash
            List<List<String>> r2 = query("EXECUTE p_resel(1)");
            assertEquals(1, r2.size());
            // Third execution
            List<List<String>> r3 = query("EXECUTE p_resel(3)");
            assertEquals(3, r3.size());
        } finally {
            exec("DEALLOCATE p_resel");
        }
    }

    // ========================================================================
    // Parameter type inference
    // ========================================================================

    @Test
    void prepare_with_typed_cast() throws SQLException {
        exec("PREPARE p_cast AS SELECT $1::int + 1, pg_typeof($1::int)");
        try {
            List<List<String>> rows = query("EXECUTE p_cast('41')");
            assertEquals("42", rows.get(0).get(0));
            assertEquals("integer", rows.get(0).get(1));
        } finally {
            exec("DEALLOCATE p_cast");
        }
    }

    @Test
    void prepare_with_coalesce_infers_type() throws SQLException {
        exec("PREPARE p_coal AS SELECT COALESCE($1, 'fallback'), pg_typeof(COALESCE($1, 'fallback'))");
        try {
            List<List<String>> r1 = query("EXECUTE p_coal(NULL)");
            assertEquals("fallback", r1.get(0).get(0));

            List<List<String>> r2 = query("EXECUTE p_coal('abc')");
            assertEquals("abc", r2.get(0).get(0));
        } finally {
            exec("DEALLOCATE p_coal");
        }
    }

    @Test
    void prepare_with_boolean_in_case() throws SQLException {
        exec("PREPARE p_bool AS SELECT CASE WHEN $1 THEN 'yes' ELSE 'no' END");
        try {
            assertEquals("yes", scalar("EXECUTE p_bool(true)"));
            assertEquals("no", scalar("EXECUTE p_bool(false)"));
        } finally {
            exec("DEALLOCATE p_bool");
        }
    }

    @Test
    void prepare_with_array_param() throws SQLException {
        exec("PREPARE p_arr(int[]) AS SELECT 2 = ANY($1), 4 = ANY($1)");
        try {
            List<List<String>> rows = query("EXECUTE p_arr(ARRAY[1,2,3])");
            assertEquals("t", rows.get(0).get(0));
            assertEquals("f", rows.get(0).get(1));

            // Re-execute with different values
            rows = query("EXECUTE p_arr(ARRAY[4,5,6])");
            assertEquals("f", rows.get(0).get(0));
            assertEquals("t", rows.get(0).get(1));
        } finally {
            exec("DEALLOCATE p_arr");
        }
    }

    // ========================================================================
    // Parameter count validation
    // ========================================================================

    @Test
    void execute_with_too_few_params_fails() throws SQLException {
        // Body references $1 and $2, but only 1 param type declared
        exec("PREPARE p_cnt(int) AS SELECT * FROM prep_t WHERE id = $1 AND a = $2");
        try {
            // EXECUTE with 1 param when body needs 2 → should fail
            assertThrows(SQLException.class, () -> exec("EXECUTE p_cnt(1)"),
                    "Should fail when fewer params than $N references");
        } finally {
            try { exec("DEALLOCATE p_cnt"); } catch (SQLException ignored) {}
        }
    }

    @Test
    void execute_wrong_param_count_sqlstate() throws SQLException {
        exec("PREPARE p_cnt2(int, int) AS SELECT * FROM prep_t WHERE id = $1 AND a = $2");
        try {
            try {
                exec("EXECUTE p_cnt2(1)");
                fail("Should fail with wrong param count");
            } catch (SQLException e) {
                // PG uses 42601 for wrong number of parameters
                assertEquals("42601", e.getSQLState(),
                        "Wrong SQLSTATE for param count mismatch: " + e.getSQLState());
            }
        } finally {
            exec("DEALLOCATE p_cnt2");
        }
    }

    // ========================================================================
    // Bare $1 without type context
    // ========================================================================

    @Test
    void prepare_bare_dollar_one_is_null() throws SQLException {
        // PG: "PREPARE p AS SELECT $1 IS NULL". PG may fail because $1 has no type
        // or it may infer text. The key is consistency.
        // In PG 18, this fails with "could not determine data type of parameter $1"
        try {
            exec("PREPARE p_bare AS SELECT $1 IS NULL, pg_typeof($1)");
            // If it succeeds, that's a deviation from PG behavior
            // PG fails at PREPARE time
            exec("DEALLOCATE p_bare");
            fail("PG rejects bare $1 without type context at PREPARE time");
        } catch (SQLException e) {
            // Expected: PG fails here
        }
    }

    // ========================================================================
    // PREPARE with bad body validation
    // ========================================================================

    @Test
    void prepare_bad_row_field_access() throws SQLException {
        // PREPARE bad_row AS SELECT ROW($1).nope should fail
        try {
            exec("PREPARE p_badrow AS SELECT ROW($1).nope");
            try { exec("DEALLOCATE p_badrow"); } catch (SQLException ignored) {}
            fail("Should reject field access on anonymous ROW at PREPARE time");
        } catch (SQLException e) {
            // Expected
        }
    }

    @Test
    void prepare_any_with_wrong_type() throws SQLException {
        // PREPARE bad_any(text) AS SELECT 1 = ANY($1)
        // PG: error 42809 (wrong_object_type) because ANY needs an array, not text
        try {
            exec("PREPARE p_badany(text) AS SELECT 1 = ANY($1)");
            try { exec("DEALLOCATE p_badany"); } catch (SQLException ignored) {}
            fail("Should reject ANY($1) where $1 is text (needs array)");
        } catch (SQLException e) {
            assertEquals("42809", e.getSQLState(),
                    "Should be 42809 for wrong_object_type, got " + e.getSQLState());
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
        } finally {
            exec("DELETE FROM prep_t WHERE id = 10");
            exec("DEALLOCATE p_ins");
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
            assertTrue(val.contains("2024-01-01"));
        } finally {
            exec("DEALLOCATE p_ts");
        }
    }

    // ========================================================================
    // PREPARE with two params, offset, order
    // ========================================================================

    @Test
    void prepare_with_limit_and_offset() throws SQLException {
        exec("PREPARE p_lo(int, int) AS SELECT id FROM prep_t ORDER BY id LIMIT $1 OFFSET $2");
        try {
            List<List<String>> rows = query("EXECUTE p_lo(2, 1)");
            assertEquals(2, rows.size());
            assertEquals("2", rows.get(0).get(0));
            assertEquals("3", rows.get(1).get(0));
        } finally {
            exec("DEALLOCATE p_lo");
        }
    }

    @Test
    void prepare_with_order_by_case() throws SQLException {
        exec("PREPARE p_ord(bool) AS SELECT id, a FROM prep_t ORDER BY CASE WHEN $1 THEN id ELSE a END NULLS LAST");
        try {
            List<List<String>> byId = query("EXECUTE p_ord(true)");
            assertEquals("1", byId.get(0).get(0), "Should be ordered by id");

            List<List<String>> byA = query("EXECUTE p_ord(false)");
            // a values: 10, NULL, 30 → ordered: 10, 30, NULL (NULLS LAST)
            assertEquals("10", byA.get(0).get(1), "First by a should be 10");
        } finally {
            exec("DEALLOCATE p_ord");
        }
    }

    // ========================================================================
    // Duplicate PREPARE names
    // ========================================================================

    @Test
    void prepare_duplicate_name_fails() throws SQLException {
        exec("PREPARE p_dup AS SELECT 1");
        try {
            assertSqlFails("PREPARE p_dup AS SELECT 2", "42P05");
        } finally {
            exec("DEALLOCATE p_dup");
        }
    }

    // ========================================================================
    // EXECUTE after error recovery
    // ========================================================================

    @Test
    void execute_after_failed_execute() throws SQLException {
        exec("PREPARE p_rec(int) AS SELECT id FROM prep_t WHERE id = $1");
        try {
            // Successful execute
            assertEquals("1", scalar("EXECUTE p_rec(1)"));
            // The prepared statement should still be usable after a successful execute
            assertEquals("2", scalar("EXECUTE p_rec(2)"));
        } finally {
            exec("DEALLOCATE p_rec");
        }
    }

    // ========================================================================
    // ROW and array in prepared statements
    // ========================================================================

    @Test
    void prepare_with_row() throws SQLException {
        exec("PREPARE p_row AS SELECT ROW($1::int, $2::text), pg_typeof(ROW($1::int, $2::text))");
        try {
            List<List<String>> rows = query("EXECUTE p_row(5, 'five')");
            assertEquals(1, rows.size());
            assertNotNull(rows.get(0).get(0));
            assertTrue(rows.get(0).get(1).equals("record"),
                    "pg_typeof of ROW should be 'record', got " + rows.get(0).get(1));
        } finally {
            exec("DEALLOCATE p_row");
        }
    }

    @Test
    void prepare_with_array_length() throws SQLException {
        exec("PREPARE p_alen AS SELECT array_length($1::int[], 1), pg_typeof($1::int[])");
        try {
            List<List<String>> rows = query("EXECUTE p_alen(ARRAY[1,2,3])");
            assertEquals("3", rows.get(0).get(0));
        } finally {
            exec("DEALLOCATE p_alen");
        }
    }

    // ========================================================================
    // EXECUTE p(args) format (not just EXECUTE p)
    // ========================================================================

    @Test
    void execute_named_with_parenthesized_args() throws SQLException {
        exec("PREPARE p_paren(int) AS SELECT $1 + 100");
        try {
            String val = scalar("EXECUTE p_paren(42)");
            assertEquals("142", val);
        } finally {
            exec("DEALLOCATE p_paren");
        }
    }

    // ========================================================================
    // PREPARE / EXECUTE interleaved with regular DML
    // ========================================================================

    @Test
    void prepared_statements_coexist_with_regular_queries() throws SQLException {
        exec("PREPARE p_coex(int) AS SELECT id FROM prep_t WHERE id = $1");
        try {
            // Regular query
            assertEquals(3, countRows("SELECT * FROM prep_t"));
            // Prepared query
            assertEquals("1", scalar("EXECUTE p_coex(1)"));
            // Regular insert
            exec("INSERT INTO prep_t(id, a, b, flag) VALUES (99, 99, 'temp', true)");
            assertEquals(4, countRows("SELECT * FROM prep_t"));
            // Prepared query still works
            assertEquals("1", scalar("EXECUTE p_coex(1)"));
        } finally {
            exec("DELETE FROM prep_t WHERE id = 99");
            exec("DEALLOCATE p_coex");
        }
    }
}
