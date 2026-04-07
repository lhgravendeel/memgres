package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 1: SQLSTATE correctness. Validates that error codes match PG18.
 * Tests cover operator errors, function errors, type mismatches, DDL errors,
 * and edge cases where memgres produces the wrong SQLSTATE.
 */
class SqlStateCorrectnessTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ss_t (id INT PRIMARY KEY, a INT, b TEXT)");
            s.execute("INSERT INTO ss_t VALUES (1, 10, 'x'), (2, 20, 'y')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS ss_t CASCADE");
            } catch (SQLException ignored) {}
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    /** Assert that SQL produces an error with the expected SQLSTATE. */
    static void assertSqlState(String sql, String expectedState) {
        try {
            try (Statement s = conn.createStatement()) { s.execute(sql); }
            fail("Expected error with SQLSTATE " + expectedState + " for: " + sql);
        } catch (SQLException e) {
            assertEquals(expectedState, e.getSQLState(),
                    "Wrong SQLSTATE for: " + sql + ", got: " + e.getMessage());
        }
    }

    // ========================================================================
    // Operator type mismatch: correct SQLSTATE codes
    // ========================================================================

    @Test void text_minus_text_gives_42725() {
        // 'a' - 'b'; PG: operator is not unique (42725)
        assertSqlState("SELECT 'a' - 'b'", "42725");
    }

    @Test void array_contains_type_mismatch_gives_42883() {
        // ARRAY[1,2] @> ARRAY['x']; PG: operator does not exist (42883)
        assertSqlState("SELECT ARRAY[1,2] @> ARRAY['x']", "42883");
    }

    @Test void range_division_gives_42883() {
        // int4range / int4range, no such operator
        assertSqlState("SELECT int4range(1,5) / int4range(2,3)", "42883");
    }

    @Test void jsonb_multiply_gives_42883() {
        assertSqlState("SELECT '{\"a\":1}'::jsonb * 2", "42883");
    }

    @Test void geometry_function_text_arg_gives_42725() {
        // center('not_a_circle'); PG: function is not unique (42725) for text arg
        assertSqlState("SELECT center('not_a_circle')", "42725");
    }

    @Test void area_text_arg_gives_42725() {
        assertSqlState("SELECT area('not_a_box')", "42725");
    }

    @Test void box_bad_text_gives_22P02() {
        // box('a', 'b'); PG: invalid input syntax (22P02)
        assertSqlState("SELECT box('a', 'b')", "22P02");
    }

    @Test void polygon_bad_array_gives_42883() {
        assertSqlState("SELECT polygon(ARRAY['x','y'])", "42883");
    }

    // ========================================================================
    // DEFAULT in function arguments: 42601
    // ========================================================================

    @Test void default_as_function_arg_gives_42601() throws SQLException {
        exec("CREATE FUNCTION ss_add(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b $$");
        try {
            assertSqlState("SELECT ss_add(DEFAULT, 5)", "42601");
        } finally {
            exec("DROP FUNCTION IF EXISTS ss_add(int, int)");
        }
    }

    // ========================================================================
    // Array concat with incompatible type: 22P02
    // ========================================================================

    @Test void array_concat_string_gives_22P02() {
        // ARRAY[1,2] || 'x'; PG: invalid input syntax (22P02)
        assertSqlState("SELECT ARRAY[1,2] || 'x'", "22P02");
    }

    // ========================================================================
    // chr(-1): 22023
    // ========================================================================

    @Test void chr_negative_gives_22023() {
        assertSqlState("SELECT chr(-1)", "22023");
    }

    // ========================================================================
    // Bit string different length: 22026
    // ========================================================================

    @Test void bit_different_length_bitwise_gives_22026() {
        // B'1010' & B'11'; PG: cannot AND bit strings of different sizes (22026)
        assertSqlState("SELECT B'1010' & B'11'", "22026");
    }

    // ========================================================================
    // range_merge with wrong type: 42804
    // ========================================================================

    @Test void range_merge_wrong_type_gives_42804() {
        assertSqlState("SELECT range_merge('x', 'y')", "42804");
    }

    // ========================================================================
    // int4range bad bounds: 42601
    // ========================================================================

    @Test void int4range_bad_bounds_gives_42601() {
        assertSqlState("SELECT int4range(1,5,'bad')", "42601");
    }

    // ========================================================================
    // jsonb operators wrong SQLSTATE
    // ========================================================================

    @Test void jsonb_hash_arrow_non_array_gives_22P02() {
        assertSqlState("SELECT '{\"a\":1}'::jsonb #> 'not_an_array'", "22P02");
    }

    @Test void jsonb_exists_any_non_array_gives_22P02() {
        assertSqlState("SELECT '{\"a\":1}'::jsonb ?| 'x'", "22P02");
    }

    @Test void jsonb_insert_bad_index_gives_22P02() {
        assertSqlState("SELECT jsonb_insert('{\"a\":[1,2,3]}'::jsonb, '{a,x}', '1'::jsonb)", "22P02");
    }

    // ========================================================================
    // Transaction-aborted state
    // ========================================================================

    @Test void error_in_failed_transaction_preserves_sqlstate() throws SQLException {
        // After a failed statement in a transaction, subsequent statements should
        // get 25P02 (in_failed_sql_transaction), NOT re-report the original error
        exec("BEGIN");
        try {
            exec("SELECT 1/0");
        } catch (SQLException ignored) {}
        try {
            exec("SELECT 1");
            fail("Should error in failed transaction");
        } catch (SQLException e) {
            assertEquals("25P02", e.getSQLState(), "Should get in_failed_sql_transaction");
        } finally {
            try { exec("ROLLBACK"); } catch (SQLException ignored) {}
        }
    }

    // ========================================================================
    // CREATE FUNCTION with invalid body: 42P13
    // ========================================================================

    @Test void create_function_empty_body_gives_42P13() {
        // $$ SELECT $$ is empty body; PG18 gives 42P13 (invalid_function_definition)
        assertSqlState("CREATE FUNCTION ss_bad(a int) RETURNS int LANGUAGE SQL AS $$ SELECT $$", "42P13");
    }

    // ========================================================================
    // LOCK TABLE invalid mode
    // ========================================================================

    @Test void lock_invalid_mode_gives_42601() throws SQLException {
        assertSqlState("LOCK TABLE ss_t IN no_such_mode MODE", "42601");
    }

    // ========================================================================
    // PREPARE/EXECUTE cascading: deallocated stmt gives 26000
    // ========================================================================

    @Test void execute_deallocated_gives_26000() throws SQLException {
        exec("PREPARE ss_p AS SELECT * FROM ss_t");
        exec("DEALLOCATE ss_p");
        assertSqlState("EXECUTE ss_p", "26000");
    }

    // ========================================================================
    // ON CONFLICT without target: 42601 (PG18 syntax error, no inference spec)
    // ========================================================================

    @Test void on_conflict_no_target_gives_42P10() throws SQLException {
        exec("CREATE TABLE ss_upsert (id INT PRIMARY KEY, val TEXT)");
        try {
            assertSqlState(
                "INSERT INTO ss_upsert VALUES (1, 'x') ON CONFLICT DO UPDATE SET val = 'y'",
                "42601");
        } finally {
            exec("DROP TABLE ss_upsert");
        }
    }

    // ========================================================================
    // Recursive CTE type mismatch
    // ========================================================================

    @Test void recursive_cte_text_int_mismatch_gives_42804_or_22P02() throws SQLException {
        // PG gives 42804 or 22P02 depending on context
        try {
            exec("WITH RECURSIVE bad(n) AS (SELECT 1 UNION ALL SELECT 'x' FROM bad) SELECT * FROM bad");
            fail("Should error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("42804".equals(state) || "22P02".equals(state),
                    "Expected 42804 or 22P02, got " + state);
        }
    }

    // ========================================================================
    // INSERT duplicate on PK: correct SQLSTATE context
    // ========================================================================

    @Test void insert_returning_nonexistent_column_gives_42703() throws SQLException {
        // INSERT ... RETURNING nope; PG gives 42703 (column does not exist)
        assertSqlState("INSERT INTO ss_t(id, a, b) VALUES (99, 1, 'dup') RETURNING nope", "42703");
    }

    // ========================================================================
    // DO block DECLARE without semicolon
    // ========================================================================

    @Test void do_block_declare_no_semicolon_gives_42601() {
        assertSqlState("DO $$ DECLARE x int BEGIN x := 1; END $$", "42601");
    }

    // ========================================================================
    // DISTINCT DISTINCT: syntax error
    // ========================================================================

    @Test void double_distinct_gives_42601() {
        assertSqlState("SELECT DISTINCT DISTINCT a FROM ss_t", "42601");
    }

    @Test void all_then_distinct_gives_42601() {
        assertSqlState("SELECT ALL DISTINCT a FROM ss_t", "42601");
    }

    // ========================================================================
    // version + integer: 22P02
    // ========================================================================

    @Test void version_plus_int_type_mismatch_gives_22P02() {
        // UPDATE ... SET version = version + 1 WHERE version = 'x' → 22P02
        // (trying to compare integer column with text 'x')
        assertSqlState(
            "CREATE TABLE ss_ver (id INT, version INT); " +
            "UPDATE ss_ver SET version = version + 1 WHERE id = 1 AND version = 'x'",
            "22P02");
    }
}
