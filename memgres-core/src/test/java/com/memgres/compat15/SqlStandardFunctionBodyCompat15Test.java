package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 26 failures from sql-standard-function-body.sql where Memgres diverges from PG 18.
 *
 * SQL standard function bodies use RETURN expr or BEGIN ATOMIC ... END syntax (PG 14+).
 * PG 18 rejects BEGIN ATOMIC blocks that are missing the closing END (syntax error),
 * while Memgres accepts them. Some tests verify that Memgres incorrectly succeeds
 * where PG would error, and others verify wrong results or missing functionality.
 */
class SqlStandardFunctionBodyCompat15Test {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS ssf_test CASCADE");
            s.execute("CREATE SCHEMA ssf_test");
            s.execute("SET search_path = ssf_test, public");

            // Base data table
            s.execute("CREATE TABLE ssf_data (id integer PRIMARY KEY, val integer, label text)");
            s.execute("INSERT INTO ssf_data VALUES (1, 10, 'alpha'), (2, 20, 'beta'), (3, 30, 'gamma')");

            // Functions used by multiple tests (RETURN expr form -- these work on both PG and Memgres)
            s.execute("CREATE FUNCTION ssf_add(a integer, b integer) RETURNS integer "
                    + "LANGUAGE sql RETURN a + b");
            s.execute("CREATE FUNCTION ssf_double(x integer) RETURNS integer "
                    + "LANGUAGE sql RETURN x * 2");

            // Table for section 20 (INSERT in BEGIN ATOMIC)
            s.execute("CREATE TABLE ssf_log (id serial PRIMARY KEY, msg text)");

            // Table for section 21 (UPDATE in BEGIN ATOMIC)
            s.execute("CREATE TABLE ssf_counter (id integer PRIMARY KEY, val integer)");
            s.execute("INSERT INTO ssf_counter VALUES (1, 0)");

            // Table for section 23 (RETURNS TABLE with BEGIN ATOMIC)
            s.execute("CREATE TABLE ssf_people (id integer, name text, age integer)");
            s.execute("INSERT INTO ssf_people VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Carol', 35)");

            // Table for section 27 (multi-statement BEGIN ATOMIC)
            s.execute("CREATE TABLE ssf_side (id serial, val text)");

            // Table for section 31 (DELETE ... RETURNING in BEGIN ATOMIC)
            s.execute("CREATE TABLE ssf_del_test (id integer PRIMARY KEY, val text)");
            s.execute("INSERT INTO ssf_del_test VALUES (1, 'remove-me'), (2, 'keep')");

            // Table for section 33 (dependency tracking)
            s.execute("CREATE TABLE ssf_dep_table (id integer PRIMARY KEY, val text)");

            // Section 30: RETURN with window function inside subquery
            // This function should be creatable and callable; Memgres fails to create it
            s.execute("CREATE FUNCTION ssf_first_rank() RETURNS integer "
                    + "LANGUAGE sql "
                    + "RETURN (SELECT val FROM ("
                    + "  SELECT val, ROW_NUMBER() OVER (ORDER BY val DESC) AS rn"
                    + "  FROM (VALUES (10),(20),(30)) t(val)"
                    + ") sub WHERE rn = 1)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS ssf_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    // ==================================================================================
    // Stmt 10: CREATE FUNCTION with BEGIN ATOMIC missing END should be a syntax error
    // PG: ERROR [42601] syntax error at end of input
    // Memgres: OK 0 rows affected (incorrectly succeeds)
    // ==================================================================================

    @Test
    void stmt10_beginAtomicAddMissingEndShouldError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE FUNCTION ssf_atomic_add(a integer, b integer) RETURNS integer "
                        + "LANGUAGE sql BEGIN ATOMIC SELECT a + b");
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("syntax error"),
                "Expected syntax error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 12: ssf_atomic_add should not exist (since CREATE in stmt 10 should have failed)
    // PG: ERROR [42883] function ssf_atomic_add(integer, integer) does not exist
    // Memgres: OK (ssf_atomic_add) [12]
    // ==================================================================================

    @Test
    void stmt12_atomicAddShouldNotExist() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT ssf_atomic_add(5, 7)")) {
                rs.next();
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("function ssf_atomic_add"),
                "Expected 'function ssf_atomic_add' in error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 13: CREATE FUNCTION with BEGIN ATOMIC multi-statement missing END
    // PG: ERROR [42601] syntax error at end of input
    // Memgres: OK 0 rows affected
    // ==================================================================================

    @Test
    void stmt13_beginAtomicMultiMissingEndShouldError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE FUNCTION ssf_atomic_multi(x integer) RETURNS integer "
                        + "LANGUAGE sql BEGIN ATOMIC SELECT x + 1");
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("syntax error"),
                "Expected syntax error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 16: ssf_atomic_multi should not exist
    // PG: ERROR [42883] function ssf_atomic_multi(integer) does not exist
    // Memgres: OK (ssf_atomic_multi) [6]
    // ==================================================================================

    @Test
    void stmt16_atomicMultiShouldNotExist() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT ssf_atomic_multi(5)")) {
                rs.next();
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("function ssf_atomic_multi"),
                "Expected 'function ssf_atomic_multi' in error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 54: CREATE FUNCTION with INSERT in BEGIN ATOMIC missing END
    // PG: ERROR [42601] syntax error at end of input
    // Memgres: OK 0 rows affected
    // ==================================================================================

    @Test
    void stmt54_beginAtomicInsertMissingEndShouldError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE FUNCTION ssf_insert_log(m text) RETURNS integer "
                        + "LANGUAGE sql BEGIN ATOMIC INSERT INTO ssf_log (msg) VALUES (m) RETURNING id");
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("syntax error"),
                "Expected syntax error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 56: ssf_insert_log should not exist
    // PG: ERROR [42883] function ssf_insert_log(unknown) does not exist
    // Memgres: OK (ssf_insert_log) [1]
    // ==================================================================================

    @Test
    void stmt56_insertLogShouldNotExist() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT ssf_insert_log('test entry')")) {
                rs.next();
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("function ssf_insert_log"),
                "Expected 'function ssf_insert_log' in error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 57: ssf_log should have no rows (since function creation failed, no insert happened)
    // PG: 0 rows
    // Memgres: 1 row [test entry]
    // ==================================================================================

    @Test
    void stmt57_logTableShouldBeEmpty() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT msg FROM ssf_log WHERE id = 1")) {
            assertFalse(rs.next(), "Expected no rows in ssf_log (function should not have been created)");
        }
    }

    // ==================================================================================
    // Stmt 61: CREATE FUNCTION with UPDATE in BEGIN ATOMIC missing END
    // PG: ERROR [42601] syntax error at end of input
    // Memgres: OK 0 rows affected
    // ==================================================================================

    @Test
    void stmt61_beginAtomicUpdateMissingEndShouldError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE FUNCTION ssf_increment(target_id integer) RETURNS integer "
                        + "LANGUAGE sql BEGIN ATOMIC UPDATE ssf_counter SET val = val + 1 "
                        + "WHERE id = target_id RETURNING val");
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("syntax error"),
                "Expected syntax error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 63: ssf_increment should not exist (first call)
    // PG: ERROR [42883] function ssf_increment(integer) does not exist
    // Memgres: OK (ssf_increment) [1]
    // ==================================================================================

    @Test
    void stmt63_incrementShouldNotExistFirstCall() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT ssf_increment(1)")) {
                rs.next();
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("function ssf_increment"),
                "Expected 'function ssf_increment' in error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 64: ssf_increment should not exist (second call)
    // PG: ERROR [42883] function ssf_increment(integer) does not exist
    // Memgres: OK (ssf_increment) [2]
    // ==================================================================================

    @Test
    void stmt64_incrementShouldNotExistSecondCall() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT ssf_increment(1)")) {
                rs.next();
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("function ssf_increment"),
                "Expected 'function ssf_increment' in error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 66: CREATE FUNCTION RETURNS SETOF with BEGIN ATOMIC missing END
    // PG: ERROR [42601] syntax error at end of input
    // Memgres: OK 0 rows affected
    // ==================================================================================

    @Test
    void stmt66_beginAtomicSetofMissingEndShouldError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE FUNCTION ssf_series(n integer) RETURNS SETOF integer "
                        + "LANGUAGE sql BEGIN ATOMIC SELECT generate_series(1, n)");
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("syntax error"),
                "Expected syntax error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 68: ssf_series should not exist
    // PG: ERROR [42883] function ssf_series(integer) does not exist
    // Memgres: OK (ssf_series) [1] ; [2] ; [3]
    // ==================================================================================

    @Test
    void stmt68_seriesShouldNotExist() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT * FROM ssf_series(3)")) {
                rs.next();
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("function ssf_series"),
                "Expected 'function ssf_series' in error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 71: CREATE FUNCTION RETURNS TABLE with BEGIN ATOMIC missing END
    // PG: ERROR [42601] syntax error at end of input
    // Memgres: OK 0 rows affected
    // ==================================================================================

    @Test
    void stmt71_beginAtomicReturnsTableMissingEndShouldError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE FUNCTION ssf_adults() RETURNS TABLE(name text, age integer) "
                        + "LANGUAGE sql BEGIN ATOMIC SELECT name, age FROM ssf_people "
                        + "WHERE age >= 30 ORDER BY name");
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("syntax error"),
                "Expected syntax error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 73: ssf_adults should not exist
    // PG: ERROR [42883] function ssf_adults() does not exist
    // Memgres: OK (ssf_adults) 0 rows
    // ==================================================================================

    @Test
    void stmt73_adultsShouldNotExist() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT * FROM ssf_adults()")) {
                rs.next();
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("function ssf_adults"),
                "Expected 'function ssf_adults' in error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 80: CREATE FUNCTION with INOUT params in BEGIN ATOMIC missing END
    // PG: ERROR [42601] syntax error at end of input
    // Memgres: OK 0 rows affected
    // ==================================================================================

    @Test
    void stmt80_beginAtomicSwapMissingEndShouldError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE FUNCTION ssf_swap(INOUT a integer, INOUT b integer) "
                        + "LANGUAGE sql BEGIN ATOMIC SELECT b, a");
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("syntax error"),
                "Expected syntax error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 82: ssf_swap should not exist; PG gives 42883, Memgres gives XX000 (wrong error)
    // PG: ERROR [42883] function ssf_swap(integer, integer) does not exist
    // Memgres: ERROR [XX000] Internal error: Index 1 out of bounds for length 1
    // ==================================================================================

    @Test
    void stmt82_swapShouldNotExistWithCorrectError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT * FROM ssf_swap(10, 20)")) {
                rs.next();
            }
        });
        assertEquals("42883", ex.getSQLState(),
                "Expected SQL state 42883 (function does not exist), got: " + ex.getSQLState());
        assertTrue(ex.getMessage().toLowerCase().contains("function ssf_swap"),
                "Expected 'function ssf_swap' in error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 84: CREATE FUNCTION multi-stmt BEGIN ATOMIC missing END
    // PG: ERROR [42601] syntax error at end of input
    // Memgres: OK 0 rows affected
    // ==================================================================================

    @Test
    void stmt84_beginAtomicMultiStmtMissingEndShouldError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE FUNCTION ssf_multi_stmt(x text) RETURNS text "
                        + "LANGUAGE sql BEGIN ATOMIC INSERT INTO ssf_side (val) VALUES (x)");
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("syntax error"),
                "Expected syntax error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 87: ssf_multi_stmt should not exist
    // PG: ERROR [42883] function ssf_multi_stmt(unknown) does not exist
    // Memgres: OK (ssf_multi_stmt) [NULL]
    // ==================================================================================

    @Test
    void stmt87_multiStmtShouldNotExist() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT ssf_multi_stmt('hello')")) {
                rs.next();
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("function ssf_multi_stmt"),
                "Expected 'function ssf_multi_stmt' in error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 88: ssf_side should have no rows with val='hello' (function creation should have failed)
    // PG: 0 rows
    // Memgres: 1 row [hello]
    // ==================================================================================

    @Test
    void stmt88_sideTableShouldNotContainHello() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM ssf_side WHERE val = 'hello'")) {
            assertFalse(rs.next(),
                    "Expected no rows in ssf_side (function ssf_multi_stmt should not have been created)");
        }
    }

    // ==================================================================================
    // Stmt 90: RETURN expr with LANGUAGE plpgsql should be an error
    // PG: ERROR [42P13] inline SQL function body only valid for language SQL
    // Memgres: OK 0 rows affected (incorrectly succeeds)
    // ==================================================================================

    @Test
    void stmt90_returnExprWithPlpgsqlShouldError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE FUNCTION ssf_mix_fail(x integer) RETURNS integer "
                        + "LANGUAGE plpgsql RETURN x + 1");
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("error"),
                "Expected an error about inline SQL function body with plpgsql, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 96: ssf_first_rank() should return 30 (window function in subquery)
    // PG: OK (result) [30]
    // Memgres: ERROR [42883] function ssf_first_rank() does not exist
    // ==================================================================================

    @Test
    void stmt96_firstRankShouldReturn30() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ssf_first_rank() AS result")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals(30, rs.getInt("result"),
                    "ssf_first_rank() should return 30 (highest value via window function)");
            assertFalse(rs.next(), "Expected exactly one row");
        }
    }

    // ==================================================================================
    // Stmt 99: CREATE FUNCTION with DELETE in BEGIN ATOMIC missing END
    // PG: ERROR [42601] syntax error at end of input
    // Memgres: OK 0 rows affected
    // ==================================================================================

    @Test
    void stmt99_beginAtomicDeleteMissingEndShouldError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE FUNCTION ssf_delete_and_return(target_id integer) RETURNS text "
                        + "LANGUAGE sql BEGIN ATOMIC DELETE FROM ssf_del_test "
                        + "WHERE id = target_id RETURNING val");
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("syntax error"),
                "Expected syntax error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 101: ssf_delete_and_return should not exist
    // PG: ERROR [42883] function ssf_delete_and_return(integer) does not exist
    // Memgres: OK (ssf_delete_and_return) [remove-me]
    // ==================================================================================

    @Test
    void stmt101_deleteAndReturnShouldNotExist() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT ssf_delete_and_return(1)")) {
                rs.next();
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("function ssf_delete_and_return"),
                "Expected 'function ssf_delete_and_return' in error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 102: ssf_del_test should still have 2 rows (no deletion since function should not exist)
    // PG: (cnt) [2]
    // Memgres: (cnt) [1]
    // ==================================================================================

    @Test
    void stmt102_delTestShouldHaveTwoRows() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::integer AS cnt FROM ssf_del_test")) {
            assertTrue(rs.next(), "Expected one result row");
            assertEquals(2, rs.getInt("cnt"),
                    "ssf_del_test should have 2 rows (delete function should not have been created)");
        }
    }

    // ==================================================================================
    // Stmt 110: CREATE FUNCTION SETOF with STABLE and BEGIN ATOMIC missing END
    // PG: ERROR [42601] syntax error at end of input
    // Memgres: OK 0 rows affected
    // ==================================================================================

    @Test
    void stmt110_beginAtomicStableSetofMissingEndShouldError() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute("CREATE FUNCTION ssf_dep_func() RETURNS SETOF text "
                        + "LANGUAGE sql STABLE BEGIN ATOMIC SELECT val FROM ssf_dep_table");
            }
        });
        assertTrue(ex.getMessage().toLowerCase().contains("syntax error"),
                "Expected syntax error, got: " + ex.getMessage());
    }

    // ==================================================================================
    // Stmt 113: After DROP TABLE ssf_dep_table CASCADE, ssf_dep_func should be gone
    // PG: (cnt) [0]
    // Memgres: (cnt) [1] -- function survives because Memgres created it (despite bad syntax)
    // Note: We run DROP TABLE CASCADE first, then check pg_proc
    // ==================================================================================

    @Test
    void stmt113_depFuncShouldNotExistAfterCascadeDrop() throws Exception {
        try (Statement s = conn.createStatement()) {
            // Drop table with CASCADE (should drop dependent function too)
            s.execute("DROP TABLE IF EXISTS ssf_dep_table CASCADE");

            try (ResultSet rs = s.executeQuery(
                    "SELECT count(*)::integer AS cnt FROM pg_proc WHERE proname = 'ssf_dep_func'")) {
                assertTrue(rs.next(), "Expected one result row");
                assertEquals(0, rs.getInt("cnt"),
                        "ssf_dep_func should not exist after CASCADE drop of ssf_dep_table");
            }
        }
    }
}
