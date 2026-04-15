package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class PlpgsqlCaseCompatTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP SCHEMA IF EXISTS plcase_test CASCADE");
            stmt.execute("CREATE SCHEMA plcase_test");
            stmt.execute("SET search_path = plcase_test, public");
        }

        // Each CREATE FUNCTION is wrapped in try-catch because Memgres does not
        // support CASE statements in PL/pgSQL and will reject the function body.

        tryExec("CREATE FUNCTION plcase_simple(val integer) RETURNS text "
                + "LANGUAGE plpgsql AS $$ BEGIN "
                + "CASE val "
                + "WHEN 1 THEN RETURN 'one'; "
                + "WHEN 2 THEN RETURN 'two'; "
                + "WHEN 3 THEN RETURN 'three'; "
                + "ELSE RETURN 'other'; "
                + "END CASE; END; $$");

        tryExec("CREATE FUNCTION plcase_no_else(val integer) RETURNS text "
                + "LANGUAGE plpgsql AS $$ BEGIN "
                + "CASE val "
                + "WHEN 1 THEN RETURN 'one'; "
                + "WHEN 2 THEN RETURN 'two'; "
                + "END CASE; "
                + "RETURN 'unreachable'; END; $$");

        tryExec("CREATE FUNCTION plcase_searched(val integer) RETURNS text "
                + "LANGUAGE plpgsql AS $$ BEGIN "
                + "CASE "
                + "WHEN val < 0 THEN RETURN 'negative'; "
                + "WHEN val = 0 THEN RETURN 'zero'; "
                + "WHEN val BETWEEN 1 AND 10 THEN RETURN 'small'; "
                + "WHEN val BETWEEN 11 AND 100 THEN RETURN 'medium'; "
                + "ELSE RETURN 'large'; "
                + "END CASE; END; $$");

        tryExec("CREATE FUNCTION plcase_multi_when(val integer) RETURNS text "
                + "LANGUAGE plpgsql AS $$ BEGIN "
                + "CASE val "
                + "WHEN 1, 2, 3 THEN RETURN 'low'; "
                + "WHEN 4, 5, 6 THEN RETURN 'mid'; "
                + "WHEN 7, 8, 9 THEN RETURN 'high'; "
                + "ELSE RETURN 'out of range'; "
                + "END CASE; END; $$");

        tryExec("CREATE FUNCTION plcase_assign(val integer) RETURNS text "
                + "LANGUAGE plpgsql AS $$ DECLARE result text; BEGIN "
                + "CASE val "
                + "WHEN 1 THEN result := 'first'; "
                + "WHEN 2 THEN result := 'second'; "
                + "ELSE result := 'unknown'; "
                + "END CASE; "
                + "RETURN result; END; $$");

        tryExec("CREATE FUNCTION plcase_multi_stmt(val integer) RETURNS text "
                + "LANGUAGE plpgsql AS $$ DECLARE label text; doubled integer; BEGIN "
                + "CASE "
                + "WHEN val > 0 THEN label := 'positive'; doubled := val * 2; "
                + "WHEN val = 0 THEN label := 'zero'; doubled := 0; "
                + "ELSE label := 'negative'; doubled := val * -2; "
                + "END CASE; "
                + "RETURN label || ':' || doubled::text; END; $$");

        tryExec("CREATE FUNCTION plcase_text(status text) RETURNS text "
                + "LANGUAGE plpgsql AS $$ BEGIN "
                + "CASE status "
                + "WHEN 'active' THEN RETURN 'User is active'; "
                + "WHEN 'inactive' THEN RETURN 'User is inactive'; "
                + "WHEN 'banned' THEN RETURN 'User is banned'; "
                + "ELSE RETURN 'Unknown status: ' || COALESCE(status, 'NULL'); "
                + "END CASE; END; $$");

        tryExec("CREATE FUNCTION plcase_nested(category text, val integer) RETURNS text "
                + "LANGUAGE plpgsql AS $$ BEGIN "
                + "CASE category "
                + "WHEN 'number' THEN "
                + "  CASE "
                + "  WHEN val > 0 THEN RETURN 'positive number'; "
                + "  WHEN val = 0 THEN RETURN 'zero'; "
                + "  ELSE RETURN 'negative number'; "
                + "  END CASE; "
                + "WHEN 'size' THEN "
                + "  CASE "
                + "  WHEN val < 10 THEN RETURN 'small'; "
                + "  WHEN val < 100 THEN RETURN 'medium'; "
                + "  ELSE RETURN 'large'; "
                + "  END CASE; "
                + "ELSE RETURN 'unknown category'; "
                + "END CASE; END; $$");

        tryExec("CREATE FUNCTION plcase_in_loop() RETURNS text "
                + "LANGUAGE plpgsql AS $$ DECLARE i integer; result text := ''; BEGIN "
                + "FOR i IN 1..5 LOOP "
                + "CASE i "
                + "WHEN 1 THEN result := result || 'one '; "
                + "WHEN 2 THEN result := result || 'two '; "
                + "WHEN 3 THEN result := result || 'three '; "
                + "ELSE result := result || i::text || ' '; "
                + "END CASE; "
                + "END LOOP; "
                + "RETURN trim(result); END; $$");

        tryExec("CREATE FUNCTION plcase_null_operand() RETURNS text "
                + "LANGUAGE plpgsql AS $$ DECLARE val integer := NULL; BEGIN "
                + "CASE val "
                + "WHEN 1 THEN RETURN 'one'; "
                + "WHEN NULL THEN RETURN 'null-when'; "
                + "ELSE RETURN 'fell through to else'; "
                + "END CASE; END; $$");

        tryExec("CREATE FUNCTION plcase_is_null(val integer) RETURNS text "
                + "LANGUAGE plpgsql AS $$ BEGIN "
                + "CASE "
                + "WHEN val IS NULL THEN RETURN 'null value'; "
                + "WHEN val > 0 THEN RETURN 'positive'; "
                + "ELSE RETURN 'non-positive'; "
                + "END CASE; END; $$");

        tryExec("CREATE FUNCTION plcase_raise(val integer) RETURNS text "
                + "LANGUAGE plpgsql AS $$ BEGIN "
                + "CASE "
                + "WHEN val < 0 THEN RAISE EXCEPTION 'negative value not allowed: %', val; "
                + "WHEN val = 0 THEN RAISE NOTICE 'zero value received'; RETURN 'zero'; "
                + "ELSE RETURN 'ok: ' || val::text; "
                + "END CASE; END; $$");

        tryExec("CREATE TABLE plcase_log (id serial, msg text)");

        tryExec("CREATE FUNCTION plcase_dml(action text) RETURNS text "
                + "LANGUAGE plpgsql AS $$ BEGIN "
                + "CASE action "
                + "WHEN 'insert' THEN "
                + "  INSERT INTO plcase_log (msg) VALUES ('inserted'); RETURN 'inserted'; "
                + "WHEN 'delete' THEN "
                + "  DELETE FROM plcase_log; RETURN 'deleted'; "
                + "ELSE RETURN 'noop'; "
                + "END CASE; END; $$");

        tryExec("CREATE FUNCTION plcase_bool(flag boolean) RETURNS text "
                + "LANGUAGE plpgsql AS $$ BEGIN "
                + "CASE flag "
                + "WHEN true THEN RETURN 'yes'; "
                + "WHEN false THEN RETURN 'no'; "
                + "ELSE RETURN 'null'; "
                + "END CASE; END; $$");

        tryExec("CREATE FUNCTION plcase_var_operand(input integer) RETURNS text "
                + "LANGUAGE plpgsql AS $$ DECLARE category integer; BEGIN "
                + "category := input / 10; "
                + "CASE category "
                + "WHEN 0 THEN RETURN 'single digit'; "
                + "WHEN 1 THEN RETURN 'teens'; "
                + "WHEN 2 THEN RETURN 'twenties'; "
                + "ELSE RETURN 'thirty+'; "
                + "END CASE; END; $$");

        tryExec("CREATE FUNCTION plcase_both(val integer) RETURNS text "
                + "LANGUAGE plpgsql AS $$ DECLARE prefix text; suffix text; BEGIN "
                + "CASE "
                + "WHEN val > 0 THEN prefix := 'pos'; "
                + "ELSE prefix := 'non-pos'; "
                + "END CASE; "
                + "SELECT CASE WHEN val % 2 = 0 THEN 'even' ELSE 'odd' END INTO suffix; "
                + "RETURN prefix || '-' || suffix; END; $$");
    }

    private static void tryExec(String sql) {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            // Expected: Memgres does not support CASE in PL/pgSQL
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ========================================================================
    // 1. Simple CASE with WHEN/ELSE
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #1: simple CASE returns matching branch text")
    void testSimpleCase() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT plcase_simple(1) AS r1, plcase_simple(2) AS r2, "
                             + "plcase_simple(3) AS r3, plcase_simple(99) AS r4")) {
            assertTrue(rs.next());
            assertEquals("one", rs.getString("r1"));
            assertEquals("two", rs.getString("r2"));
            assertEquals("three", rs.getString("r3"));
            assertEquals("other", rs.getString("r4"));
        }
    }

    // ========================================================================
    // 2. CASE without ELSE — success path
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #2a: CASE without ELSE returns value when matched")
    void testNoElseMatched() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT plcase_no_else(1) AS result")) {
            assertTrue(rs.next());
            assertEquals("one", rs.getString("result"));
        }
    }

    // ========================================================================
    // 2b. CASE without ELSE — error path (SQLSTATE 20000)
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #2b: CASE without ELSE raises case_not_found (20000)")
    void testNoElseNotMatched() throws Exception {
        try {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT plcase_no_else(99)");
            }
            fail("Expected case_not_found error");
        } catch (SQLException e) {
            assertEquals("20000", e.getSQLState());
        }
    }

    // ========================================================================
    // 3. Searched CASE (boolean expressions)
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #3: searched CASE evaluates boolean expressions")
    void testSearchedCase() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT plcase_searched(-5) AS r1, plcase_searched(0) AS r2, "
                             + "plcase_searched(7) AS r3, plcase_searched(50) AS r4, "
                             + "plcase_searched(500) AS r5")) {
            assertTrue(rs.next());
            assertEquals("negative", rs.getString("r1"));
            assertEquals("zero", rs.getString("r2"));
            assertEquals("small", rs.getString("r3"));
            assertEquals("medium", rs.getString("r4"));
            assertEquals("large", rs.getString("r5"));
        }
    }

    // ========================================================================
    // 4. CASE with multiple values in WHEN
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #4: CASE with multiple values per WHEN branch")
    void testMultiWhen() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT plcase_multi_when(2) AS r1, plcase_multi_when(5) AS r2, "
                             + "plcase_multi_when(8) AS r3, plcase_multi_when(0) AS r4")) {
            assertTrue(rs.next());
            assertEquals("low", rs.getString("r1"));
            assertEquals("mid", rs.getString("r2"));
            assertEquals("high", rs.getString("r3"));
            assertEquals("out of range", rs.getString("r4"));
        }
    }

    // ========================================================================
    // 5. CASE that assigns to variable
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #5: CASE assigns result to variable")
    void testCaseAssign() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT plcase_assign(1) AS r1, plcase_assign(2) AS r2, "
                             + "plcase_assign(3) AS r3")) {
            assertTrue(rs.next());
            assertEquals("first", rs.getString("r1"));
            assertEquals("second", rs.getString("r2"));
            assertEquals("unknown", rs.getString("r3"));
        }
    }

    // ========================================================================
    // 6. CASE with multiple statements per branch
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #6: CASE with multiple statements per branch")
    void testMultiStmt() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT plcase_multi_stmt(5) AS r1, plcase_multi_stmt(0) AS r2, "
                             + "plcase_multi_stmt(-3) AS r3")) {
            assertTrue(rs.next());
            assertEquals("positive:10", rs.getString("r1"));
            assertEquals("zero:0", rs.getString("r2"));
            assertEquals("negative:6", rs.getString("r3"));
        }
    }

    // ========================================================================
    // 7. CASE on text operand
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #7: CASE on text operand")
    void testTextCase() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT plcase_text('active') AS r1, plcase_text('inactive') AS r2, "
                             + "plcase_text('banned') AS r3, plcase_text('pending') AS r4")) {
            assertTrue(rs.next());
            assertEquals("User is active", rs.getString("r1"));
            assertEquals("User is inactive", rs.getString("r2"));
            assertEquals("User is banned", rs.getString("r3"));
            assertEquals("Unknown status: pending", rs.getString("r4"));
        }
    }

    // ========================================================================
    // 8. Nested CASE statements
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #8: nested CASE statements")
    void testNestedCase() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT plcase_nested('number', 5) AS r1, plcase_nested('number', 0) AS r2, "
                             + "plcase_nested('size', 50) AS r3, plcase_nested('other', 0) AS r4")) {
            assertTrue(rs.next());
            assertEquals("positive number", rs.getString("r1"));
            assertEquals("zero", rs.getString("r2"));
            assertEquals("medium", rs.getString("r3"));
            assertEquals("unknown category", rs.getString("r4"));
        }
    }

    // ========================================================================
    // 9. CASE inside a loop
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #9: CASE inside FOR loop")
    void testCaseInLoop() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT plcase_in_loop() AS result")) {
            assertTrue(rs.next());
            assertEquals("one two three 4 5", rs.getString("result"));
        }
    }

    // ========================================================================
    // 10. CASE with NULL operand
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #10: CASE with NULL operand falls to ELSE")
    void testNullOperand() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT plcase_null_operand() AS result")) {
            assertTrue(rs.next());
            assertEquals("fell through to else", rs.getString("result"));
        }
    }

    // ========================================================================
    // 11. Searched CASE with IS NULL
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #11: searched CASE with IS NULL test")
    void testIsNull() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT plcase_is_null(NULL) AS r1, plcase_is_null(5) AS r2, "
                             + "plcase_is_null(-1) AS r3")) {
            assertTrue(rs.next());
            assertEquals("null value", rs.getString("r1"));
            assertEquals("positive", rs.getString("r2"));
            assertEquals("non-positive", rs.getString("r3"));
        }
    }

    // ========================================================================
    // 12a. CASE with RAISE — success path
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #12a: CASE with RAISE returns ok for positive")
    void testRaiseSuccess() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT plcase_raise(5) AS result")) {
            assertTrue(rs.next());
            assertEquals("ok: 5", rs.getString("result"));
        }
    }

    // ========================================================================
    // 12b. CASE with RAISE — error path (SQLSTATE P0001)
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #12b: CASE with RAISE throws for negative (P0001)")
    void testRaiseError() throws Exception {
        try {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT plcase_raise(-1)");
            }
            fail("Expected raise_exception error");
        } catch (SQLException e) {
            assertEquals("P0001", e.getSQLState());
        }
    }

    // ========================================================================
    // 13a. CASE with DML — insert path
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #13a: CASE with DML performs insert")
    void testDmlInsert() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT plcase_dml('insert') AS result")) {
            assertTrue(rs.next());
            assertEquals("inserted", rs.getString("result"));
        }
    }

    // ========================================================================
    // 13b. CASE with DML — verify row count after insert
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #13b: CASE with DML verifies row inserted")
    void testDmlInsertCount() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM plcase_log");
            stmt.executeQuery("SELECT plcase_dml('insert')");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT count(*)::integer AS cnt FROM plcase_log")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("cnt"));
            }
        }
    }

    // ========================================================================
    // 13c. CASE with DML — delete path
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #13c: CASE with DML performs delete")
    void testDmlDelete() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT plcase_dml('delete') AS result")) {
            assertTrue(rs.next());
            assertEquals("deleted", rs.getString("result"));
        }
    }

    // ========================================================================
    // 14. CASE on boolean operand
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #14: CASE on boolean operand")
    void testBoolCase() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT plcase_bool(true) AS r1, plcase_bool(false) AS r2, "
                             + "plcase_bool(NULL) AS r3")) {
            assertTrue(rs.next());
            assertEquals("yes", rs.getString("r1"));
            assertEquals("no", rs.getString("r2"));
            assertEquals("null", rs.getString("r3"));
        }
    }

    // ========================================================================
    // 15. CASE on variable operand
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #15: CASE on computed variable operand")
    void testVarOperand() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT plcase_var_operand(5) AS r1, plcase_var_operand(15) AS r2, "
                             + "plcase_var_operand(25) AS r3, plcase_var_operand(42) AS r4")) {
            assertTrue(rs.next());
            assertEquals("single digit", rs.getString("r1"));
            assertEquals("teens", rs.getString("r2"));
            assertEquals("twenties", rs.getString("r3"));
            assertEquals("thirty+", rs.getString("r4"));
        }
    }

    // ========================================================================
    // 16. Both simple and searched CASE in one function
    // ========================================================================

    @Test
    @DisplayName("PG-vs-Memgres #16: both CASE statement and SQL CASE expression")
    void testBothCases() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT plcase_both(3) AS r1, plcase_both(4) AS r2, "
                             + "plcase_both(0) AS r3")) {
            assertTrue(rs.next());
            assertEquals("pos-odd", rs.getString("r1"));
            assertEquals("pos-even", rs.getString("r2"));
            assertEquals("non-pos-even", rs.getString("r3"));
        }
    }
}
