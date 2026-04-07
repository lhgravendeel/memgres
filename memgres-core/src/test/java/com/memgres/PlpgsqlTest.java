package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 13: PL/pgSQL Full Language tests.
 * Variable declarations, control flow, exception handling, triggers, procedures, etc.
 */
class PlpgsqlTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ==================== Basic Functions ====================

    @Test
    void testSimpleFunction() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION add_nums(a INTEGER, b INTEGER) RETURNS INTEGER AS $$ " +
                    "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT add_nums(3, 4)");
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
            stmt.execute("DROP FUNCTION add_nums");
        }
    }

    @Test
    void testFunctionWithDeclare() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION double_it(x INTEGER) RETURNS INTEGER AS $$ " +
                    "DECLARE result INTEGER; " +
                    "BEGIN result := x * 2; RETURN result; END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT double_it(5)");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            stmt.execute("DROP FUNCTION double_it");
        }
    }

    @Test
    void testFunctionWithDefaultValue() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION greet(name TEXT) RETURNS TEXT AS $$ " +
                    "DECLARE greeting TEXT; " +
                    "BEGIN greeting := 'Hello, ' || name || '!'; RETURN greeting; END; " +
                    "$$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT greet('World')");
            assertTrue(rs.next());
            assertEquals("Hello, World!", rs.getString(1));
            stmt.execute("DROP FUNCTION greet");
        }
    }

    @Test
    void testFunctionCalledInWhere() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE fn_data (id INTEGER, val INTEGER)");
            stmt.execute("INSERT INTO fn_data VALUES (1, 10), (2, 20), (3, 30)");
            stmt.execute("CREATE FUNCTION min_val() RETURNS INTEGER AS $$ " +
                    "BEGIN RETURN 15; END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT id FROM fn_data WHERE val > min_val() ORDER BY id");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
            stmt.execute("DROP FUNCTION min_val");
            stmt.execute("DROP TABLE fn_data");
        }
    }

    // ==================== IF/ELSIF/ELSE ====================

    @Test
    void testIfElse() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION classify(n INTEGER) RETURNS TEXT AS $$ " +
                    "BEGIN " +
                    "IF n < 0 THEN RETURN 'negative'; " +
                    "ELSIF n = 0 THEN RETURN 'zero'; " +
                    "ELSE RETURN 'positive'; " +
                    "END IF; " +
                    "END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT classify(-1), classify(0), classify(5)");
            assertTrue(rs.next());
            assertEquals("negative", rs.getString(1));
            assertEquals("zero", rs.getString(2));
            assertEquals("positive", rs.getString(3));
            stmt.execute("DROP FUNCTION classify");
        }
    }

    // ==================== LOOP with EXIT ====================

    @Test
    void testLoopWithExit() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION count_to(n INTEGER) RETURNS INTEGER AS $$ " +
                    "DECLARE i INTEGER; " +
                    "BEGIN " +
                    "i := 0; " +
                    "LOOP " +
                    "  i := i + 1; " +
                    "  EXIT WHEN i >= n; " +
                    "END LOOP; " +
                    "RETURN i; " +
                    "END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT count_to(5)");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            stmt.execute("DROP FUNCTION count_to");
        }
    }

    // ==================== WHILE LOOP ====================

    @Test
    void testWhileLoop() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION factorial(n INTEGER) RETURNS INTEGER AS $$ " +
                    "DECLARE result INTEGER; i INTEGER; " +
                    "BEGIN " +
                    "result := 1; i := 1; " +
                    "WHILE i <= n LOOP " +
                    "  result := result * i; " +
                    "  i := i + 1; " +
                    "END LOOP; " +
                    "RETURN result; " +
                    "END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT factorial(5)");
            assertTrue(rs.next());
            assertEquals(120, rs.getInt(1));
            stmt.execute("DROP FUNCTION factorial");
        }
    }

    // ==================== FOR integer range ====================

    @Test
    void testForRange() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sum_range(low INTEGER, high INTEGER) RETURNS INTEGER AS $$ " +
                    "DECLARE total INTEGER; " +
                    "BEGIN " +
                    "total := 0; " +
                    "FOR i IN low..high LOOP " +
                    "  total := total + i; " +
                    "END LOOP; " +
                    "RETURN total; " +
                    "END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT sum_range(1, 10)");
            assertTrue(rs.next());
            assertEquals(55, rs.getInt(1));
            stmt.execute("DROP FUNCTION sum_range");
        }
    }

    // ==================== FOR query loop ====================

    @Test
    void testForQueryLoop() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE fql_data (id INTEGER, val INTEGER)");
            stmt.execute("INSERT INTO fql_data VALUES (1, 10), (2, 20), (3, 30)");
            stmt.execute("CREATE FUNCTION sum_vals() RETURNS INTEGER AS $$ " +
                    "DECLARE total INTEGER; rec RECORD; " +
                    "BEGIN " +
                    "total := 0; " +
                    "FOR rec IN SELECT val FROM fql_data LOOP " +
                    "  total := total + rec; " +
                    "END LOOP; " +
                    "RETURN total; " +
                    "END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT sum_vals()");
            assertTrue(rs.next());
            assertEquals(60, rs.getInt(1));
            stmt.execute("DROP FUNCTION sum_vals");
            stmt.execute("DROP TABLE fql_data");
        }
    }

    // ==================== RAISE ====================

    @Test
    void testRaiseException() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION must_be_positive(n INTEGER) RETURNS INTEGER AS $$ " +
                    "BEGIN " +
                    "IF n <= 0 THEN " +
                    "  RAISE EXCEPTION 'Value must be positive, got %', n; " +
                    "END IF; " +
                    "RETURN n; " +
                    "END; $$ LANGUAGE plpgsql");
            // Positive value should work
            ResultSet rs = stmt.executeQuery("SELECT must_be_positive(5)");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            // Negative value should throw
            assertThrows(SQLException.class, () ->
                    stmt.executeQuery("SELECT must_be_positive(-1)"));
            stmt.execute("DROP FUNCTION must_be_positive");
        }
    }

    // ==================== Exception handling ====================

    @Test
    void testExceptionHandler() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION safe_divide(a INTEGER, b INTEGER) RETURNS INTEGER AS $$ " +
                    "BEGIN " +
                    "  RETURN a / b; " +
                    "EXCEPTION " +
                    "  WHEN division_by_zero THEN RETURN -1; " +
                    "END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT safe_divide(10, 2)");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
            rs = stmt.executeQuery("SELECT safe_divide(10, 0)");
            assertTrue(rs.next());
            assertEquals(-1, rs.getInt(1));
            stmt.execute("DROP FUNCTION safe_divide");
        }
    }

    @Test
    void testExceptionHandlerOthers() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION safe_func() RETURNS TEXT AS $$ " +
                    "BEGIN " +
                    "  RETURN 1 / 0; " +
                    "EXCEPTION " +
                    "  WHEN others THEN RETURN 'caught'; " +
                    "END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT safe_func()");
            assertTrue(rs.next());
            assertEquals("caught", rs.getString(1));
            stmt.execute("DROP FUNCTION safe_func");
        }
    }

    // ==================== Triggers with PL/pgSQL ====================

    @Test
    void testTriggerWithIfTgOp() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE audit_tbl (id SERIAL PRIMARY KEY, name TEXT, op TEXT)");
            stmt.execute("CREATE FUNCTION audit_trigger_fn() RETURNS trigger AS $$ " +
                    "BEGIN " +
                    "IF TG_OP = 'INSERT' THEN " +
                    "  NEW.op = 'inserted'; " +
                    "ELSIF TG_OP = 'UPDATE' THEN " +
                    "  NEW.op = 'updated'; " +
                    "END IF; " +
                    "RETURN NEW; " +
                    "END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE TRIGGER audit_trg BEFORE INSERT OR UPDATE ON audit_tbl " +
                    "FOR EACH ROW EXECUTE FUNCTION audit_trigger_fn()");
            stmt.execute("INSERT INTO audit_tbl (name) VALUES ('alpha')");
            ResultSet rs = stmt.executeQuery("SELECT op FROM audit_tbl WHERE name = 'alpha'");
            assertTrue(rs.next());
            assertEquals("inserted", rs.getString(1));

            stmt.execute("UPDATE audit_tbl SET name = 'beta' WHERE id = 1");
            rs = stmt.executeQuery("SELECT op FROM audit_tbl WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("updated", rs.getString(1));

            stmt.execute("DROP TRIGGER audit_trg ON audit_tbl");
            stmt.execute("DROP FUNCTION audit_trigger_fn");
            stmt.execute("DROP TABLE audit_tbl");
        }
    }

    @Test
    void testTriggerWithTgTableName() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tgt_test (id INTEGER, source TEXT)");
            stmt.execute("CREATE FUNCTION stamp_source() RETURNS trigger AS $$ " +
                    "BEGIN " +
                    "NEW.source = TG_TABLE_NAME; " +
                    "RETURN NEW; " +
                    "END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE TRIGGER stamp_trg BEFORE INSERT ON tgt_test " +
                    "FOR EACH ROW EXECUTE FUNCTION stamp_source()");
            stmt.execute("INSERT INTO tgt_test (id) VALUES (1)");
            ResultSet rs = stmt.executeQuery("SELECT source FROM tgt_test WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("tgt_test", rs.getString(1));
            stmt.execute("DROP TABLE tgt_test");
            stmt.execute("DROP FUNCTION stamp_source");
        }
    }

    // ==================== PERFORM ====================

    @Test
    void testPerform() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE perf_test (id SERIAL PRIMARY KEY, val TEXT)");
            stmt.execute("CREATE FUNCTION do_insert(v TEXT) RETURNS VOID AS $$ " +
                    "BEGIN " +
                    "PERFORM 1; " +  // just a no-op PERFORM
                    "INSERT INTO perf_test (val) VALUES (v); " +
                    "END; $$ LANGUAGE plpgsql");
            // Call the function using SELECT (returns void/null)
            stmt.executeQuery("SELECT do_insert('hello')");
            ResultSet rs = stmt.executeQuery("SELECT val FROM perf_test");
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
            stmt.execute("DROP FUNCTION do_insert");
            stmt.execute("DROP TABLE perf_test");
        }
    }

    // ==================== EXECUTE dynamic SQL ====================

    @Test
    void testExecuteDynamicSql() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE dyn_test (id INTEGER, name TEXT)");
            stmt.execute("CREATE FUNCTION insert_dynamic(tid INTEGER, tname TEXT) RETURNS VOID AS $$ " +
                    "BEGIN " +
                    "EXECUTE 'INSERT INTO dyn_test VALUES (' || tid || ', ' || quote_literal(tname) || ')'; " +
                    "END; $$ LANGUAGE plpgsql");
            stmt.executeQuery("SELECT insert_dynamic(1, 'Alice')");
            ResultSet rs = stmt.executeQuery("SELECT name FROM dyn_test WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
            stmt.execute("DROP FUNCTION insert_dynamic");
            stmt.execute("DROP TABLE dyn_test");
        }
    }

    // ==================== CREATE PROCEDURE + CALL ====================

    @Test
    void testCreateProcedureAndCall() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE proc_test (id SERIAL PRIMARY KEY, val INTEGER)");
            stmt.execute("CREATE PROCEDURE insert_val(v INTEGER) LANGUAGE plpgsql AS $$ " +
                    "BEGIN " +
                    "INSERT INTO proc_test (val) VALUES (v); " +
                    "END; $$");
            stmt.execute("CALL insert_val(42)");
            ResultSet rs = stmt.executeQuery("SELECT val FROM proc_test");
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
            stmt.execute("DROP PROCEDURE insert_val");
            stmt.execute("DROP TABLE proc_test");
        }
    }

    // ==================== SQL function (non-plpgsql) ====================

    @Test
    void testSqlLanguageFunction() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sql_add(a INTEGER, b INTEGER) RETURNS INTEGER AS $$ " +
                    "SELECT a + b; $$ LANGUAGE sql");
            ResultSet rs = stmt.executeQuery("SELECT sql_add(10, 20)");
            assertTrue(rs.next());
            assertEquals(30, rs.getInt(1));
            stmt.execute("DROP FUNCTION sql_add");
        }
    }

    // ==================== FOUND variable ====================

    @Test
    void testFoundVariable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION check_found() RETURNS BOOLEAN AS $$ " +
                    "DECLARE total INTEGER; " +
                    "BEGIN " +
                    "total := 0; " +
                    "FOR i IN 1..3 LOOP " +
                    "  total := total + i; " +
                    "END LOOP; " +
                    "RETURN FOUND; " +
                    "END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT check_found()");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            stmt.execute("DROP FUNCTION check_found");
        }
    }

    // ==================== Nested blocks ====================

    @Test
    void testNestedBlock() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION nested_test() RETURNS INTEGER AS $$ " +
                    "DECLARE x INTEGER; " +
                    "BEGIN " +
                    "x := 10; " +
                    "BEGIN " +
                    "  x := x + 5; " +
                    "END; " +
                    "RETURN x; " +
                    "END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT nested_test()");
            assertTrue(rs.next());
            assertEquals(15, rs.getInt(1));
            stmt.execute("DROP FUNCTION nested_test");
        }
    }

    // ==================== Multiple return paths ====================

    @Test
    void testMultipleReturns() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION abs_val(n INTEGER) RETURNS INTEGER AS $$ " +
                    "BEGIN " +
                    "IF n < 0 THEN RETURN -n; END IF; " +
                    "RETURN n; " +
                    "END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT abs_val(-7), abs_val(3)");
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            stmt.execute("DROP FUNCTION abs_val");
        }
    }

    // ==================== Function using SQL queries ====================

    @Test
    void testFunctionWithSqlQuery() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE emp (id INTEGER, name TEXT, salary INTEGER)");
            stmt.execute("INSERT INTO emp VALUES (1, 'Alice', 50000), (2, 'Bob', 60000), (3, 'Carol', 70000)");
            stmt.execute("CREATE FUNCTION get_max_salary() RETURNS INTEGER AS $$ " +
                    "DECLARE max_sal INTEGER; " +
                    "BEGIN " +
                    "SELECT INTO max_sal MAX(salary) FROM emp; " +
                    "RETURN max_sal; " +
                    "END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT get_max_salary()");
            assertTrue(rs.next());
            assertEquals(70000, rs.getInt(1));
            stmt.execute("DROP FUNCTION get_max_salary");
            stmt.execute("DROP TABLE emp");
        }
    }

    // ==================== GET DIAGNOSTICS ====================

    @Test
    void testGetDiagnostics() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE diag_test (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO diag_test VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            stmt.execute("CREATE FUNCTION count_deleted() RETURNS INTEGER AS $$ " +
                    "DECLARE cnt INTEGER; " +
                    "BEGIN " +
                    "DELETE FROM diag_test WHERE id > 1; " +
                    "GET DIAGNOSTICS cnt = ROW_COUNT; " +
                    "RETURN cnt; " +
                    "END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT count_deleted()");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            stmt.execute("DROP FUNCTION count_deleted");
            stmt.execute("DROP TABLE diag_test");
        }
    }

    // ==================== OR REPLACE ====================

    @Test
    void testOrReplaceFunction() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION replaceable() RETURNS INTEGER AS $$ " +
                    "BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT replaceable()");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            // Replace it
            stmt.execute("CREATE OR REPLACE FUNCTION replaceable() RETURNS INTEGER AS $$ " +
                    "BEGIN RETURN 2; END; $$ LANGUAGE plpgsql");
            rs = stmt.executeQuery("SELECT replaceable()");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            stmt.execute("DROP FUNCTION replaceable");
        }
    }

    // ==================== CONTINUE in loop ====================

    @Test
    void testContinueInLoop() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sum_odd(n INTEGER) RETURNS INTEGER AS $$ " +
                    "DECLARE total INTEGER; " +
                    "BEGIN " +
                    "total := 0; " +
                    "FOR i IN 1..n LOOP " +
                    "  CONTINUE WHEN i % 2 = 0; " +
                    "  total := total + i; " +
                    "END LOOP; " +
                    "RETURN total; " +
                    "END; $$ LANGUAGE plpgsql");
            // 1+3+5+7+9 = 25
            ResultSet rs = stmt.executeQuery("SELECT sum_odd(10)");
            assertTrue(rs.next());
            assertEquals(25, rs.getInt(1));
            stmt.execute("DROP FUNCTION sum_odd");
        }
    }

    // ==================== RAISE with ERRCODE ====================

    @Test
    void testRaiseWithErrcode() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION check_code(n INTEGER) RETURNS TEXT AS $$ " +
                    "BEGIN " +
                    "IF n = 0 THEN " +
                    "  RAISE EXCEPTION 'bad value' USING ERRCODE = '22012'; " +
                    "END IF; " +
                    "RETURN 'ok'; " +
                    "END; $$ LANGUAGE plpgsql");
            ResultSet rs = stmt.executeQuery("SELECT check_code(1)");
            assertTrue(rs.next());
            assertEquals("ok", rs.getString(1));
            try {
                stmt.executeQuery("SELECT check_code(0)");
                fail("Should have thrown");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("bad value"));
            }
            stmt.execute("DROP FUNCTION check_code");
        }
    }
}
