package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PL/pgSQL exception condition handling differences between PostgreSQL and Memgres.
 * Covers 23 differences identified in plpgsql-exception-conditions.sql.
 *
 * All tests are INTENDED TO FAIL: PostgreSQL correctly catches these exception conditions,
 * but Memgres either returns 'no error' or lets the error propagate uncaught.
 */
class PlpgsqlExceptionCompatTest {
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

            // Create table for no_data_found / too_many_rows tests
            tryExec(stmt, "CREATE TABLE exc_test_data (id int PRIMARY KEY, val text)");
            tryExec(stmt, "INSERT INTO exc_test_data VALUES (1, 'a'), (2, 'b'), (3, 'c')");

            // Create table for integrity_constraint_violation test
            tryExec(stmt, "CREATE TABLE exc_uniq (id int PRIMARY KEY)");
            tryExec(stmt, "INSERT INTO exc_uniq VALUES (1)");

            // 1. no_data_found
            tryExec(stmt,
                "CREATE FUNCTION exc_no_data_found() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "DECLARE r record;\n" +
                "BEGIN\n" +
                "  SELECT * INTO STRICT r FROM exc_test_data WHERE id = 9999;\n" +
                "  RETURN 'no error';\n" +
                "EXCEPTION\n" +
                "  WHEN no_data_found THEN RETURN 'caught no_data_found';\n" +
                "END;\n" +
                "$$");

            // 2. too_many_rows
            tryExec(stmt,
                "CREATE FUNCTION exc_too_many_rows() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "DECLARE r record;\n" +
                "BEGIN\n" +
                "  SELECT * INTO STRICT r FROM exc_test_data;\n" +
                "  RETURN 'no error';\n" +
                "EXCEPTION\n" +
                "  WHEN too_many_rows THEN RETURN 'caught too_many_rows';\n" +
                "END;\n" +
                "$$");

            // 3. insufficient_privilege
            tryExec(stmt,
                "CREATE FUNCTION exc_insufficient_privilege() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '42501' USING MESSAGE = 'simulated insufficient privilege';\n" +
                "EXCEPTION\n" +
                "  WHEN insufficient_privilege THEN RETURN 'caught insufficient_privilege';\n" +
                "END;\n" +
                "$$");

            // 4. deadlock_detected
            tryExec(stmt,
                "CREATE FUNCTION exc_deadlock_detected() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '40P01' USING MESSAGE = 'simulated deadlock';\n" +
                "EXCEPTION\n" +
                "  WHEN deadlock_detected THEN RETURN 'caught deadlock_detected';\n" +
                "END;\n" +
                "$$");

            // 5. lock_not_available
            tryExec(stmt,
                "CREATE FUNCTION exc_lock_not_available() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '55P03' USING MESSAGE = 'simulated lock not available';\n" +
                "EXCEPTION\n" +
                "  WHEN lock_not_available THEN RETURN 'caught lock_not_available';\n" +
                "END;\n" +
                "$$");

            // 6. invalid_cursor_state
            tryExec(stmt,
                "CREATE FUNCTION exc_invalid_cursor_state() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '24000' USING MESSAGE = 'simulated invalid cursor state';\n" +
                "EXCEPTION\n" +
                "  WHEN invalid_cursor_state THEN RETURN 'caught invalid_cursor_state';\n" +
                "END;\n" +
                "$$");

            // 7. invalid_transaction_state
            tryExec(stmt,
                "CREATE FUNCTION exc_invalid_transaction_state() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '25000' USING MESSAGE = 'simulated invalid transaction state';\n" +
                "EXCEPTION\n" +
                "  WHEN invalid_transaction_state THEN RETURN 'caught invalid_transaction_state';\n" +
                "END;\n" +
                "$$");

            // 8. null_value_not_allowed
            tryExec(stmt,
                "CREATE FUNCTION exc_null_value_not_allowed() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '22004' USING MESSAGE = 'simulated null value not allowed';\n" +
                "EXCEPTION\n" +
                "  WHEN null_value_not_allowed THEN RETURN 'caught null_value_not_allowed';\n" +
                "END;\n" +
                "$$");

            // 9. invalid_regular_expression
            tryExec(stmt,
                "CREATE FUNCTION exc_invalid_regular_expression() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '2201B' USING MESSAGE = 'simulated invalid regular expression';\n" +
                "EXCEPTION\n" +
                "  WHEN invalid_regular_expression THEN RETURN 'caught invalid_regular_expression';\n" +
                "END;\n" +
                "$$");

            // 10. datetime_field_overflow
            tryExec(stmt,
                "CREATE FUNCTION exc_datetime_field_overflow() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '22008' USING MESSAGE = 'simulated datetime field overflow';\n" +
                "EXCEPTION\n" +
                "  WHEN datetime_field_overflow THEN RETURN 'caught datetime_field_overflow';\n" +
                "END;\n" +
                "$$");

            // 11. object_in_use
            tryExec(stmt,
                "CREATE FUNCTION exc_object_in_use() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '55006' USING MESSAGE = 'simulated object in use';\n" +
                "EXCEPTION\n" +
                "  WHEN object_in_use THEN RETURN 'caught object_in_use';\n" +
                "END;\n" +
                "$$");

            // 12. program_limit_exceeded
            tryExec(stmt,
                "CREATE FUNCTION exc_program_limit_exceeded() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '54000' USING MESSAGE = 'simulated program limit exceeded';\n" +
                "EXCEPTION\n" +
                "  WHEN program_limit_exceeded THEN RETURN 'caught program_limit_exceeded';\n" +
                "END;\n" +
                "$$");

            // 13. case_not_found
            tryExec(stmt,
                "CREATE FUNCTION exc_case_not_found() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '20000' USING MESSAGE = 'simulated case not found';\n" +
                "EXCEPTION\n" +
                "  WHEN case_not_found THEN RETURN 'caught case_not_found';\n" +
                "END;\n" +
                "$$");

            // 14. with_check_option_violation
            tryExec(stmt,
                "CREATE FUNCTION exc_with_check_option_violation() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '44000' USING MESSAGE = 'simulated with check option violation';\n" +
                "EXCEPTION\n" +
                "  WHEN with_check_option_violation THEN RETURN 'caught with_check_option_violation';\n" +
                "END;\n" +
                "$$");

            // 15. triggered_action_exception
            tryExec(stmt,
                "CREATE FUNCTION exc_triggered_action_exception() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '09000' USING MESSAGE = 'simulated triggered action exception';\n" +
                "EXCEPTION\n" +
                "  WHEN triggered_action_exception THEN RETURN 'caught triggered_action_exception';\n" +
                "END;\n" +
                "$$");

            // 16. plpgsql_error
            tryExec(stmt,
                "CREATE FUNCTION exc_plpgsql_error() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE 'P0000' USING MESSAGE = 'simulated plpgsql error';\n" +
                "EXCEPTION\n" +
                "  WHEN plpgsql_error THEN RETURN 'caught plpgsql_error';\n" +
                "END;\n" +
                "$$");

            // 17. invalid_escape_sequence
            tryExec(stmt,
                "CREATE FUNCTION exc_invalid_escape_sequence() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '22025' USING MESSAGE = 'simulated invalid escape sequence';\n" +
                "EXCEPTION\n" +
                "  WHEN invalid_escape_sequence THEN RETURN 'caught invalid_escape_sequence';\n" +
                "END;\n" +
                "$$");

            // 18. name_too_long
            tryExec(stmt,
                "CREATE FUNCTION exc_name_too_long() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '42622' USING MESSAGE = 'simulated name too long';\n" +
                "EXCEPTION\n" +
                "  WHEN name_too_long THEN RETURN 'caught name_too_long';\n" +
                "END;\n" +
                "$$");

            // 19. external_routine_exception
            tryExec(stmt,
                "CREATE FUNCTION exc_external_routine_exception() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '38000' USING MESSAGE = 'simulated external routine exception';\n" +
                "EXCEPTION\n" +
                "  WHEN external_routine_exception THEN RETURN 'caught external_routine_exception';\n" +
                "END;\n" +
                "$$");

            // 20. data_exception (class-level catch for class 22)
            tryExec(stmt,
                "CREATE FUNCTION exc_data_exception() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "DECLARE x int;\n" +
                "BEGIN\n" +
                "  x := 1 / 0;\n" +
                "  RETURN 'no error';\n" +
                "EXCEPTION\n" +
                "  WHEN data_exception THEN RETURN 'caught data_exception';\n" +
                "END;\n" +
                "$$");

            // 21. integrity_constraint_violation (class-level catch for class 23)
            tryExec(stmt,
                "CREATE FUNCTION exc_integrity_constraint_violation() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  INSERT INTO exc_uniq VALUES (1);\n" +
                "  RETURN 'no error';\n" +
                "EXCEPTION\n" +
                "  WHEN integrity_constraint_violation THEN RETURN 'caught integrity_constraint_violation';\n" +
                "END;\n" +
                "$$");

            // 22. syntax_error_or_access_rule_violation (class-level catch for class 42)
            tryExec(stmt,
                "CREATE FUNCTION exc_syntax_error_or_access_rule_violation() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE '42501' USING MESSAGE = 'simulated access rule violation';\n" +
                "EXCEPTION\n" +
                "  WHEN syntax_error_or_access_rule_violation THEN RETURN 'caught syntax_error_or_access_rule_violation';\n" +
                "END;\n" +
                "$$");

            // 23. custom SQLSTATE
            tryExec(stmt,
                "CREATE FUNCTION exc_custom_sqlstate() RETURNS text LANGUAGE plpgsql AS $$\n" +
                "BEGIN\n" +
                "  RAISE SQLSTATE 'MG001' USING MESSAGE = 'simulated custom error';\n" +
                "EXCEPTION\n" +
                "  WHEN SQLSTATE 'MG001' THEN RETURN 'caught custom SQLSTATE MG001';\n" +
                "END;\n" +
                "$$");
        }
    }

    private static void tryExec(Statement stmt, String sql) {
        try {
            stmt.execute(sql);
        } catch (SQLException e) {
            System.err.println("Setup warning: " + e.getMessage());
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private String callFunction(String functionName) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT " + functionName + "()")) {
            assertTrue(rs.next(), "Expected a result from " + functionName);
            return rs.getString(1);
        }
    }

    // 1. no_data_found: SELECT INTO STRICT with no matching rows
    @Test
    void testNoDataFound() throws SQLException {
        assertEquals("caught no_data_found", callFunction("exc_no_data_found"));
    }

    // 2. too_many_rows: SELECT INTO STRICT with multiple matching rows
    @Test
    void testTooManyRows() throws SQLException {
        assertEquals("caught too_many_rows", callFunction("exc_too_many_rows"));
    }

    // 3. insufficient_privilege: SQLSTATE 42501
    @Test
    void testInsufficientPrivilege() throws SQLException {
        assertEquals("caught insufficient_privilege", callFunction("exc_insufficient_privilege"));
    }

    // 4. deadlock_detected: SQLSTATE 40P01
    @Test
    void testDeadlockDetected() throws SQLException {
        assertEquals("caught deadlock_detected", callFunction("exc_deadlock_detected"));
    }

    // 5. lock_not_available: SQLSTATE 55P03
    @Test
    void testLockNotAvailable() throws SQLException {
        assertEquals("caught lock_not_available", callFunction("exc_lock_not_available"));
    }

    // 6. invalid_cursor_state: SQLSTATE 24000
    @Test
    void testInvalidCursorState() throws SQLException {
        assertEquals("caught invalid_cursor_state", callFunction("exc_invalid_cursor_state"));
    }

    // 7. invalid_transaction_state: SQLSTATE 25000
    @Test
    void testInvalidTransactionState() throws SQLException {
        assertEquals("caught invalid_transaction_state", callFunction("exc_invalid_transaction_state"));
    }

    // 8. null_value_not_allowed: SQLSTATE 22004
    @Test
    void testNullValueNotAllowed() throws SQLException {
        assertEquals("caught null_value_not_allowed", callFunction("exc_null_value_not_allowed"));
    }

    // 9. invalid_regular_expression: SQLSTATE 2201B
    @Test
    void testInvalidRegularExpression() throws SQLException {
        assertEquals("caught invalid_regular_expression", callFunction("exc_invalid_regular_expression"));
    }

    // 10. datetime_field_overflow: SQLSTATE 22008
    @Test
    void testDatetimeFieldOverflow() throws SQLException {
        assertEquals("caught datetime_field_overflow", callFunction("exc_datetime_field_overflow"));
    }

    // 11. object_in_use: SQLSTATE 55006
    @Test
    void testObjectInUse() throws SQLException {
        assertEquals("caught object_in_use", callFunction("exc_object_in_use"));
    }

    // 12. program_limit_exceeded: SQLSTATE 54000
    @Test
    void testProgramLimitExceeded() throws SQLException {
        assertEquals("caught program_limit_exceeded", callFunction("exc_program_limit_exceeded"));
    }

    // 13. case_not_found: SQLSTATE 20000
    @Test
    void testCaseNotFound() throws SQLException {
        assertEquals("caught case_not_found", callFunction("exc_case_not_found"));
    }

    // 14. with_check_option_violation: SQLSTATE 44000
    @Test
    void testWithCheckOptionViolation() throws SQLException {
        assertEquals("caught with_check_option_violation", callFunction("exc_with_check_option_violation"));
    }

    // 15. triggered_action_exception: SQLSTATE 09000
    @Test
    void testTriggeredActionException() throws SQLException {
        assertEquals("caught triggered_action_exception", callFunction("exc_triggered_action_exception"));
    }

    // 16. plpgsql_error: SQLSTATE P0000
    @Test
    void testPlpgsqlError() throws SQLException {
        assertEquals("caught plpgsql_error", callFunction("exc_plpgsql_error"));
    }

    // 17. invalid_escape_sequence: SQLSTATE 22025
    @Test
    void testInvalidEscapeSequence() throws SQLException {
        assertEquals("caught invalid_escape_sequence", callFunction("exc_invalid_escape_sequence"));
    }

    // 18. name_too_long: SQLSTATE 42622
    @Test
    void testNameTooLong() throws SQLException {
        assertEquals("caught name_too_long", callFunction("exc_name_too_long"));
    }

    // 19. external_routine_exception: SQLSTATE 38000
    @Test
    void testExternalRoutineException() throws SQLException {
        assertEquals("caught external_routine_exception", callFunction("exc_external_routine_exception"));
    }

    // 20. data_exception: class-level catch (class 22) for division_by_zero
    @Test
    void testDataException() throws SQLException {
        assertEquals("caught data_exception", callFunction("exc_data_exception"));
    }

    // 21. integrity_constraint_violation: class-level catch (class 23) for unique violation
    @Test
    void testIntegrityConstraintViolation() throws SQLException {
        assertEquals("caught integrity_constraint_violation", callFunction("exc_integrity_constraint_violation"));
    }

    // 22. syntax_error_or_access_rule_violation: class-level catch (class 42)
    @Test
    void testSyntaxErrorOrAccessRuleViolation() throws SQLException {
        assertEquals("caught syntax_error_or_access_rule_violation",
                callFunction("exc_syntax_error_or_access_rule_violation"));
    }

    // 23. custom SQLSTATE: WHEN SQLSTATE 'MG001'
    @Test
    void testCustomSqlstate() throws SQLException {
        assertEquals("caught custom SQLSTATE MG001", callFunction("exc_custom_sqlstate"));
    }
}
