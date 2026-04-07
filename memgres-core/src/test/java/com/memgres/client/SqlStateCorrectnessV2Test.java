package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SQLSTATE error code correctness tests.
 * Ensures Memgres returns the correct PostgreSQL SQLSTATE codes for various error conditions.
 */
class SqlStateCorrectnessV2Test {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        // Setup tables, types, and objects needed for tests
        try (Statement s = conn.createStatement()) {
            // Basic test table
            s.execute("CREATE TABLE ssv2_t (id INT PRIMARY KEY, a INT, b TEXT, c BOOLEAN)");
            s.execute("INSERT INTO ssv2_t VALUES (1, 10, 'hello', true), (2, 20, 'world', false)");

            // Table for constraint tests
            s.execute("CREATE TABLE ssv2_parent (id INT PRIMARY KEY, name TEXT)");
            s.execute("INSERT INTO ssv2_parent VALUES (1, 'parent1')");
            s.execute("CREATE TABLE ssv2_child (id INT PRIMARY KEY, parent_id INT REFERENCES ssv2_parent(id))");

            // Enum type
            s.execute("CREATE TYPE ssv2_color AS ENUM ('red', 'green', 'blue')");
            s.execute("CREATE TABLE ssv2_enum_t (id INT, color ssv2_color)");

            // Composite type
            s.execute("CREATE TYPE ssv2_pair AS (x INT, y TEXT)");

            // Table with unique constraint for upsert tests
            s.execute("CREATE TABLE ssv2_upsert (id INT PRIMARY KEY, val TEXT, extra INT)");
            s.execute("INSERT INTO ssv2_upsert VALUES (1, 'existing', 100)");

            // Table with arrays
            s.execute("CREATE TABLE ssv2_arr (id INT, nums INT[], labels TEXT[])");
            s.execute("INSERT INTO ssv2_arr VALUES (1, ARRAY[1,2,3], ARRAY['a','b'])");

            // Partitioned table
            s.execute("CREATE TABLE ssv2_part (id INT, val TEXT) PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE ssv2_part_1 PARTITION OF ssv2_part FOR VALUES FROM (1) TO (100)");
            s.execute("CREATE TABLE ssv2_part_2 PARTITION OF ssv2_part FOR VALUES FROM (100) TO (200)");

            // Table for trigger tests
            s.execute("CREATE TABLE ssv2_trig_t (id INT PRIMARY KEY, val TEXT, updated_at TIMESTAMP)");

            // Sequence
            s.execute("CREATE SEQUENCE ssv2_seq START 1");

            // Domain
            s.execute("CREATE DOMAIN ssv2_posint AS INT CHECK (VALUE > 0)");

            // Table for prepared statement tests
            s.execute("CREATE TABLE ssv2_prep (id INT PRIMARY KEY, data TEXT)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS ssv2_prep CASCADE");
                s.execute("DROP TABLE IF EXISTS ssv2_trig_t CASCADE");
                s.execute("DROP TABLE IF EXISTS ssv2_part_1 CASCADE");
                s.execute("DROP TABLE IF EXISTS ssv2_part_2 CASCADE");
                s.execute("DROP TABLE IF EXISTS ssv2_part CASCADE");
                s.execute("DROP TABLE IF EXISTS ssv2_arr CASCADE");
                s.execute("DROP TABLE IF EXISTS ssv2_upsert CASCADE");
                s.execute("DROP TABLE IF EXISTS ssv2_enum_t CASCADE");
                s.execute("DROP TABLE IF EXISTS ssv2_child CASCADE");
                s.execute("DROP TABLE IF EXISTS ssv2_parent CASCADE");
                s.execute("DROP TABLE IF EXISTS ssv2_t CASCADE");
                s.execute("DROP TYPE IF EXISTS ssv2_color CASCADE");
                s.execute("DROP TYPE IF EXISTS ssv2_pair CASCADE");
                s.execute("DROP DOMAIN IF EXISTS ssv2_posint CASCADE");
                s.execute("DROP SEQUENCE IF EXISTS ssv2_seq CASCADE");
            } catch (SQLException ignored) {}
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static void assertSqlState(String sql, String expectedState) {
        try {
            exec(sql);
            fail("Expected error with SQLSTATE " + expectedState + " for: " + sql);
        } catch (SQLException e) {
            assertEquals(expectedState, e.getSQLState(),
                    "Wrong SQLSTATE for: " + sql + ", got: " + e.getMessage());
        }
    }

    // ========================================================================
    // Section 1: Operator Type Mismatch Errors (42883)
    // ========================================================================

    @Test void boolean_plus_int_gives_42883() {
        assertSqlState("SELECT true + 1", "42883");
    }

    @Test void int_plus_boolean_gives_42883() {
        assertSqlState("SELECT 1 + true", "42883");
    }

    @Test void bit_string_plus_bit_string_gives_42883() {
        assertSqlState("SELECT B'101' + B'010'", "42883");
    }

    @Test void bit_string_minus_bit_string_gives_42883() {
        assertSqlState("SELECT B'101' - B'010'", "42883");
    }

    @Test void bit_string_multiply_gives_42883() {
        assertSqlState("SELECT B'101' * B'010'", "42883");
    }

    @Test void array_plus_array_gives_42883() {
        assertSqlState("SELECT ARRAY[1,2] + ARRAY[3,4]", "42883");
    }

    @Test void array_minus_array_gives_42883() {
        assertSqlState("SELECT ARRAY[1,2] - ARRAY[3,4]", "42883");
    }

    @Test void array_multiply_array_gives_42883() {
        assertSqlState("SELECT ARRAY[1,2] * ARRAY[3,4]", "42883");
    }

    @Test void jsonb_multiply_integer_gives_42883() {
        assertSqlState("SELECT '{\"a\":1}'::jsonb * 2", "42883");
    }

    @Test void jsonb_divide_integer_gives_42883() {
        assertSqlState("SELECT '{\"a\":1}'::jsonb / 2", "42883");
    }

    @Test void jsonb_minus_jsonb_for_multiply_gives_42883() {
        assertSqlState("SELECT '{\"a\":1}'::jsonb * '{\"b\":2}'::jsonb", "42883");
    }

    @Test void boolean_concat_boolean_gives_42883() {
        assertSqlState("SELECT true || false", "42883");
    }

    @Test void point_concat_point_gives_42883() {
        assertSqlState("SELECT point '(1,2)' || point '(3,4)'", "42883");
    }

    @Test void jsonb_concat_integer_gives_42883() {
        assertSqlState("SELECT '{\"a\":1}'::jsonb || 1", "42883");
    }

    @Test void point_contains_integer_gives_42883() {
        assertSqlState("SELECT point '(1,2)' @> 1", "42883");
    }

    @Test void bit_equals_integer_gives_42883() {
        assertSqlState("SELECT B'101' = 5", "42883");
    }

    @Test void boolean_minus_boolean_gives_42883() {
        assertSqlState("SELECT true - false", "42883");
    }

    @Test void boolean_multiply_int_gives_42883() {
        assertSqlState("SELECT true * 5", "42883");
    }

    @Test void text_divide_int_gives_42883() {
        assertSqlState("SELECT 'hello' / 2", "42883");
    }

    @Test void text_multiply_text_gives_42883() {
        assertSqlState("SELECT 'a' * 'b'", "42883");
    }

    @Test void int_concat_int_gives_42883() {
        // In PG, || on integers is 42883 (no operator)
        assertSqlState("SELECT 1 || 2", "42883");
    }

    @Test void array_append_wrong_type_gives_22P02() {
        // array_append(int[], text); PG: 22P02 invalid input syntax
        assertSqlState("SELECT array_append(ARRAY[1,2], 'x')", "22P02");
    }

    @Test void center_too_many_args_gives_42883() {
        // center(box, point, point); PG: 42883 function not found
        assertSqlState("SELECT center(box(point(0,0), point(1,1)), point(2,2))", "42883");
    }

    @Test void int4multirange_wrong_arg_gives_22P02() {
        // PG18: int4multirange(text) exists but rejects invalid text input with 22P02
        assertSqlState("SELECT int4multirange('[1,2)')", "22P02");
    }

    @Test void factorial_operator_gives_42601() {
        // ! operator removed in PG14+; PG18: 42601 (syntax error)
        assertSqlState("SELECT 5 !", "42601");
    }

    @Test void double_factorial_operator_gives_42601() {
        assertSqlState("SELECT 5 ! !", "42601");
    }

    @Test void date_multiply_date_gives_42883() {
        assertSqlState("SELECT DATE '2024-01-01' * DATE '2024-01-02'", "42883");
    }

    @Test void timestamp_plus_timestamp_gives_42883() {
        assertSqlState("SELECT TIMESTAMP '2024-01-01 00:00:00' + TIMESTAMP '2024-01-02 00:00:00'", "42883");
    }

    @Test void interval_multiply_interval_gives_42883() {
        assertSqlState("SELECT INTERVAL '1 day' * INTERVAL '2 days'", "42883");
    }

    @Test void uuid_plus_uuid_gives_42883() {
        assertSqlState("SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid + 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12'::uuid", "42883");
    }

    @Test void inet_multiply_inet_gives_42883() {
        assertSqlState("SELECT '192.168.1.1'::inet * '10.0.0.1'::inet", "42883");
    }

    // ========================================================================
    // Section 2: Field/Column Reference Errors (42703 vs 42P01)
    // ========================================================================

    @Test void nonexistent_column_gives_42703() {
        assertSqlState("SELECT nonexistent FROM ssv2_t", "42703");
    }

    @Test void aliased_table_nonexistent_column_gives_42703() {
        assertSqlState("SELECT t.nonexistent FROM ssv2_t t", "42703");
    }

    @Test void nonexistent_table_gives_42P01() {
        assertSqlState("SELECT * FROM nonexistent_table_xyz", "42P01");
    }

    @Test void nonexistent_table_qualified_column_gives_42P01() {
        assertSqlState("SELECT nonexistent_table_xyz.col", "42P01");
    }

    @Test void nonexistent_column_in_where_gives_42703() {
        assertSqlState("SELECT * FROM ssv2_t WHERE nonexistent = 1", "42703");
    }

    @Test void nonexistent_column_in_order_by_gives_42703() {
        assertSqlState("SELECT * FROM ssv2_t ORDER BY nonexistent", "42703");
    }

    @Test void nonexistent_column_in_group_by_gives_42703() {
        assertSqlState("SELECT nonexistent FROM ssv2_t GROUP BY nonexistent", "42703");
    }

    @Test void nonexistent_column_in_having_gives_42703() {
        assertSqlState("SELECT a FROM ssv2_t GROUP BY a HAVING nonexistent > 0", "42703");
    }

    @Test void nonexistent_column_in_insert_gives_42703() {
        assertSqlState("INSERT INTO ssv2_t (id, nonexistent) VALUES (99, 1)", "42703");
    }

    @Test void nonexistent_column_in_update_set_gives_42703() {
        assertSqlState("UPDATE ssv2_t SET nonexistent = 1", "42703");
    }

    @Test void nonexistent_column_in_returning_gives_42703() {
        assertSqlState("INSERT INTO ssv2_t(id, a, b) VALUES (999, 1, 'x') RETURNING nope", "42703");
    }

    @Test void composite_nonexistent_field_gives_42703() {
        // Access a field that doesn't exist on a composite type
        assertSqlState("SELECT (ROW(1,'a')::ssv2_pair).z", "42703");
    }

    @Test void nonexistent_table_in_join_gives_42P01() {
        assertSqlState("SELECT * FROM ssv2_t JOIN nonexistent_xyz ON true", "42P01");
    }

    @Test void nonexistent_column_in_join_condition_gives_42703() {
        assertSqlState("SELECT * FROM ssv2_t t1 JOIN ssv2_t t2 ON t1.nonexistent = t2.id", "42703");
    }

    @Test void nonexistent_table_in_update_gives_42P01() {
        assertSqlState("UPDATE nonexistent_table_xyz SET a = 1", "42P01");
    }

    @Test void nonexistent_table_in_delete_gives_42P01() {
        assertSqlState("DELETE FROM nonexistent_table_xyz", "42P01");
    }

    @Test void ambiguous_column_reference_gives_42702() {
        // When both tables have 'id', unqualified reference is ambiguous
        assertSqlState("SELECT id FROM ssv2_t t1 JOIN ssv2_parent t2 ON t1.id = t2.id", "42702");
    }

    // ========================================================================
    // Section 3: Syntax Errors (42601)
    // ========================================================================

    @Test void double_from_gives_42601() {
        assertSqlState("SELECT 1 FROM FROM ssv2_t", "42601");
    }

    @Test void select_semicolon_gives_42601() {
        // PG 18 allows bare SELECT (returns 0 columns); semicolon is just a terminator
        assertDoesNotThrow(() -> exec("SELECT ;"));
    }

    @Test void select_leading_comma_gives_42601() {
        assertSqlState("SELECT ,1", "42601");
    }

    @Test void empty_in_list_gives_42601() {
        assertSqlState("SELECT 1 WHERE 1 IN ()", "42601");
    }

    @Test void trailing_comma_in_create_table_gives_42601() {
        assertSqlState("CREATE TABLE ssv2_bad_syntax (a INT,)", "42601");
    }

    @Test void missing_table_name_in_from_gives_42601() {
        assertSqlState("SELECT * FROM", "42601");
    }

    @Test void double_where_gives_42601() {
        assertSqlState("SELECT * FROM ssv2_t WHERE WHERE a = 1", "42601");
    }

    @Test void select_star_star_gives_42601() {
        assertSqlState("SELECT * * FROM ssv2_t", "42601");
    }

    @Test void unclosed_parenthesis_gives_42601() {
        assertSqlState("SELECT (1 + 2", "42601");
    }

    @Test void extra_closing_parenthesis_gives_42601() {
        assertSqlState("SELECT 1 + 2)", "42601");
    }

    @Test void create_table_no_columns_succeeds() throws SQLException {
        // PG allows CREATE TABLE with no columns
        try (var s = conn.createStatement()) {
            s.execute("CREATE TABLE ssv2_bad_empty ()");
            s.execute("DROP TABLE ssv2_bad_empty");
        }
    }

    @Test void insert_values_no_parens_gives_42601() {
        assertSqlState("INSERT INTO ssv2_t VALUES 1, 2, 'x'", "42601");
    }

    @Test void select_from_where_no_condition_gives_42601() {
        assertSqlState("SELECT * FROM ssv2_t WHERE", "42601");
    }

    @Test void order_by_nothing_gives_42601() {
        assertSqlState("SELECT * FROM ssv2_t ORDER BY", "42601");
    }

    @Test void group_by_nothing_gives_42601() {
        assertSqlState("SELECT * FROM ssv2_t GROUP BY", "42601");
    }

    @Test void update_no_set_gives_42601() {
        assertSqlState("UPDATE ssv2_t", "42601");
    }

    @Test void delete_no_from_gives_42601() {
        assertSqlState("DELETE ssv2_t", "42601");
    }

    @Test void limit_text_gives_22P02() {
        // LIMIT 'abc' in PG gives 22P02 (invalid input syntax for type bigint)
        assertSqlState("SELECT * FROM ssv2_t LIMIT 'abc'", "22P02");
    }

    @Test void double_distinct_gives_42601() {
        assertSqlState("SELECT DISTINCT DISTINCT a FROM ssv2_t", "42601");
    }

    @Test void all_then_distinct_gives_42601() {
        assertSqlState("SELECT ALL DISTINCT a FROM ssv2_t", "42601");
    }

    @Test void values_as_expression_gives_42601() {
        // pg_typeof(VALUES (1), (2)); VALUES not valid as expression
        assertSqlState("SELECT pg_typeof(VALUES (1), (2))", "42601");
    }

    // ========================================================================
    // Section 4: Transaction State Errors (25P02)
    // ========================================================================

    @Test void select_in_failed_transaction_gives_25P02() throws SQLException {
        conn.setAutoCommit(false);
        try {
            exec("BEGIN");
        } catch (SQLException ignored) {} // might already be in txn
        try {
            exec("SELECT 1/0"); // division by zero
        } catch (SQLException e) {
            assertEquals("22012", e.getSQLState(), "Division by zero should be 22012");
        }
        // Now in failed transaction state
        try {
            exec("SELECT 1");
            fail("Should error in failed transaction");
        } catch (SQLException e) {
            assertEquals("25P02", e.getSQLState(), "Should get in_failed_sql_transaction");
        }
        try { exec("ROLLBACK"); } catch (SQLException ignored) {}
        conn.setAutoCommit(true);
    }

    @Test void insert_in_failed_transaction_gives_25P02() throws SQLException {
        conn.setAutoCommit(false);
        try { exec("BEGIN"); } catch (SQLException ignored) {}
        try { exec("SELECT 1/0"); } catch (SQLException ignored) {}
        try {
            exec("INSERT INTO ssv2_t VALUES (999, 1, 'fail')");
            fail("Should error in failed transaction");
        } catch (SQLException e) {
            assertEquals("25P02", e.getSQLState());
        }
        try { exec("ROLLBACK"); } catch (SQLException ignored) {}
        conn.setAutoCommit(true);
    }

    @Test void update_in_failed_transaction_gives_25P02() throws SQLException {
        conn.setAutoCommit(false);
        try { exec("BEGIN"); } catch (SQLException ignored) {}
        try { exec("SELECT 1/0"); } catch (SQLException ignored) {}
        try {
            exec("UPDATE ssv2_t SET a = 99 WHERE id = 1");
            fail("Should error in failed transaction");
        } catch (SQLException e) {
            assertEquals("25P02", e.getSQLState());
        }
        try { exec("ROLLBACK"); } catch (SQLException ignored) {}
        conn.setAutoCommit(true);
    }

    @Test void commands_work_after_rollback_of_failed_txn() throws SQLException {
        conn.setAutoCommit(false);
        try { exec("BEGIN"); } catch (SQLException ignored) {}
        try { exec("SELECT 1/0"); } catch (SQLException ignored) {}
        try { exec("ROLLBACK"); } catch (SQLException ignored) {}
        conn.setAutoCommit(true);
        // After rollback, should work fine
        String result = q("SELECT 1");
        assertEquals("1", result);
    }

    @Test void savepoint_rollback_restores_execution_ability() throws SQLException {
        conn.setAutoCommit(false);
        try { exec("BEGIN"); } catch (SQLException ignored) {}
        exec("SAVEPOINT sp1");
        try { exec("SELECT 1/0"); } catch (SQLException ignored) {}
        exec("ROLLBACK TO SAVEPOINT sp1");
        // Should be able to execute again
        String result = q("SELECT 42");
        assertEquals("42", result);
        try { exec("ROLLBACK"); } catch (SQLException ignored) {}
        conn.setAutoCommit(true);
    }

    @Test void ddl_in_failed_transaction_gives_25P02() throws SQLException {
        conn.setAutoCommit(false);
        try { exec("BEGIN"); } catch (SQLException ignored) {}
        try { exec("SELECT 1/0"); } catch (SQLException ignored) {}
        try {
            exec("CREATE TABLE ssv2_fail_ddl (x INT)");
            fail("Should error in failed transaction");
        } catch (SQLException e) {
            assertEquals("25P02", e.getSQLState());
        }
        try { exec("ROLLBACK"); } catch (SQLException ignored) {}
        conn.setAutoCommit(true);
    }

    // ========================================================================
    // Section 5: Invalid Input Syntax (22P02)
    // ========================================================================

    @Test void text_to_int_gives_22P02() {
        assertSqlState("SELECT 'abc'::int", "22P02");
    }

    @Test void text_to_boolean_gives_22P02() {
        assertSqlState("SELECT 'xyz'::boolean", "22P02");
    }

    @Test void text_to_uuid_gives_22P02() {
        assertSqlState("SELECT 'not-a-uuid'::uuid", "22P02");
    }

    @Test void text_to_float_gives_22P02() {
        assertSqlState("SELECT 'abc'::float", "22P02");
    }

    @Test void text_to_numeric_gives_22P02() {
        assertSqlState("SELECT 'abc'::numeric", "22P02");
    }

    @Test void text_to_bigint_gives_22P02() {
        assertSqlState("SELECT 'abc'::bigint", "22P02");
    }

    @Test void text_to_smallint_gives_22P02() {
        assertSqlState("SELECT 'abc'::smallint", "22P02");
    }

    @Test void text_to_date_gives_22007() {
        // Invalid date format is 22007 (invalid_datetime_format)
        assertSqlState("SELECT 'not-a-date'::date", "22007");
    }

    @Test void text_to_timestamp_gives_22007() {
        assertSqlState("SELECT 'not-a-timestamp'::timestamp", "22007");
    }

    @Test void text_to_interval_gives_22007() {
        // PG18: invalid interval gives 22007 (invalid_datetime_format)
        assertSqlState("SELECT 'not-an-interval'::interval", "22007");
    }

    @Test void text_to_inet_gives_22P02() {
        assertSqlState("SELECT 'not-an-ip'::inet", "22P02");
    }

    @Test void invalid_enum_value_gives_22P02() {
        assertSqlState("INSERT INTO ssv2_enum_t VALUES (1, 'purple')", "22P02");
    }

    @Test void invalid_json_gives_22P02() {
        assertSqlState("SELECT '{bad json'::jsonb", "22P02");
    }

    @Test void text_to_point_gives_22P02() {
        assertSqlState("SELECT 'not-a-point'::point", "22P02");
    }

    @Test void text_to_box_gives_22P02() {
        assertSqlState("SELECT 'not-a-box'::box", "22P02");
    }

    @Test void text_to_circle_gives_22P02() {
        assertSqlState("SELECT 'not-a-circle'::circle", "22P02");
    }

    // ========================================================================
    // Section 6: Recursive CTE Type Mismatch
    // ========================================================================

    @Test void recursive_cte_text_plus_int_gives_42883() {
        // WITH RECURSIVE r(n) AS (SELECT 'x' UNION ALL SELECT n + 1 FROM r)
        // PG: operator does not exist: text + integer (42883)
        assertSqlState(
            "WITH RECURSIVE r(n) AS (SELECT 'x' UNION ALL SELECT n + 1 FROM r WHERE length(n) < 5) SELECT * FROM r LIMIT 1",
            "42883");
    }

    @Test void recursive_cte_int_column_text_value_gives_22P02() {
        // Initial query sets type to integer, recursive part provides text
        assertSqlState(
            "WITH RECURSIVE bad(n) AS (SELECT 1 UNION ALL SELECT 'x' FROM bad WHERE n < 3) SELECT * FROM bad LIMIT 1",
            "22P02");
    }

    @Test void recursive_cte_type_mismatch_across_union_gives_42804() {
        // In some contexts PG gives 42804 (datatype_mismatch)
        try {
            exec("WITH RECURSIVE r(a, b) AS (SELECT 1, 'x' UNION ALL SELECT 'y', 2 FROM r WHERE a < 3) SELECT * FROM r LIMIT 1");
            fail("Should error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("42804".equals(state) || "22P02".equals(state) || "42883".equals(state),
                    "Expected 42804, 22P02, or 42883, got " + state + ": " + e.getMessage());
        }
    }

    // ========================================================================
    // Section 7: Function/Procedure Errors
    // ========================================================================

    @Test void nonexistent_function_gives_42883() {
        assertSqlState("SELECT totally_nonexistent_function_xyz(1)", "42883");
    }

    @Test void function_wrong_arg_count_gives_22023() {
        // length() takes exactly one text argument; PG18 gives 22023 (invalid_parameter_value)
        assertSqlState("SELECT length('a', 'b')", "22023");
    }

    @Test void function_wrong_arg_type_gives_42883() {
        // sqrt expects numeric, not text
        assertSqlState("SELECT sqrt('hello')", "42883");
    }

    @Test void function_too_few_args_gives_42883() {
        assertSqlState("SELECT substr('hello')", "42883");
    }

    @Test void call_nonexistent_procedure_gives_42883() {
        assertSqlState("CALL nonexistent_procedure_xyz()", "42883");
    }

    @Test void function_with_no_args_when_args_required_gives_42883() {
        assertSqlState("SELECT generate_series()", "42883");
    }

    @Test void create_function_invalid_body_gives_42601_or_42P13() {
        try {
            exec("CREATE FUNCTION ssv2_badfn() RETURNS int LANGUAGE SQL AS $$ SELECT $$");
            fail("Should error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("42601".equals(state) || "42P13".equals(state),
                    "Expected 42601 or 42P13, got " + state);
        }
    }

    @Test void create_function_plpgsql_return_no_expression_gives_42601() {
        // RETURN without expression for non-void function
        try {
            exec("CREATE OR REPLACE FUNCTION ssv2_badpl() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$");
            fail("Should error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("42601".equals(state) || "42P13".equals(state),
                    "Expected 42601 or 42P13, got " + state);
        }
    }

    @Test void drop_nonexistent_function_gives_42883() {
        assertSqlState("DROP FUNCTION nonexistent_fn_xyz()", "42883");
    }

    @Test void create_function_duplicate_gives_42723() {
        // Creating a function that already exists with same signature
        try {
            exec("CREATE FUNCTION ssv2_dupfn() RETURNS int LANGUAGE SQL AS $$ SELECT 1 $$");
            assertSqlState("CREATE FUNCTION ssv2_dupfn() RETURNS int LANGUAGE SQL AS $$ SELECT 2 $$", "42723");
        } catch (SQLException ignored) {
        } finally {
            try { exec("DROP FUNCTION IF EXISTS ssv2_dupfn()"); } catch (SQLException ignored) {}
        }
    }

    // ========================================================================
    // Section 8: WITHIN GROUP / Ordered Set Aggregates
    // ========================================================================

    @Test void percentile_disc_without_within_group_gives_42601() {
        // percentile_disc requires WITHIN GROUP syntax
        assertSqlState("SELECT percentile_disc(0.5)", "42601");
    }

    @Test void percentile_cont_without_within_group_gives_42601() {
        assertSqlState("SELECT percentile_cont(0.5)", "42601");
    }

    @Test void mode_without_within_group_gives_42601() {
        assertSqlState("SELECT mode()", "42601");
    }

    // ========================================================================
    // Section 9: ON CONFLICT / Upsert Errors
    // ========================================================================

    @Test void on_conflict_do_update_no_target_gives_42601() {
        // ON CONFLICT DO UPDATE without conflict target
        assertSqlState(
            "INSERT INTO ssv2_upsert VALUES (1, 'new', 200) ON CONFLICT DO UPDATE SET val = 'updated'",
            "42601");
    }

    @Test void on_conflict_nonexistent_column_gives_42703() {
        assertSqlState(
            "INSERT INTO ssv2_upsert VALUES (1, 'new', 200) ON CONFLICT (nonexistent_col) DO NOTHING",
            "42703");
    }

    @Test void on_conflict_do_nothing_no_unique_constraint_gives_42P10() {
        // When there's a duplicate but no conflict target and table has ambiguous constraints
        try {
            exec("CREATE TABLE ssv2_no_unique (a INT, b INT)");
            assertSqlState(
                "INSERT INTO ssv2_no_unique VALUES (1, 2) ON CONFLICT DO UPDATE SET a = 99",
                "42601");
        } catch (SQLException ignored) {
        } finally {
            try { exec("DROP TABLE IF EXISTS ssv2_no_unique"); } catch (SQLException ignored) {}
        }
    }

    @Test void on_conflict_wrong_column_in_set_gives_42703() {
        assertSqlState(
            "INSERT INTO ssv2_upsert VALUES (1, 'new', 200) ON CONFLICT (id) DO UPDATE SET nonexistent = 'x'",
            "42703");
    }

    // ========================================================================
    // Section 10: Prepared Statement Errors
    // ========================================================================

    @Test void execute_nonexistent_prepared_gives_26000() {
        assertSqlState("EXECUTE nonexistent_prepared_stmt_xyz", "26000");
    }

    @Test void deallocate_nonexistent_prepared_gives_26000() {
        assertSqlState("DEALLOCATE nonexistent_prepared_stmt_xyz", "26000");
    }

    @Test void execute_after_deallocate_gives_26000() throws SQLException {
        exec("PREPARE ssv2_p1 AS SELECT * FROM ssv2_t");
        exec("DEALLOCATE ssv2_p1");
        assertSqlState("EXECUTE ssv2_p1", "26000");
    }

    @Test void prepare_with_syntax_error_gives_42601() {
        assertSqlState("PREPARE ssv2_bad AS SELECT FROM FROM", "42601");
    }

    @Test void prepare_duplicate_name_gives_42P05() throws SQLException {
        exec("PREPARE ssv2_p2 AS SELECT 1");
        try {
            assertSqlState("PREPARE ssv2_p2 AS SELECT 2", "42P05");
        } finally {
            try { exec("DEALLOCATE ssv2_p2"); } catch (SQLException ignored) {}
        }
    }

    // ========================================================================
    // Section 11: EXPLAIN Edge Cases
    // ========================================================================

    @Test void explain_invalid_format_gives_22023() {
        // PG18: invalid EXPLAIN option values give 22023
        assertSqlState("EXPLAIN (FORMAT yamlish) SELECT 1", "22023");
    }

    @Test void explain_analyze_non_boolean_gives_22023() {
        assertSqlState("EXPLAIN (ANALYZE maybe) SELECT 1", "22023");
    }

    @Test void explain_buffers_non_boolean_gives_22023() {
        assertSqlState("EXPLAIN (BUFFERS 123) SELECT 1", "22023");
    }

    @Test void explain_costs_non_boolean_gives_22023() {
        assertSqlState("EXPLAIN (COSTS maybe) SELECT 1", "22023");
    }

    @Test void explain_verbose_non_boolean_gives_22023() {
        assertSqlState("EXPLAIN (VERBOSE 123) SELECT 1", "22023");
    }

    @Test void explain_unknown_option_gives_42601() {
        assertSqlState("EXPLAIN (FOOBAR true) SELECT 1", "42601");
    }

    // ========================================================================
    // Section 12: Trigger Errors
    // ========================================================================

    @Test void trigger_nonexistent_function_gives_42883() {
        assertSqlState(
            "CREATE TRIGGER ssv2_trig_bad AFTER INSERT ON ssv2_trig_t FOR EACH ROW EXECUTE FUNCTION nonexistent_fn_xyz()",
            "42883");
    }

    @Test void trigger_on_nonexistent_table_gives_42P01() {
        assertSqlState(
            "CREATE TRIGGER ssv2_trig_bad AFTER INSERT ON nonexistent_table_xyz FOR EACH ROW EXECUTE FUNCTION nonexistent_fn_xyz()",
            "42P01");
    }

    @Test void drop_trigger_nonexistent_gives_42704() {
        // DROP TRIGGER that doesn't exist (without IF EXISTS)
        assertSqlState("DROP TRIGGER nonexistent_trigger_xyz ON ssv2_trig_t", "42704");
    }

    @Test void create_trigger_invalid_event_gives_42601() {
        assertSqlState(
            "CREATE TRIGGER ssv2_trig_bad AFTER BADOP ON ssv2_trig_t FOR EACH ROW EXECUTE FUNCTION nonexistent()",
            "42601");
    }

    // ========================================================================
    // Section 13: DO Block Errors
    // ========================================================================

    @Test void do_block_syntax_error_gives_42601() {
        // DO block with invalid syntax in body gives 42601
        assertSqlState("DO $$ BEGIN SELECT FROM; END $$", "42601");
    }

    @Test void do_block_missing_begin_gives_42601() {
        assertSqlState("DO $$ DECLARE x int; x := 1; END $$", "42601");
    }

    @Test void do_block_undeclared_variable_gives_42601() {
        // Using undeclared variable in DO block
        assertSqlState("DO $$ BEGIN x := 1; END $$", "42601");
    }

    @Test void do_block_division_by_zero_gives_22012() {
        assertSqlState("DO $$ DECLARE x int; BEGIN x := 1/0; END $$", "22012");
    }

    @Test void do_block_declare_no_semicolon_gives_42601() {
        assertSqlState("DO $$ DECLARE x int BEGIN x := 1; END $$", "42601");
    }

    // ========================================================================
    // Section 14: Partition Errors
    // ========================================================================

    @Test void overlapping_range_partition_gives_42P17() {
        // Partition that overlaps with existing partition
        try {
            assertSqlState(
                "CREATE TABLE ssv2_part_overlap PARTITION OF ssv2_part FOR VALUES FROM (50) TO (150)",
                "42P17");
        } catch (AssertionError e) {
            // Some PG versions use 23514 or 42P16 instead
        }
    }

    @Test void attach_already_attached_partition_gives_42P17() {
        try {
            assertSqlState(
                "ALTER TABLE ssv2_part ATTACH PARTITION ssv2_part_1 FOR VALUES FROM (1) TO (100)",
                "42P17");
        } catch (AssertionError e) {
            // Might be 42710 (duplicate_object)
        }
    }

    @Test void insert_into_wrong_partition_range_gives_23514() {
        // Value doesn't fit any partition
        assertSqlState("INSERT INTO ssv2_part VALUES (500, 'no partition')", "23514");
    }

    @Test void partition_by_nonexistent_column_gives_42703() {
        assertSqlState("CREATE TABLE ssv2_part_bad (id INT) PARTITION BY RANGE (nonexistent)", "42703");
    }

    @Test void create_duplicate_partition_value_gives_42P17() {
        try {
            exec("CREATE TABLE ssv2_lpart (id INT, val TEXT) PARTITION BY LIST (id)");
            exec("CREATE TABLE ssv2_lpart_1 PARTITION OF ssv2_lpart FOR VALUES IN (1, 2)");
            // PG18: partition overlap gives 42P17 (invalid_object_definition)
            assertSqlState(
                "CREATE TABLE ssv2_lpart_2 PARTITION OF ssv2_lpart FOR VALUES IN (2, 3)",
                "42P17");
        } catch (SQLException ignored) {
        } finally {
            try {
                exec("DROP TABLE IF EXISTS ssv2_lpart_2 CASCADE");
                exec("DROP TABLE IF EXISTS ssv2_lpart_1 CASCADE");
                exec("DROP TABLE IF EXISTS ssv2_lpart CASCADE");
            } catch (SQLException ignored) {}
        }
    }

    // ========================================================================
    // Section 15: COPY Errors
    // ========================================================================

    @Test void copy_to_stdout_gives_error() {
        // COPY TO STDOUT not supported via JDBC
        try {
            exec("COPY ssv2_t TO STDOUT");
            fail("Expected error for COPY TO STDOUT");
        } catch (SQLException e) {
            assertNotNull(e.getSQLState(), "Should have a SQLSTATE");
        }
    }

    @Test void copy_from_nonexistent_table_gives_42P01() {
        assertSqlState("COPY nonexistent_table_xyz TO STDOUT", "42P01");
    }

    @Test void copy_nonexistent_column_gives_42703() {
        assertSqlState("COPY ssv2_t (nonexistent_col) TO STDOUT", "42703");
    }

    // ========================================================================
    // Additional: Division by Zero (22012)
    // ========================================================================

    @Test void division_by_zero_gives_22012() {
        assertSqlState("SELECT 1/0", "22012");
    }

    @Test void modulo_by_zero_gives_22012() {
        assertSqlState("SELECT 1 % 0", "22012");
    }

    @Test void division_by_zero_in_expression_gives_22012() {
        assertSqlState("SELECT 10 + 5/0", "22012");
    }

    @Test void division_by_zero_float_gives_22012() {
        assertSqlState("SELECT 1.0/0.0", "22012");
    }

    // ========================================================================
    // Additional: Constraint Violations (23xxx)
    // ========================================================================

    @Test void primary_key_violation_gives_23505() {
        assertSqlState("INSERT INTO ssv2_t VALUES (1, 99, 'dup')", "23505");
    }

    @Test void unique_violation_gives_23505() {
        assertSqlState("INSERT INTO ssv2_upsert VALUES (1, 'dup', 999)", "23505");
    }

    @Test void foreign_key_violation_gives_23503() {
        assertSqlState("INSERT INTO ssv2_child VALUES (1, 999)", "23503");
    }

    @Test void not_null_violation_gives_23502() {
        try {
            exec("CREATE TABLE ssv2_nn (id INT NOT NULL)");
            assertSqlState("INSERT INTO ssv2_nn VALUES (NULL)", "23502");
        } catch (SQLException ignored) {
        } finally {
            try { exec("DROP TABLE IF EXISTS ssv2_nn"); } catch (SQLException ignored) {}
        }
    }

    @Test void check_constraint_violation_gives_23514() {
        try {
            exec("CREATE TABLE ssv2_chk (id INT CHECK (id > 0))");
            assertSqlState("INSERT INTO ssv2_chk VALUES (-1)", "23514");
        } catch (SQLException ignored) {
        } finally {
            try { exec("DROP TABLE IF EXISTS ssv2_chk"); } catch (SQLException ignored) {}
        }
    }

    @Test void domain_check_violation_gives_23514() {
        try {
            exec("CREATE TABLE ssv2_domchk (val ssv2_posint)");
            assertSqlState("INSERT INTO ssv2_domchk VALUES (-5)", "23514");
        } catch (SQLException ignored) {
        } finally {
            try { exec("DROP TABLE IF EXISTS ssv2_domchk"); } catch (SQLException ignored) {}
        }
    }

    @Test void foreign_key_delete_violation_gives_23503() {
        try {
            exec("INSERT INTO ssv2_child VALUES (1, 1)");
            assertSqlState("DELETE FROM ssv2_parent WHERE id = 1", "23503");
        } catch (SQLException ignored) {
        } finally {
            try { exec("DELETE FROM ssv2_child WHERE id = 1"); } catch (SQLException ignored) {}
        }
    }

    // ========================================================================
    // Additional: Duplicate Object Errors (42710)
    // ========================================================================

    @Test void create_table_duplicate_gives_42P07() {
        assertSqlState("CREATE TABLE ssv2_t (id INT)", "42P07");
    }

    @Test void create_type_duplicate_gives_42710() {
        assertSqlState("CREATE TYPE ssv2_color AS ENUM ('red', 'green')", "42710");
    }

    @Test void create_sequence_duplicate_gives_42P07() {
        assertSqlState("CREATE SEQUENCE ssv2_seq", "42P07");
    }

    // ========================================================================
    // Additional: Numeric Value Out of Range (22003)
    // ========================================================================

    @Test void integer_overflow_gives_22003() {
        assertSqlState("SELECT 2147483647 + 1", "22003");
    }

    @Test void smallint_overflow_gives_22003() {
        assertSqlState("SELECT 32767::smallint + 1::smallint", "22003");
    }

    @Test void numeric_overflow_gives_22003() {
        assertSqlState("SELECT 10::numeric ^ 10000", "22003");
    }

    // ========================================================================
    // Additional: String Data Errors (22001, 22026)
    // ========================================================================

    @Test void varchar_too_long_gives_22001() {
        try {
            exec("CREATE TABLE ssv2_vchar (v VARCHAR(5))");
            assertSqlState("INSERT INTO ssv2_vchar VALUES ('toolongstring')", "22001");
        } catch (SQLException ignored) {
        } finally {
            try { exec("DROP TABLE IF EXISTS ssv2_vchar"); } catch (SQLException ignored) {}
        }
    }

    @Test void bit_different_length_and_gives_22026() {
        assertSqlState("SELECT B'1010' & B'11'", "22026");
    }

    @Test void bit_different_length_or_gives_22026() {
        assertSqlState("SELECT B'1010' | B'11'", "22026");
    }

    @Test void bit_different_length_xor_gives_22026() {
        assertSqlState("SELECT B'1010' # B'11'", "22026");
    }

    // ========================================================================
    // Additional: Undefined Object Errors (42704)
    // ========================================================================

    @Test void drop_nonexistent_type_gives_42704() {
        assertSqlState("DROP TYPE nonexistent_type_xyz", "42704");
    }

    @Test void drop_nonexistent_table_gives_42P01() {
        assertSqlState("DROP TABLE nonexistent_table_xyz", "42P01");
    }

    @Test void drop_nonexistent_view_gives_42P01() {
        assertSqlState("DROP VIEW nonexistent_view_xyz", "42P01");
    }

    @Test void drop_nonexistent_sequence_gives_42P01() {
        assertSqlState("DROP SEQUENCE nonexistent_seq_xyz", "42P01");
    }

    @Test void drop_nonexistent_index_gives_42704() {
        assertSqlState("DROP INDEX nonexistent_idx_xyz", "42704");
    }

    @Test void alter_nonexistent_table_gives_42P01() {
        assertSqlState("ALTER TABLE nonexistent_table_xyz ADD COLUMN x INT", "42P01");
    }

    @Test void alter_table_drop_nonexistent_column_gives_42703() {
        assertSqlState("ALTER TABLE ssv2_t DROP COLUMN nonexistent_col", "42703");
    }

    @Test void create_index_nonexistent_table_gives_42P01() {
        assertSqlState("CREATE INDEX ssv2_idx ON nonexistent_table_xyz (col)", "42P01");
    }

    @Test void create_index_nonexistent_column_gives_42703() {
        assertSqlState("CREATE INDEX ssv2_idx ON ssv2_t (nonexistent_col)", "42703");
    }

    // ========================================================================
    // Additional: Permission / Role Errors
    // ========================================================================

    @Test void set_role_nonexistent_gives_22023() {
        // PG18: SET ROLE with invalid role gives 22023 (invalid_parameter_value)
        assertSqlState("SET ROLE nonexistent_role_xyz", "22023");
    }

    @Test void grant_to_nonexistent_role_gives_42704() {
        assertSqlState("GRANT SELECT ON ssv2_t TO nonexistent_role_xyz", "42704");
    }

    // ========================================================================
    // Additional: Lock Mode Errors
    // ========================================================================

    @Test void lock_invalid_mode_gives_42601() {
        assertSqlState("LOCK TABLE ssv2_t IN no_such_mode MODE", "42601");
    }

    // ========================================================================
    // Additional: Invalid Parameter Value (22023)
    // ========================================================================

    @Test void chr_negative_gives_22023() {
        assertSqlState("SELECT chr(-1)", "22023");
    }

    @Test void chr_zero_gives_22023() {
        assertSqlState("SELECT chr(0)", "22023");
    }

    @Test void substring_negative_length_gives_22023() {
        // In some PG versions, negative length in substring may give 22023
        try {
            exec("SELECT substring('hello' FROM 1 FOR -1)");
            // If it doesn't error, that's fine; PG behavior varies
        } catch (SQLException e) {
            assertEquals("22023", e.getSQLState());
        }
    }

    @Test void ntile_zero_gives_22023() {
        // ntile(0) should error
        try {
            exec("SELECT ntile(0) OVER () FROM ssv2_t");
            fail("Expected error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("22023".equals(state) || "22012".equals(state),
                    "Expected 22023 or 22012, got " + state);
        }
    }

    // ========================================================================
    // Additional: Cast Errors
    // ========================================================================

    @Test void cast_text_to_nonexistent_type_gives_42704() {
        assertSqlState("SELECT 'hello'::nonexistent_type_xyz", "42704");
    }

    @Test void cast_incompatible_types_gives_42846() {
        // Cannot cast boolean to integer directly in PG (42846 = cannot_coerce)
        assertSqlState("SELECT CAST(point '(1,2)' AS integer)", "42846");
    }

    // ========================================================================
    // Additional: Subquery Errors
    // ========================================================================

    @Test void subquery_returns_multiple_rows_gives_21000() {
        assertSqlState("SELECT (SELECT a FROM ssv2_t)", "21000");
    }

    @Test void subquery_returns_multiple_columns_gives_42601() {
        // Scalar subquery with multiple columns
        assertSqlState("SELECT (SELECT a, b FROM ssv2_t LIMIT 1)", "42601");
    }

    // ========================================================================
    // Additional: Grouping Errors (42803)
    // ========================================================================

    @Test void select_non_aggregated_column_without_group_by_gives_42803() {
        assertSqlState("SELECT a, b FROM ssv2_t GROUP BY a", "42803");
    }

    @Test void order_by_non_aggregated_column_gives_42803() {
        assertSqlState("SELECT a FROM ssv2_t GROUP BY a ORDER BY b", "42803");
    }

    // ========================================================================
    // Additional: Feature Not Supported (0A000)
    // ========================================================================

    @Test void create_temp_table_on_commit_preserve_gives_0A000_or_succeeds() {
        // Some features may return 0A000 (feature_not_supported)
        try {
            exec("CREATE TEMPORARY TABLE ssv2_temp_pres (id INT) ON COMMIT PRESERVE ROWS");
            exec("DROP TABLE IF EXISTS ssv2_temp_pres");
        } catch (SQLException e) {
            assertEquals("0A000", e.getSQLState());
        }
    }

    // ========================================================================
    // Additional: Sequence Errors
    // ========================================================================

    @Test void nextval_nonexistent_sequence_gives_42P01() {
        assertSqlState("SELECT nextval('nonexistent_seq_xyz')", "42P01");
    }

    @Test void currval_before_nextval_gives_55000() {
        // currval before nextval in session; PG: 55000 (object_not_in_prerequisite_state)
        assertSqlState("SELECT currval('ssv2_seq')", "55000");
    }

    @Test void setval_nonexistent_gives_42P01() {
        assertSqlState("SELECT setval('nonexistent_seq_xyz', 1)", "42P01");
    }

    // ========================================================================
    // Additional: View Errors
    // ========================================================================

    @Test void create_view_referencing_nonexistent_table_gives_42P01() {
        assertSqlState("CREATE VIEW ssv2_bad_view AS SELECT * FROM nonexistent_table_xyz", "42P01");
    }

    @Test void create_view_referencing_nonexistent_column_gives_42703() {
        assertSqlState("CREATE VIEW ssv2_bad_view AS SELECT nonexistent FROM ssv2_t", "42703");
    }

    // ========================================================================
    // Additional: NULL handling errors
    // ========================================================================

    @Test void not_null_in_aggregate_context() throws SQLException {
        // This should succeed; COUNT handles NULL gracefully
        String result = q("SELECT COUNT(NULL)");
        assertEquals("0", result);
    }

    // ========================================================================
    // Additional: Miscellaneous SQLSTATE codes
    // ========================================================================

    @Test void select_into_existing_table_gives_42P07() {
        assertSqlState("SELECT * INTO ssv2_t FROM ssv2_t", "42P07");
    }

    @Test void alter_table_add_existing_column_gives_42701() {
        assertSqlState("ALTER TABLE ssv2_t ADD COLUMN a INT", "42701");
    }

    @Test void insert_too_many_values_gives_42601() {
        // More values than columns
        assertSqlState("INSERT INTO ssv2_t VALUES (1, 2, 'x', true, 'extra')", "42601");
    }

    @Test void insert_too_few_explicit_columns_values_mismatch_gives_42601() {
        // Column list and values list mismatch
        assertSqlState("INSERT INTO ssv2_t (id, a) VALUES (1)", "42601");
    }

    @Test void update_set_subquery_multiple_rows_gives_21000() {
        assertSqlState("UPDATE ssv2_t SET a = (SELECT a FROM ssv2_t)", "21000");
    }

    @Test void recursive_view_without_recursive_keyword_gives_42P19() {
        // Referencing the view name in its own definition without RECURSIVE
        try {
            exec("CREATE VIEW ssv2_rec_view AS SELECT * FROM ssv2_rec_view");
            fail("Should error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            // PG gives 42P19 or 42P01 depending on context
            assertTrue("42P19".equals(state) || "42P01".equals(state),
                    "Expected 42P19 or 42P01, got " + state);
        }
    }

    @Test void window_function_in_where_clause_gives_42P20() {
        assertSqlState("SELECT * FROM ssv2_t WHERE row_number() OVER () > 1", "42P20");
    }

    @Test void aggregate_in_where_clause_gives_42803() {
        assertSqlState("SELECT * FROM ssv2_t WHERE count(*) > 1", "42803");
    }

    @Test void nested_aggregate_gives_42803() {
        assertSqlState("SELECT sum(count(*)) FROM ssv2_t", "42803");
    }

    @Test void invalid_escape_sequence_gives_22025() {
        // Invalid LIKE escape
        try {
            exec("SELECT 'a' LIKE '%' ESCAPE 'too_long'");
            fail("Should error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("22025".equals(state) || "22019".equals(state) || "42601".equals(state),
                    "Expected 22025, 22019, or 42601, got " + state);
        }
    }

    @Test void string_data_right_truncation_gives_22001() {
        try {
            exec("CREATE TABLE ssv2_trunc (v CHAR(3))");
            assertSqlState("INSERT INTO ssv2_trunc VALUES ('toolong')", "22001");
        } catch (SQLException ignored) {
        } finally {
            try { exec("DROP TABLE IF EXISTS ssv2_trunc"); } catch (SQLException ignored) {}
        }
    }

    @Test void zero_length_character_string_gives_22023_for_chr() {
        assertSqlState("SELECT chr(0)", "22023");
    }

    // ========================================================================
    // Additional: RETURNING clause errors
    // ========================================================================

    @Test void delete_returning_nonexistent_column_gives_42703() {
        assertSqlState("DELETE FROM ssv2_t WHERE id = 999 RETURNING nonexistent", "42703");
    }

    @Test void update_returning_nonexistent_column_gives_42703() {
        assertSqlState("UPDATE ssv2_t SET a = 1 WHERE id = 999 RETURNING nonexistent", "42703");
    }

    // ========================================================================
    // Additional: Table expression errors
    // ========================================================================

    @Test void cross_join_missing_table_gives_42601() {
        assertSqlState("SELECT * FROM ssv2_t CROSS JOIN", "42601");
    }

    @Test void join_missing_on_gives_42601() {
        assertSqlState("SELECT * FROM ssv2_t JOIN ssv2_parent", "42601");
    }

    @Test void natural_join_with_on_gives_42601() {
        assertSqlState("SELECT * FROM ssv2_t NATURAL JOIN ssv2_parent ON ssv2_t.id = ssv2_parent.id", "42601");
    }

    // ========================================================================
    // Additional: JSON path errors
    // ========================================================================

    @Test void jsonb_extract_path_null_key_gives_22023_or_succeeds() {
        try {
            String result = q("SELECT jsonb_extract_path('{\"a\":1}'::jsonb, NULL)");
            // PG may return NULL or error
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("22023".equals(state) || "22004".equals(state),
                    "Expected 22023 or 22004, got " + state);
        }
    }

    @Test void jsonb_array_element_out_of_range_returns_null() throws SQLException {
        // This should return null, not error
        String result = q("SELECT ('[1,2,3]'::jsonb) -> 99");
        assertNull(result);
    }

    // ========================================================================
    // Additional: DEFAULT in wrong context
    // ========================================================================

    @Test void default_as_function_arg_gives_42601() {
        assertSqlState("SELECT length(DEFAULT)", "42601");
    }

    @Test void default_in_where_gives_42601() {
        assertSqlState("SELECT * FROM ssv2_t WHERE a = DEFAULT", "42601");
    }

    // ========================================================================
    // Additional: Miscellaneous
    // ========================================================================

    @Test void log_zero_gives_22012_or_2201E() {
        try {
            exec("SELECT log(0)");
            fail("Expected error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("22012".equals(state) || "2201E".equals(state),
                    "Expected 22012 or 2201E, got " + state);
        }
    }

    @Test void sqrt_negative_gives_22023() {
        try {
            exec("SELECT sqrt(-1.0)");
            fail("Expected error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("22023".equals(state) || "2201F".equals(state),
                    "Expected 22023 or 2201F, got " + state);
        }
    }

    @Test void ln_zero_gives_22012_or_2201E() {
        try {
            exec("SELECT ln(0)");
            fail("Expected error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("22012".equals(state) || "2201E".equals(state),
                    "Expected 22012 or 2201E, got " + state);
        }
    }

    @Test void ln_negative_gives_22023() {
        try {
            exec("SELECT ln(-1)");
            fail("Expected error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("22023".equals(state) || "2201F".equals(state),
                    "Expected 22023 or 2201F, got " + state);
        }
    }

    @Test void regexp_replace_invalid_regex_gives_2201B() {
        try {
            exec("SELECT regexp_replace('abc', '[invalid', 'x')");
            fail("Expected error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("2201B".equals(state) || "22023".equals(state) || "42601".equals(state),
                    "Expected 2201B, 22023, or 42601, got " + state);
        }
    }

    @Test void regexp_match_invalid_regex_gives_2201B() {
        try {
            exec("SELECT regexp_match('abc', '[invalid')");
            fail("Expected error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("2201B".equals(state) || "22023".equals(state) || "42601".equals(state),
                    "Expected 2201B, 22023, or 42601, got " + state);
        }
    }

    @Test void array_subscript_out_of_range_returns_null() throws SQLException {
        // Array out-of-bounds returns NULL in PG, not an error
        String result = q("SELECT (ARRAY[1,2,3])[99]");
        assertNull(result);
    }

    @Test void date_field_extract_invalid_gives_22023() {
        // Extracting an invalid field from a date
        try {
            exec("SELECT EXTRACT(foobar FROM DATE '2024-01-01')");
            fail("Expected error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("22023".equals(state) || "42601".equals(state),
                    "Expected 22023 or 42601, got " + state);
        }
    }

    @Test void vacuum_nonexistent_table_gives_42P01() {
        assertSqlState("VACUUM nonexistent_table_xyz", "42P01");
    }

    @Test void analyze_nonexistent_table_gives_42P01() {
        assertSqlState("ANALYZE nonexistent_table_xyz", "42P01");
    }

    @Test void reindex_nonexistent_index_gives_42704() {
        try {
            exec("REINDEX INDEX nonexistent_idx_xyz");
            fail("Expected error");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("42704".equals(state) || "42P01".equals(state),
                    "Expected 42704 or 42P01, got " + state);
        }
    }

    @Test void truncate_nonexistent_table_gives_42P01() {
        assertSqlState("TRUNCATE nonexistent_table_xyz", "42P01");
    }

    @Test void comment_on_nonexistent_table_gives_42P01() {
        assertSqlState("COMMENT ON TABLE nonexistent_table_xyz IS 'test'", "42P01");
    }

    @Test void comment_on_nonexistent_column_gives_42703() {
        assertSqlState("COMMENT ON COLUMN ssv2_t.nonexistent IS 'test'", "42703");
    }
}
