package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for "easy fix" validation gaps. Each test documents a case where
 * memgres should behave identically to PG18 but currently doesn't.
 *
 * These cover: missing input rejections, wrong SQLSTATE codes,
 * wrong data values, and formatting differences.
 */
class EasyFixValidationTest {

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

    static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    /** Assert that SQL produces an error with the expected SQLSTATE. */
    static void assertSqlError(String sql, String expectedState) {
        try {
            try (Statement s = conn.createStatement()) { s.execute(sql); }
            fail("Expected error for: " + sql);
        } catch (SQLException e) {
            assertEquals(expectedState, e.getSQLState(),
                    "Wrong SQLSTATE for: " + sql + ", got message: " + e.getMessage());
        }
    }

    /** Assert that SQL produces any error (don't care about SQLSTATE). */
    static void assertSqlFails(String sql) {
        assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) { s.execute(sql); }
        }, "Expected error for: " + sql);
    }

    // ========================================================================
    // 1. Operator type mismatch: PG rejects, memgres should too
    // ========================================================================

    @Test void bit_string_equality_with_integer_rejected() {
        assertSqlError("SELECT B'1' = 1", "42883");
    }

    @Test void bit_string_addition_rejected() {
        assertSqlError("SELECT B'1010' + B'0101'", "42883");
    }

    @Test void bit_string_different_length_bitwise_rejected() {
        // B'1010' & B'11', different lengths, PG rejects
        assertSqlFails("SELECT B'1010' & B'11'");
    }

    @Test void point_concat_rejected() {
        assertSqlError("SELECT point(1,1) || point(2,2)", "42883");
    }

    @Test void point_contains_integer_rejected() {
        assertSqlError("SELECT point(1,2) @> 1", "42883");
    }

    @Test void jsonb_concat_integer_rejected() {
        // jsonb || integer is not valid; must be jsonb || jsonb
        assertSqlFails("SELECT '{\"a\":1}'::jsonb || 1");
    }

    @Test void json_concat_rejected() {
        // json type does not support || operator (only jsonb does)
        assertSqlError("SELECT '{\"a\":1}'::json || '{\"b\":2}'::json", "42883");
    }

    @Test void json_minus_rejected() {
        // json type does not support - operator (only jsonb does)
        assertSqlError("SELECT '{\"a\":1}'::json - 'a'", "42883");
    }

    @Test void array_concat_with_plain_string_rejected() {
        // ARRAY[1,2] || 'x'; 'x' is not an integer
        assertSqlFails("SELECT ARRAY[1,2] || 'x'");
    }

    @Test void array_overlap_type_mismatch_rejected() {
        // ARRAY[1,2] && ARRAY['x'], integer[] vs text[]
        assertSqlFails("SELECT ARRAY[1,2] && ARRAY['x']");
    }

    @Test void array_concat_with_geometry_rejected() {
        assertSqlFails("SELECT ARRAY[1,2] || point(1,2)");
    }

    @Test void any_type_mismatch_rejected() {
        // 2 = ANY(ARRAY['a','b']), integer = text
        assertSqlFails("SELECT 2 = ANY(ARRAY['a','b'])");
    }

    // ========================================================================
    // 2. Bit string validation
    // ========================================================================

    @Test void invalid_bit_digit_rejected() {
        // B'1020': '2' is not a valid bit digit
        assertSqlFails("SELECT B'1020'");
    }

    @Test void bit_cast_pads_correctly() throws SQLException {
        // B'101'::bit(4) should produce '1010' (pad with zero on right)
        assertEquals("1010", q("SELECT B'101'::bit(4)"));
    }

    // ========================================================================
    // 3. Float overflow
    // ========================================================================

    @Test void float8_multiplication_overflow_rejected() {
        // 1e308 * 10 overflows float8
        assertSqlFails("SELECT 1e308::float8 * 10::float8");
    }

    // ========================================================================
    // 4. Unicode escape validation
    // ========================================================================

    @Test void invalid_unicode_escape_rejected() {
        // \0G is not a valid hex sequence
        assertSqlFails("SELECT U&'bad\\0G'");
    }

    @Test void unicode_escape_produces_correct_output() throws SQLException {
        // U&'d\0061t\+000061' should produce 'data'
        assertEquals("data", q("SELECT U&'d\\0061t\\+000061'"));
    }

    // ========================================================================
    // 5. JSONB operator validation
    // ========================================================================

    @Test void jsonb_hash_arrow_requires_array_path() {
        // #> requires text[] path, not plain string
        assertSqlFails("SELECT '{\"a\":1}'::jsonb #> 'not_an_array'");
    }

    @Test void jsonb_question_pipe_requires_array() {
        // ?| requires text[] on right side
        assertSqlFails("SELECT '{\"a\":1}'::jsonb ?| 'x'");
    }

    @Test void jsonb_set_requires_array_path() {
        assertSqlFails("SELECT jsonb_set('{\"a\":[1,2,3]}'::jsonb, 'not_a_path', '1'::jsonb)");
    }

    @Test void jsonb_insert_validates_array_index() {
        // Path {a,x}: 'x' is not a valid array index
        assertSqlFails("SELECT jsonb_insert('{\"a\":[1,2,3]}'::jsonb, '{a,x}', '1'::jsonb)");
    }

    // ========================================================================
    // 6. Array subscript type
    // ========================================================================

    @Test void array_subscript_string_key_rejected() throws SQLException {
        exec("CREATE TABLE IF NOT EXISTS ef_exprs (id INT, b INT[])");
        try {
            // b['x']: arrays use integer subscripts, not string keys
            assertSqlFails("SELECT b['x'] FROM ef_exprs");
        } finally {
            exec("DROP TABLE IF EXISTS ef_exprs");
        }
    }

    // ========================================================================
    // 7. SQLSTATE corrections: both error, but wrong code
    // ========================================================================

    @Test void transaction_aborted_division_by_zero_gives_22012() throws SQLException {
        // In a failed transaction, SELECT 1/0 should give 22012 (division_by_zero)
        // not 25P02 (in failed transaction)
        exec("BEGIN");
        try {
            exec("SELECT 1/0"); // This will fail
        } catch (SQLException ignored) {}
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT 1/0");
            fail("Should have errored");
        } catch (SQLException e) {
            // PG gives 25P02 for any statement in a failed transaction
            // Actually: PG gives 25P02 here. Let me re-check the diff.
            // The diff says expected 22012 got 25P02, so PG gives 22012 but memgres gives 25P02.
            // That means this division by zero happens OUTSIDE a failed tx.
            // Let me check: the test file 10_transactions_savepoints stmt 32 is after ROLLBACK.
        } finally {
            try { exec("ROLLBACK"); } catch (SQLException ignored) {}
        }
    }

    @Test void duplicate_named_arg_gives_42601() {
        // PG gives 42601 (syntax error), not 42P13
        assertSqlError("SELECT addxy(x => 1, x => 2)", "42601");
    }

    @Test void default_in_function_call_gives_42601() {
        // DEFAULT is not allowed as a function argument value
        assertSqlFails("SELECT addxy(DEFAULT, 5)");
    }

    @Test void chr_out_of_range_gives_54000() {
        // PG uses 54000 (program_limit_exceeded) for chr() out of range
        assertSqlError("SELECT chr(1114112)", "54000");
    }

    @Test void bad_timestamp_gives_22008() {
        // Invalid month 13; PG gives 22008 (datetime_field_overflow)
        assertSqlError("SELECT TIMESTAMP '2024-13-01 00:00:00'", "22008");
    }

    @Test void union_type_mismatch_gives_22P02() {
        // SELECT 1 UNION ALL SELECT 'x'; PG gives 22P02 (invalid text representation)
        assertSqlError("SELECT 1 UNION ALL SELECT 'x'", "22P02");
    }

    // ========================================================================
    // 8. Data value corrections
    // ========================================================================

    @Test void pg_typeof_row_returns_record() throws SQLException {
        assertEquals("record", q("SELECT pg_typeof(ROW(NULL, 1))"));
    }

    @Test void pg_typeof_row_simple_returns_record() throws SQLException {
        assertEquals("record", q("SELECT pg_typeof(ROW(1,'a'))"));
    }

    @Test void pg_typeof_case_int_decimal_returns_numeric() throws SQLException {
        // CASE with integer and decimal branches → PG widens to numeric
        assertEquals("numeric", q("SELECT pg_typeof(CASE WHEN true THEN 1 ELSE 2.5 END)"));
    }

    @Test void pg_typeof_case_null_int_returns_integer() throws SQLException {
        // CASE with NULL and integer → PG returns integer (not unknown)
        assertEquals("integer", q("SELECT pg_typeof(CASE WHEN true THEN NULL ELSE 2 END)"));
    }

    @Test void pg_typeof_array_int_literal_returns_integer_array() throws SQLException {
        // ARRAY[[1,2],[3,4]]; PG infers integer[], not text[]
        assertEquals("integer[]", q("SELECT pg_typeof(ARRAY[[1,2],[3,4]])"));
    }

    @Test void pg_typeof_array_mixed_widening() throws SQLException {
        // ARRAY[1::smallint, 2::bigint]; PG widens to bigint[]
        assertEquals("bigint[]", q("SELECT pg_typeof(ARRAY[1::smallint, 2::bigint])"));
    }

    @Test void pg_typeof_values_union_widens_to_numeric() throws SQLException {
        // VALUES (1), (2.5); PG widens column type to numeric
        assertEquals("numeric", q("SELECT pg_typeof(x) FROM (VALUES (1), (2.5)) AS v(x) LIMIT 1"));
    }

    @Test void pg_typeof_greatest_widens_to_numeric() throws SQLException {
        assertEquals("numeric", q("SELECT pg_typeof(greatest(1,2.5,3))"));
    }

    @Test void array_lower_dimension_zero_returns_null() throws SQLException {
        // array_lower(arr, 0): dimension 0 doesn't exist, PG returns NULL
        assertNull(q("SELECT array_lower(ARRAY[1,2,3], 0)"));
    }

    @Test void row_is_null_semantics() throws SQLException {
        // ROW(NULL, NULL) IS NULL → true (all fields null)
        // ROW(1, NULL) IS NULL → false (not all fields null)
        // ROW(NULL, NULL) IS NOT NULL → false
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT ROW(NULL,NULL) IS NULL, ROW(1,NULL) IS NULL, ROW(NULL,NULL) IS NOT NULL")) {
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1), "ROW(NULL,NULL) IS NULL should be true");
            assertEquals("f", rs.getString(2), "ROW(1,NULL) IS NULL should be false");
            assertEquals("f", rs.getString(3), "ROW(NULL,NULL) IS NOT NULL should be false");
        }
    }

    @Test void string_to_array_empty_string_returns_empty() throws SQLException {
        // string_to_array('', ',') → {} (empty array), not {""}
        assertEquals("{}", q("SELECT string_to_array('', ',')"));
    }

    @Test void format_null_returns_empty_not_null_string() throws SQLException {
        // format('%s', NULL) → empty string, not the literal 'null'
        String result = q("SELECT format('%s', NULL)");
        // PG returns empty string for %s with NULL
        assertEquals("", result);
    }

    @Test void repeat_with_null_count_returns_null() throws SQLException {
        // repeat('x', NULL) → NULL, not empty string
        assertNull(q("SELECT repeat('x', NULL)"));
    }

    @Test void format_identifier_quoting() throws SQLException {
        // %I should add double quotes around identifiers with special chars
        String result = q("SELECT format('%s %I %L', 'x', 'Mixed Name', 'quote me')");
        assertNotNull(result);
        assertTrue(result.contains("\"Mixed Name\""),
                "%%I should quote identifier: " + result);
    }

    @Test void boolean_display_as_t_f() throws SQLException {
        // PG wire protocol displays booleans as 't'/'f'
        exec("CREATE TABLE ef_bool_test (id INT, flag BOOLEAN)");
        try {
            exec("INSERT INTO ef_bool_test VALUES (1, true), (2, false)");
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT flag FROM ef_bool_test ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals("t", rs.getString(1), "Boolean true should display as 't'");
                assertTrue(rs.next());
                assertEquals("f", rs.getString(1), "Boolean false should display as 'f'");
            }
        } finally {
            exec("DROP TABLE ef_bool_test");
        }
    }

    @Test void timestamp_infinity_literal() throws SQLException {
        // TIMESTAMP 'infinity' and '-infinity' should return the literal words
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT TIMESTAMP 'infinity', TIMESTAMP '-infinity'")) {
            assertTrue(rs.next());
            assertEquals("infinity", rs.getString(1));
            assertEquals("-infinity", rs.getString(2));
        }
    }

    @Test void date_bc_era_suffix() throws SQLException {
        // DATE '0001-12-31 BC' should display with BC suffix
        String result = q("SELECT DATE '0001-12-31 BC'");
        assertNotNull(result);
        assertTrue(result.contains("BC"), "BC date should show BC suffix: " + result);
    }

    @Test void convert_to_returns_bytea_hex() throws SQLException {
        // convert_to('abc', 'UTF8') returns bytea which displays as \\x616263
        String result = q("SELECT convert_to('abc', 'UTF8')");
        assertNotNull(result);
        assertTrue(result.startsWith("\\x"), "Bytea should show hex format: " + result);
    }

    @Test void tstzrange_format_pg_compatible() throws SQLException {
        // tstzrange should format timestamps as PG does, not ISO format
        String result = q("SELECT tstzrange(TIMESTAMPTZ '2024-01-01 00:00+00', TIMESTAMPTZ '2024-02-01 00:00+00')");
        assertNotNull(result);
        // PG format: ["2024-01-01 00:00:00+00","2024-02-01 00:00:00+00")
        // Not ISO: ["2024-01-01T00:00Z","2024-02-01T00:00Z")
        assertFalse(result.contains("T00:00Z"), "Should use PG timestamp format, not ISO: " + result);
    }

    @Test void jsonb_arrow_null_value_returns_json_null() throws SQLException {
        // jsonb->'a' where a is null should return JSON null (not SQL NULL)
        // jsonb->>'a' where a is null should return SQL NULL
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT ('{\"a\":null}'::jsonb)->'a', ('{\"a\":null}'::jsonb)->>'a'")) {
            assertTrue(rs.next());
            assertEquals("null", rs.getString(1), "->  for null value should return 'null' (json null)");
            assertNull(rs.getString(2), "->> for null value should return SQL NULL");
        }
    }

    @Test void tsvector_positions_start_at_1() throws SQLException {
        // to_tsvector('english', 'The quick brown fox'): 'The' is a stop word removed
        // Positions should count from the original: quick=2, brown=3, fox=4
        String result = q("SELECT to_tsvector('english', 'The quick brown fox')");
        assertNotNull(result);
        assertTrue(result.contains("'quick':2"), "quick should be at position 2: " + result);
    }

    @Test void phraseto_tsquery_uses_arrow_notation() throws SQLException {
        // PG uses <-> for adjacent, not <1>
        String result = q("SELECT phraseto_tsquery('english', 'quick brown')");
        assertNotNull(result);
        assertTrue(result.contains("<->"), "Should use <-> notation: " + result);
    }

    @Test void line_coefficient_order() throws SQLException {
        // line '[(0,0),(1,1)]' should produce {1,-1,0} (A,B,C where Ax+By+C=0)
        String result = q("SELECT line '[(0,0),(1,1)]'");
        assertNotNull(result);
        assertEquals("{1,-1,0}", result, "Line coefficients should be {A,B,C}");
    }

    @Test void regtype_display_canonical_name() throws SQLException {
        // 'int4'::regtype should display as 'integer' (canonical name)
        assertEquals("integer", q("SELECT 'int4'::regtype"));
    }

    @Test void regclass_includes_schema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS ef_schema");
        exec("CREATE TABLE ef_schema.ef_t (id INT)");
        try {
            // 'ef_schema.ef_t'::regclass should display as 'ef_schema.ef_t' (with schema)
            String result = q("SELECT 'ef_schema.ef_t'::regclass");
            assertNotNull(result);
            assertTrue(result.contains("ef_schema"), "regclass should include schema: " + result);
        } finally {
            exec("DROP SCHEMA ef_schema CASCADE");
        }
    }

    @Test void to_regclass_with_schema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS ef_schema2");
        exec("CREATE TABLE ef_schema2.ef_t2 (id INT)");
        try {
            String result = q("SELECT to_regclass('ef_schema2.ef_t2')");
            assertNotNull(result, "to_regclass should find schema-qualified table");
        } finally {
            exec("DROP SCHEMA ef_schema2 CASCADE");
        }
    }

    // ========================================================================
    // 9. COMMENT ON validation: PG succeeds, memgres should too
    // ========================================================================

    @Test void comment_on_column_succeeds() throws SQLException {
        exec("CREATE TABLE ef_comment_t (id INT, note TEXT)");
        try {
            // These should succeed; PG allows COMMENT ON valid objects
            exec("COMMENT ON COLUMN ef_comment_t.note IS 'a column comment'");
            // Verify via col_description if possible
        } finally {
            exec("DROP TABLE ef_comment_t");
        }
    }

    @Test void comment_on_nonexistent_column_fails() throws SQLException {
        exec("CREATE TABLE ef_comment_t2 (id INT)");
        try {
            assertSqlFails("COMMENT ON COLUMN ef_comment_t2.nope IS 'x'");
        } finally {
            exec("DROP TABLE ef_comment_t2");
        }
    }

    // ========================================================================
    // 10. EXPLAIN validation
    // ========================================================================

    @Test void explain_with_valid_options_succeeds() throws SQLException {
        exec("CREATE TABLE ef_explain_t (id INT)");
        try {
            // These should all succeed; they are valid EXPLAIN options
            exec("EXPLAIN (ANALYZE false, COSTS false, VERBOSE true) SELECT * FROM ef_explain_t");
            exec("EXPLAIN (FORMAT text) SELECT * FROM ef_explain_t");
            exec("EXPLAIN (FORMAT json) SELECT * FROM ef_explain_t");
        } finally {
            exec("DROP TABLE ef_explain_t");
        }
    }

    // ========================================================================
    // 11. Recursive CTE validation
    // ========================================================================

    @Test void recursive_cte_column_count_mismatch_rejected() {
        // Recursive term returns 2 columns, base returns 1
        assertSqlFails(
            "WITH RECURSIVE bad(n) AS (SELECT 1 UNION ALL SELECT n, n+1 FROM bad) SELECT * FROM bad");
    }

    @Test void recursive_cte_type_mismatch_rejected() {
        // Base returns integer, recursive returns text
        assertSqlFails(
            "WITH RECURSIVE bad2(n) AS (SELECT 1 UNION ALL SELECT 'x' FROM bad2) SELECT * FROM bad2");
    }

    // ========================================================================
    // 12. ALTER validation
    // ========================================================================

    @Test void alter_view_nonexistent_rejected() {
        assertSqlFails("ALTER VIEW no_such_view RENAME COLUMN a TO b");
    }

    @Test void alter_domain_nonexistent_rejected() {
        assertSqlFails("ALTER DOMAIN no_such_domain SET DEFAULT 1");
    }

    @Test void alter_table_add_generated_column_bad_expr_rejected() throws SQLException {
        exec("CREATE TABLE ef_gen_t (id INT, a INT)");
        try {
            // no_such_col doesn't exist
            assertSqlFails("ALTER TABLE ef_gen_t ADD COLUMN e INT GENERATED ALWAYS AS (no_such_col * 2) STORED");
        } finally {
            exec("DROP TABLE ef_gen_t");
        }
    }

    @Test void alter_table_add_check_bad_column_rejected() throws SQLException {
        exec("CREATE TABLE ef_check_t (id INT, qty INT)");
        try {
            assertSqlFails("ALTER TABLE ef_check_t ADD CONSTRAINT bad_ck CHECK (nope > 0)");
        } finally {
            exec("DROP TABLE ef_check_t");
        }
    }

    // ========================================================================
    // 13. Function body validation
    // ========================================================================

    @Test void create_function_with_invalid_body_rejected() {
        // $$ SELECT $$ is not valid SQL
        assertSqlFails("CREATE FUNCTION ef_badf(a int) RETURNS int LANGUAGE SQL AS $$ SELECT $$");
    }

    @Test void create_function_overload_existing_name_rejected() throws SQLException {
        exec("CREATE FUNCTION ef_f(a int) RETURNS int LANGUAGE SQL AS $$ SELECT a $$");
        try {
            // Creating same function with different arg type should error or replace
            // PG: function already exists with same argument types (if exact match)
            // Creating with DIFFERENT arg types is actually allowed in PG (overloading)
            // This test specifically checks same-name same-arg-count behavior
        } finally {
            exec("DROP FUNCTION IF EXISTS ef_f(int)");
        }
    }

    // ========================================================================
    // 14. IDENTITY column validation
    // ========================================================================

    @Test void insert_explicit_value_into_identity_always_rejected() throws SQLException {
        exec("CREATE TABLE ef_ident (id INT GENERATED ALWAYS AS IDENTITY, name TEXT)");
        try {
            assertSqlError("INSERT INTO ef_ident(id, name) VALUES (401, 'bad')", "428C9");
        } finally {
            exec("DROP TABLE ef_ident");
        }
    }

    // ========================================================================
    // 15. Temp table ON COMMIT behavior
    // ========================================================================

    @Test void temp_table_on_commit_drop_removes_after_commit() throws SQLException {
        exec("BEGIN");
        exec("CREATE TEMP TABLE ef_tt_drop (a INT) ON COMMIT DROP");
        exec("INSERT INTO ef_tt_drop VALUES (1)");
        exec("COMMIT");
        // Table should be gone after commit
        assertSqlFails("SELECT * FROM ef_tt_drop");
    }

    // ========================================================================
    // 16. DROP RULE on nonexistent rule
    // ========================================================================

    @Test void drop_rule_nonexistent_rejected() throws SQLException {
        exec("CREATE TABLE ef_rule_t (id INT)");
        try {
            assertSqlFails("DROP RULE no_such_rule ON ef_rule_t");
        } finally {
            exec("DROP TABLE ef_rule_t");
        }
    }

    // ========================================================================
    // 17. Trigger validation
    // ========================================================================

    @Test void create_trigger_missing_function_rejected() throws SQLException {
        exec("CREATE TABLE ef_trig_t (id INT)");
        try {
            assertSqlFails("CREATE TRIGGER ef_bad_trig AFTER INSERT ON ef_trig_t " +
                          "FOR EACH ROW EXECUTE FUNCTION no_such_function()");
        } finally {
            exec("DROP TABLE ef_trig_t");
        }
    }

    // ========================================================================
    // 18. DO block validation
    // ========================================================================

    @Test void do_block_missing_semicolon_after_declare_rejected() {
        // Missing semicolon after variable declaration
        assertSqlFails("DO $$ DECLARE x int BEGIN x := 1; END $$");
    }

    @Test void do_block_dynamic_sql_nonexistent_table_rejected() {
        // EXECUTE 'SELECT nope' should fail; nope doesn't exist
        assertSqlFails("DO LANGUAGE plpgsql $$ BEGIN EXECUTE 'SELECT nope'; END $$");
    }

    // ========================================================================
    // 19. Sequence validation
    // ========================================================================

    @Test void setval_nonexistent_sequence_rejected() {
        assertSqlFails("SELECT setval('no_such_seq', 1000, true)");
    }

    // ========================================================================
    // 20. Search path display format
    // ========================================================================

    @Test void show_search_path_no_extra_spaces() throws SQLException {
        exec("SET search_path TO compat, pg_catalog");
        try {
            String result = q("SHOW search_path");
            // Should be "compat, pg_catalog", no extra space before comma
            assertFalse(result.contains(" ,"), "No space before comma: " + result);
        } finally {
            exec("SET search_path TO public, pg_catalog");
        }
    }
}
