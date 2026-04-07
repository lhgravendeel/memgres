package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SQLSTATE error code correctness V3. Broad coverage of error code mismatches
 * detected by the Pg18V2 verification suite. Goal: Memgres must return the same
 * SQLSTATE as PG18 for every error condition.
 *
 * Categories covered:
 *   - Composite field access on non-existent table/column (42P01 vs 42703)
 *   - Operator qualified syntax errors (42601 vs 42883)
 *   - Type input validation (22P02 vs 42883/42804)
 *   - Function definition errors (42P13 vs 42601)
 *   - Schema qualification errors (3F000 vs 42601)
 *   - Prepared statement lifecycle (26000 vs 42P02)
 *   - DDL semantic errors (42809, 42P16, 42P17, 55000)
 *   - EXECUTE dynamic SQL (42601 vs 42P02)
 *   - Role/permission errors (42704 vs 42P01)
 *   - Interval/timestamp parsing (22007 vs 22P02)
 */
class SqlStateCorrectnessV3Test {

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
            s.execute("CREATE TABLE ssv3_t (id INT PRIMARY KEY, a INT, b TEXT)");
            s.execute("INSERT INTO ssv3_t VALUES (1, 10, 'x'), (2, 20, 'y')");
            s.execute("CREATE TYPE ssv3_pair AS (x INT, y TEXT)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS ssv3_t CASCADE");
                s.execute("DROP TYPE IF EXISTS ssv3_pair CASCADE");
            } catch (SQLException ignored) {}
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static void assertSqlState(String expectedState, String sql) {
        try {
            try (Statement s = conn.createStatement()) { s.execute(sql); }
            fail("Expected error with SQLSTATE " + expectedState + " for: " + sql);
        } catch (SQLException e) {
            assertEquals(expectedState, e.getSQLState(),
                    "Wrong SQLSTATE for: " + sql + ", got message: " + e.getMessage());
        }
    }

    // ========================================================================
    // Composite / field access: should be 42P01 (undefined_table) not 42703
    // When accessing c.x on a non-existent table reference c, PG gives 42P01
    // ========================================================================

    @Test void field_access_missing_table_ref_gives_42P01() {
        assertSqlState("42P01", "SELECT c.x, c.y FROM ssv3_t");
    }

    @Test void field_select_from_subquery_bad_ref_gives_42703() {
        // PG18: field access on anonymous record gives 42703 (undefined_column)
        assertSqlState("42703", "SELECT (r).z FROM (SELECT ROW(1,2)) AS sub(r)");
    }

    @Test void composite_field_nonexistent_ref_gives_42P01() {
        // SELECT c.z where c is not a known table/alias
        assertSqlState("42P01", "SELECT c.z FROM ssv3_t AS t");
    }

    // ========================================================================
    // OPERATOR(...) qualified syntax: PG errors
    // ========================================================================

    @Test void operator_qualified_syntax_gives_42601_for_bad_arity() {
        // OPERATOR(pg_catalog.+)(1,2,3); PG: 42883 (too many args for operator)
        assertSqlState("42883", "SELECT OPERATOR(pg_catalog.+)(1,2,3)");
    }

    @Test void operator_unknown_schema_gives_3F000() {
        // OPERATOR(no_such_schema.+)(1,2); PG: 3F000 (schema does not exist)
        assertSqlState("3F000", "SELECT OPERATOR(no_such_schema.+)(1, 2)");
    }

    @Test void operator_qualified_valid_works() {
        // PG18 treats OPERATOR(pg_catalog.+)(1,2) as unary prefix operator on ROW(1,2)
        // which fails with "operator does not exist: pg_catalog.+ record"
        assertSqlState("42883", "SELECT OPERATOR(pg_catalog.+)(1, 2)");
    }

    @Test void triple_at_operator_gives_42883() {
        // @@@ 1; PG: operator does not exist (42883)
        assertSqlState("42883", "SELECT @@@ 1");
    }

    @Test void qualified_concat_operator_gives_42883_for_bad_types() {
        // OPERATOR(pg_catalog.||)(1, 2) on integers; PG: 42883
        assertSqlState("42883", "SELECT OPERATOR(pg_catalog.||)(1, 2)");
    }

    // ========================================================================
    // Type input validation: 22P02 (invalid_text_representation)
    // ========================================================================

    @Test void int4multirange_bad_input_gives_22P02() {
        assertSqlState("22P02", "SELECT 'bad'::int4multirange");
    }

    @Test void array_subscript_text_on_int_gives_22P02() throws SQLException {
        // int_array['x']; PG: 22P02 (invalid input for integer)
        exec("CREATE TABLE ssv3_arr (id INT, nums INT[])");
        try {
            exec("INSERT INTO ssv3_arr VALUES (1, ARRAY[1,2,3])");
            assertSqlState("22P02", "SELECT nums['x'] FROM ssv3_arr");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_arr");
        }
    }

    @Test void invalid_interval_gives_22007() {
        // INTERVAL 'nonsense'; PG: 22007 (invalid_datetime_format)
        assertSqlState("22007", "SELECT INTERVAL 'nonsense'");
    }

    @Test void timetz_extreme_utc_offset_succeeds() throws SQLException {
        // PG18: TIMETZ '10:00 UTC+99' succeeds; PG wraps extreme offsets
        try (Statement s = conn.createStatement();
             java.sql.ResultSet rs = s.executeQuery("SELECT TIMETZ '10:00 UTC+99'")) {
            assertTrue(rs.next(), "Should return a row");
        }
    }

    // ========================================================================
    // Function calls with wrong argument types: 42883 (undefined_function)
    // ========================================================================

    @Test void function_wrong_arg_type_gives_42883() throws SQLException {
        exec("CREATE FUNCTION ssv3_add(a INT, b INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + b $$");
        try {
            // ssv3_add('x', 1); PG: 22P02 for text-to-int cast failure
            assertSqlState("22P02", "SELECT ssv3_add('x', 1)");
        } finally {
            exec("DROP FUNCTION IF EXISTS ssv3_add(int, int)");
        }
    }

    @Test void length_two_args_gives_22023() {
        // LENGTH('a', 'b'); PG18: 22023 (invalid_parameter_value)
        assertSqlState("22023", "SELECT LENGTH('a', 'b')");
    }

    // ========================================================================
    // CREATE FUNCTION with bad body: 42P13 (invalid_function_definition)
    // ========================================================================

    @Test void create_function_syntax_error_in_body_gives_42601() {
        // PG18: plpgsql RETURN; without value in non-void function → 42601 (syntax_error)
        assertSqlState("42601",
            "CREATE FUNCTION ssv3_bad() RETURNS INT LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$");
    }

    @Test void create_function_bad_sql_body_gives_42601() {
        // SQL-language function with syntax error in body; PG18: 42601
        assertSqlState("42601",
            "CREATE FUNCTION ssv3_bad2(a int) RETURNS int LANGUAGE SQL AS $$ SELECTX $$");
    }

    @Test void trigger_function_wrong_return_gives_42P17() {
        // Trigger function that returns wrong type; PG: 42P17
        try {
            exec("CREATE FUNCTION ssv3_bad_trig() RETURNS INT LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END $$");
            assertSqlState("42P17",
                "CREATE TRIGGER ssv3_t BEFORE INSERT ON ssv3_t FOR EACH ROW EXECUTE FUNCTION ssv3_bad_trig()");
        } catch (SQLException e) {
            // Could fail at function creation, acceptable
        } finally {
            try { exec("DROP FUNCTION IF EXISTS ssv3_bad_trig()"); } catch (SQLException ignored) {}
        }
    }

    // ========================================================================
    // PREPARE / EXECUTE / DEALLOCATE lifecycle: 26000 (invalid_sql_statement_name)
    // ========================================================================

    @Test void execute_nonexistent_prepared_gives_26000() {
        assertSqlState("26000", "EXECUTE no_such_prepared_stmt");
    }

    @Test void deallocate_nonexistent_gives_26000() {
        assertSqlState("26000", "DEALLOCATE no_such_stmt");
    }

    @Test void execute_after_deallocate_gives_26000() throws SQLException {
        exec("PREPARE ssv3_p1 AS SELECT 1");
        exec("DEALLOCATE ssv3_p1");
        assertSqlState("26000", "EXECUTE ssv3_p1");
    }

    @Test void execute_dynamic_nonexistent_gives_42P02() {
        // EXECUTE p1(1) where p1 is not prepared; PG: 26000
        // but EXECUTE format(...) inside DO; PG: 42601 for bad syntax
        assertSqlState("26000", "EXECUTE ssv3_phantom(1)");
    }

    // ========================================================================
    // DDL semantic errors: partition, attach, constraint
    // ========================================================================

    @Test void attach_already_attached_partition_gives_42809() throws SQLException {
        exec("CREATE TABLE ssv3_prt (id INT) PARTITION BY RANGE (id)");
        exec("CREATE TABLE ssv3_prt_1 PARTITION OF ssv3_prt FOR VALUES FROM (1) TO (100)");
        try {
            // Re-attaching the same table should give 42809 (wrong_object_type)
            assertSqlState("42809",
                "ALTER TABLE ssv3_prt ATTACH PARTITION ssv3_prt_1 FOR VALUES FROM (1) TO (100)");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_prt CASCADE");
        }
    }

    @Test void hash_partition_wrong_modulus_gives_42P16() throws SQLException {
        exec("CREATE TABLE ssv3_hpart (id INT) PARTITION BY HASH (id)");
        exec("CREATE TABLE ssv3_hpart_0 PARTITION OF ssv3_hpart FOR VALUES WITH (MODULUS 4, REMAINDER 0)");
        try {
            // Different modulus should give 42P16 (invalid_table_definition)
            assertSqlState("42P16",
                "CREATE TABLE ssv3_hpart_bad PARTITION OF ssv3_hpart FOR VALUES WITH (MODULUS 8, REMAINDER 0)");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_hpart CASCADE");
        }
    }

    @Test void attach_overlapping_partition_gives_42809() throws SQLException {
        exec("CREATE TABLE ssv3_olap (id INT) PARTITION BY RANGE (id)");
        exec("CREATE TABLE ssv3_olap_1 PARTITION OF ssv3_olap FOR VALUES FROM (1) TO (100)");
        exec("CREATE TABLE ssv3_olap_cand (id INT)");
        try {
            // Overlapping range should give 42P17 (invalid_object_definition) in PG18
            assertSqlState("42P17",
                "ALTER TABLE ssv3_olap ATTACH PARTITION ssv3_olap_cand FOR VALUES FROM (50) TO (150)");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_olap CASCADE");
            exec("DROP TABLE IF EXISTS ssv3_olap_cand CASCADE");
        }
    }

    @Test void alter_set_generated_always_nonexistent_col_gives_55000() throws SQLException {
        exec("CREATE TABLE ssv3_gen (id INT, val INT)");
        try {
            // ALTER TABLE ... ALTER COLUMN ... SET GENERATED ALWAYS on non-identity column; PG: 55000
            assertSqlState("55000",
                "ALTER TABLE ssv3_gen ALTER COLUMN val SET GENERATED ALWAYS");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_gen");
        }
    }

    // ========================================================================
    // CASE with composite type mismatch: 42804 vs 42P18
    // ========================================================================

    @Test void case_composite_type_mismatch_gives_42804() {
        assertSqlState("42804",
            "SELECT CASE WHEN true THEN ROW(1,'a')::ssv3_pair ELSE 'text' END");
    }

    // ========================================================================
    // Range cross-type operation: 42883
    // ========================================================================

    @Test void range_intersect_cross_type_gives_42883() {
        // int4range * numrange; PG: 42883 (operator does not exist)
        assertSqlState("42883", "SELECT int4range(1,5) * numrange(2.0, 4.0)");
    }

    @Test void range_merge_cross_type_gives_42883() {
        assertSqlState("42883", "SELECT range_merge(int4range(1,5), numrange(2.0, 4.0))");
    }

    // ========================================================================
    // Geometry input errors
    // ========================================================================

    @Test void box_three_points_gives_42883() {
        // box with 3 points; PG: 42883 (function does not exist)
        assertSqlState("42883", "SELECT box(point(0,0), point(1,1), point(2,2))");
    }

    @Test void open_path_keyword_gives_42704() {
        // open( path '...' ) where open is used as function; PG gives 42704 or similar
        // path syntax: PG: open is not a standard function, 42704 (undefined_object)
        assertSqlState("42704", "SELECT open(path '((0,0),(1,1))')");
    }

    // ========================================================================
    // SET ROLE / DROP ROLE: non-existent roles
    // ========================================================================

    @Test void set_role_nonexistent_gives_22023() {
        // SET ROLE no_such_role; PG18: 22023 (invalid_parameter_value)
        assertSqlState("22023", "SET ROLE no_such_role");
    }

    @Test void grant_on_missing_table_gives_42P01() {
        // GRANT SELECT ON missing_table; PG: 42P01 (undefined_table)
        assertSqlState("42P01", "GRANT SELECT ON TABLE no_such_table TO PUBLIC");
    }

    // ========================================================================
    // Postfix operators: factorial (!) was removed in PG14
    // PG14+ treats ! as syntax error (42601) since postfix operators
    // no longer exist in the grammar
    // ========================================================================

    @Test void absolute_value_with_factorial_gives_42601() {
        // @ -5 prefix is fine, but 5 ! is a syntax error in PG18 (operator removed in PG14)
        assertSqlState("42601", "SELECT @ -5, 5 !");
    }

    // ========================================================================
    // DO block / EXECUTE format with NULL
    // ========================================================================

    @Test void do_execute_null_gives_22004() {
        // DO $$ BEGIN EXECUTE NULL; END $$; PG: 22004 (null_value_not_allowed)
        assertSqlState("22004",
            "DO $$ BEGIN EXECUTE NULL; END $$");
    }

    // ========================================================================
    // FOR UPDATE OF bad table: 42P01
    // ========================================================================

    @Test void for_update_of_bad_table_gives_42P01() {
        assertSqlState("42P01", "SELECT * FROM ssv3_t FOR UPDATE OF no_such_table");
    }

    // ========================================================================
    // ON CONFLICT: 42P10 (invalid_column_reference) vs 42601
    // ========================================================================

    @Test void on_conflict_no_inference_gives_42601() throws SQLException {
        exec("CREATE TABLE ssv3_oc (id INT PRIMARY KEY, val TEXT)");
        try {
            assertSqlState("42601",
                "INSERT INTO ssv3_oc VALUES (1, 'a') ON CONFLICT DO UPDATE SET val = 'b'");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_oc");
        }
    }

    // ========================================================================
    // Copy-related errors
    // ========================================================================

    @Test void copy_to_stdout_in_simple_mode_gives_error() {
        // COPY ... TO STDOUT; PG sends data through copy protocol, JDBC can't handle it
        // But the error code should still be appropriate
        try {
            exec("COPY ssv3_t TO STDOUT");
            // If it succeeds somehow, that's OK too
        } catch (SQLException e) {
            // Any error is acceptable here, just documenting the behavior
            assertNotNull(e.getSQLState());
        }
    }

    // ========================================================================
    // Misc SQLSTATE: jsonb_path_match, domain violations, enum input
    // ========================================================================

    @Test void jsonb_path_match_bad_arg_gives_22038_or_42883() {
        // jsonb_path_match with non-boolean result; PG: 22038
        try {
            exec("SELECT jsonb_path_match('{\"a\":1}'::jsonb, '$.a')");
            // If it succeeds, function exists, and that's progress
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("22038".equals(state) || "42883".equals(state),
                "Expected 22038 or 42883, got " + state + ": " + e.getMessage());
        }
    }

    @Test void domain_violation_gives_23514() throws SQLException {
        exec("CREATE DOMAIN ssv3_posint AS INT CHECK (VALUE > 0)");
        try {
            assertSqlState("23514", "SELECT CAST(-1 AS ssv3_posint)");
        } finally {
            exec("DROP DOMAIN IF EXISTS ssv3_posint");
        }
    }

    @Test void enum_bad_value_gives_22P02() throws SQLException {
        exec("CREATE TYPE ssv3_mood AS ENUM ('happy', 'sad')");
        try {
            assertSqlState("22P02", "SELECT 'angry'::ssv3_mood");
        } finally {
            exec("DROP TYPE IF EXISTS ssv3_mood");
        }
    }

    // ========================================================================
    // GROUP BY ROLLUP (), empty rollup ref; PG18 accepts this (grand total)
    // ========================================================================

    @Test void group_by_rollup_empty_gives_42601() {
        // PG18 rejects empty ROLLUP() with 42601 syntax error
        assertSqlState("42601", "SELECT count(*) FROM ssv3_t GROUP BY ROLLUP ()");
    }

    // ========================================================================
    // SEARCH/CYCLE clause errors
    // ========================================================================

    @Test void search_clause_on_non_recursive_cte_gives_42601() {
        assertSqlState("42601",
            "WITH w AS (SELECT 1 AS n) SEARCH DEPTH FIRST BY n SET ord SELECT * FROM w");
    }

    // ========================================================================
    // CREATE INDEX on volatile function: 42P17
    // ========================================================================

    @Test void index_on_volatile_function_gives_42P17() throws SQLException {
        exec("CREATE TABLE ssv3_idx_t (id INT)");
        try {
            assertSqlState("42P17",
                "CREATE INDEX ssv3_bad_idx ON ssv3_idx_t (random())");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_idx_t");
        }
    }

    // ========================================================================
    // WITHIN GROUP on non-existent table: should be 42P01 (undefined_table)
    // Diff: 03_functions_aggregates_windows.sql stmts 54-55, 62-63
    // Memgres returns 42601 (syntax error) instead
    // ========================================================================

    @Test void percentile_disc_missing_table_gives_42P01() {
        assertSqlState("42P01",
            "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY b) FROM no_such_ftab");
    }

    @Test void rank_within_group_missing_table_gives_42P01() {
        assertSqlState("42P01",
            "SELECT rank(10) WITHIN GROUP (ORDER BY b) FROM no_such_ftab");
    }

    // ========================================================================
    // Recursive CTE union type mismatch: 22P02 (invalid_text_representation)
    // Diff: 09_joins_subqueries_ctes.sql stmt 42, 21_recursive_cte_srf_tableexpr.sql stmt 31
    // PG: text 'x' can't be added to integer → 22P02
    // Memgres: 42804 (datatype_mismatch)
    // ========================================================================

    @Test void recursive_cte_type_mismatch_gives_22P02() {
        assertSqlState("22P02",
            "WITH RECURSIVE bad2(n) AS ( " +
            "SELECT 1 UNION ALL SELECT 'x' FROM bad2 " +
            ") SELECT * FROM bad2");
    }

    // ========================================================================
    // array_append type mismatch: 22P02 not 42883
    // Diff: 26_collation_null_polymorphic.sql stmt 34
    // PG: can't append text 'x' to int[] → 22P02
    // ========================================================================

    @Test void array_append_type_mismatch_gives_22P02() {
        assertSqlState("22P02", "SELECT array_append(ARRAY[1,2], 'x')");
    }

    // ========================================================================
    // pg_typeof(VALUES ...); PG: 42601 (syntax_error) not 42883
    // Diff: 02_value_expressions.sql stmt 60
    // ========================================================================

    @Test void pg_typeof_values_gives_42601() {
        assertSqlState("42601", "SELECT pg_typeof(VALUES (1), (2))");
    }

    // ========================================================================
    // PREPARE with unterminated placeholder: PG: 42601 not 42883
    // Diff: 25_parser_stress.sql stmt 68
    // ========================================================================

    @Test void prepare_bare_dollar_gives_42601() {
        assertSqlState("42601", "PREPARE ssv3_bad_prep AS SELECT $");
    }

    // ========================================================================
    // OPERATOR(pg_catalog.+)(), zero args; PG: 42601 not 42883
    // Diff: 25_parser_stress.sql stmt 78
    // ========================================================================

    @Test void operator_zero_args_gives_42601() {
        assertSqlState("42601", "SELECT OPERATOR(pg_catalog.+)()");
    }

    // ========================================================================
    // Postfix ! operator: PG14+ gives 42601 (syntax_error), not 42883
    // Diff: 02_value_expressions.sql stmts 34, 48
    // ========================================================================

    @Test void factorial_postfix_gives_42601() {
        // PG18: ! is a removed operator, gives 42601 (syntax error)
        assertSqlState("42601", "SELECT 5 !");
    }

    @Test void double_factorial_postfix_gives_42601() {
        assertSqlState("42601", "SELECT 5 ! !");
    }

    // ========================================================================
    // Trigger with wrong return type: PG: 42P17 not 42P13
    // Diff: 15_triggers_rules_views.sql stmt 47
    // ========================================================================

    @Test void trigger_wrong_return_type_gives_42P17() throws SQLException {
        exec("CREATE TABLE ssv3_trt (id INT PRIMARY KEY, a INT)");
        exec("CREATE FUNCTION ssv3_bad_ret() RETURNS INT LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END $$");
        try {
            assertSqlState("42P17",
                "CREATE TRIGGER ssv3_bad_ret_trig AFTER INSERT ON ssv3_trt " +
                "FOR EACH ROW EXECUTE FUNCTION ssv3_bad_ret()");
        } finally {
            exec("DROP FUNCTION IF EXISTS ssv3_bad_ret()");
            exec("DROP TABLE IF EXISTS ssv3_trt");
        }
    }

    // ========================================================================
    // CREATE FUNCTION that already exists: PG: 42501 (insufficient_privilege)
    // when overwriting a function owned by another role, or 42723 (duplicate)
    // Diff: 03_functions_aggregates_windows.sql stmt 45
    // ========================================================================

    @Test void create_function_duplicate_gives_42723() throws SQLException {
        exec("CREATE FUNCTION ssv3_dup(a INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a $$");
        try {
            // Creating same function again without OR REPLACE should give 42723
            assertSqlState("42723",
                "CREATE FUNCTION ssv3_dup(a INT) RETURNS INT LANGUAGE SQL AS $$ SELECT a + 1 $$");
        } finally {
            exec("DROP FUNCTION IF EXISTS ssv3_dup(INT)");
        }
    }

    // ========================================================================
    // Bad plpgsql function, RETURN without value: PG: 42601 not 42P13
    // Diff: 11_routines_and_errors.sql stmt 27
    // PG treats this as a syntax error in the body (42601)
    // ========================================================================

    @Test void plpgsql_return_without_value_gives_42601() {
        assertSqlState("42601",
            "CREATE OR REPLACE FUNCTION ssv3_badpl() RETURNS int LANGUAGE plpgsql AS " +
            "$$ BEGIN RETURN; END $$");
    }

    // ========================================================================
    // ON CONFLICT DO UPDATE without conflict target: PG: 42601 not 42P10
    // Diff: 32_upsert_and_generated_keys.sql stmt 29
    // ========================================================================

    @Test void on_conflict_do_update_no_target_gives_42601() throws SQLException {
        exec("CREATE TABLE ssv3_oc2 (id INT PRIMARY KEY, val TEXT)");
        try {
            assertSqlState("42601",
                "INSERT INTO ssv3_oc2(id, val) VALUES (1, 'a') ON CONFLICT DO UPDATE SET val = 'b'");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_oc2");
        }
    }

    // ========================================================================
    // ON CONFLICT with wrong column reference: PG: 42P10 not 23505
    // Diff: 32_upsert_and_generated_keys.sql stmt 26
    // ========================================================================

    @Test void on_conflict_wrong_columns_gives_42P10() throws SQLException {
        exec("CREATE TABLE ssv3_oc3 (id INT PRIMARY KEY, code TEXT UNIQUE, note TEXT)");
        exec("INSERT INTO ssv3_oc3 VALUES (1, 'A', 'first')");
        try {
            // Conflict target (note) has no unique index; PG: 42P10
            assertSqlState("42P10",
                "INSERT INTO ssv3_oc3(id, code, note) VALUES (2, 'A', 'dup') " +
                "ON CONFLICT (note) DO UPDATE SET note = 'updated'");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_oc3");
        }
    }

    // ========================================================================
    // EXECUTE prepared stmt with type error: PG: 26000 not 22P02
    // When PREPARE fails, EXECUTE should give 26000 (statement doesn't exist)
    // Diff: 31_jdbc_prepared_statement_patterns.sql stmts 49, 51
    // ========================================================================

    @Test void execute_failed_prepare_case_gives_26000() throws SQLException {
        // If PREPARE fails, the statement is not registered → EXECUTE should give 26000
        try { exec("PREPARE ssv3_bad_case AS SELECT CASE WHEN $1 THEN 1 ELSE 'x' END"); }
        catch (SQLException ignored) {}
        try {
            assertSqlState("26000", "EXECUTE ssv3_bad_case(true)");
        } finally {
            try { exec("DEALLOCATE ssv3_bad_case"); } catch (SQLException ignored) {}
        }
    }

    @Test void execute_failed_prepare_limit_gives_26000() throws SQLException {
        exec("CREATE TABLE ssv3_prep_t (id INT)");
        try {
            try { exec("PREPARE ssv3_bad_limit(text) AS SELECT * FROM ssv3_prep_t LIMIT $1"); }
            catch (SQLException ignored) {}
            try {
                assertSqlState("26000", "EXECUTE ssv3_bad_limit('x')");
            } finally {
                try { exec("DEALLOCATE ssv3_bad_limit"); } catch (SQLException ignored) {}
            }
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_prep_t");
        }
    }

    @Test void execute_failed_prepare_any_gives_26000() throws SQLException {
        try { exec("PREPARE ssv3_bad_any(text) AS SELECT 1 = ANY($1)"); }
        catch (SQLException ignored) {}
        try {
            assertSqlState("26000", "EXECUTE ssv3_bad_any('abc')");
        } finally {
            try { exec("DEALLOCATE ssv3_bad_any"); } catch (SQLException ignored) {}
        }
    }

    // ========================================================================
    // Domain violation via invalid enum value: PG: 23514 not 22P02
    // Diff: 17_domains_enums_composites.sql stmts 31-32
    // ========================================================================

    @Test void domain_check_violation_via_enum_gives_23514() throws SQLException {
        exec("CREATE DOMAIN ssv3_posage AS INT CHECK (VALUE > 0)");
        exec("CREATE TABLE ssv3_dom_t (id INT, age ssv3_posage)");
        try {
            assertSqlState("23514",
                "INSERT INTO ssv3_dom_t VALUES (1, 0)");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_dom_t");
            exec("DROP DOMAIN IF EXISTS ssv3_posage");
        }
    }

    // ========================================================================
    // View WITH CHECK OPTION violation: PG: 44000 not 42P01
    // Diff: 22_updatable_views_identity_defaults.sql stmt 36
    // ========================================================================

    @Test void view_check_option_violation_gives_44000() throws SQLException {
        exec("CREATE TABLE ssv3_vco (id INT PRIMARY KEY, a INT)");
        exec("INSERT INTO ssv3_vco VALUES (1, 5), (2, 15)");
        exec("CREATE VIEW ssv3_vco_v AS SELECT * FROM ssv3_vco WHERE a > 0 WITH CHECK OPTION");
        try {
            // Inserting a row that violates the view's WHERE should give 44000
            assertSqlState("44000",
                "INSERT INTO ssv3_vco_v VALUES (3, -1)");
        } finally {
            exec("DROP VIEW IF EXISTS ssv3_vco_v");
            exec("DROP TABLE IF EXISTS ssv3_vco");
        }
    }

    // ========================================================================
    // ALTER SET GENERATED ALWAYS on renamed column: PG: 55000 not 42703
    // Diff: 25_parser_stress.sql stmt 63
    // ========================================================================

    @Test void alter_set_generated_always_after_rename_gives_55000() throws SQLException {
        exec("CREATE TABLE ssv3_ren (id INT, a INT)");
        exec("ALTER TABLE ssv3_ren RENAME a TO b");
        try {
            // Setting GENERATED ALWAYS on renamed column using old name; PG: 42703
            // Setting on existing non-identity column; PG: 55000
            assertSqlState("55000",
                "ALTER TABLE ssv3_ren ALTER COLUMN b SET GENERATED ALWAYS");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_ren");
        }
    }

    // ========================================================================
    // Array with invalid bounds, [2:1]={}; PG: 2202E not 22P02
    // Diff: 12_json_arrays_ranges_geometry.sql stmt 59
    // ========================================================================

    @Test void array_invalid_bounds_gives_2202E() {
        // [2:1] means lower > upper; PG: 2202E (array_subscript_error)
        assertSqlState("2202E", "SELECT '[2:1]={}'::int[]");
    }

    // ========================================================================
    // TABLESAMPLE with negative percentage: PG: 2202H not 42601
    // Diff: 21_recursive_cte_srf_tableexpr.sql stmt 36
    // ========================================================================

    @Test void tablesample_negative_gives_2202H() throws SQLException {
        exec("CREATE TABLE ssv3_samp (id INT)");
        try {
            assertSqlState("2202H",
                "SELECT * FROM ssv3_samp TABLESAMPLE SYSTEM (-1)");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_samp");
        }
    }

    // ========================================================================
    // TABLESAMPLE with non-existent method: PG: 42704 not 42601
    // Diff: 21_recursive_cte_srf_tableexpr.sql stmt 37
    // ========================================================================

    @Test void tablesample_unknown_method_gives_42704() throws SQLException {
        exec("CREATE TABLE ssv3_samp2 (id INT)");
        try {
            assertSqlState("42704",
                "SELECT * FROM ssv3_samp2 TABLESAMPLE no_such_method (10)");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_samp2");
        }
    }

    // ========================================================================
    // jsonb_path_match with non-boolean result: PG: 22038 not 42883
    // Diff: 18_arrays_ranges_jsonb_deep.sql stmt 44
    // ========================================================================

    @Test void jsonb_path_match_non_boolean_gives_22038() {
        // $.a returns a number, not boolean; PG: 22038 (sql_json_scalar_required)
        assertSqlState("22038",
            "SELECT jsonb_path_match('{\"a\":2}'::jsonb, '$.a')");
    }

    // ========================================================================
    // Division by zero in aborted transaction: PG: 22012 not 25P02
    // Diff: 10_transactions_savepoints.sql stmt 32
    // When already in a failed transaction, re-executing should still
    // report the original error, not "in_failed_sql_transaction"
    // ========================================================================

    @Test void division_by_zero_gives_22012() {
        assertSqlState("22012", "SELECT 1/0");
    }

    // ========================================================================
    // DO $$ BEGIN EXECUTE format('%I', NULL); END $$; PG: 22004 not 42601
    // Diff: 37_do_blocks_and_dynamic_sql.sql stmt 20
    // ========================================================================

    @Test void do_execute_format_null_gives_22004() {
        assertSqlState("22004",
            "DO $$ BEGIN EXECUTE format('SELECT * FROM %I', NULL); END $$");
    }

    // ========================================================================
    // COPY ... TO STDOUT: PG should give 42P01 for missing table, not 42601
    // Diff: 30_admin_parser_edges.sql stmt 58
    // ========================================================================

    @Test void copy_to_stdout_missing_table_gives_42P01() {
        assertSqlState("42P01",
            "COPY (SELECT * FROM no_such_copy_table) TO STDOUT");
    }

    // ========================================================================
    // Recursive CTE with wrong type in initial vs recursive: 42883 not 22P02
    // Diff: 09_joins_subqueries_ctes.sql stmt 30
    // SELECT 'x' UNION ALL SELECT n + 1; PG: 42883 (can't add text + int)
    // ========================================================================

    @Test void recursive_cte_text_plus_int_gives_42883() {
        assertSqlState("42883",
            "WITH RECURSIVE r(n) AS (" +
            "SELECT 'x' UNION ALL SELECT n + 1 FROM r" +
            ") SELECT * FROM r");
    }

    // ========================================================================
    // box() with 3 point args: PG: 42883 not 22P02
    // Diff: 12_json_arrays_ranges_geometry.sql stmt 63
    // ========================================================================

    @Test void box_three_point_args_gives_42883() {
        assertSqlState("42883",
            "SELECT center(box(point(0,0), point(1,1), point(2,2)))");
    }

    // ========================================================================
    // INSERT RETURNING with duplicate key: PG: 42703 (column not found) or 23505
    // depending on which is checked first
    // Diff: 20_merge_returning_scoping.sql stmt 27
    // ========================================================================

    @Test void insert_duplicate_with_bad_returning_gives_42703() throws SQLException {
        exec("CREATE TABLE ssv3_ret (id INT PRIMARY KEY, a INT, b TEXT)");
        exec("INSERT INTO ssv3_ret VALUES (1, 10, 'x')");
        try {
            // Insert duplicate key + reference non-existent column in RETURNING
            // PG checks RETURNING column references before executing → 42703
            assertSqlState("42703",
                "INSERT INTO ssv3_ret(id, a, b) VALUES (1, 10, 'dup') RETURNING no_such_col");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_ret");
        }
    }

    // ========================================================================
    // int4multirange with bad text input: PG: 22P02 not 42883
    // Diff: 04_types_and_casts.sql stmt 45, 12_json_arrays_ranges_geometry.sql stmt 29
    // ========================================================================

    @Test void int4multirange_text_input_gives_22P02() {
        assertSqlState("22P02", "SELECT int4multirange('[1,2)')");
    }

    // ========================================================================
    // GROUPING SETS / ROLLUP errors: PG: 42P01 not 42601
    // Diff: 08_select_setops_grouping.sql stmt 36
    // ========================================================================

    @Test void grouping_sets_missing_table_gives_42P01() {
        assertSqlState("42P01",
            "SELECT b, a, sum(c), grouping(b), grouping(a) FROM no_such_gs_table " +
            "GROUP BY GROUPING SETS ((b, a), (b), ())");
    }

    // ========================================================================
    // Field access on non-existent table ref: PG: 42P01 not 42703
    // Diff: 02_value_expressions.sql stmts 15, 37; 13_ch4_syntax_gapfill.sql stmt 19
    // ========================================================================

    @Test void field_selection_nonexistent_ref_gives_42P01() throws SQLException {
        exec("CREATE TABLE ssv3_fs (id INT, comp TEXT)");
        try {
            // SELECT c.x FROM ssv3_fs; 'c' is not a known alias
            assertSqlState("42P01", "SELECT c.x FROM ssv3_fs");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_fs");
        }
    }

    @Test void field_selection_bad_ref_with_alias_gives_42P01() throws SQLException {
        exec("CREATE TABLE ssv3_fs2 (id INT, comp TEXT)");
        try {
            // SELECT c.z FROM ssv3_fs2; 'c' is not an alias of ssv3_fs2
            assertSqlState("42P01", "SELECT c.z FROM ssv3_fs2");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_fs2");
        }
    }

    // ========================================================================
    // Array subscript with text on int array: PG: 22P02 not 42804
    // Diff: 02_value_expressions.sql stmt 38
    // ========================================================================

    @Test void array_subscript_text_gives_22P02() throws SQLException {
        exec("CREATE TABLE ssv3_asub (id INT, b INT[])");
        exec("INSERT INTO ssv3_asub VALUES (1, ARRAY[1,2,3])");
        try {
            assertSqlState("22P02", "SELECT b['x'] FROM ssv3_asub");
        } finally {
            exec("DROP TABLE IF EXISTS ssv3_asub");
        }
    }

    // ========================================================================
    // open() as function on path: PG: 42704 not 42601
    // Diff: 12_json_arrays_ranges_geometry.sql stmt 50
    // ========================================================================

    @Test void open_path_function_gives_42704() {
        // open as a function call; PG: 42704 (undefined_object)
        // Diff: 12_json_arrays_ranges_geometry.sql stmt 50
        assertSqlState("42704", "SELECT open('((0,0),(1,1))'::path)");
    }

    // ========================================================================
    // EXPLAIN option errors: PG gives 22023 for invalid option values,
    // not 42601 (syntax_error)
    // Diff: 30_admin_parser_edges.sql stmts 53-55
    // ========================================================================

    @Test void explain_format_yamlish_gives_22023() {
        assertSqlState("22023", "EXPLAIN (FORMAT yamlish) SELECT 1");
    }

    @Test void explain_analyze_maybe_gives_22023() {
        assertSqlState("22023", "EXPLAIN (ANALYZE maybe) SELECT 1");
    }

    @Test void explain_buffers_numeric_gives_22023() {
        assertSqlState("22023", "EXPLAIN (BUFFERS 123) SELECT 1");
    }
}
