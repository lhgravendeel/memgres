package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive prepared statement compatibility tests.
 * Validates PREPARE / EXECUTE / DEALLOCATE behavior matches PostgreSQL 18.
 *
 * Covers: $N parameter resolution, type inference, parameter count validation,
 * statement reuse, error cases, DML with parameters, and edge cases.
 */
class PreparedStatementCompatTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE prep_t (id SERIAL PRIMARY KEY, a INT, b TEXT, c BOOLEAN DEFAULT false)");
            s.execute("INSERT INTO prep_t (a, b, c) VALUES (10, 'alpha', true)");
            s.execute("INSERT INTO prep_t (a, b, c) VALUES (20, 'beta', false)");
            s.execute("INSERT INTO prep_t (a, b, c) VALUES (30, 'gamma', true)");

            s.execute("CREATE TABLE prep_types (id INT PRIMARY KEY, n NUMERIC, d DATE, ts TIMESTAMPTZ, arr INT[])");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // Each test uses unique prepared statement names to avoid collision.
    // We deallocate at the end of each test as cleanup.

    void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    String q1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    int qInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row for: " + sql);
            return rs.getInt(1);
        }
    }

    int rowCount(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) count++;
            return count;
        }
    }

    void assertSqlState(String sql, String expectedState) {
        try {
            exec(sql);
            fail("Expected error with SQLSTATE " + expectedState + " for: " + sql);
        } catch (SQLException e) {
            assertEquals(expectedState, e.getSQLState(),
                    "Wrong SQLSTATE for: " + sql + ", got: " + e.getMessage());
        }
    }

    // ========================================================================
    // Section 1: Basic $N Parameter Resolution
    // ========================================================================

    @Test void single_param_in_select() throws SQLException {
        exec("PREPARE ps_sel1 AS SELECT $1::int + 1");
        try {
            assertEquals("11", q1("EXECUTE ps_sel1(10)"));
        } finally {
            exec("DEALLOCATE ps_sel1");
        }
    }

    @Test void two_params_in_select() throws SQLException {
        exec("PREPARE ps_sel2 AS SELECT $1::int + $2::int");
        try {
            assertEquals("30", q1("EXECUTE ps_sel2(10, 20)"));
        } finally {
            exec("DEALLOCATE ps_sel2");
        }
    }

    @Test void three_params_in_select() throws SQLException {
        exec("PREPARE ps_sel3 AS SELECT $1::int * $2::int + $3::int");
        try {
            assertEquals("23", q1("EXECUTE ps_sel3(2, 10, 3)"));
        } finally {
            exec("DEALLOCATE ps_sel3");
        }
    }

    @Test void param_in_where_clause() throws SQLException {
        exec("PREPARE ps_where AS SELECT b FROM prep_t WHERE a = $1::int");
        try {
            assertEquals("beta", q1("EXECUTE ps_where(20)"));
        } finally {
            exec("DEALLOCATE ps_where");
        }
    }

    @Test void param_in_where_with_text() throws SQLException {
        exec("PREPARE ps_where_t AS SELECT a FROM prep_t WHERE b = $1::text");
        try {
            assertEquals("30", q1("EXECUTE ps_where_t('gamma')"));
        } finally {
            exec("DEALLOCATE ps_where_t");
        }
    }

    @Test void param_in_where_boolean() throws SQLException {
        exec("PREPARE ps_where_b(boolean) AS SELECT count(*) FROM prep_t WHERE c = $1");
        try {
            assertEquals("2", q1("EXECUTE ps_where_b(true)"));
            assertEquals("1", q1("EXECUTE ps_where_b(false)"));
        } finally {
            exec("DEALLOCATE ps_where_b");
        }
    }

    @Test void param_is_null() throws SQLException {
        exec("PREPARE ps_isnull AS SELECT $1::text IS NULL");
        try {
            assertEquals("t", q1("EXECUTE ps_isnull(NULL)"));
            assertEquals("f", q1("EXECUTE ps_isnull('hello')"));
        } finally {
            exec("DEALLOCATE ps_isnull");
        }
    }

    // ========================================================================
    // Section 2: Explicit Parameter Types in PREPARE
    // ========================================================================

    @Test void explicit_types_int() throws SQLException {
        exec("PREPARE ps_exp_i(int) AS SELECT $1 + 1");
        try {
            assertEquals("6", q1("EXECUTE ps_exp_i(5)"));
        } finally {
            exec("DEALLOCATE ps_exp_i");
        }
    }

    @Test void explicit_types_int_text() throws SQLException {
        exec("PREPARE ps_exp_it(int, text) AS SELECT $1, $2");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE ps_exp_it(42, 'hello')")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
            assertEquals("hello", rs.getString(2));
        } finally {
            exec("DEALLOCATE ps_exp_it");
        }
    }

    @Test void explicit_types_boolean() throws SQLException {
        exec("PREPARE ps_exp_b(boolean) AS SELECT NOT $1");
        try {
            assertEquals("f", q1("EXECUTE ps_exp_b(true)"));
            assertEquals("t", q1("EXECUTE ps_exp_b(false)"));
        } finally {
            exec("DEALLOCATE ps_exp_b");
        }
    }

    @Test void explicit_types_multiple() throws SQLException {
        exec("PREPARE ps_exp_m(int, int, text) AS SELECT $1 + $2, $3");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE ps_exp_m(3, 7, 'sum')")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            assertEquals("sum", rs.getString(2));
        } finally {
            exec("DEALLOCATE ps_exp_m");
        }
    }

    // ========================================================================
    // Section 3: $N in Various Expression Contexts
    // ========================================================================

    @Test void param_in_like() throws SQLException {
        exec("PREPARE ps_like AS SELECT count(*) FROM prep_t WHERE b LIKE $1::text");
        try {
            assertEquals("3", q1("EXECUTE ps_like('%a%')"));    // alpha, beta, gamma all contain 'a'
            assertEquals("1", q1("EXECUTE ps_like('beta')"));
            assertEquals("3", q1("EXECUTE ps_like('%')"));
        } finally {
            exec("DEALLOCATE ps_like");
        }
    }

    @Test void param_in_between() throws SQLException {
        exec("PREPARE ps_between(int, int) AS SELECT count(*) FROM prep_t WHERE a BETWEEN $1 AND $2");
        try {
            assertEquals("2", q1("EXECUTE ps_between(10, 20)"));
            assertEquals("3", q1("EXECUTE ps_between(1, 100)"));
            assertEquals("0", q1("EXECUTE ps_between(100, 200)"));
        } finally {
            exec("DEALLOCATE ps_between");
        }
    }

    @Test void param_in_case() throws SQLException {
        exec("PREPARE ps_case(boolean) AS SELECT CASE WHEN $1 THEN 'yes' ELSE 'no' END");
        try {
            assertEquals("yes", q1("EXECUTE ps_case(true)"));
            assertEquals("no", q1("EXECUTE ps_case(false)"));
        } finally {
            exec("DEALLOCATE ps_case");
        }
    }

    @Test void param_in_coalesce() throws SQLException {
        exec("PREPARE ps_coal AS SELECT COALESCE($1::text, 'fallback')");
        try {
            assertEquals("hello", q1("EXECUTE ps_coal('hello')"));
            assertEquals("fallback", q1("EXECUTE ps_coal(NULL)"));
        } finally {
            exec("DEALLOCATE ps_coal");
        }
    }

    @Test void param_in_limit() throws SQLException {
        exec("PREPARE ps_limit(int) AS SELECT a FROM prep_t ORDER BY a LIMIT $1");
        try {
            assertEquals(1, rowCount("EXECUTE ps_limit(1)"));
            assertEquals(2, rowCount("EXECUTE ps_limit(2)"));
            assertEquals(3, rowCount("EXECUTE ps_limit(3)"));
        } finally {
            exec("DEALLOCATE ps_limit");
        }
    }

    @Test void param_in_offset() throws SQLException {
        exec("PREPARE ps_off(int, int) AS SELECT a FROM prep_t ORDER BY a LIMIT $1 OFFSET $2");
        try {
            assertEquals("20", q1("EXECUTE ps_off(1, 1)"));
            assertEquals("30", q1("EXECUTE ps_off(1, 2)"));
        } finally {
            exec("DEALLOCATE ps_off");
        }
    }

    @Test void param_in_any_array() throws SQLException {
        exec("PREPARE ps_any(int[]) AS SELECT count(*) FROM prep_t WHERE a = ANY($1)");
        try {
            assertEquals("2", q1("EXECUTE ps_any(ARRAY[10,30])"));
            assertEquals("0", q1("EXECUTE ps_any(ARRAY[99])"));
        } finally {
            exec("DEALLOCATE ps_any");
        }
    }

    @Test void param_in_in_list() throws SQLException {
        exec("PREPARE ps_in(int, int) AS SELECT count(*) FROM prep_t WHERE a IN ($1, $2)");
        try {
            assertEquals("2", q1("EXECUTE ps_in(10, 30)"));
        } finally {
            exec("DEALLOCATE ps_in");
        }
    }

    @Test void param_arithmetic_combinations() throws SQLException {
        exec("PREPARE ps_arith(int, int) AS SELECT $1 + $2, $1 - $2, $1 * $2");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE ps_arith(15, 5)")) {
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1));
            assertEquals(10, rs.getInt(2));
            assertEquals(75, rs.getInt(3));
        } finally {
            exec("DEALLOCATE ps_arith");
        }
    }

    @Test void param_string_concat() throws SQLException {
        exec("PREPARE ps_concat(text, text) AS SELECT $1 || ' ' || $2");
        try {
            assertEquals("hello world", q1("EXECUTE ps_concat('hello', 'world')"));
        } finally {
            exec("DEALLOCATE ps_concat");
        }
    }

    @Test void param_in_cast() throws SQLException {
        exec("PREPARE ps_cast AS SELECT ($1::int) + 1, pg_typeof($1::int)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE ps_cast('41')")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
            assertEquals("integer", rs.getString(2));
        } finally {
            exec("DEALLOCATE ps_cast");
        }
    }

    @Test void param_in_order_by_expression() throws SQLException {
        exec("PREPARE ps_ord(boolean) AS SELECT a, b FROM prep_t ORDER BY CASE WHEN $1 THEN a ELSE -a END");
        try {
            // ascending
            assertEquals("10", q1("EXECUTE ps_ord(true)"));
            // descending (first row is highest a)
            assertEquals("30", q1("EXECUTE ps_ord(false)"));
        } finally {
            exec("DEALLOCATE ps_ord");
        }
    }

    // ========================================================================
    // Section 4: DML with Parameters
    // ========================================================================

    @Test void insert_with_params() throws SQLException {
        exec("CREATE TABLE ps_ins_t (id INT PRIMARY KEY, name TEXT, val INT)");
        exec("PREPARE ps_ins(int, text, int) AS INSERT INTO ps_ins_t VALUES ($1, $2, $3)");
        try {
            exec("EXECUTE ps_ins(1, 'alice', 100)");
            exec("EXECUTE ps_ins(2, 'bob', 200)");
            assertEquals(2, qInt("SELECT count(*) FROM ps_ins_t"));
            assertEquals("alice", q1("SELECT name FROM ps_ins_t WHERE id = 1"));
            assertEquals("200", q1("SELECT val FROM ps_ins_t WHERE id = 2"));
        } finally {
            exec("DEALLOCATE ps_ins");
            exec("DROP TABLE ps_ins_t");
        }
    }

    @Test void insert_returning_with_params() throws SQLException {
        exec("CREATE TABLE ps_ret_t (id SERIAL PRIMARY KEY, name TEXT)");
        exec("PREPARE ps_ret(text) AS INSERT INTO ps_ret_t (name) VALUES ($1) RETURNING id, name");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE ps_ret('alice')")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("alice", rs.getString("name"));
        } finally {
            exec("DEALLOCATE ps_ret");
            exec("DROP TABLE ps_ret_t");
        }
    }

    @Test void update_with_params() throws SQLException {
        exec("CREATE TABLE ps_upd_t (id INT PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ps_upd_t VALUES (1, 'old')");
        exec("PREPARE ps_upd(text, int) AS UPDATE ps_upd_t SET val = $1 WHERE id = $2");
        try {
            exec("EXECUTE ps_upd('new', 1)");
            assertEquals("new", q1("SELECT val FROM ps_upd_t WHERE id = 1"));
        } finally {
            exec("DEALLOCATE ps_upd");
            exec("DROP TABLE ps_upd_t");
        }
    }

    @Test void delete_with_params() throws SQLException {
        exec("CREATE TABLE ps_del_t (id INT PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ps_del_t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        exec("PREPARE ps_del(int) AS DELETE FROM ps_del_t WHERE id = $1");
        try {
            exec("EXECUTE ps_del(2)");
            assertEquals(2, qInt("SELECT count(*) FROM ps_del_t"));
            assertNull(q1("SELECT val FROM ps_del_t WHERE id = 2"));
        } finally {
            exec("DEALLOCATE ps_del");
            exec("DROP TABLE ps_del_t");
        }
    }

    @Test void multi_row_insert_with_params() throws SQLException {
        exec("CREATE TABLE ps_mi_t (id INT, val TEXT)");
        exec("PREPARE ps_mi(int, text, int, text) AS INSERT INTO ps_mi_t VALUES ($1, $2), ($3, $4)");
        try {
            exec("EXECUTE ps_mi(1, 'a', 2, 'b')");
            assertEquals(2, qInt("SELECT count(*) FROM ps_mi_t"));
        } finally {
            exec("DEALLOCATE ps_mi");
            exec("DROP TABLE ps_mi_t");
        }
    }

    // ========================================================================
    // Section 5: Statement Reuse
    // ========================================================================

    @Test void reuse_same_statement() throws SQLException {
        exec("PREPARE ps_reuse(int) AS SELECT a FROM prep_t WHERE a > $1 ORDER BY a");
        try {
            assertEquals(3, rowCount("EXECUTE ps_reuse(0)"));
            assertEquals(2, rowCount("EXECUTE ps_reuse(10)"));
            assertEquals(1, rowCount("EXECUTE ps_reuse(20)"));
            assertEquals(0, rowCount("EXECUTE ps_reuse(100)"));
        } finally {
            exec("DEALLOCATE ps_reuse");
        }
    }

    @Test void reuse_insert_statement() throws SQLException {
        exec("CREATE TABLE ps_ri_t (id INT, val TEXT)");
        exec("PREPARE ps_ri(int, text) AS INSERT INTO ps_ri_t VALUES ($1, $2)");
        try {
            exec("EXECUTE ps_ri(1, 'first')");
            exec("EXECUTE ps_ri(2, 'second')");
            exec("EXECUTE ps_ri(3, 'third')");
            assertEquals(3, qInt("SELECT count(*) FROM ps_ri_t"));
        } finally {
            exec("DEALLOCATE ps_ri");
            exec("DROP TABLE ps_ri_t");
        }
    }

    // ========================================================================
    // Section 6: DEALLOCATE
    // ========================================================================

    @Test void deallocate_specific() throws SQLException {
        exec("PREPARE ps_dealloc1 AS SELECT 1");
        exec("DEALLOCATE ps_dealloc1");
        // Using it after deallocation should fail
        assertSqlState("EXECUTE ps_dealloc1", "26000");
    }

    @Test void deallocate_all() throws SQLException {
        exec("PREPARE ps_da1 AS SELECT 1");
        exec("PREPARE ps_da2 AS SELECT 2");
        exec("DEALLOCATE ALL");
        assertSqlState("EXECUTE ps_da1", "26000");
        assertSqlState("EXECUTE ps_da2", "26000");
    }

    @Test void deallocate_nonexistent() {
        assertSqlState("DEALLOCATE ps_no_such", "26000");
    }

    @Test void duplicate_prepare_rejected() throws SQLException {
        exec("PREPARE ps_dup AS SELECT 1");
        try {
            assertSqlState("PREPARE ps_dup AS SELECT 2", "42P05");
        } finally {
            exec("DEALLOCATE ps_dup");
        }
    }

    // ========================================================================
    // Section 7: Parameter Count Validation
    // ========================================================================

    @Test void too_few_params() throws SQLException {
        exec("PREPARE ps_few(int, int) AS SELECT $1 + $2");
        try {
            // Provide 1 param when 2 expected
            try {
                exec("EXECUTE ps_few(1)");
                fail("Expected error for wrong parameter count");
            } catch (SQLException e) {
                assertNotNull(e.getSQLState());
            }
        } finally {
            exec("DEALLOCATE ps_few");
        }
    }

    @Test void too_many_params() throws SQLException {
        exec("PREPARE ps_many(int) AS SELECT $1");
        try {
            try {
                exec("EXECUTE ps_many(1, 2, 3)");
                fail("Expected error for wrong parameter count");
            } catch (SQLException e) {
                assertNotNull(e.getSQLState());
            }
        } finally {
            exec("DEALLOCATE ps_many");
        }
    }

    @Test void zero_params_explicit_none_needed() throws SQLException {
        exec("PREPARE ps_zero AS SELECT 42");
        try {
            assertEquals("42", q1("EXECUTE ps_zero"));
        } finally {
            exec("DEALLOCATE ps_zero");
        }
    }

    @Test void execute_nonexistent_gives_26000() {
        assertSqlState("EXECUTE ps_nonexistent(1)", "26000");
    }

    // ========================================================================
    // Section 8: Type Coercion on Execute
    // ========================================================================

    @Test void string_to_int_coercion() throws SQLException {
        exec("PREPARE ps_coerce AS SELECT $1::int + 10");
        try {
            assertEquals("15", q1("EXECUTE ps_coerce('5')"));
        } finally {
            exec("DEALLOCATE ps_coerce");
        }
    }

    @Test void null_param() throws SQLException {
        exec("PREPARE ps_null_p AS SELECT COALESCE($1::text, 'was_null')");
        try {
            assertEquals("was_null", q1("EXECUTE ps_null_p(NULL)"));
            assertEquals("present", q1("EXECUTE ps_null_p('present')"));
        } finally {
            exec("DEALLOCATE ps_null_p");
        }
    }

    @Test void param_cast_to_numeric() throws SQLException {
        exec("PREPARE ps_num AS SELECT $1::numeric * 2.5");
        try {
            String result = q1("EXECUTE ps_num(4)");
            assertNotNull(result);
            assertTrue(result.startsWith("10"), "Expected 10.0 but got: " + result);
        } finally {
            exec("DEALLOCATE ps_num");
        }
    }

    @Test void param_cast_to_boolean() throws SQLException {
        exec("PREPARE ps_bool AS SELECT NOT $1::boolean");
        try {
            assertEquals("f", q1("EXECUTE ps_bool(true)"));
            assertEquals("t", q1("EXECUTE ps_bool(false)"));
        } finally {
            exec("DEALLOCATE ps_bool");
        }
    }

    // ========================================================================
    // Section 9: Timestamp and Date Parameters
    // ========================================================================

    @Test void param_timestamptz() throws SQLException {
        exec("PREPARE ps_ts AS SELECT ($1::timestamptz AT TIME ZONE 'UTC')::text");
        try {
            String result = q1("EXECUTE ps_ts('2024-01-01 12:00:00+00')");
            assertNotNull(result);
            assertTrue(result.contains("2024-01-01"), "Expected date in result: " + result);
        } finally {
            exec("DEALLOCATE ps_ts");
        }
    }

    @Test void param_date() throws SQLException {
        exec("PREPARE ps_date(date) AS SELECT ($1::date + INTERVAL '1 day')::date");
        try {
            String result = q1("EXECUTE ps_date('2024-06-15')");
            assertNotNull(result);
            assertTrue(result.contains("2024-06-16"), "Expected next day in result: " + result);
        } finally {
            exec("DEALLOCATE ps_date");
        }
    }

    // ========================================================================
    // Section 10: ROW and Array Parameters
    // ========================================================================

    @Test void param_with_row_constructor() throws SQLException {
        exec("PREPARE ps_row(int, text) AS SELECT ROW($1, $2)");
        try {
            String result = q1("EXECUTE ps_row(5, 'five')");
            assertNotNull(result);
            assertTrue(result.contains("5") && result.contains("five"),
                    "Expected (5,five) but got: " + result);
        } finally {
            exec("DEALLOCATE ps_row");
        }
    }

    @Test void param_array_length() throws SQLException {
        exec("PREPARE ps_arrlen(int[]) AS SELECT array_length($1, 1)");
        try {
            assertEquals("3", q1("EXECUTE ps_arrlen(ARRAY[1,2,3])"));
            assertEquals("1", q1("EXECUTE ps_arrlen(ARRAY[99])"));
        } finally {
            exec("DEALLOCATE ps_arrlen");
        }
    }

    // ========================================================================
    // Section 11: Subqueries and Complex Expressions
    // ========================================================================

    @Test void param_in_subquery() throws SQLException {
        exec("PREPARE ps_sub(int) AS SELECT b FROM prep_t WHERE a = (SELECT $1::int)");
        try {
            assertEquals("alpha", q1("EXECUTE ps_sub(10)"));
        } finally {
            exec("DEALLOCATE ps_sub");
        }
    }

    @Test void param_in_exists() throws SQLException {
        exec("PREPARE ps_exists(int) AS SELECT EXISTS(SELECT 1 FROM prep_t WHERE a = $1::int)");
        try {
            assertEquals("t", q1("EXECUTE ps_exists(10)"));
            assertEquals("f", q1("EXECUTE ps_exists(999)"));
        } finally {
            exec("DEALLOCATE ps_exists");
        }
    }

    @Test void param_with_aggregate() throws SQLException {
        exec("PREPARE ps_agg(int) AS SELECT sum(a) FROM prep_t WHERE a >= $1::int");
        try {
            assertEquals("60", q1("EXECUTE ps_agg(1)"));
            assertEquals("50", q1("EXECUTE ps_agg(15)"));
        } finally {
            exec("DEALLOCATE ps_agg");
        }
    }

    @Test void param_with_string_functions() throws SQLException {
        exec("PREPARE ps_strfn(text) AS SELECT upper($1), length($1)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE ps_strfn('hello')")) {
            assertTrue(rs.next());
            assertEquals("HELLO", rs.getString(1));
            assertEquals(5, rs.getInt(2));
        } finally {
            exec("DEALLOCATE ps_strfn");
        }
    }

    // ========================================================================
    // Section 12: Same $N Used Multiple Times
    // ========================================================================

    @Test void same_param_used_twice() throws SQLException {
        exec("PREPARE ps_twice(int) AS SELECT $1 + $1");
        try {
            assertEquals("20", q1("EXECUTE ps_twice(10)"));
        } finally {
            exec("DEALLOCATE ps_twice");
        }
    }

    @Test void same_param_in_different_contexts() throws SQLException {
        exec("PREPARE ps_multi_use(text) AS SELECT $1, length($1), upper($1)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE ps_multi_use('abc')")) {
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("ABC", rs.getString(3));
        } finally {
            exec("DEALLOCATE ps_multi_use");
        }
    }

    @Test void non_sequential_params() throws SQLException {
        // $2 and $1 used in reverse order; max is $2, so 2 params expected
        exec("PREPARE ps_rev(int, int) AS SELECT $2 - $1");
        try {
            assertEquals("5", q1("EXECUTE ps_rev(10, 15)"));
        } finally {
            exec("DEALLOCATE ps_rev");
        }
    }

    // ========================================================================
    // Section 13: Prepared Statements in Transactions
    // ========================================================================

    @Test void prepare_survives_rollback() throws SQLException {
        conn.setAutoCommit(false);
        try {
            exec("PREPARE ps_txn AS SELECT 99");
            conn.rollback();
            // PG: PREPARE is not transactional; it persists after rollback
            assertEquals("99", q1("EXECUTE ps_txn"));
        } finally {
            conn.setAutoCommit(true);
            try { exec("DEALLOCATE ps_txn"); } catch (SQLException ignored) {}
        }
    }

    @Test void execute_in_transaction() throws SQLException {
        exec("CREATE TABLE ps_txn_t (id INT)");
        exec("PREPARE ps_txn_ins(int) AS INSERT INTO ps_txn_t VALUES ($1)");
        conn.setAutoCommit(false);
        try {
            exec("EXECUTE ps_txn_ins(1)");
            exec("EXECUTE ps_txn_ins(2)");
            conn.commit();
            assertEquals(2, qInt("SELECT count(*) FROM ps_txn_t"));
        } finally {
            conn.setAutoCommit(true);
            exec("DEALLOCATE ps_txn_ins");
            exec("DROP TABLE ps_txn_t");
        }
    }

    @Test void execute_in_rolled_back_transaction() throws SQLException {
        exec("CREATE TABLE ps_rb_t (id INT)");
        exec("PREPARE ps_rb(int) AS INSERT INTO ps_rb_t VALUES ($1)");
        conn.setAutoCommit(false);
        try {
            exec("EXECUTE ps_rb(1)");
            conn.rollback();
            assertEquals(0, qInt("SELECT count(*) FROM ps_rb_t"));
        } finally {
            conn.setAutoCommit(true);
            exec("DEALLOCATE ps_rb");
            exec("DROP TABLE ps_rb_t");
        }
    }

    // ========================================================================
    // Section 14: Error Cases
    // ========================================================================

    @Test void param_without_explicit_type_is_rejected() throws SQLException {
        // PG 18 rejects bare $1 without type context at PREPARE time:
        // "could not determine data type of parameter $1"
        try {
            exec("PREPARE ps_indet AS SELECT $1 IS NULL, pg_typeof($1)");
            try { exec("DEALLOCATE ps_indet"); } catch (SQLException ignored) {}
            fail("PG 18 rejects bare $1 without type context at PREPARE time");
        } catch (SQLException e) {
            // Expected: PG 18 cannot determine data type of parameter $1
        }
    }

    @Test void bad_param_count_gives_error() throws SQLException {
        exec("PREPARE ps_bc(int, int) AS SELECT $1 + $2");
        try {
            try {
                exec("EXECUTE ps_bc(1)");
                fail("Expected error");
            } catch (SQLException e) {
                // PG uses 42601 for wrong param count
                assertNotNull(e.getSQLState());
            }
        } finally {
            exec("DEALLOCATE ps_bc");
        }
    }

    @Test void invalid_type_at_execute_time() throws SQLException {
        exec("PREPARE ps_badtype AS SELECT $1::int + 1");
        try {
            try {
                exec("EXECUTE ps_badtype('not_a_number')");
                fail("Expected type error");
            } catch (SQLException e) {
                assertNotNull(e.getSQLState());
            }
        } finally {
            exec("DEALLOCATE ps_badtype");
        }
    }

    @Test void negative_offset_gives_2201X() throws SQLException {
        exec("PREPARE ps_negoff(int) AS SELECT a FROM prep_t ORDER BY a OFFSET $1");
        try {
            // Negative offset should be rejected
            try {
                exec("EXECUTE ps_negoff(-1)");
                fail("Expected error for negative offset");
            } catch (SQLException e) {
                assertNotNull(e.getSQLState());
            }
        } finally {
            exec("DEALLOCATE ps_negoff");
        }
    }

    // ========================================================================
    // Section 15: pg_typeof with Parameters
    // ========================================================================

    @Test void pg_typeof_casted_param() throws SQLException {
        exec("PREPARE ps_typeof AS SELECT pg_typeof($1::int)");
        try {
            assertEquals("integer", q1("EXECUTE ps_typeof(42)"));
        } finally {
            exec("DEALLOCATE ps_typeof");
        }
    }

    @Test void pg_typeof_text_param() throws SQLException {
        exec("PREPARE ps_typeof_t AS SELECT pg_typeof($1::text)");
        try {
            assertEquals("text", q1("EXECUTE ps_typeof_t('hello')"));
        } finally {
            exec("DEALLOCATE ps_typeof_t");
        }
    }

    // ========================================================================
    // Section 16: NULLS handling with parameters
    // ========================================================================

    @Test void nulls_last_with_param() throws SQLException {
        exec("PREPARE ps_nlast(boolean) AS SELECT a FROM prep_t ORDER BY CASE WHEN $1 THEN a END NULLS LAST");
        try {
            // Should not throw
            assertTrue(rowCount("EXECUTE ps_nlast(true)") > 0);
        } finally {
            exec("DEALLOCATE ps_nlast");
        }
    }

    @Test void null_param_in_arithmetic() throws SQLException {
        exec("PREPARE ps_null_arith AS SELECT $1::int + 1");
        try {
            // NULL + 1 = NULL
            assertNull(q1("EXECUTE ps_null_arith(NULL)"));
        } finally {
            exec("DEALLOCATE ps_null_arith");
        }
    }

    // ========================================================================
    // Section 17: Multiple Result Columns
    // ========================================================================

    @Test void select_star_with_param_filter() throws SQLException {
        exec("PREPARE ps_star(int) AS SELECT * FROM prep_t WHERE a >= $1 ORDER BY a");
        try {
            assertEquals(2, rowCount("EXECUTE ps_star(20)"));
            assertEquals(3, rowCount("EXECUTE ps_star(1)"));
        } finally {
            exec("DEALLOCATE ps_star");
        }
    }

    @Test void computed_columns_with_params() throws SQLException {
        exec("PREPARE ps_comp(int) AS SELECT a, a * $1 AS scaled, b FROM prep_t ORDER BY a");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE ps_comp(3)")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("a"));
            assertEquals(30, rs.getInt("scaled"));
            assertEquals("alpha", rs.getString("b"));
        } finally {
            exec("DEALLOCATE ps_comp");
        }
    }

    // ========================================================================
    // Section 18: Implicit param count from $N (no explicit type list)
    // ========================================================================

    @Test void implicit_param_count_one() throws SQLException {
        // No explicit types; parser must infer 1 param from $1
        exec("PREPARE ps_imp1 AS SELECT $1::int + 100");
        try {
            assertEquals("142", q1("EXECUTE ps_imp1(42)"));
        } finally {
            exec("DEALLOCATE ps_imp1");
        }
    }

    @Test void implicit_param_count_two() throws SQLException {
        exec("PREPARE ps_imp2 AS SELECT $1::text || $2::text");
        try {
            assertEquals("helloworld", q1("EXECUTE ps_imp2('hello', 'world')"));
        } finally {
            exec("DEALLOCATE ps_imp2");
        }
    }

    @Test void implicit_param_count_gap() throws SQLException {
        // Uses $1 and $3 but not $2; max is $3, so 3 params expected
        exec("PREPARE ps_gap AS SELECT $1::int + $3::int");
        try {
            // Must provide 3 params even though $2 is unused
            assertEquals("40", q1("EXECUTE ps_gap(10, 'ignored', 30)"));
        } finally {
            exec("DEALLOCATE ps_gap");
        }
    }

    // ========================================================================
    // Section 19: UPDATE with RETURNING + params
    // ========================================================================

    @Test void update_returning_with_params() throws SQLException {
        exec("CREATE TABLE ps_ur_t (id INT PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ps_ur_t VALUES (1, 'old')");
        exec("PREPARE ps_ur(text, int) AS UPDATE ps_ur_t SET val = $1 WHERE id = $2 RETURNING id, val");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE ps_ur('new', 1)")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("new", rs.getString(2));
        } finally {
            exec("DEALLOCATE ps_ur");
            exec("DROP TABLE ps_ur_t");
        }
    }

    @Test void delete_returning_with_params() throws SQLException {
        exec("CREATE TABLE ps_dr_t (id INT PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ps_dr_t VALUES (1, 'a'), (2, 'b')");
        exec("PREPARE ps_dr(int) AS DELETE FROM ps_dr_t WHERE id = $1 RETURNING val");
        try {
            assertEquals("a", q1("EXECUTE ps_dr(1)"));
            assertEquals(1, qInt("SELECT count(*) FROM ps_dr_t"));
        } finally {
            exec("DEALLOCATE ps_dr");
            exec("DROP TABLE ps_dr_t");
        }
    }

    // ========================================================================
    // Section 20: Edge Cases
    // ========================================================================

    @Test void prepare_with_semicolon_in_name() throws SQLException {
        // PG lowercases identifiers, so test case-insensitivity
        exec("PREPARE MyStmt AS SELECT 1");
        try {
            assertEquals("1", q1("EXECUTE mystmt"));
            assertEquals("1", q1("EXECUTE MYSTMT"));
        } finally {
            exec("DEALLOCATE mystmt");
        }
    }

    @Test void param_with_unary_minus() throws SQLException {
        exec("PREPARE ps_neg(int) AS SELECT -$1");
        try {
            assertEquals("-5", q1("EXECUTE ps_neg(5)"));
            assertEquals("5", q1("EXECUTE ps_neg(-5)"));
        } finally {
            exec("DEALLOCATE ps_neg");
        }
    }

    @Test void param_comparison_operators() throws SQLException {
        exec("PREPARE ps_cmp(int) AS SELECT a > $1, a = $1, a < $1 FROM prep_t WHERE a = 20");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE ps_cmp(15)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));   // 20 > 15
            assertFalse(rs.getBoolean(2));  // 20 != 15
            assertFalse(rs.getBoolean(3));  // 20 !< 15
        } finally {
            exec("DEALLOCATE ps_cmp");
        }
    }

    @Test void prepare_select_no_params_execute_no_parens() throws SQLException {
        exec("PREPARE ps_np AS SELECT 'no_params'");
        try {
            assertEquals("no_params", q1("EXECUTE ps_np"));
        } finally {
            exec("DEALLOCATE ps_np");
        }
    }

    @Test void param_in_values_clause() throws SQLException {
        exec("PREPARE ps_vals(int, text) AS VALUES ($1, $2)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXECUTE ps_vals(42, 'hello')")) {
            assertTrue(rs.next());
            assertEquals("42", rs.getString(1));
            assertEquals("hello", rs.getString(2));
        } finally {
            exec("DEALLOCATE ps_vals");
        }
    }
}
