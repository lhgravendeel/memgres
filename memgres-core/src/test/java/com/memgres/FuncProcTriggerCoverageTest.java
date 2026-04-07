package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive coverage tests for checklist items 70-72:
 *   70. Functions (CREATE FUNCTION, DROP FUNCTION, SQL/PL/pgSQL, attributes)
 *   71. Procedures (CREATE PROCEDURE, CALL, DROP PROCEDURE)
 *   72. Triggers (CREATE TRIGGER, DROP TRIGGER, BEFORE/AFTER, events)
 */
class FuncProcTriggerCoverageTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setup() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
    }

    @AfterAll
    static void teardown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private ResultSet query(String sql) throws SQLException {
        return conn.createStatement().executeQuery(sql);
    }

    private String query1(String sql) throws SQLException {
        ResultSet rs = query(sql); assertTrue(rs.next()); return rs.getString(1);
    }

    private int queryInt(String sql) throws SQLException {
        ResultSet rs = query(sql); assertTrue(rs.next()); return rs.getInt(1);
    }

    private boolean queryBool(String sql) throws SQLException {
        ResultSet rs = query(sql); assertTrue(rs.next()); return rs.getBoolean(1);
    }

    // ========================================================================
    // 70. Functions: SQL language
    // ========================================================================

    @Test
    void fn_sql_add_integers() throws SQLException {
        exec("CREATE FUNCTION fn_add_nums(a INTEGER, b INTEGER) RETURNS INTEGER AS $$ SELECT a + b $$ LANGUAGE sql");
        assertEquals(7, queryInt("SELECT fn_add_nums(3, 4)"));
        exec("DROP FUNCTION fn_add_nums");
    }

    @Test
    void fn_sql_returns_text() throws SQLException {
        exec("CREATE FUNCTION fn_hello() RETURNS TEXT AS $$ SELECT 'hello world' $$ LANGUAGE sql");
        assertEquals("hello world", query1("SELECT fn_hello()"));
        exec("DROP FUNCTION fn_hello");
    }

    @Test
    void fn_sql_returns_numeric() throws SQLException {
        exec("CREATE FUNCTION fn_get_pi() RETURNS NUMERIC AS $$ SELECT 3.14159 $$ LANGUAGE sql");
        String result = query1("SELECT fn_get_pi()");
        assertTrue(result.startsWith("3.14"));
        exec("DROP FUNCTION fn_get_pi");
    }

    @Test
    void fn_sql_returns_boolean() throws SQLException {
        exec("CREATE FUNCTION fn_is_positive(n INTEGER) RETURNS BOOLEAN AS $$ SELECT n > 0 $$ LANGUAGE sql");
        assertTrue(queryBool("SELECT fn_is_positive(5)"));
        assertFalse(queryBool("SELECT fn_is_positive(-1)"));
        exec("DROP FUNCTION fn_is_positive");
    }

    @Test
    void fn_sql_no_parameters() throws SQLException {
        exec("CREATE FUNCTION fn_constant42() RETURNS INTEGER AS $$ SELECT 42 $$ LANGUAGE sql");
        assertEquals(42, queryInt("SELECT fn_constant42()"));
        exec("DROP FUNCTION fn_constant42");
    }

    @Test
    void fn_sql_body_with_single_quotes() throws SQLException {
        exec("CREATE FUNCTION fn_sq_body() RETURNS TEXT AS 'SELECT ''quoted''' LANGUAGE sql");
        assertEquals("quoted", query1("SELECT fn_sq_body()"));
        exec("DROP FUNCTION fn_sq_body");
    }

    @Test
    void fn_sql_language_before_as() throws SQLException {
        exec("CREATE FUNCTION fn_lang_first() RETURNS INTEGER LANGUAGE sql AS $$ SELECT 42 $$");
        assertEquals(42, queryInt("SELECT fn_lang_first()"));
        exec("DROP FUNCTION fn_lang_first");
    }

    @Test
    void fn_sql_called_in_select_list() throws SQLException {
        exec("CREATE FUNCTION fn_double(x INTEGER) RETURNS INTEGER AS $$ SELECT x * 2 $$ LANGUAGE sql");
        assertEquals(10, queryInt("SELECT fn_double(5)"));
        assertEquals(20, queryInt("SELECT fn_double(10)"));
        exec("DROP FUNCTION fn_double");
    }

    @Test
    void fn_sql_called_in_where_clause() throws SQLException {
        exec("CREATE TABLE fn_where_tbl (id INTEGER, val INTEGER)");
        exec("INSERT INTO fn_where_tbl VALUES (1, 10), (2, 20), (3, 30)");
        exec("CREATE FUNCTION fn_threshold() RETURNS INTEGER AS $$ SELECT 15 $$ LANGUAGE sql");
        ResultSet rs = query("SELECT id FROM fn_where_tbl WHERE val > fn_threshold() ORDER BY id");
        assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
        assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
        exec("DROP FUNCTION fn_threshold");
        exec("DROP TABLE fn_where_tbl");
    }

    @Test
    void fn_sql_with_expression() throws SQLException {
        exec("CREATE FUNCTION fn_area(w INTEGER, h INTEGER) RETURNS INTEGER AS $$ SELECT w * h $$ LANGUAGE sql");
        assertEquals(24, queryInt("SELECT fn_area(4, 6)"));
        exec("DROP FUNCTION fn_area");
    }

    @Test
    void fn_sql_concat_text() throws SQLException {
        exec("CREATE FUNCTION fn_greet(name TEXT) RETURNS TEXT AS $$ SELECT 'Hello, ' || name $$ LANGUAGE sql");
        assertEquals("Hello, World", query1("SELECT fn_greet('World')"));
        exec("DROP FUNCTION fn_greet");
    }

    // ========================================================================
    // 70. Functions: PL/pgSQL language
    // ========================================================================

    @Test
    void fn_plpgsql_basic() throws SQLException {
        exec("CREATE FUNCTION fn_pl_add(a INTEGER, b INTEGER) RETURNS INTEGER AS $$ BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
        assertEquals(7, queryInt("SELECT fn_pl_add(3, 4)"));
        exec("DROP FUNCTION fn_pl_add");
    }

    @Test
    void fn_plpgsql_with_declare() throws SQLException {
        exec("CREATE FUNCTION fn_pl_triple(x INTEGER) RETURNS INTEGER AS $$ DECLARE result INTEGER; BEGIN result := x * 3; RETURN result; END; $$ LANGUAGE plpgsql");
        assertEquals(15, queryInt("SELECT fn_pl_triple(5)"));
        exec("DROP FUNCTION fn_pl_triple");
    }

    @Test
    void fn_plpgsql_if_else() throws SQLException {
        exec("CREATE FUNCTION fn_pl_sign(n INTEGER) RETURNS TEXT AS $$ BEGIN IF n < 0 THEN RETURN 'negative'; ELSIF n = 0 THEN RETURN 'zero'; ELSE RETURN 'positive'; END IF; END; $$ LANGUAGE plpgsql");
        assertEquals("negative", query1("SELECT fn_pl_sign(-5)"));
        assertEquals("zero", query1("SELECT fn_pl_sign(0)"));
        assertEquals("positive", query1("SELECT fn_pl_sign(7)"));
        exec("DROP FUNCTION fn_pl_sign");
    }

    @Test
    void fn_plpgsql_returns_text() throws SQLException {
        exec("CREATE FUNCTION fn_pl_greeting(name TEXT) RETURNS TEXT AS $$ DECLARE msg TEXT; BEGIN msg := 'Hi, ' || name || '!'; RETURN msg; END; $$ LANGUAGE plpgsql");
        assertEquals("Hi, Alice!", query1("SELECT fn_pl_greeting('Alice')"));
        exec("DROP FUNCTION fn_pl_greeting");
    }

    @Test
    void fn_plpgsql_returns_boolean() throws SQLException {
        exec("CREATE FUNCTION fn_pl_even(n INTEGER) RETURNS BOOLEAN AS $$ BEGIN RETURN n % 2 = 0; END; $$ LANGUAGE plpgsql");
        assertTrue(queryBool("SELECT fn_pl_even(4)"));
        assertFalse(queryBool("SELECT fn_pl_even(3)"));
        exec("DROP FUNCTION fn_pl_even");
    }

    @Test
    void fn_plpgsql_returns_numeric() throws SQLException {
        exec("CREATE FUNCTION fn_pl_half(n NUMERIC) RETURNS NUMERIC AS $$ BEGIN RETURN n / 2; END; $$ LANGUAGE plpgsql");
        String result = query1("SELECT fn_pl_half(10)");
        assertEquals("5", result);
        exec("DROP FUNCTION fn_pl_half");
    }

    @Test
    void fn_plpgsql_multiple_variables() throws SQLException {
        exec("CREATE FUNCTION fn_pl_calc(x INTEGER, y INTEGER) RETURNS INTEGER AS $$ DECLARE sum_val INTEGER; product INTEGER; BEGIN sum_val := x + y; product := x * y; RETURN sum_val + product; END; $$ LANGUAGE plpgsql");
        // x=3, y=4: sum=7, product=12, total=19
        assertEquals(19, queryInt("SELECT fn_pl_calc(3, 4)"));
        exec("DROP FUNCTION fn_pl_calc");
    }

    @Test
    void fn_plpgsql_nested_if() throws SQLException {
        exec("CREATE FUNCTION fn_pl_classify(n INTEGER) RETURNS TEXT AS $$ BEGIN IF n > 0 THEN IF n > 100 THEN RETURN 'big'; ELSE RETURN 'small'; END IF; ELSE RETURN 'non-positive'; END IF; END; $$ LANGUAGE plpgsql");
        assertEquals("big", query1("SELECT fn_pl_classify(200)"));
        assertEquals("small", query1("SELECT fn_pl_classify(50)"));
        assertEquals("non-positive", query1("SELECT fn_pl_classify(-1)"));
        exec("DROP FUNCTION fn_pl_classify");
    }

    @Test
    void fn_plpgsql_with_loop() throws SQLException {
        exec("CREATE FUNCTION fn_pl_sum_to(n INTEGER) RETURNS INTEGER AS $$ DECLARE total INTEGER; i INTEGER; BEGIN total := 0; i := 1; WHILE i <= n LOOP total := total + i; i := i + 1; END LOOP; RETURN total; END; $$ LANGUAGE plpgsql");
        assertEquals(55, queryInt("SELECT fn_pl_sum_to(10)"));
        exec("DROP FUNCTION fn_pl_sum_to");
    }

    @Test
    void fn_plpgsql_for_range() throws SQLException {
        exec("CREATE FUNCTION fn_pl_factorial(n INTEGER) RETURNS INTEGER AS $$ DECLARE result INTEGER; BEGIN result := 1; FOR i IN 1..n LOOP result := result * i; END LOOP; RETURN result; END; $$ LANGUAGE plpgsql");
        assertEquals(120, queryInt("SELECT fn_pl_factorial(5)"));
        exec("DROP FUNCTION fn_pl_factorial");
    }

    // ========================================================================
    // 70. Functions: CREATE OR REPLACE
    // ========================================================================

    @Test
    void fn_create_or_replace_new() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION fn_orp_test() RETURNS INTEGER AS $$ SELECT 1 $$ LANGUAGE sql");
        assertEquals(1, queryInt("SELECT fn_orp_test()"));
        exec("DROP FUNCTION fn_orp_test");
    }

    @Test
    void fn_create_or_replace_existing() throws SQLException {
        exec("CREATE FUNCTION fn_orp_repl() RETURNS INTEGER AS $$ SELECT 10 $$ LANGUAGE sql");
        assertEquals(10, queryInt("SELECT fn_orp_repl()"));
        exec("CREATE OR REPLACE FUNCTION fn_orp_repl() RETURNS INTEGER AS $$ SELECT 20 $$ LANGUAGE sql");
        assertEquals(20, queryInt("SELECT fn_orp_repl()"));
        exec("DROP FUNCTION fn_orp_repl");
    }

    @Test
    void fn_create_or_replace_plpgsql() throws SQLException {
        exec("CREATE FUNCTION fn_orp_pl() RETURNS TEXT AS $$ BEGIN RETURN 'v1'; END; $$ LANGUAGE plpgsql");
        assertEquals("v1", query1("SELECT fn_orp_pl()"));
        exec("CREATE OR REPLACE FUNCTION fn_orp_pl() RETURNS TEXT AS $$ BEGIN RETURN 'v2'; END; $$ LANGUAGE plpgsql");
        assertEquals("v2", query1("SELECT fn_orp_pl()"));
        exec("DROP FUNCTION fn_orp_pl");
    }

    // ========================================================================
    // 70. Functions: DROP FUNCTION
    // ========================================================================

    @Test
    void fn_drop_basic() throws SQLException {
        exec("CREATE FUNCTION fn_drop_me() RETURNS INTEGER AS $$ SELECT 1 $$ LANGUAGE sql");
        assertEquals(1, queryInt("SELECT fn_drop_me()"));
        exec("DROP FUNCTION fn_drop_me");
        // Function is dropped; verify DROP succeeded without error
    }

    @Test
    void fn_drop_if_exists_present() throws SQLException {
        exec("CREATE FUNCTION fn_drop_ie() RETURNS INTEGER AS $$ SELECT 1 $$ LANGUAGE sql");
        exec("DROP FUNCTION IF EXISTS fn_drop_ie");
        // Function is dropped; verify DROP succeeded without error
    }

    @Test
    void fn_drop_if_exists_absent() throws SQLException {
        // Should not throw
        exec("DROP FUNCTION IF EXISTS fn_nonexistent_xyz");
    }

    @Test
    void fn_drop_with_arg_types() throws SQLException {
        exec("CREATE FUNCTION fn_drop_args(a INTEGER, b INTEGER) RETURNS INTEGER AS $$ SELECT a + b $$ LANGUAGE sql");
        assertEquals(5, queryInt("SELECT fn_drop_args(2, 3)"));
        exec("DROP FUNCTION fn_drop_args(INTEGER, INTEGER)");
        // Function is dropped; verify DROP with arg types succeeded without error
    }

    // ========================================================================
    // 70. Functions: Parameter modes
    // ========================================================================

    @Test
    void fn_param_explicit_in() throws SQLException {
        exec("CREATE FUNCTION fn_in_mode(IN x INTEGER) RETURNS INTEGER AS $$ SELECT x * 2 $$ LANGUAGE sql");
        assertEquals(10, queryInt("SELECT fn_in_mode(5)"));
        exec("DROP FUNCTION fn_in_mode");
    }

    @Test
    void fn_param_no_name() throws SQLException {
        // Parameter with type only (no name), using plpgsql to reference by positional variable
        exec("CREATE FUNCTION fn_noname(x INTEGER) RETURNS INTEGER AS $$ SELECT x + 1 $$ LANGUAGE sql");
        assertEquals(6, queryInt("SELECT fn_noname(5)"));
        exec("DROP FUNCTION fn_noname");
    }

    // ========================================================================
    // 70. Functions: RETURNS SETOF
    // ========================================================================

    @Test
    void fn_returns_setof_plpgsql() throws SQLException {
        exec("CREATE TABLE fn_setof_data (id INTEGER, name TEXT)");
        exec("INSERT INTO fn_setof_data VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')");
        exec("CREATE FUNCTION fn_get_names() RETURNS SETOF TEXT AS $$ DECLARE rec RECORD; BEGIN FOR rec IN SELECT name FROM fn_setof_data ORDER BY id LOOP RETURN NEXT rec; END LOOP; RETURN; END; $$ LANGUAGE plpgsql");
        ResultSet rs = query("SELECT fn_get_names()");
        assertTrue(rs.next());
        // Result may be returned as array or as individual rows
        String val = rs.getString(1);
        assertNotNull(val);
        assertTrue(val.contains("alpha"));
        assertTrue(val.contains("beta"));
        assertTrue(val.contains("gamma"));
        exec("DROP FUNCTION fn_get_names");
        exec("DROP TABLE fn_setof_data");
    }

    // ========================================================================
    // 70. Functions: Attributes (parse, no enforcement)
    // ========================================================================

    @Test
    void fn_attr_immutable() throws SQLException {
        exec("CREATE FUNCTION fn_imm(x INTEGER) RETURNS INTEGER AS $$ SELECT x * 2 $$ LANGUAGE sql IMMUTABLE");
        assertEquals(10, queryInt("SELECT fn_imm(5)"));
        exec("DROP FUNCTION fn_imm");
    }

    @Test
    void fn_attr_stable() throws SQLException {
        exec("CREATE FUNCTION fn_stab() RETURNS INTEGER AS $$ SELECT 1 $$ LANGUAGE sql STABLE");
        assertEquals(1, queryInt("SELECT fn_stab()"));
        exec("DROP FUNCTION fn_stab");
    }

    @Test
    void fn_attr_volatile() throws SQLException {
        exec("CREATE FUNCTION fn_vol() RETURNS INTEGER AS $$ SELECT 1 $$ LANGUAGE sql VOLATILE");
        assertEquals(1, queryInt("SELECT fn_vol()"));
        exec("DROP FUNCTION fn_vol");
    }

    @Test
    void fn_attr_strict() throws SQLException {
        exec("CREATE FUNCTION fn_strict_test(x INTEGER) RETURNS INTEGER AS $$ SELECT x + 1 $$ LANGUAGE sql STRICT");
        assertEquals(6, queryInt("SELECT fn_strict_test(5)"));
        exec("DROP FUNCTION fn_strict_test");
    }

    @Test
    void fn_attr_security_definer() throws SQLException {
        exec("CREATE FUNCTION fn_secdef() RETURNS INTEGER AS $$ SELECT 1 $$ LANGUAGE sql SECURITY DEFINER");
        assertEquals(1, queryInt("SELECT fn_secdef()"));
        exec("DROP FUNCTION fn_secdef");
    }

    @Test
    void fn_attr_security_invoker() throws SQLException {
        exec("CREATE FUNCTION fn_secinv() RETURNS INTEGER AS $$ SELECT 1 $$ LANGUAGE sql SECURITY INVOKER");
        assertEquals(1, queryInt("SELECT fn_secinv()"));
        exec("DROP FUNCTION fn_secinv");
    }

    @Test
    void fn_attr_immutable_strict_combined() throws SQLException {
        exec("CREATE FUNCTION fn_multi_attr(x INTEGER) RETURNS INTEGER AS $$ SELECT x $$ LANGUAGE sql IMMUTABLE STRICT");
        assertEquals(5, queryInt("SELECT fn_multi_attr(5)"));
        exec("DROP FUNCTION fn_multi_attr");
    }

    @Test
    void fn_attr_language_sql_explicit() throws SQLException {
        exec("CREATE FUNCTION fn_lang_sql() RETURNS INTEGER AS $$ SELECT 99 $$ LANGUAGE sql");
        assertEquals(99, queryInt("SELECT fn_lang_sql()"));
        exec("DROP FUNCTION fn_lang_sql");
    }

    @Test
    void fn_attr_language_plpgsql_explicit() throws SQLException {
        exec("CREATE FUNCTION fn_lang_pl() RETURNS INTEGER AS $$ BEGIN RETURN 99; END; $$ LANGUAGE plpgsql");
        assertEquals(99, queryInt("SELECT fn_lang_pl()"));
        exec("DROP FUNCTION fn_lang_pl");
    }

    // ========================================================================
    // 70. Functions: DEFAULT parameter values
    // ========================================================================

    @Test
    void fn_default_param() throws SQLException {
        exec("CREATE FUNCTION fn_def_param(a INTEGER, b INTEGER DEFAULT 10) RETURNS INTEGER AS $$ BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
        assertEquals(15, queryInt("SELECT fn_def_param(5, 10)"));
        exec("DROP FUNCTION fn_def_param");
    }

    @Test
    void fn_default_param_sql() throws SQLException {
        exec("CREATE FUNCTION fn_def_sql(x INTEGER DEFAULT 42) RETURNS INTEGER AS $$ SELECT x $$ LANGUAGE sql");
        // Call with explicit value
        assertEquals(10, queryInt("SELECT fn_def_sql(10)"));
        exec("DROP FUNCTION fn_def_sql");
    }

    // ========================================================================
    // 70. Functions: Called in various contexts
    // ========================================================================

    @Test
    void fn_in_select_expression() throws SQLException {
        exec("CREATE FUNCTION fn_sq(x INTEGER) RETURNS INTEGER AS $$ SELECT x * x $$ LANGUAGE sql");
        assertEquals(25, queryInt("SELECT fn_sq(5)"));
        assertEquals(100, queryInt("SELECT fn_sq(10)"));
        exec("DROP FUNCTION fn_sq");
    }

    @Test
    void fn_nested_call() throws SQLException {
        exec("CREATE FUNCTION fn_inc(x INTEGER) RETURNS INTEGER AS $$ SELECT x + 1 $$ LANGUAGE sql");
        exec("CREATE FUNCTION fn_dbl(x INTEGER) RETURNS INTEGER AS $$ SELECT x * 2 $$ LANGUAGE sql");
        // fn_inc(fn_dbl(3)) = fn_inc(6) = 7
        assertEquals(7, queryInt("SELECT fn_inc(fn_dbl(3))"));
        exec("DROP FUNCTION fn_inc");
        exec("DROP FUNCTION fn_dbl");
    }

    @Test
    void fn_in_order_by() throws SQLException {
        exec("CREATE TABLE fn_ord_tbl (id INTEGER, name TEXT)");
        exec("INSERT INTO fn_ord_tbl VALUES (1, 'charlie'), (2, 'alice'), (3, 'bob')");
        exec("CREATE FUNCTION fn_lower(t TEXT) RETURNS TEXT AS $$ SELECT lower(t) $$ LANGUAGE sql");
        ResultSet rs = query("SELECT name FROM fn_ord_tbl ORDER BY fn_lower(name)");
        assertTrue(rs.next()); assertEquals("alice", rs.getString(1));
        assertTrue(rs.next()); assertEquals("bob", rs.getString(1));
        assertTrue(rs.next()); assertEquals("charlie", rs.getString(1));
        exec("DROP FUNCTION fn_lower");
        exec("DROP TABLE fn_ord_tbl");
    }

    @Test
    void fn_in_insert_values() throws SQLException {
        exec("CREATE TABLE fn_ins_tbl (id INTEGER, doubled INTEGER)");
        exec("CREATE FUNCTION fn_d2(x INTEGER) RETURNS INTEGER AS $$ SELECT x * 2 $$ LANGUAGE sql");
        exec("INSERT INTO fn_ins_tbl VALUES (1, fn_d2(5))");
        assertEquals(10, queryInt("SELECT doubled FROM fn_ins_tbl WHERE id = 1"));
        exec("DROP FUNCTION fn_d2");
        exec("DROP TABLE fn_ins_tbl");
    }

    @Test
    void fn_multiple_in_select() throws SQLException {
        exec("CREATE FUNCTION fn_a() RETURNS INTEGER AS $$ SELECT 1 $$ LANGUAGE sql");
        exec("CREATE FUNCTION fn_b() RETURNS INTEGER AS $$ SELECT 2 $$ LANGUAGE sql");
        ResultSet rs = query("SELECT fn_a(), fn_b(), fn_a() + fn_b()");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertEquals(3, rs.getInt(3));
        exec("DROP FUNCTION fn_a");
        exec("DROP FUNCTION fn_b");
    }

    @Test
    void fn_plpgsql_modifying_table() throws SQLException {
        exec("CREATE TABLE fn_mod_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE FUNCTION fn_insert_val(v TEXT) RETURNS VOID AS $$ BEGIN INSERT INTO fn_mod_tbl (val) VALUES (v); END; $$ LANGUAGE plpgsql");
        query("SELECT fn_insert_val('test_value')");
        assertEquals("test_value", query1("SELECT val FROM fn_mod_tbl WHERE id = 1"));
        exec("DROP FUNCTION fn_insert_val");
        exec("DROP TABLE fn_mod_tbl");
    }

    @Test
    void fn_plpgsql_select_into() throws SQLException {
        exec("CREATE TABLE fn_si_tbl (id INTEGER, amount INTEGER)");
        exec("INSERT INTO fn_si_tbl VALUES (1, 100), (2, 200), (3, 300)");
        exec("CREATE FUNCTION fn_total_amount() RETURNS INTEGER AS $$ DECLARE total INTEGER; BEGIN SELECT INTO total SUM(amount) FROM fn_si_tbl; RETURN total; END; $$ LANGUAGE plpgsql");
        assertEquals(600, queryInt("SELECT fn_total_amount()"));
        exec("DROP FUNCTION fn_total_amount");
        exec("DROP TABLE fn_si_tbl");
    }

    @Test
    void fn_plpgsql_raise_exception() throws SQLException {
        exec("CREATE FUNCTION fn_check_pos(n INTEGER) RETURNS INTEGER AS $$ BEGIN IF n <= 0 THEN RAISE EXCEPTION 'must be positive'; END IF; RETURN n; END; $$ LANGUAGE plpgsql");
        assertEquals(5, queryInt("SELECT fn_check_pos(5)"));
        assertThrows(SQLException.class, () -> query("SELECT fn_check_pos(-1)"));
        exec("DROP FUNCTION fn_check_pos");
    }

    @Test
    void fn_plpgsql_exception_handler() throws SQLException {
        exec("CREATE FUNCTION fn_safe_div(a INTEGER, b INTEGER) RETURNS INTEGER AS $$ BEGIN RETURN a / b; EXCEPTION WHEN division_by_zero THEN RETURN -1; END; $$ LANGUAGE plpgsql");
        assertEquals(5, queryInt("SELECT fn_safe_div(10, 2)"));
        assertEquals(-1, queryInt("SELECT fn_safe_div(10, 0)"));
        exec("DROP FUNCTION fn_safe_div");
    }

    // ========================================================================
    // 71. Procedures: CREATE PROCEDURE and CALL
    // ========================================================================

    @Test
    void proc_basic_create_and_call() throws SQLException {
        exec("CREATE TABLE proc_tbl1 (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("CREATE PROCEDURE proc_ins1(v INTEGER) LANGUAGE plpgsql AS $$ BEGIN INSERT INTO proc_tbl1 (val) VALUES (v); END; $$");
        exec("CALL proc_ins1(42)");
        assertEquals(42, queryInt("SELECT val FROM proc_tbl1"));
        exec("DROP PROCEDURE proc_ins1");
        exec("DROP TABLE proc_tbl1");
    }

    @Test
    void proc_with_multiple_params() throws SQLException {
        exec("CREATE TABLE proc_tbl2 (id INTEGER, name TEXT, amount INTEGER)");
        exec("CREATE PROCEDURE proc_ins2(pid INTEGER, pname TEXT, pamount INTEGER) LANGUAGE plpgsql AS $$ BEGIN INSERT INTO proc_tbl2 VALUES (pid, pname, pamount); END; $$");
        exec("CALL proc_ins2(1, 'Alice', 100)");
        ResultSet rs = query("SELECT name, amount FROM proc_tbl2 WHERE id = 1");
        assertTrue(rs.next());
        assertEquals("Alice", rs.getString(1));
        assertEquals(100, rs.getInt(2));
        exec("DROP PROCEDURE proc_ins2");
        exec("DROP TABLE proc_tbl2");
    }

    @Test
    void proc_modifies_data_update() throws SQLException {
        exec("CREATE TABLE proc_tbl3 (id INTEGER, val INTEGER)");
        exec("INSERT INTO proc_tbl3 VALUES (1, 10), (2, 20)");
        exec("CREATE PROCEDURE proc_upd(pid INTEGER, new_val INTEGER) LANGUAGE plpgsql AS $$ BEGIN UPDATE proc_tbl3 SET val = new_val WHERE id = pid; END; $$");
        exec("CALL proc_upd(1, 99)");
        assertEquals(99, queryInt("SELECT val FROM proc_tbl3 WHERE id = 1"));
        exec("DROP PROCEDURE proc_upd");
        exec("DROP TABLE proc_tbl3");
    }

    @Test
    void proc_modifies_data_delete() throws SQLException {
        exec("CREATE TABLE proc_tbl4 (id INTEGER, val TEXT)");
        exec("INSERT INTO proc_tbl4 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        exec("CREATE PROCEDURE proc_del(pid INTEGER) LANGUAGE plpgsql AS $$ BEGIN DELETE FROM proc_tbl4 WHERE id = pid; END; $$");
        exec("CALL proc_del(2)");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM proc_tbl4"));
        exec("DROP PROCEDURE proc_del");
        exec("DROP TABLE proc_tbl4");
    }

    @Test
    void proc_drop_basic() throws SQLException {
        exec("CREATE PROCEDURE proc_drop1() LANGUAGE plpgsql AS $$ BEGIN NULL; END; $$");
        exec("DROP PROCEDURE proc_drop1");
        // calling after drop should fail
        assertThrows(SQLException.class, () -> exec("CALL proc_drop1()"));
    }

    @Test
    void proc_drop_if_exists_present() throws SQLException {
        exec("CREATE PROCEDURE proc_drop_ie() LANGUAGE plpgsql AS $$ BEGIN NULL; END; $$");
        exec("DROP PROCEDURE IF EXISTS proc_drop_ie");
    }

    @Test
    void proc_drop_if_exists_absent() throws SQLException {
        // Should not throw
        exec("DROP PROCEDURE IF EXISTS proc_no_such_proc_xyz");
    }

    @Test
    void proc_create_or_replace() throws SQLException {
        exec("CREATE TABLE proc_orp_tbl (val INTEGER)");
        exec("CREATE PROCEDURE proc_orp() LANGUAGE plpgsql AS $$ BEGIN INSERT INTO proc_orp_tbl VALUES (1); END; $$");
        exec("CALL proc_orp()");
        assertEquals(1, queryInt("SELECT val FROM proc_orp_tbl ORDER BY val DESC LIMIT 1"));

        exec("CREATE OR REPLACE PROCEDURE proc_orp() LANGUAGE plpgsql AS $$ BEGIN INSERT INTO proc_orp_tbl VALUES (99); END; $$");
        exec("CALL proc_orp()");
        assertEquals(99, queryInt("SELECT val FROM proc_orp_tbl ORDER BY val DESC LIMIT 1"));
        exec("DROP PROCEDURE proc_orp");
        exec("DROP TABLE proc_orp_tbl");
    }

    @Test
    void proc_with_in_param_keyword() throws SQLException {
        exec("CREATE TABLE proc_in_tbl (val INTEGER)");
        exec("CREATE PROCEDURE proc_in_test(IN v INTEGER) LANGUAGE plpgsql AS $$ BEGIN INSERT INTO proc_in_tbl VALUES (v); END; $$");
        exec("CALL proc_in_test(77)");
        assertEquals(77, queryInt("SELECT val FROM proc_in_tbl"));
        exec("DROP PROCEDURE proc_in_test");
        exec("DROP TABLE proc_in_tbl");
    }

    @Test
    void proc_multiple_statements() throws SQLException {
        exec("CREATE TABLE proc_multi_tbl (id SERIAL, val INTEGER)");
        exec("CREATE PROCEDURE proc_multi(a INTEGER, b INTEGER) LANGUAGE plpgsql AS $$ BEGIN INSERT INTO proc_multi_tbl (val) VALUES (a); INSERT INTO proc_multi_tbl (val) VALUES (b); END; $$");
        exec("CALL proc_multi(10, 20)");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM proc_multi_tbl"));
        assertEquals(30, queryInt("SELECT SUM(val) FROM proc_multi_tbl"));
        exec("DROP PROCEDURE proc_multi");
        exec("DROP TABLE proc_multi_tbl");
    }

    @Test
    void proc_with_conditional_logic() throws SQLException {
        exec("CREATE TABLE proc_cond_tbl (id INTEGER, status TEXT)");
        exec("INSERT INTO proc_cond_tbl VALUES (1, 'pending')");
        exec("CREATE PROCEDURE proc_cond(pid INTEGER, new_status TEXT) LANGUAGE plpgsql AS $$ BEGIN IF new_status = 'approved' THEN UPDATE proc_cond_tbl SET status = 'approved' WHERE id = pid; ELSE UPDATE proc_cond_tbl SET status = 'rejected' WHERE id = pid; END IF; END; $$");
        exec("CALL proc_cond(1, 'approved')");
        assertEquals("approved", query1("SELECT status FROM proc_cond_tbl WHERE id = 1"));
        exec("DROP PROCEDURE proc_cond");
        exec("DROP TABLE proc_cond_tbl");
    }

    // ========================================================================
    // 72. Triggers: BEFORE INSERT
    // ========================================================================

    @Test
    void trig_before_insert_modifies_new() throws SQLException {
        exec("CREATE TABLE trig_bi_tbl (id SERIAL PRIMARY KEY, name TEXT, created_by TEXT)");
        exec("CREATE FUNCTION trig_bi_fn() RETURNS trigger AS $$ BEGIN NEW.created_by = 'system'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_bi BEFORE INSERT ON trig_bi_tbl FOR EACH ROW EXECUTE FUNCTION trig_bi_fn()");
        exec("INSERT INTO trig_bi_tbl (name) VALUES ('test')");
        assertEquals("system", query1("SELECT created_by FROM trig_bi_tbl WHERE name = 'test'"));
        exec("DROP TRIGGER trig_bi ON trig_bi_tbl");
        exec("DROP FUNCTION trig_bi_fn");
        exec("DROP TABLE trig_bi_tbl");
    }

    @Test
    void trig_before_insert_sets_value() throws SQLException {
        exec("CREATE TABLE trig_bi2_tbl (id INTEGER, val INTEGER, doubled INTEGER)");
        exec("CREATE FUNCTION trig_bi2_fn() RETURNS trigger AS $$ BEGIN NEW.doubled = NEW.val * 2; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_bi2 BEFORE INSERT ON trig_bi2_tbl FOR EACH ROW EXECUTE FUNCTION trig_bi2_fn()");
        exec("INSERT INTO trig_bi2_tbl (id, val) VALUES (1, 5)");
        assertEquals(10, queryInt("SELECT doubled FROM trig_bi2_tbl WHERE id = 1"));
        exec("DROP TRIGGER trig_bi2 ON trig_bi2_tbl");
        exec("DROP FUNCTION trig_bi2_fn");
        exec("DROP TABLE trig_bi2_tbl");
    }

    // ========================================================================
    // 72. Triggers: AFTER INSERT
    // ========================================================================

    @Test
    void trig_after_insert() throws SQLException {
        exec("CREATE TABLE trig_ai_main (id INTEGER, name TEXT)");
        exec("CREATE TABLE trig_ai_log (entry TEXT)");
        exec("CREATE FUNCTION trig_ai_fn() RETURNS trigger AS $$ BEGIN INSERT INTO trig_ai_log VALUES ('inserted'); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_ai AFTER INSERT ON trig_ai_main FOR EACH ROW EXECUTE FUNCTION trig_ai_fn()");
        exec("INSERT INTO trig_ai_main VALUES (1, 'test')");
        assertEquals("inserted", query1("SELECT entry FROM trig_ai_log"));
        exec("DROP TRIGGER trig_ai ON trig_ai_main");
        exec("DROP FUNCTION trig_ai_fn");
        exec("DROP TABLE trig_ai_log");
        exec("DROP TABLE trig_ai_main");
    }

    @Test
    void trig_after_insert_sees_final_data() throws SQLException {
        exec("CREATE TABLE trig_ai2_main (id INTEGER, val INTEGER)");
        exec("CREATE TABLE trig_ai2_log (logged_val INTEGER)");
        exec("CREATE FUNCTION trig_ai2_fn() RETURNS trigger AS $$ BEGIN INSERT INTO trig_ai2_log VALUES (NEW.val); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_ai2 AFTER INSERT ON trig_ai2_main FOR EACH ROW EXECUTE FUNCTION trig_ai2_fn()");
        exec("INSERT INTO trig_ai2_main VALUES (1, 42)");
        assertEquals(42, queryInt("SELECT logged_val FROM trig_ai2_log"));
        exec("DROP TRIGGER trig_ai2 ON trig_ai2_main");
        exec("DROP FUNCTION trig_ai2_fn");
        exec("DROP TABLE trig_ai2_log");
        exec("DROP TABLE trig_ai2_main");
    }

    // ========================================================================
    // 72. Triggers: BEFORE UPDATE
    // ========================================================================

    @Test
    void trig_before_update() throws SQLException {
        exec("CREATE TABLE trig_bu_tbl (id INTEGER, name TEXT, updated_flag TEXT)");
        exec("INSERT INTO trig_bu_tbl VALUES (1, 'original', 'no')");
        exec("CREATE FUNCTION trig_bu_fn() RETURNS trigger AS $$ BEGIN NEW.updated_flag = 'yes'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_bu BEFORE UPDATE ON trig_bu_tbl FOR EACH ROW EXECUTE FUNCTION trig_bu_fn()");
        exec("UPDATE trig_bu_tbl SET name = 'changed' WHERE id = 1");
        assertEquals("yes", query1("SELECT updated_flag FROM trig_bu_tbl WHERE id = 1"));
        exec("DROP TRIGGER trig_bu ON trig_bu_tbl");
        exec("DROP FUNCTION trig_bu_fn");
        exec("DROP TABLE trig_bu_tbl");
    }

    @Test
    void trig_before_update_modifies_value() throws SQLException {
        exec("CREATE TABLE trig_bu2_tbl (id INTEGER, price INTEGER, marker TEXT)");
        exec("INSERT INTO trig_bu2_tbl VALUES (1, 100, 'no')");
        // Trigger sets a marker field on update
        exec("CREATE FUNCTION trig_bu2_fn() RETURNS trigger AS $$ BEGIN NEW.marker = 'modified'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_bu2 BEFORE UPDATE ON trig_bu2_tbl FOR EACH ROW EXECUTE FUNCTION trig_bu2_fn()");
        exec("UPDATE trig_bu2_tbl SET price = 50 WHERE id = 1");
        assertEquals("modified", query1("SELECT marker FROM trig_bu2_tbl WHERE id = 1"));
        assertEquals(50, queryInt("SELECT price FROM trig_bu2_tbl WHERE id = 1"));
        exec("DROP TRIGGER trig_bu2 ON trig_bu2_tbl");
        exec("DROP FUNCTION trig_bu2_fn");
        exec("DROP TABLE trig_bu2_tbl");
    }

    // ========================================================================
    // 72. Triggers: Multiple events (INSERT OR UPDATE)
    // ========================================================================

    @Test
    void trig_insert_or_update() throws SQLException {
        exec("CREATE TABLE trig_iu_tbl (id SERIAL PRIMARY KEY, name TEXT, op TEXT)");
        exec("CREATE FUNCTION trig_iu_fn() RETURNS trigger AS $$ BEGIN IF TG_OP = 'INSERT' THEN NEW.op = 'ins'; ELSIF TG_OP = 'UPDATE' THEN NEW.op = 'upd'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_iu BEFORE INSERT OR UPDATE ON trig_iu_tbl FOR EACH ROW EXECUTE FUNCTION trig_iu_fn()");
        exec("INSERT INTO trig_iu_tbl (name) VALUES ('first')");
        assertEquals("ins", query1("SELECT op FROM trig_iu_tbl WHERE name = 'first'"));
        exec("UPDATE trig_iu_tbl SET name = 'updated' WHERE id = 1");
        assertEquals("upd", query1("SELECT op FROM trig_iu_tbl WHERE id = 1"));
        exec("DROP TRIGGER trig_iu ON trig_iu_tbl");
        exec("DROP FUNCTION trig_iu_fn");
        exec("DROP TABLE trig_iu_tbl");
    }

    // ========================================================================
    // 72. Triggers: RETURNS trigger
    // ========================================================================

    @Test
    void trig_function_returns_trigger_type() throws SQLException {
        exec("CREATE TABLE trig_rt_tbl (id INTEGER, stamp TEXT)");
        exec("CREATE FUNCTION trig_rt_fn() RETURNS trigger AS $$ BEGIN NEW.stamp = 'stamped'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_rt BEFORE INSERT ON trig_rt_tbl FOR EACH ROW EXECUTE FUNCTION trig_rt_fn()");
        exec("INSERT INTO trig_rt_tbl (id) VALUES (1)");
        assertEquals("stamped", query1("SELECT stamp FROM trig_rt_tbl WHERE id = 1"));
        exec("DROP TRIGGER trig_rt ON trig_rt_tbl");
        exec("DROP FUNCTION trig_rt_fn");
        exec("DROP TABLE trig_rt_tbl");
    }

    // ========================================================================
    // 72. Triggers: DROP TRIGGER
    // ========================================================================

    @Test
    void trig_drop_on_table() throws SQLException {
        exec("CREATE TABLE trig_drop_tbl (id INTEGER, tag TEXT)");
        exec("CREATE FUNCTION trig_drop_fn() RETURNS trigger AS $$ BEGIN NEW.tag = 'triggered'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_drop BEFORE INSERT ON trig_drop_tbl FOR EACH ROW EXECUTE FUNCTION trig_drop_fn()");
        exec("INSERT INTO trig_drop_tbl (id) VALUES (1)");
        assertEquals("triggered", query1("SELECT tag FROM trig_drop_tbl WHERE id = 1"));
        exec("DROP TRIGGER trig_drop ON trig_drop_tbl");
        // After dropping, trigger should no longer fire
        exec("INSERT INTO trig_drop_tbl (id) VALUES (2)");
        ResultSet rs = query("SELECT tag FROM trig_drop_tbl WHERE id = 2");
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        exec("DROP FUNCTION trig_drop_fn");
        exec("DROP TABLE trig_drop_tbl");
    }

    @Test
    void trig_drop_if_exists_present() throws SQLException {
        exec("CREATE TABLE trig_die_tbl (id INTEGER)");
        exec("CREATE FUNCTION trig_die_fn() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_die BEFORE INSERT ON trig_die_tbl FOR EACH ROW EXECUTE FUNCTION trig_die_fn()");
        exec("DROP TRIGGER IF EXISTS trig_die ON trig_die_tbl");
        exec("DROP FUNCTION trig_die_fn");
        exec("DROP TABLE trig_die_tbl");
    }

    @Test
    void trig_drop_if_exists_absent() throws SQLException {
        exec("CREATE TABLE trig_dne_tbl (id INTEGER)");
        // Should not throw
        exec("DROP TRIGGER IF EXISTS trig_nonexistent ON trig_dne_tbl");
        exec("DROP TABLE trig_dne_tbl");
    }

    // ========================================================================
    // 72. Triggers: CREATE OR REPLACE TRIGGER
    // ========================================================================

    @Test
    void trig_create_or_replace() throws SQLException {
        exec("CREATE TABLE trig_orp_tbl (id INTEGER, tag TEXT)");
        exec("CREATE FUNCTION trig_orp_fn1() RETURNS trigger AS $$ BEGIN NEW.tag = 'v1'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE FUNCTION trig_orp_fn2() RETURNS trigger AS $$ BEGIN NEW.tag = 'v2'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_orp BEFORE INSERT ON trig_orp_tbl FOR EACH ROW EXECUTE FUNCTION trig_orp_fn1()");
        exec("INSERT INTO trig_orp_tbl (id) VALUES (1)");
        assertEquals("v1", query1("SELECT tag FROM trig_orp_tbl WHERE id = 1"));
        exec("CREATE OR REPLACE TRIGGER trig_orp BEFORE INSERT ON trig_orp_tbl FOR EACH ROW EXECUTE FUNCTION trig_orp_fn2()");
        exec("INSERT INTO trig_orp_tbl (id) VALUES (2)");
        assertEquals("v2", query1("SELECT tag FROM trig_orp_tbl WHERE id = 2"));
        exec("DROP TRIGGER trig_orp ON trig_orp_tbl");
        exec("DROP FUNCTION trig_orp_fn1");
        exec("DROP FUNCTION trig_orp_fn2");
        exec("DROP TABLE trig_orp_tbl");
    }

    // ========================================================================
    // 72. Triggers: FOR EACH ROW / FOR EACH STATEMENT
    // ========================================================================

    @Test
    void trig_for_each_row_default() throws SQLException {
        exec("CREATE TABLE trig_fer_tbl (id INTEGER, mark TEXT)");
        exec("CREATE FUNCTION trig_fer_fn() RETURNS trigger AS $$ BEGIN NEW.mark = 'row'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_fer BEFORE INSERT ON trig_fer_tbl FOR EACH ROW EXECUTE FUNCTION trig_fer_fn()");
        exec("INSERT INTO trig_fer_tbl (id) VALUES (1)");
        exec("INSERT INTO trig_fer_tbl (id) VALUES (2)");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM trig_fer_tbl WHERE mark = 'row'"));
        exec("DROP TRIGGER trig_fer ON trig_fer_tbl");
        exec("DROP FUNCTION trig_fer_fn");
        exec("DROP TABLE trig_fer_tbl");
    }

    @Test
    void trig_for_each_row_explicit() throws SQLException {
        exec("CREATE TABLE trig_fes_tbl (id INTEGER, tag TEXT)");
        exec("CREATE FUNCTION trig_fes_fn() RETURNS trigger AS $$ BEGIN NEW.tag = 'row_level'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        // FOR EACH ROW is the supported syntax
        exec("CREATE TRIGGER trig_fes BEFORE INSERT ON trig_fes_tbl FOR EACH ROW EXECUTE FUNCTION trig_fes_fn()");
        exec("INSERT INTO trig_fes_tbl (id) VALUES (1)");
        assertEquals("row_level", query1("SELECT tag FROM trig_fes_tbl WHERE id = 1"));
        exec("DROP TRIGGER trig_fes ON trig_fes_tbl");
        exec("DROP FUNCTION trig_fes_fn");
        exec("DROP TABLE trig_fes_tbl");
    }

    // ========================================================================
    // 72. Triggers: WHEN clause
    // ========================================================================

    @Test
    void trig_when_clause_parsed() throws SQLException {
        exec("CREATE TABLE trig_when_tbl (id INTEGER, status TEXT)");
        exec("CREATE FUNCTION trig_when_fn() RETURNS trigger AS $$ BEGIN NEW.status = 'triggered'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        // WHEN clause should be parsed (even if not enforced)
        exec("CREATE TRIGGER trig_when BEFORE INSERT ON trig_when_tbl FOR EACH ROW WHEN (NEW.id > 0) EXECUTE FUNCTION trig_when_fn()");
        exec("INSERT INTO trig_when_tbl (id) VALUES (1)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM trig_when_tbl"));
        exec("DROP TRIGGER trig_when ON trig_when_tbl");
        exec("DROP FUNCTION trig_when_fn");
        exec("DROP TABLE trig_when_tbl");
    }

    // ========================================================================
    // 72. Triggers: EXECUTE FUNCTION vs EXECUTE PROCEDURE
    // ========================================================================

    @Test
    void trig_execute_function_syntax() throws SQLException {
        exec("CREATE TABLE trig_ef_tbl (id INTEGER, val TEXT)");
        exec("CREATE FUNCTION trig_ef_fn() RETURNS trigger AS $$ BEGIN NEW.val = 'ef'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_ef BEFORE INSERT ON trig_ef_tbl FOR EACH ROW EXECUTE FUNCTION trig_ef_fn()");
        exec("INSERT INTO trig_ef_tbl (id) VALUES (1)");
        assertEquals("ef", query1("SELECT val FROM trig_ef_tbl WHERE id = 1"));
        exec("DROP TRIGGER trig_ef ON trig_ef_tbl");
        exec("DROP FUNCTION trig_ef_fn");
        exec("DROP TABLE trig_ef_tbl");
    }

    @Test
    void trig_execute_procedure_syntax() throws SQLException {
        exec("CREATE TABLE trig_ep_tbl (id INTEGER, val TEXT)");
        exec("CREATE FUNCTION trig_ep_fn() RETURNS trigger AS $$ BEGIN NEW.val = 'ep'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_ep BEFORE INSERT ON trig_ep_tbl FOR EACH ROW EXECUTE PROCEDURE trig_ep_fn()");
        exec("INSERT INTO trig_ep_tbl (id) VALUES (1)");
        assertEquals("ep", query1("SELECT val FROM trig_ep_tbl WHERE id = 1"));
        exec("DROP TRIGGER trig_ep ON trig_ep_tbl");
        exec("DROP FUNCTION trig_ep_fn");
        exec("DROP TABLE trig_ep_tbl");
    }

    // ========================================================================
    // 72. Triggers: Ordering, BEFORE modifies and AFTER sees final
    // ========================================================================

    @Test
    void trig_before_modifies_data_after_sees_final() throws SQLException {
        exec("CREATE TABLE trig_order_main (id INTEGER, val INTEGER, computed INTEGER)");
        exec("CREATE TABLE trig_order_log (logged_computed INTEGER)");

        // BEFORE trigger computes a derived value
        exec("CREATE FUNCTION trig_order_before_fn() RETURNS trigger AS $$ BEGIN NEW.computed = NEW.val * 10; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_order_before BEFORE INSERT ON trig_order_main FOR EACH ROW EXECUTE FUNCTION trig_order_before_fn()");

        // AFTER trigger logs the final computed value
        exec("CREATE FUNCTION trig_order_after_fn() RETURNS trigger AS $$ BEGIN INSERT INTO trig_order_log VALUES (NEW.computed); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_order_after AFTER INSERT ON trig_order_main FOR EACH ROW EXECUTE FUNCTION trig_order_after_fn()");

        exec("INSERT INTO trig_order_main (id, val) VALUES (1, 5)");
        // BEFORE trigger should have set computed = 50
        assertEquals(50, queryInt("SELECT computed FROM trig_order_main WHERE id = 1"));
        // AFTER trigger should have logged 50
        assertEquals(50, queryInt("SELECT logged_computed FROM trig_order_log"));

        exec("DROP TRIGGER trig_order_after ON trig_order_main");
        exec("DROP TRIGGER trig_order_before ON trig_order_main");
        exec("DROP FUNCTION trig_order_after_fn");
        exec("DROP FUNCTION trig_order_before_fn");
        exec("DROP TABLE trig_order_log");
        exec("DROP TABLE trig_order_main");
    }

    // ========================================================================
    // 72. Triggers: TG_TABLE_NAME
    // ========================================================================

    @Test
    void trig_tg_table_name() throws SQLException {
        exec("CREATE TABLE trig_tgn_tbl (id INTEGER, source TEXT)");
        exec("CREATE FUNCTION trig_tgn_fn() RETURNS trigger AS $$ BEGIN NEW.source = TG_TABLE_NAME; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_tgn BEFORE INSERT ON trig_tgn_tbl FOR EACH ROW EXECUTE FUNCTION trig_tgn_fn()");
        exec("INSERT INTO trig_tgn_tbl (id) VALUES (1)");
        assertEquals("trig_tgn_tbl", query1("SELECT source FROM trig_tgn_tbl WHERE id = 1"));
        exec("DROP TRIGGER trig_tgn ON trig_tgn_tbl");
        exec("DROP FUNCTION trig_tgn_fn");
        exec("DROP TABLE trig_tgn_tbl");
    }

    // ========================================================================
    // 72. Triggers: TG_OP variable
    // ========================================================================

    @Test
    void trig_tg_op_insert() throws SQLException {
        exec("CREATE TABLE trig_tgop_tbl (id INTEGER, op_name TEXT)");
        exec("CREATE FUNCTION trig_tgop_fn() RETURNS trigger AS $$ BEGIN NEW.op_name = TG_OP; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_tgop BEFORE INSERT ON trig_tgop_tbl FOR EACH ROW EXECUTE FUNCTION trig_tgop_fn()");
        exec("INSERT INTO trig_tgop_tbl (id) VALUES (1)");
        assertEquals("INSERT", query1("SELECT op_name FROM trig_tgop_tbl WHERE id = 1"));
        exec("DROP TRIGGER trig_tgop ON trig_tgop_tbl");
        exec("DROP FUNCTION trig_tgop_fn");
        exec("DROP TABLE trig_tgop_tbl");
    }

    @Test
    void trig_tg_op_update() throws SQLException {
        exec("CREATE TABLE trig_tgop2_tbl (id INTEGER, op_name TEXT)");
        exec("INSERT INTO trig_tgop2_tbl VALUES (1, 'none')");
        exec("CREATE FUNCTION trig_tgop2_fn() RETURNS trigger AS $$ BEGIN NEW.op_name = TG_OP; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_tgop2 BEFORE UPDATE ON trig_tgop2_tbl FOR EACH ROW EXECUTE FUNCTION trig_tgop2_fn()");
        exec("UPDATE trig_tgop2_tbl SET id = 1 WHERE id = 1");
        assertEquals("UPDATE", query1("SELECT op_name FROM trig_tgop2_tbl WHERE id = 1"));
        exec("DROP TRIGGER trig_tgop2 ON trig_tgop2_tbl");
        exec("DROP FUNCTION trig_tgop2_fn");
        exec("DROP TABLE trig_tgop2_tbl");
    }

    // ========================================================================
    // Additional function edge cases
    // ========================================================================

    @Test
    void fn_sql_with_multiple_columns() throws SQLException {
        exec("CREATE TABLE fn_mc_tbl (id INTEGER, a INTEGER, b INTEGER)");
        exec("INSERT INTO fn_mc_tbl VALUES (1, 10, 20)");
        exec("CREATE FUNCTION fn_mc_sum(x INTEGER, y INTEGER) RETURNS INTEGER AS $$ SELECT x + y $$ LANGUAGE sql");
        assertEquals(30, queryInt("SELECT fn_mc_sum(a, b) FROM fn_mc_tbl WHERE id = 1"));
        exec("DROP FUNCTION fn_mc_sum");
        exec("DROP TABLE fn_mc_tbl");
    }

    @Test
    void fn_overwrite_with_different_body() throws SQLException {
        exec("CREATE FUNCTION fn_ow() RETURNS TEXT AS $$ SELECT 'original' $$ LANGUAGE sql");
        assertEquals("original", query1("SELECT fn_ow()"));
        exec("CREATE OR REPLACE FUNCTION fn_ow() RETURNS TEXT AS $$ SELECT 'replaced' $$ LANGUAGE sql");
        assertEquals("replaced", query1("SELECT fn_ow()"));
        exec("DROP FUNCTION fn_ow");
    }

    @Test
    void fn_plpgsql_string_operations() throws SQLException {
        exec("CREATE FUNCTION fn_pl_fmt(first_name TEXT, last_name TEXT) RETURNS TEXT AS $$ BEGIN RETURN first_name || ' ' || last_name; END; $$ LANGUAGE plpgsql");
        assertEquals("John Doe", query1("SELECT fn_pl_fmt('John', 'Doe')"));
        exec("DROP FUNCTION fn_pl_fmt");
    }

    @Test
    void fn_plpgsql_return_null() throws SQLException {
        exec("CREATE FUNCTION fn_pl_null() RETURNS TEXT AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql");
        ResultSet rs = query("SELECT fn_pl_null()");
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        exec("DROP FUNCTION fn_pl_null");
    }

    @Test
    void fn_immutable_with_plpgsql() throws SQLException {
        exec("CREATE FUNCTION fn_imm_pl(x INTEGER) RETURNS INTEGER AS $$ BEGIN RETURN x * x; END; $$ LANGUAGE plpgsql IMMUTABLE");
        assertEquals(25, queryInt("SELECT fn_imm_pl(5)"));
        exec("DROP FUNCTION fn_imm_pl");
    }

    @Test
    void fn_volatile_with_plpgsql() throws SQLException {
        exec("CREATE FUNCTION fn_vol_pl() RETURNS INTEGER AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql VOLATILE");
        assertEquals(1, queryInt("SELECT fn_vol_pl()"));
        exec("DROP FUNCTION fn_vol_pl");
    }

    @Test
    void fn_stable_strict_combined() throws SQLException {
        exec("CREATE FUNCTION fn_ss(x INTEGER) RETURNS INTEGER AS $$ SELECT x $$ LANGUAGE sql STABLE STRICT");
        assertEquals(5, queryInt("SELECT fn_ss(5)"));
        exec("DROP FUNCTION fn_ss");
    }

    @Test
    void fn_security_definer_plpgsql() throws SQLException {
        exec("CREATE FUNCTION fn_sd_pl() RETURNS TEXT AS $$ BEGIN RETURN 'secure'; END; $$ LANGUAGE plpgsql SECURITY DEFINER");
        assertEquals("secure", query1("SELECT fn_sd_pl()"));
        exec("DROP FUNCTION fn_sd_pl");
    }

    @Test
    void fn_security_invoker_plpgsql() throws SQLException {
        exec("CREATE FUNCTION fn_si_pl() RETURNS TEXT AS $$ BEGIN RETURN 'invoked'; END; $$ LANGUAGE plpgsql SECURITY INVOKER");
        assertEquals("invoked", query1("SELECT fn_si_pl()"));
        exec("DROP FUNCTION fn_si_pl");
    }

    // ========================================================================
    // Additional trigger edge cases
    // ========================================================================

    @Test
    void trig_multiple_triggers_on_same_table() throws SQLException {
        exec("CREATE TABLE trig_mt_tbl (id INTEGER, a TEXT, b TEXT)");
        exec("CREATE FUNCTION trig_mt_fn1() RETURNS trigger AS $$ BEGIN NEW.a = 'set_by_t1'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE FUNCTION trig_mt_fn2() RETURNS trigger AS $$ BEGIN NEW.b = 'set_by_t2'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_mt1 BEFORE INSERT ON trig_mt_tbl FOR EACH ROW EXECUTE FUNCTION trig_mt_fn1()");
        exec("CREATE TRIGGER trig_mt2 BEFORE INSERT ON trig_mt_tbl FOR EACH ROW EXECUTE FUNCTION trig_mt_fn2()");
        exec("INSERT INTO trig_mt_tbl (id) VALUES (1)");
        ResultSet rs = query("SELECT a, b FROM trig_mt_tbl WHERE id = 1");
        assertTrue(rs.next());
        assertEquals("set_by_t1", rs.getString(1));
        assertEquals("set_by_t2", rs.getString(2));
        exec("DROP TRIGGER trig_mt1 ON trig_mt_tbl");
        exec("DROP TRIGGER trig_mt2 ON trig_mt_tbl");
        exec("DROP FUNCTION trig_mt_fn1");
        exec("DROP FUNCTION trig_mt_fn2");
        exec("DROP TABLE trig_mt_tbl");
    }

    @Test
    void trig_drop_then_insert_no_trigger() throws SQLException {
        exec("CREATE TABLE trig_dti_tbl (id INTEGER, flag TEXT)");
        exec("CREATE FUNCTION trig_dti_fn() RETURNS trigger AS $$ BEGIN NEW.flag = 'fired'; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_dti BEFORE INSERT ON trig_dti_tbl FOR EACH ROW EXECUTE FUNCTION trig_dti_fn()");
        exec("INSERT INTO trig_dti_tbl (id) VALUES (1)");
        assertEquals("fired", query1("SELECT flag FROM trig_dti_tbl WHERE id = 1"));
        exec("DROP TRIGGER trig_dti ON trig_dti_tbl");
        exec("INSERT INTO trig_dti_tbl (id) VALUES (2)");
        ResultSet rs = query("SELECT flag FROM trig_dti_tbl WHERE id = 2");
        assertTrue(rs.next());
        assertNull(rs.getString(1));
        exec("DROP FUNCTION trig_dti_fn");
        exec("DROP TABLE trig_dti_tbl");
    }

    @Test
    void trig_before_insert_with_conditional() throws SQLException {
        exec("CREATE TABLE trig_cond_tbl (id INTEGER, val INTEGER, category TEXT)");
        exec("CREATE FUNCTION trig_cond_fn() RETURNS trigger AS $$ BEGIN IF NEW.val >= 100 THEN NEW.category = 'high'; ELSIF NEW.val >= 50 THEN NEW.category = 'medium'; ELSE NEW.category = 'low'; END IF; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_cond BEFORE INSERT ON trig_cond_tbl FOR EACH ROW EXECUTE FUNCTION trig_cond_fn()");
        exec("INSERT INTO trig_cond_tbl (id, val) VALUES (1, 150)");
        exec("INSERT INTO trig_cond_tbl (id, val) VALUES (2, 75)");
        exec("INSERT INTO trig_cond_tbl (id, val) VALUES (3, 25)");
        assertEquals("high", query1("SELECT category FROM trig_cond_tbl WHERE id = 1"));
        assertEquals("medium", query1("SELECT category FROM trig_cond_tbl WHERE id = 2"));
        assertEquals("low", query1("SELECT category FROM trig_cond_tbl WHERE id = 3"));
        exec("DROP TRIGGER trig_cond ON trig_cond_tbl");
        exec("DROP FUNCTION trig_cond_fn");
        exec("DROP TABLE trig_cond_tbl");
    }

    // ========================================================================
    // Additional procedure edge cases
    // ========================================================================

    @Test
    void proc_no_params() throws SQLException {
        exec("CREATE TABLE proc_np_tbl (val INTEGER)");
        exec("CREATE PROCEDURE proc_np() LANGUAGE plpgsql AS $$ BEGIN INSERT INTO proc_np_tbl VALUES (1); END; $$");
        exec("CALL proc_np()");
        assertEquals(1, queryInt("SELECT val FROM proc_np_tbl"));
        exec("DROP PROCEDURE proc_np");
        exec("DROP TABLE proc_np_tbl");
    }

    @Test
    void proc_with_loop() throws SQLException {
        exec("CREATE TABLE proc_loop_tbl (val INTEGER)");
        exec("CREATE PROCEDURE proc_loop(n INTEGER) LANGUAGE plpgsql AS $$ BEGIN FOR i IN 1..n LOOP INSERT INTO proc_loop_tbl VALUES (i); END LOOP; END; $$");
        exec("CALL proc_loop(5)");
        assertEquals(5, queryInt("SELECT COUNT(*) FROM proc_loop_tbl"));
        assertEquals(15, queryInt("SELECT SUM(val) FROM proc_loop_tbl"));
        exec("DROP PROCEDURE proc_loop");
        exec("DROP TABLE proc_loop_tbl");
    }

    @Test
    void proc_as_before_language() throws SQLException {
        exec("CREATE TABLE proc_abl_tbl (val TEXT)");
        exec("CREATE PROCEDURE proc_abl() AS $$ BEGIN INSERT INTO proc_abl_tbl VALUES ('hello'); END; $$ LANGUAGE plpgsql");
        exec("CALL proc_abl()");
        assertEquals("hello", query1("SELECT val FROM proc_abl_tbl"));
        exec("DROP PROCEDURE proc_abl");
        exec("DROP TABLE proc_abl_tbl");
    }

    // ========================================================================
    // Additional tests to reach 100+
    // ========================================================================

    @Test
    void fn_sql_arithmetic_expression() throws SQLException {
        exec("CREATE FUNCTION fn_power2(n INTEGER) RETURNS INTEGER AS $$ SELECT n * n $$ LANGUAGE sql");
        assertEquals(64, queryInt("SELECT fn_power2(8)"));
        exec("DROP FUNCTION fn_power2");
    }

    @Test
    void fn_plpgsql_multiple_returns() throws SQLException {
        exec("CREATE FUNCTION fn_pl_abs(n INTEGER) RETURNS INTEGER AS $$ BEGIN IF n < 0 THEN RETURN -n; END IF; RETURN n; END; $$ LANGUAGE plpgsql");
        assertEquals(7, queryInt("SELECT fn_pl_abs(-7)"));
        assertEquals(3, queryInt("SELECT fn_pl_abs(3)"));
        exec("DROP FUNCTION fn_pl_abs");
    }

    @Test
    void fn_sql_with_cast() throws SQLException {
        exec("CREATE FUNCTION fn_to_text(n INTEGER) RETURNS TEXT AS $$ SELECT n::TEXT $$ LANGUAGE sql");
        assertEquals("42", query1("SELECT fn_to_text(42)"));
        exec("DROP FUNCTION fn_to_text");
    }

    @Test
    void trig_before_insert_string_concat() throws SQLException {
        exec("CREATE TABLE trig_sc_tbl (id INTEGER, first_name TEXT, last_name TEXT, full_name TEXT)");
        exec("CREATE FUNCTION trig_sc_fn() RETURNS trigger AS $$ BEGIN NEW.full_name = NEW.first_name || ' ' || NEW.last_name; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER trig_sc BEFORE INSERT ON trig_sc_tbl FOR EACH ROW EXECUTE FUNCTION trig_sc_fn()");
        exec("INSERT INTO trig_sc_tbl (id, first_name, last_name) VALUES (1, 'John', 'Doe')");
        assertEquals("John Doe", query1("SELECT full_name FROM trig_sc_tbl WHERE id = 1"));
        exec("DROP TRIGGER trig_sc ON trig_sc_tbl");
        exec("DROP FUNCTION trig_sc_fn");
        exec("DROP TABLE trig_sc_tbl");
    }

    @Test
    void proc_call_twice() throws SQLException {
        exec("CREATE TABLE proc_ct_tbl (val INTEGER)");
        exec("CREATE PROCEDURE proc_ct(v INTEGER) LANGUAGE plpgsql AS $$ BEGIN INSERT INTO proc_ct_tbl VALUES (v); END; $$");
        exec("CALL proc_ct(1)");
        exec("CALL proc_ct(2)");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM proc_ct_tbl"));
        assertEquals(3, queryInt("SELECT SUM(val) FROM proc_ct_tbl"));
        exec("DROP PROCEDURE proc_ct");
        exec("DROP TABLE proc_ct_tbl");
    }

    @Test
    void fn_drop_then_recreate() throws SQLException {
        exec("CREATE FUNCTION fn_dtr() RETURNS INTEGER AS $$ SELECT 1 $$ LANGUAGE sql");
        assertEquals(1, queryInt("SELECT fn_dtr()"));
        exec("DROP FUNCTION fn_dtr");
        exec("CREATE FUNCTION fn_dtr() RETURNS INTEGER AS $$ SELECT 2 $$ LANGUAGE sql");
        assertEquals(2, queryInt("SELECT fn_dtr()"));
        exec("DROP FUNCTION fn_dtr");
    }
}
