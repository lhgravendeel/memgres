package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for multi-variable FOR loops in PL/pgSQL: FOR k, v IN SELECT ... LOOP
 */
class PlpgsqlMultiVarForTest {

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
            s.execute("CREATE TABLE mv_data (k text, v int)");
            s.execute("INSERT INTO mv_data VALUES ('x', 10), ('y', 20), ('z', 30)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private String query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertFalse(rs.next());
            return val;
        }
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // =========================================================================
    // A. Basic multi-variable FOR query loop
    // =========================================================================

    @Test
    void two_variables_from_two_column_query() throws SQLException {
        exec("""
            CREATE OR REPLACE FUNCTION mv_two_vars() RETURNS text LANGUAGE plpgsql AS $$
            DECLARE
              result text := '';
              k text;
              v int;
            BEGIN
              FOR k, v IN SELECT mv_data.k, mv_data.v FROM mv_data ORDER BY mv_data.k LOOP
                IF result <> '' THEN result := result || ','; END IF;
                result := result || k || '=' || v;
              END LOOP;
              RETURN result;
            END;
            $$
        """);
        assertEquals("x=10,y=20,z=30", query("SELECT mv_two_vars()"));
    }

    @Test
    void three_variables() throws SQLException {
        exec("""
            CREATE OR REPLACE FUNCTION mv_three_vars() RETURNS text LANGUAGE plpgsql AS $$
            DECLARE
              result text := '';
              a int; b text; c int;
            BEGIN
              FOR a, b, c IN SELECT 1, 'hello', 100 UNION ALL SELECT 2, 'world', 200 LOOP
                IF result <> '' THEN result := result || ';'; END IF;
                result := result || a || ':' || b || ':' || c;
              END LOOP;
              RETURN result;
            END;
            $$
        """);
        assertEquals("1:hello:100;2:world:200", query("SELECT mv_three_vars()"));
    }

    // =========================================================================
    // B. Multi-variable FOR with hstore each()
    // =========================================================================

    @Test
    void multi_var_for_with_hstore_each() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS hstore");
        exec("""
            CREATE OR REPLACE FUNCTION mv_hstore_each(h hstore) RETURNS text LANGUAGE plpgsql AS $$
            DECLARE
              result text := '';
              k text;
              v text;
            BEGIN
              FOR k, v IN SELECT * FROM each(h) LOOP
                IF result <> '' THEN result := result || ','; END IF;
                result := result || k || '=>' || COALESCE(v, 'NULL');
              END LOOP;
              RETURN result;
            END;
            $$
        """);
        // hstore each() returns (key text, value text) — order is by key
        String result = query("SELECT mv_hstore_each('b=>2, a=>1'::hstore)");
        // hstore iteration order: keys are sorted
        assertTrue(result.contains("a=>1"), "Should contain a=>1: " + result);
        assertTrue(result.contains("b=>2"), "Should contain b=>2: " + result);
    }

    // =========================================================================
    // C. Edge cases
    // =========================================================================

    @Test
    void fewer_columns_than_variables() throws SQLException {
        // When query returns fewer columns than variables, extra variables get NULL
        exec("""
            CREATE OR REPLACE FUNCTION mv_fewer_cols() RETURNS text LANGUAGE plpgsql AS $$
            DECLARE
              a text;
              b text;
            BEGIN
              FOR a, b IN SELECT 'only_one' LOOP
                RETURN a || ',' || COALESCE(b, 'NULL');
              END LOOP;
              RETURN 'no rows';
            END;
            $$
        """);
        assertEquals("only_one,NULL", query("SELECT mv_fewer_cols()"));
    }

    @Test
    void multi_var_with_exit() throws SQLException {
        exec("""
            CREATE OR REPLACE FUNCTION mv_with_exit() RETURNS text LANGUAGE plpgsql AS $$
            DECLARE
              k text; v int;
            BEGIN
              FOR k, v IN SELECT mv_data.k, mv_data.v FROM mv_data ORDER BY mv_data.k LOOP
                IF v = 20 THEN EXIT; END IF;
              END LOOP;
              RETURN k || '=' || v;
            END;
            $$
        """);
        assertEquals("y=20", query("SELECT mv_with_exit()"));
    }

    @Test
    void multi_var_with_continue() throws SQLException {
        exec("""
            CREATE OR REPLACE FUNCTION mv_with_continue() RETURNS text LANGUAGE plpgsql AS $$
            DECLARE
              result text := '';
              k text; v int;
            BEGIN
              FOR k, v IN SELECT mv_data.k, mv_data.v FROM mv_data ORDER BY mv_data.k LOOP
                IF v = 20 THEN CONTINUE; END IF;
                IF result <> '' THEN result := result || ','; END IF;
                result := result || k || '=' || v;
              END LOOP;
              RETURN result;
            END;
            $$
        """);
        assertEquals("x=10,z=30", query("SELECT mv_with_continue()"));
    }

    @Test
    void multi_var_empty_result() throws SQLException {
        exec("""
            CREATE OR REPLACE FUNCTION mv_empty() RETURNS text LANGUAGE plpgsql AS $$
            DECLARE
              k text; v int;
            BEGIN
              FOR k, v IN SELECT mv_data.k, mv_data.v FROM mv_data WHERE false LOOP
                RETURN 'should not reach';
              END LOOP;
              RETURN 'empty';
            END;
            $$
        """);
        assertEquals("empty", query("SELECT mv_empty()"));
    }

    // =========================================================================
    // D. Multi-variable FOR EXECUTE
    // =========================================================================

    @Test
    void multi_var_for_execute() throws SQLException {
        exec("""
            CREATE OR REPLACE FUNCTION mv_for_execute() RETURNS text LANGUAGE plpgsql AS $$
            DECLARE
              result text := '';
              k text; v int;
            BEGIN
              FOR k, v IN EXECUTE 'SELECT k, v FROM mv_data ORDER BY k' LOOP
                IF result <> '' THEN result := result || ','; END IF;
                result := result || k || '=' || v;
              END LOOP;
              RETURN result;
            END;
            $$
        """);
        assertEquals("x=10,y=20,z=30", query("SELECT mv_for_execute()"));
    }
}
