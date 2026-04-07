package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DO block and PL/pgSQL features found in real-world migration scripts.
 */
class DoBlockPlpgsqlCompatTest {

    static Memgres memgres;
    static Connection conn;

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

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // =========================================================================
    // Labeled blocks (<<label_name>>)
    // =========================================================================

    @Test
    void testLabeledBlock() throws SQLException {
        // Common in real-world migration scripts
        exec("""
            DO $$
            <<my_block>>
            DECLARE
                x int := 0;
            BEGIN
                x := x + 1;
            END my_block;
            $$
        """);
    }

    @Test
    void testLabeledBlockWithConditionalLogic() throws SQLException {
        exec("CREATE TABLE lb_test (id serial PRIMARY KEY, name text)");
        exec("""
            DO $$
            <<migration>>
            DECLARE
                row_count int;
            BEGIN
                SELECT COUNT(*) INTO row_count FROM lb_test;
                IF row_count = 0 THEN
                    INSERT INTO lb_test (name) VALUES ('seed');
                END IF;
            END migration;
            $$
        """);
        assertEquals("seed", query1("SELECT name FROM lb_test"));
    }

    // =========================================================================
    // FOREACH ... IN ARRAY
    // =========================================================================

    @Test
    void testForeachInArray() throws SQLException {
        exec("CREATE TABLE foreach_test (id serial PRIMARY KEY, tbl_name text)");
        exec("""
            DO $$
            DECLARE
                i text;
                arr text[] := array['users', 'orders', 'products'];
            BEGIN
                FOREACH i IN ARRAY arr LOOP
                    INSERT INTO foreach_test (tbl_name) VALUES (i);
                END LOOP;
            END;
            $$
        """);
        assertEquals("3", query1("SELECT COUNT(*) FROM foreach_test"));
    }

    @Test
    void testForeachInArrayWithIndex() throws SQLException {
        exec("CREATE TABLE foreach_idx_test (idx int, val text)");
        exec("""
            DO $$
            DECLARE
                i text;
                idx int := 0;
                arr text[] := array['a', 'b', 'c'];
            BEGIN
                FOREACH i IN ARRAY arr LOOP
                    idx := idx + 1;
                    INSERT INTO foreach_idx_test VALUES (idx, i);
                END LOOP;
            END;
            $$
        """);
        assertEquals("b", query1("SELECT val FROM foreach_idx_test WHERE idx = 2"));
    }

    // =========================================================================
    // EXECUTE FORMAT (dynamic SQL)
    // =========================================================================

    @Test
    void testExecuteFormatCreateTable() throws SQLException {
        exec("""
            DO $$
            DECLARE
                tbl_name text := 'dynamic_table';
            BEGIN
                EXECUTE FORMAT('CREATE TABLE %I (id serial PRIMARY KEY, val text)', tbl_name);
            END;
            $$
        """);
        exec("INSERT INTO dynamic_table (val) VALUES ('created_dynamically')");
        assertEquals("created_dynamically", query1("SELECT val FROM dynamic_table"));
    }

    @Test
    void testExecuteFormatWithMultipleArgs() throws SQLException {
        exec("CREATE TABLE format_target (id serial PRIMARY KEY, category text, active boolean DEFAULT true)");
        exec("""
            DO $$
            DECLARE
                tbl text := 'format_target';
                col text := 'category';
            BEGIN
                EXECUTE FORMAT('INSERT INTO %I (%I) VALUES (%L)', tbl, col, 'test_value');
            END;
            $$
        """);
        assertEquals("test_value", query1("SELECT category FROM format_target"));
    }

    @Test
    void testExecuteFormatLoop() throws SQLException {
        // Pattern: create RLS policies for multiple tables dynamically
        exec("""
            DO $$
            DECLARE
                tbl text;
                tables text[] := array['alpha', 'beta', 'gamma'];
            BEGIN
                FOREACH tbl IN ARRAY tables LOOP
                    EXECUTE FORMAT('CREATE TABLE %I (id serial PRIMARY KEY, val text)', tbl);
                END LOOP;
            END;
            $$
        """);
        exec("INSERT INTO alpha (val) VALUES ('ok')");
        assertEquals("ok", query1("SELECT val FROM alpha"));
    }

    // =========================================================================
    // RAISE NOTICE / WARNING / EXCEPTION
    // =========================================================================

    @Test
    void testRaiseNotice() throws SQLException {
        exec("""
            DO $$
            BEGIN
                RAISE NOTICE 'Migration step complete: % rows processed', 42;
            END;
            $$
        """);
    }

    @Test
    void testRaiseWarning() throws SQLException {
        exec("""
            DO $$
            BEGIN
                RAISE WARNING 'Deprecated feature used';
            END;
            $$
        """);
    }

    @Test
    void testRaiseException() throws SQLException {
        assertThrows(SQLException.class, () -> exec("""
            DO $$
            BEGIN
                RAISE EXCEPTION 'Something went wrong: %', 'details here';
            END;
            $$
        """));
    }

    // =========================================================================
    // EXCEPTION handling (BEGIN ... EXCEPTION WHEN ... THEN ...)
    // =========================================================================

    @Test
    void testExceptionHandling() throws SQLException {
        exec("""
            DO $$
            BEGIN
                CREATE TABLE exc_test (id serial PRIMARY KEY);
            EXCEPTION WHEN duplicate_table THEN
                RAISE NOTICE 'Table already exists, skipping';
            END;
            $$
        """);
        // Run again; should not error
        exec("""
            DO $$
            BEGIN
                CREATE TABLE exc_test (id serial PRIMARY KEY);
            EXCEPTION WHEN duplicate_table THEN
                RAISE NOTICE 'Table already exists, skipping';
            END;
            $$
        """);
    }

    @Test
    void testExceptionWhenOthers() throws SQLException {
        // WHEN OTHERS catches all exceptions
        exec("""
            DO $$
            BEGIN
                EXECUTE 'SELECT 1 FROM nonexistent_table';
            EXCEPTION WHEN OTHERS THEN
                RAISE NOTICE 'Caught error: %', SQLERRM;
            END;
            $$
        """);
    }

    // =========================================================================
    // SELECT INTO in PL/pgSQL
    // =========================================================================

    @Test
    void testSelectInto() throws SQLException {
        exec("CREATE TABLE si_test (id serial PRIMARY KEY, val int)");
        exec("INSERT INTO si_test (val) VALUES (42)");
        exec("""
            DO $$
            DECLARE
                result int;
            BEGIN
                SELECT val INTO result FROM si_test WHERE id = 1;
                IF result != 42 THEN
                    RAISE EXCEPTION 'Expected 42, got %', result;
                END IF;
            END;
            $$
        """);
    }

    @Test
    void testSelectIntoStrict() throws SQLException {
        exec("CREATE TABLE sis_test (id serial PRIMARY KEY, val int)");
        exec("INSERT INTO sis_test (val) VALUES (10)");
        exec("""
            DO $$
            DECLARE
                result int;
            BEGIN
                SELECT val INTO STRICT result FROM sis_test WHERE id = 1;
            END;
            $$
        """);
    }

    // =========================================================================
    // IF NOT EXISTS pattern in DO block
    // =========================================================================

    @Test
    void testConditionalColumnAddition() throws SQLException {
        exec("CREATE TABLE cond_col_test (id serial PRIMARY KEY, name text)");
        exec("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'cond_col_test' AND column_name = 'email'
                ) THEN
                    ALTER TABLE cond_col_test ADD COLUMN email text;
                END IF;
            END;
            $$
        """);
        exec("INSERT INTO cond_col_test (name, email) VALUES ('test', 'test@test.com')");
    }

    @Test
    void testConditionalTypeCreation() throws SQLException {
        exec("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'custom_status') THEN
                    CREATE TYPE custom_status AS ENUM ('active', 'inactive', 'pending');
                END IF;
            END;
            $$
        """);
        exec("CREATE TABLE cs_test (id serial PRIMARY KEY, status custom_status)");
        exec("INSERT INTO cs_test (status) VALUES ('active')");
    }

    // =========================================================================
    // FOR ... LOOP with query
    // =========================================================================

    @Test
    void testForLoopWithQuery() throws SQLException {
        exec("CREATE TABLE for_source (id serial PRIMARY KEY, val int)");
        exec("CREATE TABLE for_dest (id int, doubled int)");
        exec("INSERT INTO for_source (val) VALUES (1), (2), (3)");
        exec("""
            DO $$
            DECLARE
                rec RECORD;
            BEGIN
                FOR rec IN SELECT id, val FROM for_source LOOP
                    INSERT INTO for_dest VALUES (rec.id, rec.val * 2);
                END LOOP;
            END;
            $$
        """);
        assertEquals("6", query1("SELECT doubled FROM for_dest WHERE id = 3"));
    }

    // =========================================================================
    // PERFORM (execute query, discard result)
    // =========================================================================

    @Test
    void testPerform() throws SQLException {
        exec("CREATE TABLE perf_test (id serial PRIMARY KEY, counter int DEFAULT 0)");
        exec("INSERT INTO perf_test (counter) VALUES (0)");
        exec("""
            DO $$
            BEGIN
                PERFORM id FROM perf_test WHERE id = 1;
                IF FOUND THEN
                    UPDATE perf_test SET counter = counter + 1 WHERE id = 1;
                END IF;
            END;
            $$
        """);
        assertEquals("1", query1("SELECT counter FROM perf_test WHERE id = 1"));
    }

    // =========================================================================
    // RETURN QUERY in PL/pgSQL functions
    // =========================================================================

    @Test
    void testReturnQuery() throws SQLException {
        exec("CREATE TABLE rq_test (id serial PRIMARY KEY, name text, active boolean)");
        exec("INSERT INTO rq_test (name, active) VALUES ('a', true), ('b', false), ('c', true)");
        exec("""
            CREATE FUNCTION get_active_names()
            RETURNS SETOF text LANGUAGE plpgsql AS $$
            BEGIN
                RETURN QUERY SELECT name FROM rq_test WHERE active = true ORDER BY name;
            END;
            $$
        """);
        assertEquals("a", query1("SELECT * FROM get_active_names() LIMIT 1"));
    }

    // =========================================================================
    // WHILE ... LOOP
    // =========================================================================

    @Test
    void testWhileLoop() throws SQLException {
        exec("CREATE TABLE while_test (i int)");
        exec("""
            DO $$
            DECLARE
                counter int := 1;
            BEGIN
                WHILE counter <= 5 LOOP
                    INSERT INTO while_test VALUES (counter);
                    counter := counter + 1;
                END LOOP;
            END;
            $$
        """);
        assertEquals("5", query1("SELECT COUNT(*) FROM while_test"));
    }

    // =========================================================================
    // EXIT / CONTINUE in loops
    // =========================================================================

    @Test
    void testExitWhen() throws SQLException {
        exec("CREATE TABLE exit_test (val int)");
        exec("""
            DO $$
            DECLARE
                i int := 0;
            BEGIN
                LOOP
                    i := i + 1;
                    EXIT WHEN i > 3;
                    INSERT INTO exit_test VALUES (i);
                END LOOP;
            END;
            $$
        """);
        assertEquals("3", query1("SELECT COUNT(*) FROM exit_test"));
    }

    @Test
    void testContinueWhen() throws SQLException {
        exec("CREATE TABLE continue_test (val int)");
        exec("""
            DO $$
            BEGIN
                FOR i IN 1..10 LOOP
                    CONTINUE WHEN i % 2 = 0;
                    INSERT INTO continue_test VALUES (i);
                END LOOP;
            END;
            $$
        """);
        // Only odd numbers: 1, 3, 5, 7, 9
        assertEquals("5", query1("SELECT COUNT(*) FROM continue_test"));
    }
}
