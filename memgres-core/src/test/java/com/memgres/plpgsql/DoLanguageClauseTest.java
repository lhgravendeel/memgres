package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DO statements with the optional LANGUAGE clause.
 *
 * PostgreSQL syntax:  DO [ LANGUAGE lang_name ] code
 *
 * The LANGUAGE clause can appear before the code body and is optional.
 * When omitted, plpgsql is assumed. The language name can be an identifier
 * or a string literal ('plpgsql').
 */
class DoLanguageClauseTest {

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
    // LANGUAGE as string literal before body
    // =========================================================================

    @Test
    void testDoLanguageStringLiteralPlpgsql() throws SQLException {
        exec("""
            DO LANGUAGE 'plpgsql' $$
            BEGIN
                RAISE NOTICE 'language as string literal';
            END;
            $$
        """);
    }

    @Test
    void testDoLanguageStringLiteralSql() throws SQLException {
        exec("""
            DO LANGUAGE 'sql'
            $$ SELECT 1; $$
        """);
    }

    // =========================================================================
    // LANGUAGE as identifier before body
    // =========================================================================

    @Test
    void testDoLanguageIdentifierPlpgsql() throws SQLException {
        exec("""
            DO LANGUAGE plpgsql $$
            BEGIN
                RAISE NOTICE 'language as identifier';
            END;
            $$
        """);
    }

    @Test
    void testDoLanguageIdentifierSql() throws SQLException {
        exec("""
            DO LANGUAGE sql
            $$ SELECT 1; $$
        """);
    }

    // =========================================================================
    // Without LANGUAGE clause (default = plpgsql, baseline)
    // =========================================================================

    @Test
    void testDoWithoutLanguageClause() throws SQLException {
        exec("""
            DO $$
            BEGIN
                RAISE NOTICE 'no language clause';
            END;
            $$
        """);
    }

    // =========================================================================
    // With actual logic inside the DO body
    // =========================================================================

    @Test
    void testDoLanguageWithConditionalExtension() throws SQLException {
        // Real-world pattern: conditionally create an extension
        exec("""
            DO LANGUAGE 'plpgsql' $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'btree_gist') THEN
                    CREATE EXTENSION IF NOT EXISTS btree_gist;
                END IF;
            END;
            $$
        """);
    }

    @Test
    void testDoLanguageWithTableCreation() throws SQLException {
        exec("""
            DO LANGUAGE 'plpgsql' $$
            BEGIN
                CREATE TABLE IF NOT EXISTS lang_test (id serial PRIMARY KEY, val text);
                INSERT INTO lang_test (val) VALUES ('from_do_block');
            END;
            $$
        """);
        assertEquals("from_do_block", query1("SELECT val FROM lang_test"));
    }

    @Test
    void testDoLanguageWithDeclareBlock() throws SQLException {
        exec("CREATE TABLE lang_declare_test (id serial PRIMARY KEY, result int)");
        exec("""
            DO LANGUAGE 'plpgsql' $$
            DECLARE
                x int := 10;
                y int := 20;
            BEGIN
                INSERT INTO lang_declare_test (result) VALUES (x + y);
            END;
            $$
        """);
        assertEquals("30", query1("SELECT result FROM lang_declare_test"));
    }

    @Test
    void testDoLanguageWithExceptionHandler() throws SQLException {
        exec("""
            DO LANGUAGE 'plpgsql' $$
            BEGIN
                CREATE TABLE lang_exc_test (id serial PRIMARY KEY);
            EXCEPTION WHEN duplicate_table THEN
                RAISE NOTICE 'table already exists';
            END;
            $$
        """);
        // Run again; should not error thanks to exception handler
        exec("""
            DO LANGUAGE 'plpgsql' $$
            BEGIN
                CREATE TABLE lang_exc_test (id serial PRIMARY KEY);
            EXCEPTION WHEN duplicate_table THEN
                RAISE NOTICE 'table already exists';
            END;
            $$
        """);
    }

    // =========================================================================
    // LANGUAGE with tagged dollar quotes
    // =========================================================================

    @Test
    void testDoLanguageWithTaggedDollarQuote() throws SQLException {
        exec("""
            DO LANGUAGE 'plpgsql' $body$
            BEGIN
                RAISE NOTICE 'tagged dollar quote';
            END;
            $body$
        """);
    }

    @Test
    void testDoLanguageWithTripleDollar() throws SQLException {
        exec("""
            DO LANGUAGE 'plpgsql' $$$
            BEGIN
                RAISE NOTICE 'triple dollar';
            END;
            $$$
        """);
    }

    // =========================================================================
    // LANGUAGE clause is case-insensitive
    // =========================================================================

    @Test
    void testDoLanguageCaseInsensitive() throws SQLException {
        exec("""
            DO language 'plpgsql' $$
            BEGIN
                RAISE NOTICE 'lowercase language keyword';
            END;
            $$
        """);
    }

    @Test
    void testDoLanguageMixedCase() throws SQLException {
        exec("""
            DO Language 'plpgsql' $$
            BEGIN
                RAISE NOTICE 'mixed case';
            END;
            $$
        """);
    }

    // =========================================================================
    // LANGUAGE 'plpgsql' with FOR loop
    // =========================================================================

    @Test
    void testDoLanguageWithForLoop() throws SQLException {
        exec("CREATE TABLE lang_loop (i int)");
        exec("""
            DO LANGUAGE 'plpgsql' $$
            BEGIN
                FOR i IN 1..5 LOOP
                    INSERT INTO lang_loop VALUES (i);
                END LOOP;
            END;
            $$
        """);
        assertEquals("5", query1("SELECT COUNT(*) FROM lang_loop"));
    }

    // =========================================================================
    // LANGUAGE 'plpgsql' with IF NOT EXISTS pattern
    // =========================================================================

    @Test
    void testDoLanguageConditionalColumnAdd() throws SQLException {
        exec("CREATE TABLE lang_cond (id serial PRIMARY KEY, name text)");
        exec("""
            DO LANGUAGE 'plpgsql' $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'lang_cond' AND column_name = 'email'
                ) THEN
                    ALTER TABLE lang_cond ADD COLUMN email text;
                END IF;
            END;
            $$
        """);
        exec("INSERT INTO lang_cond (name, email) VALUES ('test', 'test@test.com')");
        assertEquals("test@test.com", query1("SELECT email FROM lang_cond WHERE name = 'test'"));
    }

    // =========================================================================
    // LANGUAGE 'plpgsql' with PERFORM
    // =========================================================================

    @Test
    void testDoLanguageWithPerform() throws SQLException {
        exec("CREATE TABLE lang_perf (id serial PRIMARY KEY, visited boolean DEFAULT false)");
        exec("INSERT INTO lang_perf (visited) VALUES (false)");
        exec("""
            DO LANGUAGE 'plpgsql' $$
            BEGIN
                PERFORM id FROM lang_perf WHERE id = 1;
                IF FOUND THEN
                    UPDATE lang_perf SET visited = true WHERE id = 1;
                END IF;
            END;
            $$
        """);
        assertEquals("true", query1("SELECT visited::text FROM lang_perf WHERE id = 1"));
    }
}
