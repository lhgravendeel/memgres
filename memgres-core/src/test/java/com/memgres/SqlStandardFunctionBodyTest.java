package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SQL-standard function body syntax (PG 14+):
 * - CREATE FUNCTION ... RETURN expr
 * - CREATE FUNCTION ... BEGIN ATOMIC ... END
 */
class SqlStandardFunctionBodyTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private String queryString(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // ── RETURN expr ──────────────────────────────────────────────────────

    @Test
    void returnExpr_simpleAddition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_add(a int, b int) RETURNS int LANGUAGE SQL RETURN a + b");
            assertEquals(5, queryInt("SELECT ssf_add(2, 3)"));
        }
    }

    @Test
    void returnExpr_stringLiteral() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_hello() RETURNS text LANGUAGE SQL RETURN 'hello world'");
            assertEquals("hello world", queryString("SELECT ssf_hello()"));
        }
    }

    @Test
    void returnExpr_withCoalesce() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_coalesce(a text, b text) RETURNS text LANGUAGE SQL RETURN COALESCE(a, b)");
            assertEquals("fallback", queryString("SELECT ssf_coalesce(NULL, 'fallback')"));
        }
    }

    @Test
    void returnExpr_arithmetic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_mul(x int, y int) RETURNS int LANGUAGE SQL RETURN x * y");
            assertEquals(42, queryInt("SELECT ssf_mul(6, 7)"));
        }
    }

    @Test
    void returnExpr_withCast() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_to_text(n int) RETURNS text LANGUAGE SQL RETURN n::text");
            assertEquals("42", queryString("SELECT ssf_to_text(42)"));
        }
    }

    @Test
    void returnExpr_withAttributes() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_imm(x int) RETURNS int LANGUAGE SQL IMMUTABLE RETURN x + 1");
            assertEquals(6, queryInt("SELECT ssf_imm(5)"));
        }
    }

    @Test
    void returnExpr_orReplace() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_repl(x int) RETURNS int LANGUAGE SQL RETURN x + 1");
            assertEquals(6, queryInt("SELECT ssf_repl(5)"));
            s.execute("CREATE OR REPLACE FUNCTION ssf_repl(x int) RETURNS int LANGUAGE SQL RETURN x + 100");
            assertEquals(105, queryInt("SELECT ssf_repl(5)"));
        }
    }

    @Test
    void returnExpr_caseExpression() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_sign(x int) RETURNS text LANGUAGE SQL RETURN CASE WHEN x > 0 THEN 'positive' WHEN x < 0 THEN 'negative' ELSE 'zero' END");
            assertEquals("positive", queryString("SELECT ssf_sign(5)"));
            assertEquals("negative", queryString("SELECT ssf_sign(-3)"));
            assertEquals("zero", queryString("SELECT ssf_sign(0)"));
        }
    }

    @Test
    void returnExpr_languageBeforeReturns() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_lang_first(x int) RETURNS int LANGUAGE SQL RETURN x * 2");
            assertEquals(10, queryInt("SELECT ssf_lang_first(5)"));
        }
    }

    // ── BEGIN ATOMIC ... END ─────────────────────────────────────────────

    @Test
    void beginAtomic_singleSelect() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_ba_simple(x int) RETURNS int LANGUAGE SQL BEGIN ATOMIC SELECT x + 10; END");
            assertEquals(15, queryInt("SELECT ssf_ba_simple(5)"));
        }
    }

    @Test
    void beginAtomic_multipleStatements() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ssf_ba_tbl (id serial PRIMARY KEY, val text)");
            s.execute("CREATE FUNCTION ssf_ba_insert(v text) RETURNS int LANGUAGE SQL BEGIN ATOMIC INSERT INTO ssf_ba_tbl(val) VALUES(v) RETURNING id; END");
            assertEquals(1, queryInt("SELECT ssf_ba_insert('hello')"));
            assertEquals(2, queryInt("SELECT ssf_ba_insert('world')"));
        }
    }

    @Test
    void beginAtomic_withCaseEnd() throws SQLException {
        // Ensure nested CASE ... END doesn't prematurely close BEGIN ATOMIC
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_ba_case(x int) RETURNS text LANGUAGE SQL BEGIN ATOMIC SELECT CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END; END");
            assertEquals("pos", queryString("SELECT ssf_ba_case(1)"));
            assertEquals("neg", queryString("SELECT ssf_ba_case(-1)"));
        }
    }

    @Test
    void beginAtomic_withAttributes() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_ba_attr(x int) RETURNS int LANGUAGE SQL IMMUTABLE BEGIN ATOMIC SELECT x * 3; END");
            assertEquals(15, queryInt("SELECT ssf_ba_attr(5)"));
        }
    }

    @Test
    void beginAtomic_orReplace() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_ba_repl(x int) RETURNS int LANGUAGE SQL BEGIN ATOMIC SELECT x + 1; END");
            assertEquals(6, queryInt("SELECT ssf_ba_repl(5)"));
            s.execute("CREATE OR REPLACE FUNCTION ssf_ba_repl(x int) RETURNS int LANGUAGE SQL BEGIN ATOMIC SELECT x + 100; END");
            assertEquals(105, queryInt("SELECT ssf_ba_repl(5)"));
        }
    }

    // ── Procedures with SQL-standard body ──────────────────────────────

    @Test
    void returnExpr_procedure() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ssf_proc_tbl (id serial PRIMARY KEY, val text)");
            s.execute("CREATE PROCEDURE ssf_proc_ins(v text) LANGUAGE SQL BEGIN ATOMIC INSERT INTO ssf_proc_tbl(val) VALUES(v); END");
            s.execute("CALL ssf_proc_ins('test')");
            assertEquals("test", queryString("SELECT val FROM ssf_proc_tbl WHERE id = 1"));
        }
    }

    // ── No explicit LANGUAGE clause ──────────────────────────────────────

    @Test
    void returnExpr_implicitLanguageSql() throws SQLException {
        // RETURN without explicit LANGUAGE should default to sql
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_no_lang(x int) RETURNS int RETURN x + 5");
            assertEquals(8, queryInt("SELECT ssf_no_lang(3)"));
        }
    }

    @Test
    void beginAtomic_implicitLanguageSql() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_ba_no_lang(x int) RETURNS int BEGIN ATOMIC SELECT x + 5; END");
            assertEquals(8, queryInt("SELECT ssf_ba_no_lang(3)"));
        }
    }

    // ── Subqueries in RETURN ─────────────────────────────────────────────

    @Test
    void returnExpr_withSubquery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ssf_sub_tbl (id int, val int)");
            s.execute("INSERT INTO ssf_sub_tbl VALUES (1, 10), (2, 20), (3, 30)");
            s.execute("CREATE FUNCTION ssf_max_val() RETURNS int LANGUAGE SQL RETURN (SELECT max(val) FROM ssf_sub_tbl)");
            assertEquals(30, queryInt("SELECT ssf_max_val()"));
        }
    }

    // ── Nested CASE in BEGIN ATOMIC (multiple) ───────────────────────────

    @Test
    void beginAtomic_multipleCaseEnd() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_ba_multi_case(a int, b int) RETURNS text LANGUAGE SQL BEGIN ATOMIC " +
                    "SELECT CASE WHEN a > 0 THEN 'a_pos' ELSE 'a_neg' END || '_' || CASE WHEN b > 0 THEN 'b_pos' ELSE 'b_neg' END; END");
            assertEquals("a_pos_b_neg", queryString("SELECT ssf_ba_multi_case(1, -1)"));
        }
    }

    // ── RETURN with string containing special chars ──────────────────────

    @Test
    void returnExpr_concatStrings() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_greet(name text) RETURNS text LANGUAGE SQL RETURN 'Hello, ' || name || '!'");
            assertEquals("Hello, World!", queryString("SELECT ssf_greet('World')"));
        }
    }

    // ── pg_proc reflection ───────────────────────────────────────────────

    @Test
    void returnExpr_languageIsSql() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION ssf_lang_check(x int) RETURNS int LANGUAGE SQL RETURN x");
            String lang = queryString("SELECT lanname FROM pg_language l JOIN pg_proc p ON p.prolang = l.oid WHERE p.proname = 'ssf_lang_check'");
            assertEquals("sql", lang);
        }
    }
}
