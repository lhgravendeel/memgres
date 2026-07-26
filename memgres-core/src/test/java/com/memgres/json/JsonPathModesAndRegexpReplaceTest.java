package com.memgres.json;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A jsonpath may open with a strict or lax mode word, and its variables bind from the vars
 * argument. In a regexp_replace replacement only a backref is special. Expectations captured
 * from a live PostgreSQL 18.0 server.
 *
 * <p>N32 jsonpath vars, like_regex and strict/lax modes, N26 regexp_replace replacement text.
 */
class JsonPathModesAndRegexpReplaceTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }

    private static String expr(String sql) throws SQLException {
        List<String> r = rows(sql);
        return r.isEmpty() ? null : r.get(0);
    }

    private static String state(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql));
        return e.getSQLState();
    }

    // ------------------------------------------------------------------
    // N32 — jsonpath modes and variables
    // ------------------------------------------------------------------

    @Test
    void aModeWordIsPartOfThePath() throws Exception {
        assertEquals("1", expr("SELECT jsonb_path_query('{\"b\":1}','strict $.b')::text"));
        assertEquals("1", expr("SELECT jsonb_path_query('{\"b\":1}','lax $.b')::text"));
    }

    /** lax treats a scalar as a one-element array for an array accessor. */
    @Test
    void laxUnwrapsAScalarForAnArrayAccessor() throws Exception {
        assertEquals(Arrays.asList("1"), rows("SELECT jsonb_path_query('1','lax $[*]')::text"));
    }

    @Test
    void strictTurnsAMissingStepIntoAnError() {
        assertEquals("2203A", state("SELECT jsonb_path_query('{\"a\":1}','strict $.b')"));
        assertEquals("2203A", state("SELECT jsonb_path_exists('{\"a\":1}','strict $.b')"));
    }

    @Test
    void laxReturnsNoRowsForAMissingStep() throws Exception {
        assertEquals(0, rows("SELECT jsonb_path_query('{\"a\":1}','lax $.b')::text").size());
    }

    @Test
    void filterVariablesBindFromTheVarsArgument() throws Exception {
        assertEquals(Arrays.asList("2", "3"), rows(
                "SELECT jsonb_path_query('[1,2,3]','$[*] ? (@ > $min)','{\"min\":1}')::text"));
        assertEquals(Arrays.asList("2", "3"), rows(
                "SELECT q::text FROM jsonb_path_query('[1,2,3]','$[*] ? (@ > $min)','{\"min\":1}') q"));
    }

    // ------------------------------------------------------------------
    // N26 — the replacement text is not a Java replacement string
    // ------------------------------------------------------------------

    @Test
    void aDollarSignInAReplacementIsLiteralText() throws Exception {
        assertEquals("ax$yc", expr("SELECT regexp_replace('abc','b','x$y')"));
        assertEquals("a$1c", expr("SELECT regexp_replace('abc','b','$1')"));
    }

    @Test
    void numberedBackrefsStillSubstitute() throws Exception {
        assertEquals("a[b]c", expr("SELECT regexp_replace('abc','(b)','[\\1]')"));
        assertEquals("a<b>c", expr("SELECT regexp_replace('abc','(b)','<\\&>')"));
    }

    /** PG substitutes the empty string for a group the pattern does not have. */
    @Test
    void aBackrefToAMissingGroupSubstitutesNothing() throws Exception {
        assertEquals("ac", expr("SELECT regexp_replace('abc','b','\\1')"));
    }
}
