package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a rule answers a statement with.
 *
 * <p>A rule whose action is a query answers the statement with that query's rows, whether or not
 * the statement wrote a RETURNING: {@code DO INSTEAD SELECT} is how pg_settings takes an UPDATE.
 * Kept only for a RETURNING, such a statement came back with a bare command tag -- and a statement
 * that matched no row came back with one too, where PostgreSQL still answers in the query's shape.
 */
class WhatARuleAnswersWithTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement()) {
            assertTrue(s.execute(sql), sql + " should answer with rows");
            try (ResultSet rs = s.getResultSet()) {
                int width = rs.getMetaData().getColumnCount();
                StringBuilder header = new StringBuilder();
                for (int i = 1; i <= width; i++) {
                    if (i > 1) header.append('|');
                    header.append(rs.getMetaData().getColumnName(i));
                }
                out.add(header.toString());
                while (rs.next()) {
                    StringBuilder line = new StringBuilder();
                    for (int i = 1; i <= width; i++) {
                        if (i > 1) line.append('|');
                        line.append(rs.getString(i));
                    }
                    out.add(line.toString());
                }
            }
        }
        return out;
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** The rule's query is the statement's answer. */
    @Test
    void whatADoInsteadQueryAnswers() throws SQLException {
        exec("CREATE TABLE zxg_t (name text, setting text)");
        exec("CREATE VIEW zxg_v AS SELECT name, setting FROM zxg_t");
        exec("INSERT INTO zxg_t VALUES ('a','1')");
        exec("CREATE RULE zxg_u AS ON UPDATE TO zxg_v"
                + " DO INSTEAD SELECT old.name AS n, new.setting AS s");
        assertEquals(java.util.Arrays.asList("n|s", "a|9"),
                rows("UPDATE zxg_v SET setting = '9' WHERE name = 'a'"));
        // A statement that matched nothing still answers in the query's shape.
        assertEquals(java.util.Arrays.asList("n|s"),
                rows("UPDATE zxg_v SET setting = '9' WHERE name = 'nothing'"));
        exec("DROP VIEW zxg_v CASCADE");
        exec("DROP TABLE zxg_t");
    }

    /** pg_settings is writable through the rules PostgreSQL ships on it. */
    @Test
    void whatWritingPgSettingsDoes() throws SQLException {
        assertEquals(java.util.Arrays.asList("set_config", "17MB"),
                rows("UPDATE pg_settings SET setting = '17MB' WHERE name = 'work_mem'"));
        assertEquals("17MB", one("SHOW work_mem"));
        assertEquals(java.util.Arrays.asList("set_config", "4MB"),
                rows("UPDATE pg_settings SET setting = '4MB' WHERE name = 'work_mem'"));
        assertEquals("4MB", one("SHOW work_mem"));
        // A parameter nothing answers to is no row to write, and the answer is empty.
        assertEquals(java.util.Arrays.asList("set_config"),
                rows("UPDATE pg_settings SET setting = '1' WHERE name = 'zxg_nosuch'"));
        // And the rules are in the catalogue.
        assertEquals("pg_settings_n,pg_settings_u",
                one("SELECT string_agg(rulename, ',' ORDER BY rulename)"
                        + " FROM pg_rules WHERE tablename = 'pg_settings'"));
    }
}
