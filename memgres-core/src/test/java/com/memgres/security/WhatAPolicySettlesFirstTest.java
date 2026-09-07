package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What CREATE POLICY settles first.
 *
 * <p>The roles a policy names are settled before anything is looked up about the relation, so a
 * policy written for a role nobody created is reported as that whatever is wrong with the
 * relation. Checked last, the reader was told about a missing schema or a relation of the wrong
 * kind and fixed that, only to be told about the role on the next attempt.
 */
class WhatAPolicySettlesFirstTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE VIEW zwv_v AS SELECT 1 AS a");
        exec("CREATE TABLE zwv_t (i int)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP VIEW zwv_v");
            exec("DROP TABLE zwv_t");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The role is what is reported, whatever else is wrong. */
    @Test
    void whatIsReportedFirst() {
        assertEquals("42704", stateOf(
                "CREATE POLICY zwv_p ON zwv_nothing FOR SELECT TO zwv_norole USING (true)"));
        assertEquals("42704", stateOf(
                "CREATE POLICY zwv_p ON zwv_nosch.zwv_t FOR SELECT TO zwv_norole USING (true)"));
        assertEquals("42704", stateOf(
                "CREATE POLICY zwv_p ON zwv_v FOR SELECT TO zwv_norole USING (true)"));
        // With the roles settled, the relation is what is left to complain about.
        assertEquals("42P01", stateOf("CREATE POLICY zwv_p ON zwv_nothing FOR SELECT USING (true)"));
        assertEquals("3F000", stateOf(
                "CREATE POLICY zwv_p ON zwv_nosch.zwv_t FOR SELECT USING (true)"));
        assertEquals("42809", stateOf("CREATE POLICY zwv_p ON zwv_v FOR SELECT USING (true)"));
    }

    /** ALTER POLICY settles its roles the same way. */
    @Test
    void whatAlteringSettlesFirst() throws SQLException {
        exec("CREATE ROLE zwv_r");
        exec("CREATE POLICY zwv_p1 ON zwv_t FOR SELECT TO zwv_r USING (true)");
        assertEquals("42704", stateOf("ALTER POLICY zwv_p1 ON zwv_nothing TO zwv_norole"));
        assertEquals("42704", stateOf("ALTER POLICY zwv_p1 ON zwv_t TO zwv_norole"));
        assertEquals("42P01", stateOf("ALTER POLICY zwv_p1 ON zwv_nothing TO zwv_r"));
        assertNull(stateOf("ALTER POLICY zwv_p1 ON zwv_t TO zwv_r"));
        exec("DROP POLICY zwv_p1 ON zwv_t");
        exec("DROP ROLE zwv_r");
    }
}
