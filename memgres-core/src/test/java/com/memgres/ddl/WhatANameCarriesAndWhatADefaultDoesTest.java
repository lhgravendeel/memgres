package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a name carries, and what a default that cannot be worked out does.
 *
 * <p>A name running on into more letters is a different name: a column called
 * {@code current_date_col} carries the letters of {@code current_date} and is not it. Looked for
 * in the text of an expression, those letters made a generation expression over such a column
 * refuse as volatile and an index over one refuse for holding a value function it does not hold.
 *
 * <p>A default is evaluated while the row is being written, and what goes wrong there is what
 * PostgreSQL reports. Kept as the text it was written as, a column defaulting to {@code 1/0} was
 * filled with the string.
 *
 * <p>A relation name belongs to one relation of any kind, and the grammar's integer is four bytes
 * wide -- a count too wide for one is a syntax error where the digits stand.
 */
class WhatANameCarriesAndWhatADefaultDoesTest {

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

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** A column whose name begins with a value word is a column, not the value word. */
    @Test
    void aColumnThatBeginsWithAValueWord() throws SQLException {
        assertNull(stateOf("CREATE TABLE zwn_gg (current_date_col int,"
                + " g int GENERATED ALWAYS AS (current_date_col + 1) STORED)"));
        exec("CREATE TABLE zwn_ii (localtime_col int)");
        assertNull(stateOf("CREATE INDEX zwn_ix ON zwn_ii ((localtime_col + 1))"));
        // The value word itself is still refused where PostgreSQL refuses it.
        assertEquals("42P17", stateOf("CREATE TABLE zwn_bad (d date"
                + " GENERATED ALWAYS AS (current_date) STORED)"));
        exec("DROP TABLE zwn_ii");
        exec("DROP TABLE zwn_gg");
    }

    /** A default that cannot be worked out is what the write reports. */
    @Test
    void whatADefaultThatCannotBeWorkedOutDoes() throws SQLException {
        exec("CREATE TABLE zwn_dt (a text DEFAULT (1/0)::text)");
        assertEquals("22012", stateOf("INSERT INTO zwn_dt DEFAULT VALUES"));
        assertTrue(messageOf("INSERT INTO zwn_dt DEFAULT VALUES").contains("division by zero"));
        assertEquals("0", one("SELECT count(*)::text FROM zwn_dt"));
        exec("CREATE TABLE zwn_dz (a int DEFAULT 1/0)");
        assertEquals("22012", stateOf("INSERT INTO zwn_dz DEFAULT VALUES"));
        exec("DROP TABLE zwn_dz");
        exec("DROP TABLE zwn_dt");
    }

    /** A relation name is taken by whatever kind of relation holds it. */
    @Test
    void whichRelationsHoldAName() throws SQLException {
        exec("CREATE TABLE zwn_v AS SELECT 1 AS y");
        assertEquals("42P07", stateOf("CREATE TABLE zwn_v AS SELECT 2 AS y"));
        exec("CREATE VIEW zwn_w AS SELECT 1 AS y");
        assertEquals("42P07", stateOf("CREATE TABLE zwn_w AS SELECT 2 AS y"));
        assertTrue(messageOf("CREATE TABLE zwn_w AS SELECT 2 AS y")
                .contains("relation \"zwn_w\" already exists"));
        exec("CREATE SEQUENCE zwn_s");
        assertEquals("42P07", stateOf("CREATE TABLE zwn_s AS SELECT 2 AS y"));
        // IF NOT EXISTS leaves what is there alone.
        assertNull(stateOf("CREATE TABLE IF NOT EXISTS zwn_v AS SELECT 3 AS y"));
        assertEquals("1", one("SELECT y::text FROM zwn_v"));
        exec("DROP SEQUENCE zwn_s");
        exec("DROP VIEW zwn_w");
        exec("DROP TABLE zwn_v");
    }

    /** A hash partition's counts are the grammar's integers. */
    @Test
    void whatAHashBoundMayBe() throws SQLException {
        exec("CREATE TABLE zwn_ht (a int) PARTITION BY HASH (a)");
        assertEquals("42601", stateOf("CREATE TABLE zwn_ht1 PARTITION OF zwn_ht"
                + " FOR VALUES WITH (MODULUS 99999999999, REMAINDER 0)"));
        assertTrue(messageOf("CREATE TABLE zwn_ht1 PARTITION OF zwn_ht"
                + " FOR VALUES WITH (MODULUS 99999999999, REMAINDER 0)")
                .contains("syntax error at or near \"99999999999\""));
        assertNull(stateOf("CREATE TABLE zwn_ht1 PARTITION OF zwn_ht"
                + " FOR VALUES WITH (MODULUS 4, REMAINDER 0)"));
        exec("DROP TABLE zwn_ht");
    }
}
