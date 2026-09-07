package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What an exclusion key needs of its access method.
 *
 * <p>An exclusion constraint is enforced by an index, and an index key needs an operator class the
 * method can compare with. Checked only for the btree methods, {@code EXCLUDE USING gist (a WITH
 * &lt;&gt;)} over an integer was recorded and enforced nothing -- gist has no class for a scalar
 * type until btree_gist adds one. A temporal key is the same constraint under another name, and
 * needs the same class for the columns it compares for equality.
 */
class WhatAnExclusionKeyNeedsTest {

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

    /** Without btree_gist, gist has nothing to compare a scalar with. */
    @Test
    void whatGistCanKeyOn() throws SQLException {
        assertEquals("42704",
                stateOf("CREATE TABLE zwq_a (a int, EXCLUDE USING gist (a WITH <>))"));
        assertEquals("42704",
                stateOf("CREATE TABLE zwq_b (a text, EXCLUDE USING gist (a WITH =))"));
        // A temporal key is the same constraint written another way.
        assertEquals("42704", stateOf("CREATE TABLE zwq_c (id int, v daterange,"
                + " UNIQUE (id, v WITHOUT OVERLAPS))"));
        // A range is what gist indexes without help.
        assertNull(stateOf("CREATE TABLE zwq_d (a int4range, EXCLUDE USING gist (a WITH &&))"));
        // And btree keys a scalar on its own.
        assertNull(stateOf("CREATE TABLE zwq_e (a int, EXCLUDE USING btree (a WITH =))"));
        exec("DROP TABLE zwq_d");
        exec("DROP TABLE zwq_e");
    }

    /** With btree_gist installed, the scalar columns have a class again. */
    @Test
    void whatBtreeGistAdds() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS btree_gist");
        assertNull(stateOf("CREATE TABLE zwq_f (a int, EXCLUDE USING gist (a WITH <>))"));
        assertNull(stateOf("CREATE TABLE zwq_g (id int, v daterange,"
                + " UNIQUE (id, v WITHOUT OVERLAPS))"));
        exec("DROP TABLE zwq_f");
        exec("DROP TABLE zwq_g");
        exec("DROP EXTENSION btree_gist");
    }
}
