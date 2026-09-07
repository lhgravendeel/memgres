package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a MOVE reports to GET DIAGNOSTICS.
 *
 * <p>A MOVE passes over rows without handing any back, and how many it passed over is the only
 * thing it has to report. Left at whatever the statement before it set, {@code ROW_COUNT} after a
 * MOVE of two said nothing had moved -- while the cursor had in fact moved, so the FETCH that
 * followed disagreed with the count of the step that got it there.
 */
class WhatAMoveReportsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE TABLE zwm_t (id int)");
        exec("INSERT INTO zwm_t SELECT g FROM generate_series(1,6) g");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TABLE zwm_t");
            conn.close();
        }
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

    /** The count is how far the cursor went, and the FETCH after it agrees. */
    @Test
    void whatAMoveCounts() throws SQLException {
        assertEquals("2/3", moving("move forward 2 from c"));
        // A MOVE of one row, then the row after it.
        assertEquals("1/2", moving("move forward 1 from c"));
        // A MOVE past the end reports only the rows there were to pass over, and finds nothing.
        assertEquals("6/", moving("move forward 20 from c"));
        // Moving nowhere moves nothing.
        assertEquals("0/1", moving("move forward 0 from c"));
    }

    /** Run a body that moves, reads ROW_COUNT, then fetches. */
    private static String moving(String move) throws SQLException {
        exec("CREATE FUNCTION zwm_f() RETURNS text AS $$"
                + " declare c scroll cursor for select id from zwm_t order by id; v int; n int;"
                + " begin open c; " + move + "; get diagnostics n = row_count;"
                + " fetch from c into v; close c;"
                + " return n::text || '/' || coalesce(v::text,''); end $$ LANGUAGE plpgsql");
        try {
            return one("SELECT zwm_f()");
        } finally {
            exec("DROP FUNCTION zwm_f()");
        }
    }
}
