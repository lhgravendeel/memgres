package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a loop written over a cursor reads.
 *
 * <p>{@code FOR r IN c LOOP}, where c is a cursor the block declared, loops over what that cursor
 * was declared to read: PostgreSQL opens it, runs the body over its rows, and closes it again.
 * Taken for a query written out in full, the cursor's name was handed to the parser as SQL and the
 * loop was a syntax error -- and a cursor declared with parameters could not be looped over at all.
 */
class WhatALoopOverACursorReadsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE TABLE zwc_t (id int, nm text)");
        exec("INSERT INTO zwc_t VALUES (1,'a'),(2,'b'),(3,'c')");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TABLE zwc_t");
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

    /** The loop reads what the cursor was declared to read. */
    @Test
    void whatTheLoopReads() throws SQLException {
        assertEquals("abc", running(
                "declare c cursor for select nm from zwc_t order by id; r record; s text := '';",
                "for r in c loop s := s || r.nm; end loop;"));
        // The loop closes the cursor again, so a second loop reads it afresh.
        assertEquals("abcabc", running(
                "declare c cursor for select nm from zwc_t order by id; r record; s text := '';",
                "for r in c loop s := s || r.nm; end loop;"
                        + " for r in c loop s := s || r.nm; end loop;"));
        // A cursor declared with parameters is looped over with them.
        assertEquals("bc", running(
                "declare c cursor (lo int) for select nm from zwc_t where id >= lo order by id;"
                        + " r record; s text := '';",
                "for r in c(2) loop s := s || r.nm; end loop;"));
    }

    /** Run a body that ends by returning s. */
    private static String running(String declarations, String body) throws SQLException {
        exec("CREATE FUNCTION zwc_f() RETURNS text AS $$ " + declarations
                + " begin " + body + " return s; end $$ LANGUAGE plpgsql");
        try {
            return one("SELECT zwc_f()");
        } finally {
            exec("DROP FUNCTION zwc_f()");
        }
    }
}
