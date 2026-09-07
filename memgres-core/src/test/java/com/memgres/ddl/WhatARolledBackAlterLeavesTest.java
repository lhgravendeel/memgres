package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What an ALTER leaves behind when its transaction rolls back.
 *
 * <p>DDL is transactional, so a statement whose transaction rolls back never happened. Recorded
 * nowhere, a label added to an enum inside a rolled-back transaction stayed on the type, and every
 * later read of it saw a value the database had agreed to forget -- a value no row could hold and
 * no dump would carry. A sequence left with the increment or the bounds a rolled-back statement
 * gave it goes on handing out numbers nobody asked for, for the rest of the session.
 *
 * <p>What is committed is still committed: the record only matters when the transaction ends the
 * other way.
 */
class WhatARolledBackAlterLeavesTest {

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

    private static String labelsOf(String type) throws SQLException {
        return one("SELECT string_agg(enumlabel, ',' ORDER BY enumsortorder) FROM pg_enum e"
                + " JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='" + type + "'");
    }

    /** A label added in a rolled-back transaction is not on the type. */
    @Test
    void whatARolledBackAddValueLeaves() throws SQLException {
        exec("CREATE TYPE zwr_e AS ENUM ('a','b')");
        exec("BEGIN");
        exec("ALTER TYPE zwr_e ADD VALUE 'c'");
        exec("ROLLBACK");
        assertEquals("a,b", labelsOf("zwr_e"));
        // A rename goes back too.
        exec("ALTER TYPE zwr_e ADD VALUE 'c'");
        assertEquals("a,b,c", labelsOf("zwr_e"));
        exec("BEGIN");
        exec("ALTER TYPE zwr_e RENAME VALUE 'c' TO 'z'");
        exec("ROLLBACK");
        assertEquals("a,b,c", labelsOf("zwr_e"));
        // And what is committed stays.
        exec("BEGIN");
        exec("ALTER TYPE zwr_e ADD VALUE 'd'");
        exec("COMMIT");
        assertEquals("a,b,c,d", labelsOf("zwr_e"));
        exec("DROP TYPE zwr_e");
    }

    /** A sequence rolled back keeps the settings it had. */
    @Test
    void whatARolledBackAlterSequenceLeaves() throws SQLException {
        exec("CREATE SEQUENCE zwr_s");
        exec("BEGIN");
        exec("ALTER SEQUENCE zwr_s INCREMENT BY 7 MAXVALUE 5000 CYCLE");
        exec("ROLLBACK");
        assertEquals("1|9223372036854775807|false",
                one("SELECT increment_by::text || '|' || max_value::text || '|' || cycle::text"
                        + " FROM pg_sequences WHERE sequencename='zwr_s'"));
        // The numbers it hands out are the ones it always would have.
        assertEquals("1", one("SELECT nextval('zwr_s')::text"));
        assertEquals("2", one("SELECT nextval('zwr_s')::text"));
        // And a committed change is kept.
        exec("BEGIN");
        exec("ALTER SEQUENCE zwr_s INCREMENT BY 10");
        exec("COMMIT");
        assertEquals("10", one("SELECT increment_by::text FROM pg_sequences"
                + " WHERE sequencename='zwr_s'"));
        assertEquals("12", one("SELECT nextval('zwr_s')::text"));
        exec("DROP SEQUENCE zwr_s");
    }
}
