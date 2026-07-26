package com.memgres.cursor;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * WHERE CURRENT OF names the row a cursor is sitting on, and a cursor that has not fetched yet
 * is on no row. lastval() is undefined until the session calls nextval. Expectations captured
 * from a live PostgreSQL 18.0 server.
 *
 * <p>N57 cursor positioning, N58 lastval SQLSTATE.
 */
class CursorPositionAndLastvalTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() {
        if (memgres != null) memgres.close();
    }

    private static Connection open() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    private static String expr(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    // ------------------------------------------------------------------
    // N57 — a cursor before its first fetch is on no row
    // ------------------------------------------------------------------

    @Test
    void currentOfBeforeTheFirstFetchIsAnError() throws Exception {
        try (Connection c = open()) {
            exec(c, "CREATE TABLE cpl_a (id int PRIMARY KEY, v int)");
            exec(c, "INSERT INTO cpl_a VALUES (1,10),(2,20)");
            c.setAutoCommit(false);
            exec(c, "DECLARE cpl_c CURSOR FOR SELECT id FROM cpl_a ORDER BY id");

            SQLException e = assertThrows(SQLException.class,
                    () -> exec(c, "UPDATE cpl_a SET v = 99 WHERE CURRENT OF cpl_c"));
            assertEquals("24000", e.getSQLState());
            c.rollback();
        }
    }

    @Test
    void currentOfAfterAFetchNamesThatRow() throws Exception {
        try (Connection c = open()) {
            exec(c, "CREATE TABLE cpl_b (id int PRIMARY KEY, v int)");
            exec(c, "INSERT INTO cpl_b VALUES (1,10),(2,20)");
            c.setAutoCommit(false);
            exec(c, "DECLARE cpl_c2 CURSOR FOR SELECT id FROM cpl_b ORDER BY id");
            assertEquals("1", expr(c, "FETCH 1 FROM cpl_c2"));
            exec(c, "UPDATE cpl_b SET v = 99 WHERE CURRENT OF cpl_c2");
            c.commit();
            c.setAutoCommit(true);

            assertEquals("99", expr(c, "SELECT v::text FROM cpl_b WHERE id = 1"));
            assertEquals("20", expr(c, "SELECT v::text FROM cpl_b WHERE id = 2"));
        }
    }

    @Test
    void currentOfPastTheEndIsAlsoAnError() throws Exception {
        try (Connection c = open()) {
            exec(c, "CREATE TABLE cpl_c3 (id int PRIMARY KEY, v int)");
            exec(c, "INSERT INTO cpl_c3 VALUES (1,10)");
            c.setAutoCommit(false);
            exec(c, "DECLARE cpl_cc CURSOR FOR SELECT id FROM cpl_c3");
            exec(c, "FETCH ALL FROM cpl_cc");

            SQLException e = assertThrows(SQLException.class,
                    () -> exec(c, "UPDATE cpl_c3 SET v = 99 WHERE CURRENT OF cpl_cc"));
            assertEquals("24000", e.getSQLState());
            c.rollback();
        }
    }

    // ------------------------------------------------------------------
    // N58 — lastval is undefined until the session calls nextval
    // ------------------------------------------------------------------

    @Test
    void lastvalBeforeAnyNextvalReportsItsOwnState() throws Exception {
        try (Connection c = open()) {
            SQLException e = assertThrows(SQLException.class, () -> exec(c, "SELECT lastval()"));
            assertEquals("55000", e.getSQLState());
        }
    }

    @Test
    void lastvalIsScopedToTheSession() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "CREATE SEQUENCE cpl_seq");
            expr(a, "SELECT nextval('cpl_seq')");
            assertEquals("1", expr(a, "SELECT lastval()::text"));

            SQLException e = assertThrows(SQLException.class, () -> exec(b, "SELECT lastval()"));
            assertEquals("55000", e.getSQLState(), "another session has its own lastval");
        }
    }
}
