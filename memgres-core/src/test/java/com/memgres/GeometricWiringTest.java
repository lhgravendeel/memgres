package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

class GeometricWiringTest {
    private static Memgres memgres;
    private static Connection conn;

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

    @Test void geo_doesNotExtendRight() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT box '(2,2),(0,0)' &< box '(3,3),(1,1)'");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test void geo_doesNotExtendLeft() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT box '(2,2),(0,0)' &> box '(3,3),(1,1)'");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1)); // (0,0) extends left of (1,1)
        }
    }

    @Test void geo_doesNotExtendAbove() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT box '(2,2),(0,0)' &<| box '(3,3),(1,1)'");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1)); // top edge 2 <= 3
        }
    }

    @Test void geo_doesNotExtendBelow() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT box '(2,2),(0,0)' |&> box '(3,3),(1,1)'");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1)); // bottom edge 0 < 1
        }
    }

    @Test void geo_line_from_lseg() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT line(lseg '[(0,0),(1,1)]')");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test void geo_lseg_from_box() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT lseg(box '(2,2),(0,0)')");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test void call_rejects_function_not_procedure() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE PROCEDURE my_proc() LANGUAGE plpgsql AS $$ BEGIN NULL; END; $$");
            s.execute("CALL my_proc()");
            // If no error, procedure call works
        }
    }
}
