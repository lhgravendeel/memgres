package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that ORDER BY ... DESC NULLS FIRST works correctly when the SELECT
 * list contains a set-returning function (SRF).
 */
class SrfOrderByNullsTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void desc_nulls_first_with_srf() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE srf_sort (id int, val int)");
            s.execute("INSERT INTO srf_sort VALUES (1, NULL), (2, 10), (3, 5)");
            ResultSet rs = s.executeQuery(
                    "SELECT id, val, generate_series(1,1) AS gs FROM srf_sort ORDER BY val DESC NULLS FIRST");
            // Expected order: NULL first, then 10, then 5
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"), "NULL should come first with DESC NULLS FIRST");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"), "10 should come second");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"), "5 should come last");
        }
    }

    @Test void asc_nulls_last_with_srf() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE srf_sort2 (id int, val int)");
            s.execute("INSERT INTO srf_sort2 VALUES (1, NULL), (2, 10), (3, 5)");
            ResultSet rs = s.executeQuery(
                    "SELECT id, val, generate_series(1,1) AS gs FROM srf_sort2 ORDER BY val ASC NULLS LAST");
            // Expected: 5, 10, NULL
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
        }
    }
}
