package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that ON CONFLICT DO UPDATE ... WHERE false does NOT count/return the row.
 * PG: when WHERE evaluates to false, the row is silently skipped — no insert counted,
 * no row in RETURNING.
 */
class OnConflictWherefalseTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void where_false_no_returning() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE ocwf_t (id int PRIMARY KEY, val text)");
            s.execute("INSERT INTO ocwf_t VALUES (1, 'orig')");
            ResultSet rs = s.executeQuery(
                    "INSERT INTO ocwf_t VALUES (1, 'new') " +
                    "ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE false " +
                    "RETURNING *");
            assertFalse(rs.next(), "WHERE false should produce no RETURNING rows");
        }
    }

    @Test void where_false_does_not_modify() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE ocwf_t2 (id int PRIMARY KEY, val text)");
            s.execute("INSERT INTO ocwf_t2 VALUES (1, 'orig')");
            s.execute("INSERT INTO ocwf_t2 VALUES (1, 'new') " +
                    "ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE false");
            ResultSet rs = s.executeQuery("SELECT val FROM ocwf_t2 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("orig", rs.getString("val"), "Row should be unchanged");
        }
    }

    @Test void where_true_does_update_and_return() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE ocwf_t3 (id int PRIMARY KEY, val text)");
            s.execute("INSERT INTO ocwf_t3 VALUES (1, 'orig')");
            ResultSet rs = s.executeQuery(
                    "INSERT INTO ocwf_t3 VALUES (1, 'new') " +
                    "ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE true " +
                    "RETURNING val");
            assertTrue(rs.next());
            assertEquals("new", rs.getString("val"));
        }
    }
}
