package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that targetless ON CONFLICT DO NOTHING checks ALL unique constraints,
 * not just the PK. PG: INSERT ... ON CONFLICT DO NOTHING suppresses any
 * unique violation without requiring a specific target.
 */
class OnConflictDoNothingAllUniqueTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void suppresses_unique_constraint_violation() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE ocdn_t (id serial PRIMARY KEY, email text UNIQUE, name text)");
            s.execute("INSERT INTO ocdn_t (email, name) VALUES ('a@b.com', 'Alice')");
            // This should NOT raise 23505 — the unique on email should be silently suppressed
            s.execute("INSERT INTO ocdn_t (email, name) VALUES ('a@b.com', 'Bob') ON CONFLICT DO NOTHING");
            ResultSet rs = s.executeQuery("SELECT count(*) AS c FROM ocdn_t");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("c"), "Conflicting row should have been suppressed");
        }
    }

    @Test void suppresses_pk_violation() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE ocdn_pk (id int PRIMARY KEY, val text)");
            s.execute("INSERT INTO ocdn_pk VALUES (1, 'first')");
            s.execute("INSERT INTO ocdn_pk VALUES (1, 'second') ON CONFLICT DO NOTHING");
            ResultSet rs = s.executeQuery("SELECT val FROM ocdn_pk WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("first", rs.getString("val"));
        }
    }

    @Test void suppresses_multi_column_unique() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE ocdn_mc (a int, b int, c text, UNIQUE(a, b))");
            s.execute("INSERT INTO ocdn_mc VALUES (1, 2, 'first')");
            s.execute("INSERT INTO ocdn_mc VALUES (1, 2, 'second') ON CONFLICT DO NOTHING");
            ResultSet rs = s.executeQuery("SELECT count(*) AS c FROM ocdn_mc");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("c"));
        }
    }

    @Test void inserts_when_no_conflict() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE ocdn_ok (id int PRIMARY KEY, email text UNIQUE)");
            s.execute("INSERT INTO ocdn_ok VALUES (1, 'a@b.com')");
            s.execute("INSERT INTO ocdn_ok VALUES (2, 'c@d.com') ON CONFLICT DO NOTHING");
            ResultSet rs = s.executeQuery("SELECT count(*) AS c FROM ocdn_ok");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("c"));
        }
    }
}
