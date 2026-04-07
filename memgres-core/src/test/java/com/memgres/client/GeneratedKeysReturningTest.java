package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document section 3 (Java/JDBC): Generated keys and RETURNING through JDBC.
 * Tests RETURN_GENERATED_KEYS, explicit RETURNING, multi-row returning,
 * identity columns, ON CONFLICT RETURNING, zero-row inserts.
 */
class GeneratedKeysReturningTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    // --- RETURN_GENERATED_KEYS with serial ---

    @Test void serial_generated_keys() throws Exception {
        exec("CREATE TABLE gk_ser(id serial PRIMARY KEY, v text)");
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO gk_ser(v) VALUES ('a')", Statement.RETURN_GENERATED_KEYS);
            try (ResultSet rs = s.getGeneratedKeys()) {
                assertTrue(rs.next(), "Should return generated key");
                int id = rs.getInt(1);
                assertTrue(id > 0, "Generated ID should be positive");
            }
        }
        exec("DROP TABLE gk_ser");
    }

    @Test void identity_generated_keys() throws Exception {
        exec("CREATE TABLE gk_ident(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v text)");
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO gk_ident(v) VALUES ('hello')", Statement.RETURN_GENERATED_KEYS);
            try (ResultSet rs = s.getGeneratedKeys()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
        exec("DROP TABLE gk_ident");
    }

    // --- Explicit RETURNING ---

    @Test void insert_returning_all_columns() throws Exception {
        exec("CREATE TABLE gk_ret(id serial PRIMARY KEY, v text, created timestamp DEFAULT now())");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "INSERT INTO gk_ret(v) VALUES ('test') RETURNING *")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("test", rs.getString("v"));
            assertNotNull(rs.getTimestamp("created"));
        }
        exec("DROP TABLE gk_ret");
    }

    @Test void insert_returning_specific_columns() throws Exception {
        exec("CREATE TABLE gk_ret2(id serial PRIMARY KEY, a int, b text)");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "INSERT INTO gk_ret2(a, b) VALUES (10, 'x') RETURNING id, b")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("x", rs.getString("b"));
        }
        exec("DROP TABLE gk_ret2");
    }

    @Test void insert_returning_expression() throws Exception {
        exec("CREATE TABLE gk_ret3(id serial PRIMARY KEY, a int)");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "INSERT INTO gk_ret3(a) VALUES (5) RETURNING id, a * 2 AS doubled")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("doubled"));
        }
        exec("DROP TABLE gk_ret3");
    }

    // --- Multi-row RETURNING ---

    @Test void multi_row_insert_returning() throws Exception {
        exec("CREATE TABLE gk_multi(id serial PRIMARY KEY, v text)");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "INSERT INTO gk_multi(v) VALUES ('a'),('b'),('c') RETURNING id, v")) {
            int count = 0;
            while (rs.next()) {
                count++;
                assertTrue(rs.getInt("id") > 0);
                assertNotNull(rs.getString("v"));
            }
            assertEquals(3, count, "Should return 3 rows");
        }
        exec("DROP TABLE gk_multi");
    }

    // --- UPDATE RETURNING ---

    @Test void update_returning() throws Exception {
        exec("CREATE TABLE gk_upd(id int PRIMARY KEY, v int)");
        exec("INSERT INTO gk_upd VALUES (1, 10), (2, 20)");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "UPDATE gk_upd SET v = v + 1 WHERE id = 1 RETURNING id, v")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(11, rs.getInt("v"));
            assertFalse(rs.next());
        }
        exec("DROP TABLE gk_upd");
    }

    // --- DELETE RETURNING ---

    @Test void delete_returning() throws Exception {
        exec("CREATE TABLE gk_del(id int PRIMARY KEY, v text)");
        exec("INSERT INTO gk_del VALUES (1,'x'),(2,'y')");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "DELETE FROM gk_del WHERE id = 1 RETURNING *")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("x", rs.getString("v"));
        }
        exec("DROP TABLE gk_del");
    }

    // --- ON CONFLICT RETURNING ---

    @Test void upsert_returning_inserted() throws Exception {
        exec("CREATE TABLE gk_upsert(id int PRIMARY KEY, v text)");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "INSERT INTO gk_upsert VALUES (1, 'new') ON CONFLICT (id) DO UPDATE SET v = 'updated' RETURNING id, v")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("new", rs.getString("v")); // first time, inserted
        }
        exec("DROP TABLE gk_upsert");
    }

    @Test void upsert_returning_updated() throws Exception {
        exec("CREATE TABLE gk_ups2(id int PRIMARY KEY, v text)");
        exec("INSERT INTO gk_ups2 VALUES (1, 'old')");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "INSERT INTO gk_ups2 VALUES (1, 'new') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v RETURNING id, v")) {
            assertTrue(rs.next());
            assertEquals("new", rs.getString("v")); // conflict, updated
        }
        exec("DROP TABLE gk_ups2");
    }

    @Test void upsert_do_nothing_returning_zero_rows() throws Exception {
        exec("CREATE TABLE gk_ups3(id int PRIMARY KEY, v text)");
        exec("INSERT INTO gk_ups3 VALUES (1, 'existing')");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "INSERT INTO gk_ups3 VALUES (1, 'dup') ON CONFLICT DO NOTHING RETURNING *")) {
            assertFalse(rs.next(), "DO NOTHING with conflict should return 0 rows");
        }
        exec("DROP TABLE gk_ups3");
    }

    // --- Zero-row insert ---

    @Test void insert_select_returning_zero_rows() throws Exception {
        exec("CREATE TABLE gk_zero_src(id int)");
        exec("CREATE TABLE gk_zero_dst(id int)");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "INSERT INTO gk_zero_dst SELECT * FROM gk_zero_src RETURNING *")) {
            assertFalse(rs.next(), "Empty source should produce zero returning rows");
        }
        exec("DROP TABLE gk_zero_src");
        exec("DROP TABLE gk_zero_dst");
    }

    // --- Identity by default vs always ---

    @Test void identity_by_default_allows_explicit() throws Exception {
        exec("CREATE TABLE gk_idbd(id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v text)");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "INSERT INTO gk_idbd(id, v) VALUES (100, 'explicit') RETURNING id")) {
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
        }
        exec("DROP TABLE gk_idbd");
    }

    @Test void identity_always_rejects_explicit() throws Exception {
        exec("CREATE TABLE gk_ida(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v text)");
        assertThrows(SQLException.class, () -> {
            conn.createStatement().executeQuery("INSERT INTO gk_ida(id, v) VALUES (100, 'bad') RETURNING id");
        });
        exec("DROP TABLE gk_ida");
    }
}
