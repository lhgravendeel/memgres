package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class TypeSystemCompatTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("SET timezone = 'UTC'");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    @DisplayName("timetz should include timezone offset in output")
    void testTimetzIncludesTimezoneOffset() throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE tz_test (id integer PRIMARY KEY, t timetz)");
            stmt.execute("INSERT INTO tz_test VALUES (1, '09:00:00+00')");
        }
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT t FROM tz_test WHERE id = 1")) {
            assertTrue(rs.next());
            String result = rs.getString("t");
            assertTrue(result.contains("+00"),
                    "timetz should include timezone offset, got: " + result);
        }
    }

    @Test
    @DisplayName("timetz arithmetic should support interval addition")
    void testTimetzArithmetic() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT ('12:30:00+00'::timetz + interval '2 hours') AS later")) {
            assertTrue(rs.next());
            String result = rs.getString("later");
            assertNotNull(result);
            assertTrue(result.startsWith("14:30:00"),
                    "Expected 14:30:00+00, got: " + result);
        }
    }

    @Test
    @DisplayName("regproc cast should display function name not OID")
    void testRegprocCastDisplaysFunctionName() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT 'now'::regproc::text AS f")) {
            assertTrue(rs.next());
            assertEquals("now", rs.getString("f"));
        }
    }

    @Test
    @DisplayName("jsonpath type should exist and accept path expressions")
    void testJsonpathTypeExists() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT '$.store.book[*].author'::jsonpath AS jp")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("jp"));
        }
    }

    @Test
    @DisplayName("macaddr8 function should convert macaddr to macaddr8")
    void testMacaddr8Function() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT macaddr8('08:00:2b:01:02:03'::macaddr) AS m8")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("m8"));
        }
    }
}
