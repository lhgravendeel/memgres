package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

class UnusedMethodWiringTest {
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

    // 1. inet bitwise AND
    @Test
    void testInetBitwiseAnd() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT '192.168.1.5'::inet & '255.255.255.0'::inet")) {
            assertTrue(rs.next());
            assertEquals("192.168.1.0", rs.getString(1));
        }
    }

    // 2. inet bitwise OR
    @Test
    void testInetBitwiseOr() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT '192.168.1.0'::inet | '0.0.0.255'::inet")) {
            assertTrue(rs.next());
            assertEquals("192.168.1.255", rs.getString(1));
        }
    }

    // 3. inet bitwise NOT
    @Test
    void testInetBitwiseNot() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ~ '192.168.1.1'::inet")) {
            assertTrue(rs.next());
            assertEquals("63.87.254.254", rs.getString(1));
        }
    }

    // 4. bytea substring
    @Test
    void testByteaSubstring() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT substring('hello'::bytea, 2, 3)")) {
            assertTrue(rs.next());
            // bytea result may come back as hex-encoded or raw depending on driver
            String result = rs.getString(1);
            assertTrue(result.contains("ell") || result.contains("656c6c"),
                    "Expected 'ell' or hex equivalent but got: " + result);
        }
    }

    // 5. encode(bytea, 'hex') for actual byte[] input
    @Test
    void testEncodeByteaHex() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT encode('hello'::bytea, 'hex')")) {
            assertTrue(rs.next());
            assertEquals("68656c6c6f", rs.getString(1));
        }
    }

    // 6. @ absolute value operator
    @Test
    void testAbsoluteValueOperatorInt() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT @ -5")) {
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    void testAbsoluteValueOperatorFloat() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT @ -3.14")) {
            assertTrue(rs.next());
            assertEquals(3.14, rs.getDouble(1), 0.001);
        }
    }

    // 7. is_horizontal / is_vertical with 2 points
    @Test
    void testIsHorizontalTwoPoints() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT is_horizontal(point '(0,0)', point '(5,0)')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void testIsVerticalTwoPoints() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT is_vertical(point '(0,0)', point '(0,5)')")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    // 8. ts_token_type uses real method
    @Test
    void testTsTokenType() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ts_token_type('default')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertNotNull(result);
            // Should contain multiple token types, not just one hardcoded
            assertTrue(result.contains("asciiword"), "Should contain asciiword");
            assertTrue(result.contains("word"), "Should contain word");
            assertTrue(result.contains("email"), "Should contain email");
        }
    }

    // 9. ts_parse
    @Test
    void testTsParse() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ts_parse('default', 'hello world')")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertNotNull(result);
            assertTrue(result.contains("hello"), "Should contain 'hello'");
            assertTrue(result.contains("world"), "Should contain 'world'");
        }
    }

    // 10. unnest(tsvector)
    @Test
    void testUnnestTsVector() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT unnest('hello:1 world:2'::tsvector)")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertNotNull(result);
            assertTrue(result.contains("hello") || result.contains("world"),
                    "Should contain lexemes from the tsvector");
        }
    }

    // 11. Domain CHECK constraint enforcement
    @Test
    void testDomainCheckConstraintSuccess() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)");
            s.execute("CREATE TABLE domain_check_t (x positive_int)");
            s.execute("INSERT INTO domain_check_t VALUES (5)");
            try (ResultSet rs = s.executeQuery("SELECT x FROM domain_check_t")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
            }
        }
    }

    @Test
    void testDomainCheckConstraintFailure() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE DOMAIN positive_int2 AS integer CHECK (VALUE > 0)");
            s.execute("CREATE TABLE domain_check_t2 (x positive_int2)");
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO domain_check_t2 VALUES (-1)"));
        }
    }

    // 12. Domain DEFAULT value
    @Test
    void testDomainDefaultValue() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE DOMAIN my_text AS text DEFAULT 'hello'");
            s.execute("CREATE TABLE domain_def_t (id serial, x my_text)");
            s.execute("INSERT INTO domain_def_t (id) VALUES (1)");
            try (ResultSet rs = s.executeQuery("SELECT x FROM domain_def_t WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("hello", rs.getString(1));
            }
        }
    }

    // 13. Enum ordinal ordering
    @Test
    void testEnumOrdinalOrdering() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
            s.execute("CREATE TABLE mood_t (m mood)");
            s.execute("INSERT INTO mood_t VALUES ('happy'), ('sad'), ('ok')");
            try (ResultSet rs = s.executeQuery("SELECT m FROM mood_t ORDER BY m")) {
                assertTrue(rs.next());
                assertEquals("sad", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("ok", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("happy", rs.getString(1));
            }
        }
    }
}
