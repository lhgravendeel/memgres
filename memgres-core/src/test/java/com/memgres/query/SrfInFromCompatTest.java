package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class SrfInFromCompatTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    @DisplayName("regexp_split_to_table should split string by regex and return rows")
    void testRegexpSplitToTableExists() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT word FROM regexp_split_to_table('hello world foo', E'\\\\s+') AS word")) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertEquals(3, count);
        }
    }

    @Test
    @DisplayName("jsonb_array_elements cast to integer should return integer values")
    void testJsonbArrayElementsCastToInteger() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT elem::integer FROM jsonb_array_elements('[1, 2, 3]'::jsonb) AS elem ORDER BY elem::integer")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        }
    }
}
