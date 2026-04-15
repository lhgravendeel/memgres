package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PG normalizes jsonpath output by quoting all object keys.
 */
class JsonpathNormalizationTest {
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
    @DisplayName("jsonpath should quote object keys")
    void testJsonpathQuotesKeys() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT '$.store.book[*].author'::jsonpath AS jp")) {
            assertTrue(rs.next());
            assertEquals("$.\"store\".\"book\"[*].\"author\"", rs.getString("jp"),
                    "jsonpath output should quote object keys like PG");
        }
    }
}
