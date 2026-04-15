package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class GroupingSetsCompatTest {
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
            stmt.execute("CREATE TABLE gs_sales (region text, product text, amount integer)");
            stmt.execute("INSERT INTO gs_sales VALUES "
                    + "('east', 'widget', 100), ('east', 'gadget', 200), "
                    + "('west', 'widget', 150), ('west', 'gadget', 250)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    @DisplayName("Mixed GROUP BY and GROUPING SETS should produce correct row count")
    void testMixedGroupByAndGroupingSetsCount() throws Exception {
        // PG returns 6 rows, Memgres returns 7
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT count(*)::integer AS cnt FROM ("
                             + "SELECT region, product, sum(amount) "
                             + "FROM gs_sales "
                             + "GROUP BY region, GROUPING SETS ((product), ())"
                             + ") sub")) {
            assertTrue(rs.next());
            assertEquals(6, rs.getInt("cnt"));
        }
    }
}
