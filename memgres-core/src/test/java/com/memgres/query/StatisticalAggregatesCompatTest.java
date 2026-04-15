package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class StatisticalAggregatesCompatTest {
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
    @DisplayName("covar_pop with single row should return 0.00 not NULL")
    void testCovarPopSingleRow() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT round(covar_pop(y, x)::numeric, 2) AS cov_pop " +
                     "FROM (VALUES (1::double precision, 2::double precision)) AS t(x, y)")) {
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal("cov_pop");
            assertNotNull(result, "covar_pop should return 0.00 for a single row, not NULL");
            assertEquals(new BigDecimal("0.00"), result);
        }
    }
}
