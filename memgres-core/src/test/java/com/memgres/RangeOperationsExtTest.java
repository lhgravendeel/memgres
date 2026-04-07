package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for extended range operations: union, intersection, adjacent,
 * daterange/tsrange constructors, lower_inc/upper_inc, range_merge.
 */
class RangeOperationsExtTest {

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

    static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // Range operators
    @Test void range_union() throws SQLException {
        assertEquals("[1,8)", q("SELECT int4range(1,5) + int4range(5,8)"));
    }

    @Test void range_intersection() throws SQLException {
        assertEquals("[3,5)", q("SELECT int4range(1,5) * int4range(3,8)"));
    }

    // Range functions
    @Test void lower_inc() throws SQLException {
        assertEquals("t", q("SELECT lower_inc(int4range(1,5))"));
    }

    @Test void upper_inc() throws SQLException {
        assertEquals("f", q("SELECT upper_inc(int4range(1,5))"));
    }

    @Test void range_merge() throws SQLException {
        assertEquals("[1,8)", q("SELECT range_merge(int4range(1,5), int4range(5,8))"));
    }

    // Null bounds
    @Test void range_null_lower_bound() throws SQLException {
        String result = q("SELECT int4range(NULL, 5)");
        assertNotNull(result);
        assertTrue(result.startsWith("("), "NULL lower should be unbounded: " + result);
    }

    // Date range constructor
    @Test void daterange_constructor() throws SQLException {
        String result = q("SELECT daterange(DATE '2024-01-01', DATE '2024-02-01')");
        assertNotNull(result, "daterange should work");
        assertTrue(result.contains("2024-01-01"), "Should contain start date: " + result);
    }

    // tsrange constructor
    @Test void tsrange_constructor() throws SQLException {
        String result = q("SELECT tsrange(TIMESTAMP '2024-01-01 10:00', TIMESTAMP '2024-01-01 11:00')");
        assertNotNull(result, "tsrange should work");
    }
}
