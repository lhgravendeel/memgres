package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Compatibility tests for PG 16 date/time functions.
 *
 * These tests document known differences between PostgreSQL and Memgres
 * for fractional-second precision in make_time and make_timestamp.
 * They are INTENDED TO FAIL against Memgres until the differences are resolved.
 *
 * Source: datetime-functions-extended.sql (2 diffs)
 */
class DateTimeFunctionsCompatTest {

    private Memgres memgres;

    @BeforeEach
    void setUp() {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterEach
    void tearDown() {
        if (memgres != null) {
            memgres.close();
        }
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(),
                memgres.getPassword()
        );
    }

    /**
     * PG returns "14:30:45.123" but Memgres returns "14:30:45.122999".
     * Fractional seconds lose precision due to floating-point representation.
     */
    @Test
    void makeTimeFractionalSecondsPrecision() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT make_time(14, 30, 45.123) AS t")) {
            assertTrue(rs.next());
            String result = rs.getString("t");
            assertEquals("14:30:45.123", result,
                    "make_time fractional seconds should match PG output; "
                            + "Memgres returns " + result);
        }
    }

    /**
     * PG returns "2024-07-04 12:00:30.5" but Memgres returns "2024-07-04 12:00:30.500000".
     * Trailing zeros are not stripped from the fractional seconds.
     */
    @Test
    void makeTimestampFractionalSecondsPrecision() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT make_timestamp(2024, 7, 4, 12, 0, 30.5) AS ts")) {
            assertTrue(rs.next());
            String result = rs.getString("ts");
            assertEquals("2024-07-04 12:00:30.5", result,
                    "make_timestamp fractional seconds should match PG output; "
                            + "Memgres returns " + result);
        }
    }
}
