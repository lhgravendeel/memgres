package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the PG JDBC driver's adaptive binary transfer feature.
 *
 * After executing the same PreparedStatement ~5 times, the PG JDBC driver
 * starts requesting binary format for result columns it knows how to decode.
 * This means our PgWire handler must correctly encode values in PG binary
 * format, not just send text bytes with a binary format code.
 *
 * The symptom of getting this wrong:
 *   java.lang.IllegalArgumentException: number of bytes should be at-least 8
 *   at org.postgresql.util.ByteConverter.numeric(ByteConverter.java:130)
 *
 * This happens because the driver expects PG binary numeric (base-10000 digits
 * with ndigits/weight/sign/dscale header) but receives raw ASCII text.
 */
class AdaptiveBinaryTransferTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // =========================================================================
    // Core scenario: NUMERIC column read via PreparedStatement reuse
    // After ~5 executions, JDBC switches to binary transfer for numeric columns.
    // =========================================================================

    @Test
    void testNumericColumnAfterManyExecutions() throws SQLException {
        exec("CREATE TABLE abt_numeric (id serial PRIMARY KEY, price numeric(10,2) NOT NULL)");
        for (int i = 1; i <= 10; i++) {
            exec("INSERT INTO abt_numeric (price) VALUES (" + (i * 9.99) + ")");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT price FROM abt_numeric WHERE id = ?")) {
            for (int i = 1; i <= 10; i++) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Row " + i + " must exist");
                    double price = rs.getDouble("price");
                    assertTrue(price > 0, "Price must be positive on iteration " + i);
                }
            }
        }
    }

    @Test
    void testNumericColumnWithGetBigDecimal() throws SQLException {
        exec("CREATE TABLE abt_decimal (id serial PRIMARY KEY, amount numeric(12,4) NOT NULL)");
        for (int i = 1; i <= 10; i++) {
            exec("INSERT INTO abt_decimal (amount) VALUES (" + (i * 123.4567) + ")");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT amount FROM abt_decimal WHERE id = ?")) {
            for (int i = 1; i <= 10; i++) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Row " + i + " must exist");
                    BigDecimal amount = rs.getBigDecimal("amount");
                    assertNotNull(amount, "Amount must not be null on iteration " + i);
                    assertTrue(amount.compareTo(BigDecimal.ZERO) > 0,
                            "Amount must be positive on iteration " + i);
                }
            }
        }
    }

    // =========================================================================
    // Double precision column: similar binary transfer risk
    // =========================================================================

    @Test
    void testDoublePrecisionAfterManyExecutions() throws SQLException {
        exec("CREATE TABLE abt_double (id serial PRIMARY KEY, score double precision NOT NULL)");
        for (int i = 1; i <= 10; i++) {
            exec("INSERT INTO abt_double (score) VALUES (" + (i * 3.14159) + ")");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT score FROM abt_double WHERE id = ?")) {
            for (int i = 1; i <= 10; i++) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Row " + i + " must exist");
                    double score = rs.getDouble("score");
                    assertTrue(score > 0, "Score must be positive on iteration " + i);
                }
            }
        }
    }

    // =========================================================================
    // Mixed types in one query (the real-world pattern)
    // =========================================================================

    @Test
    void testMixedTypesAfterManyExecutions() throws SQLException {
        exec("""
            CREATE TABLE abt_mixed (
                id serial PRIMARY KEY,
                name text NOT NULL,
                price numeric(10,2),
                weight double precision,
                active boolean DEFAULT true,
                created_at timestamp DEFAULT now()
            )
        """);
        for (int i = 1; i <= 10; i++) {
            exec("INSERT INTO abt_mixed (name, price, weight) VALUES ('item_" + i + "', "
                    + (i * 10.5) + ", " + (i * 0.75) + ")");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, name, price, weight, active, created_at FROM abt_mixed WHERE id = ?")) {
            for (int i = 1; i <= 10; i++) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Row " + i + " must exist");
                    assertEquals(i, rs.getInt("id"));
                    assertEquals("item_" + i, rs.getString("name"));
                    assertNotNull(rs.getBigDecimal("price"),
                            "price must not be null on iteration " + i);
                    assertTrue(rs.getDouble("weight") > 0,
                            "weight must be positive on iteration " + i);
                    assertTrue(rs.getBoolean("active"),
                            "active must be true on iteration " + i);
                    assertNotNull(rs.getTimestamp("created_at"),
                            "created_at must not be null on iteration " + i);
                }
            }
        }
    }

    // =========================================================================
    // TRUNCATE + re-insert + re-query pattern (matches real test cleanup)
    // =========================================================================

    @Test
    void testTruncateAndReinsertCycle() throws SQLException {
        exec("""
            CREATE TABLE abt_cycle (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                name text NOT NULL,
                balance numeric(12,2) DEFAULT 0,
                rating double precision DEFAULT 0
            )
        """);

        try (PreparedStatement insert = conn.prepareStatement(
                     "INSERT INTO abt_cycle (name, balance, rating) VALUES (?, ?, ?) RETURNING id");
             PreparedStatement select = conn.prepareStatement(
                     "SELECT name, balance, rating FROM abt_cycle WHERE id = ?")) {

            for (int cycle = 0; cycle < 3; cycle++) {
                // Insert 5 rows
                UUID[] ids = new UUID[5];
                for (int i = 0; i < 5; i++) {
                    insert.setString(1, "user_" + cycle + "_" + i);
                    insert.setBigDecimal(2, new BigDecimal("100.50").add(new BigDecimal(i)));
                    insert.setDouble(3, 4.5 + i * 0.1);
                    try (ResultSet rs = insert.executeQuery()) {
                        assertTrue(rs.next());
                        ids[i] = (UUID) rs.getObject(1);
                    }
                }

                // Read them back (this is where binary transfer kicks in after cycle 1)
                for (int i = 0; i < 5; i++) {
                    select.setObject(1, ids[i]);
                    try (ResultSet rs = select.executeQuery()) {
                        assertTrue(rs.next(),
                                "Must find row on cycle " + cycle + " iteration " + i);
                        assertEquals("user_" + cycle + "_" + i, rs.getString("name"));
                        BigDecimal balance = rs.getBigDecimal("balance");
                        assertNotNull(balance,
                                "balance must not be null on cycle " + cycle + " iteration " + i);
                        double rating = rs.getDouble("rating");
                        assertTrue(rating > 0,
                                "rating must be positive on cycle " + cycle + " iteration " + i);
                    }
                }

                // Truncate and repeat (simulates test cleanup between test methods)
                exec("TRUNCATE abt_cycle CASCADE");
            }
        }
    }

    // =========================================================================
    // Smallint, integer, bigint binary transfer
    // =========================================================================

    @Test
    void testIntegerTypesAfterManyExecutions() throws SQLException {
        exec("""
            CREATE TABLE abt_ints (
                id serial PRIMARY KEY,
                small_val smallint,
                int_val integer,
                big_val bigint
            )
        """);
        for (int i = 1; i <= 10; i++) {
            exec("INSERT INTO abt_ints (small_val, int_val, big_val) VALUES ("
                    + (i * 10) + ", " + (i * 1000) + ", " + (i * 100000L) + ")");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT small_val, int_val, big_val FROM abt_ints WHERE id = ?")) {
            for (int i = 1; i <= 10; i++) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Row " + i + " must exist");
                    assertEquals(i * 10, rs.getShort("small_val"));
                    assertEquals(i * 1000, rs.getInt("int_val"));
                    assertEquals(i * 100000L, rs.getLong("big_val"));
                }
            }
        }
    }

    // =========================================================================
    // Real-world pattern: find-by-key with many columns
    // =========================================================================

    @Test
    void testDaoStyleFindByKey() throws SQLException {
        exec("""
            CREATE TABLE abt_plans (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                name text NOT NULL,
                max_checks integer NOT NULL DEFAULT 5,
                check_interval integer NOT NULL DEFAULT 300,
                price_monthly numeric(10,2),
                price_yearly numeric(10,2),
                trial_days integer DEFAULT 14,
                is_public boolean DEFAULT true,
                sort_order double precision DEFAULT 0,
                created_at timestamp DEFAULT now()
            )
        """);

        // Insert a plan
        UUID planId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO abt_plans (name, max_checks, check_interval, price_monthly, price_yearly, sort_order) "
                        + "VALUES (?, ?, ?, ?, ?, ?) RETURNING id")) {
            ps.setString(1, "Pro Plan");
            ps.setInt(2, 100);
            ps.setInt(3, 60);
            ps.setBigDecimal(4, new BigDecimal("29.99"));
            ps.setBigDecimal(5, new BigDecimal("299.90"));
            ps.setDouble(6, 2.0);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                planId = (UUID) rs.getObject(1);
            }
        }

        // Simulate repeated find-by-id (reuses PreparedStatement internally)
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id, name, max_checks, check_interval, price_monthly, price_yearly, "
                        + "trial_days, is_public, sort_order, created_at "
                        + "FROM abt_plans WHERE id = ?")) {
            for (int i = 0; i < 10; i++) {
                ps.setObject(1, planId);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Must find plan on iteration " + i);
                    assertEquals("Pro Plan", rs.getString("name"));
                    assertEquals(100, rs.getInt("max_checks"));
                    assertEquals(60, rs.getInt("check_interval"));

                    BigDecimal monthly = rs.getBigDecimal("price_monthly");
                    assertNotNull(monthly, "price_monthly null on iteration " + i);
                    assertEquals(0, new BigDecimal("29.99").compareTo(monthly),
                            "price_monthly must be 29.99 on iteration " + i + ", got " + monthly);

                    BigDecimal yearly = rs.getBigDecimal("price_yearly");
                    assertNotNull(yearly, "price_yearly null on iteration " + i);

                    assertEquals(14, rs.getInt("trial_days"));
                    assertTrue(rs.getBoolean("is_public"));

                    double sortOrder = rs.getDouble("sort_order");
                    assertEquals(2.0, sortOrder, 0.001,
                            "sort_order must be 2.0 on iteration " + i);

                    assertNotNull(rs.getTimestamp("created_at"),
                            "created_at null on iteration " + i);
                }
            }
        }
    }

    // =========================================================================
    // Stress test: 20 iterations to be well past the adaptive threshold
    // =========================================================================

    @Test
    void testTwentyIterationsWithNumeric() throws SQLException {
        exec("CREATE TABLE abt_stress (id serial PRIMARY KEY, val numeric(15,5) NOT NULL)");
        for (int i = 1; i <= 20; i++) {
            exec("INSERT INTO abt_stress (val) VALUES (" + (i * 1.23456) + ")");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT val FROM abt_stress WHERE id = ?")) {
            for (int i = 1; i <= 20; i++) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Row " + i + " must exist");
                    BigDecimal val = rs.getBigDecimal("val");
                    assertNotNull(val, "val must not be null on iteration " + i);
                    assertTrue(val.compareTo(BigDecimal.ZERO) > 0,
                            "val must be positive on iteration " + i + ", got " + val);
                }
            }
        }
    }

    // =========================================================================
    // Null values in numeric columns with binary transfer
    // =========================================================================

    @Test
    void testNullNumericWithBinaryTransfer() throws SQLException {
        exec("CREATE TABLE abt_nulls (id serial PRIMARY KEY, price numeric(10,2), score double precision)");
        exec("INSERT INTO abt_nulls (price, score) VALUES (NULL, NULL)");
        exec("INSERT INTO abt_nulls (price, score) VALUES (19.99, 4.5)");

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT price, score FROM abt_nulls WHERE id = ?")) {
            for (int round = 0; round < 8; round++) {
                // Query the null row
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    rs.getBigDecimal("price");
                    assertTrue(rs.wasNull(), "price should be null on round " + round);
                    rs.getDouble("score");
                    assertTrue(rs.wasNull(), "score should be null on round " + round);
                }

                // Query the non-null row
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    BigDecimal price = rs.getBigDecimal("price");
                    assertNotNull(price, "price must not be null on round " + round);
                    assertEquals(0, new BigDecimal("19.99").compareTo(price));
                    assertEquals(4.5, rs.getDouble("score"), 0.001);
                }
            }
        }
    }
}
