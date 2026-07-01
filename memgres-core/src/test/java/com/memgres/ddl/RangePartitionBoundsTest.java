package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for partition bound parsing with various literal formats:
 * typed literals (DATE '...'), cast syntax ('...'::date), signed numbers, etc.
 */
class RangePartitionBoundsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- Plain string literals (baseline — already works) ----

    @Test
    void range_partition_plain_string_bounds() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE rp_plain (d date) PARTITION BY RANGE (d)");
            s.execute("CREATE TABLE rp_plain_2026 PARTITION OF rp_plain FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')");
            s.execute("INSERT INTO rp_plain VALUES ('2026-06-15')");
            ResultSet rs = s.executeQuery("SELECT d FROM rp_plain");
            assertTrue(rs.next());
            assertEquals("2026-06-15", rs.getString(1));
            s.execute("DROP TABLE rp_plain CASCADE");
        }
    }

    // ---- Typed literals (DATE '...', TIMESTAMP '...') ----

    @Test
    void range_partition_typed_date_bounds() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE rp_typed (d date) PARTITION BY RANGE (d)");
            s.execute("CREATE TABLE rp_typed_apr PARTITION OF rp_typed FOR VALUES FROM (DATE '2026-04-01') TO (DATE '2026-05-01')");
            s.execute("INSERT INTO rp_typed VALUES ('2026-04-15')");
            ResultSet rs = s.executeQuery("SELECT d FROM rp_typed");
            assertTrue(rs.next());
            assertEquals("2026-04-15", rs.getString(1));
            s.execute("DROP TABLE rp_typed CASCADE");
        }
    }

    @Test
    void range_partition_typed_timestamp_bounds() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE rp_ts (ts timestamp) PARTITION BY RANGE (ts)");
            s.execute("CREATE TABLE rp_ts_2026 PARTITION OF rp_ts FOR VALUES FROM (TIMESTAMP '2026-01-01 00:00:00') TO (TIMESTAMP '2027-01-01 00:00:00')");
            s.execute("INSERT INTO rp_ts VALUES ('2026-06-15 12:00:00')");
            ResultSet rs = s.executeQuery("SELECT ts FROM rp_ts");
            assertTrue(rs.next());
            assertNotNull(rs.getTimestamp(1));
            s.execute("DROP TABLE rp_ts CASCADE");
        }
    }

    // ---- Cast syntax ('...'::type) ----

    @Test
    void range_partition_cast_bounds() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE rp_cast (d date) PARTITION BY RANGE (d)");
            s.execute("CREATE TABLE rp_cast_apr PARTITION OF rp_cast FOR VALUES FROM ('2026-04-01'::date) TO ('2026-05-01'::date)");
            s.execute("INSERT INTO rp_cast VALUES ('2026-04-15')");
            ResultSet rs = s.executeQuery("SELECT d FROM rp_cast");
            assertTrue(rs.next());
            assertEquals("2026-04-15", rs.getString(1));
            s.execute("DROP TABLE rp_cast CASCADE");
        }
    }

    // ---- Signed numeric bounds ----

    @Test
    void range_partition_signed_numeric_bounds() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE rp_neg (n int) PARTITION BY RANGE (n)");
            s.execute("CREATE TABLE rp_neg_low PARTITION OF rp_neg FOR VALUES FROM (-100) TO (0)");
            s.execute("CREATE TABLE rp_neg_high PARTITION OF rp_neg FOR VALUES FROM (0) TO (100)");
            s.execute("INSERT INTO rp_neg VALUES (-50)");
            s.execute("INSERT INTO rp_neg VALUES (50)");
            ResultSet rs = s.executeQuery("SELECT n FROM rp_neg ORDER BY n");
            assertTrue(rs.next());
            assertEquals(-50, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(50, rs.getInt(1));
            s.execute("DROP TABLE rp_neg CASCADE");
        }
    }

    // ---- MINVALUE / MAXVALUE ----

    @Test
    void range_partition_minvalue_maxvalue() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE rp_mm (n int) PARTITION BY RANGE (n)");
            s.execute("CREATE TABLE rp_mm_low PARTITION OF rp_mm FOR VALUES FROM (MINVALUE) TO (0)");
            s.execute("CREATE TABLE rp_mm_high PARTITION OF rp_mm FOR VALUES FROM (0) TO (MAXVALUE)");
            s.execute("INSERT INTO rp_mm VALUES (-999)");
            s.execute("INSERT INTO rp_mm VALUES (999)");
            ResultSet rs = s.executeQuery("SELECT n FROM rp_mm ORDER BY n");
            assertTrue(rs.next());
            assertEquals(-999, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(999, rs.getInt(1));
            s.execute("DROP TABLE rp_mm CASCADE");
        }
    }

    // ---- Multi-column range partitions ----

    @Test
    void range_partition_multi_column_typed() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE rp_mc (d date, n int) PARTITION BY RANGE (d, n)");
            s.execute("CREATE TABLE rp_mc_p1 PARTITION OF rp_mc FOR VALUES FROM ('2026-01-01', 0) TO ('2026-12-31', 100)");
            s.execute("INSERT INTO rp_mc VALUES ('2026-06-15', 50)");
            ResultSet rs = s.executeQuery("SELECT d, n FROM rp_mc");
            assertTrue(rs.next());
            assertEquals(50, rs.getInt("n"));
            s.execute("DROP TABLE rp_mc CASCADE");
        }
    }

    // ---- LIST partition with typed literals ----

    @Test
    void list_partition_typed_date_values() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lp_typed (d date) PARTITION BY LIST (d)");
            s.execute("CREATE TABLE lp_typed_a PARTITION OF lp_typed FOR VALUES IN (DATE '2026-04-01', DATE '2026-04-02')");
            s.execute("INSERT INTO lp_typed VALUES ('2026-04-01')");
            ResultSet rs = s.executeQuery("SELECT d FROM lp_typed");
            assertTrue(rs.next());
            assertEquals("2026-04-01", rs.getString(1));
            s.execute("DROP TABLE lp_typed CASCADE");
        }
    }

    // ---- DEFAULT partition ----

    @Test
    void range_partition_with_default() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE rp_def (n int) PARTITION BY RANGE (n)");
            s.execute("CREATE TABLE rp_def_p1 PARTITION OF rp_def FOR VALUES FROM (0) TO (100)");
            s.execute("CREATE TABLE rp_def_default PARTITION OF rp_def DEFAULT");
            s.execute("INSERT INTO rp_def VALUES (50)");
            s.execute("INSERT INTO rp_def VALUES (200)");
            ResultSet rs = s.executeQuery("SELECT n FROM rp_def ORDER BY n");
            assertTrue(rs.next());
            assertEquals(50, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(200, rs.getInt(1));
            s.execute("DROP TABLE rp_def CASCADE");
        }
    }

    // ---- Positive sign ----

    @Test
    void range_partition_positive_sign() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE rp_pos (n int) PARTITION BY RANGE (n)");
            s.execute("CREATE TABLE rp_pos_p1 PARTITION OF rp_pos FOR VALUES FROM (+0) TO (+100)");
            s.execute("INSERT INTO rp_pos VALUES (50)");
            ResultSet rs = s.executeQuery("SELECT n FROM rp_pos");
            assertTrue(rs.next());
            assertEquals(50, rs.getInt(1));
            s.execute("DROP TABLE rp_pos CASCADE");
        }
    }

    // ---- Hash partitioning ----

    @Test
    void hash_partition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE hp (id int) PARTITION BY HASH (id)");
            s.execute("CREATE TABLE hp_0 PARTITION OF hp FOR VALUES WITH (MODULUS 4, REMAINDER 0)");
            s.execute("CREATE TABLE hp_1 PARTITION OF hp FOR VALUES WITH (MODULUS 4, REMAINDER 1)");
            s.execute("CREATE TABLE hp_2 PARTITION OF hp FOR VALUES WITH (MODULUS 4, REMAINDER 2)");
            s.execute("CREATE TABLE hp_3 PARTITION OF hp FOR VALUES WITH (MODULUS 4, REMAINDER 3)");
            s.execute("INSERT INTO hp VALUES (1)");
            s.execute("INSERT INTO hp VALUES (2)");
            s.execute("INSERT INTO hp VALUES (3)");
            s.execute("INSERT INTO hp VALUES (4)");
            ResultSet rs = s.executeQuery("SELECT id FROM hp ORDER BY id");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(4, rs.getInt(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE hp CASCADE");
        }
    }

    // ---- LIST partition with plain text strings ----

    @Test
    void list_partition_plain_text_values() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lp_text (status text) PARTITION BY LIST (status)");
            s.execute("CREATE TABLE lp_text_active PARTITION OF lp_text FOR VALUES IN ('active', 'pending')");
            s.execute("CREATE TABLE lp_text_done PARTITION OF lp_text FOR VALUES IN ('done', 'archived')");
            s.execute("INSERT INTO lp_text VALUES ('active')");
            s.execute("INSERT INTO lp_text VALUES ('pending')");
            s.execute("INSERT INTO lp_text VALUES ('done')");
            s.execute("INSERT INTO lp_text VALUES ('archived')");
            ResultSet rs = s.executeQuery("SELECT status FROM lp_text ORDER BY status");
            assertTrue(rs.next()); assertEquals("active", rs.getString(1));
            assertTrue(rs.next()); assertEquals("archived", rs.getString(1));
            assertTrue(rs.next()); assertEquals("done", rs.getString(1));
            assertTrue(rs.next()); assertEquals("pending", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE lp_text CASCADE");
        }
    }
}
