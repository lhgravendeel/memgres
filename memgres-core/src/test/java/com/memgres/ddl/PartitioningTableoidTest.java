package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for partitioned tables with tableoid pseudo-column and partition
 * bound validation error codes.
 *
 * Key PG behaviors:
 * - tableoid::regclass shows which partition a row comes from
 * - Overlapping range partitions fail with 42P17 (invalid_object_definition)
 * - Duplicate list partition values fail with 42P17
 * - Hash partition with conflicting modulus/remainder fail with 42P16
 */
class PartitioningTableoidTest {

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

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static int countRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = 0; while (rs.next()) n++; return n;
        }
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    // ========================================================================
    // tableoid pseudo-column on partitioned tables
    // ========================================================================

    @Test
    void tableoid_on_range_partitioned_table() throws SQLException {
        exec("CREATE TABLE pr(id int PRIMARY KEY, val text) PARTITION BY RANGE (id)");
        exec("CREATE TABLE pr_1 PARTITION OF pr FOR VALUES FROM (1) TO (100)");
        exec("CREATE TABLE pr_2 PARTITION OF pr FOR VALUES FROM (100) TO (200)");
        exec("INSERT INTO pr VALUES (1, 'a'), (50, 'b'), (150, 'c')");
        try {
            List<List<String>> rows = query("SELECT tableoid::regclass, id, val FROM pr ORDER BY id");
            assertEquals(3, rows.size());
            // Row id=1 and id=50 should be in pr_1, id=150 in pr_2
            assertTrue(rows.get(0).get(0).contains("pr_1"),
                    "id=1 should be in pr_1: " + rows.get(0).get(0));
            assertTrue(rows.get(2).get(0).contains("pr_2"),
                    "id=150 should be in pr_2: " + rows.get(2).get(0));
        } finally {
            exec("DROP TABLE IF EXISTS pr CASCADE");
        }
    }

    @Test
    void tableoid_on_list_partitioned_table() throws SQLException {
        exec("CREATE TABLE pl(code text PRIMARY KEY, val int) PARTITION BY LIST (code)");
        exec("CREATE TABLE pl_a PARTITION OF pl FOR VALUES IN ('A', 'B')");
        exec("CREATE TABLE pl_c PARTITION OF pl FOR VALUES IN ('C')");
        exec("INSERT INTO pl VALUES ('A', 1), ('B', 2), ('C', 3)");
        try {
            List<List<String>> rows = query("SELECT tableoid::regclass, code, val FROM pl ORDER BY code");
            assertEquals(3, rows.size());
            assertTrue(rows.get(0).get(0).contains("pl_a"));
            assertTrue(rows.get(2).get(0).contains("pl_c"));
        } finally {
            exec("DROP TABLE IF EXISTS pl CASCADE");
        }
    }

    @Test
    void tableoid_on_hash_partitioned_table() throws SQLException {
        exec("CREATE TABLE ph(id int PRIMARY KEY, val text) PARTITION BY HASH (id)");
        exec("CREATE TABLE ph_0 PARTITION OF ph FOR VALUES WITH (modulus 2, remainder 0)");
        exec("CREATE TABLE ph_1 PARTITION OF ph FOR VALUES WITH (modulus 2, remainder 1)");
        exec("INSERT INTO ph VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        try {
            List<List<String>> rows = query("SELECT tableoid::regclass, id, val FROM ph ORDER BY id");
            assertEquals(3, rows.size());
            // Each row should report either ph_0 or ph_1
            for (List<String> row : rows) {
                assertTrue(row.get(0).contains("ph_0") || row.get(0).contains("ph_1"),
                        "tableoid should resolve to a partition: " + row.get(0));
            }
        } finally {
            exec("DROP TABLE IF EXISTS ph CASCADE");
        }
    }

    // ========================================================================
    // Partition bound overlap / conflict errors
    // ========================================================================

    @Test
    void overlapping_range_partition_fails_with_42P17() throws SQLException {
        exec("CREATE TABLE pr2(id int PRIMARY KEY) PARTITION BY RANGE (id)");
        exec("CREATE TABLE pr2_1 PARTITION OF pr2 FOR VALUES FROM (1) TO (100)");
        try {
            // Overlapping range: 50-150 overlaps with 1-100
            try {
                exec("CREATE TABLE pr2_bad PARTITION OF pr2 FOR VALUES FROM (50) TO (150)");
                fail("Overlapping range partition should fail");
            } catch (SQLException e) {
                assertEquals("42P17", e.getSQLState(),
                        "Overlapping partition should be 42P17, got " + e.getSQLState());
            }
        } finally {
            exec("DROP TABLE IF EXISTS pr2 CASCADE");
        }
    }

    @Test
    void duplicate_list_partition_value_fails_with_42P17() throws SQLException {
        exec("CREATE TABLE pl2(code text) PARTITION BY LIST (code)");
        exec("CREATE TABLE pl2_a PARTITION OF pl2 FOR VALUES IN ('A')");
        try {
            try {
                exec("CREATE TABLE pl2_dup PARTITION OF pl2 FOR VALUES IN ('A')");
                fail("Duplicate list value should fail");
            } catch (SQLException e) {
                assertEquals("42P17", e.getSQLState(),
                        "Duplicate list value should be 42P17, got " + e.getSQLState());
            }
        } finally {
            exec("DROP TABLE IF EXISTS pl2 CASCADE");
        }
    }

    @Test
    void hash_partition_conflicting_remainder_fails() throws SQLException {
        exec("CREATE TABLE ph2(id int) PARTITION BY HASH (id)");
        exec("CREATE TABLE ph2_0 PARTITION OF ph2 FOR VALUES WITH (modulus 2, remainder 0)");
        try {
            try {
                // Same modulus and remainder (duplicate)
                exec("CREATE TABLE ph2_dup PARTITION OF ph2 FOR VALUES WITH (modulus 2, remainder 0)");
                fail("Duplicate hash partition should fail");
            } catch (SQLException e) {
                // PG uses 42P16 for this
                assertEquals("42P16", e.getSQLState(),
                        "Conflicting hash partition should be 42P16, got " + e.getSQLState());
            }
        } finally {
            exec("DROP TABLE IF EXISTS ph2 CASCADE");
        }
    }

    // ========================================================================
    // tableoid on regular (non-partitioned) tables
    // ========================================================================

    @Test
    void tableoid_available_on_regular_table() throws SQLException {
        exec("CREATE TABLE reg_t(id int PRIMARY KEY)");
        exec("INSERT INTO reg_t VALUES (1)");
        try {
            String oid = scalar("SELECT tableoid::regclass FROM reg_t WHERE id = 1");
            assertNotNull(oid, "tableoid should be available on regular tables");
            assertTrue(oid.contains("reg_t"), "tableoid::regclass should resolve to table name");
        } finally {
            exec("DROP TABLE IF EXISTS reg_t");
        }
    }
}
