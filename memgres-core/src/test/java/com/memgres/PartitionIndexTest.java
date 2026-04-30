package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CREATE INDEX auto-propagation to partitions and
 * ALTER INDEX ATTACH PARTITION validation.
 *
 * PG behavior:
 *  - CREATE INDEX on a partitioned table auto-creates matching child indexes on each partition
 *  - Auto-created child indexes are automatically attached (visible in pg_inherits)
 *  - ALTER INDEX ATTACH PARTITION rejects if parent already has a child for that partition
 */
class PartitionIndexTest {

    static Memgres memgres;
    static Connection conn;
    static Connection extConn; // extended protocol (default, matches FeatureComparisonReport)

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        extConn = DriverManager.getConnection(
                memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        extConn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (extConn != null) extConn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private String scalarStr(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A. CREATE INDEX on partitioned table auto-propagates to partitions
    // =========================================================================

    @Test
    void create_index_on_partitioned_table_auto_creates_child_index() throws SQLException {
        exec("CREATE TABLE pi_a1 (id int, region int) PARTITION BY LIST (region)");
        exec("CREATE TABLE pi_a1_p1 PARTITION OF pi_a1 FOR VALUES IN (1)");
        exec("CREATE TABLE pi_a1_p2 PARTITION OF pi_a1 FOR VALUES IN (2)");
        exec("CREATE INDEX pi_a1_parent_idx ON pi_a1 (id)");

        // Child indexes should exist in pg_indexes for each partition
        int p1Count = scalarInt(
                "SELECT count(*)::int FROM pg_indexes WHERE tablename = 'pi_a1_p1'");
        assertTrue(p1Count >= 1,
                "CREATE INDEX on partitioned table should auto-create index on partition pi_a1_p1; got " + p1Count);

        int p2Count = scalarInt(
                "SELECT count(*)::int FROM pg_indexes WHERE tablename = 'pi_a1_p2'");
        assertTrue(p2Count >= 1,
                "CREATE INDEX on partitioned table should auto-create index on partition pi_a1_p2; got " + p2Count);
    }

    @Test
    void auto_created_child_index_is_attached_in_pg_inherits() throws SQLException {
        exec("CREATE TABLE pi_a2 (id int, region int) PARTITION BY LIST (region)");
        exec("CREATE TABLE pi_a2_p1 PARTITION OF pi_a2 FOR VALUES IN (1)");
        exec("CREATE INDEX pi_a2_parent_idx ON pi_a2 (id)");

        // The auto-created child index should appear in pg_inherits as a child of the parent index
        int inheritCount = scalarInt(
                "SELECT count(*)::int FROM pg_index i "
                + "JOIN pg_inherits h ON h.inhrelid = i.indexrelid "
                + "JOIN pg_class pc ON h.inhparent = pc.oid "
                + "WHERE pc.relname = 'pi_a2_parent_idx'");
        assertEquals(1, inheritCount,
                "Auto-created child index should be wired in pg_inherits under parent index");
    }

    @Test
    void auto_created_child_indexes_work_with_multiple_partitions() throws SQLException {
        exec("CREATE TABLE pi_a3 (id int, region int) PARTITION BY LIST (region)");
        exec("CREATE TABLE pi_a3_p1 PARTITION OF pi_a3 FOR VALUES IN (1)");
        exec("CREATE TABLE pi_a3_p2 PARTITION OF pi_a3 FOR VALUES IN (2)");
        exec("CREATE TABLE pi_a3_p3 PARTITION OF pi_a3 FOR VALUES IN (3)");
        exec("CREATE INDEX pi_a3_idx ON pi_a3 (id)");

        int inheritCount = scalarInt(
                "SELECT count(*)::int FROM pg_index i "
                + "JOIN pg_inherits h ON h.inhrelid = i.indexrelid "
                + "JOIN pg_class pc ON h.inhparent = pc.oid "
                + "WHERE pc.relname = 'pi_a3_idx'");
        assertEquals(3, inheritCount,
                "All 3 partition child indexes should be in pg_inherits");
    }

    @Test
    void auto_created_child_index_is_usable_for_queries() throws SQLException {
        exec("CREATE TABLE pi_a4 (id int, region int) PARTITION BY LIST (region)");
        exec("CREATE TABLE pi_a4_p1 PARTITION OF pi_a4 FOR VALUES IN (1)");
        exec("CREATE INDEX pi_a4_idx ON pi_a4 (id)");
        exec("INSERT INTO pi_a4 VALUES (10, 1), (20, 1), (30, 1)");

        int count = scalarInt("SELECT count(*)::int FROM pi_a4 WHERE id = 20");
        assertEquals(1, count);
    }

    @Test
    void exact_report_sequence_auto_child_visible_after_failed_attach() throws SQLException {
        // Exact sequence from round15-index-variants.sql
        exec("CREATE TABLE pi_rpt (id int, region int) PARTITION BY LIST (region)");
        exec("CREATE TABLE pi_rpt_1 PARTITION OF pi_rpt FOR VALUES IN (1)");
        exec("CREATE INDEX pi_rpt_parent_idx ON pi_rpt (id)");
        exec("CREATE INDEX pi_rpt_1_idx ON pi_rpt_1 (id)");
        try {
            exec("ALTER INDEX pi_rpt_parent_idx ATTACH PARTITION pi_rpt_1_idx");
        } catch (SQLException ignored) {
            // Expected to fail with 55000
        }

        // The auto-created child should still be visible in pg_inherits
        int count = scalarInt(
                "SELECT count(*)::int AS c FROM pg_index i "
                + "JOIN pg_inherits h ON h.inhrelid = i.indexrelid "
                + "JOIN pg_class pc ON h.inhparent = pc.oid "
                + "WHERE pc.relname = 'pi_rpt_parent_idx'");
        assertEquals(1, count,
                "Auto-created child index should be in pg_inherits even after failed manual ATTACH");
    }

    @Test
    void extended_protocol_auto_child_visible_in_pg_inherits() throws SQLException {
        // Same sequence but using extended query protocol (matches FeatureComparisonReport)
        try (Statement s = extConn.createStatement()) {
            s.execute("CREATE TABLE pi_ext (id int, region int) PARTITION BY LIST (region)");
            s.execute("CREATE TABLE pi_ext_1 PARTITION OF pi_ext FOR VALUES IN (1)");
            s.execute("CREATE INDEX pi_ext_parent_idx ON pi_ext (id)");
            s.execute("CREATE INDEX pi_ext_1_idx ON pi_ext_1 (id)");
            try {
                s.execute("ALTER INDEX pi_ext_parent_idx ATTACH PARTITION pi_ext_1_idx");
            } catch (SQLException ignored) {}
        }

        try (Statement s = extConn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT count(*)::int AS c FROM pg_index i "
                + "JOIN pg_inherits h ON h.inhrelid = i.indexrelid "
                + "JOIN pg_class pc ON h.inhparent = pc.oid "
                + "WHERE pc.relname = 'pi_ext_parent_idx'")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1),
                    "Extended protocol: auto-created child index should be in pg_inherits");
        }
    }

    // =========================================================================
    // B. ALTER INDEX ATTACH PARTITION — validation
    // =========================================================================

    @Test
    void alter_index_attach_rejects_when_parent_already_has_child_for_partition() throws SQLException {
        // This is the exact scenario from round15-index-variants.sql:
        // CREATE INDEX on partitioned table auto-creates child, then manual ATTACH fails
        exec("CREATE TABLE pi_b1 (id int, region int) PARTITION BY LIST (region)");
        exec("CREATE TABLE pi_b1_p1 PARTITION OF pi_b1 FOR VALUES IN (1)");
        exec("CREATE INDEX pi_b1_parent_idx ON pi_b1 (id)");
        // Parent already has an auto-created child for pi_b1_p1
        exec("CREATE INDEX pi_b1_manual_idx ON pi_b1_p1 (id)");

        try {
            exec("ALTER INDEX pi_b1_parent_idx ATTACH PARTITION pi_b1_manual_idx");
            fail("ALTER INDEX ATTACH should reject when parent already has a child for the partition");
        } catch (SQLException e) {
            assertEquals("55000", e.getSQLState(),
                    "Expected SQLSTATE 55000; got " + e.getSQLState() + " — " + e.getMessage());
            assertTrue(e.getMessage().contains("cannot attach"),
                    "Error message should contain 'cannot attach'; got: " + e.getMessage());
        }
    }

    @Test
    void alter_index_attach_succeeds_when_no_auto_created_child() throws SQLException {
        // If partition is added AFTER the index, no auto-created child exists,
        // so manual create + attach should work
        exec("CREATE TABLE pi_b2 (id int, region int) PARTITION BY LIST (region)");
        exec("CREATE INDEX pi_b2_parent_idx ON pi_b2 (id)");
        // Add partition AFTER index — PG would auto-create here too, but test the attach path
        exec("CREATE TABLE pi_b2_p1 (id int, region int)");
        // Manually attach as partition (not via PARTITION OF, so no auto-index)
        exec("ALTER TABLE pi_b2 ATTACH PARTITION pi_b2_p1 FOR VALUES IN (1)");
        exec("CREATE INDEX pi_b2_child_idx ON pi_b2_p1 (id)");

        // This should succeed — parent has no pre-existing child for pi_b2_p1
        exec("ALTER INDEX pi_b2_parent_idx ATTACH PARTITION pi_b2_child_idx");

        int inheritCount = scalarInt(
                "SELECT count(*)::int FROM pg_index i "
                + "JOIN pg_inherits h ON h.inhrelid = i.indexrelid "
                + "JOIN pg_class pc ON h.inhparent = pc.oid "
                + "WHERE pc.relname = 'pi_b2_parent_idx'");
        assertEquals(1, inheritCount,
                "Manually attached child should appear in pg_inherits");
    }

    // =========================================================================
    // C. UNIQUE index propagation still works (regression check)
    // =========================================================================

    @Test
    void create_unique_index_on_partitioned_table_propagates_constraint() throws SQLException {
        exec("CREATE TABLE pi_c1 (id int, region int) PARTITION BY LIST (region)");
        exec("CREATE TABLE pi_c1_p1 PARTITION OF pi_c1 FOR VALUES IN (1)");
        exec("CREATE UNIQUE INDEX pi_c1_uidx ON pi_c1 (id, region)");

        // Unique constraint should be enforced on partition via auto-propagated index
        exec("INSERT INTO pi_c1 VALUES (1, 1)");
        try {
            exec("INSERT INTO pi_c1 VALUES (1, 1)");
            fail("Unique constraint should be enforced on partition");
        } catch (SQLException e) {
            assertEquals("23505", e.getSQLState());
        }
    }

    @Test
    void create_partitioned_table_with_invalid_pk_does_not_leave_table_behind() throws SQLException {
        // PG atomically rolls back CREATE TABLE when PK validation fails.
        // The table should not exist after the error.
        String tableName = "pi_partial_rollback";
        try {
            exec("CREATE TABLE " + tableName + " (id int PRIMARY KEY, region text) PARTITION BY LIST (region)");
            fail("Should reject PK that doesn't include partition column");
        } catch (SQLException e) {
            assertEquals("0A000", e.getSQLState());
        }
        // Table must not exist — matches PG atomic rollback behavior
        try {
            exec("SELECT 1 FROM " + tableName + " LIMIT 1");
            fail("Table should not exist after failed CREATE TABLE");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("does not exist"),
                    "Expected 'does not exist' error, got: " + e.getMessage());
        }
    }
}
