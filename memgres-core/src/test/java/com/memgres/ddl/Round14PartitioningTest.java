package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: Partitioning edge cases.
 *
 * - PARTITION BY expression (not just bare column)
 * - Multi-column HASH partitioning
 * - ATTACH CONCURRENTLY
 * - Partitioned unique indexes
 * - Row migration when UPDATE changes partition key
 * - FK between partitioned tables
 * - DEFAULT partition
 */
class Round14PartitioningTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // A. PARTITION BY expression
    // =========================================================================

    @Test
    void partition_by_expression() throws SQLException {
        // PG allows PARTITION BY RANGE ((lower(s)))
        exec("CREATE TABLE r14_part_expr (id int, s text) PARTITION BY RANGE ((lower(s)))");
        exec("CREATE TABLE r14_part_expr_a PARTITION OF r14_part_expr FOR VALUES FROM ('a') TO ('m')");
        exec("CREATE TABLE r14_part_expr_b PARTITION OF r14_part_expr FOR VALUES FROM ('m') TO ('z')");
        exec("INSERT INTO r14_part_expr VALUES (1, 'ABCDEF'), (2, 'xyz')");
        assertEquals(1, scalarInt("SELECT count(*)::int FROM r14_part_expr_a"));
        assertEquals(1, scalarInt("SELECT count(*)::int FROM r14_part_expr_b"));
    }

    // =========================================================================
    // B. Multi-column HASH partitioning
    // =========================================================================

    @Test
    void multi_column_hash_partitioning() throws SQLException {
        exec("CREATE TABLE r14_mh (a int, b int) PARTITION BY HASH (a, b)");
        exec("CREATE TABLE r14_mh_0 PARTITION OF r14_mh FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
        exec("CREATE TABLE r14_mh_1 PARTITION OF r14_mh FOR VALUES WITH (MODULUS 2, REMAINDER 1)");
        exec("INSERT INTO r14_mh SELECT i, i*2 FROM generate_series(1,10) i");
        assertEquals(10, scalarInt("SELECT count(*)::int FROM r14_mh"));
    }

    // =========================================================================
    // C. DEFAULT partition
    // =========================================================================

    @Test
    void default_partition_catches_outliers() throws SQLException {
        exec("CREATE TABLE r14_dp (id int) PARTITION BY RANGE (id)");
        exec("CREATE TABLE r14_dp_lo PARTITION OF r14_dp FOR VALUES FROM (1) TO (10)");
        exec("CREATE TABLE r14_dp_def PARTITION OF r14_dp DEFAULT");
        exec("INSERT INTO r14_dp VALUES (5), (50), (100)");
        assertEquals(1, scalarInt("SELECT count(*)::int FROM r14_dp_lo"));
        assertEquals(2, scalarInt("SELECT count(*)::int FROM r14_dp_def"));
    }

    // =========================================================================
    // D. ATTACH / DETACH partitions
    // =========================================================================

    @Test
    void attach_detach_partition() throws SQLException {
        exec("CREATE TABLE r14_att (id int) PARTITION BY RANGE (id)");
        exec("CREATE TABLE r14_att_1 (id int)");
        exec("INSERT INTO r14_att_1 VALUES (5)");
        exec("ALTER TABLE r14_att ATTACH PARTITION r14_att_1 FOR VALUES FROM (1) TO (10)");
        assertEquals(1, scalarInt("SELECT count(*)::int FROM r14_att"));

        // Detach then verify standalone
        exec("ALTER TABLE r14_att DETACH PARTITION r14_att_1");
        assertEquals(0, scalarInt("SELECT count(*)::int FROM r14_att"));
        assertEquals(1, scalarInt("SELECT count(*)::int FROM r14_att_1"));
    }

    @Test
    void attach_concurrently_parses() throws SQLException {
        // ATTACH PARTITION does not take CONCURRENTLY; DETACH does (PG 14+).
        exec("CREATE TABLE r14_ac (id int) PARTITION BY RANGE (id)");
        exec("CREATE TABLE r14_ac_1 PARTITION OF r14_ac FOR VALUES FROM (1) TO (10)");
        try {
            // DETACH PARTITION ... CONCURRENTLY
            exec("ALTER TABLE r14_ac DETACH PARTITION r14_ac_1 CONCURRENTLY");
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"), "DETACH CONCURRENTLY must parse, got: " + msg);
        }
    }

    @Test
    void detach_finalize_parses() throws SQLException {
        exec("CREATE TABLE r14_df (id int) PARTITION BY RANGE (id)");
        exec("CREATE TABLE r14_df_1 PARTITION OF r14_df FOR VALUES FROM (1) TO (10)");
        try {
            // DETACH ... FINALIZE completes a concurrent detach
            exec("ALTER TABLE r14_df DETACH PARTITION r14_df_1 FINALIZE");
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"), "DETACH FINALIZE must parse, got: " + msg);
        }
    }

    // =========================================================================
    // E. Row migration on partition key UPDATE
    // =========================================================================

    @Test
    void update_partition_key_moves_row() throws SQLException {
        exec("CREATE TABLE r14_move (id int, region text) PARTITION BY LIST (region)");
        exec("CREATE TABLE r14_move_us PARTITION OF r14_move FOR VALUES IN ('US')");
        exec("CREATE TABLE r14_move_eu PARTITION OF r14_move FOR VALUES IN ('EU')");
        exec("INSERT INTO r14_move VALUES (1, 'US')");
        exec("UPDATE r14_move SET region = 'EU' WHERE id = 1");
        // Row should have moved from r14_move_us to r14_move_eu
        assertEquals(0, scalarInt("SELECT count(*)::int FROM r14_move_us"));
        assertEquals(1, scalarInt("SELECT count(*)::int FROM r14_move_eu"));
    }

    // =========================================================================
    // F. Partitioned table UNIQUE index
    // =========================================================================

    @Test
    void partitioned_unique_index_over_all_partitions() throws SQLException {
        exec("CREATE TABLE r14_pu (id int, region text) PARTITION BY LIST (region)");
        exec("CREATE TABLE r14_pu_us PARTITION OF r14_pu FOR VALUES IN ('US')");
        exec("CREATE TABLE r14_pu_eu PARTITION OF r14_pu FOR VALUES IN ('EU')");
        // UNIQUE index must include partition key in PG
        exec("CREATE UNIQUE INDEX r14_pu_idx ON r14_pu (id, region)");
        exec("INSERT INTO r14_pu VALUES (1, 'US')");
        exec("INSERT INTO r14_pu VALUES (1, 'EU')"); // Different region → ok
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO r14_pu VALUES (1, 'US')"));
        assertNotNull(ex.getMessage());
    }

    @Test
    void partitioned_unique_without_partition_key_rejected() throws SQLException {
        exec("CREATE TABLE r14_pu2 (id int, region text) PARTITION BY LIST (region)");
        // UNIQUE(id) without region should be rejected
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("CREATE UNIQUE INDEX r14_pu2_idx ON r14_pu2 (id)"));
        assertNotNull(ex.getMessage());
    }

    // =========================================================================
    // G. FK from non-partitioned to partitioned
    // =========================================================================

    @Test
    void fk_referencing_partitioned_table() throws SQLException {
        exec("CREATE TABLE r14_fk_p (id int PRIMARY KEY, region text) "
                + "PARTITION BY LIST (region)");
        exec("CREATE TABLE r14_fk_p_us PARTITION OF r14_fk_p FOR VALUES IN ('US')");
        exec("INSERT INTO r14_fk_p VALUES (1, 'US')");
        exec("CREATE TABLE r14_fk_c (pid int REFERENCES r14_fk_p(id))");
        // Valid FK insert
        exec("INSERT INTO r14_fk_c VALUES (1)");
        // Invalid FK insert should error
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO r14_fk_c VALUES (99)"));
        assertNotNull(ex.getMessage());
    }

    // =========================================================================
    // H. Partition info catalogs
    // =========================================================================

    @Test
    void pg_partition_tree_lists_partitions() throws SQLException {
        exec("CREATE TABLE r14_pt_root (id int) PARTITION BY RANGE (id)");
        exec("CREATE TABLE r14_pt_a PARTITION OF r14_pt_root FOR VALUES FROM (1) TO (10)");
        exec("CREATE TABLE r14_pt_b PARTITION OF r14_pt_root FOR VALUES FROM (10) TO (20)");
        // pg_partition_tree(root) returns root + both partitions = 3 rows
        assertEquals(3, scalarInt(
                "SELECT count(*)::int FROM pg_partition_tree('r14_pt_root'::regclass)"));
    }

    @Test
    void pg_partition_ancestors() throws SQLException {
        exec("CREATE TABLE r14_anc_root (id int) PARTITION BY RANGE (id)");
        exec("CREATE TABLE r14_anc_a PARTITION OF r14_anc_root FOR VALUES FROM (1) TO (10)");
        // Ancestors of a leaf: self + root = 2
        assertEquals(2, scalarInt(
                "SELECT count(*)::int FROM pg_partition_ancestors('r14_anc_a'::regclass)"));
    }

    @Test
    void pg_partition_root() throws SQLException {
        exec("CREATE TABLE r14_root (id int) PARTITION BY RANGE (id)");
        exec("CREATE TABLE r14_root_a PARTITION OF r14_root FOR VALUES FROM (1) TO (10)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_partition_root('r14_root_a'::regclass)::text")) {
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertTrue(v.contains("r14_root"), "root must be r14_root, got: " + v);
        }
    }
}
