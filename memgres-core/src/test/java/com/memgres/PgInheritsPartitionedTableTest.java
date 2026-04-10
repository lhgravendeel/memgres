package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * B2: Comprehensive tests for pg_inherits, pg_partitioned_table,
 * pg_class partition columns, and pg_get_partkeydef().
 */
class PgInheritsPartitionedTableTest {

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

    // ===== pg_inherits: Partition relationships =====

    @Test
    void pgInheritsRangePartition() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pi_range (id int, val text) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE pi_range_p1 PARTITION OF pi_range FOR VALUES FROM (1) TO (100)");
            st.execute("CREATE TABLE pi_range_p2 PARTITION OF pi_range FOR VALUES FROM (100) TO (200)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT c.relname AS child_name " +
                    "FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid " +
                    "JOIN pg_class p ON p.oid = i.inhparent " +
                    "WHERE p.relname = 'pi_range' " +
                    "ORDER BY child_name")) {
                assertTrue(rs.next());
                assertEquals("pi_range_p1", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("pi_range_p2", rs.getString(1));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE pi_range_p2");
            st.execute("DROP TABLE pi_range_p1");
            st.execute("DROP TABLE pi_range");
        }
    }

    @Test
    void pgInheritsListPartition() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pi_list (id int, status text) PARTITION BY LIST (status)");
            st.execute("CREATE TABLE pi_list_active PARTITION OF pi_list FOR VALUES IN ('active')");
            st.execute("CREATE TABLE pi_list_inactive PARTITION OF pi_list FOR VALUES IN ('inactive')");
            try (ResultSet rs = st.executeQuery(
                    "SELECT c.relname " +
                    "FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid " +
                    "JOIN pg_class p ON p.oid = i.inhparent " +
                    "WHERE p.relname = 'pi_list' ORDER BY c.relname")) {
                assertTrue(rs.next());
                assertEquals("pi_list_active", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("pi_list_inactive", rs.getString(1));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE pi_list_active");
            st.execute("DROP TABLE pi_list_inactive");
            st.execute("DROP TABLE pi_list");
        }
    }

    @Test
    void pgInheritsHashPartition() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pi_hash (id int, val text) PARTITION BY HASH (id)");
            st.execute("CREATE TABLE pi_hash_p0 PARTITION OF pi_hash FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
            st.execute("CREATE TABLE pi_hash_p1 PARTITION OF pi_hash FOR VALUES WITH (MODULUS 2, REMAINDER 1)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT c.relname " +
                    "FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid " +
                    "JOIN pg_class p ON p.oid = i.inhparent " +
                    "WHERE p.relname = 'pi_hash' ORDER BY c.relname")) {
                assertTrue(rs.next());
                assertEquals("pi_hash_p0", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("pi_hash_p1", rs.getString(1));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE pi_hash_p0");
            st.execute("DROP TABLE pi_hash_p1");
            st.execute("DROP TABLE pi_hash");
        }
    }

    @Test
    void pgInheritsDefaultPartition() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pi_def (id int) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE pi_def_p1 PARTITION OF pi_def FOR VALUES FROM (1) TO (100)");
            st.execute("CREATE TABLE pi_def_default PARTITION OF pi_def DEFAULT");
            try (ResultSet rs = st.executeQuery(
                    "SELECT c.relname " +
                    "FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid " +
                    "JOIN pg_class p ON p.oid = i.inhparent " +
                    "WHERE p.relname = 'pi_def' ORDER BY c.relname")) {
                assertTrue(rs.next());
                assertEquals("pi_def_default", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("pi_def_p1", rs.getString(1));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE pi_def_default");
            st.execute("DROP TABLE pi_def_p1");
            st.execute("DROP TABLE pi_def");
        }
    }

    @Test
    void pgInheritsInheritance() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pi_parent (id int, name text)");
            st.execute("CREATE TABLE pi_child (extra int) INHERITS (pi_parent)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT c.relname AS child, p.relname AS parent " +
                    "FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid " +
                    "JOIN pg_class p ON p.oid = i.inhparent " +
                    "WHERE p.relname = 'pi_parent'")) {
                assertTrue(rs.next());
                assertEquals("pi_child", rs.getString("child"));
                assertEquals("pi_parent", rs.getString("parent"));
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE pi_child");
            st.execute("DROP TABLE pi_parent");
        }
    }

    @Test
    void pgInheritsInhseqno() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pi_seq (id int) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE pi_seq_p1 PARTITION OF pi_seq FOR VALUES FROM (1) TO (50)");
            st.execute("CREATE TABLE pi_seq_p2 PARTITION OF pi_seq FOR VALUES FROM (50) TO (100)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT c.relname, i.inhseqno " +
                    "FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid " +
                    "JOIN pg_class p ON p.oid = i.inhparent " +
                    "WHERE p.relname = 'pi_seq' ORDER BY i.inhseqno")) {
                assertTrue(rs.next());
                assertEquals("pi_seq_p1", rs.getString(1));
                assertTrue(rs.getInt(2) >= 1);
                assertTrue(rs.next());
                assertEquals("pi_seq_p2", rs.getString(1));
                assertTrue(rs.getInt(2) >= 1);
                assertFalse(rs.next());
            }
            st.execute("DROP TABLE pi_seq_p2");
            st.execute("DROP TABLE pi_seq_p1");
            st.execute("DROP TABLE pi_seq");
        }
    }

    @Test
    void pgInheritsInhdetachpending() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pi_dp (id int) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE pi_dp_p1 PARTITION OF pi_dp FOR VALUES FROM (1) TO (100)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT i.inhdetachpending " +
                    "FROM pg_inherits i " +
                    "JOIN pg_class p ON p.oid = i.inhparent " +
                    "WHERE p.relname = 'pi_dp'")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
            }
            st.execute("DROP TABLE pi_dp_p1");
            st.execute("DROP TABLE pi_dp");
        }
    }

    @Test
    void pgInheritsEmptyForUnpartitioned() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pi_plain (id int)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*) FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhparent " +
                    "WHERE c.relname = 'pi_plain'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            st.execute("DROP TABLE pi_plain");
        }
    }

    // ===== pg_partitioned_table =====

    @Test
    void pgPartitionedTableRange() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ppt_range (id int, val text) PARTITION BY RANGE (id)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT pt.partstrat, pt.partnatts " +
                    "FROM pg_partitioned_table pt " +
                    "JOIN pg_class c ON c.oid = pt.partrelid " +
                    "WHERE c.relname = 'ppt_range'")) {
                assertTrue(rs.next());
                assertEquals("r", rs.getString(1));
                assertEquals(1, rs.getInt(2));
            }
            st.execute("DROP TABLE ppt_range");
        }
    }

    @Test
    void pgPartitionedTableList() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ppt_list (id int, status text) PARTITION BY LIST (status)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT pt.partstrat " +
                    "FROM pg_partitioned_table pt " +
                    "JOIN pg_class c ON c.oid = pt.partrelid " +
                    "WHERE c.relname = 'ppt_list'")) {
                assertTrue(rs.next());
                assertEquals("l", rs.getString(1));
            }
            st.execute("DROP TABLE ppt_list");
        }
    }

    @Test
    void pgPartitionedTableHash() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ppt_hash (id int) PARTITION BY HASH (id)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT pt.partstrat " +
                    "FROM pg_partitioned_table pt " +
                    "JOIN pg_class c ON c.oid = pt.partrelid " +
                    "WHERE c.relname = 'ppt_hash'")) {
                assertTrue(rs.next());
                assertEquals("h", rs.getString(1));
            }
            st.execute("DROP TABLE ppt_hash");
        }
    }

    @Test
    void pgPartitionedTableDefaultPartition() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ppt_def (id int) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE ppt_def_p1 PARTITION OF ppt_def FOR VALUES FROM (1) TO (100)");
            st.execute("CREATE TABLE ppt_def_default PARTITION OF ppt_def DEFAULT");
            try (ResultSet rs = st.executeQuery(
                    "SELECT pt.partdefid, dc.relname " +
                    "FROM pg_partitioned_table pt " +
                    "JOIN pg_class c ON c.oid = pt.partrelid " +
                    "LEFT JOIN pg_class dc ON dc.oid = pt.partdefid " +
                    "WHERE c.relname = 'ppt_def'")) {
                assertTrue(rs.next());
                assertNotNull(rs.getObject(1));
                assertEquals("ppt_def_default", rs.getString(2));
            }
            st.execute("DROP TABLE ppt_def_default");
            st.execute("DROP TABLE ppt_def_p1");
            st.execute("DROP TABLE ppt_def");
        }
    }

    @Test
    void pgPartitionedTableNotPresentForRegularTable() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ppt_plain (id int)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*) FROM pg_partitioned_table pt " +
                    "JOIN pg_class c ON c.oid = pt.partrelid " +
                    "WHERE c.relname = 'ppt_plain'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            st.execute("DROP TABLE ppt_plain");
        }
    }

    // ===== pg_class: relkind, relispartition, relpartbound =====

    @Test
    void relkindIsPartitionedForParent() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE rk_parent (id int) PARTITION BY RANGE (id)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT relkind FROM pg_class WHERE relname = 'rk_parent'")) {
                assertTrue(rs.next());
                assertEquals("p", rs.getString(1));
            }
            st.execute("DROP TABLE rk_parent");
        }
    }

    @Test
    void relkindIsRegularForPartitionChild() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE rk_parent2 (id int) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE rk_child2 PARTITION OF rk_parent2 FOR VALUES FROM (1) TO (100)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT relkind FROM pg_class WHERE relname = 'rk_child2'")) {
                assertTrue(rs.next());
                // Partition children are regular tables in PG (relkind='r')
                assertEquals("r", rs.getString(1));
            }
            st.execute("DROP TABLE rk_child2");
            st.execute("DROP TABLE rk_parent2");
        }
    }

    @Test
    void relispartitionTrueForChild() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE rip_parent (id int) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE rip_child PARTITION OF rip_parent FOR VALUES FROM (1) TO (100)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT relispartition FROM pg_class WHERE relname = 'rip_child'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
            st.execute("DROP TABLE rip_child");
            st.execute("DROP TABLE rip_parent");
        }
    }

    @Test
    void relispartitionFalseForParent() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE rip_parent2 (id int) PARTITION BY RANGE (id)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT relispartition FROM pg_class WHERE relname = 'rip_parent2'")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
            }
            st.execute("DROP TABLE rip_parent2");
        }
    }

    @Test
    void relispartitionFalseForRegularTable() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE rip_plain (id int)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT relispartition FROM pg_class WHERE relname = 'rip_plain'")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
            }
            st.execute("DROP TABLE rip_plain");
        }
    }

    @Test
    void relpartboundRangePartition() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE rpb_range (id int) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE rpb_range_p1 PARTITION OF rpb_range FOR VALUES FROM (1) TO (100)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT relpartbound FROM pg_class WHERE relname = 'rpb_range_p1'")) {
                assertTrue(rs.next());
                String bound = rs.getString(1);
                assertNotNull(bound);
                assertTrue(bound.contains("FROM") && bound.contains("TO"),
                        "Expected range bound, got: " + bound);
                assertTrue(bound.contains("1") && bound.contains("100"),
                        "Expected bounds with 1 and 100, got: " + bound);
            }
            st.execute("DROP TABLE rpb_range_p1");
            st.execute("DROP TABLE rpb_range");
        }
    }

    @Test
    void relpartboundListPartition() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE rpb_list (status text) PARTITION BY LIST (status)");
            st.execute("CREATE TABLE rpb_list_a PARTITION OF rpb_list FOR VALUES IN ('active', 'pending')");
            try (ResultSet rs = st.executeQuery(
                    "SELECT relpartbound FROM pg_class WHERE relname = 'rpb_list_a'")) {
                assertTrue(rs.next());
                String bound = rs.getString(1);
                assertNotNull(bound);
                assertTrue(bound.contains("IN") && bound.contains("active"),
                        "Expected list bound, got: " + bound);
            }
            st.execute("DROP TABLE rpb_list_a");
            st.execute("DROP TABLE rpb_list");
        }
    }

    @Test
    void relpartboundHashPartition() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE rpb_hash (id int) PARTITION BY HASH (id)");
            st.execute("CREATE TABLE rpb_hash_p0 PARTITION OF rpb_hash FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT relpartbound FROM pg_class WHERE relname = 'rpb_hash_p0'")) {
                assertTrue(rs.next());
                String bound = rs.getString(1);
                assertNotNull(bound);
                assertTrue(bound.toLowerCase().contains("modulus") && bound.contains("2"),
                        "Expected hash bound, got: " + bound);
            }
            st.execute("DROP TABLE rpb_hash_p0");
            st.execute("DROP TABLE rpb_hash");
        }
    }

    @Test
    void relpartboundDefaultPartition() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE rpb_def (id int) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE rpb_def_default PARTITION OF rpb_def DEFAULT");
            try (ResultSet rs = st.executeQuery(
                    "SELECT relpartbound FROM pg_class WHERE relname = 'rpb_def_default'")) {
                assertTrue(rs.next());
                String bound = rs.getString(1);
                assertNotNull(bound);
                assertEquals("DEFAULT", bound);
            }
            st.execute("DROP TABLE rpb_def_default");
            st.execute("DROP TABLE rpb_def");
        }
    }

    @Test
    void relpartboundNullForParent() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE rpb_parent (id int) PARTITION BY RANGE (id)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT relpartbound FROM pg_class WHERE relname = 'rpb_parent'")) {
                assertTrue(rs.next());
                assertNull(rs.getString(1));
            }
            st.execute("DROP TABLE rpb_parent");
        }
    }

    @Test
    void relpartboundNullForRegularTable() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE rpb_plain (id int)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT relpartbound FROM pg_class WHERE relname = 'rpb_plain'")) {
                assertTrue(rs.next());
                assertNull(rs.getString(1));
            }
            st.execute("DROP TABLE rpb_plain");
        }
    }

    // ===== pg_get_partkeydef() =====

    @Test
    void pgGetPartkeydefRange() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pkd_range (sale_date date, amount int) PARTITION BY RANGE (sale_date)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT pg_get_partkeydef(c.oid) " +
                    "FROM pg_class c WHERE c.relname = 'pkd_range'")) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                assertNotNull(result);
                assertTrue(result.toLowerCase().contains("range"), "Expected RANGE, got: " + result);
                assertTrue(result.contains("sale_date"), "Expected column name, got: " + result);
            }
            st.execute("DROP TABLE pkd_range");
        }
    }

    @Test
    void pgGetPartkeydefList() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pkd_list (id int, region text) PARTITION BY LIST (region)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT pg_get_partkeydef(c.oid) " +
                    "FROM pg_class c WHERE c.relname = 'pkd_list'")) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                assertNotNull(result);
                assertTrue(result.toLowerCase().contains("list"), "Expected LIST, got: " + result);
                assertTrue(result.contains("region"), "Expected column name, got: " + result);
            }
            st.execute("DROP TABLE pkd_list");
        }
    }

    @Test
    void pgGetPartkeydefHash() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pkd_hash (id int) PARTITION BY HASH (id)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT pg_get_partkeydef(c.oid) " +
                    "FROM pg_class c WHERE c.relname = 'pkd_hash'")) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                assertNotNull(result);
                assertTrue(result.toLowerCase().contains("hash"), "Expected HASH, got: " + result);
                assertTrue(result.contains("id"), "Expected column name, got: " + result);
            }
            st.execute("DROP TABLE pkd_hash");
        }
    }

    @Test
    void pgGetPartkeydefNullForRegularTable() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pkd_plain (id int)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT pg_get_partkeydef(c.oid) " +
                    "FROM pg_class c WHERE c.relname = 'pkd_plain'")) {
                assertTrue(rs.next());
                assertNull(rs.getString(1));
            }
            st.execute("DROP TABLE pkd_plain");
        }
    }

    // ===== pg_get_expr(relpartbound) =====

    @Test
    void pgGetExprRelpartbound() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pge_parent (id int) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE pge_child PARTITION OF pge_parent FOR VALUES FROM (1) TO (100)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT pg_get_expr(c.relpartbound, c.oid) " +
                    "FROM pg_class c WHERE c.relname = 'pge_child'")) {
                assertTrue(rs.next());
                String bound = rs.getString(1);
                assertNotNull(bound);
                assertTrue(bound.contains("FROM") && bound.contains("TO"),
                        "Expected range bound expression, got: " + bound);
            }
            st.execute("DROP TABLE pge_child");
            st.execute("DROP TABLE pge_parent");
        }
    }

    // ===== Multi-level partitioning =====

    @Test
    void pgInheritsMultiLevel() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ml_root (id int, dt date) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE ml_mid PARTITION OF ml_root FOR VALUES FROM (1) TO (100) PARTITION BY RANGE (dt)");
            st.execute("CREATE TABLE ml_leaf PARTITION OF ml_mid FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')");

            // ml_mid's parent is ml_root
            try (ResultSet rs = st.executeQuery(
                    "SELECT p.relname FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid " +
                    "JOIN pg_class p ON p.oid = i.inhparent " +
                    "WHERE c.relname = 'ml_mid'")) {
                assertTrue(rs.next());
                assertEquals("ml_root", rs.getString(1));
            }
            // ml_leaf's parent is ml_mid
            try (ResultSet rs = st.executeQuery(
                    "SELECT p.relname FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid " +
                    "JOIN pg_class p ON p.oid = i.inhparent " +
                    "WHERE c.relname = 'ml_leaf'")) {
                assertTrue(rs.next());
                assertEquals("ml_mid", rs.getString(1));
            }
            // ml_mid is both partitioned (relkind='p') and a partition (relispartition=true)
            try (ResultSet rs = st.executeQuery(
                    "SELECT relkind, relispartition FROM pg_class WHERE relname = 'ml_mid'")) {
                assertTrue(rs.next());
                assertEquals("p", rs.getString(1)); // sub-partitioned table
                assertTrue(rs.getBoolean(2)); // is a partition of ml_root
            }

            st.execute("DROP TABLE ml_leaf");
            st.execute("DROP TABLE ml_mid");
            st.execute("DROP TABLE ml_root");
        }
    }

    // ===== ATTACH/DETACH and catalog updates =====

    @Test
    void attachPartitionShowsInPgInherits() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE att_parent (id int) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE att_child (id int)"); // standalone table
            // Before attach: no pg_inherits row
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*) FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid WHERE c.relname = 'att_child'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            st.execute("ALTER TABLE att_parent ATTACH PARTITION att_child FOR VALUES FROM (1) TO (100)");
            // After attach: shows in pg_inherits
            try (ResultSet rs = st.executeQuery(
                    "SELECT p.relname FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid " +
                    "JOIN pg_class p ON p.oid = i.inhparent " +
                    "WHERE c.relname = 'att_child'")) {
                assertTrue(rs.next());
                assertEquals("att_parent", rs.getString(1));
            }
            st.execute("DROP TABLE att_child");
            st.execute("DROP TABLE att_parent");
        }
    }

    @Test
    void detachPartitionRemovesFromPgInherits() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE det_parent (id int) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE det_child PARTITION OF det_parent FOR VALUES FROM (1) TO (100)");
            // Before detach: present in pg_inherits
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*) FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid WHERE c.relname = 'det_child'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            st.execute("ALTER TABLE det_parent DETACH PARTITION det_child");
            // After detach: gone from pg_inherits
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*) FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid WHERE c.relname = 'det_child'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            st.execute("DROP TABLE det_child");
            st.execute("DROP TABLE det_parent");
        }
    }

    // ===== ALTER TABLE INHERIT / NO INHERIT =====

    @Test
    void alterTableInheritShowsInPgInherits() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE inh_parent (id int, name text)");
            st.execute("CREATE TABLE inh_child (id int, name text)");
            st.execute("ALTER TABLE inh_child INHERIT inh_parent");
            try (ResultSet rs = st.executeQuery(
                    "SELECT p.relname FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid " +
                    "JOIN pg_class p ON p.oid = i.inhparent " +
                    "WHERE c.relname = 'inh_child'")) {
                assertTrue(rs.next());
                assertEquals("inh_parent", rs.getString(1));
            }
            st.execute("ALTER TABLE inh_child NO INHERIT inh_parent");
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*) FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid WHERE c.relname = 'inh_child'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            st.execute("DROP TABLE inh_child");
            st.execute("DROP TABLE inh_parent");
        }
    }

    // ===== Schema-qualified queries (typical pg_dump pattern) =====

    @Test
    void pgDumpStylePartitionQuery() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE dump_sales (id int, sale_date date, amount numeric) PARTITION BY RANGE (sale_date)");
            st.execute("CREATE TABLE dump_sales_2024 PARTITION OF dump_sales FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')");
            st.execute("CREATE TABLE dump_sales_2025 PARTITION OF dump_sales FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')");
            // pg_dump-style query: list partitions with their bounds
            try (ResultSet rs = st.executeQuery(
                    "SELECT c.relname, " +
                    "  c.relispartition, " +
                    "  pg_get_expr(c.relpartbound, c.oid) AS partition_expr, " +
                    "  p.relname AS parent_name " +
                    "FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid " +
                    "JOIN pg_class p ON p.oid = i.inhparent " +
                    "WHERE p.relname = 'dump_sales' " +
                    "ORDER BY c.relname")) {
                assertTrue(rs.next());
                assertEquals("dump_sales_2024", rs.getString("relname"));
                assertTrue(rs.getBoolean("relispartition"));
                assertNotNull(rs.getString("partition_expr"));
                assertTrue(rs.getString("partition_expr").contains("FROM"));
                assertEquals("dump_sales", rs.getString("parent_name"));

                assertTrue(rs.next());
                assertEquals("dump_sales_2025", rs.getString("relname"));
                assertFalse(rs.next());
            }
            // pg_dump-style: get partition key definition
            try (ResultSet rs = st.executeQuery(
                    "SELECT pg_get_partkeydef(c.oid) " +
                    "FROM pg_class c WHERE c.relname = 'dump_sales'")) {
                assertTrue(rs.next());
                String keydef = rs.getString(1);
                assertNotNull(keydef);
                assertTrue(keydef.toLowerCase().contains("range"));
                assertTrue(keydef.contains("sale_date"));
            }
            st.execute("DROP TABLE dump_sales_2025");
            st.execute("DROP TABLE dump_sales_2024");
            st.execute("DROP TABLE dump_sales");
        }
    }

    // ===== Combined pg_partitioned_table + pg_inherits query =====

    @Test
    void combinedPartitionMetadataQuery() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE comb_orders (id int, region text) PARTITION BY LIST (region)");
            st.execute("CREATE TABLE comb_orders_us PARTITION OF comb_orders FOR VALUES IN ('US')");
            st.execute("CREATE TABLE comb_orders_eu PARTITION OF comb_orders FOR VALUES IN ('EU')");
            st.execute("CREATE TABLE comb_orders_default PARTITION OF comb_orders DEFAULT");
            // Combined query: parent strategy + children list
            try (ResultSet rs = st.executeQuery(
                    "SELECT pt.partstrat, " +
                    "  (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = pt.partrelid) AS num_partitions " +
                    "FROM pg_partitioned_table pt " +
                    "JOIN pg_class c ON c.oid = pt.partrelid " +
                    "WHERE c.relname = 'comb_orders'")) {
                assertTrue(rs.next());
                assertEquals("l", rs.getString(1));
                assertEquals(3, rs.getInt(2));
            }
            st.execute("DROP TABLE comb_orders_default");
            st.execute("DROP TABLE comb_orders_eu");
            st.execute("DROP TABLE comb_orders_us");
            st.execute("DROP TABLE comb_orders");
        }
    }

    // ===== Edge case: partitioned table with no partitions yet =====

    @Test
    void partitionedTableNoPartitionsYet() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE np_parent (id int) PARTITION BY RANGE (id)");
            // Parent is in pg_partitioned_table
            try (ResultSet rs = st.executeQuery(
                    "SELECT pt.partstrat FROM pg_partitioned_table pt " +
                    "JOIN pg_class c ON c.oid = pt.partrelid " +
                    "WHERE c.relname = 'np_parent'")) {
                assertTrue(rs.next());
                assertEquals("r", rs.getString(1));
            }
            // No children in pg_inherits
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*) FROM pg_inherits i " +
                    "JOIN pg_class p ON p.oid = i.inhparent " +
                    "WHERE p.relname = 'np_parent'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            // relkind is 'p'
            try (ResultSet rs = st.executeQuery(
                    "SELECT relkind FROM pg_class WHERE relname = 'np_parent'")) {
                assertTrue(rs.next());
                assertEquals("p", rs.getString(1));
            }
            st.execute("DROP TABLE np_parent");
        }
    }

    // ===== Namespace-aware query (using public schema) =====

    @Test
    void pgInheritsWithNamespace() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ns_parent (id int) PARTITION BY RANGE (id)");
            st.execute("CREATE TABLE ns_child PARTITION OF ns_parent FOR VALUES FROM (1) TO (100)");
            try (ResultSet rs = st.executeQuery(
                    "SELECT c.relname, n.nspname " +
                    "FROM pg_inherits i " +
                    "JOIN pg_class c ON c.oid = i.inhrelid " +
                    "JOIN pg_class p ON p.oid = i.inhparent " +
                    "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                    "WHERE p.relname = 'ns_parent' AND n.nspname = 'public'")) {
                assertTrue(rs.next());
                assertEquals("ns_child", rs.getString(1));
                assertEquals("public", rs.getString(2));
            }
            st.execute("DROP TABLE ns_child");
            st.execute("DROP TABLE ns_parent");
        }
    }
}
