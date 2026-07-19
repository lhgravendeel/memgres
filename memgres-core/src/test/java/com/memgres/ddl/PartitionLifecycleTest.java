package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lifecycle tests for partitioned tables: TRUNCATE, DROP, DETACH/ATTACH, and
 * partition-bound typing (MINVALUE/MAXVALUE sentinels, NULL list values).
 */
class PartitionLifecycleTest {

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

    private static int count(Statement s, String table) throws SQLException {
        try (ResultSet rs = s.executeQuery("SELECT count(*) FROM " + table)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // ---- TRUNCATE ----

    @Test
    void truncate_partitioned_parent_empties_all_partitions() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tr_parent (id int, v text) PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE tr_p1 PARTITION OF tr_parent FOR VALUES FROM (0) TO (100)");
            s.execute("CREATE TABLE tr_p2 PARTITION OF tr_parent FOR VALUES FROM (100) TO (200)");
            s.execute("INSERT INTO tr_parent VALUES (10, 'a'), (110, 'b'), (150, 'c')");
            assertEquals(3, count(s, "tr_parent"));
            assertEquals(1, count(s, "tr_p1"));
            assertEquals(2, count(s, "tr_p2"));

            s.execute("TRUNCATE tr_parent");

            assertEquals(0, count(s, "tr_parent"), "parent must be empty after TRUNCATE");
            assertEquals(0, count(s, "tr_p1"), "partition 1 must be empty after TRUNCATE of parent");
            assertEquals(0, count(s, "tr_p2"), "partition 2 must be empty after TRUNCATE of parent");
            s.execute("DROP TABLE tr_parent");
        }
    }

    @Test
    void truncate_parent_empties_multi_level_partitions() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tr_ml (region text, id int) PARTITION BY LIST (region)");
            s.execute("CREATE TABLE tr_ml_us PARTITION OF tr_ml FOR VALUES IN ('us') PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE tr_ml_us_1 PARTITION OF tr_ml_us FOR VALUES FROM (0) TO (100)");
            s.execute("CREATE TABLE tr_ml_eu PARTITION OF tr_ml FOR VALUES IN ('eu')");
            s.execute("INSERT INTO tr_ml VALUES ('us', 5), ('eu', 7)");
            assertEquals(2, count(s, "tr_ml"));
            assertEquals(1, count(s, "tr_ml_us_1"));

            s.execute("TRUNCATE tr_ml");

            assertEquals(0, count(s, "tr_ml"));
            assertEquals(0, count(s, "tr_ml_us"));
            assertEquals(0, count(s, "tr_ml_us_1"), "leaf sub-partition must be empty after TRUNCATE of top parent");
            assertEquals(0, count(s, "tr_ml_eu"));
            s.execute("DROP TABLE tr_ml");
        }
    }

    @Test
    void truncate_only_partitioned_parent_errors() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tro_parent (id int) PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE tro_p1 PARTITION OF tro_parent FOR VALUES FROM (0) TO (100)");
            s.execute("INSERT INTO tro_parent VALUES (1)");

            SQLException ex = assertThrows(SQLException.class, () -> s.execute("TRUNCATE ONLY tro_parent"));
            assertEquals("42809", ex.getSQLState(),
                    "TRUNCATE ONLY on a partitioned table must raise feature_not_supported, got: " + ex.getMessage());
            assertTrue(ex.getMessage().contains("cannot truncate only a partitioned table"),
                    "unexpected message: " + ex.getMessage());
            // Rows must be untouched after the failed TRUNCATE
            assertEquals(1, count(s, "tro_parent"));
            s.execute("DROP TABLE tro_parent");
        }
    }

    @Test
    void truncate_only_leaf_partition_is_allowed() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE trl_parent (id int) PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE trl_p1 PARTITION OF trl_parent FOR VALUES FROM (0) TO (100)");
            s.execute("CREATE TABLE trl_p2 PARTITION OF trl_parent FOR VALUES FROM (100) TO (200)");
            s.execute("INSERT INTO trl_parent VALUES (1), (150)");

            s.execute("TRUNCATE ONLY trl_p1");

            assertEquals(0, count(s, "trl_p1"));
            assertEquals(1, count(s, "trl_p2"), "sibling partition must keep its rows");
            assertEquals(1, count(s, "trl_parent"));
            s.execute("DROP TABLE trl_parent");
        }
    }

    // ---- DROP TABLE ----

    @Test
    void drop_parent_drops_all_partitions() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE dp_parent (id int) PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE dp_p1 PARTITION OF dp_parent FOR VALUES FROM (0) TO (100)");
            s.execute("CREATE TABLE dp_p2 PARTITION OF dp_parent FOR VALUES FROM (100) TO (200)");
            s.execute("INSERT INTO dp_parent VALUES (10), (110)");

            s.execute("DROP TABLE dp_parent");

            for (String t : new String[]{"dp_parent", "dp_p1", "dp_p2"}) {
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.executeQuery("SELECT * FROM " + t));
                assertEquals("42P01", ex.getSQLState(),
                        t + " should no longer exist, got: " + ex.getMessage());
            }
        }
    }

    @Test
    void drop_parent_drops_multi_level_partitions() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE dml_parent (region text, id int) PARTITION BY LIST (region)");
            s.execute("CREATE TABLE dml_us PARTITION OF dml_parent FOR VALUES IN ('us') PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE dml_us_1 PARTITION OF dml_us FOR VALUES FROM (0) TO (100)");
            s.execute("INSERT INTO dml_parent VALUES ('us', 5)");

            s.execute("DROP TABLE dml_parent");

            for (String t : new String[]{"dml_parent", "dml_us", "dml_us_1"}) {
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.executeQuery("SELECT * FROM " + t));
                assertEquals("42P01", ex.getSQLState(),
                        t + " should no longer exist, got: " + ex.getMessage());
            }
        }
    }

    @Test
    void drop_partition_removes_it_from_routing() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE dg_parent (id int) PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE dg_p1 PARTITION OF dg_parent FOR VALUES FROM (0) TO (100)");
            s.execute("CREATE TABLE dg_p2 PARTITION OF dg_parent FOR VALUES FROM (100) TO (200)");
            s.execute("INSERT INTO dg_parent VALUES (10), (110)");

            s.execute("DROP TABLE dg_p1");

            // No ghost rows through the parent
            assertEquals(1, count(s, "dg_parent"));
            // INSERT into the dropped range must fail: no partition covers it anymore
            SQLException ex = assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO dg_parent VALUES (20)"));
            assertEquals("23514", ex.getSQLState(),
                    "insert into dropped partition's range must fail, got: " + ex.getMessage());
            assertTrue(ex.getMessage().contains("no partition of relation"),
                    "unexpected message: " + ex.getMessage());
            // The still-attached partition keeps working
            s.execute("INSERT INTO dg_parent VALUES (120)");
            assertEquals(2, count(s, "dg_parent"));
            s.execute("DROP TABLE dg_parent");
        }
    }

    @Test
    void drop_only_partition_then_insert_errors() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE dop_parent (id int) PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE dop_p1 PARTITION OF dop_parent FOR VALUES FROM (0) TO (100)");
            s.execute("DROP TABLE dop_p1");

            SQLException ex = assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO dop_parent VALUES (1)"));
            assertEquals("23514", ex.getSQLState(),
                    "insert into partitioned table without partitions must fail, got: " + ex.getMessage());
            assertEquals(0, count(s, "dop_parent"), "no ghost row may remain visible through the parent");
            s.execute("DROP TABLE dop_parent");
        }
    }

    // ---- Partition bound typing ----

    @Test
    void text_keyed_range_with_maxvalue_routes_high_values() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE txt_parent (name text) PARTITION BY RANGE (name)");
            s.execute("CREATE TABLE txt_am PARTITION OF txt_parent FOR VALUES FROM (MINVALUE) TO ('m')");
            s.execute("CREATE TABLE txt_mz PARTITION OF txt_parent FOR VALUES FROM ('m') TO (MAXVALUE)");

            s.execute("INSERT INTO txt_parent VALUES ('apple'), ('zebra'), ('mango')");

            assertEquals(3, count(s, "txt_parent"));
            assertEquals(1, count(s, "txt_am"));
            assertEquals(2, count(s, "txt_mz"), "'zebra' and 'mango' must land in the [m, MAXVALUE) partition");
            s.execute("DROP TABLE txt_parent");
        }
    }

    @Test
    void date_keyed_range_with_minvalue_maxvalue() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE dt_parent (d date) PARTITION BY RANGE (d)");
            s.execute("CREATE TABLE dt_old PARTITION OF dt_parent FOR VALUES FROM (MINVALUE) TO ('2026-01-01')");
            s.execute("CREATE TABLE dt_new PARTITION OF dt_parent FOR VALUES FROM ('2026-01-01') TO (MAXVALUE)");

            s.execute("INSERT INTO dt_parent VALUES ('1999-06-15'), ('2030-01-01')");

            assertEquals(1, count(s, "dt_old"));
            assertEquals(1, count(s, "dt_new"));
            s.execute("DROP TABLE dt_parent");
        }
    }

    @Test
    void numeric_range_partitioning_still_works() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE num_parent (n int) PARTITION BY RANGE (n)");
            s.execute("CREATE TABLE num_low PARTITION OF num_parent FOR VALUES FROM (MINVALUE) TO (0)");
            s.execute("CREATE TABLE num_mid PARTITION OF num_parent FOR VALUES FROM (0) TO (100)");
            s.execute("CREATE TABLE num_high PARTITION OF num_parent FOR VALUES FROM (100) TO (MAXVALUE)");

            s.execute("INSERT INTO num_parent VALUES (-5), (50), (100), (99999)");

            assertEquals(1, count(s, "num_low"));
            assertEquals(1, count(s, "num_mid"));
            assertEquals(2, count(s, "num_high"));
            // Out-of-range check still applies on a bounded middle partition set
            s.execute("DROP TABLE num_parent");
        }
    }

    @Test
    void multi_column_range_routing() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mc_parent (a int, b int) PARTITION BY RANGE (a, b)");
            s.execute("CREATE TABLE mc_p1 PARTITION OF mc_parent FOR VALUES FROM (0, 0) TO (10, MAXVALUE)");
            s.execute("CREATE TABLE mc_p2 PARTITION OF mc_parent FOR VALUES FROM (10, MAXVALUE) TO (20, MAXVALUE)");

            s.execute("INSERT INTO mc_parent VALUES (5, 500)");   // -> mc_p1 (5 < 10)
            s.execute("INSERT INTO mc_parent VALUES (10, 3)");    // -> mc_p1 ((10,3) < (10,MAXVALUE))
            s.execute("INSERT INTO mc_parent VALUES (15, 1)");    // -> mc_p2

            assertEquals(2, count(s, "mc_p1"));
            assertEquals(1, count(s, "mc_p2"));

            SQLException ex = assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO mc_parent VALUES (25, 0)"));
            assertEquals("23514", ex.getSQLState(),
                    "row above all bounds must fail, got: " + ex.getMessage());
            s.execute("DROP TABLE mc_parent");
        }
    }

    // ---- NULL routing for LIST partitions ----

    @Test
    void list_partition_with_null_receives_null_rows() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ln_parent (v text) PARTITION BY LIST (v)");
            s.execute("CREATE TABLE ln_ab PARTITION OF ln_parent FOR VALUES IN ('a', 'b')");
            s.execute("CREATE TABLE ln_null PARTITION OF ln_parent FOR VALUES IN (NULL)");

            s.execute("INSERT INTO ln_parent VALUES ('a'), (NULL)");

            assertEquals(2, count(s, "ln_parent"));
            assertEquals(1, count(s, "ln_ab"));
            assertEquals(1, count(s, "ln_null"), "SQL NULL must route to the partition declaring NULL");
            try (ResultSet rs = s.executeQuery("SELECT v FROM ln_null")) {
                assertTrue(rs.next());
                assertNull(rs.getString(1));
            }
            s.execute("DROP TABLE ln_parent");
        }
    }

    @Test
    void list_partition_null_and_value_mix() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE lm_parent (v text) PARTITION BY LIST (v)");
            s.execute("CREATE TABLE lm_mixed PARTITION OF lm_parent FOR VALUES IN ('x', NULL)");

            s.execute("INSERT INTO lm_parent VALUES ('x'), (NULL)");

            assertEquals(2, count(s, "lm_mixed"));
            s.execute("DROP TABLE lm_parent");
        }
    }

    @Test
    void list_null_without_null_partition_goes_to_default() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ld_parent (v text) PARTITION BY LIST (v)");
            s.execute("CREATE TABLE ld_a PARTITION OF ld_parent FOR VALUES IN ('a')");
            s.execute("CREATE TABLE ld_def PARTITION OF ld_parent DEFAULT");

            s.execute("INSERT INTO ld_parent VALUES (NULL)");

            assertEquals(0, count(s, "ld_a"));
            assertEquals(1, count(s, "ld_def"), "NULL without a NULL list partition must go to DEFAULT");
            s.execute("DROP TABLE ld_parent");
        }
    }

    @Test
    void list_null_without_null_or_default_partition_errors() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE le_parent (v text) PARTITION BY LIST (v)");
            s.execute("CREATE TABLE le_a PARTITION OF le_parent FOR VALUES IN ('a')");

            SQLException ex = assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO le_parent VALUES (NULL)"));
            assertEquals("23514", ex.getSQLState(),
                    "NULL with no NULL/DEFAULT partition must fail, got: " + ex.getMessage());
            s.execute("DROP TABLE le_parent");
        }
    }

    @Test
    void range_null_key_goes_to_default_partition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE rn_parent (n int) PARTITION BY RANGE (n)");
            s.execute("CREATE TABLE rn_all PARTITION OF rn_parent FOR VALUES FROM (MINVALUE) TO (MAXVALUE)");
            s.execute("CREATE TABLE rn_def PARTITION OF rn_parent DEFAULT");

            s.execute("INSERT INTO rn_parent VALUES (NULL)");

            assertEquals(0, count(s, "rn_all"), "NULL never matches a RANGE partition");
            assertEquals(1, count(s, "rn_def"));
            s.execute("DROP TABLE rn_parent");
        }
    }

    // ---- DETACH / ATTACH ----

    @Test
    void detach_partition_removes_routing_and_keeps_table() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE de_parent (id int) PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE de_p1 PARTITION OF de_parent FOR VALUES FROM (0) TO (100)");
            s.execute("INSERT INTO de_parent VALUES (10)");

            s.execute("ALTER TABLE de_parent DETACH PARTITION de_p1");

            assertEquals(0, count(s, "de_parent"), "detached partition's rows must not be visible via parent");
            assertEquals(1, count(s, "de_p1"), "detached partition keeps its rows as standalone table");
            SQLException ex = assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO de_parent VALUES (10)"));
            assertEquals("23514", ex.getSQLState(),
                    "detached range must no longer accept routed inserts, got: " + ex.getMessage());
            s.execute("DROP TABLE de_parent");
            s.execute("DROP TABLE de_p1");
        }
    }

    @Test
    void attach_partition_with_overlapping_bounds_errors() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE at_parent (id int) PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE at_p1 PARTITION OF at_parent FOR VALUES FROM (0) TO (100)");
            s.execute("CREATE TABLE at_new (id int)");

            SQLException ex = assertThrows(SQLException.class,
                    () -> s.execute("ALTER TABLE at_parent ATTACH PARTITION at_new FOR VALUES FROM (50) TO (150)"));
            assertTrue("42P17".equals(ex.getSQLState()) || "42P16".equals(ex.getSQLState()),
                    "overlapping ATTACH must fail with an invalid object definition error, got: "
                            + ex.getSQLState() + " / " + ex.getMessage());

            // The failed ATTACH must not leave the table half-attached
            SQLException ins = assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO at_parent VALUES (120)"));
            assertEquals("23514", ins.getSQLState(),
                    "row in the rejected partition's range must fail, got: " + ins.getMessage());

            // A non-overlapping ATTACH then works
            s.execute("ALTER TABLE at_parent ATTACH PARTITION at_new FOR VALUES FROM (100) TO (200)");
            s.execute("INSERT INTO at_parent VALUES (120)");
            assertEquals(1, count(s, "at_new"));
            s.execute("DROP TABLE at_parent");
        }
    }

    @Test
    void attach_reuses_minvalue_maxvalue_sentinels() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE am_parent (name text) PARTITION BY RANGE (name)");
            s.execute("CREATE TABLE am_tail (name text)");
            s.execute("ALTER TABLE am_parent ATTACH PARTITION am_tail FOR VALUES FROM ('m') TO (MAXVALUE)");

            s.execute("INSERT INTO am_parent VALUES ('zebra')");
            assertEquals(1, count(s, "am_tail"));
            s.execute("DROP TABLE am_parent");
        }
    }

    // ---- Transaction rollback keeps routing consistent ----

    @Test
    void rollback_of_drop_partition_restores_routing() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE rb_parent (id int) PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE rb_p1 PARTITION OF rb_parent FOR VALUES FROM (0) TO (100)");
            s.execute("INSERT INTO rb_parent VALUES (10)");
            conn.setAutoCommit(false);
            try {
                s.execute("DROP TABLE rb_p1");
                conn.rollback();
            } finally {
                conn.setAutoCommit(true);
            }
            assertEquals(1, count(s, "rb_parent"), "rolled-back DROP must restore partition rows via parent");
            s.execute("INSERT INTO rb_parent VALUES (20)");
            assertEquals(2, count(s, "rb_p1"));
            s.execute("DROP TABLE rb_parent");
        }
    }
}
