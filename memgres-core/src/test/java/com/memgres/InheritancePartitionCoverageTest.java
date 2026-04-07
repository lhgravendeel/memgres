package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive coverage tests for checklist items 94-95.
 *
 * 94. Table inheritance: INHERITS clause, ONLY keyword, parent-child queries,
 *     ALTER TABLE INHERIT/NO INHERIT, constraint inheritance
 * 95. Table partitioning: PARTITION BY RANGE/LIST/HASH, CREATE TABLE ... PARTITION OF,
 *     partition bounds, DEFAULT partition, multi-level partitioning,
 *     ALTER TABLE ATTACH/DETACH PARTITION, partition pruning, INSERT routing
 */
class InheritancePartitionCoverageTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
    }

    @AfterAll
    static void stop() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private ResultSet q(String sql) throws Exception {
        return conn.createStatement().executeQuery(sql);
    }

    private void exec(String sql) throws Exception {
        conn.createStatement().execute(sql);
    }

    private int queryInt(String sql) throws Exception {
        ResultSet rs = q(sql);
        assertTrue(rs.next());
        return rs.getInt(1);
    }

    private String queryStr(String sql) throws Exception {
        ResultSet rs = q(sql);
        assertTrue(rs.next());
        return rs.getString(1);
    }

    // =========================================================================
    // 94. TABLE INHERITANCE
    // =========================================================================

    @Test
    @DisplayName("Inheritance: basic INHERITS clause, child gets parent columns plus its own")
    void inh_basic_inherits_clause() throws Exception {
        exec("CREATE TABLE inh_parent1 (id INT, name TEXT)");
        exec("CREATE TABLE inh_child1 (age INT) INHERITS (inh_parent1)");

        // Child should have all three columns: id, name (from parent) + age (own)
        exec("INSERT INTO inh_child1 (id, name, age) VALUES (1, 'Alice', 30)");
        ResultSet rs = q("SELECT id, name, age FROM inh_child1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("Alice", rs.getString("name"));
        assertEquals(30, rs.getInt("age"));
        assertFalse(rs.next());
    }

    @Test
    @DisplayName("Inheritance: insert into child, query parent sees child rows")
    void inh_parent_sees_child_rows() throws Exception {
        exec("CREATE TABLE inh_parent2 (id INT, val TEXT)");
        exec("CREATE TABLE inh_child2 (extra INT) INHERITS (inh_parent2)");

        exec("INSERT INTO inh_parent2 (id, val) VALUES (1, 'parent_row')");
        exec("INSERT INTO inh_child2 (id, val, extra) VALUES (2, 'child_row', 99)");

        // Query on parent should see both rows (parent + child)
        ResultSet rs = q("SELECT id, val FROM inh_parent2 ORDER BY id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("parent_row", rs.getString("val"));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt("id"));
        assertEquals("child_row", rs.getString("val"));
        assertFalse(rs.next());
    }

    @Test
    @DisplayName("Inheritance: ONLY keyword excludes child rows")
    void inh_only_keyword() throws Exception {
        exec("CREATE TABLE inh_parent3 (id INT, val TEXT)");
        exec("CREATE TABLE inh_child3 (extra INT) INHERITS (inh_parent3)");

        exec("INSERT INTO inh_parent3 (id, val) VALUES (1, 'parent_only')");
        exec("INSERT INTO inh_child3 (id, val, extra) VALUES (2, 'child_only', 10)");

        // ONLY parent should exclude child rows
        ResultSet rs = q("SELECT id, val FROM ONLY inh_parent3 ORDER BY id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("parent_only", rs.getString("val"));
        assertFalse(rs.next());
    }

    @Test
    @DisplayName("Inheritance: multiple inheritance, child inherits from two parents")
    void inh_multiple_inheritance() throws Exception {
        exec("CREATE TABLE inh_mp1 (a INT)");
        exec("CREATE TABLE inh_mp2 (b TEXT)");
        exec("CREATE TABLE inh_mc1 (c BOOLEAN) INHERITS (inh_mp1, inh_mp2)");

        exec("INSERT INTO inh_mc1 (a, b, c) VALUES (1, 'hello', true)");
        ResultSet rs = q("SELECT a, b, c FROM inh_mc1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("a"));
        assertEquals("hello", rs.getString("b"));
        assertTrue(rs.getBoolean("c"));
        assertFalse(rs.next());
    }

    @Test
    @DisplayName("Inheritance: ALTER TABLE ... INHERIT adds dynamic inheritance")
    void inh_alter_inherit() throws Exception {
        exec("CREATE TABLE inh_dyn_parent (id INT, name TEXT)");
        exec("CREATE TABLE inh_dyn_child (id INT, name TEXT, extra INT)");

        exec("INSERT INTO inh_dyn_child (id, name, extra) VALUES (1, 'dyn', 42)");

        // Before INHERIT, parent should not see child rows
        ResultSet rs = q("SELECT COUNT(*) FROM inh_dyn_parent");
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));

        // Add inheritance
        exec("ALTER TABLE inh_dyn_child INHERIT inh_dyn_parent");

        // Now parent should see child rows
        rs = q("SELECT id, name FROM inh_dyn_parent");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("dyn", rs.getString("name"));
    }

    @Test
    @DisplayName("Inheritance: ALTER TABLE ... NO INHERIT removes inheritance")
    void inh_alter_no_inherit() throws Exception {
        exec("CREATE TABLE inh_noinherit_p (id INT, val TEXT)");
        exec("CREATE TABLE inh_noinherit_c (extra INT) INHERITS (inh_noinherit_p)");

        exec("INSERT INTO inh_noinherit_c (id, val, extra) VALUES (1, 'x', 10)");

        // Parent should see child rows
        assertEquals(1, queryInt("SELECT COUNT(*) FROM inh_noinherit_p"));

        // Remove inheritance
        exec("ALTER TABLE inh_noinherit_c NO INHERIT inh_noinherit_p");

        // Now parent should not see child rows
        assertEquals(0, queryInt("SELECT COUNT(*) FROM inh_noinherit_p"));

        // Child data is still there
        assertEquals(1, queryInt("SELECT COUNT(*) FROM inh_noinherit_c"));
    }

    @Test
    @DisplayName("Inheritance: deep chain grandparent -> parent -> child")
    void inh_deep_chain() throws Exception {
        exec("CREATE TABLE inh_gp (id INT, gp_col TEXT)");
        exec("CREATE TABLE inh_p (p_col TEXT) INHERITS (inh_gp)");
        exec("CREATE TABLE inh_gc (gc_col TEXT) INHERITS (inh_p)");

        exec("INSERT INTO inh_gp (id, gp_col) VALUES (1, 'grandparent')");
        exec("INSERT INTO inh_p (id, gp_col, p_col) VALUES (2, 'parent', 'pval')");
        exec("INSERT INTO inh_gc (id, gp_col, p_col, gc_col) VALUES (3, 'grandchild', 'pval2', 'gcval')");

        // Grandparent should see all three rows (getAllRows is recursive)
        assertEquals(3, queryInt("SELECT COUNT(*) FROM inh_gp"));

        // Parent should see 2 rows (own + grandchild)
        assertEquals(2, queryInt("SELECT COUNT(*) FROM inh_p"));

        // Grandchild should see 1 row
        assertEquals(1, queryInt("SELECT COUNT(*) FROM inh_gc"));
    }

    @Test
    @DisplayName("Inheritance: UPDATE on parent table affects own rows only")
    void inh_update_parent() throws Exception {
        exec("CREATE TABLE inh_upd_p (id INT, val TEXT)");
        exec("CREATE TABLE inh_upd_c (extra INT) INHERITS (inh_upd_p)");

        exec("INSERT INTO inh_upd_p (id, val) VALUES (1, 'old')");
        exec("INSERT INTO inh_upd_c (id, val, extra) VALUES (2, 'cold', 5)");

        // UPDATE on parent table, only own rows
        exec("UPDATE inh_upd_p SET val = 'new' WHERE id = 1");
        assertEquals("new", queryStr("SELECT val FROM ONLY inh_upd_p WHERE id = 1"));

        // Child row should be unaffected
        assertEquals("cold", queryStr("SELECT val FROM inh_upd_c WHERE id = 2"));
    }

    @Test
    @DisplayName("Inheritance: DELETE on parent table only removes own rows")
    void inh_delete_parent() throws Exception {
        exec("CREATE TABLE inh_del_p (id INT, val TEXT)");
        exec("CREATE TABLE inh_del_c (extra INT) INHERITS (inh_del_p)");

        exec("INSERT INTO inh_del_p (id, val) VALUES (1, 'parent_del')");
        exec("INSERT INTO inh_del_c (id, val, extra) VALUES (2, 'child_del', 7)");

        // Delete from parent (own rows only since getRows is used)
        exec("DELETE FROM inh_del_p WHERE id = 1");

        // Parent own row should be gone
        assertEquals(0, queryInt("SELECT COUNT(*) FROM ONLY inh_del_p"));

        // Child row still visible
        assertEquals(1, queryInt("SELECT COUNT(*) FROM inh_del_c"));

        // Parent query (inheriting) should still see child
        assertEquals(1, queryInt("SELECT COUNT(*) FROM inh_del_p"));
    }

    @Test
    @DisplayName("Inheritance: child with additional columns beyond parent")
    void inh_child_extra_columns() throws Exception {
        exec("CREATE TABLE inh_extra_p (id INT)");
        exec("CREATE TABLE inh_extra_c (name TEXT, age INT, active BOOLEAN) INHERITS (inh_extra_p)");

        exec("INSERT INTO inh_extra_c (id, name, age, active) VALUES (1, 'Bob', 25, true)");

        // Query child with all columns
        ResultSet rs = q("SELECT id, name, age, active FROM inh_extra_c");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("Bob", rs.getString("name"));
        assertEquals(25, rs.getInt("age"));
        assertTrue(rs.getBoolean("active"));

        // Query parent sees only parent columns (id)
        rs = q("SELECT id FROM inh_extra_p");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
    }

    @Test
    @DisplayName("Inheritance: column inheritance works correctly with matching column names")
    void inh_column_dedup() throws Exception {
        // When parent already has a column, child should not duplicate it
        exec("CREATE TABLE inh_dedup_p (id INT, shared TEXT)");
        exec("CREATE TABLE inh_dedup_c (own_col INT) INHERITS (inh_dedup_p)");

        exec("INSERT INTO inh_dedup_c (id, shared, own_col) VALUES (1, 'test', 42)");

        ResultSet rs = q("SELECT id, shared, own_col FROM inh_dedup_c");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("test", rs.getString("shared"));
        assertEquals(42, rs.getInt("own_col"));
    }

    // =========================================================================
    // 95. TABLE PARTITIONING: RANGE
    // =========================================================================

    @Test
    @DisplayName("Partition RANGE: basic range partition creation and structure")
    void part_range_basic() throws Exception {
        exec("CREATE TABLE part_r1 (id INT, val INT) PARTITION BY RANGE (val)");
        exec("CREATE TABLE part_r1_lo PARTITION OF part_r1 FOR VALUES FROM (1) TO (100)");
        exec("CREATE TABLE part_r1_hi PARTITION OF part_r1 FOR VALUES FROM (100) TO (200)");

        // Insert into parent; should route to correct partition
        exec("INSERT INTO part_r1 (id, val) VALUES (1, 50)");
        exec("INSERT INTO part_r1 (id, val) VALUES (2, 150)");

        // Query parent sees all rows
        assertEquals(2, queryInt("SELECT COUNT(*) FROM part_r1"));
    }

    @Test
    @DisplayName("Partition RANGE: INSERT routing to correct partition")
    void part_range_insert_routing() throws Exception {
        exec("CREATE TABLE part_r2 (id INT, score INT) PARTITION BY RANGE (score)");
        exec("CREATE TABLE part_r2_low PARTITION OF part_r2 FOR VALUES FROM (0) TO (50)");
        exec("CREATE TABLE part_r2_mid PARTITION OF part_r2 FOR VALUES FROM (50) TO (100)");

        exec("INSERT INTO part_r2 (id, score) VALUES (1, 25)");
        exec("INSERT INTO part_r2 (id, score) VALUES (2, 75)");

        // Verify routing by querying partition directly
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_r2_low"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_r2_mid"));

        // Verify correct data in each partition
        assertEquals(25, queryInt("SELECT score FROM part_r2_low"));
        assertEquals(75, queryInt("SELECT score FROM part_r2_mid"));
    }

    @Test
    @DisplayName("Partition RANGE: multiple date-like ranges")
    void part_range_multiple() throws Exception {
        exec("CREATE TABLE part_r3 (id INT, category INT) PARTITION BY RANGE (category)");
        exec("CREATE TABLE part_r3_a PARTITION OF part_r3 FOR VALUES FROM (1) TO (10)");
        exec("CREATE TABLE part_r3_b PARTITION OF part_r3 FOR VALUES FROM (10) TO (20)");
        exec("CREATE TABLE part_r3_c PARTITION OF part_r3 FOR VALUES FROM (20) TO (30)");

        exec("INSERT INTO part_r3 (id, category) VALUES (1, 5)");
        exec("INSERT INTO part_r3 (id, category) VALUES (2, 15)");
        exec("INSERT INTO part_r3 (id, category) VALUES (3, 25)");

        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_r3_a"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_r3_b"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_r3_c"));
        assertEquals(3, queryInt("SELECT COUNT(*) FROM part_r3"));
    }

    @Test
    @DisplayName("Partition RANGE: MINVALUE/MAXVALUE bounds")
    void part_range_minvalue_maxvalue() throws Exception {
        exec("CREATE TABLE part_r4 (id INT, val INT) PARTITION BY RANGE (val)");
        exec("CREATE TABLE part_r4_neg PARTITION OF part_r4 FOR VALUES FROM (MINVALUE) TO (0)");
        exec("CREATE TABLE part_r4_pos PARTITION OF part_r4 FOR VALUES FROM (0) TO (MAXVALUE)");

        exec("INSERT INTO part_r4 (id, val) VALUES (1, -50)");
        exec("INSERT INTO part_r4 (id, val) VALUES (2, 50)");

        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_r4_neg"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_r4_pos"));

        assertEquals(-50, queryInt("SELECT val FROM part_r4_neg"));
        assertEquals(50, queryInt("SELECT val FROM part_r4_pos"));
    }

    @Test
    @DisplayName("Partition RANGE: query partition directly")
    void part_range_query_partition() throws Exception {
        exec("CREATE TABLE part_r5 (id INT, amount INT) PARTITION BY RANGE (amount)");
        exec("CREATE TABLE part_r5_small PARTITION OF part_r5 FOR VALUES FROM (0) TO (1000)");
        exec("CREATE TABLE part_r5_big PARTITION OF part_r5 FOR VALUES FROM (1000) TO (10000)");

        exec("INSERT INTO part_r5 (id, amount) VALUES (1, 500)");
        exec("INSERT INTO part_r5 (id, amount) VALUES (2, 5000)");

        // Direct partition query
        ResultSet rs = q("SELECT id, amount FROM part_r5_small");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals(500, rs.getInt("amount"));
        assertFalse(rs.next());
    }

    @Test
    @DisplayName("Partition RANGE: parent query sees all partition rows")
    void part_range_parent_sees_all() throws Exception {
        exec("CREATE TABLE part_r6 (id INT, val INT) PARTITION BY RANGE (val)");
        exec("CREATE TABLE part_r6_p1 PARTITION OF part_r6 FOR VALUES FROM (0) TO (50)");
        exec("CREATE TABLE part_r6_p2 PARTITION OF part_r6 FOR VALUES FROM (50) TO (100)");

        exec("INSERT INTO part_r6 (id, val) VALUES (1, 10)");
        exec("INSERT INTO part_r6 (id, val) VALUES (2, 60)");
        exec("INSERT INTO part_r6 (id, val) VALUES (3, 30)");
        exec("INSERT INTO part_r6 (id, val) VALUES (4, 80)");

        assertEquals(4, queryInt("SELECT COUNT(*) FROM part_r6"));

        // Verify ordering works on the parent
        ResultSet rs = q("SELECT id FROM part_r6 ORDER BY val");
        assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
        assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
        assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
        assertTrue(rs.next()); assertEquals(4, rs.getInt(1));
        assertFalse(rs.next());
    }

    // =========================================================================
    // 95. TABLE PARTITIONING: LIST
    // =========================================================================

    @Test
    @DisplayName("Partition LIST: basic list partition with FOR VALUES IN")
    void part_list_basic() throws Exception {
        exec("CREATE TABLE part_l1 (id INT, status TEXT) PARTITION BY LIST (status)");
        exec("CREATE TABLE part_l1_active PARTITION OF part_l1 FOR VALUES IN ('active')");
        exec("CREATE TABLE part_l1_inactive PARTITION OF part_l1 FOR VALUES IN ('inactive')");

        exec("INSERT INTO part_l1 (id, status) VALUES (1, 'active')");
        exec("INSERT INTO part_l1 (id, status) VALUES (2, 'inactive')");

        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_l1_active"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_l1_inactive"));
        assertEquals(2, queryInt("SELECT COUNT(*) FROM part_l1"));
    }

    @Test
    @DisplayName("Partition LIST: INSERT routing for list partitions")
    void part_list_routing() throws Exception {
        exec("CREATE TABLE part_l2 (id INT, region TEXT) PARTITION BY LIST (region)");
        exec("CREATE TABLE part_l2_us PARTITION OF part_l2 FOR VALUES IN ('US')");
        exec("CREATE TABLE part_l2_eu PARTITION OF part_l2 FOR VALUES IN ('EU')");

        exec("INSERT INTO part_l2 (id, region) VALUES (1, 'US')");
        exec("INSERT INTO part_l2 (id, region) VALUES (2, 'EU')");
        exec("INSERT INTO part_l2 (id, region) VALUES (3, 'US')");

        assertEquals(2, queryInt("SELECT COUNT(*) FROM part_l2_us"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_l2_eu"));
    }

    @Test
    @DisplayName("Partition LIST: multiple values in one partition")
    void part_list_multi_values() throws Exception {
        exec("CREATE TABLE part_l3 (id INT, color TEXT) PARTITION BY LIST (color)");
        exec("CREATE TABLE part_l3_warm PARTITION OF part_l3 FOR VALUES IN ('red', 'orange', 'yellow')");
        exec("CREATE TABLE part_l3_cool PARTITION OF part_l3 FOR VALUES IN ('blue', 'green')");

        exec("INSERT INTO part_l3 (id, color) VALUES (1, 'red')");
        exec("INSERT INTO part_l3 (id, color) VALUES (2, 'orange')");
        exec("INSERT INTO part_l3 (id, color) VALUES (3, 'blue')");
        exec("INSERT INTO part_l3 (id, color) VALUES (4, 'yellow')");

        assertEquals(3, queryInt("SELECT COUNT(*) FROM part_l3_warm"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_l3_cool"));
    }

    @Test
    @DisplayName("Partition LIST: multiple list partitions with integer keys")
    void part_list_multiple_int() throws Exception {
        exec("CREATE TABLE part_l4 (id INT, priority INT) PARTITION BY LIST (priority)");
        exec("CREATE TABLE part_l4_low PARTITION OF part_l4 FOR VALUES IN (1, 2)");
        exec("CREATE TABLE part_l4_med PARTITION OF part_l4 FOR VALUES IN (3)");
        exec("CREATE TABLE part_l4_high PARTITION OF part_l4 FOR VALUES IN (4, 5)");

        exec("INSERT INTO part_l4 (id, priority) VALUES (1, 1)");
        exec("INSERT INTO part_l4 (id, priority) VALUES (2, 3)");
        exec("INSERT INTO part_l4 (id, priority) VALUES (3, 5)");
        exec("INSERT INTO part_l4 (id, priority) VALUES (4, 2)");

        assertEquals(2, queryInt("SELECT COUNT(*) FROM part_l4_low"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_l4_med"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_l4_high"));
    }

    // =========================================================================
    // 95. TABLE PARTITIONING: HASH
    // =========================================================================

    @Test
    @DisplayName("Partition HASH: basic hash partition with MODULUS and REMAINDER")
    void part_hash_basic() throws Exception {
        exec("CREATE TABLE part_h1 (id INT, data TEXT) PARTITION BY HASH (id)");
        exec("CREATE TABLE part_h1_p0 PARTITION OF part_h1 FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
        exec("CREATE TABLE part_h1_p1 PARTITION OF part_h1 FOR VALUES WITH (MODULUS 2, REMAINDER 1)");

        exec("INSERT INTO part_h1 (id, data) VALUES (1, 'a')");
        exec("INSERT INTO part_h1 (id, data) VALUES (2, 'b')");

        // All rows should be routed to one of the two partitions
        int p0 = queryInt("SELECT COUNT(*) FROM part_h1_p0");
        int p1 = queryInt("SELECT COUNT(*) FROM part_h1_p1");
        assertEquals(2, p0 + p1);
        assertEquals(2, queryInt("SELECT COUNT(*) FROM part_h1"));
    }

    @Test
    @DisplayName("Partition HASH: INSERT routing distributes rows")
    void part_hash_routing() throws Exception {
        exec("CREATE TABLE part_h2 (id INT, val TEXT) PARTITION BY HASH (id)");
        exec("CREATE TABLE part_h2_p0 PARTITION OF part_h2 FOR VALUES WITH (MODULUS 3, REMAINDER 0)");
        exec("CREATE TABLE part_h2_p1 PARTITION OF part_h2 FOR VALUES WITH (MODULUS 3, REMAINDER 1)");
        exec("CREATE TABLE part_h2_p2 PARTITION OF part_h2 FOR VALUES WITH (MODULUS 3, REMAINDER 2)");

        for (int i = 0; i < 9; i++) {
            exec("INSERT INTO part_h2 (id, val) VALUES (" + i + ", 'v" + i + "')");
        }

        int p0 = queryInt("SELECT COUNT(*) FROM part_h2_p0");
        int p1 = queryInt("SELECT COUNT(*) FROM part_h2_p1");
        int p2 = queryInt("SELECT COUNT(*) FROM part_h2_p2");
        assertEquals(9, p0 + p1 + p2);
        assertEquals(9, queryInt("SELECT COUNT(*) FROM part_h2"));
    }

    @Test
    @DisplayName("Partition HASH: full modulus coverage")
    void part_hash_full_coverage() throws Exception {
        exec("CREATE TABLE part_h3 (id INT, data TEXT) PARTITION BY HASH (id)");
        exec("CREATE TABLE part_h3_p0 PARTITION OF part_h3 FOR VALUES WITH (MODULUS 4, REMAINDER 0)");
        exec("CREATE TABLE part_h3_p1 PARTITION OF part_h3 FOR VALUES WITH (MODULUS 4, REMAINDER 1)");
        exec("CREATE TABLE part_h3_p2 PARTITION OF part_h3 FOR VALUES WITH (MODULUS 4, REMAINDER 2)");
        exec("CREATE TABLE part_h3_p3 PARTITION OF part_h3 FOR VALUES WITH (MODULUS 4, REMAINDER 3)");

        for (int i = 0; i < 20; i++) {
            exec("INSERT INTO part_h3 (id, data) VALUES (" + i + ", 'val')");
        }

        int total = queryInt("SELECT COUNT(*) FROM part_h3_p0")
                + queryInt("SELECT COUNT(*) FROM part_h3_p1")
                + queryInt("SELECT COUNT(*) FROM part_h3_p2")
                + queryInt("SELECT COUNT(*) FROM part_h3_p3");
        assertEquals(20, total);
        assertEquals(20, queryInt("SELECT COUNT(*) FROM part_h3"));
    }

    // =========================================================================
    // 95. TABLE PARTITIONING: DEFAULT
    // =========================================================================

    @Test
    @DisplayName("Partition DEFAULT: catches unmatched rows")
    void part_default_catches_unmatched() throws Exception {
        exec("CREATE TABLE part_d1 (id INT, cat TEXT) PARTITION BY LIST (cat)");
        exec("CREATE TABLE part_d1_a PARTITION OF part_d1 FOR VALUES IN ('A')");
        exec("CREATE TABLE part_d1_def PARTITION OF part_d1 DEFAULT");

        exec("INSERT INTO part_d1 (id, cat) VALUES (1, 'A')");
        exec("INSERT INTO part_d1 (id, cat) VALUES (2, 'B')");
        exec("INSERT INTO part_d1 (id, cat) VALUES (3, 'C')");

        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_d1_a"));
        assertEquals(2, queryInt("SELECT COUNT(*) FROM part_d1_def"));
        assertEquals(3, queryInt("SELECT COUNT(*) FROM part_d1"));
    }

    @Test
    @DisplayName("Partition DEFAULT: insert with no explicit partition match goes to default")
    void part_default_range_fallback() throws Exception {
        exec("CREATE TABLE part_d2 (id INT, val INT) PARTITION BY RANGE (val)");
        exec("CREATE TABLE part_d2_lo PARTITION OF part_d2 FOR VALUES FROM (0) TO (100)");
        exec("CREATE TABLE part_d2_def PARTITION OF part_d2 DEFAULT");

        exec("INSERT INTO part_d2 (id, val) VALUES (1, 50)");   // goes to lo
        exec("INSERT INTO part_d2 (id, val) VALUES (2, 200)");  // goes to default

        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_d2_lo"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_d2_def"));
    }

    // =========================================================================
    // 95. TABLE PARTITIONING: MULTI-LEVEL
    // =========================================================================

    @Test
    @DisplayName("Partition multi-level: sub-partitioned table")
    void part_multilevel_basic() throws Exception {
        exec("CREATE TABLE part_ml (id INT, year INT, quarter INT) PARTITION BY RANGE (year)");
        exec("CREATE TABLE part_ml_2023 PARTITION OF part_ml FOR VALUES FROM (2023) TO (2024) PARTITION BY RANGE (quarter)");
        exec("CREATE TABLE part_ml_2023_q1 PARTITION OF part_ml_2023 FOR VALUES FROM (1) TO (2)");
        exec("CREATE TABLE part_ml_2023_q2 PARTITION OF part_ml_2023 FOR VALUES FROM (2) TO (3)");

        exec("INSERT INTO part_ml (id, year, quarter) VALUES (1, 2023, 1)");
        exec("INSERT INTO part_ml (id, year, quarter) VALUES (2, 2023, 2)");

        // Rows should be in the leaf sub-partitions
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_ml_2023_q1"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_ml_2023_q2"));
    }

    @Test
    @DisplayName("Partition multi-level: insert routes through sub-partition")
    void part_multilevel_routing() throws Exception {
        exec("CREATE TABLE part_mlr (id INT, region INT, dept INT) PARTITION BY RANGE (region)");
        exec("CREATE TABLE part_mlr_r1 PARTITION OF part_mlr FOR VALUES FROM (1) TO (10) PARTITION BY RANGE (dept)");
        exec("CREATE TABLE part_mlr_r1_d1 PARTITION OF part_mlr_r1 FOR VALUES FROM (1) TO (50)");
        exec("CREATE TABLE part_mlr_r1_d2 PARTITION OF part_mlr_r1 FOR VALUES FROM (50) TO (100)");

        exec("INSERT INTO part_mlr (id, region, dept) VALUES (1, 5, 25)");
        exec("INSERT INTO part_mlr (id, region, dept) VALUES (2, 5, 75)");

        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_mlr_r1_d1"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_mlr_r1_d2"));
    }

    @Test
    @DisplayName("Partition multi-level: query at each level sees correct rows")
    void part_multilevel_root_query() throws Exception {
        exec("CREATE TABLE part_mlq (id INT, a INT, b INT) PARTITION BY RANGE (a)");
        exec("CREATE TABLE part_mlq_a1 PARTITION OF part_mlq FOR VALUES FROM (0) TO (50) PARTITION BY RANGE (b)");
        exec("CREATE TABLE part_mlq_a1_b1 PARTITION OF part_mlq_a1 FOR VALUES FROM (0) TO (50)");
        exec("CREATE TABLE part_mlq_a1_b2 PARTITION OF part_mlq_a1 FOR VALUES FROM (50) TO (100)");
        exec("CREATE TABLE part_mlq_a2 PARTITION OF part_mlq FOR VALUES FROM (50) TO (100)");

        exec("INSERT INTO part_mlq (id, a, b) VALUES (1, 10, 20)");
        exec("INSERT INTO part_mlq (id, a, b) VALUES (2, 10, 70)");
        exec("INSERT INTO part_mlq (id, a, b) VALUES (3, 60, 30)");

        // Leaf sub-partitions have the routed rows
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_mlq_a1_b1"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_mlq_a1_b2"));

        // Mid-level sub-partitioned partition sees its sub-partition rows via getAllRows
        assertEquals(2, queryInt("SELECT COUNT(*) FROM part_mlq_a1"));

        // Direct leaf partition has its row
        ResultSet rs = q("SELECT id, a, b FROM ONLY part_mlq_a2");
        assertTrue(rs.next());
        assertEquals(3, rs.getInt("id"));
        assertEquals(60, rs.getInt("a"));
        assertFalse(rs.next());
    }

    // =========================================================================
    // 95. TABLE PARTITIONING: ATTACH / DETACH
    // =========================================================================

    @Test
    @DisplayName("Partition ATTACH: attach partition with bounds")
    void part_attach_partition() throws Exception {
        exec("CREATE TABLE part_att (id INT, val INT) PARTITION BY RANGE (val)");
        exec("CREATE TABLE part_att_p1 (id INT, val INT)"); // standalone table

        exec("INSERT INTO part_att_p1 (id, val) VALUES (1, 50)");

        // Attach as a partition
        exec("ALTER TABLE part_att ATTACH PARTITION part_att_p1 FOR VALUES FROM (0) TO (100)");

        // Now parent should see the row
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_att"));

        // Inserting into parent should route to the attached partition
        exec("INSERT INTO part_att (id, val) VALUES (2, 75)");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM part_att_p1"));
    }

    @Test
    @DisplayName("Partition DETACH: detach partition becomes standalone")
    void part_detach_partition() throws Exception {
        exec("CREATE TABLE part_det (id INT, val INT) PARTITION BY RANGE (val)");
        exec("CREATE TABLE part_det_p1 PARTITION OF part_det FOR VALUES FROM (0) TO (100)");

        exec("INSERT INTO part_det (id, val) VALUES (1, 50)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_det"));

        // Detach partition
        exec("ALTER TABLE part_det DETACH PARTITION part_det_p1");

        // Parent should now be empty (no partitions)
        assertEquals(0, queryInt("SELECT COUNT(*) FROM part_det"));

        // Detached partition still has data as standalone table
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_det_p1"));
    }

    @Test
    @DisplayName("Partition ATTACH/DETACH: query after operations")
    void part_attach_detach_query() throws Exception {
        exec("CREATE TABLE part_ad (id INT, cat TEXT) PARTITION BY LIST (cat)");
        exec("CREATE TABLE part_ad_a PARTITION OF part_ad FOR VALUES IN ('A')");
        exec("CREATE TABLE part_ad_b (id INT, cat TEXT)");

        exec("INSERT INTO part_ad (id, cat) VALUES (1, 'A')");
        exec("INSERT INTO part_ad_b (id, cat) VALUES (2, 'B')");

        // Before attach: parent sees only partition A
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_ad"));

        // Attach B
        exec("ALTER TABLE part_ad ATTACH PARTITION part_ad_b FOR VALUES IN ('B')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM part_ad"));

        // Detach A
        exec("ALTER TABLE part_ad DETACH PARTITION part_ad_a");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_ad"));
        assertEquals("B", queryStr("SELECT cat FROM part_ad"));
    }

    // =========================================================================
    // 95. TABLE PARTITIONING: EDGE CASES
    // =========================================================================

    @Test
    @DisplayName("Partition: insert with no matching partition throws error")
    void part_no_match_error() throws Exception {
        exec("CREATE TABLE part_err (id INT, val INT) PARTITION BY RANGE (val)");
        exec("CREATE TABLE part_err_p1 PARTITION OF part_err FOR VALUES FROM (0) TO (100)");

        // Value 200 does not match any partition and there is no DEFAULT
        assertThrows(Exception.class, () ->
                exec("INSERT INTO part_err (id, val) VALUES (1, 200)"));
    }

    @Test
    @DisplayName("Partition: multiple inserts with routing verification")
    void part_multiple_inserts_verify() throws Exception {
        exec("CREATE TABLE part_mi (id INT, status TEXT) PARTITION BY LIST (status)");
        exec("CREATE TABLE part_mi_new PARTITION OF part_mi FOR VALUES IN ('new')");
        exec("CREATE TABLE part_mi_done PARTITION OF part_mi FOR VALUES IN ('done')");
        exec("CREATE TABLE part_mi_fail PARTITION OF part_mi FOR VALUES IN ('failed')");

        exec("INSERT INTO part_mi (id, status) VALUES (1, 'new')");
        exec("INSERT INTO part_mi (id, status) VALUES (2, 'done')");
        exec("INSERT INTO part_mi (id, status) VALUES (3, 'failed')");
        exec("INSERT INTO part_mi (id, status) VALUES (4, 'new')");
        exec("INSERT INTO part_mi (id, status) VALUES (5, 'done')");

        assertEquals(2, queryInt("SELECT COUNT(*) FROM part_mi_new"));
        assertEquals(2, queryInt("SELECT COUNT(*) FROM part_mi_done"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM part_mi_fail"));
        assertEquals(5, queryInt("SELECT COUNT(*) FROM part_mi"));
    }

    @Test
    @DisplayName("Partition: partition has same column types as parent")
    void part_same_column_types() throws Exception {
        exec("CREATE TABLE part_types (id INT, name TEXT, amount NUMERIC) PARTITION BY RANGE (id)");
        exec("CREATE TABLE part_types_p1 PARTITION OF part_types FOR VALUES FROM (1) TO (100)");

        exec("INSERT INTO part_types (id, name, amount) VALUES (1, 'test', 99.99)");

        ResultSet rs = q("SELECT id, name, amount FROM part_types_p1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("id"));
        assertEquals("test", rs.getString("name"));
        assertEquals(99.99, rs.getDouble("amount"), 0.001);
    }

    // =========================================================================
    // DELETE / UPDATE on partitioned tables
    // =========================================================================

    @Test
    @DisplayName("Partition: DELETE from partitioned parent removes rows across partitions")
    void part_delete_across_partitions() throws Exception {
        exec("CREATE TABLE part_del (id INT, region TEXT) PARTITION BY LIST (region)");
        exec("CREATE TABLE part_del_us PARTITION OF part_del FOR VALUES IN ('US')");
        exec("CREATE TABLE part_del_eu PARTITION OF part_del FOR VALUES IN ('EU')");
        exec("INSERT INTO part_del VALUES (1, 'US'), (2, 'EU'), (3, 'US'), (4, 'EU')");
        assertEquals(4, queryInt("SELECT COUNT(*) FROM part_del"));

        exec("DELETE FROM part_del WHERE region = 'US'");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM part_del"));
        assertEquals(0, queryInt("SELECT COUNT(*) FROM part_del_us"));
        assertEquals(2, queryInt("SELECT COUNT(*) FROM part_del_eu"));
    }

    @Test
    @DisplayName("Partition: DELETE ALL from partitioned parent removes all rows")
    void part_delete_all_partitions() throws Exception {
        exec("CREATE TABLE part_dela (id INT, cat INT) PARTITION BY RANGE (cat)");
        exec("CREATE TABLE part_dela_1 PARTITION OF part_dela FOR VALUES FROM (0) TO (50)");
        exec("CREATE TABLE part_dela_2 PARTITION OF part_dela FOR VALUES FROM (50) TO (100)");
        exec("INSERT INTO part_dela VALUES (1, 10), (2, 60), (3, 30)");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM part_dela"));

        exec("DELETE FROM part_dela");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM part_dela"));
        assertEquals(0, queryInt("SELECT COUNT(*) FROM part_dela_1"));
        assertEquals(0, queryInt("SELECT COUNT(*) FROM part_dela_2"));
    }

    @Test
    @DisplayName("Partition: UPDATE on partitioned parent modifies rows in partitions")
    void part_update_across_partitions() throws Exception {
        exec("CREATE TABLE part_upd (id INT, region TEXT, val INT) PARTITION BY LIST (region)");
        exec("CREATE TABLE part_upd_us PARTITION OF part_upd FOR VALUES IN ('US')");
        exec("CREATE TABLE part_upd_eu PARTITION OF part_upd FOR VALUES IN ('EU')");
        exec("INSERT INTO part_upd VALUES (1, 'US', 100), (2, 'EU', 200), (3, 'US', 300)");

        exec("UPDATE part_upd SET val = val + 1 WHERE region = 'US'");
        ResultSet rs = q("SELECT id, val FROM part_upd WHERE region = 'US' ORDER BY id");
        assertTrue(rs.next());
        assertEquals(101, rs.getInt("val"));
        assertTrue(rs.next());
        assertEquals(301, rs.getInt("val"));
        assertFalse(rs.next());
        // EU unchanged
        assertEquals(200, queryInt("SELECT val FROM part_upd WHERE region = 'EU'"));
    }
}
