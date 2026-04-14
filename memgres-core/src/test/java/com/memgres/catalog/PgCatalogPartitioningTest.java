package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 4 failures from pg-catalog-partitioning.sql where Memgres
 * diverges from PostgreSQL 18 behavior.
 *
 * Stmt 11 - pg_get_partkeydef('pcp_range'::regclass) returns NULL instead of RANGE (id)
 * Stmt 25 - partnatts query on pcp_multi fails with "relation does not exist"
 * Stmt 26 - pg_get_partkeydef('pcp_multi'::regclass) fails with "relation does not exist"
 * Stmt 59 - pg_dump-style query fails with "type regnamespace does not exist"
 */
class PgCatalogPartitioningTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS pcp_test CASCADE");
            s.execute("CREATE SCHEMA pcp_test");
            s.execute("SET search_path = pcp_test, public");

            // 1. Basic RANGE partitioned table
            s.execute("CREATE TABLE pcp_range (id integer, val text) PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE pcp_range_p1 PARTITION OF pcp_range FOR VALUES FROM (1) TO (100)");
            s.execute("CREATE TABLE pcp_range_p2 PARTITION OF pcp_range FOR VALUES FROM (100) TO (200)");

            // 6. LIST partitioned table
            s.execute("CREATE TABLE pcp_list (id integer, status text) PARTITION BY LIST (status)");
            s.execute("CREATE TABLE pcp_list_active PARTITION OF pcp_list FOR VALUES IN ('active')");
            s.execute("CREATE TABLE pcp_list_inactive PARTITION OF pcp_list FOR VALUES IN ('inactive', 'archived')");

            // 7. HASH partitioned table
            s.execute("CREATE TABLE pcp_hash (id integer, val text) PARTITION BY HASH (id)");
            s.execute("CREATE TABLE pcp_hash_p0 PARTITION OF pcp_hash FOR VALUES WITH (MODULUS 3, REMAINDER 0)");
            s.execute("CREATE TABLE pcp_hash_p1 PARTITION OF pcp_hash FOR VALUES WITH (MODULUS 3, REMAINDER 1)");
            s.execute("CREATE TABLE pcp_hash_p2 PARTITION OF pcp_hash FOR VALUES WITH (MODULUS 3, REMAINDER 2)");

            // 8. Multi-column partition key
            s.execute("CREATE TABLE pcp_multi (a integer, b integer, val text) PARTITION BY RANGE (a, b)");
            s.execute("CREATE TABLE pcp_multi_p1 PARTITION OF pcp_multi FOR VALUES FROM (1, 1) TO (10, 10)");

            // 9. Multi-level partitioning
            s.execute("CREATE TABLE pcp_multi_level (id integer, region text, val text) PARTITION BY LIST (region)");
            s.execute("CREATE TABLE pcp_ml_us PARTITION OF pcp_multi_level FOR VALUES IN ('us') PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE pcp_ml_us_1 PARTITION OF pcp_ml_us FOR VALUES FROM (1) TO (1000)");
            s.execute("CREATE TABLE pcp_ml_us_2 PARTITION OF pcp_ml_us FOR VALUES FROM (1000) TO (2000)");
            s.execute("CREATE TABLE pcp_ml_eu PARTITION OF pcp_multi_level FOR VALUES IN ('eu')");

            // 14. Table inheritance (non-partition)
            s.execute("CREATE TABLE pcp_parent (id integer, val text)");
            s.execute("CREATE TABLE pcp_child () INHERITS (pcp_parent)");

            // 18. DEFAULT partition
            s.execute("CREATE TABLE pcp_def_parent (id integer, val text) PARTITION BY LIST (val)");
            s.execute("CREATE TABLE pcp_def_a PARTITION OF pcp_def_parent FOR VALUES IN ('a')");
            s.execute("CREATE TABLE pcp_def_default PARTITION OF pcp_def_parent DEFAULT");
            s.execute("INSERT INTO pcp_def_parent VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS pcp_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row from: " + sql);
            return rs.getString(1);
        }
    }

    /**
     * Stmt 11: pg_get_partkeydef('pcp_range'::regclass) should return
     * 'RANGE (id)' but Memgres returns NULL.
     */
    @Test
    void testPgGetPartkeydefReturnsRangeId() throws SQLException {
        String partkeydef = query1(
                "SELECT pg_get_partkeydef('pcp_range'::regclass) AS partkeydef");
        assertEquals("RANGE (id)", partkeydef,
                "pg_get_partkeydef should return 'RANGE (id)' for a RANGE-partitioned table");
    }

    /**
     * Stmt 25: SELECT partnatts FROM pg_partitioned_table WHERE partrelid =
     * 'pcp_multi'::regclass should return 2, but Memgres errors with
     * "relation \"pcp_multi\" does not exist".
     */
    @Test
    void testMultiColumnPartitionPartnatts() throws SQLException {
        String partnatts = query1(
                "SELECT partnatts FROM pg_partitioned_table "
                        + "WHERE partrelid = 'pcp_multi'::regclass");
        assertEquals("2", partnatts,
                "partnatts should be 2 for a multi-column RANGE partition key (a, b)");
    }

    /**
     * Stmt 26: pg_get_partkeydef('pcp_multi'::regclass) should return
     * 'RANGE (a, b)', but Memgres errors with
     * "relation \"pcp_multi\" does not exist".
     */
    @Test
    void testPgGetPartkeydefMultiColumn() throws SQLException {
        String partkeydef = query1(
                "SELECT pg_get_partkeydef('pcp_multi'::regclass) AS partkeydef");
        assertEquals("RANGE (a, b)", partkeydef,
                "pg_get_partkeydef should return 'RANGE (a, b)' for a multi-column partitioned table");
    }

    /**
     * Stmt 59: pg_dump-style partition introspection query using regnamespace
     * should succeed, but Memgres errors with "type \"regnamespace\" does not exist".
     */
    @Test
    void testPgDumpStylePartitionQueryWithRegnamespace() throws SQLException {
        String hasResults = query1(
                "SELECT count(*) > 0 AS has_results FROM ("
                        + " SELECT c.relname AS child, p.relname AS parent"
                        + " FROM pg_inherits i"
                        + " JOIN pg_class c ON i.inhrelid = c.oid"
                        + " JOIN pg_class p ON i.inhparent = p.oid"
                        + " WHERE p.relnamespace = 'pcp_test'::regnamespace"
                        + ") sub");
        assertEquals("t", hasResults,
                "pg_dump-style query using regnamespace should find partition inheritance rows");
    }
}
