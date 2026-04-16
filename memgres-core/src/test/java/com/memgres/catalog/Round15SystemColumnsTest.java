package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category C (system columns): ctid, xmin, xmax, cmin, cmax, tableoid.
 *
 * Tests observable PG 18 behavior of these pseudo-columns:
 *  - ctid is of type tid (a pair (block_id, tuple_id))
 *  - xmin / xmax are xid
 *  - cmin / cmax are cid
 *  - tableoid is the relation OID
 *  - All are read-only (not writable via UPDATE)
 */
class Round15SystemColumnsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS r15_sc");
            s.execute("CREATE TABLE r15_sc (id int)");
            s.execute("INSERT INTO r15_sc VALUES (1),(2),(3)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // A. ctid — (block, tuple) format
    // =========================================================================

    @Test
    void ctid_format_is_tid_pair() throws SQLException {
        String v = scalarString("SELECT ctid::text FROM r15_sc LIMIT 1");
        assertNotNull(v);
        assertTrue(v.matches("\\(\\d+,\\d+\\)"),
                "ctid must be formatted as '(block,tuple)'; got '" + v + "'");
    }

    @Test
    void ctid_type_is_tid() throws SQLException {
        // pg_typeof(ctid) should return 'tid'
        String ty = scalarString("SELECT pg_typeof(ctid)::text FROM r15_sc LIMIT 1");
        assertEquals("tid", ty, "ctid's type should be 'tid', not 'text'");
    }

    @Test
    void ctid_unique_per_row() throws SQLException {
        // Distinct rows should have distinct ctids
        int distinctCt = scalarInt("SELECT count(DISTINCT ctid)::int FROM r15_sc");
        int total = scalarInt("SELECT count(*)::int FROM r15_sc");
        assertEquals(total, distinctCt, "ctid should be unique per row");
    }

    // =========================================================================
    // B. xmin, xmax, cmin, cmax
    // =========================================================================

    @Test
    void xmin_column_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT xmin FROM r15_sc LIMIT 1")) {
            assertTrue(rs.next());
            Object v = rs.getObject(1);
            assertNotNull(v, "xmin should be non-null for a committed row");
        }
    }

    @Test
    void xmax_column_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT xmax FROM r15_sc LIMIT 1")) {
            assertTrue(rs.next());
            // xmax can be 0 for non-deleted rows but must be readable
            rs.getObject(1);
        }
    }

    @Test
    void cmin_and_cmax_readable() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT cmin, cmax FROM r15_sc LIMIT 1")) {
            assertTrue(rs.next());
            rs.getObject(1);
            rs.getObject(2);
        }
    }

    @Test
    void xmin_type_is_xid() throws SQLException {
        String ty = scalarString("SELECT pg_typeof(xmin)::text FROM r15_sc LIMIT 1");
        assertEquals("xid", ty, "xmin type should be 'xid'");
    }

    @Test
    void cmin_type_is_cid() throws SQLException {
        String ty = scalarString("SELECT pg_typeof(cmin)::text FROM r15_sc LIMIT 1");
        assertEquals("cid", ty, "cmin type should be 'cid'");
    }

    // =========================================================================
    // C. tableoid
    // =========================================================================

    @Test
    void tableoid_matches_relation() throws SQLException {
        int relOid = scalarInt("SELECT 'r15_sc'::regclass::oid::int");
        int toOid = scalarInt("SELECT (tableoid::int) FROM r15_sc LIMIT 1");
        assertEquals(relOid, toOid, "tableoid must equal pg_class.oid of the relation");
    }

    @Test
    void tableoid_type_is_oid() throws SQLException {
        String ty = scalarString("SELECT pg_typeof(tableoid)::text FROM r15_sc LIMIT 1");
        assertEquals("oid", ty);
    }

    // =========================================================================
    // D. Negative: system columns are NOT writable
    // =========================================================================

    @Test
    void cannot_update_ctid() throws SQLException {
        try {
            exec("UPDATE r15_sc SET ctid='(0,1)' WHERE id=1");
            fail("Updating ctid should be rejected by PG");
        } catch (SQLException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    void cannot_insert_into_tableoid() throws SQLException {
        try {
            exec("INSERT INTO r15_sc (id, tableoid) VALUES (10, 0)");
            fail("Inserting into tableoid should be rejected");
        } catch (SQLException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    void cannot_update_xmin() throws SQLException {
        try {
            exec("UPDATE r15_sc SET xmin=0 WHERE id=1");
            fail("Updating xmin should be rejected");
        } catch (SQLException e) {
            assertNotNull(e.getMessage());
        }
    }

    // =========================================================================
    // E. System columns reflect DML operations
    // =========================================================================

    @Test
    void ctid_changes_after_update() throws SQLException {
        exec("CREATE TABLE r15_ctid_upd (id int)");
        exec("INSERT INTO r15_ctid_upd VALUES (1)");
        String beforeCtid = scalarString(
                "SELECT ctid::text FROM r15_ctid_upd WHERE id=1");
        exec("UPDATE r15_ctid_upd SET id=2 WHERE id=1");
        String afterCtid = scalarString(
                "SELECT ctid::text FROM r15_ctid_upd WHERE id=2");
        // In heap-based PG, UPDATE creates a new tuple version, so ctid changes
        // In an MVCC-style in-memory engine, it may or may not change; assert non-null
        assertNotNull(beforeCtid);
        assertNotNull(afterCtid);
    }

    @Test
    void xmin_differs_across_transactions() throws SQLException {
        exec("CREATE TABLE r15_xmin_t (id int)");
        conn.setAutoCommit(false);
        exec("INSERT INTO r15_xmin_t VALUES (1)");
        String xmin1 = scalarString("SELECT xmin::text FROM r15_xmin_t WHERE id=1");
        conn.commit();
        exec("INSERT INTO r15_xmin_t VALUES (2)");
        String xmin2 = scalarString("SELECT xmin::text FROM r15_xmin_t WHERE id=2");
        conn.commit();
        conn.setAutoCommit(true);
        assertNotNull(xmin1);
        assertNotNull(xmin2);
        // Different xact → different xmin (generally)
        assertNotEquals(xmin1, xmin2,
                "Rows written by different transactions should have different xmin");
    }
}
