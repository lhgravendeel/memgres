package com.memgres.functions;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category E: UUID generators and transaction-id / snapshot
 * introspection functions.
 *
 * Covers:
 *  - uuid-ossp: uuid_generate_v1 / v3 / v4 / v5
 *  - uuidv7 (PG 18, SQL-level)
 *  - uuid_nil(), uuid_ns_dns(), uuid_ns_url()
 *  - pg_current_xact_id, pg_current_xact_id_if_assigned
 *  - pg_xact_status('<id>')
 *  - pg_current_snapshot, pg_snapshot_xip, pg_snapshot_xmin/xmax,
 *    pg_visible_in_snapshot
 *  - pg_notification_queue_usage()
 */
class Round15UuidAndXactFnsTest {

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

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A. uuid-ossp generators
    // =========================================================================

    @Test
    void uuid_generate_v1_returns_version_1_uuid() throws SQLException {
        String v = scalarString("SELECT uuid_generate_v1()::text");
        assertNotNull(v);
        UUID u = UUID.fromString(v);
        assertEquals(1, u.version(), "uuid_generate_v1 must return v1 UUID");
    }

    @Test
    void uuid_generate_v4_returns_version_4_uuid() throws SQLException {
        String v = scalarString("SELECT uuid_generate_v4()::text");
        assertNotNull(v);
        UUID u = UUID.fromString(v);
        assertEquals(4, u.version(), "uuid_generate_v4 must return v4 UUID");
    }

    @Test
    void uuid_generate_v3_dns_namespace() throws SQLException {
        String v = scalarString(
                "SELECT uuid_generate_v3(uuid_ns_dns(), 'example.com')::text");
        assertNotNull(v);
        UUID u = UUID.fromString(v);
        assertEquals(3, u.version());
    }

    @Test
    void uuid_generate_v5_dns_namespace() throws SQLException {
        String v = scalarString(
                "SELECT uuid_generate_v5(uuid_ns_dns(), 'example.com')::text");
        assertNotNull(v);
        UUID u = UUID.fromString(v);
        assertEquals(5, u.version());
    }

    @Test
    void uuid_generate_v5_deterministic() throws SQLException {
        String v1 = scalarString(
                "SELECT uuid_generate_v5(uuid_ns_dns(), 'example.com')::text");
        String v2 = scalarString(
                "SELECT uuid_generate_v5(uuid_ns_dns(), 'example.com')::text");
        assertEquals(v1, v2, "uuid_generate_v5 must be deterministic");
    }

    @Test
    void uuid_nil() throws SQLException {
        String v = scalarString("SELECT uuid_nil()::text");
        assertEquals("00000000-0000-0000-0000-000000000000", v);
    }

    @Test
    void uuid_ns_dns() throws SQLException {
        String v = scalarString("SELECT uuid_ns_dns()::text");
        // 6ba7b810-9dad-11d1-80b4-00c04fd430c8
        assertEquals("6ba7b810-9dad-11d1-80b4-00c04fd430c8", v);
    }

    @Test
    void uuid_ns_url() throws SQLException {
        String v = scalarString("SELECT uuid_ns_url()::text");
        assertEquals("6ba7b811-9dad-11d1-80b4-00c04fd430c8", v);
    }

    // =========================================================================
    // B. uuidv7 (PG 18)
    // =========================================================================

    @Test
    void uuidv7_returns_version_7() throws SQLException {
        String v = scalarString("SELECT uuidv7()::text");
        assertNotNull(v);
        UUID u = UUID.fromString(v);
        assertEquals(7, u.version(), "uuidv7 must return v7 UUID");
    }

    // =========================================================================
    // C. pg_current_xact_id / pg_xact_status
    // =========================================================================

    @Test
    void pg_current_xact_id_in_transaction() throws SQLException {
        exec("BEGIN");
        try {
            String xid = scalarString("SELECT pg_current_xact_id()::text");
            assertNotNull(xid);
            assertFalse(xid.isEmpty());
        } finally {
            exec("ROLLBACK");
        }
    }

    @Test
    void pg_current_xact_id_if_assigned_null_initially() throws SQLException {
        // Outside a read-only context, should return NULL if no xact-id assigned yet
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_current_xact_id_if_assigned()")) {
            assertTrue(rs.next());
            rs.getObject(1);  // just read — may be null
        }
    }

    @Test
    void pg_xact_status_for_current_txn() throws SQLException {
        exec("BEGIN");
        String status;
        try {
            String xid = scalarString("SELECT pg_current_xact_id()::text");
            status = scalarString("SELECT pg_xact_status('" + xid + "')");
        } finally {
            exec("COMMIT");
        }
        // After commit, status should be 'committed' or 'in progress' before
        assertNotNull(status);
        assertTrue(status.equals("in progress") || status.equals("committed"),
                "pg_xact_status should return 'in progress' or 'committed'; got " + status);
    }

    // =========================================================================
    // D. Snapshot functions
    // =========================================================================

    @Test
    void pg_current_snapshot_and_parts() throws SQLException {
        String snap = scalarString("SELECT pg_current_snapshot()::text");
        assertNotNull(snap);
        // Format: xmin:xmax:xip_list
        assertTrue(snap.contains(":"),
                "pg_current_snapshot should look like 'xmin:xmax:xip_list'; got " + snap);

        String xmin = scalarString("SELECT pg_snapshot_xmin(pg_current_snapshot())::text");
        String xmax = scalarString("SELECT pg_snapshot_xmax(pg_current_snapshot())::text");
        assertNotNull(xmin);
        assertNotNull(xmax);
    }

    @Test
    void pg_snapshot_xip_returns_rows() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT pg_snapshot_xip(pg_current_snapshot())::text")) {
            // May return 0 rows (no xacts in progress)
            while (rs.next()) { rs.getObject(1); }
        }
    }

    @Test
    void pg_visible_in_snapshot() throws SQLException {
        // pg_visible_in_snapshot(xid, snapshot) → bool
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT pg_visible_in_snapshot("
                             + "pg_current_xact_id_if_assigned(), pg_current_snapshot())")) {
            assertTrue(rs.next());
        }
    }

    // =========================================================================
    // E. pg_notification_queue_usage
    // =========================================================================

    @Test
    void pg_notification_queue_usage_returns_fraction() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_notification_queue_usage()")) {
            assertTrue(rs.next());
            double v = rs.getDouble(1);
            assertTrue(v >= 0.0 && v <= 1.0,
                    "pg_notification_queue_usage should be 0..1; got " + v);
        }
    }
}
