package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for row-level locking syntax: FOR UPDATE, FOR NO KEY UPDATE,
 * FOR SHARE, FOR KEY SHARE, NOWAIT, SKIP LOCKED, and OF table.
 *
 * Also covers the queue pattern: CTE with FOR UPDATE SKIP LOCKED + UPDATE FROM.
 */
class LockingSyntaxTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE lock_t(id int PRIMARY KEY, status text, priority int, payload text)");
        exec("INSERT INTO lock_t VALUES (1,'ready',10,'a'),(2,'ready',5,'b'),(3,'done',1,'c')");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static int countRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = 0; while (rs.next()) n++; return n;
        }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // All locking modes
    // ========================================================================

    @Test void for_update() throws SQLException {
        assertEquals(3, countRows("SELECT * FROM lock_t ORDER BY id FOR UPDATE"));
    }

    @Test void for_no_key_update() throws SQLException {
        assertEquals(3, countRows("SELECT * FROM lock_t ORDER BY id FOR NO KEY UPDATE"));
    }

    @Test void for_share() throws SQLException {
        assertEquals(3, countRows("SELECT * FROM lock_t ORDER BY id FOR SHARE"));
    }

    @Test void for_key_share() throws SQLException {
        assertEquals(3, countRows("SELECT * FROM lock_t ORDER BY id FOR KEY SHARE"));
    }

    @Test void for_update_nowait() throws SQLException {
        assertEquals(3, countRows("SELECT * FROM lock_t ORDER BY id FOR UPDATE NOWAIT"));
    }

    @Test void for_update_skip_locked() throws SQLException {
        int count = countRows(
                "SELECT * FROM lock_t ORDER BY priority DESC, id FOR UPDATE SKIP LOCKED LIMIT 1");
        assertEquals(1, count);
    }

    // ========================================================================
    // Invalid lock combinations
    // ========================================================================

    @Test
    void for_update_for_share_multiple_clauses() throws SQLException {
        assertEquals(3, countRows("SELECT * FROM lock_t FOR UPDATE FOR SHARE"));
    }

    @Test
    void for_invalid_lock_mode_fails() {
        assertThrows(SQLException.class,
                () -> exec("SELECT * FROM lock_t FOR no_such_lock"),
                "Invalid lock mode should fail");
    }

    @Test
    void for_update_of_nonexistent_table_fails() {
        assertThrows(SQLException.class,
                () -> exec("SELECT * FROM lock_t ORDER BY id FOR UPDATE OF no_such_table"),
                "FOR UPDATE OF nonexistent table should fail");
    }

    // ========================================================================
    // Queue pattern: CTE + FOR UPDATE SKIP LOCKED + UPDATE FROM + RETURNING
    // ========================================================================

    @Test
    void queue_pattern_with_returning() throws SQLException {
        exec("CREATE TABLE wq(id int PRIMARY KEY, status text, attempts int DEFAULT 0, payload text)");
        exec("INSERT INTO wq VALUES (1,'ready',0,'a'),(2,'ready',0,'b'),(3,'done',0,'c')");
        try {
            // This is the exact pattern from 38_locking_queue_and_optimistic_locking.sql
            String result = scalar("""
                WITH next_item AS (
                  SELECT id
                  FROM wq
                  WHERE status = 'ready'
                  ORDER BY id
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
                )
                UPDATE wq w
                SET status = 'running',
                    attempts = attempts + 1
                FROM next_item n
                WHERE w.id = n.id
                RETURNING w.id
                """);
            assertNotNull(result, "Queue pattern should return the updated row id");
            assertEquals("1", result, "Should pick id=1 (first ready by id)");
        } finally {
            exec("DROP TABLE IF EXISTS wq");
        }
    }

    // ========================================================================
    // Optimistic locking pattern
    // ========================================================================

    @Test
    void optimistic_locking_version_check() throws SQLException {
        exec("CREATE TABLE ver_t(id int PRIMARY KEY, version int NOT NULL, note text)");
        exec("INSERT INTO ver_t VALUES (1, 1, 'v1')");
        try {
            // First update: version matches → success
            int updated = 0;
            try (Statement s = conn.createStatement()) {
                updated = s.executeUpdate(
                        "UPDATE ver_t SET note = 'v1-updated', version = version + 1 WHERE id = 1 AND version = 1");
            }
            assertEquals(1, updated, "First optimistic update should succeed");

            // Second update: version no longer matches → 0 rows
            try (Statement s = conn.createStatement()) {
                updated = s.executeUpdate(
                        "UPDATE ver_t SET note = 'stale', version = version + 1 WHERE id = 1 AND version = 1");
            }
            assertEquals(0, updated, "Stale optimistic update should update 0 rows");

            assertEquals("v1-updated", scalar("SELECT note FROM ver_t WHERE id = 1"));
        } finally {
            exec("DROP TABLE IF EXISTS ver_t");
        }
    }
}
