package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document sections 25-26: Optimistic locking patterns and schema evolution.
 * Tests version-column locking, timestamp-based locking, retry logic,
 * affected-row counts, ALTER TABLE operations, enum expansion,
 * and concurrent reads during schema changes.
 */
class OptimisticLockingTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(false);
        return c;
    }

    Connection newAutoCommitConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    // --- 1. Version column: UPDATE succeeds when version matches ---

    @Test void version_column_update_succeeds() throws Exception {
        try (Connection c = newAutoCommitConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ol_ver1(id int PRIMARY KEY, name text, version int NOT NULL)");
            s.execute("INSERT INTO ol_ver1 VALUES (1, 'item', 0)");
            int affected = s.executeUpdate(
                    "UPDATE ol_ver1 SET name = 'updated', version = version + 1 WHERE id = 1 AND version = 0");
            assertEquals(1, affected, "Should update exactly 1 row when version matches");
            try (ResultSet rs = s.executeQuery("SELECT version FROM ol_ver1 WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "Version should be incremented to 1");
            }
            s.execute("DROP TABLE ol_ver1");
        }
    }

    // --- 2. Stale version: UPDATE returns 0 rows ---

    @Test void stale_version_returns_zero_rows() throws Exception {
        try (Connection c = newAutoCommitConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ol_ver2(id int PRIMARY KEY, name text, version int NOT NULL)");
            s.execute("INSERT INTO ol_ver2 VALUES (1, 'item', 5)");
            int affected = s.executeUpdate(
                    "UPDATE ol_ver2 SET name = 'stale_attempt', version = version + 1 WHERE id = 1 AND version = 3");
            assertEquals(0, affected, "Stale version should affect 0 rows");
            try (ResultSet rs = s.executeQuery("SELECT name, version FROM ol_ver2 WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("item", rs.getString(1), "Name should remain unchanged");
                assertEquals(5, rs.getInt(2), "Version should remain unchanged");
            }
            s.execute("DROP TABLE ol_ver2");
        }
    }

    // --- 3. Two concurrent sessions: first wins, second gets 0 rows ---

    @Test void concurrent_sessions_first_wins() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            c1.createStatement().execute(
                    "CREATE TABLE ol_conc(id int PRIMARY KEY, name text, version int NOT NULL)");
            c1.createStatement().execute("INSERT INTO ol_conc VALUES (1, 'original', 0)");
            c1.commit();
            c2.commit();

            // Both sessions read version 0
            int ver1, ver2;
            try (ResultSet rs = c1.createStatement().executeQuery(
                    "SELECT version FROM ol_conc WHERE id = 1")) {
                assertTrue(rs.next());
                ver1 = rs.getInt(1);
            }
            try (ResultSet rs = c2.createStatement().executeQuery(
                    "SELECT version FROM ol_conc WHERE id = 1")) {
                assertTrue(rs.next());
                ver2 = rs.getInt(1);
            }
            assertEquals(ver1, ver2, "Both sessions should read the same version");

            // Session 1 updates first
            int affected1 = c1.createStatement().executeUpdate(
                    "UPDATE ol_conc SET name = 'session1', version = version + 1 WHERE id = 1 AND version = " + ver1);
            assertEquals(1, affected1, "First session should succeed");
            c1.commit();

            // Session 2 tries to update with stale version
            int affected2 = c2.createStatement().executeUpdate(
                    "UPDATE ol_conc SET name = 'session2', version = version + 1 WHERE id = 1 AND version = " + ver2);
            assertEquals(0, affected2, "Second session should fail with stale version");
            c2.commit();

            // Verify session 1's update won
            try (ResultSet rs = c1.createStatement().executeQuery(
                    "SELECT name, version FROM ol_conc WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("session1", rs.getString(1));
                assertEquals(1, rs.getInt(2));
            }
            c1.createStatement().execute("DROP TABLE ol_conc");
            c1.commit();
        }
    }

    // --- 4. Timestamp-based optimistic lock ---

    @Test void timestamp_based_optimistic_lock() throws Exception {
        try (Connection c = newAutoCommitConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ol_ts(id int PRIMARY KEY, name text, updated_at timestamp NOT NULL DEFAULT now())");
            s.execute("INSERT INTO ol_ts VALUES (1, 'item', '2025-01-01 12:00:00')");

            // Read current timestamp
            String ts;
            try (ResultSet rs = s.executeQuery("SELECT updated_at FROM ol_ts WHERE id = 1")) {
                assertTrue(rs.next());
                ts = rs.getString(1);
            }

            // Update with matching timestamp
            int affected = s.executeUpdate(
                    "UPDATE ol_ts SET name = 'updated', updated_at = now() WHERE id = 1 AND updated_at = '" + ts + "'");
            assertEquals(1, affected, "Should update when timestamp matches");

            // Try again with old timestamp: should fail
            int stale = s.executeUpdate(
                    "UPDATE ol_ts SET name = 'stale', updated_at = now() WHERE id = 1 AND updated_at = '" + ts + "'");
            assertEquals(0, stale, "Old timestamp should not match after update");

            s.execute("DROP TABLE ol_ts");
        }
    }

    // --- 5. Retry logic: re-read and retry after stale version ---

    @Test void retry_after_stale_version() throws Exception {
        try (Connection c = newAutoCommitConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ol_retry(id int PRIMARY KEY, name text, version int NOT NULL)");
            s.execute("INSERT INTO ol_retry VALUES (1, 'item', 0)");

            // Simulate someone else updating first
            s.executeUpdate("UPDATE ol_retry SET name = 'other', version = version + 1 WHERE id = 1 AND version = 0");

            // Our attempt with stale version 0
            int affected = s.executeUpdate(
                    "UPDATE ol_retry SET name = 'ours', version = version + 1 WHERE id = 1 AND version = 0");
            assertEquals(0, affected, "Should fail with stale version");

            // Retry: re-read current version
            int currentVersion;
            String currentName;
            try (ResultSet rs = s.executeQuery("SELECT name, version FROM ol_retry WHERE id = 1")) {
                assertTrue(rs.next());
                currentName = rs.getString(1);
                currentVersion = rs.getInt(2);
            }
            assertEquals("other", currentName);
            assertEquals(1, currentVersion);

            // Retry with fresh version
            int retryAffected = s.executeUpdate(
                    "UPDATE ol_retry SET name = 'ours', version = version + 1 WHERE id = 1 AND version = " + currentVersion);
            assertEquals(1, retryAffected, "Retry with fresh version should succeed");

            try (ResultSet rs = s.executeQuery("SELECT name, version FROM ol_retry WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("ours", rs.getString(1));
                assertEquals(2, rs.getInt(2));
            }
            s.execute("DROP TABLE ol_retry");
        }
    }

    // --- 6. Affected row count via executeUpdate return value ---

    @Test void affected_row_count_via_executeUpdate() throws Exception {
        try (Connection c = newAutoCommitConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ol_count(id int PRIMARY KEY, val text, version int NOT NULL)");
            s.execute("INSERT INTO ol_count VALUES (1,'a',0),(2,'b',0),(3,'c',0)");

            // Update multiple rows (version matches all)
            int affected = s.executeUpdate(
                    "UPDATE ol_count SET val = 'x', version = version + 1 WHERE version = 0");
            assertEquals(3, affected, "Should report 3 affected rows");

            // Update with stale version
            int stale = s.executeUpdate(
                    "UPDATE ol_count SET val = 'y', version = version + 1 WHERE version = 0");
            assertEquals(0, stale, "All rows now at version 1, stale update should affect 0");

            // Update single row by id and version
            int single = s.executeUpdate(
                    "UPDATE ol_count SET val = 'z', version = version + 1 WHERE id = 2 AND version = 1");
            assertEquals(1, single, "Should affect exactly 1 row");

            s.execute("DROP TABLE ol_count");
        }
    }

    // --- 7. Schema evolution: ALTER TABLE ADD COLUMN with default ---

    @Test void alter_table_add_column_with_default() throws Exception {
        try (Connection c = newAutoCommitConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ol_addcol(id int PRIMARY KEY, name text)");
            s.execute("INSERT INTO ol_addcol VALUES (1, 'alice'), (2, 'bob')");

            s.execute("ALTER TABLE ol_addcol ADD COLUMN status text NOT NULL DEFAULT 'active'");

            try (ResultSet rs = s.executeQuery("SELECT id, name, status FROM ol_addcol ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("alice", rs.getString("name"));
                assertEquals("active", rs.getString("status"), "Default should be applied to existing rows");
                assertTrue(rs.next());
                assertEquals("active", rs.getString("status"));
                assertFalse(rs.next());
            }

            // Insert new row without specifying status: should get default
            s.execute("INSERT INTO ol_addcol(id, name) VALUES (3, 'charlie')");
            try (ResultSet rs = s.executeQuery("SELECT status FROM ol_addcol WHERE id = 3")) {
                assertTrue(rs.next());
                assertEquals("active", rs.getString(1));
            }
            s.execute("DROP TABLE ol_addcol");
        }
    }

    // --- 8. Schema evolution: ALTER TABLE DROP COLUMN, verify data intact ---

    @Test void alter_table_drop_column_data_intact() throws Exception {
        try (Connection c = newAutoCommitConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ol_dropcol(id int PRIMARY KEY, name text, obsolete text, score int)");
            s.execute("INSERT INTO ol_dropcol VALUES (1, 'alice', 'remove_me', 100)");
            s.execute("INSERT INTO ol_dropcol VALUES (2, 'bob', 'also_remove', 200)");

            s.execute("ALTER TABLE ol_dropcol DROP COLUMN obsolete");

            // Remaining columns should still have correct data
            try (ResultSet rs = s.executeQuery("SELECT id, name, score FROM ol_dropcol ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("alice", rs.getString("name"));
                assertEquals(100, rs.getInt("score"));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt("id"));
                assertEquals("bob", rs.getString("name"));
                assertEquals(200, rs.getInt("score"));
                assertFalse(rs.next());
            }

            // Verify the dropped column is no longer accessible
            SQLException ex = assertThrows(SQLException.class, () -> {
                s.executeQuery("SELECT obsolete FROM ol_dropcol");
            });
            assertNotNull(ex.getSQLState());
            s.execute("DROP TABLE ol_dropcol");
        }
    }

    // --- 9. Schema evolution: ALTER TABLE ALTER COLUMN TYPE with USING ---

    @Test void alter_column_type_with_using() throws Exception {
        try (Connection c = newAutoCommitConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ol_alttype(id int PRIMARY KEY, price text)");
            s.execute("INSERT INTO ol_alttype VALUES (1, '19.99'), (2, '42.50')");

            s.execute("ALTER TABLE ol_alttype ALTER COLUMN price TYPE numeric(10,2) USING price::numeric(10,2)");

            try (ResultSet rs = s.executeQuery("SELECT id, price FROM ol_alttype ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals(19.99, rs.getDouble("price"), 0.001);
                assertTrue(rs.next());
                assertEquals(42.50, rs.getDouble("price"), 0.001);
            }

            // Verify arithmetic works on the converted column
            try (ResultSet rs = s.executeQuery("SELECT sum(price) FROM ol_alttype")) {
                assertTrue(rs.next());
                assertEquals(62.49, rs.getDouble(1), 0.001);
            }
            s.execute("DROP TABLE ol_alttype");
        }
    }

    // --- 10. Schema evolution: ADD CHECK CONSTRAINT NOT VALID, then VALIDATE ---

    @Test void add_check_constraint_not_valid_then_validate() throws Exception {
        try (Connection c = newAutoCommitConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TABLE ol_chk(id int PRIMARY KEY, quantity int)");
            s.execute("INSERT INTO ol_chk VALUES (1, 10), (2, 20)");

            // Add constraint as NOT VALID, which does not check existing rows
            s.execute("ALTER TABLE ol_chk ADD CONSTRAINT chk_qty_positive CHECK (quantity > 0) NOT VALID");

            // Existing data is not checked, but new inserts must comply
            SQLException ex = assertThrows(SQLException.class, () -> {
                s.execute("INSERT INTO ol_chk VALUES (3, -5)");
            });
            assertTrue(ex.getMessage().contains("chk_qty_positive") || ex.getSQLState().equals("23514"),
                    "New inserts should be checked even with NOT VALID");

            // VALIDATE the constraint (existing rows happen to comply)
            s.execute("ALTER TABLE ol_chk VALIDATE CONSTRAINT chk_qty_positive");

            // After validation, constraint is fully enforced
            SQLException ex2 = assertThrows(SQLException.class, () -> {
                s.execute("INSERT INTO ol_chk VALUES (4, 0)");
            });
            assertNotNull(ex2.getSQLState());

            // Valid insert should still work
            s.execute("INSERT INTO ol_chk VALUES (5, 50)");
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM ol_chk")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
            s.execute("DROP TABLE ol_chk");
        }
    }

    // --- 11. Enum expansion: CREATE TYPE, ALTER TYPE ADD VALUE ---

    @Test void enum_expansion_alter_type_add_value() throws Exception {
        try (Connection c = newAutoCommitConn()) {
            Statement s = c.createStatement();
            s.execute("CREATE TYPE ol_status AS ENUM ('active', 'inactive')");
            s.execute("CREATE TABLE ol_enum(id int PRIMARY KEY, status ol_status NOT NULL)");
            s.execute("INSERT INTO ol_enum VALUES (1, 'active'), (2, 'inactive')");

            // Expand enum with new value
            s.execute("ALTER TYPE ol_status ADD VALUE 'archived'");

            // Use the new value
            s.execute("INSERT INTO ol_enum VALUES (3, 'archived')");
            try (ResultSet rs = s.executeQuery("SELECT status FROM ol_enum WHERE id = 3")) {
                assertTrue(rs.next());
                assertEquals("archived", rs.getString(1));
            }

            // Add value with positioning
            s.execute("ALTER TYPE ol_status ADD VALUE 'suspended' BEFORE 'inactive'");
            s.execute("INSERT INTO ol_enum VALUES (4, 'suspended')");
            try (ResultSet rs = s.executeQuery("SELECT status FROM ol_enum WHERE id = 4")) {
                assertTrue(rs.next());
                assertEquals("suspended", rs.getString(1));
            }

            // Verify original data is intact
            try (ResultSet rs = s.executeQuery("SELECT status FROM ol_enum WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("active", rs.getString(1));
            }

            s.execute("DROP TABLE ol_enum");
            s.execute("DROP TYPE ol_status");
        }
    }

    // --- 12. Concurrent reads during schema change ---

    @Test void concurrent_reads_during_schema_change() throws Exception {
        try (Connection setup = newAutoCommitConn()) {
            setup.createStatement().execute(
                    "CREATE TABLE ol_schema_conc(id int PRIMARY KEY, name text)");
            setup.createStatement().execute(
                    "INSERT INTO ol_schema_conc VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')");
        }

        // Reader thread: performs reads before, during, and after schema change
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CyclicBarrier barrier = new CyclicBarrier(2);

        Future<Boolean> readerFuture = executor.submit(() -> {
            try (Connection reader = newConn()) {
                // Read before schema change
                try (ResultSet rs = reader.createStatement().executeQuery(
                        "SELECT count(*) FROM ol_schema_conc")) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1));
                }
                reader.commit();

                // Signal that reader is ready
                barrier.await(5, TimeUnit.SECONDS);
                // Wait for writer to complete
                barrier.await(5, TimeUnit.SECONDS);

                // Read after schema change: should see new column
                try (ResultSet rs = reader.createStatement().executeQuery(
                        "SELECT id, name, email FROM ol_schema_conc ORDER BY id")) {
                    assertTrue(rs.next());
                    assertEquals("alice", rs.getString("name"));
                    assertEquals("none", rs.getString("email"));
                }
                reader.commit();
                return true;
            }
        });

        // Writer: performs ALTER TABLE
        try (Connection writer = newConn()) {
            barrier.await(5, TimeUnit.SECONDS);
            writer.createStatement().execute(
                    "ALTER TABLE ol_schema_conc ADD COLUMN email text NOT NULL DEFAULT 'none'");
            writer.commit();
            barrier.await(5, TimeUnit.SECONDS);
        }

        assertTrue(readerFuture.get(10, TimeUnit.SECONDS), "Reader should complete successfully");
        executor.shutdownNow();

        try (Connection cleanup = newAutoCommitConn()) {
            cleanup.createStatement().execute("DROP TABLE ol_schema_conc");
        }
    }
}
