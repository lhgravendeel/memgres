package com.memgres.client;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document sections 24 and 15: Queue/worker patterns with real concurrency.
 * Tests job queues using FOR UPDATE SKIP LOCKED, NOWAIT, advisory locks,
 * retry counters, priority queues, dead letter patterns, and batch claiming.
 */
class QueueWorkerPatternTest {

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

    // --- 1. Basic queue pattern: INSERT, SELECT FOR UPDATE SKIP LOCKED, process, DELETE ---

    @Test void basic_queue_insert_claim_delete() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute(
                    "CREATE TABLE qw_basic(id serial PRIMARY KEY, payload text, status text DEFAULT 'pending')");
            c.createStatement().execute(
                    "INSERT INTO qw_basic(payload) VALUES ('job-a'),('job-b'),('job-c')");
            c.commit();

            // Claim one job
            ResultSet rs = c.createStatement().executeQuery(
                    "SELECT id, payload FROM qw_basic WHERE status = 'pending' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1");
            assertTrue(rs.next());
            int claimedId = rs.getInt(1);
            assertEquals("job-a", rs.getString(2));
            rs.close();

            // Delete the claimed job (simulate processing)
            int deleted = c.createStatement().executeUpdate(
                    "DELETE FROM qw_basic WHERE id = " + claimedId);
            assertEquals(1, deleted);
            c.commit();

            // Verify only two jobs remain
            rs = c.createStatement().executeQuery("SELECT count(*) FROM qw_basic");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            rs.close();
            c.commit();

            c.createStatement().execute("DROP TABLE qw_basic");
            c.commit();
        }
    }

    // --- 2. Two workers competing: each gets a different row ---

    @Test void two_workers_skip_locked_get_different_rows() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            c1.createStatement().execute(
                    "CREATE TABLE qw_compete(id serial PRIMARY KEY, payload text)");
            c1.createStatement().execute(
                    "INSERT INTO qw_compete(payload) VALUES ('j1'),('j2'),('j3')");
            c1.commit();
            c2.commit();

            // Worker 1 claims first available job
            ResultSet rs1 = c1.createStatement().executeQuery(
                    "SELECT id, payload FROM qw_compete ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1");
            assertTrue(rs1.next());
            int w1Id = rs1.getInt(1);
            String w1Payload = rs1.getString(2);
            rs1.close();

            // Worker 2 claims next available job (skips locked row)
            ResultSet rs2 = c2.createStatement().executeQuery(
                    "SELECT id, payload FROM qw_compete ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1");
            assertTrue(rs2.next());
            int w2Id = rs2.getInt(1);
            String w2Payload = rs2.getString(2);
            rs2.close();

            assertNotEquals(w1Id, w2Id, "Workers must claim different rows");
            assertEquals("j1", w1Payload);
            assertEquals("j2", w2Payload);

            c1.commit();
            c2.commit();
            c1.createStatement().execute("DROP TABLE qw_compete");
            c1.commit();
        }
    }

    // --- 3. Job visibility after rollback: rolled-back job becomes available again ---

    @Test void rolled_back_job_becomes_available() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            c1.createStatement().execute(
                    "CREATE TABLE qw_rollback(id serial PRIMARY KEY, payload text)");
            c1.createStatement().execute(
                    "INSERT INTO qw_rollback(payload) VALUES ('task-x')");
            c1.commit();
            c2.commit();

            // Worker 1 claims the job
            ResultSet rs1 = c1.createStatement().executeQuery(
                    "SELECT id FROM qw_rollback ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1");
            assertTrue(rs1.next());
            int claimedId = rs1.getInt(1);
            rs1.close();

            // Worker 2 sees nothing (row is locked)
            ResultSet rs2 = c2.createStatement().executeQuery(
                    "SELECT id FROM qw_rollback ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1");
            assertFalse(rs2.next(), "Locked row should be skipped, queue appears empty");
            rs2.close();
            c2.commit();

            // Worker 1 rolls back (simulates failure)
            c1.rollback();

            // Worker 2 can now see and claim the job
            rs2 = c2.createStatement().executeQuery(
                    "SELECT id FROM qw_rollback ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1");
            assertTrue(rs2.next(), "Job should be available after rollback");
            assertEquals(claimedId, rs2.getInt(1));
            rs2.close();
            c2.commit();

            c1.createStatement().execute("DROP TABLE qw_rollback");
            c1.commit();
        }
    }

    // --- 4. Retry counter update pattern ---

    @Test void retry_counter_increment() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute(
                    "CREATE TABLE qw_retry(id serial PRIMARY KEY, payload text, retry_count int DEFAULT 0, status text DEFAULT 'pending')");
            c.createStatement().execute(
                    "INSERT INTO qw_retry(payload) VALUES ('flaky-job')");
            c.commit();

            // Simulate 3 retry attempts
            for (int attempt = 1; attempt <= 3; attempt++) {
                ResultSet rs = c.createStatement().executeQuery(
                        "SELECT id FROM qw_retry WHERE status = 'pending' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1");
                assertTrue(rs.next());
                int jobId = rs.getInt(1);
                rs.close();

                // Increment retry count
                c.createStatement().executeUpdate(
                        "UPDATE qw_retry SET retry_count = retry_count + 1 WHERE id = " + jobId);
                c.commit();
            }

            // Verify retry_count is 3
            ResultSet rs = c.createStatement().executeQuery(
                    "SELECT retry_count FROM qw_retry WHERE payload = 'flaky-job'");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            rs.close();
            c.commit();

            c.createStatement().execute("DROP TABLE qw_retry");
            c.commit();
        }
    }

    // --- 5. Priority queue: ORDER BY priority FOR UPDATE SKIP LOCKED LIMIT 1 ---

    @Test void priority_queue_highest_priority_first() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute(
                    "CREATE TABLE qw_priority(id serial PRIMARY KEY, payload text, priority int DEFAULT 0)");
            c.createStatement().execute(
                    "INSERT INTO qw_priority(payload, priority) VALUES ('low', 10),('high', 1),('medium', 5)");
            c.commit();

            // Claim jobs in priority order (lower number = higher priority)
            List<String> order = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                ResultSet rs = c.createStatement().executeQuery(
                        "SELECT id, payload FROM qw_priority ORDER BY priority FOR UPDATE SKIP LOCKED LIMIT 1");
                assertTrue(rs.next());
                int jobId = rs.getInt(1);
                order.add(rs.getString(2));
                rs.close();
                c.createStatement().executeUpdate("DELETE FROM qw_priority WHERE id = " + jobId);
                c.commit();
            }

            assertEquals(Cols.listOf("high", "medium", "low"), order,
                    "Jobs should be claimed in priority order");

            c.createStatement().execute("DROP TABLE qw_priority");
            c.commit();
        }
    }

    // --- 6. Queue drain: process all items until none left ---

    @Test void queue_drain_until_empty() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute(
                    "CREATE TABLE qw_drain(id serial PRIMARY KEY, payload text)");
            c.createStatement().execute(
                    "INSERT INTO qw_drain(payload) VALUES ('a'),('b'),('c'),('d'),('e')");
            c.commit();

            int processed = 0;
            while (true) {
                ResultSet rs = c.createStatement().executeQuery(
                        "SELECT id FROM qw_drain ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1");
                if (!rs.next()) {
                    rs.close();
                    c.commit();
                    break;
                }
                int jobId = rs.getInt(1);
                rs.close();
                c.createStatement().executeUpdate("DELETE FROM qw_drain WHERE id = " + jobId);
                c.commit();
                processed++;
            }

            assertEquals(5, processed, "All 5 jobs should be processed");

            // Confirm table is empty
            ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM qw_drain");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            rs.close();
            c.commit();

            c.createStatement().execute("DROP TABLE qw_drain");
            c.commit();
        }
    }

    // --- 7. NOWAIT on locked row throws error ---

    @Test void nowait_on_locked_queue_row_throws() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            c1.createStatement().execute(
                    "CREATE TABLE qw_nowait(id serial PRIMARY KEY, payload text)");
            c1.createStatement().execute(
                    "INSERT INTO qw_nowait(payload) VALUES ('only-job')");
            c1.commit();
            c2.commit();

            // Worker 1 locks the row
            c1.createStatement().executeQuery(
                    "SELECT id FROM qw_nowait WHERE id = 1 FOR UPDATE");

            // Worker 2 with NOWAIT should fail immediately
            SQLException ex = assertThrows(SQLException.class, () -> {
                c2.createStatement().executeQuery(
                        "SELECT id FROM qw_nowait WHERE id = 1 FOR UPDATE NOWAIT");
            });
            assertEquals("55P03", ex.getSQLState(), "NOWAIT should raise lock_not_available (55P03)");

            c1.commit();
            c2.rollback();
            c1.createStatement().execute("DROP TABLE qw_nowait");
            c1.commit();
        }
    }

    // --- 8. Multiple job types with status column filtering ---

    @Test void multiple_job_types_with_status_filter() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute(
                    "CREATE TABLE qw_types(id serial PRIMARY KEY, job_type text, status text DEFAULT 'pending', payload text)");
            c.createStatement().execute(
                    "INSERT INTO qw_types(job_type, payload) VALUES " +
                    "('email','send welcome'),('email','send receipt'),('sms','verify phone'),('report','generate pdf')");
            c.commit();

            // Claim only email jobs
            List<String> emailJobs = new ArrayList<>();
            while (true) {
                ResultSet rs = c.createStatement().executeQuery(
                        "SELECT id, payload FROM qw_types WHERE job_type = 'email' AND status = 'pending' " +
                        "ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1");
                if (!rs.next()) { rs.close(); c.commit(); break; }
                int jobId = rs.getInt(1);
                emailJobs.add(rs.getString(2));
                rs.close();
                c.createStatement().executeUpdate(
                        "UPDATE qw_types SET status = 'processing' WHERE id = " + jobId);
                c.commit();
            }

            assertEquals(2, emailJobs.size());
            assertTrue(emailJobs.contains("send welcome"));
            assertTrue(emailJobs.contains("send receipt"));

            // Verify sms and report jobs still pending
            ResultSet rs = c.createStatement().executeQuery(
                    "SELECT count(*) FROM qw_types WHERE status = 'pending'");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1), "Non-email jobs should remain pending");
            rs.close();
            c.commit();

            c.createStatement().execute("DROP TABLE qw_types");
            c.commit();
        }
    }

    // --- 9. Claiming batch of jobs: LIMIT N FOR UPDATE SKIP LOCKED ---

    @Test void claim_batch_of_jobs() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            c1.createStatement().execute(
                    "CREATE TABLE qw_batch(id serial PRIMARY KEY, payload text)");
            c1.createStatement().execute(
                    "INSERT INTO qw_batch(payload) VALUES ('b1'),('b2'),('b3'),('b4'),('b5')");
            c1.commit();
            c2.commit();

            // Worker 1 claims a batch of 3
            List<Integer> w1Ids = new ArrayList<>();
            ResultSet rs1 = c1.createStatement().executeQuery(
                    "SELECT id FROM qw_batch ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 3");
            while (rs1.next()) w1Ids.add(rs1.getInt(1));
            rs1.close();
            assertEquals(3, w1Ids.size(), "Worker 1 should claim exactly 3 jobs");

            // Worker 2 claims remaining with same batch query
            List<Integer> w2Ids = new ArrayList<>();
            ResultSet rs2 = c2.createStatement().executeQuery(
                    "SELECT id FROM qw_batch ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 3");
            while (rs2.next()) w2Ids.add(rs2.getInt(1));
            rs2.close();
            assertEquals(2, w2Ids.size(), "Worker 2 should claim remaining 2 jobs");

            // No overlap
            Set<Integer> all = new HashSet<>(w1Ids);
            all.addAll(w2Ids);
            assertEquals(5, all.size(), "All 5 jobs should be distributed without overlap");

            c1.commit();
            c2.commit();
            c1.createStatement().execute("DROP TABLE qw_batch");
            c1.commit();
        }
    }

    // --- 10. Job completion: UPDATE status = 'done' WHERE id = ? ---

    @Test void job_completion_status_update() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute(
                    "CREATE TABLE qw_complete(id serial PRIMARY KEY, payload text, status text DEFAULT 'pending', completed_at timestamp)");
            c.createStatement().execute(
                    "INSERT INTO qw_complete(payload) VALUES ('process-me')");
            c.commit();

            // Claim the job
            ResultSet rs = c.createStatement().executeQuery(
                    "SELECT id, payload FROM qw_complete WHERE status = 'pending' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1");
            assertTrue(rs.next());
            int jobId = rs.getInt(1);
            assertEquals("process-me", rs.getString(2));
            rs.close();

            // Mark as done with timestamp
            int updated = c.createStatement().executeUpdate(
                    "UPDATE qw_complete SET status = 'done', completed_at = now() WHERE id = " + jobId);
            assertEquals(1, updated);
            c.commit();

            // Verify status and timestamp
            rs = c.createStatement().executeQuery(
                    "SELECT status, completed_at FROM qw_complete WHERE id = " + jobId);
            assertTrue(rs.next());
            assertEquals("done", rs.getString(1));
            assertNotNull(rs.getTimestamp(2), "completed_at should be set");
            rs.close();

            // No more pending jobs
            rs = c.createStatement().executeQuery(
                    "SELECT count(*) FROM qw_complete WHERE status = 'pending'");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            rs.close();
            c.commit();

            c.createStatement().execute("DROP TABLE qw_complete");
            c.commit();
        }
    }

    // --- 11. Dead letter pattern: move failed jobs to separate table ---

    @Test void dead_letter_pattern() throws Exception {
        try (Connection c = newConn()) {
            c.createStatement().execute(
                    "CREATE TABLE qw_jobs(id serial PRIMARY KEY, payload text, retry_count int DEFAULT 0, status text DEFAULT 'pending')");
            c.createStatement().execute(
                    "CREATE TABLE qw_dead_letter(id int PRIMARY KEY, payload text, retry_count int, failed_at timestamp DEFAULT now())");
            c.createStatement().execute(
                    "INSERT INTO qw_jobs(payload) VALUES ('will-fail')");
            c.commit();

            int maxRetries = 3;
            // Simulate retries until max reached
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                ResultSet rs = c.createStatement().executeQuery(
                        "SELECT id FROM qw_jobs WHERE status = 'pending' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1");
                assertTrue(rs.next());
                int jobId = rs.getInt(1);
                rs.close();
                c.createStatement().executeUpdate(
                        "UPDATE qw_jobs SET retry_count = retry_count + 1 WHERE id = " + jobId);
                c.commit();
            }

            // Move jobs that exceeded max retries to dead letter table
            int moved = c.createStatement().executeUpdate(
                    "INSERT INTO qw_dead_letter(id, payload, retry_count) " +
                    "SELECT id, payload, retry_count FROM qw_jobs WHERE retry_count >= " + maxRetries);
            assertEquals(1, moved);
            c.createStatement().executeUpdate(
                    "DELETE FROM qw_jobs WHERE retry_count >= " + maxRetries);
            c.commit();

            // Verify main queue is empty
            ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM qw_jobs");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            rs.close();

            // Verify dead letter has the job
            rs = c.createStatement().executeQuery(
                    "SELECT payload, retry_count, failed_at FROM qw_dead_letter");
            assertTrue(rs.next());
            assertEquals("will-fail", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertNotNull(rs.getTimestamp(3), "failed_at should be set");
            assertFalse(rs.next());
            rs.close();
            c.commit();

            c.createStatement().execute("DROP TABLE qw_dead_letter");
            c.createStatement().execute("DROP TABLE qw_jobs");
            c.commit();
        }
    }

    // --- 12. Advisory locks: pg_advisory_lock / pg_try_advisory_lock ---

    @Test void advisory_lock_basic() throws Exception {
        try (Connection c = newConn()) {
            long lockId = 12345L;
            // Acquire advisory lock
            ResultSet rs = c.createStatement().executeQuery(
                    "SELECT pg_advisory_lock(" + lockId + ")");
            assertTrue(rs.next());
            rs.close();

            // Release advisory lock
            rs = c.createStatement().executeQuery(
                    "SELECT pg_advisory_unlock(" + lockId + ")");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1), "Unlock should return true");
            rs.close();
            c.commit();
        }
    }

    @Test void advisory_try_lock_succeeds_when_free() throws Exception {
        try (Connection c = newConn()) {
            long lockId = 22222L;
            ResultSet rs = c.createStatement().executeQuery(
                    "SELECT pg_try_advisory_lock(" + lockId + ")");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1), "Should acquire free advisory lock");
            rs.close();

            // Release
            c.createStatement().executeQuery("SELECT pg_advisory_unlock(" + lockId + ")");
            c.commit();
        }
    }

    @Test void advisory_try_lock_fails_when_held() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            long lockId = 33333L;

            // Session 1 acquires the lock
            ResultSet rs1 = c1.createStatement().executeQuery(
                    "SELECT pg_advisory_lock(" + lockId + ")");
            assertTrue(rs1.next());
            rs1.close();

            // Session 2 tries to acquire: should fail (non-blocking)
            ResultSet rs2 = c2.createStatement().executeQuery(
                    "SELECT pg_try_advisory_lock(" + lockId + ")");
            assertTrue(rs2.next());
            assertFalse(rs2.getBoolean(1), "Should fail to acquire held advisory lock");
            rs2.close();

            // Session 1 releases
            c1.createStatement().executeQuery("SELECT pg_advisory_unlock(" + lockId + ")");
            c1.commit();

            // Session 2 can now acquire
            rs2 = c2.createStatement().executeQuery(
                    "SELECT pg_try_advisory_lock(" + lockId + ")");
            assertTrue(rs2.next());
            assertTrue(rs2.getBoolean(1), "Should acquire after release");
            rs2.close();

            c2.createStatement().executeQuery("SELECT pg_advisory_unlock(" + lockId + ")");
            c2.commit();
        }
    }

    @Test void advisory_xact_lock_released_on_commit() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            long lockId = 44444L;

            // Session 1 acquires a transaction-level advisory lock
            ResultSet rs = c1.createStatement().executeQuery(
                    "SELECT pg_advisory_xact_lock(" + lockId + ")");
            assertTrue(rs.next());
            rs.close();

            // Session 2 cannot acquire
            ResultSet rs2 = c2.createStatement().executeQuery(
                    "SELECT pg_try_advisory_xact_lock(" + lockId + ")");
            assertTrue(rs2.next());
            assertFalse(rs2.getBoolean(1), "Lock should be held by session 1");
            rs2.close();
            c2.commit();

            // Session 1 commits; lock should be auto-released
            c1.commit();

            // Session 2 can now acquire
            rs2 = c2.createStatement().executeQuery(
                    "SELECT pg_try_advisory_xact_lock(" + lockId + ")");
            assertTrue(rs2.next());
            assertTrue(rs2.getBoolean(1), "Lock should be available after commit");
            rs2.close();
            c2.commit();
        }
    }

    @Test void advisory_lock_as_queue_coordinator() throws Exception {
        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            c1.createStatement().execute(
                    "CREATE TABLE qw_adv_queue(id serial PRIMARY KEY, payload text)");
            c1.createStatement().execute(
                    "INSERT INTO qw_adv_queue(payload) VALUES ('x'),('y')");
            c1.commit();
            c2.commit();

            long queueLockId = 55555L;

            // Worker 1 acquires advisory lock to coordinate queue access
            ResultSet rs = c1.createStatement().executeQuery(
                    "SELECT pg_try_advisory_lock(" + queueLockId + ")");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            rs.close();

            // Worker 1 claims a job while holding advisory lock
            rs = c1.createStatement().executeQuery(
                    "SELECT id, payload FROM qw_adv_queue ORDER BY id LIMIT 1");
            assertTrue(rs.next());
            int jobId = rs.getInt(1);
            assertEquals("x", rs.getString(2));
            rs.close();
            c1.createStatement().executeUpdate("DELETE FROM qw_adv_queue WHERE id = " + jobId);

            // Worker 2 tries advisory lock and fails, knowing another worker is active
            ResultSet rs2 = c2.createStatement().executeQuery(
                    "SELECT pg_try_advisory_lock(" + queueLockId + ")");
            assertTrue(rs2.next());
            assertFalse(rs2.getBoolean(1), "Worker 2 should know queue is being processed");
            rs2.close();

            // Worker 1 releases advisory lock
            c1.createStatement().executeQuery("SELECT pg_advisory_unlock(" + queueLockId + ")");
            c1.commit();

            // Worker 2 can now take over
            rs2 = c2.createStatement().executeQuery(
                    "SELECT pg_try_advisory_lock(" + queueLockId + ")");
            assertTrue(rs2.next());
            assertTrue(rs2.getBoolean(1));
            rs2.close();

            rs2 = c2.createStatement().executeQuery(
                    "SELECT id, payload FROM qw_adv_queue ORDER BY id LIMIT 1");
            assertTrue(rs2.next());
            assertEquals("y", rs2.getString(2));
            rs2.close();

            c2.createStatement().executeQuery("SELECT pg_advisory_unlock(" + queueLockId + ")");
            c2.commit();

            c1.createStatement().execute("DROP TABLE qw_adv_queue");
            c1.commit();
        }
    }
}
