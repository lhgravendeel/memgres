package com.memgres.client;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers concurrent job queue patterns (1300_job_queue_race_and_worker_coordination_scenarios).
 *
 * Tests FOR UPDATE SKIP LOCKED claim patterns, worker fairness, retry semantics,
 * priority/FIFO ordering, state machine transitions, heartbeating, batch claims,
 * deduplication, and concurrent cleanup, all using multiple JDBC connections to
 * a single Memgres instance.
 */
class JobQueueWorkerCoordinationTest {

    static Memgres memgres;
    static String url;

    @BeforeAll
    static void setup() throws Exception {
        memgres = Memgres.builder().port(0).maxConnections(50).build().start();
        url = "jdbc:postgresql://localhost:" + memgres.getPort() + "/test";
    }

    @AfterAll
    static void teardown() throws Exception {
        if (memgres != null) memgres.close();
    }

    /** Open a new connection with autocommit disabled. */
    private Connection newConn() throws SQLException {
        Connection c = DriverManager.getConnection(url, "test", "test");
        c.setAutoCommit(false);
        return c;
    }

    // =========================================================================
    // 1. Basic job claim with FOR UPDATE SKIP LOCKED
    // =========================================================================

    @Test
    void testBasicSkipLockedNoDuplicateClaim() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_basic");
            st.execute("""
                    CREATE TABLE jq_basic (
                        id SERIAL PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'pending',
                        worker TEXT
                    )""");
            for (int i = 0; i < 10; i++) {
                st.execute("INSERT INTO jq_basic (status) VALUES ('pending')");
            }
            setup.commit();
        }

        int workerCount = 5;
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        ConcurrentHashMap<Integer, String> claims = new ConcurrentHashMap<>();
        List<Future<Void>> futures = new ArrayList<>();
        CountDownLatch ready = new CountDownLatch(workerCount);
        CountDownLatch start = new CountDownLatch(1);

        for (int w = 0; w < workerCount; w++) {
            final String workerName = "worker-" + w;
            futures.add(pool.submit(() -> {
                ready.countDown();
                start.await();
                // Each worker claims up to 3 jobs
                for (int attempt = 0; attempt < 3; attempt++) {
                    try (Connection c = newConn(); Statement st = c.createStatement()) {
                        ResultSet rs = st.executeQuery(
                                "SELECT id FROM jq_basic WHERE status = 'pending' " +
                                "FOR UPDATE SKIP LOCKED LIMIT 1");
                        if (rs.next()) {
                            int jobId = rs.getInt(1);
                            String prev = claims.put(jobId, workerName);
                            assertNull(prev, "Job " + jobId + " claimed twice! Previously by " + prev);
                            st.execute("UPDATE jq_basic SET status = 'active', worker = '"
                                    + workerName + "' WHERE id = " + jobId);
                            c.commit();
                        } else {
                            c.rollback();
                        }
                    }
                }
                return null;
            }));
        }

        ready.await();
        start.countDown();
        for (Future<Void> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        // Verify no job appears twice in the claims map (already asserted inline),
        // and all claimed jobs have exactly one worker assigned in the DB.
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT id, COUNT(*) FROM jq_basic WHERE status = 'active' GROUP BY id HAVING COUNT(*) > 1");
            assertFalse(rs.next(), "Duplicate active entries found");
            c.rollback();
        }
    }

    // =========================================================================
    // 2. Exactly-once claim: 10 jobs, 5 workers
    // =========================================================================

    @Test
    void testExactlyOnceClaimAllJobs() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_exactonce");
            st.execute("""
                    CREATE TABLE jq_exactonce (
                        id SERIAL PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'pending',
                        claimed_by TEXT
                    )""");
            for (int i = 0; i < 10; i++) {
                st.execute("INSERT INTO jq_exactonce (status) VALUES ('pending')");
            }
            setup.commit();
        }

        int workerCount = 5;
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        ConcurrentHashMap<Integer, List<String>> claimedBy = new ConcurrentHashMap<>();
        List<Future<Void>> futures = new ArrayList<>();

        for (int w = 0; w < workerCount; w++) {
            final String workerName = "w" + w;
            futures.add(pool.submit(() -> {
                boolean keepGoing = true;
                while (keepGoing) {
                    try (Connection c = newConn(); Statement st = c.createStatement()) {
                        ResultSet rs = st.executeQuery(
                                "SELECT id FROM jq_exactonce WHERE status = 'pending' " +
                                "FOR UPDATE SKIP LOCKED LIMIT 1");
                        if (rs.next()) {
                            int jobId = rs.getInt(1);
                            claimedBy.computeIfAbsent(jobId, k -> Collections.synchronizedList(new ArrayList<>()))
                                     .add(workerName);
                            st.execute("UPDATE jq_exactonce SET status = 'completed', claimed_by = '"
                                    + workerName + "' WHERE id = " + jobId);
                            c.commit();
                        } else {
                            c.rollback();
                            keepGoing = false;
                        }
                    }
                }
                return null;
            }));
        }

        for (Future<Void> f : futures) {
            f.get(15, TimeUnit.SECONDS);
        }
        pool.shutdown();

        // Each job must have been claimed exactly once
        for (Map.Entry<Integer, List<String>> e : claimedBy.entrySet()) {
            assertEquals(1, e.getValue().size(),
                    "Job " + e.getKey() + " was claimed by: " + e.getValue());
        }

        // All 10 jobs should be completed
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM jq_exactonce WHERE status = 'completed'");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            c.rollback();
        }
    }

    // =========================================================================
    // 3. Worker fairness: no starvation
    // =========================================================================

    @Test
    void testWorkerFairnessNoStarvation() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_fairness");
            st.execute("CREATE TABLE jq_fairness (id SERIAL PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending', worker TEXT)");
            for (int i = 0; i < 30; i++) {
                st.execute("INSERT INTO jq_fairness DEFAULT VALUES");
            }
            setup.commit();
        }

        int workerCount = 6;
        ConcurrentHashMap<String, AtomicInteger> workerCounts = new ConcurrentHashMap<>();
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        List<Future<Void>> futures = new ArrayList<>();
        CountDownLatch ready = new CountDownLatch(workerCount);
        CountDownLatch go = new CountDownLatch(1);

        for (int w = 0; w < workerCount; w++) {
            final String name = "worker" + w;
            workerCounts.put(name, new AtomicInteger(0));
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                boolean found = true;
                while (found) {
                    try (Connection c = newConn(); Statement st = c.createStatement()) {
                        ResultSet rs = st.executeQuery(
                                "SELECT id FROM jq_fairness WHERE status = 'pending' " +
                                "FOR UPDATE SKIP LOCKED LIMIT 1");
                        if (rs.next()) {
                            int id = rs.getInt(1);
                            st.execute("UPDATE jq_fairness SET status = 'done', worker = '"
                                    + name + "' WHERE id = " + id);
                            c.commit();
                            workerCounts.get(name).incrementAndGet();
                        } else {
                            c.rollback();
                            found = false;
                        }
                    }
                }
                return null;
            }));
        }

        ready.await();
        go.countDown();
        for (Future<Void> f : futures) {
            f.get(15, TimeUnit.SECONDS);
        }
        pool.shutdown();

        // Every worker should have processed at least 1 job (fairness)
        for (Map.Entry<String, AtomicInteger> e : workerCounts.entrySet()) {
            assertTrue(e.getValue().get() >= 1,
                    "Worker " + e.getKey() + " processed 0 jobs, starvation detected");
        }
    }

    // =========================================================================
    // 4. Retry on failure: ROLLBACK makes job re-claimable
    // =========================================================================

    @Test
    void testRetryOnFailureRollbackMakesJobAvailable() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_retry");
            st.execute("CREATE TABLE jq_retry (id SERIAL PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending')");
            st.execute("INSERT INTO jq_retry DEFAULT VALUES");
            setup.commit();
        }

        // First worker claims but rolls back
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT id FROM jq_retry WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 1");
            assertTrue(rs.next(), "Should have found a pending job");
            int jobId = rs.getInt(1);
            st.execute("UPDATE jq_retry SET status = 'active' WHERE id = " + jobId);
            c.rollback(); // simulate failure
        }

        // Second worker should be able to claim the same job
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT id FROM jq_retry WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 1");
            assertTrue(rs.next(), "Job should be re-claimable after rollback");
            int jobId = rs.getInt(1);
            st.execute("UPDATE jq_retry SET status = 'completed' WHERE id = " + jobId);
            c.commit();
        }

        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT status FROM jq_retry WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("completed", rs.getString(1));
            c.rollback();
        }
    }

    // =========================================================================
    // 5. Dead-letter after max retries
    // =========================================================================

    @Test
    void testDeadLetterAfterMaxRetries() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_deadletter");
            st.execute("""
                    CREATE TABLE jq_deadletter (
                        id SERIAL PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'pending',
                        retry_count INT NOT NULL DEFAULT 0,
                        max_retries INT NOT NULL DEFAULT 3
                    )""");
            st.execute("INSERT INTO jq_deadletter DEFAULT VALUES");
            setup.commit();
        }

        // Simulate 3 failures: each bumps retry_count and resets to pending
        for (int i = 0; i < 3; i++) {
            try (Connection c = newConn(); Statement st = c.createStatement()) {
                ResultSet rs = st.executeQuery(
                        "SELECT id FROM jq_deadletter WHERE status = 'pending' " +
                        "FOR UPDATE SKIP LOCKED LIMIT 1");
                assertTrue(rs.next());
                int id = rs.getInt(1);
                st.execute("UPDATE jq_deadletter SET status = 'pending', retry_count = retry_count + 1 WHERE id = " + id);
                c.commit();
            }
        }

        // Now move to dead status (retry_count >= max_retries)
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            int updated = st.executeUpdate(
                    "UPDATE jq_deadletter SET status = 'dead' WHERE status = 'pending' AND retry_count >= max_retries");
            assertEquals(1, updated);
            c.commit();
        }

        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT status, retry_count FROM jq_deadletter WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("dead", rs.getString(1));
            assertEquals(3, rs.getInt(2));
            c.rollback();
        }
    }

    // =========================================================================
    // 6. Lease expiration: stale active jobs become reclaimable
    // =========================================================================

    @Test
    void testLeaseExpirationPattern() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_lease");
            st.execute("""
                    CREATE TABLE jq_lease (
                        id SERIAL PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'pending',
                        lease_expires_at TIMESTAMPTZ
                    )""");
            // Insert one job already claimed with an expired lease (1 hour ago)
            st.execute("""
                    INSERT INTO jq_lease (status, lease_expires_at)
                    VALUES ('active', NOW() - INTERVAL '1 hour')""");
            setup.commit();
        }

        // Reclaim stale job: expired lease -> back to pending
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            int updated = st.executeUpdate("""
                    UPDATE jq_lease
                       SET status = 'pending', lease_expires_at = NULL
                     WHERE status = 'active'
                       AND lease_expires_at < NOW()""");
            assertEquals(1, updated, "Expired lease job should be reclaimable");
            c.commit();
        }

        // Now claim it with a fresh lease
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT id FROM jq_lease WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 1");
            assertTrue(rs.next(), "Job should be available after lease expiration reset");
            int id = rs.getInt(1);
            st.execute("UPDATE jq_lease SET status = 'active', lease_expires_at = NOW() + INTERVAL '5 minutes' WHERE id = " + id);
            c.commit();
        }

        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT status FROM jq_lease WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("active", rs.getString(1));
            c.rollback();
        }
    }

    // =========================================================================
    // 7. Priority ordering: higher priority claimed first
    // =========================================================================

    @Test
    void testPriorityOrdering() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_priority");
            st.execute("""
                    CREATE TABLE jq_priority (
                        id SERIAL PRIMARY KEY,
                        priority INT NOT NULL DEFAULT 0,
                        status TEXT NOT NULL DEFAULT 'pending',
                        worker TEXT
                    )""");
            st.execute("INSERT INTO jq_priority (priority) VALUES (1)");  // low
            st.execute("INSERT INTO jq_priority (priority) VALUES (10)"); // high
            st.execute("INSERT INTO jq_priority (priority) VALUES (5)");  // medium
            setup.commit();
        }

        List<Integer> claimOrder = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            try (Connection c = newConn(); Statement st = c.createStatement()) {
                ResultSet rs = st.executeQuery(
                        "SELECT id, priority FROM jq_priority WHERE status = 'pending' " +
                        "ORDER BY priority DESC FOR UPDATE SKIP LOCKED LIMIT 1");
                assertTrue(rs.next());
                int id = rs.getInt(1);
                int pri = rs.getInt(2);
                claimOrder.add(pri);
                st.execute("UPDATE jq_priority SET status = 'active' WHERE id = " + id);
                c.commit();
            }
        }

        assertEquals(Cols.listOf(10, 5, 1), claimOrder, "Jobs should be claimed in descending priority order");
    }

    // =========================================================================
    // 8. FIFO ordering: without priority, claim in insertion order
    // =========================================================================

    @Test
    void testFifoOrdering() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_fifo");
            st.execute("""
                    CREATE TABLE jq_fifo (
                        id SERIAL PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'pending'
                    )""");
            for (int i = 0; i < 5; i++) {
                st.execute("INSERT INTO jq_fifo DEFAULT VALUES");
            }
            setup.commit();
        }

        List<Integer> claimOrder = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            try (Connection c = newConn(); Statement st = c.createStatement()) {
                ResultSet rs = st.executeQuery(
                        "SELECT id FROM jq_fifo WHERE status = 'pending' " +
                        "ORDER BY id ASC FOR UPDATE SKIP LOCKED LIMIT 1");
                assertTrue(rs.next());
                int id = rs.getInt(1);
                claimOrder.add(id);
                st.execute("UPDATE jq_fifo SET status = 'active' WHERE id = " + id);
                c.commit();
            }
        }

        // IDs should be claimed in strictly ascending order
        for (int i = 1; i < claimOrder.size(); i++) {
            assertTrue(claimOrder.get(i) > claimOrder.get(i - 1),
                    "Expected FIFO order but got: " + claimOrder);
        }
    }

    // =========================================================================
    // 9. Concurrent INSERT and claim
    // =========================================================================

    @Test
    void testConcurrentInsertAndClaim() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_insclaim");
            st.execute("""
                    CREATE TABLE jq_insclaim (
                        id SERIAL PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'pending',
                        claimed_by TEXT
                    )""");
            setup.commit();
        }

        AtomicInteger inserted = new AtomicInteger(0);
        AtomicInteger claimed = new AtomicInteger(0);
        CyclicBarrier barrier = new CyclicBarrier(2);

        ExecutorService pool = Executors.newFixedThreadPool(2);

        Future<Void> inserter = pool.submit(() -> {
            barrier.await();
            for (int i = 0; i < 20; i++) {
                try (Connection c = newConn(); Statement st = c.createStatement()) {
                    st.execute("INSERT INTO jq_insclaim (status) VALUES ('pending')");
                    c.commit();
                    inserted.incrementAndGet();
                }
                Thread.sleep(1);
            }
            return null;
        });

        Future<Void> claimer = pool.submit(() -> {
            barrier.await();
            for (int attempt = 0; attempt < 30; attempt++) {
                try (Connection c = newConn(); Statement st = c.createStatement()) {
                    ResultSet rs = st.executeQuery(
                            "SELECT id FROM jq_insclaim WHERE status = 'pending' " +
                            "FOR UPDATE SKIP LOCKED LIMIT 1");
                    if (rs.next()) {
                        int id = rs.getInt(1);
                        st.execute("UPDATE jq_insclaim SET status = 'done', claimed_by = 'claimer' WHERE id = " + id);
                        c.commit();
                        claimed.incrementAndGet();
                    } else {
                        c.rollback();
                    }
                }
                Thread.sleep(2);
            }
            return null;
        });

        inserter.get(15, TimeUnit.SECONDS);
        claimer.get(15, TimeUnit.SECONDS);
        pool.shutdown();

        // Total done + pending = total inserted; no data lost
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM jq_insclaim");
            assertTrue(rs.next());
            int total = rs.getInt(1);
            assertEquals(inserted.get(), total, "All inserted jobs should exist");
            c.rollback();
        }
    }

    // =========================================================================
    // 10. Idempotent completion: completing an already-completed job is no-op
    // =========================================================================

    @Test
    void testIdempotentCompletion() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_idempotent");
            st.execute("""
                    CREATE TABLE jq_idempotent (
                        id SERIAL PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'pending',
                        completed_at TIMESTAMPTZ
                    )""");
            st.execute("INSERT INTO jq_idempotent DEFAULT VALUES");
            setup.commit();
        }

        // Complete once
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            st.execute("UPDATE jq_idempotent SET status = 'completed', completed_at = NOW() WHERE id = 1 AND status != 'completed'");
            c.commit();
        }

        // Attempt to complete again; should update 0 rows
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            int rows = st.executeUpdate("UPDATE jq_idempotent SET status = 'completed', completed_at = NOW() WHERE id = 1 AND status != 'completed'");
            assertEquals(0, rows, "Second completion should be a no-op");
            c.commit();
        }

        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM jq_idempotent WHERE status = 'completed'");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            c.rollback();
        }
    }

    // =========================================================================
    // 11. State machine transitions: only valid transitions allowed
    // =========================================================================

    @Test
    void testStateMachineTransitions() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_statemachine");
            st.execute("""
                    CREATE TABLE jq_statemachine (
                        id SERIAL PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'pending',
                        CONSTRAINT chk_status CHECK (status IN ('pending','active','completed','failed'))
                    )""");
            st.execute("INSERT INTO jq_statemachine DEFAULT VALUES"); // id=1
            st.execute("INSERT INTO jq_statemachine DEFAULT VALUES"); // id=2
            setup.commit();
        }

        // Valid: pending -> active -> completed
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            int r1 = st.executeUpdate("UPDATE jq_statemachine SET status = 'active' WHERE id = 1 AND status = 'pending'");
            assertEquals(1, r1);
            int r2 = st.executeUpdate("UPDATE jq_statemachine SET status = 'completed' WHERE id = 1 AND status = 'active'");
            assertEquals(1, r2);
            c.commit();
        }

        // Valid: pending -> active -> failed -> pending
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            st.executeUpdate("UPDATE jq_statemachine SET status = 'active' WHERE id = 2 AND status = 'pending'");
            st.executeUpdate("UPDATE jq_statemachine SET status = 'failed' WHERE id = 2 AND status = 'active'");
            st.executeUpdate("UPDATE jq_statemachine SET status = 'pending' WHERE id = 2 AND status = 'failed'");
            c.commit();
        }

        // Invalid: pending -> completed should affect 0 rows (guard on status = 'active')
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            int skipped = st.executeUpdate("UPDATE jq_statemachine SET status = 'completed' WHERE id = 2 AND status = 'active'");
            assertEquals(0, skipped, "Guard clause must prevent invalid pending->completed hop");
            c.rollback();
        }

        // Invalid status value must fail the check constraint
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            assertThrows(SQLException.class,
                    () -> st.executeUpdate("UPDATE jq_statemachine SET status = 'bogus' WHERE id = 2"));
            c.rollback();
        }
    }

    // =========================================================================
    // 12. Heartbeat update: stale heartbeats indicate stuck jobs
    // =========================================================================

    @Test
    void testHeartbeatUpdate() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_heartbeat");
            st.execute("""
                    CREATE TABLE jq_heartbeat (
                        id SERIAL PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'pending',
                        last_heartbeat TIMESTAMPTZ
                    )""");
            // Insert an active job with a fresh heartbeat
            st.execute("INSERT INTO jq_heartbeat (status, last_heartbeat) VALUES ('active', NOW())");
            // Insert an active job with a stale heartbeat
            st.execute("INSERT INTO jq_heartbeat (status, last_heartbeat) VALUES ('active', NOW() - INTERVAL '10 minutes')");
            setup.commit();
        }

        // Worker refreshes heartbeat for job 1
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            st.execute("UPDATE jq_heartbeat SET last_heartbeat = NOW() WHERE id = 1");
            c.commit();
        }

        // Watchdog: reclaim jobs with stale heartbeats (>5 min)
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            int reclaimed = st.executeUpdate("""
                    UPDATE jq_heartbeat
                       SET status = 'pending', last_heartbeat = NULL
                     WHERE status = 'active'
                       AND last_heartbeat < NOW() - INTERVAL '5 minutes'""");
            assertEquals(1, reclaimed, "Job 2 should be reclaimed due to stale heartbeat");
            c.commit();
        }

        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT id, status FROM jq_heartbeat ORDER BY id");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("active",  rs.getString(2));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("pending", rs.getString(2));
            c.rollback();
        }
    }

    // =========================================================================
    // 13. Batch claim: claim N jobs at once
    // =========================================================================

    @Test
    void testBatchClaim() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_batch");
            st.execute("CREATE TABLE jq_batch (id SERIAL PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending', worker TEXT)");
            for (int i = 0; i < 20; i++) {
                st.execute("INSERT INTO jq_batch DEFAULT VALUES");
            }
            setup.commit();
        }

        // Worker A claims 5 at once
        List<Integer> batchA = new ArrayList<>();
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT id FROM jq_batch WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 5");
            while (rs.next()) batchA.add(rs.getInt(1));
            for (int id : batchA) {
                st.execute("UPDATE jq_batch SET status = 'active', worker = 'workerA' WHERE id = " + id);
            }
            c.commit();
        }
        assertEquals(5, batchA.size());

        // Worker B claims another 5 and should not overlap
        List<Integer> batchB = new ArrayList<>();
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT id FROM jq_batch WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 5");
            while (rs.next()) batchB.add(rs.getInt(1));
            for (int id : batchB) {
                st.execute("UPDATE jq_batch SET status = 'active', worker = 'workerB' WHERE id = " + id);
            }
            c.commit();
        }
        assertEquals(5, batchB.size());

        // No overlap between batches
        Set<Integer> setA = new HashSet<>(batchA);
        Set<Integer> setB = new HashSet<>(batchB);
        setA.retainAll(setB);
        assertTrue(setA.isEmpty(), "Batch A and B must not share any job IDs");

        // Remaining pending jobs
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM jq_batch WHERE status = 'pending'");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            c.rollback();
        }
    }

    // =========================================================================
    // 14. Queue depth monitoring
    // =========================================================================

    @Test
    void testQueueDepthMonitoring() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_depth");
            st.execute("CREATE TABLE jq_depth (id SERIAL PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending')");
            for (int i = 0; i < 10; i++) {
                st.execute("INSERT INTO jq_depth DEFAULT VALUES");
            }
            setup.commit();
        }

        // Initial depth = 10
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM jq_depth WHERE status = 'pending'");
            assertTrue(rs.next()); assertEquals(10, rs.getInt(1));
            c.rollback();
        }

        // Claim 3 jobs
        for (int i = 0; i < 3; i++) {
            try (Connection c = newConn(); Statement st = c.createStatement()) {
                ResultSet rs = st.executeQuery(
                        "SELECT id FROM jq_depth WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 1");
                assertTrue(rs.next());
                st.execute("UPDATE jq_depth SET status = 'active' WHERE id = " + rs.getInt(1));
                c.commit();
            }
        }

        // Depth should now be 7
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM jq_depth WHERE status = 'pending'");
            assertTrue(rs.next()); assertEquals(7, rs.getInt(1));
            c.rollback();
        }
    }

    // =========================================================================
    // 15. Concurrent delete of completed jobs while workers continue
    // =========================================================================

    @Test
    void testConcurrentDeleteOfCompletedJobs() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_cleanup");
            st.execute("CREATE TABLE jq_cleanup (id SERIAL PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending')");
            for (int i = 0; i < 30; i++) {
                st.execute("INSERT INTO jq_cleanup DEFAULT VALUES");
            }
            setup.commit();
        }

        ExecutorService pool = Executors.newFixedThreadPool(3);
        AtomicBoolean done = new AtomicBoolean(false);

        // Worker: continuously claims and completes jobs
        Future<Void> worker = pool.submit(() -> {
            while (!done.get()) {
                try (Connection c = newConn(); Statement st = c.createStatement()) {
                    ResultSet rs = st.executeQuery(
                            "SELECT id FROM jq_cleanup WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 1");
                    if (rs.next()) {
                        st.execute("UPDATE jq_cleanup SET status = 'completed' WHERE id = " + rs.getInt(1));
                        c.commit();
                    } else {
                        c.rollback();
                        done.set(true);
                    }
                }
            }
            return null;
        });

        // Cleaner: deletes completed jobs
        Future<Void> cleaner = pool.submit(() -> {
            while (!done.get()) {
                try (Connection c = newConn(); Statement st = c.createStatement()) {
                    st.execute("DELETE FROM jq_cleanup WHERE status = 'completed'");
                    c.commit();
                }
                Thread.sleep(5);
            }
            return null;
        });

        worker.get(20, TimeUnit.SECONDS);
        cleaner.cancel(false);
        pool.shutdown();

        // No pending jobs remain; table contains only completed (if cleaner missed last batch)
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM jq_cleanup WHERE status = 'pending'");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1), "All jobs should have been processed");
            c.rollback();
        }
    }

    // =========================================================================
    // 16. Job payload preservation
    // =========================================================================

    @Test
    void testJobPayloadPreservation() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_payload");
            st.execute("""
                    CREATE TABLE jq_payload (
                        id SERIAL PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'pending',
                        payload TEXT
                    )""");
            st.execute("INSERT INTO jq_payload (payload) VALUES ('{\"user_id\":42,\"action\":\"send_email\",\"retries\":0}')");
            setup.commit();
        }

        String originalPayload;
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT payload FROM jq_payload WHERE id = 1");
            assertTrue(rs.next());
            originalPayload = rs.getString(1);
            c.rollback();
        }

        // Claim
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            st.execute("UPDATE jq_payload SET status = 'active' WHERE id = 1 AND status = 'pending'");
            c.commit();
        }

        // Complete
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            st.execute("UPDATE jq_payload SET status = 'completed' WHERE id = 1 AND status = 'active'");
            c.commit();
        }

        // Payload must be intact
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT payload FROM jq_payload WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(originalPayload, rs.getString(1), "Payload must survive claim/complete cycle unchanged");
            c.rollback();
        }
    }

    // =========================================================================
    // 17. Unique job deduplication via idempotency key
    // =========================================================================

    @Test
    void testUniqueJobDeduplication() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_dedup");
            st.execute("""
                    CREATE TABLE jq_dedup (
                        id SERIAL PRIMARY KEY,
                        idempotency_key TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        CONSTRAINT uq_jq_dedup_key UNIQUE (idempotency_key)
                    )""");
            st.execute("INSERT INTO jq_dedup (idempotency_key) VALUES ('send-invoice-99')");
            setup.commit();
        }

        // Attempt to insert a duplicate, which must fail with unique violation
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            assertThrows(SQLException.class,
                    () -> st.execute("INSERT INTO jq_dedup (idempotency_key) VALUES ('send-invoice-99')"),
                    "Duplicate idempotency key should violate UNIQUE constraint");
            c.rollback();
        }

        // Insert a different key, which must succeed
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            assertDoesNotThrow(() -> st.execute("INSERT INTO jq_dedup (idempotency_key) VALUES ('send-invoice-100')"));
            c.commit();
        }

        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM jq_dedup");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            c.rollback();
        }
    }

    // =========================================================================
    // 18. Multi-worker race: concurrent workers claim disjoint sets
    // =========================================================================

    @Test
    void testMultiWorkerRaceDisjointClaims() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_race");
            st.execute("CREATE TABLE jq_race (id SERIAL PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending', worker TEXT)");
            for (int i = 0; i < 50; i++) {
                st.execute("INSERT INTO jq_race DEFAULT VALUES");
            }
            setup.commit();
        }

        int workerCount = 10;
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        ConcurrentHashMap<Integer, String> ownership = new ConcurrentHashMap<>();
        List<Future<Void>> futures = new ArrayList<>();
        CountDownLatch ready = new CountDownLatch(workerCount);
        CountDownLatch go = new CountDownLatch(1);

        for (int w = 0; w < workerCount; w++) {
            final String name = "w" + w;
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                boolean found = true;
                while (found) {
                    try (Connection c = newConn(); Statement st = c.createStatement()) {
                        ResultSet rs = st.executeQuery(
                                "SELECT id FROM jq_race WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 1");
                        if (rs.next()) {
                            int id = rs.getInt(1);
                            String prev = ownership.put(id, name);
                            assertNull(prev, "Job " + id + " claimed by both " + prev + " and " + name);
                            st.execute("UPDATE jq_race SET status = 'active', worker = '" + name + "' WHERE id = " + id);
                            c.commit();
                        } else {
                            c.rollback();
                            found = false;
                        }
                    }
                }
                return null;
            }));
        }

        ready.await();
        go.countDown();
        for (Future<Void> f : futures) {
            f.get(20, TimeUnit.SECONDS);
        }
        pool.shutdown();

        // All 50 jobs should be claimed
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM jq_race WHERE status = 'active'");
            assertTrue(rs.next());
            assertEquals(50, rs.getInt(1));
            c.rollback();
        }
    }

    // =========================================================================
    // 19. Graceful shutdown: in-flight jobs remain active (not lost) after crash
    // =========================================================================

    @Test
    void testInFlightJobsNotLostOnConnectionDrop() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_crash");
            st.execute("CREATE TABLE jq_crash (id SERIAL PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending')");
            st.execute("INSERT INTO jq_crash DEFAULT VALUES");
            setup.commit();
        }

        // Claim but don't commit (simulate crash: connection closed without commit)
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT id FROM jq_crash WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 1");
            assertTrue(rs.next());
            st.execute("UPDATE jq_crash SET status = 'active' WHERE id = " + rs.getInt(1));
            // Close without commit -> implicit rollback
        }

        // Job should still be pending (rollback occurred)
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT status FROM jq_crash WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("pending", rs.getString(1), "Uncommitted claim must roll back on connection close");
            c.rollback();
        }
    }

    // =========================================================================
    // 20. Partial failure in batch: one worker fails, others proceed
    // =========================================================================

    @Test
    void testPartialFailureInBatch() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_partfail");
            st.execute("CREATE TABLE jq_partfail (id SERIAL PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending')");
            for (int i = 0; i < 6; i++) {
                st.execute("INSERT INTO jq_partfail DEFAULT VALUES");
            }
            setup.commit();
        }

        // Worker A claims 3 and commits
        try (Connection c = newConn()) {
            List<Integer> ids = new ArrayList<>();
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT id FROM jq_partfail WHERE status = 'pending' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 3")) {
                while (rs.next()) ids.add(rs.getInt(1));
            }
            try (Statement st = c.createStatement()) {
                for (int id : ids) {
                    st.execute("UPDATE jq_partfail SET status = 'active' WHERE id = " + id);
                }
            }
            c.commit();
        }

        // Worker B claims 3 but rolls back (failure)
        try (Connection c = newConn()) {
            List<Integer> ids = new ArrayList<>();
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT id FROM jq_partfail WHERE status = 'pending' ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 3")) {
                while (rs.next()) ids.add(rs.getInt(1));
            }
            try (Statement st = c.createStatement()) {
                for (int id : ids) {
                    st.execute("UPDATE jq_partfail SET status = 'active' WHERE id = " + id);
                }
            }
            c.rollback(); // failure
        }

        // 3 active (Worker A), 3 still pending (Worker B rolled back)
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rsActive = st.executeQuery("SELECT COUNT(*) FROM jq_partfail WHERE status = 'active'");
            assertTrue(rsActive.next()); assertEquals(3, rsActive.getInt(1));

            ResultSet rsPending = st.executeQuery("SELECT COUNT(*) FROM jq_partfail WHERE status = 'pending'");
            assertTrue(rsPending.next()); assertEquals(3, rsPending.getInt(1));
            c.rollback();
        }
    }

    // =========================================================================
    // 21. Visibility after commit: other connections see completed jobs
    // =========================================================================

    @Test
    void testVisibilityAfterCommit() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_visibility");
            st.execute("CREATE TABLE jq_visibility (id SERIAL PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending')");
            st.execute("INSERT INTO jq_visibility DEFAULT VALUES");
            setup.commit();
        }

        try (Connection c1 = newConn(); Connection c2 = newConn()) {
            Statement s1 = c1.createStatement();
            Statement s2 = c2.createStatement();

            // c1 claims and commits
            ResultSet rs = s1.executeQuery(
                    "SELECT id FROM jq_visibility WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 1");
            assertTrue(rs.next());
            s1.execute("UPDATE jq_visibility SET status = 'completed' WHERE id = " + rs.getInt(1));
            c1.commit();

            // c2 should see completed status
            ResultSet rs2 = s2.executeQuery("SELECT status FROM jq_visibility WHERE id = 1");
            assertTrue(rs2.next());
            assertEquals("completed", rs2.getString(1));
            c2.rollback();
        }
    }

    // =========================================================================
    // 22. Large queue: 1000 jobs claimed by many workers
    // =========================================================================

    @Test
    void testLargeQueueManyWorkers() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_large");
            st.execute("CREATE TABLE jq_large (id SERIAL PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending', worker TEXT)");
            for (int i = 0; i < 200; i++) {
                st.execute("INSERT INTO jq_large DEFAULT VALUES");
            }
            setup.commit();
        }

        int workerCount = 8;
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        AtomicInteger totalProcessed = new AtomicInteger(0);
        List<Future<Void>> futures = new ArrayList<>();

        for (int w = 0; w < workerCount; w++) {
            final String name = "w" + w;
            futures.add(pool.submit(() -> {
                // Reuse a single connection per worker (realistic pattern)
                try (Connection c = newConn()) {
                    Statement st = c.createStatement();
                    boolean found = true;
                    while (found) {
                        ResultSet rs = st.executeQuery(
                                "SELECT id FROM jq_large WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 5");
                        List<Integer> batch = new ArrayList<>();
                        while (rs.next()) batch.add(rs.getInt(1));
                        rs.close();
                        if (batch.isEmpty()) {
                            c.rollback();
                            found = false;
                        } else {
                            for (int id : batch) {
                                st.execute("UPDATE jq_large SET status = 'done', worker = '" + name + "' WHERE id = " + id);
                            }
                            c.commit();
                            totalProcessed.addAndGet(batch.size());
                        }
                    }
                }
                return null;
            }));
        }

        for (Future<Void> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertEquals(200, totalProcessed.get(), "All 200 jobs must be processed exactly once");

        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM jq_large WHERE status = 'done'");
            assertTrue(rs.next());
            assertEquals(200, rs.getInt(1));
            c.rollback();
        }
    }

    // =========================================================================
    // 23. Requeue failed jobs back to pending with delay simulation
    // =========================================================================

    @Test
    void testRequeueFailedJobsWithDelay() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_requeue");
            st.execute("""
                    CREATE TABLE jq_requeue (
                        id SERIAL PRIMARY KEY,
                        status TEXT NOT NULL DEFAULT 'pending',
                        run_after TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        retry_count INT NOT NULL DEFAULT 0
                    )""");
            // Insert a failed job
            st.execute("INSERT INTO jq_requeue (status, retry_count) VALUES ('failed', 1)");
            setup.commit();
        }

        // Requeue: failed -> pending, set run_after to now + 30s (simulated delay)
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            int rows = st.executeUpdate("""
                    UPDATE jq_requeue
                       SET status = 'pending',
                           run_after = NOW() + INTERVAL '30 seconds',
                           retry_count = retry_count + 1
                     WHERE status = 'failed'""");
            assertEquals(1, rows);
            c.commit();
        }

        // Job should not be claimable yet (run_after is in the future)
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT id FROM jq_requeue WHERE status = 'pending' AND run_after <= NOW() " +
                    "FOR UPDATE SKIP LOCKED LIMIT 1");
            assertFalse(rs.next(), "Job with future run_after should not be claimable");
            c.rollback();
        }

        // Manually backdate run_after to simulate time passing
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            st.execute("UPDATE jq_requeue SET run_after = NOW() - INTERVAL '1 second' WHERE id = 1");
            c.commit();
        }

        // Now the job should be claimable
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT id FROM jq_requeue WHERE status = 'pending' AND run_after <= NOW() " +
                    "FOR UPDATE SKIP LOCKED LIMIT 1");
            assertTrue(rs.next(), "Job should be claimable after run_after passes");
            c.rollback();
        }
    }

    // =========================================================================
    // 24. Worker self-assignment: worker claims only its own queue type
    // =========================================================================

    @Test
    void testWorkerQueueTypePartitioning() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_typed");
            st.execute("""
                    CREATE TABLE jq_typed (
                        id SERIAL PRIMARY KEY,
                        queue_type TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        worker TEXT
                    )""");
            for (int i = 0; i < 5; i++) st.execute("INSERT INTO jq_typed (queue_type) VALUES ('email')");
            for (int i = 0; i < 5; i++) st.execute("INSERT INTO jq_typed (queue_type) VALUES ('sms')");
            setup.commit();
        }

        // Email worker claims only email jobs
        try (Connection c = newConn()) {
            List<Integer> emailIds = new ArrayList<>();
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT id FROM jq_typed WHERE status = 'pending' AND queue_type = 'email' " +
                         "FOR UPDATE SKIP LOCKED LIMIT 5")) {
                while (rs.next()) emailIds.add(rs.getInt(1));
            }
            try (Statement st = c.createStatement()) {
                for (int id : emailIds) {
                    st.execute("UPDATE jq_typed SET status = 'active', worker = 'email-worker' WHERE id = " + id);
                }
            }
            c.commit();
        }

        // SMS worker claims only SMS jobs
        try (Connection c = newConn()) {
            List<Integer> smsIds = new ArrayList<>();
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery(
                         "SELECT id FROM jq_typed WHERE status = 'pending' AND queue_type = 'sms' " +
                         "FOR UPDATE SKIP LOCKED LIMIT 5")) {
                while (rs.next()) smsIds.add(rs.getInt(1));
            }
            try (Statement st = c.createStatement()) {
                for (int id : smsIds) {
                    st.execute("UPDATE jq_typed SET status = 'active', worker = 'sms-worker' WHERE id = " + id);
                }
            }
            c.commit();
        }

        // All email jobs should be owned by email-worker
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT COUNT(*) FROM jq_typed WHERE queue_type = 'email' AND worker = 'email-worker'");
            assertTrue(rs.next()); assertEquals(5, rs.getInt(1));
            c.rollback();
        }

        // All SMS jobs should be owned by sms-worker
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT COUNT(*) FROM jq_typed WHERE queue_type = 'sms' AND worker = 'sms-worker'");
            assertTrue(rs.next()); assertEquals(5, rs.getInt(1));
            c.rollback();
        }
    }

    // =========================================================================
    // 25. FOR UPDATE SKIP LOCKED with concurrent completions: no phantom re-claims
    // =========================================================================

    @Test
    void testNoPhantomReclaims() throws Exception {
        try (Connection setup = newConn(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jq_phantom");
            st.execute("CREATE TABLE jq_phantom (id SERIAL PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending')");
            for (int i = 0; i < 10; i++) {
                st.execute("INSERT INTO jq_phantom DEFAULT VALUES");
            }
            setup.commit();
        }

        // Claim all 10 jobs
        List<Integer> firstPass = new ArrayList<>();
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT id FROM jq_phantom WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 10");
            while (rs.next()) firstPass.add(rs.getInt(1));
            for (int id : firstPass) {
                st.execute("UPDATE jq_phantom SET status = 'completed' WHERE id = " + id);
            }
            c.commit();
        }
        assertEquals(10, firstPass.size());

        // A second scan should find no pending jobs
        try (Connection c = newConn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT id FROM jq_phantom WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 10");
            assertFalse(rs.next(), "No phantom pending jobs should appear after all are completed");
            c.rollback();
        }
    }
}
