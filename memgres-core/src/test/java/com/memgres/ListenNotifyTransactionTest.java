package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that NOTIFY respects transaction boundaries, matching PG18 behavior:
 * - Notifications are deferred until COMMIT
 * - ROLLBACK discards pending notifications
 * - Outside a transaction (autocommit), notifications are delivered immediately
 */
class ListenNotifyTransactionTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (memgres != null) memgres.close();
    }

    private Connection connect() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private PGNotification[] poll(Connection c) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute("SELECT 1"); }
        return c.unwrap(PGConnection.class).getNotifications();
    }

    @Test
    void notify_in_rolled_back_transaction_is_not_delivered() throws Exception {
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) { s.execute("LISTEN rollback_ch"); }

            // Send NOTIFY inside a transaction, then ROLLBACK
            notifier.setAutoCommit(false);
            try (Statement s = notifier.createStatement()) {
                s.execute("NOTIFY rollback_ch, 'should not arrive'");
            }
            notifier.rollback();
            notifier.setAutoCommit(true);

            PGNotification[] notes = poll(listener);
            assertTrue(notes == null || notes.length == 0,
                    "Notifications from rolled-back transaction must NOT be delivered");
        }
    }

    @Test
    void notify_in_committed_transaction_is_delivered() throws Exception {
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) { s.execute("LISTEN commit_ch"); }

            // Send NOTIFY inside a transaction, then COMMIT
            notifier.setAutoCommit(false);
            try (Statement s = notifier.createStatement()) {
                s.execute("NOTIFY commit_ch, 'committed msg'");
            }
            notifier.commit();
            notifier.setAutoCommit(true);

            PGNotification[] notes = poll(listener);
            assertNotNull(notes, "Should receive notification after COMMIT");
            assertEquals(1, notes.length);
            assertEquals("committed msg", notes[0].getParameter());
        }
    }

    @Test
    void notify_not_visible_before_commit() throws Exception {
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) { s.execute("LISTEN deferred_ch"); }

            notifier.setAutoCommit(false);
            try (Statement s = notifier.createStatement()) {
                s.execute("NOTIFY deferred_ch, 'deferred'");
            }

            // Poll BEFORE commit; should NOT see notification yet
            PGNotification[] notesBefore = poll(listener);
            assertTrue(notesBefore == null || notesBefore.length == 0,
                    "Notification must NOT be visible before COMMIT");

            // Now commit
            notifier.commit();
            notifier.setAutoCommit(true);

            // Poll AFTER commit; should see it now
            PGNotification[] notesAfter = poll(listener);
            assertNotNull(notesAfter);
            assertEquals(1, notesAfter.length);
            assertEquals("deferred", notesAfter[0].getParameter());
        }
    }

    @Test
    void multiple_notifies_in_transaction_all_delivered_on_commit() throws Exception {
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) { s.execute("LISTEN batch_ch"); }

            notifier.setAutoCommit(false);
            try (Statement s = notifier.createStatement()) {
                s.execute("NOTIFY batch_ch, 'msg1'");
                s.execute("NOTIFY batch_ch, 'msg2'");
                s.execute("NOTIFY batch_ch, 'msg3'");
            }
            notifier.commit();
            notifier.setAutoCommit(true);

            PGNotification[] notes = poll(listener);
            assertNotNull(notes);
            assertEquals(3, notes.length, "All 3 notifications should arrive after COMMIT");
            assertEquals("msg1", notes[0].getParameter());
            assertEquals("msg2", notes[1].getParameter());
            assertEquals("msg3", notes[2].getParameter());
        }
    }

    @Test
    void pg_notify_in_rolled_back_transaction_is_not_delivered() throws Exception {
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) { s.execute("LISTEN fn_rollback_ch"); }

            notifier.setAutoCommit(false);
            try (Statement s = notifier.createStatement()) {
                s.execute("SELECT pg_notify('fn_rollback_ch', 'should not arrive')");
            }
            notifier.rollback();
            notifier.setAutoCommit(true);

            PGNotification[] notes = poll(listener);
            assertTrue(notes == null || notes.length == 0,
                    "pg_notify() in rolled-back tx must NOT deliver");
        }
    }

    @Test
    void notify_in_autocommit_delivered_immediately() throws Exception {
        // Outside a transaction (autocommit=true), notifications are immediate
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) { s.execute("LISTEN auto_ch"); }

            // notifier is autocommit=true (default)
            try (Statement s = notifier.createStatement()) {
                s.execute("NOTIFY auto_ch, 'immediate'");
            }

            PGNotification[] notes = poll(listener);
            assertNotNull(notes);
            assertEquals(1, notes.length);
            assertEquals("immediate", notes[0].getParameter());
        }
    }

    @Test
    void savepoint_rollback_discards_notifications_after_savepoint() throws Exception {
        // In PG: ROLLBACK TO SAVEPOINT discards notifications issued after the savepoint
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) { s.execute("LISTEN sp_ch"); }

            notifier.setAutoCommit(false);
            try (Statement s = notifier.createStatement()) {
                s.execute("NOTIFY sp_ch, 'before savepoint'");
                s.execute("SAVEPOINT sp1");
                s.execute("NOTIFY sp_ch, 'after savepoint'");
                s.execute("ROLLBACK TO SAVEPOINT sp1");
            }
            notifier.commit();
            notifier.setAutoCommit(true);

            PGNotification[] notes = poll(listener);
            assertNotNull(notes);
            // Only 'before savepoint' should arrive because 'after savepoint' was rolled back
            assertEquals(1, notes.length, "Only notification before savepoint should survive");
            assertEquals("before savepoint", notes[0].getParameter());
        }
    }
}
