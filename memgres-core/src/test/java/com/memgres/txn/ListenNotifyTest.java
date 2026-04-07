package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document section 14 (Java/JDBC): LISTEN/NOTIFY channel messaging.
 * Tests LISTEN, NOTIFY (with and without payload), UNLISTEN,
 * UNLISTEN *, transaction semantics, idempotency, special characters,
 * and autocommit delivery.
 */
class ListenNotifyTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    PGNotification[] poll(Connection c) throws SQLException {
        // Issue a dummy query to trigger ReadyForQuery which delivers pending notifications
        try (Statement s = c.createStatement()) { s.execute("SELECT 1"); }
        return c.unwrap(PGConnection.class).getNotifications();
    }

    // --- 1. LISTEN channel_name succeeds ---

    @Test void listen_channel_succeeds() throws Exception {
        try (Connection c = newConn()) {
            assertDoesNotThrow(() -> exec(c, "LISTEN ln_basic"));
        }
    }

    // --- 2. NOTIFY channel_name succeeds ---

    @Test void notify_channel_succeeds() throws Exception {
        try (Connection c = newConn()) {
            assertDoesNotThrow(() -> exec(c, "NOTIFY ln_notify_basic"));
        }
    }

    // --- 3. NOTIFY with payload ---

    @Test void notify_with_payload_delivered() throws Exception {
        try (Connection listener = newConn(); Connection notifier = newConn()) {
            exec(listener, "LISTEN ln_payload");
            exec(notifier, "NOTIFY ln_payload, 'hello world'");

            PGNotification[] notes = poll(listener);
            assertNotNull(notes, "Should receive notifications");
            assertEquals(1, notes.length);
            assertEquals("ln_payload", notes[0].getName());
            assertEquals("hello world", notes[0].getParameter());
        }
    }

    // --- 4. UNLISTEN channel_name ---

    @Test void unlisten_stops_delivery() throws Exception {
        try (Connection listener = newConn(); Connection notifier = newConn()) {
            exec(listener, "LISTEN ln_unlisten");
            exec(listener, "UNLISTEN ln_unlisten");
            exec(notifier, "NOTIFY ln_unlisten, 'should not arrive'");

            PGNotification[] notes = poll(listener);
            assertTrue(notes == null || notes.length == 0,
                    "Should NOT receive notification after UNLISTEN");
        }
    }

    // --- 5. UNLISTEN * (all channels) ---

    @Test void unlisten_all_stops_all_channels() throws Exception {
        try (Connection listener = newConn(); Connection notifier = newConn()) {
            exec(listener, "LISTEN ln_star_a");
            exec(listener, "LISTEN ln_star_b");
            exec(listener, "UNLISTEN *");
            exec(notifier, "NOTIFY ln_star_a, 'a'");
            exec(notifier, "NOTIFY ln_star_b, 'b'");

            PGNotification[] notes = poll(listener);
            assertTrue(notes == null || notes.length == 0,
                    "Should NOT receive notifications after UNLISTEN *");
        }
    }

    // --- 6. NOTIFY on channel with no listeners (no error) ---

    @Test void notify_no_listeners_succeeds() throws Exception {
        try (Connection c = newConn()) {
            assertDoesNotThrow(() -> exec(c, "NOTIFY ln_nobody_listening, 'void'"));
        }
    }

    // --- 7. LISTEN same channel twice (idempotent) ---

    @Test void listen_same_channel_twice_is_idempotent() throws Exception {
        try (Connection listener = newConn(); Connection notifier = newConn()) {
            exec(listener, "LISTEN ln_idempotent");
            exec(listener, "LISTEN ln_idempotent");
            exec(notifier, "NOTIFY ln_idempotent, 'once'");

            PGNotification[] notes = poll(listener);
            assertNotNull(notes, "Should receive notification");
            assertEquals(1, notes.length, "Duplicate LISTEN should not cause duplicate delivery");
            assertEquals("once", notes[0].getParameter());
        }
    }

    // --- 8. NOTIFY inside transaction (delivered on commit) ---

    @Test void notify_inside_transaction_delivered_on_commit() throws Exception {
        try (Connection listener = newConn(); Connection notifier = newConn()) {
            exec(listener, "LISTEN ln_tx_commit");

            notifier.setAutoCommit(false);
            exec(notifier, "NOTIFY ln_tx_commit, 'committed'");

            // Poll before commit: should not see notification
            PGNotification[] beforeCommit = poll(listener);
            assertTrue(beforeCommit == null || beforeCommit.length == 0,
                    "Notification must NOT be visible before COMMIT");

            notifier.commit();
            notifier.setAutoCommit(true);

            // Poll after commit: should see notification
            PGNotification[] afterCommit = poll(listener);
            assertNotNull(afterCommit, "Should receive notification after COMMIT");
            assertEquals(1, afterCommit.length);
            assertEquals("ln_tx_commit", afterCommit[0].getName());
            assertEquals("committed", afterCommit[0].getParameter());
        }
    }

    // --- 9. NOTIFY inside rolled-back transaction (not delivered) ---

    @Test void notify_inside_rolled_back_transaction_not_delivered() throws Exception {
        try (Connection listener = newConn(); Connection notifier = newConn()) {
            exec(listener, "LISTEN ln_tx_rollback");

            notifier.setAutoCommit(false);
            exec(notifier, "NOTIFY ln_tx_rollback, 'should vanish'");
            notifier.rollback();
            notifier.setAutoCommit(true);

            PGNotification[] notes = poll(listener);
            assertTrue(notes == null || notes.length == 0,
                    "Notifications from rolled-back transaction must NOT be delivered");
        }
    }

    // --- 10. Channel name with special characters (quoted identifier) ---

    @Test void channel_name_with_special_characters() throws Exception {
        try (Connection listener = newConn(); Connection notifier = newConn()) {
            exec(listener, "LISTEN \"ln_special-ch.name\"");
            exec(notifier, "NOTIFY \"ln_special-ch.name\", 'special'");

            PGNotification[] notes = poll(listener);
            assertNotNull(notes, "Should receive notification on quoted channel");
            assertEquals(1, notes.length);
            assertEquals("ln_special-ch.name", notes[0].getName());
            assertEquals("special", notes[0].getParameter());
        }
    }

    // --- 11. Empty payload ---

    @Test void notify_with_empty_payload() throws Exception {
        try (Connection listener = newConn(); Connection notifier = newConn()) {
            exec(listener, "LISTEN ln_empty_payload");
            exec(notifier, "NOTIFY ln_empty_payload");

            PGNotification[] notes = poll(listener);
            assertNotNull(notes, "Should receive notification with empty payload");
            assertEquals(1, notes.length);
            assertEquals("ln_empty_payload", notes[0].getName());
            assertEquals("", notes[0].getParameter());
        }
    }

    // --- 12. LISTEN/NOTIFY in autocommit mode ---

    @Test void listen_notify_in_autocommit_mode() throws Exception {
        try (Connection listener = newConn(); Connection notifier = newConn()) {
            // Both connections are in autocommit mode (default from newConn)
            assertTrue(listener.getAutoCommit(), "Listener should be in autocommit mode");
            assertTrue(notifier.getAutoCommit(), "Notifier should be in autocommit mode");

            exec(listener, "LISTEN ln_autocommit");
            exec(notifier, "NOTIFY ln_autocommit, 'immediate'");

            PGNotification[] notes = poll(listener);
            assertNotNull(notes, "Autocommit NOTIFY should deliver immediately");
            assertEquals(1, notes.length);
            assertEquals("ln_autocommit", notes[0].getName());
            assertEquals("immediate", notes[0].getParameter());
        }
    }
}
