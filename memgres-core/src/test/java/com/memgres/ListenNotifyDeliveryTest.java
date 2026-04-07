package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests actual PgWire notification delivery via the PG JDBC driver's
 * getNotifications() API. Verifies end-to-end: NOTIFY → PgWire → JDBC.
 */
class ListenNotifyDeliveryTest {

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
        // Issue a dummy query to trigger ReadyForQuery which delivers pending notifications
        try (Statement s = c.createStatement()) { s.execute("SELECT 1"); }
        return c.unwrap(PGConnection.class).getNotifications();
    }

    // ========================================================================
    // Cross-connection delivery
    // ========================================================================

    @Test
    void notify_delivered_to_listener() throws Exception {
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) { s.execute("LISTEN delivery_ch"); }
            try (Statement s = notifier.createStatement()) { s.execute("NOTIFY delivery_ch, 'hello'"); }

            PGNotification[] notes = poll(listener);
            assertNotNull(notes, "Should receive notifications");
            assertTrue(notes.length > 0, "Should have at least one notification");
            assertEquals("delivery_ch", notes[0].getName());
            assertEquals("hello", notes[0].getParameter());
        }
    }

    @Test
    void self_notification() throws Exception {
        // PG delivers notifications to the sender too if they're listening
        try (Connection c = connect()) {
            try (Statement s = c.createStatement()) {
                s.execute("LISTEN self_ch");
                s.execute("NOTIFY self_ch, 'self'");
            }

            PGNotification[] notes = poll(c);
            assertNotNull(notes);
            assertTrue(notes.length > 0, "Should receive self-notification");
            assertEquals("self", notes[0].getParameter());
        }
    }

    @Test
    void unlisten_stops_delivery() throws Exception {
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) {
                s.execute("LISTEN stop_ch");
                s.execute("UNLISTEN stop_ch");
            }
            try (Statement s = notifier.createStatement()) {
                s.execute("NOTIFY stop_ch, 'should not arrive'");
            }

            PGNotification[] notes = poll(listener);
            assertTrue(notes == null || notes.length == 0,
                    "Should NOT receive notification after UNLISTEN");
        }
    }

    @Test
    void unlisten_all_stops_all_channels() throws Exception {
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) {
                s.execute("LISTEN ch_a");
                s.execute("LISTEN ch_b");
                s.execute("UNLISTEN *");
            }
            try (Statement s = notifier.createStatement()) {
                s.execute("NOTIFY ch_a, 'a'");
                s.execute("NOTIFY ch_b, 'b'");
            }

            PGNotification[] notes = poll(listener);
            assertTrue(notes == null || notes.length == 0,
                    "Should NOT receive notifications after UNLISTEN *");
        }
    }

    @Test
    void multiple_listeners_all_receive() throws Exception {
        try (Connection l1 = connect(); Connection l2 = connect(); Connection notifier = connect()) {
            try (Statement s = l1.createStatement()) { s.execute("LISTEN multi_ch"); }
            try (Statement s = l2.createStatement()) { s.execute("LISTEN multi_ch"); }
            try (Statement s = notifier.createStatement()) { s.execute("NOTIFY multi_ch, 'broadcast'"); }

            PGNotification[] n1 = poll(l1);
            PGNotification[] n2 = poll(l2);
            assertNotNull(n1); assertTrue(n1.length > 0, "l1 should receive");
            assertNotNull(n2); assertTrue(n2.length > 0, "l2 should receive");
            assertEquals("broadcast", n1[0].getParameter());
            assertEquals("broadcast", n2[0].getParameter());
        }
    }

    @Test
    void notify_with_empty_payload() throws Exception {
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) { s.execute("LISTEN empty_ch"); }
            try (Statement s = notifier.createStatement()) { s.execute("NOTIFY empty_ch"); }

            PGNotification[] notes = poll(listener);
            assertNotNull(notes);
            assertTrue(notes.length > 0);
            assertEquals("", notes[0].getParameter());
        }
    }

    @Test
    void pg_notify_function_delivers() throws Exception {
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) { s.execute("LISTEN fn_ch"); }
            try (Statement s = notifier.createStatement()) {
                s.execute("SELECT pg_notify('fn_ch', 'from function')");
            }

            PGNotification[] notes = poll(listener);
            assertNotNull(notes);
            assertTrue(notes.length > 0, "pg_notify() should deliver");
            assertEquals("fn_ch", notes[0].getName());
            assertEquals("from function", notes[0].getParameter());
        }
    }

    @Test
    void multiple_notifications_queued() throws Exception {
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) { s.execute("LISTEN queue_ch"); }
            try (Statement s = notifier.createStatement()) {
                s.execute("NOTIFY queue_ch, 'msg1'");
                s.execute("NOTIFY queue_ch, 'msg2'");
                s.execute("NOTIFY queue_ch, 'msg3'");
            }

            PGNotification[] notes = poll(listener);
            assertNotNull(notes);
            assertEquals(3, notes.length, "Should receive all 3 notifications");
            assertEquals("msg1", notes[0].getParameter());
            assertEquals("msg2", notes[1].getParameter());
            assertEquals("msg3", notes[2].getParameter());
        }
    }

    @Test
    void notification_has_pid() throws Exception {
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) { s.execute("LISTEN pid_ch"); }
            try (Statement s = notifier.createStatement()) { s.execute("NOTIFY pid_ch, 'test'"); }

            PGNotification[] notes = poll(listener);
            assertNotNull(notes);
            assertTrue(notes.length > 0);
            // PID should be non-zero
            assertTrue(notes[0].getPID() != 0, "PID should be non-zero");
        }
    }

    @Test
    void channel_name_case_insensitive() throws Exception {
        try (Connection listener = connect(); Connection notifier = connect()) {
            try (Statement s = listener.createStatement()) { s.execute("LISTEN MyChannel"); }
            try (Statement s = notifier.createStatement()) { s.execute("NOTIFY mychannel, 'case test'"); }

            PGNotification[] notes = poll(listener);
            assertNotNull(notes);
            assertTrue(notes.length > 0, "Case-insensitive channel should match");
        }
    }

    @Test
    void connection_close_removes_listener() throws Exception {
        Connection temp = connect();
        try (Statement s = temp.createStatement()) { s.execute("LISTEN temp_close_ch"); }
        temp.close(); // Should remove listener

        try (Connection notifier = connect()) {
            try (Statement s = notifier.createStatement()) {
                s.execute("NOTIFY temp_close_ch, 'no one listening'");
            }
            // No error should occur even though no one is listening
        }
    }
}
