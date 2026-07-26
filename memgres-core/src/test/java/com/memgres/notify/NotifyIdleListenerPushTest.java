package com.memgres.notify;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A listener sitting idle on the socket has to receive a NOTIFY without issuing another
 * statement — that is the canonical LISTEN/NOTIFY worker pattern. Expectations match a live
 * PostgreSQL 18.0 server, where getNotifications(timeout) returns as soon as one arrives.
 *
 * <p>N17 NOTIFY is never pushed to idle listeners.
 */
class NotifyIdleListenerPushTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() {
        if (memgres != null) memgres.close();
    }

    private static Connection open() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    @Test
    void anIdleListenerReceivesANotifyWithoutIssuingAStatement() throws Exception {
        try (Connection listener = open(); Connection notifier = open()) {
            exec(listener, "LISTEN idle_chan");

            // The listener does nothing further; only the notifier acts
            exec(notifier, "NOTIFY idle_chan, 'hello'");

            PGNotification[] got = listener.unwrap(PGConnection.class).getNotifications(5000);
            assertNotNull(got, "an idle listener must be woken by the NOTIFY");
            assertEquals(1, got.length);
            assertEquals("idle_chan", got[0].getName());
            assertEquals("hello", got[0].getParameter());
        }
    }

    @Test
    void aNotifyArrivingWhileBlockedIsDelivered() throws Exception {
        try (Connection listener = open(); Connection notifier = open()) {
            exec(listener, "LISTEN slow_chan");

            Thread t = new Thread(() -> {
                try {
                    Thread.sleep(200);
                    exec(notifier, "NOTIFY slow_chan, 'late'");
                } catch (Exception ignored) {
                    // the assertion below reports the failure
                }
            });
            t.start();

            long start = System.currentTimeMillis();
            PGNotification[] got = listener.unwrap(PGConnection.class).getNotifications(5000);
            long waited = System.currentTimeMillis() - start;
            t.join(5000);

            assertNotNull(got, "the blocked listener must wake when the NOTIFY is sent");
            assertEquals("late", got[0].getParameter());
            assertTrue(waited < 4000, "it should return when the notify arrives, not on timeout");
        }
    }

    /** A notification still reaches a listener that does issue another statement. */
    @Test
    void pollingAfterAStatementStillWorks() throws Exception {
        try (Connection listener = open(); Connection notifier = open()) {
            exec(listener, "LISTEN poll_chan");
            exec(notifier, "NOTIFY poll_chan, 'p'");
            exec(listener, "SELECT 1");

            PGNotification[] got = listener.unwrap(PGConnection.class).getNotifications(5000);
            assertNotNull(got);
            assertEquals("p", got[0].getParameter());
        }
    }

    @Test
    void aNotifyOnAnotherChannelIsNotDelivered() throws Exception {
        try (Connection listener = open(); Connection notifier = open()) {
            exec(listener, "LISTEN mine");
            exec(notifier, "NOTIFY yours, 'x'");

            PGNotification[] got = listener.unwrap(PGConnection.class).getNotifications(300);
            assertTrue(got == null || got.length == 0, "only the listened channel delivers");
        }
    }
}
