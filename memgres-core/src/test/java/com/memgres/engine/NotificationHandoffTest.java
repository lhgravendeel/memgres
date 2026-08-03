package com.memgres.engine;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * What a session does with a notification between queueing it and writing it.
 *
 * <p>Two things write the queue: a push to a listener sitting idle on its socket, and the drain the
 * listener's own next statement does before its ReadyForQuery. The push is a write scheduled onto
 * another connection's event loop, so it does not happen when it is asked for — and a notification
 * taken off the queue for a write that has not happened yet is in neither place. A listener whose
 * statement drained the queue in that moment found nothing, and read the notification only after
 * its own ReadyForQuery, which is one statement too late for a client that asks for its
 * notifications when that statement returns.
 *
 * <p>So the rule is that the queue keeps a notification until something has actually written it.
 * The tests below hold the queue at exactly that moment, which is what no end-to-end test can do:
 * whether the window is hit is decided by which thread ran first.
 */
class NotificationHandoffTest {

    private static Session session() {
        return new Session(new Database());
    }

    /** Asking for a write is not performing one, so the notification is still there to be written. */
    @Test
    void aNotificationStaysQueuedUntilItIsWritten() {
        Session session = session();
        AtomicInteger asked = new AtomicInteger();
        // A flusher that does nothing stands for the scheduled write that has not run yet.
        session.setNotificationSink(asked::incrementAndGet);

        session.addNotification(new Notification(1, "ch", "payload"));

        assertEquals(1, asked.get(), "the connection should have been asked to write");
        assertEquals(1, session.getPendingNotifications().size(),
                "and the notification should still be there for whoever writes it");
        Notification queued = session.getPendingNotifications().poll();
        assertNotNull(queued);
        assertEquals("ch", queued.channel());
        assertEquals("payload", queued.payload());
    }

    /** Whichever of the two writes first takes it, and the other finds nothing left to write. */
    @Test
    void whicheverWritesFirstTakesIt() {
        Session session = session();
        session.setNotificationSink(() -> {
            // Stands for the push actually running: it takes what is there.
            while (session.getPendingNotifications().poll() != null) {
                // written
            }
        });

        session.addNotification(new Notification(1, "ch", "one"));

        assertEquals(0, session.getPendingNotifications().size(),
                "the push wrote it, so the statement's drain has nothing to write");
    }

    /** With no connection to push to, the queue holds everything for the next drain. */
    @Test
    void withNoConnectionToPushToTheQueueHoldsThem() {
        Session session = session();
        session.addNotification(new Notification(1, "ch", "one"));
        session.addNotification(new Notification(1, "ch", "two"));
        assertEquals(2, session.getPendingNotifications().size());
    }
}
