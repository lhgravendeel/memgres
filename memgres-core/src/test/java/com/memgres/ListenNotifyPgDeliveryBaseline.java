package com.memgres;

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

import java.sql.*;

/**
 * Verifies LISTEN/NOTIFY delivery against real PG18.
 * NOT a test. Run manually.
 *
 * Usage:
 *   mvn -pl memgres-core exec:java \
 *     -Dexec.mainClass="com.memgres.ListenNotifyPgDeliveryBaseline" \
 *     -Dexec.classpathScope=test
 */
public class ListenNotifyPgDeliveryBaseline {

    private static final String PG_URL = "jdbc:postgresql://localhost:5432/memgrestest";
    private static final String PG_USER = "memgres";
    private static final String PG_PASS = "memgres";

    public static void main(String[] args) throws Exception {
        System.out.println("=== PG18 LISTEN/NOTIFY Delivery Baseline ===\n");

        // Test 1: cross-connection delivery
        try (Connection listener = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Connection notifier = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            listener.setAutoCommit(true);
            notifier.setAutoCommit(true);

            exec(listener, "LISTEN test_ch");
            exec(notifier, "NOTIFY test_ch, 'hello'");
            exec(listener, "SELECT 1"); // trigger delivery

            PGNotification[] notes = listener.unwrap(PGConnection.class).getNotifications();
            report("Cross-conn delivery", notes, "hello");
        }

        // Test 2: self-notification
        try (Connection c = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            c.setAutoCommit(true);
            exec(c, "LISTEN self_ch");
            exec(c, "NOTIFY self_ch, 'self'");
            exec(c, "SELECT 1");

            PGNotification[] notes = c.unwrap(PGConnection.class).getNotifications();
            report("Self-notification", notes, "self");
        }

        // Test 3: UNLISTEN stops delivery
        try (Connection listener = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Connection notifier = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            listener.setAutoCommit(true);
            notifier.setAutoCommit(true);

            exec(listener, "LISTEN stop_ch");
            exec(listener, "UNLISTEN stop_ch");
            exec(notifier, "NOTIFY stop_ch, 'should not arrive'");
            exec(listener, "SELECT 1");

            PGNotification[] notes = listener.unwrap(PGConnection.class).getNotifications();
            if (notes == null || notes.length == 0)
                System.out.println("  OK: UNLISTEN stops delivery (no notifications)");
            else
                System.out.println("  FAIL: Got " + notes.length + " notifications after UNLISTEN");
        }

        // Test 4: pg_notify function
        try (Connection listener = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Connection notifier = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            listener.setAutoCommit(true);
            notifier.setAutoCommit(true);

            exec(listener, "LISTEN fn_ch");
            exec(notifier, "SELECT pg_notify('fn_ch', 'from function')");
            exec(listener, "SELECT 1");

            PGNotification[] notes = listener.unwrap(PGConnection.class).getNotifications();
            report("pg_notify() delivery", notes, "from function");
        }

        // Test 5: multiple queued
        try (Connection listener = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Connection notifier = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            listener.setAutoCommit(true);
            notifier.setAutoCommit(true);

            exec(listener, "LISTEN q_ch");
            exec(notifier, "NOTIFY q_ch, 'msg1'");
            exec(notifier, "NOTIFY q_ch, 'msg2'");
            exec(notifier, "NOTIFY q_ch, 'msg3'");
            exec(listener, "SELECT 1");

            PGNotification[] notes = listener.unwrap(PGConnection.class).getNotifications();
            System.out.println("  Multiple queued: got " + (notes != null ? notes.length : 0) + " notifications");
            if (notes != null) {
                for (PGNotification n : notes) {
                    System.out.println("    payload=" + n.getParameter() + " pid=" + n.getPID());
                }
            }
        }

        // Test 6: empty payload (NOTIFY without payload)
        try (Connection listener = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Connection notifier = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            listener.setAutoCommit(true);
            notifier.setAutoCommit(true);

            exec(listener, "LISTEN empty_ch");
            exec(notifier, "NOTIFY empty_ch");
            exec(listener, "SELECT 1");

            PGNotification[] notes = listener.unwrap(PGConnection.class).getNotifications();
            if (notes != null && notes.length > 0) {
                System.out.println("  Empty payload: '" + notes[0].getParameter() + "' (length=" + notes[0].getParameter().length() + ")");
            } else {
                System.out.println("  FAIL: No notification for empty payload");
            }
        }

        System.out.println("\n=== Done ===");
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    private static void report(String test, PGNotification[] notes, String expectedPayload) {
        if (notes != null && notes.length > 0) {
            boolean match = expectedPayload.equals(notes[0].getParameter());
            System.out.println("  " + (match ? "OK" : "MISMATCH") + ": " + test +
                    ", channel=" + notes[0].getName() + " payload='" + notes[0].getParameter() +
                    "' pid=" + notes[0].getPID());
        } else {
            System.out.println("  FAIL: " + test + " (no notifications received)");
        }
    }
}
