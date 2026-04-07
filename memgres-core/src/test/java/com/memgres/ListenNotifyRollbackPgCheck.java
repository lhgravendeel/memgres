package com.memgres;

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import java.sql.*;

/**
 * Check PG behavior: does NOTIFY in a rolled-back transaction deliver?
 * NOT a test. Run manually against real PG.
 */
public class ListenNotifyRollbackPgCheck {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:postgresql://localhost:5432/memgrestest";
        try (Connection listener = DriverManager.getConnection(url, "memgres", "memgres");
             Connection notifier = DriverManager.getConnection(url, "memgres", "memgres")) {
            listener.setAutoCommit(true);
            notifier.setAutoCommit(false);

            // Listener subscribes
            try (Statement s = listener.createStatement()) { s.execute("LISTEN rollback_ch"); }

            // Notifier sends in transaction then ROLLBACK
            try (Statement s = notifier.createStatement()) {
                s.execute("NOTIFY rollback_ch, 'rolled back msg'");
            }
            notifier.rollback();

            // Check if listener got it
            try (Statement s = listener.createStatement()) { s.execute("SELECT 1"); }
            PGNotification[] notes = listener.unwrap(PGConnection.class).getNotifications();
            if (notes == null || notes.length == 0) {
                System.out.println("PG CONFIRMED: NOTIFY in rolled-back transaction is NOT delivered");
            } else {
                System.out.println("UNEXPECTED: PG delivered notification from rolled-back tx: " + notes[0].getParameter());
            }

            // Now test COMMIT
            try (Statement s = notifier.createStatement()) {
                s.execute("NOTIFY rollback_ch, 'committed msg'");
            }
            notifier.commit();

            try (Statement s = listener.createStatement()) { s.execute("SELECT 1"); }
            notes = listener.unwrap(PGConnection.class).getNotifications();
            if (notes != null && notes.length > 0) {
                System.out.println("PG CONFIRMED: NOTIFY in committed transaction IS delivered: " + notes[0].getParameter());
            } else {
                System.out.println("UNEXPECTED: PG did NOT deliver notification from committed tx");
            }
        }
    }
}
