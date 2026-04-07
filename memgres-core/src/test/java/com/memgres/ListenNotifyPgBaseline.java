package com.memgres;

import java.sql.*;
import java.util.*;

/**
 * Collects PG18 baseline output for LISTEN/NOTIFY and work queue patterns.
 * NOT a test. Run manually against a real PostgreSQL instance.
 *
 * Usage:
 *   mvn -pl memgres-core exec:java \
 *     -Dexec.mainClass="com.memgres.ListenNotifyPgBaseline" \
 *     -Dexec.classpathScope=test
 */
public class ListenNotifyPgBaseline {

    private static final String PG_URL = "jdbc:postgresql://localhost:5432/memgrestest";
    private static final String PG_USER = "memgres";
    private static final String PG_PASS = "memgres";

    public static void main(String[] args) throws Exception {
        System.out.println("=== LISTEN/NOTIFY PG18 Baseline ===\n");

        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            conn.setAutoCommit(true);
            System.out.println("Connected to: " + conn.getMetaData().getDatabaseProductVersion());

            runSection(conn, "LISTEN/NOTIFY Syntax", new String[]{
                "LISTEN my_channel",
                "NOTIFY my_channel",
                "NOTIFY my_channel, 'hello world'",
                "NOTIFY my_channel, ''",
                "UNLISTEN my_channel",
                "UNLISTEN *",
                "UNLISTEN never_listened_channel",
                "LISTEN dup_ch",
                "LISTEN dup_ch",
                "UNLISTEN dup_ch",
            });

            runSection(conn, "pg_notify()", new String[]{
                "SELECT pg_notify('test_channel', 'payload')",
                "SELECT pg_notify('ch', '')",
                "SELECT pg_notify('ch', NULL)",
            });

            runSectionExpectError(conn, "pg_notify errors", new String[]{
                "SELECT pg_notify('ch')",              // too few args
                "NOTIFY",                               // missing channel
                "LISTEN",                               // missing channel
            });

            // Work queue setup
            exec(conn, "DROP TABLE IF EXISTS wq CASCADE");
            exec(conn, "CREATE TABLE wq (id SERIAL PRIMARY KEY, payload TEXT)");
            exec(conn, "INSERT INTO wq (payload) VALUES ('j1'), ('j2'), ('j3')");

            runSection(conn, "Work Queue CTE Pattern", new String[]{
                """
                WITH job AS (
                    SELECT id, payload FROM wq ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1
                )
                DELETE FROM wq WHERE id IN (SELECT id FROM job) RETURNING *
                """,
                "SELECT count(*) FROM wq",
            });

            exec(conn, "DROP TABLE wq CASCADE");

            // FOR UPDATE variants
            exec(conn, "DROP TABLE IF EXISTS ft CASCADE");
            exec(conn, "CREATE TABLE ft (id INT PRIMARY KEY, val TEXT)");
            exec(conn, "INSERT INTO ft VALUES (1, 'a'), (2, 'b')");

            runSection(conn, "FOR UPDATE Variants", new String[]{
                "SELECT * FROM ft FOR UPDATE",
                "SELECT * FROM ft FOR SHARE",
                "SELECT * FROM ft FOR NO KEY UPDATE",
                "SELECT * FROM ft FOR KEY SHARE",
                "SELECT * FROM ft ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1",
                "SELECT * FROM ft FOR UPDATE NOWAIT",
            });

            exec(conn, "DROP TABLE ft CASCADE");

            // LOCK TABLE
            exec(conn, "DROP TABLE IF EXISTS lt CASCADE");
            exec(conn, "CREATE TABLE lt (id INT)");
            runSection(conn, "LOCK TABLE", new String[]{
                "BEGIN",
                "LOCK TABLE lt IN ACCESS SHARE MODE",
                "COMMIT",
                "BEGIN",
                "LOCK TABLE lt IN EXCLUSIVE MODE",
                "COMMIT",
                "BEGIN",
                "LOCK TABLE lt IN ACCESS EXCLUSIVE MODE NOWAIT",
                "COMMIT",
            });
            exec(conn, "DROP TABLE lt CASCADE");

            // Transaction + LISTEN
            runSection(conn, "LISTEN in Transaction", new String[]{
                "BEGIN",
                "LISTEN tx_ch",
                "COMMIT",
                "UNLISTEN tx_ch",
            });

            // CTE + DELETE + JOIN
            exec(conn, "DROP TABLE IF EXISTS orders CASCADE");
            exec(conn, "DROP TABLE IF EXISTS customers CASCADE");
            exec(conn, "CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT)");
            exec(conn, "CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INT, total NUMERIC)");
            exec(conn, "INSERT INTO customers (name) VALUES ('Alice'), ('Bob')");
            exec(conn, "INSERT INTO orders (customer_id, total) VALUES (1, 100.00), (1, 200.00), (2, 50.00)");

            runSection(conn, "CTE DELETE RETURNING with JOIN", new String[]{
                """
                WITH deleted AS (
                    DELETE FROM orders WHERE customer_id = 1 RETURNING id, customer_id, total
                )
                SELECT d.id, c.name, d.total FROM deleted d JOIN customers c ON d.customer_id = c.id ORDER BY d.id
                """,
                "SELECT count(*) FROM orders",
            });

            exec(conn, "DROP TABLE orders CASCADE");
            exec(conn, "DROP TABLE customers CASCADE");

            System.out.println("\n=== Baseline collection complete ===");
        }
    }

    private static void exec(Connection conn, String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static void runSection(Connection conn, String sectionName, String[] sqls) {
        System.out.println("\n--- " + sectionName + " ---");
        for (String sql : sqls) {
            runSql(conn, sql.trim());
        }
    }

    private static void runSectionExpectError(Connection conn, String sectionName, String[] sqls) {
        System.out.println("\n--- " + sectionName + " (expected errors) ---");
        for (String sql : sqls) {
            runSql(conn, sql.trim());
        }
    }

    private static void runSql(Connection conn, String sql) {
        String display = sql.replaceAll("\\s+", " ").trim();
        if (display.length() > 80) display = display.substring(0, 80) + "...";
        try (Statement s = conn.createStatement()) {
            boolean hasRs = s.execute(sql);
            if (hasRs) {
                try (ResultSet rs = s.getResultSet()) {
                    ResultSetMetaData md = rs.getMetaData();
                    int cols = md.getColumnCount();
                    List<String> colNames = new ArrayList<>();
                    for (int i = 1; i <= cols; i++) colNames.add(md.getColumnName(i));
                    System.out.printf("  OK [%s]: %s%n", String.join(",", colNames), display);
                    while (rs.next()) {
                        List<String> vals = new ArrayList<>();
                        for (int i = 1; i <= cols; i++) {
                            String v = rs.getString(i);
                            vals.add(v == null ? "NULL" : v);
                        }
                        System.out.printf("    ROW: %s%n", String.join(" | ", vals));
                    }
                }
            } else {
                int uc = s.getUpdateCount();
                System.out.printf("  OK [%d rows]: %s%n", uc, display);
            }
        } catch (SQLException e) {
            System.out.printf("  ERROR [%s]: %s (%s)%n", e.getSQLState(), display, e.getMessage().split("\n")[0].trim());
        }
    }
}
