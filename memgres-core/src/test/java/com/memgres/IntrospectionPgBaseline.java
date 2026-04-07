package com.memgres;

import java.sql.*;
import java.util.*;

/**
 * Collects PG18 baseline for introspection functions.
 * NOT a test. Run manually against a real PostgreSQL instance.
 *
 * Usage:
 *   mvn -pl memgres-core exec:java \
 *     -Dexec.mainClass="com.memgres.IntrospectionPgBaseline" \
 *     -Dexec.classpathScope=test
 */
public class IntrospectionPgBaseline {

    private static final String PG_URL = "jdbc:postgresql://localhost:5432/memgrestest";
    private static final String PG_USER = "memgres";
    private static final String PG_PASS = "memgres";

    public static void main(String[] args) throws Exception {
        System.out.println("=== Introspection PG18 Baseline ===\n");

        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            conn.setAutoCommit(true);
            Statement s = conn.createStatement();

            // Setup
            s.execute("DROP VIEW IF EXISTS vd_view CASCADE");
            s.execute("DROP TABLE IF EXISTS vd_test CASCADE");
            s.execute("CREATE TABLE vd_test (id INT, name TEXT, active BOOLEAN)");
            s.execute("CREATE VIEW vd_view AS SELECT id, name FROM vd_test WHERE active = true");

            // pg_get_viewdef
            runQuery(conn, "pg_get_viewdef", "SELECT pg_get_viewdef('vd_view'::regclass)");
            runQuery(conn, "pg_get_viewdef(pretty)", "SELECT pg_get_viewdef('vd_view'::regclass, true)");

            s.execute("DROP VIEW vd_view");
            s.execute("DROP TABLE vd_test CASCADE");

            // pg_get_functiondef
            s.execute("CREATE OR REPLACE FUNCTION add_nums(a INT, b INT) RETURNS INT LANGUAGE sql AS $$ SELECT a + b $$");
            runQuery(conn, "pg_get_functiondef", "SELECT pg_get_functiondef('add_nums'::regproc)");
            runQuery(conn, "pg_get_function_arguments", "SELECT pg_get_function_arguments('add_nums'::regproc)");
            runQuery(conn, "pg_get_function_result", "SELECT pg_get_function_result('add_nums'::regproc)");
            s.execute("DROP FUNCTION add_nums");

            // col_description
            s.execute("CREATE TABLE comment_test (id INT, name TEXT)");
            s.execute("COMMENT ON COLUMN comment_test.name IS 'The user display name'");
            runQuery(conn, "col_description", "SELECT col_description('comment_test'::regclass, 2)");
            s.execute("DROP TABLE comment_test CASCADE");

            // obj_description
            s.execute("CREATE TABLE obj_desc_test (id INT)");
            s.execute("COMMENT ON TABLE obj_desc_test IS 'A test table for descriptions'");
            runQuery(conn, "obj_description", "SELECT obj_description('obj_desc_test'::regclass, 'pg_class')");
            s.execute("DROP TABLE obj_desc_test CASCADE");

            // Size functions
            s.execute("CREATE TABLE size_test (id SERIAL, data TEXT)");
            s.execute("INSERT INTO size_test (data) SELECT 'row ' || generate_series(1, 100)");
            runQuery(conn, "pg_relation_size", "SELECT pg_relation_size('size_test'::regclass)");
            runQuery(conn, "pg_table_size", "SELECT pg_table_size('size_test'::regclass)");
            runQuery(conn, "pg_total_relation_size", "SELECT pg_total_relation_size('size_test'::regclass)");
            runQuery(conn, "pg_database_size", "SELECT pg_database_size(current_database())");
            s.execute("DROP TABLE size_test CASCADE");

            // information_schema.views
            s.execute("CREATE TABLE is_test (id INT, val TEXT)");
            s.execute("CREATE VIEW is_view AS SELECT id, val FROM is_test WHERE id > 0");
            runQuery(conn, "information_schema.views",
                    "SELECT view_definition FROM information_schema.views WHERE table_name = 'is_view'");
            s.execute("DROP VIEW is_view");
            s.execute("DROP TABLE is_test CASCADE");

            // information_schema.routines
            s.execute("CREATE FUNCTION is_fn(x INT) RETURNS INT LANGUAGE sql AS $$ SELECT x * 2 $$");
            runQuery(conn, "information_schema.routines",
                    "SELECT routine_type FROM information_schema.routines WHERE routine_name = 'is_fn'");
            s.execute("DROP FUNCTION is_fn");

            System.out.println("\n=== Done ===");
        }
    }

    private static void runQuery(Connection conn, String label, String sql) {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (rs.next()) {
                String val = rs.getString(1);
                System.out.println("  " + label + ": " + (val != null ? val.replace("\n", "\\n") : "NULL"));
            } else {
                System.out.println("  " + label + ": (no rows)");
            }
        } catch (SQLException e) {
            System.out.println("  " + label + " ERROR [" + e.getSQLState() + "]: " + e.getMessage().split("\n")[0]);
        }
    }
}
