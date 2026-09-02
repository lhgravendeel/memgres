package com.memgres.sqlverify;

import java.sql.*;

/** The reference database each replay script gets to itself. */
final class ReplayDb {
    private ReplayDb() {}

    static final String URL =
            "jdbc:postgresql://localhost:5432/memgrestest?preferQueryMode=simple";

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(URL, "memgres", "memgres");
    }

    /**
     * A database, and a set of roles, of this script's own.
     *
     * <p>A role belongs to the cluster and not to a database, so recreating the database leaves
     * every role the last script made still standing. Replayed in a batch, the second script to
     * create a role was told it already existed, where memgres — started fresh each time — made
     * it happily: a hundred and twenty-six statements read as memgres accepting what PostgreSQL
     * rejects, and not one of them was about memgres.
     */
    static void recreate() {
        try (Connection c = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/postgres", "memgres", "memgres");
             Statement s = c.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS memgrestest WITH (FORCE)");
            s.execute("CREATE DATABASE memgrestest");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        dropLeftoverRoles();
    }

    /** Every role the scripts have made, taken back out. The built-in ones are left alone. */
    private static void dropLeftoverRoles() {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            java.util.List<String> roles = new java.util.ArrayList<String>();
            try (ResultSet rs = s.executeQuery(
                    "SELECT rolname FROM pg_roles WHERE rolname NOT LIKE 'pg\\_%'"
                            + " AND rolname <> 'memgres'")) {
                while (rs.next()) roles.add(rs.getString(1));
            }
            for (String r : roles) {
                String quoted = "\"" + r.replace("\"", "\"\"") + "\"";
                quietly(s, "DROP OWNED BY " + quoted + " CASCADE");
                quietly(s, "DROP ROLE " + quoted);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void quietly(Statement s, String sql) {
        try {
            s.execute(sql);
        } catch (SQLException ignored) {
            // a role that owns something in another database cannot be dropped from here, and one
            // that another depends on goes when that one does
        }
    }

    static void rollback(Connection c) {
        try (Statement s = c.createStatement()) {
            s.execute("ROLLBACK");
        } catch (SQLException ignored) {
            // an autocommit connection has nothing to roll back
        }
    }
}
