package com.memgres.sqlverify;

import java.nio.file.*;
import java.sql.*;

/** Probe: run a .sql file (or -c "sql") against the live PostgreSQL 18 reference server. */
public class PgFields {
    static final String URL = "jdbc:postgresql://localhost:5432/memgrestest" + Probe.QUERY_MODE;

    public static void main(String[] args) throws Exception {
        String sql;
        if (args.length >= 2 && args[0].equals("-c")) sql = args[1];
        else sql = new String(Files.readAllBytes(Paths.get(args[0])), "UTF-8");
        try (Connection c = DriverManager.getConnection(URL, "memgres", "memgres")) {
            Probe.runScript(c, sql);
        }
    }
}
