package com.memgres.sqlverify;

import com.memgres.core.Memgres;

import java.nio.file.*;
import java.sql.*;

/** Probe: run a .sql file (or -c "sql") against an embedded memgres server. */
public class MemFields {
    public static void main(String[] args) throws Exception {
        String sql;
        if (args.length >= 2 && args[0].equals("-c")) sql = args[1];
        else sql = new String(Files.readAllBytes(Paths.get(args[0])), "UTF-8");
        Memgres m = Memgres.builder().port(0).build().start();
        try (Connection c = DriverManager.getConnection(
                m.getJdbcUrl() + Probe.QUERY_MODE, m.getUser(), m.getPassword())) {
            Probe.runScript(c, sql);
        } finally {
            m.close();
        }
    }
}
