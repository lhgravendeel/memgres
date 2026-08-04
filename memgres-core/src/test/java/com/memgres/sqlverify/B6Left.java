package com.memgres.sqlverify;

import com.memgres.core.Memgres;
import java.sql.*;

/** Scratch probe: what is still open on branch 6's headline items. */
public class B6Left {

    static String one(Connection c, String sql) {
        try (Statement s = c.createStatement()) {
            s.setQueryTimeout(20);
            if (s.execute(sql)) {
                StringBuilder sb = new StringBuilder();
                try (ResultSet rs = s.getResultSet()) {
                    int n = rs.getMetaData().getColumnCount();
                    int r = 0;
                    while (rs.next() && r++ < 6) {
                        for (int i = 1; i <= n; i++) sb.append(rs.getString(i)).append(i < n ? "|" : "");
                        sb.append(";");
                    }
                }
                return "OK[" + sb + "]";
            }
            return "OK";
        } catch (SQLException e) {
            return "ERR[" + e.getSQLState() + "] "
                    + e.getMessage().split("\n")[0].replace("ERROR: ", "").trim();
        }
    }

    static final String[] SETUP = {
        "DROP SCHEMA IF EXISTS b6l_a CASCADE", "DROP SCHEMA IF EXISTS b6l_b CASCADE",
        "CREATE SCHEMA b6l_a", "CREATE SCHEMA b6l_b",
    };

    static final String[][] CASES = {
        {"enum of the same name in two schemas",
         "CREATE TYPE b6l_a.e AS ENUM ('x'); CREATE TYPE b6l_b.e AS ENUM ('p')"},
        {"how many such enums exist",
         "SELECT count(*)::text FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace"
         + " WHERE t.typname='e' AND n.nspname IN ('b6l_a','b6l_b')"},
        {"each enum keeps its own labels",
         "SELECT (SELECT string_agg(enumlabel,',' ORDER BY enumsortorder) FROM pg_enum e"
         + " JOIN pg_type t ON t.oid=e.enumtypid JOIN pg_namespace n ON n.oid=t.typnamespace"
         + " WHERE t.typname='e' AND n.nspname='b6l_a')"},
        {"domain of the same name in two schemas",
         "CREATE DOMAIN b6l_a.d AS int; CREATE DOMAIN b6l_b.d AS text"},
        {"composite of the same name in two schemas",
         "CREATE TYPE b6l_a.ct AS (a int); CREATE TYPE b6l_b.ct AS (b text)"},
        {"range of the same name in two schemas",
         "CREATE TYPE b6l_a.rg AS RANGE (subtype = int4);"
         + " CREATE TYPE b6l_b.rg AS RANGE (subtype = text)"},
        {"a column of each enum reads its own labels",
         "CREATE TABLE b6l_a.t (v b6l_a.e); INSERT INTO b6l_a.t VALUES ('x');"
         + " SELECT v::text FROM b6l_a.t"},
        {"comment on same-named tables in two schemas",
         "CREATE TABLE b6l_a.ct2 (a int); CREATE TABLE b6l_b.ct2 (a int);"
         + " COMMENT ON TABLE b6l_a.ct2 IS 'acom'; COMMENT ON TABLE b6l_b.ct2 IS 'bcom';"
         + " SELECT obj_description('b6l_a.ct2'::regclass), obj_description('b6l_b.ct2'::regclass)"},
        {"obj_description on a sequence",
         "CREATE SEQUENCE b6l_a.sq; COMMENT ON SEQUENCE b6l_a.sq IS 'sc';"
         + " SELECT obj_description('b6l_a.sq'::regclass)"},
        {"obj_description on a view",
         "CREATE VIEW b6l_a.vv AS SELECT 1 AS x; COMMENT ON VIEW b6l_a.vv IS 'vc';"
         + " SELECT obj_description('b6l_a.vv'::regclass)"},
    };

    public static void main(String[] a) throws Exception {
        Memgres m = Memgres.builder().port(0).build();
        m.start();
        Connection mg = DriverManager.getConnection(m.getJdbcUrl() + "?preferQueryMode=simple",
                m.getUser(), m.getPassword());
        Connection pg = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/memgrestest", "memgres", "memgres");
        for (Connection c : new Connection[]{pg, mg}) for (String s : SETUP) one(c, s);
        int diffs = 0;
        for (String[] c : CASES) {
            String p = null, g = null;
            for (String part : c[1].split(";")) {
                if (part.trim().isEmpty()) continue;
                p = one(pg, part.trim());
                g = one(mg, part.trim());
            }
            if (p.equals(g)) {
                System.out.println("SAME  " + c[0] + "  -> " + p);
            } else {
                diffs++;
                System.out.println("DIFF  " + c[0]);
                System.out.println("        PG : " + p);
                System.out.println("        MEM: " + g);
            }
        }
        System.out.println("\nDIFFS = " + diffs + " of " + CASES.length);
        for (Connection c : new Connection[]{pg, mg}) {
            one(c, "DROP SCHEMA IF EXISTS b6l_a CASCADE");
            one(c, "DROP SCHEMA IF EXISTS b6l_b CASCADE");
        }
        pg.close(); mg.close(); m.close();
        System.exit(0);
    }
}
