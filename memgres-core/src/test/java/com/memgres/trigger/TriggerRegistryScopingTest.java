package com.memgres.trigger;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Trigger registration and transition tables must not reach outside the statement or
 * schema they belong to. Expectations captured from a live PostgreSQL 18.0 server.
 *
 * <p>N2 transition table destroying a real table, N3 schema-blind trigger removal and
 * lost triggers after a rolled-back DROP, N30 missing UPDATE triggers on ON CONFLICT.
 */
class TriggerRegistryScopingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    // ------------------------------------------------------------------
    // N2 — a transition name that collides with a real table
    // ------------------------------------------------------------------

    @Test
    void transitionTableDoesNotDestroyRealTableOfTheSameName() throws Exception {
        exec("CREATE TABLE realt (x int)");
        exec("INSERT INTO realt VALUES (42)");
        exec("CREATE TABLE n2base (id int)");
        exec("CREATE FUNCTION n2f() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER n2trg AFTER INSERT ON n2base REFERENCING NEW TABLE AS realt "
                + "FOR EACH STATEMENT EXECUTE FUNCTION n2f()");

        exec("INSERT INTO n2base VALUES (1)");

        assertEquals(Arrays.asList("42"), rows("SELECT x FROM realt"));
    }

    // ------------------------------------------------------------------
    // N3 — trigger removal is scoped to the dropped table's schema
    // ------------------------------------------------------------------

    @Test
    void droppingSameNamedTableInAnotherSchemaKeepsTriggers() throws Exception {
        exec("CREATE SCHEMA n3s");
        exec("CREATE TABLE n3tt (id int)");
        exec("CREATE TABLE n3s.n3tt (id int)");
        exec("CREATE TABLE n3log (msg text)");
        exec("CREATE FUNCTION n3f() RETURNS trigger AS $$ BEGIN "
                + "INSERT INTO n3log VALUES ('fired'); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER n3trg BEFORE INSERT ON n3tt FOR EACH ROW EXECUTE FUNCTION n3f()");

        exec("DROP TABLE n3s.n3tt");

        exec("INSERT INTO n3tt VALUES (1)");
        assertEquals(Arrays.asList("1"), rows("SELECT count(*) FROM n3log"));
    }

    @Test
    void rolledBackDropRestoresTriggers() throws Exception {
        exec("CREATE TABLE n3r (id int)");
        exec("CREATE TABLE n3rlog (msg text)");
        exec("CREATE FUNCTION n3rf() RETURNS trigger AS $$ BEGIN "
                + "INSERT INTO n3rlog VALUES ('fired'); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER n3rtrg BEFORE INSERT ON n3r FOR EACH ROW EXECUTE FUNCTION n3rf()");

        conn.setAutoCommit(false);
        try {
            exec("DROP TABLE n3r");
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }

        exec("INSERT INTO n3r VALUES (1)");
        assertEquals(Arrays.asList("1"), rows("SELECT count(*) FROM n3rlog"));
    }

    // ------------------------------------------------------------------
    // N30 — the conflict path is an UPDATE and fires UPDATE row triggers
    // ------------------------------------------------------------------

    @Test
    void onConflictDoUpdateFiresUpdateRowTriggers() throws Exception {
        exec("CREATE TABLE n30t (id int PRIMARY KEY, v int)");
        exec("CREATE TABLE n30log (msg text)");
        exec("CREATE FUNCTION n30f() RETURNS trigger AS $$ BEGIN "
                + "INSERT INTO n30log VALUES (TG_WHEN || ' ' || TG_OP); RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER n30b BEFORE UPDATE ON n30t FOR EACH ROW EXECUTE FUNCTION n30f()");
        exec("CREATE TRIGGER n30a AFTER UPDATE ON n30t FOR EACH ROW EXECUTE FUNCTION n30f()");
        exec("INSERT INTO n30t VALUES (1, 1)");

        exec("INSERT INTO n30t VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v");

        assertEquals(Arrays.asList("AFTER UPDATE", "BEFORE UPDATE"),
                rows("SELECT msg FROM n30log ORDER BY msg"));
        assertEquals(Arrays.asList("1|2"), rows("SELECT id, v FROM n30t"));
    }

    /** A BEFORE trigger may still modify the row the conflict path writes. */
    @Test
    void onConflictDoUpdateHonoursBeforeTriggerChanges() throws Exception {
        exec("CREATE TABLE n30m (id int PRIMARY KEY, v int)");
        exec("CREATE FUNCTION n30mf() RETURNS trigger AS $$ BEGIN "
                + "NEW.v := NEW.v * 10; RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER n30mb BEFORE UPDATE ON n30m FOR EACH ROW EXECUTE FUNCTION n30mf()");
        exec("INSERT INTO n30m VALUES (1, 1)");

        exec("INSERT INTO n30m VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v");

        assertEquals(Arrays.asList("1|50"), rows("SELECT id, v FROM n30m"));
    }
}
