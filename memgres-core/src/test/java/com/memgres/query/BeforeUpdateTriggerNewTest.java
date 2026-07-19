package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that BEFORE UPDATE triggers see the proposed NEW values (after SET),
 * not the pre-SET old values. PG fires BEFORE triggers with NEW already
 * containing the SET-clause results.
 */
class BeforeUpdateTriggerNewTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void before_trigger_sees_new_values() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE bt_t (id int PRIMARY KEY, val int, audit text)");
            s.execute("INSERT INTO bt_t VALUES (1, 10, NULL)");
            // Trigger that records what NEW.val is into audit column
            s.execute("CREATE OR REPLACE FUNCTION bt_fn() RETURNS trigger LANGUAGE plpgsql AS $$ " +
                    "BEGIN NEW.audit := 'saw_val=' || NEW.val; RETURN NEW; END; $$");
            s.execute("CREATE TRIGGER bt_trg BEFORE UPDATE ON bt_t FOR EACH ROW EXECUTE FUNCTION bt_fn()");
            s.execute("UPDATE bt_t SET val = 99 WHERE id = 1");
            ResultSet rs = s.executeQuery("SELECT val, audit FROM bt_t WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(99, rs.getInt("val"));
            assertEquals("saw_val=99", rs.getString("audit"),
                    "BEFORE trigger should see NEW.val = 99 (post-SET), not 10 (pre-SET)");
        }
    }

    @Test void before_trigger_can_modify_new() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE bt_t2 (id int PRIMARY KEY, val int)");
            s.execute("INSERT INTO bt_t2 VALUES (1, 10)");
            // Trigger that doubles the proposed new value
            s.execute("CREATE OR REPLACE FUNCTION bt_fn2() RETURNS trigger LANGUAGE plpgsql AS $$ " +
                    "BEGIN NEW.val := NEW.val * 2; RETURN NEW; END; $$");
            s.execute("CREATE TRIGGER bt_trg2 BEFORE UPDATE ON bt_t2 FOR EACH ROW EXECUTE FUNCTION bt_fn2()");
            s.execute("UPDATE bt_t2 SET val = 50 WHERE id = 1");
            ResultSet rs = s.executeQuery("SELECT val FROM bt_t2 WHERE id = 1");
            assertTrue(rs.next());
            // Trigger doubles 50 → 100 (only works if trigger sees NEW.val=50, not 10)
            assertEquals(100, rs.getInt("val"));
        }
    }
}
