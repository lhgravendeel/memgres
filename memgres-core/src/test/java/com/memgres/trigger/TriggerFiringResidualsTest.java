package com.memgres.trigger;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Residual trigger-firing bugs from bugs-review.md H6:
 *  1. Plain UPDATE / DELETE / multi-row INSERT must be atomic when a row trigger raises
 *     mid-statement (PostgreSQL rolls back the whole statement's prior side effects).
 *  2. MERGE must fire BEFORE row triggers (UPDATE and INSERT), honoring a BEFORE trigger
 *     that modifies NEW or returns NULL to skip the row.
 */
class TriggerFiringResidualsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---------- Bug 1: statement atomicity on mid-statement trigger error ----------

    @Test
    void plain_update_rolled_back_when_before_trigger_raises_midstatement() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tfr_u1 (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO tfr_u1 VALUES (1,10),(2,20)");
            s.execute("CREATE OR REPLACE FUNCTION tfr_raise_on2() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN IF NEW.id = 2 THEN RAISE EXCEPTION 'boom on 2'; END IF; RETURN NEW; END; $$");
            s.execute("CREATE TRIGGER tfr_bu1 BEFORE UPDATE ON tfr_u1 FOR EACH ROW EXECUTE FUNCTION tfr_raise_on2()");

            SQLException ex = assertThrows(SQLException.class, () -> s.execute("UPDATE tfr_u1 SET v = v + 1"));
            assertTrue(ex.getMessage().contains("boom on 2"), "message was: " + ex.getMessage());

            // Whole statement rolled back: row 1 must NOT be left at 11.
            assertRows(s, "SELECT id, v FROM tfr_u1 ORDER BY id", new Object[][]{{1, 10}, {2, 20}});

            s.execute("DROP TABLE tfr_u1");
            s.execute("DROP FUNCTION tfr_raise_on2()");
        }
    }

    @Test
    void plain_update_rolled_back_when_after_trigger_raises_midstatement() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tfr_u2 (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO tfr_u2 VALUES (1,10),(2,20)");
            s.execute("CREATE OR REPLACE FUNCTION tfr_after_raise_on2() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN IF NEW.id = 2 THEN RAISE EXCEPTION 'after boom'; END IF; RETURN NEW; END; $$");
            s.execute("CREATE TRIGGER tfr_au2 AFTER UPDATE ON tfr_u2 FOR EACH ROW EXECUTE FUNCTION tfr_after_raise_on2()");

            SQLException ex = assertThrows(SQLException.class, () -> s.execute("UPDATE tfr_u2 SET v = v + 1"));
            assertTrue(ex.getMessage().contains("after boom"), "message was: " + ex.getMessage());

            assertRows(s, "SELECT id, v FROM tfr_u2 ORDER BY id", new Object[][]{{1, 10}, {2, 20}});

            s.execute("DROP TABLE tfr_u2");
            s.execute("DROP FUNCTION tfr_after_raise_on2()");
        }
    }

    @Test
    void multirow_insert_rolled_back_when_before_trigger_raises_midstatement() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tfr_i1 (id int PRIMARY KEY, v int)");
            s.execute("CREATE OR REPLACE FUNCTION tfr_ins_raise_on2() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN IF NEW.id = 2 THEN RAISE EXCEPTION 'ins boom'; END IF; RETURN NEW; END; $$");
            s.execute("CREATE TRIGGER tfr_bi1 BEFORE INSERT ON tfr_i1 FOR EACH ROW EXECUTE FUNCTION tfr_ins_raise_on2()");

            SQLException ex = assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO tfr_i1 VALUES (1,10),(2,20),(3,30)"));
            assertTrue(ex.getMessage().contains("ins boom"), "message was: " + ex.getMessage());

            // Whole multi-row INSERT rolled back: table must be empty.
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM tfr_i1")) {
                assertTrue(rs.next());
                assertEquals(0L, ((Number) rs.getObject(1)).longValue());
            }

            s.execute("DROP TABLE tfr_i1");
            s.execute("DROP FUNCTION tfr_ins_raise_on2()");
        }
    }

    @Test
    void multirow_delete_rolled_back_when_after_trigger_raises_midstatement() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tfr_d1 (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO tfr_d1 VALUES (1,10),(2,20),(3,30)");
            s.execute("CREATE OR REPLACE FUNCTION tfr_del_raise_on2() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN IF OLD.id = 2 THEN RAISE EXCEPTION 'del boom'; END IF; RETURN OLD; END; $$");
            s.execute("CREATE TRIGGER tfr_ad1 AFTER DELETE ON tfr_d1 FOR EACH ROW EXECUTE FUNCTION tfr_del_raise_on2()");

            SQLException ex = assertThrows(SQLException.class, () -> s.execute("DELETE FROM tfr_d1 WHERE v > 0"));
            assertTrue(ex.getMessage().contains("del boom"), "message was: " + ex.getMessage());

            // Whole DELETE rolled back: all three rows remain.
            assertRows(s, "SELECT id, v FROM tfr_d1 ORDER BY id",
                    new Object[][]{{1, 10}, {2, 20}, {3, 30}});

            s.execute("DROP TABLE tfr_d1");
            s.execute("DROP FUNCTION tfr_del_raise_on2()");
        }
    }

    // ---------- Bug 2: MERGE fires BEFORE row triggers ----------

    @Test
    void merge_fires_before_update_and_before_insert_triggers_modifying_new() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tfr_mt (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO tfr_mt VALUES (1,10)");
            s.execute("CREATE TABLE tfr_ms (id int, v int)");
            s.execute("INSERT INTO tfr_ms VALUES (1,100),(2,200)");
            s.execute("CREATE OR REPLACE FUNCTION tfr_bump() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN NEW.v := NEW.v + 1; RETURN NEW; END; $$");
            s.execute("CREATE TRIGGER tfr_bu BEFORE UPDATE ON tfr_mt FOR EACH ROW EXECUTE FUNCTION tfr_bump()");
            s.execute("CREATE TRIGGER tfr_bi BEFORE INSERT ON tfr_mt FOR EACH ROW EXECUTE FUNCTION tfr_bump()");

            s.execute("MERGE INTO tfr_mt t USING tfr_ms s ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET v = s.v "
                    + "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)");

            // BEFORE triggers ran and each added 1: id1 -> 100+1, id2 -> 200+1.
            assertRows(s, "SELECT id, v FROM tfr_mt ORDER BY id", new Object[][]{{1, 101}, {2, 201}});

            s.execute("DROP TABLE tfr_mt");
            s.execute("DROP TABLE tfr_ms");
            s.execute("DROP FUNCTION tfr_bump()");
        }
    }

    @Test
    void merge_before_update_trigger_raise_aborts_and_rolls_back() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tfr_mt2 (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO tfr_mt2 VALUES (1,10),(2,20)");
            s.execute("CREATE TABLE tfr_ms2 (id int, v int)");
            s.execute("INSERT INTO tfr_ms2 VALUES (1,100),(2,200)");
            s.execute("CREATE OR REPLACE FUNCTION tfr_raise_on2b() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN IF NEW.id = 2 THEN RAISE EXCEPTION 'merge boom'; END IF; RETURN NEW; END; $$");
            s.execute("CREATE TRIGGER tfr_bu2 BEFORE UPDATE ON tfr_mt2 FOR EACH ROW EXECUTE FUNCTION tfr_raise_on2b()");

            SQLException ex = assertThrows(SQLException.class, () -> s.execute(
                    "MERGE INTO tfr_mt2 t USING tfr_ms2 s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v"));
            assertTrue(ex.getMessage().contains("merge boom"), "message was: " + ex.getMessage());

            // MERGE is atomic: BEFORE trigger firing on the target must have aborted the whole
            // statement, so no row was updated.
            assertRows(s, "SELECT id, v FROM tfr_mt2 ORDER BY id", new Object[][]{{1, 10}, {2, 20}});

            s.execute("DROP TABLE tfr_mt2");
            s.execute("DROP TABLE tfr_ms2");
            s.execute("DROP FUNCTION tfr_raise_on2b()");
        }
    }

    @Test
    void merge_before_update_trigger_returning_null_skips_row() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tfr_mt3 (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO tfr_mt3 VALUES (1,10),(3,30)");
            s.execute("CREATE TABLE tfr_ms3 (id int, v int)");
            s.execute("INSERT INTO tfr_ms3 VALUES (1,100),(3,300)");
            s.execute("CREATE OR REPLACE FUNCTION tfr_skip3() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN IF NEW.id = 3 THEN RETURN NULL; END IF; RETURN NEW; END; $$");
            s.execute("CREATE TRIGGER tfr_bu3 BEFORE UPDATE ON tfr_mt3 FOR EACH ROW EXECUTE FUNCTION tfr_skip3()");

            s.execute("MERGE INTO tfr_mt3 t USING tfr_ms3 s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v");

            // id=3 skipped by trigger (still 30); id=1 updated to 100.
            assertRows(s, "SELECT id, v FROM tfr_mt3 ORDER BY id", new Object[][]{{1, 100}, {3, 30}});

            s.execute("DROP TABLE tfr_mt3");
            s.execute("DROP TABLE tfr_ms3");
            s.execute("DROP FUNCTION tfr_skip3()");
        }
    }

    // ---------- helpers ----------

    private static void assertRows(Statement s, String sql, Object[][] expected) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) {
            for (Object[] exp : expected) {
                assertTrue(rs.next(), "expected another row for: " + sql);
                for (int c = 0; c < exp.length; c++) {
                    assertEquals(((Number) exp[c]).intValue(), rs.getInt(c + 1),
                            "column " + (c + 1) + " of " + sql);
                }
            }
            assertFalse(rs.next(), "unexpected extra row for: " + sql);
        }
    }
}
