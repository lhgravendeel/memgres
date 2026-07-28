package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An object graph closed into a loop used to be accepted and then blow the Java stack on every
 * later walk of it, leaving relations that could be neither queried nor dropped. PostgreSQL
 * refuses the statement that closes the loop — 42P07 for inheritance and partitioning, 42P17 for
 * views and rules — and reports runaway recursion as 54001. These tests pin both the refusals and
 * the neighbouring cases that must keep working.
 */
class CycleDetectionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void assertFails(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    // ---- inheritance ----

    @Test
    void inheritanceMayNotCloseALoop() throws Exception {
        exec("CREATE TABLE cdl_a (x int)");
        exec("CREATE TABLE cdl_b (x int)");
        exec("ALTER TABLE cdl_b INHERIT cdl_a");
        assertFails("42P07", "circular inheritance not allowed", "ALTER TABLE cdl_a INHERIT cdl_b");
        // both relations are still usable afterwards, which they would not be if the link had stuck
        assertEquals("0", scalar("SELECT count(*) FROM cdl_a"));
        assertEquals("0", scalar("SELECT count(*) FROM cdl_b"));
        exec("ALTER TABLE cdl_b NO INHERIT cdl_a");
        exec("DROP TABLE cdl_a");
        exec("DROP TABLE cdl_b");
    }

    @Test
    void aTableMayNotInheritFromItself() throws Exception {
        exec("CREATE TABLE cdl_self (x int)");
        assertFails("42P07", "circular inheritance not allowed", "ALTER TABLE cdl_self INHERIT cdl_self");
        assertEquals("0", scalar("SELECT count(*) FROM cdl_self"));
        exec("DROP TABLE cdl_self");
    }

    @Test
    void aLoopThreeLevelsLongIsAlsoFound() throws Exception {
        exec("CREATE TABLE cdl_i1 (x int)");
        exec("CREATE TABLE cdl_i2 (x int)");
        exec("CREATE TABLE cdl_i3 (x int)");
        exec("ALTER TABLE cdl_i2 INHERIT cdl_i1");
        exec("ALTER TABLE cdl_i3 INHERIT cdl_i2");
        assertFails("42P07", "circular inheritance not allowed", "ALTER TABLE cdl_i1 INHERIT cdl_i3");
        exec("INSERT INTO cdl_i3 VALUES (7)");
        assertEquals("1", scalar("SELECT count(*) FROM cdl_i1"));
        exec("DROP TABLE cdl_i3");
        exec("DROP TABLE cdl_i2");
        exec("DROP TABLE cdl_i1");
    }

    @Test
    void legitimateInheritanceStillWorks() throws Exception {
        exec("CREATE TABLE cdl_p1 (x int)");
        exec("CREATE TABLE cdl_c1 (x int)");
        exec("ALTER TABLE cdl_c1 INHERIT cdl_p1");
        exec("INSERT INTO cdl_c1 VALUES (5)");
        exec("INSERT INTO cdl_p1 VALUES (6)");
        assertEquals("2", scalar("SELECT count(*) FROM cdl_p1"));
        assertEquals("1", scalar("SELECT count(*) FROM ONLY cdl_p1"));
        exec("DROP TABLE cdl_p1 CASCADE");
    }

    // ---- partitioning ----

    @Test
    void attachPartitionMayNotCloseALoop() throws Exception {
        exec("CREATE TABLE cdl_pp (i int) PARTITION BY RANGE (i)");
        assertFails("42P07", "circular inheritance not allowed",
                "ALTER TABLE cdl_pp ATTACH PARTITION cdl_pp FOR VALUES FROM (1) TO (9)");
        assertEquals("0", scalar("SELECT count(*) FROM cdl_pp"));
        exec("DROP TABLE cdl_pp");

        exec("CREATE TABLE cdl_q1 (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE cdl_q2 PARTITION OF cdl_q1 FOR VALUES FROM (1) TO (9) PARTITION BY RANGE (i)");
        assertFails("42P07", "circular inheritance not allowed",
                "ALTER TABLE cdl_q2 ATTACH PARTITION cdl_q1 FOR VALUES FROM (2) TO (3)");
        exec("DROP TABLE cdl_q1 CASCADE");
    }

    @Test
    void legitimatePartitioningStillWorks() throws Exception {
        exec("CREATE TABLE cdl_r1 (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE cdl_r2 (i int)");
        exec("ALTER TABLE cdl_r1 ATTACH PARTITION cdl_r2 FOR VALUES FROM (1) TO (9)");
        exec("INSERT INTO cdl_r1 VALUES (3)");
        assertEquals("1", scalar("SELECT count(*) FROM cdl_r1"));
        exec("DROP TABLE cdl_r1 CASCADE");

        // a partition of a partition is still a legal attachment
        exec("CREATE TABLE cdl_s1 (i int) PARTITION BY RANGE (i)");
        exec("CREATE TABLE cdl_s2 PARTITION OF cdl_s1 FOR VALUES FROM (1) TO (9) PARTITION BY RANGE (i)");
        exec("CREATE TABLE cdl_s3 (i int)");
        exec("ALTER TABLE cdl_s2 ATTACH PARTITION cdl_s3 FOR VALUES FROM (2) TO (3)");
        exec("INSERT INTO cdl_s1 VALUES (2)");
        assertEquals("1", scalar("SELECT count(*) FROM cdl_s3"));
        exec("DROP TABLE cdl_s1 CASCADE");
    }

    // ---- foreign key cascades ----

    @Test
    void twoTablesCascadingIntoEachOtherDeleteOnce() throws Exception {
        exec("CREATE TABLE cdl_fa (id int PRIMARY KEY, b int)");
        exec("CREATE TABLE cdl_fb (id int PRIMARY KEY, a int REFERENCES cdl_fa(id) ON DELETE CASCADE)");
        exec("ALTER TABLE cdl_fa ADD FOREIGN KEY (b) REFERENCES cdl_fb(id) ON DELETE CASCADE");
        exec("INSERT INTO cdl_fa VALUES (1, NULL)");
        exec("INSERT INTO cdl_fb VALUES (1, 1)");
        exec("UPDATE cdl_fa SET b = 1 WHERE id = 1");
        exec("DELETE FROM cdl_fa WHERE id = 1");
        assertEquals("0", scalar("SELECT count(*) FROM cdl_fa"));
        assertEquals("0", scalar("SELECT count(*) FROM cdl_fb"));
        exec("DROP TABLE cdl_fa CASCADE");
        exec("DROP TABLE cdl_fb CASCADE");
    }

    @Test
    void aThreeTableCascadeRingTerminates() throws Exception {
        exec("CREATE TABLE cdl_c3a (id int PRIMARY KEY, r int)");
        exec("CREATE TABLE cdl_c3b (id int PRIMARY KEY, r int REFERENCES cdl_c3a(id) ON DELETE CASCADE)");
        exec("CREATE TABLE cdl_c3c (id int PRIMARY KEY, r int REFERENCES cdl_c3b(id) ON DELETE CASCADE)");
        exec("ALTER TABLE cdl_c3a ADD FOREIGN KEY (r) REFERENCES cdl_c3c(id) ON DELETE CASCADE");
        exec("INSERT INTO cdl_c3a VALUES (1, NULL)");
        exec("INSERT INTO cdl_c3b VALUES (1, 1)");
        exec("INSERT INTO cdl_c3c VALUES (1, 1)");
        exec("UPDATE cdl_c3a SET r = 1 WHERE id = 1");
        exec("DELETE FROM cdl_c3a WHERE id = 1");
        assertEquals("0", scalar("SELECT count(*) FROM cdl_c3b"));
        assertEquals("0", scalar("SELECT count(*) FROM cdl_c3c"));
        exec("DROP TABLE cdl_c3a CASCADE");
        exec("DROP TABLE cdl_c3b CASCADE");
        exec("DROP TABLE cdl_c3c CASCADE");
    }

    @Test
    void anUpdateCascadeRingPropagatesOnce() throws Exception {
        exec("CREATE TABLE cdl_ua (id int PRIMARY KEY, r int)");
        exec("CREATE TABLE cdl_ub (id int PRIMARY KEY, r int REFERENCES cdl_ua(id) ON UPDATE CASCADE)");
        exec("ALTER TABLE cdl_ua ADD FOREIGN KEY (r) REFERENCES cdl_ub(id) ON UPDATE CASCADE");
        exec("INSERT INTO cdl_ua VALUES (1, NULL)");
        exec("INSERT INTO cdl_ub VALUES (1, 1)");
        exec("UPDATE cdl_ua SET r = 1 WHERE id = 1");
        exec("UPDATE cdl_ua SET id = 2 WHERE id = 1");
        assertEquals("2", scalar("SELECT id FROM cdl_ua"));
        assertEquals("2", scalar("SELECT r FROM cdl_ub"));
        exec("DROP TABLE cdl_ua CASCADE");
        exec("DROP TABLE cdl_ub CASCADE");
    }

    @Test
    void aRowReferencingItsOwnKeyInsertsAndDeletes() throws Exception {
        exec("CREATE TABLE cdl_t (a int PRIMARY KEY, b int, "
                + "FOREIGN KEY (b) REFERENCES cdl_t(a) ON DELETE CASCADE)");
        exec("INSERT INTO cdl_t VALUES (0, 0)");
        exec("DELETE FROM cdl_t WHERE a = 0");
        assertEquals("0", scalar("SELECT count(*) FROM cdl_t"));
        exec("DROP TABLE cdl_t");
    }

    @Test
    void aSelfReferencingChainCascadesAllTheWayDown() throws Exception {
        exec("CREATE TABLE cdl_tc (a int PRIMARY KEY, b int REFERENCES cdl_tc(a) ON DELETE CASCADE)");
        exec("INSERT INTO cdl_tc VALUES (1, NULL)");
        exec("INSERT INTO cdl_tc VALUES (2, 1)");
        exec("INSERT INTO cdl_tc VALUES (3, 2)");
        exec("DELETE FROM cdl_tc WHERE a = 1");
        assertEquals("0", scalar("SELECT count(*) FROM cdl_tc"));
        exec("DROP TABLE cdl_tc");
    }

    @Test
    void ordinaryCascadeAndRestrictAreUnaffected() throws Exception {
        exec("CREATE TABLE cdl_ga (id int PRIMARY KEY)");
        exec("CREATE TABLE cdl_gb (id int PRIMARY KEY, a int REFERENCES cdl_ga(id) ON DELETE CASCADE)");
        exec("INSERT INTO cdl_ga VALUES (1), (2)");
        exec("INSERT INTO cdl_gb VALUES (10, 1), (20, 2)");
        exec("DELETE FROM cdl_ga WHERE id = 1");
        assertEquals("20", scalar("SELECT id FROM cdl_gb ORDER BY id"));
        exec("DROP TABLE cdl_gb");
        exec("DROP TABLE cdl_ga");

        exec("CREATE TABLE cdl_ha (id int PRIMARY KEY)");
        exec("CREATE TABLE cdl_hb (id int PRIMARY KEY, a int REFERENCES cdl_ha(id))");
        exec("INSERT INTO cdl_ha VALUES (1)");
        exec("INSERT INTO cdl_hb VALUES (10, 1)");
        assertFails("23503", "violates foreign key constraint", "DELETE FROM cdl_ha WHERE id = 1");
        exec("DROP TABLE cdl_hb");
        exec("DROP TABLE cdl_ha");
    }

    // ---- views ----

    @Test
    void viewsDefinedInTermsOfEachOtherAreRejectedOnRead() throws Exception {
        exec("CREATE TABLE cdl_vt (i int)");
        exec("CREATE VIEW cdl_v1 AS SELECT i FROM cdl_vt");
        exec("CREATE VIEW cdl_v2 AS SELECT i FROM cdl_v1");
        exec("CREATE OR REPLACE VIEW cdl_v1 AS SELECT i FROM cdl_v2");
        // the relation named is the one the expansion came back to
        assertFails("42P17", "infinite recursion detected in rules for relation \"cdl_v1\"",
                "SELECT count(*) FROM cdl_v1");
        assertFails("42P17", "infinite recursion detected in rules for relation \"cdl_v2\"",
                "SELECT count(*) FROM cdl_v2");
        exec("DROP VIEW cdl_v1 CASCADE");
        exec("DROP TABLE cdl_vt CASCADE");
    }

    @Test
    void aViewSelectingFromItselfIsRejectedOnRead() throws Exception {
        exec("CREATE TABLE cdl_svt (i int)");
        exec("CREATE VIEW cdl_sv AS SELECT i FROM cdl_svt");
        exec("CREATE OR REPLACE VIEW cdl_sv AS SELECT i FROM cdl_sv");
        assertFails("42P17", "infinite recursion detected in rules for relation \"cdl_sv\"",
                "SELECT count(*) FROM cdl_sv");
        exec("DROP VIEW cdl_sv");
        exec("DROP TABLE cdl_svt");
    }

    @Test
    void ordinaryViewChainsStillResolve() throws Exception {
        exec("CREATE TABLE cdl_wt (i int)");
        exec("INSERT INTO cdl_wt VALUES (1), (2)");
        exec("CREATE VIEW cdl_w1 AS SELECT i FROM cdl_wt");
        exec("CREATE VIEW cdl_w2 AS SELECT i FROM cdl_w1 WHERE i > 1");
        assertEquals("1", scalar("SELECT count(*) FROM cdl_w2"));
        assertEquals("2", scalar("SELECT count(*) FROM cdl_w1"));
        exec("DROP VIEW cdl_w2");
        exec("DROP VIEW cdl_w1");
        exec("DROP TABLE cdl_wt");
    }

    // ---- triggers ----

    @Test
    void aTriggerWritingToItsOwnTableRunsOutOfDepth() throws Exception {
        exec("CREATE TABLE cdl_trg (i int)");
        exec("CREATE FUNCTION cdl_trg_f() RETURNS trigger LANGUAGE plpgsql AS $$ "
                + "BEGIN INSERT INTO cdl_trg VALUES (NEW.i + 1); RETURN NEW; END $$");
        exec("CREATE TRIGGER cdl_trg_t AFTER INSERT ON cdl_trg "
                + "FOR EACH ROW EXECUTE FUNCTION cdl_trg_f()");
        assertFails("54001", "stack depth limit exceeded", "INSERT INTO cdl_trg VALUES (1)");
        // the failed statement leaves nothing behind
        assertEquals("0", scalar("SELECT count(*) FROM cdl_trg"));
        exec("DROP TABLE cdl_trg CASCADE");
    }

    @Test
    void triggerChainsThatTerminateStillRun() throws Exception {
        exec("CREATE TABLE cdl_src (i int)");
        exec("CREATE TABLE cdl_log (i int)");
        exec("CREATE FUNCTION cdl_log_f() RETURNS trigger LANGUAGE plpgsql AS $$ "
                + "BEGIN INSERT INTO cdl_log VALUES (NEW.i); RETURN NEW; END $$");
        exec("CREATE TRIGGER cdl_log_t AFTER INSERT ON cdl_src "
                + "FOR EACH ROW EXECUTE FUNCTION cdl_log_f()");
        exec("INSERT INTO cdl_src VALUES (1), (2)");
        assertEquals("2", scalar("SELECT count(*) FROM cdl_log"));
        exec("DROP TABLE cdl_src CASCADE");
        exec("DROP TABLE cdl_log CASCADE");

        // a self-inserting trigger that stops on its own must reach its own end
        exec("CREATE TABLE cdl_dp (i int)");
        exec("CREATE FUNCTION cdl_dp_f() RETURNS trigger LANGUAGE plpgsql AS $$ "
                + "BEGIN IF NEW.i < 50 THEN INSERT INTO cdl_dp VALUES (NEW.i + 1); END IF; "
                + "RETURN NEW; END $$");
        exec("CREATE TRIGGER cdl_dp_t AFTER INSERT ON cdl_dp "
                + "FOR EACH ROW EXECUTE FUNCTION cdl_dp_f()");
        exec("INSERT INTO cdl_dp VALUES (1)");
        assertEquals("50", scalar("SELECT count(*) FROM cdl_dp"));
        exec("DROP TABLE cdl_dp CASCADE");
    }

    // ---- rules ----

    @Test
    void aRuleRewritingOntoItsOwnTableIsRejected() throws Exception {
        exec("CREATE TABLE cdl_rt (i int)");
        exec("CREATE RULE cdl_rt_r AS ON INSERT TO cdl_rt DO ALSO INSERT INTO cdl_rt VALUES (NEW.i + 1)");
        assertFails("42P17", "infinite recursion detected in rules for relation \"cdl_rt\"",
                "INSERT INTO cdl_rt VALUES (1)");
        assertEquals("0", scalar("SELECT count(*) FROM cdl_rt"));
        exec("DROP TABLE cdl_rt CASCADE");

        exec("CREATE TABLE cdl_it (i int)");
        exec("CREATE RULE cdl_it_r AS ON INSERT TO cdl_it DO INSTEAD INSERT INTO cdl_it VALUES (NEW.i)");
        assertFails("42P17", "infinite recursion detected in rules for relation \"cdl_it\"",
                "INSERT INTO cdl_it VALUES (1)");
        assertEquals("0", scalar("SELECT count(*) FROM cdl_it"));
        exec("DROP TABLE cdl_it CASCADE");
    }

    @Test
    void aRuleOntoAnotherTableIsUnaffected() throws Exception {
        exec("CREATE TABLE cdl_ra (i int)");
        exec("CREATE TABLE cdl_rb (i int)");
        exec("CREATE RULE cdl_ra_r AS ON INSERT TO cdl_ra DO ALSO INSERT INTO cdl_rb VALUES (NEW.i)");
        exec("INSERT INTO cdl_ra VALUES (1)");
        assertEquals("1", scalar("SELECT i FROM cdl_rb"));
        exec("DROP TABLE cdl_ra CASCADE");
        exec("DROP TABLE cdl_rb CASCADE");
    }

    // ---- PL/pgSQL recursion depth ----

    @Test
    void plpgsqlRecursionReachesOrdinaryDepths() throws Exception {
        exec("CREATE FUNCTION cdl_f(n int) RETURNS int LANGUAGE plpgsql AS $$ "
                + "BEGIN IF n <= 0 THEN RETURN 0; END IF; RETURN 1 + cdl_f(n - 1); END $$");
        assertEquals("0", scalar("SELECT cdl_f(0)"));
        assertEquals("100", scalar("SELECT cdl_f(100)"));
        // 500 frames is an ordinary recursive traversal and PostgreSQL manages it comfortably
        assertEquals("500", scalar("SELECT cdl_f(500)"));
        assertFails("54001", "stack depth limit exceeded", "SELECT cdl_f(100000)");
        exec("DROP FUNCTION cdl_f(int)");
    }

    @Test
    void mutualRecursionIsBoundedTheSameWay() throws Exception {
        exec("CREATE FUNCTION cdl_m2(n int) RETURNS int LANGUAGE plpgsql AS $$ "
                + "BEGIN RETURN n; END $$");
        exec("CREATE FUNCTION cdl_m1(n int) RETURNS int LANGUAGE plpgsql AS $$ "
                + "BEGIN IF n <= 0 THEN RETURN 0; END IF; RETURN 1 + cdl_m2(n - 1); END $$");
        exec("CREATE OR REPLACE FUNCTION cdl_m2(n int) RETURNS int LANGUAGE plpgsql AS $$ "
                + "BEGIN IF n <= 0 THEN RETURN 0; END IF; RETURN 1 + cdl_m1(n - 1); END $$");
        assertEquals("100", scalar("SELECT cdl_m1(100)"));
        assertFails("54001", "stack depth limit exceeded", "SELECT cdl_m1(100000)");
        exec("DROP FUNCTION cdl_m1(int)");
        exec("DROP FUNCTION cdl_m2(int)");
    }
}
