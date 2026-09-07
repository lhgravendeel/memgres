package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a rolled-back trigger statement leaves.
 *
 * <p>DDL is transactional, so a trigger created in a transaction that was rolled back was never
 * created and one dropped there is still there. Applied straight onto the live registry with
 * nothing recorded, a rolled-back CREATE TRIGGER went on firing -- and the name it had taken was
 * still taken, so creating it again for real was refused.
 */
class WhatARolledBackTriggerLeavesTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE TABLE zwb_t (id int, n int)");
        exec("CREATE FUNCTION zwb_set() RETURNS trigger LANGUAGE plpgsql AS $$"
                + " BEGIN NEW.n := 99; RETURN NEW; END $$");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TABLE zwb_t");
            exec("DROP FUNCTION zwb_set()");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** A trigger a rolled-back transaction created was never created. */
    @Test
    void whatARolledBackCreateLeaves() throws SQLException {
        exec("BEGIN");
        exec("CREATE TRIGGER zwb_tg BEFORE INSERT ON zwb_t FOR EACH ROW EXECUTE FUNCTION zwb_set()");
        exec("ROLLBACK");
        assertEquals("0", one("SELECT count(*)::text FROM pg_trigger WHERE tgname = 'zwb_tg'"));
        exec("INSERT INTO zwb_t VALUES (1, 5)");
        assertEquals("5", one("SELECT n::text FROM zwb_t WHERE id = 1"));
        // And the name it took is free again.
        exec("CREATE TRIGGER zwb_tg BEFORE INSERT ON zwb_t FOR EACH ROW EXECUTE FUNCTION zwb_set()");
    }

    /** A trigger a rolled-back transaction dropped is still there. */
    @Test
    void whatARolledBackDropLeaves() throws SQLException {
        exec("CREATE TRIGGER zwb_tg2 BEFORE INSERT ON zwb_t FOR EACH ROW EXECUTE FUNCTION zwb_set()");
        exec("BEGIN");
        exec("DROP TRIGGER zwb_tg2 ON zwb_t");
        exec("ROLLBACK");
        exec("INSERT INTO zwb_t VALUES (2, 5)");
        assertEquals("99", one("SELECT n::text FROM zwb_t WHERE id = 2"));
        exec("DROP TRIGGER zwb_tg2 ON zwb_t");
    }
}
