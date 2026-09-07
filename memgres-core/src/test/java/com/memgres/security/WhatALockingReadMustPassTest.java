package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a locking read has to pass.
 *
 * <p>A row a query means to lock is a row it means to write, so it has to pass the policies that
 * govern writing as well as the ones that govern reading. Filtered as an ordinary read,
 * {@code SELECT ... FOR UPDATE} handed back rows the reader was not allowed to lock -- and a
 * relation with a read policy and no write policy locked every row it could see, which is the
 * whole of what row security was there to prevent.
 */
class WhatALockingReadMustPassTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE ROLE zwl_u LOGIN");
        exec("GRANT CREATE ON SCHEMA public TO zwl_u");
        exec("SET ROLE zwl_u");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("RESET ROLE");
            exec("REVOKE CREATE ON SCHEMA public FROM zwl_u");
            exec("DROP ROLE zwl_u");
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

    /** A read policy alone lets a row be read but not locked. */
    @Test
    void whatALockingReadSees() throws SQLException {
        exec("CREATE TABLE zwl_r (id int)");
        exec("INSERT INTO zwl_r VALUES (1),(2),(3)");
        exec("ALTER TABLE zwl_r ENABLE ROW LEVEL SECURITY");
        exec("ALTER TABLE zwl_r FORCE ROW LEVEL SECURITY");
        exec("CREATE POLICY zwl_s ON zwl_r FOR SELECT USING (id < 3)");
        assertEquals("2", one("SELECT count(*)::text FROM zwl_r"));
        // Nothing may be locked: there is no policy that governs writing.
        assertEquals("0", locked("FOR UPDATE"));
        assertEquals("0", locked("FOR SHARE"));
        // A write policy lets through what both it and the read policy allow.
        exec("CREATE POLICY zwl_w ON zwl_r FOR UPDATE USING (id = 1)");
        assertEquals("1", locked("FOR UPDATE"));
        assertEquals("1", locked("FOR SHARE"));
        // And a policy for every command governs both halves at once.
        exec("DROP POLICY zwl_w ON zwl_r");
        exec("CREATE POLICY zwl_all ON zwl_r FOR ALL USING (true)");
        assertEquals("3", locked("FOR UPDATE"));
        assertEquals("3", one("SELECT count(*)::text FROM zwl_r"));
        exec("DROP TABLE zwl_r");
    }

    private static String locked(String clause) throws SQLException {
        return one("SELECT count(*)::text FROM (SELECT id FROM zwl_r " + clause + ") q");
    }
}
