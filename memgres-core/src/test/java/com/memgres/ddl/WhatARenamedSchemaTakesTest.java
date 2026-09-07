package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a renamed schema takes with it.
 *
 * <p>A view reads the relation, not the name the relation happened to be reached by, and renaming
 * the schema gives every relation in it a new one. Left as they were written, a view over a
 * relation in the renamed schema named a schema that was gone: it could not be read at all, and
 * its printed definition said so.
 */
class WhatARenamedSchemaTakesTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
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

    /** The view goes on reading, under the schema's new name. */
    @Test
    void whatAViewOverItFollows() throws SQLException {
        exec("CREATE SCHEMA zxd_s");
        exec("CREATE TABLE zxd_s.dep (a int)");
        exec("INSERT INTO zxd_s.dep VALUES (1)");
        exec("CREATE VIEW zxd_s.depv AS SELECT a FROM zxd_s.dep");
        exec("ALTER SCHEMA zxd_s RENAME TO zxd_s3");
        assertEquals("1", one("SELECT count(*)::text FROM zxd_s3.depv"));
        assertEquals(" SELECT a\n   FROM zxd_s3.dep;",
                one("SELECT definition FROM pg_views WHERE viewname = 'depv'"));
        exec("DROP SCHEMA zxd_s3 CASCADE");
    }
}
