package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a view follows when its base relation is renamed.
 *
 * <p>PostgreSQL records a view's dependencies as OIDs, so renaming the relation under it leaves
 * the view working and its printed definition showing the new name. memgres stores a parse tree
 * that names relations by string, and a {@code SELECT *} is expanded when the view is created into
 * references qualified by the relation's own name. Those qualifiers were left where they were: the
 * view could no longer be read at all, and its definition printed a name nothing answered to.
 */
class WhatAViewFollowsTest {

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

    /** A view over SELECT * keeps reading, and prints what it now reads. */
    @Test
    void whatAWildcardViewFollows() throws SQLException {
        exec("CREATE TABLE zxa_t (a int)");
        exec("INSERT INTO zxa_t VALUES (1),(2)");
        exec("CREATE VIEW zxa_v AS SELECT * FROM zxa_t");
        exec("ALTER TABLE zxa_t RENAME TO zxa_t2");
        assertEquals("2", one("SELECT count(*)::text FROM zxa_v"));
        assertEquals(" SELECT a\n   FROM zxa_t2;",
                one("SELECT definition FROM pg_views WHERE viewname = 'zxa_v'"));
        // A renamed column is followed too, under the name the view gave it.
        exec("ALTER TABLE zxa_t2 RENAME COLUMN a TO b");
        assertEquals(" SELECT b AS a\n   FROM zxa_t2;",
                one("SELECT definition FROM pg_views WHERE viewname = 'zxa_v'"));
        assertEquals("2", one("SELECT count(*)::text FROM zxa_v"));
        exec("DROP VIEW zxa_v");
        exec("DROP TABLE zxa_t2");
    }

    /** A name an alias gave to something else is not the relation's, and stays put. */
    @Test
    void whatAnAliasKeeps() throws SQLException {
        exec("CREATE TABLE zxa_p (a int)");
        exec("CREATE TABLE zxa_q (a int)");
        exec("INSERT INTO zxa_q VALUES (7)");
        exec("CREATE VIEW zxa_w AS SELECT zxa_p.a FROM zxa_q AS zxa_p");
        exec("ALTER TABLE zxa_p RENAME TO zxa_p2");
        assertEquals("7", one("SELECT a::text FROM zxa_w"));
        exec("DROP VIEW zxa_w");
        exec("DROP TABLE zxa_p2");
        exec("DROP TABLE zxa_q");
    }
}
