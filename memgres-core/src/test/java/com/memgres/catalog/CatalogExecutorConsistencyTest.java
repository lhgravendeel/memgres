package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Two places where the catalog and the executor described the database differently: a rule made a
 * table look like it published its changes, and a text search configuration appeared in the
 * catalog while the parser could not find it. Both read to a client as the database contradicting
 * itself. Expectations captured from a live PostgreSQL 18.0 server.
 *
 * <p>A3 rules and replica identity, A7 text search configuration registration.
 */
class CatalogExecutorConsistencyTest {

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

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static String state(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql));
        return e.getSQLState();
    }

    // ---- A3: a rule is not a publication ----

    /** An INSTEAD NOTHING rule swallows the delete; it does not make the table a publisher. */
    @Test
    void aRuleDoesNotMakeATablePublishItsDeletes() throws Exception {
        exec("DROP TABLE IF EXISTS cec_r CASCADE");
        exec("CREATE TABLE cec_r (id int)");
        exec("CREATE RULE cec_rr AS ON DELETE TO cec_r DO INSTEAD NOTHING");
        exec("INSERT INTO cec_r VALUES (1)");
        exec("DELETE FROM cec_r");
        assertEquals("1", one("SELECT count(*) FROM cec_r"));
        exec("DROP RULE cec_rr ON cec_r");
        exec("DROP TABLE cec_r");
    }

    @Test
    void anUpdateRuleDoesNotBlockUpdatesEither() throws Exception {
        exec("DROP TABLE IF EXISTS cec_r2 CASCADE");
        exec("CREATE TABLE cec_r2 (id int)");
        exec("CREATE RULE cec_rr2 AS ON UPDATE TO cec_r2 DO INSTEAD NOTHING");
        exec("INSERT INTO cec_r2 VALUES (1)");
        exec("UPDATE cec_r2 SET id = 2");
        assertEquals("1", one("SELECT id FROM cec_r2"));
        exec("DROP RULE cec_rr2 ON cec_r2");
        exec("DROP TABLE cec_r2");
    }

    /** Dropping the rule has to retire the behaviour, not just the name in the catalog. */
    @Test
    void aDroppedRuleStopsSuppressingTheDelete() throws Exception {
        exec("DROP TABLE IF EXISTS cec_r3 CASCADE");
        exec("CREATE TABLE cec_r3 (id int)");
        exec("CREATE RULE cec_rr3 AS ON DELETE TO cec_r3 DO INSTEAD NOTHING");
        exec("INSERT INTO cec_r3 VALUES (1)");
        exec("DELETE FROM cec_r3");
        assertEquals("1", one("SELECT count(*) FROM cec_r3"));
        exec("DROP RULE cec_rr3 ON cec_r3");
        exec("DELETE FROM cec_r3");
        assertEquals("0", one("SELECT count(*) FROM cec_r3"));
        exec("DROP TABLE cec_r3");
    }

    /** A table that really is published, and has no replica identity, is still refused. */
    @Test
    void aPublishedTableWithoutAReplicaIdentityIsStillRefused() throws Exception {
        exec("DROP TABLE IF EXISTS cec_pub CASCADE");
        exec("CREATE TABLE cec_pub (id int)");
        exec("CREATE PUBLICATION cec_p FOR TABLE cec_pub");
        exec("INSERT INTO cec_pub VALUES (1)");
        try {
            assertEquals("55000", state("DELETE FROM cec_pub"));
        } finally {
            exec("DROP PUBLICATION cec_p");
            exec("DROP TABLE cec_pub");
        }
    }

    /** With a primary key to identify rows by, the same published table is fine. */
    @Test
    void aPublishedTableWithAPrimaryKeyIsAccepted() throws Exception {
        exec("DROP TABLE IF EXISTS cec_pub2 CASCADE");
        exec("CREATE TABLE cec_pub2 (id int PRIMARY KEY)");
        exec("CREATE PUBLICATION cec_p2 FOR TABLE cec_pub2");
        exec("INSERT INTO cec_pub2 VALUES (1)");
        try {
            exec("DELETE FROM cec_pub2");
            assertEquals("0", one("SELECT count(*) FROM cec_pub2"));
        } finally {
            exec("DROP PUBLICATION cec_p2");
            exec("DROP TABLE cec_pub2");
        }
    }

    // ---- A7: a registered text search configuration is usable ----

    /** If the configuration shows up in the catalog, the parser has to be able to use it. */
    @Test
    void aCreatedTextSearchConfigurationCanBeUsed() throws Exception {
        exec("DROP TEXT SEARCH CONFIGURATION IF EXISTS cec_cfg");
        exec("CREATE TEXT SEARCH CONFIGURATION cec_cfg (COPY = simple)");
        assertEquals("cec_cfg", one("SELECT cfgname FROM pg_ts_config WHERE cfgname='cec_cfg'"));
        assertEquals(one("SELECT to_tsvector('simple','hello world')::text"),
                     one("SELECT to_tsvector('cec_cfg','hello world')::text"));
        exec("DROP TEXT SEARCH CONFIGURATION cec_cfg");
    }

    @Test
    void aCopiedConfigurationWorksForToTsqueryToo() throws Exception {
        exec("DROP TEXT SEARCH CONFIGURATION IF EXISTS cec_cfg2");
        exec("CREATE TEXT SEARCH CONFIGURATION cec_cfg2 (COPY = english)");
        assertEquals(one("SELECT to_tsquery('english','running')::text"),
                     one("SELECT to_tsquery('cec_cfg2','running')::text"));
        exec("DROP TEXT SEARCH CONFIGURATION cec_cfg2");
    }

    /** Dropping it takes it back out of both the catalog and the parser's reach. */
    @Test
    void aDroppedConfigurationIsGoneFromBoth() throws Exception {
        exec("DROP TEXT SEARCH CONFIGURATION IF EXISTS cec_cfg3");
        exec("CREATE TEXT SEARCH CONFIGURATION cec_cfg3 (COPY = simple)");
        exec("DROP TEXT SEARCH CONFIGURATION cec_cfg3");
        assertEquals("0", one("SELECT count(*) FROM pg_ts_config WHERE cfgname='cec_cfg3'"));
        assertEquals("42704", state("SELECT to_tsvector('cec_cfg3','hello')"));
    }

    /** A name that was never created is an error, not a silent fallback. */
    @Test
    void anUnknownConfigurationIsAnError() {
        assertEquals("42704", state("SELECT to_tsvector('cec_nosuch','hello')"));
    }
}
