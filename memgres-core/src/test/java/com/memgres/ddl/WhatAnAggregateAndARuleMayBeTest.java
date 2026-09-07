package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What kind of aggregate a declaration makes, and what a rule's action may be.
 *
 * <p>An ordered-set aggregate is written {@code (direct args ORDER BY sort args)} and is a kind of
 * its own: PostgreSQL records it as kind "o" and says how many of its arguments stand in front of
 * WITHIN GROUP. Read and thrown away, every such aggregate was recorded as an ordinary one taking
 * no direct arguments -- and the ORDER BY in a DROP was swallowed into the argument beside it, so
 * DROP AGGREGATE could not name an aggregate the same session had just created.
 *
 * <p>A rule rewrites a query into other queries, so its action is one of those: a SELECT, an
 * INSERT, an UPDATE, a DELETE or a NOTIFY, and nothing else. PostgreSQL's grammar has no place for
 * anything more, so a CREATE or a DROP written there is a syntax error at the word itself. Read as
 * a command like any other, the rule was accepted and fired DDL on every insert.
 */
class WhatAnAggregateAndARuleMayBeTest {

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

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** An ordered-set aggregate is recorded as one, and can be dropped by its own signature. */
    @Test
    void whatKindOfAggregateADeclarationMakes() throws SQLException {
        exec("CREATE AGGREGATE zwm_os (float8 ORDER BY float8) (SFUNC = ordered_set_transition,"
                + " STYPE = internal, FINALFUNC = percentile_cont_float8_final)");
        assertEquals("o|1", one("SELECT aggkind::text || '|' || aggnumdirectargs::text"
                + " FROM pg_aggregate WHERE aggfnoid::regproc::text = 'zwm_os'"));
        assertNull(stateOf("DROP AGGREGATE zwm_os(float8 ORDER BY float8)"));
        // An ordinary aggregate is kind "n" and has no direct arguments.
        exec("CREATE FUNCTION zwm_add(int,int) RETURNS int LANGUAGE sql IMMUTABLE"
                + " AS $$ SELECT $1+$2 $$");
        exec("CREATE AGGREGATE zwm_n (int) (SFUNC = zwm_add, STYPE = int)");
        assertEquals("n|0", one("SELECT aggkind::text || '|' || aggnumdirectargs::text"
                + " FROM pg_aggregate WHERE aggfnoid::regproc::text = 'zwm_n'"));
        exec("DROP AGGREGATE zwm_n(int)");
        exec("DROP FUNCTION zwm_add(int,int)");
    }

    /** A rule's action is a query, and anything else is a syntax error where it stands. */
    @Test
    void whatARulesActionMayBe() throws SQLException {
        assertEquals("42601", stateOf("CREATE RULE zwm_e AS ON INSERT TO zwm_nosuch"
                + " DO ALSO CREATE TABLE zwm_bad (i int)"));
        assertEquals("42601", stateOf("CREATE RULE zwm_f AS ON INSERT TO zwm_nosuch"
                + " DO ALSO DROP TABLE zwm_bad"));
        // A query is read as it always was, and the relation is what is missing.
        assertEquals("42P01", stateOf("CREATE RULE zwm_g AS ON INSERT TO zwm_nosuch"
                + " DO ALSO NOTIFY zwm_ch"));
        assertEquals("42P01", stateOf("CREATE RULE zwm_h AS ON INSERT TO zwm_nosuch"
                + " DO ALSO SELECT 1"));
        // And over a relation that is there, the rule is made.
        exec("CREATE TABLE zwm_r (i int)");
        exec("CREATE TABLE zwm_log (i int)");
        assertNull(stateOf("CREATE RULE zwm_ok AS ON INSERT TO zwm_r"
                + " DO ALSO INSERT INTO zwm_log VALUES (NEW.i)"));
        exec("INSERT INTO zwm_r VALUES (7)");
        assertEquals("7", one("SELECT i::text FROM zwm_log"));
        exec("DROP TABLE zwm_r");
        exec("DROP TABLE zwm_log");
    }
}
