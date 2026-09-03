package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Words that only look like SQL, trigger functions the server provides, and how wide a power is.
 *
 * <p>A function body and a generation expression are checked by reading them as text, and a word
 * inside a string literal is not a word of the statement: {@code SELECT 'COLLATE nosuch'} names no
 * collation. Nor is the name behind a cast marker a column — it is the type being cast to.
 *
 * <p>How wide a power is, is its base's logarithm times the exponent: counting the base's digits
 * instead made {@code 10^100000} twice as wide as it is.
 */
class BodyTextTriggersAndPowersTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
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

    /** A word inside a string literal is not a word of the statement. */
    @Test
    void wordsInsideAStringAreNotWordsOfTheStatement() throws SQLException {
        assertNull(stateOf("CREATE FUNCTION zbt_f6() RETURNS text LANGUAGE sql"
                + " AS $$ SELECT 'COLLATE nosuchcollation_zbt' $$"));
        assertEquals("COLLATE nosuchcollation_zbt", one("SELECT zbt_f6()"));
        assertNull(stateOf("CREATE FUNCTION zbt_f7() RETURNS text LANGUAGE sql"
                + " AS $$ SELECT 'x::nosuchtype' $$"));
        assertEquals("x::nosuchtype", one("SELECT zbt_f7()"));
        // Written outside the quotes the same words are read as the statement they are.
        assertEquals("42704", stateOf("CREATE FUNCTION zbt_f8() RETURNS text LANGUAGE sql"
                + " AS $$ SELECT 'x'::nosuchtype $$"));
        exec("DROP FUNCTION zbt_f6()");
        exec("DROP FUNCTION zbt_f7()");
    }

    /** The name behind a cast marker is a type, not a column. */
    @Test
    void aCastTargetIsNotAColumnReference() throws SQLException {
        assertNull(stateOf("CREATE TABLE zbt_gu (s text, g uuid GENERATED ALWAYS AS (s::uuid) STORED)"));
        assertNull(stateOf("CREATE TABLE zbt_gi (s text, g int GENERATED ALWAYS AS (s::int) STORED)"));
        exec("DROP TABLE zbt_gu");
        exec("DROP TABLE zbt_gi");
    }

    /** The server provides trigger functions of its own. */
    @Test
    void aTriggerFunctionTheServerProvides() throws SQLException {
        exec("CREATE TABLE zbt_ts (body text, v tsvector)");
        exec("CREATE TRIGGER zbt_tsv BEFORE INSERT OR UPDATE ON zbt_ts FOR EACH ROW"
                + " EXECUTE FUNCTION tsvector_update_trigger(v, 'pg_catalog.english', body)");
        exec("INSERT INTO zbt_ts (body) VALUES ('The quick brown fox')");
        assertEquals("'brown':3 'fox':4 'quick':2", one("SELECT v::text FROM zbt_ts"));
        exec("DROP TABLE zbt_ts");
    }

    /** How wide a power is, is the base's logarithm times the exponent. */
    @Test
    void howWideAPowerIs() throws SQLException {
        assertEquals("100001", one("SELECT length((10::numeric ^ 100000::numeric)::text)::text"));
        assertEquals("30103", one("SELECT length((2::numeric ^ 100000::numeric)::text)::text"));
        assertEquals("1001", one("SELECT length((10::numeric ^ 1000::numeric)::text)::text"));
        // Past what numeric can hold it is still an overflow.
        assertEquals("22003", stateOf("SELECT 10::numeric ^ 200000::numeric"));
    }
}
