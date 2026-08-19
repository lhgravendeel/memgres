package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.util.PSQLException;

import java.sql.*;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A name is folded once, where it is written, and matched as it stands.
 *
 * <p>PostgreSQL lower-cases an unquoted identifier as it reads it and keeps a quoted one exactly as
 * written; every catalogue and every lookup downstream then compares the result as it is. So a
 * relation, a routine, a constraint, an alias and a window declared under a quoted name answer to
 * that name and to no other spelling of it. Folding a name a second time — or matching it whatever
 * its case — undoes the distinction the quotes were written to make: it let {@code "ZzView"} be
 * reached as zzview, and refused {@code "ZzFn"()}, which is the only way that function can be
 * called.
 *
 * <p>The fold is a property of the language rather than of the machine the engine runs on, so it is
 * done in the root locale. Under the default one a Turkish JVM reads {@code IN} as {@code İN},
 * which is no keyword, and {@code MIN} as {@code mın}, which is no aggregate.
 *
 * <p>Every value here was measured against PostgreSQL 18.
 */
class NameFoldingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE nf_a (a int CONSTRAINT \"ZzCk\" CHECK (a > 0))");
            st.execute("INSERT INTO nf_a VALUES (1), (2), (3)");
            st.execute("CREATE VIEW \"ZzView\" AS SELECT 1 AS a");
            st.execute("CREATE FUNCTION \"ZzFn\"() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$");
            st.execute("CREATE FUNCTION zzfn2() RETURNS int LANGUAGE sql AS $$ SELECT 2 $$");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** The first column of the first row, rendered as text. */
    private static String one(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            String v = rs.getString(1);
            return rs.wasNull() ? null : v;
        }
    }

    private static String stateOf(String sql) {
        PSQLException e = assertThrows(PSQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }, "expected a refusal from: " + sql);
        return e.getSQLState();
    }

    @Test
    void unquotedRelationNameIsFoldedHoweverItIsWritten() throws Exception {
        assertEquals("3", one("SELECT count(*) FROM nf_a"));
        assertEquals("3", one("SELECT count(*) FROM NF_A"));
        assertEquals("3", one("SELECT count(*) FROM Nf_A"));
        // Quotes ask for the name as written, which is this relation only when what they hold is
        // what the fold produced.
        assertEquals("3", one("SELECT count(*) FROM \"nf_a\""));
        assertEquals("42P01", stateOf("SELECT count(*) FROM \"NF_A\""));
    }

    @Test
    void quotedRelationNameKeepsTheCaseItWasCreatedWith() throws Exception {
        assertEquals("1", one("SELECT count(*) FROM \"ZzView\""));
        assertEquals("42P01", stateOf("SELECT count(*) FROM zzview"));
        assertEquals("42P01", stateOf("SELECT count(*) FROM \"zzview\""));
        assertEquals("42P01", stateOf("SELECT count(*) FROM ZzView"));
    }

    @Test
    void columnNameIsFoldedWhereItIsWritten() throws Exception {
        assertEquals("1", one("SELECT a FROM nf_a WHERE A = 1"));
        assertEquals("1", one("SELECT \"a\" FROM nf_a WHERE a = 1"));
        assertEquals("42703", stateOf("SELECT \"A\" FROM nf_a WHERE a = 1"));
    }

    @Test
    void routineAnswersToTheNameItWasDeclaredUnder() throws Exception {
        assertEquals("1", one("SELECT \"ZzFn\"()"));
        assertEquals("42883", stateOf("SELECT zzfn()"));
        assertEquals("42883", stateOf("SELECT \"zzfn\"()"));
        assertEquals("42883", stateOf("SELECT ZzFn()"));
    }

    @Test
    void routineDeclaredUnquotedIsFoundByEverySpellingThatFoldsToIt() throws Exception {
        assertEquals("2", one("SELECT zzfn2()"));
        assertEquals("2", one("SELECT ZZFN2()"));
        assertEquals("2", one("SELECT \"zzfn2\"()"));
        assertEquals("42883", stateOf("SELECT \"ZzFn2\"()"));
    }

    /** The column a call is described by has the type the routine returns, not a default. */
    @Test
    void quotedRoutineCallIsDescribedByItsDeclaredReturnType() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT \"ZzFn\"() AS v")) {
            assertEquals(Types.INTEGER, rs.getMetaData().getColumnType(1));
        }
    }

    @Test
    void builtinIsANameOnTheSameTerms() throws Exception {
        assertEquals("1", one("SELECT abs(-1)"));
        assertEquals("1", one("SELECT ABS(-1)"));
        assertEquals("1", one("SELECT AbS(-1)"));
        assertEquals("1", one("SELECT \"abs\"(-1)"));
        assertEquals("42883", stateOf("SELECT \"ABS\"(-1)"));
    }

    @Test
    void aliasKeepsTheCaseItWasWrittenIn() throws Exception {
        assertEquals("1", one("SELECT x.a FROM nf_a x ORDER BY 1"));
        assertEquals("1", one("SELECT X.a FROM nf_a x ORDER BY 1"));
        assertEquals("1", one("SELECT \"x\".a FROM nf_a x ORDER BY 1"));
        assertEquals("42P01", stateOf("SELECT \"X\".a FROM nf_a x ORDER BY 1"));
        assertEquals("1", one("SELECT \"Q\".a FROM nf_a AS \"Q\" ORDER BY 1"));
        assertEquals("42P01", stateOf("SELECT q.a FROM nf_a AS \"Q\" ORDER BY 1"));
    }

    /** An alias is the only name the relation has left inside the query. */
    @Test
    void aliasHidesTheRelationsOwnName() {
        assertEquals("42P01", stateOf("SELECT nf_a.a FROM nf_a AS \"Q\" ORDER BY 1"));
    }

    @Test
    void windowIsTheWindowOfThatName() throws Exception {
        assertEquals("1", one("SELECT sum(a) OVER w FROM nf_a WINDOW w AS (ORDER BY a) ORDER BY 1"));
        assertEquals("1", one("SELECT sum(a) OVER W FROM nf_a WINDOW w AS (ORDER BY a) ORDER BY 1"));
        assertEquals("1",
                one("SELECT sum(a) OVER \"w\" FROM nf_a WINDOW w AS (ORDER BY a) ORDER BY 1"));
        assertEquals("42704",
                stateOf("SELECT sum(a) OVER \"W\" FROM nf_a WINDOW w AS (ORDER BY a) ORDER BY 1"));
    }

    /** "W" and w are two windows; "w" and w are one window defined twice. */
    @Test
    void twoWindowNamesCollideOnlyWhenTheyAreTheSameName() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT a, count(*) OVER \"W\", count(*) OVER \"w\" "
                     + "FROM nf_a WINDOW \"W\" AS (), \"w\" AS (PARTITION BY a) ORDER BY a")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(1, rs.getInt(3));
        }
        assertEquals("1", one("SELECT a FROM nf_a WINDOW w AS (), \"W\" AS () ORDER BY a"));
        assertEquals("42P20", stateOf("SELECT a FROM nf_a WINDOW w AS (), \"w\" AS () ORDER BY a"));
    }

    @Test
    void constraintIsNamedAsItWasWritten() throws Exception {
        assertEquals("ZzCk", one(
                "SELECT conname FROM pg_constraint WHERE conrelid = 'nf_a'::regclass"));
        assertEquals("42704", stateOf("ALTER TABLE nf_a DROP CONSTRAINT zzck"));
        assertEquals("42704", stateOf("ALTER TABLE nf_a DROP CONSTRAINT \"zzck\""));
    }

    /**
     * The fold belongs to the language, not to the machine. In a Turkish locale {@code I} folds to
     * {@code ı} and {@code i} to {@code İ}, so a fold taken in the default locale stopped IN from
     * being a keyword and MIN from being an aggregate.
     */
    @Test
    void foldDoesNotFollowTheDefaultLocale() throws Exception {
        Locale before = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            assertEquals("t", one("SELECT 1 IN (1, 2) AS r"));
            assertEquals("1", one("SELECT MIN(a) FROM nf_a"));
            assertEquals("3", one("SELECT MAX(A) FROM NF_A"));
            assertEquals("ABC", one("SELECT UPPER('abc')"));
        } finally {
            Locale.setDefault(before);
        }
    }
}
