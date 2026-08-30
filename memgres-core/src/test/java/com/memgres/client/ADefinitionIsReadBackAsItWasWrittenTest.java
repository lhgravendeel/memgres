package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A definition is read back as it was written, and a name it holds is the name that was written.
 *
 * <p>Three things stand between a definition and its reading. A clause captured as text and
 * re-parsed later — an index predicate, a generated column's expression, a routine's body, a
 * trigger's WHEN — has to say again what it said, and the lexer's value is not that: a quoted
 * identifier has lost the quotes that made it one word. A clause with more than one spelling —
 * ROLLUP, CUBE and GROUPING SETS all fold to the same list of sets — has to be written back as the
 * spelling it was, because the folded form groups differently from what the reader wrote. And
 * BETWEEN is two comparisons joined, which PostgreSQL builds when it reads the statement, so what
 * is read back never holds a NOT at all.
 *
 * <p>And a name in a query is not always a reference to a relation. A WITH clause binds the name
 * for the query under it, and a reference written with a schema names the relation in that schema
 * and no other.
 */
class ADefinitionIsReadBackAsItWasWrittenTest {

    static Memgres memgres;
    static Connection conn;

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

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String one(String sql) throws SQLException {
        List<String> rows = rows(sql);
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int c = 1; c <= rs.getMetaData().getColumnCount(); c++) {
                    if (c > 1) row.append('/');
                    row.append(rs.getString(c));
                }
                out.add(row.toString());
            }
            return out;
        }
    }

    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** A quoted name is one word, and it is still one word when the clause is read again. */
    @Test
    void aQuotedNameSurvivesBeingCapturedAndReparsed() throws SQLException {
        exec("CREATE TABLE zdw_q (\"c c\" int)");
        exec("CREATE TABLE zdw_b (\"y z\" int)");
        exec("CREATE FUNCTION zdw_twf() RETURNS trigger LANGUAGE plpgsql"
                + " AS $$ BEGIN RETURN NEW; END $$");
        try {
            // An index predicate is captured as text and read again when the index is built.
            exec("CREATE INDEX zdw_qix ON zdw_q (\"c c\") WHERE \"c c\" > 0");
            // A generated column's expression is captured the same way.
            exec("CREATE TABLE zdw_g (\"a b\" int, t int GENERATED ALWAYS AS (\"a b\" * 2) STORED)");
            exec("CREATE TABLE zdw_r (\"select\" int,"
                    + " t int GENERATED ALWAYS AS (\"select\" * 2) STORED)");
            exec("INSERT INTO zdw_g (\"a b\") VALUES (21)");
            assertEquals("42", one("SELECT t FROM zdw_g"));
            // So is a routine's body, in both of the spellings that give one.
            exec("CREATE FUNCTION zdw_fb() RETURNS int LANGUAGE sql"
                    + " BEGIN ATOMIC SELECT max(\"y z\") FROM zdw_b; END");
            exec("CREATE FUNCTION zdw_f2() RETURNS int LANGUAGE sql"
                    + " RETURN (SELECT max(\"y z\") FROM zdw_b)");
            // And a trigger's WHEN clause.
            exec("CREATE TABLE zdw_tw (\"a b\" int, n int)");
            exec("CREATE TRIGGER zdw_tg BEFORE INSERT ON zdw_tw FOR EACH ROW"
                    + " WHEN (NEW.\"a b\" > 0) EXECUTE FUNCTION zdw_twf()");
        } finally {
            exec("DROP FUNCTION IF EXISTS zdw_fb()");
            exec("DROP FUNCTION IF EXISTS zdw_f2()");
            exec("DROP TABLE IF EXISTS zdw_g, zdw_r, zdw_tw CASCADE");
            exec("DROP TABLE zdw_q, zdw_b CASCADE");
            exec("DROP FUNCTION zdw_twf()");
        }
    }

    /** NOT BETWEEN is its own pair of comparisons, and a NOT never reaches what is stored. */
    @Test
    void notBetweenIsReadBackAsThePairOfComparisonsItIs() throws SQLException {
        exec("CREATE TABLE zdw_c (i int)");
        try {
            exec("ALTER TABLE zdw_c ADD CONSTRAINT zdw_ck1 CHECK (i NOT BETWEEN 1 AND 10)");
            exec("ALTER TABLE zdw_c ADD CONSTRAINT zdw_ck2"
                    + " CHECK (i NOT BETWEEN SYMMETRIC 1 AND 10)");
            exec("ALTER TABLE zdw_c ADD CONSTRAINT zdw_ck3 CHECK (i BETWEEN 1 AND 10)");
            assertEquals(List.of(
                            "zdw_ck1/CHECK (((i < 1) OR (i > 10)))",
                            "zdw_ck2/CHECK ((((i < 1) OR (i > 10)) AND ((i < 10) OR (i > 1))))",
                            "zdw_ck3/CHECK (((i >= 1) AND (i <= 10)))"),
                    rows("SELECT conname::text, pg_get_constraintdef(oid) FROM pg_constraint"
                            + " WHERE conrelid='zdw_c'::regclass ORDER BY conname"));
            exec("CREATE INDEX zdw_px ON zdw_c ((i + 1)) WHERE i NOT BETWEEN 1 AND 5");
            assertEquals("CREATE INDEX zdw_px ON public.zdw_c USING btree (((i + 1)))"
                            + " WHERE ((i < 1) OR (i > 5))",
                    one("SELECT pg_get_indexdef('zdw_px'::regclass)"));
        } finally {
            exec("DROP TABLE zdw_c CASCADE");
        }
    }

    /** ROLLUP, CUBE and GROUPING SETS are read back as the spelling each of them was. */
    @Test
    void aGroupingClauseIsReadBackAsTheSpellingItWas() throws SQLException {
        exec("CREATE TABLE zdw_t (a int, b int, c int)");
        try {
            exec("CREATE VIEW zdw_v1 AS SELECT a, sum(c) AS s FROM zdw_t"
                    + " GROUP BY GROUPING SETS ((a), ())");
            exec("CREATE VIEW zdw_v2 AS SELECT a, b, sum(c) AS s FROM zdw_t GROUP BY ROLLUP (a, b)");
            exec("CREATE VIEW zdw_v3 AS SELECT a, b, sum(c) AS s FROM zdw_t GROUP BY CUBE (a, b)");
            exec("CREATE VIEW zdw_v4 AS SELECT a, sum(c) AS s FROM zdw_t GROUP BY a, ROLLUP (b)");
            exec("CREATE VIEW zdw_v5 AS SELECT a, sum(c) AS s FROM zdw_t"
                    + " GROUP BY GROUPING SETS (a, b)");
            exec("CREATE VIEW zdw_v6 AS SELECT a, sum(c) AS s FROM zdw_t"
                    + " GROUP BY DISTINCT ROLLUP (a, b)");
            exec("CREATE VIEW zdw_v7 AS SELECT a, sum(c) AS s FROM zdw_t GROUP BY a, b");
            assertTrue(one("SELECT pg_get_viewdef('zdw_v1'::regclass)")
                    .contains("GROUP BY GROUPING SETS ((a), ())"));
            assertTrue(one("SELECT pg_get_viewdef('zdw_v2'::regclass)")
                    .contains("GROUP BY ROLLUP(a, b)"));
            assertTrue(one("SELECT pg_get_viewdef('zdw_v3'::regclass)")
                    .contains("GROUP BY CUBE(a, b)"));
            assertTrue(one("SELECT pg_get_viewdef('zdw_v4'::regclass)")
                    .contains("GROUP BY a, ROLLUP(b)"));
            // Every member of a GROUPING SETS is a set, so it is written back parenthesised.
            assertTrue(one("SELECT pg_get_viewdef('zdw_v5'::regclass)")
                    .contains("GROUP BY GROUPING SETS ((a), (b))"));
            assertTrue(one("SELECT pg_get_viewdef('zdw_v6'::regclass)")
                    .contains("GROUP BY DISTINCT ROLLUP(a, b)"));
            // A plain list is still a plain list.
            assertTrue(one("SELECT pg_get_viewdef('zdw_v7'::regclass)").contains("GROUP BY a, b"));
        } finally {
            exec("DROP VIEW zdw_v1, zdw_v2, zdw_v3, zdw_v4, zdw_v5, zdw_v6, zdw_v7");
            exec("DROP TABLE zdw_t");
        }
    }

    /** A name a WITH clause bound is that item's, whatever relation happens to share it. */
    @Test
    void aRenameLooksPastANameAWithClauseBound() throws SQLException {
        exec("CREATE TABLE zdw_base (a int)");
        exec("INSERT INTO zdw_base VALUES (1)");
        exec("CREATE VIEW zdw_cv AS WITH zdw_base AS (SELECT 42 AS a) SELECT a FROM zdw_base");
        try {
            exec("ALTER TABLE zdw_base RENAME TO zdw_base2");
            exec("UPDATE zdw_base2 SET a = 99");
            assertEquals("42", one("SELECT a FROM zdw_cv"));
        } finally {
            exec("DROP VIEW zdw_cv");
            exec("DROP TABLE zdw_base2");
        }
    }

    /** A reference written with a schema names the relation in that schema and no other. */
    @Test
    void aSchemaQualifiedSourceIsNotShadowedByATemporaryOfTheSameName() throws SQLException {
        exec("CREATE TABLE zdw_pp (a int)");
        exec("INSERT INTO zdw_pp VALUES (7)");
        exec("CREATE TEMP TABLE zdw_pp (a int)");
        try {
            exec("CREATE VIEW zdw_tv AS SELECT a FROM public.zdw_pp");
            assertEquals("public", one("SELECT table_schema FROM information_schema.views"
                    + " WHERE table_name='zdw_tv'"));
            assertEquals("7", one("SELECT a FROM zdw_tv"));
        } finally {
            exec("DROP VIEW zdw_tv");
            exec("DROP TABLE pg_temp.zdw_pp");
            exec("DROP TABLE public.zdw_pp");
        }
    }

    /** A definition is analysed when it is written, whether or not it is filled now. */
    @Test
    void aDefinitionsQueryIsAnalysedEvenWhenNoDataIsAsked() throws SQLException {
        exec("CREATE TABLE zdw_u (a int)");
        try {
            assertEquals("42P01", stateOf(
                    "CREATE MATERIALIZED VIEW zdw_mv AS SELECT * FROM zdw_nosuchtable WITH NO DATA"));
            assertEquals("42703", stateOf(
                    "CREATE MATERIALIZED VIEW zdw_mv2 AS SELECT nosuchcol FROM zdw_u WITH NO DATA"));
            assertEquals("0", one("SELECT count(*)::int FROM pg_class"
                    + " WHERE relname IN ('zdw_mv','zdw_mv2')"));
            // One that names what is there is made, and holds no rows until it is refreshed.
            exec("CREATE MATERIALIZED VIEW zdw_mv3 AS SELECT a FROM zdw_u WITH NO DATA");
            assertEquals("1", one("SELECT count(*)::int FROM pg_class WHERE relname='zdw_mv3'"));
        } finally {
            exec("DROP MATERIALIZED VIEW IF EXISTS zdw_mv3");
            exec("DROP TABLE zdw_u");
        }
    }
}
