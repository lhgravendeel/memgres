package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What an object's schema means once two schemas may each hold a relation of the same name.
 *
 * <p>Giving sequences and indexes a schema of their own made a collision possible that could not
 * happen before, and the code that moves an object with its table did not check for it: {@code
 * ALTER TABLE a.t SET SCHEMA b} overwrote whatever {@code b} already held under the moved index's
 * or sequence's name, counter and all. PostgreSQL 18 refuses the whole statement with {@code
 * 42P07 relation "x" already exists in schema "b"} and leaves both schemas as it found them.
 *
 * <p>The same schema-awareness has to reach the places that only <em>read</em> a name, and did
 * not:
 *
 * <ul>
 *   <li>A column default draws from one particular sequence, wherever that sequence lives. Only
 *       the sequence's own schema was searched for dependents, so {@code DROP SEQUENCE} on a
 *       sequence another schema's table defaults from succeeded where PostgreSQL answers {@code
 *       2BP01}, and the next INSERT died on the default's own text with {@code 22P02}.</li>
 *   <li>{@code ALTER SEQUENCE ... SET SCHEMA} moved the sequence but left every default pointing
 *       at where it used to be. PostgreSQL's defaults reference the sequence itself and follow
 *       it; here the stored text is rewritten to say the same thing.</li>
 *   <li>{@code SELECT last_value FROM a.s} and {@code pg_get_indexdef('b.i'::regclass)} both
 *       resolved by bare name and answered about the other schema's object.</li>
 *   <li>{@code ALTER INDEX a.t_pkey RENAME TO ...} compared the whole written name against bare
 *       constraint names and so was refused for every constraint-backed index.</li>
 *   <li>{@code nextval('pg_temp.s')} took {@code pg_temp} for a literal schema name rather than
 *       the alias this session's temporary schema answers to.</li>
 * </ul>
 *
 * <p>The last group is what a relation of the wrong kind is answered with. PostgreSQL resolves
 * the name first and opens the relation second, so naming an index in a TRUNCATE, a FROM or an
 * {@code ALTER INDEX ... SET} is {@code 42809} naming what the object really is — not {@code
 * 42P01}, which says the name reaches nothing and sends the reader after a relation that is
 * sitting right there. A wrong-kind DROP additionally carries the hint that names the statement
 * that would have worked.
 *
 * <p>Every expectation here was measured on PostgreSQL 18 before it was written down.
 */
class RelationNamespaceFollowUpTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void freshSchemas() throws SQLException {
        exec("SET search_path = public");
        exec("DROP SCHEMA IF EXISTS rfa CASCADE");
        exec("DROP SCHEMA IF EXISTS rfb CASCADE");
        exec("CREATE SCHEMA rfa");
        exec("CREATE SCHEMA rfb");
    }

    // ---- ALTER TABLE ... SET SCHEMA carries the table's objects, or refuses ----

    @Test
    void movingATableOntoAnIndexNameAlreadyTakenIsRefused() throws SQLException {
        exec("CREATE TABLE rfa.t (c int)");
        exec("CREATE TABLE rfb.other (c int)");
        exec("CREATE INDEX ix ON rfa.t (c)");
        exec("CREATE INDEX ix ON rfb.other (c)");

        assertEquals("42P07", state("ALTER TABLE rfa.t SET SCHEMA rfb"));
        assertTrue(message("ALTER TABLE rfa.t SET SCHEMA rfb")
                .contains("already exists in schema \"rfb\""));
        // Nothing moved and nothing was destroyed: both indexes are still where they were.
        assertEquals("rfa|t|ix;;rfb|other|ix", rows(
                "SELECT schemaname || '|' || tablename || '|' || indexname FROM pg_indexes"
                        + " WHERE indexname='ix' ORDER BY 1"));
        assertEquals(2L, one("SELECT count(*) FROM pg_class WHERE relname='ix'"));
        assertEquals("rfa", one("SELECT n.nspname FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='t'"));
    }

    @Test
    void movingASerialTableOntoASequenceNameAlreadyTakenIsRefused() throws SQLException {
        exec("CREATE TABLE rfa.t (id serial, v text)");
        exec("CREATE SEQUENCE rfb.t_id_seq");
        exec("SELECT setval('rfb.t_id_seq', 900)");

        assertEquals("42P07", state("ALTER TABLE rfa.t SET SCHEMA rfb"));
        // The 900 is the whole point: an overwriting move lost it and reset the counter to 1.
        assertEquals(900L, one("SELECT last_value FROM rfb.t_id_seq"));
        assertEquals(2L, one("SELECT count(*) FROM pg_class WHERE relname='t_id_seq'"));
    }

    @Test
    void movingATableWithAFreeNameCarriesItsSequenceAndIndexes() throws SQLException {
        exec("CREATE TABLE rfa.t (id serial, v text)");
        exec("CREATE INDEX vx ON rfa.t (v)");
        exec("INSERT INTO rfa.t (v) VALUES ('a')");

        exec("ALTER TABLE rfa.t SET SCHEMA rfb");

        exec("INSERT INTO rfb.t (v) VALUES ('b')");
        assertEquals("1;;2", rows("SELECT id FROM rfb.t ORDER BY id"));
        assertEquals("rfb|t|vx", one(
                "SELECT schemaname || '|' || tablename || '|' || indexname"
                        + " FROM pg_indexes WHERE indexname='vx'"));
        assertEquals(2L, one("SELECT last_value FROM rfb.t_id_seq"));
    }

    // ---- a default draws from one sequence, wherever it lives ----

    @Test
    void aSequenceAnotherSchemaDefaultsFromCannotBeDroppedOutFromUnderIt() throws SQLException {
        exec("CREATE SEQUENCE pubseq");
        exec("CREATE TABLE rfa.t (id int DEFAULT nextval('pubseq'), v text)");

        assertEquals("2BP01", state("DROP SEQUENCE pubseq"));
        assertEquals(1L, one("SELECT count(*) FROM pg_class WHERE relname='pubseq'"));
        // The sequence survived, so the INSERT still has a number to draw.
        exec("INSERT INTO rfa.t (v) VALUES ('a')");
        assertEquals(1L, one("SELECT count(*) FROM rfa.t"));
        exec("DROP SEQUENCE pubseq CASCADE");
        assertNull(one("SELECT column_default FROM information_schema.columns"
                + " WHERE table_schema='rfa' AND table_name='t' AND column_name='id'"));
        // CASCADE cleared the default rather than stranding it, so the next INSERT still works.
        exec("INSERT INTO rfa.t (v) VALUES ('b')");
        assertEquals(2L, one("SELECT count(*) FROM rfa.t"));
    }

    @Test
    void theDependencyMessageNamesTheColumnAndTheTable() {
        try {
            exec("CREATE SEQUENCE pubseq2");
            exec("CREATE TABLE rfa.dep (id int DEFAULT nextval('pubseq2'))");
            SQLException e = assertThrows(SQLException.class, () -> exec("DROP SEQUENCE pubseq2"));
            // PostgreSQL qualifies a table the search path does not reach, and only then.
            assertTrue(e.getMessage().contains(
                            "default value for column id of table rfa.dep depends on sequence pubseq2"),
                    e.getMessage());
            assertTrue(e.getMessage().contains("Use DROP ... CASCADE"), e.getMessage());
        } catch (SQLException setupFailed) {
            fail(setupFailed);
        }
    }

    @Test
    void movingASequenceTakesItsDefaultsWithIt() throws SQLException {
        exec("CREATE SEQUENCE movingseq");
        exec("CREATE TABLE public.mt (id int DEFAULT nextval('movingseq'), v text)");
        exec("INSERT INTO public.mt (v) VALUES ('a')");

        exec("ALTER SEQUENCE movingseq SET SCHEMA rfa");

        exec("INSERT INTO public.mt (v) VALUES ('b')");
        assertEquals("1;;2", rows("SELECT id FROM public.mt ORDER BY id"));
        assertEquals("nextval('rfa.movingseq'::regclass)",
                one("SELECT column_default FROM information_schema.columns"
                        + " WHERE table_schema='public' AND table_name='mt' AND column_name='id'"));
        exec("DROP TABLE public.mt");
        exec("DROP SEQUENCE rfa.movingseq");
    }

    // ---- reads answer about the object the name really reaches ----

    @Test
    void readingASequenceAsARelationHonoursTheSchema() throws SQLException {
        exec("CREATE SEQUENCE public.rs");
        exec("CREATE SEQUENCE rfa.rs");
        exec("SELECT setval('rfa.rs', 100)");
        exec("SELECT setval('public.rs', 5)");

        assertEquals(100L, one("SELECT last_value FROM rfa.rs"));
        assertEquals(5L, one("SELECT last_value FROM public.rs"));
        exec("SET search_path = rfa");
        assertEquals(100L, one("SELECT last_value FROM rs"));
        exec("SET search_path = public");
        // A schema that holds no sequence of the name answers with nothing at all, not with a
        // phantom row borrowed from public.
        assertEquals("42P01", state("SELECT last_value FROM rfb.rs"));
        exec("DROP SEQUENCE public.rs");
    }

    @Test
    void pgGetIndexdefAnswersAboutTheIndexTheOidNames() throws SQLException {
        exec("CREATE TABLE rfa.t (a int, b int)");
        exec("CREATE TABLE rfb.t (a int, b int)");
        exec("CREATE INDEX i4 ON rfa.t (a)");
        exec("CREATE UNIQUE INDEX i4 ON rfb.t (b)");

        assertEquals("CREATE UNIQUE INDEX i4 ON rfb.t USING btree (b)",
                one("SELECT pg_get_indexdef('rfb.i4'::regclass)"));
        assertEquals("CREATE INDEX i4 ON rfa.t USING btree (a)",
                one("SELECT pg_get_indexdef('rfa.i4'::regclass)"));
    }

    @Test
    void aQualifiedRenameReachesAConstraintBackedIndex() throws SQLException {
        exec("CREATE TABLE rfa.t (id int CONSTRAINT pk1 PRIMARY KEY, u int CONSTRAINT uq1 UNIQUE)");

        exec("ALTER INDEX rfa.pk1 RENAME TO pk2");
        assertEquals("rfa|pk2", one("SELECT schemaname || '|' || indexname FROM pg_indexes"
                + " WHERE indexname IN ('pk1','pk2')"));
        // The qualifier still has to be honoured: a schema that holds no such index is a miss.
        assertEquals("42P01", state("ALTER INDEX rfb.uq1 RENAME TO uq2"));
        assertEquals("rfa|uq1", one("SELECT schemaname || '|' || indexname FROM pg_indexes"
                + " WHERE indexname IN ('uq1','uq2')"));
    }

    @Test
    void nextvalReachesATemporarySequenceThroughPgTemp() throws SQLException {
        exec("CREATE TEMP SEQUENCE tseq");
        assertEquals("1", str("SELECT nextval('tseq')"));
        assertEquals("2", str("SELECT nextval('pg_temp.tseq')"));
        assertEquals("3", str("SELECT nextval('tseq')"));
        exec("DROP SEQUENCE pg_temp.tseq");
    }

    // ---- a relation of the wrong kind is refused for what it is ----

    @Test
    void alterViewOnATableSaysItIsNotAView() throws SQLException {
        exec("CREATE TABLE rfa.w (a int)");
        assertEquals("42809", state("ALTER VIEW rfa.w ALTER COLUMN a SET DEFAULT 1"));
        assertTrue(message("ALTER VIEW rfa.w ALTER COLUMN a SET DEFAULT 1")
                .contains("\"w\" is not a view"));
    }

    @Test
    void alterMaterializedViewOnATableNamesTheKindItWanted() throws SQLException {
        exec("CREATE TABLE rfa.w2 (a int)");
        exec("CREATE VIEW rfa.v2 AS SELECT 1 AS x");
        assertEquals("42809", state("ALTER MATERIALIZED VIEW rfa.w2 RENAME TO w2r"));
        assertTrue(message("ALTER MATERIALIZED VIEW rfa.w2 RENAME TO w2r")
                .contains("\"w2\" is not a materialized view"));
        // A plain view is the wrong kind for it too, and the message says which kind was wanted.
        assertTrue(message("ALTER MATERIALIZED VIEW rfa.v2 RENAME TO v2r")
                .contains("\"v2\" is not a materialized view"));
    }

    @Test
    void alterIndexSetOnATableSaysItIsNotAnIndex() throws SQLException {
        exec("CREATE TABLE rfa.w3 (a int)");
        assertEquals("42809", state("ALTER INDEX rfa.w3 SET (fillfactor = 50)"));
        assertEquals("42809", state("ALTER INDEX rfa.w3 RESET (fillfactor)"));
        assertTrue(message("ALTER INDEX rfa.w3 SET (fillfactor = 50)")
                .contains("\"w3\" is not an index"));
        // The rename is the one action PostgreSQL leaves kind-blind: it renames the table.
        exec("ALTER INDEX rfa.w3 RENAME TO w3r");
        assertEquals(1L, one("SELECT count(*) FROM pg_class c JOIN pg_namespace n"
                + " ON n.oid=c.relnamespace WHERE n.nspname='rfa' AND c.relname='w3r'"));
    }

    @Test
    void truncatingAnIndexOrASequenceSaysItIsNotATable() throws SQLException {
        exec("CREATE TABLE rfa.tb (a int)");
        exec("CREATE INDEX ti ON rfa.tb (a)");
        exec("CREATE SEQUENCE rfa.ts");
        assertEquals("42809", state("TRUNCATE rfa.ti"));
        assertEquals("42809", state("TRUNCATE rfa.ts"));
        assertTrue(message("TRUNCATE rfa.ti").contains("\"ti\" is not a table"));
        assertTrue(message("TRUNCATE rfa.ts").contains("\"ts\" is not a table"));
    }

    @Test
    void readingOrWritingAnIndexCannotOpenIt() throws SQLException {
        exec("CREATE TABLE rfa.rb (a int)");
        exec("CREATE INDEX ri ON rfa.rb (a)");
        assertEquals("42809", state("SELECT * FROM rfa.ri"));
        assertEquals("42809", state("INSERT INTO rfa.ri VALUES (1)"));
        assertTrue(message("SELECT * FROM rfa.ri").contains("cannot open relation \"ri\""));
        assertTrue(message("SELECT * FROM rfa.ri")
                .contains("This operation is not supported for indexes"));
        // Unqualified, through the search path, reaches the same index and the same refusal.
        exec("SET search_path = rfa");
        assertEquals("42809", state("SELECT * FROM ri"));
        exec("SET search_path = public");
    }

    @Test
    void aWrongKindDropNamesTheStatementThatWouldHaveWorked() throws SQLException {
        exec("CREATE TABLE rfa.hb (a int)");
        exec("CREATE INDEX hi ON rfa.hb (a)");
        exec("CREATE SEQUENCE rfa.hs");
        exec("CREATE VIEW rfa.hv AS SELECT 1 AS x");
        exec("CREATE MATERIALIZED VIEW rfa.hm AS SELECT 1 AS x");

        assertHint("DROP TABLE rfa.hi", "Use DROP INDEX to remove an index.");
        assertHint("DROP TABLE rfa.hs", "Use DROP SEQUENCE to remove a sequence.");
        assertHint("DROP TABLE rfa.hv", "Use DROP VIEW to remove a view.");
        assertHint("DROP TABLE rfa.hm", "Use DROP MATERIALIZED VIEW to remove a materialized view.");
        assertHint("DROP INDEX rfa.hb", "Use DROP TABLE to remove a table.");
        assertHint("DROP SEQUENCE rfa.hb", "Use DROP TABLE to remove a table.");
        assertHint("DROP VIEW rfa.hm", "Use DROP MATERIALIZED VIEW to remove a materialized view.");
        assertHint("DROP MATERIALIZED VIEW rfa.hv", "Use DROP VIEW to remove a view.");
    }

    // ---- helpers ----

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /** The SQLSTATE the statement fails with; fails the test when it succeeds. */
    private static String state(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        } catch (SQLException e) {
            return e.getSQLState();
        }
        return fail("expected " + sql + " to be refused");
    }

    /** The whole message — including the Detail and Hint lines — the statement fails with. */
    private static String message(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        } catch (SQLException e) {
            return e.getMessage();
        }
        return fail("expected " + sql + " to be refused");
    }

    private static void assertHint(String sql, String hint) {
        String msg = message(sql);
        assertEquals("42809", state(sql), sql);
        assertTrue(msg.contains(hint), sql + " -> " + msg);
    }

    /** The first column of the first row as text; nextval comes back as a string in simple mode. */
    private static String str(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "no row from " + sql);
            return rs.getString(1);
        }
    }

    private static Object one(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "no row from " + sql);
            Object v = rs.getObject(1);
            if (v instanceof Integer) return ((Integer) v).longValue();
            if (v instanceof java.math.BigDecimal) return ((java.math.BigDecimal) v).longValue();
            return v;
        }
    }

    /** Every row's first column, joined with {@code ;;}. */
    private static String rows(String sql) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                if (sb.length() > 0) sb.append(";;");
                sb.append(rs.getString(1));
            }
        }
        return sb.toString();
    }
}
