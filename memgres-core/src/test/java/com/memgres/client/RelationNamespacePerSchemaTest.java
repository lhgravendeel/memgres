package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A sequence and an index each belong to one schema, and every answer follows from that.
 *
 * <p>Memgres kept sequences and index metadata in maps keyed by the bare name alone, one map for
 * the whole database, so a name could only be used once anywhere. That is wrong in both
 * directions, and both were measured against PostgreSQL 18 before anything was changed:
 *
 * <ul>
 *   <li>Two schemas could not each hold a sequence or an index of the same name — the second
 *       CREATE was refused with {@code 42P07} for SQL PostgreSQL accepts.</li>
 *   <li>A qualified name that named the wrong schema still found the object. {@code DROP SEQUENCE
 *       b.s} destroyed the sequence really living in {@code a}; {@code nextval('b.s')} advanced
 *       it; {@code ALTER SEQUENCE b.s RENAME TO t} renamed it <em>and</em> relocated it into
 *       {@code public}, where it then survived {@code DROP SCHEMA a CASCADE}. PostgreSQL answers
 *       {@code 42P01} to every one of those and leaves the object alone.</li>
 *   <li>Two tables in different schemas, each with a {@code serial} column, shared one counter,
 *       so the second table's first row got id 2. PostgreSQL gives each its own sequence.</li>
 * </ul>
 *
 * <p>The converse has to keep working, and is asserted here too: an unqualified name still
 * resolves through the search path, a quoted name differing only in case is still a second
 * object, and a name is still taken across relation kinds <em>within</em> one schema.
 */
class RelationNamespacePerSchemaTest {

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
        exec("DROP SCHEMA IF EXISTS nsa CASCADE");
        exec("DROP SCHEMA IF EXISTS nsb CASCADE");
        exec("CREATE SCHEMA nsa");
        exec("CREATE SCHEMA nsb");
    }

    // ---- sequences ----

    @Test
    void twoSchemasEachHoldASequenceOfTheSameName() throws SQLException {
        exec("CREATE SEQUENCE nsa.sq");
        exec("CREATE SEQUENCE nsb.sq");
        assertEquals(2L, one("SELECT count(*) FROM pg_class WHERE relname='sq'"));
        assertEquals("1", str("SELECT nextval('nsa.sq')"));
        assertEquals("2", str("SELECT nextval('nsa.sq')"));
        // nsb's counter is its own and has not moved.
        assertEquals("1", str("SELECT nextval('nsb.sq')"));
    }

    @Test
    void droppingASequenceInTheWrongSchemaDestroysNothing() throws SQLException {
        exec("CREATE SEQUENCE nsa.s1");
        exec("SET search_path = nsb");
        assertEquals("42P01", state("DROP SEQUENCE nsb.s1"));
        assertEquals(1L, one("SELECT count(*) FROM pg_class WHERE relname='s1'"));
        // Unqualified, from a schema that does not hold it, is equally a miss.
        assertEquals("42P01", state("DROP SEQUENCE s1"));
        assertEquals(1L, one("SELECT count(*) FROM pg_class WHERE relname='s1'"));
    }

    @Test
    void nextvalReachesOnlyTheSchemaItNames() throws SQLException {
        exec("CREATE SEQUENCE nsa.gs");
        exec("SET search_path = nsb");
        assertEquals("42P01", state("SELECT nextval('nsb.gs')"));
        assertEquals("42P01", state("SELECT nextval('gs')"));
        assertEquals("42P01", state("SELECT setval('nsb.gs', 10)"));
        assertEquals("42P01", state("SELECT currval('nsb.gs')"));
        // The sequence it does name is untouched: its first value is still 1.
        assertEquals("1", str("SELECT nextval('nsa.gs')"));
    }

    @Test
    void twoSerialTablesInDifferentSchemasKeepSeparateCounters() throws SQLException {
        exec("CREATE TABLE nsa.ser (id serial PRIMARY KEY)");
        exec("CREATE TABLE nsb.ser (id serial PRIMARY KEY)");
        exec("INSERT INTO nsa.ser DEFAULT VALUES");
        exec("INSERT INTO nsb.ser DEFAULT VALUES");
        assertEquals(1, one("SELECT id FROM nsa.ser"));
        assertEquals(1, one("SELECT id FROM nsb.ser"));
        assertEquals(2L, one("SELECT count(*) FROM pg_class WHERE relname='ser_id_seq'"));
        assertEquals("nsa;;nsb", rows(
                "SELECT schemaname FROM pg_sequences WHERE sequencename='ser_id_seq' ORDER BY 1"));
    }

    @Test
    void alterSequenceRenameStaysInItsOwnSchema() throws SQLException {
        exec("CREATE SEQUENCE nsa.s2");
        exec("SET search_path = nsb");
        assertEquals("42P01", state("ALTER SEQUENCE s2 RENAME TO s2r"));
        assertEquals("nsa|s2", rows("SELECT n.nspname || '|' || c.relname FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname LIKE 's2%'"));
        exec("SET search_path = public");
        exec("ALTER SEQUENCE nsa.s2 RENAME TO s2r");
        assertEquals("nsa|s2r", rows("SELECT n.nspname || '|' || c.relname FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname LIKE 's2%'"));
    }

    @Test
    void alterSequenceSetSchemaReallyMovesIt() throws SQLException {
        exec("CREATE SEQUENCE nsa.mv");
        exec("ALTER SEQUENCE nsa.mv SET SCHEMA nsb");
        assertEquals("nsb", one("SELECT n.nspname FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='mv'"));
        assertEquals("1", str("SELECT nextval('nsb.mv')"));
        assertEquals("42P01", state("SELECT nextval('nsa.mv')"));
    }

    @Test
    void aSequenceGoesWithTheSchemaThatHoldsIt() throws SQLException {
        exec("CREATE SEQUENCE nsa.dead");
        exec("DROP SCHEMA nsa CASCADE");
        assertEquals(0L, one("SELECT count(*) FROM pg_class WHERE relname='dead'"));
        assertEquals("42P01", state("SELECT nextval('dead')"));
    }

    @Test
    void anUnqualifiedSequenceStillResolvesThroughTheSearchPath() throws SQLException {
        exec("DROP SEQUENCE IF EXISTS pubseq");
        exec("CREATE SEQUENCE pubseq");
        exec("SET search_path = nsb, public");
        // Only public holds it, so the path reaches it there.
        assertEquals("1", str("SELECT nextval('pubseq')"));
        // Now nsb holds one too, and nsb comes first.
        exec("CREATE SEQUENCE nsb.pubseq");
        assertEquals("1", str("SELECT nextval('pubseq')"));
        assertEquals("2", str("SELECT nextval('public.pubseq')"));
        exec("SET search_path = public");
        exec("DROP SEQUENCE pubseq");
    }

    // ---- indexes ----

    @Test
    void twoSchemasEachHoldAnIndexOfTheSameName() throws SQLException {
        exec("CREATE TABLE nsa.ib (a int)");
        exec("CREATE TABLE nsb.ib (a int)");
        exec("CREATE INDEX ix1 ON nsa.ib (a)");
        exec("CREATE INDEX ix1 ON nsb.ib (a)");
        assertEquals(2L, one("SELECT count(*) FROM pg_class WHERE relname='ix1'"));
        assertEquals("nsa|nsb",
                one("SELECT string_agg(schemaname, '|' ORDER BY schemaname)"
                        + " FROM pg_indexes WHERE indexname='ix1'"));
        exec("CREATE UNIQUE INDEX ux1 ON nsa.ib (a)");
        exec("CREATE UNIQUE INDEX ux1 ON nsb.ib (a)");
        assertEquals(2L, one("SELECT count(*) FROM pg_class WHERE relname='ux1'"));
    }

    @Test
    void alterIndexReachesOnlyTheSchemaItNames() throws SQLException {
        exec("CREATE TABLE nsa.gb (a int)");
        exec("CREATE INDEX i1 ON nsa.gb (a)");
        exec("SET search_path = nsb");
        assertEquals("42P01", state("ALTER INDEX nsb.i1 RENAME TO i1r"));
        assertEquals("42P01", state("ALTER INDEX i1 RENAME TO i1r"));
        assertEquals("nsa|i1", rows("SELECT n.nspname || '|' || c.relname FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname LIKE 'i1%'"));
        exec("SET search_path = public");
        exec("ALTER INDEX nsa.i1 RENAME TO i1r");
        assertEquals("nsa|i1r", rows("SELECT n.nspname || '|' || c.relname FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname LIKE 'i1%'"));
    }

    @Test
    void dropIndexReachesOnlyTheSchemaItNames() throws SQLException {
        exec("CREATE TABLE nsa.gb (a int)");
        exec("CREATE INDEX i2 ON nsa.gb (a)");
        exec("SET search_path = nsb");
        assertEquals("42704", state("DROP INDEX nsb.i2"));
        assertEquals("42704", state("DROP INDEX i2"));
        assertEquals(1L, one("SELECT count(*) FROM pg_class WHERE relname='i2'"));
        exec("SET search_path = public");
        exec("DROP INDEX nsa.i2");
        assertEquals(0L, one("SELECT count(*) FROM pg_class WHERE relname='i2'"));
    }

    @Test
    void anIndexGoesWithTheSchemaThatHoldsIt() throws SQLException {
        exec("CREATE TABLE nsa.gb (a int)");
        exec("CREATE INDEX i3 ON nsa.gb (a)");
        exec("DROP SCHEMA nsa CASCADE");
        assertEquals(0L, one("SELECT count(*) FROM pg_class WHERE relname='i3'"));
    }

    // ---- the converse: one schema is still one namespace ----

    @Test
    void withinOneSchemaEveryRelationKindStillSharesTheName() throws SQLException {
        exec("CREATE TABLE nsa.gb (a int)");
        exec("CREATE INDEX ixn ON nsa.gb (a)");
        // A table may not take an index's name in the same schema, nor the reverse.
        assertEquals("42P07", state("CREATE TABLE nsa.ixn (a int)"));
        assertEquals("42P07", state("CREATE INDEX gb ON nsa.gb (a)"));
        exec("CREATE SEQUENCE nsa.sqn");
        assertEquals("42P07", state("CREATE INDEX sqn ON nsa.gb (a)"));
        assertEquals("42P07", state("CREATE SEQUENCE nsa.ixn"));
    }

    @Test
    void aQuotedNameDifferingOnlyInCaseIsASecondIndex() throws SQLException {
        exec("CREATE TABLE nsa.gb (a int)");
        exec("CREATE INDEX cix ON nsa.gb (a)");
        exec("CREATE INDEX \"cIX\" ON nsa.gb (a)");
        assertEquals(2L,
                one("SELECT count(*) FROM pg_class WHERE relname IN ('cix','cIX')"));
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
            return v instanceof Integer ? ((Integer) v).intValue() : v;
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
