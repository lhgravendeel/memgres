package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A foreign table is a relation. It is created in the schema the statement names, it owns that
 * name there against every other relation kind, it is listed by the catalogs that list relations,
 * and it goes away with its schema. Memgres used to register one globally: every foreign table
 * landed in public whatever schema was written, two relations of the same name could sit in one
 * schema because neither collision check saw the other, and a query that named one was told the
 * relation did not exist even though pg_class listed it.
 *
 * <p>Reading one is refused by its wrapper rather than by the name lookup. memgres loads no
 * foreign-data wrapper handler, so every foreign table it holds is served by a wrapper without
 * one — which is what PostgreSQL reports for a wrapper declared without a HANDLER, 55000.
 */
class ForeignTableSchemaTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE FOREIGN DATA WRAPPER fts_fdw");
        exec("CREATE SERVER fts_srv FOREIGN DATA WRAPPER fts_fdw");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static String scalarOrNull(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static void assertState(String expectedState, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(expectedState, e.getSQLState(),
                "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
    }

    private static void freshSchemas() throws SQLException {
        exec("SET search_path = public");
        exec("DROP SCHEMA IF EXISTS fts_a CASCADE");
        exec("DROP SCHEMA IF EXISTS fts_b CASCADE");
        exec("CREATE SCHEMA fts_a");
        exec("CREATE SCHEMA fts_b");
    }

    private static String nspOf(String relname) throws SQLException {
        return scalarOrNull("SELECT n.nspname FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE c.relname = '" + relname + "'");
    }

    // ------------------------------------------------------------ where it lives

    @Test
    void createdInTheSchemaTheStatementNames() throws Exception {
        freshSchemas();
        exec("CREATE FOREIGN TABLE fts_a.fts_ft1 (a int) SERVER fts_srv");
        assertEquals("fts_a", nspOf("fts_ft1"));
        assertEquals("f", scalar("SELECT relkind FROM pg_class WHERE relname = 'fts_ft1'"));
    }

    @Test
    void unqualifiedFollowsTheSearchPath() throws Exception {
        freshSchemas();
        exec("SET search_path = fts_b");
        exec("CREATE FOREIGN TABLE fts_ft2 (a int) SERVER fts_srv");
        exec("SET search_path = public");
        assertEquals("fts_b", nspOf("fts_ft2"));
    }

    @Test
    void anUnnamedSchemaIsStillPublic() throws Exception {
        freshSchemas();
        exec("CREATE FOREIGN TABLE fts_ft3 (a int) SERVER fts_srv");
        assertEquals("public", nspOf("fts_ft3"));
        exec("DROP FOREIGN TABLE fts_ft3");
    }

    @Test
    void aSchemaThatIsNotThereIsRefused() throws Exception {
        freshSchemas();
        assertState("3F000", "CREATE FOREIGN TABLE fts_nosuch.fts_ft4 (a int) SERVER fts_srv");
    }

    @Test
    void droppingTheSchemaTakesItAlong() throws Exception {
        freshSchemas();
        exec("CREATE FOREIGN TABLE fts_a.fts_ft5 (a int) SERVER fts_srv");
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_class WHERE relname = 'fts_ft5'"));
        exec("DROP SCHEMA fts_a CASCADE");
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_class WHERE relname = 'fts_ft5'"));
        assertEquals("0", scalar("SELECT count(*)::text FROM information_schema.tables"
                + " WHERE table_name = 'fts_ft5'"));
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_attribute a"
                + " JOIN pg_class c ON c.oid = a.attrelid WHERE c.relname = 'fts_ft5'"));
    }

    // ------------------------------------------------------------ the name it owns

    @Test
    void aTableCannotTakeAForeignTablesName() throws Exception {
        freshSchemas();
        exec("CREATE FOREIGN TABLE fts_a.fts_c1 (a int) SERVER fts_srv");
        assertState("42P07", "CREATE TABLE fts_a.fts_c1 (a int)");
        assertState("42P07", "CREATE VIEW fts_a.fts_c1 AS SELECT 1 AS a");
        assertState("42P07", "CREATE SEQUENCE fts_a.fts_c1");
    }

    @Test
    void aForeignTableCannotTakeARelationsName() throws Exception {
        freshSchemas();
        exec("CREATE TABLE fts_a.fts_c2 (a int)");
        exec("CREATE VIEW fts_a.fts_c3 AS SELECT 1 AS a");
        exec("CREATE SEQUENCE fts_a.fts_c4");
        assertState("42P07", "CREATE FOREIGN TABLE fts_a.fts_c2 (a int) SERVER fts_srv");
        assertState("42P07", "CREATE FOREIGN TABLE fts_a.fts_c3 (a int) SERVER fts_srv");
        assertState("42P07", "CREATE FOREIGN TABLE fts_a.fts_c4 (a int) SERVER fts_srv");
    }

    @Test
    void twiceInOneSchemaIsRefusedAndAnotherSchemaIsNot() throws Exception {
        freshSchemas();
        exec("CREATE FOREIGN TABLE fts_a.fts_c5 (a int) SERVER fts_srv");
        assertState("42P07", "CREATE FOREIGN TABLE fts_a.fts_c5 (a int) SERVER fts_srv");
        exec("CREATE TABLE fts_b.fts_c5 (a int)");
        assertEquals("2", scalar("SELECT count(*)::text FROM pg_class WHERE relname = 'fts_c5'"));
    }

    @Test
    void ifNotExistsStepsAsideForWhateverHoldsTheName() throws Exception {
        freshSchemas();
        exec("CREATE FOREIGN TABLE fts_a.fts_c6 (a int) SERVER fts_srv");
        exec("CREATE TABLE fts_a.fts_c7 (a int)");
        exec("CREATE FOREIGN TABLE IF NOT EXISTS fts_a.fts_c6 (z text) SERVER fts_srv");
        exec("CREATE FOREIGN TABLE IF NOT EXISTS fts_a.fts_c7 (z text) SERVER fts_srv");
        // both are untouched: the foreign table still has its own column, the table is a table
        assertEquals("a", scalar("SELECT a.attname FROM pg_attribute a"
                + " JOIN pg_class c ON c.oid = a.attrelid"
                + " WHERE c.relname = 'fts_c6' AND a.attnum > 0"));
        assertEquals("r", scalar("SELECT relkind FROM pg_class WHERE relname = 'fts_c7'"));
        // and on a free name it creates one
        exec("CREATE FOREIGN TABLE IF NOT EXISTS fts_a.fts_c8 (z text) SERVER fts_srv");
        assertEquals("f", scalar("SELECT relkind FROM pg_class WHERE relname = 'fts_c8'"));
    }

    // ------------------------------------------------------------ what the catalogs say

    @Test
    void listedByInformationSchema() throws Exception {
        freshSchemas();
        exec("CREATE FOREIGN TABLE fts_a.fts_i1 (a int, b text) SERVER fts_srv");
        assertEquals("FOREIGN", scalar("SELECT table_type FROM information_schema.tables"
                + " WHERE table_name = 'fts_i1'"));
        assertEquals("fts_a", scalar("SELECT table_schema FROM information_schema.tables"
                + " WHERE table_name = 'fts_i1'"));
        assertEquals("fts_a|fts_srv", scalar(
                "SELECT foreign_table_schema || '|' || foreign_server_name"
                + " FROM information_schema.foreign_tables WHERE foreign_table_name = 'fts_i1'"));
        assertEquals("integer", scalar("SELECT data_type FROM information_schema.columns"
                + " WHERE table_name = 'fts_i1' AND column_name = 'a'"));
        assertEquals("text", scalar("SELECT data_type FROM information_schema.columns"
                + " WHERE table_name = 'fts_i1' AND column_name = 'b'"));
        assertEquals("fts_a", scalar("SELECT table_schema FROM information_schema.columns"
                + " WHERE table_name = 'fts_i1' AND column_name = 'a'"));
    }

    @Test
    void pgForeignTablePointsAtThePgClassRow() throws Exception {
        freshSchemas();
        exec("CREATE FOREIGN TABLE fts_a.fts_i2 (a int) SERVER fts_srv");
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_foreign_table t"
                + " JOIN pg_class c ON c.oid = t.ftrelid WHERE c.relname = 'fts_i2'"));
        assertEquals("a", scalar("SELECT a.attname FROM pg_attribute a"
                + " JOIN pg_class c ON c.oid = a.attrelid"
                + " WHERE c.relname = 'fts_i2' AND a.attnum > 0"));
    }

    // ------------------------------------------------------------ reading one

    @Test
    void readingOneIsTheWrappersRefusal() throws Exception {
        freshSchemas();
        exec("CREATE FOREIGN TABLE fts_a.fts_r1 (a int) SERVER fts_srv");
        SQLException e = assertThrows(SQLException.class,
                () -> exec("SELECT * FROM fts_a.fts_r1"));
        assertEquals("55000", e.getSQLState());
        assertTrue(e.getMessage().contains("foreign-data wrapper \"fts_fdw\" has no handler"),
                e.getMessage());
        assertState("55000", "INSERT INTO fts_a.fts_r1 VALUES (1)");
        assertState("55000", "SELECT count(*) FROM fts_a.fts_r1");
        exec("SET search_path = fts_a");
        assertState("55000", "SELECT * FROM fts_r1");
        exec("SET search_path = public");
    }

    @Test
    void anotherSchemasNameStillDoesNotExist() throws Exception {
        freshSchemas();
        exec("CREATE FOREIGN TABLE fts_a.fts_r2 (a int) SERVER fts_srv");
        assertState("42P01", "SELECT * FROM fts_b.fts_r2");
        assertState("42P01", "SELECT * FROM public.fts_r2");
    }

    // ------------------------------------------------------------ dropping one

    @Test
    void dropHonoursTheSchema() throws Exception {
        freshSchemas();
        exec("CREATE FOREIGN TABLE fts_a.fts_d1 (a int) SERVER fts_srv");
        assertState("42704", "DROP FOREIGN TABLE fts_b.fts_d1");
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_class WHERE relname = 'fts_d1'"));
        exec("DROP FOREIGN TABLE fts_a.fts_d1");
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_class WHERE relname = 'fts_d1'"));
    }

    @Test
    void droppingOneThatIsNotThere() throws Exception {
        freshSchemas();
        assertState("42704", "DROP FOREIGN TABLE fts_nothing");
        exec("DROP FOREIGN TABLE IF EXISTS fts_nothing");
    }

    @Test
    void theWrongKindOfDropIsRefused() throws Exception {
        freshSchemas();
        exec("CREATE FOREIGN TABLE fts_a.fts_d2 (a int) SERVER fts_srv");
        exec("CREATE TABLE fts_a.fts_d3 (a int)");
        assertState("42809", "DROP TABLE fts_a.fts_d2");
        assertState("42809", "DROP FOREIGN TABLE fts_a.fts_d3");
        // and neither of them was removed by the attempt
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_class WHERE relname = 'fts_d2'"));
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_class WHERE relname = 'fts_d3'"));
    }

    @Test
    void theNameIsFreeAgainAfterADrop() throws Exception {
        freshSchemas();
        exec("CREATE FOREIGN TABLE fts_a.fts_d4 (a int) SERVER fts_srv");
        exec("DROP FOREIGN TABLE fts_a.fts_d4");
        exec("CREATE TABLE fts_a.fts_d4 (b text)");
        assertEquals("1", scalar("SELECT count(*)::text FROM information_schema.columns"
                + " WHERE table_name = 'fts_d4' AND column_name = 'b'"));
    }
}
