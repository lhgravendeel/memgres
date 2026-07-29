package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A relation's kind decides what may be done to it. Treating a materialized view as a view meant
 * DROP VIEW destroyed stored data on what is usually a typo, and treating a view as a table meant
 * renames landed on a shadow relation the catalog never reads. The same applies to a schema: a
 * move or rename has to leave every name that reaches the relation still working.
 */
class RelationKindTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static void assertState(String expectedState, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(expectedState, e.getSQLState(),
                "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
    }

    private static void freshMatview() throws SQLException {
        exec("DROP MATERIALIZED VIEW IF EXISTS rkt_mv CASCADE");
        exec("DROP TABLE IF EXISTS rkt_t CASCADE");
        exec("CREATE TABLE rkt_t (i int, j text)");
        exec("INSERT INTO rkt_t VALUES (1,'a'),(2,'b')");
        exec("CREATE MATERIALIZED VIEW rkt_mv AS SELECT i, j FROM rkt_t");
    }

    // ------------------------------------------------- materialized view kind

    @Test
    void materializedViewRejectsWrites() throws Exception {
        freshMatview();
        assertState("42809", "INSERT INTO rkt_mv VALUES (9,'z')");
        assertState("42809", "UPDATE rkt_mv SET j = 'x'");
        assertState("42809", "DELETE FROM rkt_mv");
        assertState("42809", "MERGE INTO rkt_mv t USING rkt_t s ON t.i = s.i"
                + " WHEN MATCHED THEN DELETE");
        assertEquals("2", scalar("SELECT count(*)::text FROM rkt_mv"));
    }

    @Test
    void dropViewDoesNotDestroyAMaterializedView() throws Exception {
        freshMatview();
        assertState("42809", "DROP VIEW rkt_mv");
        assertEquals("2", scalar("SELECT count(*)::text FROM rkt_mv"));
        assertState("42809", "DROP TABLE rkt_mv");
        assertEquals("2", scalar("SELECT count(*)::text FROM rkt_mv"));
        // IF EXISTS must not turn the kind mismatch into a silent drop either
        assertState("42809", "DROP VIEW IF EXISTS rkt_mv");
        assertEquals("2", scalar("SELECT count(*)::text FROM rkt_mv"));
        // the right spelling works
        exec("DROP MATERIALIZED VIEW rkt_mv");
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_class WHERE relname = 'rkt_mv'"));
    }

    @Test
    void dropMaterializedViewDoesNotDestroyAPlainView() throws Exception {
        freshMatview();
        exec("DROP VIEW IF EXISTS rkt_v CASCADE");
        exec("CREATE VIEW rkt_v AS SELECT i FROM rkt_t");
        assertState("42809", "DROP MATERIALIZED VIEW rkt_v");
        assertEquals("2", scalar("SELECT count(*)::text FROM rkt_v"));
        exec("DROP VIEW rkt_v");
    }

    @Test
    void refreshConcurrentlyNeedsAFullUniqueIndex() throws Exception {
        freshMatview();
        assertState("55000", "REFRESH MATERIALIZED VIEW CONCURRENTLY rkt_mv");
        // a partial index does not identify every row
        exec("CREATE UNIQUE INDEX rkt_mv_partial ON rkt_mv (i) WHERE i > 1");
        assertState("55000", "REFRESH MATERIALIZED VIEW CONCURRENTLY rkt_mv");
        exec("DROP INDEX rkt_mv_partial");
        // a full unique index does
        exec("CREATE UNIQUE INDEX rkt_mv_ix ON rkt_mv (i)");
        exec("REFRESH MATERIALIZED VIEW CONCURRENTLY rkt_mv");
        assertEquals("2", scalar("SELECT count(*)::text FROM rkt_mv"));
        // and it cannot be combined with WITH NO DATA: two options that contradict each other
        // are a fault in the statement, which PostgreSQL reports as 42601
        assertState("42601", "REFRESH MATERIALIZED VIEW CONCURRENTLY rkt_mv WITH NO DATA");
        exec("REFRESH MATERIALIZED VIEW rkt_mv");
        assertEquals("2", scalar("SELECT count(*)::text FROM rkt_mv"));
    }

    // ------------------------------------------------------ ALTER TABLE kinds

    @Test
    void alterTableRefusesReshapingActionsOnOtherKinds() throws Exception {
        freshMatview();
        exec("DROP VIEW IF EXISTS rkt_v CASCADE");
        exec("CREATE VIEW rkt_v AS SELECT i FROM rkt_t");
        assertState("42809", "ALTER TABLE rkt_v ALTER COLUMN i SET NOT NULL");
        assertState("42809", "ALTER TABLE rkt_v ALTER COLUMN i DROP NOT NULL");
        assertState("42809", "ALTER TABLE rkt_v ADD COLUMN z int");
        assertState("42809", "ALTER TABLE rkt_v DROP COLUMN i");
        assertState("42809", "ALTER TABLE rkt_v ADD CONSTRAINT rkt_ck CHECK (i > 0)");
        assertState("42809", "ALTER TABLE rkt_mv ADD COLUMN z int");
        exec("DROP SEQUENCE IF EXISTS rkt_s CASCADE");
        exec("CREATE SEQUENCE rkt_s");
        assertState("42809", "ALTER TABLE rkt_s ADD COLUMN z int");
        // a view column may carry a default, which INSERT through the view uses
        exec("ALTER TABLE rkt_v ALTER COLUMN i SET DEFAULT 1");
        exec("ALTER TABLE rkt_v ALTER COLUMN i DROP DEFAULT");
        exec("DROP SEQUENCE rkt_s");
        exec("DROP VIEW rkt_v");
    }

    @Test
    void alterTableOnARealTableIsUnaffected() throws Exception {
        freshMatview();
        exec("ALTER TABLE rkt_t ADD COLUMN z int");
        assertEquals("3", scalar("SELECT count(*)::text FROM information_schema.columns"
                + " WHERE table_name = 'rkt_t'"));
        exec("ALTER TABLE rkt_t ALTER COLUMN z SET DEFAULT 1");
        exec("ALTER TABLE rkt_t DROP COLUMN z");
        assertEquals("2", scalar("SELECT count(*)::text FROM information_schema.columns"
                + " WHERE table_name = 'rkt_t'"));
    }

    // ------------------------------------------------------------ view rename

    @Test
    void renamingAViewKeepsItAView() throws Exception {
        freshMatview();
        exec("DROP VIEW IF EXISTS rkt_v CASCADE");
        exec("DROP VIEW IF EXISTS rkt_v2 CASCADE");
        exec("CREATE VIEW rkt_v AS SELECT i FROM rkt_t");
        exec("ALTER TABLE rkt_v RENAME COLUMN i TO k");
        assertEquals("1", scalar("SELECT count(*)::text FROM information_schema.columns"
                + " WHERE table_name = 'rkt_v' AND column_name = 'k'"));
        assertEquals("0", scalar("SELECT count(*)::text FROM information_schema.columns"
                + " WHERE table_name = 'rkt_v' AND column_name = 'i'"));
        exec("ALTER TABLE rkt_v RENAME TO rkt_v2");
        assertEquals("2", scalar("SELECT count(*)::text FROM rkt_v2"));
        assertEquals("v", scalar("SELECT relkind FROM pg_class WHERE relname = 'rkt_v2'"));
        // it is still droppable as a view, which the shadow-relation rename broke
        exec("DROP VIEW rkt_v2");
    }

    @Test
    void alterViewSpellingsBehaveTheSame() throws Exception {
        freshMatview();
        exec("DROP VIEW IF EXISTS rkt_v3 CASCADE");
        exec("DROP VIEW IF EXISTS rkt_v4 CASCADE");
        exec("CREATE VIEW rkt_v3 AS SELECT i FROM rkt_t");
        exec("ALTER VIEW rkt_v3 RENAME COLUMN i TO m");
        assertEquals("1", scalar("SELECT count(*)::text FROM information_schema.columns"
                + " WHERE table_name = 'rkt_v3' AND column_name = 'm'"));
        exec("ALTER VIEW rkt_v3 RENAME TO rkt_v4");
        assertEquals("2", scalar("SELECT count(*)::text FROM rkt_v4"));
        exec("DROP VIEW rkt_v4");
    }

    // ------------------------------------------------------- schema namespace

    @Test
    void movingToASchemaThatDoesNotExistIsRefused() throws Exception {
        exec("DROP SCHEMA IF EXISTS rkt_s1 CASCADE");
        exec("CREATE SCHEMA rkt_s1");
        exec("CREATE TABLE rkt_s1.rkt_moved (i int)");
        exec("INSERT INTO rkt_s1.rkt_moved VALUES (1),(2)");
        assertState("3F000", "ALTER TABLE rkt_s1.rkt_moved SET SCHEMA rkt_no_such_schema");
        // the table has not gone anywhere
        assertEquals("2", scalar("SELECT count(*)::text FROM rkt_s1.rkt_moved"));
        assertEquals("rkt_s1", scalar("SELECT table_schema FROM information_schema.tables"
                + " WHERE table_name = 'rkt_moved'"));
        exec("DROP SCHEMA rkt_s1 CASCADE");
    }

    @Test
    void movingToARealSchemaWorks() throws Exception {
        exec("DROP SCHEMA IF EXISTS rkt_a CASCADE");
        exec("DROP SCHEMA IF EXISTS rkt_b CASCADE");
        exec("CREATE SCHEMA rkt_a");
        exec("CREATE SCHEMA rkt_b");
        exec("CREATE TABLE rkt_a.rkt_m (i int)");
        exec("INSERT INTO rkt_a.rkt_m VALUES (1),(2)");
        exec("ALTER TABLE rkt_a.rkt_m SET SCHEMA rkt_b");
        assertEquals("2", scalar("SELECT count(*)::text FROM rkt_b.rkt_m"));
        assertEquals("rkt_b", scalar("SELECT table_schema FROM information_schema.tables"
                + " WHERE table_name = 'rkt_m'"));
        exec("DROP SCHEMA rkt_a CASCADE");
        exec("DROP SCHEMA rkt_b CASCADE");
    }

    @Test
    void renamingASchemaCarriesItsContents() throws Exception {
        exec("DROP SCHEMA IF EXISTS rkt_r1 CASCADE");
        exec("DROP SCHEMA IF EXISTS rkt_r2 CASCADE");
        exec("CREATE SCHEMA rkt_r1");
        exec("CREATE TABLE rkt_r1.rkt_rt (i int)");
        exec("INSERT INTO rkt_r1.rkt_rt VALUES (1),(2)");
        exec("CREATE VIEW rkt_r1.rkt_rv AS SELECT i FROM rkt_r1.rkt_rt");
        exec("ALTER SCHEMA rkt_r1 RENAME TO rkt_r2");
        // reachable under the new name, and the catalog agrees
        assertEquals("2", scalar("SELECT count(*)::text FROM rkt_r2.rkt_rt"));
        assertEquals("1", scalar("SELECT count(*)::text FROM information_schema.schemata"
                + " WHERE schema_name = 'rkt_r2'"));
        assertEquals("0", scalar("SELECT count(*)::text FROM information_schema.schemata"
                + " WHERE schema_name = 'rkt_r1'"));
        assertEquals("rkt_r2", scalar("SELECT table_schema FROM information_schema.tables"
                + " WHERE table_name = 'rkt_rt'"));
        // and the old name no longer resolves
        assertState("3F000", "SELECT count(*) FROM rkt_r1.rkt_rt");
        exec("DROP SCHEMA rkt_r2 CASCADE");
    }

    @Test
    void renamingASchemaChecksBothNames() throws Exception {
        exec("DROP SCHEMA IF EXISTS rkt_n1 CASCADE");
        exec("CREATE SCHEMA rkt_n1");
        assertState("3F000", "ALTER SCHEMA rkt_no_such RENAME TO rkt_x");
        assertState("42P06", "ALTER SCHEMA rkt_n1 RENAME TO public");
        exec("DROP SCHEMA rkt_n1 CASCADE");
    }
}
