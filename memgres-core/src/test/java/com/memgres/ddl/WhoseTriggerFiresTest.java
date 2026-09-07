package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Whose trigger fires.
 *
 * <p>A trigger belongs to one relation. Held under the bare relation name alone, two schemas each
 * holding a {@code t} shared an entry: a trigger declared on {@code s.t} fired for a write to
 * {@code public.t}, a relation it was never attached to -- and the relation it really was attached
 * to fired it a second time.
 */
class WhoseTriggerFiresTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE SCHEMA zwg_s");
        exec("CREATE TABLE zwg_log (m text)");
        exec("CREATE TABLE zwg_s.zwg_t (a int)");
        exec("CREATE TABLE zwg_t (a int)");
        exec("CREATE FUNCTION zwg_note() RETURNS trigger LANGUAGE plpgsql AS $$"
                + " BEGIN INSERT INTO zwg_log VALUES ('fired'); RETURN NULL; END $$");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TABLE zwg_t");
            exec("DROP TABLE zwg_s.zwg_t");
            exec("DROP FUNCTION zwg_note()");
            exec("DROP SCHEMA zwg_s");
            exec("DROP TABLE zwg_log");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void clearLog() throws SQLException {
        exec("DELETE FROM zwg_log");
    }

    private static int fired() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::int FROM zwg_log")) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** Only the relation the trigger is on fires it. */
    @Test
    void whichRelationFiresIt() throws SQLException {
        exec("CREATE TRIGGER zwg_trunc AFTER TRUNCATE ON zwg_s.zwg_t"
                + " FOR EACH STATEMENT EXECUTE FUNCTION zwg_note()");
        exec("TRUNCATE zwg_t");
        assertEquals(0, fired());
        exec("TRUNCATE zwg_s.zwg_t");
        assertEquals(1, fired());
        // And the same for a row trigger.
        exec("CREATE TRIGGER zwg_ins AFTER INSERT ON zwg_s.zwg_t"
                + " FOR EACH ROW EXECUTE FUNCTION zwg_note()");
        exec("INSERT INTO zwg_t VALUES (1)");
        assertEquals(1, fired());
        exec("INSERT INTO zwg_s.zwg_t VALUES (1)");
        assertEquals(2, fired());
    }

    /** Dropping one schema takes only its own relation's triggers. */
    @Test
    void whatDroppingASchemaTakes() throws SQLException {
        exec("CREATE TRIGGER zwg_keep BEFORE INSERT ON zwg_t"
                + " FOR EACH ROW EXECUTE FUNCTION zwg_note()");
        exec("CREATE SCHEMA zwg_other");
        exec("CREATE TABLE zwg_other.zwg_t (a int)");
        exec("DROP SCHEMA zwg_other CASCADE");
        int before = fired();
        exec("INSERT INTO zwg_t VALUES (2)");
        assertEquals(before + 1, fired());
        exec("DROP TRIGGER zwg_keep ON zwg_t");
    }
}
