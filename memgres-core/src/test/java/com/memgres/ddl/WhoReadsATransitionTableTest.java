package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Who reads a transition table.
 *
 * <p>A transition table holds the rows the whole statement wrote, and PostgreSQL puts it in scope
 * for every trigger that declared one -- a FOR EACH ROW trigger as much as a statement one, which
 * is why each firing of a row trigger sees the same complete set. Built only for the statement
 * triggers, a row trigger reading its own transition table was told there was no such relation.
 */
class WhoReadsATransitionTableTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE TABLE zxf_t (i int)");
        exec("CREATE TABLE zxf_log (n int)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TABLE zxf_t");
            exec("DROP TABLE zxf_log");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private static List<String> logged() throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT n FROM zxf_log ORDER BY n")) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** Every firing of the row trigger sees the whole statement's rows. */
    @Test
    void whatEachFiringSees() throws SQLException {
        exec("CREATE FUNCTION zxf_new() RETURNS trigger LANGUAGE plpgsql AS $$"
                + " DECLARE n int; BEGIN SELECT count(*) INTO n FROM zxf_nt;"
                + " INSERT INTO zxf_log VALUES (n); RETURN NULL; END $$");
        exec("CREATE TRIGGER zxf_ti AFTER INSERT ON zxf_t REFERENCING NEW TABLE AS zxf_nt"
                + " FOR EACH ROW EXECUTE FUNCTION zxf_new()");
        exec("INSERT INTO zxf_t VALUES (1),(2)");
        assertEquals(java.util.Arrays.asList("2", "2"), logged());
        exec("DELETE FROM zxf_log");
        exec("DROP TRIGGER zxf_ti ON zxf_t");
        exec("DROP FUNCTION zxf_new()");
    }

    /** The same for the rows an UPDATE and a DELETE leave behind. */
    @Test
    void whatTheOldTableHolds() throws SQLException {
        exec("DELETE FROM zxf_t");
        exec("INSERT INTO zxf_t VALUES (1),(2),(3)");
        exec("CREATE FUNCTION zxf_old() RETURNS trigger LANGUAGE plpgsql AS $$"
                + " DECLARE n int; BEGIN SELECT count(*) INTO n FROM zxf_ot;"
                + " INSERT INTO zxf_log VALUES (n); RETURN NULL; END $$");
        exec("CREATE TRIGGER zxf_tu AFTER UPDATE ON zxf_t REFERENCING OLD TABLE AS zxf_ot"
                + " FOR EACH ROW EXECUTE FUNCTION zxf_old()");
        exec("UPDATE zxf_t SET i = i + 1");
        assertEquals(java.util.Arrays.asList("3", "3", "3"), logged());
        exec("DELETE FROM zxf_log");
        exec("CREATE TRIGGER zxf_td AFTER DELETE ON zxf_t REFERENCING OLD TABLE AS zxf_ot"
                + " FOR EACH ROW EXECUTE FUNCTION zxf_old()");
        exec("DELETE FROM zxf_t");
        assertEquals(java.util.Arrays.asList("3", "3", "3"), logged());
        exec("DELETE FROM zxf_log");
        exec("DROP TRIGGER zxf_tu ON zxf_t");
        exec("DROP TRIGGER zxf_td ON zxf_t");
        exec("DROP FUNCTION zxf_old()");
    }
}
