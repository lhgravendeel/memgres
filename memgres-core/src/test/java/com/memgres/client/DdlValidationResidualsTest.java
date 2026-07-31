package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DDL residuals from the sweep: a column list on CREATE TABLE AS, keys over a virtual generated
 * column, a second COLLATE clause, and what TRUNCATE says about a relation that is not a table.
 *
 * <p>The first of those is the one that mattered most — {@code CREATE TABLE t (a, b) AS query} is
 * ordinary SQL that memgres could not parse at all, so a migration writing it failed outright
 * rather than merely getting a wrong answer.
 */
class DdlValidationResidualsTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getMessage().split("\n")[0].replace("ERROR: ", "").trim();
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    // =========================================================================
    // CREATE TABLE ... (columns) AS query
    // =========================================================================

    @Test
    void aColumnListRenamesTheQuerysColumns() throws Exception {
        exec("DROP TABLE IF EXISTS dvt_q1 CASCADE");
        exec("CREATE TABLE dvt_q1 (p, q) AS SELECT 1, 2");
        assertEquals(List.of("1|2"), rows("SELECT p, q FROM dvt_q1"));

        exec("DROP TABLE IF EXISTS dvt_q2 CASCADE");
        exec("CREATE TABLE dvt_q2 (p) AS SELECT 1");
        assertEquals(List.of("1"), rows("SELECT p FROM dvt_q2"));
    }

    @Test
    void fewerNamesThanColumnsLeavesTheRestAsTheQueryNamedThem() throws Exception {
        exec("DROP TABLE IF EXISTS dvt_q3 CASCADE");
        exec("CREATE TABLE dvt_q3 (p) AS SELECT 1 AS one, 2 AS two");
        assertEquals(List.of("p", "two"),
                rows("SELECT column_name FROM information_schema.columns "
                        + "WHERE table_name = 'dvt_q3' ORDER BY ordinal_position"));
    }

    @Test
    void moreNamesThanColumnsIsRefused() {
        assertEquals("too many column names were specified",
                messageOf("CREATE TABLE dvt_q4 (p, q) AS SELECT 1"));
    }

    @Test
    void withNoDataStillTakesTheColumnList() throws Exception {
        exec("DROP TABLE IF EXISTS dvt_q5 CASCADE");
        exec("CREATE TABLE dvt_q5 (p, q) AS SELECT 1, 2 WITH NO DATA");
        assertEquals(List.of("0"), rows("SELECT count(*) FROM dvt_q5"));
        assertEquals(List.of("p", "q"),
                rows("SELECT column_name FROM information_schema.columns "
                        + "WHERE table_name = 'dvt_q5' ORDER BY ordinal_position"));
    }

    @Test
    void anOrdinaryColumnDefinitionListIsStillReadAsOne() throws Exception {
        exec("DROP TABLE IF EXISTS dvt_plain CASCADE");
        exec("CREATE TABLE dvt_plain (a int, b text, c int DEFAULT 4)");
        assertEquals(List.of("a|integer", "b|text", "c|integer"),
                rows("SELECT column_name, data_type FROM information_schema.columns "
                        + "WHERE table_name = 'dvt_plain' ORDER BY ordinal_position"),
                "a list carrying types is a definition, not a rename");
    }

    // =========================================================================
    // Keys over a VIRTUAL generated column
    // =========================================================================

    @Test
    void noKeyMayBeBuiltOverAVirtualGeneratedColumn() throws Exception {
        exec("DROP TABLE IF EXISTS dvt_g2 CASCADE");
        exec("CREATE TABLE dvt_g2 (a int, b int GENERATED ALWAYS AS (a) VIRTUAL)");

        assertEquals("primary keys on virtual generated columns are not supported",
                messageOf("ALTER TABLE dvt_g2 ADD CONSTRAINT dvt_pk PRIMARY KEY (b)"));
        assertEquals("unique constraints on virtual generated columns are not supported",
                messageOf("ALTER TABLE dvt_g2 ADD UNIQUE (b)"));

        // The stored column takes one as usual.
        exec("ALTER TABLE dvt_g2 ADD CONSTRAINT dvt_pk PRIMARY KEY (a)");
        assertEquals(List.of("dvt_pk"),
                rows("SELECT conname FROM pg_constraint "
                        + "WHERE conrelid = 'dvt_g2'::regclass AND contype = 'p'"));
    }

    // =========================================================================
    // One collation per column, one NOT NULL per domain
    // =========================================================================

    @Test
    void aSecondCollateClauseIsASyntaxError() {
        assertEquals("42601", stateOf("CREATE TABLE dvt_cc (t text COLLATE \"C\" COLLATE \"C\")"));
        assertEquals("42601",
                stateOf("CREATE DOMAIN dvt_d7 AS text COLLATE \"C\" COLLATE \"C\""));
    }

    @Test
    void notNullWrittenTwiceOnADomainIsRefused() {
        assertEquals("42P17", stateOf("CREATE DOMAIN dvt_d9 AS int NOT NULL NOT NULL"));
        assertEquals("redundant NOT NULL constraint definition",
                messageOf("CREATE DOMAIN dvt_d9b AS int NOT NULL NOT NULL"));
    }

    @Test
    void oneOfEachIsOrdinary() throws Exception {
        exec("DROP TABLE IF EXISTS dvt_ok CASCADE");
        exec("CREATE TABLE dvt_ok (t text COLLATE \"C\")");
        exec("DROP DOMAIN IF EXISTS dvt_dok CASCADE");
        exec("CREATE DOMAIN dvt_dok AS int NOT NULL");
        assertEquals(List.of("1"), rows("SELECT count(*) FROM information_schema.columns "
                + "WHERE table_name = 'dvt_ok' AND column_name = 't'"));
    }

    // =========================================================================
    // TRUNCATE
    // =========================================================================

    @Test
    void truncateSaysWhatTheRelationIsNot() throws Exception {
        exec("DROP VIEW IF EXISTS dvt_kv CASCADE");
        exec("DROP TABLE IF EXISTS dvt_kt CASCADE");
        exec("CREATE TABLE dvt_kt (i int)");
        exec("CREATE VIEW dvt_kv AS SELECT * FROM dvt_kt");

        assertEquals("\"dvt_kv\" is not a table", messageOf("TRUNCATE dvt_kv"));
        assertEquals("42809", stateOf("TRUNCATE dvt_kv"));

        // A name that resolves to nothing is the other complaint.
        assertEquals("relation \"dvt_nosuch\" does not exist", messageOf("TRUNCATE dvt_nosuch"));
        assertEquals("42P01", stateOf("TRUNCATE dvt_nosuch"));

        // And a real table truncates.
        exec("INSERT INTO dvt_kt VALUES (1),(2)");
        exec("TRUNCATE dvt_kt");
        assertEquals(List.of("0"), rows("SELECT count(*) FROM dvt_kt"));
    }
}
