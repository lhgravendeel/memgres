package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A date cannot answer a sub-day unit, a tsvector position entry must be well formed, and a
 * CREATE needs a schema to land in. Expectations captured from a live PostgreSQL 18.0 server.
 *
 * <p>N41 extract unit validation, N61 tsvector position syntax, N66 search_path with no
 * usable schema.
 */
class InputValidationResidualsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
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

    private static String expr(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static String state(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql));
        return e.getSQLState();
    }

    // ------------------------------------------------------------------
    // N41 — extract() refuses a unit the type cannot answer
    // ------------------------------------------------------------------

    @Test
    void aDateRefusesSubDayUnits() {
        assertEquals("0A000", state("SELECT extract(hour FROM DATE '2026-06-25')"));
        assertEquals("0A000", state("SELECT extract(minute FROM DATE '2026-06-25')"));
        assertEquals("0A000", state("SELECT extract(second FROM DATE '2026-06-25')"));
    }

    @Test
    void aDateStillAnswersDateUnits() throws Exception {
        assertEquals("25", expr("SELECT extract(day FROM DATE '2026-06-25')::text"));
        assertEquals("2026", expr("SELECT extract(year FROM DATE '2026-06-25')::text"));
        assertEquals("6", expr("SELECT extract(month FROM DATE '2026-06-25')::text"));
    }

    @Test
    void aTimestampStillAnswersSubDayUnits() throws Exception {
        assertEquals("13", expr("SELECT extract(hour FROM TIMESTAMP '2026-06-25 13:00')::text"));
    }

    // ------------------------------------------------------------------
    // N61 — a tsvector position entry must be well formed
    // ------------------------------------------------------------------

    @Test
    void aMalformedTsvectorPositionIsRejected() {
        assertEquals("42601", state("SELECT 'cat:1x'::tsvector"));
        assertEquals("42601", state("SELECT 'cat:1,2y'::tsvector"));
    }

    @Test
    void wellFormedTsvectorPositionsStillParse() throws Exception {
        assertEquals("'cat':1A", expr("SELECT 'cat:1A'::tsvector::text"));
        assertEquals("'cat':1,2", expr("SELECT 'cat:1,2'::tsvector::text"));
        assertEquals("'cat' 'dog'", expr("SELECT 'cat dog'::tsvector::text"));
    }

    // ------------------------------------------------------------------
    // N66 — a CREATE needs a schema to land in
    // ------------------------------------------------------------------

    @Test
    void createWithNoUsableSearchPathIsRejected() throws Exception {
        exec("SET search_path = nosuchschema");
        try {
            assertEquals("3F000", state("CREATE TABLE ivr_x (id int)"));
        } finally {
            exec("RESET search_path");
        }
    }

    /** Reading tolerates an unusable entry; only creating needs a target. */
    @Test
    void readingStillToleratesAnUnusableSearchPathEntry() throws Exception {
        exec("CREATE TABLE ivr_r (id int)");
        exec("INSERT INTO ivr_r VALUES (1)");
        exec("SET search_path = nosuchschema, public");
        try {
            assertEquals("1", expr("SELECT count(*)::text FROM ivr_r"));
        } finally {
            exec("RESET search_path");
            exec("DROP TABLE ivr_r");
        }
    }

    @Test
    void createWithAUsableSearchPathStillWorks() throws Exception {
        exec("CREATE TABLE ivr_ok (id int)");
        assertEquals("1", expr("SELECT count(*)::text FROM pg_class WHERE relname = 'ivr_ok'"));
        exec("DROP TABLE ivr_ok");
    }
}
