package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diff #34: pg_get_indexdef returns COALESCE(note, '') without ::text cast.
 * PG returns: COALESCE(note, ''::text)
 */
class PgGetIndexdefFormatTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }
    static String scalar(String sql) throws SQLException { try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) { return rs.next() ? rs.getString(1) : null; } }

    // Exact pattern from 42_index_conflict_and_definition_introspection.sql
    @Test void pg_get_indexdef_includes_text_cast() throws SQLException {
        exec("CREATE SCHEMA idx_fmt"); exec("SET search_path = idx_fmt");
        exec("CREATE TABLE idx_t(id int PRIMARY KEY, note text)");
        exec("CREATE INDEX idx_t_note_expr_idx ON idx_t ((coalesce(note, '')))");
        try {
            String def = scalar("SELECT pg_get_indexdef('idx_t_note_expr_idx'::regclass)");
            // PG: "CREATE INDEX idx_t_note_expr_idx ON idx_fmt.idx_t USING btree (COALESCE(note, ''::text))"
            assertTrue(def.contains("''::text"),
                "pg_get_indexdef should include ''::text cast, got: " + def);
        } finally {
            exec("DROP SCHEMA idx_fmt CASCADE"); exec("SET search_path = public");
        }
    }
}
