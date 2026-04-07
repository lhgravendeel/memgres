package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Issue #9: pg_get_indexdef missing closing parenthesis for expression indexes.
 * PG wraps expression index entries in extra parentheses.
 * Extended query protocol version.
 */
class ExtendedProtocolIndexdefTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    @Test void pg_get_indexdef_expression_parens() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA ext_idx_fmt");
            s.execute("SET search_path = ext_idx_fmt");
            s.execute("CREATE TABLE ext_idx_t(id int PRIMARY KEY, note text)");
            s.execute("CREATE INDEX ext_idx_t_note_expr_idx ON ext_idx_t ((coalesce(note, '')))");
        }
        try {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT pg_get_indexdef('ext_idx_t_note_expr_idx'::regclass)")) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    String def = rs.getString(1);
                    assertTrue(def.contains("''::text"),
                        "pg_get_indexdef should include ''::text cast, got: " + def);
                    // Expression indexes should have parens around the expression
                    assertTrue(def.contains("(") && def.contains(")"),
                        "Expression index should have parens, got: " + def);
                }
            }
        } finally {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA ext_idx_fmt CASCADE");
                s.execute("SET search_path = public");
            }
        }
    }
}
