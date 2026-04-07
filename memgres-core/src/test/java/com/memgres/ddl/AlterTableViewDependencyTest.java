package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diff #19: ALTER TABLE p ALTER a TYPE int, false view dependency error.
 * Diff #20: ALTER TABLE p RENAME a TO b, succeeds when PG errors.
 * Diff #21: ALTER COLUMN a SET GENERATED ALWAYS, wrong SQLSTATE (cascade from #20).
 *
 * Context: 25_parser_stress.sql creates table p(a int, b text), then
 * CREATE VIEW vv AS SELECT (empty select). The empty view does NOT reference column a,
 * so ALTER a TYPE int should succeed (no-op). But memgres detects a false dependency.
 */
class AlterTableViewDependencyTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    // Diff #19: ALTER TYPE on column not used by view should succeed
    @Test void alter_type_noop_with_empty_view() throws SQLException {
        exec("CREATE TABLE alt_p(a int, b text)");
        exec("INSERT INTO alt_p VALUES (1, 'x')");
        // Empty SELECT view that does NOT reference column a
        exec("CREATE VIEW alt_vv AS SELECT");
        try {
            // ALTER a TYPE int is a no-op (already int) and the view doesn't reference a
            exec("ALTER TABLE alt_p ALTER a TYPE int");
            // Should succeed without "cannot alter type of a column used by a view"
        } finally {
            exec("DROP VIEW IF EXISTS alt_vv");
            exec("DROP TABLE alt_p");
        }
    }

    // Diff #20: RENAME column with dependent view; PG18 allows this
    // PG auto-updates view definitions when a column is renamed.
    @Test void rename_column_with_view_dependency() throws SQLException {
        exec("CREATE TABLE alt_p2(a int, b text)");
        exec("CREATE VIEW alt_v2 AS SELECT * FROM alt_p2");
        try {
            // PG allows RENAME even with dependent views (view silently updates)
            exec("ALTER TABLE alt_p2 RENAME a TO c");
        } finally {
            exec("DROP VIEW IF EXISTS alt_v2");
            exec("DROP TABLE IF EXISTS alt_p2");
        }
    }

    // Diff #21: SET GENERATED ALWAYS on non-identity column → SQLSTATE 55000
    @Test void set_generated_always_non_identity_sqlstate() throws SQLException {
        exec("CREATE TABLE alt_p3(a int, b text)");
        try {
            try {
                exec("ALTER TABLE alt_p3 ALTER COLUMN a SET GENERATED ALWAYS");
                fail("Should fail on non-identity column");
            } catch (SQLException e) {
                assertEquals("55000", e.getSQLState(),
                    "Non-identity column SET GENERATED ALWAYS should be 55000, got " + e.getSQLState());
            }
        } finally {
            exec("DROP TABLE alt_p3");
        }
    }
}
