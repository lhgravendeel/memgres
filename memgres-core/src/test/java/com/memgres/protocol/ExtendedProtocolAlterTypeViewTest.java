package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Issue #6: ALTER TABLE ALTER TYPE false view dependency.
 * Empty view (CREATE VIEW vv AS SELECT) doesn't reference column a,
 * so ALTER a TYPE int should succeed.
 * Extended query protocol version.
 */
class ExtendedProtocolAlterTypeViewTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    @Test void alter_type_noop_with_empty_view() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ext_alt_p(a int, b text)");
            s.execute("INSERT INTO ext_alt_p VALUES (1, 'x')");
            s.execute("CREATE VIEW ext_alt_vv AS SELECT");
        }
        try {
            // ALTER a TYPE int is a no-op (already int) and the view doesn't reference a
            try (PreparedStatement ps = conn.prepareStatement("ALTER TABLE ext_alt_p ALTER a TYPE int")) {
                ps.execute();
            }
        } finally {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP VIEW IF EXISTS ext_alt_vv");
                s.execute("DROP TABLE ext_alt_p");
            }
        }
    }

    @Test void rename_column_with_dependent_view() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ext_alt_p2(a int, b text)");
            s.execute("CREATE VIEW ext_alt_v2 AS SELECT * FROM ext_alt_p2");
        }
        try {
            // PG allows RENAME even with dependent views
            try (PreparedStatement ps = conn.prepareStatement("ALTER TABLE ext_alt_p2 RENAME a TO c")) {
                ps.execute();
            }
        } finally {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP VIEW IF EXISTS ext_alt_v2");
                s.execute("DROP TABLE IF EXISTS ext_alt_p2");
            }
        }
    }

    @Test void set_generated_always_non_identity_sqlstate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ext_alt_p3(a int, b text)");
        }
        try {
            try (PreparedStatement ps = conn.prepareStatement("ALTER TABLE ext_alt_p3 ALTER COLUMN a SET GENERATED ALWAYS")) {
                SQLException e = assertThrows(SQLException.class, ps::execute);
                assertEquals("55000", e.getSQLState(),
                    "Non-identity column SET GENERATED ALWAYS should be 55000");
            }
        } finally {
            try (Statement s = conn.createStatement()) { s.execute("DROP TABLE ext_alt_p3"); }
        }
    }
}
