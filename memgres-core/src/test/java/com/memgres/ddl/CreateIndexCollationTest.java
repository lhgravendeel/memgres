package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests CREATE INDEX with a COLLATE clause: unknown collation names must be
 * rejected with SQLSTATE 42704 (same validation path used elsewhere).
 */
class CreateIndexCollationTest {

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

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    @Test
    void createIndex_invalidCollation_shouldError42704() throws SQLException {
        exec("DROP TABLE IF EXISTS idx_coll_invalid");
        exec("CREATE TABLE idx_coll_invalid (word text)");

        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE INDEX idx_coll_bad ON idx_coll_invalid (word COLLATE \"en_US.utf8\")"));
        assertEquals("42704", ex.getSQLState(),
                "CREATE INDEX with unknown collation should produce 42704; got: " + ex.getMessage());

        exec("DROP TABLE idx_coll_invalid");
    }

    @Test
    void createIndex_validCollation_shouldWork() throws SQLException {
        exec("DROP TABLE IF EXISTS idx_coll_valid");
        exec("CREATE TABLE idx_coll_valid (word text)");
        // C is always valid
        exec("CREATE INDEX idx_coll_c ON idx_coll_valid (word COLLATE \"C\")");
        exec("DROP TABLE idx_coll_valid");
    }
}
