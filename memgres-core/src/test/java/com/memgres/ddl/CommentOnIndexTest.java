package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diff #24: COMMENT ON INDEX admin_t_pkey; PG18 allows COMMENT ON INDEX
 * for PK constraint-backed indexes. Memgres should also allow it.
 */
class CommentOnIndexTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    @Test void comment_on_pk_index_should_succeed() throws SQLException {
        exec("CREATE TABLE admin_t(id int PRIMARY KEY, note text)");
        try {
            // PG18 allows COMMENT ON INDEX for PK constraint-backed indexes
            exec("COMMENT ON INDEX admin_t_pkey IS 'pk index'");
        } finally {
            exec("DROP TABLE admin_t");
        }
    }
}
