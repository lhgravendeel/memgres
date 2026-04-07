package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diff #22: SELECT a COLLATE FROM p; parser eats FROM as collation name.
 * PG: bare COLLATE before FROM uses default collation. SELECT a COLLATE FROM p = SELECT a FROM p.
 */
class ParserCollateFromTest {

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

    @Test void select_column_bare_collate_from_table() throws SQLException {
        exec("CREATE TABLE coll_p(a text, b text)");
        exec("INSERT INTO coll_p VALUES ('hello', 'world')");
        try {
            // PG accepts: SELECT a COLLATE FROM p (bare COLLATE with no collation name before FROM)
            // Parser must not interpret FROM as the collation name
            String val = scalar("SELECT a COLLATE FROM coll_p");
            assertEquals("hello", val,
                "SELECT a COLLATE FROM p should work (bare COLLATE before FROM clause)");
        } finally {
            exec("DROP TABLE coll_p");
        }
    }
}
