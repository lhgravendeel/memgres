package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What one part of a statement leaves for another.
 *
 * <p>Every part of one statement reads the same snapshot, so a row a data-modifying WITH item has
 * already written is not one the rest of the statement may write again: PostgreSQL passes over it.
 * Written twice, {@code WITH a AS (UPDATE t ... RETURNING id) DELETE FROM t WHERE id IN (SELECT id
 * FROM a)} deleted the row it had just updated, and the same statement reported deleting a row
 * PostgreSQL says it deleted none of.
 */
class WhatOnePartOfAStatementLeavesAnotherTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE TABLE zwu_t (id int, v text)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TABLE zwu_t");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void refill() throws SQLException {
        exec("DELETE FROM zwu_t");
        exec("INSERT INTO zwu_t VALUES (1,'a'),(2,'b')");
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int width = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                for (int i = 1; i <= width; i++) {
                    if (i > 1) line.append('|');
                    line.append(rs.getString(i));
                }
                out.add(line.toString());
            }
        }
        return out;
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** A row the WITH item updated is not one the DELETE may take. */
    @Test
    void whatADeleteAfterAnUpdateTakes() throws SQLException {
        assertEquals(java.util.Collections.emptyList(),
                rows("WITH a AS (UPDATE zwu_t SET v = 'upd' WHERE id = 1 RETURNING id)"
                        + " DELETE FROM zwu_t WHERE id IN (SELECT id FROM a) RETURNING id"));
        assertEquals(java.util.Arrays.asList("1|upd", "2|b"),
                rows("SELECT id, v FROM zwu_t ORDER BY id"));
    }

    /** Nor one the WITH item deleted. */
    @Test
    void whatAnUpdateAfterADeleteWrites() throws SQLException {
        assertEquals(java.util.Collections.emptyList(),
                rows("WITH a AS (DELETE FROM zwu_t WHERE id = 2 RETURNING id)"
                        + " UPDATE zwu_t SET v = 'x' WHERE id IN (SELECT id FROM a) RETURNING id"));
        assertEquals(java.util.Arrays.asList("1|a"),
                rows("SELECT id, v FROM zwu_t ORDER BY id"));
    }

    /** Nor one it updated, when the outer statement is an update too. */
    @Test
    void whatASecondUpdateWrites() throws SQLException {
        assertEquals(java.util.Collections.emptyList(),
                rows("WITH a AS (UPDATE zwu_t SET v = 'y' WHERE id = 1 RETURNING id)"
                        + " UPDATE zwu_t SET v = 'z' WHERE id IN (SELECT id FROM a) RETURNING id"));
        assertEquals(java.util.Arrays.asList("1|y", "2|b"),
                rows("SELECT id, v FROM zwu_t ORDER BY id"));
    }

    /** A row no other part touched is written as usual. */
    @Test
    void whatAnUntouchedRowGets() throws SQLException {
        assertEquals(java.util.Arrays.asList("2"),
                rows("WITH a AS (UPDATE zwu_t SET v = 'upd' WHERE id = 1 RETURNING id)"
                        + " DELETE FROM zwu_t WHERE id = 2 RETURNING id"));
        assertEquals(java.util.Arrays.asList("1|upd"),
                rows("SELECT id, v FROM zwu_t ORDER BY id"));
    }
}
