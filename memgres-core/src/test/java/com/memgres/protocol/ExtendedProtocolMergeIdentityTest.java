package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Issue #4: MERGE identity sequence off by 1.
 * After two MERGE operations, the inserted row id=6 gets c=8 instead of PG's c=7.
 * Extended query protocol version.
 */
class ExtendedProtocolMergeIdentityTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    @Test void merge_identity_sequence_value() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ext_tgt(id int PRIMARY KEY, a int, b text DEFAULT 'd', c int GENERATED ALWAYS AS IDENTITY)");
            s.execute("CREATE TABLE ext_src(id int, a int, b text)");
            s.execute("CREATE TABLE ext_src2(id int, a int, flag text)");
            s.execute("INSERT INTO ext_tgt(id, a, b) VALUES (1,10,'x'),(2,20,'y'),(3,30,'z')");
            s.execute("INSERT INTO ext_src VALUES (1,100,'sx'),(4,400,'sw'),(5,NULL,NULL)");
            s.execute("INSERT INTO ext_tgt(id, a, b) VALUES (10, 1000, 'r1')");
            s.execute("UPDATE ext_tgt AS t SET a = t.a + 1, b = upper(t.b) WHERE t.id IN (1,2)");
            s.execute("DELETE FROM ext_tgt t WHERE t.id = 3");
        }
        try {
            // First MERGE via prepared statement
            try (PreparedStatement ps = conn.prepareStatement("""
                    MERGE INTO ext_tgt AS t USING ext_src AS s ON t.id = s.id
                    WHEN MATCHED AND s.a IS NOT NULL THEN UPDATE SET a = s.a, b = coalesce(s.b, t.b)
                    WHEN NOT MATCHED AND s.id IS NOT NULL THEN INSERT (id, a, b) VALUES (s.id, s.a, s.b)
                    """)) {
                ps.execute();
            }

            try (Statement s = conn.createStatement()) {
                s.execute("INSERT INTO ext_src2 VALUES (1,111,'upd'),(2,222,'del'),(6,666,'ins')");
            }

            // Second MERGE via prepared statement
            try (PreparedStatement ps = conn.prepareStatement("""
                    MERGE INTO ext_tgt AS t USING ext_src2 AS s ON t.id = s.id
                    WHEN MATCHED AND s.flag = 'del' THEN DELETE
                    WHEN MATCHED AND s.flag = 'upd' THEN UPDATE SET a = s.a, b = s.flag
                    WHEN NOT MATCHED THEN INSERT (id, a, b) VALUES (s.id, s.a, s.flag)
                    """)) {
                ps.execute();
            }

            // Check identity value for id=6
            try (PreparedStatement ps = conn.prepareStatement("SELECT c FROM ext_tgt WHERE id = ?")) {
                ps.setInt(1, 6);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Row with id=6 should exist");
                    assertEquals(7, rs.getInt(1),
                        "Identity column c for id=6 should be 7");
                }
            }
        } finally {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE ext_src2");
                s.execute("DROP TABLE ext_src");
                s.execute("DROP TABLE ext_tgt");
            }
        }
    }
}
