package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diff #16: After two MERGE operations, row id=6 has identity c=8 instead of c=7.
 * The MERGE INSERT path advances the identity sequence one extra step.
 */
class MergeIdentitySequenceTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }
    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData(); int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) { List<String> row = new ArrayList<>(); for (int i = 1; i <= cols; i++) row.add(rs.getString(i)); rows.add(row); }
            return rows;
        }
    }

    // Exact sequence from 20_merge_returning_scoping.sql
    @Test void merge_identity_sequence_value() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, a int, b text DEFAULT 'd', c int GENERATED ALWAYS AS IDENTITY)");
        exec("CREATE TABLE src(id int, a int, b text)");
        exec("CREATE TABLE src2(id int, a int, flag text)");
        exec("INSERT INTO tgt(id, a, b) VALUES (1,10,'x'),(2,20,'y'),(3,30,'z')");
        // c values: 1, 2, 3
        exec("INSERT INTO src VALUES (1,100,'sx'),(4,400,'sw'),(5,NULL,NULL)");
        try {
            // INSERT+RETURNING to get c=4 for the inserted row (id=10)
            exec("INSERT INTO tgt(id, a, b) VALUES (10, 1000, 'r1')");
            // c=4

            // UPDATE RETURNING
            exec("UPDATE tgt AS t SET a = t.a + 1, b = upper(t.b) WHERE t.id IN (1,2)");

            // DELETE RETURNING
            exec("DELETE FROM tgt t WHERE t.id = 3");

            // First MERGE: update id=1, insert ids 4,5
            exec("""
                MERGE INTO tgt AS t USING src AS s ON t.id = s.id
                WHEN MATCHED AND s.a IS NOT NULL THEN UPDATE SET a = s.a, b = coalesce(s.b, t.b)
                WHEN NOT MATCHED AND s.id IS NOT NULL THEN INSERT (id, a, b) VALUES (s.id, s.a, s.b)
                """);
            // Inserts 2 rows → c=5, c=6

            // Second MERGE: update id=1, delete id=2, insert id=6
            exec("INSERT INTO src2 VALUES (1,111,'upd'),(2,222,'del'),(6,666,'ins')");
            exec("""
                MERGE INTO tgt AS t USING src2 AS s ON t.id = s.id
                WHEN MATCHED AND s.flag = 'del' THEN DELETE
                WHEN MATCHED AND s.flag = 'upd' THEN UPDATE SET a = s.a, b = s.flag
                WHEN NOT MATCHED THEN INSERT (id, a, b) VALUES (s.id, s.a, s.flag)
                """);
            // Inserts 1 row (id=6) → c should be 7

            // Check the identity value for id=6
            List<List<String>> rows = query("SELECT * FROM tgt ORDER BY id");
            // Find the row with id=6
            for (List<String> row : rows) {
                if ("6".equals(row.get(0))) {
                    assertEquals("7", row.get(3),
                        "Identity column c for id=6 should be 7, got " + row.get(3));
                    return;
                }
            }
            fail("Row with id=6 not found in tgt");
        } finally {
            exec("DROP TABLE src2"); exec("DROP TABLE src"); exec("DROP TABLE tgt");
        }
    }
}
