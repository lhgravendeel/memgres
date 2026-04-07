package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MERGE snapshot isolation semantics.
 *
 * PG processes all source rows against the ORIGINAL target table state (snapshot).
 * This means:
 * - If source has duplicate keys, both see the same target state → both try INSERT → PK error
 * - A source row that inserts into the target does NOT make subsequent source rows see the new row
 * - INSERT into GENERATED ALWAYS AS IDENTITY columns via MERGE should respect identity semantics
 * - MERGE with DELETE+UPDATE should not double-process a matched row
 */
class MergeSnapshotIsolationTest {

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

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    static int count(String table) throws SQLException {
        return Integer.parseInt(query("SELECT count(*) FROM " + table).get(0).get(0));
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // Duplicate source rows cause PK violation (snapshot semantics)
    // ========================================================================

    @Test
    void merge_duplicate_source_keys_causes_pk_error() throws SQLException {
        exec("CREATE TABLE msi_tgt(id int PRIMARY KEY, a int, b text DEFAULT 'd', c int GENERATED ALWAYS AS IDENTITY)");
        exec("CREATE TABLE msi_src(id int, a int, b text)");
        exec("INSERT INTO msi_tgt(id, a, b) VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");
        // Source has duplicate id=4; both should see "NOT MATCHED" against the original snapshot
        exec("INSERT INTO msi_src VALUES (1, 100, 'sx'), (4, 400, 'sw'), (4, 401, 'sw2'), (5, NULL, NULL)");
        try {
            // PG: Both id=4 source rows see original target (no id=4) → both try INSERT → PK violation
            assertThrows(SQLException.class, () -> exec("""
                MERGE INTO msi_tgt AS t
                USING msi_src AS s
                ON t.id = s.id
                WHEN MATCHED AND s.a IS NOT NULL THEN
                  UPDATE SET a = s.a, b = coalesce(s.b, t.b)
                WHEN NOT MATCHED AND s.id IS NOT NULL THEN
                  INSERT (id, a, b) VALUES (s.id, s.a, s.b)
                """), "Duplicate source keys mapping to same PK should fail");
        } finally {
            exec("DROP TABLE msi_src"); exec("DROP TABLE msi_tgt");
        }
    }

    @Test
    void merge_without_dup_source_keys_succeeds() throws SQLException {
        // Same schema but no duplicate source keys, so it should work fine
        exec("CREATE TABLE ms2_tgt(id int PRIMARY KEY, a int, b text DEFAULT 'd', c int GENERATED ALWAYS AS IDENTITY)");
        exec("CREATE TABLE ms2_src(id int, a int, b text)");
        exec("INSERT INTO ms2_tgt(id, a, b) VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");
        exec("INSERT INTO ms2_src VALUES (1, 100, 'sx'), (4, 400, 'sw'), (5, NULL, NULL)");
        try {
            exec("""
                MERGE INTO ms2_tgt AS t
                USING ms2_src AS s
                ON t.id = s.id
                WHEN MATCHED AND s.a IS NOT NULL THEN
                  UPDATE SET a = s.a, b = coalesce(s.b, t.b)
                WHEN NOT MATCHED AND s.id IS NOT NULL THEN
                  INSERT (id, a, b) VALUES (s.id, s.a, s.b)
                """);
            // id=1 matched→update, ids 2,3 no src→untouched, ids 4,5 inserted = 5
            assertEquals(5, count("ms2_tgt"), "5 rows after MERGE without dup source keys");
        } finally {
            exec("DROP TABLE ms2_src"); exec("DROP TABLE ms2_tgt");
        }
    }

    // ========================================================================
    // Row counts after two consecutive MERGE operations
    // (exact pattern from 20_merge_returning_scoping.sql)
    // ========================================================================

    @Test
    void two_merges_produce_correct_final_row_count() throws SQLException {
        exec("CREATE TABLE tm_tgt(id int PRIMARY KEY, a int, b text DEFAULT 'd', c int GENERATED ALWAYS AS IDENTITY)");
        exec("CREATE TABLE tm_src1(id int, a int, b text)");
        exec("CREATE TABLE tm_src2(id int, a int, flag text)");
        exec("INSERT INTO tm_tgt(id, a, b) VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");
        // Non-duplicate source for first merge
        exec("INSERT INTO tm_src1 VALUES (1, 100, 'sx'), (4, 400, 'sw'), (5, NULL, NULL)");
        exec("INSERT INTO tm_src2 VALUES (1, 111, 'upd'), (2, 222, 'del'), (6, 666, 'ins')");
        try {
            // First MERGE: update id=1, insert ids 4,5
            exec("""
                MERGE INTO tm_tgt AS t
                USING tm_src1 AS s
                ON t.id = s.id
                WHEN MATCHED AND s.a IS NOT NULL THEN
                  UPDATE SET a = s.a, b = coalesce(s.b, t.b)
                WHEN NOT MATCHED AND s.id IS NOT NULL THEN
                  INSERT (id, a, b) VALUES (s.id, s.a, s.b)
                """);
            assertEquals(5, count("tm_tgt"), "After first MERGE: 3 original + 2 inserted = 5");

            // Second MERGE: update id=1, delete id=2, insert id=6
            exec("""
                MERGE INTO tm_tgt AS t
                USING tm_src2 AS s
                ON t.id = s.id
                WHEN MATCHED AND s.flag = 'del' THEN DELETE
                WHEN MATCHED AND s.flag = 'upd' THEN UPDATE SET a = s.a, b = s.flag
                WHEN NOT MATCHED THEN INSERT (id, a, b) VALUES (s.id, s.a, s.flag)
                """);

            List<List<String>> rows = query("SELECT id FROM tm_tgt ORDER BY id");
            // id=1 updated, id=2 deleted, ids 3,4,5 untouched, id=6 inserted = 5
            assertEquals(5, rows.size(), "After second MERGE: 5 rows (1 deleted, 1 inserted)");
            assertFalse(rows.stream().anyMatch(r -> "2".equals(r.get(0))), "id=2 should be deleted");
            assertTrue(rows.stream().anyMatch(r -> "6".equals(r.get(0))), "id=6 should be inserted");

            // Verify scoping queries see correct counts
            List<List<String>> ctRes = query(
                    "WITH tm_tgt AS (SELECT id, a FROM tm_tgt WHERE id < 10) SELECT id, a FROM tm_tgt ORDER BY id");
            assertEquals(5, ctRes.size(), "CTE query should see 5 rows");

            assertEquals(5, query("SELECT t.id AS x, t.a FROM tm_tgt t ORDER BY x").size());
            assertEquals(5, query("""
                SELECT sub.id, sub.a FROM (
                  SELECT t.id, t.a, row_number() OVER (ORDER BY t.id) AS rn FROM tm_tgt t
                ) sub WHERE sub.rn >= 1 ORDER BY sub.id
                """).size());
        } finally {
            exec("DROP TABLE tm_src2"); exec("DROP TABLE tm_src1"); exec("DROP TABLE tm_tgt");
        }
    }

    // ========================================================================
    // MERGE should reject writing into identity column
    // ========================================================================

    @Test
    void merge_not_matched_insert_into_identity_column_fails() throws SQLException {
        exec("CREATE TABLE mi_tgt(id int PRIMARY KEY, a int, c int GENERATED ALWAYS AS IDENTITY)");
        exec("CREATE TABLE mi_src(id int, a int)");
        exec("INSERT INTO mi_tgt(id, a) VALUES (1, 10)");
        exec("INSERT INTO mi_src VALUES (1, 100), (2, 200)");
        try {
            // INSERT (id, a, c) with explicit c value should fail for GENERATED ALWAYS
            assertThrows(SQLException.class, () -> exec("""
                MERGE INTO mi_tgt AS t
                USING mi_src AS s
                ON t.id = s.id
                WHEN NOT MATCHED THEN INSERT (id, a, c) VALUES (s.id, s.a, 1)
                """), "Should reject insert into GENERATED ALWAYS identity column");
        } finally {
            exec("DROP TABLE mi_src"); exec("DROP TABLE mi_tgt");
        }
    }

    // ========================================================================
    // MERGE with duplicate WHEN MATCHED (same action type) should fail
    // ========================================================================

    @Test
    void merge_duplicate_unconditional_when_matched_fails() throws SQLException {
        exec("CREATE TABLE dm_tgt(id int PRIMARY KEY, a int)");
        exec("CREATE TABLE dm_src(id int, a int)");
        try {
            assertThrows(SQLException.class, () -> exec("""
                MERGE INTO dm_tgt t USING dm_src s ON t.id = s.id
                WHEN MATCHED THEN DELETE
                WHEN MATCHED THEN UPDATE SET a = 1
                """), "Two unconditional WHEN MATCHED clauses should fail");
        } finally {
            exec("DROP TABLE dm_src"); exec("DROP TABLE dm_tgt");
        }
    }
}
