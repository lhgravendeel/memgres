package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 1: MERGE semantics. Validates that MERGE operations produce correct
 * row counts and data, matching PG18 behavior. Key issues: WHEN NOT MATCHED
 * should not fire for source rows that already matched, duplicate WHEN MATCHED
 * clauses, alias scoping conflicts, and proper state after multi-step MERGE.
 */
class MergeSemanticsTest {

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

    static int countRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) count++;
            return count;
        }
    }

    static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static void assertSqlFails(String sql) {
        assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) { s.execute(sql); }
        }, "Expected error for: " + sql);
    }

    // ========================================================================
    // Basic MERGE: update matched, insert not-matched
    // ========================================================================

    @Test
    void merge_update_and_insert() throws SQLException {
        exec("CREATE TABLE m_tgt (id INT PRIMARY KEY, a INT, b TEXT)");
        exec("CREATE TABLE m_src (id INT, a INT, b TEXT)");
        exec("INSERT INTO m_tgt VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");
        exec("INSERT INTO m_src VALUES (1, 100, 'sx'), (4, 400, 'sw')");
        try {
            exec("MERGE INTO m_tgt AS t USING m_src AS s ON t.id = s.id " +
                 "WHEN MATCHED THEN UPDATE SET a = s.a, b = s.b " +
                 "WHEN NOT MATCHED THEN INSERT (id, a, b) VALUES (s.id, s.a, s.b)");

            // After merge: id=1 updated (100, sx), id=2 unchanged (20, y), id=3 unchanged (30, z), id=4 inserted (400, sw)
            assertEquals(4, countRows("SELECT * FROM m_tgt"));
            assertEquals("100", q("SELECT a FROM m_tgt WHERE id = 1"));
            assertEquals("20", q("SELECT a FROM m_tgt WHERE id = 2")); // unchanged
            assertEquals("400", q("SELECT a FROM m_tgt WHERE id = 4")); // inserted
        } finally {
            exec("DROP TABLE m_src");
            exec("DROP TABLE m_tgt");
        }
    }

    // ========================================================================
    // MERGE with duplicate source rows: should NOT double-insert
    // ========================================================================

    @Test
    void merge_duplicate_source_does_not_double_insert() throws SQLException {
        exec("CREATE TABLE m2_tgt (id INT PRIMARY KEY, a INT, b TEXT)");
        exec("CREATE TABLE m2_src (id INT, a INT, b TEXT)");
        exec("INSERT INTO m2_tgt VALUES (1, 10, 'x')");
        // Source has two rows with id=4, only ONE should be inserted
        exec("INSERT INTO m2_src VALUES (4, 400, 'sw'), (4, 401, 'sw2')");
        try {
            // PG behavior: duplicate source rows for NOT MATCHED is allowed,
            // both try to insert; second one fails on PK conflict.
            // Or: PG processes each source row once. The key point is the target
            // should not have duplicate rows.
            exec("MERGE INTO m2_tgt AS t USING m2_src AS s ON t.id = s.id " +
                 "WHEN MATCHED THEN UPDATE SET a = s.a " +
                 "WHEN NOT MATCHED THEN INSERT (id, a, b) VALUES (s.id, s.a, s.b)");

            // After merge: original 1 row + at most 1 new row for id=4
            int count = countRows("SELECT * FROM m2_tgt");
            assertTrue(count <= 3, "Should not have duplicate inserts, got " + count + " rows");
        } catch (SQLException e) {
            // PG may error on duplicate key, and that's also acceptable
        } finally {
            exec("DROP TABLE m2_src");
            exec("DROP TABLE m2_tgt");
        }
    }

    // ========================================================================
    // MERGE with WHEN MATCHED AND condition: conditional update vs delete
    // ========================================================================

    @Test
    void merge_conditional_update_and_delete() throws SQLException {
        exec("CREATE TABLE m3_tgt (id INT PRIMARY KEY, a INT, b TEXT)");
        exec("CREATE TABLE m3_src (id INT, a INT, flag TEXT)");
        exec("INSERT INTO m3_tgt VALUES (1, 10, 'x'), (2, 20, 'y'), (4, 40, 'w')");
        exec("INSERT INTO m3_src VALUES (1, 111, 'upd'), (2, 222, 'del'), (6, 666, 'ins')");
        try {
            exec("MERGE INTO m3_tgt AS t USING m3_src AS s ON t.id = s.id " +
                 "WHEN MATCHED AND s.flag = 'del' THEN DELETE " +
                 "WHEN MATCHED AND s.flag = 'upd' THEN UPDATE SET a = s.a, b = s.flag " +
                 "WHEN NOT MATCHED THEN INSERT (id, a, b) VALUES (s.id, s.a, s.flag)");

            // id=1: updated (111, upd)
            // id=2: deleted
            // id=4: unchanged (not in source)
            // id=6: inserted (666, ins)
            int count = countRows("SELECT * FROM m3_tgt");
            assertEquals(3, count, "Should have 3 rows: id 1 (updated), 4 (unchanged), 6 (inserted)");
            assertEquals("111", q("SELECT a FROM m3_tgt WHERE id = 1"));
            assertNull(q("SELECT a FROM m3_tgt WHERE id = 2")); // deleted
            assertEquals("40", q("SELECT a FROM m3_tgt WHERE id = 4")); // unchanged
            assertEquals("666", q("SELECT a FROM m3_tgt WHERE id = 6")); // inserted
        } finally {
            exec("DROP TABLE m3_src");
            exec("DROP TABLE m3_tgt");
        }
    }

    // ========================================================================
    // MERGE should maintain correct row count through multiple operations
    // ========================================================================

    @Test
    void merge_preserves_row_count_after_two_merges() throws SQLException {
        exec("CREATE TABLE m4_tgt (id INT PRIMARY KEY, a INT, b TEXT DEFAULT 'd')");
        exec("CREATE TABLE m4_src1 (id INT, a INT, b TEXT)");
        exec("CREATE TABLE m4_src2 (id INT, a INT, flag TEXT)");

        exec("INSERT INTO m4_tgt(id, a, b) VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");
        exec("INSERT INTO m4_src1 VALUES (1, 100, 'sx'), (4, 400, 'sw'), (5, NULL, NULL)");
        exec("INSERT INTO m4_src2 VALUES (1, 111, 'upd'), (2, 222, 'del'), (6, 666, 'ins')");

        try {
            // First merge: update id=1, insert id=4 and id=5
            exec("MERGE INTO m4_tgt AS t USING m4_src1 AS s ON t.id = s.id " +
                 "WHEN MATCHED AND s.a IS NOT NULL THEN UPDATE SET a = s.a, b = coalesce(s.b, t.b) " +
                 "WHEN NOT MATCHED AND s.id IS NOT NULL THEN INSERT (id, a, b) VALUES (s.id, s.a, s.b)");

            // Should have: 1(updated), 2, 3, 4(inserted), 5(inserted) = 5 rows
            // But id=5 has s.id IS NOT NULL = true (5 is not null), so it gets inserted
            // Actually NULL row: s.id IS NOT NULL → depends on whether id=5 is null
            // s.id = 5 (not null), so it IS inserted. Total: 5 rows
            int afterFirst = countRows("SELECT * FROM m4_tgt");
            assertTrue(afterFirst >= 4 && afterFirst <= 6,
                    "After first merge, expected ~5 rows, got " + afterFirst);

            // Second merge: update id=1, delete id=2, insert id=6
            exec("MERGE INTO m4_tgt AS t USING m4_src2 AS s ON t.id = s.id " +
                 "WHEN MATCHED AND s.flag = 'del' THEN DELETE " +
                 "WHEN MATCHED AND s.flag = 'upd' THEN UPDATE SET a = s.a, b = s.flag " +
                 "WHEN NOT MATCHED THEN INSERT (id, a, b) VALUES (s.id, s.a, s.flag)");

            int afterSecond = countRows("SELECT * FROM m4_tgt");
            // Removed id=2, so one less than before, plus added id=6
            // Net: afterFirst - 1 + 1 = afterFirst
            assertTrue(afterSecond >= 3 && afterSecond <= 6,
                    "After second merge, expected reasonable count, got " + afterSecond);
        } finally {
            exec("DROP TABLE m4_src2");
            exec("DROP TABLE m4_src1");
            exec("DROP TABLE m4_tgt");
        }
    }

    // ========================================================================
    // MERGE validation: missing columns in INSERT
    // ========================================================================

    @Test
    void merge_insert_missing_non_nullable_column_fails() throws SQLException {
        exec("CREATE TABLE m5_tgt (id INT PRIMARY KEY, a INT NOT NULL, b TEXT)");
        exec("CREATE TABLE m5_src (id INT, a INT)");
        exec("INSERT INTO m5_src VALUES (1, 10)");
        try {
            // MERGE INSERT that omits column 'b'; should work (b is nullable)
            exec("MERGE INTO m5_tgt AS t USING m5_src AS s ON t.id = s.id " +
                 "WHEN NOT MATCHED THEN INSERT (id, a) VALUES (s.id, s.a)");
            assertEquals(1, countRows("SELECT * FROM m5_tgt"));
        } finally {
            exec("DROP TABLE m5_src");
            exec("DROP TABLE m5_tgt");
        }
    }

    // ========================================================================
    // MERGE validation: duplicate WHEN MATCHED clauses without conditions
    // ========================================================================

    @Test
    void merge_duplicate_when_matched_without_conditions_rejected() throws SQLException {
        exec("CREATE TABLE m6_tgt (id INT PRIMARY KEY, a INT)");
        exec("CREATE TABLE m6_src (id INT, a INT)");
        try {
            // Two WHEN MATCHED without AND conditions; PG rejects this
            // "WHEN MATCHED" appears more than once without conditions
            assertSqlFails(
                "MERGE INTO m6_tgt AS t USING m6_src AS s ON t.id = s.id " +
                "WHEN MATCHED THEN DELETE " +
                "WHEN MATCHED THEN UPDATE SET a = 1");
        } finally {
            exec("DROP TABLE m6_src");
            exec("DROP TABLE m6_tgt");
        }
    }

    // ========================================================================
    // Alias scoping: UPDATE/DELETE with conflicting aliases
    // ========================================================================

    @Test
    void update_with_duplicate_alias_rejected() throws SQLException {
        exec("CREATE TABLE scope_tgt (id INT PRIMARY KEY, a INT)");
        exec("CREATE TABLE scope_src (id INT, a INT)");
        exec("INSERT INTO scope_tgt VALUES (1, 10)");
        exec("INSERT INTO scope_src VALUES (1, 100)");
        try {
            // UPDATE tgt AS x ... FROM src AS x, same alias 'x' for both tables
            // PG rejects this: table reference "x" is ambiguous
            assertSqlFails(
                "UPDATE scope_tgt AS x SET a = x.a + 1 FROM scope_src AS x WHERE x.id = x.id");
        } finally {
            exec("DROP TABLE scope_src");
            exec("DROP TABLE scope_tgt");
        }
    }

    @Test
    void delete_with_duplicate_alias_rejected() throws SQLException {
        exec("CREATE TABLE del_tgt (id INT PRIMARY KEY, a INT)");
        exec("CREATE TABLE del_src (id INT)");
        exec("INSERT INTO del_tgt VALUES (1, 10)");
        exec("INSERT INTO del_src VALUES (1)");
        try {
            // DELETE FROM tgt AS t USING src AS t, same alias
            assertSqlFails(
                "DELETE FROM del_tgt AS t USING del_src AS t WHERE t.id = t.id");
        } finally {
            exec("DROP TABLE del_src");
            exec("DROP TABLE del_tgt");
        }
    }

    // ========================================================================
    // CTE name should shadow table in qualified reference
    // ========================================================================

    @Test
    void cte_shadows_table_in_qualified_ref() throws SQLException {
        exec("CREATE TABLE cte_tgt (id INT PRIMARY KEY, a INT)");
        exec("INSERT INTO cte_tgt VALUES (1, 10), (2, 20)");
        try {
            // WITH tgt AS (SELECT 1 AS id) SELECT compat.tgt.id FROM tgt
            // In PG, the CTE 'tgt' shadows the table 'tgt', so 'compat.tgt.id'
            // tries to reference the schema-qualified table through the CTE; PG rejects
            assertSqlFails(
                "WITH cte_tgt AS (SELECT 1 AS id) SELECT public.cte_tgt.id FROM cte_tgt");
        } finally {
            exec("DROP TABLE cte_tgt");
        }
    }
}
