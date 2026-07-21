package com.memgres.partition;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for partition/RR integrity issues: C4, C9, C10, C11, M7.
 */
class PartitionRrIntegrityTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void stop() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private List<String> query(String sql) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append(",");
                    sb.append(rs.getString(i));
                }
                rows.add(sb.toString());
            }
        }
        return rows;
    }

    // === C4: ATTACH PARTITION column validation ===

    @Test
    void c4_attachPartitionColumnMismatchRejected() throws Exception {
        exec("CREATE TABLE c4_parent(id int, name text) PARTITION BY RANGE(id)");
        exec("CREATE TABLE c4_bad(id int, extra text, name text)");
        try {
            SQLException ex = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE c4_parent ATTACH PARTITION c4_bad FOR VALUES FROM (1) TO (10)"));
            // PG 18 reports column-set/type mismatches on ATTACH as 42804
            // (ERRCODE_DATATYPE_MISMATCH), not 42P16.
            assertEquals("42804", ex.getSQLState());
        } finally {
            exec("DROP TABLE IF EXISTS c4_parent, c4_bad");
        }
    }

    @Test
    void c4_attachPartitionRowBoundsViolation() throws Exception {
        exec("CREATE TABLE c4b_parent(id int) PARTITION BY RANGE(id)");
        exec("CREATE TABLE c4b_part(id int)");
        exec("INSERT INTO c4b_part VALUES (50)");
        try {
            SQLException ex = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE c4b_parent ATTACH PARTITION c4b_part FOR VALUES FROM (1) TO (10)"));
            assertEquals("23514", ex.getSQLState());
        } finally {
            exec("DROP TABLE IF EXISTS c4b_parent, c4b_part");
        }
    }

    // === C9: RR snapshot reflects own TRUNCATE ===

    @Test
    void c9_rrSnapshotReflectsOwnTruncate() throws Exception {
        exec("CREATE TABLE c9_t(id int)");
        exec("INSERT INTO c9_t VALUES (1),(2),(3)");
        try {
            conn.setAutoCommit(false);
            exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            assertEquals(List.of("3"), query("SELECT count(*) FROM c9_t"));
            exec("TRUNCATE c9_t");
            assertEquals(List.of("0"), query("SELECT count(*) FROM c9_t"));
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
            exec("DROP TABLE IF EXISTS c9_t");
        }
    }

    // === C10: Partitioned parent reflects own INSERT under RR ===

    @Test
    void c10_partitionedParentReflectsOwnInsertUnderRR() throws Exception {
        exec("CREATE TABLE c10_parent(id int) PARTITION BY RANGE(id)");
        exec("CREATE TABLE c10_child PARTITION OF c10_parent FOR VALUES FROM (1) TO (100)");
        exec("INSERT INTO c10_parent VALUES (1)");
        try {
            conn.setAutoCommit(false);
            exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            assertEquals(List.of("1"), query("SELECT count(*) FROM c10_parent"));
            exec("INSERT INTO c10_parent VALUES (2)");
            assertEquals(List.of("2"), query("SELECT count(*) FROM c10_parent"));
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
            exec("DROP TABLE IF EXISTS c10_parent");
        }
    }

    // === C11: ROLLBACK restores sequences after TRUNCATE RESTART IDENTITY ===

    @Test
    void c11_rollbackRestoresSequencesAfterTruncateRestartIdentity() throws Exception {
        exec("CREATE TABLE c11_t(id serial PRIMARY KEY, v text) PARTITION BY RANGE(id)");
        exec("CREATE TABLE c11_child PARTITION OF c11_t FOR VALUES FROM (1) TO (1000)");
        exec("INSERT INTO c11_t(v) VALUES ('a'),('b')");
        try {
            conn.setAutoCommit(false);
            exec("TRUNCATE c11_t RESTART IDENTITY");
            conn.rollback();
            conn.setAutoCommit(true);
            exec("INSERT INTO c11_t(v) VALUES ('c')");
            List<String> ids = query("SELECT id FROM c11_t ORDER BY id");
            assertEquals(List.of("1", "2", "3"), ids);
        } finally {
            conn.setAutoCommit(true);
            exec("DROP TABLE IF EXISTS c11_t");
        }
    }

    // === M7: RR write-write conflict raises 40001 ===

    @Test
    void m7_rrWriteWriteConflictRaises40001() throws Exception {
        exec("CREATE TABLE m7_t(id int PRIMARY KEY, v int)");
        exec("INSERT INTO m7_t VALUES (1, 0)");
        Connection conn2 = null;
        try {
            conn2 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword());
            conn2.setAutoCommit(false);

            // Session 1: start RR, read
            conn.setAutoCommit(false);
            exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            query("SELECT * FROM m7_t");

            // Session 2: start RR, update and commit
            try (Statement s2 = conn2.createStatement()) {
                s2.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                s2.execute("UPDATE m7_t SET v = 2 WHERE id = 1");
            }
            conn2.commit();

            // Session 1: try to update same row → 40001
            SQLException ex = assertThrows(SQLException.class,
                () -> exec("UPDATE m7_t SET v = 3 WHERE id = 1"));
            assertEquals("40001", ex.getSQLState());
        } finally {
            try { conn.rollback(); } catch (Exception ignored) {}
            conn.setAutoCommit(true);
            if (conn2 != null) conn2.close();
            exec("DROP TABLE IF EXISTS m7_t");
        }
    }

    @Test
    void m7_rrNoConflictWhenDifferentRows() throws Exception {
        exec("CREATE TABLE m7b_t(id int PRIMARY KEY, v int)");
        exec("INSERT INTO m7b_t VALUES (1, 0), (2, 0)");
        Connection conn2 = null;
        try {
            conn2 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword());
            conn2.setAutoCommit(false);

            // Session 1: start RR
            conn.setAutoCommit(false);
            exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            query("SELECT * FROM m7b_t");

            // Session 2: update row 2 and commit
            try (Statement s2 = conn2.createStatement()) {
                s2.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                s2.execute("UPDATE m7b_t SET v = 2 WHERE id = 2");
            }
            conn2.commit();

            // Session 1: update row 1 (different row) → should succeed
            exec("UPDATE m7b_t SET v = 3 WHERE id = 1");
            conn.commit();
        } finally {
            try { conn.rollback(); } catch (Exception ignored) {}
            conn.setAutoCommit(true);
            if (conn2 != null) conn2.close();
            exec("DROP TABLE IF EXISTS m7b_t");
        }
    }
}
