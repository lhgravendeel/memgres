package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diff #40: EXECUTE p_limit(1) crashes with "Received resultset tuples, but no field structure"
 * Diff #46: CTE + FOR UPDATE SKIP LOCKED + UPDATE FROM RETURNING crashes same way
 *
 * Root cause: PgWireHandler doesn't send RowDescription before DataRows on re-EXECUTE
 * or on CTE+UPDATE+RETURNING.
 */
class ProtocolReExecuteTest {

    static Memgres memgres;
    static Connection conn;

    static Connection connSimple;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        // DEFAULT query mode (extended protocol), which is what the verification harness uses
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        // Also a simple-mode connection for setup statements
        connSimple = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        connSimple.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception {
        if (connSimple != null) connSimple.close();
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }
    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData(); int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) { List<String> row = new ArrayList<>(); for (int i = 1; i <= cols; i++) row.add(rs.getString(i)); rows.add(row); }
            return rows;
        }
    }

    // Diff #40: exact pattern from 31_jdbc_prepared_statement_patterns.sql
    // The crash happens when re-executing a prepared statement AFTER other
    // prepared statements have executed in between, overwriting cached RowDescription.
    @Test void re_execute_after_interleaved_prepares_no_crash() throws SQLException {
        exec("CREATE TABLE re_t(id int PRIMARY KEY, a int, b text, flag boolean)");
        exec("INSERT INTO re_t VALUES (1,10,'x',true),(2,20,'y',false),(3,30,'z',NULL)");

        // Create multiple prepared statements (mirrors the verification SQL file)
        exec("PREPARE p_limit(int) AS SELECT id, a FROM re_t ORDER BY id LIMIT $1");
        exec("PREPARE p_cast AS SELECT $1::int + 1, pg_typeof($1::int)");
        exec("PREPARE p_any(int[]) AS SELECT 2 = ANY($1), 4 = ANY($1)");
        exec("PREPARE p_order(bool) AS SELECT id, a FROM re_t ORDER BY CASE WHEN $1 THEN id ELSE a END NULLS LAST");
        exec("PREPARE p_like(text) AS SELECT id, b FROM re_t WHERE coalesce(b,'') LIKE $1 ORDER BY id");
        try {
            // First execute of p_limit, works fine
            assertEquals(2, query("EXECUTE p_limit(2)").size());

            // Now execute MANY other prepared statements in between
            // (this is what happens in the verification SQL between the two p_limit calls)
            query("EXECUTE p_cast('41')");
            query("EXECUTE p_cast(NULL)");
            query("EXECUTE p_any(ARRAY[1,2,3])");
            query("EXECUTE p_order(true)");
            query("EXECUTE p_order(false)");
            query("EXECUTE p_like('x%')");
            query("EXECUTE p_like('%')");

            // NOW re-execute p_limit: this is the line that crashes in the verification suite
            // because the RowDescription cache was overwritten by the interleaved executes
            assertEquals(1, query("EXECUTE p_limit(1)").size(),
                    "Re-EXECUTE of p_limit after interleaved prepares must not crash");
            assertEquals(3, query("EXECUTE p_limit(3)").size());

            // Also re-execute p_any with different values
            List<List<String>> r = query("EXECUTE p_any(ARRAY[2,4,6])");
            assertEquals(1, r.size());
        } finally {
            exec("DEALLOCATE p_limit"); exec("DEALLOCATE p_cast");
            exec("DEALLOCATE p_any"); exec("DEALLOCATE p_order");
            exec("DEALLOCATE p_like");
            exec("DROP TABLE re_t");
        }
    }

    // Simpler case: immediate re-execute (no interleaving) should also work
    @Test void re_execute_immediate_no_crash() throws SQLException {
        exec("CREATE TABLE re2_t(id int PRIMARY KEY, a int)");
        exec("INSERT INTO re2_t VALUES (1,10),(2,20),(3,30)");
        exec("PREPARE p_simple(int) AS SELECT id, a FROM re2_t ORDER BY id LIMIT $1");
        try {
            assertEquals(2, query("EXECUTE p_simple(2)").size());
            assertEquals(1, query("EXECUTE p_simple(1)").size());
            assertEquals(3, query("EXECUTE p_simple(3)").size());
        } finally {
            exec("DEALLOCATE p_simple");
            exec("DROP TABLE re2_t");
        }
    }

    // Diff #46: exact pattern from 38_locking_queue_and_optimistic_locking.sql
    @Test void cte_for_update_skip_locked_update_returning() throws SQLException {
        exec("CREATE TABLE work_item(id int PRIMARY KEY, status text NOT NULL, priority int NOT NULL, attempts int NOT NULL DEFAULT 0, payload text)");
        exec("INSERT INTO work_item VALUES (1,'ready',10,0,'a'),(2,'ready',5,1,'b'),(3,'done',1,0,'c')");
        try {
            List<List<String>> rows = query("""
                WITH next_item AS (
                  SELECT id FROM work_item
                  WHERE status = 'ready'
                  ORDER BY priority DESC, id
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
                )
                UPDATE work_item w
                SET status = 'running', attempts = attempts + 1
                FROM next_item n
                WHERE w.id = n.id
                RETURNING w.id, w.status, w.attempts, w.payload
                """);
            assertEquals(1, rows.size(), "Should return 1 row");
            assertEquals("1", rows.get(0).get(0), "Should pick highest-priority ready item");
            assertEquals("running", rows.get(0).get(1));
        } finally {
            exec("DROP TABLE work_item");
        }
    }

    // Diff #40: The verification harness executes ROLLBACK after each error.
    // This tests that error recovery + ROLLBACK doesn't corrupt protocol state
    // for subsequent EXECUTE commands.
    @Test void re_execute_after_error_and_rollback_no_crash() throws SQLException {
        exec("CREATE TABLE re3_t(id int PRIMARY KEY, a int, b text)");
        exec("INSERT INTO re3_t VALUES (1,10,'x'),(2,20,'y'),(3,30,'z')");
        exec("PREPARE p_lim(int) AS SELECT id, a FROM re3_t ORDER BY id LIMIT $1");
        exec("PREPARE p_any(int[]) AS SELECT 2 = ANY($1), 4 = ANY($1)");
        try {
            // First execute, works fine
            assertEquals(2, query("EXECUTE p_lim(2)").size());

            // Execute other prepared statements
            query("EXECUTE p_any(ARRAY[1,2,3])");

            // Now cause errors followed by ROLLBACK (mimics harness error recovery)
            try { exec("PREPARE bad_null AS SELECT $1 + 1"); } catch (Exception ignored) {}
            try { exec("ROLLBACK"); } catch (Exception ignored) {}

            try { exec("EXECUTE bad_null(NULL)"); } catch (Exception ignored) {}
            try { exec("ROLLBACK"); } catch (Exception ignored) {}

            try { exec("SELECT * FROM nonexistent_table"); } catch (Exception ignored) {}
            try { exec("ROLLBACK"); } catch (Exception ignored) {}

            // NOW re-execute p_lim; this should still work after error recovery
            assertEquals(1, query("EXECUTE p_lim(1)").size(),
                    "Re-EXECUTE after error recovery must not crash");
            assertEquals(3, query("EXECUTE p_lim(3)").size());

            // Re-execute p_any
            query("EXECUTE p_any(ARRAY[4,5,6])");
        } finally {
            try { exec("DEALLOCATE p_lim"); } catch (Exception ignored) {}
            try { exec("DEALLOCATE p_any"); } catch (Exception ignored) {}
            try { exec("DEALLOCATE bad_null"); } catch (Exception ignored) {}
            exec("DROP TABLE re3_t");
        }
    }

    @Test void multiple_re_executes_interleaved() throws SQLException {
        exec("CREATE TABLE re2_t(id int PRIMARY KEY, v text)");
        exec("INSERT INTO re2_t VALUES (1,'a'),(2,'b'),(3,'c')");
        exec("PREPARE p_sel(int) AS SELECT id, v FROM re2_t WHERE id <= $1 ORDER BY id");
        try {
            for (int i = 0; i < 5; i++) {
                assertEquals(2, query("EXECUTE p_sel(2)").size(), "Iteration " + i);
            }
        } finally {
            exec("DEALLOCATE p_sel");
            exec("DROP TABLE re2_t");
        }
    }
}
