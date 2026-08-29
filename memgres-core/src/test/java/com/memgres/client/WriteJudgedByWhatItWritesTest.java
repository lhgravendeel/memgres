package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A write is judged by what it writes, not by the shape of the statement.
 *
 * <p>A view's {@code WITH CHECK OPTION} belongs to the view: a write that reached it through a
 * join is still a write through the view, and so is one a MERGE made. Attached to the plain
 * UPDATE path alone, the guard let the same row through under a different spelling.
 *
 * <p>A MERGE resolves the whole statement before it reads a row, so a clause no row reaches still
 * has to name things that exist — otherwise the statement succeeded or failed depending on the
 * data. Its target may be written with {@code ONLY} or with a star, and its {@code INSERT} takes
 * the {@code OVERRIDING} clause that lets a plain INSERT write an always-generated column.
 *
 * <p>And a conflict clause updates the row where it found it. A write that would put the row in
 * another partition is refused rather than moved, because there is no route from the arbiter to
 * another partition; refused as a partition-constraint violation it named the wrong fault.
 */
class WriteJudgedByWhatItWritesTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String one(String sql) throws SQLException {
        List<String> rows = rows(sql);
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int c = 1; c <= rs.getMetaData().getColumnCount(); c++) {
                    if (c > 1) row.append('/');
                    row.append(rs.getString(c));
                }
                out.add(row.toString());
            }
            return out;
        }
    }

    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getMessage();
        }
    }

    /** The guard is the view's, so every way of writing through it is held to it. */
    @Test
    void aViewsCheckOptionHoldsWhateverShapeTheWriteHas() throws SQLException {
        exec("CREATE TABLE zzw_base (id int, n int)");
        exec("INSERT INTO zzw_base VALUES (1, 5)");
        exec("CREATE VIEW zzw_vw AS SELECT id, n FROM zzw_base WHERE n < 10 WITH CHECK OPTION");
        exec("CREATE TABLE zzw_src (k int, nv int)");
        exec("INSERT INTO zzw_src VALUES (1, 500)");
        try {
            assertEquals("44000", stateOf("UPDATE zzw_vw SET n = 99"));
            assertEquals("44000",
                    stateOf("UPDATE zzw_vw v SET n = s.nv FROM zzw_src s WHERE v.id = s.k"));
            // And the row the statement was refused for is the row it found.
            assertEquals("5", one("SELECT n FROM zzw_base"));
        } finally {
            exec("DROP VIEW zzw_vw");
            exec("DROP TABLE zzw_base, zzw_src");
        }
    }

    /** A MERGE through a view is a write through the view. */
    @Test
    void aMergeThroughAViewIsHeldToTheViewsCheckOption() throws SQLException {
        exec("CREATE TABLE zzw_ga (i int PRIMARY KEY, v int)");
        exec("INSERT INTO zzw_ga VALUES (1, 1)");
        exec("CREATE VIEW zzw_gav AS SELECT i, v FROM zzw_ga WHERE v < 10 WITH CHECK OPTION");
        try {
            assertEquals("44000", stateOf("MERGE INTO zzw_gav t USING (VALUES (1, 90)) s(i, v)"
                    + " ON t.i = s.i WHEN MATCHED THEN UPDATE SET v = s.v"));
            assertEquals("1", one("SELECT v FROM zzw_ga"));
        } finally {
            exec("DROP VIEW zzw_gav");
            exec("DROP TABLE zzw_ga");
        }
    }

    /**
     * A name is looked up when the statement is read, not when the clause fires, so a clause no
     * row reaches still has to be one that could have run.
     */
    @Test
    void aMergeClauseThatNeverFiresIsStillResolved() throws SQLException {
        exec("CREATE TABLE zzw_g9 (id int, v int)");
        exec("INSERT INTO zzw_g9 VALUES (1, 1)");
        try {
            assertEquals("42703", stateOf("MERGE INTO zzw_g9 t USING (VALUES (2, 2)) s(id, v)"
                    + " ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = nosuchcol"));
            assertEquals("42703", stateOf("MERGE INTO zzw_g9 t USING (VALUES (2, 2)) s(id, v)"
                    + " ON t.id = s.id WHEN MATCHED AND nosuchcol = 1 THEN UPDATE SET v = s.v"));
            // A name that is a column of either relation is one the clause may read.
            exec("MERGE INTO zzw_g9 t USING (VALUES (1, 7)) s(id, v) ON t.id = s.id"
                    + " WHEN MATCHED THEN UPDATE SET v = s.v");
            assertEquals("7", one("SELECT v FROM zzw_g9"));
        } finally {
            exec("DROP TABLE zzw_g9");
        }
    }

    /** A MERGE reaches the relation it names, and both spellings of that say so. */
    @Test
    void aMergeTargetMayBeWrittenOnlyOrWithAStar() throws SQLException {
        exec("CREATE TABLE zzw_g5 (id int, v int)");
        exec("INSERT INTO zzw_g5 VALUES (1, 1)");
        try {
            assertEquals(List.of("1/2"), rows("MERGE INTO ONLY zzw_g5 t USING (VALUES (1, 2))"
                    + " s(id, v) ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v"
                    + " RETURNING t.id, t.v"));
            assertEquals(List.of("3"), rows("MERGE INTO zzw_g5 * t USING (VALUES (1, 3)) s(id, v)"
                    + " ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v RETURNING t.v"));
        } finally {
            exec("DROP TABLE zzw_g5");
        }
    }

    /** The INSERT of a MERGE takes the OVERRIDING clause a plain INSERT takes. */
    @Test
    void aMergeInsertMayOverrideAnAlwaysGeneratedIdentity() throws SQLException {
        exec("CREATE TABLE zzw_g6 (i int GENERATED ALWAYS AS IDENTITY, v int)");
        try {
            assertEquals("428C9", stateOf("MERGE INTO zzw_g6 t USING (VALUES (5, 50)) s(i, v)"
                    + " ON t.i = s.i WHEN NOT MATCHED THEN INSERT (i, v) VALUES (s.i, s.v)"));
            assertEquals(List.of("50"), rows("MERGE INTO zzw_g6 t USING (VALUES (5, 50)) s(i, v)"
                    + " ON t.i = s.i WHEN NOT MATCHED THEN INSERT (i, v) OVERRIDING SYSTEM VALUE"
                    + " VALUES (s.i, s.v) RETURNING t.v"));
            assertEquals("5", one("SELECT i FROM zzw_g6"));
        } finally {
            exec("DROP TABLE zzw_g6");
        }
    }

    /** merge_action() belongs to a MERGE's RETURNING list, and says so where it does not stand. */
    @Test
    void mergeActionBelongsToAMergesReturningList() {
        assertEquals("42601", stateOf("SELECT merge_action()"));
        assertTrue(messageOf("SELECT merge_action()").contains(
                "MERGE_ACTION() can only be used in the RETURNING list of a MERGE command"));
    }

    /**
     * A conflict clause updates the row it found, where it found it: there is no route from the
     * arbiter to another partition, so a write that would move the row is refused as that.
     */
    @Test
    void aConflictClauseCannotMoveTheRowToAnotherPartition() throws SQLException {
        exec("CREATE TABLE zzw_p3 (i int PRIMARY KEY, s text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE zzw_p3a PARTITION OF zzw_p3 FOR VALUES FROM (1) TO (10)");
        exec("CREATE TABLE zzw_p3b PARTITION OF zzw_p3 FOR VALUES FROM (10) TO (20)");
        exec("INSERT INTO zzw_p3 VALUES (5, 'a')");
        try {
            String sql = "INSERT INTO zzw_p3 VALUES (5, 'b') ON CONFLICT (i) DO UPDATE SET i = 17";
            assertEquals("0A000", stateOf(sql));
            assertTrue(messageOf(sql).contains("invalid ON UPDATE specification"));
            // A write that stays where the row is goes through.
            exec("INSERT INTO zzw_p3 VALUES (5, 'b') ON CONFLICT (i) DO UPDATE SET s = 'c'");
            assertEquals("c", one("SELECT s FROM zzw_p3a"));
        } finally {
            exec("DROP TABLE zzw_p3 CASCADE");
        }
    }
}
