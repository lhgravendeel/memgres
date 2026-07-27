package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A statement either happens or it does not. A multi-row INSERT refused on a later row was keeping
 * every row before it, which is worse than the error itself: the caller is told the statement
 * failed while the table holds part of its data, and nothing in the result says which part.
 */
class FailedStatementAtomicityTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static void assertState(String expectedState, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(expectedState, e.getSQLState(),
                "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
    }

    @BeforeEach
    void freshTable() throws Exception {
        exec("DROP TABLE IF EXISTS fsa_t CASCADE");
        exec("CREATE TABLE fsa_t (i int PRIMARY KEY, j text NOT NULL)");
        exec("INSERT INTO fsa_t VALUES (1,'a')");
    }

    @Test
    void aDuplicateInALaterRowUndoesTheEarlierOnes() throws Exception {
        assertState("23505", "INSERT INTO fsa_t VALUES (5,'e'),(5,'f')");
        assertEquals("1", scalar("SELECT count(*)::text FROM fsa_t"));
        assertEquals("1", scalar("SELECT string_agg(i::text, ',' ORDER BY i) FROM fsa_t"));
    }

    @Test
    void aNotNullFailureInALaterRowUndoesTheEarlierOnes() throws Exception {
        assertState("23502", "INSERT INTO fsa_t VALUES (6,'g'),(7,NULL)");
        assertEquals("1", scalar("SELECT count(*)::text FROM fsa_t"));
    }

    @Test
    void aCheckFailureInALaterRowUndoesTheEarlierOnes() throws Exception {
        exec("ALTER TABLE fsa_t ADD CONSTRAINT fsa_ck CHECK (i < 100)");
        assertState("23514", "INSERT INTO fsa_t VALUES (8,'h'),(200,'i')");
        assertEquals("1", scalar("SELECT count(*)::text FROM fsa_t"));
    }

    @Test
    void anOnConflictFailureUndoesTheRowsBeforeIt() throws Exception {
        assertState("21000", "INSERT INTO fsa_t VALUES (9,'p'),(9,'q')"
                + " ON CONFLICT (i) DO UPDATE SET j = EXCLUDED.j");
        assertEquals("1", scalar("SELECT count(*)::text FROM fsa_t"));
    }

    @Test
    void anUpdateAppliedBeforeTheFailureIsAlsoUndone() throws Exception {
        exec("INSERT INTO fsa_t VALUES (10,'ten')");
        assertState("21000", "INSERT INTO fsa_t VALUES (10,'changed'),(10,'again')"
                + " ON CONFLICT (i) DO UPDATE SET j = EXCLUDED.j");
        // the first row's update must not survive the statement that failed
        assertEquals("ten", scalar("SELECT j FROM fsa_t WHERE i = 10"));
    }

    @Test
    void insertSelectIsEquallyAllOrNothing() throws Exception {
        exec("DROP TABLE IF EXISTS fsa_src CASCADE");
        exec("CREATE TABLE fsa_src (i int, j text)");
        exec("INSERT INTO fsa_src VALUES (20,'t'),(21,'u'),(1,'dup')");
        assertState("23505", "INSERT INTO fsa_t SELECT i, j FROM fsa_src ORDER BY i DESC");
        assertEquals("0", scalar("SELECT count(*)::text FROM fsa_t WHERE i IN (20,21)"));
        exec("DROP TABLE fsa_src CASCADE");
    }

    @Test
    void aStatementThatSucceedsKeepsEveryRow() throws Exception {
        exec("INSERT INTO fsa_t VALUES (30,'x'),(31,'y'),(32,'z')");
        assertEquals("3", scalar("SELECT count(*)::text FROM fsa_t WHERE i IN (30,31,32)"));
        // a skipped conflict is not a failure, so the rest of the statement stands
        exec("INSERT INTO fsa_t VALUES (30,'skip'),(33,'w') ON CONFLICT (i) DO NOTHING");
        assertEquals("30,33", scalar("SELECT string_agg(i::text, ',' ORDER BY i)"
                + " FROM fsa_t WHERE i IN (30,33)"));
        assertEquals("x", scalar("SELECT j FROM fsa_t WHERE i = 30"));
    }

    @Test
    void theSameHoldsInsideAnExplicitTransaction() throws Exception {
        conn.setAutoCommit(false);
        try {
            assertState("23505", "INSERT INTO fsa_t VALUES (40,'m'),(40,'n')");
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }
        assertEquals("0", scalar("SELECT count(*)::text FROM fsa_t WHERE i = 40"));
        exec("INSERT INTO fsa_t VALUES (41,'m')");
        assertEquals("1", scalar("SELECT count(*)::text FROM fsa_t WHERE i = 41"));
    }
}
