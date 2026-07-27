package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PostgreSQL settles the ON CONFLICT clause while planning: a column or constraint that does not
 * exist is reported before any row is examined, and a constraint with no unique index has nothing
 * to arbitrate against. It also refuses to update the same row twice within one statement, because
 * the result would then depend on the order the rows happened to be processed.
 */
class OnConflictTargetTest {

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
        exec("DROP TABLE IF EXISTS oct_t CASCADE");
        exec("CREATE TABLE oct_t (i int PRIMARY KEY, j text, k int UNIQUE)");
        exec("INSERT INTO oct_t VALUES (1,'a',10),(2,'b',20)");
        exec("ALTER TABLE oct_t ADD CONSTRAINT oct_ck CHECK (i > 0)");
    }

    @Test
    void theAssignmentListMustNameRealColumns() {
        assertState("42703", "INSERT INTO oct_t VALUES (9,'x',90)"
                + " ON CONFLICT (i) DO UPDATE SET nosuchcol = 'x'");
        // the clause is settled while planning, so a false WHERE does not excuse it
        assertState("42703", "INSERT INTO oct_t VALUES (9,'x',90)"
                + " ON CONFLICT (i) DO UPDATE SET nosuchcol = 'x' WHERE false");
        // nor does there being no conflicting row
        assertState("42703", "INSERT INTO oct_t VALUES (99,'x',990)"
                + " ON CONFLICT (i) DO UPDATE SET nosuchcol = 'x'");
    }

    @Test
    void aNamedConstraintMustExist() {
        assertState("42704", "INSERT INTO oct_t VALUES (9,'x',90)"
                + " ON CONFLICT ON CONSTRAINT oct_no_such DO NOTHING");
    }

    @Test
    void aNamedConstraintMustHaveAUniqueIndex() {
        assertState("42809", "INSERT INTO oct_t VALUES (9,'x',90)"
                + " ON CONFLICT ON CONSTRAINT oct_ck DO NOTHING");
        assertState("42809", "INSERT INTO oct_t VALUES (9,'x',90)"
                + " ON CONFLICT ON CONSTRAINT oct_ck DO UPDATE SET j = 'y'");
    }

    @Test
    void oneStatementMayNotUpdateTheSameRowTwice() {
        assertState("21000", "INSERT INTO oct_t VALUES (20,'p',200),(20,'q',201)"
                + " ON CONFLICT (i) DO UPDATE SET j = EXCLUDED.j");
        // the same through a unique constraint rather than the primary key
        assertState("21000", "INSERT INTO oct_t VALUES (30,'p',300),(31,'q',300)"
                + " ON CONFLICT (k) DO UPDATE SET j = EXCLUDED.j");
    }

    @Test
    void doNothingMayMeetTheSameKeyRepeatedly() throws Exception {
        exec("INSERT INTO oct_t VALUES (21,'p',210),(21,'q',211) ON CONFLICT (i) DO NOTHING");
        assertEquals("p", scalar("SELECT j FROM oct_t WHERE i = 21"));
    }

    @Test
    void distinctKeysInOneStatementAreFine() throws Exception {
        exec("INSERT INTO oct_t VALUES (70,'p',700),(71,'q',710)"
                + " ON CONFLICT (i) DO UPDATE SET j = EXCLUDED.j");
        assertEquals("p,q", scalar("SELECT string_agg(j, ',' ORDER BY j)"
                + " FROM oct_t WHERE i IN (70,71)"));
    }

    @Test
    void theWorkingFormsAreUnaffected() throws Exception {
        exec("INSERT INTO oct_t VALUES (1,'upd',11) ON CONFLICT (i) DO UPDATE SET j = EXCLUDED.j");
        assertEquals("upd", scalar("SELECT j FROM oct_t WHERE i = 1"));
        exec("INSERT INTO oct_t VALUES (1,'skipped',12) ON CONFLICT (i) DO NOTHING");
        assertEquals("upd", scalar("SELECT j FROM oct_t WHERE i = 1"));
        exec("INSERT INTO oct_t VALUES (1,'byconstraint',13)"
                + " ON CONFLICT ON CONSTRAINT oct_t_pkey DO UPDATE SET j = EXCLUDED.j");
        assertEquals("byconstraint", scalar("SELECT j FROM oct_t WHERE i = 1"));
        // a conflict on the secondary unique key
        exec("INSERT INTO oct_t VALUES (50,'bykey',20) ON CONFLICT (k) DO UPDATE SET j = EXCLUDED.j");
        assertEquals("bykey", scalar("SELECT j FROM oct_t WHERE k = 20"));
        // a WHERE clause on the action still filters
        exec("INSERT INTO oct_t VALUES (1,'nope',14)"
                + " ON CONFLICT (i) DO UPDATE SET j = 'nope' WHERE oct_t.i > 100");
        assertEquals("byconstraint", scalar("SELECT j FROM oct_t WHERE i = 1"));
        // both the target and EXCLUDED may be referenced
        exec("INSERT INTO oct_t VALUES (1,'tail',15)"
                + " ON CONFLICT (i) DO UPDATE SET j = oct_t.j || '/' || EXCLUDED.j");
        assertEquals("byconstraint/tail", scalar("SELECT j FROM oct_t WHERE i = 1"));
        // a targetless DO NOTHING still absorbs any unique violation
        exec("INSERT INTO oct_t VALUES (1,'zzz',10) ON CONFLICT DO NOTHING");
        assertEquals("byconstraint/tail", scalar("SELECT j FROM oct_t WHERE i = 1"));
    }

    @Test
    void existingTargetRulesStillApply() {
        // a target column with no unique constraint behind it
        assertState("42P10", "INSERT INTO oct_t VALUES (60,'x',600) ON CONFLICT (j) DO NOTHING");
        // a target column that does not exist
        assertState("42703", "INSERT INTO oct_t VALUES (61,'x',610)"
                + " ON CONFLICT (nosuchcol) DO NOTHING");
        // DO UPDATE with no target to arbitrate on
        assertState("42601", "INSERT INTO oct_t VALUES (62,'x',620)"
                + " ON CONFLICT DO UPDATE SET j = 'x'");
    }
}
