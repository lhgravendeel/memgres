package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The index-driven answer to {@code WHERE indexed_column = value} must be the answer a full read
 * of the relation would have given, and nothing else about the read may change.
 *
 * <p>Two halves. The first says the index is reached for the shapes it should be reached for -- a
 * primary key, a unique index, an ordinary one, integer and text keys, either side of the {@code
 * =}, with an alias, with more of the clause beside it -- and that a relation big enough for the
 * difference to show is read quickly. The second is the half that matters more: every shape the
 * probe must decline, and every rule about who may see which rows that a lookup could quietly walk
 * past. A test that only proved the fast path fast would not notice the day it started answering
 * with rows a transaction is not entitled to.
 */
class IndexEqualityScanTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = connect();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test", "test", "test");
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    private void exec(String sql) throws SQLException { exec(conn, sql); }

    /** Every row of a query as comma-joined text, in the order the query returned them. */
    private static List<String> rows(Connection c, String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    private List<String> rows(String sql) throws SQLException { return rows(conn, sql); }

    private String one(String sql) throws SQLException {
        List<String> r = rows(sql);
        return r.isEmpty() ? null : r.get(0);
    }

    // ======================== the index is reached ========================

    @Test
    void primaryKeyLookupByIntegerLiteralAndByParameter() throws Exception {
        exec("CREATE TABLE ies_pk (id INTEGER PRIMARY KEY, data TEXT)");
        for (int i = 1; i <= 50; i++) {
            exec("INSERT INTO ies_pk VALUES (" + i + ", 'row" + i + "')");
        }
        assertEquals("37|row37", one("SELECT id, data FROM ies_pk WHERE id = 37"));
        assertEquals(0, rows("SELECT id FROM ies_pk WHERE id = 999").size());
        // A bound parameter is the shape a client actually sends; it has to reach the index too.
        try (PreparedStatement ps = conn.prepareStatement("SELECT data FROM ies_pk WHERE id = ?")) {
            ps.setInt(1, 12);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("row12", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void valueMayBeWrittenOnEitherSideAndTheRelationMayBeAliased() throws Exception {
        exec("CREATE TABLE ies_sides (id INTEGER PRIMARY KEY, data TEXT)");
        exec("INSERT INTO ies_sides VALUES (1,'one'),(2,'two'),(3,'three')");
        assertEquals("two", one("SELECT data FROM ies_sides WHERE id = 2"));
        assertEquals("two", one("SELECT data FROM ies_sides WHERE 2 = id"));
        assertEquals("two", one("SELECT data FROM ies_sides x WHERE x.id = 2"));
        assertEquals("two", one("SELECT data FROM ies_sides x WHERE 2 = x.id"));
        assertEquals("two", one("SELECT data FROM ies_sides AS x WHERE x.id = 2"));
        assertEquals("two", one("SELECT data FROM ies_sides WHERE ies_sides.id = 2"));
    }

    @Test
    void furtherPredicatesBesideTheEqualityAreStillApplied() throws Exception {
        exec("CREATE TABLE ies_extra (id INTEGER PRIMARY KEY, data TEXT, n INTEGER)");
        exec("INSERT INTO ies_extra VALUES (1,'a',10),(2,'b',20),(3,'c',30)");
        assertEquals("b", one("SELECT data FROM ies_extra WHERE id = 2 AND n = 20"));
        assertEquals(0, rows("SELECT data FROM ies_extra WHERE id = 2 AND n = 99").size());
        assertEquals(0, rows("SELECT data FROM ies_extra WHERE id = 2 AND data <> 'b'").size());
        assertEquals("b", one("SELECT data FROM ies_extra WHERE id = 2 AND n > 5 AND data LIKE 'b%'"));
    }

    @Test
    void uniqueIndexOnTextAndAnOrdinaryIndexWithDuplicates() throws Exception {
        exec("CREATE TABLE ies_idx (id SERIAL PRIMARY KEY, code TEXT, grp TEXT)");
        exec("CREATE UNIQUE INDEX ies_idx_code ON ies_idx (code)");
        exec("CREATE INDEX ies_idx_grp ON ies_idx (grp)");
        exec("INSERT INTO ies_idx (code, grp) VALUES ('aa','x'),('bb','x'),('cc','y'),('dd','z')");

        assertEquals("bb|x", one("SELECT code, grp FROM ies_idx WHERE code = 'bb'"));
        assertEquals(0, rows("SELECT code FROM ies_idx WHERE code = 'zz'").size());
        // An ordinary index holds every row under the key, not one of them.
        assertEquals(2, rows("SELECT code FROM ies_idx WHERE grp = 'x' ORDER BY code").size());
        assertEquals("aa", rows("SELECT code FROM ies_idx WHERE grp = 'x' ORDER BY code").get(0));
        assertEquals(1, rows("SELECT code FROM ies_idx WHERE grp = 'y'").size());
        assertEquals(0, rows("SELECT code FROM ies_idx WHERE grp = 'nothing'").size());

        // Rows leaving and arriving keep the index and the relation saying the same thing.
        exec("UPDATE ies_idx SET grp = 'y' WHERE code = 'aa'");
        assertEquals(1, rows("SELECT code FROM ies_idx WHERE grp = 'x'").size());
        assertEquals(2, rows("SELECT code FROM ies_idx WHERE grp = 'y'").size());
        exec("DELETE FROM ies_idx WHERE code = 'bb'");
        assertEquals(0, rows("SELECT code FROM ies_idx WHERE grp = 'x'").size());
        assertEquals(0, rows("SELECT code FROM ies_idx WHERE code = 'bb'").size());
    }

    @Test
    void compositeKeyNeedsEveryColumnAndAnswersTheSameEitherWay() throws Exception {
        exec("CREATE TABLE ies_comp (a INTEGER, b TEXT, val TEXT, PRIMARY KEY (a, b))");
        exec("INSERT INTO ies_comp VALUES (1,'x','ax'),(1,'y','ay'),(2,'x','bx')");
        assertEquals("ay", one("SELECT val FROM ies_comp WHERE a = 1 AND b = 'y'"));
        assertEquals("ay", one("SELECT val FROM ies_comp WHERE b = 'y' AND a = 1"));
        // Half a composite key is no key at all; the relation is read and both rows come back.
        assertEquals(2, rows("SELECT val FROM ies_comp WHERE a = 1 ORDER BY val").size());
        assertEquals(2, rows("SELECT val FROM ies_comp WHERE b = 'x' ORDER BY val").size());
    }

    @Test
    void charKeyIsPaddedTheWayItIsStored() throws Exception {
        exec("CREATE TABLE ies_char (code CHAR(5) PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ies_char VALUES ('ab', 'padded')");
        assertEquals("padded", one("SELECT val FROM ies_char WHERE code = 'ab'"));
        assertEquals("padded", one("SELECT val FROM ies_char WHERE code = 'ab   '"));
    }

    @Test
    void aLargeRelationIsLookedUpRatherThanRead() throws Exception {
        int rowCount = 30_000;
        int lookups = 2_000;
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ies_big (id INTEGER PRIMARY KEY, data TEXT)");
            StringBuilder sb = new StringBuilder("INSERT INTO ies_big VALUES ");
            for (int i = 1; i <= rowCount; i++) {
                if (i > 1) sb.append(',');
                sb.append("(").append(i).append(",'d").append(i).append("')");
            }
            s.execute(sb.toString());
        }
        long start = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement("SELECT data FROM ies_big WHERE id = ?")) {
            for (int i = 1; i <= lookups; i++) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("d" + i, rs.getString(1));
                }
            }
        }
        long elapsed = System.currentTimeMillis() - start;
        // Reading the relation for each lookup is 60 million row visits and takes far longer than
        // this; looking each one up is a few seconds of round trips.
        assertTrue(elapsed < 8_000,
                lookups + " keyed lookups over " + rowCount + " rows took " + elapsed
                        + "ms, which is a full read of the relation each time, not an index lookup");
    }

    // ======================== the index is declined ========================

    @Test
    void aFunctionOverTheColumnIsNotTheColumn() throws Exception {
        exec("CREATE TABLE ies_fn (code TEXT PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ies_fn VALUES ('ABC','upper'),('def','lower')");
        assertEquals("upper", one("SELECT val FROM ies_fn WHERE lower(code) = 'abc'"));
        assertEquals("lower", one("SELECT val FROM ies_fn WHERE upper(code) = 'DEF'"));
        assertEquals("upper", one("SELECT val FROM ies_fn WHERE length(code) = 3 AND code = 'ABC'"));
    }

    @Test
    void aCastOverTheColumnIsNotTheColumn() throws Exception {
        exec("CREATE TABLE ies_cast (id INTEGER PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ies_cast VALUES (7,'seven'),(8,'eight')");
        assertEquals("seven", one("SELECT val FROM ies_cast WHERE id::text = '7'"));
        assertEquals("eight", one("SELECT val FROM ies_cast WHERE CAST(id AS TEXT) = '8'"));
    }

    @Test
    void equalsNullMatchesNothingAndIsNullStillMatches() throws Exception {
        exec("CREATE TABLE ies_null (id INTEGER PRIMARY KEY, code TEXT)");
        exec("CREATE INDEX ies_null_code ON ies_null (code)");
        exec("INSERT INTO ies_null VALUES (1,'a'),(2,NULL)");
        assertEquals(0, rows("SELECT id FROM ies_null WHERE id = NULL").size());
        assertEquals(0, rows("SELECT id FROM ies_null WHERE code = NULL").size());
        assertEquals("2", one("SELECT id FROM ies_null WHERE code IS NULL"));
        assertEquals("1", one("SELECT id FROM ies_null WHERE code IS NOT NULL"));
    }

    @Test
    void aPartialIndexDoesNotAnswerForTheRowsItLeavesOut() throws Exception {
        exec("CREATE TABLE ies_partial (id SERIAL PRIMARY KEY, v INTEGER, flag BOOLEAN)");
        exec("CREATE INDEX ies_partial_v ON ies_partial (v) WHERE flag");
        exec("INSERT INTO ies_partial (v, flag) VALUES (5, TRUE), (5, FALSE), (6, FALSE)");
        // The index holds one of the two rows with v = 5. Both answer the clause.
        assertEquals(2, rows("SELECT id FROM ies_partial WHERE v = 5").size());
        assertEquals(1, rows("SELECT id FROM ies_partial WHERE v = 6").size());
    }

    @Test
    void anExpressionIndexDoesNotAnswerForTheColumn() throws Exception {
        exec("CREATE TABLE ies_expr (code TEXT, val TEXT)");
        exec("CREATE INDEX ies_expr_lower ON ies_expr (lower(code))");
        exec("INSERT INTO ies_expr VALUES ('ABC','one'),('abc','two')");
        assertEquals(1, rows("SELECT val FROM ies_expr WHERE code = 'ABC'").size());
        assertEquals("one", one("SELECT val FROM ies_expr WHERE code = 'ABC'"));
        assertEquals(2, rows("SELECT val FROM ies_expr WHERE lower(code) = 'abc'").size());
    }

    @Test
    void onlyAndInheritanceReadTheTablesTheyName() throws Exception {
        exec("CREATE TABLE ies_parent (id INTEGER PRIMARY KEY, val TEXT)");
        exec("CREATE TABLE ies_child (extra TEXT) INHERITS (ies_parent)");
        exec("INSERT INTO ies_parent VALUES (1,'p1'),(2,'p2')");
        exec("INSERT INTO ies_child VALUES (3,'c3','x')");
        assertEquals("p1", one("SELECT val FROM ies_parent WHERE id = 1"));
        assertEquals("p1", one("SELECT val FROM ONLY ies_parent WHERE id = 1"));
        // The child's row belongs to the parent relation unless ONLY says otherwise.
        assertEquals("c3", one("SELECT val FROM ies_parent WHERE id = 3"));
        assertEquals(0, rows("SELECT val FROM ONLY ies_parent WHERE id = 3").size());
    }

    @Test
    void aValueOfTheWrongTypeIsStillReportedNotSilentlyAnsweredWithNothing() throws Exception {
        exec("CREATE TABLE ies_type (id INTEGER PRIMARY KEY, txt TEXT)");
        exec("CREATE UNIQUE INDEX ies_type_txt ON ies_type (txt)");
        exec("INSERT INTO ies_type VALUES (1,'a')");
        // PostgreSQL: operator does not exist: text = integer
        SQLException textEqInt = assertThrows(SQLException.class,
                () -> rows("SELECT id FROM ies_type WHERE txt = 5"));
        assertEquals("42883", textEqInt.getSQLState());
        // PostgreSQL: invalid input syntax for type integer: "abc"
        SQLException intEqWord = assertThrows(SQLException.class,
                () -> rows("SELECT id FROM ies_type WHERE id = 'abc'"));
        assertEquals("22P02", intEqWord.getSQLState());
    }

    @Test
    void anEnumKeyIsAnsweredByItsOwnEquality() throws Exception {
        exec("CREATE TYPE ies_mood AS ENUM ('sad', 'ok', 'happy')");
        exec("CREATE TABLE ies_enum (m ies_mood UNIQUE, val TEXT)");
        exec("INSERT INTO ies_enum VALUES ('sad','s'),('happy','h')");
        assertEquals("h", one("SELECT val FROM ies_enum WHERE m = 'happy'"));
        assertEquals(0, rows("SELECT val FROM ies_enum WHERE m = 'ok'").size());
    }

    @Test
    void twoEqualitiesOnOneColumnCannotBothHold() throws Exception {
        exec("CREATE TABLE ies_two (id INTEGER PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ies_two VALUES (1,'a'),(2,'b')");
        assertEquals(0, rows("SELECT val FROM ies_two WHERE id = 1 AND id = 2").size());
        assertEquals("a", one("SELECT val FROM ies_two WHERE id = 1 AND id = 1"));
    }

    @Test
    void aNumericKeyIsRoundedTheWayTheColumnStoresIt() throws Exception {
        exec("CREATE TABLE ies_num (n NUMERIC(10,2) PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ies_num VALUES (1.20, 'one-twenty')");
        assertEquals("one-twenty", one("SELECT val FROM ies_num WHERE n = 1.20"));
        assertEquals("one-twenty", one("SELECT val FROM ies_num WHERE n = 1.2"));
        assertEquals("one-twenty", one("SELECT val FROM ies_num WHERE n = 1.200"));
        // Stored as 1.20; 1.2345 is a different number and matches nothing.
        assertEquals(0, rows("SELECT val FROM ies_num WHERE n = 1.2345").size());
    }

    @Test
    void aClauseThatIsNotAnEqualityIsAnsweredInFull() throws Exception {
        exec("CREATE TABLE ies_other (id INTEGER PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ies_other VALUES (1,'a'),(2,'b'),(3,'c')");
        assertEquals(2, rows("SELECT val FROM ies_other WHERE id IN (1,3)").size());
        assertEquals(2, rows("SELECT val FROM ies_other WHERE id = 1 OR id = 2").size());
        assertEquals(2, rows("SELECT val FROM ies_other WHERE id > 1").size());
        assertEquals(2, rows("SELECT val FROM ies_other WHERE id BETWEEN 2 AND 3").size());
        assertEquals(2, rows("SELECT val FROM ies_other WHERE id <> 1").size());
    }

    @Test
    void aQualifierNamingSomethingElseIsNotThisRelationsColumn() throws Exception {
        exec("CREATE TABLE ies_qual (id INTEGER PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ies_qual VALUES (1,'a'),(2,'b')");
        // The alias renames the relation, so the old name no longer reaches it.
        SQLException ex = assertThrows(SQLException.class,
                () -> rows("SELECT val FROM ies_qual x WHERE ies_qual.id = 1"));
        assertNotNull(ex.getSQLState());
    }

    @Test
    void columnAliasesRenameWhatTheQuerySees() throws Exception {
        exec("CREATE TABLE ies_alias (id INTEGER PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ies_alias VALUES (1,'a'),(2,'b')");
        assertEquals("b", one("SELECT v FROM ies_alias AS t(k, v) WHERE k = 2"));
        assertEquals(0, rows("SELECT v FROM ies_alias AS t(k, v) WHERE k = 9").size());
    }

    // ======================== what a lookup may not walk past ========================

    @Test
    void selectPrivilegeIsStillRequiredForAKeyedLookup() throws Exception {
        try (Connection c = connect()) {
            exec(c, "CREATE TABLE ies_priv (id INTEGER PRIMARY KEY, secret TEXT)");
            exec(c, "INSERT INTO ies_priv VALUES (1,'classified')");
            exec(c, "CREATE ROLE ies_reader LOGIN");
            try {
                exec(c, "SET ROLE ies_reader");
                SQLException keyed = assertThrows(SQLException.class,
                        () -> rows(c, "SELECT secret FROM ies_priv WHERE id = 1"));
                assertEquals("42501", keyed.getSQLState());
                SQLException full = assertThrows(SQLException.class,
                        () -> rows(c, "SELECT secret FROM ies_priv"));
                assertEquals("42501", full.getSQLState());
            } finally {
                exec(c, "RESET ROLE");
                exec(c, "DROP ROLE IF EXISTS ies_reader");
            }
        }
    }

    @Test
    void rowLevelSecurityStillFiltersAKeyedLookup() throws Exception {
        try (Connection c = connect()) {
            exec(c, "CREATE TABLE ies_rls (id INTEGER PRIMARY KEY, owner TEXT, val TEXT)");
            exec(c, "INSERT INTO ies_rls VALUES (1,'alice','a'),(2,'ies_bob','b')");
            exec(c, "CREATE ROLE ies_bob LOGIN");
            exec(c, "GRANT SELECT ON ies_rls TO ies_bob");
            exec(c, "ALTER TABLE ies_rls ENABLE ROW LEVEL SECURITY");
            exec(c, "CREATE POLICY ies_rls_own ON ies_rls FOR SELECT TO ies_bob USING (owner = current_user)");
            try {
                exec(c, "SET ROLE ies_bob");
                // Row 1 is alice's. The key finds it; the policy must still take it away.
                assertEquals(0, rows(c, "SELECT val FROM ies_rls WHERE id = 1").size());
                assertEquals(1, rows(c, "SELECT val FROM ies_rls WHERE id = 2").size());
                assertEquals(1, rows(c, "SELECT val FROM ies_rls").size());
            } finally {
                exec(c, "RESET ROLE");
                exec(c, "DROP POLICY IF EXISTS ies_rls_own ON ies_rls");
                // A grant names the role, so PostgreSQL will not drop it while one stands;
                // DROP OWNED BY is what takes them away.
                exec(c, "DROP OWNED BY ies_bob");
                exec(c, "DROP ROLE IF EXISTS ies_bob");
            }
        }
    }

    @Test
    void aKeyedLookupInsideATransactionStillHoldsAccessShare() throws Exception {
        exec("CREATE TABLE ies_lock (id INTEGER PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ies_lock VALUES (1,'a')");
        try (Connection reader = connect(); Connection writer = connect()) {
            exec(reader, "BEGIN");
            assertEquals("a", rows(reader, "SELECT val FROM ies_lock WHERE id = 1").get(0));
            exec(writer, "BEGIN");
            SQLException blocked = assertThrows(SQLException.class,
                    () -> exec(writer, "LOCK TABLE ies_lock IN ACCESS EXCLUSIVE MODE NOWAIT"));
            assertEquals("55P03", blocked.getSQLState());
            exec(writer, "ROLLBACK");
            exec(reader, "COMMIT");
            // With the reader gone the lock is free again, which shows it was the read holding it.
            exec(writer, "BEGIN");
            exec(writer, "LOCK TABLE ies_lock IN ACCESS EXCLUSIVE MODE NOWAIT");
            exec(writer, "ROLLBACK");
        }
    }

    @Test
    void aRepeatableReadTransactionStillSeesOnlyItsSnapshot() throws Exception {
        exec("CREATE TABLE ies_rr (id INTEGER PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ies_rr VALUES (1,'a'),(2,'b'),(3,'c')");
        try (Connection reader = connect(); Connection writer = connect()) {
            exec(reader, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            // The first read takes the snapshot. Taking it through the index must not fix the
            // snapshot at the one row the lookup returned.
            assertEquals("a", rows(reader, "SELECT val FROM ies_rr WHERE id = 1").get(0));

            exec(writer, "INSERT INTO ies_rr VALUES (4,'d')");
            exec(writer, "UPDATE ies_rr SET val = 'B' WHERE id = 2");
            exec(writer, "DELETE FROM ies_rr WHERE id = 3");

            // None of that happened as far as this transaction is concerned.
            assertEquals(0, rows(reader, "SELECT val FROM ies_rr WHERE id = 4").size());
            assertEquals("b", rows(reader, "SELECT val FROM ies_rr WHERE id = 2").get(0));
            assertEquals("c", rows(reader, "SELECT val FROM ies_rr WHERE id = 3").get(0));
            assertEquals(3, rows(reader, "SELECT val FROM ies_rr").size());
            exec(reader, "COMMIT");

            // Once it ends, the relation is what the other session made of it.
            assertEquals(3, rows(reader, "SELECT val FROM ies_rr").size());
            assertEquals("d", rows(reader, "SELECT val FROM ies_rr WHERE id = 4").get(0));
        }
    }

    @Test
    void anotherSessionsUncommittedWorkIsStillUndoneForTheReader() throws Exception {
        exec("CREATE TABLE ies_mvcc (id INTEGER PRIMARY KEY, k INTEGER, val TEXT)");
        exec("CREATE INDEX ies_mvcc_k ON ies_mvcc (k)");
        exec("INSERT INTO ies_mvcc VALUES (1,5,'five'),(2,6,'six')");
        try (Connection reader = connect(); Connection writer = connect()) {
            exec(writer, "BEGIN");
            // The index has already moved this row from key 5 to key 99, but the reader is
            // still owed the row under 5.
            exec(writer, "UPDATE ies_mvcc SET k = 99 WHERE id = 1");
            exec(writer, "INSERT INTO ies_mvcc VALUES (3,7,'seven')");
            exec(writer, "DELETE FROM ies_mvcc WHERE id = 2");

            assertEquals("five", rows(reader, "SELECT val FROM ies_mvcc WHERE k = 5").get(0));
            assertEquals(0, rows(reader, "SELECT val FROM ies_mvcc WHERE k = 99").size());
            assertEquals(0, rows(reader, "SELECT val FROM ies_mvcc WHERE id = 3").size());
            assertEquals("six", rows(reader, "SELECT val FROM ies_mvcc WHERE id = 2").get(0));

            exec(writer, "COMMIT");
            assertEquals(0, rows(reader, "SELECT val FROM ies_mvcc WHERE k = 5").size());
            assertEquals("five", rows(reader, "SELECT val FROM ies_mvcc WHERE k = 99").get(0));
            assertEquals("seven", rows(reader, "SELECT val FROM ies_mvcc WHERE id = 3").get(0));
            assertEquals(0, rows(reader, "SELECT val FROM ies_mvcc WHERE id = 2").size());
        }
    }

    @Test
    void aSessionStillSeesItsOwnUncommittedWorkThroughTheIndex() throws Exception {
        exec("CREATE TABLE ies_own (id INTEGER PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ies_own VALUES (1,'a')");
        try (Connection c = connect()) {
            exec(c, "BEGIN");
            exec(c, "INSERT INTO ies_own VALUES (2,'b')");
            exec(c, "UPDATE ies_own SET val = 'A' WHERE id = 1");
            assertEquals("b", rows(c, "SELECT val FROM ies_own WHERE id = 2").get(0));
            assertEquals("A", rows(c, "SELECT val FROM ies_own WHERE id = 1").get(0));
            exec(c, "ROLLBACK");
            // Rolling back puts the rows back where the index can find them again.
            assertEquals(0, rows(c, "SELECT val FROM ies_own WHERE id = 2").size());
            assertEquals("a", rows(c, "SELECT val FROM ies_own WHERE id = 1").get(0));
        }
    }
}
