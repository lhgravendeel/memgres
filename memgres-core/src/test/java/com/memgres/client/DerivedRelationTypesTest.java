package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * What a relation built from a query is known to hold, and what that lets a check refuse.
 *
 * <p>A derived table, a WITH item, a view, a VALUES list and a function in FROM are all built by
 * running something and describing what came back, so their columns carry the type the builder
 * read off a value. That guess was wrong often enough — a boolean came back as an integer,
 * {@code starts_with} came back as text — that nothing could be refused on the strength of it, and
 * a condition over such a column was accepted whatever its type. The type is now worked out from
 * the definition instead, which is what PostgreSQL settles it from too.
 *
 * <p>The refusals are half of it. The other half is everything around them: an ordinary join on a
 * boolean a sub-query computed, the same through a WITH item, a view and a VALUES list, a
 * correlated sub-query whose derived column shares a name with an outer relation's, a
 * record-returning call's own columns. Every one of those was rejected by an earlier attempt at
 * this, which is why each is asserted here. Every answer below was measured on PostgreSQL 18.
 */
class DerivedRelationTypesTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("DROP VIEW IF EXISTS drt_v CASCADE");
        exec("DROP TABLE IF EXISTS drt_u CASCADE");
        exec("DROP TABLE IF EXISTS drt_t CASCADE");
        exec("CREATE TABLE drt_t (id int PRIMARY KEY, i int, n numeric, s text, b boolean, d date)");
        exec("INSERT INTO drt_t VALUES (1,1,1.5,'aa',true,'2020-01-01'),"
                + "(2,0,0.0,'ab',false,'2020-01-02')");
        exec("CREATE TABLE drt_u (id int PRIMARY KEY, flag boolean)");
        exec("INSERT INTO drt_u VALUES (1,true),(2,false)");
        exec("CREATE VIEW drt_v AS SELECT id, flag AS n FROM drt_u");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP VIEW IF EXISTS drt_v CASCADE");
            exec("DROP TABLE IF EXISTS drt_u CASCADE");
            exec("DROP TABLE IF EXISTS drt_t CASCADE");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** The first line of the message a statement raises, or "OK". */
    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getMessage().split("\n")[0].replace("ERROR: ", "").trim();
        }
    }

    /** The first column of the first row, as text. */
    private static String rowsOf(String sql) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                if (sb.length() > 0) sb.append(",");
                sb.append(rs.getString(1));
            }
            return sb.toString();
        }
    }

    private static final String NOT_INT =
            "argument of WHERE must be type boolean, not type integer";
    private static final String ON_NOT_INT =
            "argument of JOIN/ON must be type boolean, not type integer";

    // =========================================================================
    // A type the definition settles is the column's type
    // =========================================================================

    @Test
    void aDerivedTableIsTypedByItsSelectList() {
        assertEquals(NOT_INT, messageOf("SELECT id FROM (SELECT id, i FROM drt_t) q WHERE i"));
        assertEquals(NOT_INT, messageOf("SELECT id FROM (SELECT * FROM drt_t) q WHERE i"));
        assertEquals("argument of WHERE must be type boolean, not type numeric",
                messageOf("SELECT id FROM (SELECT * FROM drt_t) q WHERE n"));
        assertEquals("argument of WHERE must be type boolean, not type text",
                messageOf("SELECT id FROM (SELECT * FROM drt_t) q WHERE s"));
        assertEquals(NOT_INT,
                messageOf("SELECT count(*) FROM (SELECT * FROM (SELECT i AS k FROM drt_t) y) z"
                        + " WHERE k"));
    }

    @Test
    void aWithItemIsTypedByItsQuery() {
        assertEquals(NOT_INT,
                messageOf("WITH c AS (SELECT id, i FROM drt_t) SELECT id FROM c WHERE i"));
        assertEquals(NOT_INT,
                messageOf("WITH c AS (SELECT * FROM drt_t) SELECT id FROM c WHERE i"));
    }

    /** A recursive item's columns are its non-recursive term's; the other term must match them. */
    @Test
    void aRecursiveWithItemIsTypedByItsNonRecursiveTerm() throws SQLException {
        assertEquals(NOT_INT,
                messageOf("WITH RECURSIVE r(k) AS (SELECT 1 UNION ALL SELECT k + 1 FROM r"
                        + " WHERE k < 3) SELECT count(*) FROM r WHERE k"));
        assertEquals("1",
                rowsOf("WITH RECURSIVE r(k) AS (SELECT true UNION ALL SELECT false FROM r"
                        + " WHERE k) SELECT count(*) FROM r WHERE k"));
    }

    @Test
    void aValuesListIsTypedByItsEntries() throws SQLException {
        assertEquals(NOT_INT, messageOf("SELECT count(*) FROM (VALUES (1)) v(k) WHERE k"));
        assertEquals(NOT_INT, messageOf("SELECT count(*) FROM (VALUES (1),(2)) v(k) WHERE k"));
        assertEquals("2", rowsOf("SELECT count(*) FROM (VALUES (true),(true)) v(k) WHERE k"));
    }

    @Test
    void aViewIsTypedByItsQuery() {
        assertEquals(NOT_INT, messageOf("SELECT count(*) FROM (SELECT * FROM drt_v) q WHERE id"));
        assertEquals(ON_NOT_INT, messageOf("SELECT count(*) FROM drt_t a JOIN drt_v x ON x.id"));
    }

    @Test
    void aFromFunctionIsTypedByWhatItReturns() throws SQLException {
        assertEquals(NOT_INT, messageOf("SELECT g FROM generate_series(1, 2) g WHERE g"));
        assertEquals(NOT_INT, messageOf("SELECT count(*) FROM generate_series(1, 2) AS g(x)"
                + " WHERE x"));
        assertEquals(NOT_INT, messageOf("SELECT count(*) FROM unnest(ARRAY[1,2]) u WHERE u"));
        assertEquals("1", rowsOf("SELECT count(*) FROM unnest(ARRAY[true,false]) u WHERE u"));
    }

    /** A set operation brings its arms to a common type, so only agreement settles one. */
    @Test
    void aSetOperationIsTypedOnlyWhereItsArmsAgree() throws SQLException {
        assertEquals(NOT_INT, messageOf(
                "SELECT count(*) FROM (SELECT id FROM drt_t UNION SELECT id FROM drt_t) q"
                        + " WHERE id"));
        assertEquals("2", rowsOf(
                "SELECT count(*) FROM (SELECT b FROM drt_t UNION ALL SELECT NOT b FROM drt_t) q"
                        + " WHERE b"));
    }

    /** Renaming a column does not retype it, whether the relation is derived or stored. */
    @Test
    void anAliasListRenamesWithoutRetyping() throws SQLException {
        assertEquals(NOT_INT, messageOf("SELECT count(*) FROM (SELECT i FROM drt_t) q(z) WHERE z"));
        assertEquals("1", rowsOf("SELECT count(*) FROM (SELECT b FROM drt_t) q(z) WHERE z"));
        assertEquals(NOT_INT,
                messageOf("SELECT count(*) FROM drt_t x(c1,c2,c3,c4,c5,c6) WHERE c2"));
        assertEquals("1", rowsOf("SELECT count(*) FROM drt_t x(c1,c2,c3,c4,c5,c6) WHERE c5"));
    }

    // =========================================================================
    // ...and everything around the refusal, which is what broke last time
    // =========================================================================

    /** Every join form over a boolean the sub-query computed, and a LATERAL one. */
    @Test
    void aJoinOnADerivedBooleanRuns() throws SQLException {
        String derived = " (SELECT id, starts_with(s,'a') AS f FROM drt_t) x ON x.f";
        assertEquals("4", rowsOf("SELECT count(*) FROM drt_t a JOIN" + derived));
        assertEquals("4", rowsOf("SELECT count(*) FROM drt_t a INNER JOIN" + derived));
        assertEquals("4", rowsOf("SELECT count(*) FROM drt_t a LEFT JOIN" + derived));
        assertEquals("4", rowsOf("SELECT count(*) FROM drt_t a RIGHT JOIN" + derived));
        assertEquals("4", rowsOf("SELECT count(*) FROM drt_t a JOIN"
                + " (SELECT id, isfinite(d) AS f FROM drt_t) x ON x.f"));
        assertEquals("2", rowsOf("SELECT count(*) FROM drt_t a"
                + " JOIN LATERAL (SELECT starts_with(a.s,'a') AS f) x ON x.f"));
        assertEquals("2", rowsOf("SELECT count(*) FROM drt_t a"
                + " LEFT JOIN LATERAL (SELECT starts_with(a.s,'a') AS f) x ON x.f"));
        assertEquals("2", rowsOf("SELECT count(*) FROM drt_t a,"
                + " LATERAL (SELECT starts_with(a.s,'a') AS f) x WHERE x.f"));
        assertEquals("2", rowsOf("SELECT count(*) FROM drt_t a"
                + " CROSS JOIN LATERAL (SELECT starts_with(a.s,'a') AS f) x WHERE x.f"));
        // A full join over a condition neither side can be matched on is refused by PostgreSQL
        // for a reason of its own, and says so rather than complaining about the type.
        assertEquals("FULL JOIN is only supported with merge-joinable or hash-joinable join"
                        + " conditions",
                messageOf("SELECT count(*) FROM drt_t a FULL JOIN" + derived));
    }

    /** The same through a WITH item, a view and a VALUES list. */
    @Test
    void aJoinOnABooleanFromEveryKindOfDerivedRelationRuns() throws SQLException {
        assertEquals("4", rowsOf("WITH q AS (SELECT id, starts_with(s,'a') AS f FROM drt_t)"
                + " SELECT count(*) FROM drt_t a JOIN q ON q.f"));
        assertEquals("2", rowsOf("SELECT count(*) FROM drt_t a JOIN drt_v x ON x.n"));
        assertEquals("2",
                rowsOf("SELECT count(*) FROM drt_t a JOIN (VALUES (1,true)) v(k,f) ON v.f"));
        assertEquals("2", rowsOf("SELECT count(*) FROM (SELECT b FROM drt_t) q"
                + " JOIN (SELECT flag FROM drt_u) r ON q.b"));
        assertEquals(ON_NOT_INT, messageOf("SELECT count(*) FROM (SELECT i FROM drt_t) q"
                + " JOIN (SELECT flag FROM drt_u) r ON q.i"));
    }

    /**
     * A name the derived relation supplies is that relation's column, whatever an enclosing
     * relation calls its own column of the same name — which is how an inner WHERE over a boolean
     * came to be refused for being the outer relation's integer.
     */
    @Test
    void aDerivedColumnOutranksAnOuterColumnOfTheSameName() throws SQLException {
        assertEquals("1,2", rowsOf("SELECT id FROM drt_t"
                + " WHERE EXISTS (SELECT 1 FROM (SELECT flag AS n FROM drt_u) q WHERE n)"
                + " ORDER BY id"));
        assertEquals("1,2", rowsOf("SELECT id FROM drt_t"
                + " WHERE EXISTS (SELECT 1 FROM drt_v WHERE n) ORDER BY id"));
        assertEquals("1,2", rowsOf("SELECT id FROM drt_t"
                + " WHERE EXISTS (SELECT 1 FROM (VALUES (true)) v(n) WHERE n) ORDER BY id"));
        assertEquals("2", rowsOf("SELECT count(*) FROM drt_t a"
                + " WHERE EXISTS (SELECT 1 FROM (SELECT b AS i FROM drt_t) q WHERE i)"));
        // ...and the outer name does not rescue an inner column that really is an integer
        assertEquals(NOT_INT, messageOf("SELECT count(*) FROM drt_u a"
                + " WHERE EXISTS (SELECT 1 FROM (SELECT id AS flag FROM drt_t) q WHERE flag)"));
    }

    /** A record-returning call answers to the names its own record holds. */
    @Test
    void aRecordReturningCallKeepsItsColumns() throws SQLException {
        assertEquals("a", rowsOf("SELECT key FROM jsonb_each('{\"a\":1}')"));
        assertEquals("1",
                rowsOf("SELECT count(*) FROM jsonb_each('{\"a\":1,\"b\":2}') e WHERE e.key = 'a'"));
        assertEquals("1", rowsOf(
                "SELECT count(*) FROM (SELECT * FROM jsonb_each('{\"a\":1}')) q WHERE key = 'a'"));
    }

    /** Everything else a derived column is ordinarily written into. */
    @Test
    void anOrdinaryUseOfADerivedColumnRuns() throws SQLException {
        assertEquals("1", rowsOf("SELECT count(*) FROM (SELECT * FROM drt_t) q WHERE b"));
        assertEquals("2", rowsOf("SELECT count(*) FROM (SELECT g FROM generate_series(1,2) g) q"
                + " WHERE g > 0"));
        assertEquals("1", rowsOf("SELECT count(*) FROM (SELECT n AS k FROM drt_t) q WHERE k > 0"));
        assertEquals("2", rowsOf("SELECT count(*) FROM (SELECT d AS k FROM drt_t) q"
                + " WHERE k > DATE '2019-01-01'"));
        assertEquals("1", rowsOf("SELECT count(*) FROM (SELECT count(*) AS k FROM drt_t) q"
                + " WHERE k > 0"));
        assertEquals("2", rowsOf("SELECT count(*) FROM"
                + " (SELECT id, row_number() OVER () AS k FROM drt_t) q WHERE k > 0"));
        assertEquals("1", rowsOf("SELECT count(*) FROM"
                + " (SELECT * FROM drt_t a JOIN drt_u u USING (id)) q WHERE flag"));
        assertEquals("2", rowsOf("SELECT count(*) FROM"
                + " (SELECT * FROM drt_t a CROSS JOIN drt_u u) q WHERE flag"));
        assertEquals("2",
                rowsOf("SELECT count(*) FROM (SELECT (SELECT true) AS k FROM drt_t) q WHERE k"));
        assertEquals("1",
                rowsOf("SELECT count(*) FROM (SELECT b::boolean AS k FROM drt_t) q WHERE k"));
        assertEquals("2", rowsOf("SELECT count(*) FROM drt_t"
                + " WHERE id IN (SELECT id FROM (SELECT id FROM drt_t) q)"));
    }

    /** A writing statement reads its derived relations the same way. */
    @Test
    void aWritingStatementReadsADerivedBooleanToo() throws SQLException {
        assertEquals("OK", messageOf("UPDATE drt_t SET i = i FROM (SELECT id, b AS k FROM drt_t) q"
                + " WHERE drt_t.id = q.id AND q.k"));
        assertEquals("OK", messageOf("DELETE FROM drt_u USING (SELECT id, false AS k FROM drt_t) q"
                + " WHERE drt_u.id = q.id AND q.k"));
        assertEquals("2", rowsOf("SELECT count(*) FROM drt_u"));
    }
}
