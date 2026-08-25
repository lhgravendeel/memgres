package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A jsonb key and a jsonb ordering are both the document, not the text that spells it.
 *
 * <p>jsonb is held as the text it prints as, and the operations that put a value in a set or in
 * an order compared those texts. So a primary key over jsonb admitted {@code {"a":1}} beside
 * {@code {"a":1.0}} and {@code {"b":2,"a":1}} beside {@code {"a":1,"b":2}}, all of which are one
 * value, and ORDER BY put documents in alphabetical order rather than in jsonb's own — which
 * weighs a value's kind first and a container's size before any member of it is looked at. A
 * document that is nothing but a scalar is held as an array of that one scalar, so the empty
 * array sorts below every scalar and an array of one sorts above them; inside a document there is
 * no such wrapper and a scalar is ordered by its kind alone.
 *
 * <p>json is the other way round. It has no equality at all, so there is no operator class to
 * build a b-tree over one and PostgreSQL refuses to key anything by a json column.
 */
class JsonDocumentKeyTest {

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

    private static String one(String sql) throws SQLException {
        List<String> rows = rows(sql);
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            List<String> out = new ArrayList<>();
            while (rs.next()) out.add(rs.getString(1));
            return out;
        }
    }

    private static void run(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
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

    // ------------------------------------------------------------------- keys

    /** Two spellings of one document are one key, whichever constraint holds the column. */
    @Test
    void aPrimaryKeyOverJsonbIsTheDocument() throws Exception {
        run("DROP TABLE IF EXISTS jdk_pk");
        run("CREATE TABLE jdk_pk (j jsonb PRIMARY KEY)");
        run("INSERT INTO jdk_pk VALUES ('{\"a\":1}')");
        assertEquals("23505", stateOf("INSERT INTO jdk_pk VALUES ('{\"a\":1.0}')"));
        run("INSERT INTO jdk_pk VALUES ('{\"b\":2,\"a\":1}')");
        assertEquals("23505", stateOf("INSERT INTO jdk_pk VALUES ('{\"a\":1,\"b\":2}')"));
        assertEquals("2", one("SELECT count(*) FROM jdk_pk"));
    }

    @Test
    void aUniqueConstraintOverJsonbIsTheDocument() throws Exception {
        run("DROP TABLE IF EXISTS jdk_u");
        run("CREATE TABLE jdk_u (id int, j jsonb UNIQUE)");
        run("INSERT INTO jdk_u VALUES (1, '[1,2]')");
        assertEquals("23505", stateOf("INSERT INTO jdk_u VALUES (2, '[1.0,2.00]')"));
        run("INSERT INTO jdk_u VALUES (3, '[1,2,3]')");
        assertEquals("2", one("SELECT count(*) FROM jdk_u"));
    }

    @Test
    void aUniqueIndexOverJsonbIsTheDocument() throws Exception {
        run("DROP TABLE IF EXISTS jdk_i");
        run("CREATE TABLE jdk_i (id int, j jsonb)");
        run("CREATE UNIQUE INDEX jdk_i_j ON jdk_i (j)");
        run("INSERT INTO jdk_i VALUES (1, '{\"x\": 1e3}')");
        assertEquals("23505", stateOf("INSERT INTO jdk_i VALUES (2, '{\"x\": 1000}')"));
        run("INSERT INTO jdk_i VALUES (3, '{\"x\": 1e3}') ON CONFLICT (j) DO NOTHING");
        assertEquals("1", one("SELECT count(*) FROM jdk_i"));
    }

    /** There is no b-tree over json, so nothing can be keyed by one however it is written. */
    @Test
    void jsonHasNoOperatorClassToKeyBy() throws Exception {
        run("DROP TABLE IF EXISTS jdk_j");
        assertEquals("42704", stateOf("CREATE TABLE jdk_j (j json PRIMARY KEY)"));
        assertEquals("42704", stateOf("CREATE TABLE jdk_j (j json UNIQUE)"));
        assertEquals("42704", stateOf("CREATE TABLE jdk_j (id int, j json, PRIMARY KEY (j))"));
        assertEquals("42704", stateOf("CREATE TABLE jdk_j (id int, j json, UNIQUE (j))"));
        run("CREATE TABLE jdk_j (id int, j json)");
        assertEquals("42704", stateOf("ALTER TABLE jdk_j ADD PRIMARY KEY (j)"));
        assertEquals("42704", stateOf("ALTER TABLE jdk_j ADD UNIQUE (j)"));
        assertEquals("42704", stateOf("CREATE INDEX jdk_j_j ON jdk_j (j)"));
        assertTrue(messageOf("CREATE INDEX jdk_j_j ON jdk_j (j)").contains(
                "data type json has no default operator class for access method \"btree\""));
    }

    /** A json column that is not part of a key is left alone. */
    @Test
    void aJsonColumnThatIsNotAKeyIsUntouched() throws Exception {
        run("DROP TABLE IF EXISTS jdk_plain");
        run("CREATE TABLE jdk_plain (id int PRIMARY KEY, j json)");
        run("INSERT INTO jdk_plain VALUES (1, '{\"b\":2,\"a\":1}')");
        assertEquals("{\"b\":2,\"a\":1}", one("SELECT j FROM jdk_plain"));
    }

    // --------------------------------------------------------------- ordering

    /** A value's kind is weighed first, in jsonb's own order and not the alphabet's. */
    @Test
    void oneKindOfValueSortsBeforeAnother() throws Exception {
        assertEquals("t", one("SELECT 'null'::jsonb < '\"a\"'::jsonb"));
        assertEquals("t", one("SELECT '\"a\"'::jsonb < '1'::jsonb"));
        assertEquals("t", one("SELECT '1'::jsonb < 'false'::jsonb"));
        assertEquals("t", one("SELECT 'false'::jsonb < 'true'::jsonb"));
        assertEquals("t", one("SELECT 'true'::jsonb < '[1]'::jsonb"));
        assertEquals("t", one("SELECT '[1,2]'::jsonb < '{}'::jsonb"));
    }

    /** A container is ordered by how many members it holds before either is looked into. */
    @Test
    void aContainerIsOrderedBySizeFirst() throws Exception {
        assertEquals("t", one("SELECT '[3]'::jsonb < '[1,2]'::jsonb"));
        assertEquals("t", one("SELECT '{\"z\":1}'::jsonb < '{\"a\":1,\"b\":2}'::jsonb"));
        assertEquals("t", one("SELECT '{\"a\":1}'::jsonb < '{\"a\":2}'::jsonb"));
        assertEquals("t", one("SELECT '{\"a\":2}'::jsonb < '{\"b\":1}'::jsonb"));
    }

    /**
     * A scalar document is an array of that one scalar, so the empty array has fewer elements
     * than any of them and an array of one has as many.
     */
    @Test
    void theEmptyArraySortsBelowEveryScalarDocument() throws Exception {
        assertEquals("t", one("SELECT '[]'::jsonb < 'null'::jsonb"));
        assertEquals("t", one("SELECT '[]'::jsonb < '\"a\"'::jsonb"));
        assertEquals("t", one("SELECT '[]'::jsonb < 'false'::jsonb"));
        assertEquals("f", one("SELECT '\"a\"'::jsonb < '[]'::jsonb"));
        assertEquals("t", one("SELECT '\"a\"'::jsonb < '[1]'::jsonb"));
        assertEquals("t", one("SELECT '[]'::jsonb < '[1]'::jsonb"));
        assertEquals("t", one("SELECT '[]'::jsonb < '{}'::jsonb"));
    }

    /** Inside a document there is no wrapper, and a scalar is ordered by its kind. */
    @Test
    void insideADocumentAScalarIsOrderedByItsKind() throws Exception {
        assertEquals("t", one("SELECT '[\"a\"]'::jsonb < '[[]]'::jsonb"));
        assertEquals("f", one("SELECT '{\"a\":[]}'::jsonb < '{\"a\":null}'::jsonb"));
        assertEquals("[null] [\"a\"] [1] [true] [[]] [[1]] [[1, 2]] [{}]",
                one("SELECT string_agg(v::text, ' ' ORDER BY v) FROM (VALUES ('[\"a\"]'::jsonb),"
                        + "('[[1,2]]'),('[[1]]'),('[[]]'),('[null]'),('[1]'),('[true]'),('[{}]')) t(v)"));
    }

    /** The order is the same wherever it is asked for. */
    @Test
    void theSameOrderServesOrderByAndTheAggregates() throws Exception {
        assertEquals(List.of("[]", "1", "{}"),
                rows("SELECT (v)::text AS a FROM (VALUES ('{}'::jsonb),('[]'::jsonb),('1'::jsonb))"
                        + " t(v) ORDER BY v"));
        assertEquals("[] null \"s\" 1 true {}",
                one("SELECT string_agg(v::text, ' ' ORDER BY v) FROM (VALUES ('{}'::jsonb),"
                        + "('[]'::jsonb),('1'::jsonb),('null'::jsonb),('true'::jsonb),('\"s\"'::jsonb)) t(v)"));
        // there is no max or min over jsonb: the type has an ordering but no aggregate over it
        assertEquals("42883",
                stateOf("SELECT max(v) FROM (VALUES ('{}'::jsonb),('[]'::jsonb),('1'::jsonb)) t(v)"));
        assertEquals("42883",
                stateOf("SELECT min(v) FROM (VALUES ('{}'::jsonb),('[]'::jsonb),('1'::jsonb)) t(v)"));
    }

    /** A row of a jsonb column holds the document, however that row happened to be written. */
    @Test
    void aRowOfAJsonbColumnHoldsTheDocument() throws Exception {
        assertEquals("[1, 2] {\"a\": 2, \"b\": 1}",
                one("SELECT string_agg(v::text, ' ') FROM (VALUES ('[1,2]'::jsonb),"
                        + "('{\"b\":1,\"a\":2}')) t(v)"));
        assertEquals("[1, 2] {\"a\": 2, \"b\": 1}",
                one("SELECT string_agg(v::text, ' ') FROM (SELECT '[1,2]'::jsonb AS v"
                        + " UNION ALL SELECT '{\"b\":1,\"a\":2}') t"));
        assertEquals("1", one("SELECT count(DISTINCT v) FROM (VALUES ('1.0'::jsonb),('1')) t(v)"));
    }
}
