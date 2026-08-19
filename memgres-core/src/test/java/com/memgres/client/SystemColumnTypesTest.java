package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A tuple identifier and a transaction stamp are types of their own.
 *
 * <p>The six columns a relation carries without declaring them have types like any other column:
 * the tuple identifier is a tid, the two transaction stamps are xids, the two command stamps are
 * cids, and the relation stamp is an oid. Nothing chose those types, which is why they are not
 * negotiable — a value of one prints as text but is not text, and every rule that decides
 * something from a type decides it from these. memgres had no type for a system column at all, so
 * each one was read as whatever its value happened to look like: {@code ctid} was the string it
 * prints as, and so it cast to a number, compared with one, sorted as text, and was handed to
 * functions PostgreSQL declares over no such thing.
 *
 * <p>A tid is a block and a slot within it, ordered by the block and then by the slot. An xid and a
 * cid have equality and nothing else: PostgreSQL registers no ordering over either, so nothing is
 * sorted, partitioned or ranked by one.
 *
 * <p>Every value here was measured against PostgreSQL 18.
 */
class SystemColumnTypesTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE tidt_a (id int, v text)");
            st.execute("INSERT INTO tidt_a VALUES (1, 'one'), (2, 'two'), (3, 'three')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 1) b.append('|');
                    String v = rs.getString(i);
                    b.append(rs.wasNull() ? "NULL" : v);
                }
                out.add(b.toString());
            }
            return out;
        }
    }

    private static String scalar(String sql) throws SQLException {
        List<String> out = rows(sql);
        assertEquals(1, out.size(), "expected one row from: " + sql);
        return out.get(0);
    }

    /** The SQLSTATE a statement raises, or "OK" when it does not raise at all. */
    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static org.postgresql.util.ServerErrorMessage fieldsOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }, "expected an error from: " + sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
    }

    /** A tid is a block and a slot within it, and the slot is a number. */
    @Test
    void aTupleIdentifierIsABlockAndASlotWithinIt() throws Exception {
        assertEquals("(0,1)", scalar("SELECT '(0,1)'::tid::text"));
        assertEquals("tid", scalar("SELECT pg_typeof('(0,1)'::tid)::text"));
        assertEquals("(4294967295,65535)", scalar("SELECT '(4294967295,65535)'::tid::text"));
        assertEquals("t", scalar("SELECT '(0,1)'::tid = '(0,1)'::tid"));
        assertEquals("t", scalar("SELECT '(0,1)'::tid <> '(0,2)'::tid"));
        // The slot is a number, not the digits it was written with: as text (0,10) sorts first.
        assertEquals("t", scalar("SELECT '(0,10)'::tid > '(0,9)'::tid"));
        assertEquals("t", scalar("SELECT '(0,9)'::tid < '(0,10)'::tid"));
        assertEquals("t", scalar("SELECT '(1,1)'::tid > '(0,9)'::tid"));
        assertEquals("t", scalar("SELECT '(0,1)'::tid <= '(0,1)'::tid"));
        assertEquals("t", scalar("SELECT '(0,1)'::tid >= '(0,1)'::tid"));
    }

    /** What is not a block and a slot is not a tuple identifier. */
    @Test
    void whatIsNotABlockAndASlotIsNotATupleIdentifier() {
        assertEquals("22P02", stateOf("SELECT 'nope'::tid"));
        assertEquals("22P02", stateOf("SELECT ''::tid"));
        assertEquals("22P02", stateOf("SELECT '(0,1'::tid"));
        assertEquals("22P02", stateOf("SELECT '0,1'::tid"));
        assertEquals("22P02", stateOf("SELECT '()'::tid"));
        assertEquals("22P02", stateOf("SELECT '(0)'::tid"));
        assertEquals("22P02", stateOf("SELECT '(0,70000)'::tid"));
        assertEquals("22P02", stateOf("SELECT '(0,-1)'::tid"));
        assertEquals("22P02", stateOf("SELECT '(4294967296,1)'::tid"));
        assertEquals("22P02", stateOf("SELECT '( 0 , 1 )'::tid"));
        assertEquals("22P02", stateOf("SELECT '(-2147483649,1)'::tid"));
    }

    /**
     * The block is a signed number read as the unsigned one it is, and what follows the closing
     * paren is not read at all.
     */
    @Test
    void theBlockIsSignedOnTheWayInAndUnsignedOnceItIsRead() throws Exception {
        assertEquals("(4294967295,1)", scalar("SELECT '(-1,1)'::tid::text"));
        assertEquals("(2147483648,65535)", scalar("SELECT '(-2147483648,65535)'::tid::text"));
        assertEquals("(0,1)", scalar("SELECT '(0,1)x'::tid::text"));
        assertEquals("(0,1)", scalar("SELECT ' (0,1) '::tid::text"));
        assertEquals("(1,1)", scalar("SELECT '(+1,001)'::tid::text"));
    }

    /** A command counter counts the commands of one transaction. */
    @Test
    void aCommandCounterCountsTheCommandsOfOneTransaction() throws Exception {
        assertEquals("0", scalar("SELECT '0'::cid::text"));
        assertEquals("100", scalar("SELECT '100'::cid::text"));
        assertEquals("4294967295", scalar("SELECT '4294967295'::cid::text"));
        assertEquals("cid", scalar("SELECT pg_typeof('100'::cid)::text"));
        assertEquals("t", scalar("SELECT '100'::cid = '100'::cid"));
        assertEquals("22P02", stateOf("SELECT 'abc'::cid"));
        assertEquals("22P02", stateOf("SELECT ''::cid"));
        assertEquals("22P02", stateOf("SELECT '1.5'::cid"));
    }

    /** A stamp has equality and nothing else: PostgreSQL registers no ordering over one. */
    @Test
    void aStampHasEqualityAndNothingElse() throws Exception {
        assertEquals("t", scalar("SELECT '5'::xid = '5'::xid"));
        assertEquals("t", scalar("SELECT '5'::xid <> '6'::xid"));
        assertEquals("xid", scalar("SELECT pg_typeof('5'::xid)::text"));
        assertEquals("42883", stateOf("SELECT '5'::xid > '4'::xid"));
        assertEquals("42883", stateOf("SELECT '5'::xid < '6'::xid"));
        assertEquals("42883", stateOf("SELECT '5'::xid >= '4'::xid"));
        assertEquals("42883", stateOf("SELECT '5'::cid > '4'::cid"));
        assertEquals("42883", stateOf("SELECT '5'::cid < '6'::cid"));
    }

    /** The columns nobody declared carry those types. */
    @Test
    void theColumnsNobodyDeclaredCarryThoseTypes() throws Exception {
        assertEquals(List.of("tid", "tid", "tid"),
                rows("SELECT pg_typeof(ctid)::text FROM tidt_a ORDER BY id"));
        assertEquals(List.of("xid", "xid", "xid"),
                rows("SELECT pg_typeof(xmin)::text FROM tidt_a ORDER BY id"));
        assertEquals(List.of("xid", "xid", "xid"),
                rows("SELECT pg_typeof(xmax)::text FROM tidt_a ORDER BY id"));
        assertEquals(List.of("cid", "cid", "cid"),
                rows("SELECT pg_typeof(cmin)::text FROM tidt_a ORDER BY id"));
        assertEquals(List.of("cid", "cid", "cid"),
                rows("SELECT pg_typeof(cmax)::text FROM tidt_a ORDER BY id"));
        assertEquals(List.of("oid", "oid", "oid"),
                rows("SELECT pg_typeof(tableoid)::text FROM tidt_a ORDER BY id"));
    }

    /** An operator the type does not have is an operator that does not exist. */
    @Test
    void anOperatorTheTypeDoesNotHaveDoesNotExist() {
        assertEquals("42883", stateOf("SELECT xmin > 0 FROM tidt_a"));
        assertEquals("42883", stateOf("SELECT xmin < 100 FROM tidt_a"));
        assertEquals("42883", stateOf("SELECT xmax >= 0 FROM tidt_a"));
        assertEquals("42883", stateOf("SELECT cmin > 0 FROM tidt_a"));
        assertEquals("42883", stateOf("SELECT cmax < 1 FROM tidt_a"));
        assertEquals("42883", stateOf("SELECT ctid > 0 FROM tidt_a"));
        assertEquals("42883", stateOf("SELECT tableoid + 1 FROM tidt_a"));
        assertEquals("operator does not exist: xid > integer",
                fieldsOf("SELECT xmin > 0 FROM tidt_a").getMessage());
        assertEquals("operator does not exist: oid + integer",
                fieldsOf("SELECT tableoid + 1 FROM tidt_a").getMessage());
    }

    /** The operators the types do have work. */
    @Test
    void theOperatorsTheTypesDoHaveWork() throws Exception {
        assertEquals(List.of("f", "f", "f"), rows("SELECT xmin = 0 FROM tidt_a ORDER BY id"));
        assertEquals(List.of("t", "f", "f"),
                rows("SELECT ctid = '(0,1)'::tid FROM tidt_a ORDER BY id"));
        assertEquals(List.of("t", "t", "t"), rows("SELECT tableoid > 0 FROM tidt_a ORDER BY id"));
        assertEquals(List.of("(0,1)", "(0,2)", "(0,3)"),
                rows("SELECT ctid::text FROM tidt_a ORDER BY id"));
    }

    /** No cast turns one of them into a number. */
    @Test
    void noCastTurnsOneOfThemIntoANumber() {
        assertEquals("42846", stateOf("SELECT ctid::bigint FROM tidt_a"));
        assertEquals("42846", stateOf("SELECT ctid::int FROM tidt_a"));
        assertEquals("42846", stateOf("SELECT ctid::numeric FROM tidt_a"));
        assertEquals("42846", stateOf("SELECT xmin::bigint FROM tidt_a"));
        assertEquals("42846", stateOf("SELECT xmin::int FROM tidt_a"));
        assertEquals("42846", stateOf("SELECT cmin::int FROM tidt_a"));
        assertEquals("42846", stateOf("SELECT cmax::numeric FROM tidt_a"));
        assertEquals("cannot cast type tid to bigint",
                fieldsOf("SELECT ctid::bigint FROM tidt_a").getMessage());
    }

    /** Nothing is sorted, ranked or partitioned by a stamp. */
    @Test
    void nothingIsSortedByAStamp() {
        assertEquals("42883", stateOf("SELECT id FROM tidt_a ORDER BY xmin"));
        assertEquals("42883", stateOf("SELECT id FROM tidt_a ORDER BY xmax"));
        assertEquals("42883", stateOf("SELECT id FROM tidt_a ORDER BY cmin"));
        assertEquals("42883", stateOf("SELECT id FROM tidt_a ORDER BY cmax DESC"));
        assertEquals("42883", stateOf("SELECT row_number() OVER (ORDER BY xmin) FROM tidt_a"));
        assertEquals("42883", stateOf("SELECT array_agg(id ORDER BY xmin) FROM tidt_a"));
        org.postgresql.util.ServerErrorMessage sort =
                fieldsOf("SELECT id FROM tidt_a ORDER BY xmin");
        assertEquals("could not identify an ordering operator for type xid", sort.getMessage());
        assertEquals("Use an explicit ordering operator or modify the query.", sort.getHint());
        // A window is built by sorting the rows on its partition keys, and PostgreSQL reports a
        // key it cannot sort by as a plan it cannot implement rather than an operator it lacks.
        assertEquals("0A000", stateOf("SELECT row_number() OVER (PARTITION BY cmin) FROM tidt_a"));
        org.postgresql.util.ServerErrorMessage window =
                fieldsOf("SELECT row_number() OVER (PARTITION BY cmin) FROM tidt_a");
        assertEquals("could not implement window PARTITION BY", window.getMessage());
        assertEquals("Window partitioning columns must be of sortable datatypes.",
                window.getDetail());
    }

    /** A tid and an oid are ordered, so they sort. */
    @Test
    void aTupleIdentifierAndARelationStampAreOrderedSoTheySort() throws Exception {
        assertEquals(List.of("1", "2", "3"), rows("SELECT id FROM tidt_a ORDER BY ctid"));
        assertEquals(List.of("1", "2", "3"), rows("SELECT id FROM tidt_a ORDER BY tableoid, id"));
        assertEquals(List.of("{1,2,3}"), rows("SELECT array_agg(id ORDER BY ctid) FROM tidt_a"));
        assertEquals(List.of("3", "2", "1"), rows("SELECT id FROM tidt_a ORDER BY ctid DESC"));
    }

    /** No function is declared over them. */
    @Test
    void noFunctionIsDeclaredOverThem() {
        assertEquals("42883", stateOf("SELECT length(ctid) FROM tidt_a"));
        assertEquals("42883", stateOf("SELECT length(xmin) FROM tidt_a"));
        assertEquals("42883", stateOf("SELECT upper(xmin) FROM tidt_a"));
        assertEquals("42883", stateOf("SELECT abs(cmin) FROM tidt_a"));
        assertEquals("42883", stateOf("SELECT max(xmin) FROM tidt_a"));
        assertEquals("42883", stateOf("SELECT min(cmin) FROM tidt_a"));
        assertEquals("42883", stateOf("SELECT sum(cmin) FROM tidt_a"));
        assertEquals("function length(tid) does not exist",
                fieldsOf("SELECT length(ctid) FROM tidt_a").getMessage());
        assertEquals("function max(xid) does not exist",
                fieldsOf("SELECT max(xmin) FROM tidt_a").getMessage());
    }

    /** The ones declared over anything at all, and the ones declared over tid and oid, exist. */
    @Test
    void theFunctionsThatAreDeclaredOverThemStillExist() throws Exception {
        assertEquals("3", scalar("SELECT count(ctid) FROM tidt_a"));
        assertEquals("3", scalar("SELECT count(xmin) FROM tidt_a"));
        assertEquals("(0,3)", scalar("SELECT max(ctid)::text FROM tidt_a"));
        assertEquals("(0,1)", scalar("SELECT min(ctid)::text FROM tidt_a"));
        assertEquals("tid", scalar("SELECT pg_typeof(max(ctid))::text FROM tidt_a"));
    }

    /** Equality is enough to group and to make distinct, which is all a stamp needs. */
    @Test
    void equalityIsEnoughToGroupAndToMakeDistinct() throws Exception {
        assertEquals("1", scalar("SELECT count(*) FROM (SELECT DISTINCT xmin FROM tidt_a) s"));
        assertEquals("3", scalar("SELECT count(*) FROM (SELECT DISTINCT ctid FROM tidt_a) s"));
        assertEquals("1", scalar("SELECT count(*) FROM (SELECT xmin FROM tidt_a GROUP BY xmin) s"));
        assertEquals("1", scalar("SELECT count(*) FROM (SELECT cmin FROM tidt_a GROUP BY cmin) s"));
    }

    /** The type a client is told a system column has is the type it is. */
    @Test
    void theClientIsToldTheTypeTheColumnHas() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT ctid, xmin, xmax, cmin, cmax, tableoid FROM tidt_a")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("tid", md.getColumnTypeName(1));
            assertEquals("xid", md.getColumnTypeName(2));
            assertEquals("xid", md.getColumnTypeName(3));
            assertEquals("cid", md.getColumnTypeName(4));
            assertEquals("cid", md.getColumnTypeName(5));
            assertEquals("oid", md.getColumnTypeName(6));
        }
    }
}
