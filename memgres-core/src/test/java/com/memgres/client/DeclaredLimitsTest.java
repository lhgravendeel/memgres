package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Some of PostgreSQL's limits are structural rather than semantic: an index tuple holds 32
 * attributes, a pg_proc entry holds 100 argument types, a varchar type modifier holds a length no
 * greater than 10485760 and a numeric one a precision no greater than 1000. PostgreSQL checks each
 * where the declaration is written or the call is parsed, so an index, a key, a domain or a column
 * it could never store is never created.
 *
 * <p>Accepting them instead records a definition PostgreSQL would have refused. The disagreement
 * does not stay put: it surfaces later against a statement that reads the definition back, which
 * looks like the culprit and is not.
 *
 * <p>Half of what is asserted here is the other direction — the shapes just short of each limit,
 * and the ordinary calls and indexes that pass through the same code, all of which must keep
 * working. Every expectation was measured against PostgreSQL 18.
 */
class DeclaredLimitsTest {

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

    // ---- helpers -------------------------------------------------------

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    /** The type name the wire protocol advertises for the query's first column. */
    private static String typeOf(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.getMetaData().getColumnTypeName(1);
        }
    }

    /** All rows of a query as {@code a|b} strings, in order. */
    private static String rows(String sql) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                if (sb.length() > 0) sb.append(';');
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
            }
        }
        return sb.toString();
    }

    private static void assertRejected(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    /** {@code c1, c2, ... cn}. */
    private static String cols(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= n; i++) {
            if (i > 1) sb.append(", ");
            sb.append("c").append(i);
        }
        return sb.toString();
    }

    /** {@code c1 int, c2 int, ... cn int}. */
    private static String colDefs(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= n; i++) {
            if (i > 1) sb.append(", ");
            sb.append("c").append(i).append(" int");
        }
        return sb.toString();
    }

    /** {@code 1,2,...,n}. */
    private static String nums(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= n; i++) {
            if (i > 1) sb.append(",");
            sb.append(i);
        }
        return sb.toString();
    }

    private static final String TOO_WIDE = "cannot use more than 32 columns in an index";
    private static final String TOO_MANY_ARGS = "cannot pass more than 100 arguments to a function";

    // ---- an index names at most 32 columns ------------------------------

    @Test
    void anIndexOverMoreThan32ColumnsIsRefused() throws Exception {
        exec("CREATE TABLE dl_wide (" + colDefs(33) + ")");
        assertRejected("54011", TOO_WIDE,
                "CREATE INDEX dl_wide_i33 ON dl_wide (" + cols(33) + ")");
        assertRejected("54011", TOO_WIDE,
                "CREATE UNIQUE INDEX dl_wide_u33 ON dl_wide (" + cols(33) + ")");
    }

    @Test
    void includedColumnsCountTowardsTheSame32() throws Exception {
        exec("CREATE TABLE dl_inc (" + colDefs(33) + ")");
        assertRejected("54011", TOO_WIDE,
                "CREATE INDEX dl_inc_i33 ON dl_inc (" + cols(30) + ") INCLUDE (c31, c32, c33)");
        // 30 key columns plus 2 included is exactly 32
        exec("CREATE INDEX dl_inc_i32 ON dl_inc (" + cols(30) + ") INCLUDE (c31, c32)");
    }

    @Test
    void exactly32ColumnsIsAccepted() throws Exception {
        exec("CREATE TABLE dl_w32 (" + colDefs(33) + ")");
        exec("CREATE INDEX dl_w32_i ON dl_w32 (" + cols(32) + ")");
        exec("CREATE UNIQUE INDEX dl_w32_u ON dl_w32 (" + cols(32) + ")");
        assertEquals("2", scalar("SELECT count(*) FROM pg_indexes WHERE tablename = 'dl_w32'"));
    }

    @Test
    void theRelationIsResolvedBeforeTheColumnsAreCounted() {
        assertRejected("42P01", "relation \"dl_absent\" does not exist",
                "CREATE INDEX dl_absent_i ON dl_absent (" + cols(33) + ")");
    }

    @Test
    void theColumnCountIsSettledBeforeTheAccessMethodIsLookedUp() throws Exception {
        exec("CREATE TABLE dl_am (" + colDefs(33) + ")");
        assertRejected("54011", TOO_WIDE,
                "CREATE INDEX dl_am_i ON dl_am USING dl_nosuchmethod (" + cols(33) + ")");
    }

    @Test
    void anOrdinaryIndexIsUnaffected() throws Exception {
        exec("CREATE TABLE dl_small (a int, b text)");
        exec("CREATE INDEX dl_small_ab ON dl_small (a, b)");
        exec("CREATE UNIQUE INDEX dl_small_a ON dl_small (a)");
        exec("INSERT INTO dl_small VALUES (1, 'x'), (2, 'y')");
        assertEquals("x", scalar("SELECT b FROM dl_small WHERE a = 1"));
        assertEquals("1|x;2|y", rows("SELECT a, b FROM dl_small ORDER BY a"));
    }

    // ---- a key is stored as one of those indexes ------------------------

    @Test
    void aKeyOverMoreThan32ColumnsIsRefused() throws Exception {
        exec("CREATE TABLE dl_key33 (" + colDefs(33) + ")");
        assertRejected("54011", TOO_WIDE,
                "ALTER TABLE dl_key33 ADD PRIMARY KEY (" + cols(33) + ")");
        assertRejected("54011", TOO_WIDE,
                "ALTER TABLE dl_key33 ADD UNIQUE (" + cols(33) + ")");
        assertRejected("54011", TOO_WIDE,
                "ALTER TABLE dl_key33 ADD CONSTRAINT dl_key33_pk PRIMARY KEY (" + cols(33) + ")");
        assertRejected("54011", TOO_WIDE,
                "CREATE TABLE dl_ct33 (" + colDefs(33) + ", PRIMARY KEY (" + cols(33) + "))");
        assertRejected("54011", TOO_WIDE,
                "CREATE TABLE dl_ctu33 (" + colDefs(33) + ", UNIQUE (" + cols(33) + "))");
    }

    @Test
    void a32ColumnKeyIsAccepted() throws Exception {
        exec("CREATE TABLE dl_key32 (" + colDefs(33) + ", PRIMARY KEY (" + cols(32) + "))");
        exec("ALTER TABLE dl_key32 ADD UNIQUE (" + cols(32) + ")");
    }

    @Test
    void anOrdinaryKeyStillEnforcesItself() throws Exception {
        exec("CREATE TABLE dl_pk (a int, b int, PRIMARY KEY (a, b))");
        exec("INSERT INTO dl_pk VALUES (1, 1)");
        assertRejected("23505", "duplicate key value violates unique constraint",
                "INSERT INTO dl_pk VALUES (1, 1)");
        exec("INSERT INTO dl_pk VALUES (1, 2)");
        assertEquals("2", scalar("SELECT count(*) FROM dl_pk"));
    }

    // ---- a function call passes at most 100 arguments --------------------

    @Test
    void moreThan100ArgumentsIsRefused() {
        assertRejected("54023", TOO_MANY_ARGS, "SELECT concat(" + nums(101) + ")");
        assertRejected("54023", TOO_MANY_ARGS, "SELECT concat_ws(',', " + nums(101) + ")");
        assertRejected("54023", TOO_MANY_ARGS, "SELECT num_nonnulls(" + nums(101) + ")");
        assertRejected("54023", TOO_MANY_ARGS, "SELECT num_nulls(" + nums(101) + ")");
        assertRejected("54023", TOO_MANY_ARGS, "SELECT format('%s', " + nums(101) + ")");
    }

    @Test
    void exactly100ArgumentsIsAccepted() throws Exception {
        assertEquals("192", scalar("SELECT length(concat(" + nums(100) + "))"));
        // the separator of concat_ws is an argument like any other, so 99 values is the limit
        assertEquals("287", scalar("SELECT length(concat_ws(',', " + nums(99) + "))"));
        assertEquals("100", scalar("SELECT num_nonnulls(" + nums(100) + ")"));
        assertEquals("0", scalar("SELECT num_nulls(" + nums(100) + ")"));
    }

    @Test
    void theGrammarProductionsAreNotFunctionCalls() throws Exception {
        // COALESCE, GREATEST, LEAST and NULLIF are productions of PostgreSQL's grammar rather
        // than FuncCall nodes; they never reach the argument-count check and PG accepts 101.
        assertEquals("101", scalar("SELECT greatest(" + nums(101) + ")::text"));
        assertEquals("1", scalar("SELECT least(" + nums(101) + ")::text"));
        assertEquals("1", scalar("SELECT coalesce(" + nums(101) + ")::text"));
    }

    @Test
    void ordinaryCallsAreUntouched() throws Exception {
        exec("CREATE TABLE dl_args (a text, b int)");
        exec("INSERT INTO dl_args VALUES ('x', 1), ('y', 2), (NULL, 3)");
        assertEquals("3;x1;y2", rows("SELECT concat(a, b) FROM dl_args ORDER BY 1"));
        assertEquals("x-;y-", rows("SELECT concat(a, '-') FROM dl_args"
                + " WHERE concat(a, b) <> 'zz' AND a IS NOT NULL"
                + " GROUP BY concat(a, '-') ORDER BY concat(a, '-')"));
        assertEquals("2", scalar("SELECT num_nonnulls(a, b) FROM dl_args WHERE b = 1"));
        assertEquals("1", scalar("SELECT num_nulls(a, b) FROM dl_args WHERE b = 3"));
        // NULL operands and a nested call
        assertEquals("", scalar("SELECT concat(NULL, NULL)"));
        assertEquals("192", scalar("SELECT length(concat(" + nums(100) + "))"));
    }

    @Test
    void callsThroughAViewAndASubqueryStillResolve() throws Exception {
        exec("CREATE TABLE dl_vw (a text, b int)");
        exec("INSERT INTO dl_vw VALUES ('x', 1), ('y', 2)");
        exec("CREATE VIEW dl_vw_v AS SELECT concat_ws('/', a, b) AS c FROM dl_vw");
        assertEquals("x/1;y/2",
                rows("SELECT sub.c FROM (SELECT c FROM dl_vw_v) sub ORDER BY sub.c"));
        exec("CREATE VIEW dl_vw_v2 AS SELECT concat(" + nums(100) + ") AS c");
        assertEquals("192", scalar("SELECT length(c) FROM dl_vw_v2"));
    }

    @Test
    void aDerivedColumnComparedToALiteralStillWorks() throws Exception {
        exec("CREATE TABLE dl_rn (a int)");
        exec("INSERT INTO dl_rn VALUES (1), (2), (3)");
        assertEquals("1;2;3", rows("SELECT sub.rn FROM"
                + " (SELECT row_number() OVER (ORDER BY a) AS rn FROM dl_rn) sub"
                + " WHERE sub.rn >= 1 ORDER BY sub.rn"));
    }

    // ---- a type modifier out of range ------------------------------------

    @Test
    void aDomainOverAnOutOfRangeModifierIsRefused() {
        assertRejected("22023", "length for type varchar cannot exceed 10485760",
                "CREATE DOMAIN dl_dom AS varchar(10485761)");
        assertRejected("22023", "length for type char must be at least 1",
                "CREATE DOMAIN dl_dom AS char(0)");
        assertRejected("22023", "NUMERIC precision 1001 must be between 1 and 1000",
                "CREATE DOMAIN dl_dom AS numeric(1001,2)");
    }

    @Test
    void theLargestAcceptedModifiersStillMakeADomain() throws Exception {
        exec("CREATE DOMAIN dl_dom_ok AS varchar(10485760)");
        exec("CREATE DOMAIN dl_dom_num AS numeric(1000,2)");
        exec("CREATE TABLE dl_dom_t (a dl_dom_ok, b dl_dom_num)");
        exec("INSERT INTO dl_dom_t VALUES ('hi', 1.5)");
        // The scale a domain carries is a separate question this branch does not settle:
        // PostgreSQL renders b as 1.50 and memgres as 1.5. Only the acceptance is asserted here.
        assertEquals("hi", scalar("SELECT a FROM dl_dom_t"));
        assertEquals("1", scalar("SELECT count(*) FROM dl_dom_t WHERE b = 1.5"));
        // and the largest widths are usable on a plain column too
        exec("CREATE TABLE dl_dom_p (a varchar(10485760), b numeric(1000,2))");
        exec("INSERT INTO dl_dom_p VALUES ('hi', 1.5)");
        assertEquals("hi|1.50", rows("SELECT a, b FROM dl_dom_p"));
    }

    @Test
    void theNameCollisionIsReportedBeforeTheBaseTypeModifier() throws Exception {
        exec("CREATE DOMAIN dl_dom_dup AS int");
        assertRejected("42710", "type \"dl_dom_dup\" already exists",
                "CREATE DOMAIN dl_dom_dup AS varchar(10485761)");
    }

    @Test
    void aRetypeToAnOutOfRangeModifierIsRefused() throws Exception {
        exec("CREATE TABLE dl_retype (a text)");
        assertRejected("22023", "length for type varchar cannot exceed 10485760",
                "ALTER TABLE dl_retype ALTER COLUMN a TYPE varchar(10485761)");
        assertRejected("22023", "length for type char must be at least 1",
                "ALTER TABLE dl_retype ALTER COLUMN a TYPE char(0)");
        assertRejected("22023", "NUMERIC precision 1001 must be between 1 and 1000",
                "ALTER TABLE dl_retype ALTER COLUMN a TYPE numeric(1001,2)");
    }

    @Test
    void theColumnIsLookedUpBeforeTheTargetTypeModifier() throws Exception {
        exec("CREATE TABLE dl_retype2 (a text)");
        assertRejected("42703", "column \"nosuchcol\" of relation \"dl_retype2\" does not exist",
                "ALTER TABLE dl_retype2 ALTER COLUMN nosuchcol TYPE varchar(10485761)");
    }

    @Test
    void addColumnChecksTheSameWidths() throws Exception {
        exec("CREATE TABLE dl_addcol (a text)");
        assertRejected("22023", "length for type varchar cannot exceed 10485760",
                "ALTER TABLE dl_addcol ADD COLUMN b varchar(10485761)");
        assertRejected("22023", "NUMERIC precision 1001 must be between 1 and 1000",
                "ALTER TABLE dl_addcol ADD COLUMN b numeric(1001,2)");
        assertRejected("22023", "length for type char must be at least 1",
                "ALTER TABLE dl_addcol ADD COLUMN b char(0)");
        exec("CREATE TABLE dl_widecol (a varchar(10485760), b numeric(1000,2))");
        exec("ALTER TABLE dl_widecol ALTER COLUMN a TYPE varchar(10485760)");
    }

    @Test
    void aRefusedAlterLeavesTheTableAlterable() throws Exception {
        // A rejected definition must not hold on to the table: the next ALTER on the same name
        // has to run, on this connection and on a fresh one.
        exec("CREATE TABLE dl_lock (a text)");
        exec("ALTER TABLE dl_lock ADD COLUMN ok1 int");
        assertRejected("22023", "length for type char must be at least 1",
                "ALTER TABLE dl_lock ADD COLUMN bad char(0)");
        exec("ALTER TABLE dl_lock ADD COLUMN ok2 int");
        assertRejected("42701", "column \"ok2\" of relation \"dl_lock\" already exists",
                "ALTER TABLE dl_lock ADD COLUMN ok2 int");
        exec("ALTER TABLE dl_lock ADD COLUMN ok3 int");
        exec("ALTER TABLE dl_lock RENAME TO dl_lock2");
        try (Connection other = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
             Statement st = other.createStatement()) {
            st.execute("ALTER TABLE dl_lock2 ADD COLUMN ok4 int");
        }
        exec("INSERT INTO dl_lock2 VALUES ('x', 1, 2, 3, 4)");
        assertEquals("x|1|2|3|4", rows("SELECT a, ok1, ok2, ok3, ok4 FROM dl_lock2"));
    }

    @Test
    void aRefusedUpdateLeavesTheRowUpdatable() throws Exception {
        exec("CREATE TABLE dl_row (id int primary key, v int)");
        exec("INSERT INTO dl_row VALUES (1, 1)");
        assertRejected("22003", "integer out of range",
                "UPDATE dl_row SET v = 2147483648 WHERE id = 1");
        exec("UPDATE dl_row SET v = 2 WHERE id = 1");
        assertEquals("2", scalar("SELECT v FROM dl_row WHERE id = 1"));
        try (Connection other = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
             Statement st = other.createStatement()) {
            st.execute("UPDATE dl_row SET v = 3 WHERE id = 1");
        }
        assertEquals("3", scalar("SELECT v FROM dl_row WHERE id = 1"));
        exec("DELETE FROM dl_row WHERE id = 1");
        assertEquals("0", scalar("SELECT count(*) FROM dl_row"));
    }

    // ---- array_ndims answers with an integer ------------------------------

    @Test
    void arrayNdimsIsAnInteger() throws Exception {
        assertEquals("int4", typeOf("SELECT array_ndims(ARRAY[1,2])"));
        assertEquals("int4", typeOf("SELECT array_ndims(ARRAY[[[[[[1]]]]]])"));
        assertEquals("int4", typeOf("SELECT array_ndims(NULL::int[])"));
        assertEquals("integer", scalar("SELECT pg_typeof(array_ndims(ARRAY[1,2]))::text"));
    }

    @Test
    void arrayNdimsCountsTheDimensions() throws Exception {
        assertEquals("6", scalar("SELECT array_ndims(ARRAY[[[[[[1]]]]]])"));
        assertEquals("1", scalar("SELECT array_ndims(ARRAY[1,2,3])"));
        assertEquals("2", scalar("SELECT array_ndims(ARRAY[[1,2],[3,4]])"));
        assertEquals("2", scalar("SELECT array_ndims('{{1,2},{3,4}}'::int[])"));
        assertNull(scalar("SELECT array_ndims(NULL::int[])"));
    }

    @Test
    void aSeventhDimensionDoesNotExistToBeCounted() {
        assertRejected("54000", "number of array dimensions (7) exceeds the maximum allowed (6)",
                "SELECT array_ndims(ARRAY[[[[[[[1]]]]]]])");
        // The literal reader counts the braces as it walks them and has no total to name, which
        // is why PostgreSQL words this one without the count the constructor gives.
        assertRejected("54000", "number of array dimensions exceeds the maximum allowed (6)",
                "SELECT array_ndims('{{{{{{{1}}}}}}}'::int[])");
    }

    @Test
    void theNeighbouringDimensionFunctionsAreIntegersToo() throws Exception {
        assertEquals("int4", typeOf("SELECT array_upper(ARRAY[1,2], 1)"));
        assertEquals("int4", typeOf("SELECT array_lower(ARRAY[1,2], 1)"));
        assertEquals("int4", typeOf("SELECT array_length(ARRAY[1,2], 1)"));
        assertEquals("int4", typeOf("SELECT cardinality(ARRAY[1,2])"));
        assertEquals("int4", typeOf("SELECT num_nonnulls(1, NULL)"));
        assertEquals("int4", typeOf("SELECT num_nulls(1, NULL)"));
        // array_dims is text in PostgreSQL, and stays so
        assertEquals("text", typeOf("SELECT array_dims(ARRAY[1,2])"));
        assertEquals("[1:2]", scalar("SELECT array_dims(ARRAY[1,2])"));
    }

    @Test
    void arrayNdimsOnAColumnThroughAViewAndInWhere() throws Exception {
        exec("CREATE TABLE dl_arr (a int[])");
        exec("INSERT INTO dl_arr VALUES ('{1,2}'), ('{{1,2},{3,4}}'), (NULL)");
        assertEquals("1;2", rows("SELECT array_ndims(a) FROM dl_arr"
                + " WHERE array_ndims(a) >= 1 ORDER BY array_ndims(a)"));
        assertEquals("int4", typeOf("SELECT array_ndims(a) FROM dl_arr WHERE a IS NOT NULL"));
        exec("CREATE VIEW dl_arr_v AS SELECT array_ndims(a) AS d FROM dl_arr");
        assertEquals("2", scalar("SELECT d FROM dl_arr_v WHERE d > 1"));
        assertEquals("int4", typeOf("SELECT d FROM dl_arr_v WHERE d > 1"));
        assertEquals("3", scalar("SELECT array_ndims(a) + 1 FROM dl_arr WHERE array_ndims(a) = 2"));
        assertEquals("1;2", rows("SELECT sub.d FROM (SELECT array_ndims(a) AS d FROM dl_arr) sub"
                + " WHERE sub.d IS NOT NULL ORDER BY sub.d"));
    }
}
