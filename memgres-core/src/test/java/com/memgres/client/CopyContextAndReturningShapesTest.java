package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two things a client is told about a write: what a refused COPY reports about the line it was
 * reading, and what a RETURNING clause answers with when a second relation stands beside the target.
 *
 * <p>A COPY refusal carries the line back in its CONTEXT, and PostgreSQL is exact about it: the line
 * is quoted whole up to a hundred bytes and cut at exactly a hundred beyond that, with an ellipsis
 * standing for the rest and never a cut through the middle of a character. The line it names is
 * counted in newlines, so a newline carried inside a quoted CSV field counts towards the number --
 * but only once the reader knows what the input's terminator is, which it learns from the first
 * newline standing outside quotes. A refusal about a value writes that value the way the session
 * would read it, so a timestamptz is written in the session's TimeZone and not in the offset it was
 * stored under. And the line is only still in hand at all if PostgreSQL did not buffer ahead of it,
 * which it will not do when a column's default is a volatile function -- one the user declared
 * VOLATILE as much as a built-in, and not one whose name merely appears inside a string literal.
 *
 * <p>A RETURNING clause of a write that brings in a second relation stands in the scope of both, so
 * a bare star there is the target's columns followed by every column that relation supplies, in
 * both the simple and the extended query protocol. A sub-select in that clause reading nothing of
 * the row around it is read in the snapshot the statement began with, once for the statement, and
 * not at all when the statement writes no row. A qualified name nothing in scope answers to is
 * refused with 42703 and a hint naming the column it probably meant, in which the qualifier's own
 * distance from each relation's name is charged against the three edits a suggestion may cost.
 *
 * <p>Every expectation here was read off PostgreSQL 18 before it was written down.
 */
class CopyContextAndReturningShapesTest {

    static Memgres memgres;
    /** The simple query protocol, which is what a COPY and most of the writes below are read over. */
    static Connection conn;
    /** The extended query protocol: Parse, Bind, Describe, Execute. */
    static Connection extended;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        extended = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        extended.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (extended != null) extended.close();
        if (memgres != null) memgres.close();
    }

    private static Connection open() throws SQLException {
        Connection c = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    /** The fields of the error a statement raises, as a client reads them off the wire. */
    private static ServerErrorMessage fieldsOf(String sql) {
        return fieldsOf(conn, sql);
    }

    private static ServerErrorMessage fieldsOf(Connection c, String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> exec(c, sql),
                "expected an error from: " + sql);
        assertTrue(thrown instanceof PSQLException, "expected a server error from: " + sql);
        return ((PSQLException) thrown).getServerErrorMessage();
    }

    /** The fields of the error a COPY raises over the data a client sends it. */
    private static ServerErrorMessage copyFieldsOf(String sql, String data) {
        SQLException thrown = assertThrows(SQLException.class, () -> copyIn(conn, sql, data),
                "expected an error from: " + sql);
        assertTrue(thrown instanceof PSQLException, "expected a server error from: " + sql);
        return ((PSQLException) thrown).getServerErrorMessage();
    }

    /** The CONTEXT a refused COPY carries, which is where it names the line it was reading. */
    private static String copyContextOf(String sql, String data) {
        return copyFieldsOf(sql, data).getWhere();
    }

    private static long copyIn(Connection c, String sql, String data) throws SQLException, IOException {
        CopyManager cm = new CopyManager(c.unwrap(BaseConnection.class));
        return cm.copyIn(sql, new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)));
    }

    private static int byteLength(String s) {
        return s.getBytes(StandardCharsets.UTF_8).length;
    }

    /** The first n characters of the alphabet repeated, so that a line's length is exactly known. */
    private static String filler(int n) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < n) sb.append("abcdefghijklmnopqrstuvwxyz");
        return sb.substring(0, n);
    }

    private static String repeat(char c, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) sb.append(c);
        return sb.toString();
    }

    /** Every row a statement answers with, cells joined with "|" and rows with " / ". */
    private static String rows(String sql) throws SQLException {
        return rows(conn, sql);
    }

    private static String rows(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            if (!st.execute(sql)) return "";
            try (ResultSet rs = st.getResultSet()) {
                return readRows(rs);
            }
        }
    }

    private static String readRows(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        List<String> got = new ArrayList<>();
        while (rs.next()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                if (i > 1) sb.append('|');
                String v = rs.getString(i);
                sb.append(v == null ? "NULL" : v);
            }
            got.add(sb.toString());
        }
        return String.join(" / ", got);
    }

    /** The column names a client reads off the answer, joined with a comma. */
    private static String labelsOf(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            assertTrue(st.execute(sql), "expected an answer from: " + sql);
            try (ResultSet rs = st.getResultSet()) {
                return labels(rs.getMetaData());
            }
        }
    }

    private static String labels(ResultSetMetaData md) throws SQLException {
        List<String> got = new ArrayList<>();
        for (int i = 1; i <= md.getColumnCount(); i++) got.add(md.getColumnLabel(i));
        return String.join(",", got);
    }

    private static long num(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getLong(1);
        }
    }

    // ------------------------------------------------------------ how much of the line a refused
    // ------------------------------------------------------------ COPY quotes back

    // The relations below take one column and every line sent to them holds two, so each refusal is
    // about the line as a whole and quotes it. What PostgreSQL keeps of a line is a hundred bytes,
    // so once a line is over that the CONTEXT is a fixed length whatever the rest of the line looks
    // like: the relation's name, then the hundred bytes, then the ellipsis inside the quotes.

    @Test
    void aRefusedCopyQuotesALineOfAHundredBytesWhole() throws Exception {
        exec("CREATE TABLE zzt4f_ct (a text)");
        String copy = "COPY zzt4f_ct FROM STDIN";
        String prefix = "COPY zzt4f_ct, line 1: ";

        String line99 = filler(97) + "\tX";
        assertEquals(prefix + "\"" + line99 + "\"", copyContextOf(copy, line99 + "\n"));

        String line100 = filler(98) + "\tX";
        String context = copyContextOf(copy, line100 + "\n");
        assertEquals(prefix + "\"" + line100 + "\"", context);
        assertEquals(125, byteLength(context));

        // and the refusal itself is about the extra field, not about the length
        ServerErrorMessage fields = copyFieldsOf(copy, line100 + "\n");
        assertEquals("22P04", fields.getSQLState());
        assertEquals("extra data after last expected column", fields.getMessage());
    }

    @Test
    void aLineLongerThanAHundredBytesIsCutAtExactlyAHundredAndEndsInAnEllipsis() throws Exception {
        exec("CREATE TABLE zzt4f_ct2 (a text)");
        String copy = "COPY zzt4f_ct2 FROM STDIN";
        String prefix = "COPY zzt4f_ct2, line 1: ";

        // one byte over: the hundred bytes kept end in the tab, and the X is what the ellipsis
        // stands for. 129 is this relation's name plus a hundred bytes plus the ellipsis and quotes.
        String cut101 = copyContextOf(copy, filler(99) + "\tX\n");
        assertEquals(prefix + "\"" + filler(99) + "\t...\"", cut101);
        assertEquals(129, byteLength(cut101));

        // two bytes over, and a hundred bytes over: the same hundred bytes and the same length
        String cut102 = copyContextOf(copy, filler(100) + "\tX\n");
        String cut110 = copyContextOf(copy, filler(108) + "\tX\n");
        String cut200 = copyContextOf(copy, filler(198) + "\tX\n");
        assertEquals(prefix + "\"" + filler(100) + "...\"", cut102);
        assertEquals(cut102, cut110);
        assertEquals(cut102, cut200);
        assertEquals(129, byteLength(cut110));
        assertEquals(129, byteLength(cut200));
    }

    @Test
    void theCutFallsBetweenCharactersAndNeverInsideOne() throws Exception {
        exec("CREATE TABLE zzt4f_ct3 (a text)");
        String copy = "COPY zzt4f_ct3 FROM STDIN";
        String prefix = "COPY zzt4f_ct3, line 1: ";

        // 49 two-byte characters and a tab and an X is a hundred bytes exactly: quoted whole
        String whole = copyContextOf(copy, repeat('é', 49) + "\tX\n");
        assertEquals(prefix + "\"" + repeat('é', 49) + "\tX\"", whole);
        assertEquals(126, byteLength(whole));

        // one character more and the cut falls exactly between two of them
        String cut = copyContextOf(copy, repeat('é', 50) + "\tX\n");
        assertEquals(prefix + "\"" + repeat('é', 50) + "...\"", cut);
        assertEquals(129, byteLength(cut));
        assertEquals(cut, copyContextOf(copy, repeat('é', 51) + "\tX\n"));

        // with three-byte characters the hundredth byte falls inside one, so the cut stops short of
        // it: 33 of them and the tab are 100 bytes, but 34 of them would be 102
        assertEquals(prefix + "\"" + repeat('€', 33) + "\t...\"",
                copyContextOf(copy, repeat('€', 33) + "\tX\n"));
        String stoppedShort = copyContextOf(copy, repeat('€', 34) + "\tX\n");
        assertEquals(prefix + "\"" + repeat('€', 33) + "...\"", stoppedShort);
        assertEquals(128, byteLength(stoppedShort));
        assertEquals(stoppedShort, copyContextOf(copy, repeat('€', 35) + "\tX\n"));
    }

    // ------------------------------------------------------------ which line a refused COPY says
    // ------------------------------------------------------------ it was reading

    @Test
    void aNewlineInsideAQuotedFieldCountsTowardsTheLineNumber() throws Exception {
        exec("CREATE TABLE zzt4f_cn (a text, b int)");
        String csv = "COPY zzt4f_cn FROM STDIN WITH (FORMAT csv)";

        // three rows, the second of which carries a newline: the third row is line 4
        assertEquals("COPY zzt4f_cn, line 4, column b: \"notanint\"",
                copyContextOf(csv, "aa,1\n\"x\ny\",2\nzz,notanint\n"));

        // two newlines inside that field make it line 5
        assertEquals("COPY zzt4f_cn, line 5, column b: \"notanint\"",
                copyContextOf(csv, "aa,1\n\"x\ny\nz\",2\nzz,notanint\n"));

        // a header line is a line like any other
        assertEquals("COPY zzt4f_cn, line 5, column b: \"notanint\"",
                copyContextOf("COPY zzt4f_cn FROM STDIN WITH (FORMAT csv, HEADER)",
                        "a,b\naa,1\n\"x\ny\",2\nzz,notanint\n"));

        // and a carriage return and newline together are one terminator, not two
        assertEquals("COPY zzt4f_cn, line 4, column b: \"notanint\"",
                copyContextOf(csv, "aa,1\r\n\"x\r\ny\",2\r\nzz,notanint\r\n"));
    }

    @Test
    void aNewlineIsCountedOnlyOnceTheInputsTerminatorIsKnown() throws Exception {
        exec("CREATE TABLE zzt4f_cn2 (a text, b int)");
        String csv = "COPY zzt4f_cn2 FROM STDIN WITH (FORMAT csv)";

        // the newline inside the first field is read before any newline outside quotes has settled
        // what the terminator is, and until then a carriage return is what a line is taken to end
        // with -- so this newline is not counted and the failing row is line 2
        assertEquals("COPY zzt4f_cn2, line 2, column b: \"notanint\"",
                copyContextOf(csv, "\"x\ny\",1\nzz,notanint\n"));

        // a lone carriage return in that same position is counted, and the row is line 3
        assertEquals("COPY zzt4f_cn2, line 3, column b: \"notanint\"",
                copyContextOf(csv, "\"x\ry\",1\nzz,notanint\n"));
    }

    @Test
    void theNewlinesTheFailingRowItselfCarriesAreCountedToo() throws Exception {
        exec("CREATE TABLE zzt4f_cn3 (a text, b int)");
        String csv = "COPY zzt4f_cn3 FROM STDIN WITH (FORMAT csv)";

        // the row that fails is the second, and it spans two lines of its own: line 3, not line 2
        assertEquals("COPY zzt4f_cn3, line 3, column b: \"notanint\"",
                copyContextOf(csv, "aa,1\n\"x\ny\",notanint\n"));
    }

    @Test
    void aRefusalAboutTheWholeLineCountsTheLinesTheSameWay() throws Exception {
        exec("CREATE TABLE zzt4f_cn4 (a text, b int)");
        String csv = "COPY zzt4f_cn4 FROM STDIN WITH (FORMAT csv)";

        // a quote that is never closed swallows the rest of the input, newline and all, so the line
        // it is reported against is one past the line it began on
        ServerErrorMessage unterminated = copyFieldsOf(csv, "aa,1\n\"x\ny\",2\n\"zz,3\n");
        assertEquals("22P04", unterminated.getSQLState());
        assertEquals("unterminated CSV quoted field", unterminated.getMessage());
        assertEquals("COPY zzt4f_cn4, line 5: \"\"zz,3\n\"", unterminated.getWhere());

        // and a row with one field too many is quoted whole, newline and all
        ServerErrorMessage extra = copyFieldsOf(csv, "aa,1\n\"x\ny\",2,3\n");
        assertEquals("22P04", extra.getSQLState());
        assertEquals("extra data after last expected column", extra.getMessage());
        assertEquals("COPY zzt4f_cn4, line 3: \"\"x\ny\",2,3\"", extra.getWhere());
    }

    // ------------------------------------------------------------ how a refusal writes a value it
    // ------------------------------------------------------------ read

    @Test
    void aDuplicateTimestamptzIsWrittenInTheSessionsTimeZone() throws Exception {
        try (Connection c = open()) {
            exec(c, "CREATE TABLE zzt4f_tz (t timestamptz PRIMARY KEY)");
            exec(c, "INSERT INTO zzt4f_tz VALUES ('2024-01-01 00:00:00+00')");
            String dup = "INSERT INTO zzt4f_tz VALUES ('2024-01-01 00:00:00+00')";

            exec(c, "SET TimeZone='Asia/Tokyo'");
            ServerErrorMessage tokyo = fieldsOf(c, dup);
            assertEquals("23505", tokyo.getSQLState());
            assertEquals("Key (t)=(2024-01-01 09:00:00+09) already exists.", tokyo.getDetail());

            // the same stored row, read on the other side of the date line
            exec(c, "SET TimeZone='America/New_York'");
            assertEquals("Key (t)=(2023-12-31 19:00:00-05) already exists.", fieldsOf(c, dup).getDetail());

            exec(c, "SET TimeZone='UTC'");
            assertEquals("Key (t)=(2024-01-01 00:00:00+00) already exists.", fieldsOf(c, dup).getDetail());

            exec(c, "SET TimeZone='Europe/Amsterdam'");
            assertEquals("Key (t)=(2024-01-01 01:00:00+01) already exists.", fieldsOf(c, dup).getDetail());
        }
    }

    @Test
    void aFailingRowContainingATimestamptzIsWrittenTheSameWay() throws Exception {
        try (Connection c = open()) {
            exec(c, "CREATE TABLE zzt4f_ckz (t timestamptz CHECK (t > '2030-01-01 00:00:00+00'))");
            String bad = "INSERT INTO zzt4f_ckz VALUES ('2024-01-01 00:00:00+00')";

            exec(c, "SET TimeZone='Asia/Tokyo'");
            ServerErrorMessage tokyo = fieldsOf(c, bad);
            assertEquals("23514", tokyo.getSQLState());
            assertEquals("Failing row contains (2024-01-01 09:00:00+09).", tokyo.getDetail());

            exec(c, "SET TimeZone='America/New_York'");
            assertEquals("Failing row contains (2023-12-31 19:00:00-05).", fieldsOf(c, bad).getDetail());

            exec(c, "SET TimeZone='UTC'");
            assertEquals("Failing row contains (2024-01-01 00:00:00+00).", fieldsOf(c, bad).getDetail());
        }
    }

    // ------------------------------------------------------------ when the line is still in hand
    // ------------------------------------------------------------ at all

    // PostgreSQL reads COPY input a batch at a time and writes the batch in one go, and by the time
    // such a batch is written the line that produced a row is long gone -- so the refusal names the
    // line but does not quote it. It will not batch ahead of a volatile default, because the default
    // has to be worked out row by row, and then the line is still there to quote.

    @Test
    void aCopyRefusalStillQuotesTheLineWhenAColumnDefaultIsVolatile() throws Exception {
        exec("CREATE FUNCTION zzt4f_fv() RETURNS int LANGUAGE plpgsql VOLATILE AS $$ BEGIN RETURN 7; END $$");
        exec("CREATE TABLE zzt4f_v1 (k int PRIMARY KEY, d int DEFAULT zzt4f_fv())");
        exec("INSERT INTO zzt4f_v1 (k) VALUES (1)");

        assertEquals("COPY zzt4f_v1, line 1: \"1\"",
                copyContextOf("COPY zzt4f_v1 (k) FROM STDIN", "1\n"));
        // the line it names is the line it was reading, not the first of the batch
        assertEquals("COPY zzt4f_v1, line 2: \"1\"",
                copyContextOf("COPY zzt4f_v1 (k) FROM STDIN", "9\n1\n"));
    }

    @Test
    void aDefaultThatIsNotVolatileLeavesTheRefusalWithTheLineNumberAlone() throws Exception {
        exec("CREATE FUNCTION zzt4f_fs() RETURNS int LANGUAGE plpgsql STABLE AS $$ BEGIN RETURN 7; END $$");
        exec("CREATE FUNCTION zzt4f_fi() RETURNS int LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN RETURN 7; END $$");
        exec("CREATE TABLE zzt4f_v2 (k int PRIMARY KEY, d int DEFAULT zzt4f_fs())");
        exec("CREATE TABLE zzt4f_v3 (k int PRIMARY KEY, d int DEFAULT zzt4f_fi())");
        exec("CREATE TABLE zzt4f_v4 (k int PRIMARY KEY, d int DEFAULT 42)");
        exec("INSERT INTO zzt4f_v2 (k) VALUES (1)");
        exec("INSERT INTO zzt4f_v3 (k) VALUES (1)");
        exec("INSERT INTO zzt4f_v4 (k) VALUES (1)");

        assertEquals("COPY zzt4f_v2, line 1", copyContextOf("COPY zzt4f_v2 (k) FROM STDIN", "1\n"));
        assertEquals("COPY zzt4f_v3, line 1", copyContextOf("COPY zzt4f_v3 (k) FROM STDIN", "1\n"));
        assertEquals("COPY zzt4f_v4, line 1", copyContextOf("COPY zzt4f_v4 (k) FROM STDIN", "1\n"));
        assertEquals("COPY zzt4f_v4, line 2", copyContextOf("COPY zzt4f_v4 (k) FROM STDIN", "9\n1\n"));
    }

    @Test
    void everyVolatileDefaultKeepsTheLineAndEveryOtherOneDoesNot() throws Exception {
        exec("CREATE SEQUENCE zzt4f_vsq");
        exec("CREATE FUNCTION zzt4f_sqli() RETURNS double precision LANGUAGE sql IMMUTABLE"
                + " AS $$ SELECT random() $$");
        exec("CREATE TABLE zzt4f_w1 (k int PRIMARY KEY, d double precision DEFAULT random())");
        exec("CREATE TABLE zzt4f_w2 (k int PRIMARY KEY, d timestamptz DEFAULT clock_timestamp())");
        exec("CREATE TABLE zzt4f_w3 (k int PRIMARY KEY, d uuid DEFAULT gen_random_uuid())");
        exec("CREATE TABLE zzt4f_w4 (k int PRIMARY KEY, d int DEFAULT nextval('zzt4f_vsq'))");
        exec("CREATE TABLE zzt4f_w5 (k int PRIMARY KEY, d timestamptz DEFAULT now())");
        exec("CREATE TABLE zzt4f_w6 (k int PRIMARY KEY, d timestamptz DEFAULT CURRENT_TIMESTAMP)");
        exec("CREATE TABLE zzt4f_w7 (k int PRIMARY KEY, d double precision DEFAULT zzt4f_sqli())");
        exec("CREATE TABLE zzt4f_w8 (k int PRIMARY KEY, d text DEFAULT 'random(' || 'x')");
        exec("CREATE FUNCTION zzt4f_sqlv() RETURNS bigint LANGUAGE sql VOLATILE"
                + " AS $$ SELECT count(*) FROM zzt4f_w8 $$");
        exec("CREATE TABLE zzt4f_w9 (k int PRIMARY KEY, d bigint DEFAULT zzt4f_sqlv())");
        for (int i = 1; i <= 9; i++) exec("INSERT INTO zzt4f_w" + i + " (k) VALUES (1)");

        // worked out afresh for every row, so the line is still there to quote
        assertEquals("COPY zzt4f_w1, line 1: \"1\"", copyContextOf("COPY zzt4f_w1 (k) FROM STDIN", "1\n"));
        assertEquals("COPY zzt4f_w2, line 1: \"1\"", copyContextOf("COPY zzt4f_w2 (k) FROM STDIN", "1\n"));
        assertEquals("COPY zzt4f_w3, line 1: \"1\"", copyContextOf("COPY zzt4f_w3 (k) FROM STDIN", "1\n"));

        // a sequence is volatile but is deliberately not counted here, so this one batches
        assertEquals("COPY zzt4f_w4, line 1", copyContextOf("COPY zzt4f_w4 (k) FROM STDIN", "1\n"));
        // and neither now() nor CURRENT_TIMESTAMP moves within a statement
        assertEquals("COPY zzt4f_w5, line 1", copyContextOf("COPY zzt4f_w5 (k) FROM STDIN", "1\n"));
        assertEquals("COPY zzt4f_w6, line 1", copyContextOf("COPY zzt4f_w6 (k) FROM STDIN", "1\n"));
        // what a function was declared to be is what counts, not what its body does
        assertEquals("COPY zzt4f_w7, line 1", copyContextOf("COPY zzt4f_w7 (k) FROM STDIN", "1\n"));
        // a string literal that spells out a function call is a string, not a call
        assertEquals("COPY zzt4f_w8, line 1", copyContextOf("COPY zzt4f_w8 (k) FROM STDIN", "1\n"));
        // a default that reads a relation is volatile and keeps the line
        assertEquals("COPY zzt4f_w9, line 1: \"1\"", copyContextOf("COPY zzt4f_w9 (k) FROM STDIN", "1\n"));
    }

    // ------------------------------------------------------------ what a bare star in a RETURNING
    // ------------------------------------------------------------ clause stands for

    @Test
    void aBareStarAnswersWithTheSecondRelationsColumnsToo() throws Exception {
        exec("CREATE TABLE zzt4f_rx (i int, v text, w text)");
        exec("CREATE TABLE zzt4f_rxs (j int, p text, q text)");
        exec("CREATE TABLE zzt4f_rxt (k int, r text)");
        exec("INSERT INTO zzt4f_rx VALUES (1,'a','L1'),(2,'b','L2')");
        exec("INSERT INTO zzt4f_rxs VALUES (1,'p','R1'),(3,'z','R3')");
        exec("INSERT INTO zzt4f_rxt VALUES (1,'T1'),(4,'T4')");

        String update = "UPDATE zzt4f_rx t SET v='y' FROM zzt4f_rxs u WHERE t.i=u.j RETURNING *";
        assertEquals("i,v,w,j,p,q", labelsOf(conn, update));
        assertEquals("1|y|L1|1|p|R1", rows(update));

        String delete = "DELETE FROM zzt4f_rx t USING zzt4f_rxs u WHERE t.i=u.j RETURNING *";
        assertEquals("i,v,w,j,p,q", labelsOf(conn, delete));
        exec("INSERT INTO zzt4f_rx VALUES (1,'a','L1')");
        assertEquals("1|a|L1|1|p|R1", rows(delete));
        exec("INSERT INTO zzt4f_rx VALUES (1,'a','L1')");

        // a join in the FROM adds the relation it joined too
        assertEquals("i,v,w,j,p,q,k,r", labelsOf(conn, "UPDATE zzt4f_rx t SET v='y2'"
                + " FROM zzt4f_rxs u JOIN zzt4f_rxt s ON s.k=u.j WHERE t.i=u.j RETURNING *"));
        // a derived FROM item supplies its select list
        assertEquals("i,v,w,j,p", labelsOf(conn, "UPDATE zzt4f_rx t SET v='y3'"
                + " FROM (SELECT j, p FROM zzt4f_rxs) u WHERE t.i=u.j RETURNING *"));
        // and the star stands where it was written
        assertEquals("1|y4|L1|1|p|R1|p!", rows("UPDATE zzt4f_rx t SET v='y4'"
                + " FROM zzt4f_rxs u WHERE t.i=u.j RETURNING *, u.p || '!'"));
    }

    @Test
    void aStarWrittenAgainstTheTargetStaysTheTargetsAlone() throws Exception {
        exec("CREATE TABLE zzt4f_rx2 (i int, v text, w text)");
        exec("CREATE TABLE zzt4f_rxs2 (j int, p text, q text)");
        exec("INSERT INTO zzt4f_rx2 VALUES (1,'a','L1')");
        exec("INSERT INTO zzt4f_rxs2 VALUES (1,'p','R1')");

        String qualified = "UPDATE zzt4f_rx2 t SET v='y' FROM zzt4f_rxs2 u WHERE t.i=u.j RETURNING t.*";
        assertEquals("i,v,w", labelsOf(conn, qualified));
        assertEquals("1|y|L1", rows(qualified));

        // a write with no second relation answers with its own columns, as it always did
        assertEquals("i,v,w", labelsOf(conn, "UPDATE zzt4f_rx2 SET v='z' RETURNING *"));
    }

    @Test
    void theStarIsAnsweredTheSameWayInTheExtendedProtocol() throws Exception {
        exec("CREATE TABLE zzt4f_ex (i int, v text, w text)");
        exec("CREATE TABLE zzt4f_exs (j int, p text, q text)");
        exec("CREATE TABLE zzt4f_ext (k int, r text)");
        exec("INSERT INTO zzt4f_ex VALUES (1,'a','L1'),(2,'b','L2')");
        exec("INSERT INTO zzt4f_exs VALUES (1,'p','R1'),(3,'z','R3')");
        exec("INSERT INTO zzt4f_ext VALUES (1,'T1')");

        try (PreparedStatement ps = extended.prepareStatement(
                "UPDATE zzt4f_ex t SET v='y' FROM zzt4f_exs u WHERE t.i=u.j RETURNING *")) {
            assertTrue(ps.execute(), "expected an answer from a RETURNING clause");
            try (ResultSet rs = ps.getResultSet()) {
                assertEquals("i,v,w,j,p,q", labels(rs.getMetaData()));
                assertEquals("1|y|L1|1|p|R1", readRows(rs));
            }
        }

        // a parameter placed in the statement changes nothing about the shape of its answer
        try (PreparedStatement ps = extended.prepareStatement(
                "UPDATE zzt4f_ex t SET v='y2' FROM zzt4f_exs u WHERE t.i=u.j AND t.i=? RETURNING *")) {
            ps.setInt(1, 1);
            assertTrue(ps.execute(), "expected an answer from a RETURNING clause");
            try (ResultSet rs = ps.getResultSet()) {
                assertEquals("i,v,w,j,p,q", labels(rs.getMetaData()));
                assertEquals("1|y2|L1|1|p|R1", readRows(rs));
            }
        }

        try (PreparedStatement ps = extended.prepareStatement("DELETE FROM zzt4f_ex t"
                + " USING zzt4f_exs u JOIN zzt4f_ext s ON s.k=u.j WHERE t.i=u.j RETURNING *")) {
            assertTrue(ps.execute(), "expected an answer from a RETURNING clause");
            try (ResultSet rs = ps.getResultSet()) {
                assertEquals("i,v,w,j,p,q,k,r", labels(rs.getMetaData()));
                assertEquals("1|y2|L1|1|p|R1|1|T1", readRows(rs));
            }
        }

        // and a write with no second relation is described by its own columns
        try (PreparedStatement ps = extended.prepareStatement(
                "UPDATE zzt4f_ex SET v='y3' WHERE i=2 RETURNING *")) {
            assertTrue(ps.execute(), "expected an answer from a RETURNING clause");
            try (ResultSet rs = ps.getResultSet()) {
                assertEquals("i,v,w", labels(rs.getMetaData()));
                assertEquals("2|y3|L2", readRows(rs));
            }
        }
    }

    // ------------------------------------------------------------ what a sub-select in a RETURNING
    // ------------------------------------------------------------ list is allowed to see

    @Test
    void aSubSelectInAReturningListReadsTheRowsTheStatementBeganWith() throws Exception {
        exec("CREATE TABLE zzt4f_sx (i int, v text)");
        exec("INSERT INTO zzt4f_sx VALUES (1,'a'),(2,'b')");

        // two rows stood there when the statement began, and all three written rows say so
        assertEquals("10|2 / 11|2 / 12|2", rows("INSERT INTO zzt4f_sx VALUES (10,'x'),(11,'y'),(12,'z')"
                + " RETURNING i, (SELECT count(*) FROM zzt4f_sx)"));
        assertEquals("20|5|12 / 21|5|12 / 22|5|12",
                rows("INSERT INTO zzt4f_sx SELECT g, 'g' FROM generate_series(20,22) g"
                        + " RETURNING i, (SELECT count(*) FROM zzt4f_sx), (SELECT max(i) FROM zzt4f_sx)"));

        // no row carries the value this UPDATE is assigning yet
        assertEquals("1|0 / 2|0", rows("UPDATE zzt4f_sx SET v='u' WHERE i<3"
                + " RETURNING i, (SELECT count(*) FROM zzt4f_sx WHERE v='u')"));

        // and what a DELETE is taking away is still counted while it reports it
        assertEquals("20|8 / 21|8 / 22|8", rows("DELETE FROM zzt4f_sx WHERE i>=20"
                + " RETURNING i, (SELECT count(*) FROM zzt4f_sx)"));
        assertEquals(5L, num("SELECT count(*) FROM zzt4f_sx"));

        // a sub-select that does read the row around it is answered from that row
        assertEquals("40|41", rows("INSERT INTO zzt4f_sx VALUES (40,'m') RETURNING i, (SELECT i + 1)"));
    }

    @Test
    void aSubSelectIsReadOncePerStatementAndNotAtAllWhenNoRowIsWritten() throws Exception {
        exec("CREATE TABLE zzt4f_sy (i int, v text)");
        exec("CREATE SEQUENCE zzt4f_syq");
        exec("INSERT INTO zzt4f_sy VALUES (1,'a'),(2,'b')");

        assertEquals("1|1", rows("INSERT INTO zzt4f_sy VALUES (1,'q')"
                + " RETURNING i, (SELECT nextval('zzt4f_syq'))"));
        assertEquals(1L, num("SELECT last_value FROM zzt4f_syq"));

        // a statement that writes no row never reads it, so the sequence stands where it stood
        assertEquals("", rows("UPDATE zzt4f_sy SET v='w' WHERE false"
                + " RETURNING i, (SELECT nextval('zzt4f_syq'))"));
        assertEquals(1L, num("SELECT last_value FROM zzt4f_syq"));
        assertEquals("", rows("DELETE FROM zzt4f_sy WHERE false"
                + " RETURNING i, (SELECT nextval('zzt4f_syq'))"));
        assertEquals(1L, num("SELECT last_value FROM zzt4f_syq"));

        // three rows written move it on once, and every one of them reports that one value
        assertEquals("1|2 / 2|2 / 1|2", rows("UPDATE zzt4f_sy SET v='e'"
                + " RETURNING i, (SELECT nextval('zzt4f_syq'))"));
        assertEquals(2L, num("SELECT last_value FROM zzt4f_syq"));
    }

    // ------------------------------------------------------------ a RETURNING qualifier nothing
    // ------------------------------------------------------------ in scope answers to

    // OLD and NEW name the target's row before and after the write, so a name written under them is
    // looked for in the target and nowhere else. When the target has not got it, PostgreSQL refuses
    // and offers the column it thinks was meant -- searching the whole range table, the target first
    // and then the relations the FROM or USING named.

    @Test
    void aQualifierNothingAnswersToNamesTheColumnItProbablyMeant() throws Exception {
        exec("CREATE TABLE zzt4f_hx (i int, v text)");
        exec("CREATE TABLE zzt4f_hs (j int, w text)");
        exec("INSERT INTO zzt4f_hx VALUES (1,'a')");
        exec("INSERT INTO zzt4f_hs VALUES (1,'p')");

        // the target's own row, before and after the write, is what OLD and NEW answer with
        assertEquals("a", rows("UPDATE zzt4f_hx t SET v='q' FROM zzt4f_hs u"
                + " WHERE t.i=u.j RETURNING old.v"));
        assertEquals("q2", rows("UPDATE zzt4f_hx t SET v='q2' FROM zzt4f_hs u"
                + " WHERE t.i=u.j RETURNING new.v"));

        // a column the target has not got is refused, and the second relation's is offered
        ServerErrorMessage fields = fieldsOf("UPDATE zzt4f_hx t SET v='q3' FROM zzt4f_hs u"
                + " WHERE t.i=u.j RETURNING old.w");
        assertEquals("42703", fields.getSQLState());
        assertEquals("column old.w does not exist", fields.getMessage());
        assertEquals("Perhaps you meant to reference the column \"u.w\".", fields.getHint());

        // NEW is read the same way
        assertEquals("Perhaps you meant to reference the column \"u.w\".",
                fieldsOf("UPDATE zzt4f_hx t SET v='q4' FROM zzt4f_hs u"
                        + " WHERE t.i=u.j RETURNING new.w").getHint());
        // and so is a name of the second relation's that was written under the wrong qualifier
        assertEquals("Perhaps you meant to reference the column \"u.j\".",
                fieldsOf("UPDATE zzt4f_hx t SET v='q5' FROM zzt4f_hs u"
                        + " WHERE t.i=u.j RETURNING old.j").getHint());

        // the name is folded to lower case in the message, as any unquoted name is
        ServerErrorMessage upper = fieldsOf("UPDATE zzt4f_hx t SET v='q6' FROM zzt4f_hs u"
                + " WHERE t.i=u.j RETURNING OLD.W");
        assertEquals("column old.w does not exist", upper.getMessage());
        assertEquals("Perhaps you meant to reference the column \"u.w\".", upper.getHint());
    }

    @Test
    void theRefusalIsOwedWhicheverWriteBroughtTheSecondRelationIn() throws Exception {
        exec("CREATE TABLE zzt4f_hx2 (i int, v text)");
        exec("CREATE TABLE zzt4f_hs2 (j int, w text)");
        exec("INSERT INTO zzt4f_hx2 VALUES (1,'a')");
        exec("INSERT INTO zzt4f_hs2 VALUES (1,'p')");
        String hint = "Perhaps you meant to reference the column \"u.w\".";

        assertEquals(hint, fieldsOf("DELETE FROM zzt4f_hx2 t USING zzt4f_hs2 u"
                + " WHERE t.i=u.j RETURNING old.w").getHint());
        assertEquals(hint, fieldsOf("MERGE INTO zzt4f_hx2 t USING zzt4f_hs2 u ON t.i=u.j"
                + " WHEN MATCHED THEN UPDATE SET v='m' RETURNING old.w").getHint());
        // a pairing that reaches no row is refused just the same: the clause is read before it runs
        assertEquals(hint, fieldsOf("UPDATE zzt4f_hx2 t SET v='q' FROM zzt4f_hs2 u"
                + " WHERE false RETURNING old.w").getHint());
        // and a target written without an alias of its own is read no differently
        assertEquals(hint, fieldsOf("UPDATE zzt4f_hx2 SET v='q' FROM zzt4f_hs2 u"
                + " WHERE zzt4f_hx2.i=u.j RETURNING old.w").getHint());
    }

    @Test
    void theHintOffersEveryRelationThatHoldsTheName() throws Exception {
        exec("CREATE TABLE zzt4f_hx3 (i int, v text)");
        exec("CREATE TABLE zzt4f_hs3 (j int, w text)");
        exec("CREATE TABLE zzt4f_hz3 (m int, w text)");
        exec("INSERT INTO zzt4f_hx3 VALUES (1,'a')");
        exec("INSERT INTO zzt4f_hs3 VALUES (1,'p')");
        exec("INSERT INTO zzt4f_hz3 VALUES (1,'zz')");

        assertEquals("Perhaps you meant to reference the column \"u.w\" or the column \"z.w\".",
                fieldsOf("UPDATE zzt4f_hx3 t SET v='q' FROM zzt4f_hs3 u, zzt4f_hz3 z"
                        + " WHERE t.i=u.j AND t.i=z.m RETURNING old.w").getHint());
        assertEquals("Perhaps you meant to reference the column \"u.w\" or the column \"z.w\".",
                fieldsOf("DELETE FROM zzt4f_hx3 t USING zzt4f_hs3 u, zzt4f_hz3 z"
                        + " WHERE t.i=u.j AND t.i=z.m RETURNING old.w").getHint());
    }

    @Test
    void theQualifiersOwnDistanceIsChargedAgainstTheSuggestion() throws Exception {
        exec("CREATE TABLE zzt4f_hx4 (i int, v text)");
        exec("CREATE TABLE zzt4f_hs4 (j int, w text)");
        exec("CREATE TABLE zzt4f_hz4 (m int, w text)");
        exec("CREATE TABLE zzt4f_hd4 (n int, w text)");
        exec("INSERT INTO zzt4f_hx4 VALUES (1,'a')");
        exec("INSERT INTO zzt4f_hs4 VALUES (1,'p')");
        exec("INSERT INTO zzt4f_hz4 VALUES (1,'zz')");
        exec("INSERT INTO zzt4f_hd4 VALUES (1,'d')");

        // three relations hold w, but "dd" is two edits from "old" where "u" and "z" are three, so
        // the nearer one is the only one offered
        assertEquals("Perhaps you meant to reference the column \"dd.w\".",
                fieldsOf("UPDATE zzt4f_hx4 t SET v='q' FROM zzt4f_hs4 u, zzt4f_hz4 z, zzt4f_hd4 dd"
                        + " WHERE t.i=u.j AND t.i=z.m AND t.i=dd.n RETURNING old.w").getHint());

        // a single relation is offered whether its name is one edit away or three
        assertEquals("Perhaps you meant to reference the column \"o.w\".",
                fieldsOf("UPDATE zzt4f_hx4 t SET v='q' FROM zzt4f_hs4 o"
                        + " WHERE t.i=o.j RETURNING old.w").getHint());
        assertEquals("Perhaps you meant to reference the column \"old2.w\".",
                fieldsOf("UPDATE zzt4f_hx4 t SET v='q' FROM zzt4f_hs4 old2"
                        + " WHERE t.i=old2.j RETURNING old.w").getHint());

        // but a name too far from the qualifier costs more than a suggestion is allowed
        assertNull(fieldsOf("UPDATE zzt4f_hx4 t SET v='q' FROM zzt4f_hs4 zzzzzz"
                + " WHERE t.i=zzzzzz.j RETURNING old.w").getHint());
    }

    @Test
    void aFromItemAliasedOldIsOfferedBackCasedAsItWasWritten() throws Exception {
        exec("CREATE TABLE zzt4f_hx5 (i int, v text)");
        exec("CREATE TABLE zzt4f_hs5 (j int, w text)");
        exec("INSERT INTO zzt4f_hx5 VALUES (1,'a')");
        exec("INSERT INTO zzt4f_hs5 VALUES (1,'p')");

        assertEquals("Perhaps you meant to reference the column \"OLD.w\".",
                fieldsOf("UPDATE zzt4f_hx5 t SET v='q' FROM zzt4f_hs5 \"OLD\""
                        + " WHERE t.i=\"OLD\".j RETURNING old.w").getHint());
    }

    @Test
    void aNameTooFarFromAnyColumnIsRefusedWithoutAHint() throws Exception {
        exec("CREATE TABLE zzt4f_hx6 (i int, v text)");
        exec("CREATE TABLE zzt4f_hs6 (j int, w text)");
        exec("INSERT INTO zzt4f_hx6 VALUES (1,'a')");
        exec("INSERT INTO zzt4f_hs6 VALUES (1,'p')");

        // two edits from w and two from the qualifier is one edit too many
        ServerErrorMessage ww = fieldsOf("UPDATE zzt4f_hx6 t SET v='q' FROM zzt4f_hs6 u"
                + " WHERE t.i=u.j RETURNING old.ww");
        assertEquals("42703", ww.getSQLState());
        assertEquals("column old.ww does not exist", ww.getMessage());
        assertNull(ww.getHint());

        for (String name : new String[]{"jj", "vv", "ii", "q"}) {
            ServerErrorMessage fields = fieldsOf("UPDATE zzt4f_hx6 t SET v='q' FROM zzt4f_hs6 u"
                    + " WHERE t.i=u.j RETURNING old." + name);
            assertEquals("42703", fields.getSQLState(), "for old." + name);
            assertEquals("column old." + name + " does not exist", fields.getMessage());
            assertNull(fields.getHint(), "for old." + name);
        }
    }

    @Test
    void aQualifierNamingNoRelationAtAllIsADifferentRefusal() throws Exception {
        exec("CREATE TABLE zzt4f_hx7 (i int, v text)");
        exec("CREATE TABLE zzt4f_hs7 (j int, w text)");
        exec("INSERT INTO zzt4f_hx7 VALUES (1,'a')");
        exec("INSERT INTO zzt4f_hs7 VALUES (1,'p')");

        // "ol" is not a relation in scope the way OLD is, so it is the qualifier that is reported
        ServerErrorMessage fields = fieldsOf("UPDATE zzt4f_hx7 t SET v='q' FROM zzt4f_hs7 u"
                + " WHERE t.i=u.j RETURNING ol.w");
        assertEquals("42P01", fields.getSQLState());
        assertEquals("missing FROM-clause entry for table \"ol\"", fields.getMessage());
        assertNull(fields.getHint());
    }
}
