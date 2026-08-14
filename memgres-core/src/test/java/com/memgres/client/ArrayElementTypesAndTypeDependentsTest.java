package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What an array of a written type is, and what a type another object was written with may lose.
 *
 * <p>Both were answers a client could not rely on. The value a column declares as its default was
 * not read by the column's own reader, so an array column took a bare word as a default and only
 * found out what it was much later, if ever; an element of an array was not held to the type the
 * element was declared with, so varchar(4)[] stored a seven-character element that a column of
 * varchar(4) would have refused, on INSERT, on UPDATE and on COPY alike; and a column of an array
 * over a written type recorded the same pair of facts as a column of a domain built over an array,
 * with nothing asking which of the two it was -- so pg_typeof, information_schema.columns and
 * pg_attribute each answered about the element where they should have answered about the column,
 * and an array of an enum was published with an enum's own four-byte pass-by-value layout.
 *
 * <p>The other half is what a type owes its dependents. An attribute of a composite type and a
 * domain written over another type both depend on the type they name, and a DROP of that type has
 * to say so: refuse with 2BP01, name every dependent in its DETAIL in the order PostgreSQL names
 * them, and say what to do about it. CASCADE takes the dependents rather than refusing, says what
 * it took, leaves the composite and the relation standing without the attribute and the column
 * they lost, and gives all of it back when the transaction that ran it rolls back.
 *
 * <p>Every expectation here was read off PostgreSQL 18 before it was written down.
 */
class ArrayElementTypesAndTypeDependentsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ------------------------------------------------------------ helpers

    /** The first column of the first row, as text, or "(no rows)" when the query answers none. */
    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    /** Every column of every row, rows joined with ";" and columns with "|". */
    private static String rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return String.join(";", out);
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /** The SQLSTATE a statement raises, or "OK" when it does not raise at all. */
    private static String stateOf(String sql) {
        try {
            exec(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The fields of the error a statement raises, as a client reads them off the wire. */
    private static org.postgresql.util.ServerErrorMessage fieldsOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> exec(sql),
                "expected an error from: " + sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
    }

    /** SQLSTATE and primary message of the error a statement raises, as "state: message". */
    private static String errorOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> exec(sql),
                "expected an error from: " + sql);
        org.postgresql.util.ServerErrorMessage m =
                ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
        return thrown.getSQLState() + ": " + m.getMessage();
    }

    private static String detailOf(String sql) {
        return fieldsOf(sql).getDetail();
    }

    private static String hintOf(String sql) {
        return fieldsOf(sql).getHint();
    }

    /** The message and the DETAIL of the first notice a statement raised, in that order. */
    private static String[] noticeOf(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            SQLWarning w = s.getWarnings();
            if (w == null) return new String[]{null, null};
            String detail = null;
            if (w instanceof org.postgresql.util.PSQLWarning) {
                org.postgresql.util.ServerErrorMessage m =
                        ((org.postgresql.util.PSQLWarning) w).getServerErrorMessage();
                if (m != null) detail = m.getDetail();
            }
            return new String[]{w.getMessage(), detail};
        }
    }

    /** COPY the given lines into a relation, answering the number of rows it stored. */
    private static long copyIn(String sql, String data) throws Exception {
        org.postgresql.copy.CopyManager cm =
                new org.postgresql.copy.CopyManager((org.postgresql.core.BaseConnection) conn);
        return cm.copyIn(sql, new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)));
    }

    /** The fields of the error a COPY raises, including the context PostgreSQL sends with it. */
    private static org.postgresql.util.ServerErrorMessage copyFailure(String sql, String data) {
        SQLException thrown = assertThrows(SQLException.class, () -> copyIn(sql, data),
                "expected an error from: " + sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
    }

    private static final String MALFORMED_DETAIL =
            "Array value must start with \"{\" or dimension information.";

    // ============================================================ a default is read as an array

    @Test
    void anArrayDefaultThatIsNotAnArrayIsRefusedByTheArrayReader() throws Exception {
        exec("CREATE DOMAIN zzt4d_dm AS int");
        exec("CREATE TYPE zzt4d_en AS ENUM ('a','b')");
        // The default is read when the column is defined, by the reader of the column's own type.
        // For an array column that is the array reader, and a bare word is not an array whatever
        // the array is of -- not a built-in element type, not an enum, not a domain.
        for (String type : new String[]{"text[]", "int[]", "numeric[]", "varchar(4)[]",
                "zzt4d_en[]", "zzt4d_dm[]"}) {
            String sql = "CREATE TABLE zzt4d_d1 (a " + type + " DEFAULT 'x')";
            assertEquals("22P02: malformed array literal: \"x\"", errorOf(sql), type);
            assertEquals(MALFORMED_DETAIL, detailOf(sql), type);
        }
        // and not one of those tables was defined
        assertEquals("0", scalar("SELECT count(*)::text FROM information_schema.tables"
                + " WHERE table_name = 'zzt4d_d1'"));
        exec("DROP DOMAIN zzt4d_dm");
        exec("DROP TYPE zzt4d_en");
    }

    @Test
    void anEmptyDefaultAndAWhitespaceDefaultAreNotArraysEither() {
        // The reader is shown exactly what it was given, so the literal in the message keeps its
        // spaces: an empty default is not an empty array.
        assertEquals("22P02: malformed array literal: \"\"",
                errorOf("CREATE TABLE zzt4d_d2 (a text[] DEFAULT '')"));
        assertEquals(MALFORMED_DETAIL, detailOf("CREATE TABLE zzt4d_d2 (a text[] DEFAULT '')"));
        assertEquals("22P02: malformed array literal: \"  \"",
                errorOf("CREATE TABLE zzt4d_d2 (a text[] DEFAULT '  ')"));
        assertEquals(MALFORMED_DETAIL, detailOf("CREATE TABLE zzt4d_d2 (a text[] DEFAULT '  ')"));
    }

    @Test
    void anElementOfAnArrayDefaultIsBlamedOnTheElementType() {
        // Once the braces are read, each element goes to the element type's own reader, and it is
        // that reader that fails -- so the message names integer and quotes the element alone,
        // and carries no DETAIL, because nothing about the array shape was wrong.
        String sql = "CREATE TABLE zzt4d_d3 (a int[] DEFAULT '{a}')";
        assertEquals("22P02: invalid input syntax for type integer: \"a\"", errorOf(sql));
        assertNull(detailOf(sql));
    }

    @Test
    void anAddedColumnAndAStoredGeneratedColumnReadTheirDefaultTheSameWay() throws Exception {
        exec("CREATE TYPE zzt4d_gen AS ENUM ('a','b')");
        exec("CREATE TABLE zzt4d_g1 (id int)");
        // A column added later carries a default just as one defined with the table does, and a
        // stored generated column holds an expression the same way: all three are read by the
        // column's own reader.
        assertEquals("22P02: malformed array literal: \"x\"",
                errorOf("ALTER TABLE zzt4d_g1 ADD COLUMN a text[] DEFAULT 'x'"));
        assertEquals(MALFORMED_DETAIL,
                detailOf("ALTER TABLE zzt4d_g1 ADD COLUMN a zzt4d_gen[] DEFAULT 'x'"));
        assertEquals("22P02: invalid input syntax for type integer: \"a\"",
                errorOf("ALTER TABLE zzt4d_g1 ADD COLUMN b int[] DEFAULT '{a}'"));
        assertEquals("22P02: malformed array literal: \"x\"",
                errorOf("CREATE TABLE zzt4d_g2 (id int, g text[] GENERATED ALWAYS AS ('x') STORED)"));
        assertEquals(MALFORMED_DETAIL, detailOf(
                "CREATE TABLE zzt4d_g2 (id int, g zzt4d_gen[] GENERATED ALWAYS AS ('x') STORED)"));
        assertEquals("22P02: invalid input syntax for type integer: \"a\"",
                errorOf("CREATE TABLE zzt4d_g2 (id int, g int[] GENERATED ALWAYS AS ('{a}') STORED)"));

        // What the reader accepts, the column keeps
        exec("ALTER TABLE zzt4d_g1 ADD COLUMN d varchar(4)[] DEFAULT '{ab}'");
        exec("INSERT INTO zzt4d_g1 (id) VALUES (1)");
        assertEquals("id,d", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name = 'zzt4d_g1'"));
        assertEquals("{ab}", scalar("SELECT d::text FROM zzt4d_g1"));
        exec("CREATE TABLE zzt4d_g3 (id int, g int[] GENERATED ALWAYS AS ('{1,2}') STORED)");
        exec("INSERT INTO zzt4d_g3 (id) VALUES (1)");
        assertEquals("{1,2}", scalar("SELECT g::text FROM zzt4d_g3"));
        exec("DROP TABLE zzt4d_g1");
        exec("DROP TABLE zzt4d_g3");
        exec("DROP TYPE zzt4d_gen");
    }

    @Test
    void anArrayDefaultTheReaderAcceptsIsStoredAsTheArrayItRead() throws Exception {
        exec("CREATE DOMAIN zzt4d_adm AS int");
        exec("CREATE TYPE zzt4d_aen AS ENUM ('a','b')");
        // Everything the array reader knows how to read is still read: an empty array, a literal
        // carrying its own dimensions, whitespace between elements, a NULL element, and the
        // special values a numeric or a float element can spell.
        exec("CREATE TABLE zzt4d_a1 (a int[] DEFAULT '{}', b int[] DEFAULT '[1:2]={1,2}',"
                + " c int[] DEFAULT '{ 1 , 2 }', d int[] DEFAULT '{1,NULL}',"
                + " e numeric[] DEFAULT '{NaN}', f float8[] DEFAULT '{Infinity}',"
                + " g zzt4d_adm[] DEFAULT '{1,2}', h zzt4d_aen[] DEFAULT '{a,b}',"
                + " i varchar(4)[] DEFAULT '{ab,cd}')");
        exec("INSERT INTO zzt4d_a1 DEFAULT VALUES");
        assertEquals("{}|{1,2}|{1,2}|{1,NULL}|{NaN}|{Infinity}|{1,2}|{a,b}|{ab,cd}",
                rows("SELECT a::text, b::text, c::text, d::text, e::text, f::text,"
                        + " g::text, h::text, i::text FROM zzt4d_a1"));
        exec("DROP TABLE zzt4d_a1");
        exec("DROP DOMAIN zzt4d_adm");
        exec("DROP TYPE zzt4d_aen");
    }

    // ============================================================ an element keeps its own width

    @Test
    void anOverlongElementIsRefusedOnEveryPathThatWritesTheRow() throws Exception {
        exec("CREATE TABLE zzt4d_w1 (a varchar(4)[])");
        exec("INSERT INTO zzt4d_w1 VALUES ('{abcd}')");
        // varchar(4)[] is an array of varchar(4): the element's own type refuses the value, so
        // the message names character varying(4) and not the array, on every path that writes a
        // row -- a literal, a constructor, a row read from a query, and either shape of UPDATE.
        for (String sql : new String[]{
                "INSERT INTO zzt4d_w1 VALUES ('{abcdefg}')",
                "INSERT INTO zzt4d_w1 VALUES (ARRAY['abcdefg'])",
                "INSERT INTO zzt4d_w1 SELECT ARRAY['abcdefg']",
                "UPDATE zzt4d_w1 SET a = '{abcdefg}'",
                "UPDATE zzt4d_w1 SET a = ARRAY['abcdefg']",
                "UPDATE zzt4d_w1 SET a[1] = 'abcdefg'",
                "INSERT INTO zzt4d_w1 VALUES ('{ab}'), ('{abcdefg}')"}) {
            assertEquals("22001: value too long for type character varying(4)", errorOf(sql), sql);
        }
        // and the relation holds only the row that fitted
        assertEquals("{abcd}", scalar("SELECT a::text FROM zzt4d_w1"));
        assertEquals("1", scalar("SELECT count(*)::text FROM zzt4d_w1"));
        exec("DROP TABLE zzt4d_w1");
    }

    @Test
    void copyFromStdinHoldsEachElementToItsOwnWidth() throws Exception {
        exec("CREATE TABLE zzt4d_cv (a varchar(4)[])");
        exec("CREATE TABLE zzt4d_cc (a char(3)[])");
        exec("CREATE TABLE zzt4d_cn (a numeric(4,1)[])");
        // COPY reads each field with the column's reader, so it is held to exactly what INSERT is
        // held to, and PostgreSQL says which line and which field it was reading when it stopped.
        org.postgresql.util.ServerErrorMessage tooLong =
                copyFailure("COPY zzt4d_cv FROM STDIN", "{abcdefg}\n");
        assertEquals("value too long for type character varying(4)", tooLong.getMessage());
        assertEquals("22001", tooLong.getSQLState());
        assertEquals("COPY zzt4d_cv, line 1, column a: \"{abcdefg}\"", tooLong.getWhere());
        assertEquals(1L, copyIn("COPY zzt4d_cv FROM STDIN", "{abcd}\n"));
        assertEquals("{abcd}", scalar("SELECT a::text FROM zzt4d_cv"));

        assertEquals(1L, copyIn("COPY zzt4d_cc FROM STDIN", "{ab}\n"));
        assertEquals("{\"ab \"}", scalar("SELECT a::text FROM zzt4d_cc"));
        org.postgresql.util.ServerErrorMessage padded =
                copyFailure("COPY zzt4d_cc FROM STDIN", "{abcd}\n");
        assertEquals("value too long for type character(3)", padded.getMessage());
        assertEquals("COPY zzt4d_cc, line 1, column a: \"{abcd}\"", padded.getWhere());

        assertEquals(1L, copyIn("COPY zzt4d_cn FROM STDIN", "{123.45}\n"));
        assertEquals("{123.5}", scalar("SELECT a::text FROM zzt4d_cn"));
        org.postgresql.util.ServerErrorMessage overflow =
                copyFailure("COPY zzt4d_cn FROM STDIN", "{12345.4}\n");
        assertEquals("numeric field overflow", overflow.getMessage());
        assertEquals("22003", overflow.getSQLState());
        assertEquals("A field with precision 4, scale 1 must round to an absolute value"
                + " less than 10^3.", overflow.getDetail());
        assertEquals("COPY zzt4d_cn, line 1, column a: \"{12345.4}\"", overflow.getWhere());
        // the refused line stored nothing
        assertEquals("1", scalar("SELECT count(*)::text FROM zzt4d_cn"));
        exec("DROP TABLE zzt4d_cv");
        exec("DROP TABLE zzt4d_cc");
        exec("DROP TABLE zzt4d_cn");
    }

    @Test
    void aCharacterElementIsPaddedToItsWidthAndRefusedPastIt() throws Exception {
        exec("CREATE TABLE zzt4d_w2 (a char(3)[])");
        // character(3) pads what it stores, and an array of it holds elements that were padded,
        // which is why they read back quoted.
        exec("INSERT INTO zzt4d_w2 VALUES ('{ab}')");
        assertEquals("{\"ab \"}", scalar("SELECT a::text FROM zzt4d_w2"));
        assertEquals("22001: value too long for type character(3)",
                errorOf("INSERT INTO zzt4d_w2 VALUES ('{abcd}')"));
        assertEquals("22001: value too long for type character(3)",
                errorOf("UPDATE zzt4d_w2 SET a = '{abcd}'"));
        assertEquals("{\"ab \"}", scalar("SELECT a::text FROM zzt4d_w2"));
        exec("DROP TABLE zzt4d_w2");
    }

    @Test
    void aNumericElementIsRoundedToItsScaleAndOverflowSaysWhatWouldFit() throws Exception {
        exec("CREATE TABLE zzt4d_w3 (a numeric(4,1)[])");
        exec("INSERT INTO zzt4d_w3 VALUES ('{123.45}')");
        assertEquals("{123.5}", scalar("SELECT a::text FROM zzt4d_w3"));
        // The overflow is the element type's own, so it carries the DETAIL numeric(4,1) gives.
        String sql = "INSERT INTO zzt4d_w3 VALUES ('{12345.4}')";
        assertEquals("22003: numeric field overflow", errorOf(sql));
        assertEquals("A field with precision 4, scale 1 must round to an absolute value"
                + " less than 10^3.", detailOf(sql));
        assertEquals("22003", stateOf("UPDATE zzt4d_w3 SET a = '{12345.4}'"));
        assertEquals("{123.5}", scalar("SELECT a::text FROM zzt4d_w3"));
        exec("DROP TABLE zzt4d_w3");
    }

    @Test
    void anElementOfADomainCarriesTheWidthAndTheCheckOfItsBase() throws Exception {
        exec("CREATE DOMAIN zzt4d_dv AS varchar(4)");
        exec("CREATE DOMAIN zzt4d_dp2 AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE zzt4d_w4 (a zzt4d_dv[])");
        exec("CREATE TABLE zzt4d_w5 (a zzt4d_dp2[])");
        // A domain is its base type plus its constraints: an element declared with it is held to
        // the base's width, and the base's width is what the error names.
        assertEquals("22001: value too long for type character varying(4)",
                errorOf("INSERT INTO zzt4d_w4 VALUES ('{abcdefg}')"));
        exec("INSERT INTO zzt4d_w4 VALUES ('{abcd}')");
        assertEquals("{abcd}", scalar("SELECT a::text FROM zzt4d_w4"));
        // and to the domain's own constraint, which the error names as the domain's
        org.postgresql.util.ServerErrorMessage m =
                fieldsOf("INSERT INTO zzt4d_w5 VALUES ('{-1}')");
        assertEquals("23514", m.getSQLState());
        assertEquals("value for domain zzt4d_dp2 violates check constraint \"zzt4d_dp2_check\"",
                m.getMessage());
        assertEquals("zzt4d_dp2_check", m.getConstraint());
        assertEquals("zzt4d_dp2", m.getDatatype());
        exec("INSERT INTO zzt4d_w5 VALUES ('{5}')");
        assertEquals("23514", stateOf("UPDATE zzt4d_w5 SET a = '{-3}'"));
        assertEquals("{5}", scalar("SELECT a::text FROM zzt4d_w5"));
        exec("DROP TABLE zzt4d_w4");
        exec("DROP TABLE zzt4d_w5");
        exec("DROP DOMAIN zzt4d_dv");
        exec("DROP DOMAIN zzt4d_dp2");
    }

    @Test
    void anArrayColumnThatDeclaredNoModifierIsNotHeldToOne() throws Exception {
        exec("CREATE TABLE zzt4d_w6 (a text[], b varchar[])");
        // The element width is consulted only where the column declared one; text[] and a bare
        // varchar[] declare none, and hold elements of any length.
        exec("INSERT INTO zzt4d_w6 VALUES ('{abcdefg}', '{abcdefg}')");
        assertEquals("{abcdefg}|{abcdefg}", rows("SELECT a::text, b::text FROM zzt4d_w6"));
        exec("DROP TABLE zzt4d_w6");
    }

    // ============================================================ what a column of an array is

    @Test
    void pgTypeofReadsTheColumnsOwnTypeAndNotItsElement() throws Exception {
        createTypedColumns();
        // A column of an array over a written type, and a column of a domain built over an array,
        // record the same pair of facts. Only the column's own type tells them apart.
        assertEquals("zzt4d_pdm[]|zzt4d_pen[]|zzt4d_pcp[]|zzt4d_prg[]|zzt4d_pda",
                rows("SELECT pg_typeof(a)::text, pg_typeof(b)::text, pg_typeof(c)::text,"
                        + " pg_typeof(r)::text, pg_typeof(d)::text FROM zzt4d_pt"));
        dropTypedColumns();
    }

    @Test
    void informationSchemaPublishesAnArrayOfAWrittenTypeAsThatTypesArrayType() throws Exception {
        createTypedColumns();
        // Every one of these is an ARRAY, and the underlying type it names is the array type of
        // the element -- not the element itself, and not int4's array because a domain was
        // involved somewhere. The domain over int[] is the one column whose element really is an
        // int4, because that is what the domain is over.
        assertEquals("a|ARRAY|public|_zzt4d_pdm"
                        + ";b|ARRAY|public|_zzt4d_pen"
                        + ";c|ARRAY|public|_zzt4d_pcp"
                        + ";r|ARRAY|public|_zzt4d_prg"
                        + ";d|ARRAY|pg_catalog|_int4",
                rows("SELECT column_name, data_type, udt_schema, udt_name"
                        + " FROM information_schema.columns WHERE table_name = 'zzt4d_pt'"
                        + " ORDER BY ordinal_position"));
        dropTypedColumns();
    }

    @Test
    void pgAttributeGivesAnArrayOfAnEnumTheLayoutOfAnArray() throws Exception {
        createTypedColumns();
        // An array is varlena however small the type its elements are: -1/x/f, never the enum's
        // own 4/p/t. The column of the domain over an array points at the domain, and is varlena
        // for the same reason.
        assertEquals("a|zzt4d_pdm[]|-1|x|f"
                        + ";b|zzt4d_pen[]|-1|x|f"
                        + ";c|zzt4d_pcp[]|-1|x|f"
                        + ";r|zzt4d_prg[]|-1|x|f"
                        + ";d|zzt4d_pda|-1|x|f",
                rows("SELECT attname, atttypid::regtype::text, attlen, attstorage::text, attbyval"
                        + " FROM pg_attribute WHERE attrelid = 'zzt4d_pt'::regclass"
                        + " AND attnum > 0 ORDER BY attnum"));
        // and format_type prints the column's own type either way
        assertEquals("zzt4d_pdm[]", scalar("SELECT format_type(atttypid, NULL) FROM pg_attribute"
                + " WHERE attrelid = 'zzt4d_pt'::regclass AND attname = 'a'"));
        assertEquals("zzt4d_pda", scalar("SELECT format_type(atttypid, NULL) FROM pg_attribute"
                + " WHERE attrelid = 'zzt4d_pt'::regclass AND attname = 'd'"));
        dropTypedColumns();
    }

    @Test
    void subscriptingAnArrayOfAWrittenTypeAnswersThatWrittenType() throws Exception {
        createTypedColumns();
        // One element of an array of a domain is of the domain, one of an array of an enum is of
        // the enum, and a slice of either is still the array. One element of the domain over
        // int[] is an integer, because the domain is over an array of integers.
        assertEquals("zzt4d_pdm|zzt4d_pen|zzt4d_pcp|zzt4d_prg|zzt4d_pdm[]|integer",
                rows("SELECT pg_typeof(a[1])::text, pg_typeof(b[1])::text, pg_typeof(c[1])::text,"
                        + " pg_typeof(r[1])::text, pg_typeof(a[1:2])::text, pg_typeof(d[1])::text"
                        + " FROM zzt4d_pt"));
        assertEquals("{1,2}|{a,b}|{7,8}|1|a|7",
                rows("SELECT a::text, b::text, d::text, a[1]::text, b[1]::text, d[1]::text"
                        + " FROM zzt4d_pt"));
        dropTypedColumns();
    }

    private static void createTypedColumns() throws SQLException {
        exec("CREATE DOMAIN zzt4d_pdm AS int");
        exec("CREATE TYPE zzt4d_pen AS ENUM ('a','b')");
        exec("CREATE TYPE zzt4d_pcp AS (x int)");
        exec("CREATE TYPE zzt4d_prg AS RANGE (subtype = int4)");
        exec("CREATE DOMAIN zzt4d_pda AS int[]");
        exec("CREATE TABLE zzt4d_pt (a zzt4d_pdm[], b zzt4d_pen[], c zzt4d_pcp[],"
                + " r zzt4d_prg[], d zzt4d_pda)");
        exec("INSERT INTO zzt4d_pt VALUES ('{1,2}', '{a,b}', ARRAY[ROW(1)::zzt4d_pcp],"
                + " ARRAY['[1,3)'::zzt4d_prg], '{7,8}')");
    }

    private static void dropTypedColumns() throws SQLException {
        exec("DROP TABLE zzt4d_pt");
        exec("DROP DOMAIN zzt4d_pda");
        exec("DROP DOMAIN zzt4d_pdm");
        exec("DROP TYPE zzt4d_pen");
        exec("DROP TYPE zzt4d_pcp");
        exec("DROP TYPE zzt4d_prg");
    }

    // ============================================================ what a type owes its dependents

    @Test
    void droppingATypeAnAttributeHoldsNamesTheAttributeInItsDetail() throws Exception {
        exec("CREATE TYPE zzt4d_e1 AS ENUM ('a','b')");
        exec("CREATE TYPE zzt4d_c1 AS (x zzt4d_e1, y int)");
        // An attribute of a composite type holds its type as firmly as a table column does: the
        // drop is refused, says which attribute of which composite stands in the way, and says
        // what to do about it.
        assertEquals("2BP01: cannot drop type zzt4d_e1 because other objects depend on it",
                errorOf("DROP TYPE zzt4d_e1"));
        assertEquals("column x of composite type zzt4d_c1 depends on type zzt4d_e1",
                detailOf("DROP TYPE zzt4d_e1"));
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.",
                hintOf("DROP TYPE zzt4d_e1"));
        // RESTRICT is the default, and reads the same
        assertEquals("column x of composite type zzt4d_c1 depends on type zzt4d_e1",
                detailOf("DROP TYPE zzt4d_e1 RESTRICT"));
        // a refused drop takes nothing
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_type WHERE typname = 'zzt4d_e1'"));
        exec("DROP TYPE zzt4d_c1");
        exec("DROP TYPE zzt4d_e1");
    }

    @Test
    void everyDependentAttributeIsNamedAndTheOneHoldingAnArrayComesFirst() throws Exception {
        exec("CREATE TYPE zzt4d_e2 AS ENUM ('a','b')");
        exec("CREATE TYPE zzt4d_c2 AS (x zzt4d_e2, y zzt4d_e2)");
        exec("CREATE TYPE zzt4d_cb AS (z zzt4d_e2[])");
        // Every dependent is named on a line of its own, and the order is PostgreSQL's: the
        // attribute declared as an array of the type ahead of the rest, and the attributes of one
        // composite from the last back to the first. An attribute holding an array says so.
        assertEquals(String.join("\n",
                        "column z of composite type zzt4d_cb depends on type zzt4d_e2[]",
                        "column y of composite type zzt4d_c2 depends on type zzt4d_e2",
                        "column x of composite type zzt4d_c2 depends on type zzt4d_e2"),
                detailOf("DROP TYPE zzt4d_e2"));
        exec("DROP TYPE zzt4d_e2 CASCADE");
        exec("DROP TYPE zzt4d_c2");
        exec("DROP TYPE zzt4d_cb");
    }

    @Test
    void cascadeSaysWhatItTookAndLeavesTheCompositeStanding() throws Exception {
        exec("CREATE TYPE zzt4d_e3 AS ENUM ('a','b')");
        exec("CREATE TYPE zzt4d_c3 AS (x zzt4d_e3, y int)");
        // CASCADE takes the attribute rather than the composite that holds it, and says so.
        String[] notice = noticeOf("DROP TYPE zzt4d_e3 CASCADE");
        assertEquals("drop cascades to column x of composite type zzt4d_c3", notice[0]);
        assertNull(notice[1]);
        assertEquals("c", scalar("SELECT typtype::text FROM pg_type WHERE typname = 'zzt4d_c3'"));
        // the attribute it lost keeps its number, so the attribute after it keeps its own
        assertEquals("........pg.dropped.1......../1/true,y/2/false",
                scalar("SELECT string_agg(attname || '/' || attnum::text || '/'"
                        + " || attisdropped::text, ',' ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = (SELECT typrelid FROM pg_type"
                        + " WHERE typname = 'zzt4d_c3') AND attnum > 0"));
        exec("ALTER TYPE zzt4d_c3 ADD ATTRIBUTE z int");
        assertEquals("........pg.dropped.1......../1/true,y/2/false,z/3/false",
                scalar("SELECT string_agg(attname || '/' || attnum::text || '/'"
                        + " || attisdropped::text, ',' ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = (SELECT typrelid FROM pg_type"
                        + " WHERE typname = 'zzt4d_c3') AND attnum > 0"));
        exec("DROP TYPE zzt4d_c3");
    }

    @Test
    void cascadeOverSeveralDependentsCountsThemAndNamesThemInItsDetail() throws Exception {
        exec("CREATE TYPE zzt4d_e4 AS ENUM ('a','b')");
        exec("CREATE TYPE zzt4d_c4 AS (x zzt4d_e4, y int)");
        exec("CREATE TABLE zzt4d_t4 (c zzt4d_e4, d int)");
        // With more than one dependent the notice counts them and puts the list in its DETAIL,
        // in the same order the refusal would have named them.
        String[] notice = noticeOf("DROP TYPE zzt4d_e4 CASCADE");
        assertEquals("drop cascades to 2 other objects", notice[0]);
        assertEquals(String.join("\n",
                        "drop cascades to column x of composite type zzt4d_c4",
                        "drop cascades to column c of table zzt4d_t4"),
                notice[1]);
        // the relation stands, holding the columns the type did not reach
        assertEquals("d", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name = 'zzt4d_t4'"));
        assertEquals("........pg.dropped.1......../1/true,d/2/false",
                scalar("SELECT string_agg(attname || '/' || attnum::text || '/'"
                        + " || attisdropped::text, ',' ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = 'zzt4d_t4'::regclass AND attnum > 0"));
        exec("DROP TABLE zzt4d_t4");
        exec("DROP TYPE zzt4d_c4");
    }

    @Test
    void droppingADomainUnderADomainNamesTheDomainThatDependsOnIt() throws Exception {
        exec("CREATE DOMAIN zzt4d_dd1 AS int");
        exec("CREATE DOMAIN zzt4d_dd2 AS zzt4d_dd1");
        // A domain written over another type depends on it, and a domain is a type, so the drop
        // reads the same written either way.
        assertEquals("2BP01: cannot drop type zzt4d_dd1 because other objects depend on it",
                errorOf("DROP DOMAIN zzt4d_dd1"));
        assertEquals("type zzt4d_dd2 depends on type zzt4d_dd1", detailOf("DROP DOMAIN zzt4d_dd1"));
        assertEquals("type zzt4d_dd2 depends on type zzt4d_dd1", detailOf("DROP TYPE zzt4d_dd1"));
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.",
                hintOf("DROP DOMAIN zzt4d_dd1 RESTRICT"));
        assertEquals("1", scalar("SELECT 1::zzt4d_dd2::text"));
        // CASCADE takes the dependent domain and says which
        String[] notice = noticeOf("DROP DOMAIN zzt4d_dd1 CASCADE");
        assertEquals("drop cascades to type zzt4d_dd2", notice[0]);
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_type"
                + " WHERE typname IN ('zzt4d_dd1','zzt4d_dd2')"));
    }

    @Test
    void theRefusalReachesThroughTheChainDepthFirst() throws Exception {
        exec("CREATE DOMAIN zzt4d_ed1 AS int");
        exec("CREATE DOMAIN zzt4d_ed2 AS zzt4d_ed1");
        exec("CREATE TABLE zzt4d_ta (c zzt4d_ed2, k int)");
        exec("CREATE DOMAIN zzt4d_ed3 AS zzt4d_ed2");
        exec("CREATE TABLE zzt4d_tb (c zzt4d_ed3, k int)");
        // Nothing written in terms of a dependent is missed: each line names the type it directly
        // depends on, and everything written over a dependent is listed under it before the walk
        // returns to what is left of the type being dropped.
        assertEquals(String.join("\n",
                        "type zzt4d_ed2 depends on type zzt4d_ed1",
                        "column c of table zzt4d_ta depends on type zzt4d_ed2",
                        "type zzt4d_ed3 depends on type zzt4d_ed2",
                        "column c of table zzt4d_tb depends on type zzt4d_ed3"),
                detailOf("DROP DOMAIN zzt4d_ed1"));
        // and CASCADE takes all of it, leaving both relations standing without those columns
        String[] notice = noticeOf("DROP DOMAIN zzt4d_ed1 CASCADE");
        assertEquals("drop cascades to 4 other objects", notice[0]);
        assertEquals(String.join("\n",
                        "drop cascades to type zzt4d_ed2",
                        "drop cascades to column c of table zzt4d_ta",
                        "drop cascades to type zzt4d_ed3",
                        "drop cascades to column c of table zzt4d_tb"),
                notice[1]);
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_type"
                + " WHERE typname IN ('zzt4d_ed1','zzt4d_ed2','zzt4d_ed3')"));
        assertEquals("k;k", rows("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name IN ('zzt4d_ta','zzt4d_tb')"
                + " GROUP BY table_name ORDER BY table_name"));
        exec("DROP TABLE zzt4d_ta");
        exec("DROP TABLE zzt4d_tb");
    }

    @Test
    void aDomainOverACompositeAnEnumOrAnArrayNamesWhatItDependsOn() throws Exception {
        exec("CREATE TYPE zzt4d_fc AS (x int)");
        exec("CREATE DOMAIN zzt4d_fdc AS zzt4d_fc");
        exec("CREATE TYPE zzt4d_fe AS ENUM ('a','b')");
        exec("CREATE DOMAIN zzt4d_fde AS zzt4d_fe");
        exec("CREATE DOMAIN zzt4d_fb1 AS int");
        exec("CREATE DOMAIN zzt4d_fb2 AS zzt4d_fb1[]");
        // Whatever kind of type a domain was written over, the refusal names the domain as the
        // dependent and the type as what it depends on -- and a domain written over an array of a
        // type says so, because that is the type it holds.
        assertEquals("type zzt4d_fdc depends on type zzt4d_fc", detailOf("DROP TYPE zzt4d_fc"));
        assertEquals("type zzt4d_fde depends on type zzt4d_fe", detailOf("DROP TYPE zzt4d_fe"));
        assertEquals("type zzt4d_fb2 depends on type zzt4d_fb1[]",
                detailOf("DROP DOMAIN zzt4d_fb1"));
        assertEquals("drop cascades to type zzt4d_fdc",
                noticeOf("DROP TYPE zzt4d_fc CASCADE")[0]);
        assertEquals("drop cascades to type zzt4d_fde",
                noticeOf("DROP TYPE zzt4d_fe CASCADE")[0]);
        assertEquals("drop cascades to type zzt4d_fb2",
                noticeOf("DROP DOMAIN zzt4d_fb1 CASCADE")[0]);
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_type"
                + " WHERE typname IN ('zzt4d_fdc','zzt4d_fde','zzt4d_fb2')"));
    }

    @Test
    void droppingASchemaNamesWhatWasWrittenOverItsTypesFromOutside() throws Exception {
        exec("CREATE SCHEMA zzt4d_s");
        exec("CREATE TYPE zzt4d_s.zzt4d_se AS ENUM ('a','b')");
        exec("CREATE DOMAIN public.zzt4d_sd AS zzt4d_s.zzt4d_se");
        exec("CREATE TABLE public.zzt4d_st (c zzt4d_s.zzt4d_se, k int)");
        // A schema is refused for what its own objects hold up, and the list reaches outside the
        // schema: a domain and a column elsewhere that were written over a type inside it.
        assertEquals("2BP01: cannot drop schema zzt4d_s because other objects depend on it",
                errorOf("DROP SCHEMA zzt4d_s"));
        assertEquals(String.join("\n",
                        "type zzt4d_s.zzt4d_se depends on schema zzt4d_s",
                        "type zzt4d_sd depends on type zzt4d_s.zzt4d_se",
                        "column c of table zzt4d_st depends on type zzt4d_s.zzt4d_se"),
                detailOf("DROP SCHEMA zzt4d_s"));
        String[] notice = noticeOf("DROP SCHEMA zzt4d_s CASCADE");
        assertEquals("drop cascades to 3 other objects", notice[0]);
        assertEquals(String.join("\n",
                        "drop cascades to type zzt4d_s.zzt4d_se",
                        "drop cascades to type zzt4d_sd",
                        "drop cascades to column c of table zzt4d_st"),
                notice[1]);
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_type WHERE typname = 'zzt4d_sd'"));
        assertEquals("k", scalar("SELECT string_agg(column_name, ',' ORDER BY ordinal_position)"
                + " FROM information_schema.columns WHERE table_name = 'zzt4d_st'"));
        exec("DROP TABLE zzt4d_st");
    }

    @Test
    void aCascadeRolledBackGivesTheAttributeAndTheDomainBack() throws Exception {
        exec("CREATE TYPE zzt4d_re AS ENUM ('a','b')");
        exec("CREATE TYPE zzt4d_rc AS (x zzt4d_re, y int)");
        exec("CREATE DOMAIN zzt4d_rd AS zzt4d_re");
        exec("CREATE TABLE zzt4d_rt (c zzt4d_re, d int)");
        exec("BEGIN");
        exec("DROP TYPE zzt4d_re CASCADE");
        // inside the transaction the type, the domain, the attribute and the column are all gone
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_type"
                + " WHERE typname IN ('zzt4d_re','zzt4d_rd')"));
        assertEquals("........pg.dropped.1......../1/true,y/2/false",
                scalar("SELECT string_agg(attname || '/' || attnum::text || '/'"
                        + " || attisdropped::text, ',' ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = (SELECT typrelid FROM pg_type"
                        + " WHERE typname = 'zzt4d_rc') AND attnum > 0"));
        assertEquals("1", scalar("SELECT count(*)::text FROM information_schema.columns"
                + " WHERE table_name = 'zzt4d_rt'"));
        exec("ROLLBACK");
        // and every one of them is back, under the number it had
        assertEquals("2", scalar("SELECT count(*)::text FROM pg_type"
                + " WHERE typname IN ('zzt4d_re','zzt4d_rd')"));
        assertEquals("x/1/false,y/2/false",
                scalar("SELECT string_agg(attname || '/' || attnum::text || '/'"
                        + " || attisdropped::text, ',' ORDER BY attnum) FROM pg_attribute"
                        + " WHERE attrelid = (SELECT typrelid FROM pg_type"
                        + " WHERE typname = 'zzt4d_rc') AND attnum > 0"));
        assertEquals("2", scalar("SELECT count(*)::text FROM information_schema.columns"
                + " WHERE table_name = 'zzt4d_rt'"));
        exec("DROP TYPE zzt4d_re CASCADE");
        exec("DROP TABLE zzt4d_rt");
        exec("DROP TYPE zzt4d_rc");
    }
}
