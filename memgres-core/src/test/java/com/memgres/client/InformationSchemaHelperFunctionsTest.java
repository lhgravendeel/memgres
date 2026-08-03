package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The helper functions {@code information_schema}'s own views are written in terms of.
 *
 * <p>PostgreSQL declares eleven of them — {@code _pg_char_max_length},
 * {@code _pg_char_octet_length}, {@code _pg_numeric_precision},
 * {@code _pg_numeric_precision_radix}, {@code _pg_numeric_scale},
 * {@code _pg_datetime_precision}, {@code _pg_interval_type}, {@code _pg_truetypid},
 * {@code _pg_truetypmod}, {@code _pg_index_position} and {@code _pg_expandarray} — in the
 * {@code information_schema} namespace. memgres computes the view columns natively rather than
 * by composing these, so the views agreed already; the functions themselves did not exist, and
 * every direct call an ORM or a schema browser makes was a 42883.
 *
 * <p>Every expectation below was measured on PostgreSQL 18.0, not read off memgres. The typmod
 * constants are the ones a real {@code pg_attribute} carries: varchar(10)=14, char(5)=9,
 * numeric(10,2)=655366, timestamp(3)=3, interval year=327679, interval day to second(3)=470286339.
 *
 * <p>{@code pg_logical_emit_message} is here for the same reason: it is declared in both a text
 * and a bytea overload and answers in {@code pg_lsn}.
 */
class InformationSchemaHelperFunctionsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ishf_cols (v varchar(10), n numeric(10,2), t text,"
                    + " ts timestamp(3), iv interval year, b int NOT NULL, c int NOT NULL)");
            s.execute("CREATE INDEX ishf_cols_i ON ishf_cols (t, n)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "one row for " + sql);
            String v = rs.getString(1);
            return rs.wasNull() ? null : v;
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append("|");
                    String v = rs.getString(i);
                    sb.append(rs.wasNull() ? "NULL" : v);
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    private static SQLException refusal(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        } catch (SQLException e) {
            return e;
        }
        return fail("expected a refusal from: " + sql);
    }

    // ------------------------------------------------------------------
    // _pg_char_max_length and _pg_char_octet_length
    // ------------------------------------------------------------------

    @Test
    void charMaxLengthSubtractsTheFourAVarcharTypmodCarries() throws Exception {
        assertEquals("10", scalar("SELECT information_schema._pg_char_max_length('varchar'::regtype::oid, 14)"));
        assertEquals("5", scalar("SELECT information_schema._pg_char_max_length('bpchar'::regtype::oid, 9)"));
    }

    @Test
    void aBitStringsTypmodIsItsLengthAlready() throws Exception {
        assertEquals("3", scalar("SELECT information_schema._pg_char_max_length('bit'::regtype::oid, 3)"));
        assertEquals("8", scalar("SELECT information_schema._pg_char_max_length('varbit'::regtype::oid, 8)"));
    }

    @Test
    void aTypeWithNoDeclaredLengthHasNoMaximumLength() throws Exception {
        assertNull(scalar("SELECT information_schema._pg_char_max_length('text'::regtype::oid, -1)"));
        assertNull(scalar("SELECT information_schema._pg_char_max_length('int4'::regtype::oid, -1)"));
        assertNull(scalar("SELECT information_schema._pg_char_max_length('varchar'::regtype::oid, -1)"));
    }

    @Test
    void theHelpersAreStrict() throws Exception {
        assertNull(scalar("SELECT information_schema._pg_char_max_length(NULL::oid, 14)"));
        assertNull(scalar("SELECT information_schema._pg_char_max_length('varchar'::regtype::oid, NULL)"));
        assertNull(scalar("SELECT information_schema._pg_numeric_scale(NULL::oid, NULL)"));
        assertNull(scalar("SELECT information_schema._pg_interval_type(NULL::oid, 327679)"));
        assertNull(scalar("SELECT information_schema._pg_datetime_precision('date'::regtype::oid, NULL)"));
    }

    @Test
    void octetLengthIsFourBytesACharacterUnderUtf8() throws Exception {
        assertEquals("40", scalar("SELECT information_schema._pg_char_octet_length('varchar'::regtype::oid, 14)"));
        assertEquals("20", scalar("SELECT information_schema._pg_char_octet_length('bpchar'::regtype::oid, 9)"));
    }

    @Test
    void anUnboundedStringIsReportedAsTwoToTheThirty() throws Exception {
        assertEquals("1073741824",
                scalar("SELECT information_schema._pg_char_octet_length('text'::regtype::oid, -1)"));
    }

    @Test
    void onlyAStringTypeHasAnOctetLength() throws Exception {
        assertNull(scalar("SELECT information_schema._pg_char_octet_length('bit'::regtype::oid, 3)"));
        assertNull(scalar("SELECT information_schema._pg_char_octet_length('int4'::regtype::oid, -1)"));
    }

    // ------------------------------------------------------------------
    // the numeric trio
    // ------------------------------------------------------------------

    @Test
    void theIntegerAndFloatPrecisionsAreConstants() throws Exception {
        assertEquals("16", scalar("SELECT information_schema._pg_numeric_precision('int2'::regtype::oid, -1)"));
        assertEquals("32", scalar("SELECT information_schema._pg_numeric_precision('int4'::regtype::oid, -1)"));
        assertEquals("64", scalar("SELECT information_schema._pg_numeric_precision('int8'::regtype::oid, -1)"));
        assertEquals("24", scalar("SELECT information_schema._pg_numeric_precision('float4'::regtype::oid, -1)"));
        assertEquals("53", scalar("SELECT information_schema._pg_numeric_precision('float8'::regtype::oid, -1)"));
    }

    @Test
    void numericPacksPrecisionAndScaleIntoOneTypmod() throws Exception {
        assertEquals("10", scalar("SELECT information_schema._pg_numeric_precision('numeric'::regtype::oid, 655366)"));
        assertEquals("2", scalar("SELECT information_schema._pg_numeric_scale('numeric'::regtype::oid, 655366)"));
        assertNull(scalar("SELECT information_schema._pg_numeric_precision('numeric'::regtype::oid, -1)"));
        assertNull(scalar("SELECT information_schema._pg_numeric_scale('numeric'::regtype::oid, -1)"));
    }

    @Test
    void aTypeThatIsNotANumberHasNoPrecision() throws Exception {
        assertNull(scalar("SELECT information_schema._pg_numeric_precision('text'::regtype::oid, -1)"));
        assertNull(scalar("SELECT information_schema._pg_numeric_precision_radix('text'::regtype::oid, -1)"));
        assertNull(scalar("SELECT information_schema._pg_numeric_scale('text'::regtype::oid, -1)"));
    }

    @Test
    void theRadixIsBinaryForTheMachineTypesAndTenForNumeric() throws Exception {
        assertEquals("2", scalar("SELECT information_schema._pg_numeric_precision_radix('int4'::regtype::oid, -1)"));
        assertEquals("2", scalar("SELECT information_schema._pg_numeric_precision_radix('float8'::regtype::oid, -1)"));
        assertEquals("10", scalar("SELECT information_schema._pg_numeric_precision_radix('numeric'::regtype::oid, -1)"));
    }

    @Test
    void anIntegerHasScaleZeroAndAFloatHasNoScaleAtAll() throws Exception {
        assertEquals("0", scalar("SELECT information_schema._pg_numeric_scale('int4'::regtype::oid, -1)"));
        assertNull(scalar("SELECT information_schema._pg_numeric_scale('float8'::regtype::oid, -1)"));
    }

    // ------------------------------------------------------------------
    // the date/time pair
    // ------------------------------------------------------------------

    @Test
    void datetimePrecisionIsSixWhereNoneWasWritten() throws Exception {
        assertEquals("0", scalar("SELECT information_schema._pg_datetime_precision('date'::regtype::oid, -1)"));
        assertEquals("6", scalar("SELECT information_schema._pg_datetime_precision('timestamp'::regtype::oid, -1)"));
        assertEquals("3", scalar("SELECT information_schema._pg_datetime_precision('timestamp'::regtype::oid, 3)"));
        assertEquals("4", scalar("SELECT information_schema._pg_datetime_precision('timestamptz'::regtype::oid, 4)"));
        assertEquals("6", scalar("SELECT information_schema._pg_datetime_precision('time'::regtype::oid, -1)"));
        assertEquals("2", scalar("SELECT information_schema._pg_datetime_precision('timetz'::regtype::oid, 2)"));
        assertNull(scalar("SELECT information_schema._pg_datetime_precision('int4'::regtype::oid, -1)"));
    }

    @Test
    void anIntervalTypmodCarriesAFieldMaskAboveItsPrecision() throws Exception {
        // interval year: mask 4, precision 0xFFFF meaning none written
        assertEquals("6", scalar("SELECT information_schema._pg_datetime_precision('interval'::regtype::oid, 327679)"));
        // interval day to second(3)
        assertEquals("3", scalar("SELECT information_schema._pg_datetime_precision('interval'::regtype::oid, 470286339)"));
        assertEquals("6", scalar("SELECT information_schema._pg_datetime_precision('interval'::regtype::oid, -1)"));
    }

    @Test
    void intervalTypeIsTheQualifierListUpperCased() throws Exception {
        assertEquals("YEAR", scalar("SELECT information_schema._pg_interval_type('interval'::regtype::oid, 327679)"));
        assertEquals("DAY TO SECOND(3)",
                scalar("SELECT information_schema._pg_interval_type('interval'::regtype::oid, 470286339)"));
    }

    /**
     * A plain interval has no qualifier, and the answer is NULL rather than the empty string:
     * PostgreSQL reads the qualifier out of format_type's rendering and there is nothing after
     * the type name to read.
     */
    @Test
    void anUnqualifiedIntervalHasNoIntervalType() throws Exception {
        assertNull(scalar("SELECT information_schema._pg_interval_type('interval'::regtype::oid, -1)"));
        assertNull(scalar("SELECT information_schema._pg_interval_type('int4'::regtype::oid, -1)"));
    }

    // ------------------------------------------------------------------
    // _pg_truetypid / _pg_truetypmod over whole catalog rows
    // ------------------------------------------------------------------

    @Test
    void trueTypeReadsTheAttributeAndItsTypeAsWholeRows() throws Exception {
        assertEquals(Arrays.asList(
                        "v|1043|14", "n|1700|655366", "t|25|-1", "ts|1114|3",
                        "iv|1186|327679", "b|23|-1", "c|23|-1"),
                rows("SELECT a.attname, information_schema._pg_truetypid(a.*, t.*),"
                        + " information_schema._pg_truetypmod(a.*, t.*)"
                        + " FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid"
                        + " WHERE a.attrelid = 'ishf_cols'::regclass AND a.attnum > 0"
                        + " ORDER BY a.attnum"));
    }

    @Test
    void theHelpersComposeTheWayTheViewsComposeThem() throws Exception {
        assertEquals(Arrays.asList(
                        "v|10|NULL|NULL|NULL|NULL",
                        "n|NULL|10|2|NULL|NULL",
                        "t|NULL|NULL|NULL|NULL|NULL",
                        "ts|NULL|NULL|NULL|3|NULL",
                        "iv|NULL|NULL|NULL|6|YEAR",
                        "b|NULL|32|0|NULL|NULL",
                        "c|NULL|32|0|NULL|NULL"),
                rows("SELECT a.attname,"
                        + " information_schema._pg_char_max_length(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)),"
                        + " information_schema._pg_numeric_precision(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)),"
                        + " information_schema._pg_numeric_scale(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)),"
                        + " information_schema._pg_datetime_precision(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*)),"
                        + " information_schema._pg_interval_type(information_schema._pg_truetypid(a.*, t.*), information_schema._pg_truetypmod(a.*, t.*))"
                        + " FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid"
                        + " WHERE a.attrelid = 'ishf_cols'::regclass AND a.attnum > 0"
                        + " ORDER BY a.attnum"));
    }

    /** The views these exist for must still say the same thing they said before. */
    @Test
    void informationSchemaColumnsStillAgrees() throws Exception {
        assertEquals(Arrays.asList(
                        "v|10|40|NULL|NULL|NULL|NULL|NULL",
                        "n|NULL|NULL|10|10|2|NULL|NULL",
                        "t|NULL|1073741824|NULL|NULL|NULL|NULL|NULL",
                        "ts|NULL|NULL|NULL|NULL|NULL|3|NULL",
                        "iv|NULL|NULL|NULL|NULL|NULL|6|YEAR",
                        "b|NULL|NULL|32|2|0|NULL|NULL",
                        "c|NULL|NULL|32|2|0|NULL|NULL"),
                rows("SELECT column_name, character_maximum_length, character_octet_length,"
                        + " numeric_precision, numeric_precision_radix, numeric_scale,"
                        + " datetime_precision, interval_type FROM information_schema.columns"
                        + " WHERE table_name = 'ishf_cols' AND table_schema = 'public'"
                        + " ORDER BY ordinal_position"));
    }

    // ------------------------------------------------------------------
    // _pg_index_position
    // ------------------------------------------------------------------

    @Test
    void indexPositionCountsFromOneAlongTheKeyList() throws Exception {
        // attnum 3 is t, the first key of the index; attnum 2 is n, the second
        assertEquals("1", scalar("SELECT information_schema._pg_index_position('ishf_cols_i'::regclass::oid, 3::smallint)"));
        assertEquals("2", scalar("SELECT information_schema._pg_index_position('ishf_cols_i'::regclass::oid, 2::smallint)"));
    }

    @Test
    void anAttributeTheIndexDoesNotCoverHasNoPosition() throws Exception {
        assertNull(scalar("SELECT information_schema._pg_index_position('ishf_cols_i'::regclass::oid, 1::smallint)"));
        assertNull(scalar("SELECT information_schema._pg_index_position(0::oid, 1::smallint)"));
        assertNull(scalar("SELECT information_schema._pg_index_position(NULL::oid, 1::smallint)"));
    }

    // ------------------------------------------------------------------
    // where they may stand, and where they may not be named
    // ------------------------------------------------------------------

    @Test
    void aHelperStandsWhereverAnExpressionMay() throws Exception {
        assertEquals("10", scalar("WITH c AS (SELECT information_schema._pg_char_max_length("
                + "'varchar'::regtype::oid, 14) AS x) SELECT x FROM c"));
        assertEquals("2", scalar("SELECT * FROM (SELECT information_schema._pg_numeric_scale("
                + "'numeric'::regtype::oid, 655366) AS s) q WHERE q.s = 2"));
        assertEquals("1", scalar("SELECT 1 WHERE information_schema._pg_numeric_precision("
                + "'int4'::regtype::oid, -1) = 32"));
        assertEquals("32", scalar("SELECT (SELECT information_schema._pg_numeric_precision("
                + "'int4'::regtype::oid, -1))"));
    }

    @Test
    void aHelperStandsInsideAView() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE OR REPLACE VIEW ishf_v AS SELECT"
                    + " information_schema._pg_numeric_precision('int4'::regtype::oid, -1) AS p,"
                    + " information_schema._pg_char_max_length('varchar'::regtype::oid, 14) AS l");
        }
        assertEquals(Arrays.asList("32|10"), rows("SELECT p, l FROM ishf_v"));
    }

    @Test
    void aHelperStandsInAPreparedStatement() throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT information_schema._pg_char_max_length('varchar'::regtype::oid, ?)")) {
            ps.setInt(1, 14);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("10", rs.getString(1));
            }
        }
    }

    /** They are declared in information_schema, and pg_catalog does not hold them. */
    @Test
    void aPgCatalogQualifierNamesASchemaTheyAreNotIn() throws Exception {
        SQLException e = refusal("SELECT pg_catalog._pg_char_max_length('varchar'::regtype::oid, 14)");
        assertEquals("42883", e.getSQLState());
        assertTrue(e.getMessage().contains("pg_catalog._pg_char_max_length"), e.getMessage());

        SQLException other = refusal("SELECT pg_catalog._pg_numeric_precision('int4'::regtype::oid, -1)");
        assertEquals("42883", other.getSQLState());
    }

    /**
     * With information_schema on the search path the unqualified spelling resolves.
     *
     * <p>memgres answers it whatever the path says, where PostgreSQL refuses it under the default
     * one. Refusing it here refused a view created legitimately while the schema was on the path,
     * because memgres re-resolves a stored definition against the session's current path rather
     * than the one it was written under; refusing SQL that works is the worse of the two errors.
     */
    @Test
    void anUnqualifiedCallResolvesWhenInformationSchemaIsOnTheSearchPath() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("SET search_path = public, information_schema");
        }
        try {
            assertEquals("10", scalar("SELECT _pg_char_max_length('varchar'::regtype::oid, 14)"));
            assertEquals("32", scalar("SELECT _pg_numeric_precision('int4'::regtype::oid, -1)"));
            assertEquals("YEAR", scalar("SELECT _pg_interval_type('interval'::regtype::oid, 327679)"));
            assertEquals("1", scalar("SELECT _pg_index_position('ishf_cols_i'::regclass::oid, 3::smallint)"));
        } finally {
            try (Statement s = conn.createStatement()) {
                s.execute("RESET search_path");
            }
        }
    }

    /**
     * A view written while information_schema was on the path keeps answering after it comes off.
     * PostgreSQL resolves the name once, when the view is created; this is the case that decided
     * against refusing the unqualified spelling.
     */
    @Test
    void aViewWrittenWhileTheSchemaWasOnThePathStillAnswersAfterwards() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("SET search_path = public, information_schema");
            s.execute("CREATE OR REPLACE VIEW ishf_pv AS"
                    + " SELECT _pg_char_max_length('varchar'::regtype::oid, 14) AS l");
            s.execute("RESET search_path");
        }
        assertEquals("10", scalar("SELECT l FROM ishf_pv"));
    }

    @Test
    void aCallWithNoMatchingArgumentCountResolvesToNoFunction() throws Exception {
        for (String sql : new String[]{
                "SELECT information_schema._pg_char_max_length(1043)",
                "SELECT information_schema._pg_char_max_length(1043, 14, 1)",
                "SELECT information_schema._pg_truetypid(1)",
                "SELECT information_schema._pg_index_position('ishf_cols_i'::regclass::oid)"}) {
            SQLException e = refusal(sql);
            assertEquals("42883", e.getSQLState(), sql);
            assertTrue(e.getMessage().contains("does not exist"), sql + " -> " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // pg_logical_emit_message
    // ------------------------------------------------------------------

    @Test
    void logicalEmitMessageAnswersInPgLsnInBothOverloads() throws Exception {
        assertEquals("pg_lsn", scalar("SELECT pg_typeof(pg_logical_emit_message(true, 'ishf', 'hello'::text))::text"));
        assertEquals("pg_lsn", scalar("SELECT pg_typeof(pg_logical_emit_message(true, 'ishf', '\\x0102'::bytea))::text"));
        assertEquals("pg_lsn", scalar("SELECT pg_typeof(pg_logical_emit_message(true, 'ishf', 'hello'::text, false))::text"));
        assertEquals("pg_lsn", scalar("SELECT pg_typeof(pg_logical_emit_message(true, 'ishf', '\\x0102'::bytea, true))::text"));
        // An unknown-typed payload resolves to the text overload rather than being ambiguous
        assertEquals("pg_lsn", scalar("SELECT pg_typeof(pg_logical_emit_message(true, 'ishf', 'hello'))::text"));
    }

    @Test
    void logicalEmitMessageAnswersNonNullAndIsStrict() throws Exception {
        assertEquals("true", scalar("SELECT (pg_logical_emit_message(false, 'ishf', 'hello') IS NOT NULL)::text"));
        assertEquals("true", scalar("SELECT (pg_logical_emit_message(true, 'ishf', NULL::text) IS NULL)::text"));
        assertEquals("true", scalar("SELECT (pg_logical_emit_message(NULL, 'ishf', 'x') IS NULL)::text"));
    }

    @Test
    void logicalEmitMessageTakesThreeArgumentsOrFour() throws Exception {
        SQLException tooFew = refusal("SELECT pg_logical_emit_message(true, 'ishf')");
        assertEquals("42883", tooFew.getSQLState());
        assertTrue(tooFew.getMessage().contains("pg_logical_emit_message(boolean, unknown)"),
                tooFew.getMessage());
        SQLException tooMany = refusal("SELECT pg_logical_emit_message(true, 'ishf', 'x', false, 1)");
        assertEquals("42883", tooMany.getSQLState());
    }
}
