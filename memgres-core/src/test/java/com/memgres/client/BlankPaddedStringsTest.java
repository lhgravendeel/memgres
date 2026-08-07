package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What happens to the blanks a {@code character(n)} declaration adds.
 *
 * <p>PostgreSQL stores a bpchar padded out to its declared width and reads it back trimmed: the
 * conversion from bpchar to any other string type drops the trailing blanks, and every routine
 * declared over text takes its argument through that conversion. memgres stored the padding and
 * never dropped it, so the blanks came back out of every one of them — {@code length} answered 5
 * where PostgreSQL answers 2, {@code upper} answered "AB   ", {@code reverse} answered "   ba",
 * and {@code md5} hashed a different string than PostgreSQL did.
 */
class BlankPaddedStringsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE bp_t (c char(5), t text)");
            st.execute("INSERT INTO bp_t VALUES ('ab', 'ab')");
        }
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

    // ------------------------------------------------------- read as another string type

    /** The conversion to any other string type is where the blanks go. */
    @Test
    void readingItAsAnotherStringTypeDropsTheBlanks() throws Exception {
        assertEquals("ab", scalar("SELECT ('ab'::char(5))::text"));
        assertEquals("ab", scalar("SELECT ('ab'::char(5))::varchar"));
        assertEquals("ab", scalar("SELECT ('ab'::char(5))::name"));
        assertEquals("ab", scalar("SELECT c::text FROM bp_t"));
        assertEquals("ab", scalar("SELECT c::varchar FROM bp_t"));
    }

    /** Read as itself it is still what it was declared, blanks and all. */
    @Test
    void readAsItselfItKeepsThem() throws Exception {
        assertEquals("ab   ", scalar("SELECT ('ab'::char(5))::char(5)"));
        assertEquals("ab   ", scalar("SELECT c FROM bp_t"));
        assertEquals("5", scalar("SELECT octet_length('ab'::char(5))::text"));
    }

    // ------------------------------------------------------- how long it is

    /** Its length is the length of what it says, not of what it was padded to. */
    @Test
    void itsLengthIsWhatItSays() throws Exception {
        assertEquals("2", scalar("SELECT length('ab'::char(5))::text"));
        assertEquals("2", scalar("SELECT length(c)::text FROM bp_t"));
        assertEquals("2", scalar("SELECT char_length('ab'::char(5))::text"));
        assertEquals("2", scalar("SELECT character_length('ab'::char(5))::text"));
        assertEquals("16", scalar("SELECT bit_length('ab'::char(5))::text"));
        assertEquals("2", scalar("SELECT length('ab'::char(5)::text)::text"));
    }

    // ------------------------------------------------------- read by a routine over text

    /** Every routine declared over text reads it through that conversion. */
    @Test
    void aRoutineOverTextReadsItTrimmed() throws Exception {
        assertEquals("AB", scalar("SELECT upper('ab'::char(5))"));
        assertEquals("ab", scalar("SELECT lower('AB'::char(5))"));
        assertEquals("Ab", scalar("SELECT initcap('ab'::char(5))"));
        assertEquals("ba", scalar("SELECT reverse('ab'::char(5))"));
        assertEquals("az", scalar("SELECT replace('ab'::char(5), 'b', 'z')"));
        assertEquals("az", scalar("SELECT translate('ab'::char(5), 'b', 'z')"));
        assertEquals("az", scalar("SELECT regexp_replace('ab'::char(5), 'b', 'z')"));
        assertEquals("abab", scalar("SELECT repeat('ab'::char(5), 2)"));
        assertEquals("'ab'", scalar("SELECT quote_literal('ab'::char(5))"));
        assertEquals("ab", scalar("SELECT quote_ident('ab'::char(5))"));
        assertEquals("ab", scalar("SELECT substr('ab'::char(5), 1)"));
        assertEquals("ab", scalar("SELECT left('ab'::char(5), 4)"));
        assertEquals("ab", scalar("SELECT right('ab'::char(5), 4)"));
        assertEquals(".....ab", scalar("SELECT lpad('ab'::char(5), 7, '.')"));
        assertEquals("ab.....", scalar("SELECT rpad('ab'::char(5), 7, '.')"));
        assertEquals("{a,\"\"}", scalar("SELECT string_to_array('ab'::char(5), 'b')::text"));
        // The same through a column of the type.
        assertEquals("AB", scalar("SELECT upper(c) FROM bp_t"));
        // And the hash is of what PostgreSQL hashes.
        assertEquals(scalar("SELECT md5('ab'::text)"), scalar("SELECT md5('ab'::char(5))"));
    }

    /**
     * The ones declared over "any" rather than text write each argument out as its own type
     * writes itself, so a bpchar arrives with the blanks it was declared with. It is what makes
     * concat differ from the concatenation operator.
     */
    @Test
    void aRoutineOverAnyWritesItAsItself() throws Exception {
        assertEquals("ab   |", scalar("SELECT concat('ab'::char(5), '|')"));
        assertEquals("ab   -x", scalar("SELECT concat_ws('-', 'ab'::char(5), 'x')"));
        assertEquals("ab   |", scalar("SELECT format('%s|', 'ab'::char(5))"));
        assertEquals("ab   |", scalar("SELECT concat(c, '|') FROM bp_t"));
        assertEquals("\"ab   \"", scalar("SELECT to_json('ab'::char(5))::text"));
        assertEquals("\"ab   \"", scalar("SELECT to_jsonb('ab'::char(5))::text"));
        // The concatenation operator reads it as a text, so there the blanks go.
        assertEquals("ab|", scalar("SELECT ('ab'::char(5) || '|')::text"));
    }

    // ------------------------------------------------------- what must not change

    /** Comparison has always ignored the padding, and still does. */
    @Test
    void comparisonIsUnchanged() throws Exception {
        assertEquals("true", scalar("SELECT ('ab'::char(5) = 'ab')::text"));
        assertEquals("true", scalar("SELECT ('ab'::char(5) = 'ab   ')::text"));
        assertEquals("true", scalar("SELECT ('ab'::char(5) = 'ab'::text)::text"));
        assertEquals("true", scalar("SELECT ('ab'::char(5) < 'ac')::text"));
        assertEquals("true", scalar("SELECT (c = 'ab')::text FROM bp_t"));
    }

    /** A string that was never blank-padded is untouched by any of this. */
    @Test
    void anOrdinaryStringIsUntouched() throws Exception {
        assertEquals("ab  ", scalar("SELECT 'ab  '::text"));
        assertEquals("4", scalar("SELECT length('ab  '::text)::text"));
        assertEquals("AB  ", scalar("SELECT upper('ab  '::text)"));
        assertEquals("ab  ", scalar("SELECT 'ab  '::varchar(5)"));
        assertEquals("4", scalar("SELECT length('ab  '::varchar(5))::text"));
        assertEquals("  ba", scalar("SELECT reverse('ab  '::text)"));
        assertEquals("ab", scalar("SELECT t FROM bp_t"));
    }
}
