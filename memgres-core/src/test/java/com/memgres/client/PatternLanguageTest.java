package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A pattern is read in PostgreSQL's own language, and a character is a character.
 *
 * <p>Three languages meet here and they are not the same one. A regular expression is
 * PostgreSQL's advanced kind: its class names hold what its tables say they hold, an escape it
 * does not define is an error rather than something to guess at, and a brace that begins no
 * repetition count is an ordinary brace. {@code SIMILAR TO} is a smaller language in which a dot
 * is a dot and {@code \d} is a backslash followed by a d, and it is written out into the first
 * language rather than assembled by quoting some characters and passing others through. And LIKE
 * is smaller again. Compiling all three with {@code java.util.regex} after a thin textual
 * translation left each of them answering some other language's question.
 *
 * <p>Alongside that: everything that takes a position or a length counts characters. Above
 * U+FFFF a character takes two units to store, and counting units made every such function
 * disagree with {@code length()} and cut between the halves of one character.
 */
class PatternLanguageTest {

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

    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /**
     * The letter classes hold every letter, not the twenty-six. The digit class holds the ten:
     * a decimal digit from another script is a letter to PostgreSQL and not a digit, which is
     * why the alphanumeric class holds it either way.
     */
    @Test
    void aNamedClassHoldsWhatPostgresqlSaysItHolds() throws SQLException {
        assertEquals("true", one("SELECT ('é' ~ '[[:alpha:]]')::text"));
        assertEquals("true", one("SELECT ('Ä' ~ '[[:upper:]]')::text"));
        assertEquals("true", one("SELECT ('é' ~ '\\w')::text"));
        assertEquals("XXX", one("SELECT regexp_replace('aéb','[[:alpha:]]','X','g')"));
        assertEquals("aéb", one("SELECT substring('aéb' from '[[:alpha:]]+')"));
        assertEquals("true", one("SELECT ('5' ~ '[[:digit:]]')::text"));
        assertEquals("false", one("SELECT ('٥' ~ '[[:digit:]]')::text"));
        assertEquals("true", one("SELECT ('٥' ~ '[[:alpha:]]')::text"));
        assertEquals("false", one("SELECT ('a' ~ '\\d')::text"));
    }

    /** A bracket expression works the same way inside a SIMILAR TO pattern. */
    @Test
    void aSimilarPatternReadsItsBracketExpressions() throws SQLException {
        assertEquals("true", one("SELECT ('é' SIMILAR TO '[[:alpha:]]')::text"));
    }

    /**
     * Three digits after a backslash name a character in octal; Java writes that with a leading
     * zero and read the same text as a reference to group 141.
     */
    @Test
    void anOctalEscapeNamesACharacter() throws SQLException {
        assertEquals("true", one("SELECT ('a' ~ '\\141')::text"));
    }

    /** PostgreSQL's end anchor is the end; Java's also matches before a final newline. */
    @Test
    void theEndAnchorIsTheEnd() throws SQLException {
        assertEquals("false", one("SELECT (E'ab\\n' ~ 'b\\Z')::text"));
    }

    /** A comment says nothing about what matches, and Java has no such construct. */
    @Test
    void aCommentIsRemovedRatherThanCompiled() throws SQLException {
        assertEquals("true", one("SELECT ('ab' ~ '(?# comment)ab')::text"));
    }

    /** Inside a bracket expression a backspace is a character, not a word boundary. */
    @Test
    void aBackspaceInsideBracketsIsACharacter() throws SQLException {
        assertEquals("false", one("SELECT ('a' ~ '[\\b]')::text"));
    }

    /** A brace that opens no repetition count is three ordinary characters. */
    @Test
    void aBraceThatCountsNothingIsACharacter() throws SQLException {
        assertEquals("false", one("SELECT ('abc' ~ 'a{,3}')::text"));
        assertEquals("true", one("SELECT ('a{b}' SIMILAR TO 'a{b}')::text"));
        assertEquals("true", one("SELECT ('aaa' SIMILAR TO 'a{2,3}')::text"));
    }

    /** Constructs Java has and PostgreSQL does not are refused rather than quietly accepted. */
    @Test
    void aConstructPostgresqlDoesNotHaveIsRefused() {
        assertEquals("2201B", stateOf("SELECT 'abc' ~ '(?<name>a)'"));
        assertEquals("2201B", stateOf("SELECT 'abc' ~ '\\Qa\\E'"));
        assertEquals("2201B", stateOf("SELECT 'abc' ~ '\\'"));
    }

    /**
     * In a replacement text only {@code \1} to {@code \9} and {@code \&} mean anything. A
     * backslash before anything else stands for the two characters as written.
     */
    @Test
    void aReplacementTextMeansOnlyItsBackreferences() throws SQLException {
        assertEquals("ax\\nyc", one("SELECT regexp_replace('abc', 'b', 'x\\ny')"));
        assertEquals("a\\0c", one("SELECT regexp_replace('abc','b','\\0')"));
        assertEquals("a[b]c", one("SELECT regexp_replace('abc','(b)','[\\1]')"));
        assertEquals("a&c", one("SELECT regexp_replace('abc','b','&')"));
        assertEquals("abc", one("SELECT regexp_replace('abc','b','\\&')"));
    }

    /** Positions are counted from one, and a count of matches from one too. */
    @Test
    void aStartAndACountAreNamedWhenTheyAreWrong() {
        assertEquals("22023", stateOf("SELECT regexp_substr('abc', 'a', 0)"));
        assertEquals("22023", stateOf("SELECT regexp_instr('abc', 'a', 0)"));
        assertEquals("22023", stateOf("SELECT regexp_count('abc','a',0)"));
        assertEquals("22023", stateOf("SELECT regexp_substr('abc','a',1,0)"));
        assertEquals("22023", stateOf("SELECT regexp_replace('banana','a','X',1,-1)"));
    }

    /**
     * The set-returning form reads the same language as the operators, so its option letters,
     * its classes and its newline rules are the same ones -- and a match whose text is a newline
     * is a match like any other.
     */
    @Test
    void theSetReturningFormReadsTheSameLanguage() throws SQLException {
        assertEquals("3", one("SELECT count(*)::text FROM regexp_matches(E'a\\nb', '.', 'g')"));
        assertEquals("5", one("SELECT count(*)::text FROM regexp_matches(E'a\\nb\\nc', '.', 'g')"));
        assertEquals("{ab}", one("SELECT * FROM regexp_matches('ab', 'a b', 'x')"));
        assertEquals("{b}", one("SELECT * FROM regexp_matches(E'a\\nb', '^b', 'n')"));
        assertEquals("22023", stateOf("SELECT * FROM regexp_matches('a', 'a', 'z')"));
    }

    /** In a SIMILAR TO pattern a dot is a dot and a backslash is a backslash. */
    @Test
    void aSimilarPatternIsNotARegularExpression() throws SQLException {
        assertEquals("false", one("SELECT ('a' SIMILAR TO '.' ESCAPE '!')::text"));
        assertEquals("false", one("SELECT ('a1' SIMILAR TO 'a\\d' ESCAPE '!')::text"));
        assertEquals("true", one("SELECT ('abc' SIMILAR TO '(a|b)%')::text"));
    }

    /** The escape is an operand: one character, or nothing, in which case so is the answer. */
    @Test
    void anEscapeIsOneCharacterOrNothing() throws SQLException {
        assertEquals("22025", stateOf("SELECT 'abc' SIMILAR TO 'abc' ESCAPE 'xy'"));
        assertNull(one("SELECT ('abc' LIKE 'abc' ESCAPE NULL)::text"));
    }

    /**
     * A SIMILAR pattern with no capture markers wants the whole match, and one with a group that
     * took no part in the match captured nothing.
     */
    @Test
    void aSubstringPatternAsksForWhatItMarks() throws SQLException {
        assertEquals("abc", one("SELECT substring('abc' SIMILAR 'abc' ESCAPE '#')"));
        assertEquals("abc", one("SELECT substring('abc' from 'a%c' for '#')"));
        assertNull(one("SELECT substring('abc' from '(x)?b')"));
        // Which three-argument form this is comes from the declared types, not from whether the
        // pattern happens to spell a number.
        assertNull(one("SELECT substring('abcdef', '2', '3')"));
    }

    /** A character above U+FFFF is one character everywhere it is counted. */
    @Test
    void aCharacterIsACharacterHoweverItIsStored() throws SQLException {
        assertEquals("b", one("SELECT substr(U&'a\\+01F600b', 3, 1)"));
        assertEquals("a\uD83D\uDE00", one("SELECT substr(U&'a\\+01F600b', 1, 2)"));
        assertEquals("aXb", one("SELECT overlay(U&'a\\+01F600b' placing 'X' from 2 for 1)"));
        assertEquals("a\uD83D\uDE00", one("SELECT left(U&'a\\+01F600b', 2)"));
        assertEquals("\uD83D\uDE00b", one("SELECT right(U&'a\\+01F600b', 2)"));
        assertEquals("xx\uD83D\uDE00", one("SELECT lpad(U&'\\+01F600',3,'x')"));
        assertEquals("\uD83D\uDE00xx", one("SELECT rpad(U&'\\+01F600',3,'x')"));
        assertEquals("3", one("SELECT strpos(U&'a\\+01F600b', 'b')::text"));
        assertEquals("128512", one("SELECT ascii(U&'\\+01F600')::text"));
        assertEquals("{a,\uD83D\uDE00,b}", one("SELECT string_to_array(U&'a\\+01F600b', NULL)::text"));
    }

    /** An underscore stands for one character, and one character may be two units. */
    @Test
    void anUnderscoreStandsForOneCharacter() throws SQLException {
        assertEquals("true", one("SELECT (U&'\\+01F600' LIKE '_')::text"));
        assertEquals("true", one("SELECT (U&'a\\+01F600b' LIKE 'a_b')::text"));
    }

    /**
     * Case is folded one character at a time, so a fold never changes a string's length. Java's
     * full mappings turn one character into several.
     */
    @Test
    void foldingCaseChangesNoLength() throws SQLException {
        assertEquals("STRAßE", one("SELECT upper('straße')"));
        assertEquals("6", one("SELECT length(upper('straße'))::text"));
        assertEquals("1", one("SELECT length(casefold(U&'\\0130'))::text"));
    }

    /** With no character set named, trim strips the space and only the space. */
    @Test
    void trimStripsTheSpaceAndOnlyTheSpace() throws SQLException {
        assertEquals("5", one("SELECT length(btrim(E' \\tabc\\n '))::text"));
        assertEquals("[\tabc\t]", one("SELECT '[' || trim(both from E'\\tabc\\t') || ']'"));
        // The plain call spelling names the characters second, and is a call like any other.
        assertEquals("abc", one("SELECT trim('xabcx', 'x')"));
    }

    /**
     * Naming an argument by position also moves the cursor past it, and there is no argument
     * zero because arguments are numbered from one.
     */
    @Test
    void formatCountsItsArgumentsFromOne() throws SQLException {
        assertEquals("a b", one("SELECT format('%1$s %s', 'a', 'b')"));
        assertEquals("22023", stateOf("SELECT format('%0$s','a')"));
        assertEquals("y", one("SELECT format('%*s', NULL, 'y')"));
        assertEquals("22P02", stateOf("SELECT format('%*s', 'abc', 'y')"));
    }

    /** unistr has four spellings and no others. */
    @Test
    void unistrKnowsFourSpellings() throws SQLException {
        assertEquals("data", one("SELECT unistr('dat\\U00000061')"));
        assertEquals("data", one("SELECT unistr('dat\\0061')"));
        assertEquals("42601", stateOf("SELECT unistr('\\wxyz')"));
        assertEquals("42601", stateOf("SELECT unistr('\\12')"));
    }

    /** The question is about the whole string, not about its first character. */
    @Test
    void unicodeAssignedAsksAboutTheWholeString() throws SQLException {
        assertEquals("false", one("SELECT unicode_assigned(U&'a\\0378')::text"));
        assertEquals("true", one("SELECT unicode_assigned('abc')::text"));
    }

    /** Only text starts with text. */
    @Test
    void startsWithIsATextOperator() throws SQLException {
        assertEquals("true", one("SELECT ('abc' ^@ 'ab')::text"));
        assertEquals("42883", stateOf("SELECT 1 ^@ 1"));
    }

    /** The reader's own spellings: an escape the clause names, and a literal continued. */
    @Test
    void aWrittenLiteralIsReadTheWayItIsWritten() throws SQLException {
        assertEquals("data", one("SELECT U&'d!0061t!+000061' UESCAPE '!'"));
        assertEquals("data", one("SELECT U&'\\0064\\0061\\0074\\0061'"));
        assertEquals("a\\b", one("SELECT U&'a\\\\b'"));
        assertEquals("abc", one("SELECT N'abc'"));
        assertEquals("foobar", one("SELECT 'foo'\n'bar'"));
    }

    /**
     * Only the twenty-six letters are folded in an unquoted identifier. PostgreSQL leaves every
     * other character as written, so a word with one in it names a different column from the
     * same word written in lower case.
     */
    @Test
    void anIdentifierIsFoldedAsAsciiOnly() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS zzpl_ident CASCADE");
            st.execute("CREATE TABLE zzpl_ident (MÜLLER int)");
        }
        assertEquals("mÜller", one(
                "SELECT column_name FROM information_schema.columns"
                        + " WHERE table_name = 'zzpl_ident'"));
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE zzpl_ident");
        }
    }
}
