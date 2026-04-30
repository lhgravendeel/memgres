package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category C: SIMILAR TO grammar + ESCAPE clause.
 *
 * PG 18 SIMILAR TO uses SQL-regex semantics:
 *   - % matches any substring (including empty)
 *   - _ matches any single character
 *   - | top-level alternation
 *   - + * ? Kleene / optional repeats
 *   - () grouping
 *   - [abc] [^abc] character classes
 *   - ESCAPE 'c' replaces default '\' escape char
 *
 * Observable gaps vs Memgres:
 *   - ESCAPE clause not parsed
 *   - `String.matches` semantics leak through for +, *, ?, (|), [], etc.
 */
class Round16SimilarToTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static boolean bool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    // =========================================================================
    // C1. ESCAPE clause must be parsed + applied
    // =========================================================================

    @Test
    void similar_to_with_escape_clause_parses() throws SQLException {
        // '!' becomes the escape char; !% matches literal '%'
        assertTrue(bool("SELECT 'a%b' SIMILAR TO 'a!%b' ESCAPE '!'"),
                "SIMILAR TO … ESCAPE '!' must treat '!%' as a literal '%'");
    }

    @Test
    void similar_to_default_escape_is_backslash() throws SQLException {
        // Default escape is '\' per PG spec
        assertTrue(bool("SELECT 'a%b' SIMILAR TO 'a\\%b'"),
                "Default backslash escape must match literal '%'");
    }

    @Test
    void similar_to_escape_disabled_by_empty_string() throws SQLException {
        // ESCAPE '' disables escaping; '\' is then a literal.
        assertTrue(bool("SELECT 'a\\b' SIMILAR TO 'a\\b' ESCAPE ''"),
                "SIMILAR TO … ESCAPE '' must disable escape handling");
    }

    // =========================================================================
    // C2. Top-level alternation '|'
    // =========================================================================

    @Test
    void similar_to_top_level_alternation() throws SQLException {
        assertTrue(bool("SELECT 'abc' SIMILAR TO 'a|abc|xyz'"),
                "SIMILAR TO top-level '|' must be alternation, not literal");
        assertTrue(bool("SELECT 'xyz' SIMILAR TO 'a|abc|xyz'"),
                "SIMILAR TO top-level '|' must match xyz branch");
        assertFalse(bool("SELECT 'q' SIMILAR TO 'a|abc|xyz'"),
                "SIMILAR TO must not match outside alternation set");
    }

    // =========================================================================
    // C3. Repetition operators
    // =========================================================================

    @Test
    void similar_to_repetition_star_and_plus() throws SQLException {
        assertTrue(bool("SELECT 'aaa' SIMILAR TO 'a+'"),
                "SIMILAR TO 'a+' must match one-or-more");
        assertTrue(bool("SELECT '' SIMILAR TO 'a*'"),
                "SIMILAR TO 'a*' must match empty string");
        assertFalse(bool("SELECT '' SIMILAR TO 'a+'"),
                "SIMILAR TO 'a+' must NOT match empty string");
    }

    @Test
    void similar_to_optional_question_mark() throws SQLException {
        assertTrue(bool("SELECT 'ab'  SIMILAR TO 'ab?c?'"),
                "SIMILAR TO '?' must treat preceding atom as optional");
        assertTrue(bool("SELECT 'a'   SIMILAR TO 'ab?'"),
                "SIMILAR TO 'ab?' must match 'a'");
    }

    // =========================================================================
    // C4. Character classes (POSIX + bracket expressions)
    // =========================================================================

    @Test
    void similar_to_bracket_class_matches() throws SQLException {
        assertTrue(bool("SELECT 'bat' SIMILAR TO '[bc]at'"),
                "SIMILAR TO '[bc]at' must match 'bat' or 'cat'");
        assertTrue(bool("SELECT 'cat' SIMILAR TO '[bc]at'"),
                "SIMILAR TO '[bc]at' must match 'cat'");
        assertFalse(bool("SELECT 'hat' SIMILAR TO '[bc]at'"),
                "SIMILAR TO '[bc]at' must NOT match 'hat'");
    }

    @Test
    void similar_to_posix_alpha_class() throws SQLException {
        assertTrue(bool("SELECT 'x' SIMILAR TO '[[:alpha:]]'"),
                "SIMILAR TO '[[:alpha:]]' must match a single alphabetic char");
        assertFalse(bool("SELECT '1' SIMILAR TO '[[:alpha:]]'"),
                "SIMILAR TO '[[:alpha:]]' must NOT match a digit");
    }

    // =========================================================================
    // C5. Grouping
    // =========================================================================

    @Test
    void similar_to_grouping_and_alternation_inside_group() throws SQLException {
        assertTrue(bool("SELECT 'abc' SIMILAR TO '(b|a)bc'"),
                "SIMILAR TO grouped alternation '(b|a)bc' must match 'abc'");
        assertTrue(bool("SELECT 'bbc' SIMILAR TO '(b|a)bc'"),
                "SIMILAR TO grouped alternation must also match 'bbc'");
    }

    // =========================================================================
    // C6. Anchoring: SIMILAR TO implicitly anchors the whole string
    // =========================================================================

    @Test
    void similar_to_is_anchored_at_both_ends() throws SQLException {
        // Unlike LIKE, SIMILAR TO without % must match the WHOLE string
        assertFalse(bool("SELECT 'abcdef' SIMILAR TO 'abc'"),
                "SIMILAR TO must anchor both ends — 'abcdef' vs 'abc' must be false");
        assertTrue(bool("SELECT 'abcdef' SIMILAR TO 'abc%'"),
                "SIMILAR TO with trailing % must match 'abcdef'");
    }
}
