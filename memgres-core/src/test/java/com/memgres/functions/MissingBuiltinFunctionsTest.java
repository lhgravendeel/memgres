package com.memgres.functions;

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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Built-in functions PostgreSQL 18 has and memgres did not: splitting a qualified name, folding
 * case for comparison, reading what a UUID encodes, and the constructor behind a JSON literal.
 * Expectations captured from a live PostgreSQL 18.0 server.
 *
 * <p>C1 parse_ident, C2 casefold, C3 uuid_extract_*, C4 json().
 */
class MissingBuiltinFunctionsTest {

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

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static String state(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) { s.execute(sql); }
        });
        return e.getSQLState();
    }

    // ---- C1: parse_ident ----

    @Test
    void parseIdentSplitsAQualifiedName() throws Exception {
        assertEquals("{a,b}", one("SELECT parse_ident('a.b')::text"));
        assertEquals("{a,b,c}", one("SELECT parse_ident('a.b.c')::text"));
    }

    /** An unquoted part is folded to lower case; a quoted one keeps its spelling. */
    @Test
    void parseIdentUnquotesAndFolds() throws Exception {
        assertEquals("{\"A b\",c}", one("SELECT parse_ident('\"A b\".c')::text"));
        assertEquals("{abc}", one("SELECT parse_ident('ABC')::text"));
    }

    @Test
    void parseIdentRejectsWhatIsNotAName() {
        assertEquals("22023", state("SELECT parse_ident('a.b.')"));
        assertEquals("22023", state("SELECT parse_ident('\"unclosed')"));
    }

    @Test
    void parseIdentIsNullForNull() throws Exception {
        assertNull(one("SELECT parse_ident(NULL)"));
    }

    // ---- C2: casefold ----

    @Test
    void casefoldFoldsForComparison() throws Exception {
        assertEquals("abc", one("SELECT casefold('AbC')"));
        assertEquals("abc", one("SELECT casefold('ABC')"));
    }

    @Test
    void casefoldIsNullForNull() throws Exception {
        assertNull(one("SELECT casefold(NULL)"));
    }

    // ---- C3: uuid_extract_version / uuid_extract_timestamp ----

    @Test
    void theVersionOfAUuidCanBeRead() throws Exception {
        assertEquals("4", one(
                "SELECT uuid_extract_version('00000000-0000-4000-8000-000000000000'::uuid)::text"));
        assertEquals("7", one("SELECT uuid_extract_version(uuidv7())::text"));
    }

    @Test
    void theTimestampOfAVersionSevenUuidCanBeRead() throws Exception {
        assertTrue(Boolean.parseBoolean(
                one("SELECT (uuid_extract_timestamp(uuidv7()) IS NOT NULL)::text")));
    }

    /** Only version 7 carries a timestamp; for any other version there is nothing to read. */
    @Test
    void anotherVersionHasNoTimestamp() throws Exception {
        assertNull(one(
                "SELECT uuid_extract_timestamp('00000000-0000-4000-8000-000000000000'::uuid)"));
    }

    @Test
    void uuidExtractIsNullForNull() throws Exception {
        assertNull(one("SELECT uuid_extract_version(NULL)"));
        assertNull(one("SELECT uuid_extract_timestamp(NULL)"));
    }

    // ---- C4: json() ----

    @Test
    void jsonConstructsAJsonValue() throws Exception {
        assertEquals("{\"a\":1}", one("SELECT json('{\"a\":1}')::text"));
        assertEquals("[1, 2]", one("SELECT json('[1, 2]')::text"));
    }

    @Test
    void jsonComposesWithJsonSerialize() throws Exception {
        assertEquals("{\"a\":1}", one("SELECT JSON_SERIALIZE(JSON('{\"a\":1}'))"));
    }

    @Test
    void jsonRejectsWhatIsNotJson() {
        assertEquals("22P02", state("SELECT json('{oops')"));
    }

    @Test
    void jsonIsNullForNull() throws Exception {
        assertNull(one("SELECT json(NULL)"));
    }
}
