package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Definition-time validation of indexes, composite/enum/range/shell types, domains and sequences.
 *
 * <p>Each rejection asserts the SQLSTATE and the wording PostgreSQL 18 uses, and the cases that
 * must keep being accepted are asserted alongside them — several of these checks sit next to
 * definitions that differ from a rejected one by a single option.
 */
class IndexTypeSequenceValidationTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE itsv_t (x int, y text, z int, arr int[], jb jsonb, p point)");
        exec("INSERT INTO itsv_t VALUES (1,'a',1),(2,'b',2)");
        exec("CREATE VIEW itsv_v AS SELECT * FROM itsv_t");
        exec("CREATE INDEX itsv_ix_exist ON itsv_t (x)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    /** Assert that a statement is rejected with the given SQLSTATE and first line of message. */
    static void assertRejected(String sql, String sqlState, String message) {
        try {
            exec(sql);
            fail("Expected " + sqlState + " for: " + sql);
        } catch (SQLException e) {
            assertEquals(sqlState, e.getSQLState(), "SQLSTATE for: " + sql + " (" + e.getMessage() + ")");
            String actual = e.getMessage();
            if (actual.startsWith("ERROR: ")) actual = actual.substring("ERROR: ".length());
            int nl = actual.indexOf('\n');
            if (nl >= 0) actual = actual.substring(0, nl);
            assertEquals(message, actual.trim(), "message for: " + sql);
        }
    }

    static void assertAccepted(String sql) {
        assertDoesNotThrow(() -> exec(sql), "Expected success for: " + sql);
    }

    // ========================================================================
    // CREATE INDEX — relation kind and access method
    // ========================================================================

    @Test
    void indexOnViewRejected() {
        assertRejected("CREATE INDEX itsv_i ON itsv_v (x)",
                "42809", "cannot create index on relation \"itsv_v\"");
    }

    @Test
    void unknownAccessMethodRejected() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING nosuchmethod (x)",
                "42704", "access method \"nosuchmethod\" does not exist");
    }

    @Test
    void relationKindIsCheckedBeforeAccessMethod() {
        assertRejected("CREATE INDEX itsv_i ON itsv_v USING nosuchmethod (x)",
                "42809", "cannot create index on relation \"itsv_v\"");
    }

    @Test
    void accessMethodIsCheckedBeforeIndexNameClash() {
        assertRejected("CREATE INDEX itsv_ix_exist ON itsv_t USING nosuchmethod (x)",
                "42704", "access method \"nosuchmethod\" does not exist");
    }

    @Test
    void indexNameClashStillReportedWhenDefinitionIsSound() {
        assertRejected("CREATE INDEX itsv_ix_exist ON itsv_t (y)",
                "42P07", "relation \"itsv_ix_exist\" already exists");
    }

    // ========================================================================
    // CREATE INDEX — access method capabilities
    // ========================================================================

    @Test
    void hashDoesNotSupportUniqueIndexes() {
        assertRejected("CREATE UNIQUE INDEX itsv_i ON itsv_t USING hash (x)",
                "0A000", "access method \"hash\" does not support unique indexes");
    }

    @Test
    void hashDoesNotSupportIncludedColumns() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING hash (x) INCLUDE (y)",
                "0A000", "access method \"hash\" does not support included columns");
    }

    @Test
    void hashDoesNotSupportMulticolumnIndexes() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING hash (x, y)",
                "0A000", "access method \"hash\" does not support multicolumn indexes");
    }

    @Test
    void hashDoesNotSupportOrderingOptions() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING hash (x DESC)",
                "0A000", "access method \"hash\" does not support ASC/DESC options");
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING hash (x NULLS FIRST)",
                "0A000", "access method \"hash\" does not support NULLS FIRST/LAST options");
    }

    // ========================================================================
    // CREATE INDEX — the access method needs an operator class for the type
    // ========================================================================

    @Test
    void anAccessMethodWithNoDefaultOperatorClassForTheTypeIsRejected() {
        // gin indexes the parts inside a value, so a plain integer gives it nothing to index.
        // btree_gin adds these classes, but that is a contrib extension this engine does not have.
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING gin (x)",
                "42704", "data type integer has no default operator class for access method \"gin\"");
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING gin (y)",
                "42704", "data type text has no default operator class for access method \"gin\"");
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING spgist (x)",
                "42704", "data type integer has no default operator class for access method \"spgist\"");
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING hash (p)",
                "42704", "data type point has no default operator class for access method \"hash\"");
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING brin (p)",
                "42704", "data type point has no default operator class for access method \"brin\"");
        assertRejected("CREATE INDEX itsv_i ON itsv_t (p)",
                "42704", "data type point has no default operator class for access method \"btree\"");
    }

    @Test
    void theTypesEachAccessMethodDoesSupportStillIndex() {
        assertAccepted("CREATE INDEX itsv_dop1 ON itsv_t USING gin (arr)");
        assertAccepted("CREATE INDEX itsv_dop2 ON itsv_t USING gin (jb)");
        assertAccepted("CREATE INDEX itsv_dop3 ON itsv_t USING hash (x)");
        assertAccepted("CREATE INDEX itsv_dop4 ON itsv_t USING brin (x)");
        assertAccepted("CREATE INDEX itsv_dop5 ON itsv_t USING spgist (y)");
        assertAccepted("CREATE INDEX itsv_dop6 ON itsv_t USING gist (p)");
        assertAccepted("CREATE INDEX itsv_dop7 ON itsv_t (x, y)");
        // a varchar reaches the text class the way PG does, through binary coercion
        assertAccepted("CREATE INDEX itsv_dop8 ON itsv_t ((y::varchar(10)))");
    }

    @Test
    void anExplicitOperatorClassIsCheckedInsteadOfTheDefault() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING gin (x nosuch_ops)",
                "42704", "operator class \"nosuch_ops\" does not exist for access method \"gin\"");
    }

    @Test
    void theOperatorClassIsCheckedBeforeTheOrderingOptions() {
        // PG settles the class first, so an unsupported type is reported rather than the DESC
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING gin (x DESC)",
                "42704", "data type integer has no default operator class for access method \"gin\"");
        // ... and where the class exists, the ordering option is what is wrong
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING gin (arr DESC)",
                "0A000", "access method \"gin\" does not support ASC/DESC options");
    }

    @Test
    void theAccessMethodCapabilitiesAreCheckedBeforeTheOperatorClass() {
        // both are wrong here; PG reports the capability
        assertRejected("CREATE UNIQUE INDEX itsv_i ON itsv_t USING gin (x)",
                "0A000", "access method \"gin\" does not support unique indexes");
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING gin (x) INCLUDE (y)",
                "0A000", "access method \"gin\" does not support included columns");
    }

    @Test
    void capabilitiesAreCheckedInPostgresOrder() {
        // unique before multicolumn, included before multicolumn, included before ASC/DESC
        assertRejected("CREATE UNIQUE INDEX itsv_i ON itsv_t USING hash (x, y)",
                "0A000", "access method \"hash\" does not support unique indexes");
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING hash (x, y) INCLUDE (z)",
                "0A000", "access method \"hash\" does not support included columns");
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING hash (x DESC) INCLUDE (y)",
                "0A000", "access method \"hash\" does not support included columns");
        // and the column itself is only resolved afterwards
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING hash (nosuchcol, y)",
                "0A000", "access method \"hash\" does not support multicolumn indexes");
    }

    @Test
    void otherAccessMethodCapabilities() {
        assertRejected("CREATE UNIQUE INDEX itsv_i ON itsv_t USING gin (x)",
                "0A000", "access method \"gin\" does not support unique indexes");
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING gin (x) INCLUDE (y)",
                "0A000", "access method \"gin\" does not support included columns");
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING gin (arr DESC)",
                "0A000", "access method \"gin\" does not support ASC/DESC options");
        assertRejected("CREATE UNIQUE INDEX itsv_i ON itsv_t USING brin (x)",
                "0A000", "access method \"brin\" does not support unique indexes");
        assertRejected("CREATE UNIQUE INDEX itsv_i ON itsv_t USING gist (x)",
                "0A000", "access method \"gist\" does not support unique indexes");
    }

    // ========================================================================
    // CREATE INDEX — operator classes and collations
    // ========================================================================

    @Test
    void unknownOperatorClassRejected() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t (x nosuch_ops)",
                "42704", "operator class \"nosuch_ops\" does not exist for access method \"btree\"");
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING gin (x nosuch_ops)",
                "42704", "operator class \"nosuch_ops\" does not exist for access method \"gin\"");
    }

    @Test
    void operatorClassMustAcceptTheColumnType() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t (x text_pattern_ops)",
                "42804", "operator class \"text_pattern_ops\" does not accept data type integer");
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING hash (x text_pattern_ops)",
                "42804", "operator class \"text_pattern_ops\" does not accept data type integer");
    }

    @Test
    void collateRejectedOnANonCollatableColumn() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t (x COLLATE \"C\")",
                "42804", "collations are not supported by type integer");
    }

    // ========================================================================
    // CREATE INDEX — expressions and predicates
    // ========================================================================

    @Test
    void subqueryAsIndexKeyIsASyntaxError() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t ((SELECT 1))",
                "42601", "syntax error at or near \"SELECT\"");
    }

    @Test
    void aggregateInIndexExpressionRejected() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t ((count(x)))",
                "42803", "aggregate functions are not allowed in index expressions");
        assertRejected("CREATE INDEX itsv_i ON itsv_t ((sum(x)))",
                "42803", "aggregate functions are not allowed in index expressions");
        assertRejected("CREATE INDEX itsv_i ON itsv_t ((avg(x)))",
                "42803", "aggregate functions are not allowed in index expressions");
    }

    @Test
    void statementAnalysisPrecedesAccessMethodResolution() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t USING nosuchmethod ((count(x)))",
                "42803", "aggregate functions are not allowed in index expressions");
        assertRejected("CREATE INDEX itsv_i ON itsv_t (x nosuch_ops) WHERE x IN (SELECT 1)",
                "0A000", "cannot use subquery in index predicate");
    }

    @Test
    void subqueryInIndexPredicateRejected() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t (x) WHERE x IN (SELECT 1)",
                "0A000", "cannot use subquery in index predicate");
        assertRejected("CREATE INDEX itsv_i ON itsv_t (x) WHERE EXISTS (SELECT 1)",
                "0A000", "cannot use subquery in index predicate");
    }

    @Test
    void mutablePredicateRejected() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t (x) WHERE x > random()",
                "42P17", "functions in index predicate must be marked IMMUTABLE");
        // and the predicate is checked before the key columns are resolved
        assertRejected("CREATE INDEX itsv_i ON itsv_t (nosuchcol) WHERE x > random()",
                "42P17", "functions in index predicate must be marked IMMUTABLE");
    }

    @Test
    void mutableIndexExpressionRejected() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t ((random()))",
                "42P17", "functions in index expression must be marked IMMUTABLE");
    }

    @Test
    void missingColumnsRejected() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t (nosuchcol)",
                "42703", "column \"nosuchcol\" does not exist");
        assertRejected("CREATE INDEX itsv_i ON itsv_t (x) WHERE nosuchcol > 1",
                "42703", "column \"nosuchcol\" does not exist");
        assertRejected("CREATE INDEX itsv_i ON itsv_nosuchtable (x)",
                "42P01", "relation \"itsv_nosuchtable\" does not exist");
    }

    // ========================================================================
    // CREATE INDEX — INCLUDE, and definitions that must keep working
    // ========================================================================

    @Test
    void includeColumnMustExistAndNotBeAnExpression() {
        assertRejected("CREATE INDEX itsv_i ON itsv_t (x) INCLUDE (nosuchcol)",
                "42703", "column \"nosuchcol\" does not exist");
        assertRejected("CREATE INDEX itsv_i ON itsv_t (x) INCLUDE ((y || 'a'))",
                "0A000", "expressions are not supported in included columns");
    }

    @Test
    void validIndexDefinitionsStillWork() throws SQLException {
        assertAccepted("CREATE INDEX itsv_ok1 ON itsv_t (x) INCLUDE (y)");
        assertAccepted("CREATE INDEX itsv_ok2 ON itsv_t (x, y) INCLUDE (z)");
        assertAccepted("CREATE UNIQUE INDEX itsv_ok3 ON itsv_t (x) INCLUDE (y)");
        assertAccepted("CREATE INDEX itsv_ok4 ON itsv_t USING hash (x)");
        assertAccepted("CREATE INDEX itsv_ok5 ON itsv_t USING gin (arr)");
        assertAccepted("CREATE INDEX itsv_ok6 ON itsv_t (lower(y))");
        assertAccepted("CREATE INDEX itsv_ok7 ON itsv_t ((x + z))");
        assertAccepted("CREATE INDEX itsv_ok8 ON itsv_t (x DESC NULLS LAST)");
        assertAccepted("CREATE INDEX itsv_ok9 ON itsv_t (y text_pattern_ops)");
        assertAccepted("CREATE INDEX itsv_ok10 ON itsv_t (y varchar_pattern_ops)");
        assertAccepted("CREATE INDEX itsv_ok11 ON itsv_t USING btree (x int4_ops)");
        assertAccepted("CREATE INDEX itsv_ok12 ON itsv_t USING hash (x int4_ops)");
        assertAccepted("CREATE INDEX itsv_ok13 ON itsv_t (x) WHERE x > 1");
        assertAccepted("CREATE INDEX itsv_ok14 ON itsv_t USING hash (x) WHERE x > 1");
        assertAccepted("CREATE INDEX itsv_ok15 ON itsv_t (y COLLATE \"C\" text_pattern_ops)");
        assertEquals("2", q("SELECT count(*)::text FROM itsv_t WHERE x > 0"));
    }

    // ========================================================================
    // CREATE TYPE
    // ========================================================================

    @Test
    void duplicateCompositeAttributeRejected() {
        assertRejected("CREATE TYPE itsv_ct1 AS (a int, a text)",
                "42701", "column \"a\" specified more than once");
        // an unquoted name is folded, so this is the same attribute twice
        assertRejected("CREATE TYPE itsv_ct1 AS (a int, A text)",
                "42701", "column \"a\" specified more than once");
        // the name clash is reported before the second attribute's type is resolved
        assertRejected("CREATE TYPE itsv_ct1 AS (a int, a nosuchtype)",
                "42701", "column \"a\" specified more than once");
    }

    @Test
    void quotedAttributeNamesKeepTheirCase() {
        assertAccepted("CREATE TYPE itsv_ct2 AS (\"A\" int, a text)");
    }

    @Test
    void unknownCompositeAttributeTypeRejected() {
        assertRejected("CREATE TYPE itsv_ct3 AS (a nosuchtype)",
                "42704", "type \"nosuchtype\" does not exist");
    }

    @Test
    void duplicateEnumLabelRejected() {
        assertRejected("CREATE TYPE itsv_en1 AS ENUM ('a','a')",
                "23505", "duplicate key value violates unique constraint \"pg_enum_typid_label_index\"");
        assertRejected("CREATE TYPE itsv_en1 AS ENUM ('a','b','a')",
                "23505", "duplicate key value violates unique constraint \"pg_enum_typid_label_index\"");
        assertAccepted("CREATE TYPE itsv_en2 AS ENUM ('a','b')");
    }

    @Test
    void rangeTypeNeedsAValidSubtype() {
        assertRejected("CREATE TYPE itsv_rg1 AS RANGE (COLLATION = \"C\")",
                "42601", "type attribute \"subtype\" is required");
        assertRejected("CREATE TYPE itsv_rg1 AS RANGE (SUBTYPE = nosuchtype)",
                "42704", "type \"nosuchtype\" does not exist");
        assertAccepted("CREATE TYPE itsv_rg2 AS RANGE (SUBTYPE = int4)");
    }

    @Test
    void redefiningAnExistingTypeRejected() throws SQLException {
        exec("CREATE TYPE itsv_ct4 AS (a int, b text)");
        assertRejected("CREATE TYPE itsv_ct4 AS (a int)", "42710", "type \"itsv_ct4\" already exists");
    }

    @Test
    void alterTypeAttributeChecks() throws SQLException {
        exec("CREATE TYPE itsv_ct5 AS (a int, b text)");
        assertRejected("ALTER TYPE itsv_ct5 DROP ATTRIBUTE nosuchattr",
                "42703", "column \"nosuchattr\" of relation \"itsv_ct5\" does not exist");
        assertRejected("ALTER TYPE itsv_ct5 ADD ATTRIBUTE a int",
                "42701", "column \"a\" of relation \"itsv_ct5\" already exists");
        assertRejected("ALTER TYPE itsv_ct5 RENAME ATTRIBUTE nosuchattr TO zz",
                "42703", "column \"nosuchattr\" does not exist");
        assertAccepted("ALTER TYPE itsv_ct5 ADD ATTRIBUTE c int");
        assertAccepted("ALTER TYPE itsv_ct5 DROP ATTRIBUTE c");
    }

    // ========================================================================
    // Shell types
    // ========================================================================

    @Test
    void shellTypeIsCreatedAndCanBeFilledIn() throws SQLException {
        exec("CREATE TYPE itsv_shell1");
        assertEquals("p", q("SELECT typtype FROM pg_type WHERE typname = 'itsv_shell1'"));
        assertEquals("false", q("SELECT typisdefined::text FROM pg_type WHERE typname = 'itsv_shell1'"));
        assertRejected("CREATE TYPE itsv_shell1", "42710", "type \"itsv_shell1\" already exists");
        assertRejected("CREATE TABLE itsv_shelluse (c itsv_shell1)",
                "42704", "type \"itsv_shell1\" is only a shell");
        assertAccepted("CREATE TYPE itsv_shell1 AS (a int)");
        assertAccepted("CREATE TABLE itsv_shelluse (c itsv_shell1)");
        assertAccepted("DROP TABLE itsv_shelluse");
    }

    @Test
    void shellTypeCanBeDropped() throws SQLException {
        exec("CREATE TYPE itsv_shell2");
        assertAccepted("DROP TYPE itsv_shell2");
        assertRejected("DROP TYPE itsv_shell2", "42704", "type \"itsv_shell2\" does not exist");
    }

    // ========================================================================
    // DROP TYPE dependency checking
    // ========================================================================

    @Test
    void droppingATypeAColumnUsesIsRejected() throws SQLException {
        exec("CREATE TYPE itsv_ct6 AS (a int)");
        exec("CREATE TABLE itsv_uses6 (c itsv_ct6)");
        SQLException e = assertThrows(SQLException.class, () -> exec("DROP TYPE itsv_ct6"));
        assertEquals("2BP01", e.getSQLState());
        assertTrue(e.getMessage().contains("because other objects depend on it"), e.getMessage());
        assertAccepted("DROP TYPE itsv_ct6 CASCADE");
    }

    @Test
    void droppingAnEnumAColumnUsesIsRejected() throws SQLException {
        exec("CREATE TYPE itsv_en3 AS ENUM ('a')");
        exec("CREATE TABLE itsv_uses7 (c itsv_en3)");
        SQLException e = assertThrows(SQLException.class, () -> exec("DROP TYPE itsv_en3"));
        assertEquals("2BP01", e.getSQLState());
    }

    @Test
    void droppingAnUnusedTypeStillWorks() throws SQLException {
        exec("CREATE TYPE itsv_ct7 AS (a int)");
        assertAccepted("DROP TYPE itsv_ct7");
    }

    // ========================================================================
    // CREATE DOMAIN
    // ========================================================================

    @Test
    void conflictingNullConstraintsRejected() {
        assertRejected("CREATE DOMAIN itsv_d1 AS int4 NOT NULL NULL",
                "42601", "conflicting NULL/NOT NULL constraints");
        assertRejected("CREATE DOMAIN itsv_d1 AS int4 NULL NOT NULL",
                "42601", "conflicting NULL/NOT NULL constraints");
    }

    @Test
    void multipleDefaultsRejected() {
        assertRejected("CREATE DOMAIN itsv_d1 AS int4 DEFAULT 3 DEFAULT 3",
                "42601", "multiple default expressions");
    }

    @Test
    void tableConstraintsOnADomainRejected() {
        assertRejected("CREATE DOMAIN itsv_d1 AS int4 UNIQUE",
                "42601", "unique constraints not possible for domains");
        assertRejected("CREATE DOMAIN itsv_d1 AS int4 PRIMARY KEY",
                "42601", "primary key constraints not possible for domains");
        assertRejected("CREATE DOMAIN itsv_d1 AS int4 DEFAULT 3 UNIQUE",
                "42601", "unique constraints not possible for domains");
        assertRejected("CREATE DOMAIN itsv_d1 AS int4 CONSTRAINT c REFERENCES itsv_t(x)",
                "42601", "foreign key constraints not possible for domains");
    }

    @Test
    void domainCheckMayOnlyReferenceValue() {
        assertRejected("CREATE DOMAIN itsv_d1 AS int4 CHECK (x > 0)",
                "42703", "column \"x\" does not exist");
        assertRejected("CREATE DOMAIN itsv_d1 AS int4 NOT NULL CHECK (x > 0)",
                "42703", "column \"x\" does not exist");
        assertAccepted("CREATE DOMAIN itsv_d2 AS int4 CHECK (VALUE > 0)");
        assertAccepted("CREATE DOMAIN itsv_d3 AS int4 CHECK (value > 0)");
        assertAccepted("CREATE DOMAIN itsv_d4 AS int4 CONSTRAINT c CHECK (VALUE > 0)");
    }

    @Test
    void domainBaseTypeAndCollationChecked() {
        assertRejected("CREATE DOMAIN itsv_d1 AS nosuchtype",
                "42704", "type \"nosuchtype\" does not exist");
        assertRejected("CREATE DOMAIN itsv_d1 AS int4 COLLATE \"C\"",
                "42804", "collations are not supported by type integer");
        assertAccepted("CREATE DOMAIN itsv_d5 AS text COLLATE \"C\" NOT NULL");
        assertAccepted("CREATE DOMAIN itsv_d6 AS int4 NOT NULL DEFAULT 3");
        assertAccepted("CREATE DOMAIN itsv_d7 AS int4 DEFAULT 3 NOT NULL");
        assertAccepted("CREATE DOMAIN itsv_d8 AS int4 NULL");
    }

    // ========================================================================
    // CREATE SEQUENCE
    // ========================================================================

    @Test
    void sequenceDataTypeMustBeAnIntegerType() {
        assertRejected("CREATE SEQUENCE itsv_s1 AS text",
                "22023", "sequence type must be smallint, integer, or bigint");
        assertRejected("CREATE SEQUENCE itsv_s1 AS numeric",
                "22023", "sequence type must be smallint, integer, or bigint");
        // and it is resolved before any other option is looked at
        assertRejected("CREATE SEQUENCE itsv_s1 AS text CACHE 0",
                "22023", "sequence type must be smallint, integer, or bigint");
    }

    @Test
    void incrementMustNotBeZero() {
        assertRejected("CREATE SEQUENCE itsv_s1 INCREMENT 0", "22023", "INCREMENT must not be zero");
        assertRejected("CREATE SEQUENCE itsv_s1 INCREMENT 0 CACHE 0",
                "22023", "INCREMENT must not be zero");
    }

    @Test
    void cacheMustBePositive() {
        assertRejected("CREATE SEQUENCE itsv_s1 CACHE 0", "22023", "CACHE (0) must be greater than zero");
        assertRejected("CREATE SEQUENCE itsv_s1 CACHE -1", "22023", "CACHE (-1) must be greater than zero");
        assertAccepted("CREATE SEQUENCE itsv_s2 CACHE 1");
    }

    @Test
    void minValueMustBeBelowMaxValue() {
        assertRejected("CREATE SEQUENCE itsv_s1 MINVALUE 10 MAXVALUE 5",
                "22023", "MINVALUE (10) must be less than MAXVALUE (5)");
        assertRejected("CREATE SEQUENCE itsv_s1 MINVALUE 5 MAXVALUE 5",
                "22023", "MINVALUE (5) must be less than MAXVALUE (5)");
        // the crosscheck is reached before CACHE is examined
        assertRejected("CREATE SEQUENCE itsv_s1 CACHE 0 MINVALUE 10 MAXVALUE 5",
                "22023", "MINVALUE (10) must be less than MAXVALUE (5)");
    }

    @Test
    void startMustLieWithinTheRange() {
        assertRejected("CREATE SEQUENCE itsv_s1 MINVALUE 5 MAXVALUE 10 START 1",
                "22023", "START value (1) cannot be less than MINVALUE (5)");
        assertRejected("CREATE SEQUENCE itsv_s1 MINVALUE 5 MAXVALUE 10 START 20",
                "22023", "START value (20) cannot be greater than MAXVALUE (10)");
        assertRejected("CREATE SEQUENCE itsv_s1 START 0 INCREMENT 1 MINVALUE 1",
                "22023", "START value (0) cannot be less than MINVALUE (1)");
        // a descending sequence defaults MAXVALUE to -1
        assertRejected("CREATE SEQUENCE itsv_s1 INCREMENT -1 START 5",
                "22023", "START value (5) cannot be greater than MAXVALUE (-1)");
        assertAccepted("CREATE SEQUENCE itsv_s3 MINVALUE 5 MAXVALUE 10 START 7");
    }

    @Test
    void boundsMustFitTheSequenceDataType() {
        assertRejected("CREATE SEQUENCE itsv_s1 AS smallint MAXVALUE 100000",
                "22023", "MAXVALUE (100000) is out of range for sequence data type smallint");
        assertRejected("CREATE SEQUENCE itsv_s1 AS smallint MINVALUE -100000",
                "22023", "MINVALUE (-100000) is out of range for sequence data type smallint");
        // MAXVALUE is validated before MINVALUE
        assertRejected("CREATE SEQUENCE itsv_s1 AS smallint MAXVALUE 100000 MINVALUE -100000",
                "22023", "MAXVALUE (100000) is out of range for sequence data type smallint");
        assertRejected("CREATE SEQUENCE itsv_s1 AS smallint START 40000",
                "22023", "START value (40000) cannot be greater than MAXVALUE (32767)");
        assertRejected("CREATE SEQUENCE itsv_s1 AS int MAXVALUE 3000000000",
                "22023", "MAXVALUE (3000000000) is out of range for sequence data type integer");
    }

    @Test
    void sequenceDefaultsFollowTheDataTypeAndDirection() throws SQLException {
        exec("CREATE SEQUENCE itsv_s4 AS smallint");
        assertEquals("32767", q("SELECT max_value::text FROM pg_sequences WHERE sequencename = 'itsv_s4'"));
        assertEquals("1", q("SELECT nextval('itsv_s4')::text"));
        exec("CREATE SEQUENCE itsv_s5 INCREMENT -1");
        assertEquals("-1", q("SELECT max_value::text FROM pg_sequences WHERE sequencename = 'itsv_s5'"));
        assertEquals("-9223372036854775808",
                q("SELECT min_value::text FROM pg_sequences WHERE sequencename = 'itsv_s5'"));
        exec("CREATE SEQUENCE itsv_s6 MINVALUE 5");
        assertEquals("5", q("SELECT start_value::text FROM pg_sequences WHERE sequencename = 'itsv_s6'"));
    }

    @Test
    void ownedByMustNameAnExistingColumn() {
        assertRejected("CREATE SEQUENCE itsv_s1 OWNED BY itsv_t.nosuchcol",
                "42703", "column \"nosuchcol\" of relation \"itsv_t\" does not exist");
        assertRejected("CREATE SEQUENCE itsv_s1 OWNED BY itsv_nosuchtable.x",
                "42P01", "relation \"itsv_nosuchtable\" does not exist");
        assertAccepted("CREATE SEQUENCE itsv_s7 OWNED BY itsv_t.x");
        assertAccepted("CREATE SEQUENCE itsv_s8 OWNED BY NONE");
    }

    @Test
    void pgGetSerialSequenceReportsBadNames() throws SQLException {
        assertRejected("SELECT pg_get_serial_sequence('itsv_t','nosuchcol')",
                "42703", "column \"nosuchcol\" of relation \"itsv_t\" does not exist");
        assertRejected("SELECT pg_get_serial_sequence('itsv_nosuchtable','x')",
                "42P01", "relation \"itsv_nosuchtable\" does not exist");
        // a real column with no sequence behind it is still NULL, not an error
        assertNull(q("SELECT pg_get_serial_sequence('itsv_t','z')"));
    }

    // ========================================================================
    // ALTER SEQUENCE
    // ========================================================================

    @Test
    void alterSequenceRechecksTheOptions() throws SQLException {
        exec("CREATE SEQUENCE itsv_as1 MAXVALUE 100");
        assertRejected("ALTER SEQUENCE itsv_as1 RESTART WITH 1000",
                "22023", "RESTART value (1000) cannot be greater than MAXVALUE (100)");
        assertRejected("ALTER SEQUENCE itsv_as1 INCREMENT 0", "22023", "INCREMENT must not be zero");
        assertRejected("ALTER SEQUENCE itsv_as1 CACHE 0", "22023", "CACHE (0) must be greater than zero");
        assertAccepted("ALTER SEQUENCE itsv_as1 RESTART WITH 50");
        assertEquals("50", q("SELECT nextval('itsv_as1')::text"));
    }

    @Test
    void alterSequenceRechecksStartAgainstNewBounds() throws SQLException {
        exec("CREATE SEQUENCE itsv_as2 MINVALUE 10 MAXVALUE 100");
        assertRejected("ALTER SEQUENCE itsv_as2 START WITH 5",
                "22023", "START value (5) cannot be less than MINVALUE (10)");
        exec("CREATE SEQUENCE itsv_as3");
        assertRejected("ALTER SEQUENCE itsv_as3 MINVALUE 10",
                "22023", "START value (1) cannot be less than MINVALUE (10)");
    }

    @Test
    void alterSequenceDataTypeMovesUntouchedBounds() throws SQLException {
        exec("CREATE SEQUENCE itsv_as4");
        assertAccepted("ALTER SEQUENCE itsv_as4 AS smallint");
        assertEquals("32767", q("SELECT max_value::text FROM pg_sequences WHERE sequencename = 'itsv_as4'"));
        exec("CREATE SEQUENCE itsv_as5 AS smallint");
        assertRejected("ALTER SEQUENCE itsv_as5 MAXVALUE 100000",
                "22023", "MAXVALUE (100000) is out of range for sequence data type smallint");
    }

    @Test
    void alterSequenceRaisesMaxAndRestartsTogether() throws SQLException {
        exec("CREATE SEQUENCE itsv_as6 MAXVALUE 100");
        assertAccepted("ALTER SEQUENCE itsv_as6 MAXVALUE 200 RESTART WITH 150");
        assertEquals("150", q("SELECT nextval('itsv_as6')::text"));
    }

    @Test
    void alterSequenceIfExistsOnAMissingSequence() {
        assertRejected("ALTER SEQUENCE itsv_nosuchseq RESTART",
                "42P01", "relation \"itsv_nosuchseq\" does not exist");
        assertAccepted("ALTER SEQUENCE IF EXISTS itsv_nosuchseq RESTART");
        assertAccepted("ALTER SEQUENCE IF EXISTS itsv_nosuchseq RESTART WITH 5");
    }
}
