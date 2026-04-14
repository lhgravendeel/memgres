package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 4 failures from multirange-types.sql where Memgres diverges from PG 18.
 *
 * Stmt 6:  nummultirange returns integer-rounded bounds instead of decimal
 * Stmt 7:  datemultirange returns epoch day numbers instead of date strings
 * Stmt 8:  tsmultirange returns epoch seconds instead of timestamp strings
 * Stmt 33: multirange-to-range cast returns wrong SQLSTATE (22P02 instead of 42846)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MultirangeTypesTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS mr_type_test CASCADE");
            s.execute("CREATE SCHEMA mr_type_test");
            s.execute("SET search_path = mr_type_test, public");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS mr_type_test CASCADE");
                s.execute("SET search_path = public");
            } catch (Exception ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    /**
     * Stmt 6: nummultirange constructor should preserve decimal bounds.
     * PG 18 returns {[1.5,3.5),[10.0,20.0)} but Memgres returns {[2,4),[10,20)}.
     */
    @Test
    @Order(1)
    void nummultirangePreservesDecimalBounds() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT '{[1.5,3.5),[10.0,20.0)}'::nummultirange AS result")) {
            assertTrue(rs.next(), "Expected one row");
            String result = rs.getString("result");
            assertEquals("{[1.5,3.5),[10.0,20.0)}", result,
                    "nummultirange should preserve decimal precision in bounds");
        }
    }

    /**
     * Stmt 7: datemultirange constructor should return formatted date strings.
     * PG 18 returns {[2026-01-01,2026-02-01),[2026-06-01,2026-07-01)}
     * but Memgres returns {[20454,20485),[20605,20635)}.
     */
    @Test
    @Order(2)
    void datemultirangeReturnsFormattedDates() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT '{[2026-01-01,2026-02-01),[2026-06-01,2026-07-01)}'::datemultirange AS result")) {
            assertTrue(rs.next(), "Expected one row");
            String result = rs.getString("result");
            assertEquals("{[2026-01-01,2026-02-01),[2026-06-01,2026-07-01)}", result,
                    "datemultirange should return human-readable date strings, not epoch day numbers");
        }
    }

    /**
     * Stmt 8: tsmultirange constructor should return formatted timestamp strings.
     * PG 18 returns {["2026-01-01 00:00:00","2026-01-02 00:00:00"),["2026-06-01 00:00:00","2026-06-02 00:00:00")}
     * but Memgres returns {[1767225600,1767312000),[1780272000,1780358400)}.
     */
    @Test
    @Order(3)
    void tsmultirangeReturnsFormattedTimestamps() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT '{[\"2026-01-01 00:00:00\",\"2026-01-02 00:00:00\"),[\"2026-06-01 00:00:00\",\"2026-06-02 00:00:00\")}'::tsmultirange AS result")) {
            assertTrue(rs.next(), "Expected one row");
            String result = rs.getString("result");
            assertEquals(
                    "{[\"2026-01-01 00:00:00\",\"2026-01-02 00:00:00\"),[\"2026-06-01 00:00:00\",\"2026-06-02 00:00:00\")}",
                    result,
                    "tsmultirange should return human-readable timestamp strings, not epoch seconds");
        }
    }

    /**
     * Stmt 33: Casting int4multirange to int4range should fail with SQLSTATE 42846.
     * PG 18 returns ERROR [42846]: cannot cast type int4multirange to int4range
     * but Memgres returns ERROR [22P02]: malformed range literal: "{[1,10)}".
     */
    @Test
    @Order(4)
    void castMultirangeToRangeFailsWithCorrectSqlstate() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT '{[1,10)}'::int4multirange::int4range AS result");
            fail("Expected an error when casting int4multirange to int4range");
        } catch (SQLException e) {
            assertEquals("42846", e.getSQLState(),
                    "SQLSTATE should be 42846 (cannot_coerce), not 22P02 (invalid_text_representation)");
            assertTrue(e.getMessage().contains("cannot cast type int4multirange to int4range"),
                    "Error message should indicate that multirange-to-range cast is unsupported, got: " + e.getMessage());
        }
    }
}
