package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * PG supports the + (union) operator for multirange types.
 * Memgres likely does NOT support + on multiranges, so these tests are
 * INTENDED TO FAIL until Memgres implements the + operator for multiranges,
 * matching PG behavior.
 */
public class MultirangeOpsCompatTest {

    private static Memgres memgres;
    private static Connection conn;

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

    @Test
    @DisplayName("int4multirange + int4multirange disjoint union")
    void testInt4MultirangeUnionDisjoint() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT '{[1,4)}'::int4multirange + '{[6,9)}'::int4multirange AS result")) {
            assertTrue(rs.next(), "Expected a result row");
            assertEquals("{[1,4),[6,9)}", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("int4multirange + int4multirange overlapping union merges")
    void testInt4MultirangeUnionOverlapping() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT '{[1,5)}'::int4multirange + '{[3,8)}'::int4multirange AS result")) {
            assertTrue(rs.next(), "Expected a result row");
            assertEquals("{[1,8)}", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("int4multirange + int4multirange adjacent union merges")
    void testInt4MultirangeUnionAdjacent() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT '{[1,3)}'::int4multirange + '{[3,6)}'::int4multirange AS result")) {
            assertTrue(rs.next(), "Expected a result row");
            assertEquals("{[1,6)}", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("int4multirange + int4range union")
    void testInt4MultirangeUnionWithRange() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT '{[1,4)}'::int4multirange + '[6,9)'::int4range AS result")) {
            assertTrue(rs.next(), "Expected a result row");
            assertEquals("{[1,4),[6,9)}", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("empty + non-empty int4multirange union")
    void testEmptyMultirangeUnion() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT '{}'::int4multirange + '{[1,5)}'::int4multirange AS result")) {
            assertTrue(rs.next(), "Expected a result row");
            assertEquals("{[1,5)}", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("int8multirange + int8multirange union")
    void testInt8MultirangeUnion() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT '{[1,5)}'::int8multirange + '{[3,10)}'::int8multirange AS result")) {
            assertTrue(rs.next(), "Expected a result row");
            assertEquals("{[1,10)}", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("nummultirange + nummultirange union")
    void testNumMultirangeUnion() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT '{[1.5,4.0)}'::nummultirange + '{[3.0,7.5)}'::nummultirange AS result")) {
            assertTrue(rs.next(), "Expected a result row");
            assertEquals("{[1.5,7.5)}", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("datemultirange + datemultirange union")
    void testDateMultirangeUnion() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT '{[2024-01-01,2024-03-01)}'::datemultirange + '{[2024-06-01,2024-09-01)}'::datemultirange AS result")) {
            assertTrue(rs.next(), "Expected a result row");
            assertEquals("{[2024-01-01,2024-03-01),[2024-06-01,2024-09-01)}", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("tsmultirange + tsmultirange union")
    void testTsMultirangeUnion() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT '{[\"2024-01-01\",\"2024-01-02\")}'::tsmultirange + '{[\"2024-01-02\",\"2024-01-03\")}'::tsmultirange AS result")) {
            assertTrue(rs.next(), "Expected a result row");
            assertEquals("{[\"2024-01-01 00:00:00\",\"2024-01-03 00:00:00\")}", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("combined + (union) and * (intersection) operators on multiranges")
    void testUnionAndIntersectionCombined() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT '{[1,5)}'::int4multirange + '{}'::int4multirange AS union_result, "
                 + "'{[1,5)}'::int4multirange * '{}'::int4multirange AS inter_result")) {
            assertTrue(rs.next(), "Expected a result row");
            assertEquals("{[1,5)}", rs.getString("union_result"));
            assertEquals("{}", rs.getString("inter_result"));
        }
    }
}
