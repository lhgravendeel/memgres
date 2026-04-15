package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class ArrayFunctionsCompatTest {
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

    @Test
    @DisplayName("array_fill with custom lower bounds should include bounds in output")
    void testArrayFillWithCustomLowerBounds() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT array_fill(5, ARRAY[3], ARRAY[2]) AS result")) {
            assertTrue(rs.next());
            assertEquals("[2:4]={5,5,5}", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("cardinality of 2D array should return total element count")
    void testCardinalityOf2DArray() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT cardinality(ARRAY[[1,2],[3,4]]) AS c2")) {
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("c2"));
        }
    }

    @Test
    @DisplayName("array_position should return NULL when element not found")
    void testArrayPositionNotFoundReturnsNull() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT array_position(ARRAY[1,2,3], 99) AS pos")) {
            assertTrue(rs.next());
            assertNull(rs.getObject("pos"));
        }
    }

    @Test
    @DisplayName("array_position with starting position should search from that index")
    void testArrayPositionWithStartingPosition() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT array_position(ARRAY[1,2,3,1,2,3], 1, 2) AS pos")) {
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("pos"));
        }
    }

    @Test
    @DisplayName("array_positions should return all positions of matching element")
    void testArrayPositionsExists() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT array_positions(ARRAY[1,2,3,1,2,3], 1) AS positions")) {
            assertTrue(rs.next());
            assertEquals("{1,4}", rs.getString("positions"));
        }
    }

    @Test
    @DisplayName("array_positions should return empty array when no matches")
    void testArrayPositionsNoMatches() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT array_positions(ARRAY[1,2,3], 99) AS positions")) {
            assertTrue(rs.next());
            assertEquals("{}", rs.getString("positions"));
        }
    }

    @Test
    @DisplayName("string_to_array with empty string replacement should produce NULLs")
    void testStringToArrayWithNullReplacement() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT string_to_array('a,,c', ',', '') AS result")) {
            assertTrue(rs.next());
            assertEquals("{a,NULL,c}", rs.getString("result"));
        }
    }

    @Test
    @DisplayName("string_to_array with NULL delimiter should split each character")
    void testStringToArrayNullDelimiterSplitsChars() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT string_to_array('abc', NULL) AS result")) {
            assertTrue(rs.next());
            assertEquals("{a,b,c}", rs.getString("result"));
        }
    }
}
