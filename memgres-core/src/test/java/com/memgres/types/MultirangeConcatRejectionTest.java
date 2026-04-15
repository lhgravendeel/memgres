package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PG 18 does NOT support the || (concat) operator for multirange types.
 * These tests verify Memgres correctly rejects || on multiranges.
 */
class MultirangeConcatRejectionTest {
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
    @DisplayName("int4multirange || int4multirange should error")
    void testMultirangeConcatInt4() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[1,4)}'::int4multirange || '{[6,9)}'::int4multirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("int4multirange || int4multirange overlapping should error")
    void testMultirangeConcatInt4Overlapping() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[1,5)}'::int4multirange || '{[3,8)}'::int4multirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("int4multirange || int4multirange adjacent should error")
    void testMultirangeConcatInt4Adjacent() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[1,3)}'::int4multirange || '{[3,6)}'::int4multirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("int4multirange || int4range should error")
    void testMultirangeConcatWithRange() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[1,4)}'::int4multirange || '[6,9)'::int4range AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("empty int4multirange || int4multirange should error")
    void testMultirangeConcatEmpty() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{}'::int4multirange || '{[1,5)}'::int4multirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("int8multirange || int8multirange should error")
    void testMultirangeConcatInt8() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[1,5)}'::int8multirange || '{[3,10)}'::int8multirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("nummultirange || nummultirange should error")
    void testMultirangeConcatNum() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[1.5,4.0)}'::nummultirange || '{[3.0,7.5)}'::nummultirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("datemultirange || datemultirange should error")
    void testMultirangeConcatDate() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[2024-01-01,2024-03-01)}'::datemultirange || '{[2024-06-01,2024-09-01)}'::datemultirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("tsmultirange || tsmultirange should error")
    void testMultirangeConcatTs() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[\"2024-01-01\",\"2024-01-02\")}'::tsmultirange || '{[\"2024-01-02\",\"2024-01-03\")}'::tsmultirange AS result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }

    @Test
    @DisplayName("int4multirange || combined with * should error on ||")
    void testMultirangeConcatCombined() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT '{[1,5)}'::int4multirange || '{}'::int4multirange AS union_result, "
                        + "'{[1,5)}'::int4multirange * '{}'::int4multirange AS inter_result");
            }
        });
        assertTrue(ex.getMessage().contains("operator does not exist"), ex.getMessage());
    }
}
