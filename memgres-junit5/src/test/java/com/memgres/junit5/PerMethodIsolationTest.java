package com.memgres.junit5;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests PER_METHOD isolation: each test gets a fresh database.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PerMethodIsolationTest {

    @RegisterExtension
    static MemgresExtension db = MemgresExtension.builder()
            .initStatements("CREATE TABLE counter (val INTEGER)")
            .isolation(IsolationMode.PER_METHOD)
            .build();

    @Test
    @Order(1)
    void firstTestInsertsRow() throws SQLException {
        try (Connection conn = db.getConnection()) {
            conn.createStatement().execute("INSERT INTO counter (val) VALUES (1)");
            try (ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM counter")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    @Order(2)
    void secondTestSeesEmptyTable() throws SQLException {
        // PER_METHOD means this is a fresh database, so the insert from test 1 is gone
        try (Connection conn = db.getConnection()) {
            try (ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM counter")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }
}
