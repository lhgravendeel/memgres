package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document section 2 (Java/JDBC): ResultSet metadata and column-label behavior.
 * Tests getColumnType, getColumnTypeName, getColumnLabel, getColumnName,
 * quoted/unquoted aliases, duplicate labels, mixed-case labels.
 */
class ResultSetMetadataTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        conn.createStatement().execute("CREATE TABLE md_t(id int PRIMARY KEY, name text, price numeric(10,2), active boolean, created timestamp)");
        conn.createStatement().execute("INSERT INTO md_t VALUES (1, 'widget', 19.99, true, '2024-01-15 10:00:00')");
    }
    @AfterAll static void tearDown() throws Exception {
        if (conn != null) { conn.createStatement().execute("DROP TABLE IF EXISTS md_t"); conn.close(); }
        if (memgres != null) memgres.close();
    }

    @Test void column_type_integer() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT id FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(Types.INTEGER, md.getColumnType(1));
            assertEquals("int4", md.getColumnTypeName(1));
        }
    }

    @Test void column_type_text() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT name FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(Types.VARCHAR, md.getColumnType(1));
        }
    }

    @Test void column_type_numeric() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT price FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(Types.NUMERIC, md.getColumnType(1));
        }
    }

    @Test void column_type_boolean() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT active FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            int type = md.getColumnType(1);
            assertTrue(type == Types.BOOLEAN || type == Types.BIT, "Boolean column type: " + type);
        }
    }

    @Test void column_type_timestamp() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT created FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(Types.TIMESTAMP, md.getColumnType(1));
        }
    }

    @Test void column_label_unquoted_alias() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT id AS item_id FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("item_id", md.getColumnLabel(1));
        }
    }

    @Test void column_label_quoted_alias() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT id AS \"ItemID\" FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("ItemID", md.getColumnLabel(1));
        }
    }

    @Test void column_label_no_alias() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT id FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("id", md.getColumnLabel(1));
        }
    }

    @Test void column_label_expression() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT id + 1 FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            // PG names expression columns with ?column? or the expression
            assertNotNull(md.getColumnLabel(1));
        }
    }

    @Test void column_label_expression_with_alias() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT id + 1 AS next_id FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("next_id", md.getColumnLabel(1));
        }
    }

    @Test void duplicate_column_labels() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id AS x, name AS x FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(2, md.getColumnCount());
            assertEquals("x", md.getColumnLabel(1));
            assertEquals("x", md.getColumnLabel(2));
        }
    }

    @Test void mixed_case_labels() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id AS \"MyId\", name AS lower_name FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("MyId", md.getColumnLabel(1));
            assertEquals("lower_name", md.getColumnLabel(2));
        }
    }

    @Test void column_count_multiple() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(5, md.getColumnCount());
        }
    }

    @Test void column_name_vs_label() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT id AS alias FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("alias", md.getColumnLabel(1));
            // getColumnName may return the underlying column name
            String colName = md.getColumnName(1);
            assertNotNull(colName);
        }
    }

    @Test void function_call_column_label() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("count", md.getColumnLabel(1));
        }
    }

    @Test void function_call_with_alias() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) AS total FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("total", md.getColumnLabel(1));
        }
    }

    @Test void nullable_metadata() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT id, name FROM md_t")) {
            ResultSetMetaData md = rs.getMetaData();
            // PK column should be non-nullable
            assertEquals(ResultSetMetaData.columnNoNulls, md.isNullable(1));
        }
    }
}
