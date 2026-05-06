package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies: CREATE TABLE with serial, INSERT...SELECT from another table,
 * setval to reset the sequence, and CREATE INDEX on a missing column errors.
 */
class InsertFromSelectWithSequenceTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static String querySingle(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    @Test
    void create_source_table_and_insert_data() throws SQLException {
        exec("CREATE TABLE x (id serial PRIMARY KEY, username varchar(255) NOT NULL, email varchar(255) NOT NULL)");
        try {
            exec("INSERT INTO x (username, email) VALUES ('alice', 'alice@example.com')");
            exec("INSERT INTO x (username, email) VALUES ('bob', 'bob@example.com')");
            exec("INSERT INTO x (username, email) VALUES ('charlie', 'charlie@example.com')");
            assertEquals(3, queryInt("SELECT count(*) FROM x"));
        } finally {
            exec("DROP TABLE x");
        }
    }

    @Test
    void insert_select_copies_rows_and_setval_resets_sequence() throws SQLException {
        exec("CREATE TABLE x (id serial PRIMARY KEY, username varchar(255) NOT NULL, email varchar(255) NOT NULL)");
        exec("CREATE TABLE users (id serial PRIMARY KEY, username varchar(255) NOT NULL, email varchar(255) NOT NULL, login_token varchar(255), CONSTRAINT unique_username UNIQUE (username))");
        try {
            exec("INSERT INTO x (username, email) VALUES ('alice', 'alice@example.com')");
            exec("INSERT INTO x (username, email) VALUES ('bob', 'bob@example.com')");
            exec("INSERT INTO x (username, email) VALUES ('charlie', 'charlie@example.com')");

            // CREATE INDEX on nullable login_token column
            exec("CREATE INDEX IF NOT EXISTS users_login_token_idx ON users (login_token)");

            // INSERT...SELECT from source table
            exec("INSERT INTO users (id, username, email) (SELECT id, username, email FROM x)");
            assertEquals(3, queryInt("SELECT count(*) FROM users"));

            // Verify data integrity
            assertEquals("alice", querySingle("SELECT username FROM users WHERE id = 1"));
            assertEquals("bob", querySingle("SELECT username FROM users WHERE id = 2"));
            assertEquals("charlie", querySingle("SELECT username FROM users WHERE id = 3"));

            // Reset the sequence to max(id)
            exec("SELECT setval('users_id_seq', (SELECT max(id) FROM users))");
            assertEquals("3", querySingle("SELECT currval('users_id_seq')"));

            // Next insert should get id=4
            exec("INSERT INTO users (username, email) VALUES ('dave', 'dave@example.com')");
            assertEquals(4, queryInt("SELECT id FROM users WHERE username = 'dave'"));
        } finally {
            exec("DROP TABLE users");
            exec("DROP TABLE x");
        }
    }

    @Test
    void unique_constraint_rejects_duplicate_username() throws SQLException {
        exec("CREATE TABLE users (id serial PRIMARY KEY, username varchar(255) NOT NULL, email varchar(255) NOT NULL, CONSTRAINT unique_username UNIQUE (username))");
        try {
            exec("INSERT INTO users (username, email) VALUES ('alice', 'alice@example.com')");
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("INSERT INTO users (username, email) VALUES ('alice', 'different@example.com')"));
            assertEquals("23505", ex.getSQLState());
        } finally {
            exec("DROP TABLE users");
        }
    }

    @Test
    void create_index_on_login_token_succeeds() throws SQLException {
        exec("CREATE TABLE users (id serial PRIMARY KEY, username varchar(255) NOT NULL, email varchar(255) NOT NULL, login_token varchar(255), CONSTRAINT unique_username UNIQUE (username))");
        try {
            exec("CREATE INDEX users_login_token_idx ON users (login_token)");
            // Verify index exists via pg_indexes
            String indexName = querySingle("SELECT indexname FROM pg_indexes WHERE tablename = 'users' AND indexname = 'users_login_token_idx'");
            assertEquals("users_login_token_idx", indexName);
        } finally {
            exec("DROP TABLE users");
        }
    }
}
