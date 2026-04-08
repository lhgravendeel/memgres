package com.memgres.core;

import com.memgres.engine.Database;
import com.memgres.engine.DatabaseRegistry;
import com.memgres.engine.DatabaseSnapshot;
import com.memgres.pgwire.PgWireServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * Main entry point for the in-memory PostgreSQL-compatible database.
 * Users create a Memgres instance, start it, and connect via standard JDBC.
 *
 * <pre>{@code
 * try (Memgres db = Memgres.builder().port(0).build().start()) {
 *     String jdbcUrl = db.getJdbcUrl();
 *     // use any PostgreSQL JDBC driver to connect
 * }
 * }</pre>
 */
public class Memgres implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(Memgres.class);

    /**
     * @deprecated Use {@link Builder#logAllStatements(boolean)} instead.
     */
    @Deprecated(forRemoval = true)
    public static boolean logAllStatements = false;

    private final int requestedPort;
    private final int maxConnections;
    private final String bindAddress;
    private final boolean logStatements;
    private final DatabaseRegistry registry;
    private final Database database;
    private PgWireServer server;
    private int actualPort;
    private DatabaseSnapshot snapshot;

    private Memgres(int port, int maxConnections, String bindAddress, boolean logStatements) {
        this.requestedPort = port;
        this.maxConnections = maxConnections;
        this.bindAddress = bindAddress;
        this.logStatements = logStatements;
        this.registry = new DatabaseRegistry("memgres");
        this.registry.setMaxConnections(maxConnections);
        this.database = registry.getDefaultDatabase();
    }

    public Memgres start() {
        // Instance setting takes precedence; fall back to deprecated static field
        if (logStatements) logAllStatements = true;
        server = new PgWireServer(registry);
        actualPort = server.start(requestedPort, bindAddress);
        LOG.info("Memgres started on port {}", actualPort);
        return this;
    }

    public int getPort() {
        return actualPort;
    }

    public String getJdbcUrl() {
        return "jdbc:postgresql://localhost:" + actualPort + "/memgres";
    }

    public String getUser() {
        return "memgres";
    }

    public String getPassword() {
        return "memgres";
    }

    public Database getDatabase() {
        return database;
    }

    /**
     * Captures a snapshot of all row data and sequence values.
     * Call after init scripts have run to establish a restore point.
     */
    public void snapshot() {
        this.snapshot = DatabaseSnapshot.capture(database);
        LOG.debug("Database snapshot captured");
    }

    /**
     * Restores the database to the last snapshot.
     * Schema/DDL is preserved; only row data and sequence values are rolled back.
     */
    public void restore() {
        if (snapshot == null) {
            throw new IllegalStateException("No snapshot to restore. Call snapshot() first.");
        }
        snapshot.restore(database);
        LOG.debug("Database restored to snapshot");
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop();
            LOG.info("Memgres stopped");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int port = 0; // 0 = random available port
        private int maxConnections = 100;
        private String bindAddress = "localhost";
        private boolean logAllStatements = false;

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * Set the maximum number of concurrent connections.
         * Default is 100. Use 0 for unlimited.
         */
        public Builder maxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
            return this;
        }

        /**
         * Set the address to bind the server to.
         * Default is {@code "localhost"} (loopback only).
         * Use {@code "0.0.0.0"} to listen on all interfaces.
         */
        public Builder bindAddress(String bindAddress) {
            this.bindAddress = bindAddress;
            return this;
        }

        /**
         * If true, logs every SQL statement and protocol message at INFO level.
         * Useful for debugging. Default is false.
         */
        public Builder logAllStatements(boolean logAllStatements) {
            this.logAllStatements = logAllStatements;
            return this;
        }

        public Memgres build() {
            return new Memgres(port, maxConnections, bindAddress, logAllStatements);
        }
    }
}
