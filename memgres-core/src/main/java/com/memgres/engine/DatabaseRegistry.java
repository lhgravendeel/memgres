package com.memgres.engine;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry of named databases within a single Memgres server instance.
 * Each database is fully isolated: separate tables, schemas, sequences, etc.
 */
public class DatabaseRegistry {

    private final ConcurrentHashMap<String, Database> databases = new ConcurrentHashMap<>();
    private final String defaultDbName;
    private int maxConnections = 100;
    private boolean autoCreateDatabases = true;

    public DatabaseRegistry(String defaultDbName) {
        this.defaultDbName = defaultDbName;
        Database defaultDb = new Database();
        defaultDb.setMaxConnections(maxConnections);
        defaultDb.setDatabaseRegistry(this);
        databases.put(defaultDbName, defaultDb);
    }

    /** Returns the default database (for backward compatibility). */
    public Database getDefaultDatabase() {
        return databases.get(defaultDbName);
    }

    public String getDefaultDatabaseName() {
        return defaultDbName;
    }

    /** Look up a database by name (case-sensitive). Returns null if not found. */
    public Database getDatabase(String name) {
        return databases.get(name);
    }

    /** Create a new database. Returns false if it already exists. */
    public boolean createDatabase(String name) {
        Database db = new Database();
        db.setMaxConnections(maxConnections);
        db.setDatabaseRegistry(this);
        return databases.putIfAbsent(name, db) == null;
    }

    /** Drop a database. Returns false if it doesn't exist. */
    public boolean dropDatabase(String name) {
        return databases.remove(name) != null;
    }

    /** Rename a database. Returns false if old doesn't exist or new already exists. */
    public boolean renameDatabase(String oldName, String newName) {
        Database db = databases.get(oldName);
        if (db == null) return false;
        if (databases.putIfAbsent(newName, db) != null) return false;
        databases.remove(oldName);
        return true;
    }

    /** Check if a database exists. */
    public boolean exists(String name) {
        return databases.containsKey(name);
    }

    /** Get all database names. */
    public java.util.Set<String> getDatabaseNames() {
        return databases.keySet();
    }

    public void setMaxConnections(int max) {
        this.maxConnections = max;
        for (Database db : databases.values()) {
            db.setMaxConnections(max);
        }
    }

    public boolean isAutoCreateDatabases() {
        return autoCreateDatabases;
    }

    public void setAutoCreateDatabases(boolean autoCreateDatabases) {
        this.autoCreateDatabases = autoCreateDatabases;
    }
}
