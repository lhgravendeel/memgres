package com.memgres.junit5;

/**
 * Controls when the Memgres database is reset between tests.
 */
public enum IsolationMode {

    /**
     * One Memgres instance shared across all tests in the class.
     * Init scripts run once before the first test. Fast but tests share state.
     */
    PER_CLASS,

    /**
     * A fresh Memgres instance for each test method.
     * Init scripts run before every test. Clean slate but slower.
     */
    PER_METHOD,

    /**
     * One Memgres instance shared across the entire JVM (all test classes).
     * Init scripts run once on first use. Stays alive until JVM shutdown.
     * Use with {@code snapshotAfterInit} and {@code restoreBeforeEach} for
     * integration test suites where an app connects to the database.
     */
    GLOBAL
}
