package com.memgres.junit5;

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that starts a Memgres instance for the test class.
 * Supports parameter injection of {@link java.sql.Connection},
 * {@link javax.sql.DataSource}, and {@link com.memgres.core.Memgres}
 * into test methods.
 *
 * <p>For more control (builder API, manual snapshot/restore),
 * use {@link MemgresExtension} with {@code @RegisterExtension} instead.</p>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(MemgresExtension.class)
public @interface MemgresTest {

    /**
     * SQL scripts to run on startup, in order. Classpath resources.
     */
    String[] initScripts() default {};

    /**
     * Directories containing SQL migration files to run in sorted order.
     * Classpath resources. Applied before {@link #initScripts()}.
     */
    String[] migrationDirs() default {};

    /**
     * Controls when the database is reset.
     */
    IsolationMode isolation() default IsolationMode.PER_CLASS;

    /**
     * If true, takes a snapshot after init scripts complete and
     * restores it before each test method. Useful for keeping
     * test data consistent across methods.
     */
    boolean snapshotAfterInit() default false;
}
