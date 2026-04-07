package com.memgres.junit5;

import com.memgres.core.Memgres;
import com.memgres.engine.util.IO;
import com.memgres.engine.util.Cols;
import com.memgres.engine.util.Strs;
import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Stream;

/**
 * JUnit 5 extension that manages a Memgres instance lifecycle.
 *
 * <p>Can be used via {@link MemgresTest @MemgresTest} annotation or
 * programmatically with {@link org.junit.jupiter.api.extension.RegisterExtension @RegisterExtension}:</p>
 *
 * <pre>{@code
 * @RegisterExtension
 * static MemgresExtension db = MemgresExtension.builder()
 *     .migrationDir("db/migrations")
 *     .initScript("test-data.sql")
 *     .isolation(IsolationMode.GLOBAL)
 *     .snapshotAfterInit(true)
 *     .restoreBeforeEach(true)
 *     .build();
 * }</pre>
 */
public class MemgresExtension implements BeforeAllCallback, AfterAllCallback,
        BeforeEachCallback, AfterEachCallback, ParameterResolver {

    private static final Logger LOG = LoggerFactory.getLogger(MemgresExtension.class);
    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(MemgresExtension.class);

    // Global singleton state (for IsolationMode.GLOBAL)
    private static Memgres globalInstance;
    private static boolean globalInitialized;
    private static final Object GLOBAL_LOCK = new Object();

    private final List<String> migrationDirs;
    private final List<String> initScripts;
    private final List<String> initStatements;
    private final IsolationMode isolation;
    private final int port;
    private final boolean snapshotAfterInit;
    private final boolean restoreBeforeEach;
    private final String systemProperty;

    // Instance state (managed by lifecycle callbacks)
    private Memgres memgres;

    private MemgresExtension(Builder builder) {
        this.migrationDirs = Cols.listCopyOf(builder.migrationDirs);
        this.initScripts = Cols.listCopyOf(builder.initScripts);
        this.initStatements = Cols.listCopyOf(builder.initStatements);
        this.isolation = builder.isolation;
        this.port = builder.port;
        this.snapshotAfterInit = builder.snapshotAfterInit;
        this.restoreBeforeEach = builder.restoreBeforeEach;
        this.systemProperty = builder.systemProperty;
    }

    // Default constructor for annotation-driven use
    MemgresExtension() {
        this.migrationDirs = Cols.listOf();
        this.initScripts = Cols.listOf();
        this.initStatements = Cols.listOf();
        this.isolation = IsolationMode.PER_CLASS;
        this.port = 0;
        this.snapshotAfterInit = false;
        this.restoreBeforeEach = false;
        this.systemProperty = null;
    }

    // --- Lifecycle callbacks ---

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        IsolationMode mode = resolveIsolation(context);
        if (mode == IsolationMode.GLOBAL) {
            startGlobalIfNeeded(context);
        } else if (mode == IsolationMode.PER_CLASS) {
            startAndInit(context);
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        IsolationMode mode = resolveIsolation(context);
        if (mode == IsolationMode.PER_METHOD) {
            startAndInit(context);
        } else if (resolveRestoreBeforeEach(context)) {
            restore();
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        IsolationMode mode = resolveIsolation(context);
        if (mode == IsolationMode.PER_METHOD) {
            stop();
        }
    }

    @Override
    public void afterAll(ExtensionContext context) {
        IsolationMode mode = resolveIsolation(context);
        // GLOBAL mode: never stop; the shutdown hook handles it
        if (mode != IsolationMode.GLOBAL) {
            stop();
        }
    }

    // --- Parameter resolution ---

    @Override
    public boolean supportsParameter(ParameterContext paramCtx, ExtensionContext extCtx) {
        Class<?> type = paramCtx.getParameter().getType();
        return type == Connection.class
                || type == DataSource.class
                || type == Memgres.class;
    }

    @Override
    public Object resolveParameter(ParameterContext paramCtx, ExtensionContext extCtx) {
        Class<?> type = paramCtx.getParameter().getType();
        if (type == Connection.class) {
            return createConnection(extCtx);
        } else if (type == DataSource.class) {
            return getDataSource();
        } else if (type == Memgres.class) {
            return memgres;
        }
        throw new ExtensionConfigurationException("Unsupported parameter type: " + type);
    }

    // --- Public accessors ---

    /**
     * Returns the JDBC URL for this Memgres instance.
     */
    public String getJdbcUrl() {
        ensureRunning();
        return memgres.getJdbcUrl();
    }

    /**
     * Returns a new JDBC connection to this Memgres instance.
     */
    public Connection getConnection() throws SQLException {
        ensureRunning();
        return DriverManager.getConnection(getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    /**
     * Returns a DataSource backed by this Memgres instance.
     */
    public DataSource getDataSource() {
        ensureRunning();
        return new MemgresDataSource(this);
    }

    /**
     * Returns the underlying Memgres instance.
     */
    public Memgres getMemgres() {
        ensureRunning();
        return memgres;
    }

    /**
     * Returns the port this Memgres instance is listening on.
     */
    public int getPort() {
        ensureRunning();
        return memgres.getPort();
    }

    // --- Snapshot / Restore (public API for manual use) ---

    /**
     * Captures a snapshot of all row data and sequence values.
     * Call after init scripts and/or app startup to establish a restore point.
     */
    public void snapshot() {
        ensureRunning();
        memgres.snapshot();
    }

    /**
     * Restores the database to the last snapshot.
     * Schema/DDL is preserved; only row data and sequence values are rolled back.
     */
    public void restore() {
        ensureRunning();
        memgres.restore();
    }

    // --- Manual lifecycle (for framework integration like Quarkus TestResource) ---

    /**
     * Manually starts the Memgres instance in global mode.
     * Use this when integrating with framework-specific test lifecycle managers
     * (e.g. Quarkus {@code TestResourceLifecycleManager}).
     *
     * <p>Does not run init scripts. Call {@link #runInitScripts()} separately
     * after your app has started (so app-managed migrations run first).</p>
     */
    public void startGlobal() {
        synchronized (GLOBAL_LOCK) {
            if (globalInstance == null) {
                globalInstance = Memgres.builder().port(port).build().start();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    if (globalInstance != null) {
                        globalInstance.close();
                    }
                }, "memgres-shutdown"));
                if (systemProperty != null) {
                    String url = globalInstance.getJdbcUrl();
                    System.setProperty(systemProperty, url);
                }
            }
            this.memgres = globalInstance;
        }
    }

    /**
     * Manually runs init scripts and inline statements.
     * Call after the app under test has started and run its own migrations.
     */
    public void runInitScripts() throws Exception {
        ensureRunning();
        try (Connection conn = getConnection()) {
            for (String dir : migrationDirs) {
                runMigrationDir(conn, dir);
            }
            for (String script : initScripts) {
                runScript(conn, script);
            }
            if (!initStatements.isEmpty()) {
                try (Statement stmt = conn.createStatement()) {
                    for (String sql : initStatements) {
                        stmt.execute(sql);
                    }
                }
            }
        }
    }

    // --- Internal ---

    private void startGlobalIfNeeded(ExtensionContext context) throws Exception {
        synchronized (GLOBAL_LOCK) {
            if (!globalInitialized) {
                startGlobal();
                runInit(context);
                if (resolveSnapshotAfterInit(context)) {
                    snapshot();
                }
                globalInitialized = true;
            } else {
                this.memgres = globalInstance;
            }
        }
    }

    private void startAndInit(ExtensionContext context) throws Exception {
        memgres = Memgres.builder().port(port).build().start();
        LOG.debug("Memgres started on port {} for {}", memgres.getPort(),
                context.getDisplayName());
        if (systemProperty != null) {
            System.setProperty(systemProperty, getJdbcUrl());
        }
        runInit(context);
        if (resolveSnapshotAfterInit(context)) {
            snapshot();
        }
    }

    private void stop() {
        if (memgres != null && memgres != globalInstance) {
            memgres.close();
        }
        memgres = null;
    }

    private void runInit(ExtensionContext context) throws Exception {
        List<String> dirs = resolveMigrationDirs(context);
        List<String> scripts = resolveInitScripts(context);
        List<String> stmts = this.initStatements;

        if (dirs.isEmpty() && scripts.isEmpty() && stmts.isEmpty()) {
            return;
        }

        // Use simple query mode for migration scripts, as they may contain
        // multiple statements, dollar-quoted blocks, etc. that require
        // the server-side statement splitter (simple query protocol).
        try (Connection conn = getConnection()) {
            for (String dir : dirs) {
                runMigrationDir(conn, dir);
            }
            for (String script : scripts) {
                runScript(conn, script);
            }
            if (!stmts.isEmpty()) {
                try (Statement stmt = conn.createStatement()) {
                    for (String sql : stmts) {
                        stmt.execute(sql);
                    }
                }
            }
        }
    }

    private void runMigrationDir(Connection conn, String dirPath) throws Exception {
        List<String> sqlFiles = listSqlFilesInDir(dirPath);
        if (sqlFiles.isEmpty()) {
            LOG.warn("No .sql files found in migration dir: {}", dirPath);
            return;
        }
        LOG.debug("Running {} migration files from {}", sqlFiles.size(), dirPath);
        for (String file : sqlFiles) {
            String sql = readClasspathResource(file);
            executeSql(conn, sql);
        }
    }

    private void runScript(Connection conn, String scriptPath) throws Exception {
        LOG.debug("Running init script: {}", scriptPath);
        String sql = readClasspathResource(scriptPath);
        executeSql(conn, sql);
    }

    private void executeSql(Connection conn, String sql) throws SQLException {
        // Split multi-statement SQL ourselves to handle dollar-quoting
        // (including bare $ delimiters) that the JDBC driver doesn't understand.
        List<String> statements = splitSqlStatements(sql);
        try (Statement stmt = conn.createStatement()) {
            for (int idx = 0; idx < statements.size(); idx++) {
                // Normalize bare $ dollar-quotes to $$ so the JDBC driver's parser handles them
                String s = normalizeBareDollarQuotes(statements.get(idx));
                if (!Strs.isBlank(s)) {
                    try {
                        stmt.execute(s);
                    } catch (SQLException e) {
                        String preview = s.replaceAll("\\s+", " ").trim();
                        if (preview.length() > 200) preview = preview.substring(0, 200) + "...";
                        LOG.debug("Failed SQL statement #{} ({} chars): {}", idx + 1, s.length(), preview);
                        throw new SQLException(e.getMessage() + " [SQL: " + preview + "]", e.getSQLState(), e);
                    }
                }
            }
        }
    }

    /**
     * Replaces bare $ dollar-quote delimiters with $$ so the PG JDBC driver
     * recognizes them. The JDBC driver's internal parser only handles $$ and $tag$.
     */
    static String normalizeBareDollarQuotes(String sql) {
        StringBuilder result = new StringBuilder(sql.length() + 10);
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '$') {
                // Check if this is a bare $ (not part of $$ or $tag$)
                // Look ahead: is next char $ or alphanumeric/underscore?
                int next = i + 1;
                if (next < sql.length() && (sql.charAt(next) == '$'
                        || Character.isLetterOrDigit(sql.charAt(next))
                        || sql.charAt(next) == '_')) {
                    // Part of $$ or $tag$, so skip through the whole delimiter
                    result.append(c);
                } else {
                    // Bare $: replace with $$
                    result.append("$$");
                }
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    /**
     * Splits SQL on semicolons, respecting dollar-quoted strings ($$, $tag$, and bare $),
     * single-quoted strings, and line comments.
     */
    static List<String> splitSqlStatements(String sql) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        char stringChar = 0;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            // Dollar quoting: $$, $tag$, or bare $ (followed by whitespace)
            if (!inString && c == '$') {
                int j = i + 1;
                while (j < sql.length() && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) {
                    j++;
                }
                if (j < sql.length() && sql.charAt(j) == '$') {
                    String delimiter = sql.substring(i, j + 1);
                    current.append(delimiter);
                    i = j + 1;
                    int close = sql.indexOf(delimiter, i);
                    if (close >= 0) {
                        current.append(sql, i, close + delimiter.length());
                        i = close + delimiter.length() - 1;
                    } else {
                        current.append(sql.substring(i));
                        i = sql.length() - 1;
                    }
                    continue;
                }
                if (j == i + 1 && j < sql.length() && Character.isWhitespace(sql.charAt(j))) {
                    current.append(c);
                    i = j;
                    int close = -1;
                    for (int k = i; k < sql.length(); k++) {
                        if (sql.charAt(k) == '$') {
                            if (k + 1 >= sql.length() || sql.charAt(k + 1) == ';'
                                    || Character.isWhitespace(sql.charAt(k + 1))) {
                                close = k;
                                break;
                            }
                        }
                    }
                    if (close >= 0) {
                        current.append(sql, i, close + 1);
                        i = close;
                    } else {
                        current.append(sql.substring(i));
                        i = sql.length() - 1;
                    }
                    continue;
                }
                current.append(c);
                continue;
            }

            if (inString) {
                current.append(c);
                if (c == stringChar) {
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == stringChar) {
                        current.append(sql.charAt(++i));
                    } else {
                        inString = false;
                    }
                }
            } else if (c == '\'' || c == '"') {
                inString = true;
                stringChar = c;
                current.append(c);
            } else if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                int eol = sql.indexOf('\n', i);
                if (eol < 0) eol = sql.length();
                current.append(sql, i, Math.min(eol + 1, sql.length()));
                i = eol;
            } else if (c == ';') {
                String stmt = current.toString().trim();
                if (!stmt.isEmpty()) {
                    result.add(stmt);
                }
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        String last = current.toString().trim();
        if (!last.isEmpty()) {
            result.add(last);
        }
        return result;
    }

    /**
     * Lists .sql files in a classpath directory, sorted by name.
     * Supports Flyway-style naming (V001__create.sql, V002__alter.sql)
     * since alphabetical sort matches version order.
     */
    static List<String> listSqlFilesInDir(String dirPath) throws IOException, URISyntaxException {
        String normalized = dirPath.startsWith("/") ? dirPath.substring(1) : dirPath;
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Enumeration<URL> urls = cl.getResources(normalized);

        List<String> result = new ArrayList<>();
        while (urls.hasMoreElements()) {
            URL url = urls.nextElement();
            if ("file".equals(url.getProtocol())) {
                collectFromFileSystem(Paths.get(url.toURI()), normalized, result);
            } else if ("jar".equals(url.getProtocol())) {
                collectFromJar(url, normalized, result);
            }
        }

        Collections.sort(result);
        return result;
    }

    private static void collectFromFileSystem(Path dir, String basePath, List<String> result) throws IOException {
        if (!Files.isDirectory(dir)) return;
        try (Stream<Path> files = Files.list(dir)) {
            files.filter(p -> p.toString().endsWith(".sql"))
                    .sorted()
                    .forEach(p -> result.add(basePath + "/" + p.getFileName().toString()));
        }
    }

    private static void collectFromJar(URL jarUrl, String basePath, List<String> result) throws IOException, URISyntaxException {
        URI jarUri = new URI(jarUrl.toString().split("!")[0]);
        try (FileSystem fs = FileSystems.newFileSystem(jarUri, Cols.mapOf())) {
            Path dir = fs.getPath(basePath);
            if (!Files.isDirectory(dir)) return;
            try (Stream<Path> files = Files.list(dir)) {
                files.filter(p -> p.toString().endsWith(".sql"))
                        .sorted()
                        .forEach(p -> result.add(basePath + "/" + p.getFileName().toString()));
            }
        }
    }

    static String readClasspathResource(String path) throws IOException {
        String normalized = path.startsWith("/") ? path.substring(1) : path;
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try (InputStream is = cl.getResourceAsStream(normalized)) {
            if (is == null) {
                throw new FileNotFoundException("Classpath resource not found: " + path);
            }
            return new String(IO.readAllBytes(is), StandardCharsets.UTF_8);
        }
    }

    private IsolationMode resolveIsolation(ExtensionContext context) {
        // Builder config takes precedence
        if (this.isolation != null && (this.isolation != IsolationMode.PER_CLASS
                || !migrationDirs.isEmpty() || !initScripts.isEmpty() || !initStatements.isEmpty())) {
            return this.isolation;
        }
        // Check annotation
        return context.getTestClass()
                .map(c -> c.getAnnotation(MemgresTest.class))
                .map(MemgresTest::isolation)
                .orElse(this.isolation != null ? this.isolation : IsolationMode.PER_CLASS);
    }

    private boolean resolveSnapshotAfterInit(ExtensionContext context) {
        if (snapshotAfterInit) return true;
        return context.getTestClass()
                .map(c -> c.getAnnotation(MemgresTest.class))
                .map(MemgresTest::snapshotAfterInit)
                .orElse(false);
    }

    private boolean resolveRestoreBeforeEach(ExtensionContext context) {
        if (restoreBeforeEach) return true;
        // If snapshotAfterInit is set via annotation, auto-enable restore
        return resolveSnapshotAfterInit(context);
    }

    private List<String> resolveMigrationDirs(ExtensionContext context) {
        if (!migrationDirs.isEmpty()) return migrationDirs;
        return context.getTestClass()
                .map(c -> c.getAnnotation(MemgresTest.class))
                .map(a -> Cols.listOf(a.migrationDirs()))
                .orElse(Cols.listOf());
    }

    private List<String> resolveInitScripts(ExtensionContext context) {
        if (!initScripts.isEmpty()) return initScripts;
        return context.getTestClass()
                .map(c -> c.getAnnotation(MemgresTest.class))
                .map(a -> Cols.listOf(a.initScripts()))
                .orElse(Cols.listOf());
    }

    private Connection createConnection(ExtensionContext context) {
        try {
            Connection conn = getConnection();
            context.getStore(NAMESPACE)
                    .getOrComputeIfAbsent("connections_" + context.getUniqueId(),
                            k -> new ConnectionCleanup(), ConnectionCleanup.class)
                    .add(conn);
            return conn;
        } catch (SQLException e) {
            throw new ExtensionConfigurationException("Failed to create connection", e);
        }
    }

    private void ensureRunning() {
        if (memgres == null) {
            throw new IllegalStateException(
                    "Memgres is not running. Ensure the extension lifecycle is active.");
        }
    }

    // --- Connection cleanup via ExtensionContext.Store ---

    private static class ConnectionCleanup implements ExtensionContext.Store.CloseableResource {
        private final List<Connection> connections = new ArrayList<>();

        void add(Connection conn) {
            connections.add(conn);
        }

        @Override
        public void close() throws Exception {
            for (Connection conn : connections) {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            }
        }
    }

    // --- Static reset for testing (package-private) ---

    static void resetGlobalState() {
        synchronized (GLOBAL_LOCK) {
            if (globalInstance != null) {
                globalInstance.close();
                globalInstance = null;
            }
            globalInitialized = false;
        }
    }

    // --- Builder ---

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<String> migrationDirs = new ArrayList<>();
        private final List<String> initScripts = new ArrayList<>();
        private final List<String> initStatements = new ArrayList<>();
        private IsolationMode isolation = IsolationMode.PER_CLASS;
        private int port = 0;
        private boolean snapshotAfterInit = false;
        private boolean restoreBeforeEach = false;
        private String systemProperty = null;

        /**
         * Add a classpath directory containing .sql migration files.
         * Files are executed in sorted (alphabetical) order.
         * Multiple dirs are executed in the order they are added.
         * Runs before init scripts.
         */
        public Builder migrationDir(String classpathDir) {
            migrationDirs.add(classpathDir);
            return this;
        }

        /**
         * Add a classpath SQL script to run after migrations.
         * Multiple scripts are executed in the order they are added.
         */
        public Builder initScript(String classpathScript) {
            initScripts.add(classpathScript);
            return this;
        }

        /**
         * Add inline SQL statements to execute after scripts.
         */
        public Builder initStatements(String... statements) {
            initStatements.addAll(Cols.listOf(statements));
            return this;
        }

        /**
         * Set the isolation mode. Default is {@link IsolationMode#PER_CLASS}.
         */
        public Builder isolation(IsolationMode mode) {
            this.isolation = mode;
            return this;
        }

        /**
         * Set a specific port. Default is 0 (random available port).
         */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * If true, captures a snapshot after init scripts complete.
         * Use with {@link #restoreBeforeEach(boolean)} to auto-restore.
         */
        public Builder snapshotAfterInit(boolean snapshot) {
            this.snapshotAfterInit = snapshot;
            return this;
        }

        /**
         * If true, restores the snapshot before each test method.
         * Requires a snapshot to have been taken (via {@link #snapshotAfterInit(boolean)}
         * or a manual {@link MemgresExtension#snapshot()} call).
         */
        public Builder restoreBeforeEach(boolean restore) {
            this.restoreBeforeEach = restore;
            return this;
        }

        /**
         * Sets a system property to the JDBC URL when Memgres starts.
         * Useful for passing the URL to an app under test.
         */
        public Builder systemProperty(String propertyName) {
            this.systemProperty = propertyName;
            return this;
        }

        public MemgresExtension build() {
            return new MemgresExtension(this);
        }
    }
}
