package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.CreateTypeStmt;
import com.memgres.engine.parser.ast.Statement;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * In-memory database engine. Holds schemas, tables, custom types, functions, triggers, sequences, and views.
 */
public class Database {

    private final Map<String, Schema> schemas = new ConcurrentHashMap<>();
    private final Map<String, CustomEnum> customEnums = new ConcurrentHashMap<>();
    private final Map<String, PgFunction> functions = new ConcurrentHashMap<>();
    private final Map<String, List<PgFunction>> functionOverloads = new ConcurrentHashMap<>();
    private final Map<String, List<PgTrigger>> triggers = new ConcurrentHashMap<>();
    private final Map<String, Sequence> sequences = new ConcurrentHashMap<>();
    private final Map<String, ViewDef> views = new ConcurrentHashMap<>();
    private final Map<String, DomainType> domains = new ConcurrentHashMap<>();
    private final Map<String, List<CreateTypeStmt.CompositeField>> compositeTypes = new ConcurrentHashMap<>();
    private final Map<String, PgAggregate> userAggregates = new ConcurrentHashMap<>();
    private final Map<String, PgOperator> userOperators = new ConcurrentHashMap<>();
    private final Map<String, PgOperatorFamily> userOperatorFamilies = new ConcurrentHashMap<>();
    private final Map<String, PgOperatorClass> userOperatorClasses = new ConcurrentHashMap<>();
    private final Map<String, List<String>> indexColumns = new ConcurrentHashMap<>();
    private final Map<String, String> indexTableNames = new ConcurrentHashMap<>(); // index name → schema.table
    private final Map<String, Boolean> indexUniqueFlags = new ConcurrentHashMap<>(); // index name → is unique
    private final Map<String, String> indexWhereClauses = new ConcurrentHashMap<>(); // index name → WHERE predicate
    private final Map<String, String> indexMethods = new ConcurrentHashMap<>(); // index name → access method (btree, hash, etc.)
    private final Map<String, Map<String, String>> indexReloptions = new ConcurrentHashMap<>(); // index name → storage params
    private final NotificationManager notificationManager = new NotificationManager();
    private DatabaseRegistry databaseRegistry;

    // Row locks: maps table name to (row identity -> list of lock entries)
    private final Map<String, Map<Object[], List<LockEntry>>> rowLocks = new ConcurrentHashMap<>();

    // Wait-for graph: maps a waiting session to the session it is waiting for (for deadlock detection)
    private final Map<Session, Session> waitingFor = new ConcurrentHashMap<>();

    /** A single row-lock entry recording the holding session and the requested lock mode. */
    public static class LockEntry {
        public final Session session;
        public final String mode; // "UPDATE", "NO KEY UPDATE", "SHARE", "KEY SHARE"

        public LockEntry(Session session, String mode) {
            this.session = session;
            this.mode = mode;
        }
    }

    /**
     * Returns true if two lock modes are compatible (can coexist on the same row).
     * Compatibility matrix (PostgreSQL semantics):
     * <pre>
     *                  FOR KEY SHARE  FOR SHARE  FOR NO KEY UPDATE  FOR UPDATE
     * FOR KEY SHARE         ✓             ✓             ✓              ✗
     * FOR SHARE             ✓             ✓             ✗              ✗
     * FOR NO KEY UPDATE     ✓             ✗             ✗              ✗
     * FOR UPDATE            ✗             ✗             ✗              ✗
     * </pre>
     */
    private static boolean lockModesCompatible(String modeA, String modeB) {
        if ("UPDATE".equals(modeA) || "UPDATE".equals(modeB)) {
            return false;
        }
        // NO KEY UPDATE is only compatible with KEY SHARE
        if ("NO KEY UPDATE".equals(modeA) || "NO KEY UPDATE".equals(modeB)) {
            return "KEY SHARE".equals(modeA) || "KEY SHARE".equals(modeB);
        }
        // SHARE is compatible with SHARE and KEY SHARE
        // KEY SHARE is compatible with KEY SHARE, SHARE, and NO KEY UPDATE (handled above)
        return true;
    }

    // Advisory locks: key -> set of sessions holding the lock
    private final Map<Long, Set<Session>> advisoryLocks = new ConcurrentHashMap<>();

    // Roles: name -> attributes map
    private final Map<String, Map<String, String>> roles = new ConcurrentHashMap<>();

    // Role memberships: granted role (lowercase) -> set of member roles (lowercase)
    private final Map<String, Set<String>> roleMemberships = new ConcurrentHashMap<>();

    // Granted privileges: role (lowercase) -> set of "privilege:objectType:objectName" entries
    private final Map<String, Set<String>> rolePrivileges = new ConcurrentHashMap<>();

    // Rules: "table:EVENT" -> rule type (e.g., "INSTEAD_NOTHING")
    private final Map<String, String> rules = new ConcurrentHashMap<>();

    // Object comments: "objectType:objectName" -> comment text
    private final Map<String, String> comments = new ConcurrentHashMap<>();

    // Schema object registry: maps schema name -> set of "type:name" entries
    // Used by DROP SCHEMA CASCADE to find objects belonging to a schema.
    private final Map<String, Set<String>> schemaObjectRegistry = new ConcurrentHashMap<>();

    // Object ownership: "objectType:objectName" -> owner role name (lowercase)
    private final Map<String, String> objectOwners = new ConcurrentHashMap<>();

    // Default ACL entries from ALTER DEFAULT PRIVILEGES
    private final List<DefaultAclEntry> defaultAcls = new ArrayList<>();

    /** An entry recorded by ALTER DEFAULT PRIVILEGES. */
    public static class DefaultAclEntry {
        public final String grantor;    // role that issued the statement (null = current user placeholder)
        public final String schema;     // IN SCHEMA value (may be null)
        public final String objectType; // "TABLES", "SEQUENCES", "FUNCTIONS", "TYPES", "SCHEMAS"
        public final List<String> privileges;
        public final List<String> grantees;
        public final boolean isGrant;

        public DefaultAclEntry(String grantor, String schema, String objectType,
                               List<String> privileges, List<String> grantees, boolean isGrant) {
            this.grantor = grantor;
            this.schema = schema;
            this.objectType = objectType;
            this.privileges = privileges;
            this.grantees = grantees;
            this.isGrant = isGrant;
        }
    }

    public void addDefaultAcl(DefaultAclEntry entry) { defaultAcls.add(entry); }

    public void removeDefaultAcl(String schema, String objectType, List<String> grantees) {
        defaultAcls.removeIf(e -> e.isGrant
                && objectTypeMatches(e.objectType, objectType)
                && schemaMatches(e.schema, schema)
                && grantees.stream().anyMatch(g -> e.grantees.contains(g)));
    }

    private static boolean objectTypeMatches(String a, String b) {
        return a != null && a.equalsIgnoreCase(b);
    }

    private static boolean schemaMatches(String a, String b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.equalsIgnoreCase(b);
    }

    public List<DefaultAclEntry> getDefaultAcls() { return defaultAcls; }

    // Active sessions registry (for MVCC visibility)
    private final Set<Session> activeSessions = ConcurrentHashMap.newKeySet();

    // Connection tracking
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private volatile int maxConnections = 100;

    public Database() {
        schemas.put("public", new Schema("public"));
        // Default superuser roles (similar to PG's postgres role)
        createRole("memgres", Cols.mapOf("SUPERUSER", "true", "LOGIN", "true"));
        createRole("postgres", Cols.mapOf("SUPERUSER", "true", "LOGIN", "true"));
        createRole("test", Cols.mapOf("SUPERUSER", "true", "LOGIN", "true"));
        // Register built-in trigger functions
        PgFunction suppressFunc = new PgFunction("suppress_redundant_updates_trigger", "trigger",
                "-- built-in: suppresses redundant updates", "internal");
        suppressFunc.setSchemaName("pg_catalog");
        addFunction(suppressFunc);
    }

    // ---- Connection management ----

    public void setMaxConnections(int max) {
        this.maxConnections = max;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    /**
     * Try to register a new connection. Returns true if the connection is accepted,
     * false if the max connection limit has been reached.
     */
    public boolean registerConnection() {
        while (true) {
            int current = activeConnections.get();
            if (current >= maxConnections) {
                return false;
            }
            if (activeConnections.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /**
     * Unregister a connection (on disconnect).
     */
    public void unregisterConnection() {
        activeConnections.decrementAndGet();
    }

    // ---- Session registry for MVCC ----

    public void registerSession(Session session) {
        activeSessions.add(session);
    }

    public void unregisterSession(Session session) {
        activeSessions.remove(session);
    }

    /** Get all active sessions (for MVCC visibility checks). */
    public Set<Session> getActiveSessions() {
        return activeSessions;
    }

    /** Get advisory locks map (for pg_locks). */
    public Map<Long, Set<Session>> getAdvisoryLocks() {
        return advisoryLocks;
    }

    public Schema getSchema(String name) {
        return schemas.get(name);
    }

    public Schema getOrCreateSchema(String name) {
        return schemas.computeIfAbsent(name, Schema::new);
    }

    public Map<String, Schema> getSchemas() {
        return schemas;
    }

    public void removeSchema(String name) {
        schemas.remove(name);
    }

    public NotificationManager getNotificationManager() {
        return notificationManager;
    }

    public DatabaseRegistry getDatabaseRegistry() {
        return databaseRegistry;
    }

    public void setDatabaseRegistry(DatabaseRegistry databaseRegistry) {
        this.databaseRegistry = databaseRegistry;
    }

    // Custom ENUM types
    public void addCustomEnum(CustomEnum customEnum) {
        customEnums.put(customEnum.getName().toLowerCase(), customEnum);
    }

    public CustomEnum getCustomEnum(String name) {
        return customEnums.get(name.toLowerCase());
    }

    public boolean isCustomEnum(String typeName) {
        return customEnums.containsKey(typeName.toLowerCase());
    }

    public void replaceCustomEnum(CustomEnum e) {
        customEnums.put(e.getName().toLowerCase(), e);
    }

    public void removeCustomEnum(String name) {
        customEnums.remove(name.toLowerCase());
    }

    public Map<String, CustomEnum> getCustomEnums() {
        return customEnums;
    }

    // Composite types
    public void addCompositeType(String name, List<CreateTypeStmt.CompositeField> fields) {
        compositeTypes.put(name.toLowerCase(), fields);
    }

    public boolean isCompositeType(String name) {
        return compositeTypes.containsKey(name.toLowerCase());
    }

    public List<CreateTypeStmt.CompositeField> getCompositeType(String name) {
        return compositeTypes.get(name.toLowerCase());
    }

    public void removeCompositeType(String name) {
        compositeTypes.remove(name.toLowerCase());
    }

    // User-defined aggregates
    public void addAggregate(PgAggregate agg) {
        userAggregates.put(agg.getName().toLowerCase(), agg);
    }

    public PgAggregate getAggregate(String name) {
        return userAggregates.get(name.toLowerCase());
    }

    public boolean hasAggregate(String name) {
        return userAggregates.containsKey(name.toLowerCase());
    }

    public void removeAggregate(String name) {
        userAggregates.remove(name.toLowerCase());
    }

    public Map<String, PgAggregate> getUserAggregates() {
        return userAggregates;
    }

    // User-defined operators (keyed by name+argtypes for overloading)
    public void addOperator(PgOperator op) {
        userOperators.put(op.getKey().toLowerCase(), op);
    }

    public PgOperator getOperator(String key) {
        return userOperators.get(key.toLowerCase());
    }

    public boolean hasOperator(String key) {
        return userOperators.containsKey(key.toLowerCase());
    }

    public void removeOperator(String key) {
        userOperators.remove(key.toLowerCase());
    }

    public Map<String, PgOperator> getUserOperators() {
        return userOperators;
    }

    /** Find all operators with a given name (across all arg type combinations). */
    public java.util.List<PgOperator> getOperatorsByName(String name) {
        java.util.List<PgOperator> result = new java.util.ArrayList<>();
        for (PgOperator op : userOperators.values()) {
            if (op.getName().equals(name)) result.add(op);
        }
        return result;
    }

    // User-defined operator families
    public void addOperatorFamily(PgOperatorFamily fam) {
        userOperatorFamilies.put(fam.getKey(), fam);
    }

    public PgOperatorFamily getOperatorFamily(String key) {
        return userOperatorFamilies.get(key);
    }

    public boolean hasOperatorFamily(String key) {
        return userOperatorFamilies.containsKey(key);
    }

    public void removeOperatorFamily(String key) {
        userOperatorFamilies.remove(key);
    }

    public Map<String, PgOperatorFamily> getUserOperatorFamilies() {
        return userOperatorFamilies;
    }

    // User-defined operator classes
    public void addOperatorClass(PgOperatorClass cls) {
        userOperatorClasses.put(cls.getKey(), cls);
    }

    public PgOperatorClass getOperatorClass(String key) {
        return userOperatorClasses.get(key);
    }

    public boolean hasOperatorClass(String key) {
        return userOperatorClasses.containsKey(key);
    }

    public void removeOperatorClass(String key) {
        userOperatorClasses.remove(key);
    }

    public Map<String, PgOperatorClass> getUserOperatorClasses() {
        return userOperatorClasses;
    }

    /** Remove all operator classes belonging to a given family. */
    public void removeOperatorClassesByFamily(String familyName) {
        userOperatorClasses.entrySet().removeIf(e ->
                familyName.equalsIgnoreCase(e.getValue().getFamilyName()));
    }

    // Functions, stored by name, supporting overloads with different parameter types
    public void addFunction(PgFunction function) {
        String key = function.getName().toLowerCase();
        List<PgFunction> overloads = functionOverloads.computeIfAbsent(key, k -> new ArrayList<>());
        overloads.add(function);
        functions.put(key, function); // last-added wins for simple name lookup
    }

    /** Returns the single function with this name, or the first overload if multiple exist. */
    public PgFunction getFunction(String name) {
        String key = name.toLowerCase();
        List<PgFunction> overloads = functionOverloads.get(key);
        if (overloads != null && !overloads.isEmpty()) return overloads.get(0);
        return functions.get(key);
    }

    /** Returns a function matching both name and schema, or null. */
    public PgFunction getFunction(String schema, String name) {
        if (schema == null) return getFunction(name);
        String key = name.toLowerCase();
        List<PgFunction> overloads = functionOverloads.get(key);
        if (overloads != null) {
            for (PgFunction f : overloads) {
                if (schema.equalsIgnoreCase(f.getSchemaName())) return f;
            }
        }
        PgFunction single = functions.get(key);
        if (single != null && schema.equalsIgnoreCase(single.getSchemaName())) return single;
        return null;
    }

    /** Returns all overloads for the given function name. */
    public List<PgFunction> getFunctionOverloads(String name) {
        List<PgFunction> overloads = functionOverloads.get(name.toLowerCase());
        return overloads != null ? overloads : Cols.listOf();
    }

    /** Finds the best matching overload by argument count and types. */
    public PgFunction resolveFunction(String name, int argCount, List<String> argTypeHints) {
        List<PgFunction> overloads = getFunctionOverloads(name);
        if (overloads.isEmpty()) return null;
        if (overloads.size() == 1) return overloads.get(0);

        // Try to match by argument count and type hints
        for (PgFunction f : overloads) {
            List<PgFunction.Param> inputParams = f.getParams().stream()
                    .filter(p -> !"OUT".equalsIgnoreCase(p.mode()) && !"VARIADIC".equalsIgnoreCase(p.mode()))
                    .collect(Collectors.toList());
            boolean hasVariadic = f.getParams().stream().anyMatch(p -> "VARIADIC".equalsIgnoreCase(p.mode()));
            if (hasVariadic) {
                // VARIADIC: argCount must be >= non-variadic input count
                if (argCount < inputParams.size()) continue;
            } else {
                if (inputParams.size() != argCount) continue;
            }
            if (argTypeHints != null && !argTypeHints.isEmpty()) {
                boolean match = true;
                for (int i = 0; i < argTypeHints.size() && i < inputParams.size(); i++) {
                    String hint = argTypeHints.get(i);
                    String paramType = inputParams.get(i).typeName();
                    if (hint != null && paramType != null && !typesCompatible(hint, paramType)) {
                        match = false;
                        break;
                    }
                }
                if (match) return f;
            }
        }
        // Fallback: match by arg count only
        for (PgFunction f : overloads) {
            long inputCount = f.getParams().stream()
                    .filter(p -> !"OUT".equalsIgnoreCase(p.mode()) && !"VARIADIC".equalsIgnoreCase(p.mode()))
                    .count();
            boolean hasVariadic = f.getParams().stream().anyMatch(p -> "VARIADIC".equalsIgnoreCase(p.mode()));
            if (hasVariadic) {
                if (argCount >= inputCount) return f;
            } else {
                if (inputCount == argCount) return f;
            }
        }
        return overloads.get(0); // fallback to first
    }

    public boolean typesCompatible(String argType, String paramType) {
        String a = argType.toLowerCase().trim();
        String p = paramType.toLowerCase().trim();
        if (a.equals(p)) return true;
        // Numeric family
        Set<String> numeric = Cols.setOf("int", "integer", "int4", "bigint", "int8", "smallint", "int2",
                "numeric", "decimal", "real", "float", "float4", "float8", "double precision");
        // Text family
        Set<String> textual = Cols.setOf("text", "varchar", "character varying", "char", "character", "name");
        if (numeric.contains(a) && numeric.contains(p)) return true;
        if (textual.contains(a) && textual.contains(p)) return true;
        return false;
    }

    public void removeFunction(String name) {
        String key = name.toLowerCase();
        functions.remove(key);
        functionOverloads.remove(key);
    }

    /** Remove a specific overload by name and param types. */
    public void removeFunction(String name, List<String> paramTypes) {
        String key = name.toLowerCase();
        List<PgFunction> overloads = functionOverloads.get(key);
        if (overloads == null) return;
        overloads.removeIf(f -> {
            List<String> fTypes = f.getParams().stream()
                    .filter(p -> !"OUT".equalsIgnoreCase(p.mode()))
                    .map(PgFunction.Param::typeName)
                    .collect(Collectors.toList());
            if (fTypes.size() != paramTypes.size()) return false;
            for (int i = 0; i < fTypes.size(); i++) {
                if (!typesCompatible(fTypes.get(i), paramTypes.get(i))) return false;
            }
            return true;
        });
        if (overloads.isEmpty()) {
            functionOverloads.remove(key);
            functions.remove(key);
        } else {
            functions.put(key, overloads.get(0));
        }
    }

    /** Rename a single specific overload of a function/procedure. */
    public void renameFunctionOverload(PgFunction func, String newName) {
        String oldName = func.getName();
        String oldKey = oldName.toLowerCase();
        String newKey = newName.toLowerCase();
        // Remove this specific overload from the old name's overload list
        List<PgFunction> oldOverloads = functionOverloads.get(oldKey);
        if (oldOverloads != null) {
            oldOverloads.remove(func);
            if (oldOverloads.isEmpty()) {
                functionOverloads.remove(oldKey);
                functions.remove(oldKey);
            } else {
                functions.put(oldKey, oldOverloads.get(0));
            }
        }
        // Update the function's name
        func.setName(newName);
        // Add to the new name's overload list
        List<PgFunction> newOverloads = functionOverloads.computeIfAbsent(newKey, k -> new ArrayList<>());
        newOverloads.add(func);
        functions.put(newKey, func);
        // Update schema registry
        if (oldOverloads == null || oldOverloads.isEmpty()) {
            for (Map.Entry<String, Set<String>> entry : schemaObjectRegistry.entrySet()) {
                if (entry.getValue().remove("function:" + oldKey)) {
                    entry.getValue().add("function:" + newKey);
                }
            }
        } else {
            // Old name still exists (other overloads remain), just register the new name
            for (Map.Entry<String, Set<String>> entry : schemaObjectRegistry.entrySet()) {
                entry.getValue().add("function:" + newKey);
            }
        }
        // Update object ownership key
        String oldOwner = objectOwners.remove("function:" + oldKey);
        if (oldOwner != null) objectOwners.put("function:" + newKey, oldOwner);
    }

    /** Rename a function/procedure: re-key in all maps, update the PgFunction name field. */
    public void renameFunction(String oldName, String newName) {
        String oldKey = oldName.toLowerCase();
        String newKey = newName.toLowerCase();
        List<PgFunction> overloads = functionOverloads.remove(oldKey);
        PgFunction single = functions.remove(oldKey);
        if (overloads != null) {
            for (PgFunction f : overloads) f.setName(newName);
            functionOverloads.put(newKey, overloads);
        }
        if (single != null) {
            single.setName(newName);
            functions.put(newKey, single);
        }
        // Update schema registry
        for (Map.Entry<String, Set<String>> entry : schemaObjectRegistry.entrySet()) {
            if (entry.getValue().remove("function:" + oldKey)) {
                entry.getValue().add("function:" + newKey);
            }
        }
        // Update object ownership key
        String oldOwner = objectOwners.remove("function:" + oldKey);
        if (oldOwner != null) objectOwners.put("function:" + newKey, oldOwner);
    }

    public Map<String, PgFunction> getFunctions() {
        return functions;
    }

    // Triggers
    public void addTrigger(PgTrigger trigger) {
        triggers.computeIfAbsent(trigger.getTableName().toLowerCase(), k -> new ArrayList<>())
                .add(trigger);
    }

    public List<PgTrigger> getTriggersForTable(String tableName) {
        return triggers.getOrDefault(tableName.toLowerCase(), Cols.listOf());
    }

    public Map<String, List<PgTrigger>> getAllTriggers() {
        return triggers;
    }

    public void removeTrigger(String name, String tableName) {
        List<PgTrigger> list = triggers.get(tableName.toLowerCase());
        if (list != null) {
            list.removeIf(t -> t.getName().equalsIgnoreCase(name));
        }
    }

    // Sequences
    public void addSequence(Sequence sequence) {
        sequences.put(sequence.getName().toLowerCase(), sequence);
    }

    public Sequence getSequence(String name) {
        return sequences.get(name.toLowerCase());
    }

    public void removeSequence(String name) {
        sequences.remove(name.toLowerCase());
    }

    /** Remove all sequences whose name starts with the given prefix. */
    public void removeSequencesWithPrefix(String prefix) {
        String lowerPrefix = prefix.toLowerCase();
        sequences.keySet().removeIf(k -> k.startsWith(lowerPrefix));
    }

    public boolean hasSequence(String name) {
        return sequences.containsKey(name.toLowerCase());
    }

    // Rules
    public void addRule(String table, String event, String ruleType) {
        rules.put(table.toLowerCase() + ":" + event.toUpperCase(), ruleType);
    }

    public void addRuleByName(String ruleName, String table) {
        rules.put("name:" + ruleName.toLowerCase() + ":" + table.toLowerCase(), "exists");
    }

    public boolean hasRule(String ruleName, String table) {
        return rules.containsKey("name:" + ruleName.toLowerCase() + ":" + table.toLowerCase());
    }

    public void removeRule(String ruleName, String table) {
        rules.remove("name:" + ruleName.toLowerCase() + ":" + table.toLowerCase());
    }

    public String getRule(String table, String event) {
        return rules.get(table.toLowerCase() + ":" + event.toUpperCase());
    }

    // Comments
    public void addComment(String objectType, String objectName, String comment) {
        if (comment == null) {
            comments.remove(objectType + ":" + objectName);
        } else {
            comments.put(objectType + ":" + objectName, comment);
        }
    }

    public String getComment(String objectType, String objectName) {
        return comments.get(objectType + ":" + objectName);
    }

    public Map<String, String> getComments() {
        return comments;
    }

    // Index metadata (for USING INDEX lookups)
    public void addIndex(String name, List<String> columns) {
        indexColumns.put(name.toLowerCase(), columns);
    }

    public void addIndexMeta(String name, String tableName, boolean isUnique) {
        indexTableNames.put(name.toLowerCase(), tableName);
        indexUniqueFlags.put(name.toLowerCase(), isUnique);
    }

    public void addIndexMeta(String name, String tableName, boolean isUnique, String method, String whereClause) {
        indexTableNames.put(name.toLowerCase(), tableName);
        indexUniqueFlags.put(name.toLowerCase(), isUnique);
        if (method != null) indexMethods.put(name.toLowerCase(), method);
        if (whereClause != null) indexWhereClauses.put(name.toLowerCase(), whereClause);
    }

    public String getIndexMethod(String name) {
        return indexMethods.getOrDefault(name.toLowerCase(), "btree");
    }

    public String getIndexWhereClause(String name) {
        return indexWhereClauses.get(name.toLowerCase());
    }

    public String getIndexTable(String name) {
        return indexTableNames.get(name.toLowerCase());
    }

    public boolean isUniqueIndex(String name) {
        return indexUniqueFlags.getOrDefault(name.toLowerCase(), false);
    }

    public List<String> getIndexColumns(String name) {
        return indexColumns.get(name.toLowerCase());
    }

    public Map<String, String> getIndexReloptions(String name) {
        return indexReloptions.get(name.toLowerCase());
    }

    public void setIndexReloptions(String name, Map<String, String> opts) {
        indexReloptions.put(name.toLowerCase(), opts);
    }

    public boolean hasIndex(String name) {
        return indexColumns.containsKey(name.toLowerCase());
    }

    public void removeIndex(String name) {
        indexColumns.remove(name.toLowerCase());
        indexTableNames.remove(name.toLowerCase());
        indexUniqueFlags.remove(name.toLowerCase());
        indexWhereClauses.remove(name.toLowerCase());
        indexMethods.remove(name.toLowerCase());
        indexReloptions.remove(name.toLowerCase());
    }

    /** Rename an index: re-key across all index maps and update schema registry. */
    public void renameIndex(String oldName, String newName) {
        String oldKey = oldName.toLowerCase();
        String newKey = newName.toLowerCase();
        List<String> cols = indexColumns.remove(oldKey);
        if (cols != null) indexColumns.put(newKey, cols);
        String tbl = indexTableNames.remove(oldKey);
        if (tbl != null) indexTableNames.put(newKey, tbl);
        Boolean uniq = indexUniqueFlags.remove(oldKey);
        if (uniq != null) indexUniqueFlags.put(newKey, uniq);
        String where = indexWhereClauses.remove(oldKey);
        if (where != null) indexWhereClauses.put(newKey, where);
        String method = indexMethods.remove(oldKey);
        if (method != null) indexMethods.put(newKey, method);
        Map<String, String> opts = indexReloptions.remove(oldKey);
        if (opts != null) indexReloptions.put(newKey, opts);
        // Update schema registry
        for (Map.Entry<String, Set<String>> entry : schemaObjectRegistry.entrySet()) {
            if (entry.getValue().remove("index:" + oldKey)) {
                entry.getValue().add("index:" + newKey);
            }
        }
        // Update object ownership key
        String oldOwner = objectOwners.remove("index:" + oldKey);
        if (oldOwner != null) objectOwners.put("index:" + newKey, oldOwner);
    }

    public Map<String, List<String>> getIndexColumns() {
        return indexColumns;
    }

    // Views
    public void addView(ViewDef view) {
        views.put(view.name().toLowerCase(), view);
    }

    public ViewDef getView(String name) {
        return views.get(name.toLowerCase());
    }

    public void removeView(String name) {
        views.remove(name.toLowerCase());
    }

    public boolean hasView(String name) {
        return views.containsKey(name.toLowerCase());
    }

    public Map<String, ViewDef> getViews() {
        return views;
    }

    public Map<String, Sequence> getSequences() {
        return sequences;
    }

    // Domain types
    public void addDomain(DomainType domain) {
        domains.put(domain.getName().toLowerCase(), domain);
    }

    public DomainType getDomain(String name) {
        return domains.get(name.toLowerCase());
    }

    public void removeDomain(String name) {
        domains.remove(name.toLowerCase());
    }

    public boolean isDomain(String name) {
        return domains.containsKey(name.toLowerCase());
    }

    public Map<String, DomainType> getDomains() {
        return domains;
    }

    // Advisory locks
    public boolean tryAdvisoryLock(long key, Session session) {
        Set<Session> holders = advisoryLocks.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet());
        // Synchronize on the holder set to make check-then-add atomic.
        // Without this, two sessions could both see an empty set, both pass
        // the check, and both add themselves — violating mutual exclusion.
        synchronized (holders) {
            for (Session holder : holders) {
                if (holder != session) {
                    return false;
                }
            }
            holders.add(session);
            return true;
        }
    }

    public boolean advisoryUnlock(long key, Session session) {
        Set<Session> holders = advisoryLocks.get(key);
        if (holders != null) {
            synchronized (holders) {
                return holders.remove(session);
            }
        }
        return false;
    }

    public void advisoryUnlockAll(Session session) {
        for (Set<Session> holders : advisoryLocks.values()) {
            synchronized (holders) {
                holders.remove(session);
            }
        }
    }

    // Row-level locks

    public boolean tryLockRow(String tableName, Object[] row, Session session) {
        return tryLockRow(tableName, row, session, "UPDATE");
    }

    public boolean tryLockRow(String tableName, Object[] row, Session session, String mode) {
        Map<Object[], List<LockEntry>> locks = rowLocks.computeIfAbsent(tableName.toLowerCase(), k -> new IdentityHashMap<>());
        synchronized (locks) {
            List<LockEntry> entries = locks.get(row);
            if (entries != null) {
                // Check compatibility with all existing lock holders from other sessions
                for (LockEntry existing : entries) {
                    if (existing.session != session && !lockModesCompatible(existing.mode, mode)) {
                        return false; // incompatible lock held by another session
                    }
                }
                // Remove any prior entry for this session (will re-add with potentially upgraded mode)
                entries.removeIf(e -> e.session == session);
            } else {
                entries = new ArrayList<>();
                locks.put(row, entries);
            }
            entries.add(new LockEntry(session, mode));
            return true;
        }
    }

    /**
     * Returns the session currently holding an incompatible lock on this row, or null if none.
     * Must be called with the locks map's monitor held.
     */
    private Session getBlockingSession(List<LockEntry> entries, Session requester, String mode) {
        if (entries == null) return null;
        for (LockEntry existing : entries) {
            if (existing.session != requester && !lockModesCompatible(existing.mode, mode)) {
                return existing.session;
            }
        }
        return null;
    }

    /**
     * Checks whether following the wait-for chain starting at {@code blocker} eventually leads back to
     * {@code requester}, indicating a deadlock cycle.
     */
    private boolean hasDeadlock(Session requester, Session blocker) {
        Session current = blocker;
        while (current != null) {
            if (current == requester) return true;
            current = waitingFor.get(current);
        }
        return false;
    }

    /**
     * Acquire a row lock for normal FOR UPDATE/SHARE (without NOWAIT/SKIP LOCKED).
     * Blocks with polling until the lock is available or a deadlock / timeout is detected.
     *
     * @throws MemgresException with SQLSTATE 40P01 when a deadlock is detected
     * @throws MemgresException with SQLSTATE 55P03 when the lock timeout expires
     */
    public void lockRowWaiting(String tableName, Object[] row, Session session, String mode) {
        final long safetyTimeoutMs = 5_000L; // fallback when lock_timeout is 0 (disabled)
        long lockTimeoutMs = GucSettings.parseTimeoutMillis(session.getGucSettings().get("lock_timeout"));
        final long timeoutMs = lockTimeoutMs > 0 ? lockTimeoutMs : safetyTimeoutMs;
        final long pollMs = 10L;
        final long deadline = System.currentTimeMillis() + timeoutMs;

        Map<Object[], List<LockEntry>> locks =
                rowLocks.computeIfAbsent(tableName.toLowerCase(), k -> new IdentityHashMap<>());

        while (true) {
            Session blocker;
            synchronized (locks) {
                List<LockEntry> entries = locks.get(row);
                blocker = getBlockingSession(entries, session, mode);
                if (blocker == null) {
                    // Lock is available, acquire it
                    if (entries == null) {
                        entries = new ArrayList<>();
                        locks.put(row, entries);
                    }
                    entries.removeIf(e -> e.session == session);
                    entries.add(new LockEntry(session, mode));
                    waitingFor.remove(session); // no longer waiting
                    return;
                }
            }
            // Lock not available; check for deadlock before waiting
            if (hasDeadlock(session, blocker)) {
                waitingFor.remove(session);
                throw new MemgresException("deadlock detected", "40P01");
            }
            // Register that this session is waiting for the blocker
            waitingFor.put(session, blocker);

            // Check timeout
            if (System.currentTimeMillis() >= deadline) {
                waitingFor.remove(session);
                throw new MemgresException("could not obtain lock on row in relation \"" + tableName + "\"", "55P03");
            }

            // Sleep briefly before retrying
            try {
                Thread.sleep(pollMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                waitingFor.remove(session);
                throw new MemgresException("interrupted while waiting for row lock", "57014");
            }
        }
    }

    public void unlockRow(String tableName, Object[] row) {
        Map<Object[], List<LockEntry>> locks = rowLocks.get(tableName.toLowerCase());
        if (locks != null) {
            synchronized (locks) {
                locks.remove(row);
            }
        }
    }

    public void unlockAllRows(Session session) {
        for (Map<Object[], List<LockEntry>> locks : rowLocks.values()) {
            synchronized (locks) {
                for (List<LockEntry> entries : locks.values()) {
                    entries.removeIf(e -> e.session == session);
                }
                locks.entrySet().removeIf(e -> e.getValue().isEmpty());
            }
        }
        // Clean up any wait-for entries involving this session
        waitingFor.remove(session);
        waitingFor.entrySet().removeIf(e -> e.getValue() == session);
    }

    /**
     * Returns true if the given row array is the MVCC old-values copy for a live row
     * currently being updated by another active session (uncommitted UPDATE).
     *
     * When MVCC visibility substitutes old values for a row being updated by another
     * session, the row binding in a FOR UPDATE SKIP LOCKED scan holds the old-values
     * copy rather than the actual live row.  Because the copy has a different identity
     * it is not in the row-lock map, so {@code tryLockRow} would always succeed on it --
     * allowing two workers to claim the same logical job.  This method detects that
     * situation so the SKIP LOCKED loop can skip the row instead.
     */
    public boolean isRowBeingUpdatedByOtherSession(Object[] row, Session currentSession) {
        for (Session other : activeSessions) {
            if (other == currentSession) continue;
            if (!other.isInTransaction()) continue;
            try {
                // uncommittedUpdates is a ConcurrentHashMap; inner maps are synchronizedMap-wrapped
                // IdentityHashMaps, safe to iterate without CME.
                for (Map<Object[], Object[]> tableUpdates : other.getAllUncommittedUpdates().values()) {
                    // Snapshot values to avoid issues with concurrent modification of inner map
                    Object[][] values;
                    synchronized (tableUpdates) {
                        values = tableUpdates.values().toArray(new Object[0][]);
                    }
                    for (Object[] oldValues : values) {
                        if (oldValues == row) {
                            return true;
                        }
                    }
                }
            } catch (Exception e) {
                // Concurrent commit/rollback cleared the map during iteration --
                // conservatively treat the row as being updated.
                return true;
            }
        }
        return false;
    }

    /**
     * Stored view definition.
     */
        public static final class ViewDef {
        public final String name;
        public final String schemaName;
        public final Statement query;
        public final boolean orReplace;
        public final boolean materialized;
        public final List<Column> cachedColumns;
        public final List<Object[]> cachedRows;
        public final String sourceSQL;
        public final String checkOption;
        public final Map<String, String> reloptions;

        public ViewDef(
                String name,
                String schemaName,
                Statement query,
                boolean orReplace,
                boolean materialized,
                List<Column> cachedColumns,
                List<Object[]> cachedRows,
                String sourceSQL,
                String checkOption
        ) {
            this(name, schemaName, query, orReplace, materialized, cachedColumns, cachedRows, sourceSQL, checkOption, null);
        }

        public ViewDef(
                String name,
                String schemaName,
                Statement query,
                boolean orReplace,
                boolean materialized,
                List<Column> cachedColumns,
                List<Object[]> cachedRows,
                String sourceSQL,
                String checkOption,
                Map<String, String> reloptions
        ) {
            this.name = name;
            this.schemaName = schemaName;
            this.query = query;
            this.orReplace = orReplace;
            this.materialized = materialized;
            this.cachedColumns = cachedColumns;
            this.cachedRows = cachedRows;
            this.sourceSQL = sourceSQL;
            this.checkOption = checkOption;
            this.reloptions = reloptions;
        }

        /** Convenience constructor (full, without checkOption). */
        public ViewDef(String name, String schemaName, Statement query, boolean orReplace, boolean materialized,
                       List<Column> cachedColumns, List<Object[]> cachedRows, String sourceSQL) {
            this(name, schemaName, query, orReplace, materialized, cachedColumns, cachedRows, sourceSQL, null);
        }

        /** Convenience constructor for regular views (with schema). */
        public ViewDef(String name, String schemaName, Statement query, boolean orReplace) {
            this(name, schemaName, query, orReplace, false, null, null, null, null);
        }

        /** Convenience constructor for regular views (no schema, defaults to public). */
        public ViewDef(String name, Statement query, boolean orReplace) {
            this(name, "public", query, orReplace, false, null, null, null, null);
        }

        /** Convenience constructor for materialized views (no sourceSQL). */
        public ViewDef(String name, Statement query, boolean orReplace, boolean materialized,
                       List<Column> cachedColumns, List<Object[]> cachedRows) {
            this(name, "public", query, orReplace, materialized, cachedColumns, cachedRows, null, null);
        }

        /** Convenience constructor for materialized views with schema (no sourceSQL). */
        public ViewDef(String name, String schemaName, Statement query, boolean orReplace, boolean materialized,
                       List<Column> cachedColumns, List<Object[]> cachedRows) {
            this(name, schemaName, query, orReplace, materialized, cachedColumns, cachedRows, null, null);
        }

        public String name() { return name; }
        public String schemaName() { return schemaName; }
        public Statement query() { return query; }
        public boolean orReplace() { return orReplace; }
        public boolean materialized() { return materialized; }
        public List<Column> cachedColumns() { return cachedColumns; }
        public List<Object[]> cachedRows() { return cachedRows; }
        public String sourceSQL() { return sourceSQL; }
        public String checkOption() { return checkOption; }
        public Map<String, String> reloptions() { return reloptions; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ViewDef that = (ViewDef) o;
            return java.util.Objects.equals(name, that.name)
                && java.util.Objects.equals(schemaName, that.schemaName)
                && java.util.Objects.equals(query, that.query)
                && orReplace == that.orReplace
                && materialized == that.materialized
                && java.util.Objects.equals(cachedColumns, that.cachedColumns)
                && java.util.Objects.equals(cachedRows, that.cachedRows)
                && java.util.Objects.equals(sourceSQL, that.sourceSQL)
                && java.util.Objects.equals(checkOption, that.checkOption);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, schemaName, query, orReplace, materialized, cachedColumns, cachedRows, sourceSQL, checkOption);
        }

        @Override
        public String toString() {
            return "ViewDef[name=" + name + ", " + "schemaName=" + schemaName + ", " + "query=" + query + ", " + "orReplace=" + orReplace + ", " + "materialized=" + materialized + ", " + "cachedColumns=" + cachedColumns + ", " + "cachedRows=" + cachedRows + ", " + "sourceSQL=" + sourceSQL + ", " + "checkOption=" + checkOption + "]";
        }
    }

    // ---- Role management ----

    public void createRole(String name, Map<String, String> attributes) {
        roles.put(name.toLowerCase(), new ConcurrentHashMap<>(attributes));
    }

    public boolean hasRole(String name) {
        return roles.containsKey(name.toLowerCase());
    }

    public Map<String, String> getRole(String name) {
        return roles.get(name.toLowerCase());
    }

    public void removeRole(String name) {
        String lower = name.toLowerCase();
        roles.remove(lower);
        // Clean up memberships: remove this role as a member from all groups
        for (Set<String> members : roleMemberships.values()) {
            members.remove(lower);
        }
        // Remove empty membership entries
        roleMemberships.entrySet().removeIf(e -> e.getValue().isEmpty());
        // Also remove this role's own membership entry (if it was a group)
        roleMemberships.remove(lower);
    }

    public Map<String, Map<String, String>> getRoles() {
        return roles;
    }

    // ---- Role membership ----

    public void addRoleMembership(String grantedRole, String memberRole) {
        roleMemberships.computeIfAbsent(grantedRole.toLowerCase(), k -> ConcurrentHashMap.newKeySet())
                .add(memberRole.toLowerCase());
    }

    public void removeRoleMembership(String grantedRole, String memberRole) {
        Set<String> members = roleMemberships.get(grantedRole.toLowerCase());
        if (members != null) {
            members.remove(memberRole.toLowerCase());
            if (members.isEmpty()) roleMemberships.remove(grantedRole.toLowerCase());
        }
    }

    public boolean hasRoleMemberships(String roleName) {
        Set<String> members = roleMemberships.get(roleName.toLowerCase());
        return members != null && !members.isEmpty();
    }

    public void removeAllRoleMemberships(String roleName) {
        String lower = roleName.toLowerCase();
        // Remove the role as a granted role (revoke all members)
        roleMemberships.remove(lower);
        // Remove the role as a member of other roles
        for (Set<String> members : roleMemberships.values()) {
            members.remove(lower);
        }
    }

    public Map<String, Set<String>> getRoleMemberships() {
        return roleMemberships;
    }

    // ---- Role privilege tracking ----

    public void addRolePrivilege(String role, String privilege, String objectType, String objectName) {
        rolePrivileges.computeIfAbsent(role.toLowerCase(), k -> ConcurrentHashMap.newKeySet())
                .add(privilege + ":" + objectType + ":" + objectName);
    }

    public void removeRolePrivilege(String role, String privilege, String objectType, String objectName) {
        Set<String> privs = rolePrivileges.get(role.toLowerCase());
        if (privs != null) {
            if ("ALL".equalsIgnoreCase(privilege)) {
                // Remove all privileges on this object (case-insensitive suffix match)
                final String suffixLower = ":" + objectType.toLowerCase() + ":" + objectName.toLowerCase();
                privs.removeIf(p -> p.toLowerCase().endsWith(suffixLower));
                // If revoking ALL on a TABLE, also remove any column-level grants for that table
                if ("TABLE".equalsIgnoreCase(objectType)) {
                    final String colPrefixLower = ":column:" + objectName.toLowerCase() + ".";
                    privs.removeIf(p -> p.toLowerCase().contains(colPrefixLower));
                }
            } else {
                // Case-insensitive exact match
                final String keyLower = privilege.toLowerCase() + ":" + objectType.toLowerCase() + ":" + objectName.toLowerCase();
                privs.removeIf(p -> p.toLowerCase().equals(keyLower));
            }
            if (privs.isEmpty()) rolePrivileges.remove(role.toLowerCase());
        }
    }

    public boolean hasRolePrivileges(String role) {
        Set<String> privs = rolePrivileges.get(role.toLowerCase());
        return privs != null && !privs.isEmpty();
    }

    /** Remove all privileges held by a specific role (e.g. when the role is dropped). */
    public void removeAllRolePrivileges(String role) {
        rolePrivileges.remove(role.toLowerCase());
    }

    /** Get all privileges held by a specific role. */
    public Set<String> getRolePrivileges(String role) {
        Set<String> privs = rolePrivileges.get(role.toLowerCase());
        return privs != null ? privs : java.util.Collections.emptySet();
    }

    /** Get the entire rolePrivileges map (for privilege inspection). */
    public Map<String, Set<String>> getAllRolePrivileges() {
        return rolePrivileges;
    }

    /** Remove all privileges granted on a specific object (called when the object is dropped). */
    public void removePrivilegesOnObject(String objectType, String objectName) {
        String suffix = ":" + objectType + ":" + objectName;
        for (Map.Entry<String, Set<String>> entry : rolePrivileges.entrySet()) {
            entry.getValue().removeIf(p -> p.endsWith(suffix));
        }
        // Clean up empty entries
        rolePrivileges.entrySet().removeIf(e -> e.getValue().isEmpty());
    }

    // ---- Object ownership ----

    /** Set the owner of an object. Key format: "type:name" (e.g., "table:public.my_table"). */
    public void setObjectOwner(String objectKey, String ownerRole) {
        objectOwners.put(objectKey.toLowerCase(), ownerRole.toLowerCase());
    }

    /** Get the owner of an object, or null if not tracked. */
    public String getObjectOwner(String objectKey) {
        return objectOwners.get(objectKey.toLowerCase());
    }

    /** Remove ownership entry for an object. */
    public void removeObjectOwner(String objectKey) {
        objectOwners.remove(objectKey.toLowerCase());
    }

    /** Check if a role owns any objects. */
    public boolean roleOwnsObjects(String roleName) {
        String lower = roleName.toLowerCase();
        return objectOwners.containsValue(lower);
    }

    /** Get all object keys owned by a role. */
    public List<String> getObjectsOwnedBy(String roleName) {
        String lower = roleName.toLowerCase();
        List<String> owned = new ArrayList<>();
        for (Map.Entry<String, String> entry : objectOwners.entrySet()) {
            if (lower.equals(entry.getValue())) {
                owned.add(entry.getKey());
            }
        }
        return owned;
    }

    /** Transfer all objects owned by one role to another. */
    public void reassignOwned(String fromRole, String toRole) {
        String fromLower = fromRole.toLowerCase();
        String toLower = toRole.toLowerCase();
        for (Map.Entry<String, String> entry : objectOwners.entrySet()) {
            if (fromLower.equals(entry.getValue())) {
                entry.setValue(toLower);
            }
        }
    }

    // ---- Schema object registry ----

    /**
     * Register an object as belonging to a schema.
     * @param schemaName the schema this object belongs to
     * @param objectType a category like "enum", "composite", "sequence", "domain", "index", "function", "view", "trigger"
     * @param objectName the name of the object (lowercased)
     */
    public void registerSchemaObject(String schemaName, String objectType, String objectName) {
        schemaObjectRegistry
                .computeIfAbsent(schemaName.toLowerCase(), k -> ConcurrentHashMap.newKeySet())
                .add(objectType + ":" + objectName.toLowerCase());
    }

    /**
     * Get all registered objects for a schema.
     */
    public Set<String> getSchemaObjects(String schemaName) {
        return schemaObjectRegistry.getOrDefault(schemaName.toLowerCase(), Cols.setOf());
    }

    /**
     * Remove schema from registry.
     */
    public void removeSchemaObjects(String schemaName) {
        schemaObjectRegistry.remove(schemaName.toLowerCase());
    }
}
