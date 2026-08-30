package com.memgres.engine;

import com.memgres.engine.parser.ast.Expression;
import com.memgres.engine.parser.ast.FunctionCallExpr;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The {@code has_*_privilege} family and {@code pg_has_role}.
 *
 * <p>PostgreSQL resolves every argument before it answers, in a fixed order — the role, then the
 * object, then the privilege name — and a name that does not resolve is an error rather than a
 * "yes". That order matters more than it looks: callers ask these functions whether an operation
 * is worth attempting, so a misspelled table or privilege that answers {@code true} reads as
 * permission granted, and a misspelled role that answers {@code false} hides a real grant.
 */
class CatalogPrivilegeFunctions {

    static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    // The privilege names PostgreSQL accepts per object kind. Anything else is 22023, so these
    // lists are the difference between "you may not" and "there is no such privilege".
    private static final String[] RELATION_PRIVS = {
            "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"};
    private static final String[] COLUMN_PRIVS = {"SELECT", "INSERT", "UPDATE", "REFERENCES"};
    private static final String[] SCHEMA_PRIVS = {"CREATE", "USAGE"};
    private static final String[] DATABASE_PRIVS = {"CREATE", "CONNECT", "TEMPORARY", "TEMP"};
    private static final String[] FUNCTION_PRIVS = {"EXECUTE"};
    private static final String[] SEQUENCE_PRIVS = {"USAGE", "SELECT", "UPDATE"};
    private static final String[] USAGE_PRIVS = {"USAGE"};
    private static final String[] TABLESPACE_PRIVS = {"CREATE"};
    private static final String[] PARAMETER_PRIVS = {"SET", "ALTER SYSTEM"};
    private static final String[] LARGEOBJECT_PRIVS = {"SELECT", "UPDATE"};
    private static final String[] ROLE_PRIVS = {"USAGE", "MEMBER", "SET"};

    /** Schemas every role may look into: PUBLIC holds USAGE on them out of the box. */
    private static final Set<String> IMPLICIT_USAGE_SCHEMAS =
            new HashSet<>(java.util.Arrays.asList("public", "pg_catalog", "information_schema"));

    private final AstExecutor executor;

    CatalogPrivilegeFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "has_table_privilege":
                return hasTablePrivilege(fn, ctx);
            case "has_column_privilege":
                return hasColumnPrivilege(fn, ctx);
            case "has_any_column_privilege":
                return hasAnyColumnPrivilege(fn, ctx);
            case "has_schema_privilege":
                return hasSchemaPrivilege(fn, ctx);
            case "has_database_privilege":
                return hasDatabasePrivilege(fn, ctx);
            case "has_function_privilege":
                return hasFunctionPrivilege(fn, ctx);
            case "has_sequence_privilege":
                return hasSequencePrivilege(fn, ctx);
            case "has_language_privilege":
                // A language's default ACL grants USAGE to PUBLIC.
                return hasNamedObjectPrivilege(fn, ctx, "language", "pg_language", "lanname",
                        USAGE_PRIVS, "LANGUAGE", true);
            case "has_tablespace_privilege":
                return hasNamedObjectPrivilege(fn, ctx, "tablespace", "pg_tablespace", "spcname",
                        TABLESPACE_PRIVS, "TABLESPACE", false);
            case "has_server_privilege":
                return hasNamedObjectPrivilege(fn, ctx, "server", "pg_foreign_server", "srvname",
                        USAGE_PRIVS, "FOREIGN SERVER", false);
            case "has_foreign_data_wrapper_privilege":
                return hasNamedObjectPrivilege(fn, ctx, "foreign-data wrapper",
                        "pg_foreign_data_wrapper", "fdwname", USAGE_PRIVS, "FOREIGN DATA WRAPPER", false);
            case "has_largeobject_privilege":
                return hasLargeobjectPrivilege(fn, ctx);
            case "has_type_privilege":
                return hasTypePrivilege(fn, ctx);
            case "has_parameter_privilege":
                return hasParameterPrivilege(fn, ctx);
            case "pg_has_role":
                return pgHasRole(fn, ctx);
            default:
                return NOT_HANDLED;
        }
    }

    // ---- The individual functions ----

    private Object hasTablePrivilege(FunctionCallExpr fn, RowContext ctx) {
        List<Object> a = evalArgs(fn, ctx);
        if (a == null) return null;
        boolean withRole = a.size() >= 3;
        String role = withRole ? role(a.get(0), true) : currentUserName();
        Rel rel = relation(a.get(withRole ? 1 : 0));
        List<Priv> privs = parsePrivileges(str(a.get(withRole ? 2 : 1)), RELATION_PRIVS, false);
        String key = AstExecutor.privilegeKey(rel.schema, rel.name);
        for (Priv p : privs) {
            if (holds(role, p, "TABLE", key)) return true;
        }
        return false;
    }

    private Object hasColumnPrivilege(FunctionCallExpr fn, RowContext ctx) {
        List<Object> a = evalArgs(fn, ctx);
        if (a == null) return null;
        boolean withRole = a.size() >= 4;
        String role = withRole ? role(a.get(0), true) : currentUserName();
        Rel rel = relation(a.get(withRole ? 1 : 0));
        Object colArg = a.get(withRole ? 2 : 1);
        String column;
        if (colArg instanceof Number) {
            column = columnByAttnum(rel, ((Number) colArg).intValue());
            // An attnum that names no column is not an error in PostgreSQL — the answer is unknown.
            if (column == null) return null;
        } else {
            column = str(colArg).toLowerCase(java.util.Locale.ROOT);
            if (rel.table != null && rel.table.getColumnIndex(column) < 0) {
                throw new MemgresException("column \"" + column + "\" of relation \""
                        + rel.name + "\" does not exist", "42703");
            }
        }
        List<Priv> privs = parsePrivileges(str(a.get(withRole ? 3 : 2)), COLUMN_PRIVS, false);
        String key = AstExecutor.privilegeKey(rel.schema, rel.name);
        for (Priv p : privs) {
            if (holds(role, p, "COLUMN", key + "." + column)) return true;
            if (holds(role, p, "TABLE", key)) return true;
        }
        return false;
    }

    private Object hasAnyColumnPrivilege(FunctionCallExpr fn, RowContext ctx) {
        List<Object> a = evalArgs(fn, ctx);
        if (a == null) return null;
        boolean withRole = a.size() >= 3;
        String role = withRole ? role(a.get(0), true) : currentUserName();
        Rel rel = relation(a.get(withRole ? 1 : 0));
        List<Priv> privs = parsePrivileges(str(a.get(withRole ? 2 : 1)), COLUMN_PRIVS, false);
        String key = AstExecutor.privilegeKey(rel.schema, rel.name);
        for (Priv p : privs) {
            if (holds(role, p, "TABLE", key)) return true;
            if (rel.table == null) continue;
            for (Column col : rel.table.getColumns()) {
                if (holds(role, p, "COLUMN", key + "." + col.getName().toLowerCase(java.util.Locale.ROOT))) return true;
            }
        }
        return false;
    }

    private Object hasSchemaPrivilege(FunctionCallExpr fn, RowContext ctx) {
        List<Object> a = evalArgs(fn, ctx);
        if (a == null) return null;
        boolean withRole = a.size() >= 3;
        String role = withRole ? role(a.get(0), true) : currentUserName();
        Object schemaArg = a.get(withRole ? 1 : 0);
        String schema = str(schemaArg).toLowerCase(java.util.Locale.ROOT);
        if (!(schemaArg instanceof Number) && !schemaExists(schema)) {
            throw new MemgresException("schema \"" + str(schemaArg) + "\" does not exist", "3F000");
        }
        List<Priv> privs = parsePrivileges(str(a.get(withRole ? 2 : 1)), SCHEMA_PRIVS, false);
        for (Priv p : privs) {
            if (!p.grantOption && "USAGE".equals(p.name) && IMPLICIT_USAGE_SCHEMAS.contains(schema)) return true;
            if (holds(role, p, "SCHEMA", schema)) return true;
        }
        return false;
    }

    private Object hasDatabasePrivilege(FunctionCallExpr fn, RowContext ctx) {
        List<Object> a = evalArgs(fn, ctx);
        if (a == null) return null;
        boolean withRole = a.size() >= 3;
        String role = withRole ? role(a.get(0), true) : currentUserName();
        Object dbArg = a.get(withRole ? 1 : 0);
        String db = str(dbArg);
        if (!(dbArg instanceof Number) && !databaseExists(db)) {
            throw new MemgresException("database \"" + db + "\" does not exist", "3D000");
        }
        List<Priv> privs = parsePrivileges(str(a.get(withRole ? 2 : 1)), DATABASE_PRIVS, false);
        for (Priv p : privs) {
            // PUBLIC holds CONNECT and TEMPORARY on every database unless they are revoked;
            // CREATE is the owner's alone.
            if (!p.grantOption && !"CREATE".equals(p.name)) return true;
            if (holds(role, p, "DATABASE", db.toLowerCase(java.util.Locale.ROOT))) return true;
        }
        return false;
    }

    private Object hasFunctionPrivilege(FunctionCallExpr fn, RowContext ctx) {
        List<Object> a = evalArgs(fn, ctx);
        if (a == null) return null;
        boolean withRole = a.size() >= 3;
        String role = withRole ? role(a.get(0), true) : currentUserName();
        Object fnArg = a.get(withRole ? 1 : 0);
        String signature = str(fnArg).trim();
        String bare = signature;
        int paren = bare.indexOf('(');
        if (paren >= 0) bare = bare.substring(0, paren).trim();
        if (bare.contains(".")) bare = bare.substring(bare.lastIndexOf('.') + 1);
        bare = bare.toLowerCase(java.util.Locale.ROOT);
        if (!(fnArg instanceof Number) && !functionExists(bare)) {
            throw new MemgresException("function \"" + signature + "\" does not exist", "42883");
        }
        List<Priv> privs = parsePrivileges(str(a.get(withRole ? 2 : 1)), FUNCTION_PRIVS, false);
        for (Priv p : privs) {
            if (holds(role, p, "FUNCTION", bare)) return true;
        }
        return false;
    }

    private Object hasSequencePrivilege(FunctionCallExpr fn, RowContext ctx) {
        List<Object> a = evalArgs(fn, ctx);
        if (a == null) return null;
        boolean withRole = a.size() >= 3;
        String role = withRole ? role(a.get(0), true) : currentUserName();
        Object seqArg = a.get(withRole ? 1 : 0);
        Rel rel = relation(seqArg);
        if (!(seqArg instanceof Number) && !rel.sequence) {
            throw PgErrors.wrongObjectType("\"" + str(seqArg) + "\" is not a sequence");
        }
        List<Priv> privs = parsePrivileges(str(a.get(withRole ? 2 : 1)), SEQUENCE_PRIVS, false);
        for (Priv p : privs) {
            if (holds(role, p, "SEQUENCE", rel.name)) return true;
        }
        return false;
    }

    /**
     * memgres stores no large objects, so every oid names one that is not there — and a large
     * object that is not there is what PostgreSQL answers NULL for rather than raising. The
     * privilege name is still read, because a misspelt privilege is a caller's bug either way.
     */
    private Object hasLargeobjectPrivilege(FunctionCallExpr fn, RowContext ctx) {
        List<Object> a = evalArgs(fn, ctx);
        if (a == null) return null;
        boolean withRole = a.size() >= 3;
        if (withRole) role(a.get(0), true);
        parsePrivileges(str(a.get(withRole ? 2 : 1)), LARGEOBJECT_PRIVS, false);
        return null;
    }

    private Object hasTypePrivilege(FunctionCallExpr fn, RowContext ctx) {
        List<Object> a = evalArgs(fn, ctx);
        if (a == null) return null;
        boolean withRole = a.size() >= 3;
        String role = withRole ? role(a.get(0), true) : currentUserName();
        Object typeArg = a.get(withRole ? 1 : 0);
        String type = str(typeArg);
        if (!(typeArg instanceof Number) && !typeExists(type)) {
            throw PgErrors.undefinedObject("type", type);
        }
        List<Priv> privs = parsePrivileges(str(a.get(withRole ? 2 : 1)), USAGE_PRIVS, false);
        for (Priv p : privs) {
            // A type's default ACL grants USAGE to PUBLIC; only the grant option is the owner's.
            if (!p.grantOption) return true;
            if (holds(role, p, "TYPE", type.toLowerCase(java.util.Locale.ROOT))) return true;
        }
        return false;
    }

    /** has_language/tablespace/server/foreign_data_wrapper_privilege — all name lookups in one catalog. */
    private Object hasNamedObjectPrivilege(FunctionCallExpr fn, RowContext ctx, String kind,
            String catalog, String nameColumn, String[] allowed, String objectType,
            boolean publicByDefault) {
        List<Object> a = evalArgs(fn, ctx);
        if (a == null) return null;
        boolean withRole = a.size() >= 3;
        String role = withRole ? role(a.get(0), true) : currentUserName();
        Object objArg = a.get(withRole ? 1 : 0);
        String name = str(objArg);
        if (!(objArg instanceof Number) && !catalogHasName(catalog, nameColumn, name)) {
            throw PgErrors.undefinedObject(kind, name);
        }
        List<Priv> privs = parsePrivileges(str(a.get(withRole ? 2 : 1)), allowed, false);
        for (Priv p : privs) {
            if (publicByDefault && !p.grantOption) return true;
            if (holds(role, p, objectType, name.toLowerCase(java.util.Locale.ROOT))) return true;
        }
        return false;
    }

    private Object hasParameterPrivilege(FunctionCallExpr fn, RowContext ctx) {
        List<Object> a = evalArgs(fn, ctx);
        if (a == null) return null;
        boolean withRole = a.size() >= 3;
        String role = withRole ? role(a.get(0), true) : currentUserName();
        String param = str(a.get(withRole ? 1 : 0)).toLowerCase(java.util.Locale.ROOT);
        // PostgreSQL does not require the parameter to exist: an unknown GUC simply has no grants.
        List<Priv> privs = parsePrivileges(str(a.get(withRole ? 2 : 1)), PARAMETER_PRIVS, false);
        for (Priv p : privs) {
            if (holds(role, p, "PARAMETER", param)) return true;
        }
        return false;
    }

    private Object pgHasRole(FunctionCallExpr fn, RowContext ctx) {
        List<Object> a = evalArgs(fn, ctx);
        if (a == null) return null;
        boolean withRole = a.size() >= 3;
        // pg_has_role uses get_role_oid, not the _or_public variant: PUBLIC is not a role here.
        String user = withRole ? role(a.get(0), false) : currentRoleName();
        String target = role(a.get(withRole ? 1 : 0), false);
        List<Priv> privs = parsePrivileges(str(a.get(withRole ? 2 : 1)), ROLE_PRIVS, true);
        for (Priv p : privs) {
            if (isMemberOfRole(user, target, p)) return true;
        }
        return false;
    }

    private boolean isMemberOfRole(String user, String target, Priv priv) {
        if (user.equals(target)) return true;
        // A superuser is a member of every role, however the question is spelled. PostgreSQL
        // answers that from the role's own attributes before it reads pg_auth_members, and it
        // does so for MEMBER as much as for USAGE and SET — asking about MEMBER walked the
        // memberships instead and answered false for a superuser who is a member of everything.
        Map<String, String> attrs = executor.database.getRoles().get(user);
        if (attrs != null && "true".equalsIgnoreCase(attrs.get("SUPERUSER"))) return true;
        if (priv.grantOption) return false;
        Map<String, Set<String>> memberships = executor.database.getRoleMemberships();
        Set<String> visited = new HashSet<>();
        java.util.Queue<String> queue = new java.util.ArrayDeque<>();
        queue.add(target);
        visited.add(target);
        while (!queue.isEmpty()) {
            String current = queue.poll();
            Set<String> directMembers = memberships.get(current);
            if (directMembers == null) continue;
            if (directMembers.contains(user)) return true;
            for (String member : directMembers) {
                if (visited.add(member)) queue.add(member);
            }
        }
        return false;
    }

    // ---- Argument resolution ----

    /** Evaluates every argument; returns null when any is NULL, since these functions are strict. */
    private List<Object> evalArgs(FunctionCallExpr fn, RowContext ctx) {
        List<Object> values = new ArrayList<>(fn.args().size());
        boolean sawNull = false;
        for (Expression arg : fn.args()) {
            Object value = executor.evalExpr(arg, ctx);
            if (value == null) sawNull = true;
            values.add(value);
        }
        return sawNull ? null : values;
    }

    private static String str(Object value) {
        return String.valueOf(value);
    }

    /**
     * Resolves a role argument. An OID cannot be mapped back to a name here, so it is taken to
     * mean the current user rather than reported as missing.
     */
    private String role(Object arg, boolean allowPublic) {
        if (arg instanceof Number) return currentUserName();
        String name = str(arg);
        String lower = name.toLowerCase(java.util.Locale.ROOT);
        if (allowPublic && "public".equals(lower)) return lower;
        if (executor.database.getRoles().containsKey(lower)) return lower;
        if (lower.equals(executor.sessionUser().toLowerCase(java.util.Locale.ROOT))) return lower;
        throw PgErrors.undefinedObject("role", name);
    }

    /** A relation the privilege functions can key on; raises 42P01 when nothing of that name exists. */
    private static final class Rel {
        final String schema;
        final String name;
        final Table table;
        final boolean sequence;

        Rel(String schema, String name, Table table, boolean sequence) {
            this.schema = schema;
            this.name = name;
            this.table = table;
            this.sequence = sequence;
        }
    }

    private Rel relation(Object arg) {
        if (arg instanceof Number) {
            return new Rel(executor.defaultSchema(), str(arg), null, false);
        }
        String raw = str(arg);
        String schema = executor.defaultSchema();
        String bare = raw;
        int dot = raw.indexOf('.');
        if (dot >= 0) {
            schema = raw.substring(0, dot);
            bare = raw.substring(dot + 1);
        }
        boolean isSequence = executor.database.getSequence(bare) != null;
        Table table = null;
        try {
            table = executor.resolveTable(schema, bare);
        } catch (MemgresException e) {
            // A missing schema is reported as such; every other failure means "try the other
            // relation kinds", since a view that cannot be updated is still a relation.
            if ("3F000".equals(e.getSqlState())) throw e;
        }
        if (table != null) return new Rel(schema, bare, isSequence ? null : table, isSequence);
        if (executor.database.getView(bare) != null) return new Rel(schema, bare, null, false);
        if (isSequence) return new Rel(schema, bare, null, true);
        Table catalog = catalogRelation(schema, bare);
        if (catalog != null) return new Rel(schema, bare, catalog, false);
        throw new MemgresException("relation \"" + raw + "\" does not exist", "42P01");
    }

    private Table catalogRelation(String schema, String bare) {
        String lowerSchema = schema == null ? "" : schema.toLowerCase(java.util.Locale.ROOT);
        if ("information_schema".equals(lowerSchema)) {
            return executor.systemCatalog.resolve("information_schema", bare);
        }
        if ("pg_catalog".equals(lowerSchema) || bare.toLowerCase(java.util.Locale.ROOT).startsWith("pg_")) {
            return executor.systemCatalog.resolve("pg_catalog", bare);
        }
        return null;
    }

    /** Negative attnums are system columns, whose privileges follow the table's. */
    private String columnByAttnum(Rel rel, int attnum) {
        if (attnum < 0) return "";
        if (attnum == 0 || rel.table == null) return null;
        List<Column> columns = rel.table.getColumns();
        // A dropped column keeps its number, so an attnum is not a position in what is left.
        int at = rel.table.columnIndexOfAttnum(attnum);
        if (at < 0 || at >= columns.size()) return null;
        return columns.get(at).getName().toLowerCase(java.util.Locale.ROOT);
    }

    private boolean schemaExists(String schema) {
        return executor.database.getSchema(schema) != null || IMPLICIT_USAGE_SCHEMAS.contains(schema)
                || "pg_toast".equals(schema);
    }

    private boolean databaseExists(String name) {
        if (executor.session != null && name.equalsIgnoreCase(executor.session.getDatabaseName())) return true;
        return catalogHasName("pg_database", "datname", name);
    }

    private boolean functionExists(String bare) {
        if (executor.database.getFunction(bare) != null) return true;
        for (String builtin : BuiltinFunctionNames.NAMES) {
            if (builtin.equals(bare)) return true;
        }
        return false;
    }

    private boolean typeExists(String name) {
        String lower = name.trim().toLowerCase(java.util.Locale.ROOT);
        if (lower.endsWith("[]")) lower = lower.substring(0, lower.length() - 2).trim();
        if (CatalogMetadataFunctions.canonicalTypeName(executor.database, lower) != null) return true;
        return catalogHasName("pg_type", "typname", lower);
    }

    private boolean catalogHasName(String catalog, String column, String value) {
        Table table = executor.systemCatalog.resolve("pg_catalog", catalog);
        if (table == null) return false;
        int index = table.getColumnIndex(column);
        if (index < 0) return false;
        for (Object[] row : table.getRows()) {
            if (row[index] != null && value.equalsIgnoreCase(String.valueOf(row[index]))) return true;
        }
        return false;
    }

    // ---- Privilege names ----

    /** One privilege from the comma-separated spec, with the grant-option suffix taken off. */
    private static final class Priv {
        final String name;
        final boolean grantOption;

        Priv(String name, boolean grantOption) {
            this.name = name;
            this.grantOption = grantOption;
        }
    }

    /**
     * Splits the spec the way PostgreSQL does — on commas, trimming each chunk — and rejects any
     * name the object kind does not accept. The answer is true when <em>any</em> listed privilege
     * is held.
     */
    private static List<Priv> parsePrivileges(String spec, String[] allowed, boolean roleStyle) {
        List<Priv> privs = new ArrayList<>();
        int from = 0;
        while (true) {
            int comma = spec.indexOf(',', from);
            String chunk = comma < 0 ? spec.substring(from) : spec.substring(from, comma);
            privs.add(matchPrivilege(chunk.trim(), allowed, roleStyle));
            if (comma < 0) return privs;
            from = comma + 1;
        }
    }

    private static Priv matchPrivilege(String chunk, String[] allowed, boolean roleStyle) {
        for (String name : allowed) {
            if (chunk.equalsIgnoreCase(name)) return new Priv(name, false);
            if (chunk.equalsIgnoreCase(name + " WITH GRANT OPTION")) return new Priv(name, true);
            if (roleStyle && chunk.equalsIgnoreCase(name + " WITH ADMIN OPTION")) {
                return new Priv(name, true);
            }
        }
        throw PgErrors.invalidParameter("unrecognized privilege type: \"" + chunk + "\"");
    }

    // ---- Privilege checking ----

    private boolean holds(String role, Priv priv, String objectType, String objectName) {
        if (priv.grantOption) return checkGrantOption(role, priv.name, objectType, objectName);
        return checkPrivilege(role, priv.name, objectType, objectName);
    }

    private boolean checkGrantOption(String role, String privilege, String objectType, String objectName) {
        // Superusers and owners always hold the grant option on what they can already grant.
        Map<String, String> roleAttrs = executor.database.getRoles().get(role);
        if (roleAttrs != null && "true".equalsIgnoreCase(roleAttrs.get("SUPERUSER"))) return true;
        String owner = executor.database.getObjectOwner(ownerKey(objectType, objectName));
        if (owner != null && owner.equalsIgnoreCase(role)) return true;
        return checkPrivilegeDirectOrInherited(role, privilege + "_GRANT_OPTION", objectType,
                objectName, new HashSet<String>());
    }

    boolean checkPrivilege(String roleName, String privilege, String objectType, String objectName) {
        String role = roleName.toLowerCase(java.util.Locale.ROOT);
        String object = objectName.toLowerCase(java.util.Locale.ROOT);

        Map<String, String> roleAttrs = executor.database.getRoles().get(role);
        if (roleAttrs != null && "true".equalsIgnoreCase(roleAttrs.get("SUPERUSER"))) {
            return true;
        }

        String owner = executor.database.getObjectOwner(ownerKey(objectType, object));
        if (owner != null && owner.equalsIgnoreCase(role)) {
            return true;
        }

        if (checkPrivilegeDirectOrInherited(role, privilege, objectType, object, new HashSet<String>())) {
            return true;
        }
        // A grant to PUBLIC reaches every role, so it has to be consulted for all of them.
        return !"public".equals(role)
                && checkPrivilegeDirectOrInherited("public", privilege, objectType, object,
                        new HashSet<String>());
    }

    private static String ownerKey(String objectType, String objectName) {
        String kind = objectType.toLowerCase(java.util.Locale.ROOT);
        if ("table".equals(kind) || "function".equals(kind) || "schema".equals(kind)
                || "sequence".equals(kind) || "database".equals(kind) || "type".equals(kind)) {
            return kind + ":" + objectName.toLowerCase(java.util.Locale.ROOT);
        }
        // A kind this method does not know cannot be allowed to key the same as one it does,
        // so it is marked. The marker is not an object type and cannot be written as one.
        return "?none:" + objectName;
    }

    private boolean checkPrivilegeDirectOrInherited(String roleName, String privilege,
            String objectType, String objectName, Set<String> visited) {
        if (!visited.add(roleName)) return false;

        Set<String> privs = executor.database.getRolePrivileges(roleName);
        String object = objectName.toLowerCase(java.util.Locale.ROOT);
        String checkKey = privilege.toUpperCase(java.util.Locale.ROOT) + ":" + objectType.toUpperCase(java.util.Locale.ROOT) + ":" + object;
        String allKey = "ALL:" + objectType.toUpperCase(java.util.Locale.ROOT) + ":" + object;
        if (privs.contains(checkKey) || privs.contains(allKey)) {
            return true;
        }

        Map<String, Set<String>> memberships = executor.database.getRoleMemberships();
        for (Map.Entry<String, Set<String>> entry : memberships.entrySet()) {
            String grantedRole = entry.getKey();
            if (entry.getValue().contains(roleName) && !visited.contains(grantedRole)) {
                if (checkPrivilegeDirectOrInherited(grantedRole, privilege, objectType, objectName, visited)) {
                    return true;
                }
            }
        }
        return false;
    }

    // ---- Session identity ----

    String currentUserName() {
        if (executor.session != null) {
            GucSettings guc = executor.session.getGucSettings();
            if (guc.hasSessionOverride("role")) {
                String role = guc.get("role");
                if (role != null && !role.equalsIgnoreCase("NONE") && !role.equalsIgnoreCase("DEFAULT")) {
                    return role.toLowerCase(java.util.Locale.ROOT);
                }
            }
            String sessionAuth = guc.get("session_authorization");
            if (sessionAuth != null) return sessionAuth.toLowerCase(java.util.Locale.ROOT);
        }
        return "memgres";
    }

    private String currentRoleName() {
        if (executor.session != null && executor.session.getGucSettings().hasSessionOverride("role")) {
            String role = executor.session.getGucSettings().get("role");
            if (role != null) return role.toLowerCase(java.util.Locale.ROOT);
        }
        return "memgres";
    }
}
