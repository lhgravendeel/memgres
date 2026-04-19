package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.*;

import static com.memgres.engine.CatalogHelper.*;

/**
 * Builds pg_catalog virtual tables related to security, roles, authentication,
 * sessions, and related metadata.
 * Extracted from PgCatalogBuilder to separate concerns.
 */
class CatalogSecurityBuilder {

    final Database database;
    final OidSupplier oids;

    CatalogSecurityBuilder(Database database, OidSupplier oids) {
        this.database = database;
        this.oids = oids;
    }

    Table buildPgSettings(GucSettings sessionGuc) {
        List<Column> cols = Cols.listOf(
                colNN("name", DataType.TEXT),
                colNN("setting", DataType.TEXT),
                col("unit", DataType.TEXT),
                col("category", DataType.TEXT),
                col("short_desc", DataType.TEXT),
                col("extra_desc", DataType.TEXT),
                col("context", DataType.TEXT),
                col("vartype", DataType.TEXT),
                col("source", DataType.TEXT),
                col("min_val", DataType.TEXT),
                col("max_val", DataType.TEXT),
                col("enumvals", DataType.TEXT_ARRAY),
                col("boot_val", DataType.TEXT),
                col("reset_val", DataType.TEXT),
                col("pending_restart", DataType.BOOLEAN),
                col("sourcefile", DataType.TEXT),
                col("sourceline", DataType.INTEGER)
        );
        Table table = new Table("pg_settings", cols);
        GucSettings defaults = new GucSettings();
        Map<String, String> all = defaults.getAll();
        // If session GUC is provided, merge session overrides so new keys appear too
        if (sessionGuc != null) {
            for (Map.Entry<String, String> e : sessionGuc.getAll().entrySet()) {
                if (!all.containsKey(e.getKey())) {
                    all.put(e.getKey(), e.getValue());
                }
            }
        }
        // Category and description info for well-known settings
        Map<String, String[]> meta = new LinkedHashMap<>();
        meta.put("server_version", new String[]{"Preset Options", "Shows the server version.", "internal", "string"});
        meta.put("server_version_num", new String[]{"Preset Options", "Shows the server version as an integer.", "internal", "string"});
        meta.put("server_encoding", new String[]{"Client Connection Defaults", "Shows the server (database) character set encoding.", "internal", "string"});
        meta.put("client_encoding", new String[]{"Client Connection Defaults", "Sets the client's character set encoding.", "user", "string"});
        meta.put("search_path", new String[]{"Client Connection Defaults", "Sets the schema search order for names that are not schema-qualified.", "user", "string"});
        meta.put("timezone", new String[]{"Client Connection Defaults / Locale and Formatting", "Sets the time zone for displaying and interpreting time stamps.", "user", "string"});
        meta.put("datestyle", new String[]{"Client Connection Defaults / Locale and Formatting", "Sets the display format for date and time values.", "user", "string"});
        meta.put("intervalstyle", new String[]{"Client Connection Defaults / Locale and Formatting", "Sets the display format for interval values.", "user", "string"});
        meta.put("standard_conforming_strings", new String[]{"Version and Platform Compatibility", "Causes '...' strings to treat backslashes literally.", "user", "bool"});
        meta.put("max_connections", new String[]{"Connections and Authentication", "Sets the maximum number of concurrent connections.", "postmaster", "integer"});
        meta.put("shared_buffers", new String[]{"Resource Usage / Memory", "Sets the number of shared memory buffers used by the server.", "postmaster", "string"});
        meta.put("work_mem", new String[]{"Resource Usage / Memory", "Sets the maximum memory to be used for query workspaces.", "user", "string"});
        meta.put("default_transaction_isolation", new String[]{"Client Connection Defaults", "Sets the transaction isolation level of each new transaction.", "user", "enum"});
        meta.put("transaction_isolation", new String[]{"Client Connection Defaults", "Sets the current transaction's isolation level.", "user", "enum"});
        meta.put("lc_collate", new String[]{"Preset Options", "Shows the collation order locale.", "internal", "string"});
        meta.put("lc_ctype", new String[]{"Preset Options", "Shows the character classification and case conversion locale.", "internal", "string"});
        // Unit metadata for known parameters
        Map<String, String> units = new LinkedHashMap<>();
        units.put("shared_buffers", "8kB");
        units.put("work_mem", "kB");
        units.put("maintenance_work_mem", "kB");
        units.put("effective_cache_size", "8kB");
        units.put("statement_timeout", "ms");
        units.put("lock_timeout", "ms");
        units.put("idle_in_transaction_session_timeout", "ms");
        units.put("transaction_timeout", "ms");
        units.put("track_activity_query_size", "B");
        // min_val / max_val for bounded numeric parameters
        Map<String, String[]> bounds = new LinkedHashMap<>();
        // bounds: [min_val, max_val]
        bounds.put("work_mem", new String[]{"64", "2147483647"});
        bounds.put("maintenance_work_mem", new String[]{"1024", "2147483647"});
        bounds.put("shared_buffers", new String[]{"16", "1073741823"});
        bounds.put("effective_cache_size", new String[]{"1", "2147483647"});
        bounds.put("max_connections", new String[]{"1", "262143"});
        bounds.put("statement_timeout", new String[]{"0", "2147483647"});
        bounds.put("lock_timeout", new String[]{"0", "2147483647"});
        bounds.put("idle_in_transaction_session_timeout", new String[]{"0", "2147483647"});
        // enumvals for enum parameters
        Map<String, String> enumvals = new LinkedHashMap<>();
        enumvals.put("client_min_messages", "{debug5,debug4,debug3,debug2,debug1,log,notice,warning,error}");
        enumvals.put("log_min_messages", "{debug5,debug4,debug3,debug2,debug1,info,notice,warning,error,log,fatal,panic}");
        enumvals.put("default_transaction_isolation", "{serializable,repeatable read,read committed,read uncommitted}");
        enumvals.put("transaction_isolation", "{serializable,repeatable read,read committed,read uncommitted}");
        for (Map.Entry<String, String> entry : all.entrySet()) {
            String name = entry.getKey();
            String bootValue = entry.getValue();
            // If session GUC is available, use the session's current value for 'setting'
            String settingValue = (sessionGuc != null) ? sessionGuc.get(name) : bootValue;
            if (settingValue == null) settingValue = bootValue;
            String source = (sessionGuc != null && sessionGuc.hasSessionOverride(name)) ? "session" : "default";
            String[] m = meta.get(name);
            String category = m != null ? m[0] : "Ungrouped";
            String desc = m != null ? m[1] : "";
            String ctx = m != null ? m[2] : "user";
            String vartype = m != null ? m[3] : "string";
            String unit = units.get(name);
            String[] bound = bounds.get(name);
            String minVal = bound != null ? bound[0] : null;
            String maxVal = bound != null ? bound[1] : null;
            String enumval = enumvals.get(name);
            table.insertRow(new Object[]{name, settingValue, unit, category, desc, null, ctx, vartype, source, minVal, maxVal, enumval, bootValue, bootValue, false, null, null});
        }
        return table;
    }

    Table buildPgDatabase() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("datname", DataType.TEXT),
                colNN("datdba", DataType.INTEGER),
                col("encoding", DataType.INTEGER),
                col("datlocprovider", DataType.CHAR),
                col("datistemplate", DataType.BOOLEAN),
                col("datallowconn", DataType.BOOLEAN),
                col("datconnlimit", DataType.INTEGER),
                col("datfrozenxid", DataType.XID),
                col("datminmxid", DataType.INTEGER),
                col("dattablespace", DataType.INTEGER),
                col("datcollate", DataType.TEXT),
                col("datctype", DataType.TEXT),
                col("datlocale", DataType.TEXT),
                col("daticurules", DataType.TEXT),
                col("datcollversion", DataType.TEXT),
                col("datacl", DataType.ACLITEM_ARRAY)
        );
        Table table = new Table("pg_database", cols);
        int tsOid = oids.oid("tablespace:pg_default");

        // Add template databases (always present in PostgreSQL)
        table.insertRow(new Object[]{
                oids.oid("db:template0"), "template0", 10, 6,
                "c", true, false, -1,
                722, 1, tsOid,
                "en_US.UTF-8", "en_US.UTF-8",
                null, null, null, null
        });
        table.insertRow(new Object[]{
                oids.oid("db:template1"), "template1", 10, 6,
                "c", true, true, -1,
                722, 1, tsOid,
                "en_US.UTF-8", "en_US.UTF-8",
                null, null, null, null
        });

        // Dynamically list all databases from the registry
        DatabaseRegistry reg = database.getDatabaseRegistry();
        if (reg != null) {
            for (String dbName : reg.getDatabaseNames()) {
                table.insertRow(new Object[]{
                        oids.oid("db:" + dbName), dbName, 10, 6,
                        "c", false, true, -1,
                        722, 1, tsOid,
                        "en_US.UTF-8", "en_US.UTF-8",
                        null, null, null, null
                });
            }
        } else {
            // Fallback when no registry is available
            table.insertRow(new Object[]{
                    oids.oid("db:memgres"), "memgres", 10, 6,
                    "c", false, true, -1,
                    722, 1, tsOid,
                    "en_US.UTF-8", "en_US.UTF-8",
                    null, null, null, null
            });
        }

        return table;
    }

    Table buildPgRoles() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("rolname", DataType.TEXT),
                col("rolsuper", DataType.BOOLEAN),
                col("rolinherit", DataType.BOOLEAN),
                col("rolcreaterole", DataType.BOOLEAN),
                col("rolcreatedb", DataType.BOOLEAN),
                col("rolcanlogin", DataType.BOOLEAN),
                col("rolreplication", DataType.BOOLEAN),
                col("rolconnlimit", DataType.INTEGER),
                col("rolvaliduntil", DataType.TIMESTAMPTZ),
                col("rolbypassrls", DataType.BOOLEAN),
                col("rolconfig", DataType.TEXT),
                col("rolpassword", DataType.TEXT)
        );
        Table table = new Table("pg_roles", cols);
        for (Map.Entry<String, Map<String, String>> entry : database.getRoles().entrySet()) {
            Map<String, String> attrs = entry.getValue();
            int connLimit = -1;
            String climRaw = attrs.get("CONNECTION_LIMIT");
            if (climRaw != null) {
                try { connLimit = Integer.parseInt(climRaw.trim()); } catch (NumberFormatException ignore) {}
            }
            table.insertRow(new Object[]{
                    oids.oid("role:" + entry.getKey()), entry.getKey(),
                    "true".equalsIgnoreCase(attrs.getOrDefault("SUPERUSER", "false")),
                    "true".equalsIgnoreCase(attrs.getOrDefault("INHERIT", "true")),
                    "true".equalsIgnoreCase(attrs.getOrDefault("CREATEROLE", "false")),
                    "true".equalsIgnoreCase(attrs.getOrDefault("CREATEDB", "false")),
                    "true".equalsIgnoreCase(attrs.getOrDefault("LOGIN", "false")),
                    "true".equalsIgnoreCase(attrs.getOrDefault("REPLICATION", "false")),
                    connLimit, parseValidUntil(attrs.get("VALID_UNTIL")),
                    "true".equalsIgnoreCase(attrs.getOrDefault("BYPASSRLS", "false")),
                    buildRolconfig(attrs.get("ROLCONFIG")), "********" // rolconfig, rolpassword (always masked in pg_roles)
            });
        }
        // PG18 default system roles (OIDs match PG 18 dynamic assignment, starting at 6168)
        int sysOid = 6168;
        String[][] sysRoles = {
            {"pg_database_owner"},
            {"pg_read_all_data"},
            {"pg_write_all_data"},
            {"pg_monitor"},
            {"pg_read_all_settings"},
            {"pg_read_all_stats"},
            {"pg_stat_scan_tables"},
            {"pg_read_server_files"},
            {"pg_write_server_files"},
            {"pg_execute_server_program"},
            {"pg_signal_backend"},
            {"pg_checkpoint"},
            {"pg_use_reserved_connections"},
            {"pg_create_subscription"},
            {"pg_maintain"},
        };
        for (String[] r : sysRoles) {
            table.insertRow(new Object[]{
                    sysOid++, r[0],
                    false, // not super
                    true,  // inherit
                    false, // no createrole
                    false, // no createdb
                    false, // no login
                    false, // no replication
                    -1, null, false, null, null // rolpassword null for system roles
            });
        }
        return table;
    }

    private static Object parseValidUntil(String raw) {
        if (raw == null || raw.isEmpty()) return null;
        try {
            // Try parsing as timestamptz (various formats)
            return java.time.OffsetDateTime.parse(raw, java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        } catch (Exception e1) {
            try {
                // Try date-only format
                java.time.LocalDate ld = java.time.LocalDate.parse(raw);
                return ld.atStartOfDay().atOffset(java.time.ZoneOffset.UTC);
            } catch (Exception e2) {
                try {
                    // Try with space separator (e.g., "2099-01-01 00:00:00+00")
                    String normalized = raw.replace(" ", "T");
                    if (!normalized.contains("+") && !normalized.contains("Z") && !normalized.matches(".*-\\d{2}$")) {
                        normalized += "Z";
                    }
                    return java.time.OffsetDateTime.parse(normalized, java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                } catch (Exception e3) {
                    return raw; // Return as string if parsing fails
                }
            }
        }
    }

    /**
     * Build a PG-style text array literal from the ROLCONFIG attribute.
     * Format: "{work_mem=42MB,search_path=public}" stored as text.
     */
    private static String buildRolconfig(String raw) {
        if (raw == null || raw.isEmpty()) return null;
        return "{" + raw + "}";
    }

    Table buildPgUser() {
        List<Column> cols = Cols.listOf(
                colNN("usesysid", DataType.INTEGER),
                colNN("usename", DataType.TEXT),
                col("usesuper", DataType.BOOLEAN),
                col("usecreatedb", DataType.BOOLEAN),
                col("passwd", DataType.TEXT),
                col("valuntil", DataType.TIMESTAMPTZ)
        );
        Table table = new Table("pg_user", cols);
        for (Map.Entry<String, Map<String, String>> entry : database.getRoles().entrySet()) {
            Map<String, String> attrs = entry.getValue();
            boolean canLogin = "true".equalsIgnoreCase(attrs.getOrDefault("LOGIN", "false"));
            if (canLogin) {
                table.insertRow(new Object[]{
                        oids.oid("role:" + entry.getKey()), entry.getKey(),
                        "true".equalsIgnoreCase(attrs.getOrDefault("SUPERUSER", "false")),
                        "true".equalsIgnoreCase(attrs.getOrDefault("CREATEDB", "false")),
                        null, null
                });
            }
        }
        return table;
    }

    Table buildPgAuthMembers() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("roleid", DataType.INTEGER),
                colNN("member", DataType.INTEGER),
                colNN("grantor", DataType.INTEGER),
                col("admin_option", DataType.BOOLEAN),
                col("inherit_option", DataType.BOOLEAN),
                col("set_option", DataType.BOOLEAN)
        );
        Table table = new Table("pg_auth_members", cols);
        int rowOid = 1;
        for (Map.Entry<String, java.util.Set<String>> entry : database.getRoleMemberships().entrySet()) {
            String grantedRole = entry.getKey(); // the role being granted (roleid)
            int roleOid = oids.oid("role:" + grantedRole);
            for (String memberRole : entry.getValue()) {
                int memberOid = oids.oid("role:" + memberRole);
                boolean admin = database.hasAdminOption(grantedRole, memberRole);
                table.insertRow(new Object[]{
                        rowOid++, roleOid, memberOid, 10 /* bootstrap superuser */,
                        admin, true, true
                });
            }
        }
        return table;
    }

    Table buildPgPolicy() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("polname", DataType.TEXT),
                colNN("polrelid", DataType.INTEGER),
                col("polcmd", DataType.CHAR),
                col("polpermissive", DataType.BOOLEAN),
                col("polroles", DataType.TEXT),
                col("polqual", DataType.TEXT),
                col("polwithcheck", DataType.TEXT),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_policy", cols);
        int rowOid = oids.oid("pg_policy:base");
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            String schemaName = schemaEntry.getKey();
            for (Map.Entry<String, com.memgres.engine.Table> tableEntry
                    : schemaEntry.getValue().getTables().entrySet()) {
                com.memgres.engine.Table t = tableEntry.getValue();
                int relOid = oids.oid("rel:" + schemaName + "." + t.getName());
                for (RlsPolicy policy : t.getRlsPolicies()) {
                    String cmdChar = "*"; // default = ALL
                    if (policy.getCommand() != null) {
                        switch (policy.getCommand().toUpperCase()) {
                            case "SELECT": cmdChar = "r"; break;
                            case "INSERT": cmdChar = "a"; break;
                            case "UPDATE": cmdChar = "w"; break;
                            case "DELETE": cmdChar = "d"; break;
                            default: cmdChar = "*"; break;
                        }
                    }
                    boolean permissive = !"RESTRICTIVE".equalsIgnoreCase(policy.getPolicyType());
                    String rolesText = null;
                    if (policy.getRoles() != null && !policy.getRoles().isEmpty()) {
                        rolesText = "{" + String.join(",", policy.getRoles()) + "}";
                    }
                    String qualText = policy.getUsingExpr() != null
                            ? SqlUnparser.exprToSql(policy.getUsingExpr()) : null;
                    String withCheckText = policy.getWithCheckExpr() != null
                            ? SqlUnparser.exprToSql(policy.getWithCheckExpr()) : null;
                    table.insertRow(new Object[]{
                            rowOid++, policy.getName(), relOid, cmdChar,
                            permissive, rolesText, qualText, withCheckText, 1
                    });
                }
            }
        }
        return table;
    }

    /** pg_policies view: one row per RLS policy, keyed by schema + table + policy name. */
    Table buildPgPolicies() {
        List<Column> cols = Cols.listOf(
                col("schemaname", DataType.TEXT),
                colNN("tablename", DataType.TEXT),
                colNN("policyname", DataType.TEXT),
                col("permissive", DataType.TEXT),
                col("roles", DataType.NAME_ARRAY),
                col("cmd", DataType.TEXT),
                col("qual", DataType.TEXT),
                col("with_check", DataType.TEXT)
        );
        Table table = new Table("pg_policies", cols);
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            String schemaName = schemaEntry.getKey();
            for (Map.Entry<String, com.memgres.engine.Table> tableEntry
                    : schemaEntry.getValue().getTables().entrySet()) {
                com.memgres.engine.Table t = tableEntry.getValue();
                for (RlsPolicy policy : t.getRlsPolicies()) {
                    String rolesText;
                    List<String> roles = policy.getRoles();
                    if (roles == null || roles.isEmpty()) {
                        rolesText = "{public}";
                    } else {
                        rolesText = "{" + String.join(",", roles) + "}";
                    }
                    String qualText = policy.getUsingExpr() != null
                            ? SqlUnparser.exprToSql(policy.getUsingExpr()) : null;
                    String withCheckText = policy.getWithCheckExpr() != null
                            ? SqlUnparser.exprToSql(policy.getWithCheckExpr()) : null;
                    table.insertRow(new Object[]{
                            schemaName,
                            t.getName(),
                            policy.getName(),
                            policy.getPolicyType(),
                            rolesText,
                            policy.getCommand(),
                            qualText,
                            withCheckText
                    });
                }
            }
        }
        return table;
    }

    /** pg_default_acl: rows from ALTER DEFAULT PRIVILEGES statements. */
    Table buildPgDefaultAcl() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.INTEGER),
                colNN("defaclrole", DataType.INTEGER),
                colNN("defaclnamespace", DataType.INTEGER),
                colNN("defaclobjtype", DataType.CHAR),
                col("defaclacl", DataType.TEXT)
        );
        Table table = new Table("pg_default_acl", cols);
        int rowOid = oids.oid("pg_default_acl:base");
        for (Database.DefaultAclEntry entry : database.getDefaultAcls()) {
            if (!entry.isGrant) continue; // only GRANT entries are visible
            char objType = objectTypeChar(entry.objectType);
            int nsOid = entry.schema != null ? oids.oid("ns:" + entry.schema) : 0;
            String aclText = entry.grantees.isEmpty() ? null
                    : String.join(",", entry.grantees) + "=" + String.join(",", entry.privileges);
            int grantorOid = entry.grantor != null ? oids.oid("role:" + entry.grantor) : 10;
            table.insertRow(new Object[]{
                    rowOid++,
                    grantorOid,
                    nsOid,
                    String.valueOf(objType),
                    aclText
            });
        }
        return table;
    }

    Table buildPgStatActivity() {
        List<Column> cols = Cols.listOf(
                col("datid", DataType.INTEGER),
                col("datname", DataType.TEXT),
                col("pid", DataType.INTEGER),
                col("leader_pid", DataType.INTEGER),
                col("usesysid", DataType.INTEGER),
                col("usename", DataType.TEXT),
                col("application_name", DataType.TEXT),
                col("client_addr", DataType.TEXT),
                col("client_hostname", DataType.TEXT),
                col("client_port", DataType.INTEGER),
                col("backend_start", DataType.TIMESTAMPTZ),
                col("xact_start", DataType.TIMESTAMPTZ),
                col("query_start", DataType.TIMESTAMPTZ),
                col("state_change", DataType.TIMESTAMPTZ),
                col("wait_event_type", DataType.TEXT),
                col("wait_event", DataType.TEXT),
                col("state", DataType.TEXT),
                col("backend_xid", DataType.INTEGER),
                col("backend_xmin", DataType.INTEGER),
                col("query_id", DataType.BIGINT),
                col("query", DataType.TEXT),
                col("backend_type", DataType.TEXT)
        );
        Table table = new Table("pg_stat_activity", cols);
        for (Session s : database.getActiveSessions()) {
            String dbName = s.getDatabaseName();
            int dbOid = oids.oid("db:" + dbName);
            String user = s.getConnectingUser();
            int usesysid = user != null ? oids.oid("role:" + user) : 10;
            table.insertRow(new Object[]{
                    dbOid, dbName, s.getPid(),
                    null,       // leader_pid
                    usesysid, user,
                    s.getApplicationName(),
                    null, null, -1,  // client_addr, client_hostname, client_port (local only)
                    s.getBackendStart(),
                    s.getXactStart(),
                    s.getQueryStart(),
                    s.getStateChange(),
                    null, null,      // wait_event_type, wait_event
                    s.getState(),
                    null, null,      // backend_xid, backend_xmin
                    computeQueryId(s), // query_id
                    s.getCurrentQuery(),
                    "client backend"
            });
        }
        return table;
    }

    /**
     * Compute a query_id for pg_stat_activity. Returns null if compute_query_id
     * is off, or a hash of the current query text otherwise.
     */
    private Long computeQueryId(Session s) {
        if (s.getCurrentQuery() == null || s.getCurrentQuery().isEmpty()) return null;
        GucSettings guc = s.getGucSettings();
        if (guc == null) return null;
        String setting = guc.get("compute_query_id");
        if (setting == null || "off".equalsIgnoreCase(setting)) return null;
        // "on" or "auto": compute a hash of the query text
        long hash = s.getCurrentQuery().hashCode();
        // Ensure non-zero (PG query_id is always non-zero when computed)
        return hash == 0 ? 1L : hash;
    }

    Table buildPgLocks() {
        List<Column> cols = Cols.listOf(
                col("locktype", DataType.TEXT),
                col("database", DataType.INTEGER),
                col("relation", DataType.INTEGER),
                col("page", DataType.INTEGER),
                col("tuple", DataType.SMALLINT),
                col("virtualxid", DataType.TEXT),
                col("transactionid", DataType.INTEGER),
                col("classid", DataType.INTEGER),
                col("objid", DataType.INTEGER),
                col("objsubid", DataType.SMALLINT),
                col("virtualtransaction", DataType.TEXT),
                col("pid", DataType.INTEGER),
                col("mode", DataType.TEXT),
                col("granted", DataType.BOOLEAN),
                col("fastpath", DataType.BOOLEAN),
                col("waitstart", DataType.TIMESTAMPTZ)
        );
        Table table = new Table("pg_locks", cols);
        int dbOid = oids.oid("db:memgres");

        // Expose relation locks for all tables (simulating AccessShareLock held by active sessions)
        for (Session s : database.getActiveSessions()) {
            for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
                String schemaName = schemaEntry.getKey();
                for (Map.Entry<String, Table> tableEntry : schemaEntry.getValue().getTables().entrySet()) {
                    int relOid = oids.oid("rel:" + schemaName + "." + tableEntry.getKey());
                    table.insertRow(new Object[]{
                            "relation", dbOid, relOid, null, null, null, null,
                            null, null, null,
                            String.valueOf(s.getPid()) + "/1", s.getPid(),
                            "AccessShareLock", true, false, null
                    });
                }
            }
        }

        // Expose explicit table locks acquired via LOCK TABLE
        for (Session s : database.getActiveSessions()) {
            for (Map.Entry<String, String> lockEntry : s.getTableLocks().entrySet()) {
                String tableKey = lockEntry.getKey();
                String lockMode = lockEntry.getValue();
                int relOid = oids.oid("rel:" + tableKey);
                table.insertRow(new Object[]{
                        "relation", dbOid, relOid, null, null, null, null,
                        null, null, null,
                        String.valueOf(s.getPid()) + "/1", s.getPid(),
                        lockMode, true, false, null
                });
            }
        }

        // Expose advisory locks
        for (Map.Entry<Long, java.util.Set<Session>> entry : database.getAdvisoryLocks().entrySet()) {
            long key = entry.getKey();
            for (Session s : entry.getValue()) {
                table.insertRow(new Object[]{
                        "advisory", dbOid, null, null, null, null, null,
                        0, (int) key, (short) 1,
                        String.valueOf(s.getPid()) + "/1", s.getPid(),
                        "ExclusiveLock", true, false, null
                });
            }
        }
        return table;
    }
}
