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
        GucSettings guc = sessionGuc != null ? sessionGuc : new GucSettings();
        for (Map.Entry<String, String> entry : guc.getAll().entrySet()) {
            String name = entry.getKey();
            // The metadata travels with the setting's own definition, so a parameter answers
            // for its own type, context and bounds rather than for settings in general.
            GucSettings.Def def = GucSettings.definition(name);
            String settingValue = guc.get(name);
            if (settingValue == null) settingValue = entry.getValue();
            String resetValue = def != null ? guc.getResetValue(name) : settingValue;
            if (resetValue == null) resetValue = settingValue;
            // A transaction's own settings are not chosen by the session at all: the transaction
            // machinery assigns them when it starts, and PostgreSQL reports that as "override"
            // so a client can tell a value it set from a value the server imposed.
            String source = guc.hasSessionOverride(name) ? "session"
                    : (SessionExecutor.isTransactionScopedGuc(name) ? "override" : "default");
            // L12: PG exposes mixed-case canonical names (e.g. TimeZone, DateStyle)
            // in pg_settings.name.
            String displayName = guc.getCanonicalName(name);
            table.insertRow(new Object[]{displayName, settingValue,
                    def != null ? def.unit : null,
                    def != null ? def.category : "Customized Options",
                    def != null ? def.shortDesc : null,
                    def != null ? def.extraDesc : null,
                    def != null ? def.context : "user",
                    def != null ? def.vartype : "string",
                    source,
                    def != null ? def.minVal : null,
                    def != null ? def.maxVal : null,
                    def != null ? def.enumVals : null,
                    def != null ? def.bootVal : settingValue,
                    resetValue, false, null, null});
        }
        return table;
    }

    Table buildPgDatabase() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("datname", DataType.NAME),
                colNN("datdba", DataType.OID),
                col("encoding", DataType.INTEGER),
                col("datlocprovider", DataType.INTERNAL_CHAR),
                col("datistemplate", DataType.BOOLEAN),
                col("datallowconn", DataType.BOOLEAN),
                col("datconnlimit", DataType.INTEGER),
                col("datfrozenxid", DataType.XID),
                col("datminmxid", DataType.XID),
                col("dattablespace", DataType.OID),
                col("datcollate", DataType.TEXT),
                col("datctype", DataType.TEXT),
                col("datlocale", DataType.TEXT),
                col("daticurules", DataType.TEXT),
                col("datcollversion", DataType.TEXT),
                col("dathasloginevt", DataType.BOOLEAN),
                col("datacl", DataType.ACLITEM_ARRAY)
        );
        Table table = new Table("pg_database", cols);
        int tsOid = oids.oid("tablespace:pg_default");

        // Add template databases (always present in PostgreSQL). Both carry the list initdb
        // writes for them, which gives everyone the right to connect and nothing else: read as
        // no list at all, a template answered that every role could create temporary tables in
        // it, which is the one right the list deliberately withholds.
        String templateAcl = "{=c/memgres,memgres=CTc/memgres}";
        table.insertRow(new Object[]{
                oids.oid("db:template0"), "template0", 10, 6,
                "c", true, false, -1,
                722, 1, tsOid,
                "en_US.UTF-8", "en_US.UTF-8",
                null, null, null, false, templateAcl
        });
        table.insertRow(new Object[]{
                oids.oid("db:template1"), "template1", 10, 6,
                "c", true, true, -1,
                722, 1, tsOid,
                "en_US.UTF-8", "en_US.UTF-8",
                null, null, null, false, templateAcl
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
                        null, null, null, false, null
                });
            }
        } else {
            // Fallback when no registry is available
            table.insertRow(new Object[]{
                    oids.oid("db:memgres"), "memgres", 10, 6,
                    "c", false, true, -1,
                    722, 1, tsOid,
                    "en_US.UTF-8", "en_US.UTF-8",
                    null, null, null, false, null
            });
        }

        return table;
    }

    Table buildPgRoles() {
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("rolname", DataType.NAME),
                col("rolsuper", DataType.BOOLEAN),
                col("rolinherit", DataType.BOOLEAN),
                col("rolcreaterole", DataType.BOOLEAN),
                col("rolcreatedb", DataType.BOOLEAN),
                col("rolcanlogin", DataType.BOOLEAN),
                col("rolreplication", DataType.BOOLEAN),
                col("rolconnlimit", DataType.INTEGER),
                col("rolvaliduntil", DataType.TIMESTAMPTZ),
                col("rolbypassrls", DataType.BOOLEAN),
                col("rolconfig", DataType.TEXT_ARRAY),
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
            // A superuser bypasses row-level security whatever the catalog says, and PostgreSQL
            // reports it: the bootstrap superuser's rolbypassrls is true. An RLS-auditing tool
            // reads this column to decide whose reads the policies actually constrain, so
            // reporting false for a superuser tells it the opposite of the truth.
            boolean superuser = "true".equalsIgnoreCase(attrs.getOrDefault("SUPERUSER", "false"));
            table.insertRow(new Object[]{
                    oids.oid("role:" + entry.getKey()), entry.getKey(),
                    superuser,
                    "true".equalsIgnoreCase(attrs.getOrDefault("INHERIT", "true")),
                    "true".equalsIgnoreCase(attrs.getOrDefault("CREATEROLE", "false")),
                    "true".equalsIgnoreCase(attrs.getOrDefault("CREATEDB", "false")),
                    "true".equalsIgnoreCase(attrs.getOrDefault("LOGIN", "false")),
                    "true".equalsIgnoreCase(attrs.getOrDefault("REPLICATION", "false")),
                    connLimit, parseValidUntil(attrs.get("VALID_UNTIL")),
                    "true".equalsIgnoreCase(
                            attrs.getOrDefault("BYPASSRLS", String.valueOf(superuser))),
                    buildRolconfig(attrs.get("ROLCONFIG")), "********" // rolconfig, rolpassword (always masked in pg_roles)
            });
        }
        // PG18 default system roles (OIDs match PG 18 dynamic assignment, starting at 6168)
        String[][] sysRoles = PREDEFINED_SYSTEM_ROLES;
        for (String[] r : sysRoles) {
            table.insertRow(new Object[]{
                    Integer.parseInt(r[1]), r[0],
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

    /**
     * pg_authid is the stored table pg_roles is a view over. It holds the real password and, being
     * a table rather than a view, has no rolconfig column — that one lives in pg_db_role_setting
     * and is only assembled by the view.
     */
    Table buildPgAuthid() {
        Table roles = buildPgRoles();
        List<Column> cols = Cols.listOf(
                colNN("oid", DataType.OID),
                colNN("rolname", DataType.NAME),
                col("rolsuper", DataType.BOOLEAN),
                col("rolinherit", DataType.BOOLEAN),
                col("rolcreaterole", DataType.BOOLEAN),
                col("rolcreatedb", DataType.BOOLEAN),
                col("rolcanlogin", DataType.BOOLEAN),
                col("rolreplication", DataType.BOOLEAN),
                col("rolbypassrls", DataType.BOOLEAN),
                col("rolconnlimit", DataType.INTEGER),
                col("rolpassword", DataType.TEXT),
                col("rolvaliduntil", DataType.TIMESTAMPTZ)
        );
        Table table = new Table("pg_authid", cols);
        for (Object[] r : roles.getRows()) {
            table.insertRow(new Object[]{
                    r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7],
                    r[10],  // rolbypassrls
                    r[8],   // rolconnlimit
                    r[12],  // rolpassword
                    r[9]    // rolvaliduntil
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
        // An element holding a comma or a space is quoted, the way an array of text is printed
        // everywhere else: written bare, {search_path=a, b} read back as two settings.
        StringBuilder out = new StringBuilder("{");
        boolean first = true;
        for (String setting : raw.split(",(?=[^ ])")) {
            if (!first) out.append(',');
            first = false;
            boolean quote = setting.indexOf(',') >= 0 || setting.indexOf(' ') >= 0
                    || setting.indexOf('"') >= 0 || setting.indexOf('\\') >= 0
                    || setting.indexOf('{') >= 0 || setting.indexOf('}') >= 0;
            if (!quote) {
                out.append(setting);
            } else {
                out.append('"').append(setting.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
            }
        }
        return out.append('}').toString();
    }

    Table buildPgUser() {
        return buildLoginRoleView("pg_user", "usename", "usesysid");
    }

    /**
     * pg_user and pg_shadow are the same query over the login roles, differing only in the two
     * leading column names and in pg_shadow being restricted to superusers. A tool auditing
     * row-level security reads usebypassrls off one of them, so both carry PG's full column list
     * rather than the four that were enough to list a user's name.
     */
    Table buildLoginRoleView(String viewName, String nameCol, String sysidCol) {
        List<Column> cols = Cols.listOf(
                colNN(nameCol, DataType.NAME),
                colNN(sysidCol, DataType.OID),
                col("usecreatedb", DataType.BOOLEAN),
                col("usesuper", DataType.BOOLEAN),
                col("userepl", DataType.BOOLEAN),
                col("usebypassrls", DataType.BOOLEAN),
                col("passwd", DataType.TEXT),
                col("valuntil", DataType.TIMESTAMPTZ),
                col("useconfig", DataType.TEXT_ARRAY)
        );
        Table table = new Table(viewName, cols);
        for (Map.Entry<String, Map<String, String>> entry : database.getRoles().entrySet()) {
            Map<String, String> attrs = entry.getValue();
            boolean canLogin = "true".equalsIgnoreCase(attrs.getOrDefault("LOGIN", "false"));
            if (canLogin) {
                // Same reading of BYPASSRLS as pg_roles: a superuser bypasses RLS.
                boolean superuser = "true".equalsIgnoreCase(attrs.getOrDefault("SUPERUSER", "false"));
                table.insertRow(new Object[]{
                        entry.getKey(), oids.oid("role:" + entry.getKey()),
                        "true".equalsIgnoreCase(attrs.getOrDefault("CREATEDB", "false")),
                        superuser,
                        "true".equalsIgnoreCase(attrs.getOrDefault("REPLICATION", "false")),
                        "true".equalsIgnoreCase(
                                attrs.getOrDefault("BYPASSRLS", String.valueOf(superuser))),
                        // The view exists so that a password is never read through it: whatever
                        // the role has, or has not, pg_user shows the same eight stars.
                        "********", null, null
                });
            }
        }
        return table;
    }

    /** The group roles, as PostgreSQL's pg_group view reports them. */
    /**
     * The roles PostgreSQL ships, each with the identifier PostgreSQL gives it. The numbers are
     * fixed in the catalogue rather than handed out as the server starts, so they are written
     * down here: a reader that sorts roles by their identifier sees them in the order PostgreSQL
     * lists them in, and one that looks a number up finds the role that carries it.
     */
    /** The number PostgreSQL pins one of the roles it ships to. */
    private static int predefinedRoleOid(String name) {
        for (String[] r : PREDEFINED_SYSTEM_ROLES) {
            if (r[0].equals(name)) return Integer.parseInt(r[1]);
        }
        return 0;
    }

    private static final String[][] PREDEFINED_SYSTEM_ROLES = {
        {"pg_monitor", "3373"},
        {"pg_read_all_settings", "3374"},
        {"pg_read_all_stats", "3375"},
        {"pg_stat_scan_tables", "3377"},
        {"pg_signal_backend", "4200"},
        {"pg_checkpoint", "4544"},
        {"pg_use_reserved_connections", "4550"},
        {"pg_read_server_files", "4569"},
        {"pg_write_server_files", "4570"},
        {"pg_execute_server_program", "4571"},
        {"pg_database_owner", "6171"},
        {"pg_read_all_data", "6181"},
        {"pg_write_all_data", "6182"},
        {"pg_create_subscription", "6304"},
        {"pg_maintain", "6337"},
        {"pg_signal_autovacuum_worker", "6392"},
    };

    Table buildPgGroup() {
        List<Column> cols = Cols.listOf(
                colNN("groname", DataType.NAME),
                colNN("grosysid", DataType.OID),
                col("grolist", DataType.OID_ARRAY)
        );
        Table table = new Table("pg_group", cols);
        for (Map.Entry<String, Map<String, String>> entry : database.getRoles().entrySet()) {
            Map<String, String> attrs = entry.getValue();
            if ("true".equalsIgnoreCase(attrs.getOrDefault("LOGIN", "false"))) continue;
            StringBuilder members = new StringBuilder("{");
            java.util.Set<String> memberSet = database.getRoleMemberships().get(entry.getKey());
            if (memberSet != null) {
                boolean first = true;
                for (String m : memberSet) {
                    if (!first) members.append(",");
                    members.append(oids.oid("role:" + m));
                    first = false;
                }
            }
            members.append("}");
            table.insertRow(new Object[]{
                    entry.getKey(), oids.oid("role:" + entry.getKey()), members.toString()
            });
        }
        // The roles PostgreSQL ships are roles that cannot log in, so they are groups here as
        // much as any other: a client asking pg_group which groups there are was told of none.
        for (String[] predefined : PREDEFINED_SYSTEM_ROLES) {
            table.insertRow(new Object[]{
                    predefined[0], Integer.parseInt(predefined[1]), "{}"});
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
        // pg_monitor is granted the three roles it is made of, and the server ships those
        // memberships. Listing none of them left pg_auth_members empty on a database nobody had
        // granted anything in, where a real server has three rows in it from the start.
        for (String held : new String[]{
                "pg_read_all_settings", "pg_read_all_stats", "pg_stat_scan_tables"}) {
            table.insertRow(new Object[]{
                    rowOid++, predefinedRoleOid(held), predefinedRoleOid("pg_monitor"), 10,
                    Boolean.FALSE, Boolean.TRUE, Boolean.TRUE
            });
        }
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
                colNN("oid", DataType.OID),
                colNN("polname", DataType.NAME),
                colNN("polrelid", DataType.OID),
                col("polcmd", DataType.INTERNAL_CHAR),
                col("polpermissive", DataType.BOOLEAN),
                col("polroles", DataType.TEXT),
                // A policy's expressions are parse trees in PG, not text a client can read back
                col("polqual", DataType.PG_NODE_TREE),
                col("polwithcheck", DataType.PG_NODE_TREE),
                col("xmin", DataType.INTEGER)
        );
        Table table = new Table("pg_policy", cols);
        for (Map.Entry<String, Schema> schemaEntry : database.getSchemas().entrySet()) {
            String schemaName = schemaEntry.getKey();
            for (Map.Entry<String, com.memgres.engine.Table> tableEntry
                    : schemaEntry.getValue().getTables().entrySet()) {
                com.memgres.engine.Table t = tableEntry.getValue();
                int relOid = oids.oid("rel:" + schemaName + "." + t.getName());
                for (RlsPolicy policy : t.getRlsPolicies()) {
                    String cmdChar = "*"; // default = ALL
                    if (policy.getCommand() != null) {
                        switch (policy.getCommand().toUpperCase(java.util.Locale.ROOT)) {
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
                    // What the view shows is pg_get_expr of the stored tree, which is the
                    // deparser every other definition is written by: echoing memgres's own
                    // spelling instead wrote CURRENT_USER as a call to a function.
                    String qualText = policy.getUsingExpr() != null
                            ? RuleDeparser.deparse(policy.getUsingExpr(), RuleDeparser.forTable(t))
                            : null;
                    String withCheckText = policy.getWithCheckExpr() != null
                            ? RuleDeparser.deparse(policy.getWithCheckExpr(),
                                    RuleDeparser.forTable(t))
                            : null;
                    // A policy's OID is minted under its own name, not handed out in row order,
                    // so it stays the same across rebuilds and pg_description can point at it.
                    table.insertRow(new Object[]{
                            oids.oid(ObjectIdentity.policyKey(schemaName, t.getName(),
                                    policy.getName())),
                            policy.getName(), relOid, cmdChar,
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
                // The three names are names, which is the type a reader binds them as.
                col("schemaname", DataType.NAME),
                colNN("tablename", DataType.NAME),
                colNN("policyname", DataType.NAME),
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
                    // What the view shows is pg_get_expr of the stored tree, which is the
                    // deparser every other definition is written by: echoing memgres's own
                    // spelling instead wrote CURRENT_USER as a call to a function.
                    String qualText = policy.getUsingExpr() != null
                            ? RuleDeparser.deparse(policy.getUsingExpr(), RuleDeparser.forTable(t))
                            : null;
                    String withCheckText = policy.getWithCheckExpr() != null
                            ? RuleDeparser.deparse(policy.getWithCheckExpr(),
                                    RuleDeparser.forTable(t))
                            : null;
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
                // The two identifiers are OIDs and the list is an array of ACL items, which is
                // what a reader asking the catalogue what the columns are is told.
                colNN("defaclrole", DataType.OID),
                colNN("defaclnamespace", DataType.OID),
                // The kind is one letter of PostgreSQL's own single-byte type, not a blank-padded
                // character: a reader that asks what the column is was told the wrong type.
                colNN("defaclobjtype", DataType.INTERNAL_CHAR),
                col("defaclacl", DataType.ACLITEM_ARRAY)
        );
        Table table = new Table("pg_default_acl", cols);
        int rowOid = oids.oid("pg_default_acl:base");
        // The catalogue is keyed by the role that wrote the statement, the schema and the kind
        // of object, and the list holds every grantee. A row per statement listed the same role
        // and schema twice, and wrote the privileges out by name where the column holds an
        // access list nothing could read that way.
        Map<String, List<AclItems.Grant>> byKey = new LinkedHashMap<>();
        Map<String, Database.DefaultAclEntry> firstOfKey = new LinkedHashMap<>();
        for (Database.DefaultAclEntry entry : database.getDefaultAcls()) {
            if (!entry.isGrant) continue; // only GRANT entries are visible
            String key = (entry.grantor == null ? "" : entry.grantor) + "\u0001"
                    + (entry.schema == null ? "" : entry.schema) + "\u0001"
                    + objectTypeChar(entry.objectType);
            List<AclItems.Grant> grants =
                    byKey.computeIfAbsent(key, k -> new ArrayList<AclItems.Grant>());
            firstOfKey.putIfAbsent(key, entry);
            for (String grantee : entry.grantees) {
                for (String privilege : entry.privileges) {
                    grants.add(new AclItems.Grant(grantee, privilege, false));
                }
            }
        }
        for (Map.Entry<String, List<AclItems.Grant>> keyed : byKey.entrySet()) {
            Database.DefaultAclEntry entry = firstOfKey.get(keyed.getKey());
            char objType = objectTypeChar(entry.objectType);
            int nsOid = entry.schema != null ? oids.oid("ns:" + entry.schema) : 0;
            String grantor = entry.grantor != null ? entry.grantor : "memgres";
            String aclText = AclItems.defaultPrivilegesText(
                    aclKindOf(entry.objectType), grantor, keyed.getValue());
            table.insertRow(new Object[]{
                    rowOid++,
                    entry.grantor != null ? oids.oid("role:" + entry.grantor) : 10,
                    nsOid,
                    String.valueOf(objType),
                    aclText
            });
        }
        return table;
    }

    /** The kind of object a default-privileges list is about, named in the singular. */
    private static String aclKindOf(String objectType) {
        if (objectType == null) return "TABLE";
        String upper = objectType.toUpperCase(java.util.Locale.ROOT);
        if (upper.endsWith("S")) upper = upper.substring(0, upper.length() - 1);
        return upper;
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
                // The object columns are oids and the transaction one is an xid, as PostgreSQL
                // declares them: age() is declared over xid and nothing else, so a column calling
                // itself an integer makes age(pg_locks.transactionid) a call to no function.
                col("database", DataType.OID),
                col("relation", DataType.OID),
                col("page", DataType.INTEGER),
                col("tuple", DataType.SMALLINT),
                col("virtualxid", DataType.TEXT),
                col("transactionid", DataType.XID),
                col("classid", DataType.OID),
                col("objid", DataType.OID),
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

        // The relations each session's transaction actually holds a lock on, with the mode the
        // statement took: a SELECT takes AccessShareLock, SELECT FOR UPDATE RowShareLock, and a
        // statement that changes rows RowExclusiveLock. A row for every relation whether or not
        // anything had touched it described locks nobody held.
        for (Session s : database.getActiveSessions()) {
            for (Map.Entry<String, String> held : s.getRelationLocks().entrySet()) {
                int relOid = oids.oid("rel:" + held.getKey());
                table.insertRow(new Object[]{
                        "relation", dbOid, relOid, null, null, null, null,
                        null, null, null,
                        String.valueOf(s.getPid()) + "/1", s.getPid(),
                        held.getValue(), true, false, null
                });
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

        // A session inside a transaction holds a lock on its own virtual transaction, and one on
        // its transaction id once it has been assigned one. Both are always granted and always
        // exclusive, and a client reading pg_locks to see whether it is in a transaction found
        // neither of them.
        for (Session s : database.getActiveSessions()) {
            String virtual = s.getPid() + "/1";
            // A live backend always holds a lock on its own virtual transaction, whether or not
            // a transaction block is open: every statement runs inside a transaction of some
            // kind. Showing it only inside a block left a client asking pg_locks about its own
            // backend finding nothing at all.
            table.insertRow(new Object[]{
                    "virtualxid", null, null, null, null, virtual, null,
                    null, null, null, virtual, s.getPid(), "ExclusiveLock", true, true, null
            });
            // The lock on a transaction id joins it only once one has been assigned, which is
            // when the transaction first writes or is asked for one outright. A transaction that
            // has only read has no id of its own to hold a lock over.
            if (s.getStatus() == Session.TransactionStatus.IN_TRANSACTION
                    && s.hasAssignedTransactionId()) {
                table.insertRow(new Object[]{
                        "transactionid", null, null, null, null, null, s.peekTransactionId(),
                        null, null, null, virtual, s.getPid(), "ExclusiveLock", true, false, null
                });
            }
        }

        // Expose advisory locks: one row per (session, lock, mode), with the key split into
        // classid (high 32 bits / first int) and objid (low 32 bits / second int) like PG.
        for (Database.AdvisoryLockRow row : database.getAdvisoryLockRows()) {
            table.insertRow(new Object[]{
                    "advisory", dbOid, null, null, null, null, null,
                    // An OID is unsigned, so the high half of a key with its top bit set reads
                    // as a large number and not as a negative one.
                    row.classId & 0xFFFFFFFFL, row.objId & 0xFFFFFFFFL, row.objSubId,
                    String.valueOf(row.session.getPid()) + "/1", row.session.getPid(),
                    row.exclusive ? "ExclusiveLock" : "ShareLock", true, false, null
            });
        }
        return table;
    }
}
