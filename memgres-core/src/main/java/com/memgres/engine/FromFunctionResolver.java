package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Resolves set-returning functions in FROM clauses (e.g., generate_series, unnest, jsonb_each).
 * Extracted from FromResolver to separate concerns.
 */
class FromFunctionResolver {
    private final AstExecutor executor;

    FromFunctionResolver(AstExecutor executor) {
        this.executor = executor;
    }

    /**
     * Resolve a set-returning function in FROM clause.
     */
    List<RowContext> resolveFunctionFrom(SelectStmt.FunctionFrom funcFrom) {
        String rawFname = funcFrom.functionName().toLowerCase();
        String fname = rawFname.contains(".") ? rawFname.substring(rawFname.lastIndexOf('.') + 1) : rawFname;
        String alias = funcFrom.alias() != null ? funcFrom.alias() : fname;
        List<String> colAliases = funcFrom.columnAliases();
        List<Object> evalArgs = new ArrayList<>();
        for (Expression arg : funcFrom.args()) {
            evalArgs.add(executor.evalExpr(arg, null));
        }

        if (fname.equals("generate_series")) return resolveGenerateSeries(alias, colAliases, evalArgs);
        if (fname.equals("generate_subscripts")) return resolveGenerateSubscripts(alias, colAliases, evalArgs);
        if (fname.equals("pg_indexam_has_property")) return resolvePgIndexamHasProperty(alias, evalArgs);
        if (fname.equals("pg_available_extension_versions")) return resolvePgAvailableExtensionVersions(alias);
        if (fname.equals("pg_show_all_settings")) return resolvePgShowAllSettings(alias);
        if (fname.equals("unnest")) return resolveUnnest(alias, colAliases, evalArgs);
        if (fname.equals("jsonb_each") || fname.equals("jsonb_each_text") || fname.equals("json_each") || fname.equals("json_each_text"))
            return resolveJsonEach(fname, alias, colAliases, evalArgs);
        if (fname.equals("jsonb_to_recordset") || fname.equals("json_to_recordset") || fname.equals("jsonb_to_record") || fname.equals("json_to_record"))
            return resolveJsonToRecordset(alias, colAliases, evalArgs);
        if (fname.equals("regexp_matches")) return resolveRegexpMatches(alias, colAliases, evalArgs);
        if (fname.equals("jsonb_path_query")) return resolveJsonbPathQuery(alias, colAliases, evalArgs);
        if (fname.equals("jsonb_array_elements") || fname.equals("json_array_elements") ||
            fname.equals("jsonb_array_elements_text") || fname.equals("json_array_elements_text"))
            return resolveJsonArrayElements(fname, alias, colAliases, evalArgs);
        if (fname.equals("pg_options_to_table") || fname.equals("pg_catalog.pg_options_to_table"))
            return resolvePgOptionsToTable(alias, colAliases, evalArgs);
        if (fname.equals("pg_get_sequence_data") || fname.equals("pg_catalog.pg_get_sequence_data"))
            return resolvePgGetSequenceData(alias, colAliases, evalArgs);
        if (fname.startsWith("__tablesample__:")) return resolveTablesample(fname, alias, evalArgs);
        if (fname.equals("__rows_from__")) return resolveRowsFrom(funcFrom, alias, colAliases);

        // Try user-defined function
        PgFunction userFunc = executor.database.getFunction(fname);
        if (userFunc != null) return resolveUserFunction(userFunc, alias, colAliases, evalArgs);

        throw new MemgresException("Set-returning function not supported in FROM: " + fname);
    }

    // ---- generate_series ----

    private List<RowContext> resolveGenerateSeries(String alias, List<String> colAliases, List<Object> evalArgs) {
        // Date/timestamp overload
        if (evalArgs.get(0) instanceof java.time.LocalDate || evalArgs.get(0) instanceof java.time.LocalDateTime) {
            java.time.LocalDateTime dtStart = evalArgs.get(0) instanceof java.time.LocalDate ? ((java.time.LocalDate) evalArgs.get(0)).atStartOfDay() : (java.time.LocalDateTime) evalArgs.get(0);
            java.time.LocalDateTime dtStop = evalArgs.get(1) instanceof java.time.LocalDate ? ((java.time.LocalDate) evalArgs.get(1)).atStartOfDay() : TypeCoercion.toLocalDateTime(evalArgs.get(1));
            PgInterval ivStep = evalArgs.size() > 2 ? TypeCoercion.toInterval(evalArgs.get(2)) : new PgInterval(0, 1, 0);
            String colName = firstColAlias(colAliases, alias);
            Column col = new Column(colName, DataType.TIMESTAMPTZ, true, false, null);
            Table virtualTable = new Table(alias, Cols.listOf(col));
            List<RowContext> contexts = new ArrayList<>();
            java.time.LocalDateTime cur = dtStart;
            for (int guard = 0; guard < 10000 && !cur.isAfter(dtStop); guard++) {
                Object val = cur.atZone(java.time.ZoneId.systemDefault()).toOffsetDateTime();
                Object[] row = new Object[]{val};
                virtualTable.insertRow(row);
                contexts.add(new RowContext(virtualTable, alias, row));
                cur = ivStep.addTo(cur);
            }
            return contexts;
        }
        try {
            executor.toLong(evalArgs.get(0));
            executor.toLong(evalArgs.get(1));
        } catch (NumberFormatException e) {
            throw new MemgresException("function generate_series(unknown, unknown) is not unique", "42725");
        }
        long start = executor.toLong(evalArgs.get(0));
        long stop = executor.toLong(evalArgs.get(1));
        long step = evalArgs.size() > 2 ? executor.toLong(evalArgs.get(2)) : 1;

        String colName = firstColAlias(colAliases, alias);
        Column col = new Column(colName, DataType.INTEGER, true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col));
        List<RowContext> contexts = new ArrayList<>();
        if (step > 0) {
            for (long v = start; v <= stop; v += step) {
                Object val = (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) ? (int) v : v;
                Object[] row = new Object[]{val};
                virtualTable.insertRow(row);
                contexts.add(new RowContext(virtualTable, alias, row));
            }
        } else if (step < 0) {
            for (long v = start; v >= stop; v += step) {
                Object val = (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) ? (int) v : v;
                Object[] row = new Object[]{val};
                virtualTable.insertRow(row);
                contexts.add(new RowContext(virtualTable, alias, row));
            }
        }
        return contexts;
    }

    // ---- generate_subscripts ----

    private List<RowContext> resolveGenerateSubscripts(String alias, List<String> colAliases, List<Object> evalArgs) {
        if (evalArgs.isEmpty()) throw new MemgresException("function generate_subscripts() does not exist", "42883");
        Object arrObj = evalArgs.get(0);
        int dim = evalArgs.size() > 1 ? executor.toInt(evalArgs.get(1)) : 1;
        boolean reverse = evalArgs.size() > 2 && executor.isTruthy(evalArgs.get(2));

        List<Object> elements;
        int lowerBound = 1;
        if (arrObj instanceof String && ((String) arrObj).contains("=") && ((String) arrObj).startsWith("[")) {
            String s = (String) arrObj;
            int eqIdx = s.indexOf('=');
            String bounds = s.substring(0, eqIdx);
            String content = s.substring(eqIdx + 1);
            String[] parts = bounds.substring(1, bounds.length() - 1).split(":");
            if (parts.length == 2) {
                lowerBound = Integer.parseInt(parts[0].trim());
            }
            elements = toElementList(content);
        } else {
            elements = toElementList(arrObj);
        }

        int lo = lowerBound;
        int hi = lo + elements.size() - 1;

        String colName = firstColAlias(colAliases, alias);
        Column col = new Column(colName, DataType.INTEGER, true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col));
        List<RowContext> contexts = new ArrayList<>();
        if (lo <= hi) {
            if (reverse) {
                for (int i = hi; i >= lo; i--) {
                    Object[] row = new Object[]{i};
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, alias, row));
                }
            } else {
                for (int i = lo; i <= hi; i++) {
                    Object[] row = new Object[]{i};
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, alias, row));
                }
            }
        }
        return contexts;
    }

    // ---- pg_indexam_has_property ----

    private List<RowContext> resolvePgIndexamHasProperty(String alias, List<Object> evalArgs) {
        boolean result = true;
        if (evalArgs.size() >= 2) {
            String prop = String.valueOf(evalArgs.get(1)).toLowerCase();
            switch (prop) {
                case "can_order":
                case "can_unique":
                case "can_multi_col":
                    result = true;
                    break;
                default:
                    result = false;
                    break;
            }
        }
        Column col = new Column(alias, DataType.BOOLEAN, true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col));
        Object[] row = new Object[]{result};
        virtualTable.insertRow(row);
        return Cols.listOf(new RowContext(virtualTable, alias, row));
    }

    // ---- pg_available_extension_versions ----

    private List<RowContext> resolvePgAvailableExtensionVersions(String alias) {
        List<Column> cols = Cols.listOf(
                new Column("name", DataType.TEXT, true, false, null),
                new Column("version", DataType.TEXT, true, false, null),
                new Column("superuser", DataType.BOOLEAN, true, false, null),
                new Column("trusted", DataType.BOOLEAN, true, false, null),
                new Column("relocatable", DataType.BOOLEAN, true, false, null),
                new Column("schema", DataType.TEXT, true, false, null),
                new Column("requires", DataType.TEXT, true, false, null),
                new Column("comment", DataType.TEXT, true, false, null)
        );
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        Object[] row = new Object[]{"plpgsql", "1.0", true, true, false, "pg_catalog", null, "PL/pgSQL procedural language"};
        virtualTable.insertRow(row);
        contexts.add(new RowContext(virtualTable, alias, row));
        return contexts;
    }

    // ---- pg_show_all_settings ----

    private List<RowContext> resolvePgShowAllSettings(String alias) {
        List<Column> cols = Cols.listOf(
                new Column("name", DataType.TEXT, true, false, null),
                new Column("setting", DataType.TEXT, true, false, null),
                new Column("unit", DataType.TEXT, true, false, null),
                new Column("category", DataType.TEXT, true, false, null),
                new Column("short_desc", DataType.TEXT, true, false, null),
                new Column("extra_desc", DataType.TEXT, true, false, null),
                new Column("context", DataType.TEXT, true, false, null),
                new Column("vartype", DataType.TEXT, true, false, null),
                new Column("source", DataType.TEXT, true, false, null),
                new Column("min_val", DataType.TEXT, true, false, null),
                new Column("max_val", DataType.TEXT, true, false, null),
                new Column("enumvals", DataType.TEXT, true, false, null),
                new Column("boot_val", DataType.TEXT, true, false, null),
                new Column("reset_val", DataType.TEXT, true, false, null),
                new Column("sourcefile", DataType.TEXT, true, false, null),
                new Column("sourceline", DataType.INTEGER, true, false, null),
                new Column("pending_restart", DataType.BOOLEAN, true, false, null)
        );
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        GucSettings guc = executor.session != null ? executor.session.getGucSettings() : new GucSettings();
        Map<String, String> all = guc.getAll();
        Map<String, String[]> meta = buildSettingsMetadata();
        for (Map.Entry<String, String> e : all.entrySet()) {
            String canonName = guc.getCanonicalName(e.getKey());
            Object[] row = new Object[cols.size()];
            row[0] = canonName;
            row[1] = e.getValue();
            String[] m = meta.get(e.getKey().toLowerCase());
            row[3] = m != null ? m[0] : "Ungrouped";
            row[4] = m != null ? m[1] : "";
            row[6] = m != null ? m[2] : "user";
            row[7] = m != null ? m[3] : "string";
            row[8] = "default";
            row[12] = e.getValue();
            row[13] = e.getValue();
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
        }
        return contexts;
    }

    private static Map<String, String[]> buildSettingsMetadata() {
        Map<String, String[]> meta = new java.util.LinkedHashMap<>();
        meta.put("server_version", new String[]{"Preset Options", "Shows the server version.", "internal", "string"});
        meta.put("server_version_num", new String[]{"Preset Options", "Shows the server version as an integer.", "internal", "string"});
        meta.put("server_encoding", new String[]{"Client Connection Defaults", "Shows the server character set encoding.", "internal", "string"});
        meta.put("client_encoding", new String[]{"Client Connection Defaults", "Sets the client's character set encoding.", "user", "string"});
        meta.put("client_min_messages", new String[]{"Client Connection Defaults", "Sets the message levels sent to the client.", "user", "enum"});
        meta.put("search_path", new String[]{"Client Connection Defaults", "Sets the schema search order.", "user", "string"});
        meta.put("timezone", new String[]{"Client Connection Defaults / Locale and Formatting", "Sets the time zone.", "user", "string"});
        meta.put("datestyle", new String[]{"Client Connection Defaults / Locale and Formatting", "Sets the display format for date and time.", "user", "string"});
        meta.put("intervalstyle", new String[]{"Client Connection Defaults / Locale and Formatting", "Sets the display format for interval values.", "user", "string"});
        meta.put("standard_conforming_strings", new String[]{"Version and Platform Compatibility", "Causes strings to treat backslashes literally.", "user", "bool"});
        meta.put("max_connections", new String[]{"Connections and Authentication", "Sets max concurrent connections.", "postmaster", "integer"});
        meta.put("shared_buffers", new String[]{"Resource Usage / Memory", "Sets shared memory buffers.", "postmaster", "string"});
        meta.put("work_mem", new String[]{"Resource Usage / Memory", "Sets max memory for query workspaces.", "user", "string"});
        meta.put("default_transaction_isolation", new String[]{"Client Connection Defaults", "Sets default transaction isolation.", "user", "enum"});
        meta.put("transaction_isolation", new String[]{"Client Connection Defaults", "Sets current transaction isolation.", "user", "enum"});
        meta.put("bytea_output", new String[]{"Client Connection Defaults", "Sets the output format for bytea.", "user", "enum"});
        meta.put("application_name", new String[]{"Reporting and Logging", "Sets the application name.", "user", "string"});
        meta.put("extra_float_digits", new String[]{"Client Connection Defaults", "Sets extra float digits.", "user", "integer"});
        meta.put("row_security", new String[]{"Client Connection Defaults", "Enable row security.", "user", "bool"});
        meta.put("default_tablespace", new String[]{"Client Connection Defaults", "Sets the default tablespace.", "user", "string"});
        meta.put("xmloption", new String[]{"Client Connection Defaults", "Sets whether XML data is parsed as document or content.", "user", "enum"});
        meta.put("jit", new String[]{"Query Tuning", "Allow JIT compilation.", "user", "bool"});
        meta.put("statement_timeout", new String[]{"Client Connection Defaults", "Sets statement timeout.", "user", "integer"});
        meta.put("lock_timeout", new String[]{"Client Connection Defaults", "Sets lock timeout.", "user", "integer"});
        meta.put("idle_in_transaction_session_timeout", new String[]{"Client Connection Defaults", "Sets idle-in-transaction timeout.", "user", "integer"});
        meta.put("is_superuser", new String[]{"Preset Options", "Shows whether the current user is a superuser.", "internal", "bool"});
        return meta;
    }

    // ---- unnest ----

    private List<RowContext> resolveUnnest(String alias, List<String> colAliases, List<Object> evalArgs) {
        if (evalArgs.isEmpty()) {
            throw new MemgresException("function unnest() does not exist\n  Hint: No function matches the given name and argument types.", "42883");
        }
        if (evalArgs.size() > 1) {
            return resolveMultiUnnest(alias, colAliases, evalArgs);
        }
        // Single array unnest
        Object arr = evalArgs.get(0);
        List<Object> elements = toElementList(arr);

        String colName = firstColAlias(colAliases, alias);
        List<Column> cols = new ArrayList<>();
        cols.add(new Column(colName, DataType.TEXT, true, false, null));
        boolean hasOrdinality = colAliases != null && colAliases.size() >= 2;
        String ordColName = hasOrdinality ? colAliases.get(1) : "ordinality";
        if (hasOrdinality) {
            cols.add(new Column(ordColName, DataType.BIGINT, true, false, null));
        }
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        long ord = 1;
        for (Object elem : elements) {
            Object[] row = hasOrdinality ? new Object[]{elem, ord++} : new Object[]{elem};
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
        }
        return contexts;
    }

    private List<RowContext> resolveMultiUnnest(String alias, List<String> colAliases, List<Object> evalArgs) {
        List<List<Object>> allElements = new ArrayList<>();
        int maxLen = 0;
        for (Object arr : evalArgs) {
            List<Object> elems = toElementList(arr);
            allElements.add(elems);
            maxLen = Math.max(maxLen, elems.size());
        }
        List<Column> cols = new ArrayList<>();
        for (int i = 0; i < evalArgs.size(); i++) {
            String cname = (colAliases != null && i < colAliases.size()) ? colAliases.get(i) : ("col" + (i+1));
            cols.add(new Column(cname, DataType.TEXT, true, false, null));
        }
        boolean hasOrdinality = colAliases != null && colAliases.size() > evalArgs.size();
        String ordColName = hasOrdinality ? colAliases.get(evalArgs.size()) : "ordinality";
        if (hasOrdinality) cols.add(new Column(ordColName, DataType.BIGINT, true, false, null));

        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        for (int row = 0; row < maxLen; row++) {
            Object[] rowData = new Object[cols.size()];
            for (int c = 0; c < allElements.size(); c++) {
                rowData[c] = row < allElements.get(c).size() ? allElements.get(c).get(row) : null;
            }
            if (hasOrdinality) rowData[allElements.size()] = (long)(row + 1);
            virtualTable.insertRow(rowData);
            contexts.add(new RowContext(virtualTable, alias, rowData));
        }
        return contexts;
    }

    // ---- jsonb_each / json_each ----

    private List<RowContext> resolveJsonEach(String fname, String alias, List<String> colAliases, List<Object> evalArgs) {
        Object jsonVal = evalArgs.get(0);
        String keyCol = (colAliases != null && colAliases.size() >= 1) ? colAliases.get(0) : "key";
        String valCol = (colAliases != null && colAliases.size() >= 2) ? colAliases.get(1) : "value";
        Table virtualTable = new Table(alias, Cols.listOf(
                new Column(keyCol, DataType.TEXT, true, false, null),
                new Column(valCol, DataType.TEXT, true, false, null)));
        List<RowContext> contexts = new ArrayList<>();
        if (jsonVal != null) {
            String json = jsonVal.toString().trim();
            try {
                boolean isText = fname.contains("_text");
                Map<String, String> pairs = JsonOperations.parseObjectKeys(json);
                for (Map.Entry<String, String> entry : pairs.entrySet()) {
                    String value = entry.getValue();
                    if (isText && value != null && value.startsWith("\"") && value.endsWith("\"")) {
                        value = value.substring(1, value.length() - 1);
                    }
                    Object[] row = new Object[]{entry.getKey(), value};
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, alias, row));
                }
            } catch (Exception e) { /* skip */ }
        }
        return contexts;
    }

    // ---- jsonb_to_recordset / json_to_recordset ----

    private List<RowContext> resolveJsonToRecordset(String alias, List<String> colAliases, List<Object> evalArgs) {
        Object jsonVal = evalArgs.get(0);
        List<Column> cols = new ArrayList<>();
        if (colAliases != null) {
            for (String ca : colAliases) {
                cols.add(new Column(ca, DataType.TEXT, true, false, null));
            }
        }
        if (cols.isEmpty()) cols.add(new Column("value", DataType.TEXT, true, false, null));
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        if (jsonVal != null) {
            String json = jsonVal.toString().trim();
            boolean isArray = json.startsWith("[");
            List<String> jsonObjects = new ArrayList<>();
            if (isArray) {
                json = json.substring(1, json.length() - 1).trim();
                int depth = 0;
                StringBuilder current = new StringBuilder();
                for (char c : json.toCharArray()) {
                    if (c == '{') depth++;
                    if (c == '}') depth--;
                    current.append(c);
                    if (depth == 0 && current.length() > 0) {
                        jsonObjects.add(current.toString().trim());
                        current = new StringBuilder();
                    }
                    if (c == ',' && depth == 0) current = new StringBuilder();
                }
                if (!current.isEmpty()) jsonObjects.add(current.toString().trim());
            } else {
                jsonObjects.add(json);
            }
            for (String obj : jsonObjects) {
                if (obj.isEmpty() || obj.equals(",")) continue;
                Object[] row = new Object[cols.size()];
                for (int ci = 0; ci < cols.size(); ci++) {
                    String key = cols.get(ci).getName();
                    String extracted = JsonOperations.extractKey(obj, key);
                    if (extracted != null) {
                        extracted = extracted.trim();
                        if (extracted.startsWith("\"") && extracted.endsWith("\"")) {
                            extracted = extracted.substring(1, extracted.length() - 1);
                        }
                        if (extracted.equals("true")) extracted = "t";
                        else if (extracted.equals("false")) extracted = "f";
                        else if (extracted.equals("null")) extracted = null;
                    }
                    row[ci] = extracted;
                }
                virtualTable.insertRow(row);
                contexts.add(new RowContext(virtualTable, alias, row));
            }
        }
        return contexts;
    }

    // ---- regexp_matches ----

    private List<RowContext> resolveRegexpMatches(String alias, List<String> colAliases, List<Object> evalArgs) {
        Object str = evalArgs.get(0);
        Object pattern = evalArgs.get(1);
        String flags = evalArgs.size() > 2 ? String.valueOf(evalArgs.get(2)) : "";
        if (str == null || pattern == null) return Cols.listOf();
        String colName = firstColAlias(colAliases, alias);
        Column col = new Column(colName, DataType.TEXT, true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col));
        List<RowContext> contexts = new ArrayList<>();
        int jflags = flags.contains("i") ? java.util.regex.Pattern.CASE_INSENSITIVE : 0;
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile(pattern.toString(), jflags).matcher(str.toString());
        while (matcher.find()) {
            List<String> groups = new ArrayList<>();
            for (int g = 1; g <= matcher.groupCount(); g++) groups.add(matcher.group(g));
            if (groups.isEmpty()) groups.add(matcher.group(0));
            Object[] row = new Object[]{groups};
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
            if (!flags.contains("g")) break;
        }
        return contexts;
    }

    // ---- jsonb_path_query ----

    private List<RowContext> resolveJsonbPathQuery(String alias, List<String> colAliases, List<Object> evalArgs) {
        if (evalArgs.size() < 2) throw new MemgresException("function jsonb_path_query requires at least 2 arguments", "42883");
        Object jsonVal = evalArgs.get(0);
        Object pathVal = evalArgs.get(1);
        String colName = firstColAlias(colAliases, "jsonb_path_query");
        Table virtualTable = new Table(alias, Cols.listOf(new Column(colName, DataType.JSONB, true, false, null)));
        List<RowContext> contexts = new ArrayList<>();
        if (jsonVal != null && pathVal != null) {
            String json = jsonVal.toString().trim();
            String path = pathVal.toString().trim();
            List<String> stringResults = executor.functionEvaluator.evaluateJsonPathAll(json, path);
            for (String s : stringResults) {
                String trimmed = s.trim();
                Object val;
                if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() >= 2) {
                    val = trimmed.substring(1, trimmed.length() - 1);
                } else {
                    val = s;
                }
                Object[] row = new Object[]{val};
                virtualTable.insertRow(row);
                contexts.add(new RowContext(virtualTable, alias, row));
            }
        }
        return contexts;
    }

    // ---- jsonb_array_elements / json_array_elements ----

    private List<RowContext> resolveJsonArrayElements(String fname, String alias, List<String> colAliases, List<Object> evalArgs) {
        if (evalArgs.isEmpty()) throw new MemgresException("function " + fname + "() requires 1 argument", "42883");
        Object json = evalArgs.get(0);
        boolean textMode = fname.endsWith("_text");
        String colName = firstColAlias(colAliases, "value");
        DataType dt = textMode ? DataType.TEXT : DataType.JSONB;
        boolean hasOrdinality = colAliases != null && colAliases.size() >= 2;
        List<Column> cols = new ArrayList<>();
        cols.add(new Column(colName, dt, true, false, null));
        if (hasOrdinality) {
            cols.add(new Column(colAliases.get(1), DataType.BIGINT, true, false, null));
        }
        Table virtualTable = new Table(alias, cols);
        List<RowContext> contexts = new ArrayList<>();
        if (json != null) {
            String s = json.toString().trim();
            if (s.startsWith("[")) {
                List<String> elements = JsonOperations.parseArrayElements(s);
                long ord = 1;
                for (String elem : elements) {
                    String val = elem.trim();
                    if (textMode && val.startsWith("\"") && val.endsWith("\"")) {
                        val = val.substring(1, val.length() - 1);
                    }
                    Object[] row = hasOrdinality ? new Object[]{val, ord++} : new Object[]{val};
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, alias, row));
                }
            }
        }
        return contexts;
    }

    // ---- pg_options_to_table ----

    private List<RowContext> resolvePgOptionsToTable(String alias, List<String> colAliases, List<Object> evalArgs) {
        String col1 = (colAliases != null && colAliases.size() > 0) ? colAliases.get(0) : "option_name";
        String col2 = (colAliases != null && colAliases.size() > 1) ? colAliases.get(1) : "option_value";
        Table virtualTable = new Table(alias != null ? alias : "pg_options_to_table",
                Cols.listOf(new Column(col1, DataType.TEXT, true, false, null),
                        new Column(col2, DataType.TEXT, true, false, null)));
        List<RowContext> contexts = new ArrayList<>();
        if (!evalArgs.isEmpty() && evalArgs.get(0) != null) {
            String input = evalArgs.get(0).toString();
            if (input.startsWith("{") && input.endsWith("}")) {
                input = input.substring(1, input.length() - 1);
            }
            if (!input.isEmpty()) {
                for (String opt : input.split(",")) {
                    String[] kv = opt.split("=", 2);
                    Object[] row = new Object[]{kv[0].trim(), kv.length > 1 ? kv[1].trim() : ""};
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, alias, row));
                }
            }
        }
        return contexts;
    }

    // ---- pg_get_sequence_data ----

    private List<RowContext> resolvePgGetSequenceData(String alias, List<String> colAliases, List<Object> evalArgs) {
        Object seqOidArg = evalArgs.isEmpty() ? null : evalArgs.get(0);
        int seqOid = seqOidArg != null ? ((Number) seqOidArg).intValue() : 0;
        String col1 = (colAliases != null && colAliases.size() > 0) ? colAliases.get(0) : "last_value";
        String col2 = (colAliases != null && colAliases.size() > 1) ? colAliases.get(1) : "is_called";
        String tblAlias = alias != null ? alias : "pg_get_sequence_data";
        Table virtualTable = new Table(tblAlias,
                Cols.listOf(new Column(col1, DataType.BIGINT, true, false, null),
                        new Column(col2, DataType.BOOLEAN, true, false, null)));
        List<RowContext> contexts = new ArrayList<>();
        Database db = executor.database;
        Map<String, Integer> oidMap = executor.systemCatalog.getOidMap();
        for (String seqName : CatalogHelper.getSequenceNames(db)) {
            for (Map.Entry<String, Integer> entry : oidMap.entrySet()) {
                if (entry.getValue() == seqOid && entry.getKey().startsWith("rel:") && entry.getKey().endsWith("." + seqName)) {
                    Sequence seq = db.getSequence(seqName);
                    long lastVal;
                    boolean isCalled;
                    if (seq != null) {
                        lastVal = seq.currValRaw();
                        isCalled = seq.isCalled();
                    } else {
                        long[] resolved = resolveImplicitSerial(db, seqName);
                        lastVal = resolved[0];
                        isCalled = resolved[1] != 0;
                    }
                    Object[] row = new Object[]{lastVal, isCalled};
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, tblAlias, row));
                    return contexts;
                }
            }
        }
        Object[] row = new Object[]{1L, false};
        virtualTable.insertRow(row);
        contexts.add(new RowContext(virtualTable, tblAlias, row));
        return contexts;
    }

    // ---- TABLESAMPLE ----

    private List<RowContext> resolveTablesample(String fname, String alias, List<Object> evalArgs) {
        String tableName = fname.substring("__tablesample__:".length());
        String method = evalArgs.get(0).toString();
        double pct = executor.toDouble(evalArgs.get(1));
        Long seed = evalArgs.size() > 2 ? Long.parseLong(evalArgs.get(2).toString()) : null;

        if (pct < 0) {
            throw new MemgresException("tablesample percentage must not be negative", "2202H");
        }
        if (pct > 100) {
            throw new MemgresException("tablesample percentage must be between 0 and 100", "2202H");
        }

        Table table;
        try {
            table = executor.resolveTable(null, tableName);
        } catch (MemgresException e) {
            throw new MemgresException("relation \"" + tableName + "\" does not exist", "42P01");
        }
        List<Object[]> allRows = new ArrayList<>(table.getRows());
        List<Object[]> sampledRows;

        if (pct == 100.0) {
            sampledRows = allRows;
        } else if (pct == 0.0) {
            sampledRows = new ArrayList<>();
        } else {
            java.util.Random rng = seed != null ? new java.util.Random(seed) : new java.util.Random();
            sampledRows = new ArrayList<>();
            double prob = pct / 100.0;
            for (Object[] row : allRows) {
                if (rng.nextDouble() < prob) {
                    sampledRows.add(row);
                }
            }
        }

        String tableAlias = alias != null ? alias : tableName;
        List<RowContext> contexts = new ArrayList<>();
        for (Object[] row : sampledRows) {
            contexts.add(new RowContext(table, tableAlias, row));
        }
        return contexts;
    }

    // ---- ROWS FROM ----

    private List<RowContext> resolveRowsFrom(SelectStmt.FunctionFrom funcFrom, String alias, List<String> colAliases) {
        List<List<Object>> columns = new ArrayList<>();
        int maxLen = 0;
        for (Expression arg : funcFrom.args()) {
            if (arg instanceof FunctionCallExpr) {
                FunctionCallExpr fnExpr = (FunctionCallExpr) arg;
                SelectStmt.FunctionFrom subFunc = new SelectStmt.FunctionFrom(
                    fnExpr.name(), fnExpr.args(), null, null);
                List<RowContext> rows = resolveFunctionFrom(subFunc);
                List<Object> vals = new ArrayList<>();
                for (RowContext rc : rows) {
                    List<RowContext.TableBinding> bindings = rc.getBindings();
                    Object[] rowData = bindings.isEmpty() ? null : bindings.get(0).row();
                    vals.add(rowData != null && rowData.length > 0 ? rowData[0] : null);
                }
                columns.add(vals);
                maxLen = Math.max(maxLen, vals.size());
            }
        }

        List<Column> cols = new ArrayList<>();
        List<String> ca = funcFrom.columnAliases();
        boolean hasOrdinality = false;
        int numFuncs = columns.size();
        if (ca != null && ca.size() > numFuncs) {
            hasOrdinality = true;
        }
        for (int i = 0; i < numFuncs; i++) {
            String cname = (ca != null && i < ca.size()) ? ca.get(i) : ("column" + (i + 1));
            cols.add(new Column(cname, DataType.TEXT, true, false, null));
        }
        if (hasOrdinality) {
            cols.add(new Column(ca.get(numFuncs), DataType.BIGINT, true, false, null));
        }

        String tblAlias = alias != null ? alias : "rows_from";
        Table virtualTable = new Table(tblAlias, cols);
        List<RowContext> contexts = new ArrayList<>();
        for (int row = 0; row < maxLen; row++) {
            Object[] rowData = new Object[cols.size()];
            for (int c = 0; c < numFuncs; c++) {
                rowData[c] = row < columns.get(c).size() ? columns.get(c).get(row) : null;
            }
            if (hasOrdinality) {
                rowData[numFuncs] = (long)(row + 1);
            }
            virtualTable.insertRow(rowData);
            contexts.add(new RowContext(virtualTable, tblAlias, rowData));
        }
        return contexts;
    }

    // ---- User-defined functions ----

    private List<RowContext> resolveUserFunction(PgFunction userFunc, String alias, List<String> colAliases, List<Object> evalArgs) {
        com.memgres.engine.plpgsql.PlpgsqlExecutor plExec = new com.memgres.engine.plpgsql.PlpgsqlExecutor(executor, executor.database, executor.session);
        Object result = plExec.executeFunction(userFunc, evalArgs);
        if (result instanceof List<?>) {
            List<?> resultList = (List<?>) result;
            List<PgFunction.Param> params = userFunc.getParams();
            String returnType = userFunc.getReturnType();

            List<Column> cols = new ArrayList<>();
            if (returnType != null && returnType.toUpperCase().startsWith("SETOF ")) {
                String refTable = returnType.substring(6).trim();
                try {
                    Table sourceTable = executor.resolveTable("public", refTable);
                    if (sourceTable != null) {
                        cols.addAll(sourceTable.getColumns());
                    }
                } catch (Exception e) {
                    // Not a table, use single column
                }
            }

            Table virtualTable;
            List<RowContext> contexts = new ArrayList<>();
            if (!resultList.isEmpty() && resultList.get(0) instanceof Object[]) {
                Object[] firstRow = (Object[]) resultList.get(0);
                if (cols.isEmpty()) {
                    for (int i = 0; i < firstRow.length; i++) {
                        cols.add(new Column("column" + (i + 1), DataType.TEXT, true, false, null));
                    }
                }
                int colIdx = 0;
                for (PgFunction.Param p : params) {
                    if ("OUT".equalsIgnoreCase(p.mode()) && colIdx < cols.size()) {
                        cols.set(colIdx, new Column(p.name() != null ? p.name() : "column" + (colIdx + 1),
                                DataType.TEXT, true, false, null));
                        colIdx++;
                    }
                }
                virtualTable = new Table(alias, cols);
                for (Object row : resultList) {
                    Object[] rowArr = (Object[]) row;
                    virtualTable.insertRow(rowArr);
                    contexts.add(new RowContext(virtualTable, alias, rowArr));
                }
            } else {
                // For RETURNS TABLE with single-column results, use OUT param name
                String colName = alias;
                if ("TABLE".equalsIgnoreCase(returnType)) {
                    for (PgFunction.Param p : params) {
                        if ("OUT".equalsIgnoreCase(p.mode()) && p.name() != null) {
                            colName = p.name();
                            break;
                        }
                    }
                }
                cols.add(new Column(colName, DataType.TEXT, true, false, null));
                virtualTable = new Table(alias, cols);
                for (Object val : resultList) {
                    Object[] row = new Object[]{val};
                    virtualTable.insertRow(row);
                    contexts.add(new RowContext(virtualTable, alias, row));
                }
            }
            return contexts;
        }
        // Non-list result: OUT-param record function or scalar-returning function in FROM
        List<PgFunction.Param> outParams = new ArrayList<>();
        for (PgFunction.Param p : userFunc.getParams()) {
            String mode = p.mode() != null ? p.mode().toUpperCase() : "IN";
            if ("OUT".equals(mode) || "INOUT".equals(mode)) outParams.add(p);
        }
        if (!outParams.isEmpty()) {
            List<Column> cols = new ArrayList<>();
            for (int i = 0; i < outParams.size(); i++) {
                PgFunction.Param op = outParams.get(i);
                String cname = (colAliases != null && i < colAliases.size()) ? colAliases.get(i)
                        : (op.name() != null ? op.name() : ("column" + (i + 1)));
                DataType dt = DataType.fromPgName(op.typeName());
                cols.add(new Column(cname, dt != null ? dt : DataType.TEXT, true, false, null));
            }
            Table virtualTable = new Table(alias, cols);
            List<RowContext> contexts = new ArrayList<>();
            Object[] row;
            if (result instanceof Object[]) {
                row = (Object[]) result;
            } else {
                row = new Object[]{result};
            }
            virtualTable.insertRow(row);
            contexts.add(new RowContext(virtualTable, alias, row));
            return contexts;
        }
        // For RETURNS record with caller-provided column aliases, expand the record
        if ("record".equalsIgnoreCase(userFunc.getReturnType()) && colAliases != null && !colAliases.isEmpty()) {
            Object[] rowArr;
            if (result instanceof Object[]) {
                rowArr = (Object[]) result;
            } else if (result instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) result;
                rowArr = map.values().toArray();
            } else {
                rowArr = new Object[]{result};
            }
            List<Column> cols = new ArrayList<>();
            for (int i = 0; i < colAliases.size(); i++) {
                cols.add(new Column(colAliases.get(i), DataType.TEXT, true, false, null));
            }
            Table virtualTable = new Table(alias, cols);
            List<RowContext> contexts = new ArrayList<>();
            virtualTable.insertRow(rowArr);
            contexts.add(new RowContext(virtualTable, alias, rowArr));
            return contexts;
        }
        // Scalar function in FROM
        String colName = firstColAlias(colAliases, alias);
        Column col = new Column(colName, DataType.TEXT, true, false, null);
        Table virtualTable = new Table(alias, Cols.listOf(col));
        List<RowContext> contexts = new ArrayList<>();
        Object[] row = new Object[]{result};
        virtualTable.insertRow(row);
        contexts.add(new RowContext(virtualTable, alias, row));
        return contexts;
    }

    // ---- Shared helpers ----

    private static String firstColAlias(List<String> colAliases, String fallback) {
        return (colAliases != null && !colAliases.isEmpty()) ? colAliases.get(0) : fallback;
    }

    /**
     * Resolve an implicit SERIAL sequence's current value from the table's serial counter.
     */
    private long[] resolveImplicitSerial(Database db, String seqName) {
        if (seqName.endsWith("_seq")) {
            String prefix = seqName.substring(0, seqName.length() - 4);
            int lastUnderscore = prefix.lastIndexOf('_');
            if (lastUnderscore > 0) {
                String tblName = prefix.substring(0, lastUnderscore);
                for (Schema schema : db.getSchemas().values()) {
                    Table tbl = schema.getTable(tblName);
                    if (tbl != null) {
                        long counter = tbl.getSerialCounter();
                        if (counter > 1) {
                            return new long[]{counter - 1, 1};
                        } else {
                            return new long[]{1, 0};
                        }
                    }
                }
            }
        }
        return new long[]{1, 0};
    }

    static List<Object> toElementList(Object arr) {
        if (arr instanceof List<?>) {
            List<?> l = (List<?>) arr;
            List<Object> result = new ArrayList<>();
            for (Object e : l) result.add(e);
            return result;
        }
        if (arr instanceof String && ((String) arr).startsWith("{") && ((String) arr).endsWith("}")) {
            String s = (String) arr;
            String inner = s.substring(1, s.length() - 1).trim();
            if (inner.isEmpty()) return new ArrayList<>();
            List<Object> result = new ArrayList<>();
            for (String part : inner.split(",", -1)) {
                String t = part.trim();
                if (t.equalsIgnoreCase("NULL")) result.add(null);
                else if (t.startsWith("\"") && t.endsWith("\"")) result.add(t.substring(1, t.length()-1));
                else result.add(parseNumericIfPossible(t));
            }
            return result;
        }
        if (arr == null) return new ArrayList<>();
        return Cols.listOf(arr);
    }

    private static Object parseNumericIfPossible(String s) {
        if (s == null || s.isEmpty()) return s;
        try { return Integer.parseInt(s); } catch (NumberFormatException e) { }
        try { return Long.parseLong(s); } catch (NumberFormatException e) { }
        try { return new java.math.BigDecimal(s); } catch (NumberFormatException e) { }
        return s;
    }

    /**
     * Apply column aliases to a list of columns, modifying in place.
     * Shared by subquery and LATERAL handling code.
     */
    static List<Column> applyColumnAliases(List<Column> columns, List<String> aliases) {
        if (aliases == null) return columns;
        List<Column> result = new ArrayList<>(columns);
        for (int i = 0; i < aliases.size() && i < result.size(); i++) {
            Column orig = result.get(i);
            result.set(i, new Column(aliases.get(i), orig.getType(), orig.isNullable(), orig.isPrimaryKey(), orig.getDefaultValue()));
        }
        return result;
    }
}
