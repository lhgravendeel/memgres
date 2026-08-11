package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * JSON function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class JsonFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    private final AstExecutor executor;

    JsonFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    /**
     * The base argument says which row type to build. Bare {@code record} names no columns, so
     * there is nothing to populate and PG refuses rather than inventing a shape.
     */
    private void requireConcreteRowType(FunctionCallExpr fn, String name) {
        if (fn.args().isEmpty()) return;
        com.memgres.engine.parser.ast.Expression base = fn.args().get(0);
        if (!(base instanceof com.memgres.engine.parser.ast.CastExpr)) return;
        String typeName = ((com.memgres.engine.parser.ast.CastExpr) base).typeName();
        if (typeName != null && "record".equalsIgnoreCase(typeName.trim())) {
            MemgresException e = new MemgresException(
                    "could not determine row type for result of " + name, "0A000");
            // There are two ways to say what the row looks like, and PostgreSQL names both
            // rather than only refusing the one that was written.
            e.setHint("Provide a non-null record argument, or call the function in the FROM"
                    + " clause using a column definition list.");
            throw e;
        }
    }

    /**
     * PG will not guess what a caller meant by handing an object to an array function: it names
     * the container it got and stops. Answering 0, NULL or the input unchanged instead hid the
     * mistake behind a value nothing downstream could tell from a real one. The json and jsonb
     * families word the same complaint differently, so both wordings are kept.
     */
    static void requireJsonArray(String name, String json) {
        if (JsonOperations.isArray(json)) return;
        boolean object = JsonOperations.isObject(json);
        if (name.startsWith("jsonb_")) {
            throw new MemgresException(
                    "cannot extract elements from " + (object ? "an object" : "a scalar"), "22023");
        }
        throw new MemgresException(
                "cannot call " + name + " on " + (object ? "a non-array" : "a scalar"), "22023");
    }

    /** The key-listing functions need an object; an array and a scalar are named apart. */
    static void requireJsonObject(String name, String json) {
        if (JsonOperations.isObject(json)) return;
        throw new MemgresException("cannot call " + name + " on "
                + (JsonOperations.isArray(json) ? "an array" : "a scalar"), "22023");
    }

    /**
     * The members a json_each or json_object_keys call walks. The jsonb family sees the keys in
     * the order jsonb stores them; the json family sees the document as it was written.
     */
    static Map<String, String> eachMembers(String name, String json) {
        return name.startsWith("jsonb_")
                ? JsonOperations.parseObjectKeys(json) : JsonOperations.parseObjectKeysInOrder(json);
    }

    /** json_each words its refusal as deconstruction, jsonb_each as a call on a non-object. */
    static void requireJsonEachObject(String name, String json) {
        if (JsonOperations.isObject(json)) return;
        if (name.startsWith("jsonb_")) {
            throw new MemgresException("cannot call " + name + " on a non-object", "22023");
        }
        throw new MemgresException(JsonOperations.isArray(json)
                ? "cannot deconstruct an array as an object" : "cannot deconstruct a scalar", "22023");
    }

    private void requireArgs(FunctionCallExpr fn, int min) {
        if (fn.args().size() < min) {
            throw new MemgresException(
                "function " + fn.name() + "() does not exist" +
                (fn.args().isEmpty() ? "" : "\n  Hint: No function matches the given name and argument types. You might need to add explicit type casts."), "42883");
        }
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "jsonb_build_object":
            case "json_build_object": {
                // Arguments alternate key and value, so an odd count leaves a key with no value
                if (fn.args().size() % 2 != 0) {
                    throw new MemgresException("argument list must have even number of elements\n"
                            + "  Hint: The arguments of " + name + "() must consist of alternating keys and values.",
                            "22023");
                }
                boolean jsonb = name.startsWith("jsonb");
                StringBuilder sb = new StringBuilder("{");
                for (int i = 0; i < fn.args().size(); i += 2) {
                    if (i > 0) sb.append(", ");
                    Object key = executor.evalExpr(fn.args().get(i), ctx);
                    if (key == null) {
                        // The two families report a null key differently, down to the SQLSTATE
                        throw jsonb
                                ? new MemgresException("argument " + (i + 1) + ": key must not be null", "22023")
                                : new MemgresException("null value not allowed for object key", "22004");
                    }
                    Object val = wholeRowOrValue(fn.args().get(i + 1), ctx);
                    sb.append("\"").append(key).append("\"").append(jsonb ? ": " : " : ");
                    appendJsonValue(sb, val);
                }
                sb.append("}");
                // jsonb orders its keys and keeps the last of any repeated one; json keeps what
                // it was written, repeats and all.
                return jsonb ? normalizedIfStructured(sb.toString()) : sb.toString();
            }
            case "jsonb_build_array":
            case "json_build_array": {
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < fn.args().size(); i++) {
                    if (i > 0) sb.append(", ");
                    appendJsonValue(sb, wholeRowOrValue(fn.args().get(i), ctx));
                }
                sb.append("]");
                return name.startsWith("jsonb")
                        ? normalizedIfStructured(sb.toString()) : sb.toString();
            }
            case "array_to_json": {
                // array_to_json writes an array as a JSON array, and its second argument asks for
                // the top-level elements to be put on lines of their own.
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                PgArray array = PgArray.from(arg);
                if (array == null) {
                    throw new MemgresException("function array_to_json("
                            + AstExecutor.pgTypeNameOf(arg) + ") does not exist", "42883");
                }
                boolean lineFeeds = fn.args().size() >= 2
                        && executor.isTruthy(executor.evalExpr(fn.args().get(1), ctx));
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < array.size(); i++) {
                    if (i > 0) sb.append(lineFeeds ? ",\n " : ",");
                    appendJsonValue(sb, array.get(i));
                }
                return sb.append(']').toString();
            }
            case "to_json":
            case "to_jsonb": {
                // A row is an object here too. Writing the composite's own text and quoting it
                // handed a client one string with every field run together inside it.
                Object arg = wholeRowOrValue(fn.args().get(0), ctx);
                if (arg == null) return null; // both are strict: nothing in, nothing out
                // A value that is already a json document is that document. Reading it as a Java
                // string instead quoted it, so to_jsonb of a jsonb wrapped it in a second layer.
                DataType argType = executor.exprEvaluator.inferExprType(fn.args().get(0));
                if (argType == DataType.JSON || argType == DataType.JSONB) {
                    String document = TypeCoercion.toString(arg);
                    return name.equals("to_jsonb") ? normalizedIfStructured(document) : document;
                }
                StringBuilder sb = new StringBuilder();
                appendJsonValue(sb, arg);
                String written = sb.toString();
                // jsonb is not the text it was handed: it orders its keys and spaces them out.
                return name.equals("to_jsonb") ? normalizedIfStructured(written) : written;
            }
            case "row_to_json": {
                Object arg = wholeRowOrValue(fn.args().get(0), ctx);
                if (arg == null) return null; // strict, like the rest of the family
                boolean pretty = false;
                if (fn.args().size() >= 2) {
                    Object prettyArg = executor.evalExpr(fn.args().get(1), ctx);
                    if (prettyArg instanceof Boolean) pretty = (Boolean) prettyArg;
                    else if (prettyArg != null) pretty = executor.isTruthy(prettyArg);
                }
                // A row built by ROW(...) is as much a row as one taken from a table: it names
                // its fields f1, f2 and so on rather than after any column, and it is still an
                // object. Reading only the one that came from a table left the other printing
                // the composite's own text, so row_to_json(row(1)) answered (1) and not {"f1":1}.
                Map<String, Object> fields = rowFields(arg);
                if (fields != null) return rowMapToJson(fields, pretty, pretty ? 1 : 0);
                if (arg instanceof java.util.List<?>) {
                    java.util.List<?> list = (java.util.List<?>) arg;
                    Map<String, Object> named = new java.util.LinkedHashMap<>();
                    for (int i = 0; i < list.size(); i++) named.put("f" + (i + 1), list.get(i));
                    return rowMapToJson(named, pretty, pretty ? 1 : 0);
                }
                return arg.toString();
            }
            case "jsonb_path_query": {
                PathCall call = preparePath(fn, ctx);
                if (call == null) return new ArrayList<>();
                try {
                    return new ArrayList<>(evaluateJsonPathAll(call.json, call.path));
                } catch (MemgresException e) {
                    if (!call.silent || !isSuppressible(e)) throw e;
                    return new ArrayList<>(); // Return as List for SRF expansion
                }
            }
            case "jsonb_path_query_array": {
                // PG: collect all jsonpath matches into a jsonb array.
                PathCall call = preparePath(fn, ctx);
                if (call == null) return null;
                List<String> results;
                try {
                    results = evaluateJsonPathAll(call.json, call.path);
                } catch (MemgresException e) {
                    if (!call.silent || !isSuppressible(e)) throw e;
                    results = new ArrayList<>();
                }
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < results.size(); i++) {
                    if (i > 0) sb.append(", ");
                    sb.append(results.get(i));
                }
                sb.append("]");
                return sb.toString();
            }
            case "jsonb_path_query_first": {
                PathCall call = preparePath(fn, ctx);
                if (call == null) return null;
                try {
                    List<String> results = evaluateJsonPathAll(call.json, call.path);
                    return results.isEmpty() ? null : results.get(0);
                } catch (MemgresException e) {
                    if (!call.silent || !isSuppressible(e)) throw e;
                    return null;
                }
            }
            case "jsonb_path_exists": {
                PathCall call = preparePath(fn, ctx);
                if (call == null) return null;
                try {
                    return evaluateJsonPathExists(call.json, call.path);
                } catch (MemgresException e) {
                    if (!call.silent || !isSuppressible(e)) throw e;
                    return null;
                }
            }
            case "jsonb_path_match": {
                // A predicate is not a path, so this one takes the arguments without the check
                requireArgs(fn, 2);
                Object jsonVal = executor.evalExpr(fn.args().get(0), ctx);
                Object pathVal = executor.evalExpr(fn.args().get(1), ctx);
                if (jsonVal == null || pathVal == null) return null;
                String path = pathVal.toString().trim();
                if (fn.args().size() > 2) {
                    Object varsVal = executor.evalExpr(fn.args().get(2), ctx);
                    if (varsVal != null) path = bindJsonPathVars(path, varsVal.toString());
                }
                boolean silent = fn.args().size() > 3
                        && executor.isTruthy(executor.evalExpr(fn.args().get(3), ctx));
                try {
                    return evalPathMatch(JsonOperations.normalizeJsonb(jsonVal.toString()), path);
                } catch (MemgresException e) {
                    if (!silent || !isSuppressible(e)) throw e;
                    return null;
                }
            }
            // _tz variants: delegate to non-tz equivalents (timezone-aware, but we treat them the same)
            case "jsonb_path_exists_tz":
                return eval("jsonb_path_exists", fn, ctx);
            case "jsonb_path_match_tz":
                return eval("jsonb_path_match", fn, ctx);
            case "jsonb_path_query_tz":
                return eval("jsonb_path_query", fn, ctx);
            case "jsonb_path_query_first_tz":
                return eval("jsonb_path_query_first", fn, ctx);
            case "jsonb_path_query_array_tz":
                return eval("jsonb_path_query_array", fn, ctx);
            case "jsonb_typeof":
            case "json_typeof": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return "null";
                String s = arg.toString().trim();
                if (s.startsWith("{")) return "object";
                if (s.startsWith("[")) return "array";
                if (s.equals("true") || s.equals("false")) return "boolean";
                if (s.equals("null")) return "null";
                try { Double.parseDouble(s); return "number"; } catch (Exception e) { /* ignore */ }
                return "string";
            }
            case "jsonb_array_length":
            case "json_array_length": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return null;
                String s = arg.toString().trim();
                if (!JsonOperations.isArray(s)) {
                    throw new MemgresException("cannot get array length of a "
                            + (JsonOperations.isObject(s) ? "non-array" : "scalar"), "22023");
                }
                return JsonOperations.parseArrayElements(s).size();
            }
            case "jsonb_pretty": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : JsonOperations.pretty(arg.toString());
            }
            case "jsonb_extract_path_text":
            case "json_extract_path_text": {
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                if (json == null) return null;
                if (fn.args().size() > 1) {
                    List<String> path = new ArrayList<>();
                    for (int pi = 1; pi < fn.args().size(); pi++) {
                        path.add(String.valueOf(executor.evalExpr(fn.args().get(pi), ctx)));
                    }
                    return JsonOperations.extractPathText(json.toString(), path);
                }
                return json.toString();
            }
            case "jsonb_extract_path":
            case "json_extract_path": {
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                if (json == null) return null;
                if (fn.args().size() > 1) {
                    List<String> path = new ArrayList<>();
                    for (int pi = 1; pi < fn.args().size(); pi++) {
                        path.add(String.valueOf(executor.evalExpr(fn.args().get(pi), ctx)));
                    }
                    return JsonOperations.extractPath(json.toString(), path);
                }
                return json.toString();
            }
            case "json_array_elements":
            case "jsonb_array_elements":
            case "json_array_elements_text":
            case "jsonb_array_elements_text": {
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                // A set-returning function given NULL produces no rows at all
                if (json == null) return new ArrayList<>();
                String s = json.toString().trim();
                requireJsonArray(name, s);
                List<Object> elements = new ArrayList<>();
                for (String elem : JsonOperations.parseArrayElements(s)) {
                    if (name.endsWith("_text")) elem = JsonOperations.jsonValueToText(elem);
                    elements.add(elem);
                }
                return elements; // Return as List for SRF expansion
            }
            case "json_each":
            case "jsonb_each":
            case "json_each_text":
            case "jsonb_each_text": {
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                // A set-returning function given NULL produces no rows at all
                if (json == null) return new ArrayList<>();
                String s = json.toString().trim();
                requireJsonEachObject(name, s);
                boolean asText = name.endsWith("_text");
                List<Object> rows = new ArrayList<>();
                for (Map.Entry<String, String> entry : eachMembers(name, s).entrySet()) {
                    String value = entry.getValue() == null ? null : entry.getValue().trim();
                    rows.add(RecordValue.of("key", entry.getKey(), "value",
                            asText ? JsonOperations.jsonValueToText(value) : value));
                }
                return rows; // Return as List for SRF expansion
            }
            case "jsonb_object_keys":
            case "json_object_keys": {
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                if (json == null) return new ArrayList<>();
                String s = json.toString().trim();
                requireJsonObject(name, s);
                return new ArrayList<>(eachMembers(name, s).keySet());
            }
            case "jsonb_set": {
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                if (json == null) return null;
                // hstore subscript update: jsonb_set(hstore, '{key}', value) → merge key into hstore
                if (json instanceof HstoreValue) {
                    Object pathArg = executor.evalExpr(fn.args().get(1), ctx);
                    Object newVal = executor.evalExpr(fn.args().get(2), ctx);
                    List<String> hpath = parsePathArg(pathArg);
                    if (!hpath.isEmpty()) {
                        String key = hpath.get(0);
                        String val = newVal != null ? newVal.toString() : null;
                        // Strip surrounding quotes from JSON string value (to_jsonb wraps in quotes)
                        if (val != null && val.startsWith("\"") && val.endsWith("\"")) {
                            val = val.substring(1, val.length() - 1);
                        }
                        HstoreValue h = (HstoreValue) json;
                        java.util.Map<String, String> merged = new java.util.LinkedHashMap<>(h.getData());
                        merged.put(key, val);
                        return new HstoreValue(merged);
                    }
                    return json;
                }
                Object pathArg = executor.evalExpr(fn.args().get(1), ctx);
                // Validate path is text[] format
                if (pathArg instanceof String && !(pathArg instanceof java.util.List) && !((String) pathArg).trim().startsWith("{")) {
                    String ps = (String) pathArg;
                    throw new MemgresException("malformed array literal: \"" + ps + "\"", "22P02");
                }
                Object newVal = executor.evalExpr(fn.args().get(2), ctx);
                boolean createMissing = fn.args().size() < 4
                        || executor.isTruthy(executor.evalExpr(fn.args().get(3), ctx));
                List<String> path = parsePathArg(pathArg);
                String newValStr = jsonValueStr(newVal);
                return JsonOperations.jsonbSet(json.toString(), path, newValStr, createMissing);
            }
            case "jsonb_set_lax": {
                // PG 13+: jsonb_set_lax(target, path, new_value [, create_if_missing [, null_value_treatment]])
                //   null_value_treatment: 'raise_exception'|'use_json_null'|'delete_key'|'return_target'
                //   Default behaviour on NULL new_value is 'use_json_null'.
                requireArgs(fn, 3);
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                if (json == null) return null;
                Object pathArg = executor.evalExpr(fn.args().get(1), ctx);
                Object newVal = executor.evalExpr(fn.args().get(2), ctx);
                boolean createIfMissing = fn.args().size() < 4
                        || executor.isTruthy(executor.evalExpr(fn.args().get(3), ctx));
                String treatment = "use_json_null";
                if (fn.args().size() > 4) {
                    Object tv = executor.evalExpr(fn.args().get(4), ctx);
                    if (tv != null) treatment = tv.toString().toLowerCase();
                }
                List<String> path = parsePathArg(pathArg);
                if (newVal == null) {
                    switch (treatment) {
                        case "return_target":
                            return json.toString();
                        case "delete_key":
                            // Remove the element at path; we approximate by walking the path
                            // and deleting the final key from its parent object.
                            if (path.isEmpty()) return json.toString();
                            if (path.size() == 1) {
                                return JsonOperations.deleteKey(json.toString(), path.get(0));
                            }
                            // For nested paths, build a parent navigation by jsonb_set-like walk
                            // Fall back to use_json_null if nested delete isn't trivial.
                            return JsonOperations.jsonbSet(json.toString(), path, "null");
                        case "raise_exception":
                            throw new MemgresException(
                                "JSON value must not be null", "22004");
                        case "use_json_null":
                        default:
                            return JsonOperations.jsonbSet(json.toString(), path, "null");
                    }
                }
                String newValStr = jsonValueStr(newVal);
                return JsonOperations.jsonbSet(json.toString(), path, newValStr, createIfMissing);
            }
            case "jsonb_strip_nulls": {
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                if (json == null) return null;
                // The second argument says whether a null element of an array goes the way a null
                // member of an object always does.
                boolean inArrays = fn.args().size() > 1
                        && executor.isTruthy(executor.evalExpr(fn.args().get(1), ctx));
                return JsonOperations.stripNulls(json.toString(), false, inArrays);
            }
            case "jsonb_insert": {
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                if (json == null) return null;
                Object pathArg = executor.evalExpr(fn.args().get(1), ctx);
                Object newVal = executor.evalExpr(fn.args().get(2), ctx);
                boolean insertAfter = fn.args().size() > 3 && executor.isTruthy(executor.evalExpr(fn.args().get(3), ctx));
                List<String> path = parsePathArg(pathArg);
                // Validate array index paths (non-integer keys in array contexts)
                String jsonStr = json.toString().trim();
                for (int pi = 0; pi < path.size(); pi++) {
                    String key = path.get(pi);
                    // If we're accessing an array position, the key must be numeric
                    // Navigate to the element to check if it's an array
                    String current = jsonStr;
                    for (int pj = 0; pj < pi; pj++) {
                        if (current == null) break;
                        current = JsonOperations.extractKey(current.trim(), path.get(pj));
                        if (current == null) {
                            current = JsonOperations.extractArrayElement(jsonStr, 0);
                        }
                    }
                    if (current != null && current.trim().startsWith("[")) {
                        try { Integer.parseInt(key); } catch (NumberFormatException e) {
                            throw new MemgresException("path element at position " + (pi + 1) + " is not an integer: \"" + key + "\"", "22P02");
                        }
                    }
                }
                String newValStr = jsonValueStr(newVal);
                return JsonOperations.jsonbInsert(json.toString(), path, newValStr, insertAfter);
            }
            case "jsonb_exists": {
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                Object key = executor.evalExpr(fn.args().get(1), ctx);
                if (json == null || key == null) return null;
                return JsonOperations.keyExists(json.toString(), key.toString());
            }
            case "jsonb_agg":
            case "json_agg": {
                // Aggregate function, handled in aggregate evaluation
                return null;
            }
            case "json_strip_nulls": {
                // Same as jsonb_strip_nulls but for json type (compact output, no extra spaces)
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                if (json == null) return null;
                boolean inArrays = fn.args().size() > 1
                        && executor.isTruthy(executor.evalExpr(fn.args().get(1), ctx));
                return JsonOperations.stripNulls(json.toString(), true, inArrays);
            }
            case "json_object":
            case "jsonb_object": {
                // jsonb_object(text[]) → builds a JSON object from a flat array of key/value pairs
                // jsonb_object(keys text[], values text[]) → builds a JSON object from two arrays
                requireArgs(fn, 1);
                // json keeps the spacing of its own text output, which puts spaces round the colon
                String colon = name.startsWith("jsonb") ? "\": " : "\" : ";
                Object arg1 = executor.evalExpr(fn.args().get(0), ctx);
                if (arg1 == null) return null;
                if (fn.args().size() >= 2) {
                    // Two-array form: jsonb_object(keys[], values[])
                    Object arg2 = executor.evalExpr(fn.args().get(1), ctx);
                    if (arg2 == null) return null;
                    List<Object> keys = toList(arg1);
                    List<Object> values = toList(arg2);
                    if (keys.size() != values.size()) {
                        throw new MemgresException("mismatched array dimensions", "22023");
                    }
                    StringBuilder sb = new StringBuilder("{");
                    for (int i = 0; i < keys.size(); i++) {
                        if (i > 0) sb.append(", ");
                        Object k = keys.get(i);
                        if (k == null) throw new MemgresException("null value not allowed for object key", "22023");
                        sb.append("\"").append(k.toString().replace("\\", "\\\\").replace("\"", "\\\"")).append(colon);
                        Object v = values.get(i);
                        if (v == null) sb.append("null");
                        else sb.append("\"").append(v.toString().replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
                    }
                    sb.append("}");
                    return sb.toString();
                } else {
                    // Single flat array form: jsonb_object('{k1,v1,k2,v2,...}')
                    List<Object> elems = toList(arg1);
                    if (elems.size() % 2 != 0) {
                        throw new MemgresException("array must have even number of elements", "2202E");
                    }
                    StringBuilder sb = new StringBuilder("{");
                    for (int i = 0; i < elems.size(); i += 2) {
                        if (i > 0) sb.append(", ");
                        Object k = elems.get(i);
                        if (k == null) throw new MemgresException("null value not allowed for object key", "22023");
                        sb.append("\"").append(k.toString().replace("\\", "\\\\").replace("\"", "\\\"")).append(colon);
                        Object v = elems.get(i + 1);
                        if (v == null) sb.append("null");
                        else sb.append("\"").append(v.toString().replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
                    }
                    sb.append("}");
                    return sb.toString();
                }
            }
            case "json_populate_record":
            case "jsonb_populate_record": {
                // json_populate_record(base record, json) → record with fields filled from JSON
                // In our engine, we return the JSON object as a map
                requireArgs(fn, 2);
                requireConcreteRowType(fn, name);
                Object baseArg = executor.evalExpr(fn.args().get(0), ctx);
                Object jsonArg = executor.evalExpr(fn.args().get(1), ctx);
                if (jsonArg == null) return baseArg;
                String jsonStr = jsonArg.toString().trim();
                if (!jsonStr.startsWith("{")) {
                    throw new MemgresException("cannot call json_populate_record on a non-object", "22023");
                }
                return populateRecordFromJson(baseArg, jsonStr);
            }
            case "json_populate_recordset":
            case "jsonb_populate_recordset": {
                // json_populate_recordset(base record, json_array) → setof record
                requireArgs(fn, 2);
                Object baseArg = executor.evalExpr(fn.args().get(0), ctx);
                Object jsonArg = executor.evalExpr(fn.args().get(1), ctx);
                if (jsonArg == null) return new java.util.ArrayList<>();
                String jsonStr = jsonArg.toString().trim();
                if (!jsonStr.startsWith("[")) {
                    throw new MemgresException("cannot call json_populate_recordset on a non-array", "22023");
                }
                // Parse JSON array and populate records
                List<Object> results = new java.util.ArrayList<>();
                String inner = jsonStr.substring(1, jsonStr.length() - 1).trim();
                if (!inner.isEmpty()) {
                    List<String> elements = splitJsonPairs(inner);
                    for (String elem : elements) {
                        String trimmed = elem.trim();
                        if (trimmed.startsWith("{")) {
                            results.add(populateRecordFromJson(baseArg, trimmed));
                        }
                    }
                }
                return results;
            }
            default:
                return NOT_HANDLED;
        }
    }

    /**
     * The fields of a row value, named as the row names them.
     *
     * <p>A row built by {@code ROW(...)} has no names of its own, so PostgreSQL calls its fields
     * f1, f2 and so on. A row taken from a table or a subquery is named by the columns it came
     * from, and arrives here already carrying them.
     */
    static Map<String, Object> rowFields(Object value) {
        if (value instanceof Map<?, ?>) {
            Map<String, Object> named = new java.util.LinkedHashMap<>();
            for (Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
                named.put(String.valueOf(e.getKey()), e.getValue());
            }
            return named;
        }
        if (value instanceof AstExecutor.PgRow) {
            List<Object> values = ((AstExecutor.PgRow) value).values();
            Map<String, Object> named = new java.util.LinkedHashMap<>();
            for (int i = 0; i < values.size(); i++) named.put("f" + (i + 1), values.get(i));
            return named;
        }
        return null;
    }

    /**
     * jsonb text under the ordering and spacing jsonb keeps. A scalar has neither keys to order
     * nor members to space, and running one through the normalizer only risked changing it.
     */
    static String normalizedIfStructured(String json) {
        if (json == null) return null;
        String trimmed = json.trim();
        return trimmed.startsWith("{") || trimmed.startsWith("[")
                ? JsonOperations.normalizeJsonb(json) : json;
    }

    /** An already-shaped value written out as the JSON text it stands for. */
    String jsonTextOf(Object shaped) {
        StringBuilder sb = new StringBuilder();
        appendJsonValue(sb, shaped);
        return sb.toString();
    }

    /**
     * The value of an argument seen as the JSON it stands for. A bare alias names every column of
     * the relation it is bound to rather than a column of that name, an array is a JSON array and
     * a composite is a JSON object -- none of the three is the text they print as.
     */
    Object wholeRowOrValue(Expression arg, RowContext ctx) {
        String relation = wholeRowRelation(arg);
        if (relation != null && ctx != null) {
            RowContext.TableBinding b = ctx.getBinding(relation);
            // The relation may belong to a query this one is written inside rather than to this
            // one: to_jsonb(o.*) in a subquery names the row of the query around it, because that
            // is where PostgreSQL resolves a name this query's own FROM clause has not got.
            for (Iterator<RowContext> it = executor.outerContextStack.descendingIterator();
                    b == null && it.hasNext(); ) {
                b = it.next().getBinding(relation);
            }
            if (b != null && b.table() != null && !b.table().getColumns().isEmpty()) {
                Map<String, Object> record = new java.util.LinkedHashMap<>();
                for (int i = 0; i < b.table().getColumns().size(); i++) {
                    Column column = b.table().getColumns().get(i);
                    record.put(column.getName(), columnValueAsJson(b.row()[i], column));
                }
                return record;
            }
        }
        if (arg instanceof ColumnRef && ctx != null) {
            ColumnRef ref = (ColumnRef) arg;
            Column column = ctx.resolveColumnDef(ref.table(), ref.column());
            if (column != null) return columnValueAsJson(executor.evalExpr(arg, ctx), column);
        }
        if (arg instanceof ArrayExpr && ((ArrayExpr) arg).isRow()) {
            List<Expression> elements = ((ArrayExpr) arg).elements();
            Map<String, Object> record = new java.util.LinkedHashMap<>();
            for (int i = 0; i < elements.size(); i++) {
                record.put("f" + (i + 1), wholeRowOrValue(elements.get(i), ctx));
            }
            return record;
        }
        Object value = executor.evalExpr(arg, ctx);
        Map<String, Object> declared = fieldsOfDeclaredComposite(arg, value, ctx);
        if (declared != null) return declared;
        return shapedByType(value, executor.binaryOpEvaluator.declaredTypeForResolution(arg, ctx));
    }

    /** What a column of a table holds, seen as the JSON its declared type stands for. */
    private Object columnValueAsJson(Object value, Column column) {
        if (value == null) return null;
        boolean json = column.getType() == DataType.JSON || column.getType() == DataType.JSONB;
        if (column.getArrayElementType() != null) {
            return arrayElements(asArray(value), column.getCompositeTypeName(), json);
        }
        if (column.getCompositeTypeName() != null) {
            Map<String, Object> named = namedComposite(value, column.getCompositeTypeName());
            if (named != null) return named;
        }
        return json ? value : plainIfText(value);
    }

    /**
     * The members of an array, each seen as what its own element type stands for: an array of a
     * composite holds objects rather than the text each of its elements prints as.
     */
    private Object arrayElements(Object array, String compositeType, boolean json) {
        if (!(array instanceof List<?>)) return array;
        List<Object> converted = new ArrayList<Object>();
        for (Object element : (List<?>) array) {
            Map<String, Object> named =
                    compositeType == null ? null : namedComposite(element, compositeType);
            if (named != null) converted.add(named);
            else if (element instanceof List<?>) converted.add(arrayElements(element, compositeType, json));
            else converted.add(json ? element : plainIfText(element));
        }
        return converted;
    }

    /**
     * A value seen as the JSON its declared type stands for: an array as an array, and anything
     * else that is not itself JSON as the text it is. Guessing from the value alone read a text
     * of braces as an object, so a column holding {@code {1,2}} was written out unquoted.
     */
    private static Object shapedByType(Object value, String declaredType) {
        if (declaredType == null) return value;
        if (declaredType.endsWith("[]")) return asArray(value);
        return declaredType.equals("json") || declaredType.equals("jsonb")
                ? value : plainIfText(value);
    }

    private static Object plainIfText(Object value) {
        return value instanceof String ? new PlainText((String) value) : value;
    }

    private static Object asArray(Object value) {
        if (!(value instanceof String)) return value;
        String text = ((String) value).trim();
        return text.startsWith("{") ? FunctionEvaluator.parseSimplePgArray(text) : value;
    }

    /** Text that is text whatever it looks like: a string of braces is not an object. */
    static final class PlainText {
        final String text;

        PlainText(String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }
    }

    /**
     * The relation a whole-row argument names, or null when the argument is an ordinary value.
     * {@code t} and {@code t.*} both stand for every column of the relation t is bound to.
     */
    private static String wholeRowRelation(Expression arg) {
        if (arg instanceof CompositeStarExpr) {
            return wholeRowRelation(((CompositeStarExpr) arg).expr());
        }
        if (arg instanceof WildcardExpr) {
            WildcardExpr w = (WildcardExpr) arg;
            return w.catalog() == null && w.schema() == null ? w.table() : null;
        }
        if (arg instanceof ColumnRef) {
            ColumnRef ref = (ColumnRef) arg;
            if (ref.table() == null) return ref.column();
            return "*".equals(ref.column()) ? ref.table() : null;
        }
        return null;
    }

    /**
     * A composite the statement named gives its fields the names it declared them with, so
     * {@code ROW(1, 'a')::rj_comp} is an object of a and b and not one of f1 and f2.
     */
    private Map<String, Object> fieldsOfDeclaredComposite(Expression arg, Object value, RowContext ctx) {
        String typeName = executor.resolveCompositeTypeName(arg, ctx);
        return typeName == null ? null : namedComposite(value, typeName);
    }

    /** A composite value under the names its type declared, whether it is held as a row or as text. */
    private Map<String, Object> namedComposite(Object value, String typeName) {
        List<CreateTypeStmt.CompositeField> declared = executor.database.getRowType(typeName);
        if (declared == null || declared.isEmpty()) return null;
        List<Object> values;
        if (value instanceof AstExecutor.PgRow) values = ((AstExecutor.PgRow) value).values();
        else if (value instanceof List<?>) values = new ArrayList<Object>((List<?>) value);
        else if (value instanceof String && ((String) value).trim().startsWith("(")) {
            values = executor.compositeTypeHandler
                    .parseCompositeToRow(((String) value).trim(), typeName).values();
        } else return null;
        if (values.size() != declared.size()) return null;
        Map<String, Object> named = new LinkedHashMap<>();
        for (int i = 0; i < declared.size(); i++) named.put(declared.get(i).name(), values.get(i));
        return named;
    }

    private void appendJsonValue(StringBuilder sb, Object val) {
        if (val instanceof PlainText) {
            String text = ((PlainText) val).text;
            sb.append("\"").append(text.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
            return;
        }
        Map<String, Object> fields = rowFields(val);
        if (fields != null) {
            // A row inside a row is an object inside an object, not the text a composite prints as.
            sb.append(rowMapToJson(fields, false, 0));
            return;
        }
        if (val == null) {
            sb.append("null");
        } else if (val instanceof byte[]) {
            // A bytea is the hex text PG writes it as; Java's own toString named the array object.
            sb.append("\"\\\\x").append(ByteaOperations.bytesToHex((byte[]) val)).append("\"");
        } else if (val instanceof Number || val instanceof Boolean) {
            sb.append(val);
        } else if (val instanceof List<?>) {
            List<?> list = (List<?>) val;
            sb.append("[");
            for (int j = 0; j < list.size(); j++) {
                if (j > 0) sb.append(",");
                appendJsonValue(sb, list.get(j));
            }
            sb.append("]");
        } else {
            String s = val.toString();
            // Check if it's already valid JSON (object or array)
            if ((s.startsWith("{") && s.endsWith("}")) || (s.startsWith("[") && s.endsWith("]"))) {
                sb.append(s);
            } else {
                sb.append("\"").append(s.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
            }
        }
    }

    private static String repeat(String s, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(s);
        }
        return sb.toString();
    }

    private String rowMapToJson(java.util.Map<?, ?> map, boolean pretty, int indent) {
        StringBuilder sb = new StringBuilder("{");
        // Asked for it pretty, PostgreSQL breaks the line between one field and the next and
        // nowhere else — so a row of a single field is written on one line whatever was asked.
        String sep = pretty ? ",\n" + repeat(" ", Math.max(indent, 1)) : ",";
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!first) sb.append(sep);
            first = false;
            sb.append("\"").append(entry.getKey()).append("\":");
            appendJsonValue(sb, entry.getValue());
        }
        sb.append("}");
        return sb.toString();
    }

    private Map<String, Object> populateRecordFromJson(Object baseArg, String jsonStr) {
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        // If base is a map, start with its values
        if (baseArg instanceof Map<?, ?>) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) baseArg).entrySet()) {
                result.put(entry.getKey().toString(), entry.getValue());
            }
        }
        // Parse JSON object and overlay values
        String inner = jsonStr.substring(1, jsonStr.length() - 1).trim();
        if (!inner.isEmpty()) {
            List<String> pairs = splitJsonPairs(inner);
            for (String pair : pairs) {
                int colonIdx = pair.indexOf(':');
                if (colonIdx < 0) continue;
                String key = pair.substring(0, colonIdx).trim();
                String val = pair.substring(colonIdx + 1).trim();
                if (key.startsWith("\"") && key.endsWith("\"")) key = key.substring(1, key.length() - 1);
                Object parsed;
                if (val.equals("null")) parsed = null;
                else if (val.startsWith("\"") && val.endsWith("\"")) parsed = val.substring(1, val.length() - 1).replace("\\\"", "\"").replace("\\\\", "\\");
                else if (val.equals("true")) parsed = true;
                else if (val.equals("false")) parsed = false;
                else {
                    try { parsed = Integer.parseInt(val); }
                    catch (NumberFormatException e1) {
                        try { parsed = Long.parseLong(val); }
                        catch (NumberFormatException e2) {
                            try { parsed = Double.parseDouble(val); }
                            catch (NumberFormatException e3) { parsed = val; }
                        }
                    }
                }
                result.put(key, parsed);
            }
        }
        return result;
    }

    String extractJsonKey(String json, String key) {
        return JsonOperations.extractKey(json, key);
    }

    /**
     * The column names a record-returning built-in produces, or null when the expression does not
     * call one. {@code (jsonb_each(x)).*} has to know the shape before a row has been read.
     */
    static List<String> recordFieldNames(Expression expr) {
        if (!(expr instanceof FunctionCallExpr)) return null;
        String name = FunctionEvaluator.stripSchemaPrefix(((FunctionCallExpr) expr).name().toLowerCase());
        if (name.equals("json_each") || name.equals("jsonb_each")
                || name.equals("json_each_text") || name.equals("jsonb_each_text")) {
            return Cols.listOf("key", "value");
        }
        return null;
    }

    /**
     * A jsonpath may open with a {@code strict} or {@code lax} mode word. lax is the default and
     * the mode only changes how a missing step is reported, so the word is consumed here and the
     * strictness recorded for the evaluator.
     */
    /**
     * Bind {@code $name} references from the vars object PG takes as the third argument. The
     * root {@code $} is never a variable, so only a {@code $} followed by an identifier binds.
     */
    static String bindJsonPathVars(String path, String varsJson) {
        if (varsJson == null || varsJson.trim().isEmpty()) return path;
        Map<String, String> vars = JsonOperations.parseObjectKeys(varsJson.trim());
        if (vars.isEmpty()) return path;
        StringBuilder sb = new StringBuilder(path.length());
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '$' && i + 1 < path.length()
                    && (Character.isLetter(path.charAt(i + 1)) || path.charAt(i + 1) == '_')) {
                int j = i + 1;
                while (j < path.length()
                        && (Character.isLetterOrDigit(path.charAt(j)) || path.charAt(j) == '_')) j++;
                String value = vars.get(path.substring(i + 1, j));
                if (value != null) {
                    sb.append(value.trim());
                    i = j - 1;
                    continue;
                }
            }
            sb.append(c);
        }
        return sb.toString();
    }

    static String stripJsonPathMode(String path) {
        String p = path.trim();
        if (p.regionMatches(true, 0, "strict", 0, 6) && p.length() > 6
                && Character.isWhitespace(p.charAt(6))) {
            return p.substring(6).trim();
        }
        if (p.regionMatches(true, 0, "lax", 0, 3) && p.length() > 3
                && Character.isWhitespace(p.charAt(3))) {
            return p.substring(3).trim();
        }
        return p;
    }

    /** True when the path opens with the strict mode word. */
    static boolean isStrictJsonPath(String path) {
        String p = path.trim();
        return p.regionMatches(true, 0, "strict", 0, 6) && p.length() > 6
                && Character.isWhitespace(p.charAt(6));
    }

    List<String> evaluateJsonPathAll(String json, String path) {
        final boolean strict = isStrictJsonPath(path);
        path = stripJsonPathMode(path);
        if (JsonPathArithmetic.isArithmetic(path)) {
            final String doc = json;
            return JsonPathArithmetic.evaluate(path, new JsonPathArithmetic.PathEvaluator() {
                @Override public List<String> evaluate(String operandPath) {
                    return evaluateJsonPath(doc, operandPath, strict);
                }
            });
        }
        return evaluateJsonPath(json, path, strict);
    }

    /**
     * The @? and @@ operators, and the silent argument, answer NULL rather than raising when the
     * document does not have the shape the path asks for. Only errors from walking the document
     * are swallowed — a path that does not parse is still a syntax error.
     */
    static boolean isSuppressible(MemgresException e) {
        String state = e.getSqlState();
        return state != null && state.startsWith("22");
    }

    /** The arguments of a jsonb_path_* call, evaluated once. */
    private static final class PathCall {
        final String json;
        final String path;
        final boolean silent;

        PathCall(String json, String path, boolean silent) {
            this.json = json;
            this.path = path;
            this.silent = silent;
        }
    }

    /**
     * Evaluate the arguments, bind the vars object into the path and check its shape.
     *
     * @return the call to make, or null when either the document or the path is NULL
     */
    private PathCall preparePath(FunctionCallExpr fn, RowContext ctx) {
        requireArgs(fn, 2);
        Object jsonVal = executor.evalExpr(fn.args().get(0), ctx);
        Object pathVal = executor.evalExpr(fn.args().get(1), ctx);
        if (jsonVal == null || pathVal == null) return null;
        String path = pathVal.toString().trim();
        if (fn.args().size() > 2) {
            Object varsVal = executor.evalExpr(fn.args().get(2), ctx);
            if (varsVal != null) path = bindJsonPathVars(path, varsVal.toString());
        }
        boolean silent = fn.args().size() > 3
                && executor.isTruthy(executor.evalExpr(fn.args().get(3), ctx));
        // The document argument is jsonb, so a literal written any other way is read as jsonb
        // first: what the path selects out of it is printed the way jsonb prints, not the way the
        // literal happened to be spaced.
        String document = JsonOperations.normalizeJsonb(jsonVal.toString());
        // The mode word is part of the path; check the shape after it. An expression may open
        // with an operand of its own, so only something that is neither is a syntax error.
        String bodyPath = stripJsonPathMode(path);
        if (!bodyPath.startsWith("$") && !bodyPath.startsWith("@")
                && !JsonPathArithmetic.isArithmetic(bodyPath)) {
            throw new MemgresException("syntax error at or near \""
                    + bodyPath.substring(0, Math.min(3, bodyPath.length())) + "\" of jsonpath input", "42601");
        }
        return new PathCall(document, path, silent);
    }

    /**
     * jsonb_path_match: the path has to produce exactly one boolean. A path that produces a
     * number, nothing at all, or more than one item is not a mistake PG guesses its way past --
     * a caller asking "does this match" gets told the question had no single answer.
     */
    Object evalPathMatch(String json, String path) {
        // Handle exists(...) syntax
        if (path.startsWith("exists(") && path.endsWith(")")) {
            return evaluateJsonPathExists(json, path.substring(7, path.length() - 1).trim());
        }
        if (isTopLevelPredicate(path)) return evaluateJsonPathPredicate(json, path);
        // The mode word is part of the path; check the shape after it
        String bodyPath = stripJsonPathMode(path);
        if (!bodyPath.startsWith("$") && !bodyPath.startsWith("@")) {
            throw new MemgresException("syntax error at or near \""
                    + bodyPath.substring(0, Math.min(3, bodyPath.length())) + "\" of jsonpath input", "42601");
        }
        List<String> results = evaluateJsonPathAll(json, path);
        if (results.size() == 1) {
            String rs = results.get(0).trim();
            if (rs.equals("true")) return true;
            if (rs.equals("false")) return false;
        }
        throw new MemgresException("single boolean result is expected", "22038");
    }

    /** True when the path is a comparison rather than a path that selects items. */
    private static boolean isTopLevelPredicate(String path) {
        return findTopLevelComparison(path) != null;
    }

    /** The comparison operator and its position in the path, or null when there is none. */
    private static String[] findTopLevelComparison(String path) {
        String[] ops = {"==", "!=", ">=", "<=", ">", "<"};
        for (String op : ops) {
            int depth = 0;
            boolean inString = false;
            for (int i = 0; i + op.length() <= path.length(); i++) {
                char c = path.charAt(i);
                if (c == '\\' && inString) { i++; continue; }
                if (c == '"') { inString = !inString; continue; }
                if (inString) continue;
                if (c == '(' || c == '[' || c == '{') depth++;
                else if (c == ')' || c == ']' || c == '}') depth--;
                else if (depth == 0 && path.startsWith(op, i)) {
                    return new String[]{op, Integer.toString(i)};
                }
            }
        }
        return null;
    }

    private List<String> evaluateJsonPath(String json, String path, boolean strict) {
        // PG does not support recursive descent ($..key) — throw syntax error
        if (path.contains("..")) {
            throw new MemgresException("syntax error at or near \".\" of jsonpath input", "42601");
        }
        // Validate path syntax — reject nested brackets like $.[[invalid
        if (path.contains("[[")) {
            throw new MemgresException("syntax error at or near \"[\" of jsonpath input", "42601");
        }
        // Strip leading $
        String rest = path.startsWith("$") ? path.substring(1) : path;

        // Check for filter expression: <path> ? (<filter>)<postPath>
        // e.g. $.a[*] ? (@ > 2)  or  $.items[*] ? (@.qty >= 2).sku
        String filterExpr = null;
        String postFilterPath = null;
        int qIdx = rest.indexOf('?');
        if (qIdx >= 0) {
            String afterQ = rest.substring(qIdx + 1).trim();
            rest = rest.substring(0, qIdx).trim();
            // Find matching closing paren to separate filter from post-filter path
            if (afterQ.startsWith("(")) {
                int depth = 0;
                int closeIdx = -1;
                for (int pi = 0; pi < afterQ.length(); pi++) {
                    if (afterQ.charAt(pi) == '(') depth++;
                    else if (afterQ.charAt(pi) == ')') {
                        depth--;
                        if (depth == 0) { closeIdx = pi; break; }
                    }
                }
                if (closeIdx >= 0) {
                    filterExpr = afterQ.substring(1, closeIdx).trim();
                    postFilterPath = afterQ.substring(closeIdx + 1).trim();
                    if (postFilterPath.isEmpty()) postFilterPath = null;
                } else {
                    filterExpr = afterQ;
                    if (filterExpr.startsWith("(") && filterExpr.endsWith(")")) {
                        filterExpr = filterExpr.substring(1, filterExpr.length() - 1).trim();
                    }
                }
            } else {
                filterExpr = afterQ;
            }
        }

        // Walk the path segments
        List<String> current = new ArrayList<>();
        current.add(json.trim());

        // Split path on dots and brackets, handling [*] and [n]
        int i = 0;
        while (i < rest.length()) {
            char c = rest.charAt(i);
            if (c == '.') {
                i++;
                String key;
                boolean quotedKey = false;
                // A member name may be written in double quotes, and PostgreSQL's own jsonpath
                // output always writes it that way: '$.a'::jsonpath prints as $."a". A key read
                // with its quotes still attached matches no member, so the whole path selects
                // nothing -- which is how @? and @@ came to answer false for a key that is there.
                if (i < rest.length() && rest.charAt(i) == '"') {
                    int q = i + 1;
                    StringBuilder name = new StringBuilder();
                    while (q < rest.length() && rest.charAt(q) != '"') {
                        if (rest.charAt(q) == '\\' && q + 1 < rest.length()) q++;
                        name.append(rest.charAt(q));
                        q++;
                    }
                    if (q >= rest.length()) {
                        throw new MemgresException("unexpected end of jsonpath input", "42601");
                    }
                    key = name.toString();
                    quotedKey = true;
                    i = q + 1;
                } else {
                    int start = i;
                    while (i < rest.length() && rest.charAt(i) != '.' && rest.charAt(i) != '[') i++;
                    key = rest.substring(start, i);
                }
                if (quotedKey) {
                    // A quoted name is a member name and nothing else: it is never a wildcard and
                    // never an item method, however it happens to be spelled.
                    List<String> next = new ArrayList<>();
                    for (String node : current) applyMember(node.trim(), key, strict, next);
                    current = next;
                } else if (!key.isEmpty()) {
                    if (key.equals("*")) {
                        // Wildcard: expand all values of the object
                        List<String> next = new ArrayList<>();
                        for (String node : current) {
                            node = node.trim();
                            if (node.startsWith("{")) {
                                Map<String, String> map = JsonOperations.parseObjectKeys(node);
                                for (String v : map.values()) next.add(v.trim());
                            } else if (node.startsWith("[")) {
                                // For arrays, .* expands all elements
                                List<String> elems = JsonOperations.parseArrayElements(node);
                                for (String e : elems) next.add(e.trim());
                            }
                        }
                        current = next;
                    } else if (JsonPathItems.isMethod(key)) {
                        current = JsonPathItems.apply(key, current, strict);
                    } else {
                        List<String> next = new ArrayList<>();
                        for (String node : current) applyMember(node.trim(), key, strict, next);
                        current = next;
                    }
                }
            } else if (c == '[') {
                i++;
                int end = rest.indexOf(']', i);
                if (end < 0) break;
                String idxStr = rest.substring(i, end).trim();
                i = end + 1;
                if (idxStr.equals("*")) {
                    List<String> next = new ArrayList<>();
                    for (String node : current) {
                        node = node.trim();
                        if (node.startsWith("[")) {
                            List<String> elems = JsonOperations.parseArrayElements(node);
                            for (String e : elems) next.add(e.trim());
                        } else if (strict) {
                            throw new MemgresException(
                                    "jsonpath wildcard array accessor can only be applied to an array",
                                    "22039");
                        } else {
                            // lax mode treats a non-array as a one-element array before indexing
                            next.add(node);
                        }
                    }
                    current = next;
                } else {
                    List<int[]> ranges = parseSubscripts(idxStr);
                    // A subscript this evaluator cannot read is left alone rather than guessed at
                    if (ranges != null) current = applySubscripts(current, ranges, strict);
                }
            } else {
                i++;
            }
        }

        // Apply filter if present
        if (filterExpr != null && !filterExpr.isEmpty()) {
            String filter = filterExpr;
            List<String> filtered = new ArrayList<>();
            for (String node : current) {
                if (evaluateJsonPathFilter(node, filter)) {
                    filtered.add(node);
                }
            }
            current = filtered;
        }

        // Apply post-filter path segments
        if (postFilterPath != null && !postFilterPath.isEmpty()) {
            List<String> postResults = new ArrayList<>();
            for (String node : current) {
                postResults.addAll(evaluateJsonPath(node, "$" + postFilterPath, strict));
            }
            current = postResults;
        }

        return current;
    }

    /**
     * A member accessor wants an object. lax mode looks inside an array once, so a path written
     * for one object also reads a list of them; strict mode says the document is not shaped the
     * way the path claims rather than quietly matching nothing.
     */
    private void applyMember(String node, String key, boolean strict, List<String> out) {
        if (JsonOperations.isObject(node)) {
            String extracted = JsonOperations.extractKey(node, key);
            if (extracted != null) {
                out.add(extracted.trim());
            } else if (strict) {
                throw new MemgresException(
                        "JSON object does not contain key \"" + key + "\"", "2203A");
            }
            return;
        }
        if (strict) {
            throw new MemgresException(
                    "jsonpath member accessor can only be applied to an object", "2203A");
        }
        if (JsonOperations.isArray(node)) {
            for (String element : JsonOperations.parseArrayElements(node)) {
                String elem = element.trim();
                if (!JsonOperations.isObject(elem)) continue;
                String extracted = JsonOperations.extractKey(elem, key);
                if (extracted != null) out.add(extracted.trim());
            }
        }
    }

    /**
     * The subscripts of one accessor, e.g. {@code [0, 2 to last]}. Each range is held as four
     * numbers -- whether the start counts from {@code last} and by how much, then the same for
     * the end -- because {@code last} is only known once the array being indexed is.
     *
     * @return null when the text is not a subscript list this evaluator understands
     */
    private static List<int[]> parseSubscripts(String text) {
        List<int[]> ranges = new ArrayList<>();
        for (String part : splitJsonPairs(text)) {
            String item = part.trim();
            if (item.isEmpty()) return null;
            int toIdx = indexOfToKeyword(item);
            String fromText = toIdx < 0 ? item : item.substring(0, toIdx).trim();
            String endText = toIdx < 0 ? item : item.substring(toIdx + 2).trim();
            int[] from = parseSubscriptBound(fromText);
            int[] end = parseSubscriptBound(endText);
            if (from == null || end == null) return null;
            ranges.add(new int[]{from[0], from[1], end[0], end[1]});
        }
        return ranges.isEmpty() ? null : ranges;
    }

    /** The position of the {@code to} keyword separating a range's two bounds, or -1. */
    private static int indexOfToKeyword(String item) {
        for (int i = 1; i + 2 < item.length(); i++) {
            if (!Character.isWhitespace(item.charAt(i - 1))) continue;
            if (!item.regionMatches(true, i, "to", 0, 2)) continue;
            if (!Character.isWhitespace(item.charAt(i + 2))) continue;
            return i;
        }
        return -1;
    }

    /** A bound is an integer, {@code last}, or {@code last} shifted by an integer. */
    private static int[] parseSubscriptBound(String text) {
        String s = text.trim();
        if (s.regionMatches(true, 0, "last", 0, 4)) {
            String rest = s.substring(4).trim();
            if (rest.isEmpty()) return new int[]{1, 0};
            char sign = rest.charAt(0);
            if (sign != '+' && sign != '-') return null;
            try {
                int offset = Integer.parseInt(rest.substring(1).trim());
                return new int[]{1, sign == '-' ? -offset : offset};
            } catch (NumberFormatException e) {
                return null;
            }
        }
        try {
            return new int[]{0, Integer.parseInt(s)};
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * lax mode clamps a subscript to what the array holds and treats a non-array as an array of
     * one; strict mode refuses both, because a path that names element 5 of a three-element array
     * has misread the document.
     */
    private List<String> applySubscripts(List<String> nodes, List<int[]> ranges, boolean strict) {
        List<String> next = new ArrayList<>();
        for (String raw : nodes) {
            String node = raw.trim();
            List<String> elements;
            if (JsonOperations.isArray(node)) {
                elements = JsonOperations.parseArrayElements(node);
            } else if (strict) {
                throw new MemgresException(
                        "jsonpath array accessor can only be applied to an array", "22039");
            } else {
                elements = Cols.listOf(node);
            }
            int size = elements.size();
            for (int[] range : ranges) {
                int from = range[0] == 1 ? size - 1 + range[1] : range[1];
                int to = range[2] == 1 ? size - 1 + range[3] : range[3];
                if (strict) {
                    if (from < 0 || from > to || to >= size) {
                        throw new MemgresException(
                                "jsonpath array subscript is out of bounds", "22033");
                    }
                } else {
                    if (from < 0) from = 0;
                    if (to > size - 1) to = size - 1;
                }
                for (int k = from; k <= to; k++) next.add(elements.get(k).trim());
            }
        }
        return next;
    }

    private boolean evaluateJsonPathFilter(String nodeJson, String filter) {
        filter = filter.trim();
        // Handle && (AND) and || (OR) logical operators — split at top level (not inside parens)
        int andIdx = findTopLevelLogicalOp(filter, "&&");
        if (andIdx >= 0) {
            String left = filter.substring(0, andIdx).trim();
            String right = filter.substring(andIdx + 2).trim();
            return evaluateJsonPathFilter(nodeJson, left) && evaluateJsonPathFilter(nodeJson, right);
        }
        int orIdx = findTopLevelLogicalOp(filter, "||");
        if (orIdx >= 0) {
            String left = filter.substring(0, orIdx).trim();
            String right = filter.substring(orIdx + 2).trim();
            return evaluateJsonPathFilter(nodeJson, left) || evaluateJsonPathFilter(nodeJson, right);
        }
        // Handle parenthesized sub-expressions
        if (filter.startsWith("(") && filter.endsWith(")")) {
            return evaluateJsonPathFilter(nodeJson, filter.substring(1, filter.length() - 1).trim());
        }
        String[] ops = {">=", "<=", "!=", "==", ">", "<"};
        for (String op : ops) {
            int opIdx = filter.indexOf(op);
            if (opIdx < 0) continue;
            String left = filter.substring(0, opIdx).trim();
            String right = filter.substring(opIdx + op.length()).trim();
            if (!left.startsWith("@")) continue;
            String nodeVal;
            if (left.equals("@")) {
                nodeVal = nodeJson.trim();
            } else if (left.startsWith("@.")) {
                String subPath = left.substring(1);
                List<String> subResults = evaluateJsonPathAll(nodeJson, "$" + subPath);
                if (subResults.isEmpty()) continue;
                nodeVal = subResults.get(0).trim();
            } else {
                continue;
            }
            if (nodeVal.startsWith("\"") && nodeVal.endsWith("\"")) {
                String nodeStr = nodeVal.substring(1, nodeVal.length() - 1);
                String rightStr = right;
                if (rightStr.startsWith("\"") && rightStr.endsWith("\""))
                    rightStr = rightStr.substring(1, rightStr.length() - 1);
                int cmp = nodeStr.compareTo(rightStr);
                switch (op) {
                    case "==":
                        return cmp == 0;
                    case "!=":
                        return cmp != 0;
                    case ">":
                        return cmp > 0;
                    case "<":
                        return cmp < 0;
                    case ">=":
                        return cmp >= 0;
                    case "<=":
                        return cmp <= 0;
                    default:
                        return false;
                }
            }
            double nodeNum;
            try {
                nodeNum = Double.parseDouble(nodeVal);
            } catch (NumberFormatException e) {
                return false;
            }
            double rightNum;
            try {
                rightNum = Double.parseDouble(right);
            } catch (NumberFormatException e) {
                return false;
            }
            switch (op) {
                case "==":
                    return nodeNum == rightNum;
                case "!=":
                    return nodeNum != rightNum;
                case ">":
                    return nodeNum > rightNum;
                case "<":
                    return nodeNum < rightNum;
                case ">=":
                    return nodeNum >= rightNum;
                case "<=":
                    return nodeNum <= rightNum;
                default:
                    return false;
            }
        }
        return false;
    }

    /** Find the index of a top-level logical operator (not inside parentheses). */
    private int findTopLevelLogicalOp(String filter, String op) {
        int depth = 0;
        for (int i = 0; i < filter.length() - op.length() + 1; i++) {
            char c = filter.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && filter.startsWith(op, i)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * A jsonpath comparison compares every item the left side produces against the right, and is
     * true as soon as one pair matches. A step that cannot be walked -- a missing key under
     * strict, a subscript past the end -- makes the answer unknown rather than an error, which is
     * why {@code jsonb_path_match} on a strict path that misses answers NULL instead of failing.
     */
    Boolean evaluateJsonPathPredicate(String json, String path) {
        String[] found = findTopLevelComparison(path);
        if (found == null) return false;
        String op = found[0];
        int at = Integer.parseInt(found[1]);
        String leftExpr = path.substring(0, at).trim();
        String rightVal = normalizePredicateLiteral(path.substring(at + op.length()).trim());
        List<String> leftResults;
        try {
            leftResults = evaluateJsonPathAll(json, leftExpr);
        } catch (MemgresException e) {
            if (!isSuppressible(e)) throw e;
            return null;
        }
        for (String left : leftResults) {
            if (compareJsonValues(left.trim(), rightVal, op)) return true;
        }
        return false;
    }

    /** The right-hand side of a comparison is a literal; only .datetime() reshapes one. */
    private static String normalizePredicateLiteral(String rightExpr) {
        String rightVal = rightExpr.trim();
        if (!rightVal.endsWith(".datetime()")) return rightVal;
        String raw = rightVal.substring(0, rightVal.length() - ".datetime()".length()).trim();
        return JsonOperations.isString(raw) ? JsonPathItems.datetime(raw) : raw;
    }

    private static boolean compareJsonValues(String leftVal, String rightVal, String op) {
        if (JsonOperations.isString(rightVal)) {
            String right = rightVal.substring(1, rightVal.length() - 1);
            String left = JsonOperations.isString(leftVal)
                    ? leftVal.substring(1, leftVal.length() - 1) : leftVal;
            return compareTo(left.compareTo(right), op);
        }
        try {
            double leftNum = Double.parseDouble(leftVal);
            double rightNum = Double.parseDouble(rightVal);
            return compareTo(Double.compare(leftNum, rightNum), op);
        } catch (NumberFormatException e) {
            int cmp = leftVal.compareTo(rightVal);
            if (op.equals("==")) return cmp == 0;
            if (op.equals("!=")) return cmp != 0;
            return false;
        }
    }

    private static boolean compareTo(int cmp, String op) {
        switch (op) {
            case "==": return cmp == 0;
            case "!=": return cmp != 0;
            case ">": return cmp > 0;
            case "<": return cmp < 0;
            case ">=": return cmp >= 0;
            case "<=": return cmp <= 0;
            default: return false;
        }
    }

    /**
     * A whole path made of a comparison always produces one item -- true, false or unknown -- so
     * it always exists. Only a path that selects items can select none; that is the case @? and
     * jsonb_path_exists are asked about.
     */
    boolean evaluateJsonPathExists(String json, String path) {
        if (path.equals("$")) return true;
        if (isTopLevelPredicate(path)) {
            evaluateJsonPathPredicate(json, path);
            return true;
        }
        return !evaluateJsonPathAll(json, path).isEmpty();
    }

    private List<String> parsePathArg(Object pathArg) {
        if (pathArg instanceof List<?>) return ((List<?>) pathArg).stream().map(Object::toString).collect(Collectors.toList());
        String s = pathArg.toString().trim();
        if (s.startsWith("{") && s.endsWith("}")) {
            String inner = s.substring(1, s.length() - 1);
            return inner.isEmpty() ? Cols.listOf() : Arrays.asList(inner.split(","));
        }
        return Cols.listOf(s);
    }

    private String jsonValueStr(Object val) {
        if (val == null) return "null";
        if (val instanceof Number || val instanceof Boolean) return val.toString();
        String s = val.toString();
        if (s.startsWith("{") || s.startsWith("[") || s.equals("null") || s.equals("true") || s.equals("false")) return s;
        if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) return s;
        try { Double.parseDouble(s); return s; } catch (Exception e) { /* not a number */ }
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    /** Convert an object to a List (handles List, PG array string format). */
    @SuppressWarnings("unchecked")
    private List<Object> toList(Object obj) {
        if (obj instanceof List<?>) return (List<Object>) obj;
        if (obj instanceof String) {
            String s = ((String) obj).trim();
            if (s.startsWith("{") && s.endsWith("}")) {
                String inner = s.substring(1, s.length() - 1).trim();
                if (inner.isEmpty()) return new ArrayList<>();
                List<Object> result = new ArrayList<>();
                for (String elem : inner.split(",", -1)) {
                    String trimmed = elem.trim();
                    if (trimmed.equalsIgnoreCase("NULL")) result.add(null);
                    else if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) result.add(trimmed.substring(1, trimmed.length() - 1));
                    else result.add(trimmed);
                }
                return result;
            }
        }
        List<Object> single = new ArrayList<>();
        single.add(obj);
        return single;
    }

    /** Split on the commas of one nesting level: object members, or a subscript list. */
    private static List<String> splitJsonPairs(String s) {
        List<String> pairs = new ArrayList<>();
        int depth = 0;
        boolean inString = false;
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && inString) { i++; continue; }
            if (c == '"') inString = !inString;
            else if (!inString) {
                if (c == '{' || c == '[') depth++;
                else if (c == '}' || c == ']') depth--;
                else if (c == ',' && depth == 0) {
                    pairs.add(s.substring(start, i).trim());
                    start = i + 1;
                }
            }
        }
        String last = s.substring(start).trim();
        if (!last.isEmpty()) pairs.add(last);
        return pairs;
    }
}
