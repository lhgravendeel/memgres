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
     * The members a json_each or json_object_keys call walks. The jsonb family sees the keys in the
     * order jsonb stores them, one to a key; the json family sees the document as it was written,
     * which means a key written twice is walked twice.
     */
    static List<JsonParser.Member> eachMembers(String name, String json) {
        if (!name.startsWith("jsonb_")) return JsonOperations.members(json);
        List<JsonParser.Member> ordered = new ArrayList<>();
        for (Map.Entry<String, String> entry : JsonOperations.parseObjectKeys(json).entrySet()) {
            ordered.add(new JsonParser.Member(entry.getKey(), entry.getValue()));
        }
        return ordered;
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
                    JsonWriter.appendString(sb, TypeCoercion.toString(key));
                    sb.append(jsonb ? ": " : " : ");
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
                // An element is what its element type says it is, as it is for to_json. Read from
                // the value alone, an element of text spelled [1,2] came out as an array of two
                // numbers rather than as the six characters it holds. An array built where
                // nothing named its type carries no element type of its own, so the expression
                // it was written as is asked instead.
                boolean documents;
                if (array.elementType() != null) {
                    DataType elementType = DataType.fromPgName(array.elementType());
                    documents = elementType == DataType.JSON || elementType == DataType.JSONB;
                } else {
                    DataType declared =
                            executor.exprEvaluator.inferExprType(fn.args().get(0));
                    DataType element = declared == null ? null : DataType.elementOf(declared);
                    documents = element == DataType.JSON || element == DataType.JSONB;
                }
                List<?> elements = (List<?>) arrayElements(array, null, documents);
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < elements.size(); i++) {
                    if (i > 0) sb.append(lineFeeds ? ",\n " : ",");
                    appendJsonValue(sb, elements.get(i));
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
            case "jsonb_path_query":
                return pathQuery(fn, ctx, false);
            case "jsonb_path_query_tz":
                return pathQuery(fn, ctx, true);
            case "jsonb_path_query_array":
                return pathQueryArray(fn, ctx, false);
            case "jsonb_path_query_array_tz":
                return pathQueryArray(fn, ctx, true);
            case "jsonb_path_query_first":
                return pathQueryFirst(fn, ctx, false);
            case "jsonb_path_query_first_tz":
                return pathQueryFirst(fn, ctx, true);
            case "jsonb_path_exists":
                return pathExists(fn, ctx, false);
            case "jsonb_path_exists_tz":
                return pathExists(fn, ctx, true);
            case "jsonb_path_match":
                return pathMatch(fn, ctx, false);
            case "jsonb_path_match_tz":
                return pathMatch(fn, ctx, true);
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
                for (JsonParser.Member member : eachMembers(name, s)) {
                    String value = member.text.trim();
                    rows.add(RecordValue.of("key", member.key, "value",
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
                List<Object> keys = new ArrayList<>();
                for (JsonParser.Member member : eachMembers(name, s)) keys.add(member.key);
                return keys;
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
                Object pathArg = executor.evalExpr(fn.args().get(1), ctx);
                Object createArg = fn.args().size() < 4 ? Boolean.TRUE
                        : executor.evalExpr(fn.args().get(3), ctx);
                // Only the new value may be NULL: it is what the treatment is there to decide
                // about. A NULL anywhere else leaves nothing to decide, and the answer is NULL.
                if (json == null || pathArg == null || createArg == null) return null;
                Object newVal = executor.evalExpr(fn.args().get(2), ctx);
                boolean createIfMissing = executor.isTruthy(createArg);
                Object treatmentArg = fn.args().size() > 4
                        ? executor.evalExpr(fn.args().get(4), ctx) : "use_json_null";
                // A treatment that is NULL is refused whatever the new value is, because it was
                // read before the new value was looked at.
                if (treatmentArg == null) throw badNullValueTreatment();
                List<String> path = parsePathArg(pathArg);
                if (newVal == null) {
                    String treatment = treatmentArg.toString();
                    switch (treatment) {
                        case "return_target":
                            // The target is given back as the document it is, not as the text it
                            // was written as: jsonb prints with its own spacing.
                            return JsonOperations.normalizeJsonb(json.toString());
                        case "delete_key":
                            // The whole document is what the empty path names, and there is no
                            // member of anything to take away, so the target is left as it is.
                            if (path.isEmpty()) {
                                return JsonOperations.normalizeJsonb(json.toString());
                            }
                            return JsonOperations.deletePath(json.toString(), path);
                        case "raise_exception":
                            throw new MemgresException("JSON value must not be null"
                                    + "\n  Detail: Exception was raised because"
                                    + " null_value_treatment is \"raise_exception\"."
                                    + "\n  Hint: To avoid, either change the"
                                    + " null_value_treatment argument or ensure that an SQL NULL"
                                    + " is not passed.", "22004");
                        case "use_json_null":
                            return JsonOperations.jsonbSet(json.toString(), path, "null");
                        default:
                            // The treatment is one of four spellings and no other. It used to be
                            // lowercased and anything unrecognised silently read as use_json_null,
                            // so a misspelling wrote a JSON null where none was asked for.
                            throw badNullValueTreatment();
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
                Object arg1 = executor.evalExpr(fn.args().get(0), ctx);
                if (arg1 == null) return null;
                List<Object> keys;
                List<Object> values;
                keys = new ArrayList<>();
                values = new ArrayList<>();
                if (fn.args().size() >= 2) {
                    // Two-array form: jsonb_object(keys[], values[])
                    Object arg2 = executor.evalExpr(fn.args().get(1), ctx);
                    if (arg2 == null) return null;
                    keys = requireFlatArray(toList(arg1));
                    values = requireFlatArray(toList(arg2));
                    if (keys.size() != values.size()) {
                        throw new MemgresException("mismatched array dimensions", "2202E");
                    }
                } else {
                    List<Object> elems = toList(arg1);
                    int depth = arrayDepth(elems);
                    if (depth > 2) {
                        throw new MemgresException("wrong number of array subscripts", "2202E");
                    }
                    if (depth == 2) {
                        // An array of pairs: each row is one key beside its value.
                        for (Object row : elems) {
                            List<?> pair = (List<?>) row;
                            if (pair.size() != 2) {
                                throw new MemgresException("array must have two columns", "2202E");
                            }
                            keys.add(pair.get(0));
                            values.add(pair.get(1));
                        }
                    } else {
                        // Single flat array form: jsonb_object('{k1,v1,k2,v2,...}')
                        if (elems.size() % 2 != 0) {
                            throw new MemgresException(
                                    "array must have even number of elements", "2202E");
                        }
                        for (int i = 0; i < elems.size(); i += 2) {
                            keys.add(elems.get(i));
                            values.add(elems.get(i + 1));
                        }
                    }
                }
                // json keeps the spacing of its own text output, which puts spaces round the colon
                String colon = name.startsWith("jsonb") ? ": " : " : ";
                StringBuilder sb = new StringBuilder("{");
                for (int i = 0; i < keys.size(); i++) {
                    if (i > 0) sb.append(", ");
                    Object k = keys.get(i);
                    if (k == null) {
                        throw new MemgresException("null value not allowed for object key", "22004");
                    }
                    JsonWriter.appendString(sb, k.toString());
                    sb.append(colon);
                    Object v = values.get(i);
                    // Every value is text here, so a null is the one that is not a string at all
                    if (v == null) sb.append("null");
                    else JsonWriter.appendString(sb, v.toString());
                }
                sb.append("}");
                return name.startsWith("jsonb")
                        ? JsonOperations.normalizeJsonb(sb.toString()) : sb.toString();
            }
            case "json_populate_record":
            case "jsonb_populate_record": {
                // json_populate_record(base record, json) → record with fields filled from JSON
                requireArgs(fn, 2);
                requireConcreteRowType(fn, name);
                Object baseArg = executor.evalExpr(fn.args().get(0), ctx);
                Object jsonArg = executor.evalExpr(fn.args().get(1), ctx);
                if (jsonArg == null) return baseArg;
                return populatedRecord(fn, ctx, baseArg,
                        JsonRecordPopulator.objectMembers(jsonArg.toString()));
            }
            case "json_populate_recordset":
            case "jsonb_populate_recordset": {
                // json_populate_recordset(base record, json_array) → setof record
                requireArgs(fn, 2);
                Object baseArg = executor.evalExpr(fn.args().get(0), ctx);
                Object jsonArg = executor.evalExpr(fn.args().get(1), ctx);
                if (jsonArg == null) return new java.util.ArrayList<>();
                List<Object> results = new java.util.ArrayList<>();
                for (List<JsonParser.Member> members
                        : JsonRecordPopulator.objectsMembers(name, jsonArg.toString())) {
                    results.add(populatedRecord(fn, ctx, baseArg, members));
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
        String declaredType = executor.binaryOpEvaluator.declaredTypeForResolution(arg, ctx);
        if (declaredType == null) declaredType = documentTypeOf(arg, ctx);
        return shapedByType(value, declaredType);
    }

    /**
     * The name of the argument's type where it is a document type and the expression itself does
     * not declare one — a call answering json or jsonb, whose value is held as the text it prints
     * as. Only a document type is asked after: a document's text and a string's text are the same
     * characters, so nothing but the type tells the two apart, whereas every other value says what
     * it is. That is what makes {@code to_jsonb(array_agg(j))} an array of documents rather than an
     * array of the strings they print as.
     */
    private String documentTypeOf(Expression arg, RowContext ctx) {
        List<RowContext.TableBinding> bindings = ctx == null ? null : ctx.getBindings();
        if (bindings == null) return null;
        DataType type = executor.exprEvaluator.inferTypeFromContext(arg, bindings);
        if (type == null) return null;
        DataType element = DataType.isArrayType(type) ? DataType.elementOf(type) : type;
        return element == DataType.JSON || element == DataType.JSONB ? type.getPgName() : null;
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
        return json ? jsonIfText(value) : plainIfText(value);
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
            else converted.add(json ? jsonIfText(element) : plainIfText(element));
        }
        return converted;
    }

    /**
     * A value seen as the JSON its declared type stands for: an array as an array, and anything
     * else that is not itself JSON as the text it is. Guessing from the value alone read a text
     * of braces as an object, so a column holding {@code {1,2}} was written out unquoted.
     */
    private Object shapedByType(Object value, String declaredType) {
        if (declaredType == null) return value;
        DataType type = DataType.fromPgName(declaredType);
        if (declaredType.endsWith("[]") || (type != null && DataType.isArrayType(type))) {
            DataType element = type == null ? null : DataType.elementOf(type);
            // Each element is what the element type stands for. An array of documents holds
            // documents, and an array of text holds text however the text is spelled: elements
            // left as bare strings were guessed at from their first character, so an array of
            // text holding "[1,2]" was written out as an array of two numbers.
            return arrayElements(asArray(value), null,
                    element == DataType.JSON || element == DataType.JSONB);
        }
        return declaredType.equals("json") || declaredType.equals("jsonb")
                ? jsonIfText(value) : plainIfText(value);
    }

    private static Object plainIfText(Object value) {
        return value instanceof String ? new PlainText((String) value) : value;
    }

    private static Object jsonIfText(Object value) {
        return value instanceof String ? new JsonDocument((String) value) : value;
    }

    /**
     * Text that is a whole JSON document rather than a string of characters. A document is written
     * where it stands, and only its type says that it is one: the document {@code 1} and the text
     * {@code 1} are the same characters, so a json value collected into a larger one was decided by
     * whether it began with a brace — which made an object or an array itself and every other
     * document a quoted string, so {@code jsonb_agg} of the number 1 answered {@code ["1"]}.
     */
    static final class JsonDocument {
        final String text;

        JsonDocument(String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }
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
            JsonWriter.appendString(sb, ((PlainText) val).text);
            return;
        }
        if (val instanceof JsonDocument) {
            sb.append(((JsonDocument) val).text);
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
        } else if (val instanceof Boolean) {
            sb.append(val);
        } else if (val instanceof Number) {
            // The engine's own text is what PostgreSQL writes here: 1e20 and not 1.0E20. A number
            // that prints as a word is no JSON number, and goes in quoted rather than bare.
            String text = pgText(val);
            if (JsonParser.isNumberText(text)) sb.append(text);
            else JsonWriter.appendString(sb, text);
        } else if (isDateOrTime(val)) {
            JsonWriter.appendString(sb, dateOrTimeText(val));
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
                JsonWriter.appendString(sb, s);
            }
        }
    }

    /**
     * A value written as a JSON document, which is what to_json makes of it. JSON_SCALAR asks the
     * same question of a scalar, so both are answered here rather than each having its own writer:
     * the one that did not go through this said a timestamp was whatever Java called it.
     */
    String documentFor(Object val) {
        StringBuilder sb = new StringBuilder();
        appendJsonValue(sb, val);
        return sb.toString();
    }

    /** The text PostgreSQL prints this value as, which is what the engine's cast to text gives. */
    private String pgText(Object val) {
        Object text = executor.castEvaluator.applyCast(val, "text", false);
        return text == null ? null : text.toString();
    }

    /** The instants and the times of day, which JSON writes as strings of their own shape. */
    private static boolean isDateOrTime(Object val) {
        return val instanceof java.time.LocalDate || val instanceof java.time.LocalDateTime
                || val instanceof java.time.OffsetDateTime || val instanceof java.time.LocalTime
                || val instanceof java.time.OffsetTime;
    }

    /**
     * A date or time as JSON writes it, which is the text PostgreSQL prints with two changes made
     * to the instants: the date and the time of day are joined by a T rather than by a space, and
     * the zone is given in hours and minutes. A date on its own, and a time on its own, are written
     * exactly as they print. Java's own toString was none of these -- it dropped a zero second
     * altogether, wrote a fraction to three places whether or not it had them, and marked a year
     * before the common era by counting down through zero instead of saying BC.
     */
    private String dateOrTimeText(Object val) {
        String text = pgText(val);
        if (text == null) return null;
        if (!(val instanceof java.time.LocalDateTime || val instanceof java.time.OffsetDateTime)) {
            return text;
        }
        int space = text.indexOf(' ');
        // A BC year is spelled with a space before the era, so only the first one joins the two
        if (space >= 0) text = text.substring(0, space) + "T" + text.substring(space + 1);
        return val instanceof java.time.OffsetDateTime ? withMinutesInZone(text) : text;
    }

    /** The same instant with its zone written as PostgreSQL writes it here, in hours and minutes. */
    private static String withMinutesInZone(String text) {
        int sign = Math.max(text.lastIndexOf('+'), text.lastIndexOf('-'));
        // The sign of a zone can only stand after the time, never inside the date
        if (sign < 0 || sign < text.indexOf('T')) return text;
        return text.length() - sign == 3 ? text + ":00" : text;
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
            // A field name is a JSON string like any other: one holding a quote wrote a document
            // whose first key swallowed the rest of the row when it was read back.
            JsonWriter.appendString(sb, String.valueOf(entry.getKey()));
            sb.append(":");
            appendJsonValue(sb, entry.getValue());
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * The record a JSON object fills, laid over the record handed in. Which fields there are, and
     * what type each of them is, comes from the row type the first argument was written as: a
     * document only says what is in it, and reading the record off the document made a field of
     * every key it happened to carry, of whatever Java made of the text under it.
     */
    private Map<String, Object> populatedRecord(FunctionCallExpr fn, RowContext ctx, Object base,
            List<JsonParser.Member> members) {
        Map<String, Object> baseFields = base == null ? null : rowFields(base);
        String typeName = executor.compositeTypeHandler.resolveCompositeTypeName(
                fn.args().get(0), ctx);
        List<CreateTypeStmt.CompositeField> fields =
                typeName == null ? null : executor.database.getCompositeType(typeName);
        JsonRecordPopulator populator = new JsonRecordPopulator(executor);
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        if (fields != null) {
            for (CreateTypeStmt.CompositeField field : fields) {
                result.put(field.name(), populator.fieldValue(members, field.name(),
                        field.typeName(), baseFields == null ? null : baseFields.get(field.name())));
            }
            return result;
        }
        // A row type this database has no declaration of leaves the record being filled to say
        // what its fields are, and the document to say so when there is no record either
        Set<String> names = new java.util.LinkedHashSet<>();
        if (baseFields != null) names.addAll(baseFields.keySet());
        for (JsonParser.Member member : members) names.add(member.key);
        for (String fieldName : names) {
            result.put(fieldName, populator.fieldValue(members, fieldName, null,
                    baseFields == null ? null : baseFields.get(fieldName)));
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
        String name = FunctionEvaluator.stripSchemaPrefix(((FunctionCallExpr) expr).name().toLowerCase(java.util.Locale.ROOT));
        if (name.equals("json_each") || name.equals("jsonb_each")
                || name.equals("json_each_text") || name.equals("jsonb_each_text")) {
            return Cols.listOf("key", "value");
        }
        return null;
    }

    /**
     * The paths already read, so that a path written as a literal is read once for the query
     * rather than once for every row it is applied to.
     */
    private static final Map<String, JsonPath> PATH_CACHE =
            Collections.synchronizedMap(new LinkedHashMap<String, JsonPath>(64, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, JsonPath> eldest) {
                    return size() > 256;
                }
            });

    static JsonPath parsePath(String path) {
        JsonPath parsed = PATH_CACHE.get(path);
        if (parsed == null) {
            parsed = JsonPath.parse(path);
            PATH_CACHE.put(path, parsed);
        }
        return parsed;
    }

    /**
     * The object {@code $name} references are read out of. PostgreSQL takes it as a document
     * rather than as a list of bindings, so anything that is not an object has no names in it to
     * find and the call is refused rather than treated as having passed none.
     */
    static JsonValue jsonPathVars(String varsJson) {
        if (varsJson == null || varsJson.trim().isEmpty()) return null;
        JsonValue vars = JsonParser.parseJsonb(varsJson);
        if (!vars.isObject()) {
            throw new MemgresException("\"vars\" argument is not an object", "22023");
        }
        return vars;
    }

    /**
     * The PASSING clause of an SQL/JSON expression as the object {@code $name} references are read
     * out of. Each value becomes the document it stands for: one that is already json or jsonb is
     * that document, and anything else is the scalar to_jsonb would write for it. Substituting the
     * value's text into the path instead made {@code PASSING '1' AS x} the number one, and a value
     * holding a dollar sign or a quote rewrote the path around it.
     */
    JsonValue passingVars(Map<String, Expression> passing, RowContext ctx) {
        if (passing == null || passing.isEmpty()) return null;
        List<String> keys = new ArrayList<>();
        List<JsonValue> values = new ArrayList<>();
        for (Map.Entry<String, Expression> e : passing.entrySet()) {
            Object value = executor.evalExpr(e.getValue(), ctx);
            keys.add(e.getKey());
            values.add(asDocument(value, executor.exprEvaluator.inferExprType(e.getValue())));
        }
        return JsonValue.object(keys, values);
    }

    /** A SQL value as the document it stands for, which is what to_jsonb writes for it. */
    private JsonValue asDocument(Object value, DataType declared) {
        if (value == null) return JsonValue.JSON_NULL;
        if (declared == DataType.JSON || declared == DataType.JSONB) {
            return JsonParser.parseJsonb(TypeCoercion.toString(value));
        }
        StringBuilder sb = new StringBuilder();
        appendJsonValue(sb, value);
        return JsonParser.parseJsonb(sb.toString());
    }

    List<String> evaluateJsonPathAll(String json, String path) {
        return evaluateJsonPathAll(json, path, null, false, false);
    }

    List<String> evaluateJsonPathAll(String json, String path, String varsJson,
                                     boolean silent, boolean tz) {
        List<JsonValue> items;
        try {
            items = JsonPathEvaluator.query(JsonParser.parseJsonb(json), parsePath(path),
                    jsonPathVars(varsJson), tz);
        } catch (MemgresException e) {
            if (!silent || !isSuppressible(e)) throw e;
            return new ArrayList<>();
        }
        List<String> out = new ArrayList<>(items.size());
        for (JsonValue item : items) out.add(JsonWriter.jsonb(item));
        return out;
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

    /** The arguments of a jsonb_path_* call, evaluated and read once. */
    private static final class PathCall {
        final JsonValue document;
        final JsonPath path;
        final JsonValue vars;
        final boolean silent;
        /** Whether the caller was one of the {@code _tz} functions. */
        final boolean tz;

        PathCall(JsonValue document, JsonPath path, JsonValue vars, boolean silent, boolean tz) {
            this.document = document;
            this.path = path;
            this.vars = vars;
            this.silent = silent;
            this.tz = tz;
        }

        List<JsonValue> items() {
            return JsonPathEvaluator.query(document, path, vars, tz);
        }
    }

    /**
     * Evaluate the arguments and read both the document and the path.
     *
     * @return the call to make, or null when either the document or the path is NULL
     */
    private PathCall preparePath(FunctionCallExpr fn, RowContext ctx, boolean tz) {
        requireArgs(fn, 2);
        Object jsonVal = executor.evalExpr(fn.args().get(0), ctx);
        Object pathVal = executor.evalExpr(fn.args().get(1), ctx);
        if (jsonVal == null || pathVal == null) return null;
        // These functions are strict, so a NULL in any argument that was written -- including the
        // optional ones -- is answered with NULL rather than by taking the argument as absent.
        JsonValue vars = null;
        if (fn.args().size() > 2) {
            Object varsVal = executor.evalExpr(fn.args().get(2), ctx);
            if (varsVal == null) return null;
            vars = jsonPathVars(varsVal.toString());
        }
        boolean silent = false;
        if (fn.args().size() > 3) {
            Object silentVal = executor.evalExpr(fn.args().get(3), ctx);
            if (silentVal == null) return null;
            silent = executor.isTruthy(silentVal);
        }
        // The document argument is jsonb, so a literal written any other way is read as jsonb
        // first: what the path selects out of it is printed the way jsonb prints, not the way the
        // literal happened to be spaced.
        return new PathCall(JsonParser.parseJsonb(jsonVal.toString()),
                parsePath(pathVal.toString()), vars, silent, tz);
    }

    /** jsonb_path_query, whose items become the rows of a set-returning call. */
    private Object pathQuery(FunctionCallExpr fn, RowContext ctx, boolean tz) {
        PathCall call = preparePath(fn, ctx, tz);
        if (call == null) return new ArrayList<>();
        List<String> out = new ArrayList<>();
        try {
            for (JsonValue item : call.items()) out.add(JsonWriter.jsonb(item));
        } catch (MemgresException e) {
            if (!call.silent || !isSuppressible(e)) throw e;
            out.clear();
        }
        return out;
    }

    private Object pathQueryArray(FunctionCallExpr fn, RowContext ctx, boolean tz) {
        PathCall call = preparePath(fn, ctx, tz);
        if (call == null) return null;
        List<JsonValue> items;
        try {
            items = call.items();
        } catch (MemgresException e) {
            if (!call.silent || !isSuppressible(e)) throw e;
            items = new ArrayList<>();
        }
        return JsonWriter.jsonb(JsonValue.array(items));
    }

    private Object pathQueryFirst(FunctionCallExpr fn, RowContext ctx, boolean tz) {
        PathCall call = preparePath(fn, ctx, tz);
        if (call == null) return null;
        try {
            List<JsonValue> items = call.items();
            return items.isEmpty() ? null : JsonWriter.jsonb(items.get(0));
        } catch (MemgresException e) {
            if (!call.silent || !isSuppressible(e)) throw e;
            return null;
        }
    }

    private Object pathExists(FunctionCallExpr fn, RowContext ctx, boolean tz) {
        PathCall call = preparePath(fn, ctx, tz);
        if (call == null) return null;
        try {
            return !call.items().isEmpty();
        } catch (MemgresException e) {
            if (!call.silent || !isSuppressible(e)) throw e;
            return null;
        }
    }

    private Object pathMatch(FunctionCallExpr fn, RowContext ctx, boolean tz) {
        PathCall call = preparePath(fn, ctx, tz);
        if (call == null) return null;
        try {
            return single(call.items());
        } catch (MemgresException e) {
            if (!call.silent || !isSuppressible(e)) throw e;
            return null;
        }
    }

    /**
     * jsonb_path_match: the path has to produce exactly one boolean. A path that produces a
     * number, nothing at all, or more than one item is not a mistake PG guesses its way past --
     * a caller asking "does this match" gets told the question had no single answer. A predicate
     * that came out unknown is the one shape that is not an error: unknown is what NULL means.
     */
    private static Boolean single(List<JsonValue> items) {
        if (items.size() == 1) {
            JsonValue only = items.get(0);
            if (only.isNull()) return null;
            if (only.kind() == JsonValue.BOOLEAN) return only.asBoolean();
        }
        throw new MemgresException("single boolean result is expected", "22038");
    }

    /** The @@ operator, which is jsonb_path_match without the arguments it never takes. */
    Object evalPathMatch(String json, String path) {
        return single(JsonPathEvaluator.query(JsonParser.parseJsonb(json), parsePath(path),
                null, false));
    }

    /**
     * A whole path made of a predicate always produces one item -- true, false or unknown -- so
     * it always exists. Only a path that selects items can select none; that is the case @? and
     * jsonb_path_exists are asked about.
     */
    boolean evaluateJsonPathExists(String json, String path) {
        return !JsonPathEvaluator.query(JsonParser.parseJsonb(json), parsePath(path), null, false)
                .isEmpty();
    }

    /** Refuses a null_value_treatment that is not one of the four jsonb_set_lax knows. */
    private static MemgresException badNullValueTreatment() {
        return new MemgresException("null_value_treatment must be \"delete_key\", "
                + "\"return_target\", \"use_json_null\", or \"raise_exception\"", "22023");
    }

    /** The path argument of a function that edits a document, which no element of may be null. */
    private List<String> parsePathArg(Object pathArg) {
        List<String> path = JsonOperations.parsePathArray(pathArg);
        JsonOperations.requireNoNullPathElement(path);
        return path;
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

    /**
     * The elements of a text array argument.
     *
     * <p>This used to be a split on commas with the quotes taken off either end, which reads only
     * the arrays that would need no quoting: an element written {@code "a,b"} was torn in two, and
     * one written {@code "a\"b"} kept the backslash that was there to let the quote through.
     */
    @SuppressWarnings("unchecked")
    private List<Object> toList(Object obj) {
        if (obj instanceof List<?>) return (List<Object>) obj;
        String s = obj.toString().trim();
        if (s.startsWith("{")) return FunctionEvaluator.parseSimplePgArray(s);
        List<Object> single = new ArrayList<>();
        single.add(obj);
        return single;
    }

    /** How many subscripts an array takes, an array literal being nested lists once parsed. */
    private static int arrayDepth(List<?> array) {
        return !array.isEmpty() && array.get(0) instanceof List<?>
                ? 1 + arrayDepth((List<?>) array.get(0)) : 1;
    }

    /** An array that must be a list of values rather than a list of arrays. */
    private static List<Object> requireFlatArray(List<Object> array) {
        if (arrayDepth(array) > 1) {
            throw new MemgresException("wrong number of array subscripts", "2202E");
        }
        return array;
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
