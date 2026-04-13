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

    private void requireArgs(FunctionCallExpr fn, int min) {
        if (fn.args().size() < min) {
            throw new MemgresException(
                "function " + fn.name() + "() does not exist" +
                (fn.args().isEmpty() ? "" : "\n  Hint: No function matches the given name and argument types."), "42883");
        }
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "jsonb_build_object":
            case "json_build_object": {
                StringBuilder sb = new StringBuilder("{");
                for (int i = 0; i < fn.args().size(); i += 2) {
                    if (i > 0) sb.append(", ");
                    Object key = executor.evalExpr(fn.args().get(i), ctx);
                    Object val = (i + 1 < fn.args().size()) ? executor.evalExpr(fn.args().get(i + 1), ctx) : null;
                    sb.append("\"").append(key).append("\": ");
                    appendJsonValue(sb, val);
                }
                sb.append("}");
                return sb.toString();
            }
            case "jsonb_build_array":
            case "json_build_array": {
                StringBuilder sb = new StringBuilder("[");
                for (int i = 0; i < fn.args().size(); i++) {
                    if (i > 0) sb.append(", ");
                    Object val = executor.evalExpr(fn.args().get(i), ctx);
                    appendJsonValue(sb, val);
                }
                sb.append("]");
                return sb.toString();
            }
            case "to_json":
            case "to_jsonb": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return "null";
                if (arg instanceof Number || arg instanceof Boolean) return arg.toString();
                String s = arg.toString();
                if ((s.startsWith("{") && s.endsWith("}")) || (s.startsWith("[") && s.endsWith("]"))) return s;
                return "\"" + arg.toString().replace("\"", "\\\"") + "\"";
            }
            case "row_to_json": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg == null) return "null";
                if (arg instanceof java.util.Map<?, ?>) {
                    java.util.Map<?, ?> map = (java.util.Map<?, ?>) arg;
                    // Convert row record (LinkedHashMap) to JSON object
                    StringBuilder sb = new StringBuilder("{");
                    boolean first = true;
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        if (!first) sb.append(",");
                        first = false;
                        sb.append("\"").append(entry.getKey()).append("\":");
                        Object v = entry.getValue();
                        if (v == null) {
                            sb.append("null");
                        } else if (v instanceof Number || v instanceof Boolean) {
                            sb.append(v);
                        } else {
                            String s = v.toString();
                            // Check if it's already JSON
                            if ((s.startsWith("{") && s.endsWith("}")) || (s.startsWith("[") && s.endsWith("]"))) {
                                sb.append(s);
                            } else {
                                sb.append("\"").append(s.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
                            }
                        }
                    }
                    sb.append("}");
                    return sb.toString();
                }
                if (arg instanceof java.util.List<?>) {
                    java.util.List<?> list = (java.util.List<?>) arg;
                    // ROW(...) value, convert to JSON
                    StringBuilder sb = new StringBuilder("{");
                    for (int i = 0; i < list.size(); i++) {
                        if (i > 0) sb.append(",");
                        sb.append("\"f").append(i + 1).append("\":");
                        Object v = list.get(i);
                        if (v == null) sb.append("null");
                        else if (v instanceof Number || v instanceof Boolean) sb.append(v);
                        else sb.append("\"").append(v.toString().replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
                    }
                    sb.append("}");
                    return sb.toString();
                }
                return arg.toString();
            }
            case "jsonb_path_query": {
                requireArgs(fn, 2);
                Object jsonVal = executor.evalExpr(fn.args().get(0), ctx);
                Object pathVal = executor.evalExpr(fn.args().get(1), ctx);
                if (jsonVal == null || pathVal == null) return new ArrayList<>();
                String path = pathVal.toString().trim();
                if (!path.startsWith("$") && !path.startsWith("@")) {
                    throw new MemgresException("syntax error at or near \"" + path.substring(0, Math.min(3, path.length())) + "\" of jsonpath input", "42601");
                }
                List<String> results = evaluateJsonPathAll(jsonVal.toString(), path);
                return new ArrayList<>(results); // Return as List for SRF expansion
            }
            case "jsonb_path_query_first": {
                requireArgs(fn, 2);
                Object jsonVal = executor.evalExpr(fn.args().get(0), ctx);
                Object pathVal = executor.evalExpr(fn.args().get(1), ctx);
                if (jsonVal == null || pathVal == null) return null;
                String path = pathVal.toString().trim();
                if (!path.startsWith("$") && !path.startsWith("@")) {
                    throw new MemgresException("syntax error at or near \"" + path.substring(0, Math.min(3, path.length())) + "\" of jsonpath input", "42601");
                }
                List<String> results = evaluateJsonPathAll(jsonVal.toString(), path);
                return results.isEmpty() ? null : results.get(0);
            }
            case "jsonb_path_exists": {
                requireArgs(fn, 2);
                Object jsonVal = executor.evalExpr(fn.args().get(0), ctx);
                Object pathVal = executor.evalExpr(fn.args().get(1), ctx);
                if (jsonVal == null || pathVal == null) return null;
                String path = pathVal.toString().trim();
                // Basic jsonpath validation: must start with $
                if (!path.startsWith("$") && !path.startsWith("@")) {
                    throw new MemgresException("syntax error at or near \"" + path.substring(0, Math.min(3, path.length())) + "\" of jsonpath input", "42601");
                }
                return evaluateJsonPathExists(jsonVal.toString(), path);
            }
            case "jsonb_path_match": {
                requireArgs(fn, 2);
                Object jsonVal = executor.evalExpr(fn.args().get(0), ctx);
                Object pathVal = executor.evalExpr(fn.args().get(1), ctx);
                if (jsonVal == null || pathVal == null) return null;
                String path = pathVal.toString().trim();
                // Handle exists(...) syntax
                if (path.startsWith("exists(") && path.endsWith(")")) {
                    String innerPath = path.substring(7, path.length() - 1).trim();
                    boolean existsResult = evaluateJsonPathExists(jsonVal.toString(), innerPath);
                    return existsResult;
                }
                // Handle predicate expressions like $.a == 2
                // Check if the path contains a comparison operator at the top level
                String[] compOps = {"==", "!=", ">=", "<=", ">", "<"};
                boolean isPredicate = false;
                for (String op : compOps) {
                    if (path.contains(op)) { isPredicate = true; break; }
                }
                if (isPredicate) {
                    Boolean predResult = evaluateJsonPathPredicate(jsonVal.toString(), path);
                    return predResult;
                }
                if (!path.startsWith("$") && !path.startsWith("@")) {
                    throw new MemgresException("syntax error at or near \"" + path.substring(0, Math.min(3, path.length())) + "\" of jsonpath input", "42601");
                }
                Object result = evaluateJsonPath(jsonVal.toString(), path);
                if (result == null) return null;
                if (result instanceof Boolean) return ((Boolean) result);
                String rs = result.toString().trim();
                if (rs.equals("true")) return true;
                if (rs.equals("false")) return false;
                // jsonb_path_match result must be boolean; SQLSTATE 22038
                throw new MemgresException("jsonpath item method .boolean() can only be applied to a boolean, numeric, or string JSON item", "22038");
            }
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
                if (s.startsWith("[")) {
                    // Count elements by commas (simplified)
                    if (s.equals("[]")) return 0;
                    int depth = 0, count = 1;
                    for (char c : s.toCharArray()) {
                        if (c == '[' || c == '{') depth++;
                        else if (c == ']' || c == '}') depth--;
                        else if (c == ',' && depth == 1) count++;
                    }
                    return count;
                }
                return 0;
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
                if (json == null) return null;
                String s = json.toString().trim();
                if (s.startsWith("[")) {
                    List<Object> elements = new ArrayList<>();
                    // Simple JSON array parsing
                    String inner = s.substring(1, s.length() - 1).trim();
                    if (!inner.isEmpty()) {
                        // Split on commas at top level
                        int depth = 0;
                        int start = 0;
                        for (int ci = 0; ci <= inner.length(); ci++) {
                            if (ci == inner.length() || (inner.charAt(ci) == ',' && depth == 0)) {
                                String elem = inner.substring(start, ci).trim();
                                if (name.endsWith("_text") && elem.startsWith("\"") && elem.endsWith("\"")) {
                                    elem = elem.substring(1, elem.length() - 1);
                                }
                                elements.add(elem);
                                start = ci + 1;
                            } else if (inner.charAt(ci) == '{' || inner.charAt(ci) == '[') depth++;
                            else if (inner.charAt(ci) == '}' || inner.charAt(ci) == ']') depth--;
                        }
                    }
                    return elements; // Return as List for SRF expansion
                }
                return null;
            }
            case "jsonb_object_keys":
            case "json_object_keys": {
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                if (json == null) return null;
                String s = json.toString().trim();
                if (s.startsWith("{")) {
                    Map<String, String> map = JsonOperations.parseObjectKeys(s);
                    return new ArrayList<>(map.keySet());
                }
                return null;
            }
            case "jsonb_set": {
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                if (json == null) return null;
                Object pathArg = executor.evalExpr(fn.args().get(1), ctx);
                // Validate path is text[] format
                if (pathArg instanceof String && !(pathArg instanceof java.util.List) && !((String) pathArg).trim().startsWith("{")) {
                    String ps = (String) pathArg;
                    throw new MemgresException("malformed array literal: \"" + ps + "\"", "22P02");
                }
                Object newVal = executor.evalExpr(fn.args().get(2), ctx);
                List<String> path = parsePathArg(pathArg);
                String newValStr = jsonValueStr(newVal);
                return JsonOperations.jsonbSet(json.toString(), path, newValStr);
            }
            case "jsonb_strip_nulls": {
                Object json = executor.evalExpr(fn.args().get(0), ctx);
                return json == null ? null : JsonOperations.stripNulls(json.toString());
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
            default:
                return NOT_HANDLED;
        }
    }

    private void appendJsonValue(StringBuilder sb, Object val) {
        if (val == null) {
            sb.append("null");
        } else if (val instanceof Number || val instanceof Boolean) {
            sb.append(val);
        } else if (val instanceof List<?>) {
            List<?> list = (List<?>) val;
            sb.append("[");
            for (int j = 0; j < list.size(); j++) {
                if (j > 0) sb.append(", ");
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

    String extractJsonKey(String json, String key) {
        return JsonOperations.extractKey(json, key);
    }

    List<String> evaluateJsonPathAll(String json, String path) {
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
                int start = i;
                while (i < rest.length() && rest.charAt(i) != '.' && rest.charAt(i) != '[') i++;
                String key = rest.substring(start, i);
                if (!key.isEmpty()) {
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
                    } else {
                        List<String> next = new ArrayList<>();
                        for (String node : current) {
                            String extracted = JsonOperations.extractKey(node, key);
                            if (extracted != null) next.add(extracted.trim());
                        }
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
                        }
                    }
                    current = next;
                } else {
                    try {
                        int idx = Integer.parseInt(idxStr);
                        List<String> next = new ArrayList<>();
                        for (String node : current) {
                            node = node.trim();
                            if (node.startsWith("[")) {
                                String elem = JsonOperations.extractArrayElement(node, idx);
                                if (elem != null) next.add(elem.trim());
                            }
                        }
                        current = next;
                    } catch (NumberFormatException e) { /* skip */ }
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
                List<String> r = evaluateJsonPathAll(node, "$" + postFilterPath);
                postResults.addAll(r);
            }
            current = postResults;
        }

        return current;
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

    Boolean evaluateJsonPathPredicate(String json, String path) {
        String[] ops = {"==", "!=", ">=", "<=", ">", "<"};
        for (String op : ops) {
            int depth = 0;
            for (int i = 0; i < path.length() - op.length() + 1; i++) {
                char c = path.charAt(i);
                if (c == '(' || c == '[' || c == '{') depth++;
                else if (c == ')' || c == ']' || c == '}') depth--;
                else if (depth == 0 && path.startsWith(op, i)) {
                    String leftExpr = path.substring(0, i).trim();
                    String rightExpr = path.substring(i + op.length()).trim();
                    List<String> leftResults = evaluateJsonPathAll(json, leftExpr);
                    if (leftResults.isEmpty()) return null;
                    String leftVal = leftResults.get(0).trim();
                    String rightVal = rightExpr.trim();
                    if (rightVal.startsWith("\"") && rightVal.endsWith("\"")) {
                        rightVal = rightVal.substring(1, rightVal.length() - 1);
                        if (leftVal.startsWith("\"") && leftVal.endsWith("\"")) {
                            leftVal = leftVal.substring(1, leftVal.length() - 1);
                        }
                        int cmp = leftVal.compareTo(rightVal);
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
                    try {
                        double leftNum = Double.parseDouble(leftVal);
                        double rightNum = Double.parseDouble(rightVal);
                        switch (op) {
                            case "==":
                                return leftNum == rightNum;
                            case "!=":
                                return leftNum != rightNum;
                            case ">":
                                return leftNum > rightNum;
                            case "<":
                                return leftNum < rightNum;
                            case ">=":
                                return leftNum >= rightNum;
                            case "<=":
                                return leftNum <= rightNum;
                            default:
                                return false;
                        }
                    } catch (NumberFormatException e) {
                        int cmp = leftVal.compareTo(rightVal);
                        switch (op) {
                            case "==":
                                return cmp == 0;
                            case "!=":
                                return cmp != 0;
                            default:
                                return false;
                        }
                    }
                }
            }
        }
        return false;
    }

    boolean evaluateJsonPathExists(String json, String path) {
        if (path.equals("$")) return true;
        List<String> results = evaluateJsonPathAll(json, path);
        return !results.isEmpty();
    }

    private Object evaluateJsonPath(String json, String path) {
        if (path.equals("$")) return json;
        List<String> results = evaluateJsonPathAll(json, path);
        if (results.isEmpty()) return null;
        String r = results.get(0).trim();
        if (r.equals("true")) return true;
        if (r.equals("false")) return false;
        return r;
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
}
