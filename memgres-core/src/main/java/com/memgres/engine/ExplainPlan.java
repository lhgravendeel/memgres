package com.memgres.engine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A node of the tree EXPLAIN prints, and the four shapes PostgreSQL prints it in.
 *
 * <p>EXPLAIN used to answer with a single flat line — "Memgres in-memory operation" for anything
 * that was not a plain SELECT, INSERT, UPDATE or DELETE — and the JSON, XML and YAML forms carried
 * that one line as their only key. A plan is a tree: a sort sits above its scan, an aggregate above
 * the sort, a CTE hangs beside the query that reads it. Nothing that reads EXPLAIN output can learn
 * anything from a flat line, and a client that asks for FORMAT JSON gets a document with the wrong
 * shape rather than a plan it can walk.
 *
 * <p>The node names, the key names and the layout are PostgreSQL's. Indentation follows its own
 * rule exactly: a counter in units of two spaces, where a named sub-plan label costs one unit, a
 * child's arrow costs two, and a node's own detail lines sit one unit past its name.
 */
final class ExplainPlan {

    /** The node name PostgreSQL prints, e.g. {@code Seq Scan} or {@code HashAggregate}. */
    private final String nodeType;
    /** The relation a scan node reads, if it reads one. */
    private String relationName;
    /** The name the query gave that relation, when it differs from the relation's own. */
    private String alias;
    /** Detail lines, in the order PostgreSQL prints them: {@code Sort Key}, {@code Filter}, … */
    private final List<String[]> details = new ArrayList<String[]>();
    /** Children, printed under an arrow. */
    private final List<ExplainPlan> children = new ArrayList<ExplainPlan>();
    /** Sub-plans printed under a label of their own, as a CTE is. */
    private final List<ExplainPlan> subPlans = new ArrayList<ExplainPlan>();
    private final List<String> subPlanLabels = new ArrayList<String>();
    /** What this node is to its parent: {@code Outer}, {@code Inner}, {@code Member}. */
    private String parentRelationship;

    ExplainPlan(String nodeType) {
        this.nodeType = nodeType;
    }

    String nodeType() { return nodeType; }

    ExplainPlan on(String relation, String aliasName) {
        this.relationName = relation;
        this.alias = aliasName;
        return this;
    }

    /** Add a detail line; {@code key} is the label PostgreSQL prints before the colon. */
    ExplainPlan detail(String key, String value) {
        if (value != null) details.add(new String[]{key, value});
        return this;
    }

    ExplainPlan child(ExplainPlan node) {
        if (node != null) children.add(node);
        return this;
    }

    ExplainPlan child(ExplainPlan node, String relationship) {
        if (node != null) {
            node.parentRelationship = relationship;
            children.add(node);
        }
        return this;
    }

    ExplainPlan subPlan(String label, ExplainPlan node) {
        if (node != null) {
            subPlanLabels.add(label);
            subPlans.add(node);
        }
        return this;
    }

    List<ExplainPlan> children() { return children; }

    /** The name as it is printed: the node type, then the relation it reads and the alias for it. */
    private String printedName() {
        StringBuilder sb = new StringBuilder(nodeType);
        if (relationName != null) {
            sb.append(" on ").append(relationName);
            if (alias != null && !alias.equals(relationName)) sb.append(' ').append(alias);
        }
        return sb.toString();
    }

    // ---- text ----

    /**
     * Render as PostgreSQL's text form.
     *
     * <p>{@code indent} counts two-space units, following PostgreSQL's own counter: a sub-plan
     * label is printed at the current level and costs one unit; a child's {@code ->  } arrow is
     * printed at the current level and costs two; the node's details sit one unit further in.
     */
    void renderText(List<String> out, int indent, String suffix) {
        StringBuilder line = new StringBuilder();
        int nodeIndent = indent;
        if (indent > 0) {
            line.append(spaces(indent * 2)).append("->  ");
            nodeIndent = indent + 2;
        }
        line.append(printedName());
        if (suffix != null) line.append(suffix);
        out.add(line.toString());
        int detailIndent = nodeIndent + 1;
        for (String[] d : details) {
            out.add(spaces(detailIndent * 2) + d[0] + ": " + d[1]);
        }
        for (int i = 0; i < subPlans.size(); i++) {
            out.add(spaces(detailIndent * 2) + subPlanLabels.get(i));
            subPlans.get(i).renderText(out, detailIndent + 1, null);
        }
        for (ExplainPlan c : children) {
            c.renderText(out, detailIndent, null);
        }
    }

    private static String spaces(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) sb.append(' ');
        return sb.toString();
    }

    // ---- structured forms ----

    /**
     * The node as an ordered map, in the key order PostgreSQL emits. The three structured formats
     * all render this same map, so they cannot drift apart from each other.
     */
    private Map<String, Object> toMap(boolean costs) {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("Node Type", nodeType);
        if (parentRelationship != null) m.put("Parent Relationship", parentRelationship);
        m.put("Parallel Aware", Boolean.FALSE);
        m.put("Async Capable", Boolean.FALSE);
        if (relationName != null) {
            m.put("Relation Name", relationName);
            m.put("Alias", alias != null ? alias : relationName);
        }
        if (costs) {
            m.put("Startup Cost", "0.00");
            m.put("Total Cost", "0.01");
            m.put("Plan Rows", Integer.valueOf(1));
            m.put("Plan Width", Integer.valueOf(4));
        }
        m.put("Disabled", Boolean.FALSE);
        for (String[] d : details) m.put(d[0], d[1]);
        List<Map<String, Object>> kids = new ArrayList<Map<String, Object>>();
        for (int i = 0; i < subPlans.size(); i++) {
            Map<String, Object> sub = subPlans.get(i).toMap(costs);
            sub.put("Subplan Name", subPlanLabels.get(i));
            kids.add(sub);
        }
        for (ExplainPlan c : children) kids.add(c.toMap(costs));
        if (!kids.isEmpty()) m.put("Plans", kids);
        return m;
    }

    String renderJson(boolean costs) {
        StringBuilder sb = new StringBuilder("[\n  {\n    \"Plan\": ");
        writeJson(sb, toMap(costs), 4);
        sb.append("\n  }\n]");
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static void writeJson(StringBuilder sb, Object value, int indent) {
        String pad = spaces(indent);
        if (value instanceof Map) {
            Map<String, Object> m = (Map<String, Object>) value;
            sb.append("{\n");
            int n = 0;
            for (Map.Entry<String, Object> e : m.entrySet()) {
                if (n++ > 0) sb.append(",\n");
                sb.append(pad).append("  \"").append(e.getKey()).append("\": ");
                writeJson(sb, e.getValue(), indent + 2);
            }
            sb.append('\n').append(pad).append('}');
        } else if (value instanceof List) {
            List<Object> l = (List<Object>) value;
            sb.append("[\n");
            for (int i = 0; i < l.size(); i++) {
                if (i > 0) sb.append(",\n");
                sb.append(pad).append("  ");
                writeJson(sb, l.get(i), indent + 2);
            }
            sb.append('\n').append(pad).append(']');
        } else if (value instanceof Boolean || value instanceof Integer) {
            sb.append(value);
        } else if (value instanceof String && isBareNumber((String) value)) {
            sb.append(value);
        } else {
            sb.append('"').append(jsonEscape(String.valueOf(value))).append('"');
        }
    }

    private static boolean isBareNumber(String s) {
        if (s.isEmpty()) return false;
        boolean dot = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '.') { if (dot) return false; dot = true; }
            else if (c < '0' || c > '9') return false;
        }
        return dot;
    }

    private static String jsonEscape(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') sb.append('\\').append(c);
            else if (c == '\n') sb.append("\\n");
            else sb.append(c);
        }
        return sb.toString();
    }

    String renderXml(boolean costs) {
        StringBuilder sb = new StringBuilder(
                "<explain xmlns=\"http://www.postgresql.org/2009/explain\">\n  <Query>\n");
        writeXml(sb, "Plan", toMap(costs), 4);
        sb.append("  </Query>\n</explain>");
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static void writeXml(StringBuilder sb, String name, Object value, int indent) {
        String pad = spaces(indent);
        String tag = xmlTag(name);
        if (value instanceof Map) {
            sb.append(pad).append('<').append(tag).append(">\n");
            for (Map.Entry<String, Object> e : ((Map<String, Object>) value).entrySet()) {
                writeXml(sb, e.getKey(), e.getValue(), indent + 2);
            }
            sb.append(pad).append("</").append(tag).append(">\n");
        } else if (value instanceof List) {
            sb.append(pad).append("<Plans>\n");
            for (Object o : (List<Object>) value) writeXml(sb, "Plan", o, indent + 2);
            sb.append(pad).append("</Plans>\n");
        } else {
            sb.append(pad).append('<').append(tag).append('>').append(xmlEscape(String.valueOf(value)))
                    .append("</").append(tag).append(">\n");
        }
    }

    /** PostgreSQL writes XML element names with hyphens where the key has spaces. */
    private static String xmlTag(String key) {
        return key.replace(' ', '-');
    }

    private static String xmlEscape(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    String renderYaml(boolean costs) {
        StringBuilder sb = new StringBuilder("- Plan: \n");
        writeYaml(sb, toMap(costs), 4);
        // PostgreSQL's YAML ends with the last property line and no trailing newline
        while (sb.length() > 0 && sb.charAt(sb.length() - 1) == '\n') sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static void writeYaml(StringBuilder sb, Map<String, Object> map, int indent) {
        String pad = spaces(indent);
        for (Map.Entry<String, Object> e : map.entrySet()) {
            Object v = e.getValue();
            if (v instanceof List) {
                sb.append(pad).append(e.getKey()).append(": \n");
                for (Object o : (List<Object>) v) {
                    sb.append(spaces(indent + 2)).append("- ");
                    Map<String, Object> m = (Map<String, Object>) o;
                    int n = 0;
                    for (Map.Entry<String, Object> me : m.entrySet()) {
                        if (n++ > 0) sb.append(spaces(indent + 4));
                        writeYamlScalar(sb, me.getKey(), me.getValue(), indent + 4);
                    }
                }
            } else {
                sb.append(pad);
                writeYamlScalar(sb, e.getKey(), v, indent);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void writeYamlScalar(StringBuilder sb, String key, Object v, int indent) {
        if (v instanceof List) {
            sb.append(key).append(": \n");
            for (Object o : (List<Object>) v) {
                sb.append(spaces(indent + 2)).append("- ");
                Map<String, Object> m = (Map<String, Object>) o;
                int n = 0;
                for (Map.Entry<String, Object> me : m.entrySet()) {
                    if (n++ > 0) sb.append(spaces(indent + 4));
                    writeYamlScalar(sb, me.getKey(), me.getValue(), indent + 4);
                }
            }
        } else if (v instanceof Boolean || v instanceof Integer) {
            sb.append(key).append(": ").append(v).append('\n');
        } else if (v instanceof String && isBareNumber((String) v)) {
            sb.append(key).append(": ").append(v).append('\n');
        } else {
            sb.append(key).append(": \"").append(String.valueOf(v)).append("\"\n");
        }
    }
}
