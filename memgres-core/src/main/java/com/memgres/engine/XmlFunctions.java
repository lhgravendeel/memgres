package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import java.util.*;

/**
 * XML function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class XmlFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    private final AstExecutor executor;

    XmlFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "xmlparse": {
                String mode = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                String text = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                return XmlOperations.xmlparse(text, "document".equalsIgnoreCase(mode));
            }
            case "xmlserialize": {
                // args: mode, xml_value, target_type
                // The result is text, and what may be written here is a type text reaches: a
                // width belongs to the type and is applied to the text, so varchar(5) is the same
                // question as any other varchar(5) and answers with the same overflow.
                String targetSpec = null;
                if (fn.args().size() >= 3) {
                    targetSpec = String.valueOf(executor.evalExpr(fn.args().get(2), ctx)).trim();
                    String targetBase = targetSpec.replaceAll("\\(.*\\)", "").trim()
                            .toLowerCase(java.util.Locale.ROOT);
                    java.util.Set<String> validTypes = Cols.setOf("text", "varchar", "character varying", "xml", "char", "character", "bpchar", "name", "bytea");
                    if (!validTypes.contains(targetBase)) {
                        throw new MemgresException("cannot cast XMLSERIALIZE result to "
                                + DataType.canonicalName(targetBase), "42846");
                    }
                }
                String mode = fn.args().size() >= 1 ? String.valueOf(executor.evalExpr(fn.args().get(0), ctx)) : "content";
                Object xmlVal = executor.evalExpr(fn.args().get(1), ctx);
                if (xmlVal != null && "document".equalsIgnoreCase(mode)) {
                    // DOCUMENT mode requires a single root element
                    if (!XmlOperations.isDocument(xmlVal.toString())) {
                        throw new MemgresException("not an XML document", "2200L");
                    }
                }
                boolean indent = fn.args().size() >= 4 && "indent".equals(String.valueOf(executor.evalExpr(fn.args().get(3), ctx)));
                if (xmlVal == null) return null;
                String serialized = indent
                        ? XmlOperations.xmlserializeIndent(xmlVal.toString())
                        : XmlOperations.xmlserialize(xmlVal.toString());
                // The type written is the type the text is read as, width and all.
                // Read as the type rather than cast to it: a value the type cannot hold is
                // refused, not shortened until it fits.
                return targetSpec == null || targetSpec.isEmpty() ? serialized
                        : TypeCoercion.heldToItsType(serialized, targetSpec);
            }
            case "xmlelement": {
                String tagName = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                Map<String, String> attributes = null;
                List<String> contents = new ArrayList<>();
                for (int i = 1; i < fn.args().size(); i++) {
                    Expression argExpr = fn.args().get(i);
                    if (argExpr instanceof FunctionCallExpr && ((FunctionCallExpr) argExpr).name().equals("__xmlattributes__")) {
                        FunctionCallExpr fc = (FunctionCallExpr) argExpr;
                        attributes = new LinkedHashMap<>();
                        for (int j = 0; j < fc.args().size(); j += 2) {
                            Object val = executor.evalExpr(fc.args().get(j), ctx);
                            String attrName = String.valueOf(executor.evalExpr(fc.args().get(j + 1), ctx));
                            // An element cannot carry the same attribute twice, so the second
                            // one is the caller's mistake rather than a value to overwrite with.
                            if (attributes.containsKey(attrName)) {
                                throw new MemgresException("XML attribute name \"" + attrName
                                        + "\" appears more than once", "42601");
                            }
                            if (val != null) attributes.put(attrName, val.toString());
                        }
                    } else {
                        Object val = executor.evalExpr(argExpr, ctx);
                        if (val == null && argExpr instanceof Literal && ((Literal) argExpr).literalType() == Literal.LiteralType.NULL) {
                            // Skip null placeholder for attributes
                            continue;
                        }
                        // XML-producing expressions: pass through without escaping (PG behavior)
                        if (val != null && isXmlExpr(argExpr)) {
                            contents.add(val.toString());
                        } else if (val instanceof java.util.List<?>) {
                            // An array becomes one <element> per element, which is how PG's
                            // array-to-xml mapping writes it.
                            for (Object element : PgArray.flatten((java.util.List<?>) val)) {
                                contents.add("<element>" + (element == null ? ""
                                        : XmlOperations.escapeXml(TypeCoercion.toString(element)))
                                        + "</element>");
                            }
                        } else {
                            contents.add(val == null ? null
                                    : XmlOperations.escapeXml(TypeCoercion.toString(val)));
                        }
                    }
                }
                return XmlOperations.xmlelement(tagName, attributes, contents);
            }
            case "xmlforest": {
                List<String> names = new ArrayList<>();
                List<Object> values = new ArrayList<>();
                List<Boolean> alreadyXml = new ArrayList<>();
                for (int i = 0; i < fn.args().size(); i += 2) {
                    Object val = executor.evalExpr(fn.args().get(i), ctx);
                    String elemName = String.valueOf(executor.evalExpr(fn.args().get(i + 1), ctx));
                    names.add(elemName);
                    values.add(val);
                    // An xml value is markup already, and xmlelement has always known that;
                    // xmlforest escaped it, so a document came out as the characters spelling it.
                    alreadyXml.add(Boolean.valueOf(isXmlExpr(fn.args().get(i))));
                }
                return XmlOperations.xmlforest(names, values, alreadyXml);
            }
            case "xmlpi": {
                String target = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                String content = fn.args().size() > 1 ? String.valueOf(executor.evalExpr(fn.args().get(1), ctx)) : null;
                if ("null".equals(content)) content = null;
                return XmlOperations.xmlpi(target, content);
            }
            case "xmlroot": {
                String xmlVal = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                Object versionObj = executor.evalExpr(fn.args().get(1), ctx);
                Object standaloneObj = fn.args().size() > 2 ? executor.evalExpr(fn.args().get(2), ctx) : null;
                String version = versionObj != null ? versionObj.toString() : null;
                String standalone = standaloneObj != null ? standaloneObj.toString() : null;
                return XmlOperations.xmlroot(xmlVal, version, standalone);
            }
            case "xmlconcat": {
                List<String> xmlValues = new ArrayList<>();
                for (Expression arg : fn.args()) {
                    Object val = executor.evalExpr(arg, ctx);
                    xmlValues.add(val == null ? null : val.toString());
                }
                return XmlOperations.xmlconcat(xmlValues);
            }
            case "xmlexists": {
                String xpathExpr = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                String xml = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                return XmlOperations.xmlexists(xpathExpr, xml);
            }
            case "xmlagg": {
                // xmlagg is an aggregate, handled via aggregation, but if called on single value:
                Object val = executor.evalExpr(fn.args().get(0), ctx);
                return val == null ? null : val.toString();
            }
            case "xmltext": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : XmlOperations.xmltext(arg.toString());
            }
            case "xmlcomment": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : XmlOperations.xmlcomment(arg.toString());
            }
            case "xml_is_well_formed": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : XmlOperations.xmlIsWellFormed(arg.toString());
            }
            case "xml_is_well_formed_document": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : XmlOperations.xmlIsWellFormedDocument(arg.toString());
            }
            case "xml_is_well_formed_content": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                return arg == null ? null : XmlOperations.xmlIsWellFormedContent(arg.toString());
            }
            case "xpath": {
                String xpathExpr = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                String xml = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                Map<String, String> nsMap = null;
                if (fn.args().size() > 2) {
                    nsMap = parseNamespaceArray(executor.evalExpr(fn.args().get(2), ctx));
                }
                List<String> results = XmlOperations.xpath(xpathExpr, xml, nsMap);
                return XmlOperations.formatXpathResult(results);
            }
            case "xpath_exists": {
                String xpathExpr = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                String xml = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                return XmlOperations.xpathExists(xpathExpr, xml);
            }
            case "table_to_xml": {
                String tableName = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                boolean nulls = executor.isTruthy(executor.evalExpr(fn.args().get(1), ctx));
                boolean tableforest = executor.isTruthy(executor.evalExpr(fn.args().get(2), ctx));
                String targetns = String.valueOf(executor.evalExpr(fn.args().get(3), ctx));
                // The relation is looked for the way any relation reference is: along the search
                // path and among the catalogues, not in public alone. A catalogue relation is a
                // relation, and asking for one as XML is a question with an answer.
                String tableSchema = "public";
                String bareTable = tableName;
                int dot = tableName.indexOf('.');
                if (dot > 0) {
                    tableSchema = tableName.substring(0, dot);
                    bareTable = tableName.substring(dot + 1);
                }
                Table tbl;
                try {
                    tbl = executor.resolveTable(tableSchema, bareTable);
                } catch (MemgresException notThere) {
                    tbl = null;
                }
                if (tbl == null) tbl = executor.systemCatalog.resolve("pg_catalog", bareTable);
                if (tbl == null) {
                    throw new MemgresException(
                            "relation \"" + tableName + "\" does not exist", "42P01");
                }
                tableName = bareTable;
                List<String> colNames = new ArrayList<>();
                for (Column col : tbl.getColumns()) colNames.add(col.getName());
                return XmlOperations.tableToXml(tableName, colNames, tbl.getRows(), nulls, tableforest, targetns);
            }
            case "query_to_xml": {
                String query = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                boolean nulls = executor.isTruthy(executor.evalExpr(fn.args().get(1), ctx));
                boolean tableforest = executor.isTruthy(executor.evalExpr(fn.args().get(2), ctx));
                String targetns = String.valueOf(executor.evalExpr(fn.args().get(3), ctx));
                // Execute the sub-query
                QueryResult qr = executor.execute(query);
                List<String> colNames = new ArrayList<>();
                for (Column col : qr.getColumns()) colNames.add(col.getName());
                return XmlOperations.queryToXml(colNames, qr.getRows(), nulls, tableforest, targetns);
            }
            case "schema_to_xml": {
                // Simplified: returns schema wrapper with table names
                String schemaName = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                return "<" + schemaName + "/>";
            }
            case "database_to_xml": {
                return "<database/>";
            }
            default:
                return NOT_HANDLED;
        }
    }

    /** Check if an expression produces XML (and thus should not be escaped). */
    private static boolean isXmlExpr(Expression expr) {
        if (expr instanceof FunctionCallExpr) {
            String n = ((FunctionCallExpr) expr).name().toLowerCase(java.util.Locale.ROOT);
            return n.startsWith("xml") || n.equals("__xmlattributes__");
        }
        if (expr instanceof CastExpr) {
            String targetType = ((CastExpr) expr).typeName().toLowerCase(java.util.Locale.ROOT);
            return targetType.equals("xml");
        }
        return false;
    }

    /** Parse a PG namespace array like ARRAY[ARRAY['prefix','uri'], ...] into a Map. */
    private static Map<String, String> parseNamespaceArray(Object nsArrayObj) {
        if (nsArrayObj == null) return null;
        // The namespace mapping usually arrives as an evaluated (nested) Java List
        // from ARRAY[ARRAY['prefix','uri'], ...]; handle that before the string form.
        if (nsArrayObj instanceof java.util.List) {
            java.util.List<?> outer = (java.util.List<?>) nsArrayObj;
            Map<String, String> nsMap = new LinkedHashMap<>();
            boolean nested = !outer.isEmpty() && outer.get(0) instanceof java.util.List;
            if (nested) {
                for (Object rowObj : outer) {
                    if (!(rowObj instanceof java.util.List)) continue;
                    java.util.List<?> row = (java.util.List<?>) rowObj;
                    if (row.size() >= 2 && row.get(0) != null) {
                        nsMap.put(row.get(0).toString(), row.get(1) == null ? "" : row.get(1).toString());
                    }
                }
            } else if (outer.size() >= 2 && outer.get(0) != null) {
                nsMap.put(outer.get(0).toString(), outer.get(1) == null ? "" : outer.get(1).toString());
            }
            return nsMap.isEmpty() ? null : nsMap;
        }
        String s = nsArrayObj.toString().trim();
        if (s.isEmpty() || s.equals("{}")) return null;
        Map<String, String> nsMap = new LinkedHashMap<>();
        // Parse {{prefix,uri},{prefix,uri}} or similar
        s = s.replaceAll("^\\{|\\}$", "");
        // Split on "},{" to get pairs
        String[] pairs = s.split("\\}\\s*,\\s*\\{");
        for (String pair : pairs) {
            pair = pair.replaceAll("^\\{|\\}$", "");
            String[] parts = pair.split(",", 2);
            if (parts.length == 2) {
                String prefix = parts[0].trim().replaceAll("^\"|\"$", "");
                String uri = parts[1].trim().replaceAll("^\"|\"$", "");
                nsMap.put(prefix, uri);
            }
        }
        return nsMap.isEmpty() ? null : nsMap;
    }
}
