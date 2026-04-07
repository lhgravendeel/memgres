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
                if (fn.args().size() >= 3) {
                    String targetType = String.valueOf(executor.evalExpr(fn.args().get(2), ctx)).toLowerCase();
                    java.util.Set<String> validTypes = Cols.setOf("text", "varchar", "character varying", "xml", "char", "character", "bytea");
                    if (!validTypes.contains(targetType)) {
                        throw new MemgresException("cannot cast type xml to " + targetType, "42846");
                    }
                }
                Object xmlVal = executor.evalExpr(fn.args().get(1), ctx);
                return xmlVal == null ? null : XmlOperations.xmlserialize(xmlVal.toString());
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
                            if (val != null) attributes.put(attrName, val.toString());
                        }
                    } else {
                        Object val = executor.evalExpr(argExpr, ctx);
                        if (val == null && argExpr instanceof Literal && ((Literal) argExpr).literalType() == Literal.LiteralType.NULL) {
                            Literal lit = (Literal) argExpr;
                            // Skip null placeholder for attributes
                            continue;
                        }
                        contents.add(val == null ? null : XmlOperations.escapeXml(val.toString()));
                    }
                }
                return XmlOperations.xmlelement(tagName, attributes, contents);
            }
            case "xmlforest": {
                List<String> names = new ArrayList<>();
                List<Object> values = new ArrayList<>();
                for (int i = 0; i < fn.args().size(); i += 2) {
                    Object val = executor.evalExpr(fn.args().get(i), ctx);
                    String elemName = String.valueOf(executor.evalExpr(fn.args().get(i + 1), ctx));
                    names.add(elemName);
                    values.add(val);
                }
                return XmlOperations.xmlforest(names, values);
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
                List<String> results = XmlOperations.xpath(xpathExpr, xml);
                return "{" + String.join(",", results) + "}";
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
                // Look up table data
                Schema schema = executor.database.getSchema("public");
                Table tbl = schema != null ? schema.getTable(tableName) : null;
                if (tbl == null) throw new MemgresException("table not found: " + tableName);
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
}
