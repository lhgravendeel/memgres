package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.*;
import org.w3c.dom.*;
import org.xml.sax.*;

/**
 * XML operations for PostgreSQL compatibility.
 * Implements xml type functions extracted from FunctionEvaluator.
 */
public final class XmlOperations {

    private XmlOperations() {}

    // ---- Parsing / Validation ----

    /** XMLPARSE(DOCUMENT '...') or XMLPARSE(CONTENT '...') */
    public static String xmlparse(String text, boolean isDocument) {
        if (text == null) return null;
        text = text.trim();
        // Validate the XML
        if (isDocument) {
            parseDocument(text);
        } else {
            // CONTENT mode: wrap in root to validate, allow fragments
            parseContent(text);
        }
        return text;
    }

    /** XMLSERIALIZE(CONTENT xml AS text) or XMLSERIALIZE(DOCUMENT xml AS type) */
    public static String xmlserialize(String xml) {
        if (xml == null) return null;
        return xml;
    }

    /** XMLSERIALIZE with INDENT: pretty-print XML with indentation. */
    public static String xmlserializeIndent(String xml) {
        if (xml == null) return null;
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(new InputSource(new StringReader(xml)));

            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc), new StreamResult(writer));
            return writer.toString().trim();
        } catch (Exception e) {
            // Fallback: return as-is
            return xml;
        }
    }

    /** IS DOCUMENT: returns true if the xml value is a well-formed XML document (single root element). */
    public static boolean isDocument(String xml) {
        if (xml == null) return false;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setErrorHandler(new SilentErrorHandler());
            builder.parse(new InputSource(new StringReader(xml)));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** xml_is_well_formed(text): checks if text is well-formed XML. */
    public static boolean xmlIsWellFormed(String text) {
        if (text == null) return false;
        try {
            parseContent(text);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** xml_is_well_formed_document(text) */
    public static boolean xmlIsWellFormedDocument(String text) {
        return isDocument(text);
    }

    /** xml_is_well_formed_content(text) */
    public static boolean xmlIsWellFormedContent(String text) {
        if (text == null) return false;
        try {
            parseContent(text);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // ---- XML Construction Functions ----

    /** xmltext(text): escapes text for XML. */
    public static String xmltext(String text) {
        if (text == null) return null;
        return escapeXml(text);
    }

    /** xmlcomment(text): creates an XML comment: <!-- text -->. */
    public static String xmlcomment(String text) {
        if (text == null) return null;
        if (text.contains("--")) {
            throw new MemgresException("XML comment must not contain \"--\"");
        }
        if (text.endsWith("-")) {
            throw new MemgresException("XML comment must not end with \"-\"");
        }
        return "<!--" + text + "-->";
    }

    /** xmlconcat(xml, xml, ...): concatenates XML values. */
    public static String xmlconcat(List<String> xmlValues) {
        if (xmlValues == null || xmlValues.isEmpty()) return null;
        StringBuilder sb = new StringBuilder();
        boolean allNull = true;
        for (String v : xmlValues) {
            if (v != null) {
                // Strip XML declarations from subsequent fragments
                String cleaned = stripXmlDeclaration(v);
                sb.append(cleaned);
                allNull = false;
            }
        }
        return allNull ? null : sb.toString();
    }

    /**
     * xmlelement(name, xmlattributes(...), content...)
     * Builds an XML element with optional attributes and content.
     */
    public static String xmlelement(String tagName, Map<String, String> attributes, List<String> contents) {
        if (tagName == null) return null;
        StringBuilder sb = new StringBuilder();
        sb.append('<').append(tagName);
        if (attributes != null && !attributes.isEmpty()) {
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                sb.append(' ').append(entry.getKey()).append("=\"").append(escapeXmlAttr(entry.getValue())).append('"');
            }
        }
        if (contents == null || contents.isEmpty() || contents.stream().allMatch(Objects::isNull)) {
            sb.append("/>");
        } else {
            sb.append('>');
            for (String content : contents) {
                if (content != null) {
                    sb.append(content);
                }
            }
            sb.append("</").append(tagName).append('>');
        }
        return sb.toString();
    }

    /**
     * xmlforest(val AS name, ...)
     * Creates a forest of XML elements.
     */
    public static String xmlforest(List<String> names, List<Object> values) {
        if (names == null || names.isEmpty()) return null;
        StringBuilder sb = new StringBuilder();
        boolean hasContent = false;
        for (int i = 0; i < names.size(); i++) {
            Object val = i < values.size() ? values.get(i) : null;
            if (val == null) continue; // NULL values produce no element
            String name = names.get(i);
            sb.append('<').append(name).append('>');
            sb.append(escapeXml(val.toString()));
            sb.append("</").append(name).append('>');
            hasContent = true;
        }
        return hasContent ? sb.toString() : null;
    }

    /** xmlpi(name target [, content]): creates an XML processing instruction. */
    public static String xmlpi(String target, String content) {
        if (target == null) return null;
        if (target.equalsIgnoreCase("xml")) {
            throw new MemgresException("invalid XML processing instruction target \"xml\"");
        }
        if (content != null && content.contains("?>")) {
            throw new MemgresException("XML processing instruction content must not contain \"?>\"");
        }
        if (content == null || content.isEmpty()) {
            return "<?" + target + "?>";
        }
        return "<?" + target + " " + content + "?>";
    }

    /** xmlroot(xml, version text, standalone yes|no|no value) */
    public static String xmlroot(String xml, String version, String standalone) {
        if (xml == null) return null;
        String body = stripXmlDeclaration(xml);
        StringBuilder decl = new StringBuilder("<?xml");
        if (version != null && !version.equalsIgnoreCase("no value")) {
            decl.append(" version=\"").append(version).append("\"");
        } else {
            decl.append(" version=\"1.0\"");
        }
        if (standalone != null && !standalone.equalsIgnoreCase("no value")) {
            decl.append(" standalone=\"").append(standalone.toLowerCase()).append("\"");
        }
        decl.append("?>");
        return decl.toString() + body;
    }

    // ---- XPath Functions ----

    /** xpath(xpath_expr, xml [, nsarray]): evaluates XPath and returns xml[]. */
    public static List<String> xpath(String xpathExpr, String xml) {
        if (xpathExpr == null || xml == null) return Cols.listOf();
        try {
            Document doc = parseToDocument(xml);
            XPathFactory xpf = XPathFactory.newInstance();
            XPath xp = xpf.newXPath();
            NodeList nodes = (NodeList) xp.evaluate(xpathExpr, doc, XPathConstants.NODESET);
            List<String> results = new ArrayList<>();
            for (int i = 0; i < nodes.getLength(); i++) {
                results.add(nodeToString(nodes.item(i)));
            }
            return results;
        } catch (XPathExpressionException e) {
            // Try evaluating as string (for text() etc.)
            try {
                Document doc = parseToDocument(xml);
                XPathFactory xpf = XPathFactory.newInstance();
                XPath xp = xpf.newXPath();
                String result = (String) xp.evaluate(xpathExpr, doc, XPathConstants.STRING);
                return result.isEmpty() ? Cols.listOf() : Cols.listOf(result);
            } catch (Exception e2) {
                throw new MemgresException("invalid XPath expression: " + xpathExpr, "10608");
            }
        } catch (Exception e) {
            throw new MemgresException("xpath error: " + e.getMessage(), "10608");
        }
    }

    /** xpath_exists(xpath_expr, xml): returns true if XPath finds any nodes. */
    public static boolean xpathExists(String xpathExpr, String xml) {
        if (xpathExpr == null || xml == null) return false;
        try {
            Document doc = parseToDocument(xml);
            XPathFactory xpf = XPathFactory.newInstance();
            XPath xp = xpf.newXPath();
            try {
                NodeList nodes = (NodeList) xp.evaluate(xpathExpr, doc, XPathConstants.NODESET);
                return nodes.getLength() > 0;
            } catch (XPathExpressionException e) {
                // Try as boolean
                Boolean result = (Boolean) xp.evaluate(xpathExpr, doc, XPathConstants.BOOLEAN);
                return result;
            }
        } catch (Exception e) {
            return false;
        }
    }

    /** xmlexists(xpath_expr PASSING BY REF xml) */
    public static boolean xmlexists(String xpathExpr, String xml) {
        return xpathExists(xpathExpr, xml);
    }

    // ---- Table/Query/Schema/Database to XML ----

    /** table_to_xml(table_name, nulls, tableforest, targetns) */
    public static String tableToXml(String tableName, List<String> columnNames, List<Object[]> rows,
                                     boolean nulls, boolean tableforest, String targetns) {
        StringBuilder sb = new StringBuilder();
        if (!tableforest) {
            sb.append("<").append(tableName).append(">\n");
        }
        for (Object[] row : rows) {
            sb.append("  <row>\n");
            for (int i = 0; i < columnNames.size(); i++) {
                Object val = i < row.length ? row[i] : null;
                String colName = columnNames.get(i);
                if (val == null) {
                    if (nulls) {
                        sb.append("    <").append(colName).append(" xsi:nil=\"true\"/>\n");
                    }
                } else {
                    sb.append("    <").append(colName).append(">")
                      .append(escapeXml(val.toString()))
                      .append("</").append(colName).append(">\n");
                }
            }
            sb.append("  </row>\n");
        }
        if (!tableforest) {
            sb.append("</").append(tableName).append(">\n");
        }
        return sb.toString();
    }

    /** query_to_xml(query, nulls, tableforest, targetns): same structure as table_to_xml but for query results. */
    public static String queryToXml(List<String> columnNames, List<Object[]> rows,
                                     boolean nulls, boolean tableforest, String targetns) {
        return tableToXml("table", columnNames, rows, nulls, tableforest, targetns);
    }

    // ---- Internal helpers ----

    static String escapeXml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;");
    }

    static String escapeXmlAttr(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;")
                   .replace("'", "&apos;");
    }

    private static String stripXmlDeclaration(String xml) {
        if (xml == null) return null;
        return xml.replaceFirst("^<\\?xml[^?]*\\?>\\s*", "");
    }

    private static void parseDocument(String text) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setErrorHandler(new SilentErrorHandler());
            builder.parse(new InputSource(new StringReader(text)));
        } catch (Exception e) {
            throw new MemgresException("invalid XML document: " + e.getMessage(), "2200M");
        }
    }

    private static void parseContent(String text) {
        // Content can be a fragment, so wrap in a root element to parse
        // Strip XML declaration before wrapping (it's only valid at start of document)
        String stripped = text.replaceFirst("^<\\?xml[^?]*\\?>\\s*", "");
        try {
            String wrapped = "<_root>" + stripped + "</_root>";
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setErrorHandler(new SilentErrorHandler());
            builder.parse(new InputSource(new StringReader(wrapped)));
        } catch (Exception e) {
            throw new MemgresException("invalid XML content: " + e.getMessage(), "2200M");
        }
    }

    private static Document parseToDocument(String xml) {
        try {
            // Try as document first
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setErrorHandler(new SilentErrorHandler());
            return builder.parse(new InputSource(new StringReader(xml)));
        } catch (Exception e) {
            // Try wrapping as content
            try {
                String wrapped = "<_root>" + xml + "</_root>";
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
                DocumentBuilder builder = factory.newDocumentBuilder();
                builder.setErrorHandler(new SilentErrorHandler());
                return builder.parse(new InputSource(new StringReader(wrapped)));
            } catch (Exception e2) {
                throw new MemgresException("invalid XML: " + e.getMessage());
            }
        }
    }

    private static String nodeToString(Node node) {
        try {
            if (node.getNodeType() == Node.TEXT_NODE || node.getNodeType() == Node.ATTRIBUTE_NODE) {
                return node.getTextContent();
            }
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(node), new StreamResult(writer));
            return writer.toString();
        } catch (TransformerException e) {
            return node.getTextContent();
        }
    }

    /** Silent error handler to suppress SAX warnings/errors during parsing. */
    private static class SilentErrorHandler implements ErrorHandler {
        @Override public void warning(SAXParseException e) {}
        @Override public void error(SAXParseException e) throws SAXException { throw e; }
        @Override public void fatalError(SAXParseException e) throws SAXException { throw e; }
    }
}
