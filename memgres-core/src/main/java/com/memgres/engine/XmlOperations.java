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

    /** Validate a string as XML content (for ::xml cast). Throws 2200N on failure. */
    public static String validateXmlCast(String text) {
        if (text == null) return null;
        text = text.trim();
        // Try as document first
        try {
            parseDocument(text);
            return text;
        } catch (MemgresException e) {
            // Try as content
        }
        // Try as content — this throws 2200N on failure
        try {
            parseContent(text);
        } catch (MemgresException e) {
            throw invalidXml(false, extractXmlError(text));
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

    /** xmlcomment(text): creates an XML comment: &lt;!-- text --&gt;. */
    public static String xmlcomment(String text) {
        if (text == null) return null;
        if (text.contains("--")) {
            throw new MemgresException("XML comment must not contain \"--\"", "2200S");
        }
        if (text.endsWith("-")) {
            throw new MemgresException("XML comment must not end with \"-\"", "2200S");
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
     * Content strings are already escaped or raw XML — they are appended as-is.
     */
    public static String xmlelement(String tagName, Map<String, String> attributes, List<String> contents) {
        if (tagName == null) return null;
        String safeName = escapeXmlName(tagName);
        StringBuilder sb = new StringBuilder();
        sb.append('<').append(safeName);
        if (attributes != null && !attributes.isEmpty()) {
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                sb.append(' ').append(escapeXmlName(entry.getKey()))
                  .append("=\"").append(escapeXmlAttr(entry.getValue())).append('"');
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
            sb.append("</").append(safeName).append('>');
        }
        return sb.toString();
    }

    /**
     * xmlforest(val AS name, ...)
     * Creates a forest of XML elements.
     */
    /**
     * xmlforest(val AS name, ...)
     *
     * @param alreadyXml which values are xml already, and so go in as the markup they are rather
     *     than as the characters that spell it -- the same rule xmlelement goes by
     */
    public static String xmlforest(List<String> names, List<Object> values,
                                   List<Boolean> alreadyXml) {
        if (names == null || names.isEmpty()) return null;
        StringBuilder sb = new StringBuilder();
        boolean hasContent = false;
        for (int i = 0; i < names.size(); i++) {
            Object val = i < values.size() ? values.get(i) : null;
            if (val == null) continue; // NULL values produce no element
            String safeName = escapeXmlName(names.get(i));
            boolean isXml = alreadyXml != null && i < alreadyXml.size()
                    && Boolean.TRUE.equals(alreadyXml.get(i));
            sb.append('<').append(safeName).append('>');
            sb.append(isXml ? val.toString() : escapeXml(val.toString()));
            sb.append("</").append(safeName).append('>');
            hasContent = true;
        }
        return hasContent ? sb.toString() : null;
    }

    /**
     * The complaint about text that is not XML.
     *
     * <p>PostgreSQL's sentence is fixed -- it names only whether a document or content was
     * wanted -- and the parser's own account of what went wrong is the detail beneath it.
     * Running the two together made the primary message a different sentence for every
     * malformation, which is not something a client can match on.
     */
    private static MemgresException invalidXml(boolean asDocument, String detail) {
        MemgresException e = new MemgresException(
                asDocument ? "invalid XML document" : "invalid XML content",
                asDocument ? "2200M" : "2200N");
        if (detail != null && !detail.isEmpty()) e.setDetail(detail);
        return e;
    }

    /** xmlpi(name target [, content]): creates an XML processing instruction. */
    public static String xmlpi(String target, String content) {
        if (target == null) return null;
        if (target.equalsIgnoreCase("xml")) {
            throw new MemgresException("invalid XML processing instruction target \"xml\"", "2200T");
        }
        if (content != null && content.contains("?>")) {
            throw new MemgresException("XML processing instruction content must not contain \"?>\"", "2200T");
        }
        if (content == null || content.isEmpty()) {
            return "<?" + target + "?>";
        }
        return "<?" + target + " " + content + "?>";
    }

    /** xmlroot(xml, version text, standalone yes|no|no value) */
    /**
     * xmlroot(xml, version text, standalone yes|no|no value)
     *
     * <p>A declaration is written only where it says something: a standalone marker always does,
     * and a version does when it is not the one every document has anyway. Writing one for
     * {@code version '1.0'} alone put twenty-one characters in front of every value that asked
     * for the version it already had.
     */
    public static String xmlroot(String xml, String version, String standalone) {
        if (xml == null) return null;
        String body = stripXmlDeclaration(xml);
        boolean saysStandalone = standalone != null && !standalone.equalsIgnoreCase("no value");
        String saidVersion = version == null || version.equalsIgnoreCase("no value")
                ? "1.0" : version;
        if (!saysStandalone && saidVersion.equals("1.0")) return body;
        StringBuilder decl = new StringBuilder("<?xml version=\"").append(saidVersion).append('"');
        if (saysStandalone) {
            decl.append(" standalone=\"").append(standalone.toLowerCase(java.util.Locale.ROOT)).append('"');
        }
        decl.append("?>");
        return decl.toString() + body;
    }

    // ---- XPath Functions ----

    /** xpath(xpath_expr, xml [, nsarray]): evaluates XPath and returns xml[]. */
    public static List<String> xpath(String xpathExpr, String xml) {
        return xpath(xpathExpr, xml, null);
    }

    /** xpath with namespace support. nsMap maps prefix→URI. */
    public static List<String> xpath(String xpathExpr, String xml, Map<String, String> nsMap) {
        if (xpathExpr == null || xml == null) return Cols.listOf();
        // xpath reads what it is given as a document rather than as content, so text that is not
        // one well-formed element is refused instead of being searched inside a root element of
        // our own making: that wrapper turned 'not xml' into a document with nothing to find and
        // answered no rows for something PostgreSQL never got as far as searching.
        if (!isDocument(xml)) {
            throw new MemgresException("could not parse XML document", "2200M");
        }
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setNamespaceAware(nsMap != null && !nsMap.isEmpty());
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setErrorHandler(new SilentErrorHandler());
            Document doc = builder.parse(new InputSource(new StringReader(xml)));

            XPathFactory xpf = XPathFactory.newInstance();
            XPath xp = xpf.newXPath();
            if (nsMap != null && !nsMap.isEmpty()) {
                final Map<String, String> ns = nsMap;
                xp.setNamespaceContext(new javax.xml.namespace.NamespaceContext() {
                    @Override public String getNamespaceURI(String prefix) {
                        String uri = ns.get(prefix);
                        return uri != null ? uri : javax.xml.XMLConstants.NULL_NS_URI;
                    }
                    @Override public String getPrefix(String uri) { return null; }
                    @Override public java.util.Iterator<String> getPrefixes(String uri) { return null; }
                });
            }
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
        String ns = (targetns != null && !targetns.isEmpty()) ? targetns : "";
        if (!tableforest) {
            sb.append("<").append(tableName);
            if (!ns.isEmpty()) {
                sb.append(" xmlns=\"").append(escapeXmlAttr(ns)).append("\"");
            }
            if (nulls) {
                sb.append(" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
            }
            sb.append(">\n");
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

    /**
     * Escape invalid XML name characters using PG's _xHHHH_ convention.
     * Valid XML name start: letter, underscore.
     * Valid XML name chars: letter, digit, hyphen, dot, underscore, colon.
     */
    static String escapeXmlName(String name) {
        if (name == null || name.isEmpty()) return name;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (i == 0) {
                // Name start char: letter or underscore
                if (Character.isLetter(c) || c == '_') {
                    sb.append(c);
                } else {
                    sb.append(String.format("_x%04X_", (int) c));
                }
            } else {
                // Name char: letter, digit, hyphen, dot, underscore, colon
                if (Character.isLetterOrDigit(c) || c == '-' || c == '.' || c == '_' || c == ':') {
                    sb.append(c);
                } else {
                    sb.append(String.format("_x%04X_", (int) c));
                }
            }
        }
        return sb.toString();
    }

    /**
     * Format xpath result array as PG-style text array.
     * Elements containing special chars are quoted.
     */
    static String formatXpathResult(List<String> results) {
        if (results.isEmpty()) return "{}";
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < results.size(); i++) {
            if (i > 0) sb.append(",");
            String val = results.get(i);
            // PG array quoting: quote only when the element is empty or contains a
            // char special to array syntax (comma, brace, quote, backslash, whitespace).
            // '<' and '>' are NOT special, so element nodes like <x>1</x> stay unquoted.
            boolean needsQuote = val.isEmpty() || val.contains(",") || val.contains("{") || val.contains("}")
                    || val.contains("\"") || val.contains("\\")
                    || val.chars().anyMatch(Character::isWhitespace);
            if (needsQuote) {
                sb.append("\"");
                sb.append(val.replace("\\", "\\\\").replace("\"", "\\\""));
                sb.append("\"");
            } else {
                sb.append(val);
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private static String stripXmlDeclaration(String xml) {
        if (xml == null) return null;
        return xml.replaceFirst("^<\\?xml[^?]*\\?>\\s*", "");
    }

    /**
     * How deeply elements may nest before the document stops being well formed.
     *
     * <p>PostgreSQL's parser stops at 256 levels, so 256 is well formed and 257 is not. Left to
     * itself the JDK parser has no limit and recurses until the stack runs out, which makes the
     * answer depend on how much stack the calling thread happened to have left.
     */
    private static final int MAX_ELEMENT_DEPTH = 256;

    private static final String MAX_DEPTH_PROPERTY =
            "http://www.oracle.com/xml/jaxp/properties/maxElementDepth";

    /** A parser factory that refuses documents nested deeper than PostgreSQL accepts. */
    private static DocumentBuilderFactory depthLimitedFactory(int allowedDepth)
            throws javax.xml.parsers.ParserConfigurationException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        try {
            factory.setAttribute(MAX_DEPTH_PROPERTY, String.valueOf(allowedDepth));
        } catch (IllegalArgumentException notSupported) {
            // An implementation that does not know the property parses as it always did.
        }
        return factory;
    }

    private static void parseDocument(String text) {
        try {
            DocumentBuilder builder = depthLimitedFactory(MAX_ELEMENT_DEPTH).newDocumentBuilder();
            builder.setErrorHandler(new SilentErrorHandler());
            builder.parse(new InputSource(new StringReader(text)));
        } catch (Exception e) {
            throw invalidXml(true, e.getMessage());
        }
    }

    private static void parseContent(String text) {
        // Content can be a fragment, so wrap in a root element to parse
        // Strip XML declaration before wrapping (it's only valid at start of document)
        String stripped = text.replaceFirst("^<\\?xml[^?]*\\?>\\s*", "");
        try {
            String wrapped = "<_root>" + stripped + "</_root>";
            // The wrapper is a level of its own, and the content underneath it may still nest as
            // deeply as a document would.
            DocumentBuilder builder =
                    depthLimitedFactory(MAX_ELEMENT_DEPTH + 1).newDocumentBuilder();
            builder.setErrorHandler(new SilentErrorHandler());
            builder.parse(new InputSource(new StringReader(wrapped)));
        } catch (Exception e) {
            throw invalidXml(false, e.getMessage());
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
            if (node.getNodeType() == Node.TEXT_NODE) {
                // Text nodes: escape for XML re-serialization (PG behavior)
                return escapeXml(node.getTextContent());
            }
            if (node.getNodeType() == Node.ATTRIBUTE_NODE) {
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

    /** Extract a short error description for XML validation failures. */
    private static String extractXmlError(String text) {
        try {
            String wrapped = "<_root>" + text + "</_root>";
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setErrorHandler(new SilentErrorHandler());
            builder.parse(new InputSource(new StringReader(wrapped)));
            return text; // shouldn't reach here
        } catch (Exception e) {
            String msg = e.getMessage();
            return msg != null ? msg : text;
        }
    }

    /** Silent error handler to suppress SAX warnings/errors during parsing. */
    private static class SilentErrorHandler implements ErrorHandler {
        @Override public void warning(SAXParseException e) {}
        @Override public void error(SAXParseException e) throws SAXException { throw e; }
        @Override public void fatalError(SAXParseException e) throws SAXException { throw e; }
    }
}
