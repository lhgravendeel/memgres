package com.memgres.pgwire;

import java.util.Map;

/**
 * Represents a decoded message from the PostgreSQL wire protocol.
 */
public class PgWireMessage {

    public enum Type {
        STARTUP,
        SSL_REQUEST,
        QUERY,
        PARSE,
        BIND,
        DESCRIBE,
        EXECUTE,
        SYNC,
        CLOSE,
        TERMINATE,
        PASSWORD,
        FLUSH,
        COPY_DATA,
        COPY_DONE,
        COPY_FAIL,
        GSSENC_REQUEST,
        NEGOTIATE_PROTOCOL,
        FUNCTION_CALL,
        PROTOCOL_ERROR
    }

    private final Type type;
    private final String query;
    private final Map<String, String> parameters;
    private final String statementName;
    private final String portalName;
    // Extended query protocol fields
    private final int[] parameterOids;
    private final byte[][] parameterValues;
    private final short[] parameterFormatCodes;
    private final short[] resultFormatCodes;
    private final byte describeType; // 'S' for statement, 'P' for portal
    private final byte closeType;   // 'S' for statement, 'P' for portal
    private final int maxRows;
    private final byte[] copyData;  // raw bytes for CopyData/CopyFail messages
    // A message the decoder could not read inside its own declared length
    private String sqlState;
    private byte offendingType;
    private boolean fatal;
    private java.util.List<String> unsupportedOptions;
    private int functionOid;
    private short resultFormat;
    private int protocolMinor;

    private PgWireMessage(Type type, String query, Map<String, String> parameters,
                          String statementName, String portalName,
                          int[] parameterOids, byte[][] parameterValues,
                          short[] parameterFormatCodes, short[] resultFormatCodes,
                          byte describeType, byte closeType, int maxRows,
                          byte[] copyData) {
        this.type = type;
        this.query = query;
        this.parameters = parameters;
        this.statementName = statementName;
        this.portalName = portalName;
        this.parameterOids = parameterOids;
        this.parameterValues = parameterValues;
        this.parameterFormatCodes = parameterFormatCodes;
        this.resultFormatCodes = resultFormatCodes;
        this.describeType = describeType;
        this.closeType = closeType;
        this.maxRows = maxRows;
        this.copyData = copyData;
    }

    private PgWireMessage(Type type, String query, Map<String, String> parameters,
                          String statementName, String portalName,
                          int[] parameterOids, byte[][] parameterValues,
                          short[] parameterFormatCodes, short[] resultFormatCodes,
                          byte describeType, byte closeType, int maxRows) {
        this(type, query, parameters, statementName, portalName, parameterOids, parameterValues,
             parameterFormatCodes, resultFormatCodes, describeType, closeType, maxRows, null);
    }

    public static PgWireMessage startup(Map<String, String> parameters, int protocolMinor) {
        PgWireMessage m = new PgWireMessage(Type.STARTUP, null, parameters, null, null,
                null, null, null, null, (byte) 0, (byte) 0, 0);
        m.protocolMinor = protocolMinor;
        return m;
    }

    public static PgWireMessage sslRequest() {
        return new PgWireMessage(Type.SSL_REQUEST, null, null, null, null,
                null, null, null, null, (byte) 0, (byte) 0, 0);
    }

    public static PgWireMessage query(String query) {
        return new PgWireMessage(Type.QUERY, query, null, null, null,
                null, null, null, null, (byte) 0, (byte) 0, 0);
    }

    public static PgWireMessage parse(String statementName, String query, int[] parameterOids) {
        return new PgWireMessage(Type.PARSE, query, null, statementName, null,
                parameterOids, null, null, null, (byte) 0, (byte) 0, 0);
    }

    public static PgWireMessage bind(String portalName, String statementName,
                                     short[] parameterFormatCodes, byte[][] parameterValues,
                                     short[] resultFormatCodes) {
        return new PgWireMessage(Type.BIND, null, null, statementName, portalName,
                null, parameterValues, parameterFormatCodes, resultFormatCodes,
                (byte) 0, (byte) 0, 0);
    }

    public static PgWireMessage describe(byte describeType, String name) {
        return new PgWireMessage(Type.DESCRIBE, null, null, name, null,
                null, null, null, null, describeType, (byte) 0, 0);
    }

    public static PgWireMessage execute(String portalName, int maxRows) {
        return new PgWireMessage(Type.EXECUTE, null, null, null, portalName,
                null, null, null, null, (byte) 0, (byte) 0, maxRows);
    }

    public static PgWireMessage sync() {
        return new PgWireMessage(Type.SYNC, null, null, null, null,
                null, null, null, null, (byte) 0, (byte) 0, 0);
    }

    public static PgWireMessage flush() {
        return new PgWireMessage(Type.FLUSH, null, null, null, null,
                null, null, null, null, (byte) 0, (byte) 0, 0);
    }

    public static PgWireMessage close(byte closeType, String name) {
        return new PgWireMessage(Type.CLOSE, null, null, name, null,
                null, null, null, null, (byte) 0, closeType, 0);
    }

    public static PgWireMessage terminate() {
        return new PgWireMessage(Type.TERMINATE, null, null, null, null,
                null, null, null, null, (byte) 0, (byte) 0, 0);
    }

    public static PgWireMessage password(String password) {
        return new PgWireMessage(Type.PASSWORD, password, null, null, null,
                null, null, null, null, (byte) 0, (byte) 0, 0);
    }

    public static PgWireMessage copyData(byte[] data) {
        return new PgWireMessage(Type.COPY_DATA, null, null, null, null,
                null, null, null, null, (byte) 0, (byte) 0, 0, data);
    }

    public static PgWireMessage copyDone() {
        return new PgWireMessage(Type.COPY_DONE, null, null, null, null,
                null, null, null, null, (byte) 0, (byte) 0, 0);
    }

    public static PgWireMessage gssEncRequest() {
        return new PgWireMessage(Type.GSSENC_REQUEST, null, null, null, null,
                null, null, null, null, (byte) 0, (byte) 0, 0);
    }

    /** The newest protocol this server has, and the named options it does not implement. */
    public static PgWireMessage negotiateProtocolVersion(int protocolMinor,
                                                         java.util.List<String> unsupportedOptions) {
        PgWireMessage m = new PgWireMessage(Type.NEGOTIATE_PROTOCOL, null, null, null, null,
                null, null, null, null, (byte) 0, (byte) 0, 0);
        m.protocolMinor = protocolMinor;
        m.unsupportedOptions = unsupportedOptions;
        return m;
    }

    public static PgWireMessage functionCall(int functionOid, short[] argumentFormatCodes,
                                             byte[][] argumentValues, short resultFormat,
                                             String protocolViolation) {
        PgWireMessage m = new PgWireMessage(Type.FUNCTION_CALL, protocolViolation, null, null, null,
                null, argumentValues, argumentFormatCodes, null, (byte) 0, (byte) 0, 0);
        m.functionOid = functionOid;
        m.resultFormat = resultFormat;
        return m;
    }

    /**
     * A message whose bytes did not match the length above them. A null sqlState means the header
     * itself was unreadable, which PG reports only in its own log before dropping the connection.
     */
    public static PgWireMessage protocolError(String sqlState, String message, byte offendingType,
                                              boolean fatal) {
        PgWireMessage m = new PgWireMessage(Type.PROTOCOL_ERROR, message, null, null, null,
                null, null, null, null, (byte) 0, (byte) 0, 0);
        m.sqlState = sqlState;
        m.offendingType = offendingType;
        m.fatal = fatal;
        return m;
    }

    public static PgWireMessage copyFail(String errorMessage) {
        return new PgWireMessage(Type.COPY_FAIL, errorMessage, null, null, null,
                null, null, null, null, (byte) 0, (byte) 0, 0);
    }

    public Type getType() { return type; }
    public String getQuery() { return query; }
    public Map<String, String> getParameters() { return parameters; }
    public String getStatementName() { return statementName; }
    public String getPortalName() { return portalName; }
    public int[] getParameterOids() { return parameterOids; }
    public byte[][] getParameterValues() { return parameterValues; }
    public short[] getParameterFormatCodes() { return parameterFormatCodes; }
    public short[] getResultFormatCodes() { return resultFormatCodes; }
    public byte getDescribeType() { return describeType; }
    public byte getCloseType() { return closeType; }
    public int getMaxRows() { return maxRows; }
    public byte[] getCopyData() { return copyData; }
    public String getSqlState() { return sqlState; }
    public byte getOffendingType() { return offendingType; }
    public boolean isFatal() { return fatal; }
    public java.util.List<String> getUnsupportedOptions() { return unsupportedOptions; }
    public int getFunctionOid() { return functionOid; }
    public short getResultFormat() { return resultFormat; }
    public int getProtocolMinor() { return protocolMinor; }
}
