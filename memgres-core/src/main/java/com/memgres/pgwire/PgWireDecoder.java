package com.memgres.pgwire;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes the PostgreSQL wire protocol v3 messages from raw bytes.
 */
public class PgWireDecoder extends ByteToMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(PgWireDecoder.class);

    private final CancelRegistry cancelRegistry;
    private boolean startupDone = false;

    public PgWireDecoder(CancelRegistry cancelRegistry) {
        this.cancelRegistry = cancelRegistry;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (!startupDone) {
            decodeStartup(ctx, in, out);
        } else {
            decodeMessage(in, out);
        }
    }

    private void decodeStartup(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (in.readableBytes() < 8) {
            return; // Wait for more data
        }

        in.markReaderIndex();
        int length = in.readInt();

        if (in.readableBytes() < length - 4) {
            in.resetReaderIndex();
            return;
        }

        int protocolVersion = in.readInt();

        // SSL request: protocol = 80877103
        if (protocolVersion == 80877103) {
            out.add(PgWireMessage.sslRequest());
            return;
        }

        // Cancel request: protocol = 80877102
        if (protocolVersion == 80877102) {
            int pid = in.readInt();
            int secretKey = in.readInt();
            cancelRegistry.cancel(pid, secretKey);
            ctx.close(); // PG protocol: cancel connection is closed immediately
            return;
        }

        // Normal startup: protocol 3.0 = 196608
        Map<String, String> params = new HashMap<>();
        while (in.isReadable()) {
            String key = readCString(in);
            if (key.isEmpty()) break;
            String value = readCString(in);
            params.put(key, value);
        }

        startupDone = true;
        out.add(PgWireMessage.startup(params));
    }

    private void decodeMessage(ByteBuf in, List<Object> out) {
        while (in.readableBytes() >= 5) {
            in.markReaderIndex();
            byte type = in.readByte();
            int length = in.readInt();

            if (in.readableBytes() < length - 4) {
                in.resetReaderIndex();
                return;
            }

            switch (type) {
                case 'Q': {
                    // Simple query
                                       String query = readCString(in);
                                       out.add(PgWireMessage.query(query));
                    break;
                }
                case 'P': {
                    // Parse (extended query)
                                       String stmtName = readCString(in);
                                       String query = readCString(in);
                                       int numParams = in.readShort() & 0xFFFF;
                                       int[] paramOids = new int[numParams];
                                       for (int i = 0; i < numParams; i++) {
                                           paramOids[i] = in.readInt();
                                       }
                                       out.add(PgWireMessage.parse(stmtName, query, paramOids));
                    break;
                }
                case 'B': {
                    // Bind
                                       String portal = readCString(in);
                                       String stmtName = readCString(in);

                                       // Read parameter format codes
                                       int numFormatCodes = in.readShort() & 0xFFFF;
                                       short[] formatCodes = new short[numFormatCodes];
                                       for (int i = 0; i < numFormatCodes; i++) {
                                           formatCodes[i] = in.readShort();
                                       }

                                       // Read parameter values
                                       int numParams = in.readShort() & 0xFFFF;
                                       byte[][] paramValues = new byte[numParams][];
                                       for (int i = 0; i < numParams; i++) {
                                           int paramLen = in.readInt();
                                           if (paramLen == -1) {
                                               paramValues[i] = null; // NULL
                                           } else {
                                               paramValues[i] = new byte[paramLen];
                                               in.readBytes(paramValues[i]);
                                           }
                                       }

                                       // Read result format codes
                                       int numResultFormats = in.readShort() & 0xFFFF;
                                       short[] resultFormats = new short[numResultFormats];
                                       for (int i = 0; i < numResultFormats; i++) {
                                           resultFormats[i] = in.readShort();
                                       }

                                       out.add(PgWireMessage.bind(portal, stmtName, formatCodes, paramValues, resultFormats));
                    break;
                }
                case 'D': {
                    // Describe
                                       byte descType = in.readByte(); // 'S' for statement, 'P' for portal
                                       String name = readCString(in);
                                       out.add(PgWireMessage.describe(descType, name));
                    break;
                }
                case 'E': {
                    // Execute
                                       String portal = readCString(in);
                                       int maxRows = in.readInt();
                                       out.add(PgWireMessage.execute(portal, maxRows));
                    break;
                }
                case 'S': {
                    // Sync
                                       out.add(PgWireMessage.sync());
                    break;
                }
                case 'H': {
                    // Flush
                                       out.add(PgWireMessage.flush());
                    break;
                }
                case 'C': {
                    // Close
                                       byte closeType = in.readByte();
                                       String name = readCString(in);
                                       out.add(PgWireMessage.close(closeType, name));
                    break;
                }
                case 'X': {
                    // Terminate
                                       out.add(PgWireMessage.terminate());
                    break;
                }
                case 'p': {
                    // Password message
                                       String password = readCString(in);
                                       out.add(PgWireMessage.password(password));
                    break;
                }
                case 'd': {
                    // CopyData
                                       int dataLen = length - 4;
                                       byte[] data = new byte[dataLen];
                                       in.readBytes(data);
                                       out.add(PgWireMessage.copyData(data));
                    break;
                }
                case 'c': {
                    // CopyDone
                                       out.add(PgWireMessage.copyDone());
                    break;
                }
                case 'f': {
                    // CopyFail
                                       String errorMsg = readCString(in);
                                       out.add(PgWireMessage.copyFail(errorMsg));
                    break;
                }
                default: {
                    LOG.warn("Unknown message type: {} ('{}')", type, (char) type);
                    in.skipBytes(length - 4);
                    break;
                }
            }
        }
    }

    private String readCString(ByteBuf buf) {
        int start = buf.readerIndex();
        int nullPos = buf.indexOf(start, buf.writerIndex(), (byte) 0);
        if (nullPos < 0) {
            return "";
        }
        byte[] bytes = new byte[nullPos - start];
        buf.readBytes(bytes);
        buf.skipBytes(1); // Skip null terminator
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
