/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.messaging;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.VoltDecimalHelper;

public class EchoServer {
    static ServerSocket server = null;
    static Socket client = null;
    static FastSerializer fs = null;

    static boolean isArray = false;
    static final byte ARRAY_BEGIN = 126;
    static final byte ARRAY_END = 127;

    static void echo(byte[] t, byte[] buffer, int length) {
        VoltType type = null;
        ByteBuffer buf;

        if (t[0] == ARRAY_BEGIN) {
            isArray = true;
            return;
        } else if (t[0] == ARRAY_END) {
            isArray = false;
            return;
        } else {
            type = VoltType.get(t[0]);
        }

        FastDeserializer fds = new FastDeserializer(buffer);
        int count = 1;
        try {
            fs.writeInt(length);
            fs.writeByte(type.getValue());

            if (isArray) {
                if (type == VoltType.TINYINT) {
                    count = fds.readInt();
                    fs.writeInt(count);
                }
                else {
                    count = fds.readShort();
                    fs.writeShort(count);
                }
            }

            for (int ii = 0; ii < count; ii++) {
                switch (type) {
                case TINYINT:
                    byte b = fds.readByte();
                    fs.write(b);
                    break;
                case SMALLINT:
                    short s = fds.readShort();
                    fs.writeShort(s);
                    break;
                case INTEGER:
                    int i = fds.readInt();
                    fs.writeInt(i);
                    break;
                case BIGINT:
                    long l = fds.readLong();
                    fs.writeLong(l);
                    break;
                case FLOAT:
                    double f = fds.readDouble();
                    fs.writeDouble(f);
                    break;
                case STRING:
                    String str = fds.readString();
                    fs.writeString(str);
                    break;
                case VARBINARY:
                    byte[] bin = fds.readVarbinary();
                    fs.writeVarbinary(bin);
                    break;
                case TIMESTAMP:
                    long micros = fds.readLong();
                    fs.writeLong(micros);
                    break;
                case DECIMAL:
                    BigDecimal bd = VoltDecimalHelper.deserializeBigDecimal(fds.buffer());
                    buf = ByteBuffer.allocate(16);
                    VoltDecimalHelper.serializeBigDecimal(bd, buf);
                    buf.flip();
                    fs.write(buf);
                    break;
                case VOLTTABLE:
                    VoltTable table = PrivateVoltTableFactory.createVoltTableFromSharedBuffer(fds.buffer());
                    buf = ByteBuffer.allocate(table.getSerializedSize());
                    table.flattenToBuffer(buf);
                    buf.flip();
                    fs.write(buf);
                    break;
                default:
                    throw new RuntimeException("FIXME: Unsupported type " + type);
                }
            }

            client.getOutputStream().write(fs.getBytes());
            fs.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        try{
            server = new ServerSocket(org.voltdb.client.Client.VOLTDB_SERVER_PORT);
        } catch (IOException e) {
            System.out.println("Could not listen on port");
            e.printStackTrace();
            System.exit(-1);
        }

        fs = new FastSerializer();
        byte[] buffer = new byte[1024 * 1024 * 2];
        try {
            while (true) {
                client = server.accept();
                client.setReceiveBufferSize(1024 * 1024 * 2);
                client.setSendBufferSize(1024 * 1024 * 2);

                byte[] lengthBytes = new byte[4];
                while (client.getInputStream().read(lengthBytes) > 0) {
                    FastDeserializer fds = new FastDeserializer(lengthBytes);
                    int length = fds.readInt();
                    int count = 0;
                    byte[] type = new byte[1];
                    count += client.getInputStream().read(type);

                    // Reads up to a full message
                    while (count < length) {
                        count += client.getInputStream().read(buffer, count - 1, buffer.length - count + 1);
                    }
                    echo(type, buffer, length);
                }

                client.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                server.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
