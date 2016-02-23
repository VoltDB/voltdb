/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.client.ClientAuthScheme;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NullCallback;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.types.TimestampType;

public class GenerateCPPTestFiles {
    private final static byte PROTOCOL_VERSION = 1;
    private final static int  TRUE_SERVER_PORT = 21212;
    private final static int  FAKE_SERVER_PORT = 31212;
    private final static int  DEFAULT_BUFFER_SIZE = 1024;

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        String clientDataDirName = ".";
        for (int idx = 0; idx < args.length; idx += 1) {
            if ("--client-dir".equals(args[idx])) {
                idx += 1;
                clientDataDirName = args[idx];
            } else {
                abend("GenerateCPPTestFiles: Unknown command line argument \"%s\"\n",
                      args[idx]);
            }
        }
        File clientDataDir = new File(clientDataDirName);
        if (clientDataDir.exists() && !clientDataDir.isDirectory()) {
            if (!clientDataDir.isDirectory()) {
                abend("GenerateCPPTestFiles: Client data dir \"%s\" exists but is not a directory.\n", clientDataDirName);
            }
        } else {
            clientDataDir.mkdirs();
        }
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.socket().bind(new InetSocketAddress("localhost", FAKE_SERVER_PORT));
        ClientConfig config = new ClientConfig("hello", "world", (ClientStatusListenerExt )null, ClientAuthScheme.HASH_SHA256);
        final org.voltdb.client.Client client = ClientFactory.createClient(config);
        Thread clientThread = new Thread() {
            @Override
            public void run() {
                try {
                    client.createConnection( "localhost", FAKE_SERVER_PORT);
                    client.close();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        };
        clientThread.setDaemon(true);
        clientThread.start();
        SocketChannel sc = ssc.accept();
        sc.socket().setTcpNoDelay(true);
        ByteBuffer authReqSHA256 = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        sc.configureBlocking(true);
        readMessage(authReqSHA256, sc);
        writeDataFile(clientDataDir, "authentication_request_sha256.msg", authReqSHA256);
        writeServerAuthenticationResponse(sc, true);
        ssc.close();
        clientThread.join(0);

        ssc = ServerSocketChannel.open();
        ssc.socket().bind(new InetSocketAddress("localhost", FAKE_SERVER_PORT));
        config = new ClientConfig("hello", "world", (ClientStatusListenerExt )null, ClientAuthScheme.HASH_SHA1);
        final org.voltdb.client.Client oclient = ClientFactory.createClient(config);
        Thread oclientThread = new Thread() {
            @Override
            public void run() {
                NullCallback ncb = new NullCallback();
                try {
                    oclient.createConnection("localhost", FAKE_SERVER_PORT);
                    oclient.callProcedure("Insert", "Hello", "World", "English");
                    try {
                        oclient.callProcedure("Insert", "Hello", "World", "English");
                    } catch (Exception e) {

                    }
                    oclient.callProcedure("Select", "English");
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        };
        oclientThread.setDaemon(true);
        oclientThread.start();
        sc = ssc.accept();
        sc.socket().setTcpNoDelay(true);
        ByteBuffer authReqSHA1 = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        sc.configureBlocking(true);
        readMessage(authReqSHA1, sc);
        writeDataFile(clientDataDir, "authentication_request.msg", authReqSHA1);
        writeServerAuthenticationResponse(sc, true);
        //
        // Read the some call procedure messages.
        //
        // The client engages us in some witty banter, which we don't
        // actually care about for the purposes of this program.  But
        // we need to read past it, and acknowledge it anyway.  We are
        // acting as a server here.
        //
        ByteBuffer subscription_request = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        readMessage(subscription_request, sc);
        writeServerCallResponse(sc, getClientData(subscription_request));

        ByteBuffer stats_request = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        readMessage(stats_request, sc);
        writeServerCallResponse(sc, getClientData(stats_request));

        ByteBuffer syscat_request = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        readMessage(syscat_request, sc);
        writeServerCallResponse(sc, getClientData(stats_request));

        //
        // Now, read the invocation requests from the client.  We can't
        // actually respond, so we fake up a response.  But this is good
        // enough for now, and we save the message.
        //
        ByteBuffer invocationRequestSuccess = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        readMessage(invocationRequestSuccess, sc);
        writeServerCallResponse(sc, getClientData(invocationRequestSuccess));

        ByteBuffer invocationRequestFailCV = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        readMessage(invocationRequestFailCV, sc);
        writeServerCallResponse(sc, getClientData(invocationRequestFailCV));
        writeDataFile(clientDataDir, "invocation_request_fail_cv.msg", invocationRequestFailCV);

        ByteBuffer invocationRequestSelect = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        readMessage(invocationRequestSelect, sc);
        writeServerCallResponse(sc, getClientData(invocationRequestSelect));
        writeDataFile(clientDataDir, "invocation_request_select.msg", invocationRequestSelect);

        oclient.close();
        ssc.close();
        oclientThread.join();

        // Now, connect to a real server.
        SocketChannel voltsc = SocketChannel.open(new InetSocketAddress("localhost", TRUE_SERVER_PORT));
        voltsc.socket().setTcpNoDelay(true);
        voltsc.configureBlocking(true);

        // Write the authentication message and then
        // read the response.  We need the response.
        ByteBuffer scratch = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        voltsc.write(authReqSHA1);
        readMessage(scratch, voltsc);
        writeDataFile(clientDataDir, "authentication_response.msg", scratch);

        // Write the three procedure messages.
        voltsc.write(invocationRequestSuccess);
        readMessage(scratch, voltsc);
        writeDataFile(clientDataDir, "invocation_response_success.msg", scratch);

        voltsc.write(invocationRequestFailCV);
        readMessage(scratch, voltsc);
        writeDataFile(clientDataDir, "invocation_response_fail_cv.msg", scratch);

        voltsc.write(invocationRequestSelect);
        readMessage(scratch, voltsc);
        writeDataFile(clientDataDir, "invocation_response_select.msg", scratch);

        voltsc.close();
        clientThread.join();

        Thread.sleep(3000);
        ssc = ServerSocketChannel.open();
        ssc.socket().bind(new InetSocketAddress("localhost", FAKE_SERVER_PORT));

        clientThread = new Thread() {
            @Override
            public void run() {
                try {
                    org.voltdb.client.Client newClient = ClientFactory.createClient();
                    newClient.createConnection( "localhost", FAKE_SERVER_PORT);
                    String strings[] = new String[] { "oh", "noes" };
                    byte bytes[] = new byte[] { 22, 33, 44 };
                    short shorts[] = new short[] { 22, 33, 44 };
                    int ints[] = new int[] { 22, 33, 44 };
                    long longs[] = new long[] { 22, 33, 44 };
                    double doubles[] = new double[] { 3, 3.1, 3.14, 3.1459 };
                    TimestampType timestamps[] = new TimestampType[] { new TimestampType(33), new TimestampType(44) };
                    BigDecimal bds[] = new BigDecimal[] { new BigDecimal( "3" ), new BigDecimal( "3.14" ), new BigDecimal( "3.1459" ) };
                    try {
                        newClient.callProcedure("foo", strings, bytes, shorts, ints, longs, doubles, timestamps, bds, null, "ohnoes!", (byte)22, (short)22, 22, (long)22, 3.1459, new TimestampType(33), new BigDecimal("3.1459"));
                    } catch (Exception e) {}
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        clientThread.setDaemon(true);
        clientThread.start();

        voltsc = ssc.accept();
        // Read the authentication message.  We don't need it.
        readMessage(scratch, voltsc);
        writeServerAuthenticationResponse(voltsc, true);

        //
        // The client engages us in some dialog.  We don't need this
        // either, but we need to read past it.
        //
        subscription_request = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        readMessage(subscription_request, voltsc);
        writeServerCallResponse(voltsc, getClientData(subscription_request));

        stats_request = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        readMessage(stats_request, voltsc);
        writeServerCallResponse(voltsc, getClientData(stats_request));

        syscat_request = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        readMessage(syscat_request, voltsc);
        writeServerCallResponse(voltsc, getClientData(stats_request));

        // Read the all-types call procedure message.
        readMessage(scratch, voltsc);
        writeServerCallResponse(voltsc, getClientData(scratch));
        writeDataFile(clientDataDir, "invocation_request_all_params.msg", scratch);
        voltsc.close();
        clientThread.join();

        ColumnInfo columns[] = new ColumnInfo[] {
                new ColumnInfo("column1", VoltType.TINYINT),
                new ColumnInfo("column2", VoltType.STRING),
                new ColumnInfo("column3", VoltType.SMALLINT),
                new ColumnInfo("column4", VoltType.INTEGER),
                new ColumnInfo("column5", VoltType.BIGINT),
                new ColumnInfo("column6", VoltType.TIMESTAMP),
                new ColumnInfo("column7", VoltType.DECIMAL)
        };
        VoltTable vt = new VoltTable(columns);
        vt.addRow( null, null, null, null, null, null, null);
        vt.addRow( 0, "", 2, 4, 5, new TimestampType(44), new BigDecimal("3.1459"));
        vt.addRow( 0, null, 2, 4, 5, null, null);
        vt.addRow( null, "woobie", null, null, null, new TimestampType(44), new BigDecimal("3.1459"));
        ByteBuffer bb = ByteBuffer.allocate(vt.getSerializedSize());
        vt.flattenToBuffer(bb);
        FastSerializer fs = new FastSerializer(vt.getSerializedSize());
        fs.write(bb);
        bb.flip();
        writeDataFile(clientDataDir, "serialized_table.bin", bb);
        clientThread.join();
    }

    private static void writeDataFile(File clientDataDir, String filename, ByteBuffer message) {
        writeDataFile(clientDataDir, filename, message.array(), message.limit());
    }

    private static void writeDataFile(File clientDataDir, String filename, byte[] array, int len) {
        File datafile = null;
        FileOutputStream fos = null;
        try {
            datafile = new File(clientDataDir, filename);
            fos = new FileOutputStream(datafile);
            fos.write(array, 0, len);
        } catch (IOException ex) {
            abend("GenerateCPPTestFiles: Can't open file \"%s\": %s\n",
                  datafile.getName(),
                  ex.getMessage());
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ex) {
                    abend("GenerateCPPTestFiles: Can't close file \"%s\": %s\n",
                          datafile.getName(),
                          ex.getMessage());
                }
            }
        }
    }

    private static void readMessage(ByteBuffer message, SocketChannel sc) throws Exception {
        message.clear();
        ByteBuffer lenbuf = ByteBuffer.wrap(message.array(), 0, 4);
        sc.read(lenbuf);
        int len = lenbuf.getInt(0);
        ByteBuffer body = null;
        if (len > message.capacity()-4) {
            throw new IllegalArgumentException(String.format("Message too big: %d > %d", len, message.capacity()-4));
        } else {
            body = ByteBuffer.wrap(message.array(), 4, len);
            sc.read(body);
            message.position(0);
            message.limit(4+len);
        }
    }

    private static long getClientData(ByteBuffer message) {
        int procNameSize = message.getInt(5);
        long clientData = message.getLong(9+procNameSize);
        return clientData;
    }


    private static void writeServerCallResponse(SocketChannel sc, long clientData) throws IOException {
        ByteBuffer message = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        // Message size.
        writeMessageHeader(message);
        // Match the client data.
        message.putLong(clientData);
        // No optional fields
        message.put((byte)0);
        // Status 1 == OK.
        message.put((byte)1);
        // App Status 1 == OK.
        message.put((byte)1);
        // cluster round trip time.
        message.putInt(100);
        // No tables.
        message.putShort((short)0);
        int size = message.position()-4;
        message.putInt(0, size);
        message.flip();
        sc.write(message);
    }


    private static void writeServerAuthenticationResponse(SocketChannel sc, boolean success) throws IOException {
        ByteBuffer message;
        message = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        writeMessageHeader(message);
        // success == 0, failure == 100
        message.put(success ? (byte)0 : (byte)100);
        // Server Host ID.
        message.putInt(1);
        // Connection ID.
        message.putLong(1);
        // Timestamp.
        TimestampType tp = new TimestampType();
        message.putLong(tp.getTime());
        // IP Address.  There's no place like home.
        message.putInt(0x7f000001);
        // Empty build string.
        message.putInt(0);
        int size = message.position() - 4;
        message.putInt(0, size);
        message.flip();
        sc.write(message);
    }


    private static void writeMessageHeader(ByteBuffer message) {
        // message header.
        message.putInt(0);
        message.put(PROTOCOL_VERSION);
    }


    private static void abend(String format, Object ...args) {
        System.err.printf(format, args);
        System.exit(100);
    }

}
