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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

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
    private enum GenerateCPPTestOperation {
        Authentication,
        Interactions,
        GenerateAllDataTypes
    }

    private static FileOutputStream getClientDataFile(String clientDataDirName, String fileName) throws FileNotFoundException {
        // Where is the client data to be generated?  Is it
        // sensible?
        File clientDir = new File(clientDataDirName);
        if (!clientDir.exists() || !clientDir.isDirectory()) {
            throw new IllegalArgumentException(String.format("ClientDirectory \"%s\" does not exist.", clientDataDirName));
        }
        return new FileOutputStream(new File(clientDir, fileName));
    }

    private static void writeClientDataFile(String clientDataDirName, String fileName, ByteBuffer message) {
        FileOutputStream fos = null;
        try {
            fos = getClientDataFile(clientDataDirName, fileName);
            fos.write(message.array(), 0, message.remaining());
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(100);
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ex) {
                    // Swallow it.
                    ;
                }
            }
        }

    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        String clientDirName = ".";
        // Get the arguments.
        EnumSet<GenerateCPPTestOperation> ops = EnumSet.noneOf(GenerateCPPTestOperation.class);
        for (int idx = 0; idx < args.length; idx += 1) {
            if ("--client-dir".equals(args[idx])) {
                idx += 1;
                clientDirName = args[idx];
            } else if ("--authentication".equals(args[idx])) {
                ops.add(GenerateCPPTestOperation.Authentication);
            } else if ("--interactions".equals(args[idx])) {
                ops.add(GenerateCPPTestOperation.Interactions);
            } else if ("--alltypes".equals(args[idx])) {
                ops.add(GenerateCPPTestOperation.GenerateAllDataTypes);
            } else if ("--allops".equals(args[idx])) {
                ops = EnumSet.allOf(GenerateCPPTestOperation.class);
            } else {
                System.err.printf("GenerateCPPTestFiles: Unknown command line argument: \"%s\"\n",
                                  args[idx]);
                System.exit(100);
            }
        }
        for (GenerateCPPTestOperation op : ops) {
            switch (op) {
            case Authentication:
                generateAuthentications(clientDirName);
                break;
            case Interactions:
                generateInteractions(clientDirName);
                break;
            case GenerateAllDataTypes:
                generateAllDataTypes(clientDirName);
                break;
            default:
                System.err.printf("GenerateCPPTestFiles: This can't happen.  Operation is \"%s\"\n", op);
                System.exit(100);
            }
        }
    }

    private static class GenConnection {
        private final Map<String, ByteBuffer> m_messages = new HashMap<String, ByteBuffer>();
        private final ServerSocketChannel m_ssc;
        private final SocketChannel m_sc;
        private final ByteBuffer m_authMessage;
        public GenConnection(ByteBuffer authMessage, ServerSocketChannel ssc, SocketChannel sc) {
            m_authMessage = authMessage;
            m_ssc = ssc;
            m_sc = sc;
        }
        public final void add(String label, ByteBuffer message) {
            m_messages.put(label, message);
        }
        public final ByteBuffer get(String label) {
            return m_messages.get(label);
        }
        public final ServerSocketChannel getSsc() {
            return m_ssc;
        }
        public final SocketChannel getSc() {
            return m_sc;
        }
        public final ByteBuffer getAuthMessage() {
            return m_authMessage;
        }

    }

    private static class ProcedureSpecification {
        private final String m_fileName;
        private final String m_responseFileName;
        private final String m_procName;
        private final Object[] m_params;
        public ProcedureSpecification(String requestFileName, String responseFileName,
                                      String procName, Object[] params) {
            m_fileName = requestFileName;
            m_responseFileName = responseFileName;
            m_procName = procName;
            m_params = params;
        }
        public final String getFileName() {
            return m_fileName;
        }
        public final String getResponseFileName() {
            return m_responseFileName;
        }
        public final String getProcName() {
            return m_procName;
        }
        public final Object[] getParams() {
            return m_params;
        }

    }
    /**
     * Create a fake server on the usual port.  When the client calls procedures
     * we store the messages.  We return a map associating labels with messages.
     * We also store the authentication request message.  We can't actually store
     * any response messages, since we don't have a real server.  We will send
     * these request messages out separately.
     *
     * @param authScheme
     * @return
     */
    public static GenConnection generateRequestMessages(ClientAuthScheme authScheme, final Map<String,ProcedureSpecification> procedures) {
        ServerSocketChannel ssc = null;
        SocketChannel sc = null;
        try {
            ssc = ServerSocketChannel.open();
            ssc.socket().bind(new InetSocketAddress("localhost", 21212));
            ClientConfig config = new ClientConfig("hello", "world", (ClientStatusListenerExt )null, authScheme);
            final org.voltdb.client.Client client = ClientFactory.createClient(config);

            // Create a thread to call a set of procedures.  We will just read the
            // answers from the server in this thread, and not process them as client interactions.
            Thread clientThread = new Thread() {
                @Override
                public void run() {
                    try {
                        client.createConnection( "localhost", 21212);
                        for (Map.Entry<String, ProcedureSpecification> oneProc : procedures.entrySet()) {
                            ProcedureSpecification spec = oneProc.getValue();
                            String procName = spec.getProcName();
                            Object[] params = spec.getParams();
                            if (oneProc.getKey().startsWith("async")) {
                                client.callProcedure(new NullCallback(), procName, params);
                            } else if (oneProc.getKey().startsWith("caught")){
                                try {
                                    client.callProcedure(procName, params);
                                } catch (Exception ex) {

                                }
                            } else {
                                client.callProcedure(procName, params);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            // Start the client calling the procedures.
            clientThread.setDaemon(true);
            clientThread.start();
            sc = ssc.accept();
            sc.socket().setTcpNoDelay(true);
            ByteBuffer message = ByteBuffer.allocate(8096);
            sc.configureBlocking(true);

            // First, read the server response.  Then prepare
            // to write what we just read by flipping it.
            sc.read(message);
            message.flip();
            GenConnection answer = new GenConnection(message, ssc, sc);
            // Write the authentication response.
            writeServerAuthenticationResponse(sc, true);
            for (Map.Entry<String, ProcedureSpecification> oneProc : procedures.entrySet()) {
                message = ByteBuffer.allocate(8096);
                sc.read(message);
                message.flip();
                answer.add(oneProc.getKey(), message);
                writeServerCallResponse(sc, getClientData(message));
            }
            clientThread.join();
            client.close();
            return answer;
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(100);
        } finally {
            if (ssc != null) {
                try {
                    ssc.close();
                } catch (IOException ex) {
                    // We don't care about this really.
                }
            }
        }
        return null;
    }

    private static long getClientData(ByteBuffer message) {
        int procNameSize = message.getInt(5);
        long clientData = message.getLong(9+procNameSize);
        return clientData;
    }

    private static void writeServerCallResponse(SocketChannel sc, long clientData) throws IOException {
        ByteBuffer message = ByteBuffer.allocate(8096);
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
        message = ByteBuffer.allocate(8096);
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

    public static void generateAuthentications(String clientDataDirName) {
        try {
            Map<String, ProcedureSpecification> noprocs = new LinkedHashMap<String, ProcedureSpecification>();
            GenConnection connection = generateRequestMessages(ClientAuthScheme.HASH_SHA256, noprocs);
            ByteBuffer message = connection.getAuthMessage();
            writeClientDataFile(clientDataDirName, "authentication_request_sha256.msg", message);
            connection.m_ssc.close();
            // Now do it all again, but this time the SHA1 hash authentication.
            GenConnection oconnection = generateRequestMessages(ClientAuthScheme.HASH_SHA1, noprocs);
            ByteBuffer omessage = oconnection.getAuthMessage();
            writeClientDataFile(clientDataDirName, "authentication_request.msg", omessage);
            oconnection.m_ssc.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(100);
        }
    }

    @SuppressWarnings("serial")
    public static void generateInteractions(String clientDataDirName) {
        // Authenticate.  We'll need this later, but
        // we won't actually need the message.
        Map<String, ProcedureSpecification> procedures = new LinkedHashMap<String, ProcedureSpecification>(){{
            put("insert1",
                new ProcedureSpecification("invocation_request_success.msg",
                                           "invocation_response_success.msg",
                                           "Insert",
                                           new String[]{"Hello", "World", "English"}));
            put("caughtInsert2",
                new ProcedureSpecification("invocation_request_failure_cv.msg",
                                           "invocation_response_failure_cv.msg",
                                           "Insert",
                                           new String[]{"Insert", "Hello", "World", "English"}));
            put("select",
                new ProcedureSpecification("invocation_request_select.msg",
                                           "invocation_response_select.msg",
                                           "Select",
                                           new String[]{"Select", "English"}));
            put("asyncStop",
                new ProcedureSpecification("stopRequest.msg",
                                           null,
                                           "@Shutdown",
                                           new String[]{}));
        }};
        // Note: This does not actually connect to a server.  It just
        //       generates a bunch of messages in the order specified above.
        //       We will then send the resulting messages out and see
        //       what comes back.
        GenConnection connection = generateRequestMessages(ClientAuthScheme.HASH_SHA1, procedures);
        try {
            connection.getSsc().close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        try {
            /**
             * Now connect to a real server, send the requests we cached above
             * and read the responses.
             */
            final SocketChannel voltsc = SocketChannel.open(new InetSocketAddress("localhost", 21212));
            voltsc.socket().setTcpNoDelay(true);
            voltsc.configureBlocking(true);
            // Write the authentication message, which is cached in
            // the connection.  We don't need to save this, since we
            // saved it in generateAuthentications.  Then read the
            // authentication response.
            voltsc.write(connection.getAuthMessage());
            // Go through the procedures table.  For each procedure,
            // labeled by its key, there should be a request message.
            // Save this request message to a file, send the
            ByteBuffer message = ByteBuffer.allocate(8096);
            voltsc.read(message);
            message.flip();
            ByteBuffer authenticationResponse = ByteBuffer.allocate(message.remaining());
            authenticationResponse.put(message);
            message.flip();
            authenticationResponse.flip();
            writeClientDataFile(clientDataDirName, "authentication_response.msg", message);
            for (Map.Entry<String, ProcedureSpecification> oneProc : procedures.entrySet()) {
                String label = oneProc.getKey();
                ByteBuffer request = connection.get(label);
                String requestFileName = oneProc.getValue().getFileName();
                String responseFileName = oneProc.getValue().getResponseFileName();
                voltsc.write(request);
                message.clear();
                voltsc.read(message);
                request.flip();
                message.flip();
                writeClientDataFile(clientDataDirName, requestFileName, request);
                writeClientDataFile(clientDataDirName, responseFileName, message);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(100);
        }
    }

    private static Object[] addAll(Object ... objs) {
        ArrayList<Object> answer = new ArrayList<Object>();
        for (Object obj : objs) {
            answer.add(obj);
        }
        return answer.toArray();
    }
    public static void generateAllDataTypes(String clientDataDirName) {
        try {
            ServerSocketChannel ssc = ServerSocketChannel.open();
            ssc.socket().bind(new InetSocketAddress("localhost", 21212));
            String strings[] = new String[] { "oh", "noes" };
            byte bytes[] = new byte[] { 22, 33, 44 };
            short shorts[] = new short[] { 22, 33, 44 };
            int ints[] = new int[] { 22, 33, 44 };
            long longs[] = new long[] { 22, 33, 44 };
            double doubles[] = new double[] { 3, 3.1, 3.14, 3.1459 };
            TimestampType timestamps[] = new TimestampType[] { new TimestampType(33), new TimestampType(44) };
            BigDecimal bds[] = new BigDecimal[] { new BigDecimal( "3" ), new BigDecimal( "3.14" ), new BigDecimal( "3.1459" ) };
            final Object[] params = addAll(strings, bytes, shorts, ints, longs, doubles, timestamps, bds, null, "ohnoes!", (byte)22,
                                     (short)22, 22, (long)22, 3.1459, new TimestampType(33), new BigDecimal("3.1459"));
            @SuppressWarnings("serial")
            Map<String, ProcedureSpecification> procedures = new LinkedHashMap<String, ProcedureSpecification>() {{
               put("caught.allData",
                   new ProcedureSpecification("invocation_request_all_params.msg", null, "foo", params));
            }};
            GenConnection connection = generateRequestMessages(ClientAuthScheme.HASH_SHA1, procedures);
            writeClientDataFile(clientDataDirName, "invocation_request_all_params.msg", connection.get("caught.allData"));

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
            FileOutputStream fos = getClientDataFile(clientDataDirName, "serialized_table.bin");
            fos.write(fs.getBytes());
            fos.flush();
            fos.close();

        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(100);
        }
        System.exit(0);
    }
}
