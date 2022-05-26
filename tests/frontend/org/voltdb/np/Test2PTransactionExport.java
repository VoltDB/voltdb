/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.np;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.regressionsuites.LocalCluster;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

/*
 * Test the 2p txn when it involves exporting to stream
 */
public class Test2PTransactionExport {
    public static String Schema = "CREATE TABLE table1 (id int PRIMARY KEY not null, value int, dummy varchar(30));"
                                + "PARTITION TABLE table1 ON COLUMN id;"
                                + "CREATE TABLE table2 (id int PRIMARY KEY not null, value int, dummy varchar(30));"
                                + "PARTITION TABLE table2 ON COLUMN id;";

    public static String PARTITION_STREAM = "CREATE STREAM str PARTITION ON COLUMN id ("
                                          + "id int not null, value int, dummy varchar(30));";
    public static String REPLICATED_STREAM = "CREATE STREAM str ("
                                           + "id int not null, value int, dummy varchar(30));";

    private static final ConcurrentMap<Long, AtomicLong> SEEN_IDS = new ConcurrentHashMap<Long, AtomicLong>();

    public  LocalCluster cluster;
    public  Client client;

    private  ServerListener m_serverSocket;
    private  final static CSVParser CSV_PARSER = new CSVParser();
    private static AtomicBoolean stopClient = new AtomicBoolean(false);
    private static int KFACTOR = 1;

    @Test
    public void TestPartitionedStream() throws Exception {
        testExport(true, "TestPartitionedStream");
    }

    @Test
    public void TestReplicatedStream() throws Exception {
        testExport(false, "TestReplicatedStream");
    }

    private void testExport(boolean partitioned, String method) throws Exception {
        try {
            setup(partitioned, method);
            for (int i = 0; i < 10; i++) {
                client.callProcedure("@AdHoc", "INSERT INTO table1 VALUES (" + i + ", 100, 'wx" + i + "');");
            }

            for (int i = 0; i < 10; i++) {
                client.callProcedure("Test2PTransactionExport$TestProc", i, 10000 + i, 100);
            }

            assertTrue(waitForStreamedAllocatedMemoryZero(client));
            assertTrue(verifyExport(10));
        } catch (Exception e) {
            fail("test export fails:" + e.getMessage());
        } finally {
            cleanup();
        }
    }

    private void setup(boolean partitionStream, String method) throws Exception {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.setUseDDLSchema(true);
        if (partitionStream) {
            builder.addLiteralSchema(Schema + PARTITION_STREAM);
        } else {
            builder.addLiteralSchema(Schema + REPLICATED_STREAM);
        }

        Properties props = new Properties();
        props.put("replicated", "true");
        props.put("skipinternals", "true");
        builder.addExport(true, ServerExportEnum.CUSTOM, props);

        cluster = new LocalCluster("test2pexport.jar", 4, 2, KFACTOR,
                                   BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING,
                                   true, additionalEnv);
        cluster.setHasLocalServer(false);
        cluster.setCallingMethodName(method);
        assertTrue(cluster.compile(builder));
        cluster.startUp();

        client = ClientFactory.createClient();
        for (String s : cluster.getListenerAddresses()) {
            client.createConnection(s);
        }

        client.callProcedure("@AdHoc", "CREATE PROCEDURE PARTITION ON TABLE table1 COLUMN id AND ON TABLE table2 COLUMN id FROM "
                                     + "CLASS org.voltdb.np.Test2PTransactionExport$TestProc");

        stopClient.set(false);
        m_serverSocket = new ServerListener(5001);
        m_serverSocket.start();
    }

    private void cleanup() throws Exception {
        try {
            if (client != null) { client.close(); }
        } finally {
            if (cluster != null) { cluster.shutDown(); }
        }
        m_serverSocket.close();
        m_serverSocket = null;
        stopClient.set(true);
        m_clients.clear();
        SEEN_IDS.clear();
    }

    /**
     * Wait for export processor to catch up and have nothing to be exported.
     *
     * @param client
     * @throws Exception
     */
    private boolean waitForStreamedAllocatedMemoryZero(Client client) throws Exception {

        client.callProcedure("@Quiesce");
        System.out.println("Quiesce done....");

        long end = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
        while (true) {
            VoltTable stats = client.callProcedure("@Statistics", "table", 0).getResults()[0];
            boolean done = true;
            while (stats.advanceRow()) {
                if ("StreamedTable".equals(stats.getString("TABLE_TYPE"))) {
                    if (0 != stats.getLong("TUPLE_ALLOCATED_MEMORY")) {
                        System.out.println("Partition Not Zero.");
                        done = false;
                        break;
                    }
                }
            }

            if (done) {
                return true;
            }
            if (System.currentTimeMillis() > end) {
                System.out.println("Waited too long...");
                System.out.println(stats);
                return false;
            }
            Thread.sleep(5000);
        }
    }

    private boolean verifyExport(int expSize) {
        long end = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
        while (true) {
            boolean passedThisTime = true;
            for (Entry<Long, AtomicLong> l : SEEN_IDS.entrySet()) {
                if (l.getValue().longValue() != (KFACTOR +1)) {
                    System.out.println("[Exact] Invalid id: " + l.getKey() + " Count: " + l.getValue().longValue());
                    passedThisTime = false;
                    break;
                }
            }
            if (passedThisTime) {
                return true;
            }
            if (System.currentTimeMillis() > end) {
                System.out.println("Waited too long...");
                return false;
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
                System.out.println("Failed this time wait for socket export to arrive.");
            }
        }
    }

    private static final List<ClientConnectionHandler> m_clients = Collections.synchronizedList(new ArrayList<ClientConnectionHandler>());

    private static class ServerListener extends Thread {

        private ServerSocket ssocket;
        private boolean stop = false;
        public ServerListener(int port) {
            try {
                ssocket = new ServerSocket(port);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        public void close() throws IOException {
            stop = true;
            ssocket.close();
        }

        @Override
        public void run() {
            System.out.println("Server listener started.");
            while (!stop) {
                try {
                    Socket clientSocket = ssocket.accept();
                    ClientConnectionHandler ch = new ClientConnectionHandler(clientSocket);
                    m_clients.add(ch);
                    ch.start();
                    System.out.println("Client # of connections: " + m_clients.size());
                } catch (IOException ex) {
                    break;
                }
            }

        }
    }

    private static class ClientConnectionHandler extends Thread {
        private final Socket m_clientSocket;
        public ClientConnectionHandler(Socket clientSocket) {
            m_clientSocket = clientSocket;
        }

        @Override
        public void run() {
            System.out.println("Starting Client handler");
            try {
                while (true) {
                    BufferedReader in = new BufferedReader(new InputStreamReader(m_clientSocket.getInputStream()));
                    while (true) {
                        String line = in.readLine();
                        //You should convert your data to params here.
                        if (line == null && stopClient.get()) {
                            System.out.println("Nothing to read");
                            break;
                        }
                        if (line == null) continue;

                        System.out.println(line);

                        String parts[] = CSV_PARSER.parseLine(line);
                        if (parts == null) {
                            System.out.println("Failed to parse exported data.");
                            continue;
                        }
                        Long i = Long.parseLong(parts[0]);
                        if (SEEN_IDS.putIfAbsent(i, new AtomicLong(1)) != null) {
                            synchronized(SEEN_IDS) {
                                SEEN_IDS.get(i).incrementAndGet();
                            }
                        }

                        int v = Integer.parseInt(parts[1]);
                        assertTrue("The value should be " + v, v == 100);
                    }
                    m_clientSocket.close();
                    System.out.println("Client Closed.");
                }
            } catch (IOException ioe) {
            }
        }
    }

    public static class TestProc extends VoltProcedure {
        public final SQLStmt get = new SQLStmt("SELECT * FROM table1 WHERE id = ?;");

        public final SQLStmt update = new SQLStmt("UPDATE table1 SET " +
                                                   " value = ?" +
                                                   " WHERE id = ?;");
        // Write to the stream with socket exporter
        public final SQLStmt insert = new SQLStmt("INSERT INTO str VALUES (?, ?, 'DUMMY');");

        public long run(int id1, int id2, int value) throws VoltAbortException {
            voltQueueSQL(get, EXPECT_ONE_ROW, id1);
            VoltTable results1[] = voltExecuteSQL(false);
            VoltTableRow r1 = results1[0].fetchRow(0);
            int val1 = (int) r1.getLong(1);

            if (val1 < value) {
                throw new VoltAbortException("Invalid value!");
            }

            voltQueueSQL(update, val1 - value, id1);
            voltQueueSQL(insert, id2, value);
            voltExecuteSQL(true);
            return 1;
        }
    }
}
