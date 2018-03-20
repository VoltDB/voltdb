/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import au.com.bytecode.opencsv_voltpatches.CSVParser;
import java.io.BufferedReader;
import java.io.File;
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
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientConfigForTest;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.TestExportBase;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.utils.VoltFile;

/**
 * Listens for connections from socket export and then counts expected rows.
 * @author akhanzode
 */
public class TestExportSuiteReplicatedSocketExport extends TestExportBase {

    private static ServerListener m_serverSocket;
    private static final CSVParser m_parser = new CSVParser();
    private static LocalCluster config;
    private static final int m_copies = 3;

    private void exportVerify(boolean exact, int copies, int expsize) {
        assertTrue(m_seenIds.size() > 0);
        long st = System.currentTimeMillis();
        //Wait 2 mins only
        long end = System.currentTimeMillis() + (2 * 60 * 1000);
        boolean passed = false;
        while (true) {
            boolean passedThisTime = true;
            for (Entry<Long, AtomicLong> l : m_seenIds.entrySet()) {
                //If we have seen at least expectedTimes number
                if (exact) {
                    if (l.getValue().longValue() != copies) {
                        System.out.println("[Exact] Invalid id: " + l.getKey() + " Count: " + l.getValue().longValue() + " Copies needed: " + copies);
                        passedThisTime = false;
                        break;
                    }
                }
            }
            if (passedThisTime) {
                passed = true;
                break;
            }
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                System.out.println("Waited too long...");
                break;
            }
            if (ctime - st > (3 * 60 * 1000)) {
                st = System.currentTimeMillis();
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {

            }
            System.out.println("Failed this time wait for socket export to arrive.");
        }
        assertTrue(m_seenIds.size() == expsize);
        System.out.println("Seen Id size is: " + m_seenIds.size() + " Passed: " + passed);
        assertTrue(passed);
    }

    private void exportVerify(boolean exact, int expsize) {
        exportVerify(exact, m_copies, expsize);
    }

    @Override
    public void setUp() throws Exception
    {
        m_username = "default";
        m_password = "password";
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        super.setUp();
        m_serverSocket = new ServerListener(5001);
        m_serverSocket.start();

    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.println("Shutting down client and server");
        for (ClientConnectionHandler s : m_clients) {
            s.stopClient();
        }

        m_clients.clear();
        m_serverSocket.close();
        m_serverSocket = null;
        m_seenIds.clear();

    }

    private static final List<ClientConnectionHandler> m_clients = Collections.synchronizedList(new ArrayList<ClientConnectionHandler>());

    private static class ServerListener extends Thread {

        private ServerSocket ssocket;

        public ServerListener(int port) {
            try {
                ssocket = new ServerSocket(port);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        public void close() throws IOException {
            ssocket.close();
        }

        @Override
        public void run() {
            System.out.println("Server listener started.");
            while (true) {
                try {
                    Socket clientSocket = ssocket.accept();
                    ClientConnectionHandler ch = new ClientConnectionHandler(clientSocket);
                    m_clients.add(ch);
                    ch.start();
                    System.out.println("Client # of connections: " + m_clients.size());
                } catch (IOException ex) {
                    ex.printStackTrace();
                    break;
                }
            }

        }
    }

    private static final ConcurrentMap<Long, AtomicLong> m_seenIds = new ConcurrentHashMap<Long, AtomicLong>();
    private static class ClientConnectionHandler extends Thread {
        private final Socket m_clientSocket;
        private boolean m_closed = false;

        public ClientConnectionHandler(Socket clientSocket) {
            m_clientSocket = clientSocket;
        }

        @Override
        public void run() {
            System.out.println("Starting Client handler");
            try {
                while (true) {
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(m_clientSocket.getInputStream()));
                    while (true) {
                        String line = in.readLine();
                        //You should convert your data to params here.
                        if (line == null && m_closed) {
                            System.out.println("Nothing to read");
                            break;
                        }
                        if (line == null) continue;
                        String parts[] = m_parser.parseLine(line);
                        if (parts == null) {
                            System.out.println("Failed to parse exported data.");
                            continue;
                        }
                        Long i = Long.parseLong(parts[0]);
                        if (m_seenIds.putIfAbsent(i, new AtomicLong(1)) != null) {
                            synchronized(m_seenIds) {
                                m_seenIds.get(i).incrementAndGet();
                            }
                        }
                    }
                    m_clientSocket.close();
                    System.out.println("Client Closed.");
                }
            } catch (IOException ioe) {
            }
        }

        public void stopClient() {
            m_closed = true;
        }
    }

    public void testExportReplicatedExportToSocket() throws Exception {
        System.out.println("testExportReplicatedExportToSocket");
        final Client client = getClient();

        client.callProcedure("@AdHoc", "create stream ex (i bigint not null)");
        StringBuilder insertSql;
        for (int i=0;i<5000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForStreamedAllocatedMemoryZero(client);
        exportVerify(true, 5000);
        for (int i=5000;i<10000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForStreamedAllocatedMemoryZero(client);
        exportVerify(true, 10000);
        for (int i=10000;i<15000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForStreamedAllocatedMemoryZero(client);
        exportVerify(true, 15000);

        for (int i=15000;i<30000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForStreamedAllocatedMemoryZero(client);
        exportVerify(true, 30000);

        for (int i=30000;i<45000;i++) {
            insertSql = new StringBuilder();
            insertSql.append("insert into ex values(" + i + ");");
            client.callProcedure("@AdHoc", insertSql.toString());
        }
        client.drain();
        waitForStreamedAllocatedMemoryZero(client);
        exportVerify(true, 45000);

    }

    public void testExportReplicatedExportToSocketRejoin() throws Exception {
        ClientConfig cconfig = new ClientConfigForTest("ry@nlikesthe", "y@nkees");
        Client client = ClientFactory.createClient(cconfig);
        client.createConnection("localhost", config.port(0));

        client.callProcedure("@AdHoc", "create table regular (i bigint not null)");
        client.callProcedure("@AdHoc", "create stream ex (i bigint not null)");
        StringBuilder insertSql = new StringBuilder();
        for (int i=0;i<50;i++) {
            insertSql.append("insert into ex values(" + i + ");");
        }
        client.callProcedure("@AdHoc", insertSql.toString());
        client.drain();
        waitForStreamedAllocatedMemoryZero(client);
        exportVerify(true, 50);
        config.killSingleHost(1);
        config.recoverOne(1, 0, "");
        Thread.sleep(2000);
        insertSql = new StringBuilder();
        for (int i=50;i<100;i++) {
            insertSql.append("insert into ex values(" + i + ");");
        }
        client.callProcedure("@AdHoc", insertSql.toString());
        client.drain();
        waitForStreamedAllocatedMemoryZero(client);
        //After recovery make sure we get exact 2 of each.
        exportVerify(false, 100);

        config.killSingleHost(2);
        config.recoverOne(2, 0, "");
        Thread.sleep(2000);
        insertSql = new StringBuilder();
        for (int i=100;i<150;i++) {
            insertSql.append("insert into ex values(" + i + ");");
        }
        client.callProcedure("@AdHoc", insertSql.toString());
        client.drain();
        waitForStreamedAllocatedMemoryZero(client);
        //After recovery make sure we get exact 2 of each.
        exportVerify(false, 150);

        //Kill host with all masters now.
        config.killSingleHost(0);
        config.recoverOne(0, 1, "");
        Thread.sleep(4000);
        client = ClientFactory.createClient(cconfig);
        client.createConnection("localhost", config.port(0));
        insertSql = new StringBuilder();
        for (int i=150;i<2000;i++) {
            insertSql.append("insert into ex values(" + i + ");");
        }
        client.callProcedure("@AdHoc", insertSql.toString());
        client.drain();
        waitForStreamedAllocatedMemoryZero(client);
        //After recovery make sure we get exact 2 of each.
        exportVerify(false, 2000);
    }

    public TestExportSuiteReplicatedSocketExport(final String name) {
        super(name);
    }

    public static junit.framework.Test suite() throws Exception
    {
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportSuiteReplicatedSocketExport.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);

        Properties props = new Properties();
        props.put("replicated", "true");
        props.put("skipinternals", "true");

        project.addExport(true /* enabled */, "custom", props);
        /*
         * compile the catalog all tests start with
         */
       config = new LocalCluster("export-ddl-cluster-rep.jar", 8, 3, 2,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config, false);
        return builder;
    }
}
