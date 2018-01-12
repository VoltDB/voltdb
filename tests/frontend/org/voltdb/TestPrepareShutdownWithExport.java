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

import org.voltdb.client.ArbitraryDurationProc;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.TestExportBase;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.utils.VoltFile;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

public class TestPrepareShutdownWithExport extends TestExportBase
{
    private ServerListener m_serverSocket;
    private final List<ClientConnectionHandler> m_clients = Collections.synchronizedList(new ArrayList<ClientConnectionHandler>());
    private final ConcurrentMap<Long, AtomicLong> m_seenIds = new ConcurrentHashMap<Long, AtomicLong>();

    public TestPrepareShutdownWithExport(String name) {
        super(name);
    }

    @Override
    public void setUp() throws Exception {
        m_username = "default";
        m_password = "password";
        VoltFile.recursivelyDelete(new File("/tmp/" + System.getProperty("user.name")));
        File f = new File("/tmp/" + System.getProperty("user.name"));
        f.mkdirs();
        super.setUp();
        m_serverSocket = new ServerListener(5001);
        m_serverSocket.start();
    }

    public void testPrepareShutdown() throws Exception {

        final Client client2 = this.getClient();
        //add tuples for export
        client2.callProcedure("@AdHoc", "create stream ex (i bigint not null)");
        for (int i= 0;i < 10000; i++) {
            client2.callProcedure("@AdHoc", "insert into ex values(" + i + ");");
        }

        final Client client = getAdminClient();
        ClientResponse resp = client.callProcedure("@PrepareShutdown");
        assertTrue(resp.getStatus() == ClientResponse.SUCCESS);

        //push out export buffer and verify if there are any export queue.
        waitForStreamedAllocatedMemoryZero(client2);
        exportVerify(false, 3, 10000);

        for (ClientConnectionHandler s : m_clients) {
            s.stopClient();
        }

        m_clients.clear();
        m_serverSocket.close();
        m_serverSocket = null;
        m_seenIds.clear();

        long sum = Long.MAX_VALUE;
        while (sum > 0) {
            resp = client2.callProcedure("@Statistics", "liveclients", 0);
            assertTrue(resp.getStatus() == ClientResponse.SUCCESS);
            VoltTable t = resp.getResults()[0];
            long trxn=0, bytes=0, msg=0;
            if (t.advanceRow()) {
                trxn = t.getLong(6);
                bytes = t.getLong(7);
                msg = t.getLong(8);
                sum =  trxn + bytes + msg;
            }
            System.out.printf("Outstanding transactions: %d, buffer bytes :%d, response messages:%d\n", trxn, bytes, msg);
            Thread.sleep(2000);
        }
        assertTrue (sum == 0);

        try{
            client.callProcedure("@Shutdown");
            fail("@Shutdown fails via admin mode");
        } catch (ProcCallException e) {
            //if execution reaches here, it indicates the expected exception was thrown.
            System.out.println("@Shutdown: cluster has been shutdown via admin mode ");
        }
    }

    static public junit.framework.Test suite() throws Exception {

        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestPrepareShutdownWithExport.class);
        Map<String, String> additionalEnv = new HashMap<String, String>();

        //use socket exporter
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(ArbitraryDurationProc.class.getResource("clientfeatures.sql"));
        project.addProcedure(ArbitraryDurationProc.class);
        project.setUseDDLSchema(true);

        //enable export
        Properties props = new Properties();
        props.put("replicated", "true");
        props.put("skipinternals", "true");
        project.addExport(true, "custom", props);

        LocalCluster config = new LocalCluster("client-all-partitions.jar", 4, 2, 0, BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setHasLocalServer(false);
        boolean compile = config.compileWithAdminMode(project, -1, false);
        assertTrue(compile);
        builder.addServerConfig(config);
        return builder;
    }

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
                        System.out.println("[Exact] Invalid id: " + l.getKey() + " Count: " + l.getValue().longValue());
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
    class ServerListener extends Thread {
        private ServerSocket ssocket;
        private boolean shuttingDown = false;
        public ServerListener(int port) {
            try {
                ssocket = new ServerSocket(port);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        public void close() throws IOException {
            shuttingDown = true;
            ssocket.close();
            ssocket = null;
        }

        @Override
        public void run() {
            while (!shuttingDown) {
                try {
                    Socket clientSocket = ssocket.accept();
                    ClientConnectionHandler ch = new ClientConnectionHandler(clientSocket);
                    m_clients.add(ch);
                    ch.start();
                } catch (IOException ex) {
                    break;
                }
            }
        }
    }

   class ClientConnectionHandler extends Thread {
        private final Socket m_clientSocket;
        private boolean m_closed = false;
        final CSVParser m_parser = new CSVParser();
        public ClientConnectionHandler(Socket clientSocket) {
            m_clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(m_clientSocket.getInputStream()));
                    while (true) {
                        String line = in.readLine();
                        //You should convert your data to params here.
                        if (line == null && m_closed) {
                            break;
                        }
                        if (line == null) continue;
                        String parts[] = m_parser.parseLine(line);
                        if (parts == null) {
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
                }
            } catch (IOException ioe) {
            }
        }

        public void stopClient() {
            m_closed = true;
        }
    }
}
