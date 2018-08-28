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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.VoltFile;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

public class TestFlushExportBufferWhenRejoin extends JUnit4LocalClusterTest
{
    private ServerListener m_serverSocket;
    private final List<ClientConnectionHandler> m_clients = Collections.synchronizedList(new ArrayList<ClientConnectionHandler>());
    private final ConcurrentMap<Long, AtomicLong> m_seenIds = new ConcurrentHashMap<Long, AtomicLong>();
    private static final String SCHEMA = "CREATE STREAM t partition on column a (a integer not null, b integer not null);";
    private static int PORT = 5001;
    private volatile Set<String> exportMessageSet = null;

    static void resetDir() throws IOException {
        File f = new File("/tmp/" + System.getProperty("user.name"));
         VoltFile.recursivelyDelete(f);
         f.mkdirs();
    }

    @Test
    public void testFlushEEBufferWithoutQuiesce() throws Exception {
        resetDir();
        m_serverSocket = new ServerListener(5001);
        m_serverSocket.start();
        VoltFile.resetSubrootForThisProcess();
        exportMessageSet = new HashSet<>();
        LocalCluster cluster = null;
        VoltProjectBuilder builder = null;
        Client client = null;
        try {
            builder = new VoltProjectBuilder();
            builder.addLiteralSchema(SCHEMA);
            builder.setUseDDLSchema(true);
            builder.setPartitionDetectionEnabled(true);
            builder.setDeadHostTimeout(4);

            //use socket exporter
            Map<String, String> additionalEnv = new HashMap<String, String>();
            System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
            additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

            //enable export
            Properties props = new Properties();
            props.put("replicated", "true");
            props.put("skipinternals", "true");
            builder.addExport(true, "custom", props);

            cluster = new LocalCluster("testFlushExportBuffer.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
            cluster.setJavaProperty("MAX_EXPORT_BUFFER_FLUSH_INTERVAL", "50000");
            cluster.setNewCli(true);
            cluster.setHasLocalServer(false);
            cluster.setJavaProperty("DISABLE_MIGRATE_PARTITION_LEADER", "false");
            cluster.overrideAnyRequestForValgrind();
            boolean success = cluster.compile(builder);
            assertTrue(success);
            cluster.startUp(true);

            ClientConfig config = new ClientConfig();
            config.setClientAffinity(true);
            config.setTopologyChangeAware(true);
            config.setConnectionResponseTimeout(4*60*1000);
            config.setProcedureCallTimeout(4*60*1000);

            client = ClientFactory.createClient(config);
            client.createConnection(cluster.getListenerAddress(0));

            // create a table to enable rejoin
            client.callProcedure("@AdHoc", "create table test (a int);");
            client.callProcedure("@AdHoc", "insert into test values(1)");

            VoltTable vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
            System.out.println("TOPO at the start:");
            System.out.println(vt.toFormattedString());
            assertEquals("0:0", vt.fetchRow(0).get(2, VoltType.STRING));
            assertEquals("1:1", vt.fetchRow(1).get(2, VoltType.STRING));

            cluster.killSingleHost(1);
            vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
            System.out.println("TOPO after kill:");
            System.out.println(vt.toFormattedString());
            assertEquals("0:0", vt.fetchRow(0).get(2, VoltType.STRING));
            assertEquals("0:1", vt.fetchRow(1).get(2, VoltType.STRING));

            for (int i = 0; i < 5; i++) {
                Thread.sleep(50);
                //add data to stream table
                client.callProcedure("@AdHoc", "insert into t values(" + i + ", 1)");
            }

            cluster.rejoinOne(1);
            long tss = System.currentTimeMillis();
            while (true) {
                Thread.sleep(100);
                // rejoin time limit (20s) reached, test marked as failed
                if (System.currentTimeMillis() - tss > 20000) {
                    assertTrue(false);
                }
                // rejoin has finished and partition leader migrated
                else {
                    vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
                    if ("2:0".equals(vt.fetchRow(0).get(2, VoltType.STRING)) && "0:1".equals(vt.fetchRow(1).get(2, VoltType.STRING))) {
                        break;
                    }
                }
            }

            // all 5 export messages should be captured
            // because during rejoin a @Quiesce call will be triggered by @Snapshotsave
            assertEquals(exportMessageSet.size(), 5);
            vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
            System.out.println("TOPO in the end:");
            System.out.println(vt.toFormattedString());
            assertEquals("2:0", vt.fetchRow(0).get(2, VoltType.STRING));
            assertEquals("0:1", vt.fetchRow(1).get(2, VoltType.STRING));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            m_serverSocket.close();
            cleanup(client, cluster);
            m_clients.clear();
            System.out.println("Everything shut down.");
        }
    }

    public static void wireupExportTableToSocketExport(String tableName) {
        String streamName = tableName;
        Map<String, Integer> portForTable = new HashMap<String, Integer>();
        portForTable.put(streamName, getNextPort());
        Properties props = new Properties();
        boolean isExportReplicated = false;
        props.put("replicated", String.valueOf(isExportReplicated));
        props.put("skipinternals", "false");
        props.put("socket.dest", "localhost:" + portForTable.get(streamName));
        props.put("timezone", "GMT");
    }

    private static Integer getNextPort() {
        return PORT++;
    }

    private void cleanup(Client client, LocalCluster cluster) {
        if ( client != null) {
            try {
                client.drain();
                client.close();
            } catch (InterruptedException | NoConnectionsException e) {
            }
        }
        if (cluster != null) {
            try {
                cluster.shutDown();
            } catch (InterruptedException e) {
                //e.printStackTrace();
            }
        }
    }

    class ServerListener extends Thread {
        private volatile ServerSocket ssocket;
        private volatile boolean shuttingDown = false;
        public ServerListener(int port) {
            try {
                ssocket = new ServerSocket(port);
                shuttingDown = false;
            } catch (IOException ex) {
                //ex.printStackTrace();
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
                    //ex.printStackTrace();
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
                        if (line != null) {
                            exportMessageSet.add(line);
                        }
                        String parts[] = m_parser.parseLine(line);
                        if (parts == null || parts.length == 0) {
                            continue;
                        }
                        Long i = Long.parseLong(parts[0]);
                        if (m_seenIds.putIfAbsent(i, new AtomicLong(1)) != null) {
                            m_seenIds.get(i).incrementAndGet();
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

    private class TopoMapEntry {
        int m_partitionKey;
        ArrayList<HostSiteId> m_hsIds = new ArrayList<>();
        HostSiteId m_hsLeader;
    }

    private static class HostSiteId implements Comparable<HostSiteId> {
        int m_hostId;
        int m_siteId;

        public HostSiteId(int hid, int sid) {
            m_hostId = hid;
            m_siteId = sid;
        }

        @Override
        public int compareTo(HostSiteId other){
            if (m_hostId == other.m_hostId) {
                return m_siteId - other.m_siteId;
            }
            return m_hostId - other.m_hostId;
        }

        @Override
        public int hashCode() {
            return m_hostId + m_siteId;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof HostSiteId == false) {
                return false;
            }
            if (obj == this) {
                return true;
            }
            HostSiteId hsid = (HostSiteId)obj;
            return m_hostId == hsid.m_hostId && m_siteId == hsid.m_siteId;
        }

        @Override
        public String toString() {
            return m_hostId + ":" + m_siteId;
        }
    }
}
