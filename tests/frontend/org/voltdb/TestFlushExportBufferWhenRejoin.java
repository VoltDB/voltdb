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
import org.voltcore.logging.VoltLogger;

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
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.TestExportBase;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.base.Strings;
import com.google_voltpatches.common.collect.Maps;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

public class TestFlushEEBufferWhenExport extends TestExportBase
{
    private ServerListener m_serverSocket;
    private final List<ClientConnectionHandler> m_clients = Collections.synchronizedList(new ArrayList<ClientConnectionHandler>());
    private final ConcurrentMap<Long, AtomicLong> m_seenIds = new ConcurrentHashMap<Long, AtomicLong>();
    private Map<Integer, TopoMapEntry> m_topoMap = new HashMap<Integer, TopoMapEntry>();
    private static final String SCHEMA =
            "CREATE STREAM t partition on column a (a integer not null, b integer not null);";
    private static int m_portCount = 5001;
    private static LocalCluster cluster = null;
    private static VoltProjectBuilder project = null;
    private static Set<String> set = null;

    public TestFlushEEBufferWhenExport(String name) {
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
        //PeriodicTopoCheck check = new PeriodicTopoCheck();
        //check.start();
    }

    @Test
    public void testFlushEEBufferWithOutQuiesce() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        Client client = null;
        set = new HashSet<>();
        try {
            // config the export settings
            wireupExportTableToSocketExport("t");

            ClientConfig config = new ClientConfig();
            config.setClientAffinity(true);
            config.setTopologyChangeAware(true);
            config.setConnectionResponseTimeout(4*60*1000);
            config.setProcedureCallTimeout(4*60*1000);
            client = ClientFactory.createClient(config);
            client.createConnection("", cluster.port(0));

            // create a table to enable rejoin
            client.callProcedure("@AdHoc", "create table test (a int);");
            client.callProcedure("@AdHoc", "insert into test values(1)");

            loadTopologyMap(client);
            cluster.killSingleHost(1);
            VoltTable vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
            System.out.println("TOPO after kill:");
            System.out.println(vt.toFormattedString());
            int pid = 1, rid = 0;    // partition id, random offset
            HostSiteId newLeader = findNewLeader(pid, rid);
            assertEquals(newLeader.m_hostId, 0);
            assertEquals(newLeader.m_siteId, 1);

            Thread insertThread = new Thread(new Runnable() {
                public void run() {
                    Client client2 = ClientFactory.createClient(config);
                    try {
                        client2.createConnection("", cluster.port(0));
                        for (int i=0; i < 80; i++) {
                            Thread.sleep(80);
                            //add data to stream table
                            client2.callProcedure("@AdHoc", "insert into t values(" + i + ", 1)");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            Thread rejoinThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(50);
                        cluster.rejoinOne(1);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            insertThread.start();
            rejoinThread.start();

            Thread.sleep(6000);
            vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
            System.out.println("TOPO in the end:");
            System.out.println(vt.toFormattedString());
            client.callProcedure("@Quisce");
            assertTrue(set.size() == 80);

            vt.resetRowPosition();
            while(vt.advanceRow()) {
                if (vt.getLong(0) == pid) {
                    break;
                }
            }
        } catch (Exception e){
        } finally {
            if (m_clients.size() > 0) m_clients.get(0).stopClient();
            m_serverSocket.close();
            m_clients.clear();
            cleanup(client, cluster);
        }
    }

    public static void wireupExportTableToSocketExport(String tableName) {
        String streamName = tableName;
        Map<String, Integer> m_portForTable = new HashMap<String, Integer>();
        if (!m_portForTable.containsKey(streamName)) {
            m_portForTable.put(streamName, getNextPort());
        }
        Properties props = new Properties();
        boolean m_isExportReplicated = false;
        props.put("replicated", String.valueOf(m_isExportReplicated));
        props.put("skipinternals", "false");
        props.put("socket.dest", "localhost:" + m_portForTable.get(streamName));
        props.put("timezone", "GMT");
        project.addExport(true /* enabled */, "custom", props, streamName);
    }

    private static Integer getNextPort() {
        return m_portCount++;
    }

    private void loadTopologyMap(Client client) throws IOException, ProcCallException {
        m_topoMap.clear();
        VoltTable vt = client.callProcedure("@GetPartitionKeys", "INTEGER").getResults()[0];
        Map<Long, Long> partitionMap = Maps.newHashMap();
        while (vt.advanceRow()) {
            partitionMap.put(vt.getLong("PARTITION_ID"), vt.getLong("PARTITION_KEY"));
        }

        VoltTable vt1 = client.callProcedure("@Statistics", "TOPO").getResults()[0];
        while (vt1.advanceRow()) {
            long pid = vt1.getLong(0);
            if (pid == 16383) continue;

            TopoMapEntry entry = new TopoMapEntry();
            String leaderStr = vt1.getString(2);
            entry.m_hsLeader = HostSiteId.parseHostSiteId(leaderStr);

            String replicasStr = vt1.getString(1);
            String[] replicas = replicasStr.split(",");

            ArrayList<HostSiteId> m_replicaHsids = new ArrayList<>();
            for (String r: replicas) {
                HostSiteId hsid = HostSiteId.parseHostSiteId(r);
                if (hsid != entry.m_hsLeader) {
                    m_replicaHsids.add(hsid);
                }
            }
            entry.m_partitionKey = (partitionMap.get(pid)).intValue();
            entry.m_hsIds = m_replicaHsids;
            m_topoMap.put((int)pid, entry);
        }

        System.out.println("TOPO before:");
        System.out.println(vt1.toFormattedString());
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
                e.printStackTrace();
            }
        }
    }

    static public junit.framework.Test suite() throws Exception {
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestFlushEEBufferWhenExport.class);
        Map<String, String> additionalEnv = new HashMap<String, String>();

        //use socket exporter
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.SocketExporter");

        project = new VoltProjectBuilder();
        project.addLiteralSchema(SCHEMA);
        project.setUseDDLSchema(true);
        project.setPartitionDetectionEnabled(true);

        //enable export
        Properties props = new Properties();
        props.put("replicated", "true");
        props.put("skipinternals", "true");
        project.addExport(true, "custom", props);

        cluster = new LocalCluster("testFlushBuffer.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        cluster.setJavaProperty("MAX_EXPORT_BUFFER_FLUSH_INTERVAL", "50000");
        cluster.setNewCli(true);
        project.setDeadHostTimeout(4);
        cluster.setDeplayBetweenNodeStartup(1000);
        cluster.setCallingMethodName("testJoinAndKill");
        cluster.overrideAnyRequestForValgrind();
        // Make sure to disable this STUPID CRAZY hasLocalServer() option
        cluster.setHasLocalServer(false);
        cluster.setJavaProperty("DISABLE_MIGRATE_PARTITION_LEADER", "true");
        boolean compile = cluster.compile(project);
        assertTrue(compile);
        builder.addServerConfig(cluster);
        return builder;
    }

    class ServerListener extends Thread {
        private ServerSocket ssocket;
        private boolean shuttingDown = false;
        public ServerListener(int port) {
            try {
                ssocket = new ServerSocket(port);
                shuttingDown = false;
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
                    ex.printStackTrace();
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
                            set.add(line);
                        }
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

    private HostSiteId findNewLeader(int pid, int randomOffset) {
        return m_topoMap.get(pid).m_hsIds.get(randomOffset);
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

        public static HostSiteId parseHostSiteId (String hostSiteStr) {
            if (Strings.isNullOrEmpty(hostSiteStr)) {
                return null;
            }

            Splitter splitter = Splitter.on(":").omitEmptyStrings().trimResults();
            List<String> arrays = splitter.splitToList(hostSiteStr);
            if (arrays.size() != 2) {
                return null;
            }
            int hid = Integer.parseInt(arrays.get(0));
            int sid = Integer.parseInt(arrays.get(1));
            return new HostSiteId(hid, sid);
        }

        @Override
        public String toString() {
            return m_hostId + ":" + m_siteId;
        }
    }
}
