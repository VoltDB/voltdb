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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.voltdb.client.ArbitraryDurationProc;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
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
    
    //private static final String table_schema = "Create Table tt (a integer not null, b integer not null);" + "Partition table tt on column a;";
    private static int m_portCount = 5001;
    private static LocalCluster cluster = null;
    private static VoltProjectBuilder project = null;
    private static Set<String> set = new HashSet<>();
    
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
    public void testFlushEEBuffer() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        Client client = null;
        try {
            //Map<String, String> additionalEnv = new HashMap<String, String>();
            //VoltProjectBuilder builder = new VoltProjectBuilder();
 
            //builder.addLiteralSchema(SCHEMA);

            //cluster.setDeplayBetweenNodeStartup(1000);

            //builder.addPartitionInfo("P_TABLE", "a");										// what is this
            //builder.addLiteralSchema(PROCEDURES);											// table (int a, int b)
            //cluster.setCallingMethodName("testNewLeaderHostDown");							// what is this?
            //builder.configureLogging(null, null, true, true, 200, Integer.MAX_VALUE, 300);
            // should be consistent with host number ????
            //builder.setSnapshotSettings( "1s", 2, "/tmp/snapshotdir1", "foo1");

            //builder.setPartitionDetectionEnabled(true);
            

            // config the export settings
            wireupExportTableToSocketExport("t");

            // bind socket listener to system
            
        	//cluster.shutDown();
            //cluster.startUp();		// a lot of things going on
            System.out.println("The cluster starts up and host count is: " + cluster.getNodeCount());
            
            ClientConfig config = new ClientConfig();
            config.setClientAffinity(true);
            config.setTopologyChangeAware(true);
            config.setConnectionResponseTimeout(4*60*1000);
            config.setProcedureCallTimeout(4*60*1000);
            client = ClientFactory.createClient(config);
            //System.out.println("The port is: " + cluster.port(0));
            client.createConnection("", cluster.port(0));

            // create a table to enable rejoin
            client.callProcedure("@AdHoc", "create table test (a int);");
            client.callProcedure("@AdHoc", "insert into test values(1)");
            
            VoltTable[] results = client.callProcedure("@GetPartitionKeys", "Integer").getResults();
			VoltTable keys = results[0];
			for (int k=0;k<keys.getRowCount();k++) {
			    long key = keys.fetchRow(k).getLong(1);
			    System.out.println("The partition key is: " + key);
			}
            
            loadTopologyMap(client);
            cluster.killSingleHost(1);
            VoltTable vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
            System.out.println("TOPO after kill:");
            System.out.println(vt.toFormattedString());
            int pid = 1, rid = 0;	// partition id, random offset
            HostSiteId newLeader = findNewLeader(pid, rid);
            assertEquals(newLeader.m_hostId, 0);
            assertEquals(newLeader.m_siteId, 1);
            
            Thread insertThread = new Thread(new Runnable() {
            	public void run() {
            		System.out.println("we started the insert");
            		Client client2 = ClientFactory.createClient(config);
                    try {
                    	Thread.sleep(2000);
            		    client2.createConnection("", cluster.port(0));
            		    Long t1 = System.currentTimeMillis();
            		    for (int i=0; i < 20; i++) {
                            //add data to stream table
            		    	Thread.sleep(150);
            		    	System.out.println("**********************We are doing " + i + "th insertion **********************");
							client2.callProcedure("@AdHoc", "insert into t values(" + i + ", 1)");
							if ((i+1) == 1) {
								//VoltTable vt_tb = client2.callProcedure("@Statistics", "Table").getResults()[0];
								//System.out.println(vt_tb.toFormattedString());
							}
                        }
            		    System.out.println("we finished the insert");
            		    Long t2 = System.currentTimeMillis();
            		    System.out.println("The total time for insertion work is " + (t2-t1) + " milliseconds");
                        //m_clients.get(0).stopClient();
					} catch (Exception e) {
						e.printStackTrace();
					}
            	}
            }); 

            Thread rejoinThread = new Thread(new Runnable() {
            	public void run() {
            		Long t1 = System.currentTimeMillis();
                    System.out.println("we started the rejoin");
                    cluster.rejoinOne(1);
        		    Long t2 = System.currentTimeMillis();
        		    System.out.println("The total time for rejoin work is " + (t2-t1) + " milliseconds");
            	}
            });		
            insertThread.start();
            rejoinThread.start();

            
            //client.drain();
            //client.callProcedure("@Quiesce");
            //System.out.println("Called Quiesce");
            
            //TopoMapEntry entry = m_topoMap.get(pid);
            //client.callProcedure(new NullCallback(),"TestMigratePartitionLeader$MigratePartitionLeaderNewLeaderFailureProc",
            //        entry.m_partitionKey, pid, newLeader.m_hostId);
            //Thread.sleep(1000);
            //cluster.rejoinOne(1);
            //System.out.println("Join finished");
            Thread.sleep(5000);
            vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
            System.out.println("TOPO in the end:");
            System.out.println(vt.toFormattedString());
            vt.resetRowPosition();
            while(vt.advanceRow()) {
                if (vt.getLong(0) == pid) {
                    break;
                }
            }
            String leaderStr = vt.getString(2).trim();
            HostSiteId oldLeader = findNewLeader(pid, 0);
            assertTrue(leaderStr.equals(oldLeader.toString()));
            //verify the new host gets the message of clearing up the status
            //assertTrue(cluster.verifyLogMessage(0, RESET_SPI_STATUS_MSG));
        } catch (Exception e){
        	//e.printStackTrace();
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
        System.out.println("The binded port for stream table is: " + m_portForTable.get(streamName));
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
        //project.addSchema(ArbitraryDurationProc.class.getResource("clientfeatures.sql"));
        //project.addProcedure(ArbitraryDurationProc.class);
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
        cluster.setJavaProperty("DISABLE_MIGRATE_PARTITION_LEADER", "false");
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
        	System.out.println("****************************Are we running this?****************************");
            while (!shuttingDown) {
                try {
                	System.out.println("****************************We try accept client socket****************************");
                    Socket clientSocket = ssocket.accept();
                	System.out.println("****************************We have accepted a client socket****************************");
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
            System.out.println("****************************We have a client socket****************************");
        }

        @Override
        public void run() {
            try {
                while (true) {
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(m_clientSocket.getInputStream()));
                    //List<String> list = new ArrayList<>();
                    while (true) {
                        String line = in.readLine();
                        //You should convert your data to params here.
                        if (line == null && m_closed) {
                            break;
                        }
                        //print out line
                        if (line != null) {
                        	//list.add(line);
                        	set.add(line);
                        	System.out.println(line);
                        	System.out.println("Now the list length is " + set.size());
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
                    //System.out.println("Socket have read " + list.size() + " lines");
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
