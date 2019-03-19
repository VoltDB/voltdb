/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.export;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.ProcedurePartitionData;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Insert;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.InsertAddedTable;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.InsertBase;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Loopback;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.RollbackInsert;
import org.voltdb_testprocs.regressionsuites.sqltypesprocs.Update_Export;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

public class TestExportBaseSocketExport extends RegressionSuite {

    protected static Map<String, ServerListener> m_serverSockets = new HashMap<String, ServerListener>();
    protected static boolean m_isExportReplicated = false;
    // needs to assign different port number per export table
    protected static Map<String, Integer> m_portForTable = new HashMap<String, Integer>();
    protected static ExportTestExpectedData m_verifier;
    protected static int m_portCount = 5001;
    protected static VoltProjectBuilder project;
    protected boolean isNewCli = Boolean.valueOf(System.getenv("NEW_CLI") == null ? "true" : System.getenv("NEW_CLI"));

    static class ServerListener extends Thread {

        private ServerSocket ssocket;
        private int m_port;
        private boolean m_close;
        private final List<ClientConnectionHandler> m_clients = Collections
                .synchronizedList(new ArrayList<ClientConnectionHandler>());
        // m_seenIds store for occurrence
        // m_data store for actually received data
        // m_queue store the key in order
        private final ConcurrentMap<Long, AtomicLong> m_seenIds = new ConcurrentHashMap<Long, AtomicLong>();
        private final ConcurrentMap<Long, String[]> m_data = new ConcurrentHashMap<Long, String[]>();
        private final ConcurrentLinkedQueue<Long> m_queue = new ConcurrentLinkedQueue<Long>();
        private static final CSVParser m_parser = new CSVParser();

        public ServerListener(int port) throws IOException {
            m_port = port;
            ssocket = new ServerSocket(m_port);
            m_close = false;
        }

        public void closeClient() {
            for (ClientConnectionHandler s : m_clients) {
                s.stopClient();
            }
            m_clients.clear();
        }

        public void close() throws IOException {
            ssocket.close();
            m_close = true;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("Server listener port:" + m_port);
            System.out.println("Server listener for port: " + m_port + " started.");
            while (true) {
                try {
                    Socket clientSocket = ssocket.accept();
                    ClientConnectionHandler ch = new ClientConnectionHandler(clientSocket);
                    m_clients.add(ch);
                    ch.start();
                    System.out.println("Client :" + m_port + " # of connections: " + m_clients.size());
                } catch (IOException ex) {
                    System.out.println("Client :" + m_port + " # of connections: " + m_clients.size());
                    if (!m_close)
                        ex.printStackTrace();
                    break;
                }
            }
        }

        public Map<Long, String[]> getData() {
            return m_data;
        }

        public long getCount(long rowSeq) {
            return m_seenIds.get(rowSeq).get();
        }

        public String[] getNext() {
            Long pkey;
            if ((pkey = m_queue.poll()) != null) {
                return m_data.get(pkey);
            }
            return null;
        }

        public int getSize() {
            return m_queue.size();
        }

        private class ClientConnectionHandler extends Thread {
            private final Socket m_clientSocket;
            private boolean m_closed = false;

            public ClientConnectionHandler(Socket clientSocket) {
                m_clientSocket = clientSocket;
            }

            @Override
            public void run() {
                Thread.currentThread().setName("Client handler:" + m_clientSocket);
                try {
                    while (true) {
                        BufferedReader in = new BufferedReader(new InputStreamReader(m_clientSocket.getInputStream()));
                        while (true) {
                            String line = in.readLine();
                            // You should convert your data to params here.
                            if (line == null && m_closed) {
                                System.out.println("Nothing to read");
                                break;
                            }
                            if (line == null)
                                continue;
                            String parts[] = m_parser.parseLine(line);
                            if (parts == null) {
                                System.out.println("Failed to parse exported data.");
                                continue;
                            }
                            Long i = Long.parseLong(parts[ExportDecoderBase.INTERNAL_FIELD_COUNT]);
                            if (m_seenIds.putIfAbsent(i, new AtomicLong(1)) != null) {
                                m_seenIds.get(i).incrementAndGet();
                                if (!Arrays.equals(m_data.get(i), parts)) {
                                    // also log the inconsistency here
                                    m_data.put(i, parts);
                                }
                            } else {
                                m_data.put(i, parts);
                                m_queue.offer(i);
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

    }

    /** Shove a table name and pkey in front of row data */
    protected Object[] convertValsToParams(String tableName, final int i, final Object[] rowdata) {
        final Object[] params = new Object[rowdata.length + 2];
        params[0] = tableName;
        params[1] = i;
        for (int ii = 0; ii < rowdata.length; ++ii)
            params[ii + 2] = rowdata[ii];
        return params;
    }

    /** Push pkey into expected row data */
    protected Object[] convertValsToRow(final int i, final char op, final Object[] rowdata) {
        final Object[] row = new Object[rowdata.length + 2];
        row[0] = (byte) (op == 'I' ? 1 : 0);
        row[1] = i;
        for (int ii = 0; ii < rowdata.length; ++ii)
            row[ii + 2] = rowdata[ii];
        return row;
    }

    /** Push pkey into expected row data */
    protected Object[] convertValsToLoaderRow(final int i, final Object[] rowdata) {
        final Object[] row = new Object[rowdata.length + 1];
        row[0] = i;
        for (int ii = 0; ii < rowdata.length; ++ii)
            row[ii + 1] = rowdata[ii];
        return row;
    }

    @Override
    public Client getClient() throws IOException {
        Client client = super.getClient();
        int sleptTimes = 0;
        while (!((ClientImpl) client).isHashinatorInitialized() && sleptTimes < 60000) {
            try {
                Thread.sleep(1);
                sleptTimes++;
            } catch (InterruptedException ex) {
                ;
            }
        }
        if (sleptTimes >= 60000) {
            throw new IOException("Failed to Initialize Hashinator.");
        }
        return client;
    }

    protected void quiesce(final Client client) throws Exception {
        client.drain();
        client.callProcedure("@Quiesce");
    }

    public static final RoleInfo GROUPS[] = new RoleInfo[] {
            new RoleInfo("export", false, false, false, false, false, false),
            new RoleInfo("proc", true, false, true, true, false, false),
            new RoleInfo("admin", true, false, true, true, false, false) };

    public static final UserInfo[] USERS = new UserInfo[] {
            new UserInfo("export", "export", new String[] { "export" }),
            new UserInfo("default", "password", new String[] { "proc" }),
            new UserInfo("admin", "admin", new String[] { "proc", "admin" }) };

    /*
     * Test suite boilerplate
     */
    public static final ProcedureInfo[] PROCEDURES = {
            new ProcedureInfo(Insert.class, new ProcedurePartitionData ("NO_NULLS", "PKEY", "1"),
                    new String[]{"proc"}),
            new ProcedureInfo(InsertBase.class, null, new String[]{"proc"}),
            new ProcedureInfo(RollbackInsert.class,
                    new ProcedurePartitionData ("NO_NULLS", "PKEY", "1"), new String[]{"proc"}),
            new ProcedureInfo(Update_Export.class,
                    new ProcedurePartitionData ("ALLOW_NULLS", "PKEY", "1"), new String[]{"proc"})
    };
    public static final ProcedureInfo[] LOOPBACK_PROCEDURES = {
            new ProcedureInfo(Loopback.class,
                    new ProcedurePartitionData("LOOPBACK_NO_NULLS", "PKEY"), new String[] { "proc" })};

    public static final ProcedureInfo[] PROCEDURES2 = {
            new ProcedureInfo(Update_Export.class,
                    new ProcedurePartitionData ("ALLOW_NULLS", "PKEY", "1"), new String[]{"proc"})
    };

    public static final ProcedureInfo[] PROCEDURES3 = {
            new ProcedureInfo(InsertAddedTable.class,
                    new ProcedurePartitionData ("ADDED_TABLE", "PKEY", "1"), new String[]{"proc"})
    };

    /**
     * Wait for export processor to catch up and have nothing to be exported.
     *
     * @param client
     * @throws Exception
     */
    public void waitForStreamedTargetAllocatedMemoryZero(Client client) throws Exception {
        boolean passed = false;

        // Quiesce to see all data flushed.
        System.out.println("Quiesce client....");
        quiesce(client);
        System.out.println("Quiesce done....");

        VoltTable stats = null;
        long ftime = 0;
        long st = System.currentTimeMillis();
        // Wait 10 mins only
        long end = System.currentTimeMillis() + (10 * 60 * 1000);
        while (true) {
            stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
            boolean passedThisTime = true;
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                System.out.println("Waited too long...");
                System.out.println(stats);
                break;
            }
            if (ctime - st > (3 * 60 * 1000)) {
                System.out.println(stats);
                st = System.currentTimeMillis();
            }
            long ts = 0;
            while (stats.advanceRow()) {
                Long tts = stats.getLong("TIMESTAMP");
                // Get highest timestamp and watch is change
                if (tts > ts) {
                    ts = tts;
                }
                long m = stats.getLong("TUPLE_PENDING");
                if (0 != m) {
                    String target = stats.getString("TARGET");
                    String ttable = stats.getString("SOURCE");
                    Long host = stats.getLong("HOST_ID");
                    Long pid = stats.getLong("PARTITION_ID");
                    if (target.isEmpty()) {
                        // Stream w/o target keeps pending data forever, log and skip counting this stream
                        System.out.println("Pending export data is not zero but target is disabled: " +
                                ttable + " pend:" + m  + " host:" + host + " partid:" + pid);
                    } else {
                        passedThisTime = false;
                        System.out.println("Partition Not Zero: " + ttable + " pend:" + m  + " host:" + host + " partid:" + pid);
                        break;
                    }
                }
            }
            if (passedThisTime) {
                if (ftime == 0) {
                    ftime = ts;
                    continue;
                }
                // we got 0 stats 2 times in row with diff highest timestamp.
                if (ftime != ts) {
                    passed = true;
                    break;
                }
                System.out.println("Passed but not ready to declare victory.");
            }
            Thread.sleep(5000);
        }
        System.out.println("Passed is: " + passed);
        // System.out.println(stats);
        assertTrue(passed);
    }

    /**
     * Wait for export processor to catch up and have nothing to be exported.
     *
     * @param client
     * @throws Exception
     */
    public void waitForStreamedTableAllocatedMemoryZero(Client client) throws Exception {
        boolean passed = false;

        // Quiesc to see all data flushed.
        System.out.println("Quiesce client....");
        quiesce(client);
        System.out.println("Quiesce done....");

        VoltTable stats = null;
        long ftime = 0;
        long st = System.currentTimeMillis();
        // Wait 10 mins only
        long end = System.currentTimeMillis() + (10 * 60 * 1000);
        while (true) {
            stats = client.callProcedure("@Statistics", "table", 0).getResults()[0];
            boolean passedThisTime = true;
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                System.out.println("Waited too long...");
                System.out.println(stats);
                break;
            }
            if (ctime - st > (3 * 60 * 1000)) {
                System.out.println(stats);
                st = System.currentTimeMillis();
            }
            long ts = 0;
            while (stats.advanceRow()) {
                String ttype = stats.getString("TABLE_TYPE");
                String ttable = stats.getString("TABLE_NAME");
                Long tts = stats.getLong("TIMESTAMP");
                Long host = stats.getLong("HOST_ID");
                Long pid = stats.getLong("PARTITION_ID");
                // Get highest timestamp and watch is change
                if (tts > ts) {
                    ts = tts;
                }
                if (ttype.equals("StreamedTable")) {
                    long m = stats.getLong("TUPLE_ALLOCATED_MEMORY");
                    if (0 != m) {
                        passedThisTime = false;
                        System.out.println("Partition Not Zero: " + ttable + ":" + m  + ":" + host + ":" + pid);
                        break;
                    }
                }
            }
            if (passedThisTime) {
                if (ftime == 0) {
                    ftime = ts;
                    continue;
                }
                // we got 0 stats 2 times in row with diff highest timestamp.
                if (ftime != ts) {
                    passed = true;
                    break;
                }
                System.out.println("Passed but not ready to declare victory.");
            }
            Thread.sleep(5000);
        }
        System.out.println("Passed is: " + passed);
        // System.out.println(stats);
        assertTrue(passed);
    }

    public void quiesceAndVerifyStream(final Client client, ExportTestExpectedData tester) throws Exception {
        client.drain();
        waitForStreamedTableAllocatedMemoryZero(client);
        tester.verifyRows();
        System.out.println("Passed!");
    }

    public void quiesceAndVerifyTarget(final Client client, ExportTestExpectedData tester) throws Exception {
        client.drain();
        waitForStreamedTargetAllocatedMemoryZero(client);
        tester.verifyRows();
        System.out.println("Passed!");
    }

    public static void wireupExportTableToCustomExport(String tableName, String procedure) {
        String streamName = tableName;

        if (!m_portForTable.containsKey(streamName)) {
            m_portForTable.put(streamName, getNextPort());
        }
        Properties props = new Properties();
        props.put("procedure", procedure);
        props.put("timezone", "GMT");
        project.addExport(true /* enabled */, "custom", props, streamName);
    }

    public static void wireupExportTableToSocketExport(String tableName) {
        String streamName = tableName;
        if (!m_portForTable.containsKey(streamName)) {
            m_portForTable.put(streamName, getNextPort());
        }
        Properties props = new Properties();
        m_isExportReplicated = false;
        props.put("replicated", String.valueOf(m_isExportReplicated));
        props.put("skipinternals", "false");
        props.put("socket.dest", "localhost:" + m_portForTable.get(streamName));
        props.put("timezone", "GMT");
        project.addExport(true /* enabled */, "custom", props, streamName);
    }

    public static void wireupExportTableToRejectingExport(String tableName) {
        String streamName = tableName;
        //This is done so that when we flip from rejecting to socket export we have ports configured for use.
        if (!m_portForTable.containsKey(streamName)) {
            m_portForTable.put(streamName, getNextPort());
        }
        Properties props = new Properties();
        m_isExportReplicated = false;
        props.put("replicated", String.valueOf(m_isExportReplicated));
        props.put("skipinternals", "false");
        props.put("timezone", "GMT");
        project.addExport(true /* enabled */, "custom", props, streamName);
    }

    private static Integer getNextPort() {
        return m_portCount++;
    }

    public void startListener() throws IOException {
        for (Entry<String, Integer> target : m_portForTable.entrySet()) {
            ServerListener m_serverSocket = new ServerListener(target.getValue());
            m_serverSockets.put(target.getKey(), m_serverSocket);
            m_serverSocket.start();
        }
    }

    public static void closeClientAndServer() throws IOException {
        for (Entry<String, Integer> target : m_portForTable.entrySet()) {
            ServerListener m_serverSocket = m_serverSockets.remove(target.getKey());
            if (m_serverSocket != null) {
                m_serverSocket.closeClient();
                m_serverSocket.close();
            }
        }
    }

    public TestExportBaseSocketExport(String s) {
        super(s);
    }

}
