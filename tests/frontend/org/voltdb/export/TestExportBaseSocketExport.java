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

package org.voltdb.export;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableLong;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.ProcedureInfo;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.exportclient.SocketExporter;
import org.voltdb.regressionsuites.RegressionSuite;
import org.voltdb_testprocs.regressionsuites.exportprocs.ExportInsertAllowNulls;
import org.voltdb_testprocs.regressionsuites.exportprocs.ExportInsertFromTableSelectSP;
import org.voltdb_testprocs.regressionsuites.exportprocs.ExportInsertNoNulls;
import org.voltdb_testprocs.regressionsuites.exportprocs.ExportRollbackInsertNoNulls;
import org.voltdb_testprocs.regressionsuites.exportprocs.InsertAddedStream;
import org.voltdb_testprocs.regressionsuites.exportprocs.TableInsertLoopback;
import org.voltdb_testprocs.regressionsuites.exportprocs.TableInsertNoNulls;

import com.google_voltpatches.common.collect.Maps;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

public class TestExportBaseSocketExport extends RegressionSuite {

    protected static Map<String, ServerListener> m_serverSockets = new HashMap<String, ServerListener>();
    protected static boolean m_isExportReplicated = false;
    // needs to assign different port number per export table
    protected static Map<String, Integer> m_portForTable = new HashMap<String, Integer>();
    protected static ExportTestExpectedData m_verifier;
    protected static int m_portCount = 5001;
    protected static VoltProjectBuilder project;
    protected static boolean m_verbose = false;

    // Default wait is 10 mins
    protected static final long DEFAULT_DELAY_MS = (10 * 60 * 1000);
    protected static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(1);

    public static class ServerListener extends Thread {

        private final ServerSocket ssocket;
        private final int m_port;
        private final int m_blockingCount;
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
            this(port, 0);
        }

        public ServerListener(int port, int blockingCount) throws IOException {
            m_port = port;
            m_blockingCount = blockingCount;
            ssocket = new ServerSocket(m_port);
            m_close = false;
        }

        public void closeClient() {
            m_close = true;
            synchronized (m_clients) {
                for (ClientConnectionHandler s : m_clients) {
                    s.stopClient();
                }
            }
            m_clients.clear();
        }

        public void close() throws IOException {
            m_close = true;
            ssocket.close();
            try {
                this.join();
            }
            catch (InterruptedException e) {
            }
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
                    if (m_close) {
                        ch.stopClient();
                        break;
                    }
                    ch.start();
                    System.out.println("Client :" + m_port + " # of connections: " + m_clients.size());
                } catch (IOException ex) {
                    System.out.println("Client :" + m_port + " # of connections: " + m_clients.size());
                    if (!m_close) {
                        ex.printStackTrace();
                    }
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

        public int getReceivedRowCount() {
            return m_queue.size();
        }

        public void ignoreRow(long rowId) {
            m_seenIds.remove(rowId);
            m_data.remove(rowId);
            m_queue.remove(rowId);
        }

        private class ClientConnectionHandler extends Thread {
            private final Socket m_clientSocket;
            private volatile boolean m_closed = false;

            public ClientConnectionHandler(Socket clientSocket) {
                m_clientSocket = clientSocket;
            }

            @Override
            public void run() {
                Thread.currentThread().setName("Client handler:" + m_clientSocket);
                final long thid = Thread.currentThread().getId();
                int syncs = 0;
                int lines = 0;
                try {
                    BufferedReader in = new BufferedReader(new InputStreamReader(m_clientSocket.getInputStream()));
                    OutputStream out = m_clientSocket.getOutputStream();
                    while (!m_closed) {
                        String line = in.readLine();
                        // You should convert your data to params here.
                        if (line == null && m_closed) {
                            System.out.println("Nothing to read");
                            break;
                        }
                        if (line == null) {
                            continue;
                        }
                        lines++;

                        // handle sync_block message
                        if (line.equals(SocketExporter.SYNC_BLOCK_MSG)) {
                            syncs++;
                            if (m_blockingCount > 0 && syncs % m_blockingCount == 0) {
                                System.out.println("Blocking client after " + lines + " lines and " + syncs + " syncs");
                                continue;
                            }
                            out.write(48); // What we send doesn't matter. Send any byte as ack.
                            out.flush();
                            continue;
                        }

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
                                if (m_verbose) {
                                    System.out.println(thid + "-Inconsistent " + i + ": " + line);
                                }
                            }
                        } else {
                            m_data.put(i, parts);
                            m_queue.offer(i);
                            if (m_verbose) {
                                System.out.println(thid + "-Consistent " + i + ": " + line);
                            }
                        }
                    }
                } catch (IOException ioe) {
                    if (!m_closed) {
                        System.out.println("ClientConnection handler exited with IOException: " + ioe.getMessage());
                    }
                }
                System.out.println("Client Closed: " + lines + " lines, " + syncs + " syncs");
            }

            public void stopClient() {
                m_closed = true;
                try {
                    m_clientSocket.close();
                    this.join();
                }
                catch (InterruptedException e) {
                }
                catch (IOException e) {
                }
            }
        }

    }


    /** Shove a table name and pkey in front of row data */
    protected Object[] convertValsToParams(String tableName, final int i, final Object[] rowdata) {
        final Object[] params = new Object[rowdata.length + 2];
        params[0] = tableName;
        params[1] = i;
        for (int ii = 0; ii < rowdata.length; ++ii) {
            params[ii + 2] = rowdata[ii];
        }
        return params;
    }

    /** Push pkey into expected row data */
    protected Object[] convertValsToRow(final int i, final char op, final Object[] rowdata) {
        final Object[] row = new Object[rowdata.length + 2];
        row[0] = (byte) (op == 'I' ? 1 : 0);
        row[1] = i;
        for (int ii = 0; ii < rowdata.length; ++ii) {
            row[ii + 2] = rowdata[ii];
        }
        return row;
    }

    /** Push pkey into expected row data */
    protected Object[] convertValsToLoaderRow(final int i, final Object[] rowdata) {
        final Object[] row = new Object[rowdata.length + 1];
        row[0] = i;
        for (int ii = 0; ii < rowdata.length; ++ii) {
            row[ii + 1] = rowdata[ii];
        }
        return row;
    }

    @Override
    public Client getClient() throws IOException {
        Client client = super.getClient();
        if (!client.waitForTopology(60_000)) {
            throw new IOException("Failed to Initialize Hashinator.");
        }
        return client;
    }

    protected static void quiesce(final Client client) throws Exception {
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
    public static final ProcedureInfo[] NONULLS_PROCEDURES = {
            new ProcedureInfo(ExportInsertNoNulls.class, new ProcedurePartitionData ("S_NO_NULLS", "PKEY", "1"),
                    new String[]{"proc"}),
            new ProcedureInfo(ExportRollbackInsertNoNulls.class,
                    new ProcedurePartitionData ("S_NO_NULLS", "PKEY", "1"), new String[]{"proc"}),
    };

    public static final ProcedureInfo[] ALLOWNULLS_PROCEDURES = {
            new ProcedureInfo(ExportInsertAllowNulls.class,
                    new ProcedurePartitionData ("S_ALLOW_NULLS", "PKEY", "1"), new String[]{"proc"}),
    };

    public static final ProcedureInfo[] LOOPBACK_PROCEDURES = {
            new ProcedureInfo(TableInsertLoopback.class,
                    new ProcedurePartitionData("LOOPBACK_NO_NULLS", "PKEY", "0"), new String[] { "proc" })
    };

    public static final ProcedureInfo[] INSERTSELECT_PROCEDURES = {
            new ProcedureInfo(ExportInsertFromTableSelectSP.class,
                    new ProcedurePartitionData("NO_NULLS", "PKEY", "1"), new String[] { "proc" }),
            new ProcedureInfo(TableInsertNoNulls.class,
                    new ProcedurePartitionData("NO_NULLS", "PKEY", "1"), new String[] { "proc" })
    };

    public static final ProcedureInfo[] ADDSTREAM_PROCEDURES = {
            new ProcedureInfo(InsertAddedStream.class,
                    new ProcedurePartitionData ("S_ADDED_STREAM", "PKEY", "1"), new String[]{"proc"})
    };

    /**
     * Wait for count of tuples specified in {@code streamCounts} to be seen in the statistics which will be retrieved
     * by {@code client}.
     * <p>
     * This also waits for pending tuples to be 0 for all partitions of the streams which are being queried.
     * <p>
     * Timeout is {@link #DEFAULT_TIMEOUT}
     *
     * @param client       to use to query stats
     * @param streamCounts map from stream name to minimum count of expected tuples
     * @throws Exception
     */
    public static void waitForExportRowsToBeDelivered(Client client, Map<String, Long> streamCounts) throws Exception {
        waitForExportRowsToBeDelivered(client, streamCounts, DEFAULT_TIMEOUT);
    }

    /**
     * Wait for count of tuples specified in {@code streamCounts} to be seen in the statistics which will be retrieved
     * by {@code client}.
     * <p>
     * This also waits for pending tuples to be 0 for all partitions of the streams which are being queried.
     *
     * @param client       to use to query stats
     * @param streamCounts map from stream name to minimum count of expected tuples
     * @param timeout      duration to wait for tuples to be visible in stats
     * @throws Exception
     */
    public static void waitForExportRowsToBeDelivered(Client client, Map<String, Long> streamCounts,
            Duration timeout) throws Exception {
        assertFalse(streamCounts.isEmpty());

        quiesce(client);

        streamCounts = forceCase(streamCounts);

        long endTime = System.nanoTime() + timeout.toNanos();
        Map<String, MutableLong> counts = new HashMap<>();

        outter: do {
            counts.clear();

            VoltTable stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
            while (stats.advanceRow()) {
                String stream = stats.getString("SOURCE");
                if (!streamCounts.containsKey(stream) || !Boolean.parseBoolean(stats.getString("ACTIVE"))) {
                    continue;
                }

                long pending = stats.getLong("TUPLE_PENDING");
                if (pending != 0) {
                    String target = stats.getString("TARGET");
                    Long host = stats.getLong("HOST_ID");
                    Long pid = stats.getLong("PARTITION_ID");
                    System.out.println(String.format("%s %s:%d has pending tuples %d. Target: %s", host, stream, pid,
                            pending, target));

                    Thread.sleep(250);
                    continue outter;
                }

                counts.computeIfAbsent(stream, s -> new MutableLong()).add(stats.getLong("TUPLE_COUNT"));
            }

            for (Map.Entry<String, Long> entry : streamCounts.entrySet()) {
                MutableLong actual = counts.get(entry.getKey());
                if (actual == null || actual.longValue() < entry.getValue().longValue()) {
                    System.out.println(String.format("Waiting for %d tuples in %s found %d", entry.getValue(),
                            entry.getKey(), actual == null ? 0 : actual.longValue()));

                    Thread.sleep(250);
                    continue outter;
                }
            }

            return;
        } while (System.nanoTime() < endTime);

        fail("Timeout waiting for counts " + streamCounts + " found " + counts);
    }

    /**
     * Wait for number of tuples to visible in the export stats retrieved by {@code client}. {@code streamCounts} is a
     * map from stream name to a map which maps partition ID to tuple count in that partition. These have to be exact
     * counts
     *
     * @param client         to use to query stats
     * @param streamCounts   map from stream name to map of partition id to count. Counts are exact
     * @param waitForPending If {@code true} this will wait for pending tuples to be 0
     * @param timeout        duration to wait for tuples to be visible in stats
     * @throws Exception
     */
    public static void waitForExportRowsByPartitionToBeDelivered(Client client,
            Map<String, Map<Integer, Integer>> streamCounts, boolean waitForPending, Duration timeout)
            throws Exception {
        assertFalse(streamCounts.isEmpty());

        quiesce(client);

        streamCounts = forceCase(streamCounts);

        long endTime = System.nanoTime() + timeout.toNanos();
        Map<String, Map<Integer, Integer>> counts = new HashMap<>();
        VoltTable stats;

        outter: do {
            counts.clear();

            stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
            while (stats.advanceRow()) {
                String stream = stats.getString("SOURCE");
                if (!streamCounts.containsKey(stream)) {
                    continue;
                }

                int partition = (int) stats.getLong("PARTITION_ID");
                if (waitForPending) {
                    long pending = stats.getLong("TUPLE_PENDING");
                    if (pending != 0) {
                        String target = stats.getString("TARGET");
                        Long host = stats.getLong("HOST_ID");
                        System.out.println(String.format("%s %s:%d has pending tuples %d. Target: %s", host, stream,
                                partition, pending, target));

                        Thread.sleep(250);
                        continue outter;
                    }
                }

                int tupleCount = (int) stats.getLong("TUPLE_COUNT");
                Integer existing = counts.computeIfAbsent(stream, s -> new HashMap<>()).put(partition, tupleCount);
                if (existing != null && existing != tupleCount) {
                    Long host = stats.getLong("HOST_ID");
                    System.out.println(String.format("%s %s:%d has different tuple count than expected %d vs %d", host,
                            stream, partition, existing, tupleCount));
                    Thread.sleep(250);
                    continue outter;
                }
            }

            for (Map.Entry<String, Map<Integer, Integer>> partitions : streamCounts.entrySet()) {
                Map<Integer, Integer> actualPartitions = counts.get(partitions.getKey());
                if (actualPartitions == null) {
                    System.out.println("No active active streams found for " + partitions.getKey());
                    Thread.sleep(250);
                    continue outter;
                }

                for (Map.Entry<Integer, Integer> partition : partitions.getValue().entrySet()) {
                    if (!Objects.equals(partition.getValue(), actualPartitions.get(partition.getKey()))) {
                        System.out.println(String.format("Waiting for tuple counts to match %s",
                                Maps.difference(streamCounts, counts).entriesDiffering()));
                        Thread.sleep(250);
                        continue outter;
                    }
                }
            }

            return;
        } while (System.nanoTime() < endTime);

        fail("Timeout waiting for counts " + streamCounts + " found " + counts + ":\n" + stats);
    }

    private static <V> Map<String, V> forceCase(Map<String, V> map) {
        return map.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toUpperCase(), Map.Entry::getValue));
    }

    /**
     * Wait for all Export Streams to be removed (after all the streams are dropped)
     *
     * @param client
     * @throws Exception
     */
    public static void waitForStreamedTargetDeallocated(Client client) throws Exception {
        boolean passed = false;

        // Quiesce to see all data flushed.
        System.out.println("Quiesce client....");
        quiesce(client);
        System.out.println("Quiesce done....");

        VoltTable stats = null;
        long st = System.currentTimeMillis();
        // Wait 10 mins only
        long end = System.currentTimeMillis() + (10 * 60 * 1000);
        while (true) {
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
            stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
            while (stats.advanceRow()) {
                passedThisTime = false;
                String target = stats.getString("TARGET");
                String ttable = stats.getString("SOURCE");
                Long host = stats.getLong("HOST_ID");
                Long pid = stats.getLong("PARTITION_ID");
                if (target.isEmpty()) {
                    // Stream w/o target keeps pending data forever, log and skip counting this stream
                    System.out.println("Stream Not Dropped but target is disabled: " +
                            ttable + " host:" + host + " partid:" + pid);
                } else {
                    System.out.println("Stream Not Dropped: " + ttable + " to " + target + " host:" + host + " partid:" + pid);
                }
            }
            if (passedThisTime) {
                passed = true;
                break;
            }
            Thread.sleep(1000);
        }
        System.out.println("Passed is: " + passed);
        // System.out.println(stats);
        assertTrue(passed);
    }

    public static void wireupExportTableToCustomExport(String tableName, String procedure) {
        String streamName = tableName;

        if (!m_portForTable.containsKey(streamName)) {
            m_portForTable.put(streamName, getNextPort());
        }
        Properties props = new Properties();
        props.put("procedure", procedure);
        props.put("timezone", "GMT");
        project.addExport(true, ServerExportEnum.CUSTOM, props, streamName);
    }

    public static void wireupExportTableToSocketExport(String streamName) {
        wireupExportTableToSocketExport(streamName, true, false);
    }

    public static void wireupExportTableToSocketExport(String streamName, boolean enabled, boolean exportReplicated) {
        if (!m_portForTable.containsKey(streamName)) {
            m_portForTable.put(streamName, getNextPort());
        }
        Properties props = new Properties();
        props.put("replicated", String.valueOf(exportReplicated));
        props.put("skipinternals", "false");
        props.put("socket.dest", "localhost:" + m_portForTable.get(streamName));
        props.put("timezone", "GMT");
        project.addExport(enabled, ServerExportEnum.CUSTOM, props, streamName);
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

    public static void closeSocketExporterClientAndServer() throws IOException {
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
