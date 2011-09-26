/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.network.Connection;
import org.voltdb.network.QueueMonitor;
import org.voltdb.network.VoltNetwork;
import org.voltdb.network.VoltProtocolHandler;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.Pair;

/**
 *   De/multiplexes transactions across a cluster
 *
 *   It is safe to synchronized on an individual connection and then the distributer, but it is always unsafe
 *   to synchronized on the distributer and then an individual connection.
 */
class Distributer {
    // collection of connections to the cluster
    private final ArrayList<NodeConnection> m_connections = new ArrayList<NodeConnection>();

    private final ArrayList<ClientStatusListener> m_listeners = new ArrayList<ClientStatusListener>();

    //Selector and connection handling, does all work in blocking selection thread
    private final VoltNetwork m_network;

    // Temporary until a distribution/affinity algorithm is written
    private int m_nextConnection = 0;

    private final int m_expectedOutgoingMessageSize;

    private final DBBPool m_pool;

    private final boolean m_useMultipleThreads;

    private final String m_hostname;

    /**
     * Server's instances id. Unique for the cluster
     */
    private Object m_clusterInstanceId[];

    private final ClientStatsLoader m_statsLoader;
    private String m_buildString;

    private static class ProcedureStats {
        private final String m_name;

        private long m_invocationsCompleted = 0;
        private long m_lastInvocationsCompleted = 0;
        private long m_invocationAborts = 0;
        private long m_lastInvocationAborts = 0;
        private long m_invocationErrors = 0;
        private long m_lastInvocationErrors = 0;

        // cumulative latency measured by client, used to calculate avg. lat.
        private long m_roundTripTime = 0;
        private long m_lastRoundTripTime = 0;

        private int m_maxRoundTripTime = Integer.MIN_VALUE;
        private int m_lastMaxRoundTripTime = Integer.MIN_VALUE;
        private int m_minRoundTripTime = Integer.MAX_VALUE;
        private int m_lastMinRoundTripTime = Integer.MAX_VALUE;

        // cumulative latency measured by the cluster, used to calculate avg lat.
        private long m_clusterRoundTripTime = 0;
        private long m_lastClusterRoundTripTime = 0;

        // 10ms buckets. Last bucket is all transactions > 190ms.
        static int m_numberOfBuckets = 20;
        private long m_clusterRoundTripTimeBuckets[] = new long[m_numberOfBuckets];
        private long m_roundTripTimeBuckets[] = new long[m_numberOfBuckets];

        private int m_maxClusterRoundTripTime = Integer.MIN_VALUE;
        private int m_lastMaxClusterRoundTripTime = Integer.MIN_VALUE;
        private int m_minClusterRoundTripTime = Integer.MAX_VALUE;
        private int m_lastMinClusterRoundTripTime = Integer.MAX_VALUE;

        public ProcedureStats(String name) {
            m_name = name;
        }

        public void update(int roundTripTime, int clusterRoundTripTime, boolean abort, boolean error) {
            m_maxRoundTripTime = Math.max(roundTripTime, m_maxRoundTripTime);
            m_lastMaxRoundTripTime = Math.max( roundTripTime, m_lastMaxRoundTripTime);
            m_minRoundTripTime = Math.min( roundTripTime, m_minRoundTripTime);
            m_lastMinRoundTripTime = Math.max( roundTripTime, m_lastMinRoundTripTime);

            m_maxClusterRoundTripTime = Math.max( clusterRoundTripTime, m_maxClusterRoundTripTime);
            m_lastMaxClusterRoundTripTime = Math.max( clusterRoundTripTime, m_lastMaxClusterRoundTripTime);
            m_minClusterRoundTripTime = Math.min( clusterRoundTripTime, m_minClusterRoundTripTime);
            m_lastMinClusterRoundTripTime = Math.min( clusterRoundTripTime, m_lastMinClusterRoundTripTime);

            m_invocationsCompleted++;
            if (abort) {
                m_invocationAborts++;
            }
            if (error) {
                m_invocationErrors++;
            }
            m_roundTripTime += roundTripTime;
            m_clusterRoundTripTime += clusterRoundTripTime;

            // calculate the latency buckets to increment and increment.
            int rttBucket = (int)(Math.floor(roundTripTime / 10));
            if (rttBucket >= m_roundTripTimeBuckets.length) {
                rttBucket = m_roundTripTimeBuckets.length - 1;
            }
            m_roundTripTimeBuckets[rttBucket] += 1;

            int rttClusterBucket = (int)(Math.floor(clusterRoundTripTime / 10));
            if (rttClusterBucket >= m_clusterRoundTripTimeBuckets.length) {
                rttClusterBucket = m_clusterRoundTripTimeBuckets.length - 1;
            }
            m_clusterRoundTripTimeBuckets[rttClusterBucket] += 1;

        }
    }

    class NodeConnection extends VoltProtocolHandler implements org.voltdb.network.QueueMonitor {
        private final AtomicInteger m_callbacksToInvoke = new AtomicInteger(0);
        private final HashMap<Long, Object[]> m_callbacks;
        private final HashMap<String, ProcedureStats> m_stats
            = new HashMap<String, ProcedureStats>();
        private final int m_hostId;
        private final long m_connectionId;
        private Connection m_connection;
        private String m_hostname;
        private boolean m_isConnected = true;

        private long m_invocationsCompleted = 0;
        private long m_lastInvocationsCompleted = 0;
        private long m_invocationAborts = 0;
        private long m_lastInvocationAborts = 0;
        private long m_invocationErrors = 0;
        private long m_lastInvocationErrors = 0;

        public NodeConnection(long ids[]) {
            m_callbacks = new HashMap<Long, Object[]>();
            m_hostId = (int)ids[0];
            m_connectionId = ids[1];
        }

        public void createWork(long handle, String name, BBContainer c, ProcedureCallback callback) {
            synchronized (this) {
                if (!m_isConnected) {
                    final ClientResponse r = new ClientResponseImpl(
                            ClientResponse.CONNECTION_LOST, new VoltTable[0],
                            "Connection to database host (" + m_hostname +
                            ") was lost before a response was received");
                    try {
                        callback.clientCallback(r);
                    } catch (Exception e) {
                        uncaughtException(callback, r, e);
                    }
                    c.discard();
                    return;
                }
                m_callbacks.put(handle, new Object[] { System.currentTimeMillis(), callback, name });
                m_callbacksToInvoke.incrementAndGet();
            }
            m_connection.writeStream().enqueue(c);
        }

        public void createWork(long handle, String name, FastSerializable f, ProcedureCallback callback) {
            synchronized (this) {
                if (!m_isConnected) {
                    final ClientResponse r = new ClientResponseImpl(
                            ClientResponse.CONNECTION_LOST, new VoltTable[0],
                            "Connection to database host (" + m_hostname +
                            ") was lost before a response was received");
                    try {
                        callback.clientCallback(r);
                    } catch (Exception e) {
                        uncaughtException(callback, r, e);
                    }
                    return;
                }
                m_callbacks.put(handle, new Object[]{ System.currentTimeMillis(), callback, name });
                m_callbacksToInvoke.incrementAndGet();
            }
            m_connection.writeStream().enqueue(f);
        }

        /**
         * Update the procedures statistics
         * @param name Name of procedure being updated
         * @param roundTrip round trip from client queued to client response callback invocation
         * @param clusterRoundTrip round trip measured within the VoltDB cluster
         * @param abort true of the procedure was aborted
         * @param failure true if the procedure failed
         */
        private void updateStats(
                String name,
                int roundTrip,
                int clusterRoundTrip,
                boolean abort,
                boolean failure) {
            ProcedureStats stats = m_stats.get(name);
            if (stats == null) {
                stats = new ProcedureStats(name);
                m_stats.put( name, stats);
            }
            stats.update(roundTrip, clusterRoundTrip, abort, failure);
        }

        @Override
        public void handleMessage(ByteBuffer buf, Connection c) {
            long now = System.currentTimeMillis();
            ClientResponseImpl response = null;
            FastDeserializer fds = new FastDeserializer(buf);
            try {
                response = fds.readObject(ClientResponseImpl.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
            ProcedureCallback cb = null;
            long callTime = 0;
            int delta = 0;
            synchronized (this) {
                Object stuff[] = m_callbacks.remove(response.getClientHandle());

                callTime = (Long)stuff[0];
                delta = (int)(now - callTime);
                cb = (ProcedureCallback)stuff[1];
                m_invocationsCompleted++;
                final byte status = response.getStatus();
                boolean abort = false;
                boolean error = false;
                if (status == ClientResponse.USER_ABORT || status == ClientResponse.GRACEFUL_FAILURE) {
                    m_invocationAborts++;
                    abort = true;
                } else if (status != ClientResponse.SUCCESS) {
                    m_invocationErrors++;
                    error = true;
                }
                updateStats((String)stuff[2], delta, response.getClusterRoundtrip(), abort, error);
            }

            if (cb != null) {
                response.setClientRoundtrip(delta);
                try {
                    cb.clientCallback(response);
                } catch (Exception e) {
                    uncaughtException(cb, response, e);
                }
                m_callbacksToInvoke.decrementAndGet();
            }
            else if (m_isConnected) {
                // TODO: what's the right error path here?
                assert(false);
                System.err.println("Invalid response: no callback");
            }
        }

        /**
         * A number specify the expected size of the majority of outgoing messages.
         * Used to determine the tipping point between where a heap byte buffer vs. direct byte buffer will be
         * used. Also effects the usage of gathering writes.
         */
        @Override
        public int getExpectedOutgoingMessageSize() {
            return m_expectedOutgoingMessageSize;
        }

        @Override
        public int getMaxRead() {
            return Integer.MAX_VALUE;
        }

        public boolean hadBackPressure() {
            return m_connection.writeStream().hadBackPressure();
        }


        @Override
        public void stopping(Connection c) {
            super.stopping(c);
            synchronized (this) {
                //Prevent queueing of new work to this connection
                synchronized (Distributer.this) {
                    m_connections.remove(this);
                    //Notify listeners that a connection has been lost
                    for (ClientStatusListener s : m_listeners) {
                        s.connectionLost(m_hostname, m_connections.size());
                    }
                }
                m_isConnected = false;

                //Invoke callbacks for all queued invocations with a failure response
                final ClientResponse r =
                    new ClientResponseImpl(
                        ClientResponse.CONNECTION_LOST, new VoltTable[0],
                        "Connection to database host (" + m_hostname +
                        ") was lost before a response was received");
                for (final Object stuff[] : m_callbacks.values()) {
                    try {
                        ((ProcedureCallback)stuff[1]).clientCallback(r);
                    } catch (Exception e) {
                        uncaughtException(((ProcedureCallback)stuff[1]), r, e);
                    }
                    m_callbacksToInvoke.decrementAndGet();
                }
            }
        }

        @Override
        public Runnable offBackPressure() {
            return new Runnable() {
                @Override
                public void run() {
                    /*
                     * Synchronization on Distributer.this is critical to ensure that queue
                     * does not report backpressure AFTER the write stream reports that backpressure
                     * has ended thus resulting in a lost wakeup.
                     */
                    synchronized (Distributer.this) {
                        for (final ClientStatusListener csl : m_listeners) {
                            csl.backpressure(false);
                        }
                    }
                }
            };
        }

        @Override
        public Runnable onBackPressure() {
            return null;
        }

        @Override
        public QueueMonitor writestreamMonitor() {
            return this;
        }

        /**
         * Get counters for invocations completed, aborted, errors. In that order.
         */
        public synchronized long[] getCounters() {
            return new long[] { m_invocationsCompleted, m_invocationAborts, m_invocationErrors };
        }

        /**
         * Get counters for invocations completed, aborted, errors. In that order.
         * Count returns count since this method was last invoked
         */
        public synchronized long[] getCountersInterval() {
            final long invocationsCompletedThisTime = m_invocationsCompleted - m_lastInvocationsCompleted;
            m_lastInvocationsCompleted = m_invocationsCompleted;

            final long invocationsAbortsThisTime = m_invocationAborts - m_lastInvocationAborts;
            m_lastInvocationAborts = m_invocationAborts;

            final long invocationErrorsThisTime = m_invocationErrors - m_lastInvocationErrors;
            m_lastInvocationErrors = m_invocationErrors;
            return new long[] {
                    invocationsCompletedThisTime,
                    invocationsAbortsThisTime,
                    invocationErrorsThisTime
            };
        }

        private int m_queuedBytes = 0;
        private final int m_maxQueuedBytes = 262144;

        @Override
        public boolean queue(int bytes) {
            m_queuedBytes += bytes;
            if (m_queuedBytes > m_maxQueuedBytes) {
                return true;
            }
            return false;
        }
    }

    void drain() throws NoConnectionsException, InterruptedException {
        boolean more;
        do {
            more = false;
            synchronized (this) {
                for (NodeConnection cxn : m_connections) {
                    more = more || cxn.m_callbacksToInvoke.get() > 0;
                }
            }
            if (more) {
                Thread.sleep(5);
            }
        } while(more);

        synchronized (this) {
            for (NodeConnection cxn : m_connections ) {
                assert(cxn.m_callbacks.size() == 0);
            }
        }
    }

    Distributer() {
        this( 128, null, false, null);
    }

    Distributer(
            int expectedOutgoingMessageSize,
            int arenaSizes[],
            boolean useMultipleThreads,
            StatsUploaderSettings statsSettings) {
        if (statsSettings != null) {
            m_statsLoader = new ClientStatsLoader(statsSettings, this);
        } else {
            m_statsLoader = null;
        }
        m_useMultipleThreads = useMultipleThreads;
        m_network = new VoltNetwork( useMultipleThreads, true, 3, null);
        m_expectedOutgoingMessageSize = expectedOutgoingMessageSize;
        m_network.start();
        m_pool = new DBBPool(false, arenaSizes, false);
        m_hostname = ConnectionUtil.getHostnameOrAddress();;

//        new Thread() {
//            @Override
//            public void run() {
//                long lastBytesRead = 0;
//                long lastBytesWritten = 0;
//                long lastRuntime = System.currentTimeMillis();
//                try {
//                    while (true) {
//                        Thread.sleep(10000);
//                        final long now = System.currentTimeMillis();
//                        Map<Long, Pair<String, long[]>> stats = m_network.getIOStats(false);
//                        long statsNums[] = stats.get(-1).getSecond();
//                        final long read = statsNums[0];
//                        final long written = statsNums[1];
//                        final long readDelta = read - lastBytesRead;
//                        final long writeDelta = written - lastBytesWritten;
//                        final long timeDelta = now - lastRuntime;
//                        lastRuntime = now;
//                        final double seconds = timeDelta / 1000.0;
//                        final double megabytesRead = readDelta / (double)(1024 * 1024);
//                        final double megabytesWritten = writeDelta / (double)(1024 * 1024);
//                        final double readRate = megabytesRead / seconds;
//                        final double writeRate = megabytesWritten / seconds;
//                        lastBytesRead = read;
//                        lastBytesWritten = written;
//                        System.err.printf("Read rate %.2f Write rate %.2f\n", readRate, writeRate);
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }.start();
    }

    void createConnection(String host, String program, String password, int port)
        throws UnknownHostException, IOException
    {
        byte hashedPassword[] = ConnectionUtil.getHashedPassword(password);
        createConnectionWithHashedCredentials(host, program, hashedPassword, port);
    }

    synchronized void createConnectionWithHashedCredentials(String host, String program, byte[] hashedPassword, int port)
        throws UnknownHostException, IOException
    {
        final Object connectionStuff[] =
            ConnectionUtil.getAuthenticatedConnection(host, program, hashedPassword, port);
        final SocketChannel aChannel = (SocketChannel)connectionStuff[0];
        final long numbers[] = (long[])connectionStuff[1];
        if (m_clusterInstanceId == null) {
            long timestamp = numbers[2];
            int addr = (int)numbers[3];
            m_clusterInstanceId = new Object[] { timestamp, addr };
            if (m_statsLoader != null) {
                try {
                    m_statsLoader.start( timestamp, addr);
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
        } else {
            if (!(((Long)m_clusterInstanceId[0]).longValue() == numbers[2]) ||
                !(((Integer)m_clusterInstanceId[1]).longValue() == numbers[3])) {
                aChannel.close();
                throw new IOException(
                        "Cluster instance id mismatch. Current is " + m_clusterInstanceId[0] + "," + m_clusterInstanceId[1]
                        + " and server's was " + numbers[2] + "," + numbers[3]);
            }
        }
        m_buildString = (String)connectionStuff[2];
        NodeConnection cxn = new NodeConnection(numbers);
        m_connections.add(cxn);
        Connection c = m_network.registerChannel( aChannel, cxn);
        cxn.m_hostname = c.getHostnameOrIP();
        cxn.m_connection = c;
    }

//    private HashMap<String, Long> reportedSizes = new HashMap<String, Long>();

    /**
     * Queue invocation on first node connection without backpressure. If there is none with without backpressure
     * then return false and don't queue the invocation
     * @param invocation
     * @param cb
     * @param expectedSerializedSize
     * @param ignoreBackPressure If true the invocation will be queued even if there is backpressure
     * @return True if the message was queued and false if the message was not queued due to backpressure
     * @throws NoConnectionsException
     */
    boolean queue(
            ProcedureInvocation invocation,
            ProcedureCallback cb,
            int expectedSerializedSize,
            final boolean ignoreBackpressure)
        throws NoConnectionsException {
        NodeConnection cxn = null;
        boolean backpressure = true;
        /*
         * Synchronization is necessary to ensure that m_connections is not modified
         * as well as to ensure that backpressure is reported correctly
         */
        synchronized (this) {
            final int totalConnections = m_connections.size();

            if (totalConnections == 0) {
                throw new NoConnectionsException("No connections.");
            }

            for (int i=0; i < totalConnections; ++i) {
                cxn = m_connections.get(Math.abs(++m_nextConnection % totalConnections));
                if (!cxn.hadBackPressure() || ignoreBackpressure) {
                    // serialize and queue the invocation
                    backpressure = false;
                    break;
                }
            }

            if (backpressure) {
                cxn = null;
                for (ClientStatusListener s : m_listeners) {
                    s.backpressure(true);
                }
            }
        }

        /*
         * Do the heavy weight serialization outside the synchronized block.
         * createWork synchronizes on an individual connection which allows for more concurrency
         */
        if (cxn != null) {
            if (m_useMultipleThreads) {
                cxn.createWork(invocation.getHandle(), invocation.getProcName(), invocation, cb);
            } else {
                final FastSerializer fs = new FastSerializer(m_pool, expectedSerializedSize);
                BBContainer c = null;
                try {
                    c = fs.writeObjectForMessaging(invocation);
                } catch (IOException e) {
                    fs.getBBContainer().discard();
                    throw new RuntimeException(e);
                }
                cxn.createWork(invocation.getHandle(), invocation.getProcName(), c, cb);
            }
//            final String invocationName = invocation.getProcName();
//            if (reportedSizes.containsKey(invocationName)) {
//                if (reportedSizes.get(invocationName) < c.b.remaining()) {
//                    System.err.println("Queued invocation for " + invocationName + " is " + c.b.remaining() + " which is greater then last value of " + reportedSizes.get(invocationName));
//                    reportedSizes.put(invocationName, (long)c.b.remaining());
//                }
//            } else {
//                reportedSizes.put(invocationName, (long)c.b.remaining());
//                System.err.println("Queued invocation for " + invocationName + " is " + c.b.remaining());
//            }


        }

        return !backpressure;
    }

    /**
     * Shutdown the VoltNetwork allowing the Ports to close and free resources
     * like memory pools
     * @throws InterruptedException
     */
    final void shutdown() throws InterruptedException {
        if (m_statsLoader != null) {
            m_statsLoader.stop();
        }
        m_network.shutdown();
        synchronized (this) {
            m_pool.clear();
        }
    }

    private void uncaughtException(ProcedureCallback cb, ClientResponse r, Throwable t) {
        boolean handledByClient = false;
        for (ClientStatusListener csl : m_listeners) {
            if (csl instanceof ClientImpl.CSL) {
                continue;
            }
            try {
               csl.uncaughtException(cb, r, t);
               handledByClient = true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (!handledByClient) {
            t.printStackTrace();
        }
    }

    synchronized void addClientStatusListener(ClientStatusListener listener) {
        if (!m_listeners.contains(listener)) {
            m_listeners.add(listener);
        }
    }

    synchronized boolean removeClientStatusListener(ClientStatusListener listener) {
        return m_listeners.remove(listener);
    }

    private final ColumnInfo connectionStatsColumns[] = new ColumnInfo[] {
            new ColumnInfo( "TIMESTAMP", VoltType.BIGINT),
            new ColumnInfo( "HOSTNAME", VoltType.STRING),
            new ColumnInfo( "CONNECTION_ID", VoltType.BIGINT),
            new ColumnInfo( "SERVER_HOST_ID", VoltType.BIGINT),
            new ColumnInfo( "SERVER_HOSTNAME", VoltType.STRING),
            new ColumnInfo( "SERVER_CONNECTION_ID", VoltType.BIGINT),
            new ColumnInfo( "INVOCATIONS_COMPLETED", VoltType.BIGINT),
            new ColumnInfo( "INVOCATIONS_ABORTED", VoltType.BIGINT),
            new ColumnInfo( "INVOCATIONS_FAILED", VoltType.BIGINT),
            new ColumnInfo( "BYTES_READ", VoltType.BIGINT),
            new ColumnInfo( "MESSAGES_READ", VoltType.BIGINT),
            new ColumnInfo( "BYTES_WRITTEN", VoltType.BIGINT),
            new ColumnInfo( "MESSAGES_WRITTEN", VoltType.BIGINT)
    };

    private final ColumnInfo procedureStatsColumns[] = new ColumnInfo[] {
            new ColumnInfo( "TIMESTAMP", VoltType.BIGINT),
            new ColumnInfo( "HOSTNAME", VoltType.STRING),
            new ColumnInfo( "CONNECTION_ID", VoltType.BIGINT),
            new ColumnInfo( "SERVER_HOST_ID", VoltType.BIGINT),
            new ColumnInfo( "SERVER_HOSTNAME", VoltType.STRING),
            new ColumnInfo( "SERVER_CONNECTION_ID", VoltType.BIGINT),
            new ColumnInfo( "PROCEDURE_NAME", VoltType.STRING),
            new ColumnInfo( "ROUNDTRIPTIME_AVG", VoltType.INTEGER),
            new ColumnInfo( "ROUNDTRIPTIME_MIN", VoltType.INTEGER),
            new ColumnInfo( "ROUNDTRIPTIME_MAX", VoltType.INTEGER),
            new ColumnInfo( "CLUSTER_ROUNDTRIPTIME_AVG", VoltType.INTEGER),
            new ColumnInfo( "CLUSTER_ROUNDTRIPTIME_MIN", VoltType.INTEGER),
            new ColumnInfo( "CLUSTER_ROUNDTRIPTIME_MAX", VoltType.INTEGER),
            new ColumnInfo( "INVOCATIONS_COMPLETED", VoltType.BIGINT),
            new ColumnInfo( "INVOCATIONS_ABORTED", VoltType.BIGINT),
            new ColumnInfo( "INVOCATIONS_FAILED", VoltType.BIGINT),
    };

    private final ColumnInfo[] getRTTStatsColumns() {
        ColumnInfo[] ci = new ColumnInfo[ProcedureStats.m_numberOfBuckets + 7];
        ci[0] = new ColumnInfo( "TIMESTAMP", VoltType.BIGINT);
        ci[1] = new ColumnInfo( "HOSTNAME", VoltType.STRING);
        ci[2] = new ColumnInfo( "CONNECTION_ID", VoltType.BIGINT);
        ci[3] = new ColumnInfo( "SERVER_HOST_ID", VoltType.BIGINT);
        ci[4] = new ColumnInfo( "SERVER_HOSTNAME", VoltType.STRING);
        ci[5] = new ColumnInfo( "SERVER_CONNECTION_ID", VoltType.BIGINT);
        ci[6] = new ColumnInfo( "PROCEDURE_NAME", VoltType.STRING);
        for (int i=0; i < ProcedureStats.m_numberOfBuckets; i++) {
            String colName = (Integer.valueOf((i+1)*10)).toString() + "MS";
            ci[i+7] = new ColumnInfo(colName, VoltType.INTEGER);
        }
        return ci;
    }

    /** Query for latency buckets for client round trip time */
    VoltTable getClientRTTLatencies(final boolean interval) {
        return getRTTLatencies(true, interval);
    }

    /** Query for latency buckets for internal cluster round trip time */
    VoltTable getClusterRTTLatencies(final boolean interval) {
        return getRTTLatencies(false, interval);
    }

    /**
     * Query for latency bucket values by procedure by connection.
     * @param clientRTT true if client rtt desired. false if cluster rtt desired
     * @param interval Must be false. Interval not yet supported
     * @return
     */
    private VoltTable getRTTLatencies(final boolean clientRTT, final boolean interval) {
        if (interval == true) {
            throw new RuntimeException("Interval stats not implemented");
        }

        final Long now = System.currentTimeMillis();
        final VoltTable retval = new VoltTable(getRTTStatsColumns());

        synchronized(m_connections) {
            for (NodeConnection cxn : m_connections) {
                synchronized(cxn) {
                    for (ProcedureStats stats : cxn.m_stats.values()) {
                        long buckets[] =
                            clientRTT ? stats.m_roundTripTimeBuckets :
                                stats.m_clusterRoundTripTimeBuckets;
                        Object row[] = new Object[ProcedureStats.m_numberOfBuckets + 7];
                        row[0] = now;
                        row[1] = m_hostname;
                        row[2] = cxn.connectionId();
                        row[3] = cxn.m_hostId;
                        row[4] = cxn.m_hostname;
                        row[5] = cxn.m_connectionId;
                        row[6] = stats.m_name;
                        for (int i=0; i < ProcedureStats.m_numberOfBuckets; i++) {
                            row[i + 7] = buckets[i];
                        }
                        retval.addRow(row);
                    }
                }
            }
        }
        return retval;
    }

    VoltTable getProcedureStats(final boolean interval) {
        final Long now = System.currentTimeMillis();
        final VoltTable retval = new VoltTable(procedureStatsColumns);

        long totalInvocations = 0;
        long totalAbortedInvocations = 0;
        long totalFailedInvocations = 0;
        long totalRoundTripTime = 0;
        int totalRoundTripMax = Integer.MIN_VALUE;
        int totalRoundTripMin = Integer.MAX_VALUE;
        long totalClusterRoundTripTime = 0;
        int totalClusterRoundTripMax = Integer.MIN_VALUE;
        int totalClusterRoundTripMin = Integer.MAX_VALUE;
        synchronized (m_connections) {
            for (NodeConnection cxn : m_connections) {
                synchronized (cxn) {
                    for (ProcedureStats stats : cxn.m_stats.values()) {
                        long invocationsCompleted = stats.m_invocationsCompleted;
                        long invocationAborts = stats.m_invocationAborts;
                        long invocationErrors = stats.m_invocationErrors;
                        long roundTripTime = stats.m_roundTripTime;
                        int maxRoundTripTime = stats.m_maxRoundTripTime;
                        int minRoundTripTime = stats.m_minRoundTripTime;
                        long clusterRoundTripTime = stats.m_clusterRoundTripTime;
                        int clusterMinRoundTripTime = stats.m_minClusterRoundTripTime;
                        int clusterMaxRoundTripTime = stats.m_maxClusterRoundTripTime;

                        if (interval) {
                            invocationsCompleted = stats.m_invocationsCompleted - stats.m_lastInvocationsCompleted;
                            if (invocationsCompleted == 0) {
                                //No invocations since last interval
                                continue;
                            }
                            stats.m_lastInvocationsCompleted = stats.m_invocationsCompleted;

                            invocationAborts = stats.m_invocationAborts - stats.m_lastInvocationAborts;
                            stats.m_lastInvocationAborts = stats.m_invocationAborts;

                            invocationErrors = stats.m_invocationErrors - stats.m_lastInvocationErrors;
                            stats.m_lastInvocationErrors = stats.m_invocationErrors;

                            roundTripTime = stats.m_roundTripTime - stats.m_lastRoundTripTime;
                            stats.m_lastRoundTripTime = stats.m_roundTripTime;

                            maxRoundTripTime = stats.m_lastMaxRoundTripTime;
                            minRoundTripTime = stats.m_lastMinRoundTripTime;

                            stats.m_lastMaxRoundTripTime = Integer.MIN_VALUE;
                            stats.m_lastMinRoundTripTime = Integer.MAX_VALUE;

                            clusterRoundTripTime = stats.m_clusterRoundTripTime - stats.m_lastClusterRoundTripTime;
                            stats.m_lastClusterRoundTripTime = stats.m_clusterRoundTripTime;

                            clusterMaxRoundTripTime = stats.m_lastMaxClusterRoundTripTime;
                            clusterMinRoundTripTime = stats.m_lastMinClusterRoundTripTime;

                            stats.m_lastMaxClusterRoundTripTime = Integer.MIN_VALUE;
                            stats.m_lastMinClusterRoundTripTime = Integer.MAX_VALUE;
                        }
                        totalInvocations += invocationsCompleted;
                        totalAbortedInvocations += invocationAborts;
                        totalFailedInvocations += invocationErrors;
                        totalRoundTripTime += roundTripTime;
                        totalRoundTripMax = Math.max(maxRoundTripTime, totalRoundTripMax);
                        totalRoundTripMin = Math.min(minRoundTripTime, totalRoundTripMin);
                        totalClusterRoundTripTime += clusterRoundTripTime;
                        totalClusterRoundTripMax = Math.max(clusterMaxRoundTripTime, totalClusterRoundTripMax);
                        totalClusterRoundTripMin = Math.min(clusterMinRoundTripTime, totalClusterRoundTripMin);
                        retval.addRow(
                                now,
                                m_hostname,
                                cxn.connectionId(),
                                cxn.m_hostId,
                                cxn.m_hostname,
                                cxn.m_connectionId,
                                stats.m_name,
                                (int)(roundTripTime / invocationsCompleted),
                                minRoundTripTime,
                                maxRoundTripTime,
                                (int)(clusterRoundTripTime / invocationsCompleted),
                                clusterMinRoundTripTime,
                                clusterMaxRoundTripTime,
                                invocationsCompleted,
                                invocationAborts,
                                invocationErrors
                                );
                    }
                }
            }
        }
        return retval;
    }

    VoltTable getConnectionStats(final boolean interval) {
        final Long now = System.currentTimeMillis();
        final VoltTable retval = new VoltTable(connectionStatsColumns);
        final Map<Long, Pair<String,long[]>> networkStats =
                        m_network.getIOStats(interval);
        long totalInvocations = 0;
        long totalAbortedInvocations = 0;
        long totalFailedInvocations = 0;
        synchronized (m_connections) {
            for (NodeConnection cxn : m_connections) {
                synchronized (cxn) {
                    long counters[];
                    if (interval) {
                        counters = cxn.getCountersInterval();
                    } else {
                        counters = cxn.getCounters();
                    }
                    totalInvocations += counters[0];
                    totalAbortedInvocations += counters[1];
                    totalFailedInvocations += counters[2];
                    final long networkCounters[] = networkStats.get(cxn.connectionId()).getSecond();
                    final String hostname = networkStats.get(cxn.connectionId()).getFirst();
                    long bytesRead = 0;
                    long messagesRead = 0;
                    long bytesWritten = 0;
                    long messagesWritten = 0;
                    if (networkCounters != null) {
                        bytesRead = networkCounters[0];
                        messagesRead = networkCounters[1];
                        bytesWritten = networkCounters[2];
                        messagesWritten = networkCounters[3];
                    }

                    retval.addRow(
                            now,
                            m_hostname,
                            cxn.connectionId(),
                            cxn.m_hostId,
                            hostname,
                            cxn.m_connectionId,
                            counters[0],
                            counters[1],
                            counters[2],
                            bytesRead,
                            messagesRead,
                            bytesWritten,
                            messagesWritten);
                }
            }
        }

        final long globalIOStats[] = networkStats.get(-1L).getSecond();
        retval.addRow(
                now,
                m_hostname,
                -1,
                -1,
                "GLOBAL",
                -1,
                totalInvocations,
                totalAbortedInvocations,
                totalFailedInvocations,
                globalIOStats[0],
                globalIOStats[1],
                globalIOStats[2],
                globalIOStats[3]);
        return retval;
    }

    public Object[] getInstanceId() {
        return m_clusterInstanceId;
    }

    public String getBuildString() {
        return m_buildString;
    }

    public ArrayList<Long> getThreadIds() {
        return m_network.getThreadIds();
    }
}
