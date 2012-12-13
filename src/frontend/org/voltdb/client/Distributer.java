/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jsr166y.ThreadLocalRandom;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.network.Connection;
import org.voltcore.network.QueueMonitor;
import org.voltcore.network.VoltNetworkPool;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.ClientResponseImpl;
import org.voltdb.JdbcDatabaseMetaDataGenerator;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientStatusListenerExt.DisconnectCause;

/**
 *   De/multiplexes transactions across a cluster
 *
 *   It is safe to synchronized on an individual connection and then the distributer, but it is always unsafe
 *   to synchronized on the distributer and then an individual connection.
 */
class Distributer {

    static final long PING_HANDLE = Long.MAX_VALUE;
    static final long TOPOLOGY_HANDLE = Long.MAX_VALUE - 1;
    static final long PROCEDURE_HANDLE = Long.MAX_VALUE - 2;

    // collection of connections to the cluster
    private final CopyOnWriteArrayList<NodeConnection> m_connections =
            new CopyOnWriteArrayList<NodeConnection>();

    private final ArrayList<ClientStatusListenerExt> m_listeners = new ArrayList<ClientStatusListenerExt>();

    //Selector and connection handling, does all work in blocking selection thread
    private final VoltNetworkPool m_network;

    // Temporary until a distribution/affinity algorithm is written
    private int m_nextConnection = 0;

    private final boolean m_useMultipleThreads;
    private final boolean m_useClientAffinity;

    private static final class Procedure {
        private final boolean readOnly;
        private final int partitionParameter;
        private Procedure(boolean readOnly, int partitionParameter) {
            this.readOnly = readOnly;
            this.partitionParameter = partitionParameter;
        }
    }

    private final Map<Integer, NodeConnection> m_partitionMasters = new HashMap<Integer, NodeConnection>();
    private final Map<Integer, NodeConnection[]> m_partitionReplicas = new HashMap<Integer, NodeConnection[]>();
    private final Map<Integer, NodeConnection> m_hostIdToConnection = new HashMap<Integer, NodeConnection>();
    private final Map<String, Procedure> m_procedureInfo = new HashMap<String, Procedure>();

    private boolean m_hashinatorInitialized = false;

    // timeout for individual procedure calls
    private final long m_procedureCallTimeoutMS;
    private final long m_connectionResponseTimeoutMS;

    public final RateLimiter m_rateLimiter = new RateLimiter();

    //private final Timer m_timer;
    private final ScheduledExecutorService m_ex =
        Executors.newSingleThreadScheduledExecutor(
                CoreUtils.getThreadFactory("VoltDB Client Reaper Thread"));
    ScheduledFuture<?> m_timeoutReaperHandle;

    /**
     * Server's instances id. Unique for the cluster
     */
    private Object m_clusterInstanceId[];

    private String m_buildString;

    class CallExpiration implements Runnable {
        @Override
        public void run() {
            try {
                // make a threadsafe copy of all connections
                ArrayList<NodeConnection> connections = new ArrayList<NodeConnection>();
                synchronized (Distributer.this) {
                    connections.addAll(m_connections);
                }

                long now = System.currentTimeMillis();

                // for each connection
                for (NodeConnection c : connections) {
                    synchronized(c) {
                        // check for connection age
                        long sinceLastResponse = now - c.m_lastResponseTime;

                        // if outstanding ping and timeout, close the connection
                        if (c.m_outstandingPing && (sinceLastResponse > m_connectionResponseTimeoutMS)) {
                            // memoize why it's closing
                            c.m_closeCause = DisconnectCause.TIMEOUT;
                            // this should trigger NodeConnection.stopping(..)
                            c.m_connection.unregister();
                        }

                        // if 1/3 of the timeout since last response, send a ping
                        if ((!c.m_outstandingPing) && (sinceLastResponse > (m_connectionResponseTimeoutMS / 3))) {
                            c.sendPing();
                        }

                        // for each outstanding procedure
                        Iterator<Entry<Long, CallbackBookeeping>> iter = c.m_callbacks.entrySet().iterator();
                        while (iter.hasNext()) {
                            Entry<Long, CallbackBookeeping> e = iter.next();
                            long handle = e.getKey();
                            CallbackBookeeping cb = e.getValue();

                            // if the timeout is expired, call the callback and remove the
                            // bookeeping data
                            if ((now - cb.timestamp) > m_procedureCallTimeoutMS) {
                                ClientResponseImpl r = new ClientResponseImpl(
                                        ClientResponse.CONNECTION_TIMEOUT,
                                        ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                                        "",
                                        new VoltTable[0],
                                        String.format("No response received in the allotted time (set to %d ms).",
                                                m_procedureCallTimeoutMS));
                                r.setClientHandle(handle);
                                r.setClientRoundtrip((int) (now - cb.timestamp));
                                r.setClusterRoundtrip((int) (now - cb.timestamp));

                                try {
                                    cb.callback.clientCallback(r);
                                } catch (Exception e1) {
                                    e1.printStackTrace();
                                }
                                iter.remove();
                                c.m_callbacksToInvoke.decrementAndGet();
                            }
                        }
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    class CallbackBookeeping {
        public CallbackBookeeping(long timestamp, ProcedureCallback callback, String name) {
            this.timestamp = timestamp;
            this.callback = callback;
            this.name = name;
        }
        long timestamp;
        ProcedureCallback callback;
        String name;
    }

    class NodeConnection extends VoltProtocolHandler implements org.voltcore.network.QueueMonitor {
        private final AtomicInteger m_callbacksToInvoke = new AtomicInteger(0);
        private final HashMap<Long, CallbackBookeeping> m_callbacks;
        private final HashMap<String, ClientStats> m_stats = new HashMap<String, ClientStats>();
        private Connection m_connection;
        private final InetSocketAddress m_socketAddress;
        private String m_hostname;
        private int m_port;
        private boolean m_isConnected = true;

        long m_lastResponseTime = System.currentTimeMillis();
        boolean m_outstandingPing = false;
        ClientStatusListenerExt.DisconnectCause m_closeCause = DisconnectCause.CONNECTION_CLOSED;

        public NodeConnection(long ids[], InetSocketAddress socketAddress) {
            m_callbacks = new HashMap<Long, CallbackBookeeping>();
            m_socketAddress = socketAddress;
        }

        public void createWork(long handle, String name, ByteBuffer c,
                ProcedureCallback callback, boolean ignoreBackpressure) {
            long now = System.currentTimeMillis();
            now = m_rateLimiter.sendTxnWithOptionalBlockAndReturnCurrentTime(
                    now, ignoreBackpressure);
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

                m_callbacks.put(handle, new CallbackBookeeping(now, callback, name));
                m_callbacksToInvoke.incrementAndGet();
            }
            m_connection.writeStream().enqueue(c);
        }

        void sendPing() {
            ProcedureInvocation invocation = new ProcedureInvocation(PING_HANDLE, "@Ping");
            ByteBuffer buf = ByteBuffer.allocate(4 + invocation.getSerializedSize());
            buf.putInt(buf.capacity() - 4);
            try {
                invocation.flattenToBuffer(buf);
                buf.flip();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            m_connection.writeStream().enqueue(buf);
            m_outstandingPing = true;
        }

        /**
         * Update the procedures statistics
         * @param procName Name of procedure being updated
         * @param roundTrip round trip from client queued to client response callback invocation
         * @param clusterRoundTrip round trip measured within the VoltDB cluster
         * @param abort true of the procedure was aborted
         * @param failure true if the procedure failed
         */
        private void updateStats(
                String procName,
                int roundTrip,
                int clusterRoundTrip,
                boolean abort,
                boolean failure) {
            ClientStats stats = m_stats.get(procName);
            if (stats == null) {
                stats = new ClientStats();
                stats.m_connectionId = connectionId();
                stats.m_hostname = m_hostname;
                stats.m_port = m_port;
                stats.m_procName = procName;
                stats.m_startTS = System.currentTimeMillis();
                stats.m_endTS = Long.MIN_VALUE;
                m_stats.put(procName, stats);
            }
            stats.update(roundTrip, clusterRoundTrip, abort, failure);
        }

        @Override
        public void handleMessage(ByteBuffer buf, Connection c) {
            long now = System.currentTimeMillis();
            ClientResponseImpl response = new ClientResponseImpl();
            try {
                response.initFromBuffer(buf);
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            ProcedureCallback cb = null;
            long callTime = 0;
            int delta = 0;
            synchronized (this) {
                // track the timestamp of the most recent read on this connection
                m_lastResponseTime = now;

                // handle ping response and get out
                if (response.getClientHandle() == PING_HANDLE) {
                    m_outstandingPing = false;
                    return;
                }

                CallbackBookeeping stuff = m_callbacks.remove(response.getClientHandle());
                // presumably (hopefully) this is a response for a timed-out message
                if (stuff == null) {
                    for (ClientStatusListenerExt listener : m_listeners) {
                        listener.lateProcedureResponse(response, m_hostname, m_port);
                    }
                }
                // handle a proper callback
                else {
                    callTime = stuff.timestamp;
                    delta = (int)(now - callTime);
                    cb = stuff.callback;
                    final byte status = response.getStatus();
                    boolean abort = false;
                    boolean error = false;
                    if (status == ClientResponse.USER_ABORT || status == ClientResponse.GRACEFUL_FAILURE) {
                        abort = true;
                    } else if (status != ClientResponse.SUCCESS) {
                        error = true;
                    }
                    int clusterRoundTrip = response.getClusterRoundtrip();
                    m_rateLimiter.transactionResponseReceived(now, clusterRoundTrip);
                    updateStats(stuff.name, delta, clusterRoundTrip, abort, error);
                }
            }

            try {
                if (response.getClientHandle() == TOPOLOGY_HANDLE) {
                    m_callbacksToInvoke.decrementAndGet();
                    synchronized (Distributer.this) {
                        VoltTable results[] = response.getResults();
                        if (results != null && results.length == 1) {
                            VoltTable vt = results[0];
                            updateAffinityTopology(vt);
                        }
                    }
                } else if (response.getClientHandle() == PROCEDURE_HANDLE) {
                    m_callbacksToInvoke.decrementAndGet();
                    synchronized (Distributer.this) {
                        VoltTable results[] = response.getResults();
                        if (results != null && results.length == 1) {
                            VoltTable vt = results[0];
                            updateProcedurePartitioning(vt);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (cb != null) {
                response.setClientRoundtrip(delta);
                assert(response.getHash() == null); // make sure it didn't sneak into wire protocol
                try {
                    cb.clientCallback(response);
                } catch (Exception e) {
                    uncaughtException(cb, response, e);
                }
                m_callbacksToInvoke.decrementAndGet();
            }
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
                    /*
                     * Repair all cluster topology data with the node connection removed
                     */
                    Iterator<Map.Entry<Integer, NodeConnection>> i = m_partitionMasters.entrySet().iterator();
                    while (i.hasNext()) {
                        Map.Entry<Integer, NodeConnection> entry = i.next();
                        if (entry.getValue() == this) {
                            i.remove();
                        }
                    }

                    i = m_hostIdToConnection.entrySet().iterator();
                    while (i.hasNext()) {
                        Map.Entry<Integer, NodeConnection> entry = i.next();
                        if (entry.getValue() == this) {
                            i.remove();
                        }
                    }

                    Iterator<Map.Entry<Integer, NodeConnection[]>> i2 = m_partitionReplicas.entrySet().iterator();
                    List<Pair<Integer, NodeConnection[]>> entriesToRewrite = new ArrayList<Pair<Integer, NodeConnection[]>>();
                    while (i2.hasNext()) {
                        Map.Entry<Integer, NodeConnection[]> entry = i2.next();
                        for (NodeConnection nc : entry.getValue()) {
                            if (nc == this) {
                                entriesToRewrite.add(Pair.of(entry.getKey(), entry.getValue()));
                            }
                        }
                    }

                    for (Pair<Integer, NodeConnection[]> entry : entriesToRewrite) {
                        m_partitionReplicas.remove(entry.getFirst());
                        NodeConnection survivors[] = new NodeConnection[entry.getSecond().length - 1];
                        if (survivors.length == 0) break;
                        int zz = 0;
                        for (int ii = 0; ii < entry.getSecond().length; ii++) {
                            if (entry.getSecond()[ii] != this) {
                                survivors[zz++] = entry.getSecond()[ii];
                            }
                        }
                        m_partitionReplicas.put(entry.getFirst(), survivors);
                    }

                    m_connections.remove(this);
                    //Notify listeners that a connection has been lost
                    for (ClientStatusListenerExt s : m_listeners) {
                        s.connectionLost(m_hostname, m_port, m_connections.size(), m_closeCause);
                    }
                }
                m_isConnected = false;

                //Invoke callbacks for all queued invocations with a failure response
                final ClientResponse r =
                    new ClientResponseImpl(
                            ClientResponse.CONNECTION_LOST, new VoltTable[0],
                            "Connection to database host (" + m_socketAddress +
                    ") was lost before a response was received");
                for (final CallbackBookeeping callBk : m_callbacks.values()) {
                    try {
                        //Client affinity doesn't register callbacks so you can have an entry
                        //with a null callback
                        if (callBk.callback != null) {
                            callBk.callback.clientCallback(r);
                        }
                    } catch (Exception e) {
                        uncaughtException(callBk.callback, r, e);
                    }
                    m_rateLimiter.transactionResponseReceived(System.currentTimeMillis(), -1);
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
                        for (final ClientStatusListenerExt csl : m_listeners) {
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

    void drain() throws InterruptedException {
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
        this( false,
                ClientConfig.DEFAULT_PROCEDURE_TIMOUT_MS,
                ClientConfig.DEFAULT_CONNECTION_TIMOUT_MS,
                false);
    }

    Distributer(
            boolean useMultipleThreads,
            long procedureCallTimeoutMS,
            long connectionResponseTimeoutMS,
            boolean useClientAffinity) {
        m_useMultipleThreads = useMultipleThreads;
        m_network = new VoltNetworkPool(
                m_useMultipleThreads ? Math.max(2, CoreUtils.availableProcessors()) / 4 : 1);
        m_network.start();
        m_procedureCallTimeoutMS = procedureCallTimeoutMS;
        m_connectionResponseTimeoutMS = connectionResponseTimeoutMS;
        m_useClientAffinity = useClientAffinity;

        // schedule the task that looks for timed-out proc calls and connections
        m_timeoutReaperHandle = m_ex.scheduleAtFixedRate(new CallExpiration(), 1, 1, TimeUnit.SECONDS);
    }

    void createConnection(String host, String program, String password, int port)
    throws UnknownHostException, IOException
    {
        byte hashedPassword[] = ConnectionUtil.getHashedPassword(password);
        createConnectionWithHashedCredentials(host, program, hashedPassword, port);
    }

    void createConnectionWithHashedCredentials(String host, String program, byte[] hashedPassword, int port)
    throws UnknownHostException, IOException
    {
        final Object socketChannelAndInstanceIdAndBuildString[] =
            ConnectionUtil.getAuthenticatedConnection(host, program, hashedPassword, port);
        InetSocketAddress address = new InetSocketAddress(host, port);
        final SocketChannel aChannel = (SocketChannel)socketChannelAndInstanceIdAndBuildString[0];
        final long instanceIdWhichIsTimestampAndLeaderIp[] = (long[])socketChannelAndInstanceIdAndBuildString[1];
        final int hostId = (int)instanceIdWhichIsTimestampAndLeaderIp[0];
        synchronized (this) {
            if (m_clusterInstanceId == null) {
                long timestamp = instanceIdWhichIsTimestampAndLeaderIp[2];
                int addr = (int)instanceIdWhichIsTimestampAndLeaderIp[3];
                m_clusterInstanceId = new Object[] { timestamp, addr };
            } else {
                if (!(((Long)m_clusterInstanceId[0]).longValue() == instanceIdWhichIsTimestampAndLeaderIp[2]) ||
                        !(((Integer)m_clusterInstanceId[1]).longValue() == instanceIdWhichIsTimestampAndLeaderIp[3])) {
                    aChannel.close();
                    throw new IOException(
                            "Cluster instance id mismatch. Current is " + m_clusterInstanceId[0] + "," + m_clusterInstanceId[1]
                                                                                                                             + " and server's was " + instanceIdWhichIsTimestampAndLeaderIp[2] + "," + instanceIdWhichIsTimestampAndLeaderIp[3]);
                }
            }
            m_buildString = (String)socketChannelAndInstanceIdAndBuildString[2];
        }
        NodeConnection cxn = new NodeConnection(instanceIdWhichIsTimestampAndLeaderIp, address);

        Connection c = m_network.registerChannel( aChannel, cxn);
        cxn.m_hostname = c.getHostnameOrIP();
        cxn.m_port = port;
        cxn.m_connection = c;
        m_connections.add(cxn);

        synchronized (this) {
            if (m_useClientAffinity) {
                ProcedureInvocation spi = new ProcedureInvocation( TOPOLOGY_HANDLE, "@Statistics", "TOPO", 0);
                //The handle is specific to topology updates and has special cased handling
                queue(spi, null, true);

                spi = new ProcedureInvocation( PROCEDURE_HANDLE, "@SystemCatalog", "PROCEDURES");
                //The handle is specific to procedure updates and has special cased handling
                queue(spi, null, true);
                m_hostIdToConnection.put(hostId, cxn);
            }
        }
    }

    //    private HashMap<String, Long> reportedSizes = new HashMap<String, Long>();

    /**
     * Queue invocation on first node connection without backpressure. If there is none with without backpressure
     * then return false and don't queue the invocation
     * @param invocation
     * @param cb
     * @param ignoreBackPressure If true the invocation will be queued even if there is backpressure
     * @return True if the message was queued and false if the message was not queued due to backpressure
     * @throws NoConnectionsException
     */
    boolean queue(
            ProcedureInvocation invocation,
            ProcedureCallback cb,
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

            /*
             * Check if the master for the partition is known. No back pressure check to ensure correct
             * routing, but backpressure will be managed anyways.
             */
            if (m_useClientAffinity && m_hashinatorInitialized) {
                final Procedure procedureInfo = m_procedureInfo.get(invocation.getProcName());
                if (procedureInfo != null) {
                    Integer hashedPartition = invocation.getHashinatedParam(procedureInfo.partitionParameter);

                    /*
                     * If the procedure is read only, load balance across replicas
                     */
                    if (procedureInfo.readOnly) {
                        NodeConnection partitionReplicas[] = m_partitionReplicas.get(hashedPartition);
                        if (partitionReplicas != null && partitionReplicas.length > 0) {
                            cxn = partitionReplicas[ThreadLocalRandom.current().nextInt(partitionReplicas.length)];
                            if (cxn.hadBackPressure()) {
                                //See if there is one without backpressure
                                for (NodeConnection nc : partitionReplicas) {
                                    if (!nc.hadBackPressure()) {
                                        cxn = nc;
                                        break;
                                    }
                                }
                            }
                            if (!cxn.hadBackPressure() || ignoreBackpressure) {
                                backpressure = false;
                            }
                        }
                    } else {
                        /*
                         * Writes have to go to the master
                         */
                        cxn = m_partitionMasters.get(hashedPartition);
                        if (cxn != null && !cxn.hadBackPressure() || ignoreBackpressure) {
                            backpressure = false;
                        }
                    }
                }
            }
            if (cxn == null) {
                for (int i=0; i < totalConnections; ++i) {
                    cxn = m_connections.get(Math.abs(++m_nextConnection % totalConnections));
                    if (!cxn.hadBackPressure() || ignoreBackpressure) {
                        // serialize and queue the invocation
                        backpressure = false;
                        break;
                    }
                }
            }

            if (backpressure) {
                cxn = null;
                for (ClientStatusListenerExt s : m_listeners) {
                    s.backpressure(true);
                }
            }
        }

        /*
         * Do the heavy weight serialization outside the synchronized block.
         * createWork synchronizes on an individual connection which allows for more concurrency
         */
        if (cxn != null) {
            ByteBuffer buf = ByteBuffer.allocate(4 + invocation.getSerializedSize());
            buf.putInt(buf.capacity() - 4);
            try {
                invocation.flattenToBuffer(buf);
                buf.flip();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            cxn.createWork(invocation.getHandle(), invocation.getProcName(), buf, cb, ignoreBackpressure);
        }

        return !backpressure;
    }

    /**
     * Shutdown the VoltNetwork allowing the Ports to close and free resources
     * like memory pools
     * @throws InterruptedException
     */
    final void shutdown() throws InterruptedException {
        // stop the old proc call reaper
        m_timeoutReaperHandle.cancel(false);
        m_ex.shutdown();
        m_ex.awaitTermination(1, TimeUnit.SECONDS);

        m_network.shutdown();
    }

    private void uncaughtException(ProcedureCallback cb, ClientResponse r, Throwable t) {
        boolean handledByClient = false;
        for (ClientStatusListenerExt csl : m_listeners) {
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

    synchronized void addClientStatusListener(ClientStatusListenerExt listener) {
        if (!m_listeners.contains(listener)) {
            m_listeners.add(listener);
        }
    }

    synchronized boolean removeClientStatusListener(ClientStatusListenerExt listener) {
        return m_listeners.remove(listener);
    }

    ClientStatsContext createStatsContext() {
        return new ClientStatsContext(this, getStatsSnapshot(), getIOStatsSnapshot());
    }

    Map<Long, Map<String, ClientStats>> getStatsSnapshot() {
        Map<Long, Map<String, ClientStats>> retval =
                new TreeMap<Long, Map<String, ClientStats>>();

            for (NodeConnection conn : m_connections) {
                synchronized (conn) {
                    Map<String, ClientStats> connMap = new TreeMap<String, ClientStats>();
                    for (Entry<String, ClientStats> e : conn.m_stats.entrySet()) {
                        connMap.put(e.getKey(), (ClientStats) e.getValue().clone());
                    }
                    retval.put(conn.connectionId(), connMap);
                }
            }


        return retval;
    }

    Map<Long, ClientIOStats> getIOStatsSnapshot() {
        Map<Long, ClientIOStats> retval = new TreeMap<Long, ClientIOStats>();

        Map<Long, Pair<String, long[]>> ioStats;
        try {
            ioStats = m_network.getIOStats(false);
        } catch (Exception e) {
            return null;
        }

        for (NodeConnection conn : m_connections) {
            Pair<String, long[]> perConnIOStats = ioStats.get(conn.connectionId());
            if (perConnIOStats == null) continue;

            long read = perConnIOStats.getSecond()[0];
            long write = perConnIOStats.getSecond()[2];

            ClientIOStats cios = new ClientIOStats(conn.connectionId(), read, write);
            retval.put(conn.connectionId(), cios);
        }

        return retval;
    }

    public Object[] getInstanceId() {
        return m_clusterInstanceId;
    }

    public String getBuildString() {
        return m_buildString;
    }

    public List<Long> getThreadIds() {
        return m_network.getThreadIds();
    }

    private void updateAffinityTopology(VoltTable vt) {
        // We're going to get the MPI back in this table, so subtract it out from the number of partitions.
        int numPartitions = vt.getRowCount() - 1;
        TheHashinator.initialize(numPartitions);
        m_hashinatorInitialized = true;
        m_partitionMasters.clear();
        m_partitionReplicas.clear();
        // The MPI's partition ID is 16383 (MpInitiator.MP_INIT_PID), so we shouldn't inadvertently
        // hash to it.  Go ahead and include it in the maps, we can use it at some point to
        // route MP transactions directly to the MPI node.
        while (vt.advanceRow()) {
            Integer partition = (int)vt.getLong("Partition");

            ArrayList<NodeConnection> connections = new ArrayList<NodeConnection>();
            for (String site : vt.getString("Sites").split(",")) {
                site = site.trim();
                Integer hostId = Integer.valueOf(site.split(":")[0]);
                if (m_hostIdToConnection.containsKey(hostId)) {
                    connections.add(m_hostIdToConnection.get(hostId));
                }
            }
            m_partitionReplicas.put(partition, connections.toArray(new NodeConnection[0]));

            Integer leaderHostId = Integer.valueOf(vt.getString("Leader").split(":")[0]);
            if (m_hostIdToConnection.containsKey(leaderHostId)) {
                m_partitionMasters.put(partition, m_hostIdToConnection.get(leaderHostId));
            }
        }
    }

    private void updateProcedurePartitioning(VoltTable vt) {
        m_procedureInfo.clear();
        while (vt.advanceRow()) {
            try {
                //Data embedded in JSON object in remarks column
                String jsString = vt.getString(6);
                JSONObject jsObj = new JSONObject(jsString);
                if (jsObj.getBoolean(JdbcDatabaseMetaDataGenerator.JSON_SINGLE_PARTITION)) {
                    int partitionParameter = jsObj.getInt(JdbcDatabaseMetaDataGenerator.JSON_PARTITION_PARAMETER);
                    boolean readOnly = jsObj.getBoolean(JdbcDatabaseMetaDataGenerator.JSON_READ_ONLY);
                    String procedureName = vt.getString(2);
                    m_procedureInfo.put(procedureName, new Procedure(readOnly, partitionParameter));
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }
}
