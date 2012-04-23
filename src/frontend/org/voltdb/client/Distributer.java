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
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientStatusListenerExt.DisconnectCause;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.network.Connection;
import org.voltdb.network.QueueMonitor;
import org.voltdb.network.VoltNetwork;
import org.voltdb.network.VoltProtocolHandler;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.Pair;

/**
 *   De/multiplexes transactions across a cluster
 *
 *   It is safe to synchronized on an individual connection and then the distributer, but it is always unsafe
 *   to synchronized on the distributer and then an individual connection.
 */
class Distributer {

    static final long PING_HANDLE = Long.MAX_VALUE;

    // collection of connections to the cluster
    private final ArrayList<NodeConnection> m_connections = new ArrayList<NodeConnection>();

    private final ArrayList<ClientStatusListenerExt> m_listeners = new ArrayList<ClientStatusListenerExt>();

    //Selector and connection handling, does all work in blocking selection thread
    private final VoltNetwork m_network;

    // Temporary until a distribution/affinity algorithm is written
    private int m_nextConnection = 0;

    private final int m_expectedOutgoingMessageSize;

    private final DBBPool m_pool;

    private final boolean m_useMultipleThreads;

    // timeout for individual procedure calls
    private final long m_procedureCallTimeoutMS;
    private final long m_connectionResponseTimeoutMS;

    public final RateLimiter m_rateLimiter = new RateLimiter();

    //private final Timer m_timer;
    private final ScheduledExecutorService m_ex =
            Executors.newSingleThreadScheduledExecutor(
                    MiscUtils.getThreadFactory("VoltDB Client Reaper Thread"));
    ScheduledFuture<?> m_timeoutReaperHandle;

    /**
     * Server's instances id. Unique for the cluster
     */
    private Object m_clusterInstanceId[];

    private String m_buildString;

    class CallExpiration implements Runnable {
        @Override
        public void run() {

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
                    for (Entry<Long, CallbackBookeeping> e : c.m_callbacks.entrySet()) {
                        long handle = e.getKey();
                        CallbackBookeeping cb = e.getValue();

                        // if the timeout is expired, call the callback and remove the
                        // bookeeping data
                        if ((now - cb.timestamp) > m_procedureCallTimeoutMS) {
                            ClientResponseImpl r = new ClientResponseImpl(
                                    ClientResponse.CONNECTION_TIMEOUT,
                                    (byte)0,
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
                            c.m_callbacks.remove(e.getKey());
                            c.m_callbacksToInvoke.decrementAndGet();
                        }
                    }
                }
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

    class NodeConnection extends VoltProtocolHandler implements org.voltdb.network.QueueMonitor {
        private final AtomicInteger m_callbacksToInvoke = new AtomicInteger(0);
        private final HashMap<Long, CallbackBookeeping> m_callbacks;
        private final HashMap<String, ClientStats> m_stats = new HashMap<String, ClientStats>();
        private final long m_connectionId;
        private Connection m_connection;
        private String m_hostname;
        private int m_port;
        private boolean m_isConnected = true;

        long m_lastResponseTime = System.currentTimeMillis();
        boolean m_outstandingPing = false;
        ClientStatusListenerExt.DisconnectCause m_closeCause = DisconnectCause.CONNECTION_CLOSED;

        public NodeConnection(long ids[]) {
            m_callbacks = new HashMap<Long, CallbackBookeeping>();
            m_connectionId = ids[1];
        }

        public void createWork(long handle, String name, BBContainer c,
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
                    c.discard();
                    return;
                }

                m_callbacks.put(handle, new CallbackBookeeping(now, callback, name));
                m_callbacksToInvoke.incrementAndGet();
            }
            m_connection.writeStream().enqueue(c);
        }

        public void createWork(long handle, String name, FastSerializable f,
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
            m_connection.writeStream().enqueue(f);
        }

        void sendPing() {
            ProcedureInvocation invocation = new ProcedureInvocation(PING_HANDLE, "@Ping");
            final FastSerializer fs = new FastSerializer(m_pool, 128);
            BBContainer c = null;
            try {
                c = fs.writeObjectForMessaging(invocation);
            } catch (IOException e) {
                fs.getBBContainer().discard();
                throw new RuntimeException(e);
            }
            m_connection.writeStream().enqueue(c);
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

            if (cb != null) {
                response.setClientRoundtrip(delta);
                try {
                    cb.clientCallback(response);
                } catch (Exception e) {
                    uncaughtException(cb, response, e);
                }
                m_callbacksToInvoke.decrementAndGet();
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
                    for (ClientStatusListenerExt s : m_listeners) {
                        s.connectionLost(m_hostname, m_port, m_connections.size(), m_closeCause);
                    }
                }
                m_isConnected = false;

                //Invoke callbacks for all queued invocations with a failure response
                final ClientResponse r =
                    new ClientResponseImpl(
                        ClientResponse.CONNECTION_LOST, new VoltTable[0],
                        "Connection to database host (" + m_hostname +
                        ") was lost before a response was received");
                for (final CallbackBookeeping callBk : m_callbacks.values()) {
                    try {
                        callBk.callback.clientCallback(r);
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
        this(128, null, false,
                ClientConfig.DEFAULT_PROCEDURE_TIMOUT_MS,
                ClientConfig.DEFAULT_CONNECTION_TIMOUT_MS);
    }

    Distributer(
            int expectedOutgoingMessageSize,
            int arenaSizes[],
            boolean useMultipleThreads,
            long procedureCallTimeoutMS,
            long connectionResponseTimeoutMS) {
        m_useMultipleThreads = useMultipleThreads;
        m_network = new VoltNetwork( useMultipleThreads, true, 3, null);
        m_expectedOutgoingMessageSize = expectedOutgoingMessageSize;
        m_network.start();
        m_pool = new DBBPool(false, arenaSizes, false);
        m_procedureCallTimeoutMS = procedureCallTimeoutMS;
        m_connectionResponseTimeoutMS = connectionResponseTimeoutMS;

        // schedule the task that looks for timed-out proc calls and connections
        m_timeoutReaperHandle = m_ex.scheduleAtFixedRate(new CallExpiration(), 1, 1, TimeUnit.SECONDS);
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
        cxn.m_port = port;
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
            if (m_useMultipleThreads) {
                cxn.createWork(invocation.getHandle(), invocation.getProcName(),
                        invocation, cb, ignoreBackpressure);
            } else {
                final FastSerializer fs = new FastSerializer(m_pool, expectedSerializedSize);
                BBContainer c = null;
                try {
                    c = fs.writeObjectForMessaging(invocation);
                } catch (IOException e) {
                    fs.getBBContainer().discard();
                    throw new RuntimeException(e);
                }
                cxn.createWork(invocation.getHandle(), invocation.getProcName(),
                        c, cb, ignoreBackpressure);
            }
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
        synchronized (this) {
            m_pool.clear();
        }
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

        synchronized (this) {
            for (NodeConnection conn : m_connections) {
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

        synchronized (this) {
            Map<Long, Pair<String, long[]>> ioStats = m_network.getIOStats(false);
            for (NodeConnection conn : m_connections) {
                Pair<String, long[]> perConnIOStats = ioStats.get(conn.connectionId());
                if (perConnIOStats == null) continue;

                long read = perConnIOStats.getSecond()[0];
                long write = perConnIOStats.getSecond()[2];

                ClientIOStats cios = new ClientIOStats(conn.connectionId(), read, write);
                retval.put(conn.connectionId(), cios);
            }
        }

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
