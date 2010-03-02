/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.network.Connection;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.network.QueueMonitor;
import org.voltdb.network.VoltNetwork;
import org.voltdb.network.VoltProtocolHandler;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;

/**
 *   De/multiplexes transactions across a cluster
 *
 *   It is safe to synchronized on an individual connection and then the distributer, but it is always unsafe
 *   to synchronized on the distributer and then an individual connection.
 */
class Distributer {

    private final ProcedureCallback.TimingContext m_context = new ProcedureCallback.TimingContext();

    // collection of connections to the cluster
    private final ArrayList<NodeConnection> m_connections = new ArrayList<NodeConnection>();

    private final ArrayList<ClientStatusListener> m_listeners = new ArrayList<ClientStatusListener>();

    //Selector and connection handling, does all work in blocking selection thread
    private VoltNetwork m_network;

    // Temporary until a distribution/affinity algorithm is written
    private int m_nextConnection = 0;

    private final int m_expectedOutgoingMessageSize;

    private final DBBPool m_pool;

    private final boolean m_useMultipleThreads;

    class NodeConnection extends VoltProtocolHandler {
        private final HashMap<Long, ProcedureCallback> m_callbacks;
        private Connection m_connection;
        public final String m_address;
        private boolean m_isConnected = true;

        public NodeConnection(String address) {
            m_callbacks = new HashMap<Long, ProcedureCallback>();
            m_address = address;
        }

        public void createWork(long handle, BBContainer c, ProcedureCallback callback) {
            synchronized (this) {
                if (!m_isConnected) {
                    final ClientResponse r = new ClientResponseImpl(
                            ClientResponse.CONNECTION_LOST, new VoltTable[0],
                            "Connection to the database was lost before a response was received");
                    callback.clientCallback(r);
                    c.discard();
                    return;
                }
                m_callbacks.put(handle, callback);
            }
            m_connection.writeStream().enqueue(c);
        }

        public void createWork(long handle, FastSerializable f, ProcedureCallback callback) {
            synchronized (this) {
                if (!m_isConnected) {
                    final ClientResponse r = new ClientResponseImpl(
                            ClientResponse.CONNECTION_LOST, new VoltTable[0],
                            "Connection to the database was lost before a response was received");
                    callback.clientCallback(r);
                    return;
                }
                m_callbacks.put(handle, callback);
            }
            m_connection.writeStream().enqueue(f);
        }

        @Override
        public void handleMessage(ByteBuffer buf, Connection c) {
            ClientResponseImpl response = null;
            FastDeserializer fds = new FastDeserializer(buf);
            try {
                response = fds.readObject(ClientResponseImpl.class);
            } catch (IOException e) {
                e.printStackTrace();
            }
            ProcedureCallback cb = null;
            synchronized (this) {
                cb = m_callbacks.remove(response.getClientHandle());
            }
            if (cb != null) {

                // record when proc returned if
                // interested in latency
                if (ProcedureCallback.measureLatency){
                    // record when proc returned if
                    // interested in latency
                    cb.closeTimer(
                            m_context,
                            Distributer.this,
                            response.clientQueueTime(),
                            response.CIAcceptTime(),
                            response.FHReceiveTime(),
                            response.FHResponseTime(),
                            response.initiatorReceiveTime());
                }

                cb.clientCallback(response);
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
                        s.connectionLost(m_address, m_connections.size());
                    }
                }
                m_isConnected = false;

                //Invoke callbacks for all queued invocations with a failure response
                final ClientResponse r =
                    new ClientResponseImpl(
                        ClientResponse.CONNECTION_LOST, new VoltTable[0],
                        "Connection to the database was lost before a response was received");
                for (final ProcedureCallback pc : m_callbacks.values()) {
                    pc.clientCallback(r);
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
            return new Runnable() {
                @Override
                public void run() {}
            };
        }

        @Override
        public QueueMonitor writestreamMonitor() {
            return null;
        }

    }

    void drain() throws NoConnectionsException {
        boolean more;
        do {
            more = false;
            synchronized (this) {
                for (NodeConnection cxn : m_connections) {
                    synchronized(cxn.m_callbacks) {
                        more = more || cxn.m_callbacks.size() > 0;
                    }
                }
            }
            Thread.yield();
        } while(more);

        synchronized (this) {
            for (NodeConnection cxn : m_connections ) {
                assert(cxn.m_callbacks.size() == 0);
            }
        }
    }

    Distributer() {
        this( 128, null, false);
    }

    Distributer(int expectedOutgoingMessageSize, int arenaSizes[], boolean useMultipleThreads) {
        m_useMultipleThreads = useMultipleThreads;
        m_network = new VoltNetwork( useMultipleThreads, true, new Runnable[0], 3);
        synchronized (m_context) {
            m_context.totalInvocationCount.put( this, new Long(0));
        }
        m_expectedOutgoingMessageSize = expectedOutgoingMessageSize;
        m_network.start();
        m_context.invocationCount.put( this, new Long(0));
        m_pool = new DBBPool(false, arenaSizes, false);

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
//                        org.voltdb.utils.Pair<Long, Long> counters = m_network.getCounters();
//                        final long read = counters.getFirst();
//                        final long written = counters.getSecond();
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

    void createConnection(String host, String program, String password)
        throws UnknownHostException, IOException
    {
        createConnection(host, program, password, Client.VOLTDB_SERVER_PORT);
    }

    synchronized void createConnection(String host, String program, String password, int port)
    throws UnknownHostException, IOException
{
    final SocketChannel aChannel = ConnectionUtil.getAuthenticatedConnection(host, program, password, port);
    NodeConnection cxn = new NodeConnection( host + ":" + port);
    m_connections.add(cxn);
    Connection c = m_network.registerChannel( aChannel, cxn);
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

            int queuedInvocations = 0;
            for (int i=0; i < totalConnections; ++i) {
                cxn = m_connections.get(Math.abs(++m_nextConnection % totalConnections));
                queuedInvocations += cxn.m_callbacks.size();
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
                cxn.createWork(invocation.getHandle(), invocation, cb);
            } else {
                final FastSerializer fs = new FastSerializer(m_pool, expectedSerializedSize);
                BBContainer c = null;
                try {
                    c = fs.writeObjectForMessaging(invocation);
                } catch (IOException e) {
                    fs.getBBContainer().discard();
                    throw new RuntimeException(e);
                }
                cxn.createWork(invocation.getHandle(), c, cb);
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
        m_network.shutdown();
        synchronized (this) {
            m_pool.clear();
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
}
