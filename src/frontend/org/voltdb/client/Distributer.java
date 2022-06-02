/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import javax.net.ssl.SSLEngine;
import javax.security.auth.Subject;

import org.cliffc_voltpatches.high_scale_lib.NonBlockingHashMap;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.network.CipherExecutor;
import org.voltcore.network.Connection;
import org.voltcore.network.QueueMonitor;
import org.voltcore.network.VoltNetworkPool;
import org.voltcore.network.VoltNetworkPool.IOStatsIntf;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientStatusListenerExt.AutoConnectionStatus;
import org.voltdb.client.ClientStatusListenerExt.DisconnectCause;
import org.voltdb.common.Constants;

import com.google_voltpatches.common.base.Strings;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.ImmutableSortedMap;
import com.google_voltpatches.common.collect.Maps;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;

/**
 * De/multiplexes transactions across a cluster
 *
 * It is safe to synchronize on an individual connection and then the distributer,
 * but it is always unsafe to synchronize on the distributer and then an
 * individual connection.
 */
class Distributer {

    private static final long PING_HANDLE = Long.MAX_VALUE;
    private static final Long ASYNC_TOPO_HANDLE = PING_HANDLE - 1;
    private static final Long ASYNC_PROC_HANDLE = PING_HANDLE - 2;
    private static final long MINIMUM_LONG_RUNNING_SYSTEM_CALL_TIMEOUT_NANOS = TimeUnit.MINUTES.toNanos(30);
    private static final long ONE_SECOND_NANOS = TimeUnit.SECONDS.toNanos(1);

    // Would be private and final except: unit tests
    static int RESUBSCRIPTION_DELAY_MS = Integer.getInteger("RESUBSCRIPTION_DELAY_MS", 10000);

    // Package access: used by client implementation
    static final long USE_DEFAULT_CLIENT_TIMEOUT = 0;

    // Parameters from constructor
    private final SslContext m_sslContext;
    private final boolean m_useMultipleThreads;
    private final long m_procedureCallTimeoutNanos;
    private final long m_connectionResponseTimeoutNanos;
    private final Subject m_subject; // JAAS authentication subject

    // Possible priority for system-originated requests
    private int m_sysRequestPrio = ProcedureInvocation.NO_PRIORITY;

    // Status listeners (from application and ClientImpl)
    private final ArrayList<ClientStatusListenerExt> m_listeners = new ArrayList<>();

    // Handles used internally are negative and decrement for each call
    public final AtomicLong m_sysHandle = new AtomicLong(-1);

    // Collection of connections to the cluster
    private final CopyOnWriteArrayList<NodeConnection> m_connections =
            new CopyOnWriteArrayList<>();

    // And connections by host id
    private final Map<Integer, NodeConnection> m_hostIdToConnection = new HashMap<>();

    // Selector and connection handling, does all work in blocking selection thread
    private final VoltNetworkPool m_network;

    // For round-robin connection set up when correct target is not connected
    private int m_nextConnection = 0;

    // Outgoing transaction rate limiter; package access for client implementation
    final RateLimiter m_rateLimiter = new RateLimiter();

    // Backpressure configuration and status
    private boolean m_lastBackpressureReport;
    private int m_backpressureQueueLimit = ClientConfig.DEFAULT_BACKPRESSURE_QUEUE_REQUEST_LIMIT;
    private int m_maxQueuedBytes = ClientConfig.DEFAULT_BACKPRESSURE_QUEUE_BYTE_LIMIT;
    private int m_queuedBytes;

    // Data for topology-aware client operation, The 'unconnected hosts' list
    // contains all nodes from the latest topo update for which we do not
    // have connections. It's not directly adjusted on connection failure.
    private boolean m_topologyChangeAware;
    private final AtomicReference<ImmutableSet<Integer>> m_unconnectedHosts = new AtomicReference<>();
    private final AtomicBoolean m_createConnectionUponTopoChangeInProgress = new AtomicBoolean(false);

    // The connection we have issued our subscriptions to. If the connection is lost
    // we will need to request subscription from a different node.
    private NodeConnection m_subscribedConnection;
    private boolean m_subscriptionRequestPending;

    // Data for client affinity operation
    private HashinatorLite m_hashinator;
    private final Map<Integer, NodeConnection> m_partitionMasters = new HashMap<>();
    private final Map<Integer, ClientAffinityStats> m_clientAffinityStats = new HashMap<>();

    // Partitioning data, used for client affinity and topo-aware purposes
    private final AtomicReference<ImmutableMap<Integer, Integer>> m_partitionKeys = new AtomicReference<>();
    private final AtomicReference<ClientResponse> m_partitionUpdateStatus = new AtomicReference<>();
    private final AtomicLong m_lastPartitionKeyFetched = new AtomicLong(0); // timestamp

    // Procedure data
    private final AtomicReference<ImmutableSortedMap<String, Procedure>> m_procedureInfo = new AtomicReference<>();

    private static final class Procedure {
        final static int PARAMETER_NONE = -1;
        private enum Type { SINGLE, MULTI, COMPOUND };
        private final Type procType;
        private final boolean readOnly;
        private final int partitionParameter;
        private final int partitionParameterType;
        private Procedure(boolean single, boolean compound, boolean readOnly,
                          int partitionParameter, int partitionParameterType) {
            this.procType = single ? Type.SINGLE : compound ? Type.COMPOUND : Type.MULTI;
            this.readOnly = readOnly;
            this.partitionParameter = single ? partitionParameter : PARAMETER_NONE;
            this.partitionParameterType = single ? partitionParameterType : PARAMETER_NONE;
        }
    }

    // Until catalog subscription is implemented, only fetch it once
    private boolean m_fetchedCatalog;

    // General background executor (despite the name)
    private final ScheduledExecutorService m_ex =
        Executors.newSingleThreadScheduledExecutor(CoreUtils.getThreadFactory("VoltDB Client Reaper Thread"));
    private ScheduledFuture<?> m_timeoutReaperHandle;

    // Executor service for ssl encryption/decryption, if ssl is enabled.
    private CipherExecutor m_cipherService;

    // Instance id and build string from cluster
    private Object[] m_clusterInstanceId;
    private String m_buildString;

    // Indicate shutting down if it is true
    private AtomicBoolean m_shutdown = new AtomicBoolean(false);

    /**
     * Handles topology updates for client affinity
     */
    private class TopoUpdateCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                return;
            }
            try {
                synchronized (Distributer.this) {
                    VoltTable[] results = clientResponse.getResults();
                    if (results != null && results.length > 1) {
                        updateAffinityTopology(results);
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Handles partition updates for client affinity
     */
    private class PartitionUpdateCallback implements ProcedureCallback {

        private final CountDownLatch m_latch;

        PartitionUpdateCallback(CountDownLatch latch) {
            m_latch = latch;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (clientResponse.getStatus() == ClientResponse.SUCCESS) {
                VoltTable[] results = clientResponse.getResults();
                if (results != null && results.length > 0) {
                    updatePartitioning(results[0]);
                }
            }

            m_partitionUpdateStatus.set(clientResponse);

            if (m_latch != null) {
                m_latch.countDown();
            }
        }
    }

    /**
     * Handles @Subscribe response
     */
    private class SubscribeCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (m_shutdown.get()) {
                return;
            }

            //Fast path subscribing retry if the connection was lost before getting a response
            if (response.getStatus() == ClientResponse.CONNECTION_LOST ) {
                if (!m_connections.isEmpty()) {
                    subscribeToNewNode();
                }
                return;
            }

            //Slow path, god knows why it didn't succeed, server could be paused and in admin mode. Don't firehose attempts.
            if (response.getStatus() != ClientResponse.SUCCESS && !m_shutdown.get()) {
                //Retry on the off chance that it will work the Nth time, or work at a different node
                m_ex.schedule(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            subscribeToNewNode();
                        } catch (Throwable t) {
                            t.printStackTrace();
                            Throwables.propagate(t);
                        }
                    }
                }, 2, TimeUnit.MINUTES);
                return;
            }
            //If success, the code in NodeConnection.stopping needs to know it has to handle selecting
            //a new node to for subscriptions, so set the pending request to false to let that code
            //know that the failure won't be handled in the callback
            synchronized (Distributer.this) {
                m_subscriptionRequestPending = false;
            }
        }
    }

    /**
     * Handles procedure updates for client affinity
     */
    private class ProcUpdateCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                return;
            }
            try {
                synchronized (Distributer.this) {
                    VoltTable[] results = clientResponse.getResults();
                    if (results != null && results.length == 1) {
                        VoltTable vt = results[0];
                        updateProcedurePartitioning(vt);
                    }
                    m_fetchedCatalog = true;
                }

            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Handles timeout for procedure calls: runs periodically
     * to check on outstanding calls. Sends pings to keep the
     * connection alive.
     */
    private class CallExpiration implements Runnable {
        @Override
        public void run() {
            try {
                // make a threadsafe copy of all connections
                ArrayList<NodeConnection> connections = new ArrayList<>();
                synchronized (Distributer.this) {
                    connections.addAll(m_connections);
                }

                // current time
                final long nowNanos = System.nanoTime();

                // for each connection
                for (final NodeConnection c : connections) {
                    // check for connection age
                    final long sinceLastResponse = Math.max(1, nowNanos - c.m_lastResponseTimeNanos);

                    // if outstanding ping and timed out, close the connection
                    if (c.m_outstandingPing && sinceLastResponse > m_connectionResponseTimeoutNanos) {
                        c.m_closeCause = DisconnectCause.TIMEOUT;
                        // this should trigger NodeConnection.stopping(..)
                        c.m_connection.unregister();
                    }

                    // if 1/3 of the timeout since last response, send a ping
                    if (!c.m_outstandingPing && sinceLastResponse > m_connectionResponseTimeoutNanos / 3) {
                        c.sendPing();
                    }

                    // for each outstanding procedure
                    for (final Map.Entry<Long, CallbackBookkeeping> e : c.m_callbacks.entrySet()) {
                        final long handle = e.getKey();
                        final CallbackBookkeeping cb = e.getValue();

                        // if the timeout is expired, call the callback and remove the
                        // bookkeeping data
                        final long deltaNanos = Math.max(1, nowNanos - cb.startNanos);
                        if (deltaNanos > cb.procedureTimeoutNanos) {

                            // For expected long operations don't use the default timeout
                            // unless it exceeds our minimum timeout for long ops
                            if (!isLongOp(cb.name) || deltaNanos >= MINIMUM_LONG_RUNNING_SYSTEM_CALL_TIMEOUT_NANOS) {
                                c.handleTimedoutCallback(handle, nowNanos);
                            }
                        }
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    /*
     * Check if the proc name is a procedure that is expected to run long.
     * Make the minimum timeout for certain long running system procedures
     * higher than the default 2 minutes.
     * You can still set the default timeout higher than even this value.
     */
    private static boolean isLongOp(String procName) {
        if (procName.startsWith("@")) {
            if (procName.equals("@UpdateApplicationCatalog") || procName.equals("@SnapshotSave")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Holds book-keeping data for in-progress transactpions.
     */
    private class CallbackBookkeeping {
        public CallbackBookkeeping(long startNanos, ProcedureCallback callback, String name, long timeoutNanos, boolean ignoreBackpressure) {
            assert(callback != null);
            this.startNanos = startNanos;
            this.callback = callback;
            this.name = name;
            this.procedureTimeoutNanos = timeoutNanos;
            this.ignoreBackpressure = ignoreBackpressure;
        }
        final long startNanos;
        final long procedureTimeoutNanos;
        final ProcedureCallback callback;
        final String name;
        final boolean ignoreBackpressure;
    }

    /**
     * Node connection - represents a single connection to a VoltDB cluster.
     * Manages addition of work queues representing procedure calls. There are
     * two variants:
     *
     * createWork - the 'normal' case. The request may block in the rate limiter
     * awaiting permission to send, based on either a simple outstanding transaction
     * count, or else consideration of the transaction send rate.
     *
     * createWorkNonBlocking - similar, but will never block; instead returns a
     * true/false status to the caller, indicating whether work was accepted into
     * the queue, or not.
     *
     * Backpressure:
     *
     * Transactions are queued in the lower-level connection. If the queue length
     * gets too large, or TCP cannot accept more data, the distributer is notified
     * of backpressure starting. When the length drops down again the distributer
     * is notified of backpressure ending.
     *
     * In non-blocking mode, refusal by the rate limiter to accept more work is
     * also considered as backpressure. The rate limiter will eventually give
     * the distributer a callback when queueing can be resumed.
     */
    private class NodeConnection extends VoltProtocolHandler implements org.voltcore.network.QueueMonitor {
        private final AtomicInteger m_callbacksToInvoke = new AtomicInteger(0);
        private final ConcurrentMap<Long, CallbackBookkeeping> m_callbacks = new ConcurrentHashMap<>();
        private final NonBlockingHashMap<String, ClientStats> m_stats = new NonBlockingHashMap<>();
        private Connection m_connection;
        private volatile boolean m_isConnected = true;
        private boolean m_nonblockingInitDone = false;

        volatile long m_lastResponseTimeNanos = System.nanoTime();
        boolean m_outstandingPing = false;
        ClientStatusListenerExt.DisconnectCause m_closeCause = DisconnectCause.CONNECTION_CLOSED;

        public NodeConnection(long[] ids) {}

        /*
         * Create work, possibly blocking in rate limiter.
         * 'ignoreBackpressure' can be true. to get rate limiter to not apply any permit tracking
         * or rate limits to transactions that should never be rejected, such as those submitted
         * from within a callback thread, or generated internally
         */
        public void createWork(final long startNanos, long handle, String name, ByteBuffer buffer,
              ProcedureCallback callback, boolean ignoreBackpressure, long timeoutNanos) {
            assert(callback != null);
            if (timeoutNanos == Distributer.USE_DEFAULT_CLIENT_TIMEOUT) {
                timeoutNanos = m_procedureCallTimeoutNanos;
            }

            // Do rate limiting or check for max outstanding related backpressure in
            // the rate limiter, which can block. If it blocks we can still get a timeout
            // exception, to give prompt timeouts.
            try {
                m_rateLimiter.prepareToSendTransaction(startNanos, timeoutNanos, ignoreBackpressure);
            } catch (TimeoutException | InterruptedException e) {
                // We timed out waiting for permission to send the transaction out on the wire,
                // due to max outstanding
                // TODO - this incorrectly results in decrementing an outstanding txn count that
                // was never incremented (existing bug, not induced by current code changes)
                invokeCallbackWithTimeout(name, callback, startNanos, System.nanoTime(),
                                          timeoutNanos, handle, ignoreBackpressure);
                return;
            }

            // Now join common code to enqueue buffer, schedule timeout, etc.
            createWorkCommon(startNanos, handle, name, buffer, callback, ignoreBackpressure, timeoutNanos);
        }

        /*
         * Create work, never blocking in rate limiter.
         */
        public boolean createWorkNonblocking(final long startNanos, long handle, String name, ByteBuffer buffer,
                                             ProcedureCallback callback, long timeoutNanos) {
            assert(callback != null);
            if (timeoutNanos == Distributer.USE_DEFAULT_CLIENT_TIMEOUT) {
                timeoutNanos = m_procedureCallTimeoutNanos;
            }

            // First time through, do any one-time setup
            synchronized (this) {
                if (!m_nonblockingInitDone) {
                    m_rateLimiter.setNonblockingResumeHook(offBackPressure());
                    m_nonblockingInitDone = true;
                }
            }

            // Check rate limiting and max outstanding related backpressure. Does not
            // block. We need to sync on the distributer so that reporting backpressure
            // on, and subsequent removal of backpressure by the rate limiter, occur
            // in the right order.
            synchronized (Distributer.this) {
                if (!m_rateLimiter.prepareToSendTransactionNonblocking()) {
                    reportBackpressure(true);
                    return false; // would block
                }
            }

            // Now join common code to enqueue buffer, schedule timeout, etc.
            createWorkCommon(startNanos, handle, name, buffer, callback, false, timeoutNanos);
            return true;
        }

        /*
         * Common tail for the two 'create work' entry points.
         * Cannot fail to enqueue the transaction,
         */
        private void createWorkCommon(final long startNanos, long handle, String name, ByteBuffer buffer,
                                      ProcedureCallback callback, boolean ignoreBackpressure,
                                      final long timeoutNanos) {
            // Drain needs to know when all callbacks have been invoked
            final int callbacksToInvoke = m_callbacksToInvoke.incrementAndGet();
            assert(callbacksToInvoke >= 0);

            // Optimistically submit the task
            assert(!m_callbacks.containsKey(handle));
            m_callbacks.put(handle, new CallbackBookkeeping(startNanos, callback, name, timeoutNanos, ignoreBackpressure));

            // Schedule an individual timeout if necessary.
            // If it is a long op, don't bother scheduling a discrete timeout
            if (timeoutNanos < ONE_SECOND_NANOS && !isLongOp(name)) {
                final long timeoutRemaining = startNanos + timeoutNanos - System.nanoTime();
                submitDiscreteTimeoutTask(handle, Math.max(0, timeoutRemaining));
            }

            // Check actually connected before enqueing. This may flag backpressure
            // in the connection, based on the size of the queue, but the message
            // is still queued. We'll see the backpressure on the next request.
            if (m_isConnected) {
                m_connection.writeStream().enqueue(buffer);
                return;
            }

            // Not connected: notify client, but first check if the disconnect or
            // expiration already handled the callback
            if (m_callbacks.remove(handle) != null) {
                String msg = String.format("Connection to database host (%s) was lost before a response was received",
                                           m_connection.getHostnameOrIP());
                ClientResponse resp = new ClientResponseImpl(ClientResponse.CONNECTION_LOST, new VoltTable[0], msg);
                try {
                    callback.clientCallback(resp);
                } catch (Exception e) {
                    uncaughtException(callback, resp, e);
                }

                // Drain needs to know when all callbacks have been invoked
                final int remainingToInvoke = m_callbacksToInvoke.decrementAndGet();
                assert(remainingToInvoke >= 0);

                // For bookkeeping, but it feels dishonest to call this here
                m_rateLimiter.transactionResponseReceived(System.nanoTime(), -1, ignoreBackpressure);
            }
        }

        /*
         * For high precision timeouts, submit a discrete task to a scheduled
         * executor service to time out the transaction. The timeout task
         * when run checks if the task is still present in the concurrent map
         * of tasks and removes it. If it wins the race to remove the map
         * then the transaction will be timed out even if a response is received
         * at the same time.
         *
         * This will race with the periodic task that checks lower resolution timeouts
         * and it is fine, the concurrent map makes sure each callback is handled exactly once
         */
        void submitDiscreteTimeoutTask(final long handle, long timeoutNanos) {
            m_ex.schedule(new Runnable() {
                @Override
                public void run() {
                    handleTimedoutCallback(handle, System.nanoTime());
                }
            }, timeoutNanos, TimeUnit.NANOSECONDS);
        }

        /*
         * Factor out the boilerplate involved in checking whether a timed out callback
         * still exists and needs to be invoked, or has already been handled by another thread.
         *
         * @param handle client handle
         * @param endNanos system time when timeout detected
         */
        private void handleTimedoutCallback(long handle, long endNanos) {
            final CallbackBookkeeping cb = m_callbacks.remove(handle);
            if (cb != null) {
                invokeCallbackWithTimeout(cb.name, cb.callback, cb.startNanos, endNanos,
                                          cb.procedureTimeoutNanos, handle, cb.ignoreBackpressure);
            }
        }

        /*
         * Factor out the boilerplate involved in invoking a callback with a timeout response.
         *
         * @param procName procedure name
         * @param callback the callback to invoke
         * @param startNanos system time when call issued
         * @param endNanos system time when timeout detected
         * @param timeoutNanos requested timeout period
         * @param handle client handle
         * @param ignoreBackpressure flag from call
         */
        private void invokeCallbackWithTimeout(String procName,
                                               ProcedureCallback callback,
                                               long startNanos,
                                               long endNanos,
                                               long timeoutNanos,
                                               long handle,
                                               boolean ignoreBackpressure) {
            ClientResponseImpl r = new ClientResponseImpl(
                    ClientResponse.CONNECTION_TIMEOUT,
                    ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                    "",
                    new VoltTable[0],
                    String.format("No response received in the allotted time (set to %d ms).",
                            TimeUnit.NANOSECONDS.toMillis(timeoutNanos)));
            long deltaNanos = Math.max(1, endNanos - startNanos);
            r.setClientHandle(handle);
            r.setClientRoundtrip(deltaNanos);
            r.setClusterRoundtrip((int)TimeUnit.NANOSECONDS.toMillis(deltaNanos));
            try {
                callback.clientCallback(r);
            } catch (Throwable t) {
                uncaughtException( callback, r, t);
            }

            //Drain needs to know when all callbacks have been invoked
            final int remainingToInvoke = m_callbacksToInvoke.decrementAndGet();
            assert(remainingToInvoke >= 0);

            m_rateLimiter.transactionResponseReceived(endNanos, -1, ignoreBackpressure);
            updateStatsForTimeout(procName, r.getClientRoundtripNanos(), r.getClusterRoundtrip());
        }

        /*
         * Sends a ping on the underlying connection, bypassing rate limits, etc.
         */
        void sendPing() {
            ProcedureInvocation invocation = makeProcedureInvocation(PING_HANDLE, "@Ping");
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

        /*
         * Updates procedure stats after timeout
         */
        private void updateStatsForTimeout(
                final String procName,
                final long roundTripNanos,
                final int clusterRoundTrip) {
            m_connection.queueTask(new Runnable() {
                @Override
                public void run() {
                    updateStats(procName, roundTripNanos, clusterRoundTrip, false, false, true);
                }
            });
        }

        /*
         * Update the procedures statistics
         * @param procName Name of procedure being updated
         * @param clusterRoundTrip round trip measured within the VoltDB cluster
         * @param abort true of the procedure was aborted
         * @param failure true if the procedure failed
         */
        private void updateStats(
                String procName,
                long roundTripNanos,
                int clusterRoundTrip,
                boolean abort,
                boolean failure,
                boolean timeout) {
            ClientStats stats = m_stats.get(procName);
            if (stats == null) {
                stats = new ClientStats();
                stats.m_connectionId = connectionId();
                stats.m_hostname = m_connection.getHostnameOrIP();
                stats.m_port = m_connection.getRemotePort();
                stats.m_procName = procName;
                stats.m_startTS = System.currentTimeMillis();
                stats.m_endTS = Long.MIN_VALUE;
                m_stats.put(procName, stats);
            }
            stats.update(roundTripNanos, clusterRoundTrip, abort, failure, timeout);
        }

        /**
         * InputHandler
         * Response handler
         */
        @Override
        public void handleMessage(ByteBuffer buf, Connection c) {
            final long endNanos = System.nanoTime();

            ClientResponseImpl response = new ClientResponseImpl();
            try {
                response.initFromBuffer(buf);
            } catch (IOException e1) {
                e1.printStackTrace();
                // TODO: return here?
            }

            // track the timestamp of the most recent read on this connection
            m_lastResponseTimeNanos = endNanos;

            final long handle = response.getClientHandle();

            // handle ping response and get out
            if (handle == PING_HANDLE) {
                m_outstandingPing = false;
                return;
            }

            if (handle == ASYNC_TOPO_HANDLE) {
                /*
                 * Really didn't want to add this block because it is not DRY
                 * for the exception handling, but trying to set + reset the async topo callback
                 * turned out to be pretty challenging
                 */
                ProcedureCallback cb = new TopoUpdateCallback();
                try {
                    cb.clientCallback(response);
                } catch (Exception e) {
                    uncaughtException(cb, response, e);
                }
                return;
            }

            if (handle == ASYNC_PROC_HANDLE) {
                ProcedureCallback cb = new ProcUpdateCallback();
                try {
                    cb.clientCallback(response);
                } catch (Exception e) {
                    uncaughtException(cb, response, e);
                }
                return;
            }

            //Race with expiration thread to be the first to remove the callback
            //from the map and process it
            final CallbackBookkeeping stuff = m_callbacks.remove(response.getClientHandle());

            // presumably (hopefully) this is a response for a timed-out message
            if (stuff == null) {
                // also ignore internal (topology and procedure) calls
                if (handle >= 0) {
                    // notify any listeners of the late response
                    for (ClientStatusListenerExt listener : m_listeners) {
                        listener.lateProcedureResponse(
                                response,
                                m_connection.getHostnameOrIP(),
                                m_connection.getRemotePort());
                    }
                }
            }
            // handle a proper callback
            else {
                final long callTimeNanos = stuff.startNanos;
                final long deltaNanos = Math.max(1, endNanos - callTimeNanos);
                final ProcedureCallback cb = stuff.callback;
                assert(cb != null);
                boolean abort = response.aborted();
                boolean error = response.failed();

                int clusterRoundTrip = response.getClusterRoundtrip();
                m_rateLimiter.transactionResponseReceived(endNanos, clusterRoundTrip, stuff.ignoreBackpressure);
                updateStats(stuff.name, deltaNanos, clusterRoundTrip, abort, error, false);
                response.setClientRoundtrip(deltaNanos);
                assert(response.getHashes() == null) : "A determinism hash snuck into the client wire protocol";
                try {
                    cb.clientCallback(response);
                } catch (Throwable t) {
                    uncaughtException(cb, response, t);
                }

                //Drain needs to know when all callbacks have been invoked
                final int remainingToInvoke = m_callbacksToInvoke.decrementAndGet();
                assert(remainingToInvoke >= 0);
            }
        }

        /**
         * InputHandler
         * Get maximum read size for connection (basically unlimited)
         */
        @Override
        public int getMaxRead() {
            return Integer.MAX_VALUE;
        }

        /**
         * Did the underlying connection report backpressure?
         */
        public boolean hadBackPressure() {
            return m_connection.writeStream().hadBackPressure();
        }

        /**
         * Update NodeConnection with new connection, and notify
         * all listeners
         */
        public void setConnection(Connection c) {
            m_connection = c;
            for (ClientStatusListenerExt listener : m_listeners) {
                listener.connectionCreated(m_connection.getHostnameOrIP(),
                                           m_connection.getRemotePort(),
                                           AutoConnectionStatus.SUCCESS);
            }
        }

        /**
         * InputHandler
         * Shut down this connection
         */
        @Override
        public void stopping(Connection c) {
            super.stopping(c);
            m_isConnected = false;
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

                m_connections.remove(this);

                //Notify listeners that a connection has been lost
                for (ClientStatusListenerExt s : m_listeners) {
                    s.connectionLost(m_connection.getHostnameOrIP(),
                                     m_connection.getRemotePort(),
                                     m_connections.size(),
                                     m_closeCause);
                }

                /*
                 * Deal with the fact that this may have been the connection that subscriptions were issued
                 * to. If a subscription request was pending, don't handle selecting a new node here
                 * let the callback see the failure and retry
                 */
                if (m_subscribedConnection == this &&
                    m_subscriptionRequestPending == false &&
                    !m_shutdown.get()) {
                    //Don't subscribe to a new node immediately
                    //to somewhat prevent a thundering herd
                    try {
                        m_ex.schedule(new Runnable() {
                            @Override
                            public void run() {
                                subscribeToNewNode();
                            }
                        }, new Random().nextInt(RESUBSCRIPTION_DELAY_MS),
                                TimeUnit.MILLISECONDS);
                    } catch (RejectedExecutionException ree) {
                        // this is for race if m_ex shuts down in the middle of schedule
                        return;
                    }

                }
            }

            //Invoke callbacks for all queued invocations with a failure response
            final ClientResponse r =
                new ClientResponseImpl(
                        ClientResponse.CONNECTION_LOST, new VoltTable[0],
                        "Connection to database host (" + m_connection.getHostnameOrIP() + ") was lost before a response was received");
            for (Map.Entry<Long, CallbackBookkeeping> e : m_callbacks.entrySet()) {
                //Check for race with other threads
                if (m_callbacks.remove(e.getKey()) == null) {
                    continue;
                }
                final CallbackBookkeeping callBk = e.getValue();
                try {
                    callBk.callback.clientCallback(r);
                }
                catch (Throwable t) {
                    uncaughtException(callBk.callback, r, t);
                }

                //Drain needs to know when all callbacks have been invoked
                final int remainingToInvoke = m_callbacksToInvoke.decrementAndGet();
                assert(remainingToInvoke >= 0);

                m_rateLimiter.transactionResponseReceived(System.nanoTime(), -1, callBk.ignoreBackpressure);
            }
        }

        /*
         * InputHandler
         * Called during port setup to create a runnable callback
         * which will eventually be called from the write stream.
         * Callback immediately reports end of backpressure to
         * all client listeners.
         */
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
                        reportBackpressure(false);
                    }
                }
            };
        }

        /*
         * InputHandler
         * Called during port setup to create a runnable callback which will
         * eventually be called from the write stream. We don't have such a
         * callback. Backpressure is instead reported to client from the
         * 'next' call to queue/queueNonblocking.
         */
        @Override
        public Runnable onBackPressure() {
            return null;
        }

        /*
         * InputHandler
         * Queue-length monitor, called by write stream to determine
         * backpressure.
         */
        @Override
        public QueueMonitor writestreamMonitor() {
            return this;
        }

        /*
         * QueueMonitor
         * called from write stream
         */
        @Override
        public boolean queue(int bytes) {
            m_queuedBytes += bytes;
            return (m_queuedBytes > m_maxQueuedBytes);
        }

        /*
         * Remote server address for this connection
         */
        public InetSocketAddress getSocketAddress() {
            return m_connection.getRemoteSocketAddress();
        }
    }

    /*
     * Drains all work for all connections
     */
    void drain() throws InterruptedException {
        boolean more;
        long sleep = 500;
        do {
            more = false;
            for (NodeConnection cxn : m_connections) {
                more = more || cxn.m_callbacksToInvoke.get() > 0;
            }
            /*
             * Back off to spinning at five millis. Try and get drain to be a little
             * more prompt. Spinning sucks!
             */
            if (more) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(sleep));
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                if (sleep < 5000) {
                    sleep += 500;
                }
            }
        } while(more);
    }

    /**
     * Set thresholds for backpressure reporting based on pending
     * request count and pending byte count.
     *
     * Reducing limit below current queue length will not cause
     * backpressure indication until next callProcedure.
     *
     * (Synchronized to avoid race with new connection setup)
     *
     * @param reqLimit:  request limit
     * @param byteLimit: byte limit
     */
    public synchronized void setBackpressureQueueThresholds(int reqLimit, int byteLimit) {
        m_maxQueuedBytes = byteLimit;
        m_backpressureQueueLimit = reqLimit;
        for (NodeConnection cxn : m_connections) {
            cxn.m_connection.writeStream().setPendingWriteBackpressureThreshold(reqLimit);
        }
    }

    ////////////////////////////////////////
    // The actual distributer starts here
    ///////////////////////////////////////

    Distributer() {
        this(false,
             ClientConfig.DEFAULT_PROCEDURE_TIMEOUT_NANOS,
             ClientConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
             null,
             null);
    }

    Distributer(boolean useMultipleThreads,
                long procedureCallTimeoutNanos,
                long connectionResponseTimeoutMS,
                Subject subject,
                SslContext sslContext) {
        m_useMultipleThreads = useMultipleThreads;
        m_sslContext = sslContext;
        if (m_sslContext != null) {
            m_cipherService = CipherExecutor.CLIENT;
            m_cipherService.startup();
        } else {
            m_cipherService = null;
        }
        m_network = new VoltNetworkPool(
                m_useMultipleThreads ? Math.max(1, CoreUtils.availableProcessors() / 4 ) : 1,
                1, null, "Client");
        m_network.start();
        m_procedureCallTimeoutNanos= procedureCallTimeoutNanos;
        m_connectionResponseTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(connectionResponseTimeoutMS);

        // schedule the task that looks for timed-out proc calls and connections
        m_timeoutReaperHandle = m_ex.scheduleAtFixedRate(new CallExpiration(), 1, 1, TimeUnit.SECONDS);
        m_subject = subject;
    }

    /*
     * Add new connection with plaintext password
     * TODO is this used?
     */
    void createConnection(String host, String username, String password, int port, ClientAuthScheme scheme)
    throws UnknownHostException, IOException
    {
        byte[] hashedPassword = ConnectionUtil.getHashedPassword(scheme, password);
        createConnectionWithHashedCredentials(host, username, hashedPassword, port, scheme);
    }

    /*
     * Add new connection with hashed password
     */
    void createConnectionWithHashedCredentials(String host, String username, byte[] hashedPassword, int port, ClientAuthScheme scheme)
    throws UnknownHostException, IOException
    {
        SSLEngine sslEngine = null;

        if (m_sslContext != null) {
            sslEngine = m_sslContext.newEngine(ByteBufAllocator.DEFAULT, host, port);
        }

        final Object[] socketChannelAndInstanceIdAndBuildString =
            ConnectionUtil.getAuthenticatedConnection(host, username, hashedPassword, port, m_subject, scheme, sslEngine,
                                                      TimeUnit.NANOSECONDS.toMillis(m_connectionResponseTimeoutNanos));
        final SocketChannel aChannel = (SocketChannel)socketChannelAndInstanceIdAndBuildString[0];
        final long[] instanceIdWhichIsTimestampAndLeaderIp = (long[])socketChannelAndInstanceIdAndBuildString[1];
        final int hostId = (int)instanceIdWhichIsTimestampAndLeaderIp[0];

        NodeConnection cxn = new NodeConnection(instanceIdWhichIsTimestampAndLeaderIp);
        Connection c = null;
        try {
            c = m_network.registerChannel(aChannel, cxn, m_cipherService, sslEngine);
        }
        catch (Exception e) {
            // Need to clean up the socket if there was any failure
            try {
                aChannel.close();
            } catch (IOException e1) {
                //Don't care connection is already lost anyways
            }
            Throwables.propagate(e);
        }

        // Save Connection in NodeConnection
        // Also executes connectionCreated methods in all listeners
        cxn.setConnection(c);

        synchronized (this) {

            // Set queue limit for new connection
            cxn.m_connection.writeStream().setPendingWriteBackpressureThreshold(m_backpressureQueueLimit);

            // If there are no connections, discard any previous connection ids and allow the client
            // to connect to a new cluster.
            // Careful, this is slightly less safe than the previous behavior.
            if (m_connections.size() == 0) {
                m_clusterInstanceId = null;
            }

            if (m_clusterInstanceId == null) {
                long timestamp = instanceIdWhichIsTimestampAndLeaderIp[2];
                int addr = (int)instanceIdWhichIsTimestampAndLeaderIp[3];
                m_clusterInstanceId = new Object[] { timestamp, addr };
            } else {
                if (!(((Long)m_clusterInstanceId[0]).longValue() == instanceIdWhichIsTimestampAndLeaderIp[2]) ||
                        !(((Integer)m_clusterInstanceId[1]).longValue() == instanceIdWhichIsTimestampAndLeaderIp[3])) {
                    // clean up the pre-registered voltnetwork connection/channel
                    c.unregister();
                    throw new IOException(
                            "Cluster instance id mismatch. Current is " + m_clusterInstanceId[0] + "," + m_clusterInstanceId[1] +
                            " and server's was " + instanceIdWhichIsTimestampAndLeaderIp[2] + "," + instanceIdWhichIsTimestampAndLeaderIp[3]);
                }
            }
            m_buildString = (String)socketChannelAndInstanceIdAndBuildString[2];

            m_connections.add(cxn);
            m_hostIdToConnection.put(hostId, cxn);
        }

        if (m_subscribedConnection == null) {
            subscribeToNewNode();
        }
    }

    /*
     * Subscribe to receive async updates on a new node connection. This will set m_subscribed
     * connection to the provided connection.
     *
     * If we are subscribing to a new connection on node failure this will also fetch the topology post node
     * failure. If the cluster hasn't finished resolving the failure it is fine, we will get the new topo through\
     */
    private void subscribeToNewNode() {
        if (m_shutdown.get()) {
            return;
        }
        //Technically necessary to synchronize for safe publication of this store
        NodeConnection cxn = null;
        synchronized (Distributer.this) {
            m_subscribedConnection = null;
            if (!m_connections.isEmpty()) {
                cxn = m_connections.get(new Random().nextInt(m_connections.size()));
                m_subscriptionRequestPending = true;
                m_subscribedConnection = cxn;
            } else {
                return;
            }
        }

        try {

            //Subscribe to topology updates before retrieving the current topo
            //so there isn't potential for lost updates
            ProcedureInvocation spi = makeProcedureInvocation(m_sysHandle.getAndDecrement(), "@Subscribe", "TOPOLOGY");
            cxn.createWork(System.nanoTime(),
                           spi.getHandle(),
                           spi.getProcName(),
                           serializeSPI(spi),
                           new SubscribeCallback(),
                           true,
                           USE_DEFAULT_CLIENT_TIMEOUT);

            spi = makeProcedureInvocation(m_sysHandle.getAndDecrement(), "@Statistics", "TOPO", 0);
            //The handle is specific to topology updates and has special-cased handling
            cxn.createWork(System.nanoTime(),
                           spi.getHandle(),
                           spi.getProcName(),
                           serializeSPI(spi),
                           new TopoUpdateCallback(),
                           true,
                           USE_DEFAULT_CLIENT_TIMEOUT);

            //Don't need to retrieve procedure updates every time we do a new subscription
            //since catalog changes aren't correlated with node failure the same way topo is
            if (!m_fetchedCatalog) {
                spi = makeProcedureInvocation(m_sysHandle.getAndDecrement(), "@SystemCatalog", "PROCEDURES");
                //The handle is specific to procedure updates and has special-cased handling
                cxn.createWork(System.nanoTime(),
                               spi.getHandle(),
                               spi.getProcName(),
                               serializeSPI(spi),
                               new ProcUpdateCallback(),
                               true,
                               USE_DEFAULT_CLIENT_TIMEOUT);
            }

            //Partition key update
            refreshPartitionKeys(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Queue invocation on first node connection without backpressure. If there is none with
     * without backpressure, then return false and don't queue the invocation.
     *
     * The 'ignoreBackpressure' option allows bypassing backpressure restrictions. It
     * is used by ClientImpl to avoid deadlocking on executing a procedure call on a
     * callback thread, which you're not supposed to do. TODO: why do we allow it?
     *
     * If there are no connections available (not just no unbackpressured connections)
     * then a NoConnectionsException will be thrown. There is one exception to this,
     * however, and that is when topology-change-awareness is enabled and automatic
     * reconnection is in effect. This case is handled via backpressure, identically
     * to all connections being backpressured.
     *
     * If this method returns 'false' then the callback has definitely not been
     * called with a ClientResponse.
     *
     * @param invocation procedure invocation
     * @param cb client callback
     * @param ignoreBackpressure if true the invocation will be queued even if there is backpressure
     * @param startNanos time at which this client call was started
     * @param timeoutNanos nanoseconds from startNanos where timeout should fire
     * @return True if the message was queued and false if the message was not queued due to backpressure
     * @throws NoConnectionsException (see above)
     */
    boolean queue(ProcedureInvocation invocation,
                  ProcedureCallback cb,
                  final boolean ignoreBackpressure,
                  final long startNanos,
                  final long timeoutNanos) throws NoConnectionsException {
        assert(invocation != null);
        assert(cb != null);
        if (m_shutdown.get()) {
            return false; // no more tasks
        }

        CxnStatsData statsData = new CxnStatsData();
        NodeConnection cxn = findCxnForQueue(invocation, ignoreBackpressure, statsData);
        if (cxn == null) {
            if (m_topologyChangeAware) {
                createConnectionsUponTopologyChange();
            }
            return false; // backpressured
        }

        ByteBuffer buf = null;
        try {
            buf = serializeSPI(invocation);
        } catch (Exception e) {
            Throwables.propagate(e);
        }

        updateAffinityStats(statsData);

        // createWork may block in the rate limiter, and always queues
        // the invocaton before returning
        cxn.createWork(startNanos, invocation.getHandle(), invocation.getProcName(),
                       buf, cb, ignoreBackpressure, timeoutNanos);
        if (m_topologyChangeAware) {
            createConnectionsUponTopologyChange();
        }
        return true;
    }

    boolean queueNonblocking(ProcedureInvocation invocation,
                             ProcedureCallback cb,
                             final long startNanos,
                             final long timeoutNanos) throws NoConnectionsException {
        assert(invocation != null);
        assert(cb != null);
        if (m_shutdown.get()) {
            return false;
        }

        CxnStatsData statsData = new CxnStatsData();
        NodeConnection cxn = findCxnForQueue(invocation, false, statsData);
        if (cxn == null) {
            if (m_topologyChangeAware) {
                createConnectionsUponTopologyChange();
            }
            return false; // backpressured
        }

        ByteBuffer buf = null;
        try {
            buf = serializeSPI(invocation);
        } catch (Exception e) {
            Throwables.propagate(e);
        }

        boolean queued = cxn.createWorkNonblocking(startNanos, invocation.getHandle(), invocation.getProcName(),
                                                   buf, cb, timeoutNanos);
        if (queued) {
            updateAffinityStats(statsData);
        }

        if (m_topologyChangeAware) {
            createConnectionsUponTopologyChange();
        }
        return queued;
    }

    /**
     * Locates an un-backpressured connection for the above 'queue'
     * methods. Null return implies backpressure (and not ignored)
     *
     * Synchronization is necessary to ensure that m_connections is
     * not modified, as well as to ensure that backpressure is
     * reported correctly.
     *
     * Ordinarily, if there are no connections at all, an exception
     * will be thrown. If we are running topology-aware with automatic
     * reconnection, then we suppress the exception, and instead treat
     * it identically to there being no unbackpressured connections.
     * Backpressure will be removed on reconnect.
     *
     * The above does not apply if ignoreBackpressure is true; it
     * makes no sense to use backpressure in such a case. However,
     * we only expect to see ignoreBackpressure true on calls made
     * by the user app on network  threads: you're not supposed to
     * do that.
     */
    private synchronized NodeConnection findCxnForQueue(ProcedureInvocation invocation,
                                                        boolean ignoreBackpressure,
                                                        CxnStatsData statsData) throws NoConnectionsException {
        final int totalConnections = m_connections.size();
        if (totalConnections == 0) {
            if (!m_topologyChangeAware) {
                throw new NoConnectionsException("No connections.");
            }
            if (ignoreBackpressure) {
                throw new NoConnectionsException("No connections (and ignoreBackpressure set).");
            }
            reportBackpressure(true);
            return null;
        }

        NodeConnection cxn = null;
        statsData.stats = null;

        final ImmutableSortedMap<String, Procedure> procedures = m_procedureInfo.get();
        Procedure procedureInfo = null;
        if (procedures != null) {
            procedureInfo = procedures.get(invocation.getProcName());
        }

        // Check if the master for the partition is known. This is where we guess
        // partition based on client affinity and known topology (if hashinator
        // initialized).
        int hashedPartition = -1; // no partition
        if (invocation.hasPartitionDestination()) { // for all-partition calls
            hashedPartition = invocation.getPartitionDestination();
        }
        else if (m_hashinator != null && procedureInfo != null) {
            switch (procedureInfo.procType) {
            case SINGLE:
                if (procedureInfo.partitionParameter != Procedure.PARAMETER_NONE &&
                    procedureInfo.partitionParameter < invocation.getPassedParamCount()) {
                    hashedPartition = m_hashinator.getHashedPartitionForParameter(procedureInfo.partitionParameterType,
                                                                                  invocation.getPartitionParamValue(procedureInfo.partitionParameter));
                }
                break;
            case MULTI:
                hashedPartition = Constants.MP_INIT_PID;
                break;
            case COMPOUND:
                // use round-robin
                break;
            }
        }

        // No backpressure check when selecting connection, to ensure correct routing,
        // but backpressure be managed anyways.
        cxn = m_partitionMasters.get(hashedPartition);
        if (cxn != null && !cxn.m_isConnected) {
            // Client affinity picked a connection that was actually disconnected.
            // Reset to null and let the round-robin choice pick a connection
            cxn = null;
        }

        // Fall back to using round-robin when:
        // - preferred connection is not available
        // - we have not yet acquired partitioning data
        // - procedure name is not (yet) known
        // - caller failed to provide value for partition column
        // - procedure is a compound procedure
        boolean roundRobin = false;
        for (int i=0; cxn==null && i<totalConnections; ++i) {
            NodeConnection tmpCxn = m_connections.get(Math.abs(++m_nextConnection % totalConnections));
            if (!tmpCxn.hadBackPressure() || ignoreBackpressure) {
                cxn = tmpCxn;
                roundRobin = true;
            }
        }

        // Return affinity stats data for updating when we actually
        // decide to send the transaction.
        ClientAffinityStats stats = m_clientAffinityStats.get(hashedPartition);
        if (stats == null) {
            stats = new ClientAffinityStats(hashedPartition);
            m_clientAffinityStats.put(hashedPartition, stats);
        }
        statsData.stats = stats;
        statsData.readOnly = (procedureInfo != null && procedureInfo.readOnly);
        statsData.roundRobin = roundRobin;

        // Still no unbackpressured connection? Report backpressure while
        // still under Distributer sync
        if (cxn == null) {
            reportBackpressure(true);
        }
        return cxn;
    }

    /**
     * Internal utility to report backpressure changes to client
     * level.  All backpressure reporting, whether originating
     * in the Distributer or in the network write stream, passes
     * through here. Caller must own the monitor lock.
     *
     * We aspire to not report a non-change, for example hearing
     * backpressure on when it is already on (this can arise, for
     * example, if queue() calls are in flight when backpressure
     * begins). However, out of fear of stalling the pipeline, for
     * now we will always report any transition to backpressure off,
     * even if we thought it was already off.
     *
     * @param bp : true if backpressure starting
     *             false if backpressure ending
     */
    private void reportBackpressure(boolean bp) {
        if (m_lastBackpressureReport ^ bp || !bp) {
            m_lastBackpressureReport = bp;
            for (ClientStatusListenerExt sl : m_listeners) {
                sl.backpressure(bp);
            }
        }
    }

    /**
     * Does affinity-stats accounting once we know we're going to send
     * a transaction on the connection found by FindCxnForQueue
     */
    private void updateAffinityStats(CxnStatsData data) {
        if (data.stats != null) {
            synchronized (data.stats) {
                if (data.roundRobin) {
                    if (data.readOnly) {
                        data.stats.addRrRead();
                    } else {
                        data.stats.addRrWrite();
                    }
                } else {
                    if (data.readOnly) {
                        data.stats.addAffinityRead();
                    } else {
                        data.stats.addAffinityWrite();
                    }
                }
            }
        }
    }

    private static class CxnStatsData {
        ClientAffinityStats stats;
        boolean readOnly;
        boolean roundRobin;
    }

    /**
     * Shutdown the VoltNetwork allowing the Ports to close and free resources
     * like memory pools
     * @throws InterruptedException
     */
    final void shutdown() throws InterruptedException {
        m_shutdown.set(true);
        if (CoreUtils.isJunitTest()) {
            m_timeoutReaperHandle.cancel(true);
            m_ex.shutdownNow();
        } else {
            // stop callbacks if we were in non-blocking mode
            m_rateLimiter.setNonblockingResumeHook(null);
            // stop the old proc call reaper
            m_timeoutReaperHandle.cancel(false);
            m_ex.shutdown();
            m_ex.awaitTermination(365, TimeUnit.DAYS);
        }

        m_network.shutdown();
        if (m_cipherService != null) {
            m_cipherService.shutdown();
            m_cipherService = null;
        }
    }

    void uncaughtException(ProcedureCallback cb, ClientResponse r, Throwable t) {
        boolean handledByClient = false;
        for (ClientStatusListenerExt csl : m_listeners) {
            if (csl instanceof ClientImpl.InternalClientStatusListener) {
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
        return new ClientStatsContext(this, getStatsSnapshot(), getIOStatsSnapshot(),
                getAffinityStatsSnapshot());
    }

    Map<Long, Map<String, ClientStats>> getStatsSnapshot() {
        Map<Long, Map<String, ClientStats>> retval =
                new TreeMap<>();

            for (NodeConnection conn : m_connections) {
                Map<String, ClientStats> connMap = new TreeMap<>();
                for (Entry<String, ClientStats> e : conn.m_stats.entrySet()) {
                    connMap.put(e.getKey(), (ClientStats) e.getValue().clone());
                }
                retval.put(conn.connectionId(), connMap);
            }


        return retval;
    }

    Map<Long, ClientIOStats> getIOStatsSnapshot() {
        Map<Long, ClientIOStats> retval = new TreeMap<>();

        Map<Long, Pair<String, long[]>> ioStats;
        try {
            ioStats = m_network.getIOStats(false, ImmutableList.<IOStatsIntf>of());
        } catch (Exception e) {
            return null;
        }

        for (NodeConnection conn : m_connections) {
            Pair<String, long[]> perConnIOStats = ioStats.get(conn.connectionId());
            if (perConnIOStats == null) {
                continue;
            }

            long read = perConnIOStats.getSecond()[0];
            long write = perConnIOStats.getSecond()[2];

            ClientIOStats cios = new ClientIOStats(conn.connectionId(), read, write);
            retval.put(conn.connectionId(), cios);
        }

        return retval;
    }

    Map<Integer, ClientAffinityStats> getAffinityStatsSnapshot()
    {
        Map<Integer, ClientAffinityStats> retval = new HashMap<>();
        // these get modified under this lock in queue()
        synchronized(this) {
            for (Entry<Integer, ClientAffinityStats> e : m_clientAffinityStats.entrySet()) {
                retval.put(e.getKey(), (ClientAffinityStats)e.getValue().clone());
            }
        }
        return retval;
    }

    public synchronized Object[] getInstanceId() {
        return m_clusterInstanceId;
    }

    public synchronized void resetInstanceId() {
        m_clusterInstanceId = null;
    }

    public String getBuildString() {
        return m_buildString;
    }

    public List<Long> getThreadIds() {
        return m_network.getThreadIds();
    }

    public List<InetSocketAddress> getConnectedHostList() {
        ArrayList<InetSocketAddress> addressList = new ArrayList<>();
        for (NodeConnection conn : m_connections) {
            addressList.add(conn.getSocketAddress());
        }
        return Collections.unmodifiableList(addressList);
    }

    public Map<String, Integer> getConnectedHostIPAndPort() {
        Map<String, Integer> connectedHostIPAndPortMap = Maps.newHashMap();
        for (NodeConnection conn : m_connections) {
            connectedHostIPAndPortMap.put(conn.getSocketAddress().getAddress().getHostAddress(), (conn.getSocketAddress().getPort()));
        }
        return Collections.unmodifiableMap(connectedHostIPAndPortMap);
    }

    private void updateAffinityTopology(VoltTable[] tables) {
        //First table contains the description of partition ids master/slave relationships
        VoltTable vt = tables[0];

        //In future let TOPO return cooked bytes when cooked and we use correct recipe
        //TODO: this method now only called if tables.length > 1; remove excess code
        boolean cooked = false;
        if (tables.length == 1) {
            //Just in case the new client connects to the old version of Volt that only returns 1 topology table
            // We're going to get the MPI back in this table, so subtract it out from the number of partitions.
            int numPartitions = vt.getRowCount() - 1;
            m_hashinator = new HashinatorLite(numPartitions); // legacy only
        } else {
            //Second table contains the hash function
            boolean advanced = tables[1].advanceRow();
            if (!advanced) {
                System.err.println("Topology description received from Volt was incomplete " +
                                   "performance will be lower because transactions can't be routed at this client");
                return;
            }
            m_hashinator = new HashinatorLite(
                    tables[1].getVarbinary("HASHCONFIG"),
                    cooked);
        }
        m_partitionMasters.clear();

        // The MPI's partition ID is 16383 (MpInitiator.MP_INIT_PID), so we shouldn't inadvertently
        // hash to it.  Go ahead and include it in the maps, we can use it at some point to
        // route MP transactions directly to the MPI node.
        Set<Integer> unconnected = new HashSet<Integer>();
        while (vt.advanceRow()) {
            Integer partition = (int)vt.getLong("Partition");

            String leader = vt.getString("Leader");
            String sites = vt.getString("Sites");
            if (Strings.isNullOrEmpty(sites) || Strings.isNullOrEmpty(leader)) {
                continue;
            }

            for (String site : sites.split(",")) {
                site = site.trim();
                Integer hostId = Integer.valueOf(site.split(":")[0]);
                if (!m_hostIdToConnection.containsKey(hostId)) {
                    unconnected.add(hostId);
               }
            }

            Integer leaderHostId = Integer.valueOf(leader.split(":")[0]);
            if (m_hostIdToConnection.containsKey(leaderHostId)) {
                m_partitionMasters.put(partition, m_hostIdToConnection.get(leaderHostId));
            }
        }
        if (m_topologyChangeAware) {
            m_unconnectedHosts.set(ImmutableSet.copyOf(unconnected));
        }
        refreshPartitionKeys(true);
    }

    private void updateProcedurePartitioning(VoltTable vt) {
        Map<String, Procedure> procs = Maps.newHashMap();
        while (vt.advanceRow()) {
            try {
                //Data embedded in JSON object in remarks column
                String jsString = vt.getString(6);
                String procedureName = vt.getString(2);
                JSONObject jsObj = new JSONObject(jsString);
                boolean readOnly = jsObj.optBoolean(Constants.JSON_READ_ONLY);
                boolean compound = jsObj.optBoolean(Constants.JSON_COMPOUND);
                boolean single = jsObj.optBoolean(Constants.JSON_SINGLE_PARTITION);
                int partitionParam = Procedure.PARAMETER_NONE;
                int paramType = Procedure.PARAMETER_NONE;
                if (single) {
                    partitionParam = jsObj.getInt(Constants.JSON_PARTITION_PARAMETER);
                    paramType = jsObj.getInt(Constants.JSON_PARTITION_PARAMETER_TYPE);
                }
                procs.put(procedureName, new Procedure(single, compound, readOnly,
                                                       partitionParam, paramType));
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        ImmutableSortedMap<String, Procedure> oldProcs = m_procedureInfo.get();
        m_procedureInfo.compareAndSet(oldProcs, ImmutableSortedMap.copyOf(procs));
    }

    private void updatePartitioning(VoltTable vt) {
        ImmutableMap.Builder<Integer, Integer> builder = ImmutableMap.builder();
        while (vt.advanceRow()) {
            //check for mock unit test
            if (vt.getColumnCount() == 2) {
                Integer partitionId = (int) vt.getLong("PARTITION_ID");
                Integer key = (int)(vt.getLong("PARTITION_KEY"));
                builder.put(partitionId, key);
            }
        }
        m_partitionKeys.set(builder.build());
    }

    /**
     * Return if Hashinator is initialed. This is useful only for non standard clients.
     * This will only only ever return true if client affinity is turned on.
     *
     * @return
     */
    public boolean isHashinatorInitialized() {
        return (m_hashinator != null);
    }

    /**
     * This is used by clients such as CSVLoader which puts processing into buckets.
     *
     * @param typeValue volt Type
     * @param value the representative value
     * @return
     */
    public long getPartitionForParameter(byte typeValue, Object value) {
        if (m_hashinator == null) {
            return -1;
        }
        return m_hashinator.getHashedPartitionForParameter(typeValue, value);
    }

    /**
     * This is used by clients such as VoltDBKafkaPartitioner which puts processing into buckets.
     *
     * @param value the representative value
     * @return
     */
    public long getPartitionForParameter(byte[] bytes) {
        if (m_hashinator == null) {
            return -1;
        }
        return m_hashinator.getHashedPartitionForParameter(bytes);
    }

    private ByteBuffer serializeSPI(ProcedureInvocation pi) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(pi.getSerializedSize() + 4);
        buf.putInt(buf.capacity() - 4);
        pi.flattenToBuffer(buf);
        buf.flip();
        return buf;
    }

    long getProcedureTimeoutNanos() {
        return m_procedureCallTimeoutNanos;
    }

    ImmutableMap<Integer, Integer> getPartitionKeys() throws NoConnectionsException, IOException, ProcCallException {
        refreshPartitionKeys(false);

        if (m_partitionUpdateStatus.get().getStatus() != ClientResponse.SUCCESS) {
            throw new ProcCallException(m_partitionUpdateStatus.get());
        }

        return m_partitionKeys.get();
    }

    /**
     * Set up partitions.
     * @param topologyUpdate  if true, it is called from topology update
     * @throws ProcCallException on any VoltDB specific failure.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     * @throws IOException if there is a Java network or connection problem.
     */
    private void refreshPartitionKeys(boolean topologyUpdate)  {

        if (m_shutdown.get()) {
            return;
        }

        try {
            ProcedureInvocation invocation = makeProcedureInvocation(m_sysHandle.getAndDecrement(), "@GetPartitionKeys", "INTEGER");
            CountDownLatch latch = null;

            if (!topologyUpdate) {
                latch = new CountDownLatch(1);
            }
            PartitionUpdateCallback cb = new PartitionUpdateCallback(latch);
            if (!queue(invocation, cb, true, System.nanoTime(), USE_DEFAULT_CLIENT_TIMEOUT)) {
                m_partitionUpdateStatus.set(new ClientResponseImpl(ClientResponseImpl.SERVER_UNAVAILABLE, new VoltTable[0],
                        "Fails to queue the partition update query, please try later."));
            }
            if (!topologyUpdate) {
                latch.await(1, TimeUnit.MINUTES);
            }
            m_lastPartitionKeyFetched.set(System.currentTimeMillis());
        } catch (InterruptedException | IOException e) {
            m_partitionUpdateStatus.set(new ClientResponseImpl(ClientResponseImpl.SERVER_UNAVAILABLE, new VoltTable[0],
                    "Fails to fetch partition keys from server:" + e.getMessage()));
        }
    }

    /**
     * Build procedure invocation for internally-generated procedure calls.
     * This all go at "almost the highest" priority; only the highest prio
     * user requests will bypass them.
     */
    private ProcedureInvocation makeProcedureInvocation(long handle, String procName, Object... parameters) {
        return new ProcedureInvocation(handle, BatchTimeoutOverrideType.NO_TIMEOUT, ProcedureInvocation.NO_PARTITION,
                                       m_sysRequestPrio, procName, parameters);
    }

    void useRequestPriority() {
        m_sysRequestPrio = Priority.HIGHEST_PRIORITY + 1;
    }

    /**
     * Configure topology-change awareness
     * (Implies autoreconnect)
     */
    void setTopologyChangeAware(boolean topoAware) {
        m_topologyChangeAware = topoAware;
    }

    /**
     * Called inline with every queued transaction, to determine if any
     * topology changes have occurred that require new connections.
     * TODO: this seems sort of expensive when there is usually
     *       going to be no change. Make more efficient?
     * TODO: there is an apparent race between the thread that sets
     *       'in progress' true and the thread that discovers a non-empty
     *       set of unconnected hosts. Is this important?
     */
    void createConnectionsUponTopologyChange() {
        if (!m_topologyChangeAware) {
            return;
        }
        if (!m_createConnectionUponTopoChangeInProgress.compareAndSet(false, true)) {
            return;
        }
        try {
            ImmutableSet<Integer> unconnected = m_unconnectedHosts.get();
            if (unconnected == null || unconnected.isEmpty()) {
                return;
            }
            ClientImpl.InternalClientStatusListener internalListener = null;
            for (ClientStatusListenerExt csl : m_listeners) {
                if (csl instanceof ClientImpl.InternalClientStatusListener) {
                    internalListener = (ClientImpl.InternalClientStatusListener) csl;
                    break;
                }
            }
            if (internalListener == null) {
                return;
            }
            unconnected = m_unconnectedHosts.getAndSet(ImmutableSet.copyOf(new HashSet<Integer>()));
            for (Integer host : unconnected) {
                if (!isHostConnected(host)) {
                    internalListener.createConnectionsUponTopologyChange();
                    break;
                }
            }
        } finally {
            m_createConnectionUponTopoChangeInProgress.set(false);
        }
    }

    /**
     * Call from client implementation when a new connection has been made
     * by the topology-aware connection task. Requests updated topology info.
     */
    void setCreateConnectionsUponTopologyChangeComplete() throws NoConnectionsException {
        m_createConnectionUponTopoChangeInProgress.set(false); // TODO: WHY?
        ProcedureInvocation spi = makeProcedureInvocation(m_sysHandle.getAndDecrement(), "@Statistics", "TOPO", 0);
        queue(spi, new TopoUpdateCallback(), true, System.nanoTime(), USE_DEFAULT_CLIENT_TIMEOUT);
    }

    /**
     * Utility: do we have a connection for this host?
     */
    boolean isHostConnected(Integer hostId) {
        return m_hostIdToConnection.containsKey(hostId);
    }
}
