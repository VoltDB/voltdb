/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import javax.security.auth.Subject;
import com.google_voltpatches.common.collect.ImmutableList;

import jsr166y.ThreadLocalRandom;

import org.cliffc_voltpatches.high_scale_lib.NonBlockingHashMap;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.network.Connection;
import org.voltcore.network.QueueMonitor;
import org.voltcore.network.VoltNetworkPool;
import org.voltcore.network.VoltNetworkPool.IOStatsIntf;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientStatusListenerExt.DisconnectCause;
import org.voltdb.client.HashinatorLite.HashinatorLiteType;
import org.voltdb.common.Constants;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableList;

/**
 *   De/multiplexes transactions across a cluster
 *
 *   It is safe to synchronized on an individual connection and then the distributer, but it is always unsafe
 *   to synchronized on the distributer and then an individual connection.
 */
class Distributer {

    static int RESUBSCRIPTION_DELAY_MS = Integer.getInteger("RESUBSCRIPTION_DELAY_MS", 10000);
    static final long PING_HANDLE = Long.MAX_VALUE;
    public static final Long ASYNC_TOPO_HANDLE = PING_HANDLE - 1;
    static final long USE_DEFAULT_TIMEOUT = 0;

    // handles used internally are negative and decrement for each call
    public final AtomicLong m_sysHandle = new AtomicLong(-1);

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
        final static int PARAMETER_NONE = -1;
        private final boolean multiPart;
        private final boolean readOnly;
        private final int partitionParameter;
        private final int partitionParameterType;
        private Procedure(boolean multiPart,
                boolean readOnly,
                int partitionParameter,
                int partitionParameterType) {
            this.multiPart = multiPart;
            this.readOnly = readOnly;
            this.partitionParameter = multiPart? PARAMETER_NONE : partitionParameter;
            this.partitionParameterType = multiPart ? PARAMETER_NONE : partitionParameterType;
        }
    }

    private final Map<Integer, NodeConnection> m_partitionMasters = new HashMap<Integer, NodeConnection>();
    private final Map<Integer, NodeConnection[]> m_partitionReplicas = new HashMap<Integer, NodeConnection[]>();
    private final Map<Integer, NodeConnection> m_hostIdToConnection = new HashMap<Integer, NodeConnection>();
    private final Map<String, Procedure> m_procedureInfo = new HashMap<String, Procedure>();
    //This is the instance of the Hashinator we picked from TOPO used only for client affinity.
    private HashinatorLite m_hashinator = null;
    //This is a global timeout that will be used if a per-procedure timeout is not provided with the procedure call.
    private final long m_procedureCallTimeoutNanos;
    private static final long MINIMUM_LONG_RUNNING_SYSTEM_CALL_TIMEOUT_MS = 30 * 60 * 1000; // 30 minutes
    private final long m_connectionResponseTimeoutNanos;
    private final Map<Integer, ClientAffinityStats> m_clientAffinityStats =
        new HashMap<Integer, ClientAffinityStats>();

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

    /*
     * The connection we have issued our subscriptions to. If the connection is lost
     * we will need to request subscription from a different node
     */
    private NodeConnection m_subscribedConnection = null;
    //Track if a request is pending so we don't accidentally handle a failed node twice
    private boolean m_subscriptionRequestPending = false;

    //Until catalog subscription is implemented, only fetch it once
    private boolean m_fetchedCatalog = false;

    /**
     * JAAS Authentication Subject
     */
    private final Subject m_subject;

    /**
     * Handles topology updates for client affinity
     */
    class TopoUpdateCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) return;
            try {
                synchronized (Distributer.this) {
                    VoltTable results[] = clientResponse.getResults();
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
     * Handles @Subscribe response
     */
    class SubscribeCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            //Pre 4.1 clusers don't know about subscribe, don't stress over it.
            if (response.getStatusString() != null &&
                response.getStatusString().contains("@Subscribe was not found")) {
                synchronized (Distributer.this) {
                    m_subscriptionRequestPending = false;
                }
                return;
            }
            //Fast path subscribing retry if the connection was lost before getting a response
            if (response.getStatus() == ClientResponse.CONNECTION_LOST && !m_connections.isEmpty()) {
                subscribeToNewNode();
                return;
            } else if (response.getStatus() == ClientResponse.CONNECTION_LOST) {
                return;
            }

            //Slow path, god knows why it didn't succeed, could be a bug. Don't firehose attempts.
            if (response.getStatus() != ClientResponse.SUCCESS && !m_ex.isShutdown()) {
                System.err.println("Error response received subscribing to topology updates.\n " +
                                   "Performance may be reduced on topology updates. Error was \"" +
                                    response.getStatusString() + "\"");
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
    class ProcUpdateCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) return;
            try {
                synchronized (Distributer.this) {
                    VoltTable results[] = clientResponse.getResults();
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

    class CallExpiration implements Runnable {
        @Override
        public void run() {
            try {
                // make a threadsafe copy of all connections
                ArrayList<NodeConnection> connections = new ArrayList<NodeConnection>();
                synchronized (Distributer.this) {
                    connections.addAll(m_connections);
                }

                final long nowNanos = System.nanoTime();

                // for each connection
                for (final NodeConnection c : connections) {
                    // check for connection age
                    final long sinceLastResponse = Math.max(1, nowNanos - c.m_lastResponseTimeNanos);

                    // if outstanding ping and timeoutMS, close the connection
                    if (c.m_outstandingPing && (sinceLastResponse > m_connectionResponseTimeoutNanos)) {
                        // memoize why it's closing
                        c.m_closeCause = DisconnectCause.TIMEOUT;
                        // this should trigger NodeConnection.stopping(..)
                        c.m_connection.unregister();
                    }

                    // if 1/3 of the timeoutMS since last response, send a ping
                    if ((!c.m_outstandingPing) && (sinceLastResponse > (m_connectionResponseTimeoutNanos / 3))) {
                        c.sendPing();
                    }

                    // for each outstanding procedure
                    for (final Map.Entry<Long, CallbackBookeeping> e : c.m_callbacks.entrySet()) {
                        final long handle = e.getKey();
                        final CallbackBookeeping cb = e.getValue();

                        // if the timeout is expired, call the callback and remove the
                        // bookeeping data
                        final long deltaNanos = Math.max(1, nowNanos - cb.timestampNanos);
                        if (deltaNanos > cb.procedureTimeoutNanos) {

                            //For expected long operations don't use the default timeout
                            //unless it is > MINIMUM_LONG_RUNNING_SYSTEM_CALL_TIMEOUT_MS
                            final boolean isLongOp = isLongOp(cb.name);
                            if (isLongOp && (deltaNanos < TimeUnit.MILLISECONDS.toNanos(MINIMUM_LONG_RUNNING_SYSTEM_CALL_TIMEOUT_MS))) {
                                continue;
                            }

                            c.handleTimedoutCallback(handle, nowNanos);
                        }
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    /*
     * Check if the proc name is a procedure that is expected to run long
     * Make the minimum timeoutMS for certain long running system procedures
     * higher than the default 2m.
     * you can still set the default timeoutMS higher than even this value
     *
     */
    private static boolean isLongOp(String procName) {
        if (procName.startsWith("@")) {
            if (procName.equals("@UpdateApplicationCatalog") || procName.equals("@SnapshotSave")) {
                return true;
            }
        }
        return false;
    }

    class CallbackBookeeping {
        public CallbackBookeeping(long timestampNanos, ProcedureCallback callback, String name, long timeoutNanos, boolean ignoreBackpressure) {
            assert(callback != null);
            this.timestampNanos = timestampNanos;
            this.callback = callback;
            this.name = name;
            this.procedureTimeoutNanos = timeoutNanos;
            this.ignoreBackpressure = ignoreBackpressure;
        }
        long timestampNanos;
        //Timeout in ms 0 means use conenction specified procedure timeoutMS.
        final long procedureTimeoutNanos;
        ProcedureCallback callback;
        String name;
        boolean ignoreBackpressure;
    }

    class NodeConnection extends VoltProtocolHandler implements org.voltcore.network.QueueMonitor {
        private final AtomicInteger m_callbacksToInvoke = new AtomicInteger(0);
        private final ConcurrentMap<Long, CallbackBookeeping> m_callbacks = new ConcurrentHashMap<Long, CallbackBookeeping>();
        private final NonBlockingHashMap<String, ClientStats> m_stats = new NonBlockingHashMap<String, ClientStats>();
        private Connection m_connection;
        private volatile boolean m_isConnected = true;

        volatile long m_lastResponseTimeNanos = System.nanoTime();
        boolean m_outstandingPing = false;
        ClientStatusListenerExt.DisconnectCause m_closeCause = DisconnectCause.CONNECTION_CLOSED;

        public NodeConnection(long ids[]) {}

        /*
         * NodeConnection uses ignoreBackpressure to get rate limiter to not
         * apply any permit tracking or rate limits to transactions that should
         * never be rejected such as those submitted from within a callback thread or
         * generated internally
         */
        public void createWork(final long nowNanos, long handle, String name, ByteBuffer c,
                ProcedureCallback callback, boolean ignoreBackpressure, long timeoutNanos) {
            assert(callback != null);

            //How long from the starting point in time to wait to get this stuff done
            timeoutNanos = (timeoutNanos == Distributer.USE_DEFAULT_TIMEOUT) ? m_procedureCallTimeoutNanos : timeoutNanos;

            //Trigger the timeout at this point in time no matter what
            final long timeoutTime = nowNanos + timeoutNanos;

            //What was the time after the rate limiter returned
            //Will be the same as timeoutNanos if it didn't block
            long afterRateLimitNanos = 0;

            /*
             * Do rate limiting or check for max outstanding related backpressure in
             * the rate limiter which can block. If it blocks we can still get a timeout
             * exception to give prompt timeouts
             */
            try {
                afterRateLimitNanos = m_rateLimiter.sendTxnWithOptionalBlockAndReturnCurrentTime(
                        nowNanos, timeoutNanos, ignoreBackpressure);
            } catch (TimeoutException e) {
                /*
                 * It's possible we need to timeout because it took too long to get
                 * the transaction out on the wire due to max outstanding
                 */
                final long deltaNanos = Math.max(1, System.nanoTime() - nowNanos);
                    invokeCallbackWithTimeout(name, callback, deltaNanos, afterRateLimitNanos,  timeoutNanos, handle, ignoreBackpressure);
                return;
            }

            assert(m_callbacks.containsKey(handle) == false);

            //Drain needs to know when all callbacks have been invoked
            final int callbacksToInvoke = m_callbacksToInvoke.incrementAndGet();
            assert(callbacksToInvoke >= 0);

            //Optimistically submit the task
            m_callbacks.put(handle, new CallbackBookeeping(nowNanos, callback, name, timeoutNanos, ignoreBackpressure));

            //Schedule the timeout to fire relative to the amount of time
            //spent getting to this point. Might fire immediately
            //some of the time, but that is fine
            final long timeoutRemaining = timeoutTime - afterRateLimitNanos;

            //Schedule an individual timeout if necessary
            //If it is a long op, don't bother scheduling a discrete timeout
            if (timeoutNanos < TimeUnit.SECONDS.toNanos(1) && !isLongOp(name)) {
                submitDiscreteTimeoutTask(handle, Math.max(0, timeoutRemaining));
            }

            //Check for disconnect
            if (!m_isConnected) {
                //Check if the disconnect or expiration already handled the callback
                if (m_callbacks.remove(handle) == null) return;
                final ClientResponse r = new ClientResponseImpl(
                        ClientResponse.CONNECTION_LOST, new VoltTable[0],
                        "Connection to database host (" + m_connection.getHostnameAndIPAndPort() +
                ") was lost before a response was received");
                try {
                    callback.clientCallback(r);
                } catch (Exception e) {
                    uncaughtException(callback, r, e);
                }

                //Drain needs to know when all callbacks have been invoked
                final int remainingToInvoke = m_callbacksToInvoke.decrementAndGet();
                assert(remainingToInvoke >= 0);

                //for bookkeeping, but it feels dishonest to call this here
                m_rateLimiter.transactionResponseReceived(nowNanos, -1, ignoreBackpressure);
                return;
            } else {
                m_connection.writeStream().enqueue(c);
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
         * still exists and needs to be invoked, or has already been handled by another thread
         */
        void handleTimedoutCallback(long handle, long nowNanos) {
            //Callback doesn't have to be there, it may have already
            //received a response or been expired by the periodic expiration task, or a discrete expiration task
            final CallbackBookeeping cb = m_callbacks.remove(handle);

            //It was handled during the race
            if (cb == null) return;

            final long deltaNanos = Math.max(1, nowNanos - cb.timestampNanos);

            invokeCallbackWithTimeout(cb.name, cb.callback, deltaNanos, nowNanos, cb.procedureTimeoutNanos, handle, cb.ignoreBackpressure);
        }

        /*
         * Factor out the boilerplate involved in invoking a callback with a timeout response
         */
        void invokeCallbackWithTimeout(String procName,
                                       ProcedureCallback callback,
                                       long deltaNanos,
                                       long nowNanos,
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
            r.setClientHandle(handle);
            r.setClientRoundtrip(deltaNanos);
            r.setClusterRoundtrip((int)TimeUnit.NANOSECONDS.toMillis(deltaNanos));
            try {
                callback.clientCallback(r);
            } catch (Throwable e1) {
                uncaughtException( callback, r, e1);
            }

            //Drain needs to know when all callbacks have been invoked
            final int remainingToInvoke = m_callbacksToInvoke.decrementAndGet();
            assert(remainingToInvoke >= 0);

            m_rateLimiter.transactionResponseReceived(nowNanos, -1, ignoreBackpressure);
            updateStatsForTimeout(procName, r.getClientRoundtripNanos(), r.getClusterRoundtrip());
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

        @Override
        public void handleMessage(ByteBuffer buf, Connection c) {
            long nowNanos = System.nanoTime();
            ClientResponseImpl response = new ClientResponseImpl();
            try {
                response.initFromBuffer(buf);
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            // track the timestamp of the most recent read on this connection
            m_lastResponseTimeNanos = nowNanos;

            final long handle = response.getClientHandle();

            // handle ping response and get out
            if (handle == PING_HANDLE) {
                m_outstandingPing = false;
                return;
            } else if (handle == ASYNC_TOPO_HANDLE) {
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

            //Race with expiration thread to be the first to remove the callback
            //from the map and process it
            final CallbackBookeeping stuff = m_callbacks.remove(response.getClientHandle());

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
                final long callTimeNanos = stuff.timestampNanos;
                final long deltaNanos = Math.max(1, nowNanos - callTimeNanos);
                final ProcedureCallback cb = stuff.callback;
                assert(cb != null);
                final byte status = response.getStatus();
                boolean abort = false;
                boolean error = false;
                if (status == ClientResponse.USER_ABORT || status == ClientResponse.GRACEFUL_FAILURE) {
                    abort = true;
                } else if (status != ClientResponse.SUCCESS) {
                    error = true;
                }

                int clusterRoundTrip = response.getClusterRoundtrip();
                m_rateLimiter.transactionResponseReceived(nowNanos, clusterRoundTrip, stuff.ignoreBackpressure);
                updateStats(stuff.name, deltaNanos, clusterRoundTrip, abort, error, false);
                response.setClientRoundtrip(deltaNanos);
                assert(response.getHash() == null); // make sure it didn't sneak into wire protocol
                try {
                    cb.clientCallback(response);
                } catch (Exception e) {
                    uncaughtException(cb, response, e);
                }

                //Drain needs to know when all callbacks have been invoked
                final int remainingToInvoke = m_callbacksToInvoke.decrementAndGet();
                assert(remainingToInvoke >= 0);
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
                    s.connectionLost(
                            m_connection.getHostnameOrIP(),
                            m_connection.getRemotePort(),
                            m_connections.size(),
                            m_closeCause);
                }

                /*
                 * Deal with the fact that this may have been the connection that subscriptions were issued
                 * to. If a subscription request was pending, don't handle selecting a new node here
                 * let the callback see the failure and retry
                 */
                if (m_useClientAffinity &&
                    m_subscribedConnection == this &&
                    m_subscriptionRequestPending == false &&
                    !m_ex.isShutdown()) {
                    //Don't subscribe to a new node immediately
                    //to somewhat prevent a thundering herd
                    m_ex.schedule(new Runnable() {
                        @Override
                        public void run() {
                            subscribeToNewNode();
                        }
                    }, new Random().nextInt(RESUBSCRIPTION_DELAY_MS), TimeUnit.MILLISECONDS);
                }
            }

            //Invoke callbacks for all queued invocations with a failure response
            final ClientResponse r =
                new ClientResponseImpl(
                        ClientResponse.CONNECTION_LOST, new VoltTable[0],
                        "Connection to database host (" + m_connection.getHostnameAndIPAndPort() +
                ") was lost before a response was received");
            for (Map.Entry<Long, CallbackBookeeping> e : m_callbacks.entrySet()) {
                //Check for race with other threads
                if (m_callbacks.remove(e.getKey()) == null) continue;
                final CallbackBookeeping callBk = e.getValue();
                try {
                    callBk.callback.clientCallback(r);
                }
                catch (Exception ex) {
                    uncaughtException(callBk.callback, r, ex);
                }

                //Drain needs to know when all callbacks have been invoked
                final int remainingToInvoke = m_callbacksToInvoke.decrementAndGet();
                assert(remainingToInvoke >= 0);

                m_rateLimiter.transactionResponseReceived(System.nanoTime(), -1, callBk.ignoreBackpressure);
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

        public InetSocketAddress getSocketAddress() {
            return m_connection.getRemoteSocketAddress();
        }
    }

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
                if (Thread.interrupted()) throw new InterruptedException();
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(sleep));
                if (Thread.interrupted()) throw new InterruptedException();
                if (sleep < 5000) {
                    sleep += 500;
                }
            }
        } while(more);
    }

    Distributer() {
        this( false,
                ClientConfig.DEFAULT_PROCEDURE_TIMOUT_NANOS,
                ClientConfig.DEFAULT_CONNECTION_TIMOUT_MS,
                false, null);
    }

    Distributer(
            boolean useMultipleThreads,
            long procedureCallTimeoutNanos,
            long connectionResponseTimeoutMS,
            boolean useClientAffinity,
            Subject subject) {
        m_useMultipleThreads = useMultipleThreads;
        m_network = new VoltNetworkPool(
                m_useMultipleThreads ? Math.max(1, CoreUtils.availableProcessors() / 4 ) : 1,
                1, null, "Client");
        m_network.start();
        m_procedureCallTimeoutNanos= procedureCallTimeoutNanos;
        m_connectionResponseTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(connectionResponseTimeoutMS);
        m_useClientAffinity = useClientAffinity;

        // schedule the task that looks for timed-out proc calls and connections
        m_timeoutReaperHandle = m_ex.scheduleAtFixedRate(new CallExpiration(), 1, 1, TimeUnit.SECONDS);
        m_subject = subject;
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
            ConnectionUtil.getAuthenticatedConnection(host, program, hashedPassword, port, m_subject);
        InetSocketAddress address = new InetSocketAddress(host, port);
        final SocketChannel aChannel = (SocketChannel)socketChannelAndInstanceIdAndBuildString[0];
        final long instanceIdWhichIsTimestampAndLeaderIp[] = (long[])socketChannelAndInstanceIdAndBuildString[1];
        final int hostId = (int)instanceIdWhichIsTimestampAndLeaderIp[0];

        NodeConnection cxn = new NodeConnection(instanceIdWhichIsTimestampAndLeaderIp);
        Connection c = m_network.registerChannel( aChannel, cxn);
        cxn.m_connection = c;

        synchronized (this) {

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
        }

        if (m_useClientAffinity) {
            synchronized (this) {
                m_hostIdToConnection.put(hostId, cxn);
            }

            if (m_subscribedConnection == null) {
                subscribeToNewNode();
            }
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
            ProcedureInvocation spi = new ProcedureInvocation(m_sysHandle.getAndDecrement(), "@Subscribe", "TOPOLOGY");
            final ByteBuffer buf = serializeSPI(spi);
            cxn.createWork(System.nanoTime(),
                    spi.getHandle(),
                    spi.getProcName(),
                    serializeSPI(spi),
                    new SubscribeCallback(),
                    true,
                    USE_DEFAULT_TIMEOUT);

            spi = new ProcedureInvocation(m_sysHandle.getAndDecrement(), "@Statistics", "TOPO", 0);
            //The handle is specific to topology updates and has special cased handling
            cxn.createWork(System.nanoTime(),
                    spi.getHandle(),
                    spi.getProcName(),
                    serializeSPI(spi),
                    new TopoUpdateCallback(),
                    true,
                    USE_DEFAULT_TIMEOUT);

            //Don't need to retrieve procedure updates every time we do a new subscription
            //since catalog changes aren't correlated with node failure the same way topo is
            if (!m_fetchedCatalog) {
                spi = new ProcedureInvocation(m_sysHandle.getAndDecrement(), "@SystemCatalog", "PROCEDURES");
                //The handle is specific to procedure updates and has special cased handling
                cxn.createWork(System.nanoTime(),
                        spi.getHandle(),
                        spi.getProcName(),
                        serializeSPI(spi),
                        new ProcUpdateCallback(),
                        true,
                        USE_DEFAULT_TIMEOUT);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Queue invocation on first node connection without backpressure. If there is none with without backpressure
     * then return false and don't queue the invocation
     * @param invocation
     * @param cb
     * @param ignoreBackpressure If true the invocation will be queued even if there is backpressure
     * @param nowNanos Current time in nanoseconds using System.nanoTime
     * @param timeoutNanos nanoseconds from nowNanos where timeout should fire
     * @return True if the message was queued and false if the message was not queued due to backpressure
     * @throws NoConnectionsException
     */
    boolean queue(
            ProcedureInvocation invocation,
            ProcedureCallback cb,
            final boolean ignoreBackpressure, final long nowNanos, final long timeoutNanos)
            throws NoConnectionsException {
        assert(invocation != null);
        assert(cb != null);

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
             * routing, but backpressure will be managed anyways. This is where we guess partition based on client
             * affinity and known topology (hashinator initialized).
             */
            if (m_useClientAffinity && (m_hashinator != null)) {
                final Procedure procedureInfo = m_procedureInfo.get(invocation.getProcName());
                Integer hashedPartition = -1;

                if (procedureInfo != null) {
                    hashedPartition = Constants.MP_INIT_PID;
                    if (( ! procedureInfo.multiPart) &&
                        // User may have passed too few parameters to allow dispatching.
                        // Avoid an indexing error here to fall through to the proper ProcCallException.
                            (procedureInfo.partitionParameter < invocation.getPassedParamCount())) {
                        hashedPartition = m_hashinator.getHashedPartitionForParameter(
                                procedureInfo.partitionParameterType,
                                invocation.getPartitionParamValue(procedureInfo.partitionParameter));
                    }
                    /*
                     * If the procedure is read only and single part, load balance across replicas
                     */
                    if (!procedureInfo.multiPart && procedureInfo.readOnly) {
                        NodeConnection partitionReplicas[] = m_partitionReplicas.get(hashedPartition);
                        if (partitionReplicas != null && partitionReplicas.length > 0) {
                            cxn = partitionReplicas[ThreadLocalRandom.current().nextInt(partitionReplicas.length)];
                            if (cxn.hadBackPressure()) {
                                //See if there is one without backpressure, make sure it's still connected
                                for (NodeConnection nc : partitionReplicas) {
                                    if (!nc.hadBackPressure() && nc.m_isConnected) {
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
                if (cxn != null && !cxn.m_isConnected) {
                    // Would be nice to log something here
                    // Client affinity picked a connection that was actually disconnected.  Reset to null
                    // and let the round-robin choice pick a connection
                    cxn = null;
                }
                ClientAffinityStats stats = m_clientAffinityStats.get(hashedPartition);
                if (stats == null) {
                    stats = new ClientAffinityStats(hashedPartition, 0, 0, 0, 0);
                    m_clientAffinityStats.put(hashedPartition, stats);
                }
                if (cxn != null) {
                    if (procedureInfo != null && procedureInfo.readOnly) {
                        stats.addAffinityRead();
                    }
                    else {
                        stats.addAffinityWrite();
                    }
                }
                // account these here because we lose the partition ID and procedure info once we
                // bust out of this scope.
                else {
                    if (procedureInfo != null && procedureInfo.readOnly) {
                        stats.addRrRead();
                    }
                    else {
                        stats.addRrWrite();
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
            ByteBuffer buf = null;
            try {
                buf = serializeSPI(invocation);
            } catch (Exception e) {
                Throwables.propagate(e);
            }
            cxn.createWork(nowNanos, invocation.getHandle(), invocation.getProcName(), buf, cb, ignoreBackpressure, timeoutNanos);
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

    void uncaughtException(ProcedureCallback cb, ClientResponse r, Throwable t) {
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
        return new ClientStatsContext(this, getStatsSnapshot(), getIOStatsSnapshot(),
                getAffinityStatsSnapshot());
    }

    Map<Long, Map<String, ClientStats>> getStatsSnapshot() {
        Map<Long, Map<String, ClientStats>> retval =
                new TreeMap<Long, Map<String, ClientStats>>();

            for (NodeConnection conn : m_connections) {
                Map<String, ClientStats> connMap = new TreeMap<String, ClientStats>();
                for (Entry<String, ClientStats> e : conn.m_stats.entrySet()) {
                    connMap.put(e.getKey(), (ClientStats) e.getValue().clone());
                }
                retval.put(conn.connectionId(), connMap);
            }


        return retval;
    }

    Map<Long, ClientIOStats> getIOStatsSnapshot() {
        Map<Long, ClientIOStats> retval = new TreeMap<Long, ClientIOStats>();

        Map<Long, Pair<String, long[]>> ioStats;
        try {
            ioStats = m_network.getIOStats(false, ImmutableList.<IOStatsIntf>of());
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

    Map<Integer, ClientAffinityStats> getAffinityStatsSnapshot()
    {
        Map<Integer, ClientAffinityStats> retval = new HashMap<Integer, ClientAffinityStats>();
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

    /**
     * Not exposed to users for the moment.
     */
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
        ArrayList<InetSocketAddress> addressList = new ArrayList<InetSocketAddress>();
        for (NodeConnection conn : m_connections) {
            addressList.add(conn.getSocketAddress());
        }
        return Collections.unmodifiableList(addressList);
    }

    private void updateAffinityTopology(VoltTable tables[]) {
        //First table contains the description of partition ids master/slave relationships
        VoltTable vt = tables[0];

        //In future let TOPO return cooked bytes when cooked and we use correct recipe
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
                    HashinatorLiteType.valueOf(tables[1].getString("HASHTYPE")),
                    tables[1].getVarbinary("HASHCONFIG"),
                    cooked);
        }
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
                String procedureName = vt.getString(2);
                JSONObject jsObj = new JSONObject(jsString);
                boolean readOnly = jsObj.getBoolean(Constants.JSON_READ_ONLY);
                if (jsObj.getBoolean(Constants.JSON_SINGLE_PARTITION)) {
                    int partitionParameter = jsObj.getInt(Constants.JSON_PARTITION_PARAMETER);
                    int partitionParameterType =
                        jsObj.getInt(Constants.JSON_PARTITION_PARAMETER_TYPE);
                    m_procedureInfo.put(procedureName,
                            new Procedure(false,readOnly, partitionParameter, partitionParameterType));
                } else {
                    // Multi Part procedure JSON descriptors omit the partitionParameter
                    m_procedureInfo.put(procedureName, new Procedure(true, readOnly, Procedure.PARAMETER_NONE,
                                Procedure.PARAMETER_NONE));
                }

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
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

    public HashinatorLiteType getHashinatorType() {
        if (m_hashinator == null) {
            return HashinatorLiteType.LEGACY;
        }
        return m_hashinator.getConfigurationType();
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
}
