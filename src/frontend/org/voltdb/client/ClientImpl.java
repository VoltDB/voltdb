/* This file is part of VoltDB.
 * Copyright (C) 2008-2021 VoltDB Inc.
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

import java.io.File;
import java.io.FileWriter;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import com.google_voltpatches.common.net.HostAndPort;
import io.netty.handler.ssl.SslContext;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.ssl.SSLConfiguration;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientStatusListenerExt.AutoConnectionStatus;
import org.voltdb.client.ClientStatusListenerExt.DisconnectCause;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.BulkLoaderState;
import org.voltdb.client.VoltBulkLoader.BulkLoaderSuccessCallback;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;
import org.voltdb.common.Constants;
import org.voltdb.utils.Encoder;


/**
 *  A client that connects to one or more nodes in a VoltCluster
 *  and provides methods to call stored procedures and receive
 *  responses.
 */
public final class ClientImpl implements Client {
    private static final Logger LOG = Logger.getLogger(ClientImpl.class.getName());

    // Time to wait before retrying a failed connection attempt
    private static final int CONNECTION_RETRY_DELAY_SEC = 10;

    // Global instance of null callback for performance (we only need one)
    private static final ProcedureCallback NULL_CALLBACK = new NullCallback();

    // Calls initiated by the user use positive handles
    private final AtomicLong m_handle = new AtomicLong(0);

    // Username and password as set from config
    private final String m_username;
    private final byte[] m_passwordHash;
    private final ClientAuthScheme m_hashScheme;

    // SSL context (null if not using SSL)
    private final SslContext m_sslContext;

    // Mux/demux connections to cluster
    private final Distributer m_distributer;

    // True for any kind of automatic reconnect.
    private final boolean m_autoReconnect;

    // For reconnect on connection loss (null if not enabled
    // or if running in topology-change-aware mode).
    private final ReconnectStatusListener m_reconnectStatusListener;

    // Execution service for topology-change-aware clients
    private final ScheduledExecutorService m_ex;
    private final boolean m_topologyChangeAware;

    // Our own status listener - package access for unit test
    final InternalClientStatusListener m_listener = new InternalClientStatusListener();

    // User's status listener
    private final ClientStatusListenerExt m_clientStatusListener;

    // Flow control mechanisms.
    // Threads belonging to the network thread pool that invokes callbacks are "blessed"
    // and should never experience backpressure. This ensures that the network thread
    // pool doesn't block when queuing procedures from a callback.
    private final Object m_backpressureLock = new Object();
    private final CopyOnWriteArrayList<Long> m_blessedThreadIds = new CopyOnWriteArrayList<>();
    private boolean m_backpressure = false;
    private boolean m_blockingQueue = true;

    // Tracks historical connections for when we have nothing
    // to ask for topo info.
    private final Set<HostAndPort> m_connectHistory;
    private boolean m_newConnectEpoch;

    // For bulk-loader use
    private final BulkLoaderState m_vblGlobals = new BulkLoaderState(this);

    // Client shutdown flag
    private volatile boolean m_isShutdown;


    /****************************************************
                        Public API
     ****************************************************/

    /**
     * Create a new client without any initial connections.
     */
    ClientImpl(ClientConfig config) {

        String username = config.m_username;
        if (config.m_subject != null) {
            username = ClientConfig.getUserNameFromSubject(config.m_subject);
        }
        m_username = username;

        m_hashScheme = config.m_hashScheme;
        if (config.m_cleartext) {
            String passwd = config.m_password != null ? config.m_password : "";
            m_passwordHash = ConnectionUtil.getHashedPassword(m_hashScheme, passwd);
        } else {
            m_passwordHash = Encoder.hexDecode(config.m_password);
        }

        if (config.m_enableSSL) {
            m_sslContext = SSLConfiguration.createClientSslContext(config.m_sslConfig);
        } else {
            m_sslContext = null;
        }

        m_distributer = new Distributer(config.m_heavyweight,
                                        config.m_procedureCallTimeoutNanos,
                                        config.m_connectionResponseTimeoutMS,
                                        config.m_subject,
                                        m_sslContext);
        m_distributer.addClientStatusListener(m_listener);

        m_autoReconnect = config.m_reconnectOnConnectionLoss;
        m_topologyChangeAware = config.m_topologyChangeAware;

        m_distributer.setTopologyChangeAware(m_topologyChangeAware, m_autoReconnect);
        if (m_topologyChangeAware) {
            m_ex = Executors.newSingleThreadScheduledExecutor(CoreUtils.getThreadFactory("Topoaware thread"));
            m_connectHistory = new LinkedHashSet<>();
        } else {
            m_ex = null;
            m_connectHistory = null;
        }

        if (m_autoReconnect && !m_topologyChangeAware) {
            m_reconnectStatusListener = new ReconnectStatusListener(this,
                                                                    config.m_initialConnectionRetryIntervalMS,
                                                                    config.m_maxConnectionRetryIntervalMS);
            m_distributer.addClientStatusListener(m_reconnectStatusListener);
        } else {
            m_reconnectStatusListener = null;
        }

        if (config.m_listener != null) {
            m_distributer.addClientStatusListener(config.m_listener);
        }
        m_clientStatusListener = config.m_listener;

        if (config.m_autoTune) {
            m_distributer.m_rateLimiter.enableAutoTuning(config.m_autoTuneTargetInternalLatency);
        }
        else {
            assert(config.m_maxOutstandingTxns > 0);
            m_distributer.m_rateLimiter.setLimits(config.m_maxTransactionsPerSecond,
                                                  config.m_maxOutstandingTxns);
        }

        m_blessedThreadIds.addAll(m_distributer.getThreadIds());
    }

    /**
     * Not in public API; used by unit test
     */
    public SslContext getSSLContext() {
        return m_sslContext;
    }

    /**
     * Synchronously invoke a procedure call blocking until a result is available,
     * with default procedure timeout and no batch timeout.
     *
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return ClientResponse for execution.
     */
    @Override
    public final ClientResponse callProcedure(String procName,
                                              Object... parameters)
        throws IOException, NoConnectionsException, ProcCallException {
        return callProcedureWithClientTimeoutImpl(BatchTimeoutOverrideType.NO_TIMEOUT,
                                                  procName,
                                                  Distributer.USE_DEFAULT_CLIENT_TIMEOUT,
                                                  TimeUnit.SECONDS,
                                                  parameters);
    }

    /**
     * Synchronously invoke a procedure call blocking until a result is available,
     * with default procedure timeout.
     *
     * @param batchTimeout procedure invocation batch timeout.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return ClientResponse for execution.
     */
    @Override
    public ClientResponse callProcedureWithTimeout(int batchTimeout,
                                                   String procName,
                                                   Object... parameters)
        throws IOException, NoConnectionsException, ProcCallException {
        return callProcedureWithClientTimeoutImpl(batchTimeout,
                                                  procName,
                                                  Distributer.USE_DEFAULT_CLIENT_TIMEOUT,
                                                  TimeUnit.SECONDS,
                                                  parameters);
    }

    /**
     * Synchronously invoke a procedure call blocking until a result is available,
     * with caller-specified procedure timeout.
     *
     * NOTE: not in Client interface. WHY?
     *
     * @param batchTimeout procedure invocation batch timeout.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param clientTimeout timeout for the procedure
     * @param unit TimeUnit of procedure timeout
     * @param parameters vararg list of procedure's parameter values.
     * @return ClientResponse for execution.
     */
    public ClientResponse callProcedureWithClientTimeout(int batchTimeout,
                                                         String procName,
                                                         long clientTimeout,
                                                         TimeUnit unit,
                                                         Object... parameters)
        throws IOException, NoConnectionsException, ProcCallException {
        return callProcedureWithClientTimeoutImpl(batchTimeout,
                                                  procName,
                                                  clientTimeout,
                                                  unit,
                                                  parameters);
    }

    /**
     * Synchronously invoke a procedure call blocking until a result is available.
     * Common implementation.
     *
     * @param batchTimeout procedure invocation batch timeout.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param clientTimeout timeout for the procedure
     * @param unit TimeUnit of procedure timeout
     * @param parameters vararg list of procedure's parameter values.
     * @return ClientResponse for execution.
     */
    private ClientResponse callProcedureWithClientTimeoutImpl(int batchTimeout,
                                                              String procName,
                                                              long clientTimeout,
                                                              TimeUnit unit,
                                                              Object... parameters)
        throws IOException, NoConnectionsException, ProcCallException {
        long handle = m_handle.getAndIncrement();
        ProcedureInvocation invocation = new ProcedureInvocation(handle, batchTimeout,
                                                                 -1, procName, parameters);
        long nanos = unit.toNanos(clientTimeout);
        return internalSyncCallProcedure(nanos, invocation);
    }

    /**
     * Asynchronously invoke a procedure call.
     *
     * @param callback TransactionCallback that will be invoked with procedure results.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return True if the procedure was queued and false otherwise
     */
    @Override
    public final boolean callProcedure(ProcedureCallback callback,
                                       String procName,
                                       Object... parameters) throws IOException {
        //Time unit doesn't matter in this case since the timeout isn't being specified
        return callProcedureWithClientTimeout(callback,
                                              BatchTimeoutOverrideType.NO_TIMEOUT,
                                              -1,
                                              procName,
                                              Distributer.USE_DEFAULT_CLIENT_TIMEOUT,
                                              TimeUnit.NANOSECONDS,
                                              parameters);
    }

    /**
     * Asynchronously invoke a procedure call with specified batch timeout.
     *
     * @param callback TransactionCallback that will be invoked with procedure results.
     * @param batchTimeout procedure invocation batch timeout.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return True if the procedure was queued and false otherwise
     */
    @Override
    public final boolean callProcedureWithTimeout(ProcedureCallback callback,
                                                  int batchTimeout,
                                                  String procName,
                                                  Object... parameters) throws IOException {
        //Time unit doesn't matter in this case since the timeout isn't being specified
        return callProcedureWithClientTimeout(callback,
                                              batchTimeout,
                                              -1,
                                              procName,
                                              Distributer.USE_DEFAULT_CLIENT_TIMEOUT,
                                              TimeUnit.NANOSECONDS,
                                              parameters);
    }

    /**
     * Asynchronously invoke a procedure call with specified batch and query timeouts.
     *
     * NOTE: not in Client interface. WHY?
     *
     * @param callback TransactionCallback that will be invoked with procedure results.
     * @param batchTimeout procedure invocation batch timeout.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param clientTimeout query timeout
     * @param clientTimeoutUnit units for query timeout
     * @param parameters vararg list of procedure's parameter values.
     * @return True if the procedure was queued and false otherwise
     */
    public boolean callProcedureWithClientTimeout(ProcedureCallback callback,
                                                  int batchTimeout,
                                                  String procName,
                                                  long clientTimeout,
                                                  TimeUnit clientTimeoutUnit,
                                                  Object... parameters) throws IOException {
        return callProcedureWithClientTimeout(callback,
                                              batchTimeout,
                                              -1,
                                              procName,
                                              clientTimeout,
                                              clientTimeoutUnit,
                                              parameters);
    }

    /**
     * Asynchronously invoke a procedure call. Common implementation.
     *
     * @param callback TransactionCallback that will be invoked with procedure results.
     * @param batchTimeout procedure invocation batch timeout.
     * @param partitionDestination or -1
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param clientTimeout timeout for the procedure
     * @param unit TimeUnit of procedure timeout
     * @param parameters vararg list of procedure's parameter values.
     * @return True if the procedure was queued and false otherwise
     */
    public boolean callProcedureWithClientTimeout(ProcedureCallback callback,
                                                  int batchTimeout,
                                                  int partitionDestination,
                                                  String procName,
                                                  long clientTimeout,
                                                  TimeUnit clientTimeoutUnit,
                                                  Object... parameters) throws IOException {
        if (callback instanceof ProcedureArgumentCacher) {
            ((ProcedureArgumentCacher) callback).setArgs(parameters);
        }

        long handle = m_handle.getAndIncrement();
        ProcedureInvocation invocation = new ProcedureInvocation(handle, batchTimeout,
                                                                 partitionDestination,
                                                                 procName, parameters);
        if (m_isShutdown) {
            return false;
        }

        if (callback == null) {
            callback = NULL_CALLBACK;
        }

        return internalAsyncCallProcedure(callback, clientTimeoutUnit.toNanos(clientTimeout), invocation);
    }

    /**
     * TODO remove
     */
    @Deprecated
    @Override
    public int calculateInvocationSerializedSize(
            String procName,
            Object... parameters) {
        final ProcedureInvocation invocation =
            new ProcedureInvocation(0, procName, parameters);
        return invocation.getSerializedSize();
    }

    /**
     * TODO remove
     */
    @Deprecated
    @Override
    public final boolean callProcedure(
           ProcedureCallback callback,
           int expectedSerializedSize,
           String procName,
           Object... parameters) throws IOException {
        return callProcedure(callback, procName, parameters);
    }

    /**
     * Implementation of synchronous procedure call.
     *
     * @param clientTimeoutNanos timeout on this query
     * @param invocation the procedure to call
     */
    private final ClientResponse internalSyncCallProcedure(long clientTimeoutNanos,
                                                           ProcedureInvocation invocation)
        throws ProcCallException, IOException {

        if (m_isShutdown) {
            throw new NoConnectionsException("Client instance is shutdown");
        }

        if (m_blessedThreadIds.contains(Thread.currentThread().getId())) {
            throw new IOException("Can't invoke a procedure synchronously from within the client callback thread " +
                    " without deadlocking the client library");
        }

        SyncCallbackLight cb = new SyncCallbackLight();
        boolean success = internalAsyncCallProcedure(cb, clientTimeoutNanos, invocation);
        if (!success) {
            final ClientResponseImpl r = new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE,
                                                                ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                                                                "",
                                                                new VoltTable[0],
                                                                "Unable to queue client request.");
            throw new ProcCallException(r);
        }

        try {
            cb.waitForResponse();
        } catch (final InterruptedException e) {
            throw new InterruptedIOException("Interrupted while waiting for response");
        }
        if (cb.getResponse().getStatus() != ClientResponse.SUCCESS) {
            throw new ProcCallException(cb.getResponse());
        }
        return cb.getResponse();
    }

    /**
     * Implementation of asynchronous procedure call.
     *
     * @param callback completion callback
     * @param clientTimeoutNanos timeout on this query
     * @param invocation the procedure to call
     */
    private final boolean internalAsyncCallProcedure(ProcedureCallback callback,
                                                     long clientTimeoutNanos,
                                                     ProcedureInvocation invocation) throws IOException {
        assert(!m_isShutdown);
        assert(callback != null);

        final long nowNanos = System.nanoTime();

        // Blessed threads (the ones that invoke callbacks) are not subject to backpressure
        boolean isBlessed = m_blessedThreadIds.contains(Thread.currentThread().getId());

        while (!m_distributer.queue(invocation, callback, isBlessed, nowNanos, clientTimeoutNanos)) {

            // Wait on backpressure, honoring the timeout settings
            final long delta = Math.max(1, System.nanoTime() - nowNanos);
            final long timeout = clientTimeoutNanos == Distributer.USE_DEFAULT_CLIENT_TIMEOUT ?
                m_distributer.getProcedureTimeoutNanos() :
                clientTimeoutNanos;

            try {
                if (backpressureBarrier(nowNanos, timeout - delta)) {
                    ClientResponse response = new ClientResponseImpl(ClientResponse.CONNECTION_TIMEOUT,
                                                                     ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                                                                     "",
                                                                     new VoltTable[0],
                                                                     String.format("No response received in the allotted time (set to %d ms).",
                                                                                   TimeUnit.NANOSECONDS.toMillis(clientTimeoutNanos)));
                    try {
                        callback.clientCallback(response);
                    }
                    catch (Throwable thrown) {
                        m_distributer.uncaughtException(callback, response, thrown);
                    }
                }
            }
            catch (InterruptedException e) {
                throw new InterruptedIOException("Interrupted while invoking procedure asynchronously");
            }
        }

        return true;
    }

    /**
     * Serializes catalog and deployment file for UpdateApplicationCatalog.
     * Catalog is serialized into byte array, deployment file is serialized into
     * string.
     *
     * @param catalogPath
     * @param deploymentPath
     * @return Parameters that can be passed to UpdateApplicationCatalog
     */
    private Object[] getUpdateCatalogParams(File catalogPath, File deploymentPath)
    throws IOException {
        Object[] params = new Object[2];
        if (catalogPath != null) {
            params[0] = ClientUtils.fileToBytes(catalogPath);
        }
        else {
            params[0] = null;
        }
        if (deploymentPath != null) {
            params[1] = new String(ClientUtils.fileToBytes(deploymentPath), Constants.UTF8ENCODING);
        }
        else {
            params[1] = null;
        }
        return params;
    }

    /**
     * Update application catalog. Deprecated in client API but still
     * used elsewhere in VoltDB.
     */
    @Override
    public ClientResponse updateApplicationCatalog(File catalogPath, File deploymentPath)
    throws IOException, ProcCallException {
        Object[] params = getUpdateCatalogParams(catalogPath, deploymentPath);
        return callProcedure("@UpdateApplicationCatalog", params);
    }

    @Override
    public boolean updateApplicationCatalog(ProcedureCallback callback,
                                            File catalogPath,
                                            File deploymentPath) throws IOException {
        Object[] params = getUpdateCatalogParams(catalogPath, deploymentPath);
        return callProcedure(callback, "@UpdateApplicationCatalog", params);
    }

    /**
     * Update classes
     */
    @Override
    public ClientResponse updateClasses(File jarPath, String classesToDelete)
    throws IOException, ProcCallException {
        byte[] jarbytes = null;
        if (jarPath != null) {
            jarbytes = ClientUtils.fileToBytes(jarPath);
        }
        return callProcedure("@UpdateClasses", jarbytes, classesToDelete);
    }

    @Override
    public boolean updateClasses(ProcedureCallback callback,
                                 File jarPath,
                                 String classesToDelete) throws IOException {
        byte[] jarbytes = null;
        if (jarPath != null) {
            jarbytes = ClientUtils.fileToBytes(jarPath);
        }
        return callProcedure(callback, "@UpdateClasses", jarbytes, classesToDelete);
    }

    /**
     * Drain active transactions from client
     */
    @Override
    public void drain() throws InterruptedException {
        if (m_isShutdown) {
            return;
        }
        if (m_blessedThreadIds.contains(Thread.currentThread().getId())) {
            throw new RuntimeException("Can't invoke drain from within the client callback thread " +
                    " without deadlocking the client library");
        }
        m_distributer.drain();
    }

    /**
     * Shutdown the client closing all network connections and release
     * all memory resources.
     */
    @Override
    public void close() throws InterruptedException {
        if (m_blessedThreadIds.contains(Thread.currentThread().getId())) {
            throw new RuntimeException("Can't invoke close from within the client callback thread " +
                    " without deadlocking the client library");
        }
        m_isShutdown = true;
        setLocalBackpressureState(false);

        if (m_reconnectStatusListener != null) {
            m_distributer.removeClientStatusListener(m_reconnectStatusListener);
            m_reconnectStatusListener.close();
        }

        if (m_ex != null) {
            if (CoreUtils.isJunitTest()) {
                m_ex.shutdownNow();
            } else {
                m_ex.shutdown();
                m_ex.awaitTermination(365, TimeUnit.DAYS);
            }
        }
        m_distributer.shutdown();
        ClientFactory.decreaseClientNum();
    }

     /**
     * Block calling thread until there is no backpressure.
     */
    @Override
    public void backpressureBarrier() throws InterruptedException {
        backpressureBarrier(0, 0);
    }

    /**
     * Wait on backpressure with a timeout. Not part of public API, but exposed
     * for test code.
     *
     * @param start time request processing started (nanoseconds since epoch), 0 for no timeout
     * @param timeoutNanos initial timeout value, ignored if start is 0
     * @return true on timeout, false otherwise (i.e., reflects last known state of backpressure)
     */
    boolean backpressureBarrier(final long start, long timeoutNanos) throws InterruptedException {
        if (m_isShutdown) {
            return false;
        }
        if (m_blessedThreadIds.contains(Thread.currentThread().getId())) {
            throw new RuntimeException("Can't invoke backpressureBarrier from within the client callback thread " +
                    " without deadlocking the client library");
        }
        if (m_backpressure) {
            synchronized (m_backpressureLock) {
                if (m_backpressure) {
                    while (m_backpressure && !m_isShutdown) {
                       if (start != 0) {
                           if (timeoutNanos <= 0) {
                               // timeout nano value is negative or zero, indicating it timed out.
                               return true;
                           }

                            //Wait on the condition for the specified timeout remaining
                            m_backpressureLock.wait(timeoutNanos / TimeUnit.MILLISECONDS.toNanos(1), (int)(timeoutNanos % TimeUnit.MILLISECONDS.toNanos(1)));

                            //Condition is true, break and return false
                            if (!m_backpressure) {
                                break;
                            }

                            //Calculate whether the timeout should be triggered
                            final long nowNanos = System.nanoTime();
                            final long deltaNanos = Math.max(1, nowNanos - start);
                            if (deltaNanos >= timeoutNanos) {
                                return true;
                            }

                            //Reassigning timeout nanos with remainder of timeout
                            timeoutNanos -= deltaNanos;
                       } else {
                           m_backpressureLock.wait();
                       }
                    }
                }
            }
        }
        return false;
    }

    /*
     * Convenient class holding details of single host. The strange implementation
     * of setValue is needed because a HostConfig is set up by reading a VoltTable
     * in which each row has a parameter name and a value.
     */
    private class HostConfig {
        String m_ipAddress;
        String m_hostName;
        int m_clientPort;
        int m_adminPort;

        void setValue(String param, String value) {
            if ("IPADDRESS".equalsIgnoreCase(param)) {
                m_ipAddress = value;
            } else if ("HOSTNAME".equalsIgnoreCase(param)) {
                m_hostName = value;
            } else if ("CLIENTPORT".equalsIgnoreCase(param)) {
                m_clientPort = Integer.parseInt(value);
            } else if ("ADMINPORT".equalsIgnoreCase(param)) {
                m_adminPort = Integer.parseInt(value);
            }
        }

        int getPort(boolean isAdmin) {
            return isAdmin ? m_adminPort : m_clientPort;
        }
    }

    /*
     * Internal listener for client events. Handles loss of connection
     * and backpressure.
     *
     * Package access: used by Distributer.
     */
    class InternalClientStatusListener extends ClientStatusListenerExt {

        private boolean m_useAdminPort = false;
        private boolean m_adminPortChecked = false;
        private AtomicInteger connectionTaskCount = new AtomicInteger(0);

        @Override
        public void backpressure(boolean status) {
            setLocalBackpressureState(status);
        }

        @Override
        public void connectionCreated(String hostname, int port, AutoConnectionStatus status) {
            if (m_topologyChangeAware && m_autoReconnect && status == AutoConnectionStatus.SUCCESS) {
                // Track potential targets for reconnection. A new epoch begins
                // on the first successful connection after having no connections;
                // previous targets are then forgotten.
                synchronized (m_connectHistory) {
                    if (m_newConnectEpoch) {
                        m_newConnectEpoch = false;
                        m_connectHistory.clear();
                    }
                    m_connectHistory.add(HostAndPort.fromParts(hostname, port));
                }
            }
        }

        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft,
                                   DisconnectCause cause) {
            if (connectionsLeft == 0) {
                if (m_topologyChangeAware && m_autoReconnect && !m_isShutdown) {
                    // Special-case handling of no connections in topo-change aware mode.
                    // Must make a connection first of all.
                    createAnyConnection();
                } else {
                    // Wake up client and let it attempt to queue work
                    // and then fail with a NoConnectionsException
                    setLocalBackpressureState(false);
                }
            }
        }

        /**
         * get a list of hosts which client does not have a connection to
         * @param vt Results from @SystemInformation
         * @return a list of hosts which client does not have a connection to
         */
        Map<Integer, HostConfig> buildUnconnectedHostConfigMap(VoltTable vt) {
            Map<Integer, HostConfig> unconnectedMap = new HashMap<Integer, HostConfig>();
            Map<Integer, HostConfig> connectedMap = new HashMap<Integer, HostConfig>();
            while (vt.advanceRow()) {
                Integer hid = (int)vt.getLong("HOST_ID");
                HostConfig config = null;
                if (!m_distributer.isHostConnected(hid)) {
                    config = unconnectedMap.get(hid);
                    if (config == null) {
                        config = new HostConfig();
                        unconnectedMap.put(hid, config);
                    }
                } else if (!m_adminPortChecked) {
                    config = connectedMap.get(hid);
                    if (config == null) {
                        config = new HostConfig();
                        connectedMap.put(hid, config);
                    }
                }
                if (config != null) {
                    config.setValue(vt.getString("KEY"), vt.getString("VALUE"));
                }
            }

            //if all existing connections use admin port, use admin port for connections to the newly discovered nodes
            if (!m_adminPortChecked) {
                Map<String, Integer> connectedIpPortPairs = m_distributer.getConnectedHostIPAndPort();
                int admintPortCount = 0;
                for (HostConfig config : connectedMap.values()){
                    Integer connectedPort = connectedIpPortPairs.get(config.m_ipAddress);
                    if (connectedPort != null && config.m_adminPort == connectedPort) {
                        admintPortCount++;
                    }
                }
                m_useAdminPort = (admintPortCount == connectedMap.values().size());
            }
            m_adminPortChecked = true;
            return unconnectedMap;
        }

        /**
         * Notify client upon a connection creation failure.
         *
         * @param host HostConfig with IP address and port
         * @param status The status of connection creation
         */
        void notifyClientConnectionCreation(HostConfig host, AutoConnectionStatus status) {
            if (host != null) {
                notifyClientConnectionCreation(host.m_hostName, host.m_clientPort, status);
            } else {
                notifyClientConnectionCreation("", -1, status);
            }
        }

        void notifyClientConnectionCreation(String host, int port, AutoConnectionStatus status) {
            if (m_clientStatusListener != null) {
                m_clientStatusListener.connectionCreated(host, port, status);
            }
        }

        /**
         * Handles completion of automatic reconnection attempt.
         * Success: kick distributer into querying updated toplogy.
         * Failure: schedule a retry.
         * @param failCount - non-zero if retry needed
         * @param first - determines which task to requeue
         */
        void retryConnectionCreationIfNeeded(int failCount, boolean first) {
            if (failCount == 0) {
                try {
                    m_distributer.setCreateConnectionsUponTopologyChangeComplete();
                } catch (Exception e) {
                    notifyClientConnectionCreation(null, AutoConnectionStatus.UNABLE_TO_CONNECT);
                }
            } else if (first) {
                m_ex.schedule(new FirstConnectionTask(this, connectionTaskCount), CONNECTION_RETRY_DELAY_SEC, TimeUnit.SECONDS);
            } else if (connectionTaskCount.get() < 2) { // TODO: why 2? current task has decremented count, so count is only count of queued
                // if there are tasks in the queue, do not need schedule again since all the tasks do the same job
                m_ex.schedule(new CreateConnectionTask(this, connectionTaskCount), CONNECTION_RETRY_DELAY_SEC, TimeUnit.SECONDS);
            }
        }

        /**
         * Find all the host which have not been connected to the client via @SystemInformation
         * and make connections (called from Distributer)
         */
        public void createConnectionsUponTopologyChange() {
            m_ex.execute(new CreateConnectionTask(this, connectionTaskCount));
        }

        /**
         * We have no connections; make the first one
         */
        public void createAnyConnection() {
            m_ex.execute(new FirstConnectionTask(this, connectionTaskCount));
        }
    }

    /*
     * Asynchronously create connection, used for topology-aware clients.
     * Runs in thread managed by ScheduledServiceExecutor.
     */
    private class CreateConnectionTask implements Runnable {
        final InternalClientStatusListener listener;
        final AtomicInteger connectionTaskCount;

        CreateConnectionTask(InternalClientStatusListener listener, AtomicInteger connectionTaskCount) {
            this.listener = listener;
            this.connectionTaskCount = connectionTaskCount;
            connectionTaskCount.incrementAndGet();
        }

        @Override
        public void run() {
            int failCount = 0;
            try {
                ClientResponse resp = callProcedure("@SystemInformation", "OVERVIEW");
                if (resp.getStatus() == ClientResponse.SUCCESS) {
                    Map<Integer, HostConfig> hosts = listener.buildUnconnectedHostConfigMap(resp.getResults()[0]);
                    for(Map.Entry<Integer, HostConfig> entry : hosts.entrySet()) {
                        HostConfig config = entry.getValue();
                        try {
                            createConnectionImpl(config.m_ipAddress,config.getPort(listener.m_useAdminPort));
                            listener.notifyClientConnectionCreation(config, AutoConnectionStatus.SUCCESS);
                        } catch (Exception e) {
                            listener.notifyClientConnectionCreation(config, AutoConnectionStatus.UNABLE_TO_CONNECT);
                            failCount++;
                        }
                    }
                } else {
                    listener.notifyClientConnectionCreation(null, AutoConnectionStatus.UNABLE_TO_QUERY_TOPOLOGY);
                    failCount++;
                }
            } catch (Exception e) {
                listener.notifyClientConnectionCreation(null, AutoConnectionStatus.UNABLE_TO_QUERY_TOPOLOGY);
                failCount++;
            } finally {
                connectionTaskCount.decrementAndGet();
                listener.retryConnectionCreationIfNeeded(failCount, false);
            }
        }
    }

    /*
     * Asynchronously create connection, used for topology-aware clients in the
     * specific case that we have zero connections left (and therefore cannot now
     * know the topology). Uses the connect history to get a connection to any
     * one of the previously-connected hosts. From there, normal topo handling
     * can take over again. Runs in thread managed by ScheduledServiceExecutor.
     */
    private class FirstConnectionTask implements Runnable {
        final InternalClientStatusListener listener;
        final AtomicInteger connectionTaskCount;
        final LinkedHashSet<HostAndPort> targets;

        FirstConnectionTask(InternalClientStatusListener listener, AtomicInteger connectionTaskCount ) {
            this.listener = listener;
            this.connectionTaskCount = connectionTaskCount;
            synchronized (m_connectHistory) {
                targets = new LinkedHashSet<>(m_connectHistory);
                m_newConnectEpoch = true; // discard old history in successful completion
            }
            connectionTaskCount.incrementAndGet();
        }

        @Override
        public void run() {
            int failCount = 0;
            try {
                for (HostAndPort hap : targets) {
                    try {
                        createConnectionImpl(hap.getHost(), hap.getPort());
                        listener.notifyClientConnectionCreation(hap.getHost(), hap.getPort(), AutoConnectionStatus.SUCCESS);
                        setLocalBackpressureState(false);
                        break; // one is enough
                    } catch (Exception e) {
                        // Does client need to know about this?
                        failCount++;
                    }
                }
                if (targets.isEmpty()) { // should not happen, but there's no way out of this
                    listener.notifyClientConnectionCreation(null, AutoConnectionStatus.NO_KNOWN_SERVERS);
                }
            }
            finally {
                connectionTaskCount.decrementAndGet();
                listener.retryConnectionCreationIfNeeded(failCount, true);
            }
        }
    }

    /*
     * Updates local backpressure state, controlling the
     * barrier/retry loop in internalAsyncCallProcedure
     */
    private void setLocalBackpressureState(boolean onoff) {
        synchronized (m_backpressureLock) {
            m_backpressure = onoff;
            if (!onoff) {
                m_backpressureLock.notifyAll();
            }
        }
    }

    /****************************************************
                        Implementation
     ****************************************************/

    @Override
    public void configureBlocking(boolean blocking) {
        m_blockingQueue = blocking;
    }

    @Override
    public ClientStatsContext createStatsContext() {
        return m_distributer.createStatsContext();
    }

    @Override
    public Object[] getInstanceId() {
        return m_distributer.getInstanceId();
    }

    @Override
    public String getBuildString() {
        return m_distributer.getBuildString();
    }

    @Override
    public boolean blocking() {
        return m_blockingQueue;
    }

    // Used by system tests
    public void resetInstanceId() {
        m_distributer.resetInstanceId();
    }

    // Package access for unit test
    static HostAndPort parseHostAndPort(String server) {
        return HostAndPort.fromString(server.trim()).withDefaultPort(Client.VOLTDB_SERVER_PORT).requireBracketsForIPv6();
    }

    @Override
    public void createConnection(String host) throws IOException {
        HostAndPort hp = parseHostAndPort(host);
        createConnectionImpl(hp.getHost(), hp.getPort());
    }

    @Override
    public void createConnection(String host, int port) throws IOException {
        createConnectionImpl(host, port);
    }

    private void createConnectionImpl(String host, int port) throws IOException {
        if (m_username == null) {
            throw new IllegalStateException("Attempted to use createConnection() with a client" +
                                            " that wasn't constructed with a username and password specified");
        }
        if (m_isShutdown) {
            throw new IOException("Client instance is shutdown");
        }
        m_distributer.createConnectionWithHashedCredentials(host, m_username, m_passwordHash, port, m_hashScheme);
    }

    @Override
    public List<InetSocketAddress> getConnectedHostList() {
        return m_distributer.getConnectedHostList();
    }

    @Override
    public int[] getThroughputAndOutstandingTxnLimits() {
        return m_distributer.m_rateLimiter.getLimits();
    }

    @Override
    public void writeSummaryCSV(ClientStats stats, String path) throws IOException {
        writeSummaryCSV(null, stats, path);
    }

    @Override
    public void writeSummaryCSV(String statsRowName, ClientStats stats, String path) throws IOException {
        // don't do anything (be silent) if empty path
        if (path == null || path.length() == 0) {
            return;
        }

        FileWriter fw = new FileWriter(path, true);
        if (statsRowName != null && ! statsRowName.isEmpty()) {
            fw.append(statsRowName).append(",");
        }
        fw.append(String.format("%d,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%d,%d\n",
                stats.getStartTimestamp(),
                stats.getDuration(),
                stats.getInvocationsCompleted(),
                stats.kPercentileLatencyAsDouble(0.0),
                stats.kPercentileLatencyAsDouble(1.0),
                stats.kPercentileLatencyAsDouble(0.95),
                stats.kPercentileLatencyAsDouble(0.99),
                stats.kPercentileLatencyAsDouble(0.999),
                stats.kPercentileLatencyAsDouble(0.9999),
                stats.kPercentileLatencyAsDouble(0.99999),
                stats.getInvocationErrors(),
                stats.getInvocationAborts(),
                stats.getInvocationTimeouts()));
        fw.close();
    }

    // Hidden method to check if Hashinator is initialized.
    public boolean isHashinatorInitialized() {
        return m_distributer.isHashinatorInitialized();
    }

    // Hidden method for getPartitionForParameter
    public long getPartitionForParameter(byte typeValue, Object value) {
        return m_distributer.getPartitionForParameter(typeValue, value);
    }

    // Hidden method for getPartitionForParameter
    public long getPartitionForParameter(byte[] bytes) {
        return m_distributer.getPartitionForParameter(bytes);
    }

    @Override
    public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, boolean upsertMode, BulkLoaderFailureCallBack failureCallback) throws Exception
    {
        synchronized(m_vblGlobals) {
            return new VoltBulkLoader(m_vblGlobals, tableName, maxBatchSize, upsertMode, failureCallback, null);
        }
    }

    @Override
    public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, BulkLoaderFailureCallBack failureCallback) throws Exception
    {
        synchronized(m_vblGlobals) {
            return new VoltBulkLoader(m_vblGlobals, tableName, maxBatchSize, failureCallback);
        }
    }

    @Override
    public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, boolean upsertMode, BulkLoaderFailureCallBack failureCallback, BulkLoaderSuccessCallback successCallback) throws Exception {
        synchronized(m_vblGlobals) {
            return new VoltBulkLoader(m_vblGlobals, tableName, maxBatchSize, upsertMode, failureCallback, successCallback);
        }
    }

    @Override
    public boolean isAutoReconnectEnabled() {
        return m_autoReconnect;
    }

    @Override
    public ClientResponseWithPartitionKey[] callAllPartitionProcedure(String procedureName, Object... params)
            throws IOException, ProcCallException {
        CountDownLatch latch = new CountDownLatch(1);
        SyncAllPartitionProcedureCallback callBack = new SyncAllPartitionProcedureCallback(latch);
        callAllPartitionProcedure(callBack, procedureName, params);
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new java.io.InterruptedIOException("Interrupted while waiting for response");
        }
        return callBack.getResponse();
    }

    @Override
    public boolean callAllPartitionProcedure(AllPartitionProcedureCallback callback, String procedureName, Object... params)
            throws IOException, ProcCallException {
        if (callback == null) {
            throw new IllegalArgumentException("AllPartitionProcedureCallback can not be null");
        }

        Object[] args = new Object[params.length + 1];
        System.arraycopy(params, 0, args, 1, params.length);

        final Map<Integer, Integer> partitionMap = m_distributer.getPartitionKeys();
        int partitionCount = partitionMap.size();
        AtomicInteger counter = new AtomicInteger(partitionCount);
        assert(partitionCount > 0);
        ClientResponseWithPartitionKey[] responses = new ClientResponseWithPartitionKey[partitionCount];
        for (Map.Entry<Integer, Integer> entry : partitionMap.entrySet()) {
            args[0] = entry.getValue();
            partitionCount--;
            OnePartitionProcedureCallback cb = new OnePartitionProcedureCallback(counter, args[0], partitionCount,
                    responses, callback);
            try {
                // Call the more complex method to ensure that the allPartition flag for the invocation is
                // set to true. This gives a nice error message if the target procedure is incompatible.
                if (!callProcedureWithClientTimeout(cb, BatchTimeoutOverrideType.NO_TIMEOUT, entry.getKey(),
                        procedureName, Distributer.USE_DEFAULT_CLIENT_TIMEOUT, TimeUnit.NANOSECONDS, args))
                {
                    final ClientResponse r = new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE, new VoltTable[0],
                            "The procedure is not queued for execution.");
                    throw new ProcCallException(r);
                }
            } catch(Exception ex) {
                try {
                    cb.exceptionCallback(ex);
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        }
        return true;
    }

    /**
     * Essentially the same code as SyncCallback, but without the overhead (memory, gc)
     * of storing the parameters of every outstanding request while waiting for a response.
     *
     */
    private final class SyncCallbackLight implements ProcedureCallback {
        private final Semaphore m_lock;
        private ClientResponse m_response;

        /**
         * Create a SyncCallbackLight instance.
         */
        public SyncCallbackLight() {
            m_response = null;
            m_lock = new Semaphore(1);
            m_lock.acquireUninterruptibly();
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            m_response = clientResponse;
            m_lock.release();
        }

        /**
         * <p>Retrieve the ClientResponse returned for this procedure invocation.</p>
         *
         * @return ClientResponse for this invocation
         */
        public ClientResponse getResponse() {
            return m_response;
        }

        /**
         * <p>Block until a response has been received for the invocation associated with this callback. Call getResponse
         * to retrieve the response or result() to retrieve the just the results.</p>
         *
         * @throws InterruptedException on interruption.
         */
        public void waitForResponse() throws InterruptedException {
            m_lock.acquire();
            m_lock.release();
        }
    }

    /**
     * Procedure call back for async callAllPartitionProcedure
     */
    private class OnePartitionProcedureCallback implements ProcedureCallback {

        final ClientResponseWithPartitionKey[] m_responses;
        final int m_index;
        final Object m_partitionKey;
        final AtomicInteger m_partitionCounter;
        final AllPartitionProcedureCallback m_cb;

        /**
         * Callback initialization
         * @param partitionKey  The partition where the call back works on
         * @param index  The index for PartitionClientResponse
         * @param responses The final result array
         */
        OnePartitionProcedureCallback(AtomicInteger counter, Object partitionKey, int index,
                  ClientResponseWithPartitionKey[] responses, AllPartitionProcedureCallback cb) {
            m_partitionCounter = counter;
            m_partitionKey = partitionKey;
            m_index = index;
            m_responses = responses;
            m_cb = cb;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            m_responses[m_index] = new ClientResponseWithPartitionKey(m_partitionKey, response);
            if (m_partitionCounter.decrementAndGet() == 0) {
                m_cb.clientCallback(m_responses);
            }
        }

        void exceptionCallback(Exception e) throws Exception {
            if ( e instanceof ProcCallException) {
                ProcCallException pe = (ProcCallException)e;
                m_responses[m_index] = new ClientResponseWithPartitionKey(m_partitionKey, pe.getClientResponse());
            } else {
                byte status = ClientResponse.GRACEFUL_FAILURE;
                if(e instanceof NoConnectionsException){
                    status = ClientResponse.CONNECTION_LOST;
                }
                final ClientResponse r = new ClientResponseImpl(status, new VoltTable[0], e.getMessage());
                m_responses[m_index] = new ClientResponseWithPartitionKey(m_partitionKey, r);
            }
            if (m_partitionCounter.decrementAndGet() == 0) {
                m_cb.clientCallback(m_responses);
            }
        }
    }

    /**
     * Sync all partition procedure call back
     */
    private class SyncAllPartitionProcedureCallback implements AllPartitionProcedureCallback {

        ClientResponseWithPartitionKey[] m_responses;
        final CountDownLatch m_latch;

        SyncAllPartitionProcedureCallback(CountDownLatch latch)  {
            m_latch = latch;
        }

        @Override
        public void clientCallback(ClientResponseWithPartitionKey[] clientResponse) throws Exception {
             m_responses = clientResponse;
             m_latch.countDown();
        }

        ClientResponseWithPartitionKey[] getResponse() {
            return m_responses;
        }
    }
}
