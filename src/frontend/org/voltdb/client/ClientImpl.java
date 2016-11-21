/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import org.voltcore.utils.CoreUtils;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.HashinatorLite.HashinatorLiteType;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.BulkLoaderState;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;
import org.voltdb.common.Constants;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.collect.ImmutableSet;

/**
 *  A client that connects to one or more nodes in a VoltCluster
 *  and provides methods to call stored procedures and receive
 *  responses.
 */
public final class ClientImpl implements Client {

    /*
     * refresh the partition key cache every 1 second
     */
    static long PARTITION_KEYS_INFO_REFRESH_FREQUENCY = 1000;

    // call initiated by the user use positive handles
    private final AtomicLong m_handle = new AtomicLong(0);

    /*
     * Username and password as set by createConnection. Used
     * to ensure that the same credentials are used every time
     * with that inconsistent API.
     */
    // stored credentials
    private boolean m_credentialsSet = false;
    private final ReentrantLock m_credentialComparisonLock =
            new ReentrantLock();
    private String m_createConnectionUsername = null;
    private byte[] m_hashedPassword = null;
    private int m_passwordHashCode = 0;
    final InternalClientStatusListener m_listener = new InternalClientStatusListener();
    ClientStatusListenerExt m_clientStatusListener = null;

    private ScheduledExecutorService m_ex = null;
    /*
     * Username and password as set by the constructor.
     */
    private final String m_username;
    private final byte m_passwordHash[];
    private final ClientAuthScheme m_hashScheme;


    /**
     * These threads belong to the network thread pool
     * that invokes callbacks. These threads are "blessed"
     * and should never experience backpressure. This ensures that the
     * network thread pool doesn't block when queuing procedures from
     * a callback.
     */
    private final CopyOnWriteArrayList<Long> m_blessedThreadIds = new CopyOnWriteArrayList<>();

    private BulkLoaderState m_vblGlobals = new BulkLoaderState(this);

    // global instance of null callback for performance (you only need one)
    private static final ProcedureCallback NULL_CALLBACK = new NullCallback();

    /****************************************************
                        Public API
     ****************************************************/

    private volatile boolean m_isShutdown = false;

    /**
     * Create a new client without any initial connections.
     * Also provide a hint indicating the expected serialized size of
     * most outgoing procedure invocations. This helps size initial allocations
     * for serializing network writes
     * @param expectedOutgoingMessageSize Expected size of procedure invocations in bytes
     * @param maxArenaSizes Maximum size arenas in the memory pool should grow to
     * @param heavyweight Whether to use multiple or a single thread
     */
    ClientImpl(ClientConfig config) {

        if (config.m_topologyChangeAware && !config.m_useClientAffinity) {
            throw new IllegalArgumentException("The client affinity must be enabled to enable topology awareness.");
        }

        m_distributer = new Distributer(
                config.m_heavyweight,
                config.m_procedureCallTimeoutNanos,
                config.m_connectionResponseTimeoutMS,
                config.m_useClientAffinity,
                config.m_sendReadsToReplicasBytDefaultIfCAEnabled,
                config.m_subject);
        m_distributer.addClientStatusListener(m_listener);
        String username = config.m_username;
        if (config.m_subject != null) {
            username = ClientConfig.getUserNameFromSubject(config.m_subject);
        }
        m_username = username;
        m_distributer.setTopologyChangeAware(config.m_topologyChangeAware);
        if (config.m_topologyChangeAware) {
            m_ex = Executors.newSingleThreadScheduledExecutor(CoreUtils.getThreadFactory("Topoaware thread"));
        }

        if (config.m_reconnectOnConnectionLoss) {
            m_reconnectStatusListener = new ReconnectStatusListener(this,
                    config.m_initialConnectionRetryIntervalMS, config.m_maxConnectionRetryIntervalMS);
            m_distributer.addClientStatusListener(m_reconnectStatusListener);
        } else {
            m_reconnectStatusListener = null;
        }

        m_hashScheme = config.m_hashScheme;
        if (config.m_cleartext) {
            m_passwordHash = ConnectionUtil.getHashedPassword(m_hashScheme, config.m_password);
        } else {
            m_passwordHash = Encoder.hexDecode(config.m_password);
        }
        if (config.m_listener != null) {
            m_distributer.addClientStatusListener(config.m_listener);
            m_clientStatusListener = config.m_listener;
        }

        assert(config.m_maxOutstandingTxns > 0);
        m_blessedThreadIds.addAll(m_distributer.getThreadIds());
        if (config.m_autoTune) {
            m_distributer.m_rateLimiter.enableAutoTuning(
                    config.m_autoTuneTargetInternalLatency);
        }
        else {
            m_distributer.m_rateLimiter.setLimits(
                    config.m_maxTransactionsPerSecond, config.m_maxOutstandingTxns);
        }
    }

    private boolean verifyCredentialsAreAlwaysTheSame(String username, byte[] hashedPassword) {
        assert(username != null);
        m_credentialComparisonLock.lock();
        try {
            if (m_credentialsSet == false) {
                m_credentialsSet = true;
                m_createConnectionUsername = username;
                if (hashedPassword != null) {
                    m_hashedPassword = Arrays.copyOf(hashedPassword, hashedPassword.length);
                    m_passwordHashCode = Arrays.hashCode(hashedPassword);
                }
                return true;
            }
            else {
                if (!m_createConnectionUsername.equals(username)) return false;
                if (hashedPassword == null)
                    return m_hashedPassword == null;
                else
                    for (int i = 0; i < hashedPassword.length; i++)
                        if (hashedPassword[i] != m_hashedPassword[i])
                            return false;
                return true;
            }
        } finally {
            m_credentialComparisonLock.unlock();
        }
    }

    public String getUsername() {
        return m_createConnectionUsername;
    }

    public int getPasswordHashCode() {
        return m_passwordHashCode;
    }

    public void createConnectionWithHashedCredentials(
            String host,
            int port,
            String program,
            byte[] hashedPassword)
                    throws IOException
    {
        if (m_isShutdown) {
            throw new IOException("Client instance is shutdown");
        }
        final String subProgram = (program == null) ? "" : program;
        final byte[] subPassword = (hashedPassword == null) ? ConnectionUtil.getHashedPassword(m_hashScheme, "") : hashedPassword;

        if (!verifyCredentialsAreAlwaysTheSame(subProgram, subPassword)) {
            throw new IOException("New connection authorization credentials do not match previous credentials for client.");
        }

        m_distributer.createConnectionWithHashedCredentials(host, subProgram, subPassword, port, m_hashScheme);
    }

    /**
     * Synchronously invoke a procedure call blocking until a result is available.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return array of VoltTable results.
     * @throws org.voltdb.client.ProcCallException
     * @throws NoConnectionsException
     */
    @Override
    public final ClientResponse callProcedure(
            String procName,
            Object... parameters)
                    throws IOException, NoConnectionsException, ProcCallException
    {
        return callProcedureWithClientTimeout(BatchTimeoutOverrideType.NO_TIMEOUT, procName,
                Distributer.USE_DEFAULT_CLIENT_TIMEOUT, TimeUnit.SECONDS, parameters);
    }

    /**
     * Synchronously invoke a procedure call blocking until a result is available.
     * @param batchTimeout procedure invocation batch timeout.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return array of VoltTable results.
     * @throws org.voltdb.client.ProcCallException
     * @throws NoConnectionsException
     */
    @Override
    public ClientResponse callProcedureWithTimeout(
            int batchTimeout,
            String procName,
            Object... parameters)
                    throws IOException, NoConnectionsException, ProcCallException
    {
        return callProcedureWithClientTimeout(batchTimeout, procName,
                Distributer.USE_DEFAULT_CLIENT_TIMEOUT, TimeUnit.SECONDS, parameters);
    }

    /**
     * Same as the namesake without allPartition option.
     */
    public ClientResponse callProcedureWithClientTimeout(
            int batchTimeout,
            String procName,
            long clientTimeout,
            TimeUnit unit,
            Object... parameters)
                    throws IOException, NoConnectionsException, ProcCallException
    {
        return callProcedureWithClientTimeout(batchTimeout, false, procName, clientTimeout, unit, parameters);
    }

    /**
     * Synchronously invoke a procedure call blocking until a result is available.
     *
     * @param batchTimeout procedure invocation batch timeout.
     * @param allPartition whether this is an all-partition invocation
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param clientTimeout timeout for the procedure
     * @param unit TimeUnit of procedure timeout
     * @param parameters vararg list of procedure's parameter values.
     * @return ClientResponse for execution.
     * @throws org.voltdb.client.ProcCallException
     * @throws NoConnectionsException
     */
    public ClientResponse callProcedureWithClientTimeout(
            int batchTimeout,
            boolean allPartition,
            String procName,
            long clientTimeout,
            TimeUnit unit,
            Object... parameters)
                    throws IOException, NoConnectionsException, ProcCallException
    {
        ProcedureInvocation invocation
            = new ProcedureInvocation(m_handle.getAndIncrement(), batchTimeout, allPartition, procName, parameters);
        return internalSyncCallProcedure(unit.toNanos(clientTimeout), invocation);
    }

    /**
     * Asynchronously invoke a procedure call.
     * @param callback TransactionCallback that will be invoked with procedure results.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return True if the procedure was queued and false otherwise
     */
    @Override
    public final boolean callProcedure(
            ProcedureCallback callback,
            String procName,
            Object... parameters)
                    throws IOException, NoConnectionsException
    {
        //Time unit doesn't matter in this case since the timeout isn't being specified
        return callProcedureWithClientTimeout(callback, BatchTimeoutOverrideType.NO_TIMEOUT, procName,
                Distributer.USE_DEFAULT_CLIENT_TIMEOUT, TimeUnit.NANOSECONDS, parameters);
    }

    /**
     * Asynchronously invoke a procedure call with timeout.
     * @param callback TransactionCallback that will be invoked with procedure results.
     * @param batchTimeout procedure invocation batch timeout.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return True if the procedure was queued and false otherwise
     */
    @Override
    public final boolean callProcedureWithTimeout(
            ProcedureCallback callback,
            int batchTimeout,
            String procName,
            Object... parameters)
                    throws IOException, NoConnectionsException
    {
        //Time unit doesn't matter in this case since the timeout isn't being specifie
        return callProcedureWithClientTimeout(
                callback,
                batchTimeout,
                false,
                procName,
                Distributer.USE_DEFAULT_CLIENT_TIMEOUT,
                TimeUnit.NANOSECONDS,
                parameters);
    }

    /**
     * Same as the namesake without allPartition option.
     */
    public boolean callProcedureWithClientTimeout(
            ProcedureCallback callback,
            int batchTimeout,
            String procName,
            long clientTimeout,
            TimeUnit clientTimeoutUnit,
            Object... parameters)
                    throws IOException, NoConnectionsException
    {
        return callProcedureWithClientTimeout(
                callback, batchTimeout, false, procName, clientTimeout, clientTimeoutUnit, parameters);
    }

    /**
     * Asynchronously invoke a procedure call.
     *
     * @param callback TransactionCallback that will be invoked with procedure results.
     * @param batchTimeout procedure invocation batch timeout.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param timeout timeout for the procedure
     * @param allPartition whether this is an all-partition invocation
     * @param unit TimeUnit of procedure timeout
     * @param parameters vararg list of procedure's parameter values.
     * @return True if the procedure was queued and false otherwise
     */
    public boolean callProcedureWithClientTimeout(
            ProcedureCallback callback,
            int batchTimeout,
            boolean allPartition,
            String procName,
            long clientTimeout,
            TimeUnit clientTimeoutUnit,
            Object... parameters)
                    throws IOException, NoConnectionsException
    {
        if (callback instanceof ProcedureArgumentCacher) {
            ((ProcedureArgumentCacher) callback).setArgs(parameters);
        }

        ProcedureInvocation invocation
                = new ProcedureInvocation(m_handle.getAndIncrement(), batchTimeout, allPartition, procName, parameters);

        return internalAsyncCallProcedure(callback, clientTimeoutUnit.toNanos(clientTimeout), invocation);
    }

    @Deprecated
    @Override
    public int calculateInvocationSerializedSize(
            String procName,
            Object... parameters)
    {
        final ProcedureInvocation invocation =
            new ProcedureInvocation(0, procName, parameters);
        return invocation.getSerializedSize();
    }

    @Deprecated
    @Override
    public final boolean callProcedure(
           ProcedureCallback callback,
           int expectedSerializedSize,
           String procName,
           Object... parameters)
                   throws NoConnectionsException, IOException
    {
        return callProcedure(callback, procName, parameters);
    }

    private final ClientResponse internalSyncCallProcedure(
            long clientTimeoutNanos,
            ProcedureInvocation invocation) throws ProcCallException, IOException {

        if (m_isShutdown) {
            throw new NoConnectionsException("Client instance is shutdown");
        }

        if (m_blessedThreadIds.contains(Thread.currentThread().getId())) {
            throw new IOException("Can't invoke a procedure synchronously from with the client callback thread " +
                    " without deadlocking the client library");
        }

        SyncCallbackLight cb = new SyncCallbackLight();

        boolean success = internalAsyncCallProcedure(cb, clientTimeoutNanos, invocation);
        if (!success) {
            final ClientResponseImpl r = new ClientResponseImpl(
                    ClientResponse.GRACEFUL_FAILURE,
                    ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                    "",
                    new VoltTable[0],
                    String.format("Unable to queue client request."));
            throw new ProcCallException(r, "Unable to queue client request.", null);
        }

        try {
            cb.waitForResponse();
        } catch (final InterruptedException e) {
            throw new java.io.InterruptedIOException("Interrupted while waiting for response");
        }
        if (cb.getResponse().getStatus() != ClientResponse.SUCCESS) {
            throw new ProcCallException(cb.getResponse(), cb.getResponse().getStatusString(), null);
        }
        return cb.getResponse();
    }

    private final boolean internalAsyncCallProcedure(
            ProcedureCallback callback,
            long clientTimeoutNanos,
            ProcedureInvocation invocation)
            throws IOException, NoConnectionsException {

        if (m_isShutdown) {
            return false;
        }

        if (callback == null) {
            callback = NULL_CALLBACK;
        }

        final long nowNanos = System.nanoTime();
        //Blessed threads (the ones that invoke callbacks) are not subject to backpressure
        boolean isBlessed = m_blessedThreadIds.contains(Thread.currentThread().getId());
        if (m_blockingQueue) {
            while (!m_distributer.queue(
                    invocation,
                    callback,
                    isBlessed, nowNanos, clientTimeoutNanos)) {

                /*
                 * Wait on backpressure honoring the timeout settings
                 */
                final long delta = Math.max(1, System.nanoTime() - nowNanos);
                final long timeout =
                        clientTimeoutNanos == Distributer.USE_DEFAULT_CLIENT_TIMEOUT ?
                                m_distributer.getProcedureTimeoutNanos() : clientTimeoutNanos;
                try {
                    if (backpressureBarrier(nowNanos, timeout - delta)) {
                        final ClientResponseImpl r = new ClientResponseImpl(
                                ClientResponse.CONNECTION_TIMEOUT,
                                ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                                "",
                                new VoltTable[0],
                                String.format("No response received in the allotted time (set to %d ms).",
                                        TimeUnit.NANOSECONDS.toMillis(clientTimeoutNanos)));
                        try {
                            callback.clientCallback(r);
                        } catch (Throwable t) {
                            m_distributer.uncaughtException(callback, r, t);
                        }
                    }
                } catch (InterruptedException e) {
                    throw new java.io.InterruptedIOException("Interrupted while invoking procedure asynchronously");
                }
            }
            return true;
        } else {
            return m_distributer.queue(
                    invocation,
                    callback,
                    isBlessed, nowNanos, clientTimeoutNanos);
        }
    }

    /**
     * Serializes catalog and deployment file for UpdateApplicationCatalog.
     * Catalog is serialized into byte array, deployment file is serialized into
     * string.
     *
     * @param catalogPath
     * @param deploymentPath
     * @return Parameters that can be passed to UpdateApplicationCatalog
     * @throws IOException If either of the files cannot be read
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

    @Override
    public ClientResponse updateApplicationCatalog(File catalogPath, File deploymentPath)
    throws IOException, NoConnectionsException, ProcCallException {
        Object[] params = getUpdateCatalogParams(catalogPath, deploymentPath);
        return callProcedure("@UpdateApplicationCatalog", params);
    }

    @Override
    public boolean updateApplicationCatalog(ProcedureCallback callback,
                                            File catalogPath,
                                            File deploymentPath)
    throws IOException, NoConnectionsException {
        Object[] params = getUpdateCatalogParams(catalogPath, deploymentPath);
        return callProcedure(callback, "@UpdateApplicationCatalog", params);
    }

    @Override
    public ClientResponse updateClasses(File jarPath, String classesToDelete)
    throws IOException, NoConnectionsException, ProcCallException
    {
        byte[] jarbytes = null;
        if (jarPath != null) {
            jarbytes = ClientUtils.fileToBytes(jarPath);
        }
        return callProcedure("@UpdateClasses", jarbytes, classesToDelete);
    }

    @Override
    public boolean updateClasses(ProcedureCallback callback,
                                 File jarPath,
                                 String classesToDelete)
    throws IOException, NoConnectionsException
    {
        byte[] jarbytes = null;
        if (jarPath != null) {
            jarbytes = ClientUtils.fileToBytes(jarPath);
        }
        return callProcedure(callback, "@UpdateClasses", jarbytes, classesToDelete);
    }

    @Override
    public void drain() throws InterruptedException {
        if (m_isShutdown) {
            return;
        }
        if (m_blessedThreadIds.contains(Thread.currentThread().getId())) {
            throw new RuntimeException("Can't invoke backpressureBarrier from within the client callback thread " +
                    " without deadlocking the client library");
        }
        m_distributer.drain();
    }

    /**
     * Shutdown the client closing all network connections and release
     * all memory resources.
     * @throws InterruptedException
     */
    @Override
    public void close() throws InterruptedException {
        if (m_blessedThreadIds.contains(Thread.currentThread().getId())) {
            throw new RuntimeException("Can't invoke backpressureBarrier from within the client callback thread " +
                    " without deadlocking the client library");
        }
        m_isShutdown = true;
        synchronized (m_backpressureLock) {
            m_backpressureLock.notifyAll();
        }

        if (m_reconnectStatusListener != null) {
            m_distributer.removeClientStatusListener(m_reconnectStatusListener);
            m_reconnectStatusListener.close();
        }

        if (m_ex != null) {
            m_ex.shutdown();
            m_ex.awaitTermination(365, TimeUnit.DAYS);
        }
        m_distributer.shutdown();
        ClientFactory.decreaseClientNum();
    }

    @Override
    public void backpressureBarrier() throws InterruptedException {
        backpressureBarrier( 0, 0);
    }

    /**
     * Wait on backpressure with a timeout. Returns true on timeout, false otherwise.
     * Timeout nanos is the initial timeout quantity which will be adjusted to reflect remaining
     * time on spurious wakeups
     */
    public boolean backpressureBarrier(final long start, long timeoutNanos) throws InterruptedException {
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
                            if (!m_backpressure) break;

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

    class HostConfig {
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

    class InternalClientStatusListener extends ClientStatusListenerExt {

        boolean m_useAdminPort = false;
        boolean m_adminPortChecked = false;
        boolean m_connectionSuccess = false;
        AtomicInteger connectionTaskCount = new AtomicInteger(0);
        @Override
        public void backpressure(boolean status) {
            synchronized (m_backpressureLock) {
                if (status) {
                    m_backpressure = true;
                } else {
                    m_backpressure = false;
                    m_backpressureLock.notifyAll();
                }
            }
        }

        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft,
                ClientStatusListenerExt.DisconnectCause cause) {
            if (connectionsLeft == 0) {
                //Wake up client and let it attempt to queue work
                //and then fail with a NoConnectionsException
                synchronized (m_backpressureLock) {
                    m_backpressure = false;
                    m_backpressureLock.notifyAll();
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
         * notify client upon a connection creation failure.
         * @param host HostConfig with IP address and port
         * @param status The status of connection creation
         */
        void nofifyClientConnectionCreation(HostConfig host, ClientStatusListenerExt.AutoConnectionStatus status) {
            if (m_clientStatusListener != null) {
                m_clientStatusListener.connectionCreated((host != null) ? host.m_hostName : "",
                        (host != null) ? host.m_clientPort : -1, status);
            }
        }
        void retryConnectionCreationIfNeeded(int failCount) {
            if (failCount == 0) {
                try {
                    m_distributer.setCreateConnectionsUponTopologyChangeComplete();
                } catch (Exception e) {
                    nofifyClientConnectionCreation(null, ClientStatusListenerExt.AutoConnectionStatus.UNABLE_TO_CONNECT);
                }
            } else if (connectionTaskCount.get() < 2) {
                //if there are tasks in the queue, do not need schedule again since all the tasks do the same job
                m_ex.schedule(new CreateConnectionTask(this, connectionTaskCount), 10, TimeUnit.SECONDS);
            }
        }

        /**
         * find all the host which have not been connected to the client via @SystemInformation
         * and make connections
         */
        public void createConnectionsUponTopologyChange() {
            m_ex.execute(new CreateConnectionTask(this, connectionTaskCount));
        }
    }

    class CreateConnectionTask implements Runnable {
        final InternalClientStatusListener listener;
        final AtomicInteger connectionTaskCount;
        public CreateConnectionTask(InternalClientStatusListener listener, AtomicInteger connectionTaskCount ) {
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
                            createConnection(config.m_ipAddress,config.getPort(listener.m_useAdminPort));
                            listener.nofifyClientConnectionCreation(config, ClientStatusListenerExt.AutoConnectionStatus.SUCCESS);
                        } catch (Exception e) {
                            listener.nofifyClientConnectionCreation(config, ClientStatusListenerExt.AutoConnectionStatus.UNABLE_TO_CONNECT);
                            failCount++;
                        }
                    }
                } else {
                    listener.nofifyClientConnectionCreation(null, ClientStatusListenerExt.AutoConnectionStatus.UNABLE_TO_QUERY_TOPOLOGY);
                    failCount++;
                }
            } catch (Exception e) {
                listener.nofifyClientConnectionCreation(null, ClientStatusListenerExt.AutoConnectionStatus.UNABLE_TO_QUERY_TOPOLOGY);
                failCount++;
            } finally {
                connectionTaskCount.decrementAndGet();
                listener.retryConnectionCreationIfNeeded(failCount);
            }
        }
    }
     /****************************************************
                        Implementation
     ****************************************************/



    static final Logger LOG = Logger.getLogger(ClientImpl.class.getName());  // Logger shared by client package.
    private final Distributer m_distributer;                             // de/multiplexes connections to a cluster
    private final Object m_backpressureLock = new Object();
    private boolean m_backpressure = false;

    private boolean m_blockingQueue = true;

    private final ReconnectStatusListener m_reconnectStatusListener;

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

    /**
     * Not exposed to users for the moment.
     */
    public void resetInstanceId() {
        m_distributer.resetInstanceId();
    }

    @Override
    public String getBuildString() {
        return m_distributer.getBuildString();
    }

    @Override
    public boolean blocking() {
        return m_blockingQueue;
    }

    private static String getHostnameFromHostnameColonPort(String server) {
        server = server.trim();
        String[] parts = server.split(":");
        if (parts.length == 1) {
            return server;
        }
        else {
            assert (parts.length == 2);
            return parts[0].trim();
        }
    }

    private static int getPortFromHostnameColonPort(String server,
            int defaultPort) {
        String[] parts = server.split(":");
        if (parts.length == 1) {
            return defaultPort;
        }
        else {
            assert (parts.length == 2);
            return Integer.parseInt(parts[1]);
        }
    }

    @Override
    public void createConnection(String host) throws UnknownHostException, IOException {
        if (m_username == null) {
            throw new IllegalStateException("Attempted to use createConnection(String host) " +
                    "with a client that wasn't constructed with a username and password specified");
        }
        int port = getPortFromHostnameColonPort(host, Client.VOLTDB_SERVER_PORT);
        host = getHostnameFromHostnameColonPort(host);
        createConnectionWithHashedCredentials(host, port, m_username, m_passwordHash);
    }

    @Override
    public void createConnection(String host, int port) throws UnknownHostException, IOException {
        if (m_username == null) {
            throw new IllegalStateException("Attempted to use createConnection(String host) " +
                    "with a client that wasn't constructed with a username and password specified");
        }
        createConnectionWithHashedCredentials(host, port, m_username, m_passwordHash);
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
        // don't do anything (be silent) if empty path
        if ((path == null) || (path.length() == 0)) {
            return;
        }

        FileWriter fw = new FileWriter(path);
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

    //Hidden method to check if Hashinator is initialized.
    public boolean isHashinatorInitialized() {
        return m_distributer.isHashinatorInitialized();
    }

    //Hidden method for getPartitionForParameter
    public long getPartitionForParameter(byte typeValue, Object value) {
        return m_distributer.getPartitionForParameter(typeValue, value);

    }

    public HashinatorLiteType getHashinatorType() {
        return m_distributer.getHashinatorType();
    }

    @Override
    public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, boolean upsertMode, BulkLoaderFailureCallBack blfcb) throws Exception
    {
        synchronized(m_vblGlobals) {
            return new VoltBulkLoader(m_vblGlobals, tableName, maxBatchSize, upsertMode, blfcb);
        }
    }

    @Override
    public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, BulkLoaderFailureCallBack blfcb) throws Exception
    {
        synchronized(m_vblGlobals) {
            return new VoltBulkLoader(m_vblGlobals, tableName, maxBatchSize, blfcb);
        }
    }

    @Override
    public ClientResponseWithPartitionKey[] callAllPartitionProcedure(String procedureName, Object... params)
            throws IOException, NoConnectionsException, ProcCallException {
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
    public boolean callAllPartitionProcedure(AllPartitionProcedureCallback callback, String procedureName,
            Object... params) throws IOException, NoConnectionsException, ProcCallException {
        if (callback == null) {
            throw new IllegalArgumentException("AllPartitionProcedureCallback can not be null");
        }

        Object[] args = new Object[params.length + 1];
        System.arraycopy(params, 0, args, 1, params.length);

        final ImmutableSet<Integer> partitionSet = m_distributer.getPartitionKeys();
        int partitionCount = partitionSet.size();
        AtomicInteger counter = new AtomicInteger(partitionCount);
        assert(partitionCount > 0);
        ClientResponseWithPartitionKey[] responses = new ClientResponseWithPartitionKey[partitionCount];
        for (Integer key : partitionSet) {
            args[0] = key;
            partitionCount--;
            OnePartitionProcedureCallback cb = new OnePartitionProcedureCallback(counter, key, partitionCount, responses, callback);
            try {
                // Call the more complex method to ensure that the allPartition flag for the invocation is
                // set to true. This gives a nice error message if the target procedure is incompatible.
                if (!callProcedureWithClientTimeout(cb, BatchTimeoutOverrideType.NO_TIMEOUT, true,
                        procedureName, Distributer.USE_DEFAULT_CLIENT_TIMEOUT, TimeUnit.NANOSECONDS, args))
                {
                    final ClientResponse r = new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE, new VoltTable[0],
                            "The procedure is not queued for execution.");
                    throw new ProcCallException(r, null, null);
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
    class OnePartitionProcedureCallback implements ProcedureCallback {

        final ClientResponseWithPartitionKey[] m_responses;
        final int m_index;
        final Object m_partitionKey;
        final AtomicInteger m_partitionCounter;
        final AllPartitionProcedureCallback m_cb;

        /**
         * Callback initialization
         * @param responseWaiter The count down latch
         * @param partitionKey  The partition where the call back works on
         * @param index  The index for PartitionClientResponse
         * @param responses The final result array
         */
        public OnePartitionProcedureCallback(AtomicInteger counter, Object partitionKey, int index,
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

        public void exceptionCallback(Exception e) throws Exception {

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

        public ClientResponseWithPartitionKey[] getResponse() {
            return m_responses;
        }
    }
}
