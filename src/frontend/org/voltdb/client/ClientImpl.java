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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.HashinatorLite.HashinatorLiteType;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.BulkLoaderState;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;
import org.voltdb.common.Constants;
import org.voltdb.utils.Encoder;

/**
 *  A client that connects to one or more nodes in a VoltCluster
 *  and provides methods to call stored procedures and receive
 *  responses.
 */
public final class ClientImpl implements Client, ReplicaProcCaller {

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
    final CSL m_listener = new CSL();

    /*
     * Username and password as set by the constructor.
     */
    private final String m_username;
    private final byte m_passwordHash[];

    /**
     * These threads belong to the network thread pool
     * that invokes callbacks. These threads are "blessed"
     * and should never experience backpressure. This ensures that the
     * network thread pool doesn't block when queuing procedures from
     * a callback.
     */
    private final CopyOnWriteArrayList<Long> m_blessedThreadIds = new CopyOnWriteArrayList<Long>();

    private BulkLoaderState m_vblGlobals = new BulkLoaderState(this);

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
        m_distributer = new Distributer(
                config.m_heavyweight,
                config.m_procedureCallTimeoutNanos,
                config.m_connectionResponseTimeoutMS,
                config.m_useClientAffinity,
                config.m_subject);
        m_distributer.addClientStatusListener(m_listener);
        String username = config.m_username;
        if (config.m_subject != null) {
            username = config.m_subject.getPrincipals().iterator().next().getName();
        }
        m_username = username;

        if (config.m_reconnectOnConnectionLoss) {
            m_reconnectStatusListener = new ReconnectStatusListener(this,
                    config.m_initialConnectionRetryIntervalMS, config.m_maxConnectionRetryIntervalMS);
            m_distributer.addClientStatusListener(m_reconnectStatusListener);
        } else {
            m_reconnectStatusListener = null;
        }

        if (config.m_cleartext) {
            m_passwordHash = ConnectionUtil.getHashedPassword(config.m_password);
        } else {
            m_passwordHash = Encoder.hexDecode(config.m_password);
        }
        if (config.m_listener != null) {
            m_distributer.addClientStatusListener(config.m_listener);
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

    public void createConnectionWithHashedCredentials(String host, int port, String program, byte[] hashedPassword)
        throws IOException
    {
        if (m_isShutdown) {
            throw new IOException("Client instance is shutdown");
        }
        final String subProgram = (program == null) ? "" : program;
        final byte[] subPassword = (hashedPassword == null) ? ConnectionUtil.getHashedPassword("") : hashedPassword;

        if (!verifyCredentialsAreAlwaysTheSame(subProgram, subPassword)) {
            throw new IOException("New connection authorization credentials do not match previous credentials for client.");
        }

        m_distributer.createConnectionWithHashedCredentials(host, subProgram, subPassword, port);
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
    public final ClientResponse callProcedure(String procName, Object... parameters)
        throws IOException, NoConnectionsException, ProcCallException
    {
        return callProcedureWithTimeout(procName, Distributer.USE_DEFAULT_TIMEOUT, TimeUnit.SECONDS, parameters);
    }

    /**
     * Synchronously invoke a procedure call blocking until a result is available.
     *
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param timeout timeout for the procedure
     * @param unit TimeUnit of procedure timeout
     * @param parameters vararg list of procedure's parameter values.
     * @return ClientResponse for execution.
     * @throws org.voltdb.client.ProcCallException
     * @throws NoConnectionsException
     */
    public ClientResponse callProcedureWithTimeout(String procName, long timeout, TimeUnit unit, Object... parameters)
            throws IOException, NoConnectionsException, ProcCallException {
        final SyncCallback cb = new SyncCallback();
        cb.setArgs(parameters);
        final ProcedureInvocation invocation
                = new ProcedureInvocation(m_handle.getAndIncrement(), procName, parameters);
        return callProcedure(cb, System.nanoTime(), unit.toNanos(timeout), invocation);
    }

    /**
     * The synchronous procedure call method for DR replication
     */
    @Override
    public ClientResponse callProcedure(
            long originalTxnId,
            long originalUniqueId,
            String procName,
            Object... parameters)
            throws IOException, NoConnectionsException, ProcCallException
    {
        final SyncCallback cb = new SyncCallback();
        cb.setArgs(parameters);
        final ProcedureInvocation invocation =
            new ProcedureInvocation(originalTxnId, originalUniqueId,
                                    m_handle.getAndIncrement(),
                                    procName, parameters);
        return callProcedure(cb, System.nanoTime(), Distributer.USE_DEFAULT_TIMEOUT, invocation);
    }

    private final ClientResponse callProcedure(SyncCallback cb, long nowNanos, long timeout, ProcedureInvocation invocation)
            throws IOException, NoConnectionsException, ProcCallException
    {
        if (m_isShutdown) {
            throw new NoConnectionsException("Client instance is shutdown");
        }

        if (m_blessedThreadIds.contains(Thread.currentThread().getId())) {
            throw new IOException("Can't invoke a procedure synchronously from with the client callback thread " +
                    " without deadlocking the client library");
        }

        m_distributer.queue(
                invocation,
                cb,
                true, nowNanos, timeout);

        try {
            cb.waitForResponse();
        } catch (final InterruptedException e) {
            throw new java.io.InterruptedIOException("Interrupted while waiting for response");
        }
        if (cb.getResponse().getStatus() != ClientResponse.SUCCESS) {
            throw new ProcCallException(cb.getResponse(), cb.getResponse().getStatusString(), null);
        }
        // cb.result() throws ProcCallException if procedure failed
        return cb.getResponse();
    }

    /**
     * Asynchronously invoke a procedure call.
     * @param callback TransactionCallback that will be invoked with procedure results.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return True if the procedure was queued and false otherwise
     */
    @Override
    public final boolean callProcedure(ProcedureCallback callback, String procName, Object... parameters)
    throws IOException, NoConnectionsException {
        //Time unit doesn't matter in this case since the timeout isn't being specified
        return callProcedureWithTimeout(callback, procName, Distributer.USE_DEFAULT_TIMEOUT, TimeUnit.NANOSECONDS, parameters);
    }

    /**
     * Asynchronously invoke a procedure call.
     *
     * @param callback TransactionCallback that will be invoked with procedure results.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param timeout timeout for the procedure
     * @param unit TimeUnit of procedure timeout
     * @param parameters vararg list of procedure's parameter values.
     * @return True if the procedure was queued and false otherwise
     */
    public boolean callProcedureWithTimeout(ProcedureCallback callback, String procName,
            long timeout, TimeUnit unit, Object... parameters) throws IOException, NoConnectionsException {
        if (m_isShutdown) {
            return false;
        }
        if (callback instanceof ProcedureArgumentCacher) {
            ((ProcedureArgumentCacher) callback).setArgs(parameters);
        }
        ProcedureInvocation invocation
                = new ProcedureInvocation(m_handle.getAndIncrement(), procName, parameters);
        return private_callProcedure(callback, 0, invocation, unit.toNanos(timeout));
    }

    /**
     * Asynchronously invoke a replicated procedure. If there is backpressure
     * this call will block until the invocation is queued. If configureBlocking(false) is invoked
     * then it will return immediately. Check
     * the return value to determine if queuing actually took place.
     *
     * @param originalTxnId The original txnId generated for this invocation.
     * @param originalTimestamp The original timestamp associated with this invocation.
     * @param callback ProcedureCallback that will be invoked with procedure results.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return <code>true</code> if the procedure was queued and
     *         <code>false</code> otherwise
     */
    @Override
    public final boolean callProcedure(
            long originalTxnId,
            long originalUniqueId,
            ProcedureCallback callback,
            String procName,
            Object... parameters)
            throws IOException, NoConnectionsException {
        if (callback instanceof ProcedureArgumentCacher) {
            ((ProcedureArgumentCacher)callback).setArgs(parameters);
        }
        ProcedureInvocation invocation =
            new ProcedureInvocation(originalTxnId, originalUniqueId,
                                    m_handle.getAndIncrement(),
                                    procName, parameters);
        return private_callProcedure(callback, 0, invocation, Distributer.USE_DEFAULT_TIMEOUT);
    }

    @Override
    public int calculateInvocationSerializedSize(String procName,
            Object... parameters) {
        final ProcedureInvocation invocation =
            new ProcedureInvocation(0, procName, parameters);
        return invocation.getSerializedSize();
    }

    @Override
    public final boolean callProcedure(
           ProcedureCallback callback,
           int expectedSerializedSize,
            String procName,
            Object... parameters)
           throws NoConnectionsException, IOException {
        if (callback instanceof ProcedureArgumentCacher) {
            ((ProcedureArgumentCacher)callback).setArgs(parameters);
        }
        ProcedureInvocation invocation =
            new ProcedureInvocation(m_handle.getAndIncrement(), procName, parameters);
        return private_callProcedure(callback, expectedSerializedSize, invocation, Distributer.USE_DEFAULT_TIMEOUT);
    }

    private final boolean private_callProcedure(
            ProcedureCallback callback,
            int expectedSerializedSize,
            ProcedureInvocation invocation, long timeoutNanos)
            throws IOException, NoConnectionsException {
        if (m_isShutdown) {
            return false;
        }

        if (callback == null) {
            callback = new NullCallback();
        }

        final long nowNanos = System.nanoTime();

        //Blessed threads (the ones that invoke callbacks) are not subject to backpressure
        boolean isBlessed = m_blessedThreadIds.contains(Thread.currentThread().getId());
        if (m_blockingQueue) {
            while (!m_distributer.queue(
                    invocation,
                    callback,
                    isBlessed, nowNanos, timeoutNanos)) {

                /*
                 * Wait on backpressure honoring the timeout settings
                 */
                final long delta = Math.max(1, System.nanoTime() - nowNanos);
                final long timeout = timeoutNanos == Distributer.USE_DEFAULT_TIMEOUT ? m_distributer.getProcedureTimeoutNanos() : timeoutNanos;
                try {
                    if (backpressureBarrier(nowNanos, timeout - delta)) {
                        final ClientResponseImpl r = new ClientResponseImpl(
                                ClientResponse.CONNECTION_TIMEOUT,
                                ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                                "",
                                new VoltTable[0],
                                String.format("No response received in the allotted time (set to %d ms).",
                                        TimeUnit.NANOSECONDS.toMillis(timeoutNanos)));
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
                    isBlessed, nowNanos, timeoutNanos);
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
        }

        m_distributer.shutdown();
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

    class CSL extends ClientStatusListenerExt {

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

    public static int getPortFromHostnameColonPort(String server,
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
    public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, BulkLoaderFailureCallBack blfcb) throws Exception
    {
        synchronized(m_vblGlobals) {
            return new VoltBulkLoader(m_vblGlobals, tableName, maxBatchSize, blfcb);
        }
    }
}
