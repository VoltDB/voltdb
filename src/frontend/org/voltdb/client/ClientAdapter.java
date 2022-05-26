/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ClientStatusListenerExt.AutoConnectionStatus;
import org.voltdb.client.ClientStatusListenerExt.DisconnectCause;

import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.BulkLoaderSuccessCallback;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

/**
 * This class presents an older <code>Client</code> interface
 * to the implementation of the newer <code>Client2</code> API,
 * and can therefore ease the transition to the newer code.
 * <p>
 * Due to underlying differences in the old and new APIs,
 * the adapter may not be completely transparent. In any
 * case, not all old <code>Client</code> methods are
 * necessarily supported.
 *
 * For documentation, see {@link Client}.
 */
public class ClientAdapter implements Client {

    private Client2 client;

    // Copies of data we need to answer certain queries
    private boolean autoConnect;
    private int maxOutstandingTxns;
    private int maxTransactionsPerSecond;

    public ClientAdapter(ClientConfig config) {
        this.client = new Client2Impl(convertConfig(config));
        this.autoConnect = config.m_topologyChangeAware | config.m_reconnectOnConnectionLoss;
        this.maxOutstandingTxns = config.m_maxOutstandingTxns;
        this.maxTransactionsPerSecond = config.m_maxTransactionsPerSecond;
    }

    //////////////////////////////////////
    // All public Client methods
    /////////////////////////////////////

    @Override
    public void createConnection(String server) throws IOException {
        client.connectSync(server);
    }

    @Override
    public void createConnection(String host, int port) throws IOException {
        client.connectSync(host, port);
    }

    @Override
    public void createAnyConnection(String serverList) throws IOException {
        client.connectSync(serverList);
    }

    @Override
    public void createAnyConnection(String serverList, long timeout, long delay) throws IOException {
        client.connectSync(serverList, timeout, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public ClientResponse callProcedure(String procName, Object... parameters)
        throws IOException, ProcCallException {
        return client.callProcedureSync(procName, parameters);
    }

    @Override
    public boolean callProcedure(ProcedureCallback callback, String procName, Object... parameters) {
        AtomicBoolean refused = new AtomicBoolean();
        client.callProcedureAsync(procName, parameters)
              .thenAccept((r) -> callback(r, callback))
              .exceptionally((x) -> except(x, refused));
        return !refused.get();
    }

    @Override
    public ClientResponse callProcedureWithTimeout(int queryTimeout, String procName, Object... parameters)
        throws IOException, ProcCallException {
        Client2CallOptions opts = new Client2CallOptions().queryTimeout(queryTimeout, TimeUnit.MILLISECONDS);
        return client.callProcedureSync(opts, procName, parameters);
    }

    @Override
    public boolean callProcedureWithTimeout(ProcedureCallback callback, int queryTimeout, String procName, Object... parameters) {
        AtomicBoolean refused = new AtomicBoolean();
        Client2CallOptions opts = new Client2CallOptions().queryTimeout(queryTimeout, TimeUnit.MILLISECONDS);
        client.callProcedureAsync(opts, procName, parameters)
              .thenAccept((r) -> callback(r, callback))
              .exceptionally((x) -> except(x, refused));
        return !refused.get();
     }

    @Override
    public ClientResponse callProcedureWithClientTimeout(int queryTimeout,
                                                         String procName,
                                                         long clientTimeout,
                                                         TimeUnit unit,
                                                         Object... parameters)
        throws IOException, ProcCallException {
        Client2CallOptions opts = new Client2CallOptions()
            .queryTimeout(queryTimeout, TimeUnit.MILLISECONDS)
            .clientTimeout(clientTimeout, unit);
        return client.callProcedureSync(opts, procName, parameters);
     }

    @Override
    public boolean callProcedureWithClientTimeout(ProcedureCallback callback,
                                                  int queryTimeout,
                                                  String procName,
                                                  long clientTimeout,
                                                  TimeUnit unit,
                                                  Object... parameters) {
        AtomicBoolean refused = new AtomicBoolean();
        Client2CallOptions opts = new Client2CallOptions()
            .queryTimeout(queryTimeout, TimeUnit.MILLISECONDS)
            .clientTimeout(clientTimeout, unit);
        client.callProcedureAsync(opts, procName, parameters)
              .thenAccept((r) -> callback(r, callback))
               .exceptionally((x) -> except(x, refused));
        return !refused.get();
     }

    @Override
    public ClientResponseWithPartitionKey[] callAllPartitionProcedure(String procName, Object... params)
        throws IOException, ProcCallException {
        return client.callAllPartitionProcedureSync(null, procName, params);
    }

    @Override
    public boolean callAllPartitionProcedure(AllPartitionProcedureCallback callback, String procName, Object... params) {
        AtomicBoolean refused = new AtomicBoolean();
        client.callAllPartitionProcedureAsync(null, procName, params)
              .thenAccept((r) -> callback(r, callback))
              .exceptionally((x) -> except(x, refused));
        return !refused.get();

    }

    @Override
    public void drain() throws InterruptedException {
        client.drain();
    }

    @Override
    public void close() throws InterruptedException {
        client.close();
    }

    @Override
    public void backpressureBarrier() throws InterruptedException {
        throw new UnsupportedOperationException("unsupported: ClientAdapter.backpressureBarrier");
    }

    @Override
    public ClientStatsContext createStatsContext() {
        return client.createStatsContext();
    }

    @Override
    public Object[] getInstanceId() {
        return client.clusterInstanceId();
    }

    @Override
    public String getBuildString() {
        return client.clusterBuildString();
    }

    @Override
    public int[] getThroughputAndOutstandingTxnLimits() {
        return new int[] { maxTransactionsPerSecond, maxOutstandingTxns };
    }

    @Override
    public List<InetSocketAddress> getConnectedHostList() {
        return client.connectedHosts();
    }

    @Override
    public boolean isAutoReconnectEnabled() {
        return autoConnect;
    }

    @Override
    public boolean isTopologyChangeAwareEnabled() {
        return autoConnect;
    }

    @Override
    public void writeSummaryCSV(String statsRowName, ClientStats stats, String path)
        throws IOException {
        if (path != null && !path.isEmpty()) {
            ClientStatsUtil.writeSummaryCSV(statsRowName, stats, path);
        }
    }

    @Override
    public void writeSummaryCSV(ClientStats stats, String path) throws IOException {
        writeSummaryCSV(null, stats, path);
    }

    @Override
    public ClientResponse updateClasses(File jarPath, String classesToDelete)
        throws IOException, ProcCallException {
        return UpdateClasses.update(this, jarPath, classesToDelete);
    }

    @Override
    public boolean updateClasses(ProcedureCallback callback, File jarPath, String classesToDelete)
        throws IOException {
        return UpdateClasses.update(this, callback, jarPath, classesToDelete);
    }

    @Override
    public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize,
                                           BulkLoaderFailureCallBack failureCallback) throws Exception {
        return getNewBulkLoader(tableName, maxBatchSize, false, failureCallback, null);
    }

    @Override
    public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, boolean upsertMode,
                                           BulkLoaderFailureCallBack failureCallback) throws Exception {
        return getNewBulkLoader(tableName, maxBatchSize, upsertMode, failureCallback, null);
    }

    @Override
    public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, boolean upsertMode,
                                           BulkLoaderFailureCallBack failureCallback,
                                           BulkLoaderSuccessCallback successCallback) throws Exception {
         return client.newBulkLoader(tableName, maxBatchSize, upsertMode, failureCallback, successCallback);
    }

    @Override
    public boolean waitForTopology(long timeout) {
        return client.waitForTopology(timeout, TimeUnit.MILLISECONDS);
    }

    //////////////////////////////////////
    // Implementation helpers
    /////////////////////////////////////

    private Void callback(ClientResponse resp, ProcedureCallback cb) {
        try {
            cb.clientCallback(resp);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return null;
    }

    private Void callback(ClientResponseWithPartitionKey[] resp, AllPartitionProcedureCallback cb) {
        try {
            cb.clientCallback(resp);
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return null;
    }

    private Void except(Throwable th, AtomicBoolean refused) {
        if (th instanceof RequestLimitException) {
            refused.set(true); // this is always synchronous
            return null;
        }
        else {
            throw new RuntimeException(th);
        }
    }

    private Client2Config convertConfig(ClientConfig c1) {
        Client2Config c2 = new Client2Config();

        c2.username(c1.m_username);
        if (c1.m_password != null && !c1.m_password.isEmpty()) {
            if (c1.m_cleartext) {
                c2.password(c1.m_password);
            }
            else {
                c2.hashedPassword(c1.m_password, c1.m_hashScheme);
            }
        }
        else if (c1.m_subject != null) {
            c2.authenticatedSubject(c1.m_subject);
        }

        if (c1.m_enableSSL) {
            c2.sslConfig = c1.m_sslConfig;
            c2.enableSSL();
        }

        c2.procedureCallTimeout(c1.m_procedureCallTimeoutNanos, TimeUnit.NANOSECONDS)
          .outstandingTransactionLimit(c1.m_maxOutstandingTxns)
          .clientRequestBackpressureLevel(c1.m_backpressureQueueRequestLimit, (int)(1.25 * c1.m_backpressureQueueRequestLimit))
          .connectionResponseTimeout(c1.m_connectionResponseTimeoutMS, TimeUnit.MILLISECONDS)
          .reconnectDelay(c1.m_initialConnectionRetryIntervalMS, c1.m_maxConnectionRetryIntervalMS, TimeUnit.MILLISECONDS);

        if (!c1.m_topologyChangeAware && !c1.m_reconnectOnConnectionLoss) {
            c2.disableConnectionMgmt();
        }

        if (c1.m_maxTransactionsPerSecond < Integer.MAX_VALUE / 2) {
            c2.transactionRateLimit(c1.m_maxTransactionsPerSecond);
        }

        if (c1.m_heavyweight) {
            throw new UnsupportedOperationException("unsupported: heavyweight clients");
        }

        if (c1.m_listener != null) {
            c2.connectFailureHandler((host, port) -> c1.m_listener.connectionCreated(host, port, AutoConnectionStatus.UNABLE_TO_CONNECT));
            c2.connectionUpHandler((host, port) -> c1.m_listener.connectionCreated(host, port, AutoConnectionStatus.SUCCESS));
            c2.connectionDownHandler((host, port) -> c1.m_listener.connectionLost(host, port, client.connectedHosts().size(), DisconnectCause.CONNECTION_CLOSED));
            c2.requestBackpressureHandler(c1.m_listener::backpressure);
            c2.lateResponseHandler(c1.m_listener::lateProcedureResponse);
        }

        if (c1.m_requestPriority > 0) {
            c2.requestPriority(c1.m_requestPriority);
        }

        return c2;
    }

}
