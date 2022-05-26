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

package org.voltdb.jdbc;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.utils.ssl.SSLConfiguration;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

/**
 * Provides a high-level wrapper around the core {@link Client} class to provide performance
 * tracking, connection pooling and Future-based asynchronous execution support. ClientConnections
 * should be obtained through the {@link JDBC4ClientConnectionPool} get methods and cannot be
 * instantiated directly.
 *
 * Extending ClientStatusListenerExt allows us to detect dropped connections, etc..
 *
 * @author Seb Coursol (copied and renamed from exampleutils)
 * @since 2.0
 */
public class JDBC4ClientConnection implements Closeable {
    private final ArrayList<String> servers;
    private final ClientConfig config;
    private AtomicReference<Client> client = new AtomicReference<Client>();

    /**
     * The base hash/key for this connection, that uniquely identifies its parameters, as defined by
     * the pool.
     */
    protected final String keyBase;

    /**
     * The actual hash/key for this connection, that uniquely identifies this specific native
     * {@link Client} wrapper.
     */
    protected final String key;

    /**
     * The number of active users on the connection. Used and managed by the pool to determine when
     * a specific {@link Client} wrapper has reached capacity (and a new one should be created).
     */
    protected short users = 0;

    /**
     * The default asynchronous operation timeout for Future-based executions (while the operation
     * may so time out on the client side, note that, technically, once submitted to the database
     * cluster, the call cannot be cancelled!).
     */
    protected long defaultAsyncTimeout = 60000;

    /**
     * Creates a new native client wrapper from the given parameters (internal use only).
     *
     * @param clientConnectionKeyBase
     *            the base hash/key for this connection, as defined by the pool.
     * @param clientConnectionKey
     *            the actual hash/key for this connection, as defined by the pool (may contain a
     *            trailing index when the pool decides a new client needs to be created based on the
     *            number of clients).
     * @param servers
     *            the list of VoltDB servers to connect to in hostname[:port] format.
     * @param user
     *            the user name to use when connecting to the server(s).
     * @param password
     *            the password to use when connecting to the server(s).
     * @param isHeavyWeight
     *            the flag indicating callback processes on this connection will be heavy (long
     *            running callbacks). By default the connection only allocates one background
     *            processing thread to process callbacks. If those callbacks run for a long time,
     *            the network stack can get clogged with pending responses that have yet to be
     *            processed, at which point the server will disconnect the application, thinking it
     *            died and is not reading responses as fast as it is pushing requests. When the flag
     *            is set to 'true', an additional 2 processing thread will deal with processing
     *            callbacks, thus mitigating the issue.
     * @param maxOutstandingTxns
     *            the number of transactions the client application may push against a specific
     *            connection before getting blocked on back-pressure. By default the connection
     *            allows 3,000 open transactions before preventing the client from posting more
     *            work, thus preventing server fire-hosing. In some cases however, with very fast,
     *            small transactions, this limit can be raised.
     * @param reconnectOnConnectionLoss
     *            Attempts to reconnect to a node with retry after connection loss
     * @throws IOException
     * @throws UnknownHostException
     */
    protected JDBC4ClientConnection(
            String clientConnectionKeyBase, String clientConnectionKey,
            String[] servers, String user, String password, boolean isHeavyWeight,
            int maxOutstandingTxns, boolean reconnectOnConnectionLoss)
                    throws UnknownHostException, IOException
    {
        this(clientConnectionKeyBase, clientConnectionKey, servers, user, password, isHeavyWeight,
             maxOutstandingTxns, reconnectOnConnectionLoss, null, null, false, -1);
    }

    /**
     * Creates a new native client wrapper from the given parameters (internal use only).
     *
     * @param clientConnectionKeyBase
     *            the base hash/key for this connection, as defined by the pool.
     * @param clientConnectionKey
     *            the actual hash/key for this connection, as defined by the pool (may contain a
     *            trailing index when the pool decides a new client needs to be created based on the
     *            number of clients).
     * @param servers
     *            the list of VoltDB servers to connect to in hostname[:port] format.
     * @param user
     *            the user name to use when connecting to the server(s).
     * @param password
     *            the password to use when connecting to the server(s).
     * @param isHeavyWeight
     *            the flag indicating callback processes on this connection will be heavy (long
     *            running callbacks). By default the connection only allocates one background
     *            processing thread to process callbacks. If those callbacks run for a long time,
     *            the network stack can get clogged with pending responses that have yet to be
     *            processed, at which point the server will disconnect the application, thinking it
     *            died and is not reading responses as fast as it is pushing requests. When the flag
     *            is set to 'true', an additional 2 processing thread will deal with processing
     *            callbacks, thus mitigating the issue.
     * @param maxOutstandingTxns
     *            the number of transactions the client application may push against a specific
     *            connection before getting blocked on back-pressure. By default the connection
     *            allows 3,000 open transactions before preventing the client from posting more
     *            work, thus preventing server fire-hosing. In some cases however, with very fast,
     *            small transactions, this limit can be raised.
     * @param reconnectOnConnectionLoss
     *            Attempts to reconnect to a node with retry after connection loss
     * @param sslConfig
     *            Contains properties - trust store path and password, key store path and password,
     *            used for connecting with server over SSL. For unencrypted connection, passed in ssl
     *            config is null
     * @param kerberosConfig
     *            Uses specified JAAS file entry id for kerberos authentication if set.
     * @throws IOException
     * @throws UnknownHostException
     */
    protected JDBC4ClientConnection(
            String clientConnectionKeyBase, String clientConnectionKey,
            String[] servers, String user, String password, boolean isHeavyWeight,
            int maxOutstandingTxns, boolean reconnectOnConnectionLoss,
            SSLConfiguration.SslConfig sslConfig, String kerberosConfig)
                    throws UnknownHostException, IOException {
        this(clientConnectionKeyBase, clientConnectionKey, servers, user, password, isHeavyWeight,
             maxOutstandingTxns, reconnectOnConnectionLoss, sslConfig, kerberosConfig, false, -1);

    }

    /**
     * Creates a new native client wrapper from the given parameters (internal use only).
     *
     * @param clientConnectionKeyBase
     *            the base hash/key for this connection, as defined by the pool.
     * @param clientConnectionKey
     *            the actual hash/key for this connection, as defined by the pool (may contain a
     *            trailing index when the pool decides a new client needs to be created based on the
     *            number of clients).
     * @param servers
     *            the list of VoltDB servers to connect to in hostname[:port] format.
     * @param user
     *            the user name to use when connecting to the server(s).
     * @param password
     *            the password to use when connecting to the server(s).
     * @param isHeavyWeight
     *            the flag indicating callback processes on this connection will be heavy (long
     *            running callbacks). By default the connection only allocates one background
     *            processing thread to process callbacks. If those callbacks run for a long time,
     *            the network stack can get clogged with pending responses that have yet to be
     *            processed, at which point the server will disconnect the application, thinking it
     *            died and is not reading responses as fast as it is pushing requests. When the flag
     *            is set to 'true', an additional 2 processing thread will deal with processing
     *            callbacks, thus mitigating the issue.
     * @param maxOutstandingTxns
     *            the number of transactions the client application may push against a specific
     *            connection before getting blocked on back-pressure. By default the connection
     *            allows 3,000 open transactions before preventing the client from posting more
     *            work, thus preventing server fire-hosing. In some cases however, with very fast,
     *            small transactions, this limit can be raised.
     * @param reconnectOnConnectionLoss
     *            Attempts to reconnect to a node with retry after connection loss
     * @param sslConfig
     *            Contains properties - trust store path and password, key store path and password,
     *            used for connecting with server over SSL. For unencrypted connection, passed in ssl
     *            config is null
     * @param kerberosConfig
     *            Uses specified JAAS file entry id for kerberos authentication if set.
     * @param topologyChangeAware
     *            make client aware of changes in topology.
     * @param priority
     *            request priority if > 0, or any value <= 0 for not specified
     *
     * @throws IOException
     * @throws UnknownHostException
     */
    protected JDBC4ClientConnection(
            String clientConnectionKeyBase, String clientConnectionKey,
            String[] servers, String user, String password, boolean isHeavyWeight,
            int maxOutstandingTxns, boolean reconnectOnConnectionLoss,
            SSLConfiguration.SslConfig sslConfig, String kerberosConfig,
            boolean topologyChangeAware, int priority)
            throws UnknownHostException, IOException
    {
        // Save the list of trimmed non-empty server names.
        this.servers = new ArrayList<String>(servers.length);
        for (String server : servers) {
            server = server.trim();
            if (!server.isEmpty()) {
                this.servers.add(server);
            }
        }
        if (this.servers.isEmpty()) {
            throw new UnknownHostException("JDBC4ClientConnection: no servers provided");
        }

        this.keyBase = clientConnectionKeyBase;
        this.key = clientConnectionKey;

        boolean enableSSL = (sslConfig != null) ? true : false;

        // Create configuration
        config = new ClientConfig(user, password);
        config.setHeavyweight(isHeavyWeight);
        config.setMaxOutstandingTxns(maxOutstandingTxns);
        config.setReconnectOnConnectionLoss(reconnectOnConnectionLoss);
        config.setTopologyChangeAware(topologyChangeAware);

        if (priority > 0) {
            config.setRequestPriority(priority);
        }

        if (enableSSL) {
            if (sslConfig.trustStorePath != null && sslConfig.trustStorePath.trim().length() > 0) {
                config.setTrustStore(sslConfig.trustStorePath, sslConfig.trustStorePassword);
            }
            config.enableSSL();
        }

        if (kerberosConfig != null) {
            config.enableKerberosAuthentication(kerberosConfig.trim());
        }

        // Create client and connect.
        createClientAndConnect();
    }

    /**
     * Private method to (re)initialize a client connection.
     * @return new ClientImpl
     * @throws UnknownHostException
     * @throws IOException
     */
    private ClientImpl createClientAndConnect() throws UnknownHostException, IOException
    {
        // Make client connections.
        ClientImpl clientTmp = (ClientImpl) ClientFactory.createClient(this.config);
        // ENG-6231: Only fail if we can't connect to any of the provided servers.
        boolean connectedAnything = false;
        for (String server : this.servers) {
            try {
                clientTmp.createConnection(server);
                connectedAnything = true;
            }
            catch (UnknownHostException e) {
            }
            catch (IOException e) {
            }
        }

        if (!connectedAnything) {
            try {
                clientTmp.close();
            } catch (InterruptedException ie) {}
            throw new IOException("Unable to connect to VoltDB cluster with servers: " + this.servers);
        }

        this.client.set(clientTmp);
        this.users++;
        return clientTmp;
    }

    /**
     * Get current client or reconnect one as needed.
     * Concurrency strategy: If the connection is lost while providing one the
     * caller will get a non-null, but ultimately bad connection that will fail.
     * But it won't cause an NPE. This method is synchronized so that a
     * reconnection won't happen simultaneously. The client won't get dropped
     * if a parallel thread comes in later trying to drop the original client
     * because dropClient() only does it if the request matches the current client.
     * @return  client
     * @throws UnknownHostException
     * @throws IOException
     */
    protected synchronized ClientImpl getClient() throws UnknownHostException, IOException
    {
        ClientImpl retClient = (ClientImpl) this.client.get() ;
        if (retClient != null) {
            return retClient;
        }
        return this.createClientAndConnect();
    }

    /**
     * Used by the pool to indicate a new thread/user is using a specific connection, helping the
     * pool determine when new connections need to be created.
     *
     * @return the reference to this connection to be returned to the calling user.
     */
    protected synchronized JDBC4ClientConnection use() {
        this.users++;
        return this;
    }

    /**
     * Used by the pool to indicate a thread/user has stopped using the connection (and optionally
     * close the underlying client if there are no more users against it).
     */
    protected synchronized void dispose() {
        this.users--;
        if (this.users == 0) {
            try {
                Client currentClient = this.client.get();
                if (currentClient != null) {
                    currentClient.close();
                }
            } catch (Exception x) {
                // ignore
            }
        }
    }

    /**
     * Drop the client connection, e.g. when a NoConnectionsException is caught.
     * It will try to reconnect as needed and appropriate.
     * @param clientToDrop caller-provided client to avoid re-nulling from another thread that comes in later
     */
    protected synchronized void dropClient(ClientImpl clientToDrop) {
        Client currentClient = this.client.get();
        if (currentClient != null && currentClient == clientToDrop) {
            try {
                currentClient.close();
                this.client.set(null);
            }
            catch (Exception x) {
                // ignore
            }
        }
        this.users = 0;
    }

    /**
     * Closes the connection, releasing it to the pool so another thread/client may pick it up. This
     * method must be closed by a user when the connection is no longer needed to avoid pool
     * pressure and leaks where the pool would keep creating new connections all the time,
     * wrongfully believing all existing connections to be actively used.
     */
    @Override
    public void close() {
        JDBC4ClientConnectionPool.dispose(this);
    }

    /**
     * Executes a procedure synchronously and returns the result to the caller. The method
     * internally tracks execution performance.
     *
     * @param procedure
     *            the name of the procedure to call.
     * @param parameters
     *            the list of parameters to pass to the procedure.
     * @return the response sent back by the VoltDB cluster for the procedure execution.
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
   public ClientResponse execute(String procedure, long timeout, TimeUnit unit, Object... parameters)
            throws NoConnectionsException, IOException, ProcCallException {
        ClientImpl currentClient = this.getClient();
        if (unit == null) {
            unit = TimeUnit.SECONDS;
        }
        try {
            // If connections are lost try reconnecting.
            ClientResponse response = currentClient.callProcedureWithClientTimeout(
                    BatchTimeoutOverrideType.NO_TIMEOUT, procedure, timeout, unit, parameters);
            return response;
        }
        catch (ProcCallException pce) {
            throw pce;
        }
        catch (NoConnectionsException e) {
            this.dropClient(currentClient);
            throw e;
        }
    }

    /**
     * Internal asynchronous callback used to track the execution performance of asynchronous calls.
     */
    private static class TrackingCallback implements ProcedureCallback {
        private final JDBC4ClientConnection Owner;
        private final String Procedure;
        private final ProcedureCallback UserCallback;

        /**
         * Creates a new callback.
         *
         * @param owner
         *            the connection to which the request was sent (and that will be receiving the
         *            response).
         * @param procedure
         *            the procedure being executed and for which we're awaiting a response.
         * @param userCallback
         *            the user-specified callback that will be called once we have tracked
         *            statistics, making this internal callback transparent to the calling
         *            application.
         */
        public TrackingCallback(JDBC4ClientConnection owner, String procedure,
                ProcedureCallback userCallback) {
            this.Owner = owner;
            this.Procedure = procedure;
            this.UserCallback = userCallback;
        }

        /**
         * Processes the server response, tracking performance statistics internally, then calling
         * the user-specified callback (if any).
         */
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (this.UserCallback != null)
                this.UserCallback.clientCallback(response);
        }
    }

    /**
     * Executes a procedure asynchronously, then calls the provided user callback with the server
     * response upon completion.
     *
     * @param callback
     *            the user-specified callback to call with the server response upon execution
     *            completion.
     * @param procedure
     *            the name of the procedure to call.
     * @param parameters
     *            the list of parameters to pass to the procedure.
     * @return the result of the submission false if the client connection was terminated and unable
     *         to post the request to the server, true otherwise.
     */
    public boolean executeAsync(ProcedureCallback callback, String procedure, Object... parameters)
            throws NoConnectionsException, IOException
    {
        ClientImpl currentClient = this.getClient();
        try {
            return currentClient.callProcedure(new TrackingCallback(this, procedure, callback),
                    procedure, parameters);
        }
        catch (NoConnectionsException e) {
            this.dropClient(currentClient);
            throw e;
        }
    }

    /**
     * Executes a procedure asynchronously, returning a Future that can be used by the caller to
     * wait upon completion before processing the server response.
     *
     * @param procedure
     *            the name of the procedure to call.
     * @param parameters
     *            the list of parameters to pass to the procedure.
     * @return the Future created to wrap around the asynchronous process.
     */
    public Future<ClientResponse> executeAsync(String procedure, Object... parameters)
            throws NoConnectionsException, IOException
    {
        ClientImpl currentClient = this.getClient();
        final JDBC4ExecutionFuture future = new JDBC4ExecutionFuture(this.defaultAsyncTimeout);
        try {
            currentClient.callProcedure(new TrackingCallback(this, procedure, new ProcedureCallback() {
                @SuppressWarnings("unused")
                final JDBC4ExecutionFuture result;
                {
                    this.result = future;
                }

                @Override
                public void clientCallback(ClientResponse response) throws Exception {
                    future.set(response);
                }
            }), procedure, parameters);
        }
        catch (NoConnectionsException e) {
            this.dropClient(currentClient);
            throw e;
        }
        return future;
    }


    /**
     * Gets the new version of the performance statistics for this connection only.
     * @return A {@link ClientStatsContext} that correctly represents the client statistics.
     */
    public ClientStatsContext getClientStatsContext() {
        if (this.client.get() == null) {
            return null;
        }
        return this.client.get().createStatsContext();
    }

    /**
     * Save statistics to a CSV file.
     *
     * @param file
     *            File path
     * @throws IOException
     */
    public void saveStatistics(ClientStats stats, String file) throws IOException {
        this.client.get().writeSummaryCSV(stats, file);
    }

    void writeSummaryCSV(ClientStats stats, String path) throws IOException {
        if (this.client.get() == null) {
            throw new IOException("Client is unavailable for writing summary CSV.");
        }
        this.client.get().writeSummaryCSV(stats, path);
    }

    /**
     * Block the current thread until all queued stored procedure invocations have received
     * responses or there are no more connections to the cluster
     *
     * @throws InterruptedException
     * @throws IOException
     * @see Client#drain()
     */
    public void drain() throws InterruptedException, IOException {
        ClientImpl currentClient = this.getClient();
        if (currentClient == null) {
            throw new IOException("Client is unavailable for drain().");
        }
        currentClient.drain();
    }

    /**
     * Blocks the current thread until there is no more backpressure or there are no more
     * connections to the database
     *
     * @throws InterruptedException
     * @throws IOException
     */
    public void backpressureBarrier() throws InterruptedException, IOException {
        ClientImpl currentClient = this.getClient();
        if (currentClient == null) {
            throw new IOException("Client is unavailable for backpressureBarrier().");
        }
        currentClient.backpressureBarrier();
    }
}
