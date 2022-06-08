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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.BulkLoaderSuccessCallback;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

/**
 * <code>Client2</code> provides the so-called "version 2" client API.
 * The overall intent is to provide an easier way to asynchronously
 * queue procedure calls.
 * <p>
 * The main public methods often come in two flavours: an Async variety
 * returns a <code>CompletableFuture</code> to represent the in-progress
 * request. A Sync version waits on that future before returning.
 * <p>
 * The Async/Sync pattern is not followed for methods that are inherently
 * synchronous (for example, changing some settings), or which must block
 * in order to usefully have the desired effect (for example, draining
 * all client requests).
 * <p>
 * Each client instance can have multiple connections to the VoltDB
 * cluster, one per cluster member. Each connection is backed by a
 * single thread that handles potentially-blocking operations, thus
 * avoiding blocking the caller's thread.
 * <p>
 * A <code>Client2</code> instance is created via a call to the
 * routine <code>ClientFactory.createClient()</code>, passing a
 * <code>Client2Config</code> object which carries configuration
 * values.
 *
 * @see ClientFactory
 * @see Client2Config
 * @see Client2CallOptions
 * @see Client2Notification
 */
public interface Client2 extends Closeable {

    /**
     * Set limits on the number of requests that can be pending
     * in the Client2 API at any one time.
     * <p>
     * Initially set from the client configuration, but may be adjusted
     * dynamically if the application wishes to tune the value.
     * <p>
     * There is a hard limit, after which requests are refused.
     * When the pending count reaches the warning level or greater,
     * the application is warned to slow down: backpressure starts.
     * When the pending count subsequently drops to the resume level
     * (or lower), the application is informed that it no longer needs
     * to slow down: backpressure ends.
     *
     * @param limit the desired hard limit on requests
     * @param warning the level at which backpressure starts
     * @param resume the level at which backpressure ends
     * @see Client2Config
     * @see Client2Notification.RequestBackpressure
     */
    void setRequestLimits(int limit, int warning, int resume);

    /**
     * Returns an estimate of the number of requests queued
     * (via callProcedure) but not yet completed. The count
     * is instantaneously valid, but of course is liable to
     * change immediately after being read.
     *
     * @return the current request count
     */
    int currentRequestCount();

    /**
     * Set limit on number of transactions that can be outstanding
     * at the VoltDB cluster from this client.
     * <p>
     * Initially set from the client configuration, but may be adjusted
     * dynamically if the application wishes to tune the value.
     * <p>
     * Attempting to reduce the limit below the current in-use
     * count will only reduce it by however many permits
     * are currently available, rather than blocking.
     *
     * @param limit the desired limit on requests
     * @return the actual new limit
     * @see Client2Config
     */
    int setOutstandingTxnLimit(int limit);

    /**
     * Returns an estimate of the number of outstanding
     * transactions. This is only useful for debugging,
     * and of course is liable to immediate change.
     *
     * @return the outstanding transaction count
     */
    int outstandingTxnCount();

    /**
     * Connect to first available server in a specified
     * list of servers, each in host:port form, and separated
     * by commas.
     * <p>
     * Host can be IPv6, IPv4, or hostname. If IPv6, it must be
     * enclosed in brackets. Port specification is optional.
     * <p>
     * This method connects to only one server. Other connections
     * may be made as a result of querying the VoltDB cluster
     * topology.
     * <p>
     * Completion is synchronous. If no connection could be set
     * up to any of the specified servers, a reattempt will be
     * scheduled after a specified delay, until a total timeout
     * has been exceeded. Use zero timeout for no retry.
     *
     * @param servers list of servers, each as host and optional port
     * @param timeout overall timeout
     * @param delay time between retries
     * @param unit units in which <code>timeout</code> and <code>delay</code> are expressed
     * @throws IOException server communication error
     */
    void connectSync(String servers, long timeout, long delay, TimeUnit unit)
        throws IOException;

    /**
     * Convenient form of {@link #connectSync(String,long,long,TimeUnit)}
     * that specifies no retry.
     *
     * @param servers list of servers, each as host and optional port
     * @throws IOException server communication error
     */
    void connectSync(String servers)
        throws IOException;

    /**
     * Connect to specified host on specified port.
     * <p>
     * Completion is synchronous. On a failure to connect, a reattempt
     * will be scheduled after a specified delay, until a total timeout
     * has been exceeded. Use zero timeout for no retry.
     *
     * @param host as address or hostname
     * @param port port number
     * @param timeout overall timeout
     * @param delay time between retries
     * @param unit units in which <code>timeout</code> and <code>delay</code> are expressed
     * @throws IOException server communication error
     */
    void connectSync(String host, int port, long timeout, long delay, TimeUnit unit)
        throws IOException;

    /**
     * Convenient form of {@link #connectSync(String,int,long,long,TimeUnit)}
     * that specifies no retry.
     *
     * @param host as address or hostname
     * @param port port number
     * @throws IOException server communication error
     */
    void connectSync(String host, int port)
        throws IOException;

    /**
     * Connect to first available server in a specified
     * list of servers, each in host:port form, and separated
     * by commas.
     * <p>
     * Host can be IPv6, IPv4, or hostname. If IPv6, it must be
     * enclosed in brackets. Port specification is optional.
     * <p>
     * This method connects to only one server. Other connections
     * may be made as a result of querying the VoltDB cluster
     * topology.
     * <p>
     * Completion is asynchronous. If no connection could be set
     * up to any of the specified servers, a reattempt will be
     * scheduled after a specified delay, until a total timeout
     * has been exceeded. Use zero timeout for no retry.
     *
     * @param servers list of servers, each as host and optional port
     * @param timeout overall timeout
     * @param delay time between retries
     * @param unit units in which <code>timeout</code> and <code>delay</code> are expressed
     * @return a {@code CompletableFuture}
     */
    CompletableFuture<Void> connectAsync(String servers, long timeout, long delay, TimeUnit unit);

    /**
     * Convenient form of {@link #connectAsync(String,long,long,TimeUnit)}
     * that specifies no retry.
     *
     * @param servers list of servers, each as host and optional port
     * @return a {@code CompletableFuture}
     */
     CompletableFuture<Void> connectAsync(String servers);

    /**
     * Connect to specified host on specified port.
     * <p>
     * Completion is asynchronous. On a failure to connect, a
     * reattempt will be scheduled after a specified delay, until
     * a total timeout has been exceeded. Use zero timeout for
     * no retry.
     *
     * @param host as address or hostname
     * @param port port number
     * @param timeout overall timeout
     * @param delay time between retries
     * @param unit units in which <code>timeout</code> and <code>delay</code> are expressed
     * @return a {@code CompletableFuture}
     */
    CompletableFuture<Void> connectAsync(String host, int port, long timeout, long delay, TimeUnit unit);

    /**
     * Convenient form of {@link #connectAsync(String,int,long,long,TimeUnit)}
     * that specifies no retry.
     *
     * @param host as address or hostname
     * @param port port number
     * @return a {@code CompletableFuture}
     */
    CompletableFuture<Void> connectAsync(String host, int port);

    /**
     * Gets a list of currently-connected hosts.
     * <p>
     * Address and port will be present; host name may
     * not be, depending on timing of reverse DNS lookup.
     *
     * @return list of <code>InetSocketAddress</code>
     */
    List<InetSocketAddress> connectedHosts();

    /**
     * Returns the 'build string' for the most-recently
     * connected VoltDB cluster.
     *
     * @return build string (null if never connected)
     */
    String clusterBuildString();

    /**
     * Returns the 'instance id' for the most-recently
     * connected VoltDB cluster.
     *
     * @return array: formation timestamp, leader address
     */
    Object[] clusterInstanceId();

    /**
     * Asynchronously call stored procedure. This call initiates a request
     * and returns immediately. The return value is a {@code CompletableFuture}
     * which can be used to retrieve the {@link ClientResponse} when the
     * request completes.
     *
     * @param procName procedure name
     * @param parameters as required by the procedure
     * @return a {@code CompletableFuture}
     * @see ClientResponse
     */
    CompletableFuture<ClientResponse> callProcedureAsync(String procName, Object... parameters);

    /**
     * Synchronously call stored procedure. This call initiates a request
     * and waits for completion. The return value is the {@link ClientResponse}.
     * A {@link ProcCallException} is raised if the response status was other
     * than success.
     *
     * @param procName procedure name
     * @param parameters as required by the procedure
     * @return the {@link ClientResponse}
     * @throws IOException server communication error
     * @throws ProcCallException the procedure call failed
     * @see ClientResponse
     * @see ProcCallException
     * @see NoConnectionsException
     */
    ClientResponse callProcedureSync(String procName, Object... parameters)
        throws IOException, ProcCallException;

    /**
     * Asynchronously call stored procedure with optional overrides
     * for selected values.
     *
     * @param options a {@link Client2CallOptions} object
     * @param procName procedure name
     * @param parameters as required by the procedure
     * @return a {@code CompletableFuture}
     * @see #callProcedureAsync(String,Object...)
     * @see Client2CallOptions
     * @see ClientResponse
     */
    CompletableFuture<ClientResponse> callProcedureAsync(Client2CallOptions options, String procName, Object... parameters);

    /**
     * Synchronously call stored procedure with optional overrides
     * for selected values.
     *
     * @param options a {@link Client2CallOptions} object
     * @param procName procedure name
     * @param parameters as required by the procedure
     * @return the {@link ClientResponse}
     * @throws IOException server communication error
     * @throws ProcCallException the procedure call failed
     * @see #callProcedureSync(String,Object...)
     * @see Client2CallOptions
     * @see ClientResponse
     * @see ProcCallException
     * @see NoConnectionsException
     */
    ClientResponse callProcedureSync(Client2CallOptions options, String procName, Object... parameters)
        throws IOException, ProcCallException;

    /**
     * Asynchronously call an all-partition stored procedure.
     * <p>
     * The method uses system procedure <code>@GetPartitionKeys</code> to determine target
     * partitions, and then execute the specified procedure on each partition, returning
     * an aggregated response. The call does not complete until all procedure instances
     * have completed.
     * <p>
     * The set of partition values is cached for a short while (around 1 second) to avoid
     * repeated requests to fetch it. It is possibly for it to be momentarily out of
     * sync. "Exactly once" execution of the procedure cannot be guaranteed.
     * <p>
     * Since each execution of the procedure on a separate partition can succeed or fail,
     * the application should check all response statuses.
     *
     * @param options a {@link Client2CallOptions} object (null if not needed)
     * @param procName procedure name
     * @param parameters as required by the procedure
     * @return a {@code CompletableFuture}
     * @see Client2CallOptions
     * @see ClientResponseWithPartitionKey
     */
    CompletableFuture<ClientResponseWithPartitionKey[]> callAllPartitionProcedureAsync(Client2CallOptions options, String procName, Object... parameters);

    /**
     * Synchronously call an all-partition stored procedure. The call blocks until
     * completion, and then returns the list of responses for all partitions.
     * <p>
     * See {@link #callAllPartitionProcedureAsync(Client2CallOptions, String, Object...)}
     * for more detail.
     * <p>
     * Unlike other synchronous procedure calls, <code>callAllPartitionProcedureSync</code>
     * does not throw a <code>ProcCallException</code> on a failure response. The application
     * should check all response statuses.
     *
     * @param options a {@link Client2CallOptions} object (null if not needed)
     * @param procName procedure name
     * @param parameters as required by the procedure
     * @return an array of client responses, one per partition ({@code ClientResponseWithPartitionKey})
     * @throws IOException server communication error
     * @see Client2CallOptions
     * @see ClientResponse
     */
    ClientResponseWithPartitionKey[] callAllPartitionProcedureSync(Client2CallOptions options, String procName, Object... parameters)
        throws IOException;

    /**
     * Drain all requests. This may block, and does not return
     * until there are no more requests pending in the client.
     *
     * @throws InterruptedException if the wait is interrupted
     */
    void drain()
        throws InterruptedException;

    /**
     * Shut down client. This may block, and does
     * not return until resources have been released.
     * <p>
     * May call {@link #drain} if application did not
     * already do so,
     */
    void close();

    /**
     * Statistics support: returns a statistics context associated
     * with this client.
     *
     * @return the {@link ClientStatsContext} created
     * @see ClientStatsContext
     */
    ClientStatsContext createStatsContext();

    /**
     * Creates a new instance of a {@link org.voltdb.client.VoltBulkLoader.VoltBulkLoader}
     * bound to this client. Multiple instances of a {@code VoltBulkLoader}
     * created by a single client will share some resources, particularly if
     * they are inserting into the same table.
     *
     * @param tableName table to which bulk inserts are to be applied
     * @param maxBatchSize size of a batch for bulk insert calls
     * @param upsertMode set to true for upsert instead of insert
     * @param failureCallback callback used for failure notification
     * @param successCallback callback on successful loads (null ok)
     * @return the {@code VoltBulkLoader} instance
     * @throws Exception if tableName can't be found in the catalog.
     */
    VoltBulkLoader newBulkLoader(String tableName, int maxBatchSize, boolean upsertMode,
                                 BulkLoaderFailureCallBack failureCallback,
                                 BulkLoaderSuccessCallback successCallback) throws Exception;

    /**
     * Wait until the VoltDB cluster topology has been determined, which
     * may take a few seconds after the initial connection. This is primarily
     * of internal interest to bulk loaders. It is inherently synchronous.
     *
     * @param timeout time limit on waiting
     * @param unit time unit for timeout
     * @return true if topology information available
     */
    boolean waitForTopology(long timeout, TimeUnit unit);
}
