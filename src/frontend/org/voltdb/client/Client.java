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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.BulkLoaderSuccessCallback;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

/**
 * A <code>Client</code> that connects to one or more nodes in a VoltDB cluster
 * and provides methods for invoking stored procedures and receiving responses.
 * <p>
 * Each client instance is normally backed by a single thread that is responsible
 * for writing requests to, and reading responses from the network, as well as
 * invoking callbacks for stored procedures that are invoked asynchronously.
 * <p>
 * There is an upper limit on the capacity of a single client instance, and it
 * may be necessary to use a pool of instances to get the best throughput and latency.
 * If a heavyweight client instance is requested it will be backed by  multiple threads,
 * but under the current implementation it is better to use multiple single threaded
 * instances.
 * <p>
 * Because callbacks are invoked directly on the network thread, the performance
 * of the client is sensitive to the amount of work and blocking done in callbacks.
 * If there is any question about whether callbacks will block or take a long time,
 * then an application should have callbacks hand off processing to a thread pool
 * controlled by the application.
 */
public interface Client {

    /**
     * Default non-admin port number for volt cluster instances.
     */
    public static final int VOLTDB_SERVER_PORT = 21212;

    /**
     * Create a connection to a VoltDB node, and add it to the set
     * of connections. This is a synchronous operation.
     *
     * @param host Hostname or IP address of the target host, including
     *             optional port in host:port format.
     * @throws UnknownHostException if the hostname can't be resolved.
     * @throws IOException if there is a Java network or connection problem.
     */
    public void createConnection(String host)
    throws UnknownHostException, IOException;

    /**
     * Create a connection to a VoltDB node, and add it to the set
     * of connections. This is a synchronous operation.
     *
     * @param host Hostname or IP address of the target host.
     * @param port Port number on target host to.
     * @throws UnknownHostException if the hostname can't be resolved.
     * @throws IOException if there is a Java network or connection problem.
     */
    public void createConnection(String host, int port)
    throws UnknownHostException, IOException;

    /**
     * Create a connection to the first available VoltDB node
     * from a specified list. Each entry in the list is
     * an address or hostname, optionally followed by a
     * port number, as for {@link #createConnection(String)}.
     * Entries are separated by commas.
     *
     * @param hostList comma-list of host specifications
     * @throws IOException if there is a Java network or connection problem.
     */
    public void createAnyConnection(String hostList)
    throws IOException;

    /**
     * Create a connection to the first available VoltDB node
     * from a specified list. Each entry in the list is
     * an address or hostname, optionally followed by a
     * port number, as for {@link #createConnection(String)}.
     * Entries are separated by commas.
     * <p>
     * If no connection can be made to any of the specified
     * hosts, then this method can retry connecting
     * after a specified delay and until a timeout has
     * expired. The timeout is only checked at the end
     * of each complete pass through the host list.
     * <p>
     * Not all errors are likely to be recoverable on retry.
     * Therefore, only <code>IOException</code>, not including
     * <code>UnknownHostException</code> will be retried.
     * If a particular host produces such an error, it will be
     * ignored on subsequent retries.
     * <p>
     * Connection progress may be monitored via a {@link ClientStatusListenerExt}.
     * The <code>connectionCreated</code> method will be invoked
     * with a status of either <code>UNABLE_TO_CONNECT</code> or
     * <code>SUCCESS</code>.
     *
     * @param hostList comma-list of host specifications
     * @param timeout approximate limit on retrying (millisecs)
     * @param delay wait time between retries (millisecs)
     * @throws IOException if there is a Java network or connection problem.
     */
    public void createAnyConnection(String hostList, long timeout, long delay)
    throws IOException;

    /**
     * Invoke a procedure. This is a synchronous call: it blocks until
     * a result is available.
     * <p>
     * A {@link ProcCallException} is thrown if the response is anything
     * other than success.
     *
     * @param procName <code>class</code> name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return {@link ClientResponse} instance of procedure call results.
     * @throws ProcCallException on any VoltDB-specific failure.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     * @throws IOException if there is a Java network or connection problem.
     */
    public ClientResponse callProcedure(String procName, Object... parameters)
    throws IOException, NoConnectionsException, ProcCallException;

    /**
     * Asynchronously invoke a procedure. This call will return when
     * the request has been queued, or if the request cannot be queued within
     * the configured timeout. Check the return value to determine if queueing
     * actually took place.
     * <p>
     * The caller provides a callback procedure that will be invoked when a
     * result is available. The callback is executed in a dedicated network thread.
     * See the {@link Client} class documentation for information on the negative
     * performance impact of slow or blocking callbacks.
     * <p>
     * If there is backpressure this call can in some circumstances block until
     * the invocation  is queued. This can be avoided by using <code>setNonblockingAsync</code>
     * in the client configuration, in which case callProcedure will return <code>false</code>
     * immediately.
     *
     * @param callback {@link ProcedureCallback} that will be invoked with procedure results.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return <code>true</code> if the procedure was queued and <code>false</code> otherwise.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     * @throws IOException if there is a Java network or connection problem.
     */
    public boolean callProcedure(ProcedureCallback callback, String procName, Object... parameters)
    throws IOException, NoConnectionsException;

    /**
     * Invoke a procedure with specified query timeout. This is a synchronous call:
     * it blocks until a result is available.
     * <p>
     * The specified query timeout applies to a read-only query or batch of read-only
     * queries, and may override the global <code>querytimeout</code> value in the
     * VoltDB cluster's configuration file. Only callers with admin privilege are
     * permitted to use a timeout longer than the global setting.
     * <p>
     * A query timeout of zero means there is no timeout applied to the query
     * or batch of queries.
     * <p>
     * For more details, refer to {@link #callProcedure(String, Object...)}.
     *
     * @param queryTimeout timeout (in milliseconds) for read-only queries or batches of queries.
     * @param procName <code>class</code> name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return {@link ClientResponse} instance of procedure call results.
     * @throws ProcCallException on any VoltDB-specific failure.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     * @throws IOException if there is a Java network or connection problem.
     */
    public ClientResponse callProcedureWithTimeout(int queryTimeout, String procName, Object... parameters)
    throws IOException, NoConnectionsException, ProcCallException;

    /**
     * Asynchronously invoke a procedure with specified query timeout. This call
     * will return when the request has been queued, or if the request cannot be
     * queued within the configured timeout. Check the return value to determine
     * if queueing actually took place.
     * <p>
     * The specified query timeout applies to a read-only query or batch of read-only
     * queries, and may override the global <code>querytimeout</code> value in the
     * VoltDB cluster's configuration file. Only callers with admin privilege are
     * permitted to use a timeout longer than the global setting.
     * <p>
     * A query timeout of zero means there is no timeout applied to the query
     * or batch of queries.
     * <p>
     * For more details, refer to {@link #callProcedure(ProcedureCallback, String, Object...)}.
     *
     * @param callback {@link ProcedureCallback} that will be invoked with procedure results.
     * @param queryTimeout timeout (in milliseconds) for read-only queries or batches of queries.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return <code>true</code> if the procedure was queued and <code>false</code> otherwise.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     * @throws IOException if there is a Java network or connection problem.
     */
    public boolean callProcedureWithTimeout(ProcedureCallback callback, int queryTimeout, String procName, Object... parameters)
    throws IOException, NoConnectionsException;

    /**
     * Synchronously invoke a procedure call, blocking until a result is available,
     * with caller-specified client timeout and query timeout.
     * <p>
     * The client timeout overrides the default set up by {@link ClientConfig#setProcedureCallTimeout}.
     * <p>
     * See {@link #callProcedureWithTimeout(int, String, Object...)} for details
     * of the query timeout.
     *
     * @param queryTimeout timeout (in milliseconds) for read-only queries or batches of queries
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param clientTimeout timeout for the procedure
     * @param unit TimeUnit of procedure timeout
     * @param parameters vararg list of procedure's parameter values.
     * @return ClientResponse for execution.
     * @throws ProcCallException on any VoltDB-specific failure.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     * @throws IOException if there is a Java network or connection problem.
     */
    public ClientResponse callProcedureWithClientTimeout(int queryTimeout,
                                                         String procName,
                                                         long clientTimeout,
                                                         TimeUnit unit,
                                                         Object... parameters)
    throws IOException, NoConnectionsException, ProcCallException;

    /**
     * Asynchronously invoke a procedure call with specified client and query timeouts.
     * <p>
     * The client timeout overrides the default set up by {@link ClientConfig#setProcedureCallTimeout}.
     * <p>
     * See {@link #callProcedureWithTimeout(ProcedureCallback, int, String, Object...)} for details
     * of the query timeout.
     *
     * @param callback TransactionCallback that will be invoked with procedure results.
     * @param queryTimeout timeout (in milliseconds) for read-only queries or batches of queries
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param clientTimeout query timeout
     * @param clientTimeoutUnit units for query timeout
     * @param parameters vararg list of procedure's parameter values.
     * @return True if the procedure was queued and false otherwise
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     * @throws IOException if there is a Java network or connection problem.
     */
    public boolean callProcedureWithClientTimeout(ProcedureCallback callback,
                                                  int queryTimeout,
                                                  String procName,
                                                  long clientTimeout,
                                                  TimeUnit clientTimeoutUnit,
                                                  Object... parameters)
    throws IOException, NoConnectionsException;

    /**
     * Synchronously updates class definitions in the VoltDB database.
     * Blocks until a result is available. A {@link ProcCallException}
     * is thrown if the response is anything other than success.
     * <p>
     * This method is a convenience method that calls through to
     * {@link UpdateClasses#update(Client,File,String)}
     *
     * @param jarPath Path to the jar file containing new/updated classes.
     * @param classesToDelete comma-separated list of classes to delete.
     * @return {@link ClientResponse} instance of procedure call results.
     * @throws IOException If the files cannot be serialized or if there is a Java network error.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     * @throws ProcCallException on any VoltDB specific failure.
     */
    public ClientResponse updateClasses(File jarPath, String classesToDelete)
    throws IOException, NoConnectionsException, ProcCallException;

    /**
     * Asynchronously updates class definitions in the VoltDB database.
     * Does not guarantee that the invocation was actually queued: check
     * the return value to determine if queuing actually took place.
     * <p>
     * This method is a convenience method that calls through to
     * {@link UpdateClasses#update(Client,ProcedureCallback,File,String)}
     *
     * @param callback ProcedureCallback that will be invoked with procedure results.
     * @param jarPath Path to the jar file containing new/updated classes.  May be null.
     * @param classesToDelete comma-separated list of classes to delete.  May be null.
     * @return <code>true</code> if the procedure was queued and <code>false</code> otherwise.
     * @throws IOException If the files cannot be serialized or if there is a Java network error.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     */
    public boolean updateClasses(ProcedureCallback callback, File jarPath, String classesToDelete)
    throws IOException, NoConnectionsException;

    /**
     * Block the current thread until all queued stored procedure invocations
     * have received responses, or there are no more connections to the cluster.
     *
     * @throws NoConnectionsException never; declared only for backward compatibility.
     * @throws InterruptedException if this blocking call is interrupted.
     */
    public void drain() throws NoConnectionsException, InterruptedException;

    /**
     * Shut down this Client, closing all network connections and releasing
     * all memory resources. A client cannot be used once it has
     * been closed.
     * <p>
     * You should call this before the Ciient is garbage-collected. Failure
     * to do so can generate errors, as <code>finalization</code> is used to
     * detect resource leaks.
     *
     * @throws InterruptedException if call is interrupted before it finishes.
     */
    public void close() throws InterruptedException;

    /**
     * Blocks the current thread until there is no more backpressure,
     * or there are no more connections to the database
     * <p>
     * This method may be used to block execution after one of the async
     * <code>callProceedure</code> methods has returned <code>false</code>,
     * indicating that the procedure could not be queued.
     *
     * @throws InterruptedException if this blocking call is interrupted.
     */
    public void backpressureBarrier() throws InterruptedException;

    /**
     * Get a {@link ClientStatsContext} instance to fetch and process performance
     * statistics. Each instance is linked to this client, but provides a custom
     * view of statistics for a desired time period.
     *
     * @see ClientStatsContext
     * @return Statistics context object linked to this client.
     */
    public ClientStatsContext createStatsContext();

    /**
     * Get an identifier for the cluster to which this client is currently connected.
     * This will be null if the client has not been connected. Currently these values have
     * logical meaning, but they should just be interpreted as a unique per-cluster value.
     *
     * @return An array of a Long and Integer containing the millisecond timestamp when the cluster was
     * started and the leader IP address (mapped as an unsigned int).
     */
    public Object[] getInstanceId();

    /**
     * Retrieve the build string that was provided by the server at connection time.
     *
     * @return Volt server build string.
     */
    public String getBuildString();

    /**
     * Get the instantaneous values of the rate-limiting values for this client.
     *
     * @return An array of two integers, representing max throughput/sec and
     * max outstanding txns.
     */
    public int[] getThroughputAndOutstandingTxnLimits();

    /**
     * Get the list of VoltDB server hosts to which this client has open TCP
     * connections. Note that this doesn't guarantee that those nodes are actually
     * alive at the precise moment this method is called. There is also a race condition
     * between calling this method and acting on the results.
     *
     * @return A list of {@link java.net.InetSocketAddress} representing the connected hosts.
     */
    public List<InetSocketAddress> getConnectedHostList();

    /**
     * Tell whether Client has turned on the auto-reconnect feature. If it is on,
     * Client will pause instead of stop when all connections to the server are lost,
     * and will resume after connections are restored.
     * <p>
     * This will always return true in topology-change-aware clients.
     *
     * @return true if the client wants to use auto-reconnect feature.
     * @see #isTopologyChangeAwareEnabled()
     */
    public boolean isAutoReconnectEnabled();

    /**
     * Tell whether Client has turned on the topologyChangeAware feature. If it is on,
     * Client attempts to connect to all nodes in the cluster as they are discovered,
     * and will automatically try to reconnect failed connections.
     *
     * @return true if the client wants to use topologyChangeAware feature.
     * @see #isAutoReconnectEnabled()
     */
    public boolean isTopologyChangeAwareEnabled();

    /**
     * Append a single line of comma-separated values to the file specified.
     * Used mainly for collecting results from benchmarks.
     * <p>
     * This is a convenience method that calls through to
     * {@link ClientStatsUtil#writeSummaryCSV(String,ClientStats,String)},
     * which you should see for details of the output format.
     *
     * @param statsRowName give the client stats row an identifiable name
     * @param stats {@link ClientStats} instance with relevant stats
     * @param path path of CSV file
     * @throws IOException on any file write error
     */
    public void writeSummaryCSV(String statsRowName, ClientStats stats, String path) throws IOException;

    /**
     * Append a single line of comma separated values to the file specified.
     * Used mainly for collecting results from benchmarks.
     *<p>
     * This is a convenience method that calls through to
     * {@link ClientStatsUtil#writeSummaryCSV(ClientStats,String)},
     * which you should see for details of the output format.
     *
     * @param stats {@link ClientStats} instance with relevant stats
     * @param path path of CSV file
     * @throws IOException on any file write error
     */
    public void writeSummaryCSV(ClientStats stats, String path) throws IOException;

    /**
     * Creates a new instance of a VoltBulkLoader that is bound to this Client.
     * Multiple instances of a VoltBulkLoader created by a single Client will share some
     * resources, particularly if they are inserting into the same table.
     *
     * @param tableName Name of table that bulk inserts are to be applied to.
     * @param maxBatchSize Batch size to collect for the table before pushing a bulk insert.
     * @param upsert set to true if want upsert instead of insert
     * @param failureCallback Callback procedure used for notification any failures.
     * @return instance of VoltBulkLoader
     * @throws Exception if tableName can't be found in the catalog.
     */
    public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, boolean upsert, BulkLoaderFailureCallBack failureCallback) throws Exception;
    public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, BulkLoaderFailureCallBack failureCallback) throws Exception;

    /**
     * Creates a new instance of a VoltBulkLoader that is bound to this Client.
     * Multiple instances of a VoltBulkLoader created by a single Client will share some
     * resources, particularly if they are inserting into the same table.
     *
     * @param tableName Name of table that bulk inserts are to be applied to.
     * @param maxBatchSize Batch size to collect for the table before pushing a bulk insert.
     * @param upsertMode set to true if want upsert instead of insert
     * @param failureCallback Callback procedure used for notification any failures.
     * @param successCallback Callback for notifications on successful load operations.
     * @return instance of VoltBulkLoader
     * @throws Exception if tableName can't be found in the catalog.
     */
    public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, boolean upsertMode, BulkLoaderFailureCallBack failureCallback, BulkLoaderSuccessCallback successCallback) throws Exception;

    /**
     * Wait until the VoltDB cluster topology has been determined, which
     * may take a few seconds after the initial connection. This is primarily
     * of internal interest to bulk loaders.
     *
     * @param timeout timeout in milliseconds
     * @return true if client has determined cluster topology.
     */
    public boolean waitForTopology(long timeout);

    /**
     * Synchronously execute a stored procedure on a set of partitions, one partition at a time.
     * <p>
     * The method uses system procedure <code>@GetPartitionKeys</code> to get a set of partition values, and
     * then execute the stored procedure one partition at a time, returning an aggregated response. It blocks
     * until results are available.
     * <p>
     * The set of partition values is cached to avoid repeated requests to fetch them. The cached set will
     * be updated when database cluster topology is updated, but it is possibly for it to be briefly out
     * of sync. Exactly once per partition cannot be guaranteed.
     * <p>
     * There may be undesirable impact on latency and throughput as a result of running a multi-partition procedure.
     * This is particularly true for longer running procedures. Using multiple, smaller procedures can also help
     * reducing latency and increasing throughput, for queries that modify large volumes of data, such as large deletes.
     * For example, multiple smaller single partition procedures are particularly useful to age out large stale
     * data where strong global consistency is not required.
     * <p>
     * When creating a single-partitioned procedure, you can use the <code>PARAMETER</code> clause to specify
     * the partitioning parameter which is used to determine the target partition. <code>PARAMETER</code> should
     * not be specified in the stored procedure used in this call, since the stored procedure will be executed on
     * every partition. If you only want to execute the procedure in the partition designated by <code>PARAMETER</code>,
     * use {@link #callProcedure(String, Object...)} instead.
     * <p>
     * When creating a stored procedure class, the first argument in the procedure's run method must be the
     * partition key, which matches the partition column type, followed by the parameters as declared in the
     * procedure. The argument partition key, not part of procedure's parameters,  is assigned during the
     * iteration of the partition set.
     * <p>
     * Example: A stored procedure with a parameter of long type and partition column of string type
     *<pre>
     *   CREATE TABLE tableWithStringPartition (id bigint NOT NULL,value_string varchar(50) NOT NULL,
     *                                          value1 bigint NOT NULL,value2 bigint NOT NULL);
     *   PARTITION TABLE tableWithStringPartition ON COLUMN value_string;
     *   CREATE PROCEDURE FROM CLASS example.Everywhere;
     *   PARTITION PROCEDURE Everywhere ON TABLE tableWithStringPartition COLUMN value_string;
     * </pre><pre>
     *    public class Everywhere extends VoltProcedure {
     *         public final SQLStmt stmt = new SQLStmt("SELECT count(*) FROM tableWithStringPartition where value1 &gt; ?;");
     *         public VoltTable[] run(String partitionKey, long value1) {
     *              voltQueueSQL(stmt, value1);
     *              return voltExecuteSQL(true);
     *         }
     *    }
     * </pre>
     * <p>
     * The execution of the stored procedure may fail on one or more partitions. Thus check the status of the response on every partition.
     *
     * @param procedureName <code>class</code> name (not qualified by package) of the partitioned java procedure to execute.
     * @param params  vararg list of procedure's parameter values.
     * @return {@link ClientResponseWithPartitionKey} instances of procedure call results.
     * @throws ProcCallException on any VoltDB specific failure.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     * @throws IOException if there is a Java network or connection problem.
     */
    public ClientResponseWithPartitionKey[] callAllPartitionProcedure(String procedureName, Object... params)
            throws IOException, NoConnectionsException, ProcCallException;

    /**
     * Asynchronously execute a stored procedure on a set of partitions, one partition at a time.
     * <p>
     * See the synchronous form, {@link #callAllPartitionProcedure(String, Object...)}, for more details.
     *
     * @param callback {@link AllPartitionProcedureCallback} that will be invoked with procedure results.
     * @param procedureName class name (not qualified by package) of the partitioned java procedure to execute.
     * @param params  vararg list of procedure's parameter values.
     * @return <code>false</code> if the procedures on all partition are not queued and <code>true</code> otherwise.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     * @throws IOException if there is a Java network or connection problem.
     * @throws ProcCallException on any VoltDB specific failure.
     */
    public boolean callAllPartitionProcedure(AllPartitionProcedureCallback callback, String procedureName, Object... params)
            throws IOException, NoConnectionsException, ProcCallException;
}
