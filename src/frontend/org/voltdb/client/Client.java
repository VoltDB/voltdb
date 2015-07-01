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
import java.util.List;

import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

/**
 *  <p>
 *  A <code>Client</code> that connects to one or more nodes in a volt cluster
 *  and provides methods for invoking stored procedures and receiving
 *  responses.</p>
 *
 *  <p>Each client instance is backed by a single thread that is responsible for writing requests and reading responses
 *  from the network as well as invoking callbacks for stored procedures that are invoked asynchronously. There is
 *  an upper limit on the capacity of a single client instance and it may be necessary to use a pool of instances
 *  to get the best throughput and latency. If a heavyweight client instance is requested it will be backed by
 *  multiple threads, but under the current implementation it is better to us multiple single threaded instances</p>
 *
 *  <p>Because callbacks are invoked directly on the network thread the performance of the client is sensitive to the
 *  amount of work and blocking done in callbacks. If there is any question about whether callbacks will block
 *  or take a long time then an application should have callbacks hand off processing to an application controlled
 *  thread pool.
 *  </p>
 */
public interface Client {

    /**
     * Default non-admin port number for volt cluster instances.
     */
    public static final int VOLTDB_SERVER_PORT = 21212;

    /**
     * <p>Create a connection to a VoltDB node and add it to the set of connections.</p>
     *
     * <p>This is a synchronous operation.</p>
     *
     * @param host Hostname or IP address of the host to connect to including
     * optional port in the hostname:port format.
     * @throws UnknownHostException if the hostname can't be resolved.
     * @throws IOException if there is a Java network or connection problem.
     */
    public void createConnection(String host)
    throws UnknownHostException, IOException;

    /**
     * <p>Create a connection to a VoltDB node.</p>
     *
     * <p>This is a synchronous operation.</p>
     *
     * @param host Hostname or IP address of the host to connect to.
     * @param port Port number on remote host to connect to.
     * @throws UnknownHostException if the hostname can't be resolved.
     * @throws IOException if there is a Java network or connection problem.
     */
    public void createConnection(String host, int port)
    throws UnknownHostException, IOException;

    /**
     * <p>Synchronously invoke a procedure. Blocks until a result is available. A {@link ProcCallException}
     * is thrown if the response is anything other then success.</p>
     *
     * @param procName <code>class</code> name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return {@link ClientResponse} instance of procedure call results.
     * @throws ProcCallException on any VoltDB specific failure.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     * @throws IOException if there is a Java network or connection problem.
     */
    public ClientResponse callProcedure(String procName, Object... parameters)
    throws IOException, NoConnectionsException, ProcCallException;

    /**
     * <p>Asynchronously invoke a replicated procedure, by providing a callback that will be invoked by the single
     * thread backing the client instance when the procedure invocation receives a response.
     * See the {@link Client} class documentation for information on the negative performance impact of slow or
     * blocking callbacks. If there is backpressure
     * this call will block until the invocation is queued. If configureBlocking(false) is invoked
     * then it will return immediately. Check the return value to determine if queueing actually took place.</p>
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
     * <p>Asynchronously invoke a replicated procedure. If there is backpressure
     * this call will block until the invocation is queued. If configureBlocking(false) is invoked
     * then it will return immediately. Check the return value to determine if queuing actually took place.</p>
     *
     * <p>An opportunity is provided to hint what the size of the invocation
     * will be once serialized. This is used to perform more efficient memory allocation and serialization. The size
     * of an invocation can be calculated using {@link #calculateInvocationSerializedSize(String, Object...)}.
     * Only Clients that are resource constrained or expect to process hundreds of thousands of txns/sec will benefit
     * from accurately determining the serialized size of message.</p>
     *
     * @deprecated because hinting at the serialized size no longer has any effect.
     * @param callback ProcedureCallback that will be invoked with procedure results.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @param expectedSerializedSize A hint indicating the size the procedure invocation is expected to be
     *                               once serialized. Allocations are done in powers of two.
     * @return <code>true</code> if the procedure was queued and <code>false</code> otherwise.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     * @throws IOException if there is a Java network or connection problem.
     */
    @Deprecated
    public boolean callProcedure(
            ProcedureCallback callback,
            int expectedSerializedSize,
            String procName,
            Object... parameters)
    throws IOException, NoConnectionsException;

    /**
     * <p>Calculate the size of a stored procedure invocation once it is serialized. This is computationally intensive
     * as the invocation is serialized as part of the calculation.</p>
     *
     * @deprecated because hinting at the serialized size no longer has any effect.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return The size of the invocation once serialized.
     */
    @Deprecated
    public int calculateInvocationSerializedSize(String procName, Object... parameters);

    /**
     * <p>Synchronously invokes UpdateApplicationCatalog procedure. Blocks until a
     * result is available. A {@link ProcCallException} is thrown if the
     * response is anything other then success.</p>
     *
     * <p>This method is a convenience method that is equivalent to reading the catalog
     * file into a byte array in Java code, then calling {@link #callProcedure(String, Object...)}
     * with "@UpdateApplicationCatalog" as the procedure name, followed by they bytes of the catalog
     * and the string value of the deployment file.</p>
     *
     * @param catalogPath Path to the catalog jar file.
     * @param deploymentPath Path to the deployment file.
     * @return {@link ClientResponse} instance of procedure call results.
     * @throws IOException If the files cannot be serialized or if there is a Java network error.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     * @throws ProcCallException on any VoltDB specific failure.
     */
    public ClientResponse updateApplicationCatalog(File catalogPath, File deploymentPath)
    throws IOException, NoConnectionsException, ProcCallException;

    /**
     * <p>Asynchronously invokes UpdateApplicationCatalog procedure. Does not
     * guarantee that the invocation is actually queued. If there is
     * backpressure on all connections to the cluster then the invocation will
     * not be queued. Check the return value to determine if queuing actually
     * took place.</p>
     *
     * <p>This method is a convenience method that is equivalent to reading the catalog
     * file into a byte array in Java code, then calling
     * {@link #callProcedure(ProcedureCallback, String, Object...)} with
     * "@UpdateApplicationCatalog" as the procedure name, followed by they bytes of the catalog
     * and the string value of the deployment file.</p>
     *
     * @param callback ProcedureCallback that will be invoked with procedure results.
     * @param catalogPath Path to the catalog jar file.
     * @param deploymentPath Path to the deployment file.
     * @return <code>true</code> if the procedure was queued and <code>false</code> otherwise.
     * @throws IOException If the files cannot be serialized or if there is a Java network error.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     */
    public boolean updateApplicationCatalog(ProcedureCallback callback,
                                            File catalogPath,
                                            File deploymentPath)
    throws IOException, NoConnectionsException;

    /**
     * <p>Synchronously invokes UpdateClasses procedure. Blocks until a
     * result is available. A {@link ProcCallException} is thrown if the
     * response is anything other then success.</p>
     *
     * <p>This method is a convenience method that is equivalent to reading a jarfile containing
     * to be added/updated into a byte array in Java code, then calling
     * {@link #callProcedure(String, Object...)}
     * with "@UpdateClasses" as the procedure name, followed by the bytes of the jarfile
     * and a string containing a comma-separates list of classes to delete from the catalog.</p>
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
     * <p>Asynchronously invokes UpdateClasses procedure. Does not
     * guarantee that the invocation is actually queued. If there is
     * backpressure on all connections to the cluster then the invocation will
     * not be queued. Check the return value to determine if queuing actually
     * took place.</p>
     *
     * <p>This method is a convenience method that is equivalent to reading a jarfile containing
     * to be added/updated into a byte array in Java code, then calling
     * {@link #callProcedure(ProcedureCallback, String, Object...)}
     * with "@UpdateClasses" as the procedure name, followed by the bytes of the jarfile
     * and a string containing a comma-separates list of classes to delete from the catalog.</p>
     *
     * @param callback ProcedureCallback that will be invoked with procedure results.
     * @param jarPath Path to the jar file containing new/updated classes.  May be null.
     * @param classesToDelete comma-separated list of classes to delete.  May be null.
     * @return <code>true</code> if the procedure was queued and <code>false</code> otherwise.
     * @throws IOException If the files cannot be serialized or if there is a Java network error.
     * @throws NoConnectionsException if this {@link Client} instance is not connected to any servers.
     */
    public boolean updateClasses(ProcedureCallback callback,
                                 File jarPath,
                                 String classesToDelete)
    throws IOException, NoConnectionsException;

    /**
     * <p>Block the current thread until all queued stored procedure invocations have received responses
     * or there are no more connections to the cluster.</p>
     *
     * @throws NoConnectionsException never, this is deprecated behavior, declared only for backward compatibility.
     * @throws InterruptedException if this blocking call is interrupted.
     */
    public void drain() throws NoConnectionsException, InterruptedException;

    /**
     * <p>Shutdown this Client, closing all network connections and release all memory resources.
     * Failing to call this method before the Client is garbage collected can generate errors because
     * <code>finalization</code> is used to detect resource leaks. A client cannot be used once it has
     * been closed.</p>
     *
     * @throws InterruptedException if call is interrupted before it finishes.
     */
    public void close() throws InterruptedException;

    /**
     * <p>Blocks the current thread until there is no more backpressure or there are no more connections
     * to the database</p>
     *
     * @throws InterruptedException if this blocking call is interrupted.
     * @deprecated The non-blocking feature set is untested and has questionable utility. If it is something you need contact us.
     */
    @Deprecated
    public void backpressureBarrier() throws InterruptedException;

    /**
     * <p>Get a {@link ClientStatsContext} instance to fetch and process performance
     * statistics. Each instance is linked to this client, but provides a custom
     * view of statistics for a desired time period.</p>
     *
     * @see ClientStatsContext
     * @return Statistics context object linked to this client.
     */
    public ClientStatsContext createStatsContext();

    /**
     * <p>Get an identifier for the cluster that this client is currently connected to.
     * This will be null if the client has not been connected. Currently these values have
     * logical meaning, but they should just be interpreted as a unique per-cluster value.</p>
     *
     * @return An array of a Long and Integer containing the millisecond timestamp when the cluster was
     * started and the leader IP address (mapped as an unsigned int).
     */
    public Object[] getInstanceId();

    /**
     * <p>Retrieve the build string that was provided by the server at connection time.</p>
     *
     * @return Volt server build string.
     */
    public String getBuildString();

    /**
     * <p>The default behavior for queueing of asynchronous procedure invocations is to block until
     * it is possible to queue the invocation. If blocking is set to false callProcedure will always return
     * immediately if it is not possible to queue the procedure invocation due to backpressure.</p>
     *
     * @param blocking Whether you want procedure calls to block on backpressure.
     * @deprecated The non-blocking feature set is untested and has questionable utility. If it is something you need contact us.
     */
    @Deprecated
    public void configureBlocking(boolean blocking);

    /**
     * <p>Will {@link #callProcedure(ProcedureCallback, String, Object...)} will return
     * immediately if a an async procedure invocation could not be queued due to backpressure.</p>
     *
     * @return true if {@link #callProcedure(ProcedureCallback, String, Object...)} will
     * block until backpressure ceases and false otherwise.
     * @deprecated The non-blocking feature set is untested and has questionable utility. If it is something you need contact us.
     */
    @Deprecated
    public boolean blocking();

    /**
     * <p>Get the instantaneous values of the rate limiting values for this client.</p>
     *
     * @return A length-2 array of integers representing max throughput/sec and
     * max outstanding txns.
     */
    public int[] getThroughputAndOutstandingTxnLimits();

    /**
     * <p>Get the list of VoltDB server hosts that this client has open TCP connections
     * to. Note that this doesn't guarantee that those nodes are actually alive at
     * the precise moment this method is called. There is also a race condition
     * between calling this method and acting on the results. It is true that the list
     * won't grow unless createConnection is called, and the list will never contain
     * hosts that weren't explicitly connected to.</p>
     *
     * @return An list of {@link java.net.InetSocketAddress} representing the connected hosts.
     */
    public List<InetSocketAddress> getConnectedHostList();

    /**
     * <p>Write a single line of comma separated values to the file specified.
     * Used mainly for collecting results from benchmarks.</p>
     *
     * <p>The format of this output is subject to change between versions</p>
     *
     * <p>Format:
     * <ol>
     * <li>Timestamp (ms) of creation of the given {@link ClientStats} instance, stats.</li>
     * <li>Duration from first procedure call within the given {@link ClientStats} instance
     *    until this call in ms.</li>
     * <li>1-percentile round trip latency estimate in ms.</li>
     * <li>Max measure round trip latency in ms.</li>
     * <li>95-percentile round trip latency estimate in ms.</li>
     * <li>99-percentile round trip latency estimate in ms.</li>
     * <li>99.9-percentile round trip latency estimate in ms.</li>
     * <li>99.99-percentile round trip latency estimate in ms.</li>
     * <li>99.999-percentile round trip latency estimate in ms.</li>
     * </ol>
     *
     * @param stats {@link ClientStats} instance with relevant stats.
     * @param path Path to write to, passed to {@link FileWriter#FileWriter(String)}.
     * @throws IOException on any file write error.
     */
    public void writeSummaryCSV(ClientStats stats, String path) throws IOException;

    /**
     * <p>Creates a new instance of a VoltBulkLoader that is bound to this Client.
     * Multiple instances of a VoltBulkLoader created by a single Client will share some
     * resources, particularly if they are inserting into the same table.</p>
     *
     * @param tableName Name of table that bulk inserts are to be applied to.
     * @param maxBatchSize Batch size to collect for the table before pushing a bulk insert.
     * @param blfcb Callback procedure used for notification of failed inserts.
     * @return instance of VoltBulkLoader
     * @throws Exception if tableName can't be found in the catalog.
     */
    public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, BulkLoaderFailureCallBack blfcb) throws Exception;
}
