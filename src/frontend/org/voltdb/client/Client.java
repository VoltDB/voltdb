/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.client;

import java.io.IOException;
import java.net.UnknownHostException;

import org.voltdb.VoltTable;

/**
 *  <p>
 *  A <code>Client</code> that connects to one or more nodes in a volt cluster
 *  and provides methods for invoking stored procedures and receiving
 *  responses. Client applications that are resource constrained (memory, CPU) or high performance
 *  (process hundreds of thousands of transactions per second) will want to pay attention to the hints that
 *  can be provided via {@link ClientFactory#createClient(int, int[])} and
 *  {@link #callProcedure(ProcedureCallback, int, String, Object...)}.
 *  Most Client applications will not need to generate enough load for these optimizations to matter.
 *  </p>
 *
 *  <p>
 *  A stored procedure invocation contains approximately 24 bytes of data per
 *  invocation plus the serialized sized of the procedure name and any parameters. The size of any specific procedure
 *  and parameter set can be calculated using
 *  {@link #calculateInvocationSerializedSize(String, Object...) calculateInvocationSerializedSize}. This method
 *  serializes the invocation and returns the serialized size and is computationally intensive and not suitable
 *  for use on a per invocation basis.
 *  </p>
 *
 *  <p>
 *  The expected serialized message size parameter provided to {@link ClientFactory#createClient(int, int[]) createClient}
 *  determines the default size of the initial memory allocation used when serializing stored procedure invocations.
 *  If the initial allocation is insufficient then the allocation is doubled necessitating an extra allocation and copy
 *  If the allocation is excessively small it may be doubled and copied several times when serializing large
 *  parameter sets.
 *  </p>
 *
 *  <p>
 *  {@link #callProcedure(ProcedureCallback, int, String, Object...)} allows the initial allocation to be hinted on a
 *  per procedure invocation basis. This is useful when an application invokes procedures where the size can vary
 *  significantly between invocations or procedures.
 *  </p>
 *
 *  <p>
 *  The <code>Client</code> performs aggressive memory pooling of {@link java.nio.DirectByteBuffer DirectByteBuffer's}. The pool
 *  contains arenas with buffers that are sized to the powers of 2 from 2^4 to 2^18. Java does not reliably
 *  garbage collect {@link java.nio.DirectByteBuffer DirectByteBuffers} so the pool never returns a buffer to the
 *  heap. The <code>maxArenaSizes</code> array passed to {@link ClientFactory#createClient(int, int[])} can be used
 *  to specify the maximum size each arena will be allowed to grow to. The client will continue to function even
 *  if the an arena is no longer able to grow. It will fall back to using slower
 *  {@link java.nio.HeapByteBuffer HeapByteBuffers} for serializing invocations.
 *  </p>
 */
public interface Client {

    // default port number for volt cluster instances.
    public static final int VOLTDB_SERVER_PORT = 21212;

     /**
     * Create a connection to another VoltDB node.
     * @param host hostname or IP address of the host to connect to
     * @param username Username to authorize. Username is ignored if authentication is disabled.
     * @param password Password to authenticate. Password is ignored if authentication is disabled.
     * @throws UnknownHostException
     * @throws IOException
     */
    public void createConnection(String host, String username, String password)
        throws UnknownHostException, IOException;

    /**
     * Synchronously invoke a procedure. Blocks until a result is available. A {@link ProcCallException}
     * is thrown if the response is anything other then success.
     * @param procName <code>class</code> name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return array of VoltTable results.
     * @throws org.voltdb.client.ProcCallException
     * @throws NoConnectionsException
     */
    public VoltTable[] callProcedure(String procName, Object... parameters)
        throws ProcCallException, NoConnectionsException;

    /**
     * Asynchronously invoke a procedure. Does not guarantee that the invocation is actually queued. If there
     * is backpressure on all connections to the cluster then the invocation will not be queued. Check the return value
     * to determine if queuing actually took place.
     * @param callback ProcedureCallback that will be invoked with procedure results.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return <code>true</code> if the procedure was queued and <code>false</code> otherwise
     */
    public boolean callProcedure(ProcedureCallback callback, String procName, Object... parameters)
    throws NoConnectionsException;

    /**
     * Asynchronously invoke a procedure. Does not guarantee that the invocation is actually queued. If there
     * is backpressure on all connections to the cluster then the invocation will not be queued. Check the return value
     * to determine if queuing actually took place. An opportunity is provided to hint what the size of the invocation
     * will be once serialized. This is used to perform more efficient memory allocation and serialization. The size
     * of an invocation can be calculated using {@link #calculateInvocationSerializedSize(String, Object...)}.
     * Only Clients that are resource constrained or expect to process hundreds of thousands of txns/sec will benefit
     * from accurately determining the serialized size of message.
     * @param callback ProcedureCallback that will be invoked with procedure results.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @param expectedSerializedSize A hint indicating the size the procedure invocation is expected to be
     *                               once serialized. Allocations are done in powers of two.
     * @return <code>true</code> if the procedure was queued and <code>false</code> otherwise
     */
    public boolean callProcedure(
            ProcedureCallback callback,
            int expectedSerializedSize,
            String procName,
             Object... parameters)
    throws NoConnectionsException;

    /**
     * Calculate the size of a stored procedure invocation once it is serialized. This is computationally intensive
     * as the invocation is serialized as part of the calculation.
     * @param procName class name (not qualified by package) of the procedure to execute.
     * @param parameters vararg list of procedure's parameter values.
     * @return The size of the invocation once serialized
     */
    public int calculateInvocationSerializedSize(String procName, Object... parameters);


    /**
     * Block the current thread until all queued stored procedure invocations have received responses
     * or there are no more connections to the cluster
     * @throws NoConnectionsException
     */
    public void drain() throws NoConnectionsException;

    /**
     * Shutdown the {@link Client} closing all network connections and release all memory resources.
     * Failing to call this method before the {@link Client} is garbage collected can generate errors because
     * <code>finalization</code> is used to detect resource leaks.
     * @throws InterruptedException
     */
    public void shutdown() throws InterruptedException;

    /**
     * Add to the list of listeners that will be notified of events
     * @param listener Listener to register
     */
    public void addClientStatusListener(ClientStatusListener listener);

    /**
     * Remove listener so that it will no longer be notified of events
     * @param listener Listener to unregister
     * @return <code>true</code> if the listener was removed and <code>false</code> if it wasn't registered
     */
    public boolean removeClientStatusListener(ClientStatusListener listener);

    /**
     * Blocks the current thread until there is no more backpressure or there are no more connections
     * to the database
     * @throws InterruptedException
     */
    public void backpressureBarrier() throws InterruptedException;
}
