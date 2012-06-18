/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.jdbc;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides support for database connection pooling, allowing for optimal application performance.
 * From benchmarking results, optimal TCP socket usage is attained when 50 threads share the same
 * socket, sending execution requests through it. The pool incorporate the logic necessary to issue
 * newly created connections or pre-existing connections in use to client threads, as well as proper
 * management of those connections (releasing resources, etc.).
 *
 * @author Seb Coursol (originally in exampleutils)
 * @since 2.0
 */
public class JDBC4ClientConnectionPool {
    private static final ConcurrentHashMap<String, JDBC4PerfCounterMap> Statistics = new ConcurrentHashMap<String, JDBC4PerfCounterMap>();
    private static final HashMap<String, JDBC4ClientConnection> ClientConnections = new HashMap<String, JDBC4ClientConnection>();

    /**
     * No instantiation allowed.
     */
    private JDBC4ClientConnectionPool() {
    }

    /**
     * Gets a client connection to the given VoltDB server(s).
     *
     * @param servers
     *            the list of VoltDB servers to connect to.
     * @param port
     *            the VoltDB native protocol port to connect to (usually 21212).
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
     * @return the client connection object the caller should use to post requests.
     * @see #get(String servers, int port)
     * @see #get(String[] servers, int port)
     * @see #get(String servers, int port, String user, String password, boolean isHeavyWeight, int
     *      maxOutstandingTxns)
     */
    public static JDBC4ClientConnection get(String[] servers, String user,
            String password, boolean isHeavyWeight, int maxOutstandingTxns) throws Exception {
        String clientConnectionKeyBase = getClientConnectionKeyBase(servers, user, password,
                isHeavyWeight, maxOutstandingTxns);
        String clientConnectionKey = clientConnectionKeyBase;

        synchronized (ClientConnections) {
            if (!ClientConnections.containsKey(clientConnectionKey))
                ClientConnections.put(clientConnectionKey, new JDBC4ClientConnection(
                        clientConnectionKeyBase, clientConnectionKey, servers, user,
                        password, isHeavyWeight, maxOutstandingTxns));
            return ClientConnections.get(clientConnectionKey).use();
        }
    }

    /**
     * Releases a connection. This method (or connection.close() must be called by the user thread
     * once the connection is no longer needed to release it back into the pool where other threads
     * can pick it up. Failure to do so will cause a memory leak as more and more new connections
     * will be created, never to be released and reused. The pool itself will run the logic to
     * decide whether the actual underlying connection should be kept alive (if other threads are
     * using it), or closed for good (if the calling thread was the last user of that connection).
     *
     * @param connection
     *            the connection to release back into the pool.
     */
    public static void dispose(JDBC4ClientConnection connection) {
        synchronized (ClientConnections) {
            connection.dispose();
            if (connection.users == 0)
                ClientConnections.remove(connection.key);
        }
    }

    /**
     * Gets the global performance statistics for a connection. The statistics are pulled across the
     * entire pool for connections with the same parameters as the given connection.
     *
     * @param connection
     *            the connection from which to retrieve statistics.
     * @return the counter map aggregated across all the connections in the pool with the same
     *         parameters as the provided connection object.
     * @see #getStatistics(JDBC4ClientConnection connection)
     * @see #getStatistics(String servers, int port)
     * @see #getStatistics(String[] servers, int port)
     * @see #getStatistics(String servers, int port, String user, String password, boolean
     *      isHeavyWeight, int maxOutstandingTxns)
     * @see #getStatistics(String[] servers, int port, String user, String password, boolean
     *      isHeavyWeight, int maxOutstandingTxns)
     */
    public static JDBC4PerfCounterMap getStatistics(JDBC4ClientConnection connection) {
        return getStatistics(connection.keyBase);
    }

    /**
     * Gets the global performance statistics for a connection with the given parameters. The
     * statistics are pulled across the entire pool for connections with the same parameters as
     * provided.
     *
     * @param servers
     *            the list of VoltDB servers to connect to.
     * @param port
     *            the VoltDB native protocol port to connect to (usually 21212).
     * @return the counter map aggregated across all the connections in the pool with the same
     *         parameters as provided.
     * @see #getStatistics(JDBC4ClientConnection connection)
     * @see #getStatistics(String servers, int port)
     * @see #getStatistics(String servers, int port, String user, String password, boolean
     *      isHeavyWeight, int maxOutstandingTxns)
     * @see #getStatistics(String[] servers, int port, String user, String password, boolean
     *      isHeavyWeight, int maxOutstandingTxns)
     */
    public static JDBC4PerfCounterMap getStatistics(String[] servers) {
        return getStatistics(getClientConnectionKeyBase(servers, "", "", false, 0));
    }

    /**
     * Gets the global performance statistics for a connection with the given parameters. The
     * statistics are pulled across the entire pool for connections with the same parameters as
     * provided.
     *
     * @param servers
     *            the list of VoltDB servers to connect to.
     * @param port
     *            the VoltDB native protocol port to connect to (usually 21212).
     * @param user
     *            the user name to use when connecting to the server(s).
     * @param password
     *            the password to use when connecting to the server(s).
     * @param isHeavyWeight
     *            the flag indicating callback processes on this connection will be heavy (long
     *            running callbacks).
     * @param maxOutstandingTxns
     *            the number of transactions the client application may push against a specific
     *            connection before getting blocked on back-pressure.
     * @return the counter map aggregated across all the connections in the pool with the same
     *         parameters as provided.
     * @see #getStatistics(JDBC4ClientConnection connection)
     * @see #getStatistics(String servers, int port)
     * @see #getStatistics(String[] servers, int port)
     * @see #getStatistics(String servers, int port, String user, String password, boolean
     *      isHeavyWeight, int maxOutstandingTxns)
     */
    public static JDBC4PerfCounterMap getStatistics(String[] servers, String user,
            String password, boolean isHeavyWeight, int maxOutstandingTxns) {
        return getStatistics(getClientConnectionKeyBase(servers, user, password,
                isHeavyWeight, maxOutstandingTxns));
    }

    /**
     * Gets the global performance statistics for a connection with the given base hash/key. The
     * statistics are pulled across the entire pool for connections with the same parameters as
     * provided.
     *
     * @param clientConnectionKeyBase
     *            the base hash/key identifying the connections from which statistics will be
     *            pulled.
     * @return the counter map aggregated across all the connections in the pool with the same
     *         parameters as provided.
     */
    protected static JDBC4PerfCounterMap getStatistics(String clientConnectionKeyBase) {
        // Admited: could get a little race condition at the very beginning, but all that'll happen
        // is that we'll lose a handful of tracking event, a loss far outweighed by overall reduced
        // contention.
        if (!Statistics.containsKey(clientConnectionKeyBase))
            Statistics.put(clientConnectionKeyBase, new JDBC4PerfCounterMap());
        return Statistics.get(clientConnectionKeyBase);
    }

    /**
     * Generates a hash/key for a connection based on the given list of connection parameters
     *
     * @param servers
     *            the list of VoltDB servers to connect to in comma separated hostname[:port] format.
     * @param user
     *            the user name to use when connecting to the server(s).
     * @param password
     *            the password to use when connecting to the server(s).
     * @param isHeavyWeight
     *            the flag indicating callback processes on this connection will be heavy (long
     *            running callbacks).
     * @param maxOutstandingTxns
     *            the number of transactions the client application may push against a specific
     *            connection before getting blocked on back-pressure.
     * @return the base hash/key for the given connection parameter
     */
    private static String getClientConnectionKeyBase(String[] servers, String user,
            String password, boolean isHeavyWeight, int maxOutstandingTxns) {
        String clientConnectionKeyBase = user + ":" + password + "@";
        for (int i = 0; i < servers.length; i++)
            clientConnectionKeyBase += servers[i].trim() + ",";
        clientConnectionKeyBase += "{"
                + Boolean.toString(isHeavyWeight) + ":" + Integer.toString(maxOutstandingTxns)
                + "}";
        return clientConnectionKeyBase;
    }

}