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

import java.util.HashMap;

import org.voltcore.utils.ssl.SSLConfiguration;

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
     * @return the client connection object the caller should use to post requests.
     */
    public static JDBC4ClientConnection get(String[] servers, String user,
            String password, boolean isHeavyWeight, int maxOutstandingTxns, boolean reconnectOnConnectionLoss) throws Exception {
        return get(servers, user, password, isHeavyWeight, maxOutstandingTxns, reconnectOnConnectionLoss, null, null, false, -1);
    }

    /**
     * Gets a client connection to the given VoltDB server(s).
     *
     * @param servers
     *            the list of VoltDB servers to connect to.
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
     * @return the client connection object the caller should use to post requests.
     * @see #get(String[] servers, String user, String password, boolean isHeavyWeight, int
     *      maxOutstandingTxns, boolean reconnectOnConnectionLoss)
     */
    public static JDBC4ClientConnection get(String[] servers, String user, String password, boolean isHeavyWeight,
                                            int maxOutstandingTxns, boolean reconnectOnConnectionLoss,
                                            SSLConfiguration.SslConfig sslConfig, String kerberosConfig) throws Exception {
        return get(servers, user, password, isHeavyWeight, maxOutstandingTxns, reconnectOnConnectionLoss,
                   sslConfig, kerberosConfig, false, -1);
    }

    /**
     * Gets a client connection to the given VoltDB server(s).
     *
     * @param servers
     *            the list of VoltDB servers to connect to.
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
     * @return the client connection object the caller should use to post requests.
     * @see #get(String[] servers, String user, String password, boolean isHeavyWeight, int
     *      maxOutstandingTxns, boolean reconnectOnConnectionLoss)
     */
    public static JDBC4ClientConnection get(String[] servers, String user, String password, boolean isHeavyWeight,
                                            int maxOutstandingTxns, boolean reconnectOnConnectionLoss,
                                            SSLConfiguration.SslConfig sslConfig, String kerberosConfig,
                                            boolean topologyChangeAware, int priority) throws Exception {
        String clientConnectionKeyBase = getClientConnectionKeyBase(servers, user, password,
                isHeavyWeight, maxOutstandingTxns, reconnectOnConnectionLoss);
        String clientConnectionKey = clientConnectionKeyBase;

        synchronized (ClientConnections) {
            if (!ClientConnections.containsKey(clientConnectionKey))
                ClientConnections.put(clientConnectionKey, new JDBC4ClientConnection(
                        clientConnectionKeyBase, clientConnectionKey, servers, user,
                        password, isHeavyWeight, maxOutstandingTxns, reconnectOnConnectionLoss,
                        sslConfig, kerberosConfig, topologyChangeAware, priority));
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
     * @param reconnectOnConnectionLoss
     *            Attempts to reconnect to a node with retry after connection loss
     * @return the base hash/key for the given connection parameter
     */
    private static String getClientConnectionKeyBase(String[] servers, String user,
            String password, boolean isHeavyWeight, int maxOutstandingTxns, boolean reconnectOnConnectionLoss) {
        String clientConnectionKeyBase = user + ":" + password + "@";
        for (int i = 0; i < servers.length; i++)
            clientConnectionKeyBase += servers[i].trim() + ",";
        clientConnectionKeyBase += "{"
                + Boolean.toString(isHeavyWeight) + ":" + Integer.toString(maxOutstandingTxns)
                + ":" + Boolean.toString(reconnectOnConnectionLoss) + "}";
        return clientConnectionKeyBase;
    }

}
