/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.voltdb.VoltDB;

/**
 * Maintain a set of the last N recently used credentials and
 * their corresponding connections to the localhost VoltDB server.
 *
 * This is used for stateless auth to VoltDB, especially for the
 * HTTP/JSON interface.
 *
 * If the sane M users connect to a Volt server over and over,
 * and M <= N, then this should be as fast an auth as possible.
 *
 * This is probably not threadsafe yet.
 */
public class AuthenticatedConnectionCache {

    final String m_hostname;
    final int m_port;
    final int m_adminPort;
    final int m_targetSize; // goal size of the client cache

    /**
     * Metadata about a connection.
     */
    class Connection {
        int refCount;
        ClientImpl client;
        String user;
        byte[] hashedPassword;
        int passHash;
    }

    // The set of active connections.
    Map<String, Connection> m_connections = new TreeMap<String, Connection>();
    // The optional unauthenticated clients which should only work if auth is off
    ClientImpl m_unauthCient = null;

    public AuthenticatedConnectionCache(int targetSize) {
        this(targetSize, "localhost");
    }

    public AuthenticatedConnectionCache(int targetSize, String serverHostname) {
        this(targetSize, serverHostname, VoltDB.DEFAULT_PORT, 0);
    }

    public AuthenticatedConnectionCache(int targetSize, String serverHostname, int serverPort, int adminPort) {
        assert(serverHostname != null);
        assert(serverPort > 0);

        m_hostname = serverHostname;
        m_port = serverPort;
        m_adminPort = adminPort;
        m_targetSize = targetSize;
    }

    public synchronized Client getClient(String userName, byte[] hashedPassword, boolean admin) throws IOException {
        // ADMIN MODE
        if (admin) {
            ClientImpl adminClient = null;
            adminClient = (ClientImpl) ClientFactory.createClient();
            if ((userName == null) || (userName == "")) {
                if ((hashedPassword != null) && (hashedPassword.length > 0)) {
                    throw new IOException("Username was null but password was not.");
                }
                adminClient.createConnection(m_hostname, m_adminPort);
            }
            else {
                adminClient.createConnectionWithHashedCredentials(m_hostname, m_adminPort, userName, hashedPassword);
            }

            return adminClient;
        }

        // UN-AUTHENTICATED
        if ((userName == null) || (userName == "")) {
            if ((hashedPassword != null) && (hashedPassword.length > 0)) {
                throw new IOException("Username was null but password was not.");
            }
            try {
                if (m_unauthCient == null) {
                    m_unauthCient = (ClientImpl) ClientFactory.createClient();
                    m_unauthCient.createConnection(m_hostname, m_port);
                }
            }
            catch (IOException e) {
                m_unauthCient = null;
                throw e;
            }

            assert(m_unauthCient != null);
            return m_unauthCient;
        }

        // AUTHENTICATED
        int passHash = 0;
        if (hashedPassword != null)
            passHash = Arrays.hashCode(hashedPassword);

        Connection conn = m_connections.get(userName);
        if (conn != null) {
            if (conn.passHash != passHash) {
                throw new IOException("Incorrect authorization credentials.");
            }
            conn.refCount++;
        }
        else {
            conn = new Connection();
            conn.refCount = 1;
            conn.passHash = passHash;
            conn.hashedPassword = Arrays.copyOf(hashedPassword, hashedPassword.length);
            conn.user = userName;
            conn.client = (ClientImpl) ClientFactory.createClient();
            conn.client.createConnectionWithHashedCredentials(m_hostname, m_port, userName, hashedPassword);
            m_connections.put(userName, conn);
            attemptToShrinkPoolIfNeeded();
        }
        return conn.client;
    }

    /**
     * Dec-ref a client.
     * @param client The client to release.
     */
    public void releaseClient(Client client) {
        ClientImpl ci = (ClientImpl) client;

        // if no username, this is the unauth client
        if (ci.getUsername().length() == 0)
            return;

        Connection conn = m_connections.get(ci.getUsername());
        if (conn == null)
            throw new RuntimeException("Released client not in pool.");
        conn.refCount--;
        attemptToShrinkPoolIfNeeded();
    }

    public synchronized void closeAll()
    {
        if (m_unauthClient != null)
        {
            try {
                m_unauthClient.close();
            } catch (InterruptedException ex) {
                throw new RuntimeException("Unable to close unauthenticated client.", ex);
            }
        }
        for (Entry<String, Connection> e : m_connections.entrySet())
        {
            try {
                e.getValue().client.close();
            } catch (InterruptedException ex) {
                throw new RuntimeException("Unable to close client from pool.", ex);
            }
        }
    }

    /**
     * If the size of the pool > target size, see if any
     * connections can be closed and removed.
     */
    private void attemptToShrinkPoolIfNeeded() {
        while (m_connections.size() > m_targetSize) {
            for (Entry<String, Connection> e : m_connections.entrySet()) {
                if (e.getValue().refCount <= 0) {
                    m_connections.remove(e.getKey());
                    try {
                        e.getValue().client.close();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException("Unable to close client from pool.", ex);
                    }
                    break; // from the for to continue the while
                }
            }
            // if unable to remove anything, exit here
            return;
        }
    }

}
