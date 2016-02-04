/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import javax.security.auth.Subject;

import org.voltcore.logging.VoltLogger;

import com.google_voltpatches.common.base.Optional;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.collect.FluentIterable;

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

    private static VoltLogger logger = new VoltLogger("HOST");
    private final static String ADMIN_SUFFIX = ":++__ADMIN__++";

    final String m_hostname;
    final String m_adminHostName;
    final int m_port;
    final int m_adminPort;
    final int m_targetSize; // goal size of the client cache
    private boolean m_isClosing;

    /**
     * Metadata about a connection.
     */
    class Connection {
        int refCount;
        ClientImpl client;
        String user;
        byte[] hashedPassword;
        int passHash;
        ClientAuthScheme scheme;
        Subject subject;
    }

    /**
     * Provides a callback to be notified on node failure. Close
     * the connection if this callback is invoked.
     */
    class StatusListener extends ClientStatusListenerExt {
        Connection m_conn = null;

        StatusListener(Connection conn)
        {
            m_conn = conn;
        }

        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            logger.debug("Connection lost was reported for internal client.");
        }
    }


    // The set of active connections.
    Map<String, Connection> m_connections = new TreeMap<String, Connection>();
    // The optional unauthenticated clients which should only work if auth is off
    ClientImpl m_unauthClient = null;
    // The optional unauthenticated adming client which should work if auth if off
    ClientImpl m_adminUnauthClient = null;

    public AuthenticatedConnectionCache(int targetSize, String serverHostname, int serverPort, String adminHostName, int adminPort) {
        assert(serverHostname != null);
        assert(serverPort > 0);

        m_hostname = serverHostname;
        m_adminHostName = adminHostName;
        m_port = serverPort;
        m_adminPort = adminPort;
        m_targetSize = targetSize;
    }

    public class ClientWithHashScheme {
        public final Client m_client;
        public final ClientAuthScheme m_scheme;

        public ClientWithHashScheme(Client client, ClientAuthScheme scheme) {
            m_client = client;
            m_scheme = scheme;
        }
    }

    public synchronized ClientWithHashScheme getClient(Subject subject, boolean admin) throws IOException {
        if (subject == null) {
            return null;
        }
        Optional<DelegatePrincipal> opt = ConnectionUtil.getDelegate(subject);
        if (!opt.isPresent()) {
            throw new IOException("Subject " + subject + " does not contain suported principals");
        }
        DelegatePrincipal principal = opt.get();
        String userName = principal.getName();
        String userNameWithAdminSuffix = null;
        if (userName != null && !userName.trim().isEmpty()) {
            if (userName.endsWith(ADMIN_SUFFIX)) {
                throw new IOException("User name cannot end with " + ADMIN_SUFFIX);
            }
            userNameWithAdminSuffix = userName + ADMIN_SUFFIX;
        }

        final ClientAuthScheme scheme = ClientAuthScheme.SPNEGO;
        String ckey = (admin ? userNameWithAdminSuffix : userName) + scheme;
        Connection conn = m_connections.get(ckey);
        if (conn != null) {
            conn.refCount++;
        } else {
            conn = new Connection();
            conn.refCount = 1;
            conn.subject = subject;
            conn.user = userName;

            ClientConfig config = new ClientConfig(subject, new StatusListener(conn));

            conn.client = (ClientImpl) ClientFactory.createClient(config);
            conn.scheme = scheme;

            try {
                conn.client.createConnection(m_hostname, (admin ? m_adminPort : m_port));
            } catch (IOException e) {
                try {
                    conn.client.close();
                } catch (InterruptedException ex) {
                    throw new IOException(
                            "Unable to close rejected authenticated "
                          + (admin ? "admin " : "") + "client connection.", ex
                          );
                }
                conn = null;
                throw e;
            }
            m_connections.put(ckey, conn);
            attemptToShrinkPoolIfNeeded();
        }
        return new ClientWithHashScheme(conn.client, scheme);
    }

    public synchronized ClientWithHashScheme getClient(String userName, String password, byte[] hashedPassword, boolean admin) throws IOException {
        String userNameWithAdminSuffix = null;
        if (userName != null && !userName.trim().isEmpty()) {
            if (userName.endsWith(ADMIN_SUFFIX)) {
                throw new IOException("User name cannot end with " + ADMIN_SUFFIX);
            }
            userNameWithAdminSuffix = userName + ADMIN_SUFFIX;
        }
        // UN-AUTHENTICATED
        if ((userName == null) || userName.trim().isEmpty()) {
            if ((hashedPassword != null) && (hashedPassword.length > 0)) {
                throw new IOException("Username was null but password was not.");
            }
            if (m_unauthClient == null)
            {
                try {
                    m_unauthClient = (ClientImpl) ClientFactory.createClient();
                    m_unauthClient.createConnection(m_hostname, m_port);
                }
                catch (IOException e) {
                    try {
                        m_unauthClient.close();
                    } catch (InterruptedException ex) {
                        throw new IOException("Unable to close rejected unauthenticated client connection", ex);
                    }
                    m_unauthClient = null;
                    throw e;

                }
            }
            if (m_adminUnauthClient == null)
            {
                try {
                    m_adminUnauthClient = (ClientImpl) ClientFactory.createClient();
                    m_adminUnauthClient.createConnection(m_hostname, m_adminPort);
                }
                catch (IOException e) {
                    try {
                        m_adminUnauthClient.close();
                    } catch (InterruptedException ex) {
                        throw new IOException("Unable to close rejected unauthenticated admin client connection", ex);
                    }
                    m_adminUnauthClient = null;
                    throw e;

                }
            }

            assert(m_unauthClient != null);
            assert(m_adminUnauthClient != null);

            return new ClientWithHashScheme(admin ? m_adminUnauthClient : m_unauthClient,
                    ClientAuthScheme.HASH_SHA256);
        }

        // AUTHENTICATED
        int passHash = 0;
        if (hashedPassword != null) {
            passHash = Arrays.hashCode(hashedPassword);
        }

        ClientAuthScheme scheme = (hashedPassword == null ?
                ClientAuthScheme.HASH_SHA256 : ClientAuthScheme.getByUnencodedLength(hashedPassword.length));
        String ckey = (admin ? userNameWithAdminSuffix : userName) + scheme;
        Connection conn = m_connections.get(ckey);
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
            if (hashedPassword != null)
            {
                conn.hashedPassword = Arrays.copyOf(hashedPassword, hashedPassword.length);
            }
            else
            {
                conn.hashedPassword = null;
            }

            // Add a callback listener for this client, to detect if
            // a connection gets closed/disconnected.  If this happens,
            // we need to remove it from the m_conections cache.
            //detect hash scheme from length of hashed password if sent instead of password.
            ClientConfig config = new ClientConfig(userName, password, true, new StatusListener(conn), scheme);

            conn.user = userName;
            conn.client = (ClientImpl) ClientFactory.createClient(config);
            conn.scheme = scheme;
            try
            {
                conn.client.createConnectionWithHashedCredentials(
                        m_hostname,
                        (admin ? m_adminPort : m_port),
                        userName, hashedPassword
                        );
            }
            catch (IOException ioe)
            {
                try {
                    conn.client.close();
                } catch (InterruptedException ex) {
                    throw new IOException(
                            "Unable to close rejected authenticated "
                          + (admin ? "admin " : "") + "client connection.", ex
                          );
                }
                conn = null;
                throw ioe;
            }
            m_connections.put(ckey, conn);
            attemptToShrinkPoolIfNeeded();
        }
        return new ClientWithHashScheme(conn.client, scheme);
    }

    private Predicate<InetSocketAddress> onAdminPort = new Predicate<InetSocketAddress>() {
        @Override
        public boolean apply(InetSocketAddress input) {
            return input.getPort() == m_adminPort;
        }
    };

    /**
     * Dec-ref a client.
     * @param client The client to release.
     * @param force this is sent true in case we just lost network and so the connection I thought this will not happen
     * in internally connected clients but have seen it in strange/unknown/unreproducible cases.
     */
    public synchronized void releaseClient(Client client, ClientAuthScheme scheme, boolean force) {
        ClientImpl ci = (ClientImpl) client;
        if (force) {
            if (client == this.m_unauthClient) {
                closeClient(this.m_unauthClient);
                this.m_unauthClient = null;
                return;
            }
            if (client == this.m_adminUnauthClient) {
                closeClient(this.m_adminUnauthClient);
                this.m_adminUnauthClient = null;
                return;
            }
        }
        // if no username, this is the unauth client
        if (ci.getUsername().length() == 0) {
            return;
        }
        StringBuilder userNameBuilder = new StringBuilder(ci.getUsername());
        if (FluentIterable.from(ci.getConnectedHostList()).allMatch(onAdminPort)) {
            userNameBuilder.append(ADMIN_SUFFIX);
        }
        String ckey = userNameBuilder.toString() + scheme;
        Connection conn = m_connections.get(ckey);
        if (conn == null) { // already closed and cleaned up by closeAll. Nothing more to do.
            return;
        }
        if (force) {
            //Dont bother with target size of pool and remove dead connection.
            m_connections.remove(ckey);
            closeClient(conn.client);
        } else {
            conn.refCount--;
        }
        attemptToShrinkPoolIfNeeded();
    }

    private synchronized void closeClient(Client client)
    {
        if (client == null) return;

        try {
            client.drain();
        } catch (Exception ex) {
            //DONTCARE
        }
        try {
            client.close();
        } catch (Exception ex) {
            //DONTCARE
        }
    }

    public boolean isClosing() {
        return m_isClosing;
    }

    //Close all and just clear stuff.
    public synchronized void closeAll()
    {
        m_isClosing = true;
        if (m_unauthClient != null)
        {
            closeClient(m_unauthClient);
            m_unauthClient = null;
        }
        if (m_adminUnauthClient != null)
        {
            closeClient(m_adminUnauthClient);
            m_adminUnauthClient = null;
        }
        for (Entry<String, Connection> e : m_connections.entrySet())
        {
            closeClient(e.getValue().client);
        }
        m_connections.clear();
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
                    closeClient(e.getValue().client);
                    break; // from the for to continue the while
                }
            }
            // if unable to remove anything, exit here
            return;
        }
    }

    //Used for testing today.
    public int getSize() {
        if (m_connections == null) return 0;
        return m_connections.size();
    }
    public Client getUnauthenticatedAdminClient() {
        return this.m_adminUnauthClient;
    }
    public Client getUnauthenticatedClient() {
        return this.m_unauthClient;
    }
}
