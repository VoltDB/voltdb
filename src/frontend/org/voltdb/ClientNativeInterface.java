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

package org.voltdb;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;
import org.voltcore.network.InputHandler;
import org.voltcore.network.QueueMonitor;
import org.voltcore.network.ReverseDNSPolicy;
import org.voltcore.network.VoltNetworkPool;
import org.voltcore.network.VoltPort;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.Pair;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.AuthSystem.AuthProvider;
import org.voltdb.CatalogContext.ProcedurePartitionInfo;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ProcedureInvocationType;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.security.AuthenticationRequest;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.base.Charsets;

/**
 * Represents VoltDB's connection to client libraries outside the cluster.
 * This class accepts new connections and manages existing connections through
 * <code>ClientConnection</code> instances.
 *
 */
public class ClientNativeInterface {

    private static final VoltLogger log = new VoltLogger(ClientInterface.class.getName());
    private static final VoltLogger authLog = new VoltLogger("AUTH");
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger networkLog = new VoltLogger("NETWORK");

    private final ClientAcceptor m_acceptor;
    private ClientAcceptor m_adminAcceptor;

    /**
     * Counter of the number of client connections. Used to enforce a limit on the maximum number of connections
     */
    private final AtomicInteger m_numConnections = new AtomicInteger(0);

    /**
     * ZooKeeper is used for @Promote to trigger a truncation snapshot.
     */
    ZooKeeper m_zk;

    /**
     * The CIHM is unique to the connection and the ACG is shared by all connections
     * serviced by the associated network thread. They are paired so as to only do a single
     * lookup.
     */
    private final ConcurrentHashMap<Long, ClientInterfaceHandleManager> m_cihm =
            new ConcurrentHashMap<Long, ClientInterfaceHandleManager>(2048, .75f, 128);

    // MAX_CONNECTIONS is updated to be (FD LIMIT - 300) after startup
    private final AtomicInteger MAX_CONNECTIONS = new AtomicInteger(800);
    private ScheduledFuture<?> m_maxConnectionUpdater;

    /** A port that accepts client connections */
    public class ClientAcceptor implements Runnable {
        private final int m_port;
        private final ServerSocketChannel m_serverSocket;
        private final VoltNetworkPool m_network;
        private volatile boolean m_running = true;
        private Thread m_thread = null;
        private final boolean m_isAdmin;
        private final InetAddress m_interface;

        /**
         * Used a cached thread pool to accept new connections.
         */
        private final ExecutorService m_executor = CoreUtils.getBoundedThreadPoolExecutor(128, 10L, TimeUnit.SECONDS,
                        CoreUtils.getThreadFactory("Client authentication threads", "Client authenticator"));

        ClientAcceptor(InetAddress intf, int port, VoltNetworkPool network, boolean isAdmin)
        {
            m_interface = intf;
            m_network = network;
            m_port = port;
            m_isAdmin = isAdmin;
            ServerSocketChannel socket;
            try {
                socket = ServerSocketChannel.open();
            } catch (IOException e) {
                if (m_isAdmin) {
                    hostLog.fatal("Failed to open admin wire protocol listener on port "
                            + m_port + "(" + e.getMessage() + ")");
                }
                else {
                    hostLog.fatal("Failed to open native wire protocol listener on port "
                            + m_port + "(" + e.getMessage() + ")");
                }
                throw new RuntimeException(e);
            }
            m_serverSocket = socket;
        }

        public void start() throws IOException {
            if (m_thread != null) {
                throw new IllegalStateException("A thread for this ClientAcceptor is already running");
            }
            if (!m_serverSocket.socket().isBound()) {
                try {
                    if (m_interface != null) {
                        m_serverSocket.socket().bind(new InetSocketAddress(m_interface, m_port));
                    } else {
                        m_serverSocket.socket().bind(new InetSocketAddress(m_port));
                    }
                }
                catch (IOException e) {
                    String msg = "Client interface failed to bind to"
                            + (m_isAdmin ? " Admin " : " ") + "port: " + m_port;
                    MiscUtils.printPortsInUse(hostLog);
                    VoltDB.crashLocalVoltDB(msg, false, e);
                }
            }
            m_running = true;
            String threadName = m_isAdmin ? "AdminPort connection acceptor" : "ClientPort connection acceptor";
            m_thread = new Thread( null, this, threadName, 262144);
            m_thread.setDaemon(true);
            m_thread.start();
        }

        public void shutdown() throws InterruptedException {
            //sync prevents interruption while shuttown down executor
            if (m_thread != null) {
                synchronized (this) {
                    m_running = false;
                    m_thread.interrupt();
                }
                m_thread.join();
            }
        }

        //Thread for Running authentication of client.
        class AuthRunnable implements Runnable {
            final SocketChannel m_socket;

            AuthRunnable(SocketChannel socket) {
                this.m_socket = socket;
            }

            @Override
            public void run() {
                if (m_socket != null) {
                    boolean success = false;
                    //Populated on timeout
                    AtomicReference<String> timeoutRef = new AtomicReference<String>();
                    try {
                        final InputHandler handler = authenticate(m_socket, timeoutRef);
                        if (handler != null) {
                            m_socket.configureBlocking(false);
                            if (handler instanceof ClientInputHandler) {
                                m_socket.socket().setTcpNoDelay(true);
                            }
                            m_socket.socket().setKeepAlive(true);

                            if (handler instanceof ClientInputHandler) {
                                m_network.registerChannel(
                                                m_socket,
                                                handler,
                                                0,
                                                ReverseDNSPolicy.ASYNCHRONOUS);
                                /*
                                 * If IV2 is enabled the logic initially enabling read is
                                 * in the started method of the InputHandler
                                 */
                            } else {
                                m_network.registerChannel(
                                        m_socket,
                                        handler,
                                        SelectionKey.OP_READ,
                                        ReverseDNSPolicy.ASYNCHRONOUS);
                            }
                            success = true;
                        }
                    } catch (Exception e) {
                        try {
                            m_socket.close();
                        } catch (IOException e1) {
                            //Don't care connection is already lost anyways
                        }
                        if (m_running) {
                            if (timeoutRef.get() != null) {
                                hostLog.warn(timeoutRef.get());
                            } else {
                                hostLog.warn("Exception authenticating and "
                                        + "registering user in ClientAcceptor", e);
                            }
                        }
                    } finally {
                        if (!success) {
                            m_numConnections.decrementAndGet();
                        }
                    }
                }
            }
        }

        @Override
        public void run() {
            try {
                do {
                    final SocketChannel socket;
                    try
                    {
                        socket = m_serverSocket.accept();
                    }
                    catch (IOException ioe)
                    {
                        if (ioe.getMessage() != null &&
                            ioe.getMessage().contains("Too many open files"))
                        {
                            networkLog.warn("Rejected accepting new connection due to too many open files");
                            continue;
                        }
                        else
                        {
                            throw ioe;
                        }
                    }

                    /*
                     * Enforce a limit on the maximum number of connections
                     */
                    if (m_numConnections.get() >= MAX_CONNECTIONS.get()) {
                        networkLog.warn("Rejected connection from " +
                                socket.socket().getRemoteSocketAddress() +
                                " because the connection limit of " + MAX_CONNECTIONS + " has been reached");
                        try {
                            /*
                             * Send rejection message with reason code
                             */
                            final ByteBuffer b = ByteBuffer.allocate(1);
                            b.put(ClientInterface.MAX_CONNECTIONS_LIMIT_ERROR);
                            b.flip();
                            socket.configureBlocking(true);
                            for (int ii = 0; ii < 4 && b.hasRemaining(); ii++) {
                                socket.write(b);
                            }
                            socket.close();
                        } catch (IOException e) {}//don't care keep running
                        continue;
                    }

                    /*
                     * Increment the number of connections even though this one hasn't been authenticated
                     * so that a flood of connection attempts (with many doomed) will not result in
                     * successful authentication of connections that would put us over the limit.
                     */
                    m_numConnections.incrementAndGet();

                    final AuthRunnable authRunnable = new AuthRunnable(socket);
                    while (true) {
                        try {
                            m_executor.execute(authRunnable);
                            break;
                        } catch (RejectedExecutionException e) {
                            Thread.sleep(1);
                        }
                    }
                } while (m_running);
            } catch (Exception e) {
                if (m_running) {
                    hostLog.error("Exception in ClientAcceptor. The acceptor has died", e);
                }
            } finally {
                try {
                    m_serverSocket.close();
                } catch (IOException e) {
                    hostLog.fatal(null, e);
                }
                //Prevent interruption
                synchronized (this) {
                    Thread.interrupted();
                    m_executor.shutdownNow();
                    try {
                        m_executor.awaitTermination(5, TimeUnit.MINUTES);
                    } catch (InterruptedException e) {
                        String msg = "Client Listener Interrupted while shutting down "
                                + (m_isAdmin ? " Admin " : " ") + "port: " + m_port;
                        VoltDB.crashLocalVoltDB(msg, false, e);
                    }
                }
            }
        }

        /**
         * Attempt to authenticate the user associated with this socket connection
         * @param socket
         * @param timeoutRef Populated with error on timeout
         * @return AuthUser a set of user permissions or null if authentication fails
         * @throws IOException
         */
        private InputHandler
        authenticate(final SocketChannel socket, final AtomicReference<String> timeoutRef) throws IOException
        {
            ByteBuffer responseBuffer = ByteBuffer.allocate(6);
            byte version = (byte)0;
            responseBuffer.putInt(2);//message length
            responseBuffer.put(version);//version

            /*
             * The login message is a length preceded name string followed by a length preceded
             * SHA-1 single hash of the password.
             */
            socket.configureBlocking(true);
            socket.socket().setTcpNoDelay(true);//Greatly speeds up requests hitting the wire
            final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);

            /*
             * Schedule a timeout to close the socket in case there is no response for the timeout
             * period. This will wake up the current thread that is blocked on reading the login message
             */
            final long start = System.currentTimeMillis();
            ScheduledFuture<?> timeoutFuture =
                    VoltDB.instance().schedulePriorityWork(new Runnable() {
                        @Override
                        public void run() {
                            long delta = System.currentTimeMillis() - start;
                            double seconds = delta / 1000.0;
                            StringBuilder sb = new StringBuilder();
                            sb.append("Timed out authenticating client from ");
                            sb.append(socket.socket().getRemoteSocketAddress().toString());
                            sb.append(String.format(" after %.2f seconds (timeout target is %.2f seconds)", seconds, ClientInterface.AUTH_TIMEOUT_MS / 1000.0));
                            timeoutRef.set(sb.toString());
                            try {
                                socket.close();
                            } catch (IOException e) {
                                //Don't care
                            }
                        }
                    }, ClientInterface.AUTH_TIMEOUT_MS, 0, TimeUnit.MILLISECONDS);

            try {
                while (lengthBuffer.hasRemaining()) {
                    int read = socket.read(lengthBuffer);
                    if (read == -1) {
                        socket.close();
                        timeoutFuture.cancel(false);
                        return null;
                    }
                }
            } catch (AsynchronousCloseException e) {}//This is the timeout firing and closing the channel

            //Didn't get the value. Client isn't going to get anymore time.
            if (lengthBuffer.hasRemaining()) {
                timeoutFuture.cancel(false);
                authLog.debug("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                              "): wire protocol violation (timeout reading message length).");
                //Send negative response
                responseBuffer.put(ClientInterface.WIRE_PROTOCOL_TIMEOUT_ERROR).flip();
                socket.write(responseBuffer);
                socket.close();
                return null;
            }
            lengthBuffer.flip();

            final int messageLength = lengthBuffer.getInt();
            if (messageLength < 0) {
                timeoutFuture.cancel(false);
                authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                             "): wire protocol violation (message length " + messageLength + " is negative).");
                //Send negative response
                responseBuffer.put(WIRE_PROTOCOL_FORMAT_ERROR).flip();
                socket.write(responseBuffer);
                socket.close();
                return null;
            }
            if (messageLength > ((1024 * 1024) * 2)) {
                timeoutFuture.cancel(false);
                authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                             "): wire protocol violation (message length " + messageLength + " is too large).");
                //Send negative response
                responseBuffer.put(WIRE_PROTOCOL_FORMAT_ERROR).flip();
                socket.write(responseBuffer);
                socket.close();
                return null;
              }

            final ByteBuffer message = ByteBuffer.allocate(messageLength);

            try {
                while (message.hasRemaining()) {
                    int read = socket.read(message);
                    if (read == -1) {
                        socket.close();
                        timeoutFuture.cancel(false);
                        return null;
                    }
                }
            } catch (AsynchronousCloseException e) {}//This is the timeout firing and closing the channel

            //Didn't get the whole message. Client isn't going to get anymore time.
            if (message.hasRemaining()) {
                timeoutFuture.cancel(false);
                authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                             "): wire protocol violation (timeout reading authentication strings).");
                //Send negative response
                responseBuffer.put(WIRE_PROTOCOL_TIMEOUT_ERROR).flip();
                socket.write(responseBuffer);
                socket.close();
                return null;
            }

            /*
             * Since we got the login message, cancel the timeout.
             * If cancellation fails then the socket is dead and the connection lost
             */
            if (!timeoutFuture.cancel(false)) {
                return null;
            }

            message.flip().position(1);//skip version
            FastDeserializer fds = new FastDeserializer(message);
            final String service = fds.readString();
            final String username = fds.readString();
            final byte password[] = new byte[20];
            //We should be left with SHA-1 bytes only.
            if (message.remaining() != 20) {
                authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress()
                        + "): user " + username + " failed authentication.");
                //Send negative response
                responseBuffer.put(AUTHENTICATION_FAILURE).flip();
                socket.write(responseBuffer);
                socket.close();
                return null;
            }
            message.get(password);

            CatalogContext context = m_catalogContext.get();

            AuthProvider ap = null;
            try {
                ap = AuthProvider.fromService(service);
            } catch (IllegalArgumentException unkownProvider) {
                // handle it bellow
            }

            if (ap == null) {
                //Send negative response
                responseBuffer.put(EXPORT_DISABLED_REJECTION).flip();
                socket.write(responseBuffer);
                socket.close();
                authLog.warn("Rejected user " + username +
                        " attempting to use disabled or unconfigured service " +
                        service + ".");
                authLog.warn("VoltDB Export services are no longer available through clients.");
                return null;
            }

            /*
             * Don't use the auth system during recovery. Not safe to use
             * the node to initiate multi-partition txns during recovery
             */
            if (!VoltDB.instance().rejoining()) {
                AuthenticationRequest arq;
                if (ap == AuthProvider.KERBEROS) {
                    arq = context.authSystem.new KerberosAuthenticationRequest(socket);
                } else {
                    arq = context.authSystem.new HashAuthenticationRequest(username, password);
                }
                /*
                 * Authenticate the user.
                 */
                boolean authenticated = arq.authenticate();

                if (!authenticated) {
                    Exception faex = arq.getAuthenticationFailureException();

                    boolean isItIo = false;
                    for (Throwable cause = faex; faex != null && !isItIo; cause = cause.getCause()) {
                        isItIo = cause instanceof IOException;
                    }

                    if (faex != null) {
                        authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                                 "):", faex);
                    } else {
                        authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                                     "): user " + username + " failed authentication.");
                    }
                    //Send negative response
                    if (!isItIo) {
                        responseBuffer.put(AUTHENTICATION_FAILURE).flip();
                        socket.write(responseBuffer);
                    }
                    socket.close();
                    return null;
                }
            } else {
                authLog.warn("Failure to authenticate connection(" + socket.socket().getRemoteSocketAddress() +
                        "): user " + username + " because this node is rejoining.");
                //Send negative response
                responseBuffer.put(AUTHENTICATION_FAILURE_DUE_TO_REJOIN).flip();
                socket.write(responseBuffer);
                socket.close();
                return null;
            }

            /*
             * Create an input handler.
             */
            InputHandler handler = new ClientInputHandler(username, m_isAdmin);

            byte buildString[] = VoltDB.instance().getBuildString().getBytes(Charsets.UTF_8);
            responseBuffer = ByteBuffer.allocate(34 + buildString.length);
            responseBuffer.putInt(30 + buildString.length);//message length
            responseBuffer.put((byte)0);//version

            //Send positive response
            responseBuffer.put((byte)0);
            responseBuffer.putInt(VoltDB.instance().getHostMessenger().getHostId());
            responseBuffer.putLong(handler.connectionId());
            responseBuffer.putLong(VoltDB.instance().getHostMessenger().getInstanceId().getTimestamp());
            responseBuffer.putInt(VoltDB.instance().getHostMessenger().getInstanceId().getCoord());
            responseBuffer.putInt(buildString.length);
            responseBuffer.put(buildString).flip();
            socket.write(responseBuffer);
            return handler;
        }
    }

    /** A port that reads client procedure invocations and writes responses */
    public class ClientInputHandler extends VoltProtocolHandler implements AdmissionControlGroup.ACGMember {
        public static final int MAX_READ = 8192 * 4;

        private Connection m_connection;
        private final boolean m_isAdmin;

        /**
         * Must use username to do a lookup via the auth system
         * rather then caching the AuthUser because the AuthUser
         * can be invalidated on catalog updates
         */
        private final String m_username;

        public ClientInputHandler(String username,
                                  boolean isAdmin)
        {
            m_username = username.intern();
            m_isAdmin = isAdmin;
        }

        public boolean isAdmin()
        {
            return m_isAdmin;
        }

        @Override
        public int getMaxRead() {
            if (m_hasDTXNBackPressure) {
                return 0;
            } else {
                return Math.max( MAX_READ, getNextMessageLength());
            }
        }

        @Override
        public void handleMessage(ByteBuffer message, Connection c) {
            try {
                final ClientResponseImpl error = handleRead(message, this, c);
                if (error != null) {
                    ByteBuffer buf = ByteBuffer.allocate(error.getSerializedSize() + 4);
                    buf.putInt(buf.capacity() - 4);
                    error.flattenToBuffer(buf).flip();
                    c.writeStream().enqueue(buf);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void started(final Connection c) {
            m_connection = c;
            m_cihm.put(c.connectionId(),
                       new ClientInterfaceHandleManager( m_isAdmin, c, m_acg.get()));
            m_acg.get().addMember(this);
            if (!m_acg.get().hasBackPressure()) {
                c.enableReadSelection();
            }
        }

        @Override
        public void stopped(Connection c) {
            m_numConnections.decrementAndGet();
            /*
             * It's necessary to free all the resources held by the IV2 ACG tracking.
             * Outstanding requests may actually still be at large
             */
            ClientInterfaceHandleManager cihm = m_cihm.remove(connectionId());
            cihm.freeOutstandingTxns();
            cihm.m_acg.removeMember(this);
            m_notifier.removeConnection(c);
        }

        /*
         * Runnables from returned by offBackPressure and onBackPressure are used
         * by the network when a specific connection signals backpressure
         * as opposed to the more global backpressure signaled by an ACG. The runnables
         * are only intended to enable/disable backpressure for the specific connection
         */
        @Override
        public Runnable offBackPressure() {
            return new Runnable() {
                @Override
                public void run() {
                    if (!m_acg.get().hasBackPressure()) {
                        m_connection.enableReadSelection();
                    }
                }
            };
        }

        @Override
        public Runnable onBackPressure() {
            return new Runnable() {
                @Override
                public void run() {
                    m_connection.disableReadSelection();
                }
            };
        }

        /*
         * Return a monitor for the number of outstanding bytes pending write to this network
         * connection
         */
        @Override
        public QueueMonitor writestreamMonitor() {
            return new QueueMonitor() {
                @Override
                public boolean queue(int bytes) {
                    return m_acg.get().queue(bytes);
                }
            };
        }

        /*
         * IV2 versions of backpressure management invoked by AdmissionControlGroup while
         * globally enabling/disabling backpressure.
         */
        @Override
        public void onBackpressure() {
            m_connection.disableReadSelection();
        }

        @Override
        public void offBackpressure() {
            m_connection.enableReadSelection();
        }
    }

    /**
     * Runs on the network thread to prepare client response. If a transaction needs to be
     * restarted, it will get restarted here.
     */
    private class ClientResponseWork implements DeferredSerialization {
        private final ClientInterfaceHandleManager cihm;
        private final InitiateResponseMessage response;
        private final Procedure catProc;
        private ClientResponseImpl clientResponse;

        private ClientResponseWork(InitiateResponseMessage response,
                                   ClientInterfaceHandleManager cihm,
                                   Procedure catProc)
        {
            this.response = response;
            this.clientResponse = response.getClientResponseData();
            this.cihm = cihm;
            this.catProc = catProc;
        }

        @Override
        public void serialize(ByteBuffer buf) throws IOException
        {
            buf.putInt(buf.capacity() - 4);
            clientResponse.flattenToBuffer(buf);
        }

        @Override
        public void cancel() {
        }

        @Override
        public int getSerializedSize() throws IOException {
            // HACK-O-RIFFIC
            // For now, figure out if this is a transaction that was ignored
            // by the ReplaySequencer and just remove the handle from the CIHM
            // without removing any handles before it which we haven't seen yet.
            ClientInterfaceHandleManager.Iv2InFlight clientData;
            if (clientResponse != null &&
                    clientResponse.getStatusString() != null &&
                    clientResponse.getStatusString().equals(ClientResponseImpl.IGNORED_TRANSACTION)) {
                clientData = cihm.removeHandle(response.getClientInterfaceHandle());
            }
            else {
                clientData = cihm.findHandle(response.getClientInterfaceHandle());
            }
            if (clientData == null) {
                return DeferredSerialization.EMPTY_MESSAGE_LENGTH;
            }

            // Reuse the creation time of the original invocation to have accurate internal latency
            if (restartTransaction(clientData.m_messageSize, clientData.m_creationTimeNanos)) {
                // If the transaction is successfully restarted, don't send a response to the
                // client yet.
                return DeferredSerialization.EMPTY_MESSAGE_LENGTH;
            }

            final long now = System.nanoTime();
            final long delta = now - clientData.m_creationTimeNanos;

            /*
             * Log initiator stats
             */
            cihm.m_acg.logTransactionCompleted(
                    cihm.connection.connectionId(),
                    cihm.connection.getHostnameOrIP(),
                    clientData.m_procName,
                    delta,
                    clientResponse.getStatus());

            clientResponse.setClientHandle(clientData.m_clientHandle);
            clientResponse.setClusterRoundtrip((int)TimeUnit.NANOSECONDS.toMillis(delta));
            clientResponse.setHash(null); // not part of wire protocol

            return clientResponse.getSerializedSize() + 4;
        }

        @Override
        public String toString() {
            return clientResponse.getClass().getName();
        }

        /**
         * Checks if the transaction needs to be restarted, if so, restart it.
         * @param messageSize the original message size when the invocation first came in
         * @param now the current timestamp
         * @return true if the transaction is restarted successfully, false otherwise.
         */
        private boolean restartTransaction(int messageSize, long nowNanos)
        {
            if (response.isMispartitioned()) {
                // Restart a mis-partitioned transaction
                assert response.getInvocation() != null;
                assert response.getCurrentHashinatorConfig() != null;
                assert(catProc != null);

                // before rehashing, update the hashinator
                TheHashinator.updateHashinator(
                        TheHashinator.getConfiguredHashinatorClass(),
                        response.getCurrentHashinatorConfig().getFirst(), // version
                        response.getCurrentHashinatorConfig().getSecond(), // config bytes
                        false); // cooked (true for snapshot serialization only)

                // if we are recovering, the mispartitioned txn must come from the log,
                // don't restart it. The correct txn will be replayed by another node.
                if (VoltDB.instance().getMode() == OperationMode.INITIALIZING) {
                    return false;
                }

                boolean isReadonly = catProc.getReadonly();

                try {
                    ProcedurePartitionInfo ppi = (ProcedurePartitionInfo)catProc.getAttachment();
                    int partition = getPartitionForProcedure(ppi.index,
                            ppi.type, response.getInvocation());
                    createTransaction(cihm.connection.connectionId(),
                            response.getInvocation(),
                            isReadonly,
                            true, // Only SP could be mis-partitioned
                            false, // Only SP could be mis-partitioned
                            partition,
                            messageSize,
                            nowNanos);
                    return true;
                } catch (Exception e) {
                    // unable to hash to a site, return an error
                    assert(clientResponse == null);
                    clientResponse = getMispartitionedErrorResponse(response.getInvocation(), catProc, e);
                }
            }

            return false;
        }
    }

    ClientNativeInterface(ClientInterface clientInterface,
                          InetAddress clientIntf,
                          int clientPort,
                          InetAddress adminIntf,
                          int adminPort) throws Exception
    {
        m_acceptor = new ClientAcceptor(clientIntf, clientPort, messenger.getNetwork(), false);
        m_adminAcceptor = new ClientAcceptor(adminIntf, adminPort, messenger.getNetwork(), true);
    }




    /**
     *
     * @param port
     * * return True if an error was generated and needs to be returned to the client
     */
    final ClientResponseImpl handleRead(ByteBuffer buf, ClientInputHandler handler, Connection ccxn) throws IOException {
        final long nowNanos = System.nanoTime();
        StoredProcedureInvocation task = new StoredProcedureInvocation();
        try {
            task.initFromBuffer(buf);
        } catch (Exception ex) {
            return new ClientResponseImpl(
                    ClientResponseImpl.UNEXPECTED_FAILURE,
                    new VoltTable[0], ex.getMessage(), ccxn.connectionId());
        }
        ClientResponseImpl error = null;

        // Check for admin mode restrictions before proceeding any further
        VoltDBInterface instance = VoltDB.instance();
        if (instance.getMode() == OperationMode.PAUSED && !handler.isAdmin())
        {
            return new ClientResponseImpl(ClientResponseImpl.SERVER_UNAVAILABLE,
                    new VoltTable[0], "Server is currently unavailable; try again later",
                    task.clientHandle);
        }

        // Deserialize the client's request and map to a catalog stored procedure
        final CatalogContext catalogContext = m_catalogContext.get();
        final AuthSystem.AuthUser user = catalogContext.authSystem.getUser(handler.m_username);

        Procedure catProc = catalogContext.procedures.get(task.procName);
        if (catProc == null) {
            catProc = catalogContext.m_defaultProcs.checkForDefaultProcedure(task.procName);
        }

        if (catProc == null) {
            String proc = task.procName;
            if (task.procName.equals("@AdHoc") || task.procName.equals("@AdHocSpForTest")) {
                // Map @AdHoc... to @AdHoc_RW_MP for validation. In the future if security is
                // configured differently for @AdHoc... variants this code will have to
                // change in order to use the proper variant based on whether the work
                // is single or multi partition and read-only or read-write.
                proc = "@AdHoc_RW_MP";
            }
            else if (task.procName.equals("@UpdateClasses")) {
                // Icky.  Map @UpdateClasses to @UpdateApplicationCatalog.  We want the
                // permissions and replication policy for @UAC, and we'll deal with the
                // parameter validation stuff separately (the different name will
                // skip the @UAC-specific policy)
                proc = "@UpdateApplicationCatalog";
            }
            Config sysProc = SystemProcedureCatalog.listing.get(proc);
            if (sysProc != null) {
                catProc = sysProc.asCatalogProcedure();
            }
        }

        if (user == null) {
            authLog.info("User " + handler.m_username + " has been removed from the system via a catalog update");
            return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                    new VoltTable[0], "User " + handler.m_username +
                    " has been removed from the system via a catalog update",
                    task.clientHandle);
        }

        if (catProc == null) {
            String errorMessage = "Procedure " + task.procName + " was not found";
            RateLimitedLogger.tryLogForMessage(System.currentTimeMillis(),
                            60, TimeUnit.SECONDS,
                            authLog,
                            Level.WARN, errorMessage + ". This message is rate limited to once every 60 seconds.");
            return new ClientResponseImpl(
                    ClientResponseImpl.UNEXPECTED_FAILURE,
                    new VoltTable[0], errorMessage, task.clientHandle);
        }

        final ProcedurePartitionInfo ppi = (ProcedurePartitionInfo)catProc.getAttachment();

        //Check permissions
        if ((error = m_permissionValidator.shouldAccept(task.procName, user, task, catProc)) != null) {
            return error;
        }

        //Check param deserialization policy for sysprocs
        if ((error = m_invocationValidator.shouldAccept(task.procName, user, task, catProc)) != null) {
            return error;
        }

        if (catProc.getSystemproc()) {
            // COMMUNITY SYSPROC SPECIAL HANDLING

            // ping just responds as fast as possible to show the connection is alive
            // nb: ping is not a real procedure, so this is checked before other "sysprocs"
            if (task.procName.equals("@Ping")) {
                return new ClientResponseImpl(ClientResponseImpl.SUCCESS, new VoltTable[0], "", task.clientHandle);
            }
            else if (task.procName.equals("@GetPartitionKeys")) {
                return dispatchGetPartitionKeys(task);
            }
            else if (task.procName.equals("@Subscribe")) {
                return dispatchSubscribe( handler, task);
            }
            else if (task.procName.equals("@Statistics")) {
                return dispatchStatistics(OpsSelector.STATISTICS, task, ccxn);
            }
            else if (task.procName.equals("@SystemCatalog")) {
                return dispatchStatistics(OpsSelector.SYSTEMCATALOG, task, ccxn);
            }
            else if (task.procName.equals("@SystemInformation")) {
                return dispatchStatistics(OpsSelector.SYSTEMINFORMATION, task, ccxn);
            }
            else if (task.procName.equals("@GC")) {
                return dispatchSystemGC(handler, task);
            }
            else if (task.procName.equals("@StopNode")) {
                return dispatchStopNode(task);
            }

            else if (task.procName.equals("@Explain")) {
                return dispatchAdHoc(task, handler, ccxn, true, user);
            }
            else if (task.procName.equals("@ExplainProc")) {
                return dispatchExplainProcedure(task, handler, ccxn, user);
            }
            else if (task.procName.equals("@SendSentinel")) {
                dispatchSendSentinel(handler.connectionId(), nowNanos, buf.capacity(), task);
                return null;
            }

            else if (task.procName.equals("@AdHoc")) {
                return dispatchAdHoc(task, handler, ccxn, false, user);
            }
            else if (task.procName.equals("@AdHocSpForTest")) {
                return dispatchAdHocSpForTest(task, handler, ccxn, false, user);
            }
            else if (task.procName.equals("@LoadMultipartitionTable")) {
                /*
                 * For IV2 DR: This will generate a sentinel for each partition,
                 * but doesn't initiate the invocation. It will fall through to
                 * the shared dispatch of sysprocs.
                 */
                if (task.getType() == ProcedureInvocationType.REPLICATED) {
                    sendSentinelsToAllPartitions(task.getOriginalTxnId());
                }
            }
            else if (task.procName.equals("@LoadSinglepartitionTable")) {
                // FUTURE: When we get rid of the legacy hashinator, this should go away
                return dispatchLoadSinglepartitionTable(buf, catProc, task, handler, ccxn);
            }

            // ERROR MESSAGE FOR PRO SYSPROC USE IN COMMUNITY

            if (!MiscUtils.isPro()) {
                SystemProcedureCatalog.Config sysProcConfig = SystemProcedureCatalog.listing.get(task.procName);
                if ((sysProcConfig != null) && (sysProcConfig.commercial)) {
                    return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                            new VoltTable[0],
                            task.procName + " is available in the Enterprise Edition of VoltDB only.",
                            task.clientHandle);
                }
            }

            // PRO SYSPROC SPECIAL HANDLING

            if (task.procName.equals("@UpdateApplicationCatalog")) {
                return dispatchUpdateApplicationCatalog(task, handler, ccxn, user);
            }
            else if (task.procName.equals("@UpdateClasses")) {
                return dispatchUpdateApplicationCatalog(task, handler, ccxn, user);
            }
            else if (task.procName.equals("@SnapshotSave")) {
                m_snapshotDaemon.requestUserSnapshot(task, ccxn);
                return null;
            }
            else if (task.procName.equals("@Promote")) {
                return dispatchPromote(catProc, buf, task, handler, ccxn);
            }
            else if (task.procName.equals("@SnapshotStatus")) {
                // SnapshotStatus is really through @Statistics now, but preserve the
                // legacy calling mechanism
                Object[] params = new Object[1];
                params[0] = "SNAPSHOTSTATUS";
                task.setParams(params);
                return dispatchStatistics(OpsSelector.STATISTICS, task, ccxn);
            }
            else if (task.procName.equals("@SnapshotScan")) {
                return dispatchStatistics(OpsSelector.SNAPSHOTSCAN, task, ccxn);
            }
            else if (task.procName.equals("@SnapshotDelete")) {
                return dispatchStatistics(OpsSelector.SNAPSHOTDELETE, task, ccxn);
            }
            else if (task.procName.equals("@SnapshotRestore")) {
                ClientResponseImpl retval = SnapshotUtil.transformRestoreParamsToJSON(task);
                if (retval != null) return retval;
            }

            // If you're going to copy and paste something, CnP the pattern
            // up above.  -rtb.

            // Verify that admin mode sysprocs are called from a client on the
            // admin port, otherwise return a failure
            if (task.procName.equals("@Pause") || task.procName.equals("@Resume")) {
                if (!handler.isAdmin()) {
                    return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                            new VoltTable[0],
                            "" + task.procName + " is not available to this client",
                            task.clientHandle);
                }
            }
        }

        int partition = -1;
        if (catProc.getSinglepartition()) {
            // break out the Hashinator and calculate the appropriate partition
            try {
                partition =
                        getPartitionForProcedure(
                                ppi.index,
                                ppi.type,
                                task);
            } catch (Exception e) {
                // unable to hash to a site, return an error
                return getMispartitionedErrorResponse(task, catProc, e);
            }
        }
        boolean success =
                createTransaction(handler.connectionId(),
                        task,
                        catProc.getReadonly(),
                        catProc.getSinglepartition(),
                        catProc.getEverysite(),
                        partition,
                        buf.capacity(),
                        nowNanos);
        if (!success) {
            // HACK: this return is for the DR agent so that it
            // will move along on duplicate replicated transactions
            // reported by the slave cluster.  We report "SUCCESS"
            // to keep the agent from choking.  ENG-2334
            return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                    new VoltTable[0],
                    ClientResponseImpl.IGNORED_TRANSACTION,
                    task.clientHandle);
        }
        return null;
    }


    private ScheduledFuture<?> m_deadConnectionFuture;
    private ScheduledFuture<?> m_topologyCheckFuture;
    public void schedulePeriodicWorks() {
        m_deadConnectionFuture = VoltDB.instance().scheduleWork(new Runnable() {
            @Override
            public void run() {
                try {
                    //Using the current time makes this vulnerable to NTP weirdness...
                    checkForDeadConnections(EstTime.currentTimeMillis());
                } catch (Exception ex) {
                    log.warn("Exception while checking for dead connections", ex);
                }
            }
        }, 200, 200, TimeUnit.MILLISECONDS);
        /*
         * Every five seconds check if the topology of the cluster has changed,
         * and if it has push an update to the clients. This should be an inexpensive operation
         * that operates on cached data and it ensures that clients eventually converge on the current
         * topology
         */
        m_topologyCheckFuture = VoltDB.instance().scheduleWork(new Runnable() {
            @Override
            public void run() {
                checkForTopologyChanges();
            }
        }, 0, TOPOLOGY_CHANGE_CHECK_MS, TimeUnit.MILLISECONDS);
    }

    private static final long CLIENT_HANGUP_TIMEOUT = Long.getLong("CLIENT_HANGUP_TIMEOUT", 30000);

    /**
     * Check for dead connections by providing each connection with the current
     * time so it can calculate the delta between now and the time the oldest message was
     * queued for sending.
     * @param now Current time in milliseconds
     */
    private final void checkForDeadConnections(final long now) {
        final ArrayList<Pair<Connection, Integer>> connectionsToRemove = new ArrayList<Pair<Connection, Integer>>();
        for (final ClientInterfaceHandleManager cihm : m_cihm.values()) {
            // Internal connections don't implement calculatePendingWriteDelta(), so check for real connection first
            if (VoltPort.class == cihm.connection.getClass()) {
                final int delta = cihm.connection.writeStream().calculatePendingWriteDelta(now);
                if (delta > CLIENT_HANGUP_TIMEOUT) {
                    connectionsToRemove.add(Pair.of(cihm.connection, delta));
                }
            }
        }

        for (final Pair<Connection, Integer> p : connectionsToRemove) {
            Connection c = p.getFirst();
            networkLog.warn("Closing connection to " + c +
                    " because it hasn't read a response that was pending for " +  p.getSecond() + " milliseconds");
            c.unregister();
        }
    }

    // BUG: this needs some more serious thinking
    // probably should be able to schedule a shutdown event
    // to the dispatcher..  Or write a "stop reading and flush
    // all your read buffers" events .. or something ..
    protected void shutdown() throws InterruptedException {
        if (m_deadConnectionFuture != null) {
            m_deadConnectionFuture.cancel(false);
            try {m_deadConnectionFuture.get();} catch (Throwable t) {}
        }
        if (m_topologyCheckFuture != null) {
            m_topologyCheckFuture.cancel(false);
            try {m_topologyCheckFuture.get();} catch (Throwable t) {}
        }
        if (m_maxConnectionUpdater != null) {
            m_maxConnectionUpdater.cancel(false);
        }
        if (m_acceptor != null) {
            m_acceptor.shutdown();
        }
        if (m_adminAcceptor != null)
        {
            m_adminAcceptor.shutdown();
        }
        if (m_localReplicasBuilder != null) {
            m_localReplicasBuilder.join(10000);
            if (m_localReplicasBuilder.isAlive()) {
                hostLog.error("Local replica map builder took more than ten seconds, probably hung");
            }
            m_localReplicasBuilder.join();
        }
    }

    private volatile Thread m_localReplicasBuilder = null;
    public void startAcceptingConnections() throws IOException {
        /*
         * Periodically check the limit on the number of open files
         */
        m_maxConnectionUpdater = VoltDB.instance().scheduleWork(new Runnable() {
            @Override
            public void run() {
                Integer limit = org.voltdb.utils.CLibrary.getOpenFileLimit();
                if (limit != null) {
                    //Leave 300 files open for "stuff"
                    MAX_CONNECTIONS.set(limit - 300);
                }
            }
        }, 0, 10, TimeUnit.MINUTES);
        m_acceptor.start();
        if (m_adminAcceptor != null)
        {
            m_adminAcceptor.start();
        }
        mayActivateSnapshotDaemon();
    }

    public Map<Long, Pair<String, long[]>> getLiveClientStats()
    {
        final Map<Long, Pair<String, long[]>> client_stats =
            new HashMap<Long, Pair<String, long[]>>();

        // m_cihm hashes connectionId to a ClientInterfaceHandleManager
        // ClientInterfaceHandleManager has the connection object.
        for (Map.Entry<Long, ClientInterfaceHandleManager> e : m_cihm.entrySet()) {
            // The internal CI adapters report negative connection ids and
            // aren't included in public stats.
            if (e.getKey() > 0) {
                long adminMode = e.getValue().isAdmin ? 1 : 0;
                long readWait = e.getValue().connection.readStream().dataAvailable();
                long writeWait = e.getValue().connection.writeStream().getOutstandingMessageCount();
                long outstandingTxns = e.getValue().getOutstandingTxns();
                client_stats.put(
                        e.getKey(), new Pair<String, long[]>(
                            e.getValue().connection.getHostnameOrIP(),
                            new long[] {adminMode, readWait, writeWait, outstandingTxns}));
            }
        }
        return client_stats;
    }
}
