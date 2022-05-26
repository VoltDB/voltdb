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

package org.voltdb;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import org.HdrHistogram_voltpatches.AbstractHistogram;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.SiteFailureForwardMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.network.CipherExecutor;
import org.voltcore.network.Connection;
import org.voltcore.network.NIOReadStream;
import org.voltcore.network.QueueMonitor;
import org.voltcore.network.ReverseDNSPolicy;
import org.voltcore.network.VoltNetworkPool;
import org.voltcore.network.VoltPort;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.network.WriteStream;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.Pair;
import org.voltcore.utils.ssl.MessagingChannel;
import org.voltdb.AuthSystem.AuthProvider;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.CatalogContext.ProcedurePartitionInfo;
import org.voltdb.ClientInterfaceHandleManager.Iv2InFlight;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.client.ClientAuthScheme;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.TLSHandshaker;
import org.voltdb.common.Constants;
import org.voltdb.dtxn.InitiatorStats.InvocationInfo;
import org.voltdb.iv2.Cartographer;
import org.voltdb.iv2.Iv2Trace;
import org.voltdb.iv2.MigratePartitionLeaderInfo;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.HashMismatchMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2EndOfLogMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.LocalMailbox;
import org.voltdb.messaging.MigratePartitionLeaderMessage;
import org.voltdb.security.AuthenticationRequest;
import org.voltdb.stats.ClientConnectionsTracker;
import org.voltdb.stats.FileDescriptorsTracker;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltTrace;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.NotSslRecordException;
import io.netty.handler.ssl.SslContext;

/**
 * Represents VoltDB's connection to client libraries outside the cluster.
 * This class accepts new connections and manages existing connections through
 * <code>ClientConnection</code> instances.
 *
 */
public class ClientInterface implements SnapshotDaemon.DaemonInitiator {

    public static final String DROP_TXN_RECOVERY = "Transaction dropped during fault recovery";
    public static final String DROP_TXN_MASTERSHIP = "Transaction dropped due to change in mastership."
                        + " It is possible the transaction was committed";

    static long TOPOLOGY_CHANGE_CHECK_MS = Long.getLong("TOPOLOGY_CHANGE_CHECK_MS", 5000);
    static long AUTH_TIMEOUT_MS = Long.getLong("AUTH_TIMEOUT_MS", 30000);
    private static final int SSL_LOG_INTERVAL = 60; // seconds
    private static final int FD_LOG_INTERVAL = 600; // seconds

    //Same as in Distributer.java
    public static final long ASYNC_TOPO_HANDLE = Long.MAX_VALUE - 1;
    //Notify clients to update procedure info cache for client affinity
    public static final long ASYNC_PROC_HANDLE = Long.MAX_VALUE - 2;

    // reasons a connection can fail
    public static final byte AUTHENTICATION_FAILURE = Constants.AUTHENTICATION_FAILURE;
    public static final byte MAX_CONNECTIONS_LIMIT_ERROR = Constants.MAX_CONNECTIONS_LIMIT_ERROR;
    public static final byte WIRE_PROTOCOL_TIMEOUT_ERROR = Constants.WIRE_PROTOCOL_TIMEOUT_ERROR;
    public static final byte WIRE_PROTOCOL_FORMAT_ERROR = Constants.WIRE_PROTOCOL_FORMAT_ERROR;
    public static final byte AUTHENTICATION_FAILURE_DUE_TO_REJOIN = Constants.AUTHENTICATION_FAILURE_DUE_TO_REJOIN;
    public static final byte EXPORT_DISABLED_REJECTION = Constants.EXPORT_DISABLED_REJECTION;

    // authentication handshake codes
    public static final byte AUTH_HANDSHAKE_VERSION = Constants.AUTH_HANDSHAKE_VERSION;
    public static final byte AUTH_SERVICE_NAME = Constants.AUTH_SERVICE_NAME;
    public static final byte AUTH_HANDSHAKE = Constants.AUTH_HANDSHAKE;

    // connection IDs used by internal adapters
    public static final long RESTORE_AGENT_CID          = Long.MIN_VALUE + 1;
    public static final long SNAPSHOT_UTIL_CID          = Long.MIN_VALUE + 2;
    public static final long ELASTIC_COORDINATOR_CID    = Long.MIN_VALUE + 3;
    public static final long TOPICS_COORDINATOR_CID    = Long.MIN_VALUE + 4;
    public static final long EXPORT_MANAGER_CID         = Long.MIN_VALUE + 5;
    public static final long EXECUTE_TASK_CID           = Long.MIN_VALUE + 6;
    public static final long DR_DISPATCHER_CID          = Long.MIN_VALUE + 7;
    public static final long RESTORE_SCHEMAS_CID        = Long.MIN_VALUE + 8;
    public static final long SHUTDONW_SAVE_CID          = Long.MIN_VALUE + 9;
    public static final long NT_REMOTE_PROC_CID         = Long.MIN_VALUE + 10;
    // public static final long UNUSED_CID (was migrate)= Long.MIN_VALUE + 11;
    public static final long TASK_MANAGER_CID           = Long.MIN_VALUE + 12;

    // Leave CL_REPLAY_BASE_CID at the end, it uses this as a base and generates more cids
    // PerPartition cids
    private static long setBaseValue(int offset) { return offset << 14; }
    public static final long CL_REPLAY_BASE_CID                = Long.MIN_VALUE + setBaseValue(1);
    public static final long DR_REPLICATION_SNAPSHOT_BASE_CID  = Long.MIN_VALUE + setBaseValue(2);
    public static final long DR_REPLICATION_NORMAL_BASE_CID    = Long.MIN_VALUE + setBaseValue(3);
    public static final long DR_REPLICATION_MP_BASE_CID        = Long.MIN_VALUE + setBaseValue(4);
    public static final long NT_ADAPTER_CID                    = Long.MIN_VALUE + setBaseValue(5);
    public static final long INTERNAL_CID                      = Long.MIN_VALUE + setBaseValue(6);

    private static final VoltLogger log = new VoltLogger(ClientInterface.class.getName());
    private static final VoltLogger authLog = new VoltLogger("AUTH");
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger networkLog = new VoltLogger("NETWORK");

    static final VoltLogger tmLog = new VoltLogger("TM");

    // Used by NT procedure to generate handle, don't use elsewhere.
    public static final int NTPROC_JUNK_ID = -2;

    /** Ad hoc async work is either regular planning, ad hoc explain, or default proc explain. */
    public enum ExplainMode {
        NONE, EXPLAIN_ADHOC, EXPLAIN_DEFAULT_PROC, EXPLAIN_JSON;
    }

    private final ClientAcceptor m_acceptor;
    private ClientAcceptor m_adminAcceptor;

    private final SnapshotDaemon m_snapshotDaemon;
    private final SnapshotDaemonAdapter m_snapshotDaemonAdapter;
    private final InternalConnectionHandler m_internalConnectionHandler;
    private final SimpleClientResponseAdapter m_executeTaskAdpater;

    // Atomically allows the catalog reference to change between access
    private final AtomicReference<CatalogContext> m_catalogContext = new AtomicReference<CatalogContext>(null);

    /**
     * ZooKeeper is used for @Promote to trigger a truncation snapshot.
     */
    ZooKeeper m_zk;

    /**
     * The CIHM is unique to the connection and the ACG is shared by all connections serviced by the associated network
     * thread. They are paired so as to only do a single lookup.
     * <p>
     * Note: An initialSize of 1024 actually creates an array of 2048 in the map
     */
    private final ConcurrentHashMap<Long, ClientInterfaceHandleManager> m_cihm = new ConcurrentHashMap<>(1024);

    private final RateLimitedClientNotifier m_notifier = new RateLimitedClientNotifier();

    private final Cartographer m_cartographer;

    //Dispatched stored procedure invocations
    private final InvocationDispatcher m_dispatcher;

    private ScheduledExecutorService m_migratePartitionLeaderExecutor;
    private ScheduledExecutorService m_replicaRemovalExecutor;
    private Object m_lock = new Object();
    /*
     * This list of ACGs is iterated to retrieve initiator statistics in IV2.
     * They are thread local, and the ACG happens to be thread local, and if you squint
     * right admission control seems like a reasonable place to store stats about
     * what has been admitted.
     */
    private final CopyOnWriteArrayList<AdmissionControlGroup> m_allACGs =
            new CopyOnWriteArrayList<AdmissionControlGroup>();

    /*
     * A thread local is a convenient way to keep the ACG out of volt core. The lookup is paired
     * with the CIHM in m_connectionSpecificStuff in fast path code.
     *
     * With these initial values if you have 16 hardware threads you will end up with 4  ACGs
     * and 32 megs/4k transactions and double that with 32 threads.
     */
    private final ThreadLocal<AdmissionControlGroup> m_acg = new ThreadLocal<AdmissionControlGroup>() {
        @Override
        public AdmissionControlGroup initialValue() {
            AdmissionControlGroup acg = new AdmissionControlGroup( 1024 * 1024 * 8, 1000);
            m_allACGs.add(acg);
            return acg;
        }
    };

    final long m_siteId;
    final Mailbox m_mailbox;

    private final FileDescriptorsTracker m_fileDescriptorTracker = new FileDescriptorsTracker();
    private final ClientConnectionsTracker m_clientConnectionsTracker = new ClientConnectionsTracker(m_fileDescriptorTracker);

    private final AtomicBoolean m_isAcceptingConnections = new AtomicBoolean(false);

    /** A port that accepts client connections */
    public class ClientAcceptor implements Runnable {
        private final int m_port;
        private final ServerSocketChannel m_serverSocket;
        private final VoltNetworkPool m_network;
        private volatile boolean m_running = true;
        private Thread m_thread = null;
        private final boolean m_isAdmin;
        private final InetAddress m_interface;
        private final SslContext m_sslContext;

        /**
         * Used a cached thread pool to accept new connections.
         */
        private final ExecutorService m_executor = CoreUtils.getBoundedThreadPoolExecutor(128, 10L, TimeUnit.SECONDS,
                        CoreUtils.getThreadFactory("Client authentication threads", "Client authenticator"));

        ClientAcceptor(InetAddress intf, int port, VoltNetworkPool network, boolean isAdmin, SslContext sslContext)
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
            m_sslContext = sslContext;
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
                    final String remoteIP = ((InetSocketAddress)(m_socket.socket().getRemoteSocketAddress())).getAddress().getHostAddress();
                    SSLEngine sslEngine = null;
                    ByteBuffer remnant = ByteBuffer.wrap(new byte[0]);

                    // Do TLS/SSL setup iff configured; client is expected to know whether
                    // the server expects it.
                    if (m_sslContext != null) {
                        try {
                            sslEngine = m_sslContext.newEngine(ByteBufAllocator.DEFAULT);
                        } catch (Exception e) {
                            networkLog.rateLimitedWarn(SSL_LOG_INTERVAL,
                                                       "Rejected new connection, failed to create SSLEngine; " +
                                                       "indicates problem with TLS/SSL configuration: %s", e.getMessage());
                            return;
                        }
                        // blocking needs to be false for handshaking.

                        String error = null;
                        String detail = "";
                        try {
                            // m_socket.configureBlocking(false);
                            m_socket.socket().setTcpNoDelay(true);
                            TLSHandshaker handshaker = new TLSHandshaker(m_socket, sslEngine);
                            boolean handshakeStatus = handshaker.handshake();
                            /*
                             * The JDK caches TLS/SSL sessions when the participants are the same (i.e.
                             * multiple connection requests from the same peer). Once a session is cached
                             * the client side ends its handshake session quickly, and is able to send
                             * the login Volt message before the server finishes its handshake. This message
                             * is caught in the servers last handshake network read.
                             */
                            if (handshakeStatus) {
                                remnant = handshaker.getRemnant();
                            } else {
                                error = "TLS/SSL handshake failed";
                            }
                        } catch (NotSslRecordException e) {
                            error = "client not using TLS/SSL";
                        } catch (SSLException e) {
                            error = "TLS/SSL handshake failed:";
                            detail = e.getMessage();
                        } catch (IOException e) {
                            error =  "error during TLS/SSL handshake:";
                            detail = e.getMessage();
                        }
                        if (error != null) {
                            // We want to rate-limit per client, so the remote IP address is part of the format.
                            String format = String.format("Rejected new connection from %s, %%s %%s", remoteIP);
                            networkLog.rateLimitedWarn(SSL_LOG_INTERVAL, format, error, detail);
                            closeSocket();
                            return;
                        }
                        // Here we want to log if the protocol/cipher changes, so the whole thing is the format
                        String format = String.format("TLS/SSL enabled on connection %s with protocol %s and with cipher %s",
                                                      remoteIP, sslEngine.getSession().getProtocol(),
                                                      sslEngine.getSession().getCipherSuite());
                        networkLog.rateLimitedInfo(SSL_LOG_INTERVAL, format);
                    }

                    boolean success = false;
                    MessagingChannel messagingChannel = MessagingChannel.get(m_socket, sslEngine);
                    AtomicReference<String> timeoutRef = null;
                    try {
                        // Enforce a limit on the maximum number of connections
                        if (m_clientConnectionsTracker.isConnectionsLimitReached()) {
                            m_clientConnectionsTracker.connectionDropped();
                            networkLog.rateLimitedWarn(FD_LOG_INTERVAL, "Rejected connection from %s because the connection limit of %s has been reached",
                                                       remoteIP, m_clientConnectionsTracker.getMaxNumberOfAllowedConnections());
                            try {
                                // Send rejection message with reason code
                                ByteBuffer b = ByteBuffer.allocate(1);
                                b.put(MAX_CONNECTIONS_LIMIT_ERROR);
                                b.flip();
                                synchronized(m_socket.blockingLock()) {
                                    m_socket.configureBlocking(true);
                                }
                                for (int ii = 0; ii < 4 && b.hasRemaining(); ii++) {
                                    messagingChannel.writeMessage(b);
                                }
                                m_socket.close();
                            } catch (IOException e) {
                                // ignore
                            }
                            return;
                        }

                        /*
                         * Increment the number of connections even though this one hasn't been authenticated
                         * so that a flood of connection attempts (with many doomed) will not result in
                         * successful authentication of connections that would put us over the limit.
                         */
                        m_clientConnectionsTracker.connectionOpened();

                        //Populated on timeout
                        timeoutRef = new AtomicReference<String>();
                        final ClientInputHandler handler = authenticate(m_socket, messagingChannel, timeoutRef, remnant);
                        if (handler != null) {
                            synchronized(m_socket.blockingLock()) {
                                m_socket.configureBlocking(false);
                                m_socket.socket().setTcpNoDelay(true);
                                m_socket.socket().setKeepAlive(true);
                            }

                            m_network.registerChannel(
                                    m_socket,
                                    handler,
                                    0,
                                    ReverseDNSPolicy.ASYNCHRONOUS,
                                    CipherExecutor.SERVER,
                                    sslEngine);
                            /*
                             * If IV2 is enabled the logic initially enabling read is
                             * in the started method of the InputHandler
                             */
                            success = true;
                        }
                    } catch (Exception e) {
                        closeSocket();
                        if (m_running) {
                            if (timeoutRef.get() != null) {
                                hostLog.warn(timeoutRef.get());
                            } else {
                                hostLog.warn("Exception authenticating and "
                                        + "registering user in ClientAcceptor", e);
                            }
                        }
                    } finally {
                        messagingChannel.cleanUp();
                        if (!success) {
                            m_clientConnectionsTracker.connectionClosed();
                        }
                    }
                }
            }

            private void closeSocket() {
                try {
                    m_socket.close();
                } catch (IOException ex) {
                    // Don't care
                }
            }
        }

        @Override
        public void run() {
            try {
                do {
                    final SocketChannel socket;
                    try {
                        socket = m_serverSocket.accept();
                    }
                    catch (IOException ioe) {
                        if (ioe.getMessage() != null &&
                            ioe.getMessage().contains("Too many open files")) {
                            networkLog.rateLimitedWarn(FD_LOG_INTERVAL, "Rejected new connection due to too many open files");
                            continue;
                        }
                        throw ioe;
                    }

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
         * @param remnant The JDK caches TLS/SSL sessions when the participants are the same (i.e.
         *   multiple connection requests from the same peer). Once a session is cached
         *   the client side ends its handshake session quickly, and is able to send
         *   the login Volt message before the server finishes its handshake. This message
         *   is caught in the servers last handshake network read.
         * @return AuthUser a set of user permissions or null if authentication fails
         * @throws IOException
         */
        private ClientInputHandler
        authenticate(final SocketChannel socket, MessagingChannel messagingChannel, final AtomicReference<String> timeoutRef, ByteBuffer remnant) throws IOException
        {
            ByteBuffer responseBuffer = ByteBuffer.allocate(6);
            byte version = (byte)0;
            responseBuffer.putInt(2);//message length
            responseBuffer.put(version);//version

            // Use sourceIP for cases where we want an IP address only
            // Use sourceNameAndIP for cases (typically logging) where we'll take a host name if we have one
            InetAddress sourceInetAddr = ((InetSocketAddress)(socket.socket().getRemoteSocketAddress())).getAddress();
            final String sourceIP = sourceInetAddr.getHostAddress();
            final String sourceNameAndIP = sourceInetAddr.toString().replaceFirst("^/", "");

            /*
             * The login message is a length preceded name string followed by a length preceded
             * SHA-1 single hash of the password.
             */
            synchronized (socket.blockingLock()) {
                socket.configureBlocking(true);
                socket.socket().setTcpNoDelay(true);//Greatly speeds up requests hitting the wire
            }

            if (remnant.hasRemaining() && (remnant.remaining() <= 4 || remnant.getInt() != remnant.remaining())) {
                throw new IOException("TLS/SSL Handshake remnant is not a valid VoltDB message: " + remnant);
            }

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
                            sb.append(sourceNameAndIP);
                            sb.append(String.format(" after %.2f seconds (timeout target is %.2f seconds)", seconds, AUTH_TIMEOUT_MS / 1000.0));
                            timeoutRef.set(sb.toString());
                            try {
                                socket.close();
                            } catch (IOException e) {
                                //Don't care
                            }
                        }
                    }, AUTH_TIMEOUT_MS, 0, TimeUnit.MILLISECONDS);

            ByteBuffer message = remnant.hasRemaining() ? remnant : null;
            if (message != null) {
                byte [] todigest = new byte[message.limit()];
                message.position(0);
                message.get(todigest).position(4);
            }
            try {
                while (message == null) {
                    message = messagingChannel.readMessage();
                }
            } catch (IOException e) {
                // Don't log a stack trace - assume a security probe sent a bad packet or the connection timed out.
                try {
                    socket.close();
                } catch (IOException e1) {
                }
                return null;
            }

            /*
             * Since we got the login message, cancel the timeout.
             * If cancellation fails then the socket is dead and the connection lost
             */
            if (!timeoutFuture.cancel(false)) {
                return null;
            }

            int aversion = message.get(); //Get version
            ClientAuthScheme hashScheme = ClientAuthScheme.HASH_SHA1;
            //If auth version is more than zero we read auth hashing scheme.
            if (aversion > 0) {
                try {
                    hashScheme = ClientAuthScheme.get(message.get());
                } catch (IllegalArgumentException ex) {
                    authLog.warn("Failure to authenticate connection: Invalid Hash Scheme presented.");
                    //Send negative response
                    responseBuffer.put(WIRE_PROTOCOL_FORMAT_ERROR).flip();
                    messagingChannel.writeMessage(responseBuffer);
                    socket.close();
                    return null;
                }
            }
            //SHA1 is deprecated log it.
            if (hashScheme == ClientAuthScheme.HASH_SHA1) {
                authLog.rateLimitedWarn(60*60, "Client connected using deprecated SHA1 hashing. SHA2 is strongly recommended for all client connections. Client IP: %s",
                                        sourceIP);
            }
            FastDeserializer fds = new FastDeserializer(message);
            final String service = fds.readString();
            final String username = fds.readString();
            final String displayableUser = AuthSystem.displayableUser(username);
            final int digestLen = ClientAuthScheme.getDigestLength(hashScheme);
            final byte password[] = new byte[digestLen];
            //We should be left with SHA bytes only which varies based on scheme.
            if (message.remaining() != digestLen) {
                authLog.warnFmt("Failure to authenticate connection(%s): user %s failed authentication.",
                                sourceNameAndIP, displayableUser);
                //Send negative response
                responseBuffer.put(AUTHENTICATION_FAILURE).flip();
                messagingChannel.writeMessage(responseBuffer);
                socket.close();
                return null;
            }
            message.get(password);

            CatalogContext context = m_catalogContext.get();

            AuthProvider ap = null;
            try {
                ap = AuthProvider.fromService(service);
            } catch (IllegalArgumentException ex) {
                // handle it below
            }

            if (ap == null) {
                //Send negative response
                responseBuffer.put(EXPORT_DISABLED_REJECTION).flip();
                messagingChannel.writeMessage(responseBuffer);
                socket.close();
                authLog.warnFmt("Rejected user %s attempting to use disabled or unconfigured service %s.",
                                displayableUser, service);
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
                    arq = context.authSystem.new KerberosAuthenticationRequest(messagingChannel);
                } else {
                    arq = context.authSystem.new HashAuthenticationRequest(username, password);
                }
                /*
                 * Authenticate the user.
                 */
                boolean authenticated = arq.authenticate(hashScheme, sourceIP);
                if (!authenticated) {
                    // Failure has already been logged by AuthSystem

                    long timestamp = System.currentTimeMillis();
                    ScheduledExecutorService es = VoltDB.instance().getSES(false);
                    if (es != null && !es.isShutdown()) {
                        es.submit(new Runnable() {
                            @Override
                            public void run() {
                                // use 'displayableUser' for better logging by the FLC
                                ((RealVoltDB)VoltDB.instance()).logMessageToFLC(timestamp, displayableUser, sourceIP);
                            }
                        });
                    }

                    // This seems iffy to me: any I/O error on anything anywhere
                    // means we don't try to respond to the client?
                    boolean isItIo = false;
                    Exception faex = arq.getAuthenticationFailureException();
                    for (Throwable cause = faex; cause != null && !isItIo; cause = cause.getCause()) {
                        isItIo = cause instanceof IOException;
                    }

                    // Send negative response
                    if (!isItIo) {
                        responseBuffer.put(AUTHENTICATION_FAILURE).flip();
                        messagingChannel.writeMessage(responseBuffer);
                    }
                    socket.close();
                    return null;
                }
            } else {
                authLog.warnFmt("Failure to authenticate connection(%s) for user %s, because this node is rejoining.",
                                sourceNameAndIP, displayableUser);
                //Send negative response
                responseBuffer.put(AUTHENTICATION_FAILURE_DUE_TO_REJOIN).flip();
                messagingChannel.writeMessage(responseBuffer);
                socket.close();
                return null;
            }

            /*
             * Create an input handler.
             */
            ClientInputHandler handler = new ClientInputHandler(username, m_isAdmin);

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
            messagingChannel.writeMessage(responseBuffer);
            return handler;
        }

    }

    /** A port that reads client procedure invocations and writes responses */
    public class ClientInputHandler extends VoltProtocolHandler implements AdmissionControlGroup.ACGMember, InvocationClientHandler {
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

        @Override
        public boolean isAdmin()
        {
            return m_isAdmin;
        }

        public String getUserName()
        {
            return m_username;
        }

        @Override
        public int getMaxRead() {
            return Math.max( MAX_READ, getNextMessageLength());
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
                       new ClientInterfaceHandleManager( m_isAdmin, c, null, m_acg.get()));
            m_acg.get().addMember(this);
            if (!m_acg.get().hasBackPressure()) {
                c.enableReadSelection();
            }
        }

        @Override
        public void stopped(Connection c) {
            m_clientConnectionsTracker.connectionClosed();

            /*
             * It's necessary to free all the resources held by the IV2 ACG tracking.
             * Outstanding requests may actually still be at large
             */
            ClientInterfaceHandleManager cihm = m_cihm.remove(connectionId());
            // might be null if closing the interface in prep for self-kill / graceful shutdown
            if (cihm != null) {
                cihm.freeOutstandingTxns();
                cihm.m_acg.removeMember(this);
            }
            // if null, check to ensure this CI is stopping
            else if (m_isAcceptingConnections.get()) {
                log.error("NULL ClientInterfaceHandleManager for active ClientInterface unexepected.");
            }

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
    public class ClientResponseWork implements DeferredSerialization {
        private final ClientInterfaceHandleManager cihm;
        private final InitiateResponseMessage response;
        private final Procedure catProc;
        private ClientResponseImpl clientResponse;
        private boolean restartMispartitionedTxn;

        private ClientResponseWork(InitiateResponseMessage response,
                                   ClientInterfaceHandleManager cihm,
                                   Procedure catProc)
        {
            this.response = response;
            this.clientResponse = response.getClientResponseData();
            this.cihm = cihm;
            this.catProc = catProc;
            restartMispartitionedTxn = true;
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

        public void setRestartMispartitionedTxn(boolean restart) {
            restartMispartitionedTxn = restart;
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
            if (response.isMispartitioned() || response.isMisrouted()) {
                // If the transaction is restarted, don't send a response to the client yet.
                if (restartTransaction(clientData.m_messageSize, clientData.m_creationTimeNanos)) {
                    return DeferredSerialization.EMPTY_MESSAGE_LENGTH;
                }
            }

            final long now = System.nanoTime();
            final long delta = now - clientData.m_creationTimeNanos;

            /*
             * Log initiator stats
             */
            cihm.m_acg.logTransactionCompleted(
                    cihm.connection.connectionId(clientData.m_clientHandle),
                    cihm.connection.getHostnameOrIP(clientData.m_clientHandle),
                    clientData.m_procName,
                    delta,
                    clientResponse.getStatus());

            final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.CI);
            if (traceLog != null) {
                traceLog.add(() -> VoltTrace.endAsync("recvtxn",
                                                      clientData.m_clientHandle,
                                                      "status", Byte.toString(clientResponse.getStatus()),
                                                      "statusString", clientResponse.getStatusString()));
            }

            clientResponse.setClientHandle(clientData.m_clientHandle);
            clientResponse.setClusterRoundtrip((int)TimeUnit.NANOSECONDS.toMillis(delta));
            clientResponse.setHashes(null); // not part of wire protocol

            return clientResponse.getSerializedSize() + 4;
        }

        @Override
        public String toString() {
            return clientResponse.getClass().getName();
        }

        public ClientResponseImpl getClientResponse() {
            return clientResponse;
        }

        /**
         * Checks if the transaction needs to be restarted, if so, restart it.
         * @param messageSize the original message size when the invocation first came in
         * @return true if the transaction is restarted successfully, false otherwise.
         */
        private boolean restartTransaction(int messageSize, long nowNanos) {

            assert response.getInvocation() != null;
            assert response.getCurrentHashinatorConfig() != null;
            assert(catProc != null);

            // before rehashing, update the hashinator
            TheHashinator.updateHashinator(
                    TheHashinator.getConfiguredHashinatorClass(),
                    response.getCurrentHashinatorConfig().getFirst(), // version
                    response.getCurrentHashinatorConfig().getSecond(), // config bytes
                    false); // cooked (true for snapshot serialization only)

            if (!restartMispartitionedTxn) {
                // We are not restarting. So set mispartitioned status,
                // so that the caller can handle it correctly.
                clientResponse.setMispartitionedResult(response.getCurrentHashinatorConfig());
                return false;
            }
            // if we are recovering, the mispartitioned txn must come from the log,
            // don't restart it. The correct txn will be replayed by another node.
            if (VoltDB.instance().getMode() == OperationMode.INITIALIZING) {
                return false;
            }

            try {
                int partition = -1;
                if (catProc.getSinglepartition()
                        && (catProc.getPartitionparameter() == -1 || response.getInvocation().hasPartitionDestination())) {
                    // Directed procedure running on partition
                    partition = response.getInvocation().getPartitionDestination();
                    assert partition != -1;
                } else {
                     // Regular partitioned procedure
                    ProcedurePartitionInfo ppi = (ProcedurePartitionInfo)catProc.getAttachment();
                    Object invocationParameter = response.getInvocation().getParameterAtIndex(ppi.index);
                    partition = TheHashinator.getPartitionForParameter(
                          ppi.type, invocationParameter);
                }
                m_dispatcher.createTransaction(cihm.connection.connectionId(),
                        response.getInvocation(),
                        catProc.getReadonly(),
                        partition != MpInitiator.MP_INIT_PID, // Only SP could be mis-partitioned
                        false, // Only SP could be mis-partitioned
                        new int [] { partition },
                        messageSize,
                        nowNanos);
                return true;
            } catch (Exception e) {
                // unable to hash to a site, return an error
                assert(clientResponse == null || clientResponse.getStatus() == ClientResponse.TXN_MISROUTED);
                hostLog.warn("Unexpected error trying to restart misrouted txn", e);
                clientResponse = getMispartitionedErrorResponse(response.getInvocation(), catProc, e);
                return false;
            }
        }
    }

    CatalogContext getCatalogContext() {
        return m_catalogContext.get();
    }

    // Wrap API to SimpleDtxnInitiator - mostly for the future
    public CreateTransactionResult createTransaction(
            final long connectionId,
            final StoredProcedureInvocation invocation,
            final boolean isReadOnly,
            final boolean isSinglePartition,
            final boolean isEveryPartition,
            final int partition,
            final int messageSize,
            final long nowNanos)
    {
        return m_dispatcher.createTransaction(
                connectionId,
                Iv2InitiateTaskMessage.UNUSED_MP_TXNID,
                0, //unused timestammp
                invocation,
                isReadOnly,
                isSinglePartition,
                isEveryPartition,
                new int [] { partition },
                messageSize,
                nowNanos,
                false);  // is for replay.
    }

    // Wrap API to SimpleDtxnInitiator - mostly for the future
    public CreateTransactionResult createTransaction(
            final long connectionId,
            final long txnId,
            final long uniqueId,
            final StoredProcedureInvocation invocation,
            final boolean isReadOnly,
            final boolean isSinglePartition,
            final boolean isEveryPartition,
            final int partition,
            final int messageSize,
            long nowNanos,
            final boolean isForReplay)
    {
        return m_dispatcher.createTransaction(
                connectionId,
                txnId,
                uniqueId,
                invocation,
                isReadOnly,
                isSinglePartition,
                isEveryPartition,
                new int [] { partition },
                messageSize,
                nowNanos,
                isForReplay);
    }

    /**
     * Static factory method to easily create a ClientInterface with the default
     * settings.
     * @throws Exception
     */
    public static ClientInterface create(
            HostMessenger messenger,
            CatalogContext context,
            ReplicationRole replicationRole,
            Cartographer cartographer,
            InetAddress clientIntf,
            int clientPort,
            InetAddress adminIntf,
            int adminPort,
            SslContext SslContext) throws Exception {

        /*
         * Construct the runnables so they have access to the list of connections
         */
        final ClientInterface ci = new ClientInterface(
                clientIntf, clientPort, adminIntf, adminPort, context, messenger, replicationRole, cartographer,
                SslContext);

        return ci;
    }

    ClientInterface(InetAddress clientIntf, int clientPort, InetAddress adminIntf, int adminPort,
                    CatalogContext context, HostMessenger messenger, ReplicationRole replicationRole,
                    Cartographer cartographer) throws Exception {
        this(clientIntf, clientPort, adminIntf, adminPort, context, messenger, replicationRole, cartographer, null);
    }

    ClientInterface(InetAddress clientIntf, int clientPort, InetAddress adminIntf, int adminPort,
            CatalogContext context, HostMessenger messenger, ReplicationRole replicationRole,
            Cartographer cartographer, SslContext sslContext) throws Exception {
        m_catalogContext.set(context);
        m_snapshotDaemon = new SnapshotDaemon(context);
        m_snapshotDaemonAdapter = new SnapshotDaemonAdapter();
        m_cartographer = cartographer;

        // pre-allocate single partition array
        m_acceptor = new ClientAcceptor(clientIntf, clientPort, messenger.getNetwork(), false, sslContext);
        m_adminAcceptor = null;
        m_adminAcceptor = new ClientAcceptor(adminIntf, adminPort, messenger.getNetwork(), true, sslContext);

        // Create the per-partition adapters before creating the mailbox. Once
        // the mailbox is created, the master promotion notification may race
        // with this.
        m_internalConnectionHandler = new InternalConnectionHandler();
        for (int pid : m_cartographer.getPartitions()) {
            m_internalConnectionHandler.addAdapter(pid, createInternalAdapter(pid));
        }

        m_mailbox = new LocalMailbox(messenger,  messenger.getHSIdForLocalSite(HostMessenger.CLIENT_INTERFACE_SITE_ID)) {
            /** m_d only used in test */
            LinkedBlockingQueue<VoltMessage> m_d = new LinkedBlockingQueue<VoltMessage>();

            @Override
            public void deliver(final VoltMessage message) {
                if (message instanceof InitiateResponseMessage) {
                    // forward response; copy is annoying. want slice of response.
                    InitiateResponseMessage response = (InitiateResponseMessage)message;
                    StoredProcedureInvocation invocation = response.getInvocation();

                    // handle all host NT procedure callbacks
                    if (response.getClientConnectionId() == NT_REMOTE_PROC_CID) {
                        m_dispatcher.handleAllHostNTProcedureResponse(response.getClientResponseData());
                        return;
                    }

                    Iv2Trace.logFinishTransaction(response, m_mailbox.getHSId());
                    ClientInterfaceHandleManager cihm = m_cihm.get(response.getClientConnectionId());
                    Procedure procedure = null;

                    if (invocation != null) {
                        procedure = getProcedureFromName(invocation.getProcName());
                        assert (procedure != null);
                    }

                    //Can be null on hangup
                    if (cihm != null) {
                        //Pass it to the network thread like a ninja
                        //Only the network can use the CIHM
                        cihm.connection.writeStream().fastEnqueue(new ClientResponseWork(response, cihm, procedure));
                        Iv2Trace.logFinishTransaction(response, m_mailbox.getHSId());
                    }
                }
                else if (message instanceof BinaryPayloadMessage) {
                    handlePartitionFailOver((BinaryPayloadMessage)message);
                }
                else if (message instanceof MigratePartitionLeaderMessage) {
                    processMigratePartitionLeaderTask((MigratePartitionLeaderMessage)message);
                }
                /*
                 * InitiateTaskMessage only get delivered here for all-host NT proc calls.
                 */
                else if (message instanceof Iv2InitiateTaskMessage) {
                    final Iv2InitiateTaskMessage itm = (Iv2InitiateTaskMessage) message;
                    final StoredProcedureInvocation invocation = itm.getStoredProcedureInvocation();

                    // get hostid for this node
                    final int hostId = CoreUtils.getHostIdFromHSId(m_mailbox.getHSId());

                    final ProcedureCallback cb = new ProcedureCallback() {
                        @Override
                        public void clientCallback(ClientResponse clientResponse) throws Exception {
                            InitiateResponseMessage responseMessage = new InitiateResponseMessage(itm);
                            // use the app status string to store the host id (as a string)
                            // ProcedureRunnerNT has a method that expects this hack
                            ((ClientResponseImpl) clientResponse).setAppStatusString(String.valueOf(hostId));
                            responseMessage.setResults((ClientResponseImpl) clientResponse);
                            responseMessage.setClientHandle(invocation.clientHandle);
                            responseMessage.setConnectionId(NT_REMOTE_PROC_CID);
                            m_mailbox.send(itm.m_sourceHSId, responseMessage);
                        }
                    };

                    m_dispatcher.getInternalAdapterNT().callProcedure(m_catalogContext.get().authSystem.getInternalAdminUser(),
                            true, 1000 * 120, cb, invocation.getProcName(), itm.getParameters());
                } else if (message instanceof SiteFailureForwardMessage) {
                    SiteFailureForwardMessage msg = (SiteFailureForwardMessage)message;
                    m_messenger.notifyOfHostDown(CoreUtils.getHostIdFromHSId(msg.m_reportingHSId));
                } else if (message instanceof HashMismatchMessage) {
                    processReplicaRemovalTask((HashMismatchMessage)message);
                } else {
                    // m_d is for test only
                    m_d.offer(message);
                }
            }

            /** This method only used in test */
            @Override
            public VoltMessage recv() {
                return m_d.poll();
            }
        };
        messenger.createMailbox(m_mailbox.getHSId(), m_mailbox);
        m_zk = messenger.getZK();
        m_siteId = m_mailbox.getHSId();

        m_executeTaskAdpater = new SimpleClientResponseAdapter(ClientInterface.EXECUTE_TASK_CID, "ExecuteTaskAdapter", true);
        bindAdapter(m_executeTaskAdpater, null);

        m_dispatcher = InvocationDispatcher.builder()
                .clientInterface(this)
                .snapshotDaemon(m_snapshotDaemon)
                .replicationRole(replicationRole)
                .cartographer(m_cartographer)
                .catalogContext(m_catalogContext)
                .mailbox(m_mailbox)
                .clientInterfaceHandleManagerMap(m_cihm)
                .siteId(m_siteId)
                .build();
        // add client interface mailbox id to ZK for NT proc
        VoltZK.registerMailBoxForNT(m_zk, m_siteId);
    }

    private InternalClientResponseAdapter createInternalAdapter(int pid) {
        InternalClientResponseAdapter internalAdapter = new InternalClientResponseAdapter(INTERNAL_CID + pid);
        bindAdapter(internalAdapter, null, true);
        return internalAdapter;
    }

    public InternalConnectionHandler getInternalConnectionHandler() {
        return m_internalConnectionHandler;
    }

    private void handlePartitionFailOver(BinaryPayloadMessage message) {
        try {
            JSONObject jsObj = new JSONObject(new String(message.m_payload, "UTF-8"));
            final int partitionId = jsObj.getInt(Cartographer.JSON_PARTITION_ID);
            final long initiatorHSId = jsObj.getLong(Cartographer.JSON_INITIATOR_HSID);
            final boolean leaderMigration = jsObj.getBoolean(Cartographer.JSON_LEADER_MIGRATION);
            for (final ClientInterfaceHandleManager cihm : m_cihm.values()) {
                try {
                    cihm.connection.queueTask(new Runnable() {
                        @Override
                        public void run() {
                            if (leaderMigration) {
                                if (cihm.repairCallback != null) {
                                    cihm.repairCallback.leaderMigrated(partitionId, initiatorHSId);
                                }
                            } else {
                                failOverConnection(partitionId, initiatorHSId, cihm.connection);
                            }
                        }
                    });
                } catch (UnsupportedOperationException ignore) {
                    // In case some internal connections don't implement queueTask()
                    if (leaderMigration) {
                        if (cihm.repairCallback != null) {
                            cihm.repairCallback.leaderMigrated(partitionId, initiatorHSId);
                        }
                    } else {
                        failOverConnection(partitionId, initiatorHSId, cihm.connection);
                    }
                }
            }

            // Create adapters here so that it works for elastic add.
            if (!m_internalConnectionHandler.hasAdapter(partitionId)) {
                m_internalConnectionHandler.addAdapter(partitionId, createInternalAdapter(partitionId));
            }
        } catch (Exception e) {
            hostLog.warn("Error handling partition fail over at ClientInterface, continuing anyways", e);
        }
    }

    /*
     * When partition mastership for a partition changes, check all outstanding
     * requests for that partition and if they aren't for the current partition master,
     * drop them and send an error response.
     */
    private void failOverConnection(Integer partitionId, Long initiatorHSId, Connection c) {
        ClientInterfaceHandleManager cihm = m_cihm.get(c.connectionId());
        if (cihm == null) {
            return;
        }

        List<Iv2InFlight> transactions =
                cihm.removeHandlesForPartitionAndInitiator(partitionId, initiatorHSId);

        if (!transactions.isEmpty()) {
            Iv2Trace.logFailoverTransaction(partitionId, initiatorHSId, transactions.size());
        }

        for (Iv2InFlight inFlight : transactions) {
            ClientResponseImpl response =
                    new ClientResponseImpl(
                            ClientResponseImpl.RESPONSE_UNKNOWN,
                            ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                            null,
                            new VoltTable[0],
                            DROP_TXN_MASTERSHIP);
            response.setClientHandle(inFlight.m_clientHandle);
            ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
            buf.putInt(buf.capacity() - 4);
            response.flattenToBuffer(buf);
            buf.flip();
            c.writeStream().enqueue(buf);
        }

        if (cihm.repairCallback != null) {
            cihm.repairCallback.repairCompleted(partitionId, initiatorHSId);
        }
    }

    /**
     * Called when the replication role of the cluster changes.
     * @param role
     */
    public void setReplicationRole(ReplicationRole role) {
        m_dispatcher.setReplicationRole(role);
    }

    /**
     * Initializes the snapshot daemon so that it's ready to take snapshots
     */
    public void initializeSnapshotDaemon(HostMessenger messenger, GlobalServiceElector gse) {
        m_snapshotDaemon.init(this, messenger, new Runnable() {
            @Override
            public void run() {
                bindAdapter(m_snapshotDaemonAdapter, null);
            }
        },
        gse);
    }

    /**
     * Tell the clientInterface about a connection adapter.
     */
    public ClientInterfaceHandleManager bindAdapter(final Connection adapter, final ClientInterfaceRepairCallback repairCallback) {
        return bindAdapter(adapter, repairCallback, false);
    }

    private ClientInterfaceHandleManager bindAdapter(final Connection adapter, final ClientInterfaceRepairCallback repairCallback, boolean addAcg) {
        if (m_cihm.get(adapter.connectionId()) == null) {
            AdmissionControlGroup acg = AdmissionControlGroup.getDummy();
            ClientInterfaceHandleManager cihm = ClientInterfaceHandleManager.makeThreadSafeCIHM(true, adapter, repairCallback, acg);
            if (addAcg) {
                m_allACGs.add(acg);
            }
            m_cihm.put(adapter.connectionId(), cihm);
        }
        return m_cihm.get(adapter.connectionId());
    }

    public void unbindAdapter(final Connection adapter) {
        ClientInterfaceHandleManager cihm = m_cihm.remove(adapter.connectionId());
        if (cihm != null) {
            m_clientConnectionsTracker.connectionClosed();

            /*
             * It's necessary to free all the resources held
             * Outstanding requests may actually still be at large
             */
            m_allACGs.remove(cihm.m_acg);
            m_notifier.removeConnection(adapter);
            cihm.freeOutstandingTxns();
        }
    }

    // if this ClientInterface's site ID is the lowest non-execution site ID
    // in the cluster, make our SnapshotDaemon responsible for snapshots
    public void mayActivateSnapshotDaemon() {
        SnapshotSchedule schedule = m_catalogContext.get().database.getSnapshotschedule().get("default");
        if (schedule != null)
        {
            final ListenableFuture<Void> future = m_snapshotDaemon.mayGoActiveOrInactive(schedule);
            future.addListener(new Runnable() {
                @Override
                public void run() {
                    try {
                        future.get();
                    } catch (InterruptedException e) {
                        VoltDB.crashLocalVoltDB("Failed to make SnapshotDaemon active", false, e);
                    } catch (ExecutionException e) {
                        VoltDB.crashLocalVoltDB("Failed to make SnapshotDaemon active", false, e);
                    }
                }
            }, CoreUtils.SAMETHREADEXECUTOR);
        }
    }

    /**
     * Returns the {@link InvocationDispatcher} used to route and issue {@link StoredProcedureInvocation}s
     * @return the {@link InvocationDispatcher} used to route and issue {@link StoredProcedureInvocation}s
     */
    public final InvocationDispatcher getDispatcher() {
        return m_dispatcher;
    }

    /**
     * Set the flag that tells this client interface to update its
     * catalog when it's threadsafe.
     */
    public void notifyOfCatalogUpdate() {
        m_catalogContext.set(VoltDB.instance().getCatalogContext());
        /*
         * Update snapshot daemon settings.
         *
         * Don't do it if the system is still initializing (CL replay),
         * because snapshot daemon may call @SnapshotScan on activation and
         * it will mess replaying txns up.
         */
        if (VoltDB.instance().getMode() != OperationMode.INITIALIZING) {
            mayActivateSnapshotDaemon();

            //add a notification to client right away
            StoredProcedureInvocation spi = new StoredProcedureInvocation();
            spi.setProcName("@SystemCatalog");
            spi.setParams("PROCEDURES");
            spi.setClientHandle(ASYNC_PROC_HANDLE);
            notifyClients(m_currentProcValues,m_currentProcSupplier,
                           spi, OpsSelector.SYSTEMCATALOG);
        }
    }

    private ClientResponseImpl errorResponse(Connection c, long handle, byte status, String reason, Exception e, boolean log) {
        String realReason = reason;
        if (e != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            realReason = sw.toString();
        }
        if (log) {
            hostLog.warn(realReason);
        }
        return new ClientResponseImpl(status,
                new VoltTable[0], realReason, handle);
    }

    /**
     *
     * * return True if an error was generated and needs to be returned to the client
     */
    final ClientResponseImpl handleRead(ByteBuffer buf, ClientInputHandler handler, Connection ccxn) {
        StoredProcedureInvocation task = new StoredProcedureInvocation();
        try {
            task.initFromBuffer(buf);
        } catch (Exception ex) {
            return new ClientResponseImpl(
                    ClientResponseImpl.UNEXPECTED_FAILURE,
                    new VoltTable[0], ex.getMessage(), ccxn.connectionId());
        }
        AuthUser user =  m_catalogContext.get().authSystem.getUser(handler.getUserName());
        if (user == null) {
            String errorMessage = "User " + handler.getUserName() + " has been removed from the system via a catalog update";
            authLog.info(errorMessage);
            return errorResponse(ccxn, task.clientHandle, ClientResponse.UNEXPECTED_FAILURE, errorMessage, null, false);
        }

        final ClientResponseImpl errResp = m_dispatcher.dispatch(task, handler, ccxn, user, null, false);

        if (errResp != null) {
            final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.CI);
            if (traceLog != null) {
                traceLog.add(() -> VoltTrace.endAsync("recvtxn",
                                                      task.getClientHandle(),
                                                      "status", Byte.toString(errResp.getStatus()),
                                                      "statusString", errResp.getStatusString()));
            }
        }

        return errResp;
    }

    public Procedure getProcedureFromName(String procName) {
        return InvocationDispatcher.getProcedureFromName(procName, m_catalogContext.get());
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

    /*
     * Boiler plate for a supplier to provide to the client notifier that allows new versions of
     * the topology to be published to the supplier
     *
     * Also a predicate for filtering out clients that don't actually want the updates
     */
    private final AtomicReference<DeferredSerialization> m_currentTopologyValues =
            new AtomicReference<>(null);
    private final Supplier<DeferredSerialization> m_currentTopologySupplier = new Supplier<DeferredSerialization>() {
        @Override
        public DeferredSerialization get() {
            return m_currentTopologyValues.get();
        }
    };

    private final AtomicReference<DeferredSerialization> m_currentProcValues = new AtomicReference<>(null);
    private final Supplier<DeferredSerialization> m_currentProcSupplier = new Supplier<DeferredSerialization>() {
        @Override
        public DeferredSerialization get() {
            return m_currentProcValues.get();
        }
    };

    /*
     * A predicate to allow the client notifier to skip clients
     * that don't want a specific kind of update
     */
    private final Predicate<ClientInterfaceHandleManager> m_wantsTopologyUpdatesPredicate =
            new Predicate<ClientInterfaceHandleManager>() {
        @Override
        public boolean apply(ClientInterfaceHandleManager input) {
            return input.wantsTopologyUpdates();
        }
    };

    /*
     * Submit a task to the stats agent to retrieve the topology and procedures. Supply a dummy
     * client response adapter to fake a connection. The adapter converts the response
     * to a listenable future and we add a listener to pick up the resulting topology
     * and check if it has changed. If it has changed, queue a task to the notifier
     * to propagate the update to clients.
     */
    private void checkForTopologyChanges() {
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setProcName("@Statistics");
        spi.setParams("TOPO", 0);
        spi.setClientHandle(ASYNC_TOPO_HANDLE);
        notifyClients(m_currentTopologyValues,m_currentTopologySupplier,
                         spi, OpsSelector.STATISTICS);

        spi = new StoredProcedureInvocation();
        spi.setProcName("@SystemCatalog");
        spi.setParams("PROCEDURES");
        spi.setClientHandle(ASYNC_PROC_HANDLE);
        notifyClients(m_currentProcValues,m_currentProcSupplier,
                        spi, OpsSelector.SYSTEMCATALOG);
    }

    private void notifyClients( AtomicReference<DeferredSerialization> values,
                                Supplier<DeferredSerialization> supplier,
                                StoredProcedureInvocation spi,
                                OpsSelector selector) {
        final Pair<SimpleClientResponseAdapter, ListenableFuture<ClientResponseImpl>> p =
                SimpleClientResponseAdapter.getAsListenableFuture();
        final ListenableFuture<ClientResponseImpl> fut = p.getSecond();
        fut.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                    final ClientResponseImpl r = fut.get();
                    if (r.getStatus() != ClientResponse.SUCCESS) {
                        hostLog.warn("Received error response retrieving stats info: " + r.getStatusString());
                        return;
                    }

                    final int size = r.getSerializedSize();
                    final ByteBuffer buf = ByteBuffer.allocate(size + 4);
                    buf.putInt(size);
                    r.flattenToBuffer(buf);
                    buf.flip();

                    //Check for no change
                    ByteBuffer oldValue = null;
                    DeferredSerialization ds = values.get();
                    if (ds != null) {
                        oldValue = ByteBuffer.allocate(ds.getSerializedSize());
                        ds.serialize(oldValue);
                        oldValue.flip();
                        if (buf.equals(oldValue)) {
                            return;
                        }
                    }

                    values.set(new DeferredSerialization() {
                        @Override
                        public void serialize(ByteBuffer outbuf) throws IOException {
                            outbuf.put(buf.duplicate());
                        }
                        @Override
                        public void cancel() {}

                        @Override
                        public int getSerializedSize() {return buf.remaining();}
                    });
                    if (oldValue != null) {
                        m_notifier.queueNotification(
                                m_cihm.values(),
                                supplier,
                                m_wantsTopologyUpdatesPredicate);
                    }
                } catch (Throwable t) {
                    hostLog.error("Error checking for updates", Throwables.getRootCause(t));
                }
            }
        }, CoreUtils.SAMETHREADEXECUTOR);
        InvocationDispatcher.dispatchStatistics(selector, spi, p.getFirst());
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
        if (m_acceptor != null) {
            m_acceptor.shutdown();
        }
        if (m_adminAcceptor != null)
        {
            m_adminAcceptor.shutdown();
        }
        if (m_snapshotDaemon != null) {
            m_snapshotDaemon.shutdown();
        }

        if (m_migratePartitionLeaderExecutor != null) {
            m_migratePartitionLeaderExecutor.shutdown();
        }
        if (m_replicaRemovalExecutor != null) {
            m_replicaRemovalExecutor.shutdown();
            m_replicaRemovalExecutor = null;
        }
        m_notifier.shutdown();
    }

    public void startAcceptingConnections() throws IOException {
        Future<?> replicaFuture = m_dispatcher.asynchronouslyDetermineLocalReplicas();

        /*
         * Periodically check the limit on the number of open files as well as number of open files itself.
         */
        m_fileDescriptorTracker.start();

        m_acceptor.start();
        if (m_adminAcceptor != null)
        {
            m_adminAcceptor.start();
        }
        mayActivateSnapshotDaemon();
        m_notifier.start();

        try {
            replicaFuture.get();
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while determining local replicas",e);
        } catch (ExecutionException e) {
            throw new IOException("Failed to determine local replicas", e.getCause());
        }

        m_isAcceptingConnections.compareAndSet(false, true);
    }

    public boolean isAcceptingConnections() {
        return m_isAcceptingConnections.get();
    }

    @Override
    public void initiateSnapshotDaemonWork(final String procedureName, long clientData, final Object params[]) {
        final Config sysProc = SystemProcedureCatalog.listing.get(procedureName);
        if (sysProc == null) {
            throw new RuntimeException("SnapshotDaemon attempted to invoke " + procedureName +
            " which is not a known procedure");
        }
        Procedure catProc = sysProc.asCatalogProcedure();
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setProcName(procedureName);
        spi.params = new FutureTask<ParameterSet>(new Callable<ParameterSet>() {
            @Override
            public ParameterSet call() {
                ParameterSet paramSet = ParameterSet.fromArrayWithCopy(params);
                return paramSet;
            }
        });
        spi.clientHandle = clientData;
        // Ugh, need to consolidate this with handleRead() somehow but not feeling it at the moment
        if (procedureName.equals("@SnapshotScan")) {
            InvocationDispatcher.dispatchStatistics(OpsSelector.SNAPSHOTSCAN, spi, m_snapshotDaemonAdapter);
            return;
        }
        else if (procedureName.equals("@SnapshotDelete")) {
            InvocationDispatcher.dispatchStatistics(OpsSelector.SNAPSHOTDELETE, spi, m_snapshotDaemonAdapter);
            return;
        }
        // initiate the transaction
        m_dispatcher.createTransaction(m_snapshotDaemonAdapter.connectionId(),
                spi, catProc.getReadonly(),
                catProc.getSinglepartition(), catProc.getEverysite(),
                new int[] { 0 }, // partition id
                0, System.nanoTime());
    }

    /**
     * A dummy connection to provide to the DTXN. It routes
     * ClientResponses back to the daemon
     *
     */
    private class SnapshotDaemonAdapter implements Connection, WriteStream {

        @Override
        public void disableReadSelection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enableReadSelection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void disableWriteSelection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enableWriteSelection() {
            throw new UnsupportedOperationException();
        }

        @Override
        public NIOReadStream readStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public WriteStream writeStream() {
            return this;
        }

        @Override
        public int calculatePendingWriteDelta(long now) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hadBackPressure() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getHostnameAndIPAndPort() {
            return "SnapshotDaemon";
        }

        @Override
        public String getHostnameOrIP() {
            return "SnapshotDaemon";
        }

        @Override
        public String getHostnameOrIP(long clientHandle) {
            return getHostnameOrIP();
        }

        @Override
        public int getRemotePort() {
            return -1;
        }

        @Override
        public InetSocketAddress getRemoteSocketAddress() {
            return null;
        }

        @Override
        public Future<?> unregister() {
            return null;
        }

        @Override
        public long connectionId()
        {
            return Long.MIN_VALUE;
        }

        @Override
        public long connectionId(long clientHandle) {
            return connectionId();
        }

        @Override
        public int getOutstandingMessageCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void fastEnqueue(final org.voltcore.utils.DeferredSerialization ds) {
            enqueue(ds);
        }

        @Override
        public void enqueue(final org.voltcore.utils.DeferredSerialization ds)
        {

            m_snapshotDaemon.processClientResponse(new Callable<ClientResponseImpl>() {
                @Override
                public ClientResponseImpl call() throws Exception {
                    ClientResponseImpl resp = new ClientResponseImpl();
                    ByteBuffer b = ByteBuffer.allocate(ds.getSerializedSize());
                    ds.serialize(b);
                    b.position(4);
                    resp.initFromBuffer(b);
                    return resp;
                }
            });
        }

        @Override
        public void enqueue(final ByteBuffer b)
        {
            m_snapshotDaemon.processClientResponse(new Callable<ClientResponseImpl>() {
                @Override
                public ClientResponseImpl call() throws Exception {
                    ClientResponseImpl resp = new ClientResponseImpl();
                    b.position(4);
                    resp.initFromBuffer(b);
                    return resp;
                }
            });
        }

        @Override
        public void enqueue(ByteBuffer[] b)
        {
            if (b.length == 1)
            {
                // Buffer chains are currently not used, just hand the first
                // buffer to the single buffer handler
                enqueue(b[0]);
            }
            else
            {
                log.error("Something is using buffer chains with enqueue");
            }
        }

        @Override
        public void queueTask(Runnable r) {
            // Called when node failure happens
            r.run();
        }
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

    public SnapshotDaemon getSnapshotDaemon() {
        return m_snapshotDaemon;
    }

    /**
     * Send a command log replay sentinel to the given partition.
     * @param uniqueId
     * @param partitionId
     */
    public void sendSentinel(long uniqueId, int partitionId) {
        m_dispatcher.sendSentinel(uniqueId, partitionId);
    }

    /**
     * Sends an end of log message to the master of that partition. This should
     * only be called at the end of replay.
     *
     * @param partitionId
     */
    public void sendEOLMessage(int partitionId) {
        final Long initiatorHSId = m_cartographer.getHSIdForMaster(partitionId);
        if (initiatorHSId == null) {
            log.warn("ClientInterface.sendEOLMessage: Master does not exist for partition: " + partitionId);
        } else {
            Iv2EndOfLogMessage message = new Iv2EndOfLogMessage(partitionId);
            m_mailbox.send(initiatorHSId, message);
        }
    }

    public List<Iterator<Map.Entry<Long, Map<String, InvocationInfo>>>> getIV2InitiatorStats() {
        ArrayList<Iterator<Map.Entry<Long, Map<String, InvocationInfo>>>> statsIterators =
                new ArrayList<Iterator<Map.Entry<Long, Map<String, InvocationInfo>>>>();
        for(AdmissionControlGroup acg : m_allACGs) {
            statsIterators.add(acg.getInitiationStatsIterator());
        }
        return statsIterators;
    }

    public FileDescriptorsTracker getFileDescriptorTracker() {
        return m_fileDescriptorTracker;
    }

    public ClientConnectionsTracker getClientConnectionsTracker() {
        return m_clientConnectionsTracker;
    }

    public List<AbstractHistogram> getLatencyStats() {
        List<AbstractHistogram> latencyStats = new ArrayList<AbstractHistogram>();
        for (AdmissionControlGroup acg : m_allACGs) {
            latencyStats.add(acg.getLatencyInfo());
        }
        return latencyStats;
    }

    //Generate a mispartitioned response also log the message.
    private ClientResponseImpl getMispartitionedErrorResponse(StoredProcedureInvocation task,
            Procedure catProc, Exception ex) {
        Object invocationParameter = null;
        try {
            invocationParameter = task.getParameterAtIndex(catProc.getPartitionparameter());
        } catch (Exception ex2) {
        }
        String exMsg = "Unknown";
        if (ex != null) {
            exMsg = ex.getMessage();
        }
        String errorMessage = "Error sending procedure " + task.getProcName()
                + " to the correct partition. Make sure parameter values are correct."
                + " Parameter value " + invocationParameter
                + ", partition column " + catProc.getPartitioncolumn().getName()
                + " type " + catProc.getPartitioncolumn().getType()
                + " Message: " + exMsg;
        authLog.warn(errorMessage);
        ClientResponseImpl clientResponse = new ClientResponseImpl(ClientResponse.UNEXPECTED_FAILURE,
                new VoltTable[0], errorMessage, task.clientHandle);
        return clientResponse;
    }

    /**
     * Call @ExecuteTask to generate a MP transaction.
     *
     * @param timeoutMS  timeout in milliseconds
     * @param params  actual parameter(s) for sub task to run
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public ClientResponse callExecuteTask(long timeoutMS, byte[] params) throws IOException, InterruptedException {
        SimpleClientResponseAdapter.SyncCallback syncCb = new SimpleClientResponseAdapter.SyncCallback();
        callExecuteTaskAsync(syncCb, params);
        return syncCb.getResponse(timeoutMS);
    }

    /**
     * Asynchronous version, call @ExecuteTask to generate a MP transaction.
     *
     * @param cb  maximum timeout in milliseconds
     * @param params  actual parameter(s) for sub task to run
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public void callExecuteTaskAsync(SimpleClientResponseAdapter.Callback cb, byte[] params) throws IOException {
        final String procedureName = "@ExecuteTask";
        Config procedureConfig = SystemProcedureCatalog.listing.get(procedureName);
        Procedure proc = procedureConfig.asCatalogProcedure();
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setProcName(procedureName);
        spi.setParams(params);
        spi.setClientHandle(m_executeTaskAdpater.registerCallback(cb));
        if (spi.getSerializedParams() == null) {
            spi = MiscUtils.roundTripForCL(spi);
        }
        synchronized (m_executeTaskAdpater) {
            m_dispatcher.createTransaction(m_executeTaskAdpater.connectionId(), spi,
                    proc.getReadonly(), proc.getSinglepartition(), proc.getEverysite(),
                    new int[] { 0 } /* Can provide anything for multi-part */,
                    spi.getSerializedSize(), System.nanoTime());
        }
    }

    /**
     * This is not designed to be a safe shutdown.
     * This is designed to stop sending messages to clients as fast as possible.
     * It is currently called from VoltDB.crash...
     *
     * Note: this really needs to work. We CAN'T respond back to the client anything
     * after we've decided to crash or it might break some of our contracts.
     *
     * @return false if we can't be assured this safely worked
     */
    public boolean ceaseAllPublicFacingTrafficImmediately() {
        try {
            if (m_acceptor != null) {
                // This call seems to block until the shutdown is done
                // which is good becasue we assume there will be no new
                // connections afterward
                m_acceptor.shutdown();
            }
            if (m_adminAcceptor != null) {
                m_adminAcceptor.shutdown();
            }
        }
        catch (InterruptedException e) {
            // this whole method is really a best effort kind of thing...
            log.error(e);
            // if we didn't succeed, let the caller know and take action
            return false;
        }
        finally {
            m_isAcceptingConnections.set(false);
            // this feels like an unclean thing to do... but should work
            // for the purposes of cutting all responses right before we deliberately
            // end the process
            // m_cihm itself is thread-safe, and the regular shutdown code won't
            // care if it's empty... so... this.
            m_cihm.clear();
        }

        return true;
    }

    public AuthUser getInternalUser() {
        return m_catalogContext.get().authSystem.getInternalAdminUser();
    }

    void handleFailedHosts(Set<Integer> failedHosts) {
        m_dispatcher.handleFailedHosts(failedHosts);
    }

    //start or stop MigratePartitionLeader task
    void processMigratePartitionLeaderTask(MigratePartitionLeaderMessage message) {
        synchronized(m_lock) {
            //start MigratePartitionLeader service
            if (message.startMigratingPartitionLeaders()) {
                if (m_migratePartitionLeaderExecutor == null) {
                    m_migratePartitionLeaderExecutor = Executors.newSingleThreadScheduledExecutor(CoreUtils.getThreadFactory("MigratePartitionLeader"));
                    final int interval = Integer.parseInt(System.getProperty("MIGRATE_PARTITION_LEADER_INTERVAL", "1"));
                    final int delay = Integer.parseInt(System.getProperty("MIGRATE_PARTITION_LEADER_DELAY", "1"));
                    m_migratePartitionLeaderExecutor.scheduleAtFixedRate(
                            () -> {
                                try {
                                    startMigratePartitionLeader(message.isForStopNode());
                                } catch (Exception e) {
                                    tmLog.error("Migrate partition leader encountered unexpected error", e);
                                } catch (Throwable t) {
                                    VoltDB.crashLocalVoltDB("Migrate partition leader encountered unexpected error",
                                            true, t);
                                }
                            },
                            delay, interval, TimeUnit.SECONDS);
                }
                hostLog.info("MigratePartitionLeader task is started.");
                return;
            }

            //stop MigratePartitionLeader service
            if (m_migratePartitionLeaderExecutor != null ) {
                m_migratePartitionLeaderExecutor.shutdown();
                m_migratePartitionLeaderExecutor = null;
                hostLog.info("MigratePartitionLeader task is stopped.");
            }
        }
    }

    /**Move partition leader from one host to another.
     * find a partition leader from a host which hosts the most partition leaders
     * and find the host which hosts the partition replica and the least number of partition leaders.
     * send MigratePartitionLeaderMessage to the host with older partition leader to initiate @MigratePartitionLeader
     * Repeatedly call this task until no qualified partition is available.
     * @param prepareStopNode if true, only move partition leaders on this host to other hosts-used via @PrepareStopNode
     * Otherwise, balance the partition leaders among all nodes.
     */
    void startMigratePartitionLeader(boolean prepareStopNode) {
        RealVoltDB voltDB = (RealVoltDB)VoltDB.instance();
        final int hostId = CoreUtils.getHostIdFromHSId(m_siteId);
        Pair<Integer, Integer> target = null;
        if (prepareStopNode) {
            target = m_cartographer.getPartitionLeaderMigrationTargetForStopNode(hostId);
        } else {
            if (voltDB.isClusterComplete()) {
                target = m_cartographer.getPartitionLeaderMigrationTarget(voltDB.getHostCount(), hostId, prepareStopNode);
            } else {
                // Out of the scheduled task
                target = new Pair<Integer, Integer> (-1, -1);
            }
        }

        //The host does not have any thing to do this time. It does not mean that the host does not
        //have more partition leaders than expected. Other hosts may have more partition leaders
        //than this one. So let other hosts do @MigratePartitionLeader first.
        if (target == null) {
            return;
        }

        final int partitionId = target.getFirst();
        final int targetHostId = target.getSecond();
        int partitionKey = -1;

        //MigratePartitionLeader is completed or there are hosts down. Stop MigratePartitionLeader service on this host
        if (targetHostId == -1 || (!prepareStopNode && !voltDB.isClusterComplete())) {
            voltDB.scheduleWork(
                    () -> {m_mailbox.deliver(new MigratePartitionLeaderMessage());},
                    0, 0, TimeUnit.SECONDS);
            return;
        }

        //Others may also iterate through the partition keys. So make a copy and find the key
        VoltTable partitionKeys = TheHashinator.getPartitionKeys(VoltType.INTEGER);
        ByteBuffer buf = ByteBuffer.allocate(partitionKeys.getSerializedSize());
        partitionKeys.flattenToBuffer(buf);
        buf.flip();
        VoltTable keyCopy = PrivateVoltTableFactory.createVoltTableFromSharedBuffer(buf);
        keyCopy.resetRowPosition();
        while (keyCopy.advanceRow()) {
            if (partitionId == keyCopy.getLong("PARTITION_ID")) {
                partitionKey = (int)(keyCopy.getLong("PARTITION_KEY"));
                break;
            }
        }

        if (partitionKey == -1) {
            tmLog.warn("Could not find the partition key for partition " + partitionId);
            return;
        }

        //grab a lock
        String errorMessage = VoltZK.createActionBlocker(m_zk, VoltZK.migratePartitionLeaderBlocker,
                CreateMode.EPHEMERAL, tmLog,
                "Migrate Partition Leader");
        if (errorMessage != null) {
            tmLog.rateLimitedInfo(60, errorMessage);
            return;
        }

        if (tmLog.isDebugEnabled()) {
            tmLog.debug(String.format("Move the leader of partition %d to host %d", partitionId, targetHostId));
            VoltTable vt = Cartographer.peekTopology(m_cartographer);
            tmLog.debug("[@MigratePartitionLeader]\n" + vt.toFormattedString());
        }

        boolean transactionStarted = false;
        Long targetHSId = m_cartographer.getHSIDForPartitionHost(targetHostId, partitionId);
        if (targetHSId == null) {
            if (tmLog.isDebugEnabled()) {
                tmLog.debug(String.format("Partition %d is no longer on host %d", partitionId, targetHostId));
            }
            return;
        }
        try {
            SimpleClientResponseAdapter.SyncCallback cb = new SimpleClientResponseAdapter.SyncCallback();
            final String procedureName = "@MigratePartitionLeader";
            Config procedureConfig = SystemProcedureCatalog.listing.get(procedureName);
            StoredProcedureInvocation spi = new StoredProcedureInvocation();
            spi.setProcName(procedureName);
            spi.setClientHandle(m_executeTaskAdpater.registerCallback(cb));
            spi.setParams(partitionKey, partitionId, targetHostId);
            if (spi.getSerializedParams() == null) {
                spi = MiscUtils.roundTripForCL(spi);
            }

            //Info saved for the node failure handling
            MigratePartitionLeaderInfo spiInfo = new MigratePartitionLeaderInfo(
                    m_cartographer.getHSIDForPartitionHost(hostId, partitionId),
                    targetHSId,
                    partitionId);
            VoltZK.createMigratePartitionLeaderInfo(m_zk, spiInfo);

            notifyPartitionMigrationStatus(partitionId, targetHSId, false);

            if (Boolean.getBoolean("TEST_MIGRATION_FAILURE")) {
                Thread.sleep(100);
                throw new IOException("failure simulation");
            }
            synchronized (m_executeTaskAdpater) {
                if (createTransaction(m_executeTaskAdpater.connectionId(),
                        spi,
                        procedureConfig.getReadonly(),
                        procedureConfig.getSinglepartition(),
                        procedureConfig.getEverysite(),
                        partitionId,
                        spi.getSerializedSize(),
                        System.nanoTime()) != CreateTransactionResult.SUCCESS) {
                    tmLog.warn(String.format("Failed to start transaction for migration of partition %d to host %d",
                            partitionId, targetHostId));
                    notifyPartitionMigrationStatus(partitionId, targetHSId, true);
                    return;
                }
            }

            transactionStarted = true;

            final long timeoutMS = 5 * 60 * 1000;
            ClientResponse resp = cb.getResponse(timeoutMS);
            if (resp != null && resp.getStatus() == ClientResponse.SUCCESS) {
                tmLog.info(String.format("The partition leader for %d has been moved to host %d.",
                        partitionId, targetHostId));
            } else {
                //not necessary a failure.
                tmLog.warn(String.format("Fail to move the leader of partition %d to host %d. %s",
                        partitionId, targetHostId, resp == null ? null : resp.getStatusString()));
                notifyPartitionMigrationStatus(partitionId, targetHSId, true);
            }
        } catch (Exception e) {
            tmLog.warn(String.format("errors in leader change for partition %d", partitionId), e);
            notifyPartitionMigrationStatus(partitionId, targetHSId, true);
        } finally {
            if (!transactionStarted) {
                return;
            }

            //wait for the Cartographer to see the new partition leader. The leader promotion process should happen instantly.
            //If the new leader does not show up in 5 min, the cluster may have experienced host-down events.
            long remainingWaitTime = TimeUnit.MINUTES.toMillis(5);
            final long waitingInterval = TimeUnit.SECONDS.toMillis(1);
            boolean anyFailedHosts = false;
            boolean migrationComplete = false;
            while (remainingWaitTime > 0) {
                try {
                    Thread.sleep(waitingInterval);
                } catch (InterruptedException ignoreIt) {
                }
                remainingWaitTime -= waitingInterval;
                Long hsId = m_cartographer.getHSIdForMaster(partitionId);
                if (hsId == null) {
                    log.warn("ClientInterface.startMigratePartitionLeader: Master does not exist for partition: "
                            + partitionId);
                    break;
                }
                if (CoreUtils.getHostIdFromHSId(hsId) == targetHostId) {
                    migrationComplete = true;
                    break;
                }

                //some hosts may be down.
                if (!voltDB.isClusterComplete() && !prepareStopNode) {
                    anyFailedHosts = true;
                    // If the target host is still alive, migration is still going on.
                    if (!voltDB.getHostMessenger().getLiveHostIds().contains(targetHostId)) {
                        break;
                    }
                }
            }

            //if there are failed hosts, this blocker will be removed in RealVoltDB.handleHostsFailedForMigratePartitionLeader()
            if (!anyFailedHosts) {
                voltDB.scheduleWork(
                        () -> removeMigrationZKNodes(),
                        5, 0, TimeUnit.SECONDS);
            }

            if (!migrationComplete) {
                notifyPartitionMigrationStatus(partitionId, targetHSId, true);
            }
        }
    }

    private void removeMigrationZKNodes() {
        VoltZK.removeActionBlocker(m_zk, VoltZK.migratePartitionLeaderBlocker, tmLog);
        VoltZK.removeMigratePartitionLeaderInfo(m_zk);
    }

    private void notifyPartitionMigrationStatus(int partitionId, long targetHSId, boolean failed) {
        for (final ClientInterfaceHandleManager cihm : m_cihm.values()) {
            if (cihm.repairCallback != null) {
                Runnable notify = () -> {
                    if (failed) {
                        cihm.repairCallback.leaderMigrationFailed(partitionId, targetHSId);
                    } else {
                        cihm.repairCallback.leaderMigrationStarted(partitionId, targetHSId);
                    }
                };

                try {
                    cihm.connection.queueTask(notify);
                } catch (UnsupportedOperationException ignore) {
                    // In case some internal connections don't implement queueTask()
                    notify.run();
                }
            }
        }
    }

    void processReplicaRemovalTask(HashMismatchMessage message) {
        final RealVoltDB db = (RealVoltDB) VoltDB.instance();
        if (db.m_leaderAppointer == null || !db.m_leaderAppointer.isLeader()) {
            if (db.rejoining() || db.isJoining()) {
                VoltDB.crashLocalVoltDB("Hash mismatch found before this node could finish " + (db.rejoining() ? "rejoin" : "join") +
                        "As a result, the rejoin operation has been canceled.");
                return;
            }
            if (message.isCheckHostMessage() && db.getLeaderSites().isEmpty()) {
                VoltDB.crashLocalVoltDB("The cluster will transfer to master-only state after hash mismatch is found." +
                        " There are no partition leaders on this host. As a result, the host is shutdown.");
            }
            return;
        } else if (message.isCheckHostMessage()) {
            return;
        }

        synchronized(m_lock) {
            if (m_replicaRemovalExecutor == null) {
                m_replicaRemovalExecutor = Executors.newSingleThreadScheduledExecutor(
                        CoreUtils.getThreadFactory("ReplicaRemoval"));
            }
            if (message.isReschedule()) {
                if (tmLog.isDebugEnabled()) {
                    tmLog.debug("@StopReplicas is blocked, reshcheduled.");
                }
                m_replicaRemovalExecutor.schedule(() -> {
                    if (!decommissionReplicas()) {
                        m_mailbox.deliver(new HashMismatchMessage(true));
                    }
                }, 2, TimeUnit.SECONDS);
            } else {
                m_replicaRemovalExecutor.submit(() -> {
                    if (!decommissionReplicas()) {
                        m_mailbox.deliver(new HashMismatchMessage(true));
                    }
                });
            }
        }
    }

    private boolean decommissionReplicas() {
        try {
            // Sanity check, if no mismatched sites are registered or have been removed on ZK, no OP.
            if (!VoltZK.hasHashMismatchedSite(m_zk)) {
                if (tmLog.isDebugEnabled()) {
                    tmLog.debug("Skip @StopReplicas, no hash mismatch sites are found.");
                }
                return true;
            }
            String errorMessage = VoltZK.createActionBlocker(m_zk, VoltZK.decommissionReplicasInProgress,
                    CreateMode.EPHEMERAL, tmLog, "remove replicas");
            if (errorMessage != null) {
                tmLog.rateLimitedInfo(60, errorMessage);
                return false;
            }
            SimpleClientResponseAdapter.SyncCallback cb = new SimpleClientResponseAdapter.SyncCallback();
            final String procedureName = "@StopReplicas";
            Config procedureConfig = SystemProcedureCatalog.listing.get(procedureName);
            StoredProcedureInvocation spi = new StoredProcedureInvocation();
            spi.setProcName(procedureName);
            spi.setParams();
            spi.setClientHandle(m_executeTaskAdpater.registerCallback(cb));
            if (spi.getSerializedParams() == null) {
                spi = MiscUtils.roundTripForCL(spi);
            }
            tmLog.info("Cluster starts to transfer to master only state.");
            synchronized (m_executeTaskAdpater) {
                if (createTransaction(m_executeTaskAdpater.connectionId(),
                        spi,
                        procedureConfig.getReadonly(),
                        procedureConfig.getSinglepartition(),
                        procedureConfig.getEverysite(),
                        MpInitiator.MP_INIT_PID,
                        spi.getSerializedSize(),
                        System.nanoTime()) != CreateTransactionResult.SUCCESS) {
                    tmLog.warn("Failed to start transaction for @StopReplicas");
                    return false;
                }
            }
            final long timeoutMS = TimeUnit.MINUTES.toMillis(2);
            ClientResponse resp = cb.getResponse(timeoutMS);
            return (resp.getStatus() == ClientResponse.SUCCESS);
        } catch (Exception e) {
            tmLog.error(String.format("The transaction of removing replicas failed: %s", e.getMessage()));
        } finally {
            VoltZK.removeActionBlocker(m_zk, VoltZK.decommissionReplicasInProgress, tmLog);
            // Send a message to the client interfaces. Hosts without partition leaders will be shutdown
            RealVoltDB voltDB = (RealVoltDB)VoltDB.instance();
            Set<Integer> liveHids = voltDB.getHostMessenger().getLiveHostIds();
            for (Integer hostId : liveHids) {
                final long ciHsid = CoreUtils.getHSIdFromHostAndSite(hostId, HostMessenger.CLIENT_INTERFACE_SITE_ID);
                m_mailbox.send(ciHsid, new HashMismatchMessage(false, true));
            }
        }
        return false;
    }
}
