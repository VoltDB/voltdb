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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.HdrHistogram_voltpatches.AbstractHistogram;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.ForeignHost;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.LocalObjectMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.network.Connection;
import org.voltcore.network.InputHandler;
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
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.AuthSystem.AuthProvider;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.CatalogContext.ProcedurePartitionInfo;
import org.voltdb.ClientInterfaceHandleManager.Iv2InFlight;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureInvocationType;
import org.voltdb.common.Constants;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.AdHocPlannedStmtBatch;
import org.voltdb.compiler.AdHocPlannerWork;
import org.voltdb.compiler.AsyncCompilerResult;
import org.voltdb.compiler.AsyncCompilerWork.AsyncCompilerWorkCompletionHandler;
import org.voltdb.compiler.CatalogChangeResult;
import org.voltdb.compiler.CatalogChangeWork;
import org.voltdb.dtxn.InitiatorStats.InvocationInfo;
import org.voltdb.iv2.Cartographer;
import org.voltdb.iv2.Iv2Trace;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2EndOfLogMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.LocalMailbox;
import org.voltdb.messaging.MultiPartitionParticipantMessage;
import org.voltdb.parser.SQLLexer;
import org.voltdb.security.AuthenticationRequest;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Predicate;
import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListenableFutureTask;

/**
 * Represents VoltDB's connection to client libraries outside the cluster.
 * This class accepts new connections and manages existing connections through
 * <code>ClientConnection</code> instances.
 *
 */
public class ClientInterface implements SnapshotDaemon.DaemonInitiator {

    static long TOPOLOGY_CHANGE_CHECK_MS = Long.getLong("TOPOLOGY_CHANGE_CHECK_MS", 5000);
    static long AUTH_TIMEOUT_MS = Long.getLong("AUTH_TIMEOUT_MS", 30000);

    //Same as in Distributer.java
    public static final long ASYNC_TOPO_HANDLE = Long.MAX_VALUE - 1;

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
    public static final long ELASTIC_JOIN_CID           = Long.MIN_VALUE + 3;
    // Leave CL_REPLAY_BASE_CID at the end, it uses this as a base and generates more cids
    public static final long CL_REPLAY_BASE_CID         = Long.MIN_VALUE + 100;

    private static final VoltLogger log = new VoltLogger(ClientInterface.class.getName());
    private static final VoltLogger authLog = new VoltLogger("AUTH");
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger networkLog = new VoltLogger("NETWORK");

    /** Ad hoc async work is either regular planning, ad hoc explain, or default proc explain. */
    public enum ExplainMode {
        NONE, EXPLAIN_ADHOC, EXPLAIN_DEFAULT_PROC;
    }

    private final ClientAcceptor m_acceptor;
    private ClientAcceptor m_adminAcceptor;

    private final SnapshotDaemon m_snapshotDaemon = new SnapshotDaemon();
    private final SnapshotDaemonAdapter m_snapshotDaemonAdapter = new SnapshotDaemonAdapter();

    // Atomically allows the catalog reference to change between access
    private final AtomicReference<CatalogContext> m_catalogContext = new AtomicReference<CatalogContext>(null);

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

    private final RateLimitedClientNotifier m_notifier = new RateLimitedClientNotifier();

    private final Cartographer m_cartographer;


    //This validator will check permissions in AUTH system.
    private final PermissionValidator m_permissionValidator = new PermissionValidator();
    //This validator will verify params or per procedure invocation vaidation.
    private final InvocationValidator m_invocationValidator;

    /*
     * Allow the async compiler thread to immediately process completed planning tasks
     * without waiting for the periodic work thread to poll the mailbox.
     */
    private final  AsyncCompilerWorkCompletionHandler m_adhocCompletionHandler = new AsyncCompilerWorkCompletionHandler() {
        @Override
        public void onCompletion(AsyncCompilerResult result) {
            processFinishedCompilerWork(result);
        }
    };

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

    // IV2 doesn't use this at all. Leave it here for now for legacy.
    private final int m_allPartitions[];
    private ImmutableMap<Integer, Long> m_localReplicas = ImmutableMap.<Integer, Long>builder().build();
    final long m_siteId;
    final long m_plannerSiteId;

    final Mailbox m_mailbox;

    /**
     * This boolean allows the DTXN to communicate to the
     * ClientInputHandler the presence of DTXN backpressure.
     * The m_connections ArrayList is used as the synchronization
     * point to ensure that modifications to read interest ops
     * that are based on the status of this information are atomic.
     * Additionally each connection must be synchronized on before modification
     * because the disabling of read selection for an individual connection
     * due to backpressure (not DTXN backpressure, client backpressure due to a client
     * that refuses to read responses) occurs inside the SimpleDTXNInitiator which
     * doesn't have access to m_connections
     */
    private final boolean m_hasDTXNBackPressure = false;

    // MAX_CONNECTIONS is updated to be (FD LIMIT - 300) after startup
    private final AtomicInteger MAX_CONNECTIONS = new AtomicInteger(800);
    private ScheduledFuture<?> m_maxConnectionUpdater;

    private final boolean m_isConfiguredForHSQL;

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
                            b.put(MAX_CONNECTIONS_LIMIT_ERROR);
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
                            sb.append(String.format(" after %.2f seconds (timeout target is %.2f seconds)", seconds, AUTH_TIMEOUT_MS / 1000.0));
                            timeoutRef.set(sb.toString());
                            try {
                                socket.close();
                            } catch (IOException e) {
                                //Don't care
                            }
                        }
                    }, AUTH_TIMEOUT_MS, 0, TimeUnit.MILLISECONDS);

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
                responseBuffer.put(WIRE_PROTOCOL_TIMEOUT_ERROR).flip();
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

    // Wrap API to SimpleDtxnInitiator - mostly for the future
    public boolean createTransaction(
            final long connectionId,
            final StoredProcedureInvocation invocation,
            final boolean isReadOnly,
            final boolean isSinglePartition,
            final boolean isEveryPartition,
            final int partition,
            final int messageSize,
            final long nowNanos)
    {
        return createTransaction(
                connectionId,
                Iv2InitiateTaskMessage.UNUSED_MP_TXNID,
                0, //unused timestammp
                invocation,
                isReadOnly,
                isSinglePartition,
                isEveryPartition,
                partition,
                messageSize,
                nowNanos,
                false);  // is for replay.
    }

    // Wrap API to SimpleDtxnInitiator - mostly for the future
    public  boolean createTransaction(
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
        assert(!isSinglePartition || (partition >= 0));
        final ClientInterfaceHandleManager cihm = m_cihm.get(connectionId);

        Long initiatorHSId = null;
        boolean isShortCircuitRead = false;

        /*
         * If this is a read only single part, check if there is a local replica,
         * if there is, send it to the replica as a short circuit read
         */
        if (isSinglePartition && !isEveryPartition) {
            if (isReadOnly) {
                initiatorHSId = m_localReplicas.get(partition);
            }
            if (initiatorHSId != null) {
                isShortCircuitRead = true;
            } else {
                initiatorHSId = m_cartographer.getHSIdForSinglePartitionMaster(partition);
            }
        }
        else {
            //Multi-part transactions go to the multi-part coordinator
            initiatorHSId = m_cartographer.getHSIdForMultiPartitionInitiator();
            // Treat all MP reads as short-circuit since they can run out-of-order
            // from their arrival order due to the MP Read-only execution pool
            if (isReadOnly) {
                isShortCircuitRead = true;
            }
        }

        if (initiatorHSId == null) {
            hostLog.error("Failed to find master initiator for partition: "
                    + Integer.toString(partition) + ". Transaction not initiated.");
            return false;
        }

        long handle = cihm.getHandle(isSinglePartition, partition, invocation.getClientHandle(),
                messageSize, nowNanos, invocation.getProcName(), initiatorHSId, isReadOnly, isShortCircuitRead);

        Iv2InitiateTaskMessage workRequest =
            new Iv2InitiateTaskMessage(m_siteId,
                    initiatorHSId,
                    Iv2InitiateTaskMessage.UNUSED_TRUNC_HANDLE,
                    txnId,
                    uniqueId,
                    isReadOnly,
                    isSinglePartition,
                    invocation,
                    handle,
                    connectionId,
                    isForReplay);

        Iv2Trace.logCreateTransaction(workRequest);
        m_mailbox.send(initiatorHSId, workRequest);
        return true;
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
            int partitionCount,
            InetAddress clientIntf,
            int clientPort,
            InetAddress adminIntf,
            int adminPort,
            long timestampTestingSalt) throws Exception {

        // create a list of all partitions
        int[] allPartitions = new int[partitionCount];
        int index = 0;
        for (Integer partition : cartographer.getPartitions()) {
            if (partition != MpInitiator.MP_INIT_PID) {
                allPartitions[index++] = partition;
            }
        }

        /*
         * Construct the runnables so they have access to the list of connections
         */
        final ClientInterface ci = new ClientInterface(
                clientIntf, clientPort, adminIntf, adminPort, context, messenger, replicationRole, cartographer, allPartitions);

        return ci;
    }

    ClientInterface(InetAddress clientIntf, int clientPort, InetAddress adminIntf, int adminPort,
            CatalogContext context, HostMessenger messenger, ReplicationRole replicationRole,
            Cartographer cartographer, int[] allPartitions) throws Exception {
        m_catalogContext.set(context);
        m_cartographer = cartographer;

        // pre-allocate single partition array
        m_allPartitions = allPartitions;
        m_acceptor = new ClientAcceptor(clientIntf, clientPort, messenger.getNetwork(), false);
        m_adminAcceptor = null;
        m_adminAcceptor = new ClientAcceptor(adminIntf, adminPort, messenger.getNetwork(), true);
        m_invocationValidator = new InvocationValidator(replicationRole);

        m_mailbox = new LocalMailbox(messenger,  messenger.getHSIdForLocalSite(HostMessenger.CLIENT_INTERFACE_SITE_ID)) {
            LinkedBlockingQueue<VoltMessage> m_d = new LinkedBlockingQueue<VoltMessage>();
            @Override
            public void deliver(final VoltMessage message) {
                if (message instanceof InitiateResponseMessage) {
                    final CatalogContext catalogContext = m_catalogContext.get();
                    // forward response; copy is annoying. want slice of response.
                    InitiateResponseMessage response = (InitiateResponseMessage)message;
                    StoredProcedureInvocation invocation = response.getInvocation();
                    Iv2Trace.logFinishTransaction(response, m_mailbox.getHSId());
                    ClientInterfaceHandleManager cihm = m_cihm.get(response.getClientConnectionId());
                    Procedure procedure = null;

                    if (invocation != null) {
                        procedure = catalogContext.procedures.get(invocation.getProcName());
                        if (procedure == null) {
                            procedure = SystemProcedureCatalog.listing.get(invocation.getProcName())
                                                              .asCatalogProcedure();
                        }
                    }

                    //Can be null on hangup
                    if (cihm != null) {
                        //Pass it to the network thread like a ninja
                        //Only the network can use the CIHM
                        cihm.connection.writeStream().fastEnqueue(new ClientResponseWork(response, cihm, procedure));
                    }
                } else if (message instanceof BinaryPayloadMessage) {
                    handlePartitionFailOver((BinaryPayloadMessage)message);
                } else {
                    m_d.offer(message);
                }
            }

            @Override
            public VoltMessage recv() {
                return m_d.poll();
            }
        };
        messenger.createMailbox(m_mailbox.getHSId(), m_mailbox);
        m_plannerSiteId = messenger.getHSIdForLocalSite(HostMessenger.ASYNC_COMPILER_SITE_ID);
        m_zk = messenger.getZK();
        m_siteId = m_mailbox.getHSId();
        m_isConfiguredForHSQL = (VoltDB.instance().getBackendTargetType() == BackendTarget.HSQLDB_BACKEND);
    }

    private void handlePartitionFailOver(BinaryPayloadMessage message) {
        try {
            JSONObject jsObj = new JSONObject(new String(message.m_payload, "UTF-8"));
            final int partitionId = jsObj.getInt(Cartographer.JSON_PARTITION_ID);
            final long initiatorHSId = jsObj.getLong(Cartographer.JSON_INITIATOR_HSID);
            for (final ClientInterfaceHandleManager cihm : m_cihm.values()) {
                try {
                    cihm.connection.queueTask(new Runnable() {
                        @Override
                        public void run() {
                            failOverConnection(partitionId, initiatorHSId, cihm.connection);
                        }
                    });
                } catch (UnsupportedOperationException ignore) {
                    // In case some internal connections don't implement queueTask()
                    failOverConnection(partitionId, initiatorHSId, cihm.connection);
                }
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
        if (cihm == null) return;

        List<Iv2InFlight> transactions =
                cihm.removeHandlesForPartitionAndInitiator( partitionId, initiatorHSId);

        for (Iv2InFlight inFlight : transactions) {
            ClientResponseImpl response =
                    new ClientResponseImpl(
                            ClientResponseImpl.RESPONSE_UNKNOWN,
                            ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                            null,
                            new VoltTable[0],
                            "Transaction dropped due to change in mastership. " +
                            "It is possible the transaction was committed");
            response.setClientHandle( inFlight.m_clientHandle );
            ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
            buf.putInt(buf.capacity() - 4);
            response.flattenToBuffer(buf);
            buf.flip();
            c.writeStream().enqueue(buf);
        }
    }

    /**
     * Called when the replication role of the cluster changes.
     * @param role
     */
    public void setReplicationRole(ReplicationRole role) {
        m_invocationValidator.setReplicationRole(role);
    }

    /**
     * Initializes the snapshot daemon so that it's ready to take snapshots
     */
    public void initializeSnapshotDaemon(HostMessenger messenger, GlobalServiceElector gse) {
        m_snapshotDaemon.init(this, messenger, new Runnable() {
            @Override
            public void run() {
                bindAdapter(m_snapshotDaemonAdapter);
            }
        },
        gse);
    }

    /**
     * Tell the clientInterface about a connection adapter.
     */
    public void bindAdapter(final Connection adapter) {
        m_cihm.put(adapter.connectionId(),
                ClientInterfaceHandleManager.makeThreadSafeCIHM(true, adapter,
                    AdmissionControlGroup.getDummy()));
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
     * Take the response from the async ad hoc planning process and put the explain
     * plan in a table with the right format.
     */
    private void processExplainPlannedStmtBatch(  AdHocPlannedStmtBatch planBatch ) {
        final Connection c = (Connection)planBatch.clientData;
        Database db = m_catalogContext.get().database;
        int size = planBatch.getPlannedStatementCount();

        VoltTable[] vt = new VoltTable[ size ];
        for (int i = 0; i < size; ++i) {
            vt[i] = new VoltTable(new VoltTable.ColumnInfo("EXECUTION_PLAN", VoltType.STRING));
            String str = planBatch.explainStatement(i, db);
            vt[i].addRow(str);
        }

        ClientResponseImpl response =
                new ClientResponseImpl(
                        ClientResponseImpl.SUCCESS,
                        ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                        null,
                        vt,
                        null);
        response.setClientHandle( planBatch.clientHandle );
        ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
        buf.putInt(buf.capacity() - 4);
        response.flattenToBuffer(buf);
        buf.flip();
        c.writeStream().enqueue(buf);
    }

    /**
     * Explain Proc for a default proc is routed through the regular Explain
     * path using ad hoc planning and all. Take the result from that async
     * process and format it like other explains for procedures.
     */
    private void processExplainDefaultProc(AdHocPlannedStmtBatch planBatch) {
        final Connection c = (Connection)planBatch.clientData;
        Database db = m_catalogContext.get().database;

        // there better be one statement if this is really sql
        // from a default procedure
        assert(planBatch.getPlannedStatementCount() == 1);
        AdHocPlannedStatement ahps = planBatch.getPlannedStatement(0);
        String sql = new String(ahps.sql, Charsets.UTF_8);
        String explain = planBatch.explainStatement(0, db);

        VoltTable vt = new VoltTable(new VoltTable.ColumnInfo( "SQL_STATEMENT", VoltType.STRING),
                new VoltTable.ColumnInfo( "EXECUTION_PLAN", VoltType.STRING));
        vt.addRow(sql, explain);

        ClientResponseImpl response =
                new ClientResponseImpl(
                        ClientResponseImpl.SUCCESS,
                        ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                        null,
                        new VoltTable[] { vt },
                        null);
        response.setClientHandle( planBatch.clientHandle );
        ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
        buf.putInt(buf.capacity() - 4);
        response.flattenToBuffer(buf);
        buf.flip();
        c.writeStream().enqueue(buf);
    }

    // Go to the catalog and fetch all the "explain plan" strings of the queries in the procedure.
    ClientResponseImpl dispatchExplainProcedure(StoredProcedureInvocation task, ClientInputHandler handler, Connection ccxn, AuthUser user) {
        ParameterSet params = task.getParams();
        /*
         * TODO: We don't actually support multiple proc names in an ExplainProc call,
         * so I THINK that the string is always a single procname symbol and all this
         * splitting and iterating is a no-op.
         */
        //String procs = (String) params.toArray()[0];
        List<String> procNames = SQLLexer.splitStatements( (String)params.toArray()[0]);
        int size = procNames.size();
        VoltTable[] vt = new VoltTable[ size ];
        for( int i=0; i<size; i++ ) {
            String procName = procNames.get(i);

            // look in the catalog
            Procedure proc = m_catalogContext.get().procedures.get(procName);
            if (proc == null) {
                // check default procs and send them off to be explained using the regular
                // adhoc explain process
                proc = m_catalogContext.get().m_defaultProcs.checkForDefaultProcedure(procName);
                if (proc != null) {
                    String sql = m_catalogContext.get().m_defaultProcs.sqlForDefaultProc(proc);
                    dispatchAdHocCommon(task, handler, ccxn, ExplainMode.EXPLAIN_DEFAULT_PROC, sql, new Object[0], null, user);
                    return null;
                }

                ClientResponseImpl errorResponse =
                        new ClientResponseImpl(
                                ClientResponseImpl.UNEXPECTED_FAILURE,
                                new VoltTable[0], "Procedure "+procName+" not in catalog",
                                task.clientHandle);
                return errorResponse;
            }

            vt[i] = new VoltTable(new VoltTable.ColumnInfo( "SQL_STATEMENT", VoltType.STRING),
                                  new VoltTable.ColumnInfo( "EXECUTION_PLAN", VoltType.STRING));

            for( Statement stmt : proc.getStatements() ) {
                vt[i].addRow( stmt.getSqltext(), Encoder.hexDecodeToString( stmt.getExplainplan() ) );
            }
        }

        ClientResponseImpl response =
                new ClientResponseImpl(
                        ClientResponseImpl.SUCCESS,
                        ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                        null,
                        vt,
                        null);
        response.setClientHandle( task.clientHandle );
        ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
        buf.putInt(buf.capacity() - 4);
        response.flattenToBuffer(buf);
        buf.flip();
        ccxn.writeStream().enqueue(buf);
        return null;
    }

    private final ClientResponseImpl dispatchAdHoc(StoredProcedureInvocation task,
            ClientInputHandler handler, Connection ccxn, boolean isExplain, AuthSystem.AuthUser user) {
        ParameterSet params = task.getParams();
        Object[] paramArray = params.toArray();
        String sql = (String) paramArray[0];
        Object[] userParams = null;
        if (params.size() > 1) {
            userParams = Arrays.copyOfRange(paramArray, 1, paramArray.length);
        }
        ExplainMode explainMode = isExplain ? ExplainMode.EXPLAIN_ADHOC : ExplainMode.NONE;
        dispatchAdHocCommon(task, handler, ccxn, explainMode, sql, userParams, null, user);
        return null;
    }

    private final ClientResponseImpl dispatchAdHocSpForTest(StoredProcedureInvocation task,
            ClientInputHandler handler, Connection ccxn, boolean isExplain, AuthSystem.AuthUser user) {
        ParameterSet params = task.getParams();
        assert(params.size() > 1);
        Object[] paramArray = params.toArray();
        String sql = (String) paramArray[0];
        // get the partition param which must exist
        Object[] userPartitionKey = Arrays.copyOfRange(paramArray, 1, 2);
        Object[] userParams = null;
        // There's no reason (any more) that AdHocSP's can't have '?' parameters, but
        // note that the explicit partition key argument is not considered one of them.
        if (params.size() > 2) {
            userParams = Arrays.copyOfRange(paramArray, 2, paramArray.length);
        }
        ExplainMode explainMode = isExplain ? ExplainMode.EXPLAIN_ADHOC : ExplainMode.NONE;
        dispatchAdHocCommon(task, handler, ccxn, explainMode, sql, userParams, userPartitionKey, user);
        return null;
    }

    private final void dispatchAdHocCommon(StoredProcedureInvocation task,
            ClientInputHandler handler, Connection ccxn, ExplainMode explainMode,
            String sql, Object[] userParams, Object[] userPartitionKey, AuthSystem.AuthUser user) {
        List<String> sqlStatements = SQLLexer.splitStatements(sql);
        String[] stmtsArray = sqlStatements.toArray(new String[sqlStatements.size()]);

        AdHocPlannerWork ahpw = new AdHocPlannerWork(
                m_siteId,
                task.clientHandle, handler.connectionId(),
                handler.isAdmin(), ccxn,
                sql, stmtsArray, userParams, null, explainMode,
                userPartitionKey == null, userPartitionKey,
                task.procName, task.type, task.originalTxnId, task.originalUniqueId,
                VoltDB.instance().getReplicationRole() == ReplicationRole.REPLICA,
                VoltDB.instance().getCatalogContext().cluster.getUseddlschema(),
                m_adhocCompletionHandler, user);
        LocalObjectMessage work = new LocalObjectMessage( ahpw );

        m_mailbox.send(m_plannerSiteId, work);
    }

    ClientResponseImpl dispatchUpdateApplicationCatalog(StoredProcedureInvocation task,
            ClientInputHandler handler, Connection ccxn, AuthSystem.AuthUser user)
    {
        ParameterSet params = task.getParams();
        // default catalogBytes to null, when passed along, will tell the
        // catalog change planner that we want to use the current catalog.
        byte[] catalogBytes = null;
        Object catalogObj = params.toArray()[0];
        if (catalogObj != null) {
            if (catalogObj instanceof String) {
                // treat an empty string as no catalog provided
                String catalogString = (String) catalogObj;
                if (!catalogString.isEmpty()) {
                    catalogBytes = Encoder.hexDecode(catalogString);
                }
            } else if (catalogObj instanceof byte[]) {
                // treat an empty array as no catalog provided
                byte[] catalogArr = (byte[]) catalogObj;
                if (catalogArr.length != 0) {
                    catalogBytes = catalogArr;
                }
            }
        }
        String deploymentString = (String) params.toArray()[1];
        LocalObjectMessage work = new LocalObjectMessage(
                new CatalogChangeWork(
                    m_siteId,
                    task.clientHandle, handler.connectionId(), ccxn.getHostnameAndIPAndPort(),
                    handler.isAdmin(), ccxn, catalogBytes, deploymentString,
                    task.procName, task.type, task.originalTxnId, task.originalUniqueId,
                    VoltDB.instance().getReplicationRole() == ReplicationRole.REPLICA,
                    VoltDB.instance().getCatalogContext().cluster.getUseddlschema(),
                    m_adhocCompletionHandler, user));

        m_mailbox.send(m_plannerSiteId, work);
        return null;
    }

    /**
     * Coward way out of the legacy hashinator hell. LoadSinglepartitionTable gets the
     * partitioning parameter as a byte array. Legacy hashinator hashes numbers and byte arrays
     * differently, so have to convert it back to long if it's a number. UGLY!!!
     */
    ClientResponseImpl dispatchLoadSinglepartitionTable(ByteBuffer buf,
                                                        Procedure catProc,
                                                        StoredProcedureInvocation task,
                                                        ClientInputHandler handler,
                                                        Connection ccxn)
    {
        int partition = -1;
        try {
            CatalogMap<Table> tables = m_catalogContext.get().database.getTables();
            int partitionParamType = getLoadSinglePartitionTablePartitionParamType(tables, task);
            byte[] valueToHash = (byte[])task.getParameterAtIndex(0);
            partition = TheHashinator.getPartitionForParameter(partitionParamType, valueToHash);
        }
        catch (Exception e) {
            authLog.warn(e.getMessage());
            return new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                                          new VoltTable[0], e.getMessage(), task.clientHandle);
        }
        assert(partition != -1);
        createTransaction(handler.connectionId(),
                          task,
                          catProc.getReadonly(),
                          catProc.getSinglepartition(),
                          catProc.getEverysite(),
                          partition,
                          buf.capacity(),
                          System.nanoTime());
        return null;
    }

    /**
     * XXX: This should go away when we get rid of the legacy hashinator.
     */
    private static int getLoadSinglePartitionTablePartitionParamType(CatalogMap<Table> tables,
                                                                     StoredProcedureInvocation spi)
        throws Exception
    {
        String tableName = (String) spi.getParameterAtIndex(1);

        // get the table from the catalog
        Table catTable = tables.getIgnoreCase(tableName);
        if (catTable == null) {
            throw new Exception(String .format("Unable to find target table \"%s\" for LoadSinglepartitionTable.",
                                               tableName));
        }

        Column pCol = catTable.getPartitioncolumn();
        return pCol.getType();
    }

    /**
     * Send a multipart sentinel to all partitions. This is only used when the
     * multipart didn't generate any sentinels for partitions, e.g. DR
     * @LoadMultipartitionTable.
     *
     * @param txnId
     */
    void sendSentinelsToAllPartitions(long txnId)
    {
        for (int partition : m_allPartitions) {
            final long initiatorHSId = m_cartographer.getHSIdForSinglePartitionMaster(partition);
            /*
             * HACK! DR LoadMultipartitionTable generates sentinels here,
             * they pretend to be for replay so that the SPIs won't generate responses for them.
             */
            sendSentinel(txnId, initiatorHSId, -1, -1, true);
        }
    }

    /**
     * Send a multipart sentinel to the specified partition. This comes from the
     * DR agent in prepare of a multipart transaction.
     *
     * @param connectionId
     * @param now
     * @param size
     * @param invocation
     */
    void dispatchSendSentinel(long connectionId, long nowNanos, int size,
                              StoredProcedureInvocation invocation)
    {
        ClientInterfaceHandleManager cihm = m_cihm.get(connectionId);
        // First parameter of the invocation is the partition ID
        int pid = (Integer) invocation.getParameterAtIndex(0);
        final long initiatorHSId = m_cartographer.getHSIdForSinglePartitionMaster(pid);
        long handle = cihm.getHandle(true, pid, invocation.getClientHandle(), size, nowNanos,
                invocation.getProcName(), initiatorHSId, true, false);

        /*
         * Sentinels will be deduped by ReplaySequencer. They don't advance the
         * last replayed txnIds.
         */
        sendSentinel(invocation.getOriginalTxnId(), initiatorHSId, handle, connectionId, false);
    }

    ClientResponseImpl dispatchStatistics(OpsSelector selector, StoredProcedureInvocation task, Connection ccxn)
    {
        try {
            OpsAgent agent = VoltDB.instance().getOpsAgent(selector);
            if (agent != null) {
                agent.performOpsAction(ccxn, task.clientHandle, selector, task.getParams());
            }
            else {
                return errorResponse(ccxn, task.clientHandle, ClientResponse.GRACEFUL_FAILURE,
                        "Unknown OPS selector", null, true);
            }

            return null;
        } catch (Exception e) {
            return errorResponse( ccxn, task.clientHandle, ClientResponse.UNEXPECTED_FAILURE, null, e, true);
        }
    }

    ClientResponseImpl dispatchPromote(Procedure sysProc,
                                       ByteBuffer buf,
                                       StoredProcedureInvocation task,
                                       ClientInputHandler handler,
                                       Connection ccxn)
    {
        if (VoltDB.instance().getReplicationRole() == ReplicationRole.NONE)
        {
            return new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                    new VoltTable[0], "@Promote issued on master cluster." +
                    " No action taken.",
                    task.clientHandle);
        }

        // This only happens on one node so we don't need to pick a leader.
        createTransaction(
                handler.connectionId(),
                task,
                sysProc.getReadonly(),
                sysProc.getSinglepartition(),
                sysProc.getEverysite(),
                0,//No partition needed for multi-part
                buf.capacity(),
                System.nanoTime());

        return null;
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

    //Run System.gc() in it's own thread because it will block
    //until collection is complete and we don't want to do that from an application thread
    //because the collector is partially concurrent and we can still make progress
    private final ExecutorService m_systemGCThread =
            CoreUtils.getCachedSingleThreadExecutor("System.gc() invocation thread", 1000);

    /*
     * Allow System.gc() to be invoked remotely even when JMX isn't enabled.
     * Can be used to perform old gen GCs on a schedule during non-peak times
     */
    private ClientResponseImpl dispatchSystemGC(final ClientInputHandler handler,
                                                final StoredProcedureInvocation task) {
        m_systemGCThread.execute(new Runnable() {
            @Override
            public void run() {
                final long start = System.nanoTime();
                System.gc();
                final long duration = System.nanoTime() - start;
                VoltTable vt = new VoltTable(
                        new ColumnInfo[] { new ColumnInfo("SYSTEM_GC_DURATION_NANOS", VoltType.BIGINT) });
                vt.addRow(duration);
                final ClientResponseImpl response = new ClientResponseImpl(
                        ClientResponseImpl.SUCCESS,
                        new VoltTable[] { vt },
                        null,
                        task.clientHandle);
                ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
                buf.putInt(buf.capacity() - 4);
                response.flattenToBuffer(buf).flip();

                ClientInterfaceHandleManager cihm = m_cihm.get(handler.connectionId());
                if (cihm == null) return;
                cihm.connection.writeStream().enqueue(buf);
            }
        });
        return null;
    }

    private ClientResponseImpl dispatchSubscribe(ClientInputHandler c, StoredProcedureInvocation task) {
        final ParameterSet ps = task.getParams();
        final Object params[] = ps.toArray();
        String err = null;
        final ClientInterfaceHandleManager cihm = m_cihm.get(c.connectionId());
        //Not sure if it can actually be null, not really important if it is
        if (cihm == null) return null;
        for (int ii = 0; ii < params.length; ii++) {
            final Object param = params[ii];
            if (param == null) {
                err = "Parameter index " + ii + " was null"; break;
            }
            if (!(param instanceof String)) {
                err = "Parameter index " + ii + " was not a String"; break;
            }

            if (param.equals("TOPOLOGY")) {
                cihm.setWantsTopologyUpdates(true);
            } else {
                err = "Parameter \"" + param + "\" is not recognized/supported"; break;
            }
        }
        return new ClientResponseImpl(
                       err == null ? ClientResponse.SUCCESS : ClientResponse.GRACEFUL_FAILURE,
                       new VoltTable[] { },
                       err,
                       task.clientHandle);
    }

    private ClientResponseImpl dispatchGetPartitionKeys(StoredProcedureInvocation task) {
        Object params[] = task.getParams().toArray();
        String typeString = "the type of partition key to return and can be one of " +
                            "INTEGER, STRING or VARCHAR (equivalent), or VARBINARY";
        if (params.length != 1 || params[0] == null) {
            return new ClientResponseImpl(
                    ClientResponse.GRACEFUL_FAILURE,
                    new VoltTable[0],
                    "GetPartitionKeys must have one string parameter specifying " + typeString,
                    task.clientHandle);
        }
        if (!(params[0] instanceof String)) {
            return new ClientResponseImpl(
                    ClientResponse.GRACEFUL_FAILURE,
                    new VoltTable[0],
                    "GetPartitionKeys must have one string parameter specifying " + typeString +
                    " provided type was " + params[0].getClass().getName(),
                    task.clientHandle);
        }
        VoltType voltType = null;
        String typeStr = ((String)params[0]).trim().toUpperCase();
        if (typeStr.equals("INTEGER")) voltType = VoltType.INTEGER;
        else if (typeStr.equals("STRING") || typeStr.equals("VARCHAR")) voltType = VoltType.STRING;
        else if (typeStr.equals("VARBINARY")) voltType = VoltType.VARBINARY;
        else {
            return new ClientResponseImpl(
                    ClientResponse.GRACEFUL_FAILURE,
                    new VoltTable[0],
                    "Type " + typeStr + " is not a supported type of partition key, " + typeString,
                    task.clientHandle);
        }
        VoltTable partitionKeys = TheHashinator.getPartitionKeys(voltType);
        if (partitionKeys == null) {
            return new ClientResponseImpl(
                    ClientResponse.GRACEFUL_FAILURE,
                    new VoltTable[0],
                    "Type " + typeStr + " is not a supported type of partition key, " + typeString,
                    task.clientHandle);
        }
        return new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[] { partitionKeys }, null, task.clientHandle);
    }

    private ClientResponseImpl dispatchStopNode(StoredProcedureInvocation task) {
        Object params[] = task.getParams().toArray();
        if (params.length != 1 || params[0] == null) {
            return new ClientResponseImpl(
                    ClientResponse.GRACEFUL_FAILURE,
                    new VoltTable[0],
                    "@StopNode must provide hostId",
                    task.clientHandle);
        }
        if (!(params[0] instanceof Integer)) {
            return new ClientResponseImpl(
                    ClientResponse.GRACEFUL_FAILURE,
                    new VoltTable[0],
                    "@StopNode must have one Integer parameter specified. Provided type was " + params[0].getClass().getName(),
                    task.clientHandle);
        }
        int ihid = (Integer) params[0];
        List<Integer> liveHids = VoltDB.instance().getHostMessenger().getLiveHostIds();
        if (!liveHids.contains(ihid)) {
            return new ClientResponseImpl(
                    ClientResponse.GRACEFUL_FAILURE,
                    new VoltTable[0],
                    "Invalid Host Id or Host Id not member of cluster: " + ihid,
                    task.clientHandle);
        }
        if (!m_cartographer.isClusterSafeIfNodeDies(liveHids, ihid)) {
            hostLog.info("Its unsafe to shutdown node with hostId: " + ihid
                    + " Cannot stop the requested node. Stopping individual nodes is only allowed on a K-safe cluster."
                    + " Use shutdown to stop the cluster.");
            return new ClientResponseImpl(
                    ClientResponse.GRACEFUL_FAILURE,
                    new VoltTable[0],
                    "Cannot stop the requested node. Stopping individual nodes is only allowed on a K-safe cluster."
                            + " Use shutdown to stop the cluster.", task.clientHandle);
        }

        int hid = VoltDB.instance().getHostMessenger().getHostId();
        if (hid == ihid) {
            //Killing myself no pill needs to be sent
            VoltDB.instance().halt();
        } else {
            //Send poison pill with target to kill
            VoltDB.instance().getHostMessenger().sendPoisonPill("@StopNode", ihid, ForeignHost.CRASH_ME);
        }
        return new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[0], "SUCCESS", task.clientHandle);
    }

    void createAdHocTransaction(final AdHocPlannedStmtBatch plannedStmtBatch, Connection c)
            throws VoltTypeException
    {
        ByteBuffer buf = null;
        try {
            buf = plannedStmtBatch.flattenPlanArrayToBuffer();
        }
        catch (IOException e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }
        assert(buf.hasArray());

        // create the execution site task
        StoredProcedureInvocation task = new StoredProcedureInvocation();
        // DR stuff
        task.type = plannedStmtBatch.work.invocationType;
        task.originalTxnId = plannedStmtBatch.work.originalTxnId;
        task.originalUniqueId = plannedStmtBatch.work.originalUniqueId;
        // pick the sysproc based on the presence of partition info
        // HSQL does not specifically implement AdHoc SP -- instead, use its always-SP implementation of AdHoc
        boolean isSinglePartition = plannedStmtBatch.isSinglePartitionCompatible() || m_isConfiguredForHSQL;
        int partition = -1;

        if (isSinglePartition) {
            if (plannedStmtBatch.isReadOnly()) {
                task.procName = "@AdHoc_RO_SP";
            }
            else {
                task.procName = "@AdHoc_RW_SP";
            }
            int type = VoltType.NULL.getValue();
            // replicated table read is single-part without a partitioning param
            // I copied this from below, but I'm not convinced that the above statement is correct
            // or that the null behavior here either (a) ever actually happens or (b) has the
            // desired intent.
            Object partitionParam = plannedStmtBatch.partitionParam();
            byte[] param = null;
            if (partitionParam != null) {
                type = VoltType.typeFromClass(partitionParam.getClass()).getValue();
                param = TheHashinator.valueToBytes(partitionParam);
            }
            partition = TheHashinator.getPartitionForParameter(type, partitionParam);

            // Send the partitioning parameter and its type along so that the site can check if
            // it's mis-partitioned. Type is needed to re-hashinate for command log re-init.
            task.setParams(param, (byte)type, buf.array());
        }
        else {
            if (plannedStmtBatch.isReadOnly()) {
                task.procName = "@AdHoc_RO_MP";
            }
            else {
                task.procName = "@AdHoc_RW_MP";
            }
            task.setParams(buf.array());
        }
        task.clientHandle = plannedStmtBatch.clientHandle;

        ClientResponseImpl error = null;
        if ((error = m_permissionValidator.shouldAccept(task.procName, plannedStmtBatch.work.user, task,
                SystemProcedureCatalog.listing.get(task.procName).asCatalogProcedure())) != null) {
            ByteBuffer buffer = ByteBuffer.allocate(error.getSerializedSize() + 4);
            buffer.putInt(buffer.capacity() - 4);
            error.flattenToBuffer(buffer).flip();
            c.writeStream().enqueue(buffer);
        }
        else {
            /*
             * Round trip the invocation to initialize it for command logging
             */
            try {
                task = MiscUtils.roundTripForCL(task);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }

            // initiate the transaction
            createTransaction(plannedStmtBatch.connectionId, task,
                    plannedStmtBatch.isReadOnly(), isSinglePartition, false,
                    partition,
                    task.getSerializedSize(), System.nanoTime());
        }
    }

    /*
     * Invoked from the AsyncCompilerWorkCompletionHandler from the AsyncCompilerAgent thread.
     * Has the effect of immediately handing the completed work to the network thread of the
     * client instance that created the work and then dispatching it.
     */
    public ListenableFutureTask<?> processFinishedCompilerWork(final AsyncCompilerResult result) {
        /*
         * Do the task in the network thread associated with the connection
         * so that access to the CIHM can be lock free for fast path work.
         * Can't access the CIHM from this thread without adding locking.
         */
        final Connection c = (Connection)result.clientData;
        final ListenableFutureTask<?> ft = ListenableFutureTask.create(new Runnable() {
            @Override
            public void run() {
                if (result.errorMsg == null) {
                    if (result instanceof AdHocPlannedStmtBatch) {
                        final AdHocPlannedStmtBatch plannedStmtBatch = (AdHocPlannedStmtBatch) result;
                        ExplainMode explainMode = plannedStmtBatch.getExplainMode();

                        // assume all stmts have the same catalog version
                        if ((plannedStmtBatch.getPlannedStatementCount() > 0) &&
                            (!plannedStmtBatch.getPlannedStatement(0).core.wasPlannedAgainstHash(m_catalogContext.get().getCatalogHash())))
                        {

                            /* The adhoc planner learns of catalog updates after the EE and the
                               rest of the system. If the adhoc sql was planned against an
                               obsolete catalog, re-plan. */
                            LocalObjectMessage work = new LocalObjectMessage(
                                    AdHocPlannerWork.rework(plannedStmtBatch.work, m_adhocCompletionHandler));

                            m_mailbox.send(m_plannerSiteId, work);
                        }
                        else if (explainMode == ExplainMode.EXPLAIN_ADHOC) {
                            processExplainPlannedStmtBatch(plannedStmtBatch);
                        }
                        else if (explainMode == ExplainMode.EXPLAIN_DEFAULT_PROC) {
                            processExplainDefaultProc(plannedStmtBatch);
                        }
                        else {
                            try {
                                createAdHocTransaction(plannedStmtBatch, c);
                            }
                            catch (VoltTypeException vte) {
                                String msg = "Unable to execute adhoc sql statement(s): " +
                                        vte.getMessage();
                                ClientResponseImpl errorResponse =
                                    new ClientResponseImpl(
                                            ClientResponseImpl.GRACEFUL_FAILURE,
                                            new VoltTable[0], msg,
                                            result.clientHandle);
                                ByteBuffer buf = ByteBuffer.allocate(errorResponse.getSerializedSize() + 4);
                                buf.putInt(buf.capacity() - 4);
                                errorResponse.flattenToBuffer(buf);
                                buf.flip();
                                c.writeStream().enqueue(buf);
                            }
                        }
                    }
                    else if (result instanceof CatalogChangeResult) {
                        final CatalogChangeResult changeResult = (CatalogChangeResult) result;

                        // if the catalog change is a null change
                        if (changeResult.encodedDiffCommands.trim().length() == 0) {
                            ClientResponseImpl shortcutResponse =
                                    new ClientResponseImpl(
                                            ClientResponseImpl.SUCCESS,
                                            new VoltTable[0], "Catalog update with no changes was skipped.",
                                            result.clientHandle);
                            ByteBuffer buf = ByteBuffer.allocate(shortcutResponse.getSerializedSize() + 4);
                            buf.putInt(buf.capacity() - 4);
                            shortcutResponse.flattenToBuffer(buf);
                            buf.flip();
                            c.writeStream().enqueue(buf);
                        }
                        else {
                            // create the execution site task
                            StoredProcedureInvocation task = new StoredProcedureInvocation();
                            task.procName = "@UpdateApplicationCatalog";
                            task.setParams(changeResult.encodedDiffCommands,
                                           changeResult.catalogHash,
                                           changeResult.catalogBytes,
                                           changeResult.expectedCatalogVersion,
                                           changeResult.deploymentString,
                                           changeResult.tablesThatMustBeEmpty,
                                           changeResult.reasonsForEmptyTables,
                                           changeResult.requiresSnapshotIsolation ? 1 : 0,
                                           changeResult.worksWithElastic ? 1 : 0,
                                           changeResult.deploymentHash);
                            task.clientHandle = changeResult.clientHandle;
                            // DR stuff
                            task.type = changeResult.invocationType;
                            task.originalTxnId = changeResult.originalTxnId;
                            task.originalUniqueId = changeResult.originalUniqueId;

                            ClientResponseImpl error = null;
                            if ((error = m_permissionValidator.shouldAccept(task.procName, result.user, task,
                                    SystemProcedureCatalog.listing.get(task.procName).asCatalogProcedure())) != null) {
                                ByteBuffer buffer = ByteBuffer.allocate(error.getSerializedSize() + 4);
                                buffer.putInt(buffer.capacity() - 4);
                                error.flattenToBuffer(buffer).flip();
                                c.writeStream().enqueue(buffer);
                            }
                            else {
                                /*
                                 * Round trip the invocation to initialize it for command logging
                                 */
                                try {
                                    task = MiscUtils.roundTripForCL(task);
                                } catch (Exception e) {
                                    hostLog.fatal(e);
                                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                                }

                                // initiate the transaction. These hard-coded values from catalog
                                // procedure are horrible, horrible, horrible.
                                createTransaction(changeResult.connectionId,
                                        task, false, false, false, 0, task.getSerializedSize(),
                                        System.nanoTime());
                            }
                        }
                    }
                    else {
                        throw new RuntimeException(
                                "Should not be able to get here (ClientInterface.checkForFinishedCompilerWork())");
                    }
                }
                else {
                    ClientResponseImpl errorResponse =
                        new ClientResponseImpl(
                                ClientResponseImpl.GRACEFUL_FAILURE,
                                new VoltTable[0], result.errorMsg,
                                result.clientHandle);
                    ByteBuffer buf = ByteBuffer.allocate(errorResponse.getSerializedSize() + 4);
                    buf.putInt(buf.capacity() - 4);
                    errorResponse.flattenToBuffer(buf);
                    buf.flip();
                    c.writeStream().enqueue(buf);
                }
            }
        }, null);
        if (c != null) {
            c.queueTask(ft);
        }

        /*
         * Add error handling in case of an unexpected exception
         */
        ft.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                     ft.get();
                } catch (Exception e) {
                    String realReason = result.errorMsg;
                    // Prefer adding detail to reporting an anonymous exception.
                    // This helped debugging when it caught a programming error
                    // -- not sure if this ever should catch anything in production code
                    // that could be explained in friendlier user terms.
                    // In that case, the root cause stack trace might be more of a distraction.
                    if (realReason == null) {
                        StringWriter sw = new StringWriter();
                        PrintWriter pw = new PrintWriter(sw);
                        e.printStackTrace(pw);
                        Throwable cause = e.getCause();
                        if (cause != null) {
                            cause.printStackTrace(pw);
                        }
                        pw.flush();
                        realReason = sw.toString();
                    }
                    ClientResponseImpl errorResponse =
                            new ClientResponseImpl(
                                    ClientResponseImpl.UNEXPECTED_FAILURE,
                                    new VoltTable[0], realReason,
                                    result.clientHandle);
                    ByteBuffer buf = ByteBuffer.allocate(errorResponse.getSerializedSize() + 4);
                    buf.putInt(buf.capacity() - 4);
                    errorResponse.flattenToBuffer(buf);
                    buf.flip();
                    c.writeStream().enqueue(buf);
                }
            }
        }, CoreUtils.SAMETHREADEXECUTOR);

        //Return the future task for test code
        return ft;
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

    /*
     * A predicate to allow the client notifier to skip clients
     * that don't want a specific kind of update
     */
    private final Predicate<ClientInterfaceHandleManager> m_wantsTopologyUpdatesPredicate =
            new Predicate<ClientInterfaceHandleManager>() {
                @Override
                public boolean apply(ClientInterfaceHandleManager input) {
                    return input.wantsTopologyUpdates();
                }};

    /*
     * Submit a task to the stats agent to retrieve the topology. Supply a dummy
     * client response adapter to fake a connection. The adapter converts the response
     * to a listenable future and we add a listener to pick up the resulting topology
     * and check if it has changed. If it has changed, queue a task to the notifier
     * to propagate the update to clients.
     */
    private void checkForTopologyChanges() {
        final Pair<SimpleClientResponseAdapter, ListenableFuture<ClientResponseImpl>> p =
                SimpleClientResponseAdapter.getAsListenableFuture();
        final ListenableFuture<ClientResponseImpl> fut = p.getSecond();
        fut.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                    final ClientResponseImpl r = fut.get();
                    if (r.getStatus() != ClientResponse.SUCCESS) {
                        hostLog.warn("Received error response retrieving topology: " + r.getStatusString());
                        return;
                    }

                    final int size = r.getSerializedSize();
                    final ByteBuffer buf = ByteBuffer.allocate(size + 4);
                    buf.putInt(size);
                    r.flattenToBuffer(buf);
                    buf.flip();

                    //Check for no change
                    ByteBuffer oldValue = null;
                    DeferredSerialization ds = m_currentTopologyValues.get();
                    if (ds != null) {
                        oldValue = ByteBuffer.allocate(ds.getSerializedSize());
                        ds.serialize(oldValue);
                    }

                    if (buf.equals(oldValue)) return;

                    m_currentTopologyValues.set(new DeferredSerialization() {
                        @Override
                        public void serialize(ByteBuffer outbuf) throws IOException {
                            outbuf.put(buf.duplicate());
                        }
                        @Override
                        public void cancel() {}

                        @Override
                        public int getSerializedSize() {
                            return buf.remaining();
                        }
                    });
                    if (oldValue != null) {
                        m_notifier.queueNotification(
                                m_cihm.values(),
                                m_currentTopologySupplier,
                                m_wantsTopologyUpdatesPredicate);
                    }

                } catch (Throwable t) {
                    hostLog.error("Error checking for topology updates", Throwables.getRootCause(t));
                }
            }
        }, CoreUtils.SAMETHREADEXECUTOR);
        final StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setProcName("@Statistics");
        spi.setParams("TOPO", 0);
        spi.setClientHandle(ASYNC_TOPO_HANDLE);
        dispatchStatistics(OpsSelector.STATISTICS, spi, p.getFirst());
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
        if (m_snapshotDaemon != null) {
            m_snapshotDaemon.shutdown();
        }
        if (m_localReplicasBuilder != null) {
            m_localReplicasBuilder.join(10000);
            if (m_localReplicasBuilder.isAlive()) {
                hostLog.error("Local replica map builder took more than ten seconds, probably hung");
            }
            m_localReplicasBuilder.join();
        }
        m_notifier.shutdown();
    }

    private volatile Thread m_localReplicasBuilder = null;
    public void startAcceptingConnections() throws IOException {
        /*
         * This does a ZK lookup which apparently is full of fail
         * if you run TestRejoinEndToEnd. Kind of lame, but initializing this data
         * immediately is not critical, request routing works without it.
         *
         * Populate the map in the background and it will be used to route
         * requests to local replicas once the info is available
         */
        m_localReplicasBuilder = new Thread() {
            @Override
            public void run() {
                    /*
                     * Assemble a map of all local replicas that will be used to determine
                     * if single part reads can be delivered and executed at local replicas
                     */
                final int thisHostId = CoreUtils.getHostIdFromHSId(m_mailbox.getHSId());
                ImmutableMap.Builder<Integer, Long> localReplicas = ImmutableMap.builder();
                for (int partition : m_cartographer.getPartitions()) {
                    for (Long replica : m_cartographer.getReplicasForPartition(partition)) {
                        if (CoreUtils.getHostIdFromHSId(replica) == thisHostId) {
                            localReplicas.put(partition, replica);
                        }
                    }
                }
                m_localReplicas = localReplicas.build();
            }
        };
        m_localReplicasBuilder.start();

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
        m_notifier.start();
    }

    /**
     * Identify the partition for an execution site task.
     * @return The partition best set up to execute the procedure.
     * @throws Exception
     */
    static int getPartitionForProcedure(int partitionIndex, VoltType partitionType,
                                        StoredProcedureInvocation task)
            throws Exception
    {
        Object invocationParameter = task.getParameterAtIndex(partitionIndex);
        return TheHashinator.getPartitionForParameter(partitionType, invocationParameter);
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
        spi.procName = procedureName;
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
            dispatchStatistics(OpsSelector.SNAPSHOTSCAN, spi, m_snapshotDaemonAdapter);
            return;
        }
        else if (procedureName.equals("@SnapshotDelete")) {
            dispatchStatistics(OpsSelector.SNAPSHOTDELETE, spi, m_snapshotDaemonAdapter);
            return;
        }
        // initiate the transaction
        createTransaction(m_snapshotDaemonAdapter.connectionId(),
                spi, catProc.getReadonly(),
                catProc.getSinglepartition(), catProc.getEverysite(),
                0,
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
     * @param txnId
     * @param partitionId
     */
    public void sendSentinel(long txnId, int partitionId) {
        final long initiatorHSId = m_cartographer.getHSIdForSinglePartitionMaster(partitionId);
        sendSentinel(txnId, initiatorHSId, -1, -1, true);
    }

    private void sendSentinel(long txnId, long initiatorHSId, long ciHandle,
                              long connectionId, boolean forReplay) {
        //The only field that is relevant is txnid, and forReplay.
        MultiPartitionParticipantMessage mppm =
                new MultiPartitionParticipantMessage(
                        m_siteId,
                        initiatorHSId,
                        txnId,
                        ciHandle,
                        connectionId,
                        false,  // isReadOnly
                        forReplay);  // isForReplay
        m_mailbox.send(initiatorHSId, mppm);
    }

    /**
     * Sends an end of log message to the master of that partition. This should
     * only be called at the end of replay.
     *
     * @param partitionId
     */
    public void sendEOLMessage(int partitionId) {
        final long initiatorHSId = m_cartographer.getHSIdForMaster(partitionId);
        Iv2EndOfLogMessage message = new Iv2EndOfLogMessage(partitionId);
        m_mailbox.send(initiatorHSId, message);
    }

    public List<Iterator<Map.Entry<Long, Map<String, InvocationInfo>>>> getIV2InitiatorStats() {
        ArrayList<Iterator<Map.Entry<Long, Map<String, InvocationInfo>>>> statsIterators =
                new ArrayList<Iterator<Map.Entry<Long, Map<String, InvocationInfo>>>>();
        for(AdmissionControlGroup acg : m_allACGs) {
            statsIterators.add(acg.getInitiationStatsIterator());
        }
        return statsIterators;
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
        String errorMessage = "Error sending procedure " + task.procName
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

}
